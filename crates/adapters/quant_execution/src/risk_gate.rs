// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Adapter bridging Nautilus [`TradingCommand`] and [`Cache`] to [`RustPlanGateEngine`]
//! via the [`PreTradeRiskGate`] trait.

use std::str::FromStr;

use nautilus_common::{
    cache::Cache,
    messages::execution::{ModifyOrder, TradingCommand},
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{
        AggregationSource, ContingencyType, OrderSide, OrderStatus, OrderType, PositionSide,
        PriceType,
    },
    identifiers::{ClientOrderId, InstrumentId},
    orders::{Order, OrderAny},
};
use nautilus_risk::PreTradeRiskGate;
use rust_decimal::Decimal;

use crate::{
    engine::RustPlanGateEngine,
    models::{RustPlanGateOrderRequest, RustPlanItemEnvelope},
};

const EXIT_INTENT_PARAM_KEYS: [&str; 3] = ["intent", "order_intent", "position_intent"];
const EXIT_INTENT_VALUES: [&str; 7] = [
    "EXIT",
    "CLOSE",
    "REDUCE",
    "STOP",
    "TAKE_PROFIT",
    "TP",
    "FLATTEN",
];
const EXIT_TAG_VALUES: [&str; 6] = [
    "EXIT",
    "CLOSE",
    "STOP_LOSS",
    "TAKE_PROFIT",
    "TP",
    "SAFE_EXIT",
];
const TAKE_PROFIT_VALUES: [&str; 2] = ["TAKE_PROFIT", "TP"];
const SAFE_EXIT_CACHE_PREFIX: &str = "__nautilus_safe_exit__";

fn normalize_symbol(instrument_id: &InstrumentId) -> String {
    instrument_id.symbol.as_str().trim().to_ascii_uppercase()
}

fn is_stop_order_type(order_type: OrderType) -> bool {
    matches!(
        order_type,
        OrderType::StopMarket
            | OrderType::StopLimit
            | OrderType::TrailingStopMarket
            | OrderType::TrailingStopLimit
    )
}

fn can_act_as_take_profit(order_type: OrderType) -> bool {
    !matches!(order_type, OrderType::StopMarket | OrderType::StopLimit)
}

fn is_effective_take_profit(order: &OrderAny) -> bool {
    can_act_as_take_profit(order.order_type()) && has_take_profit_intent(order)
}

fn is_opposite_to_plan_side(
    plan_item: Option<&RustPlanItemEnvelope>,
    order_side: OrderSide,
) -> bool {
    match plan_item {
        None => true,
        Some(item) if item.side.eq_ignore_ascii_case("LONG") => order_side == OrderSide::Sell,
        Some(item) if item.side.eq_ignore_ascii_case("SHORT") => order_side == OrderSide::Buy,
        Some(_) => false,
    }
}

fn expected_plan_entry_side(plan_item: &RustPlanItemEnvelope) -> Option<OrderSide> {
    if plan_item.side.eq_ignore_ascii_case("LONG") {
        Some(OrderSide::Buy)
    } else if plan_item.side.eq_ignore_ascii_case("SHORT") {
        Some(OrderSide::Sell)
    } else {
        None
    }
}

fn open_opposite_pos_qty(
    cache: &Cache,
    instrument_id: &InstrumentId,
    order_side: OrderSide,
) -> Decimal {
    cache
        .positions_open(None, Some(instrument_id), None, None, None)
        .into_iter()
        .filter(|pos| {
            matches!(
                (order_side, pos.side),
                (OrderSide::Sell, PositionSide::Long) | (OrderSide::Buy, PositionSide::Short)
            )
        })
        .map(|pos| pos.quantity.as_decimal())
        .sum()
}

/// Returns `true` only when `a` and `b` are distinct orders sharing the same bilateral
/// contingency type (`Oco`/`Oco` or `Ouo`/`Ouo`) and mutually listing each other in `linked_order_ids`.
fn is_mutually_oco_or_ouo_linked(a: &OrderAny, b: &OrderAny) -> bool {
    if a.client_order_id() == b.client_order_id() {
        return false;
    }
    let same_contingency = matches!(
        (a.contingency_type(), b.contingency_type()),
        (Some(ContingencyType::Oco), Some(ContingencyType::Oco))
            | (Some(ContingencyType::Ouo), Some(ContingencyType::Ouo))
    );
    if !same_contingency {
        return false;
    }
    let a_links_b = a
        .linked_order_ids()
        .is_some_and(|ids| ids.contains(&b.client_order_id()));
    let b_links_a = b
        .linked_order_ids()
        .is_some_and(|ids| ids.contains(&a.client_order_id()));
    a_links_b && b_links_a
}

/// Returns `true` when `child` is legitimately OTO-linked to `parent` via both
/// `child.parent_order_id == Some(parent.client_order_id)` and `parent.contingency_type == Oto`.
fn is_oto_child_of(child: &OrderAny, parent: &OrderAny) -> bool {
    child.parent_order_id() == Some(parent.client_order_id())
        && parent.contingency_type() == Some(ContingencyType::Oto)
        && parent
            .linked_order_ids()
            .is_some_and(|ids| ids.contains(&child.client_order_id()))
}

fn get_param_str<'a, I, K, V>(params: I, key: &str) -> Option<&'a str>
where
    I: IntoIterator<Item = (&'a K, &'a V)>,
    K: AsRef<str> + 'a,
    V: AsRef<str> + 'a,
{
    params
        .into_iter()
        .find(|(k, _)| k.as_ref() == key)
        .map(|(_, v)| v.as_ref())
}

fn has_take_profit_intent(order: &OrderAny) -> bool {
    if let Some(params) = order.exec_algorithm_params() {
        for key in EXIT_INTENT_PARAM_KEYS {
            if let Some(val) = get_param_str(params, key)
                && TAKE_PROFIT_VALUES
                    .iter()
                    .any(|expected| val.trim().eq_ignore_ascii_case(expected))
            {
                return true;
            }
        }
    }
    if let Some(tags) = order.tags() {
        for tag in tags {
            if TAKE_PROFIT_VALUES
                .iter()
                .any(|expected| tag.as_str().trim().eq_ignore_ascii_case(expected))
            {
                return true;
            }
        }
    }
    false
}

fn has_exit_intent(order: &OrderAny) -> bool {
    if order.is_reduce_only() {
        return true;
    }
    if let Some(params) = order.exec_algorithm_params() {
        for key in EXIT_INTENT_PARAM_KEYS {
            if let Some(val) = get_param_str(params, key)
                && EXIT_INTENT_VALUES
                    .iter()
                    .any(|expected| val.trim().eq_ignore_ascii_case(expected))
            {
                return true;
            }
        }
    }
    if let Some(tags) = order.tags() {
        for tag in tags {
            if EXIT_TAG_VALUES
                .iter()
                .any(|expected| tag.as_str().trim().eq_ignore_ascii_case(expected))
            {
                return true;
            }
        }
    }
    false
}

fn is_safe_exit_active(cache: &Cache, norm_sym: &str) -> bool {
    let key = format!("{SAFE_EXIT_CACHE_PREFIX}{norm_sym}");
    cache
        .get(&key)
        .ok()
        .flatten()
        .is_some_and(|bytes| bytes.as_ref() == b"1")
}

fn parse_param_decimal(order: &OrderAny, keys: &[&str]) -> Option<Decimal> {
    let params = order.exec_algorithm_params()?;
    for key in keys {
        if let Some(raw) = get_param_str(params, key)
            && let Ok(val) = Decimal::from_str(raw.trim())
        {
            return Some(val);
        }
    }
    None
}

fn resolve_eval_timestamp(order: &OrderAny, ts_now: UnixNanos) -> (i64, String) {
    let effective = if ts_now.as_u64() > 0 {
        ts_now
    } else if order.ts_init().as_u64() > 0 {
        order.ts_init()
    } else if let Some(raw) = order
        .exec_algorithm_params()
        .and_then(|p| get_param_str(p, "evaluation_ts_ns"))
        .and_then(|v| v.trim().parse::<u64>().ok())
    {
        UnixNanos::from(raw)
    } else {
        UnixNanos::default()
    };
    let ts_i64 = i64::try_from(effective.as_u64()).unwrap_or(i64::MAX);
    (ts_i64, effective.to_rfc3339())
}

fn latest_bar_close(cache: &Cache, instrument_id: &InstrumentId) -> Option<Decimal> {
    let mut best: Option<(UnixNanos, Decimal)> = None;
    for source in [AggregationSource::External, AggregationSource::Internal] {
        for bar_type in cache.bar_types(Some(instrument_id), None, source) {
            if let Some(bar) = cache.bar(bar_type) {
                let close = bar.close.as_decimal();
                if close > Decimal::ZERO
                    && best.is_none_or(|(best_ts, _)| bar.ts_init >= best_ts)
                {
                    best = Some((bar.ts_init, close));
                }
            }
        }
    }
    best.map(|(_, close)| close)
}

fn resolve_reference_price(order: &OrderAny, cache: &Cache) -> Option<Decimal> {
    let inst_id = order.instrument_id();
    cache
        .price(&inst_id, PriceType::Mid)
        .map(|p| p.as_decimal())
        .filter(|d| *d > Decimal::ZERO)
        .or_else(|| {
            cache
                .trade(&inst_id)
                .map(|t| t.price.as_decimal())
                .filter(|d| *d > Decimal::ZERO)
        })
        .or_else(|| latest_bar_close(cache, &inst_id))
        .or_else(|| parse_param_decimal(order, &["reference_price"]).filter(|d| *d > Decimal::ZERO))
}

fn resolve_atr(order: &OrderAny, plan_item: Option<&RustPlanItemEnvelope>) -> Option<Decimal> {
    parse_param_decimal(order, &["atr"])
        .filter(|d| *d > Decimal::ZERO)
        .or_else(|| plan_item.and_then(|item| item.daily_atr_14).filter(|d| *d > Decimal::ZERO))
}

fn same_instrument_or_symbol(a: &InstrumentId, b: &InstrumentId) -> bool {
    a == b || (a.venue == b.venue && normalize_symbol(a) == normalize_symbol(b))
}

fn merge_worst_stop(
    entry_side: OrderSide,
    current: Option<Decimal>,
    candidate: Option<Decimal>,
) -> Option<Decimal> {
    match (current, candidate) {
        (Some(cur), Some(cand)) => Some(match entry_side {
            OrderSide::Buy => cur.min(cand),
            OrderSide::Sell => cur.max(cand),
        }),
        (Some(cur), None) => Some(cur),
        (None, Some(cand)) => Some(cand),
        (None, None) => None,
    }
}

/// Sums remaining `leaves_qty` across unique active opposite-side OTO parent entry orders of `order`.
fn active_opposite_linked_entry_leaves_qty(
    order: &OrderAny,
    cache: &Cache,
    exclude_cids: &[ClientOrderId],
) -> Decimal {
    let inst_id = order.instrument_id();
    let side = order.order_side();
    let mut seen: Vec<ClientOrderId> = Vec::new();
    let mut sum = Decimal::ZERO;

    for linked_cid in order
        .parent_order_id()
        .into_iter()
        .chain(order.linked_order_ids().into_iter().flatten().copied())
    {
        if seen.contains(&linked_cid) || exclude_cids.contains(&linked_cid) {
            continue;
        }
        seen.push(linked_cid);
        if let Some(linked) = cache.order(&linked_cid)
            && is_oto_child_of(order, &linked)
            && !has_exit_intent(&linked)
            && same_instrument_or_symbol(&linked.instrument_id(), &inst_id)
            && linked.order_side() != side
            && !linked.is_closed()
        {
            let leaves = linked.leaves_qty().as_decimal();
            if leaves > Decimal::ZERO {
                sum += leaves;
            }
        }
    }

    sum
}

fn has_active_opposite_linked_entry(order: &OrderAny, cache: &Cache) -> bool {
    active_opposite_linked_entry_leaves_qty(order, cache, &[]) > Decimal::ZERO
}

fn is_active_exit_order(order: &OrderAny) -> bool {
    !order.is_closed()
}

fn is_active_entry_order(order: &OrderAny) -> bool {
    !order.is_closed()
        && !matches!(
            order.status(),
            OrderStatus::PendingCancel | OrderStatus::Initialized
        )
}

fn is_active_unlinked_exit_sibling(
    engine: &RustPlanGateEngine,
    order: &OrderAny,
    existing: &OrderAny,
    exclude_cids: &[ClientOrderId],
) -> bool {
    if existing.client_order_id() == order.client_order_id()
        || exclude_cids.contains(&existing.client_order_id())
    {
        return false;
    }
    if existing.status() == OrderStatus::Initialized
        && existing.parent_order_id().is_none()
        && !engine.is_admitted_exit(existing.client_order_id().as_str())
    {
        return false;
    }
    if !has_exit_intent(existing) || !is_active_exit_order(existing) {
        return false;
    }
    !is_mutually_oco_or_ouo_linked(order, existing)
}

fn is_valid_opposite_oto_parent(child: &OrderAny, parent: &OrderAny) -> bool {
    is_oto_child_of(child, parent)
        && !has_exit_intent(parent)
        && same_instrument_or_symbol(&parent.instrument_id(), &child.instrument_id())
        && parent.order_side() != child.order_side()
        && !parent.is_closed()
        && parent.leaves_qty().as_decimal() > Decimal::ZERO
}

fn active_opposite_oto_parent(
    child: &OrderAny,
    cache: &Cache,
    extra_orders: &[&OrderAny],
) -> Option<(ClientOrderId, Decimal, Decimal)> {
    let parent_cid = child.parent_order_id()?;
    if let Some(parent) = extra_orders
        .iter()
        .copied()
        .find(|o| o.client_order_id() == parent_cid)
    {
        return is_valid_opposite_oto_parent(child, parent).then(|| {
            (
                parent.client_order_id(),
                parent.leaves_qty().as_decimal(),
                parent.filled_qty().as_decimal(),
            )
        });
    }
    let parent = cache.order(&parent_cid)?;
    is_valid_opposite_oto_parent(child, &parent).then(|| {
        (
            parent.client_order_id(),
            parent.leaves_qty().as_decimal(),
            parent.filled_qty().as_decimal(),
        )
    })
}

fn is_targets_own_oto_parent(target: &OrderAny, parent_cid: ClientOrderId) -> bool {
    target.parent_order_id() == Some(parent_cid)
        || target
            .linked_order_ids()
            .is_some_and(|ids| ids.contains(&parent_cid))
}

fn is_prefill_order_status(order: &OrderAny) -> bool {
    match order.status() {
        OrderStatus::Initialized | OrderStatus::Submitted => true,
        OrderStatus::PendingUpdate => matches!(
            order.previous_status(),
            Some(OrderStatus::Initialized | OrderStatus::Submitted)
        ),
        _ => false,
    }
}

fn is_prefill_oto_child(
    order: &OrderAny,
    cache: &Cache,
    extra_orders: &[&OrderAny],
) -> bool {
    is_active_exit_order(order)
        && order.filled_qty().as_decimal() == Decimal::ZERO
        && active_opposite_oto_parent(order, cache, extra_orders).is_some_and(
            |(_, parent_leaves, parent_filled)| {
                parent_leaves > Decimal::ZERO && parent_filled == Decimal::ZERO
            },
        )
}

fn effective_oco_ouo_group_exposure(items: &[(&OrderAny, Decimal)]) -> Decimal {
    let mut cliques: Vec<(Vec<&OrderAny>, Decimal)> = Vec::new();
    for &(order, leaves) in items {
        if leaves <= Decimal::ZERO {
            continue;
        }
        if let Some((members, max_leaves)) = cliques
            .iter_mut()
            .find(|(members, _)| members.iter().all(|m| is_mutually_oco_or_ouo_linked(m, order)))
        {
            members.push(order);
            if leaves > *max_leaves {
                *max_leaves = leaves;
            }
        } else {
            cliques.push((vec![order], leaves));
        }
    }
    cliques.into_iter().map(|(_, max_leaves)| max_leaves).sum()
}

/// Sums active unlinked exit `leaves_qty` across `cache` (excluding `exclude_cids`) and
/// `extra_siblings`, collapsing mutual OCO/OUO cliques and factoring out exits backed by a
/// different active opposite-side OTO parent (only adding spillover above that parent's `leaves_qty`).
fn open_unlinked_exit_qty_with_oto_spillover(
    engine: &RustPlanGateEngine,
    target: &OrderAny,
    cache: &Cache,
    exclude_cids: &[ClientOrderId],
    extra_siblings: &[(&OrderAny, Decimal)],
    extra_orders: &[&OrderAny],
) -> Decimal {
    let inst_id = target.instrument_id();
    let side = target.order_side();
    let cached_refs = cache.orders(None, Some(&inst_id), None, None, Some(side));
    let target_is_prefill_oto = is_prefill_oto_child(target, cache, extra_orders);

    let mut direct_siblings: Vec<(&OrderAny, Decimal)> = Vec::new();
    let mut by_other_parent: ahash::AHashMap<ClientOrderId, (Decimal, Vec<(&OrderAny, Decimal)>)> =
        ahash::AHashMap::new();

    let all_siblings = cached_refs
        .iter()
        .filter_map(|existing| {
            let ord: &OrderAny = existing;
            is_active_unlinked_exit_sibling(engine, target, ord, exclude_cids)
                .then(|| (ord, ord.leaves_qty().as_decimal()))
        })
        .chain(extra_siblings.iter().copied().filter(|&(other, _)| {
            other.client_order_id() != target.client_order_id()
                && same_instrument_or_symbol(&other.instrument_id(), &inst_id)
                && other.order_side() == side
                && has_exit_intent(other)
                && is_active_exit_order(other)
                && !is_mutually_oco_or_ouo_linked(target, other)
        }));

    for (sibling, leaves) in all_siblings {
        let sibling_prefill_eligible = if target.is_reduce_only() {
            sibling.filled_qty().as_decimal() == Decimal::ZERO
        } else {
            is_prefill_order_status(sibling)
        };
        if let Some((parent_cid, parent_leaves, parent_filled)) =
            active_opposite_oto_parent(sibling, cache, extra_orders)
            && !is_targets_own_oto_parent(target, parent_cid)
            && ((parent_filled == Decimal::ZERO && sibling_prefill_eligible)
                || (target_is_prefill_oto && parent_filled > Decimal::ZERO))
        {
            let entry = by_other_parent
                .entry(parent_cid)
                .or_insert_with(|| (parent_leaves, Vec::new()));
            entry.1.push((sibling, leaves));
        } else {
            direct_siblings.push((sibling, leaves));
        }
    }

    let mut direct_unlinked_leaves = effective_oco_ouo_group_exposure(&direct_siblings);

    for (_, (parent_leaves, children)) in by_other_parent {
        let exposure = effective_oco_ouo_group_exposure(&children);
        let spillover = (exposure - parent_leaves).max(Decimal::ZERO);
        direct_unlinked_leaves += spillover;
    }

    direct_unlinked_leaves
}

fn batch_linked_entry_qty(order: &OrderAny, orders: &[OrderAny]) -> Decimal {
    let inst_id = order.instrument_id();
    let side = order.order_side();
    orders
        .iter()
        .filter(|other| {
            other.client_order_id() != order.client_order_id()
                && same_instrument_or_symbol(&other.instrument_id(), &inst_id)
                && other.order_side() != side
                && !has_exit_intent(other)
                && other.quantity().as_decimal() > Decimal::ZERO
                && is_oto_child_of(order, other)
        })
        .map(|other| other.quantity().as_decimal())
        .sum()
}

fn has_open_unlinked_exit(
    engine: &RustPlanGateEngine,
    order: &OrderAny,
    cache: &Cache,
    exclude_cids: &[ClientOrderId],
) -> bool {
    let inst_id = order.instrument_id();
    let side = order.order_side();
    cache
        .orders(None, Some(&inst_id), None, None, Some(side))
        .into_iter()
        .any(|existing| {
            is_active_unlinked_exit_sibling(engine, order, &existing, exclude_cids)
                && existing.leaves_qty().as_decimal() > Decimal::ZERO
        })
}

fn has_batch_unlinked_exit(order: &OrderAny, orders: &[OrderAny]) -> bool {
    let inst_id = order.instrument_id();
    let side = order.order_side();
    orders.iter().any(|other| {
        other.client_order_id() != order.client_order_id()
            && same_instrument_or_symbol(&other.instrument_id(), &inst_id)
            && other.order_side() == side
            && has_exit_intent(other)
            && other.quantity().as_decimal() > Decimal::ZERO
            && !is_mutually_oco_or_ouo_linked(order, other)
    })
}

fn has_post_batch_unlinked_exit(order: &OrderAny, post_batch: &[(&OrderAny, Decimal)]) -> bool {
    let inst_id = order.instrument_id();
    let side = order.order_side();
    post_batch.iter().any(|&(other, leaves)| {
        other.client_order_id() != order.client_order_id()
            && same_instrument_or_symbol(&other.instrument_id(), &inst_id)
            && other.order_side() == side
            && has_exit_intent(other)
            && is_active_exit_order(other)
            && leaves > Decimal::ZERO
            && !is_mutually_oco_or_ouo_linked(order, other)
    })
}

fn check_active_oto_child_cap(
    order: &OrderAny,
    target_qty: Decimal,
    cache: &Cache,
    exclude_cids: &[ClientOrderId],
    extra_orders: &[&OrderAny],
    extra_siblings: &[(&OrderAny, Decimal)],
    is_modify: bool,
) -> Result<(), String> {
    let Some((parent_cid, parent_leaves, parent_filled)) =
        active_opposite_oto_parent(order, cache, extra_orders)
    else {
        return Ok(());
    };
    let parent_total_qty = parent_filled + parent_leaves;
    let inst_id = order.instrument_id();
    let side = order.order_side();

    let target_filled = if is_modify {
        order.filled_qty().as_decimal()
    } else {
        Decimal::ZERO
    };
    let order_new_leaves = (target_qty - target_filled).max(Decimal::ZERO);
    let mut filled_exposure = target_filled;
    let mut same_parent_items: Vec<(&OrderAny, Decimal)> = Vec::new();
    let cached_refs = cache.orders(None, Some(&inst_id), None, None, Some(side));
    for existing in &cached_refs {
        let ord: &OrderAny = existing;
        if ord.client_order_id() == order.client_order_id()
            || exclude_cids.contains(&ord.client_order_id())
            || !has_exit_intent(ord)
        {
            continue;
        }
        let Some((sib_parent_cid, _, _)) =
            active_opposite_oto_parent(ord, cache, extra_orders)
        else {
            continue;
        };
        if sib_parent_cid != parent_cid {
            continue;
        }
        let ord_filled = ord.filled_qty().as_decimal();
        filled_exposure += ord_filled;
        if is_active_exit_order(ord) {
            let leaves = ord.leaves_qty().as_decimal();
            let contrib_leaves = if is_modify
                && is_mutually_oco_or_ouo_linked(order, ord)
                && target_filled == Decimal::ZERO
                && ord_filled == Decimal::ZERO
            {
                leaves.min(order_new_leaves)
            } else {
                leaves
            };
            same_parent_items.push((ord, contrib_leaves));
        }
    }
    for &(other, sibling_qty) in extra_siblings {
        if other.client_order_id() == order.client_order_id()
            || !same_instrument_or_symbol(&other.instrument_id(), &inst_id)
            || other.order_side() != side
            || !has_exit_intent(other)
        {
            continue;
        }
        if let Some((sib_parent_cid, _, _)) =
            active_opposite_oto_parent(other, cache, extra_orders)
            && sib_parent_cid == parent_cid
        {
            let other_filled = other.filled_qty().as_decimal();
            filled_exposure += other_filled;
            if is_active_exit_order(other) {
                let sib_leaves = (sibling_qty - other_filled).max(Decimal::ZERO);
                same_parent_items.push((other, sib_leaves));
            }
        }
    }
    same_parent_items.push((order, order_new_leaves));

    let group_exposure =
        filled_exposure + effective_oco_ouo_group_exposure(&same_parent_items);
    if group_exposure > parent_total_qty {
        let reason_code = if is_modify {
            "REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"
        } else {
            "REDUCE_ONLY_EXIT_EXCEEDS_POSITION"
        };
        return Err(format!(
            "{reason_code}: OTO exit child exposure ({group_exposure}) > linked_entry_qty ({parent_total_qty})"
        ));
    }
    Ok(())
}

fn is_verified_exit_for_submit(
    engine: &RustPlanGateEngine,
    order: &OrderAny,
    cache: &Cache,
    plan_item: Option<&RustPlanItemEnvelope>,
) -> Result<bool, String> {
    if order.is_reduce_only() {
        check_active_oto_child_cap(
            order,
            order.quantity().as_decimal(),
            cache,
            &[],
            &[],
            &[],
            false,
        )?;
        let norm_sym = normalize_symbol(&order.instrument_id());
        if is_safe_exit_active(cache, &norm_sym)
            || !has_open_unlinked_exit(engine, order, cache, &[])
        {
            return Ok(true);
        }
        let order_qty = order.quantity().as_decimal();
        if order_qty <= Decimal::ZERO {
            return Err(format!(
                "INVALID_QUANTITY: reduce-only exit quantity must be > 0, got {order_qty}"
            ));
        }
        let open_opp = open_opposite_pos_qty(cache, &order.instrument_id(), order.order_side());
        let linked_entry_qty = active_opposite_linked_entry_leaves_qty(order, cache, &[]);
        let total_opp_capacity = open_opp + linked_entry_qty;
        let existing_unlinked_qty = open_unlinked_exit_qty_with_oto_spillover(
            engine,
            order,
            cache,
            &[],
            &[],
            &[],
        );
        if existing_unlinked_qty + order_qty > total_opp_capacity {
            return Err(format!(
                "REDUCE_ONLY_EXIT_EXCEEDS_POSITION: existing_unlinked_exit_leaves ({existing_unlinked_qty}) + order_qty ({order_qty}) > total_opp_capacity ({total_opp_capacity})"
            ));
        }
        return Ok(true);
    }
    if !has_exit_intent(order) {
        return Ok(false);
    }
    if !is_opposite_to_plan_side(plan_item, order.order_side()) {
        return Ok(false);
    }
    let order_qty = order.quantity().as_decimal();
    if order_qty <= Decimal::ZERO {
        return Ok(false);
    }
    check_active_oto_child_cap(order, order_qty, cache, &[], &[], &[], false)?;
    let open_opp = open_opposite_pos_qty(cache, &order.instrument_id(), order.order_side());
    let total_opp_capacity =
        open_opp + active_opposite_linked_entry_leaves_qty(order, cache, &[]);
    if total_opp_capacity <= Decimal::ZERO || order_qty > total_opp_capacity {
        return Ok(false);
    }
    let norm_sym = normalize_symbol(&order.instrument_id());
    if is_safe_exit_active(cache, &norm_sym) {
        return Ok(true);
    }
    let existing_unlinked_qty =
        open_unlinked_exit_qty_with_oto_spillover(engine, order, cache, &[], &[], &[]);
    Ok(existing_unlinked_qty + order_qty <= total_opp_capacity)
}

fn is_verified_exit_for_batch(
    engine: &RustPlanGateEngine,
    order: &OrderAny,
    orders: &[OrderAny],
    cache: &Cache,
    batch_cids: &[ClientOrderId],
    plan_item: Option<&RustPlanItemEnvelope>,
) -> Result<bool, String> {
    let inst_id = order.instrument_id();
    let side = order.order_side();
    let norm_sym = normalize_symbol(&inst_id);
    let safe_exit = is_safe_exit_active(cache, &norm_sym);
    let extra_orders: Vec<&OrderAny> = orders.iter().collect();
    let extra_siblings: Vec<(&OrderAny, Decimal)> = orders
        .iter()
        .map(|o| (o, o.quantity().as_decimal()))
        .collect();

    if order.is_reduce_only() {
        check_active_oto_child_cap(
            order,
            order.quantity().as_decimal(),
            cache,
            batch_cids,
            &extra_orders,
            &extra_siblings,
            false,
        )?;
        if safe_exit {
            return Ok(true);
        }
        let has_unlinked_exit = has_open_unlinked_exit(engine, order, cache, batch_cids)
            || has_batch_unlinked_exit(order, orders);
        if !has_unlinked_exit {
            return Ok(true);
        }
        let order_qty = order.quantity().as_decimal();
        if order_qty <= Decimal::ZERO {
            return Err(format!(
                "INVALID_QUANTITY: reduce-only exit quantity must be > 0, got {order_qty}"
            ));
        }
        let open_opp = open_opposite_pos_qty(cache, &inst_id, side);
        let total_opp_capacity = open_opp
            + batch_linked_entry_qty(order, orders)
            + active_opposite_linked_entry_leaves_qty(order, cache, batch_cids);
        let combined_unlinked_qty = open_unlinked_exit_qty_with_oto_spillover(
            engine,
            order,
            cache,
            batch_cids,
            &extra_siblings,
            &extra_orders,
        );
        if combined_unlinked_qty + order_qty > total_opp_capacity {
            return Err(format!(
                "REDUCE_ONLY_EXIT_EXCEEDS_POSITION: combined_unlinked_exit_leaves ({combined_unlinked_qty}) + order_qty ({order_qty}) > total_opp_capacity ({total_opp_capacity})"
            ));
        }
        return Ok(true);
    }
    if !has_exit_intent(order) {
        return Ok(false);
    }
    if !is_opposite_to_plan_side(plan_item, side) {
        return Ok(false);
    }
    let order_qty = order.quantity().as_decimal();
    if order_qty <= Decimal::ZERO {
        return Ok(false);
    }
    check_active_oto_child_cap(
        order,
        order_qty,
        cache,
        batch_cids,
        &extra_orders,
        &extra_siblings,
        false,
    )?;
    let open_opp = open_opposite_pos_qty(cache, &inst_id, side);
    let total_opp_capacity = open_opp
        + batch_linked_entry_qty(order, orders)
        + active_opposite_linked_entry_leaves_qty(order, cache, batch_cids);
    if total_opp_capacity <= Decimal::ZERO || order_qty > total_opp_capacity {
        return Ok(false);
    }
    if safe_exit {
        return Ok(true);
    }
    let combined_unlinked_qty = open_unlinked_exit_qty_with_oto_spillover(
        engine,
        order,
        cache,
        batch_cids,
        &extra_siblings,
        &extra_orders,
    );
    Ok(combined_unlinked_qty + order_qty <= total_opp_capacity)
}

fn is_verified_exit_for_modify(
    order: &OrderAny,
    cache: &Cache,
    plan_item: Option<&RustPlanItemEnvelope>,
) -> bool {
    if order.is_reduce_only() {
        return true;
    }
    if !has_exit_intent(order) {
        return false;
    }
    if !is_opposite_to_plan_side(plan_item, order.order_side()) {
        return false;
    }
    open_opposite_pos_qty(cache, &order.instrument_id(), order.order_side()) > Decimal::ZERO
        || has_active_opposite_linked_entry(order, cache)
}

fn eligible_stop_trigger_price(
    candidate: &OrderAny,
    entry_inst: &InstrumentId,
    entry_side: OrderSide,
) -> Option<Decimal> {
    if !same_instrument_or_symbol(&candidate.instrument_id(), entry_inst) {
        return None;
    }
    if candidate.order_side() == entry_side {
        return None;
    }
    if is_effective_take_profit(candidate) {
        return None;
    }
    if !has_exit_intent(candidate) || !is_stop_order_type(candidate.order_type()) {
        return None;
    }
    candidate.trigger_price().map(|p| p.as_decimal())
}

fn linked_stop_price(order: &OrderAny, cache: &Cache) -> Option<Decimal> {
    let linked = order.linked_order_ids()?;
    let order_inst = order.instrument_id();
    let order_side = order.order_side();
    let mut worst: Option<Decimal> = None;
    for cid in linked {
        if let Some(linked_order) = cache.order(cid) {
            let trigger_px = eligible_stop_trigger_price(&linked_order, &order_inst, order_side);
            worst = merge_worst_stop(order_side, worst, trigger_px);
        }
    }
    worst
}

fn is_claimed_by_other_entry(
    candidate: &OrderAny,
    entry_cid: ClientOrderId,
    orders: &[OrderAny],
) -> bool {
    let candidate_cid = candidate.client_order_id();
    let candidate_inst = candidate.instrument_id();
    let candidate_side = candidate.order_side();
    orders.iter().any(|other| {
        other.client_order_id() != entry_cid
            && !has_exit_intent(other)
            && same_instrument_or_symbol(&other.instrument_id(), &candidate_inst)
            && other.order_side() != candidate_side
            && other
                .linked_order_ids()
                .is_some_and(|ids| ids.contains(&candidate_cid))
    })
}

fn is_known_entry_order(
    cid: ClientOrderId,
    candidate: &OrderAny,
    orders: &[OrderAny],
) -> bool {
    let candidate_inst = candidate.instrument_id();
    let candidate_side = candidate.order_side();
    orders.iter().any(|o| {
        o.client_order_id() == cid
            && !has_exit_intent(o)
            && same_instrument_or_symbol(&o.instrument_id(), &candidate_inst)
            && o.order_side() != candidate_side
    })
}

fn find_sibling_stop_price(entry_order: &OrderAny, orders: &[OrderAny]) -> Option<Decimal> {
    let entry_cid = entry_order.client_order_id();
    let entry_inst = entry_order.instrument_id();
    let entry_side = entry_order.order_side();

    let mut parented_worst: Option<Decimal> = None;
    let mut unowned_worst: Option<Decimal> = None;

    for candidate in orders {
        let candidate_cid = candidate.client_order_id();
        if candidate_cid == entry_cid {
            continue;
        }
        let Some(trigger_px) = eligible_stop_trigger_price(candidate, &entry_inst, entry_side)
        else {
            continue;
        };

        if let Some(parent_cid) = candidate.parent_order_id() {
            if parent_cid == entry_cid {
                parented_worst = merge_worst_stop(entry_side, parented_worst, Some(trigger_px));
                continue;
            }
            if is_known_entry_order(parent_cid, candidate, orders) {
                continue;
            }
        }

        if is_claimed_by_other_entry(candidate, entry_cid, orders) {
            continue;
        }

        unowned_worst = merge_worst_stop(entry_side, unowned_worst, Some(trigger_px));
    }

    merge_worst_stop(entry_side, parented_worst, unowned_worst)
}

fn resolve_stop_loss_price(
    order: &OrderAny,
    cache: &Cache,
    sibling_stop_price: Option<Decimal>,
) -> Option<Decimal> {
    let order_side = order.order_side();
    let param_stop = parse_param_decimal(
        order,
        &["stop_loss_price", "initial_stop_price", "stop_price"],
    );
    let linked_stop = linked_stop_price(order, cache);
    let worst = merge_worst_stop(order_side, param_stop, linked_stop);
    merge_worst_stop(order_side, worst, sibling_stop_price)
}

fn compute_cache_entry_qty(
    order: &OrderAny,
    cache: &Cache,
    exclude_cids: &[ClientOrderId],
    plan_item: Option<&RustPlanItemEnvelope>,
) -> (Decimal, Decimal) {
    let mut open_qty = Decimal::ZERO;
    let mut filled_qty = Decimal::ZERO;
    let inst_id = order.instrument_id();
    let side = order.order_side();

    for existing in cache.orders(None, Some(&inst_id), None, None, Some(side)) {
        if exclude_cids.contains(&existing.client_order_id()) {
            continue;
        }
        if existing.is_reduce_only()
            || (has_exit_intent(&existing)
                && is_opposite_to_plan_side(plan_item, existing.order_side()))
        {
            continue;
        }
        if let (Some(item), Some(params)) = (plan_item, existing.exec_algorithm_params()) {
            if let Some(existing_item_id) = get_param_str(params, "item_id")
                && existing_item_id != item.item_id
            {
                continue;
            }
            if let Some(existing_plan_id) = get_param_str(params, "plan_id")
                && existing_plan_id != item.plan_id
            {
                continue;
            }
        }
        let is_active = is_active_entry_order(&existing);
        if !is_active && existing.filled_qty().as_decimal() <= Decimal::ZERO {
            continue;
        }
        filled_qty += existing.filled_qty().as_decimal();
        if is_active {
            open_qty += existing.leaves_qty().as_decimal();
        }
    }

    (open_qty, filled_qty)
}

fn format_order_side(side: OrderSide) -> String {
    match side {
        OrderSide::Buy => "BUY".to_string(),
        OrderSide::Sell => "SELL".to_string(),
    }
}

fn format_order_type(order_type: OrderType) -> String {
    match order_type {
        OrderType::Market => "MARKET".to_string(),
        OrderType::MarketToLimit => "MARKET_TO_LIMIT".to_string(),
        OrderType::Limit => "LIMIT".to_string(),
        OrderType::StopMarket => "STOP_MARKET".to_string(),
        OrderType::StopLimit => "STOP_LIMIT".to_string(),
        OrderType::MarketIfTouched => "MARKET_IF_TOUCHED".to_string(),
        OrderType::LimitIfTouched => "LIMIT_IF_TOUCHED".to_string(),
        OrderType::TrailingStopMarket => "TRAILING_STOP_MARKET".to_string(),
        OrderType::TrailingStopLimit => "TRAILING_STOP_LIMIT".to_string(),
    }
}

fn build_gate_request(
    order: &OrderAny,
    cache: &Cache,
    ts_now: UnixNanos,
    sibling_stop_price: Option<Decimal>,
    exclude_cids: &[ClientOrderId],
    plan_item: Option<&RustPlanItemEnvelope>,
    is_verified_exit: bool,
) -> RustPlanGateOrderRequest {
    let norm_sym = normalize_symbol(&order.instrument_id());
    let (evaluation_ts_ns, evaluation_ts_iso) = resolve_eval_timestamp(order, ts_now);
    let reference_price = resolve_reference_price(order, cache);
    let atr = resolve_atr(order, plan_item);
    let stop_loss_price = if is_verified_exit {
        None
    } else {
        resolve_stop_loss_price(order, cache, sibling_stop_price)
    };
    let (cache_open_entry_qty, cache_filled_entry_qty) = if is_verified_exit {
        (Decimal::ZERO, Decimal::ZERO)
    } else {
        compute_cache_entry_qty(order, cache, exclude_cids, plan_item)
    };

    RustPlanGateOrderRequest {
        client_order_id: order.client_order_id().to_string(),
        strategy_id: order.strategy_id().to_string(),
        instrument_id: order.instrument_id().to_string(),
        symbol: norm_sym,
        venue: order.venue().to_string(),
        order_side: format_order_side(order.order_side()),
        order_type: format_order_type(order.order_type()),
        quantity: order.quantity().as_decimal(),
        is_reduce_only: order.is_reduce_only(),
        evaluation_ts_ns,
        evaluation_ts_iso,
        limit_price: order.price().map(|p| p.as_decimal()),
        reference_price,
        atr,
        // PreTradeRiskGate cannot rewrite a MarketOrder into a LimitOrder; uncollared MARKET entries must fail closed.
        collar_market_order: false,
        is_exit_intent: is_verified_exit,
        cache_open_entry_qty,
        cache_filled_entry_qty,
        stop_loss_price,
    }
}

fn check_modify_exit_capacity(
    order: &OrderAny,
    cache: &Cache,
    open_pos_qty: Decimal,
    other_unlinked_exit_leaves: Decimal,
    new_leaves: Decimal,
) -> Result<(), String> {
    let linked_entry_leaves_qty = if is_prefill_oto_child(order, cache, &[]) {
        active_opposite_linked_entry_leaves_qty(order, cache, &[])
    } else {
        Decimal::ZERO
    };
    let capacity = open_pos_qty + linked_entry_leaves_qty;
    if other_unlinked_exit_leaves + new_leaves > capacity {
        if linked_entry_leaves_qty > Decimal::ZERO {
            return Err(format!(
                "REDUCE_ONLY_MODIFY_EXCEEDS_POSITION: other_unlinked_exit_leaves ({other_unlinked_exit_leaves}) + new_leaves ({new_leaves}) > total_opp_capacity ({capacity})"
            ));
        }
        return Err(format!(
            "REDUCE_ONLY_MODIFY_EXCEEDS_POSITION: other_unlinked_exit_leaves ({other_unlinked_exit_leaves}) + new_leaves ({new_leaves}) > open_pos_qty ({open_pos_qty})"
        ));
    }
    Ok(())
}

fn check_reduce_only_modify_per_order(
    engine: &RustPlanGateEngine,
    modify: &ModifyOrder,
    order: &OrderAny,
    cache: &Cache,
) -> Result<(), String> {
    let inst_id = order.instrument_id();
    let norm_sym = normalize_symbol(&inst_id);

    // 1. Quantity invariant: reduce-only modification cannot exceed open opposite position quantity.
    let new_qty = modify
        .quantity
        .map_or_else(|| order.quantity().as_decimal(), |q| q.as_decimal());
    if new_qty <= Decimal::ZERO {
        return Err(format!(
            "INVALID_QUANTITY: reduce-only modify quantity must be > 0, got {new_qty}"
        ));
    }

    let prefill_oto = is_prefill_oto_child(order, cache, &[]);
    if prefill_oto && new_qty > order.quantity().as_decimal() {
        return Err(format!(
            "REDUCE_ONLY_MODIFY_EXCEEDS_POSITION: new_qty ({new_qty}) > order_qty ({})",
            order.quantity()
        ));
    }

    let open_pos_qty = open_opposite_pos_qty(cache, &inst_id, order.order_side());
    let new_leaves = (new_qty - order.filled_qty().as_decimal()).max(Decimal::ZERO);

    if order.is_reduce_only() {
        if open_pos_qty > Decimal::ZERO {
            let linked_entry_leaves_qty = if prefill_oto {
                active_opposite_linked_entry_leaves_qty(order, cache, &[])
            } else {
                Decimal::ZERO
            };
            let capacity = open_pos_qty + linked_entry_leaves_qty;
            if new_leaves > capacity {
                if linked_entry_leaves_qty > Decimal::ZERO {
                    return Err(format!(
                        "REDUCE_ONLY_MODIFY_EXCEEDS_POSITION: new_leaves ({new_leaves}) > total_opp_capacity ({capacity})"
                    ));
                }
                return Err(format!(
                    "REDUCE_ONLY_MODIFY_EXCEEDS_POSITION: new_leaves ({new_leaves}) > open_pos_qty ({open_pos_qty})"
                ));
            }
        } else {
            if new_qty > order.quantity().as_decimal() {
                return Err(format!(
                    "REDUCE_ONLY_MODIFY_EXCEEDS_POSITION: new_qty ({new_qty}) > order_qty ({})",
                    order.quantity()
                ));
            }
            if modify.quantity.is_some() {
                let order_position_closed = order
                    .position_id()
                    .as_ref()
                    .or_else(|| cache.position_id(&order.client_order_id()))
                    .is_some_and(|pos_id| cache.is_position_closed(pos_id));
                if order_position_closed {
                    return Err(
                        "REDUCE_ONLY_MODIFY_EXCEEDS_POSITION: position already closed".to_string(),
                    );
                }
                if !has_active_opposite_linked_entry(order, cache)
                    && !cache
                        .positions_closed_refs(None, Some(&inst_id), None, None, None)
                        .is_empty()
                {
                    return Err(
                        "REDUCE_ONLY_MODIFY_EXCEEDS_POSITION: position already closed".to_string(),
                    );
                }
            }
        }
    } else if open_pos_qty > Decimal::ZERO {
        let linked_entry_leaves_qty = if prefill_oto {
            active_opposite_linked_entry_leaves_qty(order, cache, &[])
        } else {
            Decimal::ZERO
        };
        let capacity = open_pos_qty + linked_entry_leaves_qty;
        if new_leaves > capacity {
            if linked_entry_leaves_qty > Decimal::ZERO {
                return Err(format!(
                    "REDUCE_ONLY_MODIFY_EXCEEDS_POSITION: new_leaves ({new_leaves}) > total_opp_capacity ({capacity})"
                ));
            }
            return Err(format!(
                "REDUCE_ONLY_MODIFY_EXCEEDS_POSITION: new_leaves ({new_leaves}) > open_pos_qty ({open_pos_qty})"
            ));
        }
    } else {
        let linked_entry_leaves_qty =
            active_opposite_linked_entry_leaves_qty(order, cache, &[]);
        if linked_entry_leaves_qty <= Decimal::ZERO {
            return Err(
                "REDUCE_ONLY_MODIFY_EXCEEDS_POSITION: no open position or active linked entry"
                    .to_string(),
            );
        }
        if new_leaves > linked_entry_leaves_qty {
            return Err(format!(
                "REDUCE_ONLY_MODIFY_EXCEEDS_POSITION: new_leaves ({new_leaves}) > linked_entry_leaves_qty ({linked_entry_leaves_qty})"
            ));
        }
    }

    // 2. Monotonic stop invariant: never allow loosening protective stops.
    if is_stop_order_type(order.order_type())
        && !is_effective_take_profit(order)
        && let Some(new_stop) = modify.trigger_price.map(|p| p.as_decimal())
    {
        if new_stop <= Decimal::ZERO {
            return Err(format!(
                "INVALID_STOP_LOSS_PRICE: trigger_price ({new_stop}) must be > 0"
            ));
        }
        let current_stop = order.trigger_price().map(|p| p.as_decimal());
        let safe_exit = is_safe_exit_active(cache, &norm_sym);
        let plan_item = engine.plan_item_for_symbol(&norm_sym);

        match order.order_side() {
            OrderSide::Sell => {
                if let Some(cur) = current_stop
                    && new_stop < cur
                {
                    return Err(format!(
                        "STOP_LOSS_LOOSENING_PROHIBITED: LONG stop modify {new_stop} < current {cur}"
                    ));
                }
                if !safe_exit
                    && let Some(item) = plan_item.as_ref()
                    && item.initial_stop_price > Decimal::ZERO
                    && current_stop.is_none_or(|cur| cur >= item.initial_stop_price)
                    && new_stop < item.initial_stop_price
                {
                    return Err(format!(
                        "STOP_LOSS_LOOSENING_PROHIBITED: LONG stop modify {new_stop} < initial_stop_price ({})",
                        item.initial_stop_price
                    ));
                }
            }
            OrderSide::Buy => {
                if let Some(cur) = current_stop
                    && new_stop > cur
                {
                    return Err(format!(
                        "STOP_LOSS_LOOSENING_PROHIBITED: SHORT stop modify {new_stop} > current {cur}"
                    ));
                }
                if !safe_exit
                    && let Some(item) = plan_item.as_ref()
                    && item.initial_stop_price > Decimal::ZERO
                    && current_stop.is_none_or(|cur| cur <= item.initial_stop_price)
                    && new_stop > item.initial_stop_price
                {
                    return Err(format!(
                        "STOP_LOSS_LOOSENING_PROHIBITED: SHORT stop modify {new_stop} > initial_stop_price ({})",
                        item.initial_stop_price
                    ));
                }
            }
        }
    }

    Ok(())
}

fn check_reduce_only_modify(
    engine: &RustPlanGateEngine,
    modify: &ModifyOrder,
    order: &OrderAny,
    cache: &Cache,
) -> Result<(), String> {
    check_reduce_only_modify_per_order(engine, modify, order, cache)?;

    let inst_id = order.instrument_id();
    let norm_sym = normalize_symbol(&inst_id);
    let new_qty = modify
        .quantity
        .map_or_else(|| order.quantity().as_decimal(), |q| q.as_decimal());

    check_active_oto_child_cap(order, new_qty, cache, &[], &[], &[], true)?;

    let open_pos_qty = open_opposite_pos_qty(cache, &inst_id, order.order_side());
    let new_leaves = (new_qty - order.filled_qty().as_decimal()).max(Decimal::ZERO);
    let is_non_upward_modify = new_leaves <= order.leaves_qty().as_decimal();

    if order.is_reduce_only() {
        let safe_exit = is_safe_exit_active(cache, &norm_sym);
        let has_unlinked_exit = !safe_exit && has_open_unlinked_exit(engine, order, cache, &[]);
        if has_unlinked_exit {
            if open_pos_qty > Decimal::ZERO {
                if !(is_non_upward_modify && new_leaves <= open_pos_qty) {
                    let other_unlinked_exit_leaves = open_unlinked_exit_qty_with_oto_spillover(
                        engine,
                        order,
                        cache,
                        &[],
                        &[],
                        &[],
                    );
                    check_modify_exit_capacity(
                        order,
                        cache,
                        open_pos_qty,
                        other_unlinked_exit_leaves,
                        new_leaves,
                    )?;
                }
            } else {
                let linked_entry_leaves_qty =
                    active_opposite_linked_entry_leaves_qty(order, cache, &[]);
                let other_unlinked_exit_leaves = open_unlinked_exit_qty_with_oto_spillover(
                    engine,
                    order,
                    cache,
                    &[],
                    &[],
                    &[],
                );
                if other_unlinked_exit_leaves + new_leaves > linked_entry_leaves_qty {
                    return Err(format!(
                        "REDUCE_ONLY_MODIFY_EXCEEDS_POSITION: other_unlinked_exit_leaves ({other_unlinked_exit_leaves}) + new_leaves ({new_leaves}) > linked_entry_leaves_qty ({linked_entry_leaves_qty})"
                    ));
                }
            }
        }
    } else if open_pos_qty > Decimal::ZERO {
        if !(is_non_upward_modify && new_leaves <= open_pos_qty) {
            let other_unlinked_exit_leaves = open_unlinked_exit_qty_with_oto_spillover(
                engine,
                order,
                cache,
                &[],
                &[],
                &[],
            );
            check_modify_exit_capacity(
                order,
                cache,
                open_pos_qty,
                other_unlinked_exit_leaves,
                new_leaves,
            )?;
        }
    } else {
        let other_unlinked_exit_leaves = open_unlinked_exit_qty_with_oto_spillover(
            engine,
            order,
            cache,
            &[],
            &[],
            &[],
        );
        let linked_entry_leaves_qty =
            active_opposite_linked_entry_leaves_qty(order, cache, &[]);
        if other_unlinked_exit_leaves + new_leaves > linked_entry_leaves_qty {
            return Err(format!(
                "REDUCE_ONLY_MODIFY_EXCEEDS_POSITION: other_unlinked_exit_leaves ({other_unlinked_exit_leaves}) + new_leaves ({new_leaves}) > linked_entry_leaves_qty ({linked_entry_leaves_qty})"
            ));
        }
    }

    Ok(())
}

fn check_single_modify(
    engine: &RustPlanGateEngine,
    modify: &ModifyOrder,
    order: &OrderAny,
    cache: &Cache,
    ts_now: UnixNanos,
) -> Result<(), String> {
    let norm_sym = normalize_symbol(&order.instrument_id());
    let plan_item = engine.plan_item_for_symbol(&norm_sym);
    if is_verified_exit_for_modify(order, cache, plan_item.as_ref()) {
        return check_reduce_only_modify(engine, modify, order, cache);
    }

    if let Some(item) = plan_item.as_ref()
        && let Some(expected_side) = expected_plan_entry_side(item)
        && order.order_side() != expected_side
    {
        return Err(format!(
            "SIDE_MISMATCH: order side {} does not match plan side '{}'",
            format_order_side(order.order_side()),
            item.side
        ));
    }

    if has_exit_intent(order) && plan_item.is_none() {
        return Err(format!(
            "NO_LOCKED_PLAN_ITEM: no LOCKED PlanItemContract found for symbol {norm_sym}"
        ));
    }

    let (eval_ts_ns, eval_ts_iso) = resolve_eval_timestamp(order, ts_now);
    let ref_px = resolve_reference_price(order, cache);
    let atr = resolve_atr(order, plan_item.as_ref());

    engine.validate_entry_modify(
        &norm_sym,
        modify.quantity.map(|q| q.as_decimal()),
        order.quantity().as_decimal(),
        modify.price.map(|p| p.as_decimal()),
        ref_px,
        atr,
        eval_ts_ns,
        &eval_ts_iso,
    )
}

fn reconcile_cache_reservations(engine: &RustPlanGateEngine, symbol: &str, cache: &Cache) {
    for (cid_str, admitted_qty) in engine.admitted_reservations_for_symbol(symbol) {
        let cid = ClientOrderId::from(cid_str.as_str());
        let Some(order) = cache.order(&cid) else {
            continue;
        };
        let status = order.status();
        if matches!(
            status,
            OrderStatus::PendingUpdate
                | OrderStatus::Initialized
                | OrderStatus::Submitted
                | OrderStatus::Emulated
                | OrderStatus::Released
        ) {
            continue;
        }
        let filled_qty = order.filled_qty().as_decimal();
        let order_qty = order.quantity().as_decimal();
        if matches!(
            status,
            OrderStatus::Canceled
                | OrderStatus::Expired
                | OrderStatus::Rejected
                | OrderStatus::Denied
        ) {
            engine.reconcile_terminal_order(
                &cid_str,
                symbol,
                filled_qty.min(order_qty).max(Decimal::ZERO),
            );
        } else if order_qty < admitted_qty {
            engine.reconcile_order_updated(&cid_str, symbol, order_qty);
        }
    }
}

fn check_modify_orders_batch(
    engine: &RustPlanGateEngine,
    modifies: &[ModifyOrder],
    cache: &Cache,
) -> Result<(), String> {
    let mut effective_qty_by_cid: ahash::AHashMap<ClientOrderId, Decimal> =
        ahash::AHashMap::new();
    let mut active_orders_by_cid: Vec<(ClientOrderId, OrderAny)> = Vec::new();

    for modify in modifies {
        let Some(order) = cache.order(&modify.client_order_id) else {
            continue;
        };
        if !order.is_reduce_only() {
            return Err("ENTRY_UPWARD_MODIFY_PROHIBITED".to_string());
        }
        check_reduce_only_modify_per_order(engine, modify, &order, cache)?;

        if is_active_exit_order(&order) {
            let cid = order.client_order_id();
            let prev_qty = effective_qty_by_cid
                .get(&cid)
                .copied()
                .unwrap_or_else(|| order.quantity().as_decimal());
            let next_qty = modify.quantity.map_or(prev_qty, |q| q.as_decimal());
            if !effective_qty_by_cid.contains_key(&cid) {
                active_orders_by_cid.push((cid, order.clone()));
            }
            effective_qty_by_cid.insert(cid, next_qty);
        }
    }

    if active_orders_by_cid.is_empty() {
        return Ok(());
    }

    let batch_cids: Vec<ClientOrderId> =
        active_orders_by_cid.iter().map(|(cid, _)| *cid).collect();
    let post_batch: Vec<(&OrderAny, Decimal)> = active_orders_by_cid
        .iter()
        .map(|(cid, order)| {
            let final_qty = effective_qty_by_cid[cid];
            let new_leaves = (final_qty - order.filled_qty().as_decimal()).max(Decimal::ZERO);
            (order, new_leaves)
        })
        .collect();
    let post_batch_qtys: Vec<(&OrderAny, Decimal)> = active_orders_by_cid
        .iter()
        .map(|(cid, order)| (order, effective_qty_by_cid[cid]))
        .collect();

    for &(order, new_leaves) in &post_batch {
        let final_qty = effective_qty_by_cid[&order.client_order_id()];
        check_active_oto_child_cap(
            order,
            final_qty,
            cache,
            &batch_cids,
            &[],
            &post_batch_qtys,
            true,
        )?;
        let inst_id = order.instrument_id();
        let norm_sym = normalize_symbol(&inst_id);
        if is_safe_exit_active(cache, &norm_sym) {
            continue;
        }
        if !has_open_unlinked_exit(engine, order, cache, &batch_cids)
            && !has_post_batch_unlinked_exit(order, &post_batch)
        {
            continue;
        }
        let open_pos_qty = open_opposite_pos_qty(cache, &inst_id, order.order_side());
        if open_pos_qty > Decimal::ZERO
            && new_leaves <= order.leaves_qty().as_decimal()
            && new_leaves <= open_pos_qty
        {
            continue;
        }
        let other_unlinked_exit_leaves = open_unlinked_exit_qty_with_oto_spillover(
            engine,
            order,
            cache,
            &batch_cids,
            &post_batch,
            &[],
        );
        if open_pos_qty > Decimal::ZERO {
            check_modify_exit_capacity(
                order,
                cache,
                open_pos_qty,
                other_unlinked_exit_leaves,
                new_leaves,
            )?;
        } else {
            let linked_entry_leaves_qty =
                active_opposite_linked_entry_leaves_qty(order, cache, &[]);
            if other_unlinked_exit_leaves + new_leaves > linked_entry_leaves_qty {
                return Err(format!(
                    "REDUCE_ONLY_MODIFY_EXCEEDS_POSITION: other_unlinked_exit_leaves ({other_unlinked_exit_leaves}) + new_leaves ({new_leaves}) > linked_entry_leaves_qty ({linked_entry_leaves_qty})"
                ));
            }
        }
    }

    Ok(())
}

fn prune_admitted_exits_from_cache(engine: &RustPlanGateEngine, cache: &Cache) {
    engine.prune_non_initialized_exits(|cid_str| {
        let cid = ClientOrderId::from(cid_str);
        cache
            .order(&cid)
            .is_some_and(|o| o.status() == OrderStatus::Initialized)
    });
}

impl PreTradeRiskGate for RustPlanGateEngine {
    fn check_command(
        &self,
        command: &TradingCommand,
        cache: &Cache,
        ts_now: UnixNanos,
    ) -> Result<(), String> {
        match command {
            TradingCommand::SubmitOrder(submit) => {
                prune_admitted_exits_from_cache(self, cache);
                let Some(order) = cache.order(&submit.client_order_id) else {
                    return Ok(());
                };
                let norm_sym = normalize_symbol(&order.instrument_id());
                let plan_item = self.plan_item_for_symbol(&norm_sym);
                if is_verified_exit_for_submit(self, &order, cache, plan_item.as_ref())? {
                    self.record_admitted_exit(order.client_order_id().as_str());
                    return Ok(());
                }
                reconcile_cache_reservations(self, &norm_sym, cache);
                let exclude = [submit.client_order_id];
                let req = build_gate_request(
                    &order,
                    cache,
                    ts_now,
                    None,
                    &exclude,
                    plan_item.as_ref(),
                    false,
                );
                let decision = self.evaluate_and_reserve(&req);
                if decision.allowed {
                    Ok(())
                } else {
                    Err(decision
                        .reason
                        .unwrap_or_else(|| "PLAN_GATE_DENIED".to_string()))
                }
            }
            TradingCommand::SubmitOrderList(submit_list) => {
                prune_admitted_exits_from_cache(self, cache);
                let cids = &submit_list.order_list.client_order_ids;
                if cids.is_empty() {
                    return Ok(());
                }
                let mut orders = Vec::with_capacity(cids.len());
                for cid in cids {
                    let Some(order) = cache.order(cid) else {
                        return Ok(());
                    };
                    orders.push(order.clone());
                }

                let mut leg_is_verified_exit = Vec::with_capacity(orders.len());
                let mut reconciled_symbols: Vec<String> = Vec::new();
                for order in &orders {
                    let norm_sym = normalize_symbol(&order.instrument_id());
                    let plan_item = self.plan_item_for_symbol(&norm_sym);
                    let verified_exit = is_verified_exit_for_batch(
                        self,
                        order,
                        &orders,
                        cache,
                        cids,
                        plan_item.as_ref(),
                    )?;
                    if !verified_exit && !reconciled_symbols.contains(&norm_sym) {
                        reconcile_cache_reservations(self, &norm_sym, cache);
                        reconciled_symbols.push(norm_sym);
                    }
                    leg_is_verified_exit.push(verified_exit);
                }

                let mut requests = Vec::with_capacity(orders.len());
                for (order, &is_verified_exit) in orders.iter().zip(leg_is_verified_exit.iter()) {
                    let norm_sym = normalize_symbol(&order.instrument_id());
                    let plan_item = self.plan_item_for_symbol(&norm_sym);
                    let leg_sibling_stop = if is_verified_exit {
                        None
                    } else {
                        find_sibling_stop_price(order, &orders)
                    };
                    requests.push(build_gate_request(
                        order,
                        cache,
                        ts_now,
                        leg_sibling_stop,
                        cids,
                        plan_item.as_ref(),
                        is_verified_exit,
                    ));
                }

                self.evaluate_and_reserve_batch(&requests)?;
                self.record_admitted_exits(
                    cids.iter()
                        .zip(leg_is_verified_exit.iter())
                        .filter_map(|(cid, &is_exit)| is_exit.then_some(cid.as_str())),
                );
                Ok(())
            }
            TradingCommand::ModifyOrder(modify) => {
                prune_admitted_exits_from_cache(self, cache);
                let Some(order) = cache.order(&modify.client_order_id) else {
                    return Ok(());
                };
                check_single_modify(self, modify, &order, cache, ts_now)
            }
            TradingCommand::ModifyOrders(batch) => {
                prune_admitted_exits_from_cache(self, cache);
                check_modify_orders_batch(self, &batch.modifies, cache)
            }
            _ => Ok(()),
        }
    }

    fn on_order_denied(&self, client_order_id: &ClientOrderId) {
        self.on_order_denied_rollback(&client_order_id.to_string());
    }

    fn on_order_updated(&self, client_order_id: &ClientOrderId, new_qty: Decimal) {
        self.reconcile_order_updated(&client_order_id.to_string(), "", new_qty);
    }
}
