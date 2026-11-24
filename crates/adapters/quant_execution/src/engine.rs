// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! In-memory $O(1)$ pre-trade `PlanGate` state machine and async write-behind queue.

use std::{ collections::VecDeque, sync::Arc };

use ahash::{AHashMap, AHashSet};
use parking_lot::RwLock;
use rust_decimal::{Decimal, RoundingStrategy};
use serde_json::json;

use crate::models::{RustPlanGateDecision, RustPlanGateOrderRequest, RustPlanItemEnvelope};

const ADMISSIBLE_STATUSES: [&str; 3] = ["ARMED", "TRIGGERED", "PARTIALLY_FILLED"];

#[derive(Debug, Default)]
struct PlanGateMemoryState {
    entry_freeze: bool,
    entry_freeze_reason: String,
    backend_unavailable: bool,
    backend_unavailable_reason: String,
    items_by_symbol: AHashMap<String, RustPlanItemEnvelope>,
    reserved_qty_by_symbol: AHashMap<String, Decimal>,
    reserved_notional_by_symbol: AHashMap<String, Decimal>,
    admitted_entry_reservations: AHashMap<String, (String, Decimal)>,
    admitted_exit_cids: AHashSet<String>,
    pending_mutations: VecDeque<String>,
}

struct ValidatedEntryAdmission {
    item_id: String,
    plan_id: String,
    derived_limit_price: Option<String>,
    ref_price: Decimal,
    projected_total_qty: Decimal,
}

enum BatchLegValidation<'a> {
    Bypass {
        client_order_id: &'a str,
        item_id: Option<String>,
        plan_id: Option<String>,
    },
    Admit(&'a RustPlanGateOrderRequest, ValidatedEntryAdmission),
}

/// Thread-safe, $O(1)$ in-memory pre-trade `PlanGate` admission engine with non-blocking audit queue.
#[derive(Debug, Clone, Default)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.quant_execution", from_py_object)
)]
pub struct RustPlanGateEngine {
    state: Arc<RwLock<PlanGateMemoryState>>,
}

impl RustPlanGateEngine {
    /// Creates a new in-memory `RustPlanGateEngine`.
    #[must_use]
    pub fn new(entry_freeze: bool, entry_freeze_reason: String) -> Self {
        let state = PlanGateMemoryState {
            entry_freeze,
            entry_freeze_reason,
            ..Default::default()
        };
        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Sets or clears global `ENTRY_FREEZE` in $O(1)$ memory.
    pub fn set_entry_freeze(&self, active: bool, reason: String) {
        let mut guard = self.state.write();
        guard.entry_freeze = active;
        guard.entry_freeze_reason = reason;
    }

    /// Returns whether global `ENTRY_FREEZE` is active.
    #[must_use]
    pub fn entry_freeze(&self) -> bool {
        self.state.read().entry_freeze
    }

    /// Returns the active `ENTRY_FREEZE` reason.
    #[must_use]
    pub fn entry_freeze_reason(&self) -> String {
        self.state.read().entry_freeze_reason.clone()
    }

    /// Sets or clears backend unavailability (fail-closed) in $O(1)$ memory.
    pub fn set_backend_unavailable(&self, unavailable: bool, reason: String) {
        let mut guard = self.state.write();
        guard.backend_unavailable = unavailable;
        guard.backend_unavailable_reason = reason;
    }

    /// Returns whether the backend is marked unavailable.
    #[must_use]
    pub fn backend_unavailable(&self) -> bool {
        self.state.read().backend_unavailable
    }

    /// Upserts a locked `RustPlanItemEnvelope` into the in-memory $O(1)$ lookup table.
    pub fn upsert_plan_item(&self, mut item: RustPlanItemEnvelope, preserve_higher_reservation: bool) {
        let mut guard = self.state.write();
        let sym = item.symbol.clone();
        let db_filled = item.filled_qty;
        let prev_reserved = guard
            .reserved_qty_by_symbol
            .get(&sym)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let prev_notional = guard
            .reserved_notional_by_symbol
            .get(&sym)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let next_reserved = if preserve_higher_reservation {
            prev_reserved.max(db_filled)
        } else {
            db_filled
        };
        if preserve_higher_reservation {
            item.filled_qty = next_reserved;
        }
        let next_notional = if next_reserved <= Decimal::ZERO {
            Decimal::ZERO
        } else if prev_notional > Decimal::ZERO && prev_reserved > Decimal::ZERO {
            if next_reserved == prev_reserved {
                prev_notional
            } else {
                prev_notional * (next_reserved / prev_reserved)
            }
        } else if item.target_qty > Decimal::ZERO && item.target_notional_usd > Decimal::ZERO {
            (item.target_notional_usd / item.target_qty) * next_reserved
        } else {
            Decimal::ZERO
        };
        guard.reserved_notional_by_symbol.insert(sym.clone(), next_notional);
        guard.reserved_qty_by_symbol.insert(sym.clone(), next_reserved);
        guard.items_by_symbol.insert(sym, item);
    }

    /// Removes a plan item for `symbol` from the in-memory table.
    pub fn remove_plan_item(&self, symbol: &str) {
        let mut guard = self.state.write();
        guard.items_by_symbol.remove(symbol);
        guard.reserved_qty_by_symbol.remove(symbol);
        guard.reserved_notional_by_symbol.remove(symbol);
        guard
            .admitted_entry_reservations
            .retain(|_, (sym, _)| sym != symbol);
    }

    /// Clears all cached plan items.
    pub fn clear_plan_items(&self) {
        let mut guard = self.state.write();
        guard.items_by_symbol.clear();
        guard.reserved_qty_by_symbol.clear();
        guard.reserved_notional_by_symbol.clear();
        guard.admitted_entry_reservations.clear();
    }

    /// Returns a cloned copy of the locked `RustPlanItemEnvelope` for `symbol`, if present.
    #[must_use]
    pub fn plan_item_for_symbol(&self, symbol: &str) -> Option<RustPlanItemEnvelope> {
        self.state.read().items_by_symbol.get(symbol).cloned()
    }

    /// Returns whether an active reservation is currently tracked for `client_order_id`.
    #[must_use]
    pub fn has_admitted_reservation(&self, client_order_id: &str) -> bool {
        self.state
            .read()
            .admitted_entry_reservations
            .contains_key(client_order_id)
    }

    /// Records an admitted exit `client_order_id` so same-callback `OrderStatus::Initialized`
    /// unlinked exit siblings are counted against position capacity.
    pub fn record_admitted_exit(&self, client_order_id: &str) {
        if client_order_id.is_empty() {
            return;
        }
        self.state
            .write()
            .admitted_exit_cids
            .insert(client_order_id.to_string());
    }

    /// Records multiple admitted exit `client_order_id`s atomically under a single write lock.
    pub fn record_admitted_exits<'a>(&self, cids: impl IntoIterator<Item = &'a str>) {
        let mut guard = self.state.write();
        for cid in cids {
            if !cid.is_empty() {
                guard.admitted_exit_cids.insert(cid.to_string());
            }
        }
    }

    /// Returns whether `client_order_id` was previously admitted as an exit by this gate.
    #[must_use]
    pub fn is_admitted_exit(&self, client_order_id: &str) -> bool {
        self.state
            .read()
            .admitted_exit_cids
            .contains(client_order_id)
    }

    /// Retains only admitted exit `client_order_id`s for which `keep` returns `true`.
    pub fn prune_non_initialized_exits<F>(&self, mut keep: F)
    where
        F: FnMut(&str) -> bool,
    {
        if self.state.read().admitted_exit_cids.is_empty() {
            return;
        }
        let mut guard = self.state.write();
        guard
            .admitted_exit_cids
            .retain(|cid| keep(cid.as_str()));
    }

    /// Returns `(client_order_id, reserved_qty)` snapshots for all active entry reservations
    /// currently tracked for `symbol`.
    #[must_use]
    pub fn admitted_reservations_for_symbol(&self, symbol: &str) -> Vec<(String, Decimal)> {
        let guard = self.state.read();
        if guard.admitted_entry_reservations.is_empty() {
            return Vec::new();
        }
        guard
            .admitted_entry_reservations
            .iter()
            .filter_map(|(cid, (sym, qty))| {
                if sym == symbol {
                    Some((cid.clone(), *qty))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns the currently reserved/filled quantity for `symbol`.
    #[must_use]
    pub fn reserved_qty(&self, symbol: &str) -> Decimal {
        self.state
            .read()
            .reserved_qty_by_symbol
            .get(symbol)
            .copied()
            .unwrap_or(Decimal::ZERO)
    }

    /// Evaluates pre-trade admission and atomically reserves capacity in $O(1)$ memory.
    ///
    /// Never performs network or SQL I/O on the calling thread; audit/state mutations
    /// are appended to the in-memory `pending_mutations` queue for background persistence.
    #[must_use]
    pub fn evaluate_and_reserve(&self, request: &RustPlanGateOrderRequest) -> RustPlanGateDecision {
        let mut guard = self.state.write();

        let item_opt = guard.items_by_symbol.get(&request.symbol).cloned();
        let item_id_opt = item_opt.as_ref().map(|i| i.item_id.clone());
        let plan_id_opt = item_opt.as_ref().map(|i| i.plan_id.clone());

        if request.is_reduce_only || request.is_exit_intent {
            if !request.client_order_id.is_empty() {
                guard
                    .admitted_exit_cids
                    .insert(request.client_order_id.clone());
            }
            return RustPlanGateDecision {
                allowed: true,
                reason: None,
                is_exit_bypass: true,
                derived_limit_price: None,
                item_id: item_id_opt,
                plan_id: plan_id_opt,
                new_reserved_qty: None,
                added_notional_usd: None,
            };
        }

        match Self::validate_entry_request_locked(&guard, request, Decimal::ZERO) {
            Ok(admission) => Self::commit_entry_admission_locked(&mut guard, request, admission),
            Err((reason, severity, err_item_id, err_plan_id)) => {
                Self::enqueue_denied_mutation(
                    &mut guard,
                    request,
                    &reason,
                    severity,
                    err_item_id.as_deref(),
                    err_plan_id.as_deref(),
                );
                RustPlanGateDecision {
                    allowed: false,
                    reason: Some(reason),
                    is_exit_bypass: false,
                    derived_limit_price: None,
                    item_id: err_item_id,
                    plan_id: err_plan_id,
                    new_reserved_qty: None,
                    added_notional_usd: None,
                }
            }
        }
    }

    /// Evaluates a multi-leg batch (e.g., `SubmitOrderList`) using two-phase atomic check-then-reserve.
    ///
    /// Phase 1 validates every leg without mutating any reservation or item state. Only if every
    /// leg passes does Phase 2 atomically commit reservations across all entry legs.
    ///
    /// # Errors
    ///
    /// Returns the denial reason for the first failing leg, leaving reservations unchanged.
    pub fn evaluate_and_reserve_batch(
        &self,
        requests: &[RustPlanGateOrderRequest],
    ) -> Result<Vec<RustPlanGateDecision>, String> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let mut guard = self.state.write();
        let mut batch_added_qty_by_symbol: AHashMap<String, Decimal> = AHashMap::new();
        let mut batch_seen_entry_cids: ahash::AHashSet<&str> =
            ahash::AHashSet::with_capacity(requests.len());
        let mut validated = Vec::with_capacity(requests.len());

        // Phase 1: Pure read-only validation across all legs.
        for request in requests {
            if request.is_reduce_only || request.is_exit_intent {
                let item_opt = guard.items_by_symbol.get(&request.symbol);
                validated.push(BatchLegValidation::Bypass {
                    client_order_id: request.client_order_id.as_str(),
                    item_id: item_opt.map(|i| i.item_id.clone()),
                    plan_id: item_opt.map(|i| i.plan_id.clone()),
                });
                continue;
            }

            if !request.client_order_id.is_empty()
                && !batch_seen_entry_cids.insert(request.client_order_id.as_str())
            {
                let item_opt = guard.items_by_symbol.get(&request.symbol);
                let item_id_opt = item_opt.map(|i| i.item_id.clone());
                let plan_id_opt = item_opt.map(|i| i.plan_id.clone());
                let reason = format!(
                    "DUPLICATE_CLIENT_ORDER_ID: duplicate entry client_order_id '{}' in batch",
                    request.client_order_id
                );
                Self::enqueue_denied_mutation(
                    &mut guard,
                    request,
                    &reason,
                    "WARN",
                    item_id_opt.as_deref(),
                    plan_id_opt.as_deref(),
                );
                return Err(reason);
            }

            let extra_qty = batch_added_qty_by_symbol
                .get(&request.symbol)
                .copied()
                .unwrap_or(Decimal::ZERO);

            match Self::validate_entry_request_locked(&guard, request, extra_qty) {
                Ok(admission) => {
                    *batch_added_qty_by_symbol
                        .entry(request.symbol.clone())
                        .or_insert(Decimal::ZERO) += request.quantity;
                    validated.push(BatchLegValidation::Admit(request, admission));
                }
                Err((reason, severity, err_item_id, err_plan_id)) => {
                    Self::enqueue_denied_mutation(
                        &mut guard,
                        request,
                        &reason,
                        severity,
                        err_item_id.as_deref(),
                        err_plan_id.as_deref(),
                    );
                    return Err(reason);
                }
            }
        }

        // Phase 2: Atomic commit across all admitted legs.
        let mut decisions = Vec::with_capacity(validated.len());
        for leg in validated {
            match leg {
                BatchLegValidation::Bypass {
                    client_order_id,
                    item_id,
                    plan_id,
                } => {
                    if !client_order_id.is_empty() {
                        guard
                            .admitted_exit_cids
                            .insert(client_order_id.to_string());
                    }
                    decisions.push(RustPlanGateDecision {
                        allowed: true,
                        reason: None,
                        is_exit_bypass: true,
                        derived_limit_price: None,
                        item_id,
                        plan_id,
                        new_reserved_qty: None,
                        added_notional_usd: None,
                    });
                }
                BatchLegValidation::Admit(req, admission) => {
                    decisions.push(Self::commit_entry_admission_locked(
                        &mut guard, req, admission,
                    ));
                }
            }
        }

        Ok(decisions)
    }

    fn validate_entry_stop_loss_price(
        side: &str,
        stop_px: Decimal,
        entry_ref_price: Decimal,
        implied_plan_ref_price: Decimal,
        initial_stop_price: Decimal,
    ) -> Result<(), String> {
        if stop_px <= Decimal::ZERO {
            return Err(format!(
                "INVALID_STOP_LOSS_PRICE: stop_loss_price ({stop_px}) must be > 0"
            ));
        }
        if side.eq_ignore_ascii_case("LONG") {
            if stop_px >= entry_ref_price {
                return Err(format!(
                    "INVALID_STOP_LOSS_PRICE: LONG stop_loss_price ({stop_px}) must be < entry_ref_price ({entry_ref_price})"
                ));
            }
            let effective_stop_floor = if implied_plan_ref_price > initial_stop_price
                && entry_ref_price > Decimal::ZERO
                && entry_ref_price <= initial_stop_price
            {
                let max_plan_stop_dist = implied_plan_ref_price - initial_stop_price;
                (entry_ref_price - max_plan_stop_dist).max(Decimal::new(1, 2))
            } else {
                initial_stop_price
            };
            if initial_stop_price > Decimal::ZERO && stop_px < effective_stop_floor {
                return Err(format!(
                    "STOP_LOSS_FLOOR_VIOLATION (STOP_LOSS_BELOW_PLAN_FLOOR): LONG stop_loss_price ({stop_px}) < initial_stop_price ({effective_stop_floor})"
                ));
            }
        } else {
            if stop_px <= entry_ref_price {
                return Err(format!(
                    "INVALID_STOP_LOSS_PRICE: SHORT stop_loss_price ({stop_px}) must be > entry_ref_price ({entry_ref_price})"
                ));
            }
            let effective_stop_ceiling = if implied_plan_ref_price > Decimal::ZERO
                && implied_plan_ref_price < initial_stop_price
                && entry_ref_price >= initial_stop_price
            {
                let max_plan_stop_dist = initial_stop_price - implied_plan_ref_price;
                entry_ref_price + max_plan_stop_dist
            } else {
                initial_stop_price
            };
            if initial_stop_price > Decimal::ZERO && stop_px > effective_stop_ceiling {
                return Err(format!(
                    "STOP_LOSS_FLOOR_VIOLATION (STOP_LOSS_ABOVE_PLAN_CEILING): SHORT stop_loss_price ({stop_px}) > initial_stop_price ({effective_stop_ceiling})"
                ));
            }
        }
        Ok(())
    }

    /// Validates an entry order modification (`!is_reduce_only`), denying any upward quantity
    /// modification (`ENTRY_UPWARD_MODIFY_PROHIBITED`), freeze/outage/expiration states, and
    /// any limit/trigger/stop-loss/budget violation.
    ///
    /// # Errors
    ///
    /// Returns the exact denial reason if the modification is prohibited.
    #[expect(clippy::too_many_arguments)]
    pub fn validate_entry_modify(
        &self,
        symbol: &str,
        new_qty: Option<Decimal>,
        current_qty: Decimal,
        new_limit_price: Option<Decimal>,
        current_limit_price: Option<Decimal>,
        new_trigger_price: Option<Decimal>,
        stop_loss_price: Option<Decimal>,
        reference_price: Option<Decimal>,
        atr: Option<Decimal>,
        evaluation_ts_ns: i64,
        evaluation_ts_iso: &str,
    ) -> Result<(), String> {
        if let Some(nq) = new_qty {
            if nq > current_qty {
                return Err("ENTRY_UPWARD_MODIFY_PROHIBITED".to_string());
            }
            if nq <= Decimal::ZERO {
                return Err(format!(
                    "INVALID_QUANTITY: order quantity must be > 0, got {nq}"
                ));
            }
        }

        let guard = self.state.read();
        if guard.backend_unavailable {
            return Err(format!(
                "BACKEND_UNAVAILABLE: {}",
                guard.backend_unavailable_reason
            ));
        }
        if guard.entry_freeze {
            let detail = if guard.entry_freeze_reason.is_empty() {
                String::new()
            } else {
                format!(" ({})", guard.entry_freeze_reason)
            };
            return Err(format!(
                "ENTRY_FREEZE_ACTIVE: global ENTRY_FREEZE is active{detail}"
            ));
        }
        let Some(plan_item) = guard.items_by_symbol.get(symbol) else {
            return Err(format!(
                "NO_LOCKED_PLAN_ITEM: no LOCKED PlanItemContract found for symbol {symbol}"
            ));
        };
        if !ADMISSIBLE_STATUSES.contains(&plan_item.status.as_str()) {
            return Err(format!(
                "PLAN_ITEM_STATUS_NOT_ADMISSIBLE: PlanItem {} for {symbol} has status '{}' (must be in ('ARMED', 'TRIGGERED', 'PARTIALLY_FILLED'))",
                plan_item.item_id, plan_item.status
            ));
        }
        if plan_item.valid_until_ns <= 0
            || evaluation_ts_ns <= 0
            || evaluation_ts_ns > plan_item.valid_until_ns
        {
            return Err(format!(
                "PLAN_ITEM_EXPIRED: invalid or expired evaluation_ts ({evaluation_ts_iso}, ns={evaluation_ts_ns}) / valid_until ({}, ns={})",
                plan_item.valid_until_iso, plan_item.valid_until_ns
            ));
        }

        if let Some(trig_px) = new_trigger_price
            && trig_px <= Decimal::ZERO
        {
            return Err(format!(
                "INVALID_TRIGGER_PRICE: trigger_price ({trig_px}) must be > 0"
            ));
        }

        let effective_limit_px = new_limit_price.or(current_limit_price);
        if new_limit_price.is_none()
            && new_trigger_price.is_none()
            && stop_loss_price.is_none()
            && new_qty.is_none()
        {
            return Ok(());
        }

        let implied_plan_ref_price = if plan_item.target_qty > Decimal::ZERO {
            plan_item.target_notional_usd / plan_item.target_qty
        } else {
            Decimal::ZERO
        };
        let ref_price = reference_price.unwrap_or(implied_plan_ref_price);
        if ref_price <= Decimal::ZERO {
            return Err(format!(
                "INVALID_REFERENCE_PRICE: reference_price must be > 0, got {ref_price}"
            ));
        }

        let worst_limit_px = plan_item.compute_worst_limit_price(ref_price, atr);
        if let Some(limit_px) = new_limit_price {
            if limit_px <= Decimal::ZERO {
                return Err(format!(
                    "INVALID_LIMIT_PRICE: limit_price ({limit_px}) must be > 0"
                ));
            }
            if plan_item.side.eq_ignore_ascii_case("LONG") && limit_px > worst_limit_px {
                return Err(format!(
                    "COLLAR_LIMIT_VIOLATION: BUY limit_price ({limit_px}) exceeds worst_limit_price ({worst_limit_px}) from reference_price ({ref_price})"
                ));
            }
            if plan_item.side.eq_ignore_ascii_case("SHORT") && limit_px < worst_limit_px {
                return Err(format!(
                    "COLLAR_LIMIT_VIOLATION: SELL limit_price ({limit_px}) below worst_limit_price ({worst_limit_px}) from reference_price ({ref_price})"
                ));
            }
        }
        if let Some(trig_px) = new_trigger_price {
            if plan_item.side.eq_ignore_ascii_case("LONG") && trig_px > worst_limit_px {
                return Err(format!(
                    "COLLAR_LIMIT_VIOLATION: BUY trigger_price ({trig_px}) exceeds worst_limit_price ({worst_limit_px}) from reference_price ({ref_price})"
                ));
            }
            if plan_item.side.eq_ignore_ascii_case("SHORT") && trig_px < worst_limit_px {
                return Err(format!(
                    "COLLAR_LIMIT_VIOLATION: SELL trigger_price ({trig_px}) below worst_limit_price ({worst_limit_px}) from reference_price ({ref_price})"
                ));
            }
        }

        let entry_ref_price = effective_limit_px
            .filter(|px| *px > Decimal::ZERO)
            .unwrap_or(ref_price);
        if let Some(stop_px) = stop_loss_price {
            Self::validate_entry_stop_loss_price(
                &plan_item.side,
                stop_px,
                entry_ref_price,
                implied_plan_ref_price,
                plan_item.initial_stop_price,
            )?;
        }

        let eff_stop_px = stop_loss_price.or_else(|| {
            if plan_item.initial_stop_price > Decimal::ZERO {
                Some(plan_item.initial_stop_price)
            } else {
                None
            }
        });
        let budget_check_px_opt = effective_limit_px
            .filter(|px| *px > Decimal::ZERO)
            .or_else(|| (ref_price > Decimal::ZERO).then_some(ref_price));
        if let (Some(stop_px), Some(check_px)) = (eff_stop_px, budget_check_px_opt)
            && plan_item.max_risk_budget_usd > Decimal::ZERO
        {
            let eff_order_qty = new_qty.unwrap_or(current_qty);
            let sym_reserved = guard
                .reserved_qty_by_symbol
                .get(symbol)
                .copied()
                .unwrap_or(Decimal::ZERO);
            let qty_reduction = (current_qty - eff_order_qty).max(Decimal::ZERO);
            let projected_qty = sym_reserved.saturating_sub(qty_reduction).max(eff_order_qty);
            let collar_allowance = (worst_limit_px - ref_price).abs() + Decimal::new(1, 2);
            let budget_ref_price = if implied_plan_ref_price > Decimal::ZERO
                && (implied_plan_ref_price - check_px).abs() <= collar_allowance
            {
                implied_plan_ref_price
            } else {
                check_px
            };
            let trade_risk_usd = (budget_ref_price - stop_px).abs() * projected_qty;
            if trade_risk_usd > plan_item.max_risk_budget_usd {
                return Err(format!(
                    "RISK_BUDGET_EXCEEDED: single-trade risk (|{budget_ref_price} - {stop_px}| * {projected_qty} = {trade_risk_usd}) exceeds max_risk_budget_usd ({})",
                    plan_item.max_risk_budget_usd
                ));
            }
        }

        Ok(())
    }

    fn validate_entry_request_locked(
        guard: &PlanGateMemoryState,
        request: &RustPlanGateOrderRequest,
        extra_reserved_qty: Decimal,
    ) -> Result<ValidatedEntryAdmission, (String, &'static str, Option<String>, Option<String>)>
    {
        let item_opt = guard.items_by_symbol.get(&request.symbol);
        let item_id_opt = item_opt.map(|i| i.item_id.clone());
        let plan_id_opt = item_opt.map(|i| i.plan_id.clone());

        if guard.backend_unavailable {
            let reason = format!(
                "BACKEND_UNAVAILABLE: {}",
                guard.backend_unavailable_reason
            );
            return Err((reason, "CRITICAL", item_id_opt, plan_id_opt));
        }

        if guard.entry_freeze {
            let detail = if guard.entry_freeze_reason.is_empty() {
                String::new()
            } else {
                format!(" ({})", guard.entry_freeze_reason)
            };
            let reason = format!("ENTRY_FREEZE_ACTIVE: global ENTRY_FREEZE is active{detail}");
            return Err((reason, "CRITICAL", item_id_opt, plan_id_opt));
        }

        let Some(plan_item) = item_opt else {
            let reason = format!(
                "NO_LOCKED_PLAN_ITEM: no LOCKED PlanItemContract found for symbol {}",
                request.symbol
            );
            return Err((reason, "WARN", None, None));
        };

        if !ADMISSIBLE_STATUSES.contains(&plan_item.status.as_str()) {
            let reason = format!(
                "PLAN_ITEM_STATUS_NOT_ADMISSIBLE: PlanItem {} for {} has status '{}' (must be in ('ARMED', 'TRIGGERED', 'PARTIALLY_FILLED'))",
                plan_item.item_id, request.symbol, plan_item.status
            );
            return Err((reason, "WARN", item_id_opt, plan_id_opt));
        }

        if plan_item.valid_until_ns <= 0
            || request.evaluation_ts_ns <= 0
            || request.evaluation_ts_ns > plan_item.valid_until_ns
        {
            let reason = format!(
                "PLAN_ITEM_EXPIRED: invalid or expired evaluation_ts ({}, ns={}) / valid_until ({}, ns={})",
                request.evaluation_ts_iso,
                request.evaluation_ts_ns,
                plan_item.valid_until_iso,
                plan_item.valid_until_ns
            );
            return Err((reason, "WARN", item_id_opt, plan_id_opt));
        }

        let expected_side = if plan_item.side.eq_ignore_ascii_case("LONG") {
            "BUY"
        } else {
            "SELL"
        };
        let normalized_req_side = if request.order_side.contains("BUY") {
            "BUY"
        } else if request.order_side.contains("SELL") {
            "SELL"
        } else {
            request.order_side.as_str()
        };

        if normalized_req_side != expected_side {
            let reason = format!(
                "SIDE_MISMATCH: order side {} does not match plan side '{}'",
                request.order_side, plan_item.side
            );
            return Err((reason, "WARN", item_id_opt, plan_id_opt));
        }

        if request.quantity <= Decimal::ZERO {
            let reason = format!(
                "INVALID_QUANTITY: order quantity must be > 0, got {}",
                request.quantity
            );
            return Err((reason, "WARN", item_id_opt, plan_id_opt));
        }

        if !request.client_order_id.is_empty()
            && guard
                .admitted_entry_reservations
                .contains_key(&request.client_order_id)
        {
            let reason = format!(
                "DUPLICATE_CLIENT_ORDER_ID: entry reservation already exists for '{}'",
                request.client_order_id
            );
            return Err((reason, "WARN", item_id_opt, plan_id_opt));
        }

        let prior_reserved = guard
            .reserved_qty_by_symbol
            .get(&request.symbol)
            .copied()
            .unwrap_or(plan_item.filled_qty);
        let cache_total = request.cache_filled_entry_qty + request.cache_open_entry_qty;
        let effective_prior_qty = prior_reserved.max(cache_total) + extra_reserved_qty;
        let projected_total_qty = effective_prior_qty + request.quantity;

        if projected_total_qty > plan_item.approved_qty_cap {
            let reason = format!(
                "QTY_CAP_EXCEEDED: order_qty ({}) + prior_qty ({}) = {} > approved_qty_cap ({})",
                request.quantity,
                effective_prior_qty,
                projected_total_qty,
                plan_item.approved_qty_cap
            );
            return Err((reason, "WARN", item_id_opt, plan_id_opt));
        }

        let implied_plan_ref_price = if plan_item.target_qty > Decimal::ZERO {
            plan_item.target_notional_usd / plan_item.target_qty
        } else {
            Decimal::ZERO
        };
        let ref_price = request.reference_price.unwrap_or(implied_plan_ref_price);
        if ref_price <= Decimal::ZERO {
            let reason = format!(
                "INVALID_REFERENCE_PRICE: reference_price must be > 0, got {ref_price}"
            );
            return Err((reason, "WARN", item_id_opt, plan_id_opt));
        }

        let worst_limit_px = plan_item.compute_worst_limit_price(ref_price, request.atr);

        let derived_limit_price = if let Some(limit_px) = request.limit_price {
            if limit_px <= Decimal::ZERO {
                let reason = format!(
                    "INVALID_LIMIT_PRICE: limit_price ({limit_px}) must be > 0"
                );
                return Err((reason, "WARN", item_id_opt, plan_id_opt));
            }
            if plan_item.side.eq_ignore_ascii_case("LONG") && limit_px > worst_limit_px {
                let reason = format!(
                    "COLLAR_LIMIT_VIOLATION: BUY limit_price ({limit_px}) exceeds worst_limit_price ({worst_limit_px}) from reference_price ({ref_price})"
                );
                return Err((reason, "WARN", item_id_opt, plan_id_opt));
            }
            if plan_item.side.eq_ignore_ascii_case("SHORT") && limit_px < worst_limit_px {
                let reason = format!(
                    "COLLAR_LIMIT_VIOLATION: SELL limit_price ({limit_px}) below worst_limit_price ({worst_limit_px}) from reference_price ({ref_price})"
                );
                return Err((reason, "WARN", item_id_opt, plan_id_opt));
            }
            None
        } else if request.collar_market_order {
            Some(worst_limit_px.to_string())
        } else {
            let reason = format!(
                "UNCOLLARED_MARKET_ENTRY_DENIED: uncollared {} entry orders are prohibited; supply an explicit collared limit price or enable collar_market_orders",
                request.order_type
            );
            return Err((reason, "WARN", item_id_opt, plan_id_opt));
        };

        let entry_ref_price = if ref_price > Decimal::ZERO {
            ref_price
        } else {
            request
                .limit_price
                .filter(|px| *px > Decimal::ZERO)
                .unwrap_or(Decimal::ZERO)
        };

        if let Some(stop_px) = request.stop_loss_price {
            Self::validate_entry_stop_loss_price(
                &plan_item.side,
                stop_px,
                entry_ref_price,
                implied_plan_ref_price,
                plan_item.initial_stop_price,
            )
            .map_err(|reason| (reason, "WARN", item_id_opt.clone(), plan_id_opt.clone()))?;
        }

        let effective_stop_px = request.stop_loss_price.or_else(|| {
            if plan_item.initial_stop_price > Decimal::ZERO {
                Some(plan_item.initial_stop_price)
            } else {
                None
            }
        });
        let collar_allowance = (worst_limit_px - ref_price).abs() + Decimal::new(1, 2);
        let budget_ref_price = if implied_plan_ref_price > Decimal::ZERO
            && entry_ref_price > Decimal::ZERO
            && (implied_plan_ref_price - entry_ref_price).abs() <= collar_allowance
        {
            implied_plan_ref_price
        } else if entry_ref_price > Decimal::ZERO {
            entry_ref_price
        } else {
            implied_plan_ref_price
        };
        if let Some(stop_px) = effective_stop_px
            && plan_item.max_risk_budget_usd > Decimal::ZERO
        {
            let trade_risk_usd = (budget_ref_price - stop_px).abs() * projected_total_qty;
            if trade_risk_usd > plan_item.max_risk_budget_usd {
                let reason = format!(
                    "RISK_BUDGET_EXCEEDED: single-trade risk (|{budget_ref_price} - {stop_px}| * {} = {trade_risk_usd}) exceeds max_risk_budget_usd ({})",
                    projected_total_qty, plan_item.max_risk_budget_usd
                );
                return Err((reason, "WARN", item_id_opt, plan_id_opt));
            }
        }

        Ok(ValidatedEntryAdmission {
            item_id: plan_item.item_id.clone(),
            plan_id: plan_item.plan_id.clone(),
            derived_limit_price,
            ref_price,
            projected_total_qty,
        })
    }

    fn commit_entry_admission_locked(
        guard: &mut PlanGateMemoryState,
        request: &RustPlanGateOrderRequest,
        admission: ValidatedEntryAdmission,
    ) -> RustPlanGateDecision {
        // Atomically reserve capacity and transition ARMED -> TRIGGERED in O(1) memory
        guard
            .reserved_qty_by_symbol
            .insert(request.symbol.clone(), admission.projected_total_qty);
        if let Some(mut_item) = guard.items_by_symbol.get_mut(&request.symbol) {
            mut_item.filled_qty = admission.projected_total_qty;
            if mut_item.status == "ARMED" {
                mut_item.status = "TRIGGERED".to_string();
            }
        }
        if !request.client_order_id.is_empty() {
            guard.admitted_entry_reservations.insert(
                request.client_order_id.clone(),
                (request.symbol.clone(), request.quantity),
            );
        }

        let added_notional = (request.quantity * admission.ref_price)
            .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
        let prev_notional = guard
            .reserved_notional_by_symbol
            .get(&request.symbol)
            .copied()
            .unwrap_or(Decimal::ZERO);
        guard
            .reserved_notional_by_symbol
            .insert(request.symbol.clone(), prev_notional + added_notional);

        let mutation = json!({
            "kind": "ORDER_ADMITTED",
            "plan_id": admission.plan_id,
            "item_id": admission.item_id,
            "symbol": request.symbol,
            "strategy_id": request.strategy_id,
            "client_order_id": request.client_order_id,
            "quantity": request.quantity.to_string(),
            "new_reserved_qty": admission.projected_total_qty.to_string(),
            "added_notional_usd": added_notional.to_string(),
        });
        guard.pending_mutations.push_back(mutation.to_string());

        RustPlanGateDecision {
            allowed: true,
            reason: None,
            is_exit_bypass: false,
            derived_limit_price: admission.derived_limit_price,
            item_id: Some(admission.item_id),
            plan_id: Some(admission.plan_id),
            new_reserved_qty: Some(admission.projected_total_qty.to_string()),
            added_notional_usd: Some(added_notional.to_string()),
        }
    }

    /// Idempotently rolls back the reservation previously recorded for `client_order_id`
    /// when a downstream `RiskEngine` check denies the order. Returns the released quantity.
    pub fn on_order_denied_rollback(&self, client_order_id: &str) -> Decimal {
        let mut guard = self.state.write();
        guard.admitted_exit_cids.remove(client_order_id);
        let Some((symbol, qty)) = guard.admitted_entry_reservations.remove(client_order_id) else {
            return Decimal::ZERO;
        };
        if qty > Decimal::ZERO {
            Self::release_reservation_locked(&mut guard, &symbol, qty);
        }
        qty
    }

    fn reserved_qty_for_symbol_lookup(
        guard: &PlanGateMemoryState,
        raw_symbol: &str,
        norm_sym: &str,
    ) -> Decimal {
        if norm_sym.is_empty() {
            Decimal::ZERO
        } else {
            guard
                .reserved_qty_by_symbol
                .get(raw_symbol)
                .or_else(|| guard.reserved_qty_by_symbol.get(norm_sym))
                .copied()
                .unwrap_or(Decimal::ZERO)
        }
    }

    /// Reconciles an active entry reservation downward when the venue confirms a quantity
    /// reduction (`OrderUpdated`), releasing `(prior_qty - new_qty)` back to symbol capacity.
    ///
    /// Never inflates a reservation (`new_qty >= tracked_qty` is a no-op) and ignores
    /// orders not currently tracked in `admitted_entry_reservations`.
    pub fn reconcile_order_updated(
        &self,
        client_order_id: &str,
        symbol: &str,
        new_qty: Decimal,
    ) -> Decimal {
        let norm_sym = symbol.trim().to_ascii_uppercase();
        let mut guard = self.state.write();
        let Some((tracked_sym, tracked_qty)) =
            guard.admitted_entry_reservations.get_mut(client_order_id)
        else {
            return Self::reserved_qty_for_symbol_lookup(&guard, symbol, &norm_sym);
        };
        if !norm_sym.is_empty() && !tracked_sym.eq_ignore_ascii_case(&norm_sym) {
            return Self::reserved_qty_for_symbol_lookup(&guard, symbol, &norm_sym);
        }
        let target_qty = new_qty.max(Decimal::ZERO);
        let sym = tracked_sym.clone();
        if target_qty >= *tracked_qty {
            return guard
                .reserved_qty_by_symbol
                .get(sym.as_str())
                .copied()
                .unwrap_or(Decimal::ZERO);
        }
        let delta = *tracked_qty - target_qty;
        if target_qty <= Decimal::ZERO {
            guard.admitted_entry_reservations.remove(client_order_id);
        } else {
            *tracked_qty = target_qty;
        }
        Self::release_reservation_locked(&mut guard, &sym, delta)
    }

    /// Reconciles a terminal entry order (`Canceled | Expired | Rejected | Denied`) by
    /// removing it from `admitted_entry_reservations` and releasing only its unfilled
    /// reserved capacity `(tracked_qty - filled_qty.min(tracked_qty)).max(Decimal::ZERO)`.
    ///
    /// Removing the order makes any subsequent `release_order_reservation` call an
    /// idempotent no-op and prevents double-releasing filled shares.
    pub fn reconcile_terminal_order(
        &self,
        client_order_id: &str,
        symbol: &str,
        filled_qty: Decimal,
    ) -> Decimal {
        let norm_sym = symbol.trim().to_ascii_uppercase();
        let mut guard = self.state.write();
        guard.admitted_exit_cids.remove(client_order_id);
        let Some((tracked_sym, tracked_qty)) =
            guard.admitted_entry_reservations.get(client_order_id).cloned()
        else {
            return Self::reserved_qty_for_symbol_lookup(&guard, symbol, &norm_sym);
        };
        if !norm_sym.is_empty() && !tracked_sym.eq_ignore_ascii_case(&norm_sym) {
            return Self::reserved_qty_for_symbol_lookup(&guard, symbol, &norm_sym);
        }
        guard.admitted_entry_reservations.remove(client_order_id);
        let bounded_tracked = tracked_qty.max(Decimal::ZERO);
        let kept_filled = filled_qty.max(Decimal::ZERO).min(bounded_tracked);
        let releasable = (bounded_tracked - kept_filled).max(Decimal::ZERO);
        if releasable <= Decimal::ZERO {
            return guard
                .reserved_qty_by_symbol
                .get(tracked_sym.as_str())
                .copied()
                .unwrap_or(Decimal::ZERO);
        }
        Self::release_reservation_locked(&mut guard, &tracked_sym, releasable)
    }

    /// Releases up to `quantity` for a specific `client_order_id` (removing or decrementing
    /// the tracked per-order reservation so subsequent rollback/release calls never double-release).
    pub fn release_order_reservation(
        &self,
        client_order_id: &str,
        symbol: &str,
        quantity: Decimal,
    ) -> Decimal {
        let norm_sym = symbol.trim().to_ascii_uppercase();
        let mut guard = self.state.write();
        guard.admitted_exit_cids.remove(client_order_id);
        if quantity <= Decimal::ZERO {
            return if norm_sym.is_empty() {
                Decimal::ZERO
            } else {
                guard
                    .reserved_qty_by_symbol
                    .get(&norm_sym)
                    .copied()
                    .unwrap_or(Decimal::ZERO)
            };
        }
        let Some((tracked_sym, tracked_qty)) =
            guard.admitted_entry_reservations.get_mut(client_order_id)
        else {
            return Self::reserved_qty_for_symbol_lookup(&guard, symbol, &norm_sym);
        };
        if !norm_sym.is_empty() && !tracked_sym.eq_ignore_ascii_case(&norm_sym) {
            return Self::reserved_qty_for_symbol_lookup(&guard, symbol, &norm_sym);
        }
        let target_sym = tracked_sym.clone();
        let bounded = quantity.min(*tracked_qty);
        *tracked_qty -= bounded;
        if *tracked_qty <= Decimal::ZERO {
            guard.admitted_entry_reservations.remove(client_order_id);
        }
        if bounded <= Decimal::ZERO {
            return guard
                .reserved_qty_by_symbol
                .get(target_sym.as_str())
                .copied()
                .unwrap_or(Decimal::ZERO);
        }
        Self::release_reservation_locked(&mut guard, &target_sym, bounded)
    }

    /// Releases reserved entry quantity in $O(1)$ memory and enqueues an async DB update.
    pub fn release_reservation(&self, symbol: &str, quantity: Decimal) -> Decimal {
        if quantity <= Decimal::ZERO {
            return self.reserved_qty(symbol);
        }
        let mut guard = self.state.write();
        let updated = Self::release_reservation_locked(&mut guard, symbol, quantity);
        if updated == Decimal::ZERO {
            guard
                .admitted_entry_reservations
                .retain(|_, (sym, _)| sym != symbol);
        }
        updated
    }

    fn release_reservation_locked(
        guard: &mut PlanGateMemoryState,
        symbol: &str,
        quantity: Decimal,
    ) -> Decimal {
        let current = guard
            .reserved_qty_by_symbol
            .get(symbol)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let updated = (current - quantity).max(Decimal::ZERO);
        let actual_released_qty = (current - updated).max(Decimal::ZERO);
        guard
            .reserved_qty_by_symbol
            .insert(symbol.to_string(), updated);
        let tracked_notional = guard
            .reserved_notional_by_symbol
            .get(symbol)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let mut released_notional = Decimal::ZERO;
        if let Some(item) = guard.items_by_symbol.get_mut(symbol) {
            item.filled_qty = updated;
            if updated == Decimal::ZERO && item.status == "TRIGGERED" {
                item.status = "ARMED".to_string();
            }
            if updated == Decimal::ZERO && tracked_notional > Decimal::ZERO {
                released_notional = tracked_notional;
            } else if current > Decimal::ZERO && tracked_notional > Decimal::ZERO {
                released_notional = ((tracked_notional * actual_released_qty) / current)
                    .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
            } else if item.target_qty > Decimal::ZERO && item.target_notional_usd > Decimal::ZERO {
                let implied_ref_price = item.target_notional_usd / item.target_qty;
                released_notional = (actual_released_qty * implied_ref_price)
                    .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
            }
        }
        let next_notional = if updated == Decimal::ZERO {
            Decimal::ZERO
        } else {
            (tracked_notional - released_notional).max(Decimal::ZERO)
        };
        guard
            .reserved_notional_by_symbol
            .insert(symbol.to_string(), next_notional);
        let item_id = guard
            .items_by_symbol
            .get(symbol)
            .map(|i| i.item_id.clone());
        let mutation = json!({
            "kind": "RELEASE_RESERVATION",
            "symbol": symbol,
            "item_id": item_id,
            "quantity": quantity.to_string(),
            "new_reserved_qty": updated.to_string(),
            "released_notional_usd": released_notional.to_string(),
        });
        guard.pending_mutations.push_back(mutation.to_string());
        updated
    }

    /// Drains up to `max_items` pending JSON mutation payloads for async DB persistence.
    pub fn drain_pending_mutations(&self, max_items: usize) -> Vec<String> {
        let mut guard = self.state.write();
        let limit = max_items.max(1).min(guard.pending_mutations.len());
        let mut batch = Vec::with_capacity(limit);
        for _ in 0..limit {
            if let Some(entry) = guard.pending_mutations.pop_front() {
                batch.push(entry);
            }
        }
        batch
    }

    fn enqueue_denied_mutation(
        guard: &mut PlanGateMemoryState,
        request: &RustPlanGateOrderRequest,
        reason: &str,
        severity: &str,
        item_id: Option<&str>,
        plan_id: Option<&str>,
    ) {
        let mutation = json!({
            "kind": "ORDER_DENIED",
            "severity": severity,
            "plan_id": plan_id,
            "item_id": item_id,
            "symbol": request.symbol,
            "instrument_id": request.instrument_id,
            "strategy_id": request.strategy_id,
            "client_order_id": request.client_order_id,
            "order_side": request.order_side,
            "quantity": request.quantity.to_string(),
            "reason": reason,
        });
        guard.pending_mutations.push_back(mutation.to_string());
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    fn sample_envelope() -> RustPlanItemEnvelope {
        RustPlanItemEnvelope {
            item_id: "00000000-0000-0000-0000-000000000101".to_string(),
            plan_id: "00000000-0000-0000-0000-000000000202".to_string(),
            symbol: "NVDA".to_string(),
            side: "LONG".to_string(),
            status: "ARMED".to_string(),
            target_qty: dec!(100),
            approved_qty_cap: dec!(100),
            target_notional_usd: dec!(10000),
            filled_qty: dec!(0),
            valid_until_ns: 2_000_000_000_000_000_000,
            valid_until_iso: "2033-05-18T03:33:20+00:00".to_string(),
            limit_collar_pct: dec!(1.0),
            limit_collar_atr: dec!(0.5),
            initial_stop_price: dec!(95.00),
            max_risk_budget_usd: dec!(500.00),
            daily_atr_14: None,
        }
    }

    fn sample_request(qty: Decimal) -> RustPlanGateOrderRequest {
        RustPlanGateOrderRequest {
            client_order_id: "O-1".to_string(),
            strategy_id: "STRAT-1".to_string(),
            instrument_id: "NVDA.ALPACA".to_string(),
            symbol: "NVDA".to_string(),
            venue: "ALPACA".to_string(),
            order_side: "BUY".to_string(),
            order_type: "LIMIT".to_string(),
            quantity: qty,
            is_reduce_only: false,
            evaluation_ts_ns: 1_700_000_000_000_000_000,
            evaluation_ts_iso: "2023-11-14T22:13:20+00:00".to_string(),
            limit_price: Some(dec!(100.50)),
            reference_price: Some(dec!(100.00)),
            atr: Some(dec!(2.0)),
            collar_market_order: true,
            is_exit_intent: false,
            cache_open_entry_qty: dec!(0),
            cache_filled_entry_qty: dec!(0),
            stop_loss_price: Some(dec!(96.00)),
        }
    }

    #[test]
    fn test_o1_admit_and_cap_enforcement() {
        let engine = RustPlanGateEngine::new(false, String::new());
        engine.upsert_plan_item(sample_envelope(), false);

        let d1 = engine.evaluate_and_reserve(&sample_request(dec!(60)));
        assert!(d1.allowed);
        assert_eq!(engine.reserved_qty("NVDA"), dec!(60));

        let mut req2 = sample_request(dec!(50));
        req2.client_order_id = "O-2".to_string();
        let d2 = engine.evaluate_and_reserve(&req2);
        assert!(!d2.allowed);
        assert!(d2.reason.unwrap().contains("QTY_CAP_EXCEEDED"));
        assert_eq!(engine.reserved_qty("NVDA"), dec!(60));

        let released = engine.release_reservation("NVDA", dec!(20));
        assert_eq!(released, dec!(40));

        let mut req3 = sample_request(dec!(50));
        req3.client_order_id = "O-3".to_string();
        let d3 = engine.evaluate_and_reserve(&req3);
        assert!(d3.allowed);
        assert_eq!(engine.reserved_qty("NVDA"), dec!(90));
        assert_eq!(engine.drain_pending_mutations(10).len(), 4);
    }

    #[test]
    fn test_exit_bypasses_freeze_and_outage() {
        let engine = RustPlanGateEngine::new(true, "manual freeze".to_string());
        engine.set_backend_unavailable(true, "db down".to_string());

        let mut req = sample_request(dec!(50));
        req.is_reduce_only = true;
        let decision = engine.evaluate_and_reserve(&req);
        assert!(decision.allowed);
        assert!(decision.is_exit_bypass);
    }

    #[test]
    fn test_long_effective_stop_floor_scales_below_implied_ref_and_enforces_strict_above() {
        let engine = RustPlanGateEngine::new(false, String::new());
        // implied_plan_ref_price = 10000 / 100 = 100.00, initial_stop_price = 95.00 => max_plan_stop_dist = 5.00
        engine.upsert_plan_item(sample_envelope(), false);

        // 1. Entry below implied_plan_ref_price (e.g. earlier backtest bar at 80.00):
        // effective_stop_floor = max(0.01, 80.00 - 5.00) = 75.00
        let mut req_below_ok = sample_request(dec!(20));
        req_below_ok.reference_price = Some(dec!(80.00));
        req_below_ok.limit_price = Some(dec!(80.20));
        req_below_ok.stop_loss_price = Some(dec!(76.00));
        let d_below_ok = engine.evaluate_and_reserve(&req_below_ok);
        assert!(
            d_below_ok.allowed,
            "expected stop 76.00 >= effective_stop_floor 75.00 to be admitted, got {:?}",
            d_below_ok.reason
        );

        // 2. Entry at 80.00 with stop_loss_price = 74.50 (< 75.00 effective_stop_floor) must be denied
        let mut req_below_bad = sample_request(dec!(20));
        req_below_bad.client_order_id = "O-2".to_string();
        req_below_bad.reference_price = Some(dec!(80.00));
        req_below_bad.limit_price = Some(dec!(80.20));
        req_below_bad.stop_loss_price = Some(dec!(74.50));
        let d_below_bad = engine.evaluate_and_reserve(&req_below_bad);
        assert!(!d_below_bad.allowed);
        let reason_below_bad = d_below_bad.reason.unwrap_or_default();
        assert!(reason_below_bad.contains("STOP_LOSS_BELOW_PLAN_FLOOR"));
        assert!(reason_below_bad.contains("75"));

        // 3. Entry >= implied_plan_ref_price (100.20 >= 100.00):
        // effective_stop_floor remains strictly plan_item.initial_stop_price (95.00)
        let mut req_above_bad = sample_request(dec!(20));
        req_above_bad.client_order_id = "O-3".to_string();
        req_above_bad.reference_price = Some(dec!(100.20));
        req_above_bad.limit_price = Some(dec!(100.40));
        req_above_bad.stop_loss_price = Some(dec!(94.80));
        let d_above_bad = engine.evaluate_and_reserve(&req_above_bad);
        assert!(!d_above_bad.allowed);
        let reason_above_bad = d_above_bad.reason.unwrap_or_default();
        assert!(reason_above_bad.contains("STOP_LOSS_BELOW_PLAN_FLOOR"));
        assert!(reason_above_bad.contains("95.00"));
    }

    #[test]
    fn test_short_effective_stop_ceiling_scales_above_implied_ref_and_enforces_strict_below() {
        let engine = RustPlanGateEngine::new(false, String::new());
        let mut env = sample_envelope();
        env.side = "SHORT".to_string();
        // implied_plan_ref_price = 100.00, initial_stop_price = 105.00 => max_plan_stop_dist = 5.00
        env.initial_stop_price = dec!(105.00);
        engine.upsert_plan_item(env, false);

        // 1. Entry above implied_plan_ref_price (120.00 > 100.00):
        // effective_stop_ceiling = 120.00 + 5.00 = 125.00
        let mut req_above_ok = sample_request(dec!(20));
        req_above_ok.order_side = "SELL".to_string();
        req_above_ok.reference_price = Some(dec!(120.00));
        req_above_ok.limit_price = Some(dec!(119.80));
        req_above_ok.stop_loss_price = Some(dec!(124.00));
        let d_above_ok = engine.evaluate_and_reserve(&req_above_ok);
        assert!(
            d_above_ok.allowed,
            "expected SHORT stop 124.00 <= effective_stop_ceiling 125.00 to be admitted, got {:?}",
            d_above_ok.reason
        );

        // 2. Entry at 120.00 with stop_loss_price = 125.50 (> 125.00 effective_stop_ceiling) must be denied
        let mut req_above_bad = sample_request(dec!(20));
        req_above_bad.client_order_id = "O-2".to_string();
        req_above_bad.order_side = "SELL".to_string();
        req_above_bad.reference_price = Some(dec!(120.00));
        req_above_bad.limit_price = Some(dec!(119.80));
        req_above_bad.stop_loss_price = Some(dec!(125.50));
        let d_above_bad = engine.evaluate_and_reserve(&req_above_bad);
        assert!(!d_above_bad.allowed);
        let reason_above_bad = d_above_bad.reason.unwrap_or_default();
        assert!(reason_above_bad.contains("STOP_LOSS_ABOVE_PLAN_CEILING"));
        assert!(reason_above_bad.contains("125"));

        // 3. Entry <= implied_plan_ref_price (99.80 <= 100.00):
        // effective_stop_ceiling remains strictly plan_item.initial_stop_price (105.00)
        let mut req_below_bad = sample_request(dec!(20));
        req_below_bad.client_order_id = "O-3".to_string();
        req_below_bad.order_side = "SELL".to_string();
        req_below_bad.reference_price = Some(dec!(99.80));
        req_below_bad.limit_price = Some(dec!(99.60));
        req_below_bad.stop_loss_price = Some(dec!(105.20));
        let d_below_bad = engine.evaluate_and_reserve(&req_below_bad);
        assert!(!d_below_bad.allowed);
        let reason_below_bad = d_below_bad.reason.unwrap_or_default();
        assert!(reason_below_bad.contains("STOP_LOSS_ABOVE_PLAN_CEILING"));
        assert!(reason_below_bad.contains("105.00"));
    }

    #[test]
    fn test_release_reservation_tracks_notional_and_zeros_when_ref_price_differs() {
        let engine = RustPlanGateEngine::new(false, String::new());
        // Plan item implied ref price = 10000 / 100 = 100.00
        engine.upsert_plan_item(sample_envelope(), false);

        // Admit 60 shares at entry ref_price = 100.40 (added_notional = 6024.00 != 6000.00)
        let mut req = sample_request(dec!(60));
        req.reference_price = Some(dec!(100.40));
        req.limit_price = Some(dec!(100.50));
        let d1 = engine.evaluate_and_reserve(&req);
        assert!(d1.allowed);
        assert_eq!(d1.added_notional_usd.as_deref(), Some("6024.00"));
        assert_eq!(engine.reserved_qty("NVDA"), dec!(60));
        let _ = engine.drain_pending_mutations(10);

        // Simulate sync_from_db / upsert_plan_item with preserve_higher_reservation=true
        // where DB filled_qty is still 0 (or 60): reserved_notional_by_symbol must stay 6024.00
        engine.upsert_plan_item(sample_envelope(), true);
        {
            let guard = engine.state.read();
            assert_eq!(
                guard.reserved_notional_by_symbol.get("NVDA").copied(),
                Some(dec!(6024.00))
            );
        }

        // Partial release of 20 shares: proportional notional 6024.00 * (20/60) = 2008.00 released, 4016.00 remaining
        let rem_partial = engine.release_reservation("NVDA", dec!(20));
        assert_eq!(rem_partial, dec!(40));
        let mut_partial = engine.drain_pending_mutations(10);
        assert_eq!(mut_partial.len(), 1);
        let parsed_partial: serde_json::Value = serde_json::from_str(&mut_partial[0]).unwrap();
        assert_eq!(parsed_partial["kind"], "RELEASE_RESERVATION");
        assert_eq!(parsed_partial["new_reserved_qty"], "40");
        assert_eq!(parsed_partial["released_notional_usd"], "2008.00");

        // Full release of remaining 40 shares: releases remaining 4016.00 and resets reserved_notional_by_symbol to exact 0
        let rem_full = engine.release_reservation("NVDA", dec!(40));
        assert_eq!(rem_full, dec!(0));
        let mut_full = engine.drain_pending_mutations(10);
        assert_eq!(mut_full.len(), 1);
        let parsed_full: serde_json::Value = serde_json::from_str(&mut_full[0]).unwrap();
        assert_eq!(parsed_full["kind"], "RELEASE_RESERVATION");
        assert_eq!(parsed_full["new_reserved_qty"], "0");
        assert_eq!(parsed_full["released_notional_usd"], "4016.00");

        // Verify internal reserved_notional_by_symbol is exact 0 and item status reverted to ARMED
        {
            let guard = engine.state.read();
            assert_eq!(
                guard.reserved_notional_by_symbol.get("NVDA").copied(),
                Some(Decimal::ZERO)
            );
            assert_eq!(
                guard.items_by_symbol.get("NVDA").map(|i| i.status.as_str()),
                Some("ARMED")
            );
        }

        // Subsequent full-cap 100 share entry is admitted cleanly
        let mut req2 = sample_request(dec!(100));
        req2.client_order_id = "O-2".to_string();
        let d2 = engine.evaluate_and_reserve(&req2);
        assert!(d2.allowed);
        assert_eq!(engine.reserved_qty("NVDA"), dec!(100));
    }
}
