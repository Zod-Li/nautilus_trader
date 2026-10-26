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

use ahash::AHashMap;
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
    pending_mutations: VecDeque<String>,
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
    pub fn upsert_plan_item(&self, item: RustPlanItemEnvelope, preserve_higher_reservation: bool) {
        let mut guard = self.state.write();
        let sym = item.symbol.clone();
        let db_filled = item.filled_qty;
        let next_reserved = if preserve_higher_reservation {
            guard
                .reserved_qty_by_symbol
                .get(&sym)
                .copied()
                .unwrap_or(Decimal::ZERO)
                .max(db_filled)
        } else {
            db_filled
        };
        guard.reserved_qty_by_symbol.insert(sym.clone(), next_reserved);
        guard.items_by_symbol.insert(sym, item);
    }

    /// Removes a plan item for `symbol` from the in-memory table.
    pub fn remove_plan_item(&self, symbol: &str) {
        let mut guard = self.state.write();
        guard.items_by_symbol.remove(symbol);
        guard.reserved_qty_by_symbol.remove(symbol);
    }

    /// Clears all cached plan items.
    pub fn clear_plan_items(&self) {
        let mut guard = self.state.write();
        guard.items_by_symbol.clear();
        guard.reserved_qty_by_symbol.clear();
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

        if guard.backend_unavailable {
            return RustPlanGateDecision {
                allowed: false,
                reason: Some(format!(
                    "BACKEND_UNAVAILABLE: {}",
                    guard.backend_unavailable_reason
                )),
                is_exit_bypass: false,
                derived_limit_price: None,
                item_id: item_id_opt,
                plan_id: plan_id_opt,
                new_reserved_qty: None,
                added_notional_usd: None,
            };
        }

        if guard.entry_freeze {
            let detail = if guard.entry_freeze_reason.is_empty() {
                String::new()
            } else {
                format!(" ({})", guard.entry_freeze_reason)
            };
            let reason = format!("ENTRY_FREEZE_ACTIVE: global ENTRY_FREEZE is active{detail}");
            Self::enqueue_denied_mutation(
                &mut guard,
                request,
                &reason,
                "CRITICAL",
                item_id_opt.as_deref(),
                plan_id_opt.as_deref(),
            );
            return RustPlanGateDecision {
                allowed: false,
                reason: Some(reason),
                is_exit_bypass: false,
                derived_limit_price: None,
                item_id: item_id_opt,
                plan_id: plan_id_opt,
                new_reserved_qty: None,
                added_notional_usd: None,
            };
        }

        let Some(plan_item) = item_opt else {
            let reason = format!(
                "NO_LOCKED_PLAN_ITEM: no LOCKED PlanItemContract found for symbol {}",
                request.symbol
            );
            Self::enqueue_denied_mutation(&mut guard, request, &reason, "WARN", None, None);
            return RustPlanGateDecision {
                allowed: false,
                reason: Some(reason),
                is_exit_bypass: false,
                derived_limit_price: None,
                item_id: None,
                plan_id: None,
                new_reserved_qty: None,
                added_notional_usd: None,
            };
        };

        if !ADMISSIBLE_STATUSES.contains(&plan_item.status.as_str()) {
            let reason = format!(
                "PLAN_ITEM_STATUS_NOT_ADMISSIBLE: PlanItem {} for {} has status '{}' (must be in ('ARMED', 'TRIGGERED', 'PARTIALLY_FILLED'))",
                plan_item.item_id, request.symbol, plan_item.status
            );
            Self::enqueue_denied_mutation(
                &mut guard,
                request,
                &reason,
                "WARN",
                Some(&plan_item.item_id),
                Some(&plan_item.plan_id),
            );
            return RustPlanGateDecision {
                allowed: false,
                reason: Some(reason),
                is_exit_bypass: false,
                derived_limit_price: None,
                item_id: Some(plan_item.item_id),
                plan_id: Some(plan_item.plan_id),
                new_reserved_qty: None,
                added_notional_usd: None,
            };
        }

        if request.evaluation_ts_ns > plan_item.valid_until_ns {
            let reason = format!(
                "PLAN_ITEM_EXPIRED: evaluation_ts ({}) > valid_until ({})",
                request.evaluation_ts_iso, plan_item.valid_until_iso
            );
            Self::enqueue_denied_mutation(
                &mut guard,
                request,
                &reason,
                "WARN",
                Some(&plan_item.item_id),
                Some(&plan_item.plan_id),
            );
            return RustPlanGateDecision {
                allowed: false,
                reason: Some(reason),
                is_exit_bypass: false,
                derived_limit_price: None,
                item_id: Some(plan_item.item_id),
                plan_id: Some(plan_item.plan_id),
                new_reserved_qty: None,
                added_notional_usd: None,
            };
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
            Self::enqueue_denied_mutation(
                &mut guard,
                request,
                &reason,
                "WARN",
                Some(&plan_item.item_id),
                Some(&plan_item.plan_id),
            );
            return RustPlanGateDecision {
                allowed: false,
                reason: Some(reason),
                is_exit_bypass: false,
                derived_limit_price: None,
                item_id: Some(plan_item.item_id),
                plan_id: Some(plan_item.plan_id),
                new_reserved_qty: None,
                added_notional_usd: None,
            };
        }

        if request.quantity <= Decimal::ZERO {
            let reason = format!(
                "INVALID_QUANTITY: order quantity must be > 0, got {}",
                request.quantity
            );
            Self::enqueue_denied_mutation(
                &mut guard,
                request,
                &reason,
                "WARN",
                Some(&plan_item.item_id),
                Some(&plan_item.plan_id),
            );
            return RustPlanGateDecision {
                allowed: false,
                reason: Some(reason),
                is_exit_bypass: false,
                derived_limit_price: None,
                item_id: Some(plan_item.item_id),
                plan_id: Some(plan_item.plan_id),
                new_reserved_qty: None,
                added_notional_usd: None,
            };
        }

        let prior_reserved = guard
            .reserved_qty_by_symbol
            .get(&request.symbol)
            .copied()
            .unwrap_or(plan_item.filled_qty);
        let cache_total = request.cache_filled_entry_qty + request.cache_open_entry_qty;
        let effective_prior_qty = prior_reserved.max(cache_total);
        let projected_total_qty = effective_prior_qty + request.quantity;

        if projected_total_qty > plan_item.approved_qty_cap {
            let reason = format!(
                "QTY_CAP_EXCEEDED: order_qty ({}) + prior_qty ({}) = {} > approved_qty_cap ({})",
                request.quantity,
                effective_prior_qty,
                projected_total_qty,
                plan_item.approved_qty_cap
            );
            Self::enqueue_denied_mutation(
                &mut guard,
                request,
                &reason,
                "WARN",
                Some(&plan_item.item_id),
                Some(&plan_item.plan_id),
            );
            return RustPlanGateDecision {
                allowed: false,
                reason: Some(reason),
                is_exit_bypass: false,
                derived_limit_price: None,
                item_id: Some(plan_item.item_id),
                plan_id: Some(plan_item.plan_id),
                new_reserved_qty: None,
                added_notional_usd: None,
            };
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
            Self::enqueue_denied_mutation(
                &mut guard,
                request,
                &reason,
                "WARN",
                Some(&plan_item.item_id),
                Some(&plan_item.plan_id),
            );
            return RustPlanGateDecision {
                allowed: false,
                reason: Some(reason),
                is_exit_bypass: false,
                derived_limit_price: None,
                item_id: Some(plan_item.item_id),
                plan_id: Some(plan_item.plan_id),
                new_reserved_qty: None,
                added_notional_usd: None,
            };
        }

        let worst_limit_px = plan_item.compute_worst_limit_price(ref_price, request.atr);

        let derived_limit_price = if let Some(limit_px) = request.limit_price {
            if plan_item.side.eq_ignore_ascii_case("LONG") && limit_px > worst_limit_px {
                let reason = format!(
                    "COLLAR_LIMIT_VIOLATION: BUY limit_price ({limit_px}) exceeds worst_limit_price ({worst_limit_px}) from reference_price ({ref_price})"
                );
                Self::enqueue_denied_mutation(
                    &mut guard,
                    request,
                    &reason,
                    "WARN",
                    Some(&plan_item.item_id),
                    Some(&plan_item.plan_id),
                );
                return RustPlanGateDecision {
                    allowed: false,
                    reason: Some(reason),
                    is_exit_bypass: false,
                    derived_limit_price: None,
                    item_id: Some(plan_item.item_id),
                    plan_id: Some(plan_item.plan_id),
                    new_reserved_qty: None,
                    added_notional_usd: None,
                };
            }
            if plan_item.side.eq_ignore_ascii_case("SHORT") && limit_px < worst_limit_px {
                let reason = format!(
                    "COLLAR_LIMIT_VIOLATION: SELL limit_price ({limit_px}) below worst_limit_price ({worst_limit_px}) from reference_price ({ref_price})"
                );
                Self::enqueue_denied_mutation(
                    &mut guard,
                    request,
                    &reason,
                    "WARN",
                    Some(&plan_item.item_id),
                    Some(&plan_item.plan_id),
                );
                return RustPlanGateDecision {
                    allowed: false,
                    reason: Some(reason),
                    is_exit_bypass: false,
                    derived_limit_price: None,
                    item_id: Some(plan_item.item_id),
                    plan_id: Some(plan_item.plan_id),
                    new_reserved_qty: None,
                    added_notional_usd: None,
                };
            }
            None
        } else if request.collar_market_order {
            Some(worst_limit_px.to_string())
        } else {
            None
        };

        // Atomically reserve capacity and transition ARMED -> TRIGGERED in O(1) memory
        guard
            .reserved_qty_by_symbol
            .insert(request.symbol.clone(), projected_total_qty);
        if let Some(mut_item) = guard.items_by_symbol.get_mut(&request.symbol) {
            mut_item.filled_qty = projected_total_qty;
            if mut_item.status == "ARMED" {
                mut_item.status = "TRIGGERED".to_string();
            }
        }

        let added_notional = (request.quantity * ref_price)
            .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);

        let mutation = json!({
            "kind": "ORDER_ADMITTED",
            "plan_id": plan_item.plan_id,
            "item_id": plan_item.item_id,
            "symbol": request.symbol,
            "strategy_id": request.strategy_id,
            "client_order_id": request.client_order_id,
            "quantity": request.quantity.to_string(),
            "new_reserved_qty": projected_total_qty.to_string(),
            "added_notional_usd": added_notional.to_string(),
        });
        guard.pending_mutations.push_back(mutation.to_string());

        RustPlanGateDecision {
            allowed: true,
            reason: None,
            is_exit_bypass: false,
            derived_limit_price,
            item_id: Some(plan_item.item_id),
            plan_id: Some(plan_item.plan_id),
            new_reserved_qty: Some(projected_total_qty.to_string()),
            added_notional_usd: Some(added_notional.to_string()),
        }
    }

    /// Releases reserved entry quantity in $O(1)$ memory and enqueues an async DB update.
    pub fn release_reservation(&self, symbol: &str, quantity: Decimal) -> Decimal {
        if quantity <= Decimal::ZERO {
            return self.reserved_qty(symbol);
        }
        let mut guard = self.state.write();
        let current = guard
            .reserved_qty_by_symbol
            .get(symbol)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let updated = (current - quantity).max(Decimal::ZERO);
        guard
            .reserved_qty_by_symbol
            .insert(symbol.to_string(), updated);
        if let Some(item) = guard.items_by_symbol.get_mut(symbol) {
            item.filled_qty = updated;
        }
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
            collar_market_order: false,
            is_exit_intent: false,
            cache_open_entry_qty: dec!(0),
            cache_filled_entry_qty: dec!(0),
        }
    }

    #[test]
    fn test_o1_admit_and_cap_enforcement() {
        let engine = RustPlanGateEngine::new(false, String::new());
        engine.upsert_plan_item(sample_envelope(), false);

        let d1 = engine.evaluate_and_reserve(&sample_request(dec!(60)));
        assert!(d1.allowed);
        assert_eq!(engine.reserved_qty("NVDA"), dec!(60));

        let d2 = engine.evaluate_and_reserve(&sample_request(dec!(50)));
        assert!(!d2.allowed);
        assert!(d2.reason.unwrap().contains("QTY_CAP_EXCEEDED"));
        assert_eq!(engine.reserved_qty("NVDA"), dec!(60));

        let released = engine.release_reservation("NVDA", dec!(20));
        assert_eq!(released, dec!(40));

        let d3 = engine.evaluate_and_reserve(&sample_request(dec!(50)));
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
}
