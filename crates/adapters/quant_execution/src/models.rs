// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Domain models for the `nautilus-quant-execution` in-memory `PlanGate` engine.

use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};

/// In-memory representation of a locked `PlanItemContract` risk envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.quant_execution", from_py_object)
)]
pub struct RustPlanItemEnvelope {
    pub item_id: String,
    pub plan_id: String,
    pub symbol: String,
    pub side: String,
    pub status: String,
    pub target_qty: Decimal,
    pub approved_qty_cap: Decimal,
    pub target_notional_usd: Decimal,
    pub filled_qty: Decimal,
    pub valid_until_ns: i64,
    pub valid_until_iso: String,
    pub limit_collar_pct: Decimal,
    pub limit_collar_atr: Decimal,
}

impl RustPlanItemEnvelope {
    /// Computes the worst permissible limit price for an entry order given reference price and optional ATR.
    #[must_use]
    pub fn compute_worst_limit_price(
        &self,
        reference_price: Decimal,
        atr: Option<Decimal>,
    ) -> Decimal {
        let hundred = Decimal::from(100);
        let pct_offset = reference_price * (self.limit_collar_pct / hundred);
        let allowed_offset = match atr {
            Some(atr_val) if self.limit_collar_atr > Decimal::ZERO => {
                let atr_offset = atr_val * self.limit_collar_atr;
                pct_offset.min(atr_offset)
            }
            _ => pct_offset,
        };

        let one_cent = Decimal::new(1, 2);
        if self.side.eq_ignore_ascii_case("LONG") {
            (reference_price + allowed_offset)
                .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven)
        } else {
            let candidate = (reference_price - allowed_offset)
                .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
            candidate.max(one_cent)
        }
    }
}

/// Normalized pre-trade order admission request evaluated in $O(1)$ memory by `RustPlanGateEngine`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.quant_execution", from_py_object)
)]
pub struct RustPlanGateOrderRequest {
    pub client_order_id: String,
    pub strategy_id: String,
    pub instrument_id: String,
    pub symbol: String,
    pub venue: String,
    pub order_side: String,
    pub order_type: String,
    pub quantity: Decimal,
    pub is_reduce_only: bool,
    pub evaluation_ts_ns: i64,
    pub evaluation_ts_iso: String,
    pub limit_price: Option<Decimal>,
    pub reference_price: Option<Decimal>,
    pub atr: Option<Decimal>,
    pub collar_market_order: bool,
    pub is_exit_intent: bool,
    pub cache_open_entry_qty: Decimal,
    pub cache_filled_entry_qty: Decimal,
}

/// Authoritative pre-trade admission decision returned in $O(1)$ by `RustPlanGateEngine`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.quant_execution", from_py_object)
)]
pub struct RustPlanGateDecision {
    pub allowed: bool,
    pub reason: Option<String>,
    pub is_exit_bypass: bool,
    pub derived_limit_price: Option<String>,
    pub item_id: Option<String>,
    pub plan_id: Option<String>,
    pub new_reserved_qty: Option<String>,
    pub added_notional_usd: Option<String>,
}
