// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Python (`PyO3`) bindings for `nautilus-quant-execution`.

#![expect(
    clippy::missing_errors_doc,
    reason = "errors documented on underlying Rust methods"
)]

use std::str::FromStr;

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;
use rust_decimal::Decimal;

use crate::{
    engine::RustPlanGateEngine,
    models::{RustPlanGateDecision, RustPlanGateOrderRequest, RustPlanItemEnvelope},
};

fn parse_dec(raw: &str, field_name: &str) -> PyResult<Decimal> {
    Decimal::from_str(raw.trim()).map_err(|e| {
        to_pyvalue_err(format!(
            "Invalid decimal for '{field_name}' ('{raw}'): {e}"
        ))
    })
}

fn parse_opt_dec(raw: Option<&str>, field_name: &str) -> PyResult<Option<Decimal>> {
    match raw {
        Some(s) if !s.trim().is_empty() => Ok(Some(parse_dec(s, field_name)?)),
        _ => Ok(None),
    }
}

#[pymethods]
impl RustPlanItemEnvelope {
    #[new]
    #[pyo3(signature = (
        item_id,
        plan_id,
        symbol,
        side,
        status,
        target_qty,
        approved_qty_cap,
        target_notional_usd,
        filled_qty,
        valid_until_ns,
        valid_until_iso,
        limit_collar_pct,
        limit_collar_atr,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        item_id: String,
        plan_id: String,
        symbol: String,
        side: String,
        status: String,
        target_qty: &str,
        approved_qty_cap: &str,
        target_notional_usd: &str,
        filled_qty: &str,
        valid_until_ns: i64,
        valid_until_iso: String,
        limit_collar_pct: &str,
        limit_collar_atr: &str,
    ) -> PyResult<Self> {
        Ok(Self {
            item_id,
            plan_id,
            symbol,
            side,
            status,
            target_qty: parse_dec(target_qty, "target_qty")?,
            approved_qty_cap: parse_dec(approved_qty_cap, "approved_qty_cap")?,
            target_notional_usd: parse_dec(target_notional_usd, "target_notional_usd")?,
            filled_qty: parse_dec(filled_qty, "filled_qty")?,
            valid_until_ns,
            valid_until_iso,
            limit_collar_pct: parse_dec(limit_collar_pct, "limit_collar_pct")?,
            limit_collar_atr: parse_dec(limit_collar_atr, "limit_collar_atr")?,
        })
    }

    #[getter]
    fn item_id(&self) -> &str {
        &self.item_id
    }

    #[getter]
    fn plan_id(&self) -> &str {
        &self.plan_id
    }

    #[getter]
    fn symbol(&self) -> &str {
        &self.symbol
    }

    #[getter]
    fn side(&self) -> &str {
        &self.side
    }

    #[getter]
    fn status(&self) -> &str {
        &self.status
    }
}

#[pymethods]
impl RustPlanGateOrderRequest {
    #[new]
    #[pyo3(signature = (
        client_order_id,
        strategy_id,
        instrument_id,
        symbol,
        venue,
        order_side,
        order_type,
        quantity,
        is_reduce_only,
        evaluation_ts_ns,
        evaluation_ts_iso,
        limit_price = None,
        reference_price = None,
        atr = None,
        collar_market_order = false,
        is_exit_intent = false,
        cache_open_entry_qty = "0",
        cache_filled_entry_qty = "0",
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        client_order_id: String,
        strategy_id: String,
        instrument_id: String,
        symbol: String,
        venue: String,
        order_side: String,
        order_type: String,
        quantity: &str,
        is_reduce_only: bool,
        evaluation_ts_ns: i64,
        evaluation_ts_iso: String,
        limit_price: Option<&str>,
        reference_price: Option<&str>,
        atr: Option<&str>,
        collar_market_order: bool,
        is_exit_intent: bool,
        cache_open_entry_qty: &str,
        cache_filled_entry_qty: &str,
    ) -> PyResult<Self> {
        Ok(Self {
            client_order_id,
            strategy_id,
            instrument_id,
            symbol,
            venue,
            order_side,
            order_type,
            quantity: parse_dec(quantity, "quantity")?,
            is_reduce_only,
            evaluation_ts_ns,
            evaluation_ts_iso,
            limit_price: parse_opt_dec(limit_price, "limit_price")?,
            reference_price: parse_opt_dec(reference_price, "reference_price")?,
            atr: parse_opt_dec(atr, "atr")?,
            collar_market_order,
            is_exit_intent,
            cache_open_entry_qty: parse_dec(cache_open_entry_qty, "cache_open_entry_qty")?,
            cache_filled_entry_qty: parse_dec(cache_filled_entry_qty, "cache_filled_entry_qty")?,
        })
    }
}

#[pymethods]
impl RustPlanGateDecision {
    #[getter]
    fn allowed(&self) -> bool {
        self.allowed
    }

    #[getter]
    fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    #[getter]
    fn is_exit_bypass(&self) -> bool {
        self.is_exit_bypass
    }

    #[getter]
    fn derived_limit_price(&self) -> Option<&str> {
        self.derived_limit_price.as_deref()
    }

    #[getter]
    fn item_id(&self) -> Option<&str> {
        self.item_id.as_deref()
    }

    #[getter]
    fn plan_id(&self) -> Option<&str> {
        self.plan_id.as_deref()
    }

    #[getter]
    fn new_reserved_qty(&self) -> Option<&str> {
        self.new_reserved_qty.as_deref()
    }

    #[getter]
    fn added_notional_usd(&self) -> Option<&str> {
        self.added_notional_usd.as_deref()
    }
}

#[pymethods]
impl RustPlanGateEngine {
    #[new]
    #[pyo3(signature = (entry_freeze = false, entry_freeze_reason = ""))]
    fn py_new(entry_freeze: bool, entry_freeze_reason: &str) -> Self {
        Self::new(entry_freeze, entry_freeze_reason.to_string())
    }

    #[pyo3(name = "set_entry_freeze")]
    #[pyo3(signature = (active, reason = ""))]
    fn py_set_entry_freeze(&self, active: bool, reason: &str) {
        self.set_entry_freeze(active, reason.to_string());
    }

    #[getter]
    #[pyo3(name = "entry_freeze")]
    fn py_entry_freeze(&self) -> bool {
        self.entry_freeze()
    }

    #[getter]
    #[pyo3(name = "entry_freeze_reason")]
    fn py_entry_freeze_reason(&self) -> String {
        self.entry_freeze_reason()
    }

    #[pyo3(name = "set_backend_unavailable")]
    #[pyo3(signature = (unavailable, reason = ""))]
    fn py_set_backend_unavailable(&self, unavailable: bool, reason: &str) {
        self.set_backend_unavailable(unavailable, reason.to_string());
    }

    #[getter]
    #[pyo3(name = "backend_unavailable")]
    fn py_backend_unavailable(&self) -> bool {
        self.backend_unavailable()
    }

    #[pyo3(name = "upsert_plan_item")]
    #[pyo3(signature = (item, preserve_higher_reservation = false))]
    fn py_upsert_plan_item(&self, item: RustPlanItemEnvelope, preserve_higher_reservation: bool) {
        self.upsert_plan_item(item, preserve_higher_reservation);
    }

    #[pyo3(name = "remove_plan_item")]
    fn py_remove_plan_item(&self, symbol: &str) {
        self.remove_plan_item(symbol);
    }

    #[pyo3(name = "clear_plan_items")]
    fn py_clear_plan_items(&self) {
        self.clear_plan_items();
    }

    #[pyo3(name = "reserved_qty")]
    fn py_reserved_qty(&self, symbol: &str) -> String {
        self.reserved_qty(symbol).to_string()
    }

    #[pyo3(name = "evaluate_and_reserve")]
    fn py_evaluate_and_reserve(&self, request: &RustPlanGateOrderRequest) -> RustPlanGateDecision {
        self.evaluate_and_reserve(request)
    }

    #[pyo3(name = "release_reservation")]
    fn py_release_reservation(&self, symbol: &str, quantity: &str) -> PyResult<String> {
        let qty = parse_dec(quantity, "quantity")?;
        Ok(self.release_reservation(symbol, qty).to_string())
    }

    #[pyo3(name = "drain_pending_mutations")]
    #[pyo3(signature = (max_items = 256))]
    fn py_drain_pending_mutations(&self, max_items: usize) -> Vec<String> {
        self.drain_pending_mutations(max_items)
    }
}

/// Exposed through `nautilus_trader._libnautilus.quant_execution`.
#[pymodule]
pub fn quant_execution(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustPlanItemEnvelope>()?;
    m.add_class::<RustPlanGateOrderRequest>()?;
    m.add_class::<RustPlanGateDecision>()?;
    m.add_class::<RustPlanGateEngine>()?;
    Ok(())
}
