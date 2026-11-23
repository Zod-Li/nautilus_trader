// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! External pre-trade risk gate micro-seam for [`crate::RiskEngine`].

use std::fmt::Debug;
#[cfg(feature = "python")]
use std::sync::Arc;

use nautilus_common::{cache::Cache, messages::execution::TradingCommand};
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::ClientOrderId;
use rust_decimal::Decimal;

/// Synchronous in-memory pre-trade gate evaluated at the top of `RiskEngine::execute`.
pub trait PreTradeRiskGate: Debug + Send + Sync {
    /// Validates a trading command against external pre-trade invariants using a read-only
    /// borrow of [`Cache`].
    ///
    /// # Errors
    ///
    /// Returns a human-readable denial reason if the command must be rejected before entering
    /// downstream risk checks or execution.
    fn check_command(
        &self,
        command: &TradingCommand,
        cache: &Cache,
        ts_now: UnixNanos,
    ) -> Result<(), String>;

    /// Notifies the gate that an order previously admitted by [`Self::check_command`] was
    /// subsequently denied by downstream [`crate::RiskEngine`] checks, allowing idempotent
    /// capacity rollback.
    fn on_order_denied(&self, _client_order_id: &ClientOrderId) {}

    /// Notifies the gate that an order was updated by the venue (e.g. downward quantity
    /// modification confirmed via `OrderUpdated`), allowing reservation reconciliation.
    fn on_order_updated(&self, _client_order_id: &ClientOrderId, _new_qty: Decimal) {}
}

/// Python-exposed handle wrapping a shared [`PreTradeRiskGate`] instance.
#[cfg(feature = "python")]
#[derive(Clone, Debug)]
#[pyo3::pyclass(
    module = "nautilus_trader.risk",
    name = "PreTradeRiskGateHandle",
    frozen,
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.risk")]
pub struct PyPreTradeRiskGateHandle(pub Arc<dyn PreTradeRiskGate>);
