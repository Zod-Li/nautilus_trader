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

//! [`ExecutionClientFactory`] implementation for `nautilus-alpaca`.

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::ExecutionClient,
    clock::Clock,
    factories::{ClientConfig, ExecutionClientFactory},
};
use nautilus_model::identifiers::{AccountId, ClientId, TraderId};

use crate::{
    common::consts::ALPACA_VENUE,
    config::AlpacaConfig,
    execution::AlpacaExecutionClient,
};

impl ClientConfig for AlpacaConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for creating [`AlpacaExecutionClient`] instances in Nautilus `LiveNode`.
#[derive(Debug, Clone, Default)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.alpaca", from_py_object)
)]
pub struct AlpacaExecutionClientFactory;

impl AlpacaExecutionClientFactory {
    /// Creates a new [`AlpacaExecutionClientFactory`].
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl ExecutionClientFactory for AlpacaExecutionClientFactory {
    fn create(
        &self,
        trader_id: TraderId,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let alpaca_config = config
            .as_any()
            .downcast_ref::<AlpacaConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for AlpacaExecutionClientFactory: expected AlpacaExecClientConfig, got {config:?}"
                )
            })?
            .clone();

        let client_name = if name.trim().is_empty() {
            ALPACA_VENUE
        } else {
            name.trim()
        };
        let client_id = ClientId::from(client_name);
        let account_id = AccountId::from(format!("{}-001", alpaca_config.venue));
        let client = AlpacaExecutionClient::new(
            trader_id,
            client_id,
            account_id,
            alpaca_config,
            cache,
        )?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        ALPACA_VENUE
    }

    fn config_type(&self) -> &'static str {
        "AlpacaExecClientConfig"
    }
}
