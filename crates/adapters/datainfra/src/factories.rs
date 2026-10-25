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

//! `DataInfraDataClientFactory` implementation of [`DataClientFactory`].

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{
    config::DataInfraConfig,
    data::{DATAINFRA, DataInfraDataClient},
};

/// Type alias for `LiveNode` data client configuration compatibility.
pub type DataInfraDataClientConfig = DataInfraConfig;

impl ClientConfig for DataInfraConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for creating [`DataInfraDataClient`] instances in `LiveNode`.
#[derive(Debug, Clone, Default)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.datainfra", from_py_object)
)]
pub struct DataInfraDataClientFactory;

impl DataInfraDataClientFactory {
    /// Creates a new [`DataInfraDataClientFactory`] instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl DataClientFactory for DataInfraDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let cfg = config
            .as_any()
            .downcast_ref::<DataInfraConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for DataInfraDataClientFactory. Expected DataInfraConfig, was {config:?}"
                )
            })?
            .clone();

        let client = DataInfraDataClient::new(ClientId::from(name), cfg)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        DATAINFRA
    }

    fn config_type(&self) -> &'static str {
        "DataInfraConfig"
    }
}
