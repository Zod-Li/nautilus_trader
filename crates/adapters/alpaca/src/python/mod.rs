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

//! Python (`PyO3`) bindings for the `nautilus-alpaca` execution adapter.

#![expect(
    clippy::missing_errors_doc,
    reason = "errors documented on underlying Rust methods"
)]

use nautilus_common::factories::{ClientConfig, ExecutionClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    common::{
        consts::{
            ALPACA_LIVE_HTTP_URL, ALPACA_LIVE_WS_URL, ALPACA_PAPER_HTTP_URL, ALPACA_PAPER_WS_URL,
            ALPACA_VENUE,
        },
        credential::AlpacaCredential,
        enums::AlpacaEnvironment,
    },
    config::AlpacaConfig,
    factories::AlpacaExecutionClientFactory,
};

#[expect(clippy::needless_pass_by_value)]
fn extract_alpaca_exec_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn ExecutionClientFactory>> {
    match factory.extract::<AlpacaExecutionClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract AlpacaExecutionClientFactory: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_alpaca_exec_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<AlpacaConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract AlpacaExecClientConfig: {e}"
        ))),
    }
}

#[pymethods]
impl AlpacaConfig {
    #[new]
    #[pyo3(signature = (
        api_key = None,
        api_secret = None,
        environment = "paper",
        http_base_url = None,
        ws_url = None,
        timeout_secs = 15,
        max_retries = 3,
        retry_initial_ms = 250,
        retry_max_ms = 5_000,
        rate_limit_per_sec = 10,
        venue = "ALPACA",
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        api_key: Option<String>,
        api_secret: Option<String>,
        environment: &str,
        http_base_url: Option<String>,
        ws_url: Option<String>,
        timeout_secs: u64,
        max_retries: u32,
        retry_initial_ms: u64,
        retry_max_ms: u64,
        rate_limit_per_sec: u32,
        venue: &str,
    ) -> PyResult<Self> {
        let env = match environment.trim().to_ascii_lowercase().as_str() {
            "paper" => AlpacaEnvironment::Paper,
            "live" => AlpacaEnvironment::Live,
            other => {
                return Err(to_pyvalue_err(format!(
                    "Invalid Alpaca environment '{other}', expected 'paper' or 'live'"
                )));
            }
        };

        let credential = match (api_key, api_secret) {
            (Some(k), Some(s)) => AlpacaCredential::new_checked(k, s).map_err(to_pyvalue_err)?,
            (None, None) => AlpacaCredential::from_env().map_err(to_pyvalue_err)?,
            _ => {
                return Err(to_pyvalue_err(
                    "Both api_key and api_secret must be provided together, or both omitted to read from environment",
                ));
            }
        };

        let http_url = http_base_url.unwrap_or_else(|| env.default_http_url().to_string());
        let websocket_url = ws_url.unwrap_or_else(|| env.default_ws_url().to_string());

        Self::new_checked(
            env,
            http_url,
            websocket_url,
            credential,
            timeout_secs,
            max_retries,
            retry_initial_ms,
            retry_max_ms,
            rate_limit_per_sec,
            venue,
        )
        .map_err(to_pyvalue_err)
    }

    #[getter]
    fn environment(&self) -> &'static str {
        match self.environment {
            AlpacaEnvironment::Paper => "paper",
            AlpacaEnvironment::Live => "live",
        }
    }

    #[getter]
    fn http_base_url(&self) -> &str {
        &self.http_base_url
    }

    #[getter]
    fn ws_url(&self) -> &str {
        &self.ws_url
    }

    #[getter]
    fn timeout_secs(&self) -> u64 {
        self.timeout_secs
    }

    #[getter]
    fn max_retries(&self) -> u32 {
        self.max_retries
    }

    #[getter]
    fn rate_limit_per_sec(&self) -> u32 {
        self.rate_limit_per_sec
    }

    #[getter]
    fn venue(&self) -> &str {
        &self.venue
    }

    #[getter]
    fn masked_key_id(&self) -> String {
        self.credential.masked_key_id()
    }

    fn validate_config(&self) -> PyResult<()> {
        self.validate().map_err(to_pyvalue_err)
    }

    fn __repr__(&self) -> String {
        format!(
            "AlpacaExecClientConfig(environment='{}', venue='{}', http_base_url='{}', ws_url='{}', api_key_id='{}')",
            self.environment(),
            self.venue,
            self.http_base_url,
            self.ws_url,
            self.credential.masked_key_id()
        )
    }
}

#[pymethods]
impl AlpacaExecutionClientFactory {
    #[new]
    fn py_new() -> Self {
        Self::new()
    }

    #[must_use]
    pub fn name(&self) -> &str {
        ALPACA_VENUE
    }
}

/// Exposed through `nautilus_trader._libnautilus.alpaca` and `nautilus_trader.adapters.alpaca`.
#[pymodule]
pub fn alpaca(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("ALPACA", ALPACA_VENUE)?;
    m.add("ALPACA_VENUE", ALPACA_VENUE)?;
    m.add("ALPACA_PAPER_HTTP_URL", ALPACA_PAPER_HTTP_URL)?;
    m.add("ALPACA_LIVE_HTTP_URL", ALPACA_LIVE_HTTP_URL)?;
    m.add("ALPACA_PAPER_WS_URL", ALPACA_PAPER_WS_URL)?;
    m.add("ALPACA_LIVE_WS_URL", ALPACA_LIVE_WS_URL)?;
    m.add_class::<AlpacaConfig>()?;
    m.add_class::<AlpacaExecutionClientFactory>()?;

    let registry = get_global_pyo3_registry();

    if let Err(e) =
        registry.register_exec_factory_extractor(ALPACA_VENUE.to_string(), extract_alpaca_exec_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Alpaca execution factory extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "AlpacaExecClientConfig".to_string(),
        extract_alpaca_exec_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register AlpacaExecClientConfig extractor: {e}"
        )));
    }

    Ok(())
}
