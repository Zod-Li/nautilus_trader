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

use std::str::FromStr;

use ahash::AHashMap;
use futures_util::future::join_all;
use nautilus_common::factories::{ClientConfig, ExecutionClientFactory};
use nautilus_core::{
    UnixNanos,
    python::{to_pyruntime_err, to_pyvalue_err},
    time::get_atomic_clock_realtime,
};
use nautilus_model::python::instruments::instrument_any_to_pyobject;
use nautilus_system::get_global_pyo3_registry;
use pyo3::{prelude::*, types::PyDict};
use rust_decimal::Decimal;

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
    error::AlpacaError,
    execution::block_on_alpaca_future,
    factories::AlpacaExecutionClientFactory,
    http::{AlpacaHttpClient, parse_equity_instrument_with_ref_price},
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

fn alpaca_err_to_py(err: AlpacaError) -> PyErr {
    match err {
        AlpacaError::InvalidConfig(_) | AlpacaError::Parse(_) => to_pyvalue_err(err),
        _ => to_pyruntime_err(err),
    }
}

fn parse_alpaca_environment(raw: &str) -> PyResult<AlpacaEnvironment> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "paper" => Ok(AlpacaEnvironment::Paper),
        "live" => Ok(AlpacaEnvironment::Live),
        other => Err(to_pyvalue_err(format!(
            "Invalid Alpaca environment '{other}', expected 'paper' or 'live'"
        ))),
    }
}

/// Concurrently fetches Alpaca `/v2/assets/{symbol}` metadata and converts each symbol into a
/// Nautilus `Equity` instrument with Reg NMS Rule 612 tick precision (`< $1.00 => 0.0001`,
/// `>= $1.00 => 0.01`) and whole-share `lot_size = 1` / `size_precision = 0`.
///
/// Fails closed if any symbol cannot be resolved or is inactive/non-tradable/non-`us_equity`.
#[pyfunction]
#[pyo3(
    name = "load_alpaca_equity_instruments",
    signature = (
        symbols,
        config = None,
        ref_prices = None,
        http_base_url = None,
        api_key = None,
        api_secret = None,
        environment = "paper",
        venue = None,
        timeout_secs = 15,
        max_retries = 3,
        rate_limit_per_sec = 10,
        ts_init = None,
    )
)]
#[expect(clippy::too_many_arguments)]
fn py_load_alpaca_equity_instruments(
    py: Python<'_>,
    symbols: Vec<String>,
    config: Option<AlpacaConfig>,
    ref_prices: Option<Bound<'_, PyDict>>,
    http_base_url: Option<String>,
    api_key: Option<String>,
    api_secret: Option<String>,
    environment: &str,
    venue: Option<String>,
    timeout_secs: u64,
    max_retries: u32,
    rate_limit_per_sec: u32,
    ts_init: Option<u64>,
) -> PyResult<Vec<Py<PyAny>>> {
    let mut parsed_ref_prices: AHashMap<String, Decimal> = AHashMap::new();
    if let Some(dict) = ref_prices {
        for (k, v) in dict.iter() {
            let sym_key = k.extract::<String>()?.trim().to_ascii_uppercase();
            if sym_key.is_empty() {
                return Err(to_pyvalue_err("ref_prices contains an empty symbol key"));
            }
            let val_obj = v.str()?;
            let val_str = val_obj.to_str()?.trim();
            let px = Decimal::from_str(val_str).map_err(|e| {
                to_pyvalue_err(format!("Invalid ref_price for '{sym_key}' ('{val_str}'): {e}"))
            })?;
            if px <= Decimal::ZERO {
                return Err(to_pyvalue_err(format!(
                    "ref_price for '{sym_key}' must be > 0, got {px}"
                )));
            }
            parsed_ref_prices.insert(sym_key, px);
        }
    }

    if symbols.is_empty() {
        return Ok(Vec::new());
    }

    let mut norm_symbols = Vec::with_capacity(symbols.len());
    for raw_sym in &symbols {
        let norm = raw_sym.trim().to_ascii_uppercase();
        if norm.is_empty() {
            return Err(to_pyvalue_err("symbols contains an empty symbol"));
        }
        norm_symbols.push(norm);
    }

    let effective_config = if let Some(mut cfg) = config {
        if let Some(url) = http_base_url {
            cfg.http_base_url = url.trim_end_matches('/').to_string();
        }
        if let Some(ven) = venue {
            cfg.venue = ven;
        }
        match (api_key, api_secret) {
            (Some(k), Some(s)) => {
                cfg.credential = AlpacaCredential::new_checked(k, s).map_err(to_pyvalue_err)?;
            }
            (None, None) => {}
            _ => {
                return Err(to_pyvalue_err(
                    "Both api_key and api_secret must be provided together",
                ));
            }
        }
        cfg.validate().map_err(to_pyvalue_err)?;
        cfg
    } else {
        let env = parse_alpaca_environment(environment)?;
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
        let websocket_url = env.default_ws_url().to_string();
        let venue_val = venue.unwrap_or_else(|| ALPACA_VENUE.to_string());
        AlpacaConfig::new_checked(
            env,
            http_url,
            websocket_url,
            credential,
            timeout_secs,
            max_retries,
            250,
            5_000,
            rate_limit_per_sec,
            venue_val,
        )
        .map_err(to_pyvalue_err)?
    };

    let venue_str = effective_config.venue.clone();
    let http_client = AlpacaHttpClient::new(effective_config).map_err(alpaca_err_to_py)?;
    let symbols_for_fetch = norm_symbols.clone();

    let asset_results = py.detach(|| {
        block_on_alpaca_future(async move {
            let futs = symbols_for_fetch
                .iter()
                .map(|sym| http_client.get_asset(sym));
            join_all(futs).await
        })
    });

    let ts_init_ns = ts_init.map_or_else(
        || get_atomic_clock_realtime().get_time_ns(),
        UnixNanos::from,
    );

    let mut instruments = Vec::with_capacity(norm_symbols.len());
    for (sym, asset_res) in norm_symbols.iter().zip(asset_results) {
        let asset = asset_res.map_err(alpaca_err_to_py)?;
        let ref_px = parsed_ref_prices.get(sym).copied();
        let inst = parse_equity_instrument_with_ref_price(&asset, &venue_str, ts_init_ns, ref_px)
            .map_err(alpaca_err_to_py)?;
        instruments.push(instrument_any_to_pyobject(py, inst)?);
    }

    Ok(instruments)
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
    m.add_function(wrap_pyfunction!(py_load_alpaca_equity_instruments, m)?)?;

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
