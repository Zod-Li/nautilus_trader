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

//! Python (`PyO3`) bindings for the `nautilus-datainfra` adapter.

#![expect(
    clippy::missing_errors_doc,
    reason = "errors documented on underlying Rust methods"
)]

use std::str::FromStr;

use nautilus_common::factories::{ClientConfig, DataClientFactory};
use nautilus_core::{
    UnixNanos,
    python::{to_pyruntime_err, to_pyvalue_err},
};
use nautilus_model::{
    data::{Bar, BarType, InstrumentStatus, NautilusDataType, QuoteTick},
    identifiers::InstrumentId,
    instruments::Equity,
};
use nautilus_persistence::catalog::traits::CatalogReader;
use nautilus_system::get_global_pyo3_registry;
use pyo3::{prelude::*, types::PyDict};

use crate::{
    arrow_decoder::{
        ArrowDecodeOptions, PriceAdjustmentMode, decode_bars_arrow_stream, decode_bars_parquet,
    },
    calendar::CalendarSessionGate,
    catalog::{DataInfraCatalog, block_on_catalog_future},
    clickhouse::{BarsQueryRequest, QuotesQueryRequest, StatusesQueryRequest},
    config::DataInfraConfig,
    data::DATAINFRA,
    factories::DataInfraDataClientFactory,
    instrument::DataInfraEquityMetadata,
    mapping::{DataInfraTimeframe, to_bar_type, to_instrument_id},
};

#[expect(clippy::needless_pass_by_value)]
fn extract_datainfra_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    match factory.extract::<DataInfraDataClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract DataInfraDataClientFactory: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_datainfra_data_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<DataInfraConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract DataInfraConfig: {e}"
        ))),
    }
}

#[pymethods]
impl DataInfraConfig {
    #[new]
    #[pyo3(signature = (
        clickhouse_url = None,
        clickhouse_database = None,
        request_timeout_ms = None,
        max_retries = None,
        kafka_brokers = None,
        kafka_group_id = None,
        calendar_service_url = None,
        default_venue = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        clickhouse_url: Option<String>,
        clickhouse_database: Option<String>,
        request_timeout_ms: Option<u64>,
        max_retries: Option<u32>,
        kafka_brokers: Option<Vec<String>>,
        kafka_group_id: Option<String>,
        calendar_service_url: Option<String>,
        default_venue: Option<String>,
    ) -> PyResult<Self> {
        let defaults = Self::default();
        Self::new_checked(
            clickhouse_url.unwrap_or(defaults.clickhouse_url),
            clickhouse_database.unwrap_or(defaults.clickhouse_database),
            request_timeout_ms.unwrap_or(defaults.request_timeout_ms),
            max_retries.unwrap_or(defaults.max_retries),
            kafka_brokers.unwrap_or(defaults.kafka_brokers),
            kafka_group_id.unwrap_or(defaults.kafka_group_id),
            calendar_service_url.unwrap_or(defaults.calendar_service_url),
            default_venue.unwrap_or(defaults.default_venue),
        )
        .map_err(to_pyvalue_err)
    }

    fn validate_config(&self) -> PyResult<()> {
        self.validate().map_err(to_pyvalue_err)
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
impl DataInfraDataClientFactory {
    #[new]
    fn py_new() -> Self {
        Self::new()
    }

    #[must_use]
    pub fn name(&self) -> &str {
        DATAINFRA
    }
}

/// Python wrapper for [`DataInfraCatalog`] enabling direct historical loading of
/// ClickHouse `Bar`, `QuoteTick`, and `InstrumentStatus` objects into `BacktestEngine`.
#[derive(Debug, Clone)]
#[pyclass(
    name = "DataInfraCatalog",
    module = "nautilus_trader.adapters.datainfra",
    from_py_object
)]
pub struct PyDataInfraCatalog {
    inner: DataInfraCatalog,
}

#[pymethods]
impl PyDataInfraCatalog {
    #[new]
    #[pyo3(signature = (config = None))]
    fn py_new(config: Option<DataInfraConfig>) -> PyResult<Self> {
        let cfg = config.unwrap_or_default();
        let inner = DataInfraCatalog::from_config(cfg).map_err(to_pyvalue_err)?;
        Ok(Self { inner })
    }

    #[getter]
    fn config(&self) -> DataInfraConfig {
        self.inner.config().clone()
    }

    /// Registers and returns an [`Equity`] instrument for `symbol` and `venue`.
    #[pyo3(signature = (symbol, venue = None, lot_size = None, ts_init = None))]
    fn register_equity(
        &mut self,
        symbol: &str,
        venue: Option<&str>,
        lot_size: Option<u64>,
        ts_init: Option<u64>,
    ) -> PyResult<Equity> {
        let venue_code = venue.unwrap_or(&self.inner.config().default_venue);
        let mut meta = DataInfraEquityMetadata::new(symbol, venue_code);
        if let Some(lot) = lot_size {
            meta = meta.with_lot_size(lot);
        }
        let init_ns = UnixNanos::from(ts_init.unwrap_or(0));
        let equity = meta.to_equity_any(init_ns).map_err(to_pyvalue_err)?;
        let _ = self
            .inner
            .register_equity(&meta, init_ns)
            .map_err(to_pyvalue_err)?;
        Ok(equity)
    }

    /// Queries ClickHouse `v_quant_bars` via `ArrowStream` and returns a sorted `list[Bar]`.
    #[pyo3(signature = (
        symbols,
        timeframe = "15m",
        start_ts_ns = None,
        end_ts_ns = None,
        adjustment = "adjusted",
        market_session = Some("REG"),
        limit_per_symbol = None,
        venue = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn query_bars(
        &self,
        symbols: Vec<String>,
        timeframe: &str,
        start_ts_ns: Option<u64>,
        end_ts_ns: Option<u64>,
        adjustment: &str,
        market_session: Option<&str>,
        limit_per_symbol: Option<u32>,
        venue: Option<&str>,
    ) -> PyResult<Vec<Bar>> {
        let tf = DataInfraTimeframe::from_str(timeframe).map_err(to_pyvalue_err)?;
        let adj = match adjustment.trim().to_ascii_lowercase().as_str() {
            "adjusted" | "adj" => PriceAdjustmentMode::Adjusted,
            "raw" | "unadjusted" => PriceAdjustmentMode::Raw,
            other => {
                return Err(to_pyvalue_err(format!(
                    "Unsupported adjustment mode '{other}', expected 'adjusted' or 'raw'"
                )));
            }
        };
        let venue_code = venue
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| self.inner.config().default_venue.clone());
        let mut req = BarsQueryRequest::new(symbols, tf, venue_code, adj)
            .with_time_range(start_ts_ns.map(UnixNanos::from), end_ts_ns.map(UnixNanos::from));
        req.market_session = market_session.and_then(|s| {
            let trimmed = s.trim();
            if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("ALL") {
                None
            } else {
                Some(trimmed.to_string())
            }
        });
        if let Some(lim) = limit_per_symbol {
            req = req.with_limit_per_symbol(lim);
        }
        self.inner.load_bars(&req).map_err(to_pyruntime_err)
    }

    /// Queries ClickHouse `sip_raw_quotes` via `ArrowStream` and returns a sorted `list[QuoteTick]`.
    #[pyo3(signature = (symbols, start_ts_ns = None, end_ts_ns = None, limit = None, venue = None))]
    fn query_quotes(
        &self,
        symbols: Vec<String>,
        start_ts_ns: Option<u64>,
        end_ts_ns: Option<u64>,
        limit: Option<u32>,
        venue: Option<&str>,
    ) -> PyResult<Vec<QuoteTick>> {
        let venue_code = venue
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| self.inner.config().default_venue.clone());
        let mut req = QuotesQueryRequest::new(symbols, venue_code);
        req.start_ts_ns = start_ts_ns.map(UnixNanos::from);
        req.end_ts_ns = end_ts_ns.map(UnixNanos::from);
        req.limit = limit;
        self.inner.load_quotes(&req).map_err(to_pyruntime_err)
    }

    /// Queries ClickHouse `sip_trading_statuses` via `ArrowStream` and returns a sorted `list[InstrumentStatus]`.
    #[pyo3(signature = (symbols, start_ts_ns = None, end_ts_ns = None, limit = None, venue = None))]
    fn query_statuses(
        &self,
        symbols: Vec<String>,
        start_ts_ns: Option<u64>,
        end_ts_ns: Option<u64>,
        limit: Option<u32>,
        venue: Option<&str>,
    ) -> PyResult<Vec<InstrumentStatus>> {
        let venue_code = venue
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| self.inner.config().default_venue.clone());
        let mut req = StatusesQueryRequest::new(symbols, venue_code);
        req.start_ts_ns = start_ts_ns.map(UnixNanos::from);
        req.end_ts_ns = end_ts_ns.map(UnixNanos::from);
        req.limit = limit;
        self.inner.load_statuses(&req).map_err(to_pyruntime_err)
    }

    /// Queries the latest timestamp (`u64` nanoseconds) covered by the catalog for `(data_type, identifier)`.
    #[pyo3(signature = (data_type, identifier = None))]
    fn query_last_timestamp(
        &mut self,
        data_type: &str,
        identifier: Option<&str>,
    ) -> PyResult<Option<u64>> {
        let dt = data_type
            .parse::<NautilusDataType>()
            .map_err(to_pyvalue_err)?;
        self.inner
            .query_last_timestamp(dt, identifier)
            .map_err(to_pyruntime_err)
    }

    /// Returns missing `(start_ns, end_ns)` intervals for a catalog request range.
    #[pyo3(signature = (start_ts_ns, end_ts_ns, data_type, identifier = None))]
    fn get_missing_intervals(
        &mut self,
        start_ts_ns: u64,
        end_ts_ns: u64,
        data_type: &str,
        identifier: Option<&str>,
    ) -> PyResult<Vec<(u64, u64)>> {
        let dt = data_type
            .parse::<NautilusDataType>()
            .map_err(to_pyvalue_err)?;
        self.inner
            .get_missing_intervals_for_request(
                UnixNanos::from(start_ts_ns),
                UnixNanos::from(end_ts_ns),
                dt,
                identifier,
            )
            .map_err(to_pyruntime_err)
    }
}

/// Python wrapper for [`CalendarSessionGate`] querying `calendar-service` (`:8012`).
#[derive(Debug, Clone)]
#[pyclass(
    name = "CalendarSessionGate",
    module = "nautilus_trader.adapters.datainfra",
    from_py_object
)]
pub struct PyCalendarSessionGate {
    inner: CalendarSessionGate,
}

#[pymethods]
impl PyCalendarSessionGate {
    #[new]
    #[pyo3(signature = (config = None))]
    fn py_new(config: Option<DataInfraConfig>) -> PyResult<Self> {
        let cfg = config.unwrap_or_default();
        let inner = CalendarSessionGate::from_config(&cfg).map_err(to_pyvalue_err)?;
        Ok(Self { inner })
    }

    #[pyo3(signature = (at_ts_utc = None))]
    fn check_session_status(&self, py: Python<'_>, at_ts_utc: Option<i64>) -> PyResult<Py<PyAny>> {
        let gate = self.inner.clone();
        let status = block_on_catalog_future(async move {
            gate.refresh_session_status(at_ts_utc).await
        })
        .map_err(to_pyruntime_err)?;

        let dict = PyDict::new(py);
        dict.set_item("market", status.market)?;
        dict.set_item("queried_at_utc", status.queried_at_utc)?;
        dict.set_item("trading_date", status.trading_date)?;
        dict.set_item("is_trading_day", status.is_trading_day)?;
        dict.set_item("is_rth_open", status.is_rth_open)?;
        dict.set_item("rth_open_utc", status.rth_open_utc)?;
        dict.set_item("rth_close_utc", status.rth_close_utc)?;
        dict.set_item("is_early_close", status.is_early_close)?;
        dict.set_item("next_rth_open_utc", status.next_rth_open_utc)?;
        dict.set_item("source", status.source)?;
        Ok(dict.into_any().unbind())
    }
}

/// Decodes raw Arrow IPC stream bytes from ClickHouse `v_quant_bars` into a Python `list[Bar]`.
#[pyfunction]
#[pyo3(signature = (payload, venue = "XNAS", timeframe = "15m", adjustment = "adjusted", price_precision = 2, size_precision = 0))]
pub fn decode_bars_arrow_ipc_bytes(
    payload: &[u8],
    venue: &str,
    timeframe: &str,
    adjustment: &str,
    price_precision: u8,
    size_precision: u8,
) -> PyResult<Vec<Bar>> {
    let tf = DataInfraTimeframe::from_str(timeframe).map_err(to_pyvalue_err)?;
    let adj = match adjustment.trim().to_ascii_lowercase().as_str() {
        "adjusted" | "adj" => PriceAdjustmentMode::Adjusted,
        "raw" | "unadjusted" => PriceAdjustmentMode::Raw,
        other => {
            return Err(to_pyvalue_err(format!(
                "Unsupported adjustment mode '{other}', expected 'adjusted' or 'raw'"
            )));
        }
    };
    let opts = ArrowDecodeOptions::new(venue, tf, price_precision, size_precision, adj);
    decode_bars_arrow_stream(payload, &opts).map_err(to_pyvalue_err)
}

/// Decodes raw Parquet bytes from `DataInfra` into a Python `list[Bar]`.
#[pyfunction]
#[pyo3(signature = (payload, venue = "XNAS", timeframe = "15m", adjustment = "adjusted", price_precision = 2, size_precision = 0))]
pub fn decode_bars_parquet_bytes(
    payload: &[u8],
    venue: &str,
    timeframe: &str,
    adjustment: &str,
    price_precision: u8,
    size_precision: u8,
) -> PyResult<Vec<Bar>> {
    let tf = DataInfraTimeframe::from_str(timeframe).map_err(to_pyvalue_err)?;
    let adj = match adjustment.trim().to_ascii_lowercase().as_str() {
        "adjusted" | "adj" => PriceAdjustmentMode::Adjusted,
        "raw" | "unadjusted" => PriceAdjustmentMode::Raw,
        other => {
            return Err(to_pyvalue_err(format!(
                "Unsupported adjustment mode '{other}', expected 'adjusted' or 'raw'"
            )));
        }
    };
    let opts = ArrowDecodeOptions::new(venue, tf, price_precision, size_precision, adj);
    decode_bars_parquet(bytes::Bytes::copy_from_slice(payload), &opts).map_err(to_pyvalue_err)
}

/// Maps a `(symbol, venue)` pair into a Nautilus [`InstrumentId`].
#[pyfunction]
#[pyo3(signature = (symbol, venue = "XNAS"))]
pub fn to_nautilus_instrument_id(symbol: &str, venue: &str) -> PyResult<InstrumentId> {
    to_instrument_id(symbol, venue).map_err(to_pyvalue_err)
}

/// Maps a `(symbol, venue, timeframe)` tuple into a canonical `EXTERNAL` Nautilus [`BarType`].
#[pyfunction]
#[pyo3(signature = (symbol, timeframe = "15m", venue = "XNAS"))]
pub fn to_nautilus_bar_type(symbol: &str, timeframe: &str, venue: &str) -> PyResult<BarType> {
    let tf = DataInfraTimeframe::from_str(timeframe).map_err(to_pyvalue_err)?;
    to_bar_type(symbol, venue, tf).map_err(to_pyvalue_err)
}

/// Exposed through `nautilus_trader._libnautilus.datainfra` and `nautilus_trader.adapters.datainfra`.
#[pymodule]
pub fn datainfra(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("DATAINFRA", DATAINFRA)?;
    m.add_class::<DataInfraConfig>()?;
    m.add_class::<DataInfraDataClientFactory>()?;
    m.add_class::<PyDataInfraCatalog>()?;
    m.add_class::<PyCalendarSessionGate>()?;
    m.add_function(wrap_pyfunction!(decode_bars_arrow_ipc_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(decode_bars_parquet_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(to_nautilus_instrument_id, m)?)?;
    m.add_function(wrap_pyfunction!(to_nautilus_bar_type, m)?)?;

    let registry = get_global_pyo3_registry();

    if let Err(e) =
        registry.register_factory_extractor(DATAINFRA.to_string(), extract_datainfra_data_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register DataInfra data factory extractor: {e}"
        )));
    }

    if let Err(e) = registry
        .register_config_extractor("DataInfraConfig".to_string(), extract_datainfra_data_config)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register DataInfra config extractor: {e}"
        )));
    }

    Ok(())
}
