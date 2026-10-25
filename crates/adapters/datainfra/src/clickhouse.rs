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

//! North-bound ClickHouse HTTP `ArrowStream` transport (`ClickHouseArrowTransport` &
//! `HttpClickHouseArrowTransport`) for querying `v_quant_bars`, `sip_raw_quotes`, and
//! `sip_trading_statuses` with zero intermediate disk files and strict fail-closed error handling.

use arrow::array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use http::Method;
use nautilus_core::UnixNanos;
use nautilus_model::data::{Bar, InstrumentStatus, QuoteTick};
use nautilus_network::{
    http::{HttpClient, HttpRedirectPolicy, create_standard_nautilus_headers},
    retry::{RetryConfig, RetryError, RetryManager},
};
use url::Url;

use crate::{
    arrow_decoder::{
        ArrowDecodeOptions, PriceAdjustmentMode, decode_arrow_ipc_stream, decode_bars_batches,
        decode_quotes_batches, decode_statuses_batches,
    },
    config::DataInfraConfig,
    error::{DataInfraError, DataInfraResult},
    mapping::DataInfraTimeframe,
};

/// Parameterized historical bar query request against ClickHouse `v_quant_bars`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarsQueryRequest {
    /// Target ticker symbols (e.g. `["AAPL", "NVDA"]`).
    pub symbols: Vec<String>,
    /// Canonical timeframe (`1m`, `15m`, `1h`, `4h`, `1d`, `1w`).
    pub timeframe: DataInfraTimeframe,
    /// Optional inclusive start timestamp (`UnixNanos`).
    pub start_ts_ns: Option<UnixNanos>,
    /// Optional inclusive end timestamp (`UnixNanos`).
    pub end_ts_ns: Option<UnixNanos>,
    /// Price adjustment mode (`Adjusted` vs `Raw`).
    pub adjustment: PriceAdjustmentMode,
    /// Market session filter (`"REG"` by default).
    pub market_session: Option<String>,
    /// Optional maximum row limit per symbol.
    pub limit_per_symbol: Option<u32>,
    /// Default venue code for decoded `InstrumentId`s.
    pub venue: String,
    /// Fixed-point decimal precision for decoded [`Price`](nautilus_model::types::Price) values.
    pub price_precision: u8,
    /// Fixed-point decimal precision for decoded [`Quantity`](nautilus_model::types::Quantity) values.
    pub size_precision: u8,
}

impl BarsQueryRequest {
    /// Creates a new [`BarsQueryRequest`] with default precision (`price_precision=2`, `size_precision=0`)
    /// and regular market session (`"REG"`).
    #[must_use]
    pub fn new(
        symbols: Vec<String>,
        timeframe: DataInfraTimeframe,
        venue: impl Into<String>,
        adjustment: PriceAdjustmentMode,
    ) -> Self {
        Self {
            symbols,
            timeframe,
            start_ts_ns: None,
            end_ts_ns: None,
            adjustment,
            market_session: Some("REG".to_string()),
            limit_per_symbol: None,
            venue: venue.into(),
            price_precision: 2,
            size_precision: 0,
        }
    }

    /// Sets the inclusive `[start_ts_ns, end_ts_ns]` query window.
    #[must_use]
    pub const fn with_time_range(
        mut self,
        start_ts_ns: Option<UnixNanos>,
        end_ts_ns: Option<UnixNanos>,
    ) -> Self {
        self.start_ts_ns = start_ts_ns;
        self.end_ts_ns = end_ts_ns;
        self
    }

    /// Sets the per-symbol row limit.
    #[must_use]
    pub const fn with_limit_per_symbol(mut self, limit: u32) -> Self {
        self.limit_per_symbol = Some(limit);
        self
    }

    /// Sets custom price and size decimal precisions.
    #[must_use]
    pub const fn with_precisions(mut self, price_precision: u8, size_precision: u8) -> Self {
        self.price_precision = price_precision;
        self.size_precision = size_precision;
        self
    }

    /// Converts this request's decoding settings into [`ArrowDecodeOptions`].
    #[must_use]
    pub fn decode_options(&self) -> ArrowDecodeOptions {
        ArrowDecodeOptions::new(
            self.venue.clone(),
            self.timeframe,
            self.price_precision,
            self.size_precision,
            self.adjustment,
        )
    }

    /// Validates the query request parameters (fail-closed).
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::InvalidConfig`] if `symbols` is empty or `start_ts_ns > end_ts_ns`.
    pub fn validate(&self) -> DataInfraResult<()> {
        if self.symbols.is_empty() || self.symbols.iter().all(|s| s.trim().is_empty()) {
            return Err(DataInfraError::InvalidConfig(
                "BarsQueryRequest requires at least one non-empty symbol".to_string(),
            ));
        }
        if let (Some(start), Some(end)) = (self.start_ts_ns, self.end_ts_ns)
            && start > end
        {
            return Err(DataInfraError::InvalidConfig(format!(
                "BarsQueryRequest start_ts_ns ({start}) must be <= end_ts_ns ({end})"
            )));
        }
        Ok(())
    }
}

/// Parameterized historical quote query request against ClickHouse `sip_raw_quotes`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotesQueryRequest {
    /// Target ticker symbols.
    pub symbols: Vec<String>,
    /// Optional inclusive start timestamp (`UnixNanos`).
    pub start_ts_ns: Option<UnixNanos>,
    /// Optional inclusive end timestamp (`UnixNanos`).
    pub end_ts_ns: Option<UnixNanos>,
    /// Optional maximum row limit.
    pub limit: Option<u32>,
    /// Default venue code for decoded `InstrumentId`s.
    pub venue: String,
    /// Fixed-point decimal precision for decoded [`Price`](nautilus_model::types::Price) values.
    pub price_precision: u8,
    /// Fixed-point decimal precision for decoded [`Quantity`](nautilus_model::types::Quantity) values.
    pub size_precision: u8,
}

impl QuotesQueryRequest {
    /// Creates a new [`QuotesQueryRequest`].
    #[must_use]
    pub fn new(symbols: Vec<String>, venue: impl Into<String>) -> Self {
        Self {
            symbols,
            start_ts_ns: None,
            end_ts_ns: None,
            limit: None,
            venue: venue.into(),
            price_precision: 2,
            size_precision: 0,
        }
    }

    /// Validates the quote query parameters (fail-closed).
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::InvalidConfig`] if `symbols` is empty or `start_ts_ns > end_ts_ns`.
    pub fn validate(&self) -> DataInfraResult<()> {
        if self.symbols.is_empty() || self.symbols.iter().all(|s| s.trim().is_empty()) {
            return Err(DataInfraError::InvalidConfig(
                "QuotesQueryRequest requires at least one non-empty symbol".to_string(),
            ));
        }
        if let (Some(start), Some(end)) = (self.start_ts_ns, self.end_ts_ns)
            && start > end
        {
            return Err(DataInfraError::InvalidConfig(format!(
                "QuotesQueryRequest start_ts_ns ({start}) must be <= end_ts_ns ({end})"
            )));
        }
        Ok(())
    }

    /// Converts this request's decoding settings into [`ArrowDecodeOptions`].
    #[must_use]
    pub fn decode_options(&self) -> ArrowDecodeOptions {
        ArrowDecodeOptions::new(
            self.venue.clone(),
            DataInfraTimeframe::M1,
            self.price_precision,
            self.size_precision,
            PriceAdjustmentMode::Raw,
        )
    }
}

/// Parameterized historical trading status query request against ClickHouse `sip_trading_statuses`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusesQueryRequest {
    /// Target ticker symbols.
    pub symbols: Vec<String>,
    /// Optional inclusive start timestamp (`UnixNanos`).
    pub start_ts_ns: Option<UnixNanos>,
    /// Optional inclusive end timestamp (`UnixNanos`).
    pub end_ts_ns: Option<UnixNanos>,
    /// Optional maximum row limit.
    pub limit: Option<u32>,
    /// Default venue code for decoded `InstrumentId`s.
    pub venue: String,
}

impl StatusesQueryRequest {
    /// Creates a new [`StatusesQueryRequest`].
    #[must_use]
    pub fn new(symbols: Vec<String>, venue: impl Into<String>) -> Self {
        Self {
            symbols,
            start_ts_ns: None,
            end_ts_ns: None,
            limit: None,
            venue: venue.into(),
        }
    }

    /// Validates the status query parameters (fail-closed).
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::InvalidConfig`] if `symbols` is empty or `start_ts_ns > end_ts_ns`.
    pub fn validate(&self) -> DataInfraResult<()> {
        if self.symbols.is_empty() || self.symbols.iter().all(|s| s.trim().is_empty()) {
            return Err(DataInfraError::InvalidConfig(
                "StatusesQueryRequest requires at least one non-empty symbol".to_string(),
            ));
        }
        if let (Some(start), Some(end)) = (self.start_ts_ns, self.end_ts_ns)
            && start > end
        {
            return Err(DataInfraError::InvalidConfig(format!(
                "StatusesQueryRequest start_ts_ns ({start}) must be <= end_ts_ns ({end})"
            )));
        }
        Ok(())
    }

    /// Converts this request's decoding settings into [`ArrowDecodeOptions`].
    #[must_use]
    pub fn decode_options(&self) -> ArrowDecodeOptions {
        ArrowDecodeOptions::new(
            self.venue.clone(),
            DataInfraTimeframe::M1,
            2,
            0,
            PriceAdjustmentMode::Raw,
        )
    }
}

/// North-bound transport trait for streaming `ArrowStream` batches from ClickHouse.
#[async_trait]
pub trait ClickHouseArrowTransport: Send + Sync + std::fmt::Debug {
    /// Executes a parameterized SQL query against ClickHouse with `default_format=ArrowStream`
    /// and returns the raw Arrow IPC stream bytes.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] on invalid SQL, network timeout, non-200 HTTP status, or empty response.
    async fn query_arrow_stream(
        &self,
        sql: &str,
        params: &[(String, String)],
    ) -> DataInfraResult<Bytes>;

    /// Queries `v_quant_bars` and decodes the response into Arrow `RecordBatch`es.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if validation, HTTP transport, or Arrow IPC decoding fails.
    async fn fetch_bars_batches(&self, req: &BarsQueryRequest) -> DataInfraResult<Vec<RecordBatch>> {
        req.validate()?;
        let (sql, params) = build_v_quant_bars_query(self.database(), req)?;
        let raw_bytes = self.query_arrow_stream(&sql, &params).await?;
        decode_arrow_ipc_stream(&raw_bytes)
    }

    /// Queries `v_quant_bars` and decodes the response directly into Nautilus [`Bar`]s.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if validation, HTTP transport, Arrow decoding, or bar sanity fails.
    async fn fetch_bars(&self, req: &BarsQueryRequest) -> DataInfraResult<Vec<Bar>> {
        let batches = self.fetch_bars_batches(req).await?;
        decode_bars_batches(&batches, &req.decode_options())
    }

    /// Queries `sip_raw_quotes` and decodes the response directly into Nautilus [`QuoteTick`]s.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if validation, HTTP transport, or quote decoding fails.
    async fn fetch_quotes(&self, req: &QuotesQueryRequest) -> DataInfraResult<Vec<QuoteTick>> {
        req.validate()?;
        let (sql, params) = build_sip_quotes_query(self.database(), req)?;
        let raw_bytes = self.query_arrow_stream(&sql, &params).await?;
        let batches = decode_arrow_ipc_stream(&raw_bytes)?;
        decode_quotes_batches(&batches, &req.decode_options())
    }

    /// Queries `sip_trading_statuses` and decodes the response directly into [`InstrumentStatus`] events.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if validation, HTTP transport, or status decoding fails.
    async fn fetch_statuses(
        &self,
        req: &StatusesQueryRequest,
    ) -> DataInfraResult<Vec<InstrumentStatus>> {
        req.validate()?;
        let (sql, params) = build_sip_statuses_query(self.database(), req)?;
        let raw_bytes = self.query_arrow_stream(&sql, &params).await?;
        let batches = decode_arrow_ipc_stream(&raw_bytes)?;
        decode_statuses_batches(&batches, &req.decode_options())
    }

    /// Returns the target ClickHouse database name.
    fn database(&self) -> &str;
}

/// Production HTTP `ArrowStream` ClickHouse transport built on `nautilus_network::http::HttpClient`.
#[derive(Debug, Clone)]
pub struct HttpClickHouseArrowTransport {
    config: DataInfraConfig,
    http_client: HttpClient,
    retry_manager: RetryManager<DataInfraError>,
}

impl HttpClickHouseArrowTransport {
    /// Constructs a validated [`HttpClickHouseArrowTransport`] from [`DataInfraConfig`].
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if configuration validation or `HttpClient` initialization fails.
    pub fn new(config: DataInfraConfig) -> DataInfraResult<Self> {
        config.validate()?;
        let timeout_secs = (config.request_timeout_ms / 1_000).max(1);
        let headers: std::collections::HashMap<String, String> =
            create_standard_nautilus_headers().into_iter().collect();
        let http_client = HttpClient::builder()
            .headers(headers)
            .timeout_secs(timeout_secs)
            .redirect_policy(HttpRedirectPolicy::Reject)
            .build()
            .map_err(|e| DataInfraError::Transport(format!("failed to create HttpClient: {e}")))?;

        let retry_config = RetryConfig {
            max_retries: config.max_retries,
            initial_delay_ms: 50,
            max_delay_ms: 1_000,
            backoff_factor: 2.0,
            jitter_ms: 10,
            operation_timeout_ms: Some(config.request_timeout_ms.saturating_mul(2)),
            immediate_first: false,
            max_elapsed_ms: None,
        };
        let retry_manager = RetryManager::new(retry_config);

        Ok(Self {
            config,
            http_client,
            retry_manager,
        })
    }

    /// Returns a reference to the underlying [`DataInfraConfig`].
    #[must_use]
    pub const fn config(&self) -> &DataInfraConfig {
        &self.config
    }

    fn build_query_url(&self, params: &[(String, String)]) -> DataInfraResult<String> {
        let mut parsed = Url::parse(self.config.clickhouse_url.trim()).map_err(|e| {
            DataInfraError::InvalidConfig(format!("invalid clickhouse_url: {e}"))
        })?;
        {
            let mut qp = parsed.query_pairs_mut();
            qp.append_pair("database", &self.config.clickhouse_database);
            qp.append_pair("default_format", "ArrowStream");
            for (k, v) in params {
                qp.append_pair(&format!("param_{k}"), v);
            }
        }
        Ok(parsed.to_string())
    }
}

#[async_trait]
impl ClickHouseArrowTransport for HttpClickHouseArrowTransport {
    fn database(&self) -> &str {
        &self.config.clickhouse_database
    }

    async fn query_arrow_stream(
        &self,
        sql: &str,
        params: &[(String, String)],
    ) -> DataInfraResult<Bytes> {
        let sql_trimmed = sql.trim();
        if sql_trimmed.is_empty() {
            return Err(DataInfraError::InvalidConfig(
                "ClickHouse SQL query must not be empty".to_string(),
            ));
        }

        let url = self.build_query_url(params)?;
        let body_bytes = sql_trimmed.as_bytes().to_vec();
        let client = self.http_client.clone();

        let operation = move || {
            let client = client.clone();
            let url = url.clone();
            let body = body_bytes.clone();
            async move {
                let response = client
                    .request(Method::POST, url, None, None, Some(body), None, None)
                    .await
                    .map_err(|e| {
                        DataInfraError::Transport(format!("ClickHouse HTTP request error: {e}"))
                    })?;

                let status_code = response.status.as_u16();
                if status_code == 200 {
                    if response.body.is_empty() {
                        return Err(DataInfraError::Transport(
                            "ClickHouse HTTP 200 response returned empty ArrowStream body (fail-closed)"
                                .to_string(),
                        ));
                    }
                    return Ok(response.body);
                }

                let preview = String::from_utf8_lossy(&response.body);
                let preview_short: String = preview.chars().take(300).collect();
                Err(DataInfraError::Transport(format!(
                    "ClickHouse HTTP {status_code}: {preview_short}"
                )))
            }
        };

        let should_retry = |err: &DataInfraError| -> bool {
            match err {
                DataInfraError::Transport(msg) => {
                    msg.contains("HTTP 502")
                        || msg.contains("HTTP 503")
                        || msg.contains("HTTP 504")
                        || msg.contains("HTTP 429")
                        || msg.contains("ClickHouse HTTP request error")
                }
                _ => false,
            }
        };

        let create_error = |err: RetryError| DataInfraError::Transport(err.to_string());

        self.retry_manager
            .invocation(
                "clickhouse_query_arrow_stream",
                operation,
                should_retry,
                create_error,
            )
            .execute()
            .await
    }
}

/// Builds a parameterized ClickHouse SQL query and HTTP `param_*` bindings for `v_quant_bars`.
///
/// # Errors
///
/// Returns [`DataInfraError`] if request validation fails.
pub fn build_v_quant_bars_query(
    database: &str,
    req: &BarsQueryRequest,
) -> DataInfraResult<(String, Vec<(String, String)>)> {
    req.validate()?;
    let (open_col, high_col, low_col, close_col, vol_col) = req.adjustment.column_names();

    let mut params = vec![
        ("tf".to_string(), req.timeframe.as_str().to_string()),
        (
            "symbols".to_string(),
            format_clickhouse_string_array(&req.symbols),
        ),
    ];

    let mut where_clauses = vec![
        "timeframe = {tf:String}".to_string(),
        "symbol IN {symbols:Array(String)}".to_string(),
    ];

    if let Some(ref session) = req.market_session {
        params.push(("session".to_string(), session.trim().to_string()));
        where_clauses.push("market_session = {session:String}".to_string());
    }

    if let Some(start_ns) = req.start_ts_ns {
        let start_sec = start_ns.as_u64() / 1_000_000_000;
        params.push(("start_ts".to_string(), start_sec.to_string()));
        where_clauses.push("toUnixTimestamp(bar_start_time) >= {start_ts:UInt64}".to_string());
    }

    if let Some(end_ns) = req.end_ts_ns {
        let end_sec = end_ns.as_u64() / 1_000_000_000;
        params.push(("end_ts".to_string(), end_sec.to_string()));
        where_clauses.push("toUnixTimestamp(bar_start_time) <= {end_ts:UInt64}".to_string());
    }

    let limit_clause = if let Some(limit) = req.limit_per_symbol {
        params.push(("per_symbol_limit".to_string(), limit.to_string()));
        "\nLIMIT {per_symbol_limit:UInt32} BY symbol"
    } else {
        ""
    };

    let sql = format!(
        "SELECT \
            symbol, \
            timeframe, \
            toUnixTimestamp(bar_start_time) AS ts, \
            {open_col}, \
            {high_col}, \
            {low_col}, \
            {close_col}, \
            {vol_col} \
        FROM {database}.v_quant_bars \
        WHERE {where_sql} \
        ORDER BY bar_start_time ASC, symbol ASC{limit_clause}",
        where_sql = where_clauses.join(" AND "),
    );

    Ok((sql, params))
}

/// Builds a parameterized ClickHouse SQL query and HTTP `param_*` bindings for `sip_raw_quotes`.
///
/// # Errors
///
/// Returns [`DataInfraError`] if request validation fails.
pub fn build_sip_quotes_query(
    database: &str,
    req: &QuotesQueryRequest,
) -> DataInfraResult<(String, Vec<(String, String)>)> {
    req.validate()?;
    let mut params = vec![(
        "symbols".to_string(),
        format_clickhouse_string_array(&req.symbols),
    )];
    let mut where_clauses = vec!["symbol IN {symbols:Array(String)}".to_string()];

    if let Some(start_ns) = req.start_ts_ns {
        let start_sec = start_ns.as_u64() / 1_000_000_000;
        params.push(("start_ts".to_string(), start_sec.to_string()));
        where_clauses.push("toUnixTimestamp(event_time) >= {start_ts:UInt64}".to_string());
    }
    if let Some(end_ns) = req.end_ts_ns {
        let end_sec = end_ns.as_u64() / 1_000_000_000;
        params.push(("end_ts".to_string(), end_sec.to_string()));
        where_clauses.push("toUnixTimestamp(event_time) <= {end_ts:UInt64}".to_string());
    }

    let limit_clause = if let Some(limit) = req.limit {
        params.push(("limit_rows".to_string(), limit.to_string()));
        " LIMIT {limit_rows:UInt32}"
    } else {
        ""
    };

    let sql = format!(
        "SELECT \
            symbol, \
            toUnixTimestamp64Nano(event_time) AS ts_event, \
            bid_price, \
            ask_price, \
            bid_size, \
            ask_size \
        FROM {database}.sip_raw_quotes \
        WHERE {where_sql} \
        ORDER BY event_time ASC, symbol ASC{limit_clause}",
        where_sql = where_clauses.join(" AND "),
    );

    Ok((sql, params))
}

/// Builds a parameterized ClickHouse SQL query and HTTP `param_*` bindings for `sip_trading_statuses`.
///
/// # Errors
///
/// Returns [`DataInfraError`] if request validation fails.
pub fn build_sip_statuses_query(
    database: &str,
    req: &StatusesQueryRequest,
) -> DataInfraResult<(String, Vec<(String, String)>)> {
    req.validate()?;
    let mut params = vec![(
        "symbols".to_string(),
        format_clickhouse_string_array(&req.symbols),
    )];
    let mut where_clauses = vec!["symbol IN {symbols:Array(String)}".to_string()];

    if let Some(start_ns) = req.start_ts_ns {
        let start_sec = start_ns.as_u64() / 1_000_000_000;
        params.push(("start_ts".to_string(), start_sec.to_string()));
        where_clauses.push("toUnixTimestamp(event_time) >= {start_ts:UInt64}".to_string());
    }
    if let Some(end_ns) = req.end_ts_ns {
        let end_sec = end_ns.as_u64() / 1_000_000_000;
        params.push(("end_ts".to_string(), end_sec.to_string()));
        where_clauses.push("toUnixTimestamp(event_time) <= {end_ts:UInt64}".to_string());
    }

    let limit_clause = if let Some(limit) = req.limit {
        params.push(("limit_rows".to_string(), limit.to_string()));
        " LIMIT {limit_rows:UInt32}"
    } else {
        ""
    };

    let sql = format!(
        "SELECT \
            symbol, \
            toUnixTimestamp64Nano(event_time) AS ts_event, \
            state, \
            reason_code \
        FROM {database}.sip_trading_statuses \
        WHERE {where_sql} \
        ORDER BY event_time ASC, symbol ASC{limit_clause}",
        where_sql = where_clauses.join(" AND "),
    );

    Ok((sql, params))
}

fn format_clickhouse_string_array(items: &[String]) -> String {
    let quoted: Vec<String> = items
        .iter()
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .map(|s| {
            let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
            format!("'{escaped}'")
        })
        .collect();
    format!("[{}]", quoted.join(","))
}
