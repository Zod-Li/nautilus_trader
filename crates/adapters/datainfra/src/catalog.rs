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

//! `DataInfraCatalog` implementation of [`CatalogReader`] and [`CatalogWriter`]
//! for streaming ClickHouse historical `Bar`, `QuoteTick`, and `InstrumentStatus`
//! batches into Nautilus `BacktestEngine` and `BacktestNode`.

use std::{collections::BTreeMap, future::Future, str::FromStr, sync::Arc};

use arrow::record_batch::RecordBatch;
use indexmap::IndexMap;
use nautilus_common::live::runtime::get_runtime;
use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    data::{
        Bar, BarType, Data, DataBatch, HasTsInit, InstrumentStatus, NautilusDataType,
        NautilusRecordType, QuoteTick,
    },
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_persistence::{
    catalog::{
        factory::{
            CatalogConnectConfig, CatalogFactory, CatalogParamKind, validate_catalog_params,
        },
        session::{DataBatchQueryResult, TypedDataBatchSession},
        traits::{CatalogReader, CatalogWriter, DataCatalog},
        types::{CatalogInstrumentQuery, CatalogQuery},
    },
    errors::PersistenceError,
};

use crate::{
    arrow_decoder::PriceAdjustmentMode,
    clickhouse::{
        BarsQueryRequest, ClickHouseArrowTransport, HttpClickHouseArrowTransport,
        QuotesQueryRequest, StatusesQueryRequest,
    },
    config::DataInfraConfig,
    error::DataInfraResult,
    instrument::DataInfraEquityMetadata,
    mapping::{DataInfraTimeframe, from_bar_type, from_instrument_id},
};

/// Canonical factory name for registering [`DataInfraCatalog`] in [`nautilus_persistence`].
pub const DATAINFRA_CATALOG_FACTORY_NAME: &str = "DataInfra";

const ACCEPTED_CATALOG_PARAMS: &[(&str, CatalogParamKind)] = &[
    ("database", CatalogParamKind::Text),
    ("default_venue", CatalogParamKind::Text),
    ("timeframe", CatalogParamKind::Text),
    ("adjustment", CatalogParamKind::Text),
    ("market_session", CatalogParamKind::Text),
];

/// Runs a `Future` to completion from synchronous code without panicking whether or not
/// the current thread is already inside a Tokio runtime.
pub(crate) fn block_on_catalog_future<F, T>(fut: F) -> T
where
    F: Future<Output = T> + Send,
    T: Send,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::scope(|scope| {
            scope
                .spawn(move || get_runtime().block_on(fut))
                .join()
                .expect("DataInfraCatalog async worker thread panicked")
        })
    } else {
        get_runtime().block_on(fut)
    }
}

/// ClickHouse-backed [`CatalogReader`] for Nautilus `BacktestEngine` / `BacktestNode`.
#[derive(Debug, Clone)]
pub struct DataInfraCatalog {
    config: DataInfraConfig,
    transport: Arc<dyn ClickHouseArrowTransport>,
    instruments: IndexMap<InstrumentId, InstrumentAny>,
    default_timeframe: DataInfraTimeframe,
    default_adjustment: PriceAdjustmentMode,
    default_market_session: Option<String>,
}

impl DataInfraCatalog {
    /// Creates a new [`DataInfraCatalog`] with an explicit [`ClickHouseArrowTransport`].
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if `config.validate()` fails.
    pub fn new(
        config: DataInfraConfig,
        transport: Arc<dyn ClickHouseArrowTransport>,
    ) -> DataInfraResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            transport,
            instruments: IndexMap::new(),
            default_timeframe: DataInfraTimeframe::M15,
            default_adjustment: PriceAdjustmentMode::Adjusted,
            default_market_session: Some("REG".to_string()),
        })
    }

    /// Creates a production [`DataInfraCatalog`] backed by [`HttpClickHouseArrowTransport`].
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if configuration validation or HTTP client creation fails.
    pub fn from_config(config: DataInfraConfig) -> DataInfraResult<Self> {
        let transport = Arc::new(HttpClickHouseArrowTransport::new(config.clone())?);
        Self::new(config, transport)
    }

    /// Returns a reference to the underlying [`DataInfraConfig`].
    #[must_use]
    pub const fn config(&self) -> &DataInfraConfig {
        &self.config
    }

    /// Sets the default timeframe used when a bar query specifies symbols/instruments without a full [`BarType`].
    #[must_use]
    pub const fn with_default_timeframe(mut self, timeframe: DataInfraTimeframe) -> Self {
        self.default_timeframe = timeframe;
        self
    }

    /// Sets the default price adjustment mode (`Adjusted` vs `Raw`).
    #[must_use]
    pub const fn with_default_adjustment(mut self, adjustment: PriceAdjustmentMode) -> Self {
        self.default_adjustment = adjustment;
        self
    }

    /// Sets the default market session filter (`Some("REG")` or `None` for all sessions).
    #[must_use]
    pub fn with_default_market_session(mut self, market_session: Option<String>) -> Self {
        self.default_market_session = market_session;
        self
    }

    /// Registers an equity instrument from [`DataInfraEquityMetadata`] into the catalog.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if the metadata fails validation.
    pub fn register_equity(
        &mut self,
        metadata: &DataInfraEquityMetadata,
        ts_init: UnixNanos,
    ) -> DataInfraResult<InstrumentAny> {
        let equity = metadata.to_equity_any(ts_init)?;
        let inst = InstrumentAny::Equity(equity);
        self.instruments.insert(inst.id(), inst.clone());
        Ok(inst)
    }

    /// Registers a pre-built [`InstrumentAny`] into the catalog.
    pub fn register_instrument(&mut self, instrument: InstrumentAny) {
        self.instruments.insert(instrument.id(), instrument);
    }

    /// Async helper to query historical [`Bar`]s directly from ClickHouse.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if validation, HTTP transport, or Arrow decoding fails.
    pub async fn load_bars_async(&self, req: &BarsQueryRequest) -> DataInfraResult<Vec<Bar>> {
        let mut bars = self.transport.fetch_bars(req).await?;
        bars.sort_by_key(|b| b.ts_init);
        Ok(bars)
    }

    /// Blocking helper to query historical [`Bar`]s directly from ClickHouse.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if validation, HTTP transport, or Arrow decoding fails.
    pub fn load_bars(&self, req: &BarsQueryRequest) -> DataInfraResult<Vec<Bar>> {
        let transport = Arc::clone(&self.transport);
        let req_owned = req.clone();
        let mut bars = block_on_catalog_future(async move { transport.fetch_bars(&req_owned).await })?;
        bars.sort_by_key(|b| b.ts_init);
        Ok(bars)
    }

    /// Async helper to query historical [`QuoteTick`]s directly from ClickHouse.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if validation, HTTP transport, or Arrow decoding fails.
    pub async fn load_quotes_async(
        &self,
        req: &QuotesQueryRequest,
    ) -> DataInfraResult<Vec<QuoteTick>> {
        let mut quotes = self.transport.fetch_quotes(req).await?;
        quotes.sort_by_key(|q| q.ts_init);
        Ok(quotes)
    }

    /// Blocking helper to query historical [`QuoteTick`]s directly from ClickHouse.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if validation, HTTP transport, or Arrow decoding fails.
    pub fn load_quotes(&self, req: &QuotesQueryRequest) -> DataInfraResult<Vec<QuoteTick>> {
        let transport = Arc::clone(&self.transport);
        let req_owned = req.clone();
        let mut quotes =
            block_on_catalog_future(async move { transport.fetch_quotes(&req_owned).await })?;
        quotes.sort_by_key(|q| q.ts_init);
        Ok(quotes)
    }

    /// Async helper to query historical [`InstrumentStatus`] transitions directly from ClickHouse.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if validation, HTTP transport, or Arrow decoding fails.
    pub async fn load_statuses_async(
        &self,
        req: &StatusesQueryRequest,
    ) -> DataInfraResult<Vec<InstrumentStatus>> {
        let mut statuses = self.transport.fetch_statuses(req).await?;
        statuses.sort_by_key(|s| s.ts_init);
        Ok(statuses)
    }

    /// Blocking helper to query historical [`InstrumentStatus`] transitions directly from ClickHouse.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if validation, HTTP transport, or Arrow decoding fails.
    pub fn load_statuses(
        &self,
        req: &StatusesQueryRequest,
    ) -> DataInfraResult<Vec<InstrumentStatus>> {
        let transport = Arc::clone(&self.transport);
        let req_owned = req.clone();
        let mut statuses =
            block_on_catalog_future(async move { transport.fetch_statuses(&req_owned).await })?;
        statuses.sort_by_key(|s| s.ts_init);
        Ok(statuses)
    }

    /// Returns a [`CatalogFactory`] closure suitable for registering in [`nautilus_persistence::catalog::factory::CatalogFactoryRegistry`].
    #[must_use]
    pub fn catalog_factory() -> CatalogFactory {
        Arc::new(|connect_cfg: &CatalogConnectConfig| -> anyhow::Result<DataCatalog> {
            validate_catalog_params(
                DATAINFRA_CATALOG_FACTORY_NAME,
                connect_cfg.params.as_ref(),
                ACCEPTED_CATALOG_PARAMS,
            )?;

            let mut cfg = DataInfraConfig::default();
            if !connect_cfg.uri.trim().is_empty() {
                cfg.clickhouse_url = connect_cfg.uri.trim().to_string();
            }

            let mut timeframe = DataInfraTimeframe::M15;
            let mut adjustment = PriceAdjustmentMode::Adjusted;
            let mut market_session = Some("REG".to_string());

            if let Some(ref params) = connect_cfg.params {
                if let Some(db) = params.get_str("database")
                    && !db.trim().is_empty()
                {
                    cfg.clickhouse_database = db.trim().to_string();
                }
                if let Some(venue) = params.get_str("default_venue")
                    && !venue.trim().is_empty()
                {
                    cfg.default_venue = venue.trim().to_uppercase();
                }
                if let Some(tf_str) = params.get_str("timeframe") {
                    timeframe = DataInfraTimeframe::from_str(tf_str)?;
                }
                if let Some(adj_str) = params.get_str("adjustment") {
                    adjustment = match adj_str.trim().to_ascii_lowercase().as_str() {
                        "adjusted" | "adj" => PriceAdjustmentMode::Adjusted,
                        "raw" | "unadjusted" => PriceAdjustmentMode::Raw,
                        other => {
                            anyhow::bail!("Unsupported DataInfra catalog adjustment mode '{other}'")
                        }
                    };
                }
                if let Some(sess_str) = params.get_str("market_session") {
                    let trimmed = sess_str.trim();
                    market_session = if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("ALL") {
                        None
                    } else {
                        Some(trimmed.to_string())
                    };
                }
            }

            let catalog = Self::from_config(cfg)?
                .with_default_timeframe(timeframe)
                .with_default_adjustment(adjustment)
                .with_default_market_session(market_session);
            Ok(Box::new(catalog))
        })
    }

    fn resolve_query_timeframe(&self, params: Option<&Params>) -> anyhow::Result<DataInfraTimeframe> {
        if let Some(tf_str) = params.and_then(|p| p.get_str("timeframe")) {
            return DataInfraTimeframe::from_str(tf_str).map_err(anyhow::Error::from);
        }
        Ok(self.default_timeframe)
    }

    fn resolve_query_adjustment(
        &self,
        params: Option<&Params>,
    ) -> anyhow::Result<PriceAdjustmentMode> {
        if let Some(adj_str) = params.and_then(|p| p.get_str("adjustment")) {
            return match adj_str.trim().to_ascii_lowercase().as_str() {
                "adjusted" | "adj" => Ok(PriceAdjustmentMode::Adjusted),
                "raw" | "unadjusted" => Ok(PriceAdjustmentMode::Raw),
                other => anyhow::bail!("Unsupported adjustment mode '{other}'"),
            };
        }
        Ok(self.default_adjustment)
    }

    fn resolve_query_market_session(&self, params: Option<&Params>) -> Option<String> {
        if let Some(sess) = params.and_then(|p| p.get_str("market_session")) {
            let trimmed = sess.trim();
            if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("ALL") {
                return None;
            }
            return Some(trimmed.to_string());
        }
        self.default_market_session.clone()
    }

    fn parse_bar_identifiers(
        &self,
        identifiers: Option<&[String]>,
        fallback_tf: DataInfraTimeframe,
    ) -> anyhow::Result<BTreeMap<(String, DataInfraTimeframe), Vec<String>>> {
        let mut grouped: BTreeMap<(String, DataInfraTimeframe), Vec<String>> = BTreeMap::new();

        if let Some(ids) = identifiers
            && !ids.is_empty()
        {
            for raw_id in ids {
                let trimmed = raw_id.trim();
                if trimmed.is_empty() {
                    anyhow::bail!("CatalogQuery bar identifier must not be empty");
                }
                if let Ok(bar_type) = BarType::from_str(trimmed) {
                    let (symbol, venue, tf) = from_bar_type(&bar_type)?;
                    let entry = grouped.entry((venue, tf)).or_default();
                    if !entry.contains(&symbol) {
                        entry.push(symbol);
                    }
                } else if trimmed.contains('.') {
                    let inst_id = InstrumentId::from_str(trimmed)
                        .map_err(|e| anyhow::anyhow!("Invalid InstrumentId '{trimmed}': {e}"))?;
                    let (symbol, venue) = from_instrument_id(inst_id);
                    let entry = grouped.entry((venue, fallback_tf)).or_default();
                    if !entry.contains(&symbol) {
                        entry.push(symbol);
                    }
                } else {
                    let inst_id =
                        crate::mapping::to_instrument_id(trimmed, &self.config.default_venue)?;
                    let (symbol, _) = from_instrument_id(inst_id);
                    let entry = grouped
                        .entry((self.config.default_venue.clone(), fallback_tf))
                        .or_default();
                    if !entry.contains(&symbol) {
                        entry.push(symbol);
                    }
                }
            }
        } else if !self.instruments.is_empty() {
            for inst_id in self.instruments.keys() {
                let (symbol, venue) = from_instrument_id(*inst_id);
                let entry = grouped.entry((venue, fallback_tf)).or_default();
                if !entry.contains(&symbol) {
                    entry.push(symbol);
                }
            }
        } else {
            anyhow::bail!(
                "DataInfraCatalog bar query requires non-empty identifiers or registered instruments"
            );
        }

        Ok(grouped)
    }

    fn parse_symbol_identifiers(
        &self,
        identifiers: Option<&[String]>,
    ) -> anyhow::Result<BTreeMap<String, Vec<String>>> {
        let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();

        if let Some(ids) = identifiers
            && !ids.is_empty()
        {
            for raw_id in ids {
                let trimmed = raw_id.trim();
                if trimmed.is_empty() {
                    anyhow::bail!("CatalogQuery identifier must not be empty");
                }
                if trimmed.contains('.') {
                    let inst_id = InstrumentId::from_str(trimmed)
                        .map_err(|e| anyhow::anyhow!("Invalid InstrumentId '{trimmed}': {e}"))?;
                    let (symbol, venue) = from_instrument_id(inst_id);
                    let entry = grouped.entry(venue).or_default();
                    if !entry.contains(&symbol) {
                        entry.push(symbol);
                    }
                } else {
                    let inst_id =
                        crate::mapping::to_instrument_id(trimmed, &self.config.default_venue)?;
                    let (symbol, _) = from_instrument_id(inst_id);
                    let entry = grouped.entry(self.config.default_venue.clone()).or_default();
                    if !entry.contains(&symbol) {
                        entry.push(symbol);
                    }
                }
            }
        } else if !self.instruments.is_empty() {
            for inst_id in self.instruments.keys() {
                let (symbol, venue) = from_instrument_id(*inst_id);
                let entry = grouped.entry(venue).or_default();
                if !entry.contains(&symbol) {
                    entry.push(symbol);
                }
            }
        } else {
            anyhow::bail!(
                "DataInfraCatalog query requires non-empty identifiers or registered instruments"
            );
        }

        Ok(grouped)
    }

    fn query_bars_for_catalog(&self, query: &CatalogQuery) -> anyhow::Result<Vec<Bar>> {
        if let (Some(start), Some(end)) = (query.start, query.end)
            && start > end
        {
            anyhow::bail!("CatalogQuery start ({start}) must be <= end ({end})");
        }

        let fallback_tf = self.resolve_query_timeframe(query.params.as_ref())?;
        let adjustment = self.resolve_query_adjustment(query.params.as_ref())?;
        let market_session = self.resolve_query_market_session(query.params.as_ref());
        let grouped = self.parse_bar_identifiers(query.identifiers.as_deref(), fallback_tf)?;

        let mut all_bars = Vec::new();
        for ((venue, tf), symbols) in grouped {
            let mut req = BarsQueryRequest::new(symbols, tf, venue, adjustment)
                .with_time_range(query.start, query.end);
            req.market_session = market_session.clone();
            let batch_bars = self.load_bars(&req)?;
            all_bars.extend(batch_bars);
        }

        all_bars.sort_by(|a, b| {
            a.ts_init
                .cmp(&b.ts_init)
                .then_with(|| a.bar_type.to_string().cmp(&b.bar_type.to_string()))
        });
        Ok(all_bars)
    }

    fn query_quotes_for_catalog(&self, query: &CatalogQuery) -> anyhow::Result<Vec<QuoteTick>> {
        if let (Some(start), Some(end)) = (query.start, query.end)
            && start > end
        {
            anyhow::bail!("CatalogQuery start ({start}) must be <= end ({end})");
        }

        let grouped = self.parse_symbol_identifiers(query.identifiers.as_deref())?;
        let mut all_quotes = Vec::new();
        for (venue, symbols) in grouped {
            let mut req = QuotesQueryRequest::new(symbols, venue);
            req.start_ts_ns = query.start;
            req.end_ts_ns = query.end;
            let batch_quotes = self.load_quotes(&req)?;
            all_quotes.extend(batch_quotes);
        }

        all_quotes.sort_by(|a, b| {
            a.ts_init
                .cmp(&b.ts_init)
                .then_with(|| a.instrument_id.to_string().cmp(&b.instrument_id.to_string()))
        });
        Ok(all_quotes)
    }

    fn query_statuses_for_catalog(
        &self,
        query: &CatalogQuery,
    ) -> anyhow::Result<Vec<InstrumentStatus>> {
        if let (Some(start), Some(end)) = (query.start, query.end)
            && start > end
        {
            anyhow::bail!("CatalogQuery start ({start}) must be <= end ({end})");
        }

        let grouped = self.parse_symbol_identifiers(query.identifiers.as_deref())?;
        let mut all_statuses = Vec::new();
        for (venue, symbols) in grouped {
            let mut req = StatusesQueryRequest::new(symbols, venue);
            req.start_ts_ns = query.start;
            req.end_ts_ns = query.end;
            let batch_statuses = self.load_statuses(&req)?;
            all_statuses.extend(batch_statuses);
        }

        all_statuses.sort_by(|a, b| {
            a.ts_init
                .cmp(&b.ts_init)
                .then_with(|| a.instrument_id.to_string().cmp(&b.instrument_id.to_string()))
        });
        Ok(all_statuses)
    }
}

impl CatalogReader for DataInfraCatalog {
    fn fork_query_catalog(&self) -> anyhow::Result<Option<DataCatalog>> {
        Ok(Some(Box::new(self.clone())))
    }

    fn reset_session(&mut self) {}

    fn instruments(
        &mut self,
        query: &CatalogInstrumentQuery,
    ) -> anyhow::Result<Vec<InstrumentAny>> {
        let ts_init = query.start.unwrap_or_default();
        if let Some(ref requested_ids) = query.instrument_ids {
            let mut results = Vec::with_capacity(requested_ids.len());
            for raw_id in requested_ids {
                let trimmed = raw_id.trim();
                let inst_id = if trimmed.contains('.') {
                    InstrumentId::from_str(trimmed)
                        .map_err(|e| anyhow::anyhow!("Invalid InstrumentId '{trimmed}': {e}"))?
                } else {
                    crate::mapping::to_instrument_id(trimmed, &self.config.default_venue)?
                };

                if let Some(existing) = self.instruments.get(&inst_id) {
                    results.push(existing.clone());
                } else {
                    let (symbol, venue) = from_instrument_id(inst_id);
                    let metadata = DataInfraEquityMetadata::new(symbol, venue);
                    let equity = metadata.to_equity_any(ts_init)?;
                    let inst = InstrumentAny::Equity(equity);
                    self.instruments.insert(inst_id, inst.clone());
                    results.push(inst);
                }
            }
            return Ok(results);
        }

        Ok(self.instruments.values().cloned().collect())
    }

    fn query_batch(&mut self, query: &CatalogQuery) -> anyhow::Result<DataBatch> {
        match query.data_type {
            NautilusDataType::Bar => {
                let bars = self.query_bars_for_catalog(query)?;
                Ok(DataBatch::Bar(bars.into()))
            }
            NautilusDataType::QuoteTick => {
                let quotes = self.query_quotes_for_catalog(query)?;
                Ok(DataBatch::Quote(quotes.into()))
            }
            NautilusDataType::InstrumentStatus => {
                let statuses = self.query_statuses_for_catalog(query)?;
                Ok(DataBatch::InstrumentStatus(statuses.into()))
            }
            ref other => Err(anyhow::Error::from(PersistenceError::unsupported(format!(
                "DataInfraCatalog::query_batch({other})"
            )))),
        }
    }

    fn query_batch_session(
        &mut self,
        query: &CatalogQuery,
        chunk_size: Option<usize>,
    ) -> anyhow::Result<DataBatchQueryResult> {
        match query.data_type {
            NautilusDataType::Bar => {
                let bars = self.query_bars_for_catalog(query)?;
                Ok(Box::new(TypedDataBatchSession::<Bar>::from_vec(
                    bars, chunk_size,
                )))
            }
            NautilusDataType::QuoteTick => {
                let quotes = self.query_quotes_for_catalog(query)?;
                Ok(Box::new(TypedDataBatchSession::<QuoteTick>::from_vec(
                    quotes, chunk_size,
                )))
            }
            NautilusDataType::InstrumentStatus => {
                let statuses = self.query_statuses_for_catalog(query)?;
                Ok(Box::new(
                    TypedDataBatchSession::<InstrumentStatus>::from_vec(statuses, chunk_size),
                ))
            }
            ref other => Err(anyhow::Error::from(PersistenceError::unsupported(format!(
                "DataInfraCatalog::query_batch_session({other})"
            )))),
        }
    }

    fn query_last_timestamp(
        &mut self,
        data_type: NautilusDataType,
        identifier: Option<&str>,
    ) -> anyhow::Result<Option<u64>> {
        let identifiers = identifier.map(|id| vec![id.to_string()]);
        let query = CatalogQuery::new(data_type).with_identifiers(identifiers);
        let batch = self.query_batch(&query)?;
        let last_ts = match batch {
            DataBatch::Bar(bars) => bars.as_ref().last().map(|b| b.ts_init.as_u64()),
            DataBatch::Quote(quotes) => quotes.as_ref().last().map(|q| q.ts_init.as_u64()),
            DataBatch::InstrumentStatus(statuses) => {
                statuses.as_ref().last().map(|s| s.ts_init.as_u64())
            }
            _ => None,
        };
        Ok(last_ts)
    }

    fn get_missing_intervals_for_request(
        &mut self,
        start: UnixNanos,
        end: UnixNanos,
        data_type: NautilusDataType,
        identifier: Option<&str>,
    ) -> anyhow::Result<Vec<(u64, u64)>> {
        if start > end {
            anyhow::bail!(
                "get_missing_intervals_for_request start ({start}) must be <= end ({end})"
            );
        }
        let identifiers = identifier.map(|id| vec![id.to_string()]);
        let query = CatalogQuery::new(data_type)
            .with_identifiers(identifiers)
            .with_range(Some(start), Some(end));
        let batch = self.query_batch(&query)?;

        let bounds: Option<(u64, u64)> = match batch {
            DataBatch::Bar(bars) => bars
                .as_ref()
                .first()
                .zip(bars.as_ref().last())
                .map(|(f, l)| (f.ts_init().as_u64(), l.ts_init().as_u64())),
            DataBatch::Quote(quotes) => quotes
                .as_ref()
                .first()
                .zip(quotes.as_ref().last())
                .map(|(f, l)| (f.ts_init().as_u64(), l.ts_init().as_u64())),
            DataBatch::InstrumentStatus(statuses) => statuses
                .as_ref()
                .first()
                .zip(statuses.as_ref().last())
                .map(|(f, l)| (f.ts_init().as_u64(), l.ts_init().as_u64())),
            _ => None,
        };

        let start_ns = start.as_u64();
        let end_ns = end.as_u64();
        let Some((first_ts, last_ts)) = bounds else {
            return Ok(vec![(start_ns, end_ns)]);
        };

        let mut missing = Vec::new();
        if first_ts > start_ns {
            missing.push((start_ns, first_ts.saturating_sub(1)));
        }
        if last_ts < end_ns {
            missing.push((last_ts.saturating_add(1), end_ns));
        }
        Ok(missing)
    }
}

impl CatalogWriter for DataInfraCatalog {
    fn write_instruments(&mut self, instruments: &[InstrumentAny]) -> anyhow::Result<()> {
        for inst in instruments {
            self.register_instrument(inst.clone());
        }
        Ok(())
    }

    fn write_data(
        &mut self,
        _data: &[Data],
        _start: Option<UnixNanos>,
        _end: Option<UnixNanos>,
        _params: Option<Params>,
    ) -> anyhow::Result<()> {
        Err(anyhow::Error::from(PersistenceError::unsupported(
            "DataInfraCatalog::write_data (read-only ClickHouse catalog)",
        )))
    }

    fn write_records(
        &mut self,
        _record_type: NautilusRecordType,
        _batches: &[RecordBatch],
        _params: Option<Params>,
    ) -> anyhow::Result<()> {
        Err(anyhow::Error::from(PersistenceError::unsupported(
            "DataInfraCatalog::write_records (read-only ClickHouse catalog)",
        )))
    }

    fn record_empty_coverage(
        &mut self,
        _data_type: NautilusDataType,
        _identifier: Option<&str>,
        _start: UnixNanos,
        _end: UnixNanos,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
