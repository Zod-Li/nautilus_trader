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

//! In-memory Apache Arrow (`ArrowStream` IPC & Parquet `RecordBatch`) batch decoders
//! for `DataInfra` ClickHouse (`v_quant_bars`, `sip_raw_quotes`, `sip_trading_statuses`).
//!
//! Enforces nanosecond UTC timestamp alignment (`ts_event` and `ts_init`), lossless fixed-point
//! [`Price`] / [`Quantity`] conversion, dirty-row filtering (`is_synthetic == true` or
//! `is_suspect_spike == true`), and chronological monotonicity.

use std::io::Cursor;

use arrow::{
    array::{
        Array, BooleanArray, DictionaryArray, Float32Array, Float64Array, Int32Array, Int64Array,
        LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
        UInt32Array, UInt64Array,
    },
    datatypes::{DataType, Int8Type, Int16Type, Int32Type, TimeUnit},
    ipc::reader::StreamReader,
};
use bytes::Bytes;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, InstrumentStatus, QuoteTick},
    enums::MarketStatusAction,
    types::{Price, Quantity},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use ustr::Ustr;

use crate::{
    error::{DataInfraError, DataInfraResult},
    mapping::{DataInfraTimeframe, parse_datainfra_bar_type, to_bar_type, to_instrument_id},
};

/// Selects whether ClickHouse `v_quant_bars` decoding uses split/dividend-adjusted
/// (`adj_*`) or unadjusted raw (`raw_*`) OHLCV columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum PriceAdjustmentMode {
    /// Forward-adjusted (`adj_open`, `adj_high`, `adj_low`, `adj_close`, `adj_volume`).
    #[default]
    Adjusted,
    /// Unadjusted raw (`raw_open`, `raw_high`, `raw_low`, `raw_close`, `raw_volume`).
    Raw,
}

impl PriceAdjustmentMode {
    /// Returns the primary `(open, high, low, close, volume)` column names for this mode.
    #[must_use]
    pub const fn column_names(
        self,
    ) -> (
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        &'static str,
    ) {
        match self {
            Self::Adjusted => ("adj_open", "adj_high", "adj_low", "adj_close", "adj_volume"),
            Self::Raw => ("raw_open", "raw_high", "raw_low", "raw_close", "raw_volume"),
        }
    }
}

/// Configuration options for decoding `DataInfra` Arrow `RecordBatch` streams into Nautilus domain objects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrowDecodeOptions {
    /// Default venue code used to construct [`InstrumentId`](nautilus_model::identifiers::InstrumentId)s.
    pub venue: String,
    /// Default timeframe used when a `RecordBatch` does not carry an explicit `timeframe` column.
    pub default_timeframe: DataInfraTimeframe,
    /// Fixed-point decimal precision for decoded [`Price`] values (default `4` or `2`).
    pub price_precision: u8,
    /// Fixed-point decimal precision for decoded [`Quantity`] values (default `0` or `2`).
    pub size_precision: u8,
    /// Price adjustment mode (`Adjusted` vs `Raw`).
    pub adjustment: PriceAdjustmentMode,
}

impl Default for ArrowDecodeOptions {
    fn default() -> Self {
        Self {
            venue: "NASDAQ".to_string(),
            default_timeframe: DataInfraTimeframe::M15,
            price_precision: 2,
            size_precision: 0,
            adjustment: PriceAdjustmentMode::Adjusted,
        }
    }
}

impl ArrowDecodeOptions {
    /// Constructs a new [`ArrowDecodeOptions`] with explicit parameters.
    #[must_use]
    pub fn new(
        venue: impl Into<String>,
        default_timeframe: DataInfraTimeframe,
        price_precision: u8,
        size_precision: u8,
        adjustment: PriceAdjustmentMode,
    ) -> Self {
        Self {
            venue: venue.into(),
            default_timeframe,
            price_precision,
            size_precision,
            adjustment,
        }
    }
}

/// Decodes an Apache Arrow IPC stream (`FORMAT ArrowStream`) byte buffer into `Vec<RecordBatch>`.
///
/// # Errors
///
/// Returns [`DataInfraError::Transport`] if the byte buffer is empty or is not a valid Arrow IPC stream.
pub fn decode_arrow_ipc_stream(raw_bytes: &[u8]) -> DataInfraResult<Vec<RecordBatch>> {
    if raw_bytes.is_empty() {
        return Err(DataInfraError::Transport(
            "ClickHouse ArrowStream response body is empty (fail-closed)".to_string(),
        ));
    }
    let cursor = Cursor::new(raw_bytes);
    let reader = StreamReader::try_new(cursor, None).map_err(|e| {
        DataInfraError::Transport(format!("failed to open Arrow IPC StreamReader: {e}"))
    })?;

    let mut batches = Vec::new();
    for batch_res in reader {
        let batch = batch_res.map_err(|e| {
            DataInfraError::Transport(format!("failed to decode Arrow RecordBatch from stream: {e}"))
        })?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
    Ok(batches)
}

/// Decodes an in-memory Apache Parquet byte buffer into `Vec<RecordBatch>` with zero temporary files.
///
/// # Errors
///
/// Returns [`DataInfraError::Transport`] if the Parquet payload is empty or malformed.
pub fn decode_parquet_bytes(parquet_bytes: Bytes) -> DataInfraResult<Vec<RecordBatch>> {
    if parquet_bytes.is_empty() {
        return Err(DataInfraError::Transport(
            "Parquet byte buffer is empty (fail-closed)".to_string(),
        ));
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes).map_err(|e| {
        DataInfraError::Transport(format!("failed to initialize Parquet reader: {e}"))
    })?;
    let reader = builder.build().map_err(|e| {
        DataInfraError::Transport(format!("failed to build Parquet RecordBatchReader: {e}"))
    })?;

    let mut batches = Vec::new();
    for batch_res in reader {
        let batch = batch_res.map_err(|e| {
            DataInfraError::Transport(format!("failed to read Parquet RecordBatch: {e}"))
        })?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
    Ok(batches)
}

/// Decodes `v_quant_bars` Arrow `RecordBatch` slices into chronologically ordered Nautilus [`Bar`]s.
///
/// Rows with `is_synthetic == true` or `is_suspect_spike == true` are automatically discarded.
///
/// # Errors
///
/// Returns [`DataInfraError`] if required columns are missing, prices/volumes are non-finite or
/// violate OHLC invariants, or timestamps cannot be converted into [`UnixNanos`].
pub fn decode_bars_batches(
    batches: &[RecordBatch],
    options: &ArrowDecodeOptions,
) -> DataInfraResult<Vec<Bar>> {
    let mut bars = Vec::new();
    let (primary_open, primary_high, primary_low, primary_close, primary_vol) =
        options.adjustment.column_names();

    for batch in batches {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }
        let schema = batch.schema();

        let sym_idx = find_column_index(&schema, &["symbol"])?;
        let tf_idx = schema.index_of("timeframe").ok();
        let ts_idx = find_column_index(
            &schema,
            &["ts_event", "ts", "bar_ts", "bar_start_time", "timestamp"],
        )?;
        let ts_init_idx = schema
            .index_of("ts_init")
            .ok()
            .or_else(|| schema.index_of("ingest_ts_ms").ok());

        let open_idx = find_column_index(&schema, &[primary_open, "open"])?;
        let high_idx = find_column_index(&schema, &[primary_high, "high"])?;
        let low_idx = find_column_index(&schema, &[primary_low, "low"])?;
        let close_idx = find_column_index(&schema, &[primary_close, "close"])?;
        let vol_idx = find_column_index(&schema, &[primary_vol, "volume"])?;

        let synth_idx = schema.index_of("is_synthetic").ok();
        let spike_idx = schema.index_of("is_suspect_spike").ok();

        let sym_col = batch.column(sym_idx);
        let ts_col = batch.column(ts_idx);
        let open_col = batch.column(open_idx);
        let high_col = batch.column(high_idx);
        let low_col = batch.column(low_idx);
        let close_col = batch.column(close_idx);
        let vol_col = batch.column(vol_idx);

        for row in 0..num_rows {
            if let Some(idx) = synth_idx
                && extract_bool_at(batch.column(idx), row)?
            {
                continue;
            }
            if let Some(idx) = spike_idx
                && extract_bool_at(batch.column(idx), row)?
            {
                continue;
            }

            let symbol = extract_string_at(sym_col, row, "symbol")?;
            let bar_type = if let Some(idx) = tf_idx {
                let tf_str = extract_string_at(batch.column(idx), row, "timeframe")?;
                parse_datainfra_bar_type(&symbol, &options.venue, &tf_str)?
            } else {
                to_bar_type(&symbol, &options.venue, options.default_timeframe)?
            };

            let ts_event = extract_unix_nanos_at(ts_col, row, "ts_event")?;
            let ts_init = if let Some(idx) = ts_init_idx {
                let candidate = extract_unix_nanos_at(batch.column(idx), row, "ts_init")?;
                if candidate < ts_event {
                    ts_event
                } else {
                    candidate
                }
            } else {
                ts_event
            };

            let open_f64 = extract_f64_at(open_col, row, primary_open)?;
            let high_f64 = extract_f64_at(high_col, row, primary_high)?;
            let low_f64 = extract_f64_at(low_col, row, primary_low)?;
            let close_f64 = extract_f64_at(close_col, row, primary_close)?;
            let vol_f64 = extract_f64_at(vol_col, row, primary_vol)?;

            let open = f64_to_price(open_f64, options.price_precision, "open")?;
            let high = f64_to_price(high_f64, options.price_precision, "high")?;
            let low = f64_to_price(low_f64, options.price_precision, "low")?;
            let close = f64_to_price(close_f64, options.price_precision, "close")?;
            let volume = f64_to_quantity(vol_f64, options.size_precision, "volume")?;

            let bar = Bar::new_checked(
                bar_type, open, high, low, close, volume, ts_event, ts_init,
            )
            .map_err(|e| {
                DataInfraError::ProvenanceViolation(format!(
                    "OHLC sanity violation for {symbol} at ts_event={ts_event}: {e}"
                ))
            })?;
            bars.push(bar);
        }
    }

    bars.sort_by_key(|b| (b.ts_event, b.bar_type));
    Ok(bars)
}

/// Decodes an `ArrowStream` IPC byte buffer of `v_quant_bars` directly into `Vec<Bar>`.
///
/// # Errors
///
/// Returns [`DataInfraError`] if the Arrow stream is empty, malformed, or violates bar invariants.
pub fn decode_bars_arrow_stream(
    raw_bytes: &[u8],
    options: &ArrowDecodeOptions,
) -> DataInfraResult<Vec<Bar>> {
    let batches = decode_arrow_ipc_stream(raw_bytes)?;
    decode_bars_batches(&batches, options)
}

/// Decodes a Parquet byte buffer of `v_quant_bars` directly into `Vec<Bar>`.
///
/// # Errors
///
/// Returns [`DataInfraError`] if the Parquet payload is empty, malformed, or violates bar invariants.
pub fn decode_bars_parquet(
    parquet_bytes: Bytes,
    options: &ArrowDecodeOptions,
) -> DataInfraResult<Vec<Bar>> {
    let batches = decode_parquet_bytes(parquet_bytes)?;
    decode_bars_batches(&batches, options)
}

/// Decodes `sip_raw_quotes` Arrow `RecordBatch` slices into chronologically ordered Nautilus [`QuoteTick`]s.
///
/// # Errors
///
/// Returns [`DataInfraError`] if required columns are missing or bid/ask values are invalid.
pub fn decode_quotes_batches(
    batches: &[RecordBatch],
    options: &ArrowDecodeOptions,
) -> DataInfraResult<Vec<QuoteTick>> {
    let mut quotes = Vec::new();

    for batch in batches {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }
        let schema = batch.schema();

        let sym_idx = find_column_index(&schema, &["symbol"])?;
        let ts_idx = find_column_index(
            &schema,
            &["ts_event", "quote_ts_ms", "event_time", "ts", "timestamp"],
        )?;
        let ts_init_idx = schema
            .index_of("ts_init")
            .ok()
            .or_else(|| schema.index_of("ingest_ts_ms").ok());

        let bid_px_idx = find_column_index(&schema, &["bid_price"])?;
        let ask_px_idx = find_column_index(&schema, &["ask_price"])?;
        let bid_sz_idx = find_column_index(&schema, &["bid_size"])?;
        let ask_sz_idx = find_column_index(&schema, &["ask_size"])?;

        let sym_col = batch.column(sym_idx);
        let ts_col = batch.column(ts_idx);
        let bid_px_col = batch.column(bid_px_idx);
        let ask_px_col = batch.column(ask_px_idx);
        let bid_sz_col = batch.column(bid_sz_idx);
        let ask_sz_col = batch.column(ask_sz_idx);

        for row in 0..num_rows {
            let symbol = extract_string_at(sym_col, row, "symbol")?;
            let instrument_id = to_instrument_id(&symbol, &options.venue)?;

            let ts_event = extract_unix_nanos_at(ts_col, row, "ts_event")?;
            let ts_init = if let Some(idx) = ts_init_idx {
                let candidate = extract_unix_nanos_at(batch.column(idx), row, "ts_init")?;
                if candidate < ts_event {
                    ts_event
                } else {
                    candidate
                }
            } else {
                ts_event
            };

            let bid_f64 = extract_f64_at(bid_px_col, row, "bid_price")?;
            let ask_f64 = extract_f64_at(ask_px_col, row, "ask_price")?;
            let bid_sz_f64 = extract_f64_at(bid_sz_col, row, "bid_size")?;
            let ask_sz_f64 = extract_f64_at(ask_sz_col, row, "ask_size")?;

            if ask_f64 < bid_f64 {
                return Err(DataInfraError::ProvenanceViolation(format!(
                    "crossed quote for {symbol} at ts_event={ts_event}: bid={bid_f64} > ask={ask_f64}"
                )));
            }

            let bid_price = f64_to_price(bid_f64, options.price_precision, "bid_price")?;
            let ask_price = f64_to_price(ask_f64, options.price_precision, "ask_price")?;
            let bid_size = f64_to_quantity(bid_sz_f64, options.size_precision, "bid_size")?;
            let ask_size = f64_to_quantity(ask_sz_f64, options.size_precision, "ask_size")?;

            let quote = QuoteTick::new_checked(
                instrument_id,
                bid_price,
                ask_price,
                bid_size,
                ask_size,
                ts_event,
                ts_init,
            )
            .map_err(|e| {
                DataInfraError::ProvenanceViolation(format!(
                    "invalid QuoteTick for {symbol} at ts_event={ts_event}: {e}"
                ))
            })?;
            quotes.push(quote);
        }
    }

    quotes.sort_by_key(|q| (q.ts_event, q.instrument_id));
    Ok(quotes)
}

/// Decodes an `ArrowStream` IPC byte buffer of `sip_raw_quotes` directly into `Vec<QuoteTick>`.
///
/// # Errors
///
/// Returns [`DataInfraError`] if the Arrow stream is empty, malformed, or violates quote invariants.
pub fn decode_quotes_arrow_stream(
    raw_bytes: &[u8],
    options: &ArrowDecodeOptions,
) -> DataInfraResult<Vec<QuoteTick>> {
    let batches = decode_arrow_ipc_stream(raw_bytes)?;
    decode_quotes_batches(&batches, options)
}

/// Decodes `sip_trading_statuses` Arrow `RecordBatch` slices into chronologically ordered [`InstrumentStatus`] events.
///
/// # Errors
///
/// Returns [`DataInfraError`] if required columns are missing or status values are invalid.
pub fn decode_statuses_batches(
    batches: &[RecordBatch],
    options: &ArrowDecodeOptions,
) -> DataInfraResult<Vec<InstrumentStatus>> {
    let mut statuses = Vec::new();

    for batch in batches {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }
        let schema = batch.schema();

        let sym_idx = find_column_index(&schema, &["symbol"])?;
        let ts_idx = find_column_index(
            &schema,
            &["ts_event", "status_ts_ms", "event_time", "ts", "timestamp"],
        )?;
        let ts_init_idx = schema
            .index_of("ts_init")
            .ok()
            .or_else(|| schema.index_of("ingest_ts_ms").ok());
        let state_idx = find_column_index(&schema, &["state", "status", "action"])?;
        let reason_idx = schema
            .index_of("reason_code")
            .ok()
            .or_else(|| schema.index_of("reason").ok());

        let sym_col = batch.column(sym_idx);
        let ts_col = batch.column(ts_idx);
        let state_col = batch.column(state_idx);

        for row in 0..num_rows {
            let symbol = extract_string_at(sym_col, row, "symbol")?;
            let instrument_id = to_instrument_id(&symbol, &options.venue)?;

            let ts_event = extract_unix_nanos_at(ts_col, row, "ts_event")?;
            let ts_init = if let Some(idx) = ts_init_idx {
                let candidate = extract_unix_nanos_at(batch.column(idx), row, "ts_init")?;
                if candidate < ts_event {
                    ts_event
                } else {
                    candidate
                }
            } else {
                ts_event
            };

            let state_raw = extract_string_at(state_col, row, "state")?;
            let (action, is_trading, is_ssr) = map_trading_status_state(&state_raw)?;

            let reason = if let Some(idx) = reason_idx {
                let col = batch.column(idx);
                if col.is_null(row) {
                    None
                } else {
                    let r_str = extract_string_at(col, row, "reason_code")?;
                    if r_str.trim().is_empty() {
                        None
                    } else {
                        Some(Ustr::from(r_str.trim()))
                    }
                }
            } else {
                None
            };

            let status = InstrumentStatus::new(
                instrument_id,
                action,
                ts_event,
                ts_init,
                reason,
                Some(Ustr::from(state_raw.trim())),
                Some(is_trading),
                Some(is_trading),
                Some(is_ssr),
            );
            statuses.push(status);
        }
    }

    statuses.sort_by_key(|s| (s.ts_event, s.instrument_id));
    Ok(statuses)
}

/// Decodes an `ArrowStream` IPC byte buffer of `sip_trading_statuses` directly into `Vec<InstrumentStatus>`.
///
/// # Errors
///
/// Returns [`DataInfraError`] if the Arrow stream is empty, malformed, or contains unknown status codes.
pub fn decode_statuses_arrow_stream(
    raw_bytes: &[u8],
    options: &ArrowDecodeOptions,
) -> DataInfraResult<Vec<InstrumentStatus>> {
    let batches = decode_arrow_ipc_stream(raw_bytes)?;
    decode_statuses_batches(&batches, options)
}

/// Maps a canonical `DataInfra` / `sip-feeder` trading status string to
/// `(MarketStatusAction, is_trading, is_short_sell_restricted)`.
///
/// # Errors
///
/// Returns [`DataInfraError::ProvenanceViolation`] if the status string is unrecognized.
pub fn map_trading_status_state(
    state_raw: &str,
) -> DataInfraResult<(MarketStatusAction, bool, bool)> {
    match state_raw.trim().to_uppercase().as_str() {
        "TRADING" | "T" | "Q" => Ok((MarketStatusAction::Trading, true, false)),
        "HALTED" | "HALT" | "H" | "M" => Ok((MarketStatusAction::Halt, false, false)),
        "LULD_PAUSE" | "PAUSE" | "P" | "LULD" => Ok((MarketStatusAction::Pause, false, false)),
        "SHORT_SALE_RESTRICTED" | "SSR" => Ok((MarketStatusAction::Trading, true, true)),
        "PRE_OPEN" | "PREOPEN" => Ok((MarketStatusAction::PreOpen, false, false)),
        "CLOSE" | "CLOSED" => Ok((MarketStatusAction::Close, false, false)),
        other => Err(DataInfraError::ProvenanceViolation(format!(
            "unrecognized trading status state '{other}'"
        ))),
    }
}

/// Converts a finite positive `f64` into a fixed-point Nautilus [`Price`] with `precision` decimal places.
///
/// # Errors
///
/// Returns [`DataInfraError::ProvenanceViolation`] if `value` is non-finite or `<= 0.0`.
pub fn f64_to_price(value: f64, precision: u8, field_name: &str) -> DataInfraResult<Price> {
    if !value.is_finite() || value <= 0.0 {
        return Err(DataInfraError::ProvenanceViolation(format!(
            "{field_name} must be finite and > 0.0, got {value}"
        )));
    }
    let dec = Decimal::from_f64_retain(value)
        .ok_or_else(|| {
            DataInfraError::ProvenanceViolation(format!(
                "failed to convert {field_name}={value} into Decimal"
            ))
        })?
        .round_dp_with_strategy(u32::from(precision), RoundingStrategy::MidpointNearestEven);
    Price::from_decimal_dp(dec, precision).map_err(|e| {
        DataInfraError::ProvenanceViolation(format!("invalid Price for {field_name}={value}: {e}"))
    })
}

/// Converts a finite non-negative `f64` into a fixed-point Nautilus [`Quantity`] with `precision` decimal places.
///
/// # Errors
///
/// Returns [`DataInfraError::ProvenanceViolation`] if `value` is non-finite or `< 0.0`.
pub fn f64_to_quantity(value: f64, precision: u8, field_name: &str) -> DataInfraResult<Quantity> {
    if !value.is_finite() || value < 0.0 {
        return Err(DataInfraError::ProvenanceViolation(format!(
            "{field_name} must be finite and >= 0.0, got {value}"
        )));
    }
    let dec = Decimal::from_f64_retain(value)
        .ok_or_else(|| {
            DataInfraError::ProvenanceViolation(format!(
                "failed to convert {field_name}={value} into Decimal"
            ))
        })?
        .round_dp_with_strategy(u32::from(precision), RoundingStrategy::MidpointNearestEven);
    Quantity::from_decimal_dp(dec, precision).map_err(|e| {
        DataInfraError::ProvenanceViolation(format!(
            "invalid Quantity for {field_name}={value}: {e}"
        ))
    })
}

/// Normalizes a raw integer timestamp (seconds, milliseconds, microseconds, or nanoseconds)
/// into a nanosecond UTC [`UnixNanos`].
///
/// # Errors
///
/// Returns [`DataInfraError::ProvenanceViolation`] if `raw_ts <= 0` or overflows `u64`.
pub fn normalize_epoch_to_unix_nanos(raw_ts: i64, field_name: &str) -> DataInfraResult<UnixNanos> {
    if raw_ts <= 0 {
        return Err(DataInfraError::ProvenanceViolation(format!(
            "{field_name} timestamp must be > 0, got {raw_ts}"
        )));
    }
    let ts_u64 = u64::try_from(raw_ts).map_err(|_| {
        DataInfraError::ProvenanceViolation(format!("invalid negative timestamp {raw_ts}"))
    })?;
    let nanos = if ts_u64 < 10_000_000_000 {
        ts_u64.saturating_mul(1_000_000_000)
    } else if ts_u64 < 10_000_000_000_000 {
        ts_u64.saturating_mul(1_000_000)
    } else if ts_u64 < 10_000_000_000_000_000 {
        ts_u64.saturating_mul(1_000)
    } else {
        ts_u64
    };
    Ok(UnixNanos::from(nanos))
}

fn find_column_index(schema: &arrow::datatypes::Schema, candidates: &[&str]) -> DataInfraResult<usize> {
    for &name in candidates {
        if let Ok(idx) = schema.index_of(name) {
            return Ok(idx);
        }
    }
    Err(DataInfraError::Transport(format!(
        "Arrow RecordBatch missing required column (tried {:?}, schema fields: {:?})",
        candidates,
        schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
    )))
}

fn extract_string_at(col: &dyn Array, row: usize, field_name: &str) -> DataInfraResult<String> {
    if col.is_null(row) {
        return Err(DataInfraError::ProvenanceViolation(format!(
            "null value in required string column '{field_name}' at row {row}"
        )));
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return Ok(arr.value(row).to_string());
    }
    if let Some(arr) = col.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(arr.value(row).to_string());
    }
    if let Some(arr) = col.as_any().downcast_ref::<DictionaryArray<Int8Type>>() {
        let key = arr.keys().value(row);
        let values = arr
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataInfraError::Transport(format!(
                    "unsupported dictionary value array for '{field_name}'"
                ))
            })?;
        #[expect(clippy::cast_sign_loss)]
        return Ok(values.value(key as usize).to_string());
    }
    if let Some(arr) = col.as_any().downcast_ref::<DictionaryArray<Int16Type>>() {
        let key = arr.keys().value(row);
        let values = arr
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataInfraError::Transport(format!(
                    "unsupported dictionary value array for '{field_name}'"
                ))
            })?;
        #[expect(clippy::cast_sign_loss)]
        return Ok(values.value(key as usize).to_string());
    }
    if let Some(arr) = col.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
        let key = arr.keys().value(row);
        let values = arr
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataInfraError::Transport(format!(
                    "unsupported dictionary value array for '{field_name}'"
                ))
            })?;
        #[expect(clippy::cast_sign_loss)]
        return Ok(values.value(key as usize).to_string());
    }
    Err(DataInfraError::Transport(format!(
        "unsupported Arrow data type {:?} for string column '{field_name}'",
        col.data_type()
    )))
}

fn extract_bool_at(col: &dyn Array, row: usize) -> DataInfraResult<bool> {
    if col.is_null(row) {
        return Ok(false);
    }
    if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
        return Ok(arr.value(row));
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt8Array>() {
        return Ok(arr.value(row) != 0);
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return Ok(arr.value(row) != 0);
    }
    Err(DataInfraError::Transport(format!(
        "unsupported Arrow boolean flag column type {:?}",
        col.data_type()
    )))
}

fn extract_f64_at(col: &dyn Array, row: usize, field_name: &str) -> DataInfraResult<f64> {
    if col.is_null(row) {
        return Err(DataInfraError::ProvenanceViolation(format!(
            "null value in numeric column '{field_name}' at row {row}"
        )));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        return Ok(arr.value(row));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
        return Ok(f64::from(arr.value(row)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        #[expect(clippy::cast_precision_loss)]
        return Ok(arr.value(row) as f64);
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        #[expect(clippy::cast_precision_loss)]
        return Ok(arr.value(row) as f64);
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
        return Ok(f64::from(arr.value(row)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt32Array>() {
        return Ok(f64::from(arr.value(row)));
    }
    Err(DataInfraError::Transport(format!(
        "unsupported Arrow data type {:?} for numeric column '{field_name}'",
        col.data_type()
    )))
}

fn extract_unix_nanos_at(
    col: &dyn Array,
    row: usize,
    field_name: &str,
) -> DataInfraResult<UnixNanos> {
    if col.is_null(row) {
        return Err(DataInfraError::ProvenanceViolation(format!(
            "null timestamp in '{field_name}' at row {row}"
        )));
    }
    match col.data_type() {
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            normalize_epoch_to_unix_nanos(arr.value(row), field_name)
        }
        DataType::UInt64 => {
            let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
            let raw = i64::try_from(arr.value(row)).map_err(|_| {
                DataInfraError::ProvenanceViolation(format!(
                    "timestamp overflow in '{field_name}' at row {row}"
                ))
            })?;
            normalize_epoch_to_unix_nanos(raw, field_name)
        }
        DataType::UInt32 => {
            let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
            normalize_epoch_to_unix_nanos(i64::from(arr.value(row)), field_name)
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            normalize_epoch_to_unix_nanos(i64::from(arr.value(row)), field_name)
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = col.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
            let secs = arr.value(row);
            if secs <= 0 {
                return Err(DataInfraError::ProvenanceViolation(format!(
                    "non-positive timestamp {secs}s in '{field_name}'"
                )));
            }
            #[expect(clippy::cast_sign_loss)]
            Ok(UnixNanos::from((secs as u64).saturating_mul(1_000_000_000)))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let ms = arr.value(row);
            if ms <= 0 {
                return Err(DataInfraError::ProvenanceViolation(format!(
                    "non-positive timestamp {ms}ms in '{field_name}'"
                )));
            }
            #[expect(clippy::cast_sign_loss)]
            Ok(UnixNanos::from((ms as u64).saturating_mul(1_000_000)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let us = arr.value(row);
            if us <= 0 {
                return Err(DataInfraError::ProvenanceViolation(format!(
                    "non-positive timestamp {us}us in '{field_name}'"
                )));
            }
            #[expect(clippy::cast_sign_loss)]
            Ok(UnixNanos::from((us as u64).saturating_mul(1_000)))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let ns = arr.value(row);
            if ns <= 0 {
                return Err(DataInfraError::ProvenanceViolation(format!(
                    "non-positive timestamp {ns}ns in '{field_name}'"
                )));
            }
            #[expect(clippy::cast_sign_loss)]
            Ok(UnixNanos::from(ns as u64))
        }
        other => Err(DataInfraError::Transport(format!(
            "unsupported Arrow timestamp data type {other:?} for '{field_name}'"
        ))),
    }
}
