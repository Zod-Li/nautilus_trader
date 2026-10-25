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

//! Integration tests for `nautilus-datainfra` ClickHouse `ArrowStream` / Parquet batch decoders
//! and `HttpClickHouseArrowTransport` wire-level fail-closed behavior (Ticket #02).

#![allow(linker_messages)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow::{
    array::{
        ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int8Array, Int64Array, RecordBatch,
        StringArray, TimestampNanosecondArray, UInt8Array, UInt64Array,
    },
    datatypes::{DataType, Field, Int8Type, Schema, TimeUnit},
    ipc::writer::StreamWriter,
};
use bytes::Bytes;
use nautilus_core::UnixNanos;
use nautilus_datainfra::{
    ArrowDecodeOptions, BarsQueryRequest, ClickHouseArrowTransport, DataInfraConfig,
    DataInfraError, DataInfraTimeframe, HttpClickHouseArrowTransport, PriceAdjustmentMode,
    QuotesQueryRequest, StatusesQueryRequest,
    arrow_decoder::{f64_to_price, f64_to_quantity, map_trading_status_state, normalize_epoch_to_unix_nanos},
    build_sip_quotes_query, build_sip_statuses_query, build_v_quant_bars_query,
    decode_arrow_ipc_stream, decode_bars_arrow_stream, decode_bars_batches, decode_bars_parquet,
    decode_parquet_bytes, decode_quotes_arrow_stream, decode_quotes_batches,
    decode_statuses_arrow_stream, decode_statuses_batches, parse_datainfra_bar_type,
    to_instrument_id,
};
use nautilus_model::{
    enums::MarketStatusAction,
    types::{Price, Quantity},
};
use parquet::arrow::ArrowWriter;
use rstest::rstest;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

// ================================================================================================
// Test Helpers: In-Memory Arrow IPC Stream & Parquet Encoders
// ================================================================================================

fn encode_batches_to_arrow_ipc_stream(batches: &[RecordBatch]) -> Vec<u8> {
    assert!(!batches.is_empty(), "batches must not be empty");
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut buf, &schema).expect("create Arrow IPC StreamWriter");
        for batch in batches {
            writer.write(batch).expect("write RecordBatch to IPC stream");
        }
        writer.finish().expect("finish Arrow IPC StreamWriter");
    }
    buf
}

fn encode_batches_to_parquet_bytes(batches: &[RecordBatch]) -> Bytes {
    assert!(!batches.is_empty(), "batches must not be empty");
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut writer =
            ArrowWriter::try_new(&mut buf, schema, None).expect("create Parquet ArrowWriter");
        for batch in batches {
            writer.write(batch).expect("write RecordBatch to Parquet");
        }
        writer.close().expect("close Parquet ArrowWriter");
    }
    Bytes::from(buf)
}

#[expect(clippy::too_many_arguments)]
fn build_v_quant_bars_record_batch(
    symbols: &[&str],
    timeframes: &[&str],
    ts_seconds: &[i64],
    adj_ohlcv: &[(f64, f64, f64, f64, f64)],
    raw_ohlcv: &[(f64, f64, f64, f64, f64)],
    is_synthetic: &[bool],
    is_suspect_spike: &[bool],
    ingest_ts_ms: Option<&[i64]>,
) -> RecordBatch {
    let mut fields = vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("timeframe", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("adj_open", DataType::Float64, false),
        Field::new("adj_high", DataType::Float64, false),
        Field::new("adj_low", DataType::Float64, false),
        Field::new("adj_close", DataType::Float64, false),
        Field::new("adj_volume", DataType::Float64, false),
        Field::new("raw_open", DataType::Float64, false),
        Field::new("raw_high", DataType::Float64, false),
        Field::new("raw_low", DataType::Float64, false),
        Field::new("raw_close", DataType::Float64, false),
        Field::new("raw_volume", DataType::Float64, false),
        Field::new("is_synthetic", DataType::Boolean, false),
        Field::new("is_suspect_spike", DataType::Boolean, false),
    ];

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(symbols.to_vec())),
        Arc::new(StringArray::from(timeframes.to_vec())),
        Arc::new(Int64Array::from(ts_seconds.to_vec())),
        Arc::new(Float64Array::from(
            adj_ohlcv.iter().map(|x| x.0).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            adj_ohlcv.iter().map(|x| x.1).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            adj_ohlcv.iter().map(|x| x.2).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            adj_ohlcv.iter().map(|x| x.3).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            adj_ohlcv.iter().map(|x| x.4).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            raw_ohlcv.iter().map(|x| x.0).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            raw_ohlcv.iter().map(|x| x.1).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            raw_ohlcv.iter().map(|x| x.2).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            raw_ohlcv.iter().map(|x| x.3).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            raw_ohlcv.iter().map(|x| x.4).collect::<Vec<_>>(),
        )),
        Arc::new(BooleanArray::from(is_synthetic.to_vec())),
        Arc::new(BooleanArray::from(is_suspect_spike.to_vec())),
    ];

    if let Some(ingest_ms) = ingest_ts_ms {
        fields.push(Field::new("ingest_ts_ms", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(ingest_ms.to_vec())));
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).expect("valid v_quant_bars RecordBatch")
}

fn build_quotes_record_batch(
    symbols: &[&str],
    ts_nanos: &[i64],
    bids: &[f64],
    asks: &[f64],
    bid_sizes: &[f64],
    ask_sizes: &[f64],
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "ts_event",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("bid_price", DataType::Float64, false),
        Field::new("ask_price", DataType::Float64, false),
        Field::new("bid_size", DataType::Float64, false),
        Field::new("ask_size", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(symbols.to_vec())),
            Arc::new(TimestampNanosecondArray::from(ts_nanos.to_vec())),
            Arc::new(Float64Array::from(bids.to_vec())),
            Arc::new(Float64Array::from(asks.to_vec())),
            Arc::new(Float64Array::from(bid_sizes.to_vec())),
            Arc::new(Float64Array::from(ask_sizes.to_vec())),
        ],
    )
    .expect("valid sip_raw_quotes RecordBatch")
}

fn build_statuses_record_batch(
    symbols: &[&str],
    ts_nanos: &[i64],
    states: &[&str],
    reasons: &[Option<&str>],
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("ts_event", DataType::Int64, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("reason_code", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(symbols.to_vec())),
            Arc::new(Int64Array::from(ts_nanos.to_vec())),
            Arc::new(StringArray::from(states.to_vec())),
            Arc::new(StringArray::from(reasons.to_vec())),
        ],
    )
    .expect("valid sip_trading_statuses RecordBatch")
}

fn transport_config_for_url(clickhouse_url: &str, max_retries: u32) -> DataInfraConfig {
    DataInfraConfig::new_checked(
        clickhouse_url,
        "market_data",
        2_000,
        max_retries,
        vec!["127.0.0.1:9092".to_string()],
        "nautilus-datainfra-test",
        "http://127.0.0.1:8012",
        "NASDAQ",
    )
    .expect("valid DataInfraConfig for test")
}

// ================================================================================================
// 1. Multi-Symbol & Multi-Timeframe v_quant_bars Decoding (RecordBatch, ArrowStream, Parquet)
// ================================================================================================

#[rstest]
fn test_decode_v_quant_bars_across_all_timeframes_and_symbols_adjusted_and_raw() {
    // 5 canonical timeframes (`15m`, `1h`, `4h`, `1d`, `1w`) across `AAPL`, `NVDA`, `MSFT`
    // deliberately supplied in non-monotonic timestamp order to verify sorting.
    let symbols = ["NVDA", "AAPL", "MSFT", "AAPL", "NVDA"];
    let timeframes = ["1w", "15m", "1h", "4h", "1d"];
    let ts_seconds = [
        1_700_005_000_i64,
        1_700_001_000_i64,
        1_700_002_000_i64,
        1_700_001_000_i64, // Same timestamp as AAPL 15m, different BarType
        1_700_004_000_i64,
    ];
    let adj_ohlcv = [
        (480.25, 495.50, 475.00, 490.75, 45_000_000.0),
        (185.10, 186.40, 184.90, 186.05, 1_250_300.0),
        (370.00, 372.80, 369.50, 371.25, 3_400_000.0),
        (184.00, 187.20, 183.50, 186.90, 5_800_100.0),
        (485.00, 492.00, 481.20, 489.60, 12_345_678.0),
    ];
    // Raw unadjusted prices (e.g., pre-split NVDA 10x higher, volume 10x lower)
    let raw_ohlcv = [
        (4802.50, 4955.00, 4750.00, 4907.50, 4_500_000.0),
        (185.60, 186.90, 185.40, 186.55, 1_250_300.0),
        (371.00, 373.80, 370.50, 372.25, 3_400_000.0),
        (184.50, 187.70, 184.00, 187.40, 5_800_100.0),
        (4850.00, 4920.00, 4812.00, 4896.00, 1_234_568.0),
    ];
    let is_synthetic = [false, false, false, false, false];
    let is_suspect_spike = [false, false, false, false, false];

    let batch = build_v_quant_bars_record_batch(
        &symbols,
        &timeframes,
        &ts_seconds,
        &adj_ohlcv,
        &raw_ohlcv,
        &is_synthetic,
        &is_suspect_spike,
        None,
    );

    let ipc_bytes = encode_batches_to_arrow_ipc_stream(std::slice::from_ref(&batch));
    let parquet_bytes = encode_batches_to_parquet_bytes(std::slice::from_ref(&batch));

    // --- A. Adjusted Mode ---
    let adj_opts = ArrowDecodeOptions::new(
        "NASDAQ",
        DataInfraTimeframe::M15,
        2,
        0,
        PriceAdjustmentMode::Adjusted,
    );
    let bars_from_batch =
        decode_bars_batches(std::slice::from_ref(&batch), &adj_opts).expect("decode adjusted batch");
    let bars_from_ipc =
        decode_bars_arrow_stream(&ipc_bytes, &adj_opts).expect("decode adjusted IPC stream");
    let bars_from_parquet =
        decode_bars_parquet(parquet_bytes.clone(), &adj_opts).expect("decode adjusted Parquet");

    assert_eq!(bars_from_batch.len(), 5);
    assert_eq!(bars_from_batch, bars_from_ipc);
    assert_eq!(bars_from_batch, bars_from_parquet);

    // Verify monotonic `(ts_event, bar_type)` ordering
    for pair in bars_from_batch.windows(2) {
        assert!(
            (pair[0].ts_event, pair[0].bar_type) <= (pair[1].ts_event, pair[1].bar_type),
            "bars must be sorted monotonically by (ts_event, bar_type)"
        );
    }

    // Check first two bars at ts=1_700_001_000s (AAPL 15m and AAPL 4h)
    let expected_ts_first = UnixNanos::from(1_700_001_000_000_000_000_u64);
    assert_eq!(bars_from_batch[0].ts_event, expected_ts_first);
    assert_eq!(bars_from_batch[0].ts_init, expected_ts_first);
    assert_eq!(bars_from_batch[1].ts_event, expected_ts_first);

    // Verify last bar is NVDA 1w at ts=1_700_005_000s with Adjusted prices
    let last_adj = &bars_from_batch[4];
    assert_eq!(
        last_adj.bar_type,
        parse_datainfra_bar_type("NVDA", "NASDAQ", "1w").unwrap()
    );
    assert_eq!(
        last_adj.ts_event,
        UnixNanos::from(1_700_005_000_000_000_000_u64)
    );
    assert_eq!(last_adj.open, Price::new(480.25, 2));
    assert_eq!(last_adj.high, Price::new(495.50, 2));
    assert_eq!(last_adj.low, Price::new(475.00, 2));
    assert_eq!(last_adj.close, Price::new(490.75, 2));
    assert_eq!(last_adj.volume, Quantity::new(45_000_000.0, 0));

    // --- B. Raw Mode ---
    let raw_opts = ArrowDecodeOptions::new(
        "NASDAQ",
        DataInfraTimeframe::M15,
        2,
        0,
        PriceAdjustmentMode::Raw,
    );
    let raw_bars_ipc =
        decode_bars_arrow_stream(&ipc_bytes, &raw_opts).expect("decode raw IPC stream");
    let raw_bars_parquet =
        decode_bars_parquet(parquet_bytes, &raw_opts).expect("decode raw Parquet");
    assert_eq!(raw_bars_ipc, raw_bars_parquet);
    assert_eq!(raw_bars_ipc.len(), 5);

    let last_raw = &raw_bars_ipc[4];
    assert_eq!(
        last_raw.bar_type,
        parse_datainfra_bar_type("NVDA", "NASDAQ", "1w").unwrap()
    );
    assert_eq!(last_raw.open, Price::new(4802.50, 2));
    assert_eq!(last_raw.high, Price::new(4955.00, 2));
    assert_eq!(last_raw.low, Price::new(4750.00, 2));
    assert_eq!(last_raw.close, Price::new(4907.50, 2));
    assert_eq!(last_raw.volume, Quantity::new(4_500_000.0, 0));
}

#[rstest]
fn test_decode_v_quant_bars_low_cardinality_dictionary_and_uint8_flags() {
    // ClickHouse `LowCardinality(String)` encodes as Arrow `DictionaryArray`, and booleans
    // in ClickHouse views are frequently `UInt8` (0 or 1). Verify seamless decoding.
    let dict_values = StringArray::from(vec!["AAPL", "MSFT"]);
    let dict_keys = Int8Array::from(vec![0_i8, 1_i8, 0_i8]);
    let sym_dict = DictionaryArray::<Int8Type>::try_new(dict_keys, Arc::new(dict_values))
        .expect("valid DictionaryArray<Int8Type>");

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "symbol",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("bar_start_time", DataType::UInt64, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::UInt64, false),
        Field::new("is_synthetic", DataType::UInt8, false),
        Field::new("is_suspect_spike", DataType::UInt8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(sym_dict),
            Arc::new(UInt64Array::from(vec![
                1_700_010_000_u64,
                1_700_010_900_u64,
                1_700_011_800_u64,
            ])),
            Arc::new(Float64Array::from(vec![190.1234, 380.5000, 191.0000])),
            Arc::new(Float64Array::from(vec![191.0000, 382.2500, 199.9999])),
            Arc::new(Float64Array::from(vec![189.5000, 379.7500, 190.5000])),
            Arc::new(Float64Array::from(vec![190.8765, 381.1250, 198.0000])),
            Arc::new(UInt64Array::from(vec![100_000_u64, 200_000_u64, 50_u64])),
            Arc::new(UInt8Array::from(vec![0_u8, 0_u8, 1_u8])), // 3rd row is synthetic
            Arc::new(UInt8Array::from(vec![0_u8, 0_u8, 0_u8])),
        ],
    )
    .expect("valid dictionary batch");

    let opts = ArrowDecodeOptions::new(
        "NASDAQ",
        DataInfraTimeframe::M15,
        4,
        0,
        PriceAdjustmentMode::Adjusted,
    );
    let bars = decode_bars_batches(&[batch], &opts).expect("decode dictionary batch");
    assert_eq!(bars.len(), 2, "synthetic UInt8=1 row must be filtered out");
    assert_eq!(
        bars[0].bar_type,
        parse_datainfra_bar_type("AAPL", "NASDAQ", "15m").unwrap()
    );
    assert_eq!(bars[0].open, Price::new(190.1234, 4));
    assert_eq!(bars[0].close, Price::new(190.8765, 4));
    assert_eq!(
        bars[1].bar_type,
        parse_datainfra_bar_type("MSFT", "NASDAQ", "15m").unwrap()
    );
    assert_eq!(bars[1].close, Price::new(381.1250, 4));
}

// ================================================================================================
// 2. Dirty-Row Filtering, Timestamp Alignment (`ts_init >= ts_event`) & Invariant Violations
// ================================================================================================

#[rstest]
fn test_decode_bars_filters_synthetic_and_suspect_spike_rows_and_clamps_ts_init() {
    let symbols = ["AAPL", "AAPL", "AAPL", "NVDA", "MSFT"];
    let timeframes = ["15m", "15m", "15m", "15m", "15m"];
    let ts_seconds = [
        1_700_100_000_i64, // Clean row 1 (ingest_ts_ms > ts_event)
        1_700_100_900_i64, // Dirty: is_synthetic = true
        1_700_101_800_i64, // Dirty: is_suspect_spike = true
        1_700_102_700_i64, // Dirty: both true
        1_700_103_600_i64, // Clean row 2 (ingest_ts_ms < ts_event -> clamped to ts_event)
    ];
    let adj_ohlcv = [
        (180.0, 181.0, 179.5, 180.5, 10_000.0),
        (180.5, 180.5, 180.5, 180.5, 0.0),      // Synthetic flat bar
        (180.5, 250.0, 180.0, 181.0, 500.0),    // Suspect spike bar
        (450.0, 999.0, 440.0, 455.0, 100.0),    // Both flags set
        (370.0, 375.0, 369.0, 374.0, 25_000.0), // Clean MSFT bar
    ];
    let is_synthetic = [false, true, false, true, false];
    let is_suspect_spike = [false, false, true, true, false];
    let ingest_ts_ms = [
        1_700_100_005_000_i64, // 5 seconds after ts_event
        1_700_100_900_000_i64,
        1_700_101_800_000_i64,
        1_700_102_700_000_i64,
        1_700_100_000_000_i64, // Earlier than 1_700_103_600s -> must clamp to ts_event
    ];

    let batch = build_v_quant_bars_record_batch(
        &symbols,
        &timeframes,
        &ts_seconds,
        &adj_ohlcv,
        &adj_ohlcv,
        &is_synthetic,
        &is_suspect_spike,
        Some(&ingest_ts_ms),
    );

    let opts = ArrowDecodeOptions::default();
    let bars = decode_bars_batches(&[batch], &opts).expect("decode and filter dirty bars");
    assert_eq!(bars.len(), 2, "all 3 dirty rows must be discarded");

    // Row 0: AAPL clean, ts_init preserved (> ts_event)
    assert_eq!(
        bars[0].ts_event,
        UnixNanos::from(1_700_100_000_000_000_000_u64)
    );
    assert_eq!(
        bars[0].ts_init,
        UnixNanos::from(1_700_100_005_000_000_000_u64)
    );
    assert!(bars[0].ts_init >= bars[0].ts_event);

    // Row 4: MSFT clean, ts_init clamped to ts_event
    assert_eq!(
        bars[1].ts_event,
        UnixNanos::from(1_700_103_600_000_000_000_u64)
    );
    assert_eq!(bars[1].ts_init, bars[1].ts_event);
}

#[rstest]
fn test_decode_bars_rejects_ohlc_invariant_violation_and_non_positive_prices() {
    // Case 1: High < Low
    let bad_ohlc_batch = build_v_quant_bars_record_batch(
        &["AAPL"],
        &["15m"],
        &[1_700_000_000],
        &[(185.0, 180.0, 184.0, 182.0, 1_000.0)], // high (180) < open (185) and < low (184)
        &[(185.0, 180.0, 184.0, 182.0, 1_000.0)],
        &[false],
        &[false],
        None,
    );
    let err = decode_bars_batches(&[bad_ohlc_batch], &ArrowDecodeOptions::default())
        .expect_err("OHLC violation must fail closed");
    assert!(matches!(err, DataInfraError::ProvenanceViolation(_)));

    // Case 2: Zero / negative price
    let zero_px_batch = build_v_quant_bars_record_batch(
        &["NVDA"],
        &["1h"],
        &[1_700_000_000],
        &[(0.0, 100.0, 0.0, 50.0, 1_000.0)],
        &[(0.0, 100.0, 0.0, 50.0, 1_000.0)],
        &[false],
        &[false],
        None,
    );
    let err_zero = decode_bars_batches(&[zero_px_batch], &ArrowDecodeOptions::default())
        .expect_err("zero price must fail closed");
    assert!(matches!(err_zero, DataInfraError::ProvenanceViolation(_)));
}

#[rstest]
fn test_timestamp_and_fixed_point_conversion_helpers() {
    // Seconds -> Nanos
    assert_eq!(
        normalize_epoch_to_unix_nanos(1_700_000_000, "ts").unwrap(),
        UnixNanos::from(1_700_000_000_000_000_000_u64)
    );
    // Milliseconds -> Nanos
    assert_eq!(
        normalize_epoch_to_unix_nanos(1_700_000_000_123, "ts").unwrap(),
        UnixNanos::from(1_700_000_000_123_000_000_u64)
    );
    // Microseconds -> Nanos
    assert_eq!(
        normalize_epoch_to_unix_nanos(1_700_000_000_123_456, "ts").unwrap(),
        UnixNanos::from(1_700_000_000_123_456_000_u64)
    );
    // Nanoseconds -> Nanos
    assert_eq!(
        normalize_epoch_to_unix_nanos(1_700_000_000_123_456_789, "ts").unwrap(),
        UnixNanos::from(1_700_000_000_123_456_789_u64)
    );
    // Zero or negative -> ProvenanceViolation
    assert!(matches!(
        normalize_epoch_to_unix_nanos(0, "ts"),
        Err(DataInfraError::ProvenanceViolation(_))
    ));
    assert!(matches!(
        normalize_epoch_to_unix_nanos(-500, "ts"),
        Err(DataInfraError::ProvenanceViolation(_))
    ));

    // Fixed-point rounding & validation (0.125 = 1/8 and 0.375 = 3/8 are exact in IEEE-754 f64)
    assert_eq!(
        f64_to_price(185.125, 2, "open").unwrap(),
        Price::new(185.12, 2) // MidpointNearestEven rounds .125 to even .12
    );
    assert_eq!(
        f64_to_price(185.375, 2, "open").unwrap(),
        Price::new(185.38, 2) // MidpointNearestEven rounds .375 to even .38
    );
    assert!(f64_to_price(f64::NAN, 2, "open").is_err());
    assert!(f64_to_price(f64::INFINITY, 2, "open").is_err());
    assert!(f64_to_quantity(-1.0, 0, "volume").is_err());
    assert_eq!(
        f64_to_quantity(0.0, 0, "volume").unwrap(),
        Quantity::new(0.0, 0)
    );
}

// ================================================================================================
// 3. `sip_raw_quotes` & `sip_trading_statuses` Arrow Stream Decoding
// ================================================================================================

#[rstest]
fn test_decode_sip_raw_quotes_arrow_stream_and_reject_crossed_quotes() {
    let batch = build_quotes_record_batch(
        &["NVDA", "AAPL", "MSFT"],
        &[
            1_700_000_002_000_000_000_i64,
            1_700_000_001_000_000_000_i64,
            1_700_000_001_500_000_000_i64,
        ],
        &[490.10, 185.00, 370.50],
        &[490.20, 185.05, 370.50], // MSFT is locked (bid == ask), which is valid
        &[400.0, 1_200.0, 800.0],
        &[500.0, 900.0, 600.0],
    );

    let ipc_bytes = encode_batches_to_arrow_ipc_stream(std::slice::from_ref(&batch));
    let opts = ArrowDecodeOptions::default();

    let quotes = decode_quotes_arrow_stream(&ipc_bytes, &opts).expect("decode valid quotes stream");
    assert_eq!(quotes.len(), 3);

    // Chronological order: AAPL (1.0s) -> MSFT (1.5s) -> NVDA (2.0s)
    assert_eq!(
        quotes[0].instrument_id,
        to_instrument_id("AAPL", "NASDAQ").unwrap()
    );
    assert_eq!(quotes[0].bid_price, Price::new(185.00, 2));
    assert_eq!(quotes[0].ask_price, Price::new(185.05, 2));
    assert_eq!(quotes[0].bid_size, Quantity::new(1_200.0, 0));
    assert_eq!(quotes[0].ask_size, Quantity::new(900.0, 0));

    assert_eq!(
        quotes[1].instrument_id,
        to_instrument_id("MSFT", "NASDAQ").unwrap()
    );
    assert_eq!(quotes[1].bid_price, quotes[1].ask_price);

    assert_eq!(
        quotes[2].instrument_id,
        to_instrument_id("NVDA", "NASDAQ").unwrap()
    );

    // Crossed quote (`bid_price > ask_price`) must fail closed with ProvenanceViolation
    let crossed_batch = build_quotes_record_batch(
        &["AAPL"],
        &[1_700_000_003_000_000_000_i64],
        &[185.50],
        &[185.40],
        &[100.0],
        &[100.0],
    );
    let crossed_ipc = encode_batches_to_arrow_ipc_stream(&[crossed_batch]);
    let err = decode_quotes_arrow_stream(&crossed_ipc, &opts)
        .expect_err("crossed quote must be rejected");
    assert!(
        matches!(err, DataInfraError::ProvenanceViolation(ref msg) if msg.contains("crossed quote")),
        "expected crossed quote ProvenanceViolation, got: {err:?}"
    );
}

#[rstest]
fn test_decode_sip_trading_statuses_arrow_stream_all_canonical_states() {
    let batch = build_statuses_record_batch(
        &["NVDA", "AAPL", "MSFT", "AAPL", "NVDA", "MSFT"],
        &[
            1_700_000_004_000_000_000_i64,
            1_700_000_001_000_000_000_i64,
            1_700_000_002_000_000_000_i64,
            1_700_000_003_000_000_000_i64,
            1_700_000_000_500_000_000_i64,
            1_700_000_005_000_000_000_i64,
        ],
        &[
            "SHORT_SALE_RESTRICTED",
            "TRADING",
            "HALTED",
            "LULD_PAUSE",
            "PRE_OPEN",
            "CLOSED",
        ],
        &[
            Some("SSR_TRIGGERED"),
            None,
            Some("T1_NEWS_PENDING"),
            Some("LULD_BAND"),
            Some("   "), // Blank reason -> normalized to None
            None,
        ],
    );

    let ipc_bytes = encode_batches_to_arrow_ipc_stream(&[batch]);
    let opts = ArrowDecodeOptions::default();

    let statuses =
        decode_statuses_arrow_stream(&ipc_bytes, &opts).expect("decode valid statuses stream");
    assert_eq!(statuses.len(), 6);

    // Sorted order:
    // 0: NVDA @ 0.5s -> PRE_OPEN
    assert_eq!(statuses[0].action, MarketStatusAction::PreOpen);
    assert_eq!(statuses[0].is_trading, Some(false));
    assert_eq!(statuses[0].is_short_sell_restricted, Some(false));
    assert_eq!(statuses[0].reason, None);

    // 1: AAPL @ 1.0s -> TRADING
    assert_eq!(statuses[1].action, MarketStatusAction::Trading);
    assert_eq!(statuses[1].is_trading, Some(true));
    assert_eq!(statuses[1].is_short_sell_restricted, Some(false));
    assert_eq!(statuses[1].reason, None);

    // 2: MSFT @ 2.0s -> HALTED
    assert_eq!(statuses[2].action, MarketStatusAction::Halt);
    assert_eq!(statuses[2].is_trading, Some(false));
    assert_eq!(statuses[2].reason.map(|u| u.as_str()), Some("T1_NEWS_PENDING"));

    // 3: AAPL @ 3.0s -> LULD_PAUSE
    assert_eq!(statuses[3].action, MarketStatusAction::Pause);
    assert_eq!(statuses[3].is_trading, Some(false));
    assert_eq!(statuses[3].reason.map(|u| u.as_str()), Some("LULD_BAND"));

    // 4: NVDA @ 4.0s -> SHORT_SALE_RESTRICTED
    assert_eq!(statuses[4].action, MarketStatusAction::Trading);
    assert_eq!(statuses[4].is_trading, Some(true));
    assert_eq!(statuses[4].is_short_sell_restricted, Some(true));
    assert_eq!(statuses[4].reason.map(|u| u.as_str()), Some("SSR_TRIGGERED"));

    // 5: MSFT @ 5.0s -> CLOSED
    assert_eq!(statuses[5].action, MarketStatusAction::Close);
    assert_eq!(statuses[5].is_trading, Some(false));

    // Unknown status state -> ProvenanceViolation
    assert!(matches!(
        map_trading_status_state("UNKNOWN_BOGUS_STATE"),
        Err(DataInfraError::ProvenanceViolation(_))
    ));
}

#[rstest]
fn test_decode_empty_or_corrupted_arrow_and_parquet_buffers_fail_closed() {
    assert!(matches!(
        decode_arrow_ipc_stream(&[]),
        Err(DataInfraError::Transport(_))
    ));
    assert!(matches!(
        decode_arrow_ipc_stream(b"not-an-arrow-ipc-stream"),
        Err(DataInfraError::Transport(_))
    ));
    assert!(matches!(
        decode_parquet_bytes(Bytes::new()),
        Err(DataInfraError::Transport(_))
    ));
    assert!(matches!(
        decode_parquet_bytes(Bytes::from_static(b"not-a-parquet-file")),
        Err(DataInfraError::Transport(_))
    ));

    // Also verify missing required column in RecordBatch fails closed
    let bad_schema = Arc::new(Schema::new(vec![Field::new(
        "symbol",
        DataType::Utf8,
        false,
    )]));
    let incomplete_batch = RecordBatch::try_new(
        bad_schema,
        vec![Arc::new(StringArray::from(vec!["AAPL"]))],
    )
    .unwrap();
    assert!(matches!(
        decode_bars_batches(&[incomplete_batch.clone()], &ArrowDecodeOptions::default()),
        Err(DataInfraError::Transport(_))
    ));
    assert!(matches!(
        decode_quotes_batches(&[incomplete_batch.clone()], &ArrowDecodeOptions::default()),
        Err(DataInfraError::Transport(_))
    ));
    assert!(matches!(
        decode_statuses_batches(&[incomplete_batch], &ArrowDecodeOptions::default()),
        Err(DataInfraError::Transport(_))
    ));
}

// ================================================================================================
// 4. Parameterized ClickHouse SQL Query Builder & Request Validation Tests
// ================================================================================================

#[rstest]
fn test_clickhouse_parameterized_query_builders_and_request_validation() {
    let start_ns = UnixNanos::from(1_700_000_000_000_000_000_u64);
    let end_ns = UnixNanos::from(1_700_086_400_000_000_000_u64);

    let bar_req = BarsQueryRequest::new(
        vec!["aapl".to_string(), "NVDA".to_string()],
        DataInfraTimeframe::H4,
        "NASDAQ",
        PriceAdjustmentMode::Raw,
    )
    .with_time_range(Some(start_ns), Some(end_ns))
    .with_limit_per_symbol(500)
    .with_precisions(4, 2);

    let (sql, params) = build_v_quant_bars_query("market_data", &bar_req).expect("build bar query");
    assert!(sql.contains("FROM market_data.v_quant_bars"));
    assert!(sql.contains("raw_open, raw_high, raw_low, raw_close, raw_volume"));
    assert!(sql.contains("LIMIT {per_symbol_limit:UInt32} BY symbol"));
    assert!(params.contains(&("tf".to_string(), "4h".to_string())));
    assert!(params.contains(&("symbols".to_string(), "['AAPL','NVDA']".to_string())));
    assert!(params.contains(&("session".to_string(), "REG".to_string())));
    assert!(params.contains(&("start_ts".to_string(), "1700000000".to_string())));
    assert!(params.contains(&("end_ts".to_string(), "1700086400".to_string())));
    assert!(params.contains(&("per_symbol_limit".to_string(), "500".to_string())));

    // Validation failures: empty symbols or start > end
    let empty_req = BarsQueryRequest::new(
        vec!["   ".to_string()],
        DataInfraTimeframe::M15,
        "NASDAQ",
        PriceAdjustmentMode::Adjusted,
    );
    assert!(matches!(
        empty_req.validate(),
        Err(DataInfraError::InvalidConfig(_))
    ));

    let inverted_range = BarsQueryRequest::new(
        vec!["AAPL".to_string()],
        DataInfraTimeframe::M15,
        "NASDAQ",
        PriceAdjustmentMode::Adjusted,
    )
    .with_time_range(Some(end_ns), Some(start_ns));
    assert!(matches!(
        inverted_range.validate(),
        Err(DataInfraError::InvalidConfig(_))
    ));

    // Quotes & Statuses query builders
    let mut quote_req = QuotesQueryRequest::new(vec!["MSFT".to_string()], "NASDAQ");
    quote_req.start_ts_ns = Some(start_ns);
    quote_req.end_ts_ns = Some(end_ns);
    quote_req.limit = Some(1_000);
    let (q_sql, q_params) =
        build_sip_quotes_query("market_data", &quote_req).expect("build quotes query");
    assert!(q_sql.contains("FROM market_data.sip_raw_quotes"));
    assert!(q_sql.contains("LIMIT {limit_rows:UInt32}"));
    assert!(q_params.contains(&("symbols".to_string(), "['MSFT']".to_string())));

    let mut status_req = StatusesQueryRequest::new(vec!["AAPL".to_string()], "NASDAQ");
    status_req.start_ts_ns = Some(start_ns);
    status_req.end_ts_ns = Some(end_ns);
    status_req.limit = Some(250);
    let (s_sql, s_params) =
        build_sip_statuses_query("market_data", &status_req).expect("build statuses query");
    assert!(s_sql.contains("FROM market_data.sip_trading_statuses"));
    assert!(s_params.contains(&("limit_rows".to_string(), "250".to_string())));
}

// ================================================================================================
// 5. Wire-Level `HttpClickHouseArrowTransport` Tests with Ephemeral TCP Peer
// ================================================================================================

async fn read_http_request(stream: &mut tokio::net::TcpStream) -> String {
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0_u8; 4096];
    let mut expected_total_len: Option<usize> = None;

    loop {
        let read_res = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut tmp)).await;
        let Ok(Ok(n)) = read_res else {
            break;
        };
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);

        if expected_total_len.is_none()
            && let Some(hdr_end) = buf.windows(4).position(|w| w == b"\r\n\r\n")
        {
            let body_start = hdr_end + 4;
            let headers_str = String::from_utf8_lossy(&buf[..hdr_end]).to_ascii_lowercase();
            let content_len = headers_str
                .lines()
                .find_map(|line| {
                    line.strip_prefix("content-length:")
                        .and_then(|v| v.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            expected_total_len = Some(body_start + content_len);
        }

        if let Some(total) = expected_total_len
            && buf.len() >= total
        {
            break;
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

async fn write_http_response(
    stream: &mut tokio::net::TcpStream,
    status_line: &str,
    content_type: &str,
    body: &[u8],
) {
    let header = format!(
        "HTTP/1.1 {status_line}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(header.as_bytes())
        .await
        .expect("write HTTP response headers");
    if !body.is_empty() {
        stream
            .write_all(body)
            .await
            .expect("write HTTP response body");
    }
    stream.flush().await.expect("flush HTTP response");
}

#[tokio::test]
async fn test_http_clickhouse_transport_fetch_bars_quotes_and_statuses_wire_roundtrip() {
    let bars_batch = build_v_quant_bars_record_batch(
        &["AAPL", "NVDA"],
        &["15m", "15m"],
        &[1_700_000_000, 1_700_000_900],
        &[
            (185.00, 186.50, 184.75, 186.25, 950_000.0),
            (490.00, 495.00, 488.50, 494.10, 1_800_000.0),
        ],
        &[
            (185.00, 186.50, 184.75, 186.25, 950_000.0),
            (490.00, 495.00, 488.50, 494.10, 1_800_000.0),
        ],
        &[false, false],
        &[false, false],
        None,
    );
    let bars_ipc = encode_batches_to_arrow_ipc_stream(&[bars_batch]);

    let quotes_batch = build_quotes_record_batch(
        &["AAPL"],
        &[1_700_000_001_000_000_000],
        &[186.20],
        &[186.25],
        &[500.0],
        &[700.0],
    );
    let quotes_ipc = encode_batches_to_arrow_ipc_stream(&[quotes_batch]);

    let statuses_batch = build_statuses_record_batch(
        &["NVDA"],
        &[1_700_000_002_000_000_000],
        &["TRADING"],
        &[None],
    );
    let statuses_ipc = encode_batches_to_arrow_ipc_stream(&[statuses_batch]);

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral TCP listener");
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        for _ in 0..3 {
            let (mut socket, _) = listener.accept().await.expect("accept client");
            let raw_req = read_http_request(&mut socket).await;

            // Verify ClickHouse HTTP POST contract & ArrowStream query params
            assert!(
                raw_req.starts_with("POST /?"),
                "expected POST request, got: {raw_req}"
            );
            assert!(raw_req.contains("database=market_data"));
            assert!(raw_req.contains("default_format=ArrowStream"));
            assert!(raw_req.contains("param_symbols="));

            if raw_req.contains("v_quant_bars") {
                assert!(raw_req.contains("param_tf=15m"));
                write_http_response(
                    &mut socket,
                    "200 OK",
                    "application/vnd.apache.arrow.stream",
                    &bars_ipc,
                )
                .await;
            } else if raw_req.contains("sip_raw_quotes") {
                write_http_response(
                    &mut socket,
                    "200 OK",
                    "application/vnd.apache.arrow.stream",
                    &quotes_ipc,
                )
                .await;
            } else if raw_req.contains("sip_trading_statuses") {
                write_http_response(
                    &mut socket,
                    "200 OK",
                    "application/vnd.apache.arrow.stream",
                    &statuses_ipc,
                )
                .await;
            } else {
                write_http_response(&mut socket, "400 Bad Request", "text/plain", b"Unknown SQL")
                    .await;
            }
        }
    });

    let cfg = transport_config_for_url(&format!("http://{addr}"), 0);
    let transport = HttpClickHouseArrowTransport::new(cfg).expect("create transport");
    assert_eq!(transport.database(), "market_data");

    // 1. fetch_bars
    let bar_req = BarsQueryRequest::new(
        vec!["AAPL".to_string(), "NVDA".to_string()],
        DataInfraTimeframe::M15,
        "NASDAQ",
        PriceAdjustmentMode::Adjusted,
    );
    let bars = transport
        .fetch_bars(&bar_req)
        .await
        .expect("fetch_bars over HTTP ArrowStream");
    assert_eq!(bars.len(), 2);
    assert_eq!(bars[0].close, Price::new(186.25, 2));
    assert_eq!(bars[1].close, Price::new(494.10, 2));

    // 2. fetch_quotes
    let quote_req = QuotesQueryRequest::new(vec!["AAPL".to_string()], "NASDAQ");
    let quotes = transport
        .fetch_quotes(&quote_req)
        .await
        .expect("fetch_quotes over HTTP ArrowStream");
    assert_eq!(quotes.len(), 1);
    assert_eq!(quotes[0].bid_price, Price::new(186.20, 2));
    assert_eq!(quotes[0].ask_price, Price::new(186.25, 2));

    // 3. fetch_statuses
    let status_req = StatusesQueryRequest::new(vec!["NVDA".to_string()], "NASDAQ");
    let statuses = transport
        .fetch_statuses(&status_req)
        .await
        .expect("fetch_statuses over HTTP ArrowStream");
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].action, MarketStatusAction::Trading);

    server_handle.await.expect("server task completed cleanly");
}

#[tokio::test]
async fn test_http_clickhouse_transport_fail_closed_on_400_500_empty_200_and_corrupted_stream() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral TCP listener");
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        // Request 1: HTTP 400 Bad Request
        let (mut s1, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut s1).await;
        write_http_response(
            &mut s1,
            "400 Bad Request",
            "text/plain",
            b"Code: 62. DB::Exception: Syntax error",
        )
        .await;

        // Request 2: HTTP 500 Internal Server Error
        let (mut s2, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut s2).await;
        write_http_response(
            &mut s2,
            "500 Internal Server Error",
            "text/plain",
            b"Code: 241. Memory limit exceeded",
        )
        .await;

        // Request 3: HTTP 200 OK with empty body
        let (mut s3, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut s3).await;
        write_http_response(
            &mut s3,
            "200 OK",
            "application/vnd.apache.arrow.stream",
            b"",
        )
        .await;

        // Request 4: HTTP 200 OK with corrupted Arrow IPC stream bytes
        let (mut s4, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut s4).await;
        write_http_response(
            &mut s4,
            "200 OK",
            "application/vnd.apache.arrow.stream",
            b"corrupted-arrow-ipc-payload-bytes",
        )
        .await;
    });

    let cfg = transport_config_for_url(&format!("http://{addr}"), 0);
    let transport = HttpClickHouseArrowTransport::new(cfg).expect("create transport");

    // Empty SQL fails closed before hitting the network
    let empty_sql_err = transport
        .query_arrow_stream("   ", &[])
        .await
        .expect_err("empty SQL must fail closed");
    assert!(matches!(empty_sql_err, DataInfraError::InvalidConfig(_)));

    let bar_req = BarsQueryRequest::new(
        vec!["AAPL".to_string()],
        DataInfraTimeframe::M15,
        "NASDAQ",
        PriceAdjustmentMode::Adjusted,
    );

    // 1. HTTP 400 -> Transport error containing HTTP 400
    let err_400 = transport
        .fetch_bars(&bar_req)
        .await
        .expect_err("HTTP 400 must fail closed");
    assert!(
        matches!(err_400, DataInfraError::Transport(ref msg) if msg.contains("HTTP 400")),
        "unexpected error for HTTP 400: {err_400:?}"
    );

    // 2. HTTP 500 -> Transport error containing HTTP 500
    let err_500 = transport
        .fetch_bars(&bar_req)
        .await
        .expect_err("HTTP 500 must fail closed");
    assert!(
        matches!(err_500, DataInfraError::Transport(ref msg) if msg.contains("HTTP 500")),
        "unexpected error for HTTP 500: {err_500:?}"
    );

    // 3. HTTP 200 empty body -> Transport error (fail-closed)
    let err_empty = transport
        .fetch_bars(&bar_req)
        .await
        .expect_err("empty HTTP 200 body must fail closed");
    assert!(
        matches!(err_empty, DataInfraError::Transport(ref msg) if msg.contains("empty ArrowStream body")),
        "unexpected error for empty HTTP 200: {err_empty:?}"
    );

    // 4. HTTP 200 corrupted Arrow IPC body -> Transport error
    let err_corrupt = transport
        .fetch_bars(&bar_req)
        .await
        .expect_err("corrupted Arrow IPC body must fail closed");
    assert!(
        matches!(err_corrupt, DataInfraError::Transport(ref msg) if msg.contains("StreamReader")),
        "unexpected error for corrupted Arrow IPC: {err_corrupt:?}"
    );

    server_handle.await.expect("server task completed cleanly");
}

#[tokio::test]
async fn test_http_clickhouse_transport_retries_transient_503_and_recovers() {
    let bars_batch = build_v_quant_bars_record_batch(
        &["MSFT"],
        &["1d"],
        &[1_700_000_000],
        &[(375.00, 378.50, 374.00, 377.25, 18_500_000.0)],
        &[(375.00, 378.50, 374.00, 377.25, 18_500_000.0)],
        &[false],
        &[false],
        None,
    );
    let bars_ipc = encode_batches_to_arrow_ipc_stream(&[bars_batch]);

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral TCP listener");
    let addr = listener.local_addr().unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_srv = Arc::clone(&attempts);

    let server_handle = tokio::spawn(async move {
        for i in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut socket).await;
            attempts_srv.fetch_add(1, Ordering::SeqCst);
            if i == 0 {
                write_http_response(
                    &mut socket,
                    "503 Service Unavailable",
                    "text/plain",
                    b"ClickHouse replica temporarily overloaded",
                )
                .await;
            } else {
                write_http_response(
                    &mut socket,
                    "200 OK",
                    "application/vnd.apache.arrow.stream",
                    &bars_ipc,
                )
                .await;
            }
        }
    });

    let cfg = transport_config_for_url(&format!("http://{addr}"), 2);
    let transport = HttpClickHouseArrowTransport::new(cfg).expect("create transport with retry");

    let req = BarsQueryRequest::new(
        vec!["MSFT".to_string()],
        DataInfraTimeframe::D1,
        "NASDAQ",
        PriceAdjustmentMode::Adjusted,
    );
    let bars = transport
        .fetch_bars(&req)
        .await
        .expect("retry after transient HTTP 503 should succeed");

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(bars.len(), 1);
    assert_eq!(
        bars[0].bar_type,
        parse_datainfra_bar_type("MSFT", "NASDAQ", "1d").unwrap()
    );
    assert_eq!(bars[0].close, Price::new(377.25, 2));

    server_handle.await.expect("server task completed cleanly");
}
