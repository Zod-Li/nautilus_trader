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

//! Integration tests for `DataInfraCatalog` (`CatalogReader` / `CatalogFactory`),
//! `CalendarSessionGate`, `DataInfraDataClient`, and `DataInfraDataClientFactory`
//! (Tickets #03 and #05).

#![allow(linker_messages)]

use std::{
    cell::RefCell,
    num::NonZeroUsize,
    rc::Rc,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow::{
    array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
        TimestampNanosecondArray,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    ipc::writer::StreamWriter,
};
use nautilus_common::{
    cache::{Cache, CacheView},
    clients::DataClient,
    clock::VirtualClock,
    factories::{ClientConfig, DataClientFactory},
    live::{runner::replace_data_event_sender, sender::EventSender},
    messages::{
        DataEvent, DataResponse,
        data::{
            RequestBars, RequestInstrument, RequestInstruments, RequestQuotes, SubscribeBars,
            SubscribeInstrument, SubscribeInstrumentStatus, SubscribeQuotes, UnsubscribeBars,
            UnsubscribeInstrumentStatus, UnsubscribeQuotes,
        },
    },
    msgbus::{
        self, ShareableMessageHandler, TypedHandler,
        switchboard::{get_bars_topic, get_instrument_status_topic, get_quotes_topic},
    },
};
use nautilus_core::{Params, UUID4, UnixNanos};
use nautilus_data::engine::{DataEngine, config::DataEngineConfig};
use nautilus_datainfra::{
    CalendarSessionGate, DATAINFRA, DATAINFRA_CATALOG_FACTORY_NAME, DataInfraCatalog,
    DataInfraConfig, DataInfraDataClient, DataInfraDataClientFactory, DataInfraEquityMetadata,
    DataInfraError, HttpClickHouseArrowTransport, StreamDecodeOutcome, StreamLifecycleListener,
    TOPIC_SIP_BARS_1M, TOPIC_SIP_QUOTES, TOPIC_SIP_STATUSES, parse_datainfra_bar_type,
    to_instrument_id,
};
use nautilus_model::{
    data::{
        Bar, BarType, Data, DataBatch, InstrumentStatus, NautilusDataType, NautilusRecordType,
        QuoteTick,
    },
    enums::MarketStatusAction,
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
    types::Price,
};
use nautilus_persistence::{
    catalog::{
        factory::{CatalogConnectConfig, CatalogFactoryRegistry, create_catalog},
        traits::{CatalogReader, CatalogWriter},
        types::{CatalogInstrumentQuery, CatalogQuery},
    },
    config::CatalogBackendType,
};
use parking_lot::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

// ================================================================================================
// Test Helpers: Ephemeral HTTP Server & Arrow IPC Stream Builders
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

fn make_test_config(clickhouse_url: &str, calendar_url: &str, venue: &str) -> DataInfraConfig {
    DataInfraConfig::new_checked(
        clickhouse_url,
        "market_data",
        2_000,
        0,
        vec!["127.0.0.1:9092".to_string()],
        "nautilus-datainfra-test-group",
        calendar_url,
        venue,
    )
    .expect("valid DataInfraConfig")
}

// ================================================================================================
// 1. Ticket #03: DataInfraCatalog (`CatalogReader`, `CatalogWriter`, `CatalogFactory`, & Engine)
// ================================================================================================

#[test]
fn test_datainfra_catalog_instruments_registration_and_writer_contracts() {
    let cfg = make_test_config("http://127.0.0.1:8123", "http://127.0.0.1:8012", "ALPACA");
    let mut catalog = DataInfraCatalog::from_config(cfg).expect("create DataInfraCatalog");

    // Initially no registered instruments when querying `instruments(None)`
    let empty_all = catalog
        .instruments(&CatalogInstrumentQuery::new())
        .expect("query all instruments on empty catalog");
    assert!(empty_all.is_empty());

    // Register AAPL.ALPACA explicitly via `register_equity`
    let aapl_meta = DataInfraEquityMetadata::new("AAPL", "ALPACA").with_lot_size(1);
    let aapl_inst = catalog
        .register_equity(&aapl_meta, UnixNanos::from(1_700_000_000_000_000_000_u64))
        .expect("register AAPL.ALPACA");
    assert_eq!(aapl_inst.id().to_string(), "AAPL.ALPACA");

    // Register MSFT.ALPACA via `CatalogWriter::write_instruments`
    let msft_meta = DataInfraEquityMetadata::new("MSFT", "ALPACA");
    let msft_inst = InstrumentAny::Equity(
        msft_meta
            .to_equity_any(UnixNanos::from(1_700_000_000_000_000_000_u64))
            .unwrap(),
    );
    catalog
        .write_instruments(std::slice::from_ref(&msft_inst))
        .expect("write_instruments should register into catalog");

    // Querying `instruments(None)` returns both registered instruments
    let all_registered = catalog
        .instruments(&CatalogInstrumentQuery::new())
        .expect("query all registered instruments");
    assert_eq!(all_registered.len(), 2);

    // Querying `instruments(Some(&["AAPL.ALPACA", "NVDA"]))` returns existing AAPL.ALPACA
    // and dynamically synthesizes NVDA.ALPACA using `default_venue`
    let filtered = catalog
        .instruments(
            &CatalogInstrumentQuery::new().with_instrument_ids(Some(vec![
                "AAPL.ALPACA".to_string(),
                "NVDA".to_string(),
            ])),
        )
        .expect("query specific instruments");
    assert_eq!(filtered.len(), 2);
    assert_eq!(filtered[0].id().to_string(), "AAPL.ALPACA");
    assert_eq!(filtered[1].id().to_string(), "NVDA.ALPACA");

    // Read-only catalog writer methods must fail closed
    let write_data_err = catalog
        .write_data(&[], None, None, None)
        .expect_err("write_data must fail closed on read-only ClickHouse catalog");
    assert!(write_data_err.to_string().contains("read-only"));

    let write_records_err = catalog
        .write_records(NautilusRecordType::OrderFilled, &[], None)
        .expect_err("write_records must fail closed on read-only ClickHouse catalog");
    assert!(write_records_err.to_string().contains("read-only"));

    catalog
        .record_empty_coverage(
            NautilusDataType::Bar,
            Some("AAPL.ALPACA"),
            UnixNanos::from(100_u64),
            UnixNanos::from(200_u64),
        )
        .expect("record_empty_coverage is a no-op and succeeds");
}

#[test]
fn test_datainfra_catalog_factory_registry_and_param_validation() {
    let mut registry = CatalogFactoryRegistry::new();
    registry.insert(
        DATAINFRA_CATALOG_FACTORY_NAME.to_string(),
        DataInfraCatalog::catalog_factory(),
    );

    let mut map = indexmap::IndexMap::new();
    map.insert(
        "database".to_string(),
        serde_json::Value::String("quant_ch".to_string()),
    );
    map.insert(
        "default_venue".to_string(),
        serde_json::Value::String("ALPACA".to_string()),
    );
    map.insert(
        "timeframe".to_string(),
        serde_json::Value::String("1d".to_string()),
    );
    map.insert(
        "adjustment".to_string(),
        serde_json::Value::String("raw".to_string()),
    );
    map.insert(
        "market_session".to_string(),
        serde_json::Value::String("ALL".to_string()),
    );
    let params = Params::new(map);

    let mut connect_cfg = CatalogConnectConfig::new("http://127.0.0.1:8123", None);
    connect_cfg.params = Some(params);

    let backend = CatalogBackendType::External(DATAINFRA_CATALOG_FACTORY_NAME.to_string());
    let mut catalog =
        create_catalog(&backend, &connect_cfg, &registry).expect("create DataInfra catalog");

    let insts = catalog
        .instruments(
            &CatalogInstrumentQuery::new().with_instrument_ids(Some(vec!["AAPL".to_string()])),
        )
        .expect("resolve AAPL with default_venue=ALPACA");
    assert_eq!(insts.len(), 1);
    assert_eq!(insts[0].id().to_string(), "AAPL.ALPACA");

    // Invalid adjustment param fails closed
    let mut bad_map = indexmap::IndexMap::new();
    bad_map.insert(
        "adjustment".to_string(),
        serde_json::Value::String("invalid_mode".to_string()),
    );
    let mut bad_connect = CatalogConnectConfig::new("http://127.0.0.1:8123", None);
    bad_connect.params = Some(Params::new(bad_map));
    assert!(
        create_catalog(&backend, &bad_connect, &registry).is_err(),
        "invalid adjustment mode must fail closed"
    );

    // Unknown parameter key fails closed via `validate_catalog_params`
    let mut unknown_map = indexmap::IndexMap::new();
    unknown_map.insert(
        "unexpected_param".to_string(),
        serde_json::Value::String("val".to_string()),
    );
    let mut unknown_connect = CatalogConnectConfig::new("http://127.0.0.1:8123", None);
    unknown_connect.params = Some(Params::new(unknown_map));
    assert!(
        create_catalog(&backend, &unknown_connect, &registry).is_err(),
        "unknown catalog param must fail closed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_datainfra_catalog_reader_query_batch_session_missing_intervals_and_data_engine_replay()
 {
    // Out-of-order 15m bars for AAPL across [1_700_000_000, 1_700_002_700] (4 bars)
    let bars_batch = build_v_quant_bars_record_batch(
        &["AAPL", "AAPL", "AAPL", "AAPL"],
        &["15m", "15m", "15m", "15m"],
        &[1_700_001_800, 1_700_000_000, 1_700_002_700, 1_700_000_900],
        &[
            (186.00, 187.20, 185.80, 187.00, 310_000.0),
            (185.00, 185.90, 184.50, 185.50, 250_000.0),
            (187.00, 188.10, 186.90, 187.85, 420_000.0),
            (185.50, 186.40, 185.20, 186.00, 280_000.0),
        ],
        &[
            (186.00, 187.20, 185.80, 187.00, 310_000.0),
            (185.00, 185.90, 184.50, 185.50, 250_000.0),
            (187.00, 188.10, 186.90, 187.85, 420_000.0),
            (185.50, 186.40, 185.20, 186.00, 280_000.0),
        ],
        &[false, false, false, false],
        &[false, false, false, false],
        None,
    );
    let bars_ipc = encode_batches_to_arrow_ipc_stream(&[bars_batch]);

    let quotes_batch = build_quotes_record_batch(
        &["AAPL", "AAPL"],
        &[1_700_000_500_000_000_000, 1_700_000_100_000_000_000],
        &[185.55, 185.10],
        &[185.60, 185.15],
        &[400.0, 200.0],
        &[600.0, 300.0],
    );
    let quotes_ipc = encode_batches_to_arrow_ipc_stream(&[quotes_batch]);

    let statuses_batch = build_statuses_record_batch(
        &["AAPL"],
        &[1_700_000_050_000_000_000],
        &["TRADING"],
        &[None],
    );
    let statuses_ipc = encode_batches_to_arrow_ipc_stream(&[statuses_batch]);

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral ClickHouse TCP listener");
    let ch_addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let raw_req = read_http_request(&mut socket).await;
            if raw_req.is_empty() {
                continue;
            }
            if raw_req.contains("v_quant_bars") {
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

    let cfg = make_test_config(
        &format!("http://{ch_addr}"),
        "http://127.0.0.1:8012",
        "ALPACA",
    );
    let mut catalog = DataInfraCatalog::from_config(cfg).expect("create DataInfraCatalog");

    let start_ns = UnixNanos::from(1_699_999_000_000_000_000_u64);
    let end_ns = UnixNanos::from(1_700_003_000_000_000_000_u64);

    // 1. `query_batch` for Bars with full BarType identifier `"AAPL.ALPACA-15-MINUTE-LAST-EXTERNAL"`
    let bar_query = CatalogQuery::new(NautilusDataType::Bar)
        .with_identifiers(Some(vec!["AAPL.ALPACA-15-MINUTE-LAST-EXTERNAL".to_string()]))
        .with_range(Some(start_ns), Some(end_ns));
    let batch = catalog.query_batch(&bar_query).expect("query_batch bars");
    let bars_vec: Vec<Bar> = match batch {
        DataBatch::Bar(bars) => bars.to_vec(),
        other => panic!("expected DataBatch::Bar, got {other:?}"),
    };
    assert_eq!(bars_vec.len(), 4);
    // Verify chronological ordering even though Arrow batch had scrambled rows
    let expected_ts: Vec<u64> = vec![
        1_700_000_000_000_000_000,
        1_700_000_900_000_000_000,
        1_700_001_800_000_000_000,
        1_700_002_700_000_000_000,
    ];
    assert_eq!(
        bars_vec.iter().map(|b| b.ts_init.as_u64()).collect::<Vec<_>>(),
        expected_ts
    );

    // 2. `query_batch_session` with `chunk_size = Some(2)` (streaming chunks)
    let mut session = catalog
        .query_batch_session(&bar_query, Some(2))
        .expect("query_batch_session with chunk_size=2");
    let mut chunk_lengths = Vec::new();
    let mut streamed_bars = Vec::new();
    while let Some(chunk) = session.next_batch().expect("next_batch") {
        if let DataBatch::Bar(b) = chunk {
            chunk_lengths.push(b.len());
            streamed_bars.extend(b.to_vec());
        } else {
            panic!("expected DataBatch::Bar chunk");
        }
    }
    assert_eq!(chunk_lengths, vec![2, 2]);
    assert_eq!(streamed_bars, bars_vec);

    // 3. `query_last_timestamp`
    let last_ts = catalog
        .query_last_timestamp(
            NautilusDataType::Bar,
            Some("AAPL.ALPACA-15-MINUTE-LAST-EXTERNAL"),
        )
        .expect("query_last_timestamp");
    assert_eq!(last_ts, Some(1_700_002_700_000_000_000_u64));

    // 4. `get_missing_intervals_for_request`
    let missing = catalog
        .get_missing_intervals_for_request(
            start_ns,
            end_ns,
            NautilusDataType::Bar,
            Some("AAPL.ALPACA-15-MINUTE-LAST-EXTERNAL"),
        )
        .expect("get_missing_intervals_for_request");
    assert_eq!(
        missing,
        vec![
            (
                1_699_999_000_000_000_000_u64,
                1_700_000_000_000_000_000_u64 - 1
            ),
            (
                1_700_002_700_000_000_000_u64 + 1,
                1_700_003_000_000_000_000_u64
            ),
        ]
    );

    // 5. `query_batch` for QuoteTick & InstrumentStatus
    let quote_query = CatalogQuery::new(NautilusDataType::QuoteTick)
        .with_identifiers(Some(vec!["AAPL.ALPACA".to_string()]))
        .with_range(Some(start_ns), Some(end_ns));
    let quotes_vec = match catalog.query_batch(&quote_query).expect("query quotes") {
        DataBatch::Quote(q) => q.to_vec(),
        other => panic!("expected DataBatch::Quote, got {other:?}"),
    };
    assert_eq!(quotes_vec.len(), 2);
    assert!(quotes_vec[0].ts_init < quotes_vec[1].ts_init);

    let status_query = CatalogQuery::new(NautilusDataType::InstrumentStatus)
        .with_identifiers(Some(vec!["AAPL.ALPACA".to_string()]))
        .with_range(Some(start_ns), Some(end_ns));
    let statuses_vec = match catalog.query_batch(&status_query).expect("query statuses") {
        DataBatch::InstrumentStatus(s) => s.to_vec(),
        other => panic!("expected DataBatch::InstrumentStatus, got {other:?}"),
    };
    assert_eq!(statuses_vec.len(), 1);
    assert_eq!(statuses_vec[0].action, MarketStatusAction::Trading);

    // 6. Fail-closed checks on `CatalogReader`: inverted range & unsupported data type
    let inverted_query = CatalogQuery::new(NautilusDataType::Bar)
        .with_identifiers(Some(vec!["AAPL.ALPACA-15-MINUTE-LAST-EXTERNAL".to_string()]))
        .with_range(Some(end_ns), Some(start_ns));
    assert!(catalog.query_batch(&inverted_query).is_err());

    let unsupported_query = CatalogQuery::new(NautilusDataType::TradeTick)
        .with_identifiers(Some(vec!["AAPL.ALPACA".to_string()]));
    assert!(catalog.query_batch(&unsupported_query).is_err());

    // 7. Replay the catalog-loaded Instrument, Bars, Quotes, and Statuses through `DataEngine`
    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    let cache = Rc::new(RefCell::new(Cache::default()));
    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), Some(DataEngineConfig::default()));

    let aapl_inst = catalog
        .register_equity(
            &DataInfraEquityMetadata::new("AAPL", "ALPACA"),
            UnixNanos::from(1_699_999_000_000_000_000_u64),
        )
        .expect("register AAPL.ALPACA");
    data_engine.process(&aapl_inst);

    let bar_type = BarType::from_str("AAPL.ALPACA-15-MINUTE-LAST-EXTERNAL").unwrap();
    let observed_bars: Rc<RefCell<Vec<Bar>>> = Rc::new(RefCell::new(Vec::new()));
    let observed_bars_clone = Rc::clone(&observed_bars);
    let bar_handler = TypedHandler::from(move |bar: &Bar| {
        observed_bars_clone.borrow_mut().push(*bar);
    });
    msgbus::subscribe_bars(get_bars_topic(bar_type).into(), bar_handler, None);

    for status in &statuses_vec {
        data_engine.process(status);
    }
    for quote in &quotes_vec {
        data_engine.process_data(Data::Quote(*quote));
    }
    for bar in &bars_vec {
        data_engine.process_data(Data::Bar(*bar));
    }

    let delivered = observed_bars.borrow();
    assert_eq!(delivered.len(), 4);
    assert_eq!(delivered.as_slice(), bars_vec.as_slice());
    assert_eq!(cache.borrow().bars(&bar_type).unwrap().len(), 4);
    assert_eq!(
        cache
            .borrow()
            .quote(&InstrumentId::from("AAPL.ALPACA"))
            .unwrap()
            .bid_price,
        Price::new(185.55, 2)
    );

    server_handle.abort();
}

// ================================================================================================
// 2. Ticket #05: CalendarSessionGate (RTH Open, Early Close 13:00 ET, Closed Day, Fail-Closed)
// ================================================================================================

#[tokio::test]
async fn test_calendar_session_gate_rth_open_early_close_holiday_and_fail_closed() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral calendar-service TCP listener");
    let cal_addr = listener.local_addr().unwrap();

    // Serve 5 sequential responses:
    // 1. Normal RTH open day (2026-05-04: 09:30 ET = 1_777_896_600, 16:00 ET = 1_777_920_000)
    // 2. Early-close half day (2026-07-03: 09:30 ET = 1_783_080_600, 13:00 ET = 1_783_093_200)
    // 3. Closed holiday/weekend (2026-07-04: is_trading_day = false, rth_open_utc = 0, rth_close_utc = 0)
    // 4. UNSYNCED source -> fail-closed rejection
    // 5. HTTP 503 error -> fail-closed rejection
    let server_handle = tokio::spawn(async move {
        // Request 1: Normal RTH open day
        let (mut s1, _) = listener.accept().await.unwrap();
        let req1 = read_http_request(&mut s1).await;
        assert!(req1.contains("/api/v1/calendar/session-status"));
        assert!(req1.contains("market=US_EQUITY"));
        write_http_response(
            &mut s1,
            "200 OK",
            "application/json",
            br#"{
                "market": "US_EQUITY",
                "queried_at_utc": 1777900000,
                "trading_date": "2026-05-04",
                "is_trading_day": true,
                "is_rth_open": true,
                "rth_open_utc": 1777896600,
                "rth_close_utc": 1777920000,
                "is_early_close": false,
                "next_rth_open_utc": 1777983000,
                "source": "EXCHANGE_CALENDAR"
            }"#,
        )
        .await;

        // Request 2: Early Close half-day (09:30–13:00 ET)
        let (mut s2, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut s2).await;
        write_http_response(
            &mut s2,
            "200 OK",
            "application/json",
            br#"{
                "market": "US_EQUITY",
                "queried_at_utc": 1783085000,
                "trading_date": "2026-07-03",
                "is_trading_day": true,
                "is_rth_open": true,
                "rth_open_utc": 1783080600,
                "rth_close_utc": 1783093200,
                "is_early_close": true,
                "next_rth_open_utc": 1783339800,
                "source": "ALPACA"
            }"#,
        )
        .await;

        // Request 3: Closed holiday
        let (mut s3, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut s3).await;
        write_http_response(
            &mut s3,
            "200 OK",
            "application/json",
            br#"{
                "market": "US_EQUITY",
                "queried_at_utc": 1783170000,
                "trading_date": "2026-07-04",
                "is_trading_day": false,
                "is_rth_open": false,
                "rth_open_utc": 0,
                "rth_close_utc": 0,
                "is_early_close": false,
                "next_rth_open_utc": 1783339800,
                "source": "EXCHANGE_CALENDAR"
            }"#,
        )
        .await;

        // Request 4: UNSYNCED source
        let (mut s4, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut s4).await;
        write_http_response(
            &mut s4,
            "200 OK",
            "application/json",
            br#"{
                "market": "US_EQUITY",
                "queried_at_utc": 1783170000,
                "trading_date": "2026-07-06",
                "is_trading_day": true,
                "is_rth_open": true,
                "rth_open_utc": 1783339800,
                "rth_close_utc": 1783363200,
                "is_early_close": false,
                "next_rth_open_utc": null,
                "source": "UNSYNCED"
            }"#,
        )
        .await;

        // Request 5: HTTP 503 Service Unavailable
        let (mut s5, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut s5).await;
        write_http_response(
            &mut s5,
            "503 Service Unavailable",
            "text/plain",
            b"calendar database offline",
        )
        .await;
    });

    let cfg = make_test_config(
        "http://127.0.0.1:8123",
        &format!("http://{cal_addr}"),
        "ALPACA",
    );
    let gate = CalendarSessionGate::from_config(&cfg).expect("create CalendarSessionGate");

    // Before first refresh, `is_within_cached_rth` must fail closed
    assert!(
        gate.is_within_cached_rth(UnixNanos::from(1_777_900_000_000_000_000_u64))
            .is_err(),
        "uncached CalendarSessionGate must fail closed"
    );

    // 1. Normal RTH open day (09:30–16:00 ET)
    let status_rth = gate
        .refresh_session_status(Some(1_777_900_000))
        .await
        .expect("refresh normal RTH session status");
    assert!(status_rth.is_trading_day);
    assert!(status_rth.is_rth_open);
    assert!(!status_rth.is_early_close);
    assert!(
        gate.is_within_cached_rth(UnixNanos::from(1_777_900_000_000_000_000_u64))
            .unwrap()
    );
    assert!(
        !gate
            .is_within_cached_rth(UnixNanos::from(1_777_896_000_000_000_000_u64))
            .unwrap()
    );

    // 2. Early Close half-day (09:30–13:00 ET: close = 1_783_093_200 UTC)
    let status_half = gate
        .refresh_session_status(Some(1_783_085_000))
        .await
        .expect("refresh early-close session status");
    assert!(status_half.is_trading_day);
    assert!(status_half.is_early_close);
    // 12:45 ET (1_783_092_300) -> inside RTH
    assert!(
        gate.is_within_cached_rth(UnixNanos::from(1_783_092_300_000_000_000_u64))
            .unwrap()
    );
    // 13:00 ET (1_783_093_200) -> exact early close boundary (inclusive)
    assert!(
        gate.is_within_cached_rth(UnixNanos::from(1_783_093_200_000_000_000_u64))
            .unwrap()
    );
    // 13:15 ET (1_783_094_100) -> REJECTED on early-close day!
    assert!(
        !gate
            .is_within_cached_rth(UnixNanos::from(1_783_094_100_000_000_000_u64))
            .unwrap(),
        "13:15 ET must be rejected on a 13:00 ET early-close day"
    );

    // 3. Closed holiday/weekend
    let status_closed = gate
        .refresh_session_status(Some(1_783_170_000))
        .await
        .expect("refresh closed holiday status");
    assert!(!status_closed.is_trading_day);
    assert!(!status_closed.is_rth_open);
    assert_eq!(status_closed.rth_open_ns(), None);
    assert_eq!(status_closed.rth_close_ns(), None);
    assert!(
        !gate
            .is_within_cached_rth(UnixNanos::from(1_783_170_000_000_000_000_u64))
            .unwrap()
    );

    // 4. UNSYNCED source -> fail-closed error
    let unsynced_err = gate
        .refresh_session_status(Some(1_783_170_000))
        .await
        .expect_err("UNSYNCED source must fail closed");
    assert!(matches!(unsynced_err, DataInfraError::CalendarGate(ref msg) if msg.contains("UNSYNCED")));

    // 5. HTTP 503 -> fail-closed error
    let http_err = gate
        .refresh_session_status(Some(1_783_170_000))
        .await
        .expect_err("HTTP 503 must fail closed");
    assert!(matches!(http_err, DataInfraError::CalendarGate(ref msg) if msg.contains("503")));

    server_handle.await.expect("calendar server completed cleanly");

    // 6. Unreachable server (after server shut down) -> fail-closed error
    let unreachable_err = gate
        .refresh_session_status(None)
        .await
        .expect_err("unreachable calendar-service must fail closed");
    assert!(matches!(unreachable_err, DataInfraError::CalendarGate(_)));
}

// ================================================================================================
// 3. Ticket #05: DataInfraDataClient & DataInfraDataClientFactory End-to-End Integration
// ================================================================================================

#[derive(Debug, Default)]
struct RecordingLifecycleObserver {
    connected: AtomicUsize,
    disconnected: AtomicUsize,
    last_disconnect_reason: Mutex<Option<String>>,
}

impl StreamLifecycleListener for RecordingLifecycleObserver {
    fn on_stream_connected(&self, _endpoint: &str) {
        self.connected.fetch_add(1, Ordering::SeqCst);
    }

    fn on_stream_disconnected(&self, _endpoint: &str, reason: &str) {
        self.disconnected.fetch_add(1, Ordering::SeqCst);
        *self.last_disconnect_reason.lock() = Some(reason.to_string());
    }

    fn on_stream_reconnected(&self, _endpoint: &str, _attempt: u32) {}
}

#[derive(Debug)]
struct WrongClientConfig;

impl ClientConfig for WrongClientConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[test]
fn test_datainfra_data_client_factory_creates_client_and_rejects_invalid_config() {
    let factory = DataInfraDataClientFactory::new();
    assert_eq!(factory.name(), DATAINFRA);
    assert_eq!(factory.config_type(), "DataInfraConfig");

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let cfg = make_test_config("http://127.0.0.1:8123", "http://127.0.0.1:8012", "ALPACA");
    let cache = Rc::new(RefCell::new(Cache::default()));
    let cache_view = CacheView::new(Rc::clone(&cache));
    let clock: Rc<RefCell<dyn nautilus_common::clock::Clock>> =
        Rc::new(RefCell::new(VirtualClock::new()));

    let client = factory
        .create("DATAINFRA", &cfg, cache_view.clone(), Rc::clone(&clock))
        .expect("DataInfraDataClientFactory::create should succeed");
    assert_eq!(client.client_id(), ClientId::from("DATAINFRA"));
    assert_eq!(client.venue(), Some(Venue::from("ALPACA")));
    assert!(client.is_disconnected());

    // Wrong config type must fail closed
    let bad_res = factory.create("DATAINFRA", &WrongClientConfig, cache_view, clock);
    assert!(bad_res.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_datainfra_data_client_connect_fail_closed_subscriptions_requests_and_data_engine_loop()
 {
    // 1. Spin up ephemeral calendar-service HTTP listener:
    //    - First call returns HTTP 500 (testing `connect()` fail-closed)
    //    - Second call returns HTTP 200 valid RTH status (testing `connect()` success)
    let cal_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind calendar listener");
    let cal_addr = cal_listener.local_addr().unwrap();

    let cal_server = tokio::spawn(async move {
        // Attempt 1 -> 500 error
        let (mut s1, _) = cal_listener.accept().await.unwrap();
        let _ = read_http_request(&mut s1).await;
        write_http_response(
            &mut s1,
            "500 Internal Server Error",
            "text/plain",
            b"calendar error",
        )
        .await;

        // Attempt 2 -> 200 OK RTH open
        let (mut s2, _) = cal_listener.accept().await.unwrap();
        let _ = read_http_request(&mut s2).await;
        write_http_response(
            &mut s2,
            "200 OK",
            "application/json",
            br#"{
                "market": "US_EQUITY",
                "queried_at_utc": 1777900000,
                "trading_date": "2026-05-04",
                "is_trading_day": true,
                "is_rth_open": true,
                "rth_open_utc": 1777896600,
                "rth_close_utc": 1777920000,
                "is_early_close": false,
                "next_rth_open_utc": 1777983000,
                "source": "ALPACA"
            }"#,
        )
        .await;
    });

    // 2. Spin up ephemeral ClickHouse ArrowStream listener for `request_bars` and `request_quotes`
    let bars_batch = build_v_quant_bars_record_batch(
        &["AAPL", "AAPL"],
        &["15m", "15m"],
        &[1_700_000_000, 1_700_000_900],
        &[
            (185.00, 186.00, 184.50, 185.75, 120_000.0),
            (185.75, 186.80, 185.50, 186.50, 140_000.0),
        ],
        &[
            (185.00, 186.00, 184.50, 185.75, 120_000.0),
            (185.75, 186.80, 185.50, 186.50, 140_000.0),
        ],
        &[false, false],
        &[false, false],
        None,
    );
    let bars_ipc = encode_batches_to_arrow_ipc_stream(&[bars_batch]);

    let quotes_batch = build_quotes_record_batch(
        &["AAPL"],
        &[1_700_000_200_000_000_000],
        &[185.70],
        &[185.75],
        &[300.0],
        &[500.0],
    );
    let quotes_ipc = encode_batches_to_arrow_ipc_stream(&[quotes_batch]);

    let ch_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ClickHouse listener");
    let ch_addr = ch_listener.local_addr().unwrap();

    let ch_server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = ch_listener.accept().await {
            let raw_req = read_http_request(&mut socket).await;
            if raw_req.contains("v_quant_bars") {
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
            } else {
                write_http_response(&mut socket, "400 Bad Request", "text/plain", b"Bad SQL")
                    .await;
            }
        }
    });

    let cfg = make_test_config(
        &format!("http://{ch_addr}"),
        &format!("http://{cal_addr}"),
        "ALPACA",
    );
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    let sender = EventSender::from(tx);

    let ch_transport = Arc::new(HttpClickHouseArrowTransport::new(cfg.clone()).unwrap());
    let cal_gate = CalendarSessionGate::from_config(&cfg).unwrap();
    let observer = Arc::new(RecordingLifecycleObserver::default());

    let mut client = DataInfraDataClient::with_transports_and_listener(
        ClientId::from(DATAINFRA),
        cfg,
        ch_transport,
        cal_gate,
        Some(sender),
        observer.clone(),
    )
    .expect("create DataInfraDataClient");

    // A. First `connect()` must fail closed because calendar-service returned HTTP 500
    let err_connect = client
        .connect()
        .await
        .expect_err("connect must fail closed when calendar-service returns HTTP 500");
    assert!(err_connect.to_string().contains("500"));
    assert!(!client.is_connected());
    assert_eq!(observer.connected.load(Ordering::SeqCst), 0);

    // B. Second `connect()` succeeds when calendar-service returns HTTP 200 RTH status
    client
        .connect()
        .await
        .expect("connect should succeed on healthy calendar-service");
    assert!(client.is_connected());
    assert_eq!(observer.connected.load(Ordering::SeqCst), 1);
    assert!(client.calendar_gate().cached_status().is_some());

    // C. Subscriptions: `subscribe_instrument`, `subscribe_bars`, `subscribe_quotes`, `subscribe_instrument_status`
    let aapl_id = to_instrument_id("AAPL", "ALPACA").unwrap();
    let aapl_1m_bar_type = parse_datainfra_bar_type("AAPL", "ALPACA", "1m").unwrap();
    let aapl_15m_bar_type = parse_datainfra_bar_type("AAPL", "ALPACA", "15m").unwrap();
    let venue = Venue::from("ALPACA");

    client
        .subscribe_instrument(SubscribeInstrument::new(
            vec![aapl_id],
            Some(ClientId::from(DATAINFRA)),
            Some(venue),
            UUID4::new(),
            UnixNanos::from(1_u64),
            None,
            None,
        ))
        .expect("subscribe_instrument");

    client
        .subscribe_bars(SubscribeBars::new(
            vec![aapl_1m_bar_type],
            Some(ClientId::from(DATAINFRA)),
            Some(venue),
            UUID4::new(),
            UnixNanos::from(2_u64),
            None,
            None,
        ))
        .expect("subscribe_bars");
    assert!(client.subscribed_bars().contains(&aapl_1m_bar_type));

    client
        .subscribe_quotes(SubscribeQuotes::new(
            vec![aapl_id],
            Some(ClientId::from(DATAINFRA)),
            Some(venue),
            UUID4::new(),
            UnixNanos::from(3_u64),
            None,
            None,
        ))
        .expect("subscribe_quotes");
    assert!(client.subscribed_quotes().contains(&aapl_id));

    client
        .subscribe_instrument_status(SubscribeInstrumentStatus::new(
            vec![aapl_id],
            Some(ClientId::from(DATAINFRA)),
            Some(venue),
            UUID4::new(),
            UnixNanos::from(4_u64),
            None,
            None,
        ))
        .expect("subscribe_instrument_status");
    assert!(client.subscribed_statuses().contains(&aapl_id));

    // D. Ingest real-time Kafka payloads (`ingest_kafka_payload`):
    //    1) Unsubscribed symbol (MSFT) is decoded but NOT forwarded to DataEvent channel
    let msft_quote_json = br#"{
        "symbol": "MSFT",
        "quote_ts_ms": 1711980001000,
        "ingest_ts_ms": 1711980001005,
        "bid_price": 420.10,
        "bid_size": 100.0,
        "ask_price": 420.20,
        "ask_size": 200.0,
        "mid_price": 420.15,
        "spread_bps": 2.38,
        "provider": "alpaca_sip"
    }"#;
    let msft_outcome = client
        .ingest_kafka_payload(TOPIC_SIP_QUOTES, msft_quote_json, UnixNanos::default())
        .expect("ingest unsubscribed MSFT quote");
    assert!(matches!(msft_outcome, StreamDecodeOutcome::Emitted(_)));

    //    2) Subscribed AAPL 1m bar, quote, and LULD_PAUSE status ARE forwarded to DataEvent channel
    let aapl_bar_json = br#"{
        "symbol": "AAPL",
        "timeframe": "1m",
        "bar_ts": 1711980060,
        "open": 185.10,
        "high": 186.25,
        "low": 184.90,
        "close": 186.00,
        "volume": 52400.0,
        "vwap": 185.75,
        "trade_count": 410,
        "version": 1,
        "is_synthetic": false,
        "is_correction": false,
        "is_suspect_spike": false,
        "session": "REGULAR",
        "provider": "alpaca_sip",
        "source_ts_ms": 1711980120000,
        "ingest_ts_ms": 1711980120050
    }"#;
    client
        .ingest_kafka_payload(TOPIC_SIP_BARS_1M, aapl_bar_json, UnixNanos::default())
        .expect("ingest subscribed AAPL 1m bar");

    let aapl_quote_json = br#"{
        "symbol": "AAPL",
        "quote_ts_ms": 1711980121000,
        "ingest_ts_ms": 1711980121010,
        "bid_price": 185.95,
        "bid_size": 300.0,
        "ask_price": 186.05,
        "ask_size": 400.0,
        "mid_price": 186.00,
        "spread_bps": 5.37,
        "provider": "alpaca_sip"
    }"#;
    client
        .ingest_kafka_payload(TOPIC_SIP_QUOTES, aapl_quote_json, UnixNanos::default())
        .expect("ingest subscribed AAPL quote");

    let aapl_status_json = br#"{
        "symbol": "AAPL",
        "status_ts_ms": 1711980122000,
        "ingest_ts_ms": 1711980122005,
        "state": "LULD_PAUSE",
        "reason_code": "LULD_T5",
        "provider": "alpaca_sip"
    }"#;
    client
        .ingest_kafka_payload(TOPIC_SIP_STATUSES, aapl_status_json, UnixNanos::default())
        .expect("ingest subscribed AAPL status");

    // E. Historical requests via `DataClient`: `request_instrument`, `request_instruments`, `request_bars`, `request_quotes`
    client
        .request_instrument(RequestInstrument::new(
            vec![aapl_id],
            None,
            None,
            Some(ClientId::from(DATAINFRA)),
            UUID4::new(),
            UnixNanos::from(10_u64),
            None,
        ))
        .expect("request_instrument");

    client
        .request_instruments(RequestInstruments::new(
            None,
            None,
            Some(ClientId::from(DATAINFRA)),
            Some(venue),
            UUID4::new(),
            UnixNanos::from(11_u64),
            None,
        ))
        .expect("request_instruments");

    client
        .request_bars(RequestBars::new(
            vec![aapl_15m_bar_type],
            None,
            None,
            NonZeroUsize::new(10),
            Some(ClientId::from(DATAINFRA)),
            UUID4::new(),
            UnixNanos::from(12_u64),
            None,
        ))
        .expect("request_bars");

    client
        .request_quotes(RequestQuotes::new(
            vec![aapl_id],
            None,
            None,
            NonZeroUsize::new(10),
            Some(ClientId::from(DATAINFRA)),
            UUID4::new(),
            UnixNanos::from(13_u64),
            None,
        ))
        .expect("request_quotes");

    // F. Drain all emitted `DataEvent` messages into a real `DataEngine` and assert callbacks + Cache!
    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    let cache = Rc::new(RefCell::new(Cache::default()));
    let mut data_engine =
        DataEngine::new(clock, Rc::clone(&cache), Some(DataEngineConfig::default()));

    let received_bars: Rc<RefCell<Vec<Bar>>> = Rc::new(RefCell::new(Vec::new()));
    let received_bars_cb = Rc::clone(&received_bars);
    msgbus::subscribe_bars(
        get_bars_topic(aapl_1m_bar_type).into(),
        TypedHandler::from(move |b: &Bar| received_bars_cb.borrow_mut().push(*b)),
        None,
    );

    let received_quotes: Rc<RefCell<Vec<QuoteTick>>> = Rc::new(RefCell::new(Vec::new()));
    let received_quotes_cb = Rc::clone(&received_quotes);
    msgbus::subscribe_quotes(
        get_quotes_topic(aapl_id).into(),
        TypedHandler::from(move |q: &QuoteTick| received_quotes_cb.borrow_mut().push(*q)),
        None,
    );

    let received_statuses: Rc<RefCell<Vec<InstrumentStatus>>> = Rc::new(RefCell::new(Vec::new()));
    let received_statuses_cb = Rc::clone(&received_statuses);
    msgbus::subscribe_any(
        get_instrument_status_topic(aapl_id).into(),
        ShareableMessageHandler::from_typed(move |s: &InstrumentStatus| {
            received_statuses_cb.borrow_mut().push(*s);
        }),
        None,
    );

    let mut response_bars_count = 0_usize;
    let mut response_quotes_count = 0_usize;
    let mut response_instrument_count = 0_usize;
    let mut response_instruments_count = 0_usize;

    while let Ok(evt) = rx.try_recv() {
        match evt {
            DataEvent::Instrument(inst) => data_engine.process(&inst),
            DataEvent::Data(d) => data_engine.process_data(d),
            DataEvent::InstrumentStatus(s) => data_engine.process(&s),
            DataEvent::Response(resp) => {
                match &resp {
                    DataResponse::Bars(b) => response_bars_count += b.data.len(),
                    DataResponse::Quotes(q) => response_quotes_count += q.data.len(),
                    DataResponse::Instrument(_) => response_instrument_count += 1,
                    DataResponse::Instruments(i) => response_instruments_count += i.data.len(),
                    _ => {}
                }
                data_engine.response(resp);
            }
            _ => {}
        }
    }

    assert_eq!(received_bars.borrow().len(), 1);
    assert_eq!(received_bars.borrow()[0].close, Price::new(186.00, 2));

    assert_eq!(received_quotes.borrow().len(), 1);
    assert_eq!(received_quotes.borrow()[0].instrument_id, aapl_id);
    assert_eq!(received_quotes.borrow()[0].bid_price, Price::new(185.95, 2));

    assert_eq!(received_statuses.borrow().len(), 1);
    assert_eq!(received_statuses.borrow()[0].action, MarketStatusAction::Pause);

    assert_eq!(response_instrument_count, 1);
    assert_eq!(response_instruments_count, 1);
    assert_eq!(response_bars_count, 2);
    assert_eq!(response_quotes_count, 1);

    // G. Unsubscribe and disconnect lifecycle verification
    client
        .unsubscribe_bars(&UnsubscribeBars::new(
            vec![aapl_1m_bar_type],
            Some(ClientId::from(DATAINFRA)),
            Some(venue),
            UUID4::new(),
            UnixNanos::from(20_u64),
            None,
            None,
        ))
        .expect("unsubscribe_bars");
    assert!(client.subscribed_bars().is_empty());

    client
        .unsubscribe_quotes(&UnsubscribeQuotes::new(
            vec![aapl_id],
            Some(ClientId::from(DATAINFRA)),
            Some(venue),
            UUID4::new(),
            UnixNanos::from(21_u64),
            None,
            None,
        ))
        .expect("unsubscribe_quotes");
    assert!(client.subscribed_quotes().is_empty());

    client
        .unsubscribe_instrument_status(&UnsubscribeInstrumentStatus::new(
            vec![aapl_id],
            Some(ClientId::from(DATAINFRA)),
            Some(venue),
            UUID4::new(),
            UnixNanos::from(22_u64),
            None,
            None,
        ))
        .expect("unsubscribe_instrument_status");
    assert!(client.subscribed_statuses().is_empty());

    client.disconnect().await.expect("disconnect");
    assert!(client.is_disconnected());
    assert_eq!(observer.disconnected.load(Ordering::SeqCst), 1);

    cal_server.await.expect("calendar server completed cleanly");
    ch_server.abort();
}
