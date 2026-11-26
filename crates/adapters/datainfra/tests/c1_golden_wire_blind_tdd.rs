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

//! Blind TDD Contract Verification for C1 (Live Market Data & Cross-Service Golden Wire).

#![allow(linker_messages)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow::{
    array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
    ipc::writer::StreamWriter,
};
use async_trait::async_trait;
use nautilus_common::{
    clients::DataClient,
    live::sender::EventSender,
    messages::{
        DataEvent,
        data::{SubscribeBars, UnsubscribeBars},
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_datainfra::{
    CalendarSessionGate, CanonicalStreamDecoder, DATAINFRA, DataInfraConfig, DataInfraDataClient,
    DataInfraError, DataInfraResult, DataInfraStreamEvent, HttpClickHouseArrowTransport,
    KafkaPayloadFrame, KafkaPayloadSource, StreamDecodeOutcome, TOPIC_SIP_BARS_1M,
    TOPIC_SIP_QUOTES, TOPIC_SIP_STATUSES, mapping::compute_rth_bar_close_ns_with_bounds,
    parse_datainfra_bar_type, to_instrument_id,
};

#[derive(Debug)]
struct IdleKafkaSource;

#[async_trait]
impl KafkaPayloadSource for IdleKafkaSource {
    async fn next_frame(&self) -> DataInfraResult<Option<KafkaPayloadFrame>> {
        Ok(None)
    }
}
use nautilus_model::{
    data::Data,
    enums::MarketStatusAction,
    identifiers::{ClientId, Venue},
    types::{Price, Quantity},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn read_http_request(socket: &mut TcpStream) -> String {
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0_u8; 4096];
    let mut expected_total_len: Option<usize> = None;

    loop {
        let read_res = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut tmp)).await;
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

fn build_v_quant_bars_ipc(
    symbols: &[&str],
    timeframes: &[&str],
    ts_seconds: &[i64],
    ohlcv: &[(f64, f64, f64, f64, f64)],
) -> Vec<u8> {
    let n = symbols.len();
    let falses = vec![false; n];
    let schema = Arc::new(Schema::new(vec![
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
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(symbols.to_vec())),
        Arc::new(StringArray::from(timeframes.to_vec())),
        Arc::new(Int64Array::from(ts_seconds.to_vec())),
        Arc::new(Float64Array::from(ohlcv.iter().map(|x| x.0).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(ohlcv.iter().map(|x| x.1).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(ohlcv.iter().map(|x| x.2).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(ohlcv.iter().map(|x| x.3).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(ohlcv.iter().map(|x| x.4).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(ohlcv.iter().map(|x| x.0).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(ohlcv.iter().map(|x| x.1).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(ohlcv.iter().map(|x| x.2).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(ohlcv.iter().map(|x| x.3).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(ohlcv.iter().map(|x| x.4).collect::<Vec<_>>())),
        Arc::new(BooleanArray::from(falses.clone())),
        Arc::new(BooleanArray::from(falses)),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns).expect("valid v_quant_bars batch");
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema).expect("Arrow IPC writer");
        writer.write(&batch).expect("write batch");
        writer.finish().expect("finish Arrow IPC");
    }
    buf
}

async fn write_http_response(
    socket: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(header.as_bytes()).await.unwrap();
    socket.write_all(body).await.unwrap();
    socket.flush().await.unwrap();
}

#[test]
fn test_c1_golden_wire_schema_version_1_succeeds_and_99_or_missing_fails_closed() {
    let decoder = CanonicalStreamDecoder::new("ALPACA", 2, 0, 128);

    // 1. Golden sip-feeder CanonicalBar (`schema_version=1`)
    let golden_bar = br#"{
        "schema_version": 1,
        "market": "US",
        "symbol": "AAPL",
        "timeframe": "1m",
        "bar_ts": 1711980000,
        "open": 170.10,
        "high": 171.25,
        "low": 169.85,
        "close": 170.95,
        "volume": 48200.0,
        "vwap": 170.62,
        "trade_count": 310,
        "version": 1,
        "is_synthetic": false,
        "is_correction": false,
        "is_suspect_spike": false,
        "session": "REGULAR",
        "origin": "REALTIME",
        "provider": "alpaca_sip",
        "source_ts_ms": 1711980060000,
        "ingest_ts_ms": 1711980060125
    }"#;
    let bar_out = decoder
        .decode_topic_payload(TOPIC_SIP_BARS_1M, golden_bar)
        .expect("schema_version=1 bar must decode");
    match bar_out {
        StreamDecodeOutcome::Emitted(DataInfraStreamEvent::Bar(bar)) => {
            assert_eq!(bar.bar_type.to_string(), "AAPL.ALPACA-1-MINUTE-LAST-EXTERNAL");
            assert_eq!(bar.open, Price::new(170.10, 2));
            assert_eq!(bar.high, Price::new(171.25, 2));
            assert_eq!(bar.low, Price::new(169.85, 2));
            assert_eq!(bar.close, Price::new(170.95, 2));
            assert_eq!(bar.volume, Quantity::new(48_200.0, 0));
            // bar_ts (open UTC sec) -> Nautilus ts_event (bar close UTC ns)
            assert_eq!(bar.ts_event, UnixNanos::from(1_711_980_060_000_000_000_u64));
        }
        other => panic!("expected Emitted(Bar), got {other:?}"),
    }

    // 2. Golden sip-feeder CanonicalQuote (`schema_version=1`)
    let golden_quote = br#"{
        "schema_version": 1,
        "market": "US",
        "symbol": "MSFT",
        "quote_ts_ms": 1711980005250,
        "ingest_ts_ms": 1711980005255,
        "bid_price": 420.15,
        "bid_size": 300.0,
        "ask_price": 420.20,
        "ask_size": 500.0,
        "mid_price": 420.175,
        "spread_bps": 1.19,
        "provider": "alpaca_sip"
    }"#;
    let quote_out = decoder
        .decode_topic_payload(TOPIC_SIP_QUOTES, golden_quote)
        .expect("schema_version=1 quote must decode");
    match quote_out {
        StreamDecodeOutcome::Emitted(DataInfraStreamEvent::Quote(q)) => {
            assert_eq!(q.instrument_id, to_instrument_id("MSFT", "ALPACA").unwrap());
            assert_eq!(q.bid_price, Price::new(420.15, 2));
            assert_eq!(q.ask_price, Price::new(420.20, 2));
        }
        other => panic!("expected Emitted(Quote), got {other:?}"),
    }

    // 3. Golden sip-feeder CanonicalTradingStatus (`schema_version=1`)
    let golden_status = br#"{
        "schema_version": 1,
        "market": "US",
        "symbol": "NVDA",
        "status_ts_ms": 1711980010000,
        "ingest_ts_ms": 1711980010005,
        "state": "HALTED",
        "reason_code": "LULD_T5",
        "provider": "alpaca_sip"
    }"#;
    let status_out = decoder
        .decode_topic_payload(TOPIC_SIP_STATUSES, golden_status)
        .expect("schema_version=1 status must decode");
    match status_out {
        StreamDecodeOutcome::Emitted(DataInfraStreamEvent::TradingStatus(st)) => {
            assert_eq!(st.instrument_id, to_instrument_id("NVDA", "ALPACA").unwrap());
            assert_eq!(st.action, MarketStatusAction::Halt);
        }
        other => panic!("expected Emitted(TradingStatus), got {other:?}"),
    }

    // 4. Unsupported schema_version=99 must fail closed on bar, quote, and status
    let bad_ver_bar = br#"{
        "schema_version": 99,
        "symbol": "AAPL",
        "timeframe": "1m",
        "bar_ts": 1711980120,
        "open": 170.0,
        "high": 171.0,
        "low": 169.5,
        "close": 170.5,
        "volume": 1000.0,
        "version": 1,
        "is_synthetic": false,
        "session": "REGULAR"
    }"#;
    let err_bar = decoder
        .decode_topic_payload(TOPIC_SIP_BARS_1M, bad_ver_bar)
        .expect_err("schema_version=99 bar must fail closed");
    assert!(matches!(err_bar, DataInfraError::ProvenanceViolation(_)));

    let bad_ver_quote = br#"{
        "schema_version": 99,
        "symbol": "MSFT",
        "quote_ts_ms": 1711980005250,
        "ingest_ts_ms": 1711980005255,
        "bid_price": 420.15,
        "bid_size": 300.0,
        "ask_price": 420.20,
        "ask_size": 500.0
    }"#;
    let err_quote = decoder
        .decode_topic_payload(TOPIC_SIP_QUOTES, bad_ver_quote)
        .expect_err("schema_version=99 quote must fail closed");
    assert!(matches!(err_quote, DataInfraError::ProvenanceViolation(_)));

    let bad_ver_status = br#"{
        "schema_version": 99,
        "symbol": "NVDA",
        "status_ts_ms": 1711980010000,
        "ingest_ts_ms": 1711980010005,
        "state": "TRADING"
    }"#;
    let err_status = decoder
        .decode_topic_payload(TOPIC_SIP_STATUSES, bad_ver_status)
        .expect_err("schema_version=99 status must fail closed");
    assert!(matches!(err_status, DataInfraError::ProvenanceViolation(_)));

    // 5. Missing schema_version must fail closed with an explicit error (no silent default)
    let missing_ver_bar = br#"{
        "symbol": "AAPL",
        "timeframe": "1m",
        "bar_ts": 1711980180,
        "open": 170.0,
        "high": 171.0,
        "low": 169.5,
        "close": 170.5,
        "volume": 1000.0,
        "version": 1,
        "is_synthetic": false,
        "session": "REGULAR"
    }"#;
    assert!(
        decoder
            .decode_topic_payload(TOPIC_SIP_BARS_1M, missing_ver_bar)
            .is_err(),
        "missing schema_version on bar must fail closed (no silent default)"
    );

    let missing_ver_quote = br#"{
        "symbol": "MSFT",
        "quote_ts_ms": 1711980005250,
        "ingest_ts_ms": 1711980005255,
        "bid_price": 420.15,
        "bid_size": 300.0,
        "ask_price": 420.20,
        "ask_size": 500.0
    }"#;
    assert!(
        decoder
            .decode_topic_payload(TOPIC_SIP_QUOTES, missing_ver_quote)
            .is_err(),
        "missing schema_version on quote must fail closed (no silent default)"
    );

    let missing_ver_status = br#"{
        "symbol": "NVDA",
        "status_ts_ms": 1711980010000,
        "ingest_ts_ms": 1711980010005,
        "state": "HALTED"
    }"#;
    assert!(
        decoder
            .decode_topic_payload(TOPIC_SIP_STATUSES, missing_ver_status)
            .is_err(),
        "missing schema_version on status must fail closed (no silent default)"
    );
}

#[test]
fn test_c1_sec_rule_612_sub_dollar_precision_single_canonical_rule() {
    let decoder = CanonicalStreamDecoder::new("ALPACA", 2, 0, 64);

    // Unregistered sub-dollar (< $1.00) equity must use 4 decimal places per SEC Rule 612
    let sub_dollar_bar = br#"{
        "schema_version": 1,
        "symbol": "SUBD",
        "timeframe": "1m",
        "bar_ts": 1711980000,
        "open": 0.4512,
        "high": 0.4625,
        "low": 0.4401,
        "close": 0.4589,
        "volume": 120000.0,
        "trade_count": 90,
        "vwap": 0.4521,
        "version": 1,
        "is_synthetic": false,
        "session": "REGULAR"
    }"#;
    let out = decoder
        .decode_bar_json(sub_dollar_bar)
        .expect("sub-dollar bar must decode with 4 dp");
    match out {
        StreamDecodeOutcome::Emitted(DataInfraStreamEvent::Bar(bar)) => {
            assert_eq!(bar.open.precision, 4);
            assert_eq!(bar.high.precision, 4);
            assert_eq!(bar.low.precision, 4);
            assert_eq!(bar.close.precision, 4);
            assert_eq!(bar.open, Price::new(0.4512, 4));
            assert_eq!(bar.close, Price::new(0.4589, 4));
        }
        other => panic!("expected Emitted(Bar), got {other:?}"),
    }

    let sub_dollar_quote = br#"{
        "schema_version": 1,
        "symbol": "SUBD",
        "quote_ts_ms": 1711980005250,
        "ingest_ts_ms": 1711980005255,
        "bid_price": 0.4512,
        "bid_size": 500.0,
        "ask_price": 0.4518,
        "ask_size": 700.0
    }"#;
    let q_out = decoder
        .decode_quote_json(sub_dollar_quote)
        .expect("sub-dollar quote must decode with 4 dp");
    match q_out {
        StreamDecodeOutcome::Emitted(DataInfraStreamEvent::Quote(q)) => {
            assert_eq!(q.bid_price.precision, 4);
            assert_eq!(q.ask_price.precision, 4);
            assert_eq!(q.bid_price, Price::new(0.4512, 4));
            assert_eq!(q.ask_price, Price::new(0.4518, 4));
        }
        other => panic!("expected Emitted(Quote), got {other:?}"),
    }

    // SEC Rule 612 (< $1.00 -> 4dp, >= $1.00 -> 2dp) is the single canonical precision rule
    // for all US equities (no per-instrument precision override).
    let regp_sub_dollar = br#"{
        "schema_version": 1,
        "symbol": "REGP",
        "timeframe": "1m",
        "bar_ts": 1711980000,
        "open": 0.812,
        "high": 0.825,
        "low": 0.805,
        "close": 0.819,
        "volume": 5000.0,
        "version": 1,
        "is_synthetic": false,
        "session": "REGULAR"
    }"#;
    let reg_out = decoder
        .decode_bar_json(regp_sub_dollar)
        .expect("sub-dollar $0.819 bar must decode with 4 dp per SEC Rule 612");
    match reg_out {
        StreamDecodeOutcome::Emitted(DataInfraStreamEvent::Bar(bar)) => {
            assert_eq!(bar.open.precision, 4);
            assert_eq!(bar.close.precision, 4);
            assert_eq!(bar.close, Price::new(0.819, 4));
        }
        other => panic!("expected Emitted(Bar), got {other:?}"),
    }

    let regp_at_or_above_dollar = br#"{
        "schema_version": 1,
        "symbol": "REGP",
        "timeframe": "1m",
        "bar_ts": 1711980060,
        "open": 1.01,
        "high": 1.06,
        "low": 1.00,
        "close": 1.05,
        "volume": 8000.0,
        "version": 1,
        "is_synthetic": false,
        "session": "REGULAR"
    }"#;
    let above_out = decoder
        .decode_bar_json(regp_at_or_above_dollar)
        .expect(">= $1.00 bar must decode with 2 dp per SEC Rule 612");
    match above_out {
        StreamDecodeOutcome::Emitted(DataInfraStreamEvent::Bar(bar)) => {
            assert_eq!(bar.open.precision, 2);
            assert_eq!(bar.close.precision, 2);
            assert_eq!(bar.close, Price::new(1.05, 2));
        }
        other => panic!("expected Emitted(Bar), got {other:?}"),
    }
}

#[tokio::test]
async fn test_c1_early_close_session_bounds_1300_et_clamps_close_and_rollup_bucket() {
    // Use an arbitrary non-hardcoded trading date (2026-05-15 EDT) with live :8012 early close at 13:00 ET:
    //   09:30 EDT = 13:30 UTC = 1_778_851_800
    //   12:30 EDT = 16:30 UTC = 1_778_862_600
    //   12:59 EDT = 16:59 UTC = 1_778_864_340 (1m close = 13:00 EDT = 1_778_864_400)
    //   13:00 EDT = 17:00 UTC = 1_778_864_400
    let rth_open_utc: i64 = 1_778_851_800;
    let rth_close_utc: i64 = 1_778_864_400; // 13:00 ET early close
    let rth_open_ns = u64::try_from(rth_open_utc).unwrap() * 1_000_000_000;
    let rth_close_ns = u64::try_from(rth_close_utc).unwrap() * 1_000_000_000;

    let bar_1h = parse_datainfra_bar_type("AAPL", "ALPACA", "1h").unwrap();
    let bar_4h = parse_datainfra_bar_type("AAPL", "ALPACA", "4h").unwrap();
    let bar_1d = parse_datainfra_bar_type("AAPL", "ALPACA", "1d").unwrap();

    // 1. Direct `compute_rth_bar_close_ns_with_bounds` clamping with live 13:00 ET SessionBounds
    let bucket_1230_open_ns = 1_778_862_600_u64 * 1_000_000_000;
    assert_eq!(
        compute_rth_bar_close_ns_with_bounds(
            &bar_1h,
            bucket_1230_open_ns,
            Some((rth_open_ns, rth_close_ns)),
        ),
        rth_close_ns
    );
    assert_eq!(
        compute_rth_bar_close_ns_with_bounds(
            &bar_4h,
            rth_open_ns,
            Some((rth_open_ns, rth_close_ns)),
        ),
        rth_close_ns
    );
    assert_eq!(
        compute_rth_bar_close_ns_with_bounds(
            &bar_1d,
            rth_open_ns,
            Some((rth_open_ns, rth_close_ns)),
        ),
        rth_close_ns
    );

    // 2. End-to-end DataInfraDataClient with live :8012 CalendarSessionGate returning 13:00 ET early close:
    //    `LiveBarRollupBucket` clamps terminal `1h`, `4h`, and `1d` rollup bucket closes to 13:00 ET.
    let cal_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cal_addr = cal_listener.local_addr().unwrap();

    let cal_server = tokio::spawn(async move {
        let (mut socket, _) = cal_listener.accept().await.unwrap();
        let req = read_http_request(&mut socket).await;
        assert!(req.contains("/v1/calendar/session-status"));
        write_http_response(
            &mut socket,
            "200 OK",
            "application/json",
            br#"{
                "market": "US_EQUITY",
                "queried_at_utc": 1778860000,
                "trading_date": "2026-05-15",
                "is_trading_day": true,
                "is_rth_open": true,
                "rth_open_utc": 1778851800,
                "rth_close_utc": 1778864400,
                "is_early_close": true,
                "next_rth_open_utc": 1779111000,
                "source": "ALPACA"
            }"#,
        )
        .await;
    });

    let cfg = DataInfraConfig::new_checked(
        "http://127.0.0.1:8123",
        "market_data",
        2_000,
        0,
        vec!["127.0.0.1:9092".to_string()],
        "c1-early-close-test",
        format!("http://{cal_addr}"),
        "ALPACA",
    )
    .unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    let ch_transport = Arc::new(HttpClickHouseArrowTransport::new(cfg.clone()).unwrap());
    let cal_gate = CalendarSessionGate::from_config(&cfg).unwrap();

    // Populate live :8012 session bounds via CalendarSessionGate
    let status = cal_gate
        .refresh_session_status(Some(1_778_860_000))
        .await
        .expect("refresh live 13:00 ET early-close status");
    assert!(status.is_early_close);
    assert_eq!(status.rth_close_ns(), Some(UnixNanos::from(rth_close_ns)));

    let mut client = DataInfraDataClient::with_transports(
        ClientId::from(DATAINFRA),
        cfg,
        ch_transport,
        cal_gate,
        Some(EventSender::from(tx)),
    )
    .unwrap()
    .with_calendar_check_on_connect(false)
    .with_kafka_source(Arc::new(IdleKafkaSource));
    client
        .connect()
        .await
        .expect("DataInfraDataClient connect with calendar check disabled");

    client
        .subscribe_bars(SubscribeBars::new(
            vec![bar_1h, bar_4h, bar_1d],
            Some(ClientId::from(DATAINFRA)),
            Some(Venue::from("ALPACA")),
            UUID4::new(),
            UnixNanos::from(1_u64),
            None,
            None,
        ))
        .unwrap();

    // Ingest 09:30 ET, 12:30 ET, and terminal 12:59 ET (close = 13:00 ET) 1m bars
    let make_1m = |bar_ts: i64, open: f64, high: f64, low: f64, close: f64, vol: f64| {
        format!(
            r#"{{"schema_version":1,"symbol":"AAPL","timeframe":"1m","bar_ts":{bar_ts},"open":{open:.2},"high":{high:.2},"low":{low:.2},"close":{close:.2},"volume":{vol:.1},"version":1,"is_synthetic":false,"is_correction":false,"is_suspect_spike":false,"session":"REGULAR","provider":"alpaca_sip","source_ts_ms":{0},"ingest_ts_ms":{0}}}"#,
            (bar_ts + 60) * 1000
        )
    };

    // 09:30 ET bar
    client
        .ingest_kafka_payload(
            TOPIC_SIP_BARS_1M,
            make_1m(1_778_851_800, 190.00, 191.00, 189.50, 190.50, 10_000.0).as_bytes(),
            UnixNanos::default(),
        )
        .unwrap();
    // 12:30 ET bar -> rolls over 09:30..10:30 1h bucket
    client
        .ingest_kafka_payload(
            TOPIC_SIP_BARS_1M,
            make_1m(1_778_862_600, 190.50, 192.00, 190.00, 191.50, 12_000.0).as_bytes(),
            UnixNanos::default(),
        )
        .unwrap();
    // Drain 09:30..10:30 1h bar
    let mut pre_close_bars = Vec::new();
    while let Ok(DataEvent::Data(Data::Bar(b))) = rx.try_recv() {
        pre_close_bars.push(b);
    }
    assert_eq!(pre_close_bars.len(), 1);
    assert_eq!(pre_close_bars[0].bar_type, bar_1h);

    // 12:59 ET bar (close = 13:00 ET) -> flushes terminal 1h (12:30..13:00), 4h (09:30..13:00), and 1d (09:30..13:00)!
    client
        .ingest_kafka_payload(
            TOPIC_SIP_BARS_1M,
            make_1m(1_778_864_340, 191.50, 192.80, 191.20, 192.40, 15_000.0).as_bytes(),
            UnixNanos::default(),
        )
        .unwrap();

    let mut terminal_bars = Vec::new();
    while let Ok(DataEvent::Data(Data::Bar(b))) = rx.try_recv() {
        terminal_bars.push(b);
    }
    assert_eq!(
        terminal_bars.len(),
        3,
        "terminal 12:59 ET bar on 13:00 ET early-close day must flush 1h, 4h, and 1d bars"
    );
    for bt in [bar_1h, bar_4h, bar_1d] {
        let emitted = terminal_bars
            .iter()
            .find(|b| b.bar_type == bt)
            .unwrap_or_else(|| panic!("expected terminal bar for {bt}"));
        assert_eq!(
            emitted.ts_event,
            UnixNanos::from(rth_close_ns),
            "terminal {bt} close must be clamped cleanly to 13:00 ET"
        );
    }

    cal_server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_c1_cross_day_early_close_bucket_rollover_and_late_1m_revision_ts_event() {
    // Day 1: 2026-05-15 EDT, early close 13:00 ET (09:30=1_778_851_800, 13:00=1_778_864_400)
    // Day 2: 2026-05-18 EDT, regular close 16:00 ET (09:30=1_779_111_000, 16:00=1_779_134_400)
    let day1_close_ns = 1_778_864_400_u64 * 1_000_000_000;
    let day2_close_ns = 1_779_134_400_u64 * 1_000_000_000;

    let cal_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cal_addr = cal_listener.local_addr().unwrap();

    let cal_server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = cal_listener.accept().await {
            let req = read_http_request(&mut socket).await;
            let req_line = req.lines().next().unwrap_or("");
            assert!(
                req_line.contains("/v1/calendar/session-status"),
                "unexpected calendar path: {req_line}"
            );
            let at_ts = req_line
                .split("at_ts=")
                .nth(1)
                .and_then(|rest| rest.split(&['&', ' '][..]).next())
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(1_778_860_000);
            let body = if at_ts < 1_779_000_000 {
                br#"{
                    "market": "US_EQUITY",
                    "queried_at_utc": 1778860000,
                    "trading_date": "2026-05-15",
                    "is_trading_day": true,
                    "is_rth_open": true,
                    "rth_open_utc": 1778851800,
                    "rth_close_utc": 1778864400,
                    "is_early_close": true,
                    "next_rth_open_utc": 1779111000,
                    "source": "ALPACA"
                }"#
                .as_slice()
            } else {
                br#"{
                    "market": "US_EQUITY",
                    "queried_at_utc": 1779120000,
                    "trading_date": "2026-05-18",
                    "is_trading_day": true,
                    "is_rth_open": true,
                    "rth_open_utc": 1779111000,
                    "rth_close_utc": 1779134400,
                    "is_early_close": false,
                    "next_rth_open_utc": 1779197400,
                    "source": "ALPACA"
                }"#
                .as_slice()
            };
            write_http_response(&mut socket, "200 OK", "application/json", body).await;
        }
    });

    let cfg = DataInfraConfig::new_checked(
        "http://127.0.0.1:8123",
        "market_data",
        2_000,
        0,
        vec!["127.0.0.1:9092".to_string()],
        "c1-cross-day-rollover-test",
        format!("http://{cal_addr}"),
        "ALPACA",
    )
    .unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    let ch_transport = Arc::new(HttpClickHouseArrowTransport::new(cfg.clone()).unwrap());
    let cal_gate = CalendarSessionGate::from_config(&cfg).unwrap();
    cal_gate
        .refresh_session_status(Some(1_778_860_000))
        .await
        .expect("seed Day-1 early-close status");

    let mut client = DataInfraDataClient::with_transports(
        ClientId::from(DATAINFRA),
        cfg,
        ch_transport,
        cal_gate,
        Some(EventSender::from(tx)),
    )
    .unwrap()
    .with_calendar_check_on_connect(false)
    .with_kafka_source(Arc::new(IdleKafkaSource));
    client.connect().await.unwrap();

    let bar_1d = parse_datainfra_bar_type("AAPL", "ALPACA", "1d").unwrap();
    client
        .subscribe_bars(SubscribeBars::new(
            vec![bar_1d],
            Some(ClientId::from(DATAINFRA)),
            Some(Venue::from("ALPACA")),
            UUID4::new(),
            UnixNanos::from(1_u64),
            None,
            None,
        ))
        .unwrap();

    let make_1m =
        |bar_ts: i64, open: f64, high: f64, low: f64, close: f64, vol: f64, ver: u32| {
            let is_corr = ver > 1;
            format!(
                r#"{{"schema_version":1,"symbol":"AAPL","timeframe":"1m","bar_ts":{bar_ts},"open":{open:.2},"high":{high:.2},"low":{low:.2},"close":{close:.2},"volume":{vol:.1},"version":{ver},"is_synthetic":false,"is_correction":{is_corr},"is_suspect_spike":false,"session":"REGULAR","provider":"alpaca_sip","source_ts_ms":{0},"ingest_ts_ms":{0}}}"#,
                (bar_ts + 60) * 1000 + i64::from(ver)
            )
        };

    // Ingest two Day-1 1m bars before 13:00 ET close (09:30 ET and 12:00 ET) -> bucket stays open.
    client
        .ingest_kafka_payload(
            TOPIC_SIP_BARS_1M,
            make_1m(1_778_851_800, 190.00, 191.00, 189.50, 190.50, 10_000.0, 1).as_bytes(),
            UnixNanos::default(),
        )
        .unwrap();
    client
        .ingest_kafka_payload(
            TOPIC_SIP_BARS_1M,
            make_1m(1_778_860_800, 190.50, 192.00, 190.20, 191.80, 12_000.0, 1).as_bytes(),
            UnixNanos::default(),
        )
        .unwrap();
    assert!(rx.try_recv().is_err(), "Day-1 1d bar must stay open before close/rollover");

    // Roll over via Day-2 09:30 ET 1m bar (`1_779_111_000`).
    // Must emit Day-1's 1d bar stamped with Day-1's 13:00 ET close (NOT Day-2's 16:00 ET!).
    client
        .ingest_kafka_payload(
            TOPIC_SIP_BARS_1M,
            make_1m(1_779_111_000, 192.00, 193.00, 191.50, 192.50, 8_000.0, 1).as_bytes(),
            UnixNanos::default(),
        )
        .unwrap();

    let DataEvent::Data(Data::Bar(day1_bar)) = rx
        .try_recv()
        .expect("Day-2 rollover bar must emit closed Day-1 1d bar")
    else {
        panic!("expected Data::Bar for Day-1 rollover");
    };
    assert_eq!(day1_bar.bar_type, bar_1d);
    assert_eq!(day1_bar.ts_event, UnixNanos::from(day1_close_ns));
    assert_ne!(day1_bar.ts_event, UnixNanos::from(day2_close_ns));
    assert_eq!(day1_bar.open, Price::new(190.00, 2));
    assert_eq!(day1_bar.close, Price::new(191.80, 2));
    assert_eq!(day1_bar.volume, Quantity::new(22_000.0, 0));

    // Late 1m revision (version=2) for Day-1 (12:00 ET) must re-emit Day-1's 1d bar
    // still stamped with Day-1's 13:00 ET close `ts_event`.
    client
        .ingest_kafka_payload(
            TOPIC_SIP_BARS_1M,
            make_1m(1_778_860_800, 190.50, 193.40, 190.20, 193.10, 15_000.0, 2).as_bytes(),
            UnixNanos::default(),
        )
        .unwrap();

    let DataEvent::Data(Data::Bar(day1_revised)) = rx
        .try_recv()
        .expect("late Day-1 1m revision must emit revised Day-1 1d bar")
    else {
        panic!("expected Data::Bar for late Day-1 revision");
    };
    assert_eq!(day1_revised.bar_type, bar_1d);
    assert_eq!(day1_revised.ts_event, UnixNanos::from(day1_close_ns));
    assert_ne!(day1_revised.ts_event, UnixNanos::from(day2_close_ns));
    assert_eq!(day1_revised.high, Price::new(193.40, 2));
    assert_eq!(day1_revised.close, Price::new(193.10, 2));
    assert_eq!(day1_revised.volume, Quantity::new(25_000.0, 0));

    cal_server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_c1_mid_bucket_clickhouse_1m_prewarm_sub_dollar_and_resubscribe_reset() {
    // Anchor a 1h bucket strictly around wall-clock `now_secs` with ZERO sleep:
    // rth_open = now_secs - 600 (10m elapsed) -> 1h bucket_open = rth_open, flush_close = rth_open + 3600.
    // Historical 1m pre-warm bars at `ts = rth_open` close at `rth_open + 60 <= now_secs - 540 < now_secs`,
    // so ClickHouse `fetch_bars` never filters them out at any wall-clock second.
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let rth_open_secs = now_secs - 600;
    let rth_close_secs = rth_open_secs + 3600;
    let bucket_open_secs = rth_open_secs;
    let bucket_close_secs = rth_close_secs;
    let trading_date = jiff::Timestamp::from_second(now_secs)
        .unwrap()
        .in_tz("America/New_York")
        .unwrap()
        .date()
        .to_string();

    let cal_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cal_addr = cal_listener.local_addr().unwrap();
    let cal_body = format!(
        r#"{{
            "market": "US_EQUITY",
            "queried_at_utc": {now_secs},
            "trading_date": "{trading_date}",
            "is_trading_day": true,
            "is_rth_open": true,
            "rth_open_utc": {rth_open_secs},
            "rth_close_utc": {rth_close_secs},
            "is_early_close": false,
            "next_rth_open_utc": {rth_close_secs},
            "source": "ALPACA"
        }}"#
    )
    .into_bytes();

    let cal_server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = cal_listener.accept().await {
            let _req = read_http_request(&mut socket).await;
            write_http_response(&mut socket, "200 OK", "application/json", &cal_body).await;
        }
    });

    // ClickHouse HTTP server returning two distinct sub-dollar 1m pre-warm batches:
    // Query #1 (initial subscribe): 0.4500 open, 0.4400 low, 5000 vol
    // Query #2 (re-subscribe reset): 0.4600 open, 0.4550 low, 3000 vol
    let ipc_1 = build_v_quant_bars_ipc(
        &["SUBD"],
        &["1m"],
        &[bucket_open_secs],
        &[(0.4500, 0.4550, 0.4400, 0.4520, 5000.0)],
    );
    let ipc_2 = build_v_quant_bars_ipc(
        &["SUBD"],
        &["1m"],
        &[bucket_open_secs],
        &[(0.4600, 0.4650, 0.4550, 0.4620, 3000.0)],
    );
    let ch_calls = Arc::new(AtomicUsize::new(0));
    let ch_calls_srv = Arc::clone(&ch_calls);

    let ch_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ch_addr = ch_listener.local_addr().unwrap();
    let ch_server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = ch_listener.accept().await {
            let _req = read_http_request(&mut socket).await;
            let idx = ch_calls_srv.fetch_add(1, Ordering::SeqCst);
            let payload = if idx == 0 { &ipc_1 } else { &ipc_2 };
            write_http_response(
                &mut socket,
                "200 OK",
                "application/vnd.apache.arrow.stream",
                payload,
            )
            .await;
        }
    });

    let cfg = DataInfraConfig::new_checked(
        format!("http://{ch_addr}"),
        "market_data",
        2_000,
        0,
        vec!["127.0.0.1:9092".to_string()],
        "c1-mid-bucket-prewarm-test",
        format!("http://{cal_addr}"),
        "ALPACA",
    )
    .unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    let ch_transport = Arc::new(HttpClickHouseArrowTransport::new(cfg.clone()).unwrap());
    let cal_gate = CalendarSessionGate::from_config(&cfg).unwrap();
    cal_gate
        .refresh_session_status(Some(now_secs))
        .await
        .expect("populate mid-bucket RTH status");

    let mut client = DataInfraDataClient::with_transports(
        ClientId::from(DATAINFRA),
        cfg,
        ch_transport,
        cal_gate,
        Some(EventSender::from(tx)),
    )
    .unwrap()
    .with_calendar_check_on_connect(false)
    .with_kafka_source(Arc::new(IdleKafkaSource));
    client.connect().await.unwrap();

    let subd_1h = parse_datainfra_bar_type("SUBD", "ALPACA", "1h").unwrap();

    // 1. Subscribe mid-bucket -> triggers ClickHouse 1m pre-warm for sub-dollar SUBD ($0.4500, 4dp)
    client
        .subscribe_bars(SubscribeBars::new(
            vec![subd_1h],
            Some(ClientId::from(DATAINFRA)),
            Some(Venue::from("ALPACA")),
            UUID4::new(),
            UnixNanos::from(1_u64),
            None,
            None,
        ))
        .unwrap();
    assert_eq!(ch_calls.load(Ordering::SeqCst), 1);

    // 2. Send a live Kafka 1m bar completing the 1h bucket and crossing >= $1.00 (2dp) ->
    //    must aggregate 4dp pre-warm + 2dp live bar at 4dp without precision panic!
    let final_min_ts = bucket_open_secs + 59 * 60;
    let live_bar_v1 = format!(
        r#"{{"schema_version":1,"symbol":"SUBD","timeframe":"1m","bar_ts":{final_min_ts},"open":1.01,"high":1.08,"low":1.00,"close":1.05,"volume":7000.0,"version":1,"is_synthetic":false,"is_correction":false,"is_suspect_spike":false,"session":"REGULAR","provider":"alpaca_sip","source_ts_ms":{0},"ingest_ts_ms":{0}}}"#,
        (final_min_ts + 60) * 1000
    );
    client
        .ingest_kafka_payload(TOPIC_SIP_BARS_1M, live_bar_v1.as_bytes(), UnixNanos::default())
        .unwrap();

    let DataEvent::Data(Data::Bar(bar1)) = rx
        .try_recv()
        .expect("completing 1m bar must emit combined pre-warm + live 1h bar")
    else {
        panic!("expected Data::Bar");
    };
    assert_eq!(bar1.bar_type, subd_1h);
    assert_eq!(bar1.open, Price::new(0.4500, 4));
    assert_eq!(bar1.low, Price::new(0.4400, 4));
    assert_eq!(bar1.high, Price::new(1.0800, 4));
    assert_eq!(bar1.close, Price::new(1.0500, 4));
    assert_eq!(bar1.volume, Quantity::new(12_000.0, 0));
    assert_eq!(
        bar1.ts_event,
        UnixNanos::from(u64::try_from(bucket_close_secs).unwrap() * 1_000_000_000)
    );

    // 3. Unsubscribe and re-subscribe -> clears old emitted bucket and pre-warms cleanly
    client
        .unsubscribe_bars(&UnsubscribeBars::new(
            vec![subd_1h],
            Some(ClientId::from(DATAINFRA)),
            Some(Venue::from("ALPACA")),
            UUID4::new(),
            UnixNanos::from(2_u64),
            None,
            None,
        ))
        .unwrap();
    client
        .subscribe_bars(SubscribeBars::new(
            vec![subd_1h],
            Some(ClientId::from(DATAINFRA)),
            Some(Venue::from("ALPACA")),
            UUID4::new(),
            UnixNanos::from(3_u64),
            None,
            None,
        ))
        .unwrap();
    assert_eq!(ch_calls.load(Ordering::SeqCst), 2);

    let live_bar_v2 = format!(
        r#"{{"schema_version":1,"symbol":"SUBD","timeframe":"1m","bar_ts":{final_min_ts},"open":0.4620,"high":0.4700,"low":0.4590,"close":0.4680,"volume":4000.0,"version":2,"is_synthetic":false,"is_correction":true,"is_suspect_spike":false,"session":"REGULAR","provider":"alpaca_sip","source_ts_ms":{0},"ingest_ts_ms":{0}}}"#,
        (final_min_ts + 60) * 1000 + 2
    );
    client
        .ingest_kafka_payload(TOPIC_SIP_BARS_1M, live_bar_v2.as_bytes(), UnixNanos::default())
        .unwrap();

    let DataEvent::Data(Data::Bar(bar2)) = rx
        .try_recv()
        .expect("re-subscribe must clear old bucket and emit new pre-warmed + live 1h bar")
    else {
        panic!("expected Data::Bar after re-subscribe");
    };
    assert_eq!(bar2.bar_type, subd_1h);
    assert_eq!(bar2.open, Price::new(0.4600, 4));
    assert_eq!(bar2.low, Price::new(0.4550, 4));
    assert_eq!(bar2.high, Price::new(0.4700, 4));
    assert_eq!(bar2.close, Price::new(0.4680, 4));
    assert_eq!(bar2.volume, Quantity::new(7_000.0, 0));
    assert_eq!(
        bar2.ts_event,
        UnixNanos::from(u64::try_from(bucket_close_secs).unwrap() * 1_000_000_000)
    );

    cal_server.abort();
    ch_server.abort();
}
