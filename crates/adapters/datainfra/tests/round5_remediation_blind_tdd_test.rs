// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
// -------------------------------------------------------------------------------------------------

//! Blind TDD regression tests for Round 5 `nautilus-datainfra` (`D4-R5-P1-1` & `D4-R5-P1-2`):
//! - `D4-R5-P1-1`: `fetch_bars` with `limit_per_symbol = Some(N)` and `end_ts_ns = Some(...)`
//!   over-fetches `lim + 2` in SQL, filters `ts_event <= end_ts_ns`, sorts ascending, and returns
//!   the newest `N` closed bars without losing the `N`-th newest closed bar to boundary filtering;
//!   `DataInfraDataClient::request_bars` similarly preserves `limit = Some(N)` when excluding
//!   unclosed live bars.
//! - `D4-R5-P1-2`: `can_rollup_from_1m` disallows `1w` / `W1` (`step > 86_400s`), while allowing
//!   intraday / `1d` rollups (`<= 86_400s`).

#![allow(linker_messages)]

use std::{num::NonZeroUsize, sync::Arc};

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
        DataEvent, DataResponse,
        data::{RequestBars, SubscribeBars},
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_datainfra::{
    BarsQueryRequest, CalendarSessionGate, ClickHouseArrowTransport, DATAINFRA, DataInfraConfig,
    DataInfraDataClient, DataInfraResult, DataInfraTimeframe, HttpClickHouseArrowTransport,
    KafkaPayloadFrame, KafkaPayloadSource, PriceAdjustmentMode, TOPIC_SIP_BARS_1M,
    data::TracingStreamLifecycleListener, parse_datainfra_bar_type,
};
use nautilus_model::{
    data::Data,
    identifiers::{ClientId, Venue},
};
use parking_lot::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn ny_ns(year: i16, month: i8, day: i8, hour: i8, min: i8, sec: i8) -> u64 {
    let dt = jiff::civil::date(year, month, day)
        .at(hour, min, sec, 0)
        .in_tz("America/New_York")
        .expect("valid NY datetime");
    u64::try_from(dt.timestamp().as_nanosecond()).expect("non-negative ns")
}

fn encode_arrow_ipc_stream(batches: &[RecordBatch]) -> Vec<u8> {
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
    }
    buf
}

fn build_bars_batch(
    symbols: &[&str],
    timeframes: &[&str],
    ts_seconds: &[i64],
    closes: &[f64],
) -> RecordBatch {
    let n = symbols.len();
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
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(symbols.to_vec())),
        Arc::new(StringArray::from(timeframes.to_vec())),
        Arc::new(Int64Array::from(ts_seconds.to_vec())),
        Arc::new(Float64Array::from(closes.to_vec())),
        Arc::new(Float64Array::from(closes.iter().map(|c| c + 1.0).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(closes.iter().map(|c| c - 1.0).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(closes.to_vec())),
        Arc::new(Float64Array::from(vec![10_000.0; n])),
        Arc::new(Float64Array::from(closes.to_vec())),
        Arc::new(Float64Array::from(closes.iter().map(|c| c + 1.0).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(closes.iter().map(|c| c - 1.0).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(closes.to_vec())),
        Arc::new(Float64Array::from(vec![10_000.0; n])),
        Arc::new(BooleanArray::from(vec![false; n])),
        Arc::new(BooleanArray::from(vec![false; n])),
    ];
    RecordBatch::try_new(schema, cols).unwrap()
}

async fn read_http_req(stream: &mut tokio::net::TcpStream) -> String {
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 1024];
    let mut expected_total_len: Option<usize> = None;

    loop {
        let n = stream.read(&mut tmp).await.unwrap_or(0);
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if expected_total_len.is_none()
            && let Some(hdr_end) = buf.windows(4).position(|w| w == b"\r\n\r\n")
        {
            let body_start = hdr_end + 4;
            let headers = String::from_utf8_lossy(&buf[..hdr_end]);
            let clen = headers
                .lines()
                .find_map(|l| {
                    let (k, v) = l.split_once(':')?;
                    k.trim()
                        .eq_ignore_ascii_case("content-length")
                        .then(|| v.trim().parse::<usize>().ok())?
                })
                .unwrap_or(0);
            expected_total_len = Some(body_start + clen);
        }
        if let Some(total) = expected_total_len
            && buf.len() >= total
        {
            break;
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

fn extract_sql_limit_param(raw_req: &str) -> Option<usize> {
    let req_line = raw_req.lines().next()?;
    let path_and_q = req_line.split_whitespace().nth(1)?;
    let query = path_and_q.split_once('?')?.1;
    for pair in query.split('&') {
        let (k, v) = pair.split_once('=')?;
        if k == "param_per_symbol_limit" || k == "param_limit" {
            return v.parse::<usize>().ok();
        }
    }
    None
}

#[derive(Debug)]
struct IdleKafkaSource;

#[async_trait]
impl KafkaPayloadSource for IdleKafkaSource {
    async fn next_frame(&self) -> DataInfraResult<Option<KafkaPayloadFrame>> {
        std::future::pending().await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_round5_d4_p1_1_fetch_bars_and_request_bars_overfetch_limit_plus_2() {
    // Dataset on 2026-10-22 (EDT): 4 15m bars ordered DESC in ClickHouse:
    // - 10:15..10:30 (ts_event = 10:30 > end_ts = 10:20) -> UNCLOSED, must be excluded!
    // - 10:00..10:15 (ts_event = 10:15 <= 10:20) -> newest closed (#1)
    // - 09:45..10:00 (ts_event = 10:00 <= 10:20) -> 2nd newest closed (#2)
    // - 09:30..09:45 (ts_event = 09:45 <= 10:20) -> 3rd newest closed (#3)
    //
    // Asking for limit = 2 MUST NOT lose 09:45..10:00 when 10:15..10:30 is filtered out!
    let t09_30 = ny_ns(2026, 10, 22, 9, 30, 0);
    let t09_45 = ny_ns(2026, 10, 22, 9, 45, 0);
    let t10_00 = ny_ns(2026, 10, 22, 10, 0, 0);
    let t10_15 = ny_ns(2026, 10, 22, 10, 15, 0);
    let end_10_20 = ny_ns(2026, 10, 22, 10, 20, 0);

    // Also include a duplicate/extra post-end row (`10:15` appearing twice or `10:18` unclosed)
    // to prove `lim + 2` handles up to 2 boundary-filtered rows!
    let all_rows_desc: Vec<(&str, &str, i64, f64)> = vec![
        ("AAPL", "15m", i64::try_from(t10_15 / 1_000_000_000).unwrap(), 104.0),
        ("AAPL", "15m", i64::try_from(t10_00 / 1_000_000_000).unwrap(), 103.0),
        ("AAPL", "15m", i64::try_from(t09_45 / 1_000_000_000).unwrap(), 102.0),
        ("AAPL", "15m", i64::try_from(t09_30 / 1_000_000_000).unwrap(), 101.0),
    ];

    let captured_limits: Arc<Mutex<Vec<Option<usize>>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_limits_srv = captured_limits.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();

    let srv = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                accept_res = listener.accept() => {
                    let Ok((mut s, _)) = accept_res else { break; };
                    let raw = read_http_req(&mut s).await;
                    let lim = extract_sql_limit_param(&raw);
                    captured_limits_srv.lock().push(lim);
                    let take_n = lim.unwrap_or(all_rows_desc.len()).min(all_rows_desc.len());
                    let slice = &all_rows_desc[..take_n];
                    let syms: Vec<&str> = slice.iter().map(|r| r.0).collect();
                    let tfs: Vec<&str> = slice.iter().map(|r| r.1).collect();
                    let tss: Vec<i64> = slice.iter().map(|r| r.2).collect();
                    let cls: Vec<f64> = slice.iter().map(|r| r.3).collect();
                    let batch = build_bars_batch(&syms, &tfs, &tss, &cls);
                    let ipc = encode_arrow_ipc_stream(&[batch]);
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apache.arrow.stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        ipc.len()
                    );
                    s.write_all(hdr.as_bytes()).await.unwrap();
                    s.write_all(&ipc).await.unwrap();
                    let _ = s.shutdown().await;
                }
            }
        }
    });

    let cfg = DataInfraConfig::new_checked(
        format!("http://{addr}"),
        "market_data",
        2_000,
        0,
        vec!["127.0.0.1:9092".to_string()],
        "nautilus-datainfra-r5-test",
        "http://127.0.0.1:8012",
        "ALPACA",
    )
    .unwrap();

    // 1. Direct HttpClickHouseArrowTransport::fetch_bars(limit=Some(2), end_ts_ns=Some(10:20))
    let transport = Arc::new(HttpClickHouseArrowTransport::new(cfg.clone()).unwrap());
    let mut req = BarsQueryRequest::new(
        vec!["AAPL".to_string()],
        DataInfraTimeframe::M15,
        "ALPACA",
        PriceAdjustmentMode::Adjusted,
    );
    req.start_ts_ns = Some(UnixNanos::from(t09_30));
    req.end_ts_ns = Some(UnixNanos::from(end_10_20));
    req.limit_per_symbol = Some(2);

    let bars = transport.fetch_bars(&req).await.unwrap();
    assert_eq!(
        bars.len(),
        2,
        "fetch_bars(limit=2) MUST return 2 newest closed bars without truncation when 1 unclosed bar is dropped"
    );
    assert_eq!(
        bars.iter().map(|b| b.ts_event.as_u64()).collect::<Vec<_>>(),
        vec![t10_00, t10_15],
        "bars must be sorted ascending and retain 09:45..10:00 (ts_event=10:00) + 10:00..10:15 (ts_event=10:15)"
    );
    assert_eq!(
        captured_limits.lock()[0],
        Some(4),
        "SQL LIMIT param for limit_per_symbol=2 with end_ts_ns MUST over-fetch lim + 2 (= 4)"
    );

    // 2. End-to-end DataInfraDataClient::request_bars(limit=Some(2), end=10:20)
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    let cal_gate = CalendarSessionGate::from_config(&cfg).unwrap();
    let client = DataInfraDataClient::with_transports_and_listener(
        ClientId::from(DATAINFRA),
        cfg,
        transport,
        cal_gate,
        Some(EventSender::from(tx)),
        Arc::new(TracingStreamLifecycleListener),
    )
    .unwrap()
    .with_kafka_source(Arc::new(IdleKafkaSource));

    let bar_type = parse_datainfra_bar_type("AAPL", "ALPACA", "15m").unwrap();
    client
        .request_bars(RequestBars::new(
            vec![bar_type],
            Some(UnixNanos::from(t09_30).to_datetime_utc()),
            Some(UnixNanos::from(end_10_20).to_datetime_utc()),
            NonZeroUsize::new(2),
            Some(ClientId::from(DATAINFRA)),
            UUID4::new(),
            UnixNanos::from(100_u64),
            None,
        ))
        .expect("request_bars limit=2");

    match rx.try_recv().expect("expected BarsResponse") {
        DataEvent::Response(DataResponse::Bars(resp)) => {
            assert_eq!(
                resp.data.len(),
                2,
                "request_bars(limit=2) must retain newest 2 closed bars after excluding unclosed live bar"
            );
            assert_eq!(
                resp.data.iter().map(|b| b.ts_event.as_u64()).collect::<Vec<_>>(),
                vec![t10_00, t10_15]
            );
        }
        other => panic!("expected DataResponse::Bars, got {other:?}"),
    }

    let _ = stop_tx.send(());
    srv.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_round5_d4_p1_2_no_1m_rollup_for_1w_while_allowing_intraday_and_1d() {
    // Subscribe to 15m, 1d (`step <= 86_400s`), and 1w (`W1`, `step = 604_800s > 86_400s`),
    // then ingest `1m` Kafka bars across 15m bucket close, next trading day, and next week.
    // `can_rollup_from_1m` MUST emit rolled-up `15m` and `1d` bars, and MUST NEVER roll up `1m -> 1w`.
    let cfg = DataInfraConfig::new_checked(
        "http://127.0.0.1:9",
        "market_data",
        2_000,
        0,
        vec!["127.0.0.1:9092".to_string()],
        "nautilus-datainfra-r5-rollup",
        "http://127.0.0.1:8012",
        "ALPACA",
    )
    .unwrap();

    let transport = Arc::new(HttpClickHouseArrowTransport::new(cfg.clone()).unwrap());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    let cal_gate = CalendarSessionGate::from_config(&cfg).unwrap();
    let mut client = DataInfraDataClient::with_transports_and_listener(
        ClientId::from(DATAINFRA),
        cfg,
        transport,
        cal_gate,
        Some(EventSender::from(tx)),
        Arc::new(TracingStreamLifecycleListener),
    )
    .unwrap()
    .with_kafka_source(Arc::new(IdleKafkaSource))
    .with_calendar_check_on_connect(false);

    client.connect().await.expect("connect with calendar check disabled");

    let venue = Venue::from("ALPACA");
    let bt_15m = parse_datainfra_bar_type("AAPL", "ALPACA", "15m").unwrap();
    let bt_1d = parse_datainfra_bar_type("AAPL", "ALPACA", "1d").unwrap();
    let bt_1w = parse_datainfra_bar_type("AAPL", "ALPACA", "1w").unwrap();

    client
        .subscribe_bars(SubscribeBars::new(
            vec![bt_15m, bt_1d, bt_1w],
            Some(ClientId::from(DATAINFRA)),
            Some(venue),
            UUID4::new(),
            UnixNanos::from(1_u64),
            None,
            None,
        ))
        .expect("subscribe 15m, 1d, 1w");

    let send_1m = |cli: &mut DataInfraDataClient, bar_ts_sec: i64, close: f64| {
        let payload = format!(
            r#"{{"schema_version":1,"symbol":"AAPL","timeframe":"1m","bar_ts":{bar_ts_sec},"open":{close},"high":{high},"low":{low},"close":{close},"volume":1000.0,"vwap":{close},"trade_count":10,"version":1,"is_synthetic":false,"is_correction":false,"is_suspect_spike":false,"session":"REGULAR","provider":"alpaca_sip","source_ts_ms":{src_ms},"ingest_ts_ms":{src_ms}}}"#,
            high = close + 0.5,
            low = close - 0.5,
            src_ms = (bar_ts_sec + 60) * 1000,
        );
        cli.ingest_kafka_payload(
            TOPIC_SIP_BARS_1M,
            payload.as_bytes(),
            UnixNanos::from(u64::try_from((bar_ts_sec + 60) * 1_000_000_000).unwrap()),
        )
        .expect("ingest 1m bar");
    };

    // Thu 2026-10-22 09:30 ET .. 09:44 ET (15 bars in the first 15m bucket)
    let thu_0930 = i64::try_from(ny_ns(2026, 10, 22, 9, 30, 0) / 1_000_000_000).unwrap();
    for m in 0..15 {
        send_1m(&mut client, thu_0930 + m * 60, 180.0 + m as f64);
    }
    // Thu 2026-10-22 09:45 ET closes the 09:30..09:45 15m bucket!
    send_1m(&mut client, thu_0930 + 15 * 60, 196.0);

    // Fri 2026-10-23 09:30 ET crosses into next trading day -> closes Thu 1d bucket!
    let fri_0930 = i64::try_from(ny_ns(2026, 10, 23, 9, 30, 0) / 1_000_000_000).unwrap();
    send_1m(&mut client, fri_0930, 200.0);

    // Mon 2026-10-26 09:30 ET crosses into next calendar week -> closes Fri 1d, MUST NOT emit 1w!
    let mon_0930 = i64::try_from(ny_ns(2026, 10, 26, 9, 30, 0) / 1_000_000_000).unwrap();
    send_1m(&mut client, mon_0930, 205.0);

    let mut emitted_types = Vec::new();
    while let Ok(evt) = rx.try_recv() {
        if let DataEvent::Data(Data::Bar(b)) = evt {
            emitted_types.push(b.bar_type);
        }
    }

    assert!(
        emitted_types.contains(&bt_15m),
        "1m Kafka stream MUST roll up into intraday 15m when subscribed; emitted={emitted_types:?}"
    );
    assert!(
        emitted_types.contains(&bt_1d),
        "1m Kafka stream MUST roll up into 1d (step == 86_400s) when subscribed; emitted={emitted_types:?}"
    );
    assert!(
        !emitted_types.contains(&bt_1w),
        "can_rollup_from_1m MUST return false for 1w / W1 (step > 86_400s); emitted={emitted_types:?}"
    );
}
