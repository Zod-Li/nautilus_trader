// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
// -------------------------------------------------------------------------------------------------

//! Blind behavioral regression tests for Round 4 `nautilus-datainfra` fixes (`D3-P0-4` & `D4-P1-3`):
//! - `D3-P0-4`: `compute_rth_bar_close_ns` aligns UTC-midnight `1d` bars (`00:00:00Z`) to
//!   `16:00 ET` (or `13:00 ET` early close), preserves `1w` `natural_close_ns`, and never
//!   moves a post-close `1d` open backwards (`close_ns > bar_open_ns`).
//! - `D4-P1-3`: `clickhouse.rs` bar filtering strictly requires `ts_event <= end_u64`
//!   (no `allow_exact_open_point` leakage).

#![allow(linker_messages)]

use std::{sync::Arc, time::Duration};

use arrow::{
    array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
    ipc::writer::StreamWriter,
};
use nautilus_core::UnixNanos;
use nautilus_datainfra::{
    ArrowDecodeOptions, BarsQueryRequest, ClickHouseArrowTransport, DataInfraCatalog,
    DataInfraConfig, DataInfraTimeframe, HttpClickHouseArrowTransport, PriceAdjustmentMode,
    decode_bars_batches, mapping::compute_rth_bar_close_ns, parse_datainfra_bar_type,
};
use nautilus_model::data::{Bar, DataBatch, NautilusDataType};
use nautilus_persistence::catalog::{traits::CatalogReader, types::CatalogQuery};
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

fn utc_ns(year: i16, month: i8, day: i8, hour: i8, min: i8, sec: i8) -> u64 {
    let dt = jiff::civil::date(year, month, day)
        .at(hour, min, sec, 0)
        .to_zoned(jiff::tz::TimeZone::UTC)
        .expect("valid UTC datetime");
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

fn build_bars_batch(symbols: &[&str], timeframes: &[&str], ts_seconds: &[i64]) -> RecordBatch {
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
        Arc::new(Float64Array::from(vec![100.0; n])),
        Arc::new(Float64Array::from(vec![101.0; n])),
        Arc::new(Float64Array::from(vec![99.0; n])),
        Arc::new(Float64Array::from(vec![100.5; n])),
        Arc::new(Float64Array::from(vec![10_000.0; n])),
        Arc::new(Float64Array::from(vec![100.0; n])),
        Arc::new(Float64Array::from(vec![101.0; n])),
        Arc::new(Float64Array::from(vec![99.0; n])),
        Arc::new(Float64Array::from(vec![100.5; n])),
        Arc::new(Float64Array::from(vec![10_000.0; n])),
        Arc::new(BooleanArray::from(vec![false; n])),
        Arc::new(BooleanArray::from(vec![false; n])),
    ];
    RecordBatch::try_new(schema, cols).unwrap()
}

async fn read_http_req(stream: &mut tokio::net::TcpStream) -> String {
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0_u8; 2048];
    let mut expected_total: Option<usize> = None;
    loop {
        let Ok(Ok(n)) = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut tmp)).await
        else {
            break;
        };
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if expected_total.is_none()
            && let Some(hdr_end) = buf.windows(4).position(|w| w == b"\r\n\r\n")
        {
            let body_start = hdr_end + 4;
            let hdrs = String::from_utf8_lossy(&buf[..hdr_end]).to_ascii_lowercase();
            let cl = hdrs
                .lines()
                .find_map(|l| {
                    l.strip_prefix("content-length:")
                        .and_then(|v| v.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            expected_total = Some(body_start + cl);
        }
        if let Some(tot) = expected_total
            && buf.len() >= tot
        {
            break;
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

#[test]
fn test_round4_d3_p0_4_compute_rth_bar_close_ns_1d_utc_midnight_early_close_1w_and_post_close() {
    let bt_1d = parse_datainfra_bar_type("AAPL", "ALPACA", "1d").unwrap();
    let bt_1w = parse_datainfra_bar_type("AAPL", "ALPACA", "1w").unwrap();

    // 1. Regular trading day with UTC-midnight `1d` open (EDT: 2026-10-22T00:00:00Z = Oct 21 20:00 EDT)
    //    MUST align to 2026-10-22 16:00:00 ET (NOT Oct 21 and NOT 2026-10-23T00:00:00Z!)
    let oct22_utc_mid = utc_ns(2026, 10, 22, 0, 0, 0);
    assert_eq!(
        compute_rth_bar_close_ns(&bt_1d, oct22_utc_mid),
        ny_ns(2026, 10, 22, 16, 0, 0)
    );

    // 2. Standard time (EST) UTC-midnight `1d` open (2026-11-16T00:00:00Z) -> 2026-11-16 16:00:00 ET
    let nov16_utc_mid = utc_ns(2026, 11, 16, 0, 0, 0);
    assert_eq!(
        compute_rth_bar_close_ns(&bt_1d, nov16_utc_mid),
        ny_ns(2026, 11, 16, 16, 0, 0)
    );

    // 3. Early close half-days (`13:00 ET`) from UTC-midnight `1d` open:
    //    - Black Friday 2026-11-27T00:00:00Z -> 2026-11-27 13:00:00 ET
    //    - Christmas Eve Thursday 2026-12-24T00:00:00Z -> 2026-12-24 13:00:00 ET
    //    - July 3 Thursday 2025-07-03T00:00:00Z -> 2025-07-03 13:00:00 ET
    assert_eq!(
        compute_rth_bar_close_ns(&bt_1d, utc_ns(2026, 11, 27, 0, 0, 0)),
        ny_ns(2026, 11, 27, 13, 0, 0)
    );
    assert_eq!(
        compute_rth_bar_close_ns(&bt_1d, utc_ns(2026, 12, 24, 0, 0, 0)),
        ny_ns(2026, 12, 24, 13, 0, 0)
    );
    assert_eq!(
        compute_rth_bar_close_ns(&bt_1d, utc_ns(2025, 7, 3, 0, 0, 0)),
        ny_ns(2025, 7, 3, 13, 0, 0)
    );

    // 4. RTH-open `1d` bar (09:30 ET) aligns to 16:00 ET (or 13:00 ET early close)
    assert_eq!(
        compute_rth_bar_close_ns(&bt_1d, ny_ns(2026, 10, 22, 9, 30, 0)),
        ny_ns(2026, 10, 22, 16, 0, 0)
    );
    assert_eq!(
        compute_rth_bar_close_ns(&bt_1d, ny_ns(2026, 11, 27, 9, 30, 0)),
        ny_ns(2026, 11, 27, 13, 0, 0)
    );

    // 5. `1w` bar preserves `natural_close_ns` (`bar_open_ns + 604_800_000_000_000`)
    let w_step = 604_800_000_000_000u64;
    let mon_utc_mid = utc_ns(2026, 10, 19, 0, 0, 0);
    let mon_rth_open = ny_ns(2026, 10, 19, 9, 30, 0);
    assert_eq!(
        compute_rth_bar_close_ns(&bt_1w, mon_utc_mid),
        mon_utc_mid + w_step
    );
    assert_eq!(
        compute_rth_bar_close_ns(&bt_1w, mon_rth_open),
        mon_rth_open + w_step
    );

    // 6. Post-close `1d` open never moves backwards (`close_ns > bar_open_ns`)
    for post_open in [
        ny_ns(2026, 10, 22, 16, 0, 0),
        ny_ns(2026, 10, 22, 17, 30, 0),
        ny_ns(2026, 11, 27, 13, 0, 0),
        ny_ns(2026, 11, 27, 14, 0, 0),
    ] {
        let close_ns = compute_rth_bar_close_ns(&bt_1d, post_open);
        assert!(
            close_ns > post_open,
            "post-close 1d open {post_open} must never move backwards: got close_ns={close_ns}"
        );
        assert_eq!(close_ns, post_open + 86_400_000_000_000);
    }

    // 7. Arrow batch decode of UTC-midnight `1d` bar preserves `ts_event == 16:00 ET`
    let ts_sec = i64::try_from(oct22_utc_mid / 1_000_000_000).unwrap();
    let batch = build_bars_batch(&["AAPL"], &["1d"], &[ts_sec]);
    let opts = ArrowDecodeOptions::new(
        "ALPACA",
        DataInfraTimeframe::D1,
        2,
        0,
        PriceAdjustmentMode::Adjusted,
    );
    let bars = decode_bars_batches(&[batch], &opts).unwrap();
    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].ts_event.as_u64(), ny_ns(2026, 10, 22, 16, 0, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_round4_d4_p1_3_clickhouse_fetch_bars_strict_ts_event_le_end_no_exact_open_leak() {
    // 15m bars opening at 10:00 ET, 10:15 ET, 10:30 ET:
    // - 10:00 ET -> ts_event = 10:15 ET
    // - 10:15 ET -> ts_event = 10:30 ET
    // - 10:30 ET -> ts_event = 10:45 ET (MUST be excluded when end_ts_ns == 10:30 ET!)
    let t10_00 = ny_ns(2026, 10, 22, 10, 0, 0);
    let t10_15 = ny_ns(2026, 10, 22, 10, 15, 0);
    let t10_30 = ny_ns(2026, 10, 22, 10, 30, 0);

    let m15_batch = build_bars_batch(
        &["AAPL", "AAPL", "AAPL"],
        &["15m", "15m", "15m"],
        &[
            i64::try_from(t10_00 / 1_000_000_000).unwrap(),
            i64::try_from(t10_15 / 1_000_000_000).unwrap(),
            i64::try_from(t10_30 / 1_000_000_000).unwrap(),
        ],
    );
    let m15_ipc = encode_arrow_ipc_stream(&[m15_batch]);

    // 1d UTC-midnight bars: Oct 21 (ts_event = Oct 21 16:00 ET), Oct 22 (ts_event = Oct 22 16:00 ET)
    // When end_ts_ns == Oct 22 00:00:00Z (exact open of Oct 22 bar), Oct 22 MUST be excluded!
    let oct21_mid = utc_ns(2026, 10, 21, 0, 0, 0);
    let oct22_mid = utc_ns(2026, 10, 22, 0, 0, 0);
    let d1_batch = build_bars_batch(
        &["AAPL", "AAPL"],
        &["1d", "1d"],
        &[
            i64::try_from(oct21_mid / 1_000_000_000).unwrap(),
            i64::try_from(oct22_mid / 1_000_000_000).unwrap(),
        ],
    );
    let d1_ipc = encode_arrow_ipc_stream(&[d1_batch]);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ch_addr = listener.local_addr().unwrap();
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();

    let srv = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                accept_res = listener.accept() => {
                    let Ok((mut socket, _)) = accept_res else { break; };
                    let req = read_http_req(&mut socket).await;
                    let payload = if req.contains("param_tf=1d") {
                        &d1_ipc
                    } else {
                        &m15_ipc
                    };
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apache.arrow.stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        payload.len()
                    );
                    socket.write_all(hdr.as_bytes()).await.unwrap();
                    socket.write_all(payload).await.unwrap();
                    let _ = socket.shutdown().await;
                }
            }
        }
    });

    let cfg = DataInfraConfig::new_checked(
        format!("http://{ch_addr}"),
        "market_data",
        2_000,
        0,
        vec!["127.0.0.1:9092".to_string()],
        "nautilus-datainfra-r4-test",
        "http://127.0.0.1:8012",
        "ALPACA",
    )
    .unwrap();

    // Part A: Direct `HttpClickHouseArrowTransport::fetch_bars`
    let transport = HttpClickHouseArrowTransport::new(cfg.clone()).unwrap();
    let mut req_15m = BarsQueryRequest::new(
        vec!["AAPL".to_string()],
        DataInfraTimeframe::M15,
        "ALPACA",
        PriceAdjustmentMode::Adjusted,
    );
    req_15m.start_ts_ns = Some(UnixNanos::from(t10_00));
    req_15m.end_ts_ns = Some(UnixNanos::from(t10_30)); // exact open of the 10:30..10:45 bar!
    let bars_15m = transport.fetch_bars(&req_15m).await.unwrap();
    let ts_events_15m: Vec<u64> = bars_15m.iter().map(|b| b.ts_event.as_u64()).collect();
    assert_eq!(
        ts_events_15m,
        vec![t10_15, t10_30],
        "10:30..10:45 bar (ts_event=10:45 > end_ts=10:30) must NOT leak via allow_exact_open_point"
    );

    let mut req_1d = BarsQueryRequest::new(
        vec!["AAPL".to_string()],
        DataInfraTimeframe::D1,
        "ALPACA",
        PriceAdjustmentMode::Adjusted,
    );
    req_1d.start_ts_ns = Some(UnixNanos::from(oct21_mid));
    req_1d.end_ts_ns = Some(UnixNanos::from(oct22_mid)); // exact UTC midnight open of Oct 22!
    let bars_1d = transport.fetch_bars(&req_1d).await.unwrap();
    assert_eq!(
        bars_1d.iter().map(|b| b.ts_event.as_u64()).collect::<Vec<_>>(),
        vec![ny_ns(2026, 10, 21, 16, 0, 0)],
        "Oct 22 1d bar (ts_event=16:00 ET > end_ts=00:00Z) must NOT leak when end_ts equals exact bar open"
    );

    // Part B: End-to-end via `DataInfraCatalog::query_batch` in a blocking context
    let mut catalog = DataInfraCatalog::from_config(cfg).unwrap();
    let q = CatalogQuery::new(NautilusDataType::Bar)
        .with_identifiers(Some(vec!["AAPL.ALPACA-15-MINUTE-LAST-EXTERNAL".to_string()]))
        .with_range(Some(UnixNanos::from(t10_00)), Some(UnixNanos::from(t10_30)));
    let cat_bars: Vec<Bar> = tokio::task::spawn_blocking(move || match catalog.query_batch(&q).unwrap() {
        DataBatch::Bar(b) => b.to_vec(),
        other => panic!("expected DataBatch::Bar, got {other:?}"),
    })
    .await
    .unwrap();
    assert_eq!(
        cat_bars.iter().map(|b| b.ts_event.as_u64()).collect::<Vec<_>>(),
        vec![t10_15, t10_30]
    );

    let _ = stop_tx.send(());
    srv.await.unwrap();
}
