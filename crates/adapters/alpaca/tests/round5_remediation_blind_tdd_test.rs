// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
// -------------------------------------------------------------------------------------------------

//! Blind TDD regression tests for Round 5 `nautilus-alpaca` (`D5-R5-P1-2` & `D5-R5-P1-3`):
//! - `D5-R5-P1-2`: `AlpacaHttpClient::get_order` and `AlpacaHttpClient::list_orders_paginated`
//!   send `nested=true` on the HTTP wire query string so bracket/OTO child legs are visible
//!   during reconciliation.
//! - `D5-R5-P1-3`: `ExecutionClient::modify_order` when `PATCH` returns `"status": "replaced"`
//!   with `replaced_by` follows the replacement chain, emits `OrderUpdated` for the final
//!   replacement order, AND reconciles synchronous fill/terminal state on that final order,
//!   while NOT emitting a spurious `OrderCanceled` when Alpaca `PATCH` sets `"status": "replaced"`
//!   on the new replacement order itself without `replaced_by`.

#![allow(linker_messages)]

use std::{
    cell::RefCell,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use nautilus_alpaca::{
    ALPACA_VENUE, AlpacaConfig, AlpacaCredential, AlpacaEnvironment, AlpacaExecClientConfig,
    AlpacaExecutionClient, AlpacaHttpClient,
};
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    live::runner::replace_exec_event_sender,
    messages::{ExecutionEvent, execution::ModifyOrder},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    enums::{OrderSide, TimeInForce},
    events::{OrderAccepted, OrderEventAny, OrderSubmitted},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TraderId, VenueOrderId,
    },
    orders::{LimitOrder, Order, OrderAny},
    types::{Price, Quantity},
};
use parking_lot::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn read_http_request(stream: &mut tokio::net::TcpStream) -> (String, String, String) {
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
            let headers_str = String::from_utf8_lossy(&buf[..hdr_end]);
            let content_len = headers_str
                .lines()
                .find_map(|line| {
                    let (k, v) = line.split_once(':')?;
                    if k.trim().eq_ignore_ascii_case("content-length") {
                        v.trim().parse::<usize>().ok()
                    } else {
                        None
                    }
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

    let raw = String::from_utf8_lossy(&buf).into_owned();
    let (headers_part, body_part) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
    let request_line = headers_part.lines().next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();
    (method, path, body_part.to_string())
}

async fn write_http_response(stream: &mut tokio::net::TcpStream, status_line: &str, body: &str) {
    let resp = format!(
        "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(resp.as_bytes()).await.unwrap();
    let _ = stream.shutdown().await;
}

fn build_limit_order(cid: &str, symbol: &str, side: OrderSide, qty: &str, price: &str) -> OrderAny {
    OrderAny::Limit(
        LimitOrder::new_checked(
            TraderId::from("TRADER-001"),
            StrategyId::from("STRAT-001"),
            InstrumentId::from(format!("{symbol}.ALPACA").as_str()),
            ClientOrderId::from(cid),
            side,
            Quantity::from(qty),
            Price::from(price),
            TimeInForce::Day,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
        )
        .expect("valid LimitOrder"),
    )
}

fn mark_accepted(order: &mut OrderAny, vid: &str) {
    let sub = OrderEventAny::Submitted(OrderSubmitted::new(
        order.trader_id(),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        AccountId::from("ALPACA-001"),
        UUID4::new(),
        UnixNanos::from(1_700_000_000_000_000_001u64),
        UnixNanos::from(1_700_000_000_000_000_001u64),
    ));
    order.apply(sub).expect("apply OrderSubmitted");
    let acc = OrderEventAny::Accepted(OrderAccepted::new(
        order.trader_id(),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        VenueOrderId::from(vid),
        AccountId::from("ALPACA-001"),
        UUID4::new(),
        UnixNanos::from(1_700_000_000_000_000_002u64),
        UnixNanos::from(1_700_000_000_000_000_002u64),
        false,
    ));
    order.apply(acc).expect("apply OrderAccepted");
}

async fn drain_events(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    idle_ms: u64,
) -> Vec<OrderEventAny> {
    let mut out = Vec::new();
    while let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(idle_ms), rx.recv()).await
    {
        if let ExecutionEvent::Order(oe) = ev {
            out.push(oe);
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_round5_d5_p1_2_get_order_and_list_orders_paginated_send_nested_true() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let captured_paths: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_srv = captured_paths.clone();
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        let mut list_page = 0usize;
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                accept_res = listener.accept() => {
                    let Ok((mut s, _)) = accept_res else { break; };
                    let (m, p, _) = read_http_request(&mut s).await;
                    captured_srv.lock().push(format!("{m} {p}"));
                    let route_p = p.split('?').next().unwrap_or(p.as_str());
                    match (m.as_str(), route_p) {
                        ("GET", "/v2/orders/VEN-PARENT-1") => {
                            write_http_response(
                                &mut s,
                                "200 OK",
                                r#"{"id":"VEN-PARENT-1","client_order_id":"CID-BRACKET-PARENT","created_at":"2026-03-31T15:00:00Z","symbol":"AAPL","qty":"10","filled_qty":"10","filled_avg_price":"210.00","order_class":"bracket","type":"limit","side":"buy","time_in_force":"day","limit_price":"210.00","status":"filled","legs":[{"id":"VEN-LEG-TP","client_order_id":"CID-TP","created_at":"2026-03-31T15:00:00Z","symbol":"AAPL","qty":"10","filled_qty":"0","order_class":"bracket","type":"limit","side":"sell","time_in_force":"day","limit_price":"230.00","status":"new"},{"id":"VEN-LEG-SL","client_order_id":"CID-SL","created_at":"2026-03-31T15:00:00Z","symbol":"AAPL","qty":"10","filled_qty":"0","order_class":"bracket","type":"stop","side":"sell","time_in_force":"day","stop_price":"195.00","status":"held"}]}"#,
                            ).await;
                        }
                        ("GET", "/v2/orders") => {
                            list_page += 1;
                            if list_page == 1 {
                                write_http_response(
                                    &mut s,
                                    "200 OK",
                                    r#"[{"id":"ORD-PG1-1","client_order_id":"CID-PG1-1","created_at":"2026-03-31T15:10:00Z","symbol":"NVDA","qty":"5","filled_qty":"0","order_class":"bracket","type":"limit","side":"buy","time_in_force":"day","limit_price":"140.00","status":"accepted","legs":[{"id":"ORD-PG1-SL","client_order_id":"CID-PG1-SL","created_at":"2026-03-31T15:10:00Z","symbol":"NVDA","qty":"5","filled_qty":"0","order_class":"bracket","type":"stop","side":"sell","time_in_force":"day","stop_price":"130.00","status":"held"}]},{"id":"ORD-PG1-2","client_order_id":"CID-PG1-2","created_at":"2026-03-31T15:09:00Z","symbol":"MSFT","qty":"3","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"400.00","status":"accepted"}]"#,
                                ).await;
                            } else {
                                write_http_response(
                                    &mut s,
                                    "200 OK",
                                    r#"[{"id":"ORD-PG2-1","client_order_id":"CID-PG2-1","created_at":"2026-03-31T15:08:00Z","symbol":"AMD","qty":"8","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"150.00","status":"accepted"}]"#,
                                ).await;
                            }
                        }
                        (other_m, other_p) => panic!("unexpected {other_m} {other_p}"),
                    }
                }
            }
        }
    });

    let cred = AlpacaCredential::new_checked("PKR5HTTPKEY12345", "SKR5HTTPSECRET8765").unwrap();
    let cfg = AlpacaConfig::new_checked(
        AlpacaEnvironment::Paper,
        &base_url,
        "ws://127.0.0.1:9/stream",
        cred,
        5,
        0,
        25,
        200,
        100,
        ALPACA_VENUE,
    )
    .unwrap();
    let http = AlpacaHttpClient::new(cfg).unwrap();

    // 1. get_order sends nested=true & returns nested bracket child legs
    let ord = http.get_order("VEN-PARENT-1").await.unwrap();
    assert_eq!(ord.id, "VEN-PARENT-1");
    let legs = ord.legs.as_ref().expect("nested=true must preserve child legs");
    assert_eq!(legs.len(), 2);

    // 2. list_orders_paginated sends nested=true on every paginated page
    let listed = http
        .list_orders_paginated(Some("all"), None, None, 2)
        .await
        .unwrap();
    assert_eq!(listed.len(), 3);
    assert!(
        listed[0].legs.as_ref().is_some_and(|l| l.len() == 1),
        "paginated listing with nested=true must preserve nested legs"
    );

    let paths = captured_paths.lock().clone();
    assert_eq!(paths.len(), 3, "expected 1 get_order + 2 paginated list_orders requests, got {paths:?}");
    assert!(
        paths[0].contains("nested=true"),
        "AlpacaHttpClient::get_order MUST send nested=true on wire, got {}",
        paths[0]
    );
    assert!(
        paths[1].contains("nested=true") && paths[2].contains("nested=true"),
        "AlpacaHttpClient::list_orders_paginated MUST send nested=true on ALL pages, got {paths:?}"
    );

    let _ = stop_tx.send(());
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_round5_d5_p1_3_modify_order_replaced_with_chain_reconciles_fill_and_without_replaced_by_no_spurious_cancel()
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let get_calls = Arc::new(AtomicUsize::new(0));
    let get_calls_srv = get_calls.clone();
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                accept_res = listener.accept() => {
                    let Ok((mut s, _)) = accept_res else { break; };
                    let (m, p, _) = read_http_request(&mut s).await;
                    let route_p = p.split('?').next().unwrap_or(p.as_str());
                    match (m.as_str(), route_p) {
                        // Scenario 1: PATCH returns status="replaced" WITH replaced_by -> follow chain
                        // VEN-MOD1-B (replaced -> VEN-MOD1-C) -> VEN-MOD1-C (status="filled"!)
                        ("PATCH", "/v2/orders/VEN-MOD1-A") => {
                            write_http_response(
                                &mut s,
                                "200 OK",
                                r#"{"id":"VEN-MOD1-B","replaces":"VEN-MOD1-A","replaced_by":"VEN-MOD1-C","client_order_id":"CID-MOD-FILL","created_at":"2026-03-31T15:10:00Z","symbol":"AAPL","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"212.50","status":"replaced"}"#,
                            ).await;
                        }
                        ("GET", "/v2/orders/VEN-MOD1-C") => {
                            get_calls_srv.fetch_add(1, Ordering::SeqCst);
                            write_http_response(
                                &mut s,
                                "200 OK",
                                r#"{"id":"VEN-MOD1-C","replaces":"VEN-MOD1-B","client_order_id":"CID-MOD-FILL","created_at":"2026-03-31T15:10:01Z","symbol":"AAPL","qty":"10","filled_qty":"10","filled_avg_price":"212.50","type":"limit","side":"buy","time_in_force":"day","limit_price":"212.50","status":"filled"}"#,
                            ).await;
                        }
                        // Scenario 2: PATCH returns status="replaced" WITH replaced_by -> VEN-MOD2-C (status="canceled")
                        ("PATCH", "/v2/orders/VEN-MOD2-A") => {
                            write_http_response(
                                &mut s,
                                "200 OK",
                                r#"{"id":"VEN-MOD2-B","replaces":"VEN-MOD2-A","replaced_by":"VEN-MOD2-C","client_order_id":"CID-MOD-CANC","created_at":"2026-03-31T15:11:00Z","symbol":"MSFT","qty":"5","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"402.00","status":"replaced"}"#,
                            ).await;
                        }
                        ("GET", "/v2/orders/VEN-MOD2-C") => {
                            get_calls_srv.fetch_add(1, Ordering::SeqCst);
                            write_http_response(
                                &mut s,
                                "200 OK",
                                r#"{"id":"VEN-MOD2-C","replaces":"VEN-MOD2-B","client_order_id":"CID-MOD-CANC","created_at":"2026-03-31T15:11:01Z","symbol":"MSFT","qty":"5","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"402.00","status":"canceled"}"#,
                            ).await;
                        }
                        // Scenario 3: Alpaca PATCH sets status="replaced" on the NEW order itself
                        // WITHOUT `replaced_by` -> MUST emit OrderUpdated and MUST NOT emit spurious OrderCanceled!
                        ("PATCH", "/v2/orders/VEN-MOD3-OLD") => {
                            write_http_response(
                                &mut s,
                                "200 OK",
                                r#"{"id":"VEN-MOD3-NEW","replaces":"VEN-MOD3-OLD","replaced_by":null,"client_order_id":"CID-MOD-NOSPURIOUS","created_at":"2026-03-31T15:12:00Z","symbol":"NVDA","qty":"8","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"142.00","status":"replaced"}"#,
                            ).await;
                        }
                        (other_m, other_p) => panic!("unexpected {other_m} {other_p}"),
                    }
                }
            }
        }
    });

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
    replace_exec_event_sender(tx.clone());
    let cache = Rc::new(RefCell::new(Cache::default()));
    let cred = AlpacaCredential::new_checked("PKR5EXECKEY12345", "SKR5EXECSECRET8765").unwrap();
    let cfg = AlpacaExecClientConfig::new_checked(
        AlpacaEnvironment::Paper,
        &base_url,
        "ws://127.0.0.1:9/stream",
        cred,
        5,
        0,
        25,
        200,
        100,
        ALPACA_VENUE,
    )
    .unwrap();
    let mut client = AlpacaExecutionClient::new(
        TraderId::from("TRADER-001"),
        ClientId::from(ALPACA_VENUE),
        AccountId::from("ALPACA-001"),
        cfg,
        cache.clone(),
    )
    .unwrap()
    .with_ws_enabled(false);
    client.set_sender(tx);

    let mut ord1 = build_limit_order("CID-MOD-FILL", "AAPL", OrderSide::Buy, "10", "210.00");
    mark_accepted(&mut ord1, "VEN-MOD1-A");
    let mut ord2 = build_limit_order("CID-MOD-CANC", "MSFT", OrderSide::Buy, "5", "400.00");
    mark_accepted(&mut ord2, "VEN-MOD2-A");
    let mut ord3 = build_limit_order("CID-MOD-NOSPURIOUS", "NVDA", OrderSide::Buy, "8", "140.00");
    mark_accepted(&mut ord3, "VEN-MOD3-OLD");
    {
        let mut c = cache.borrow_mut();
        c.add_order(ord1, None, Some(ClientId::from(ALPACA_VENUE)), false).unwrap();
        c.add_order(ord2, None, Some(ClientId::from(ALPACA_VENUE)), false).unwrap();
        c.add_order(ord3, None, Some(ClientId::from(ALPACA_VENUE)), false).unwrap();
    }

    // 1. Replaced with chain -> final order FILLED
    client
        .modify_order(ModifyOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from(ALPACA_VENUE)),
            StrategyId::from("STRAT-001"),
            InstrumentId::from("AAPL.ALPACA"),
            ClientOrderId::from("CID-MOD-FILL"),
            Some(VenueOrderId::from("VEN-MOD1-A")),
            Some(Quantity::from("10")),
            Some(Price::from("212.50")),
            None,
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            None,
            None,
        ))
        .unwrap();
    let evs1 = drain_events(&mut rx, 250).await;
    assert!(
        evs1.iter().any(|e| matches!(
            e,
            OrderEventAny::Updated(u)
                if u.client_order_id == ClientOrderId::from("CID-MOD-FILL")
                    && u.venue_order_id == Some(VenueOrderId::from("VEN-MOD1-C"))
        )),
        "modify_order must follow chain to VEN-MOD1-C and emit OrderUpdated, got {evs1:?}"
    );
    assert!(
        evs1.iter().any(|e| matches!(
            e,
            OrderEventAny::Filled(f)
                if f.client_order_id == ClientOrderId::from("CID-MOD-FILL")
                    && f.venue_order_id == VenueOrderId::from("VEN-MOD1-C")
                    && f.last_qty == Quantity::from("10")
        )),
        "modify_order MUST reconcile synchronous fill on final replaced order VEN-MOD1-C, got {evs1:?}"
    );
    assert!(
        !evs1.iter().any(|e| matches!(e, OrderEventAny::Canceled(_))),
        "filled replacement chain must not emit spurious OrderCanceled, got {evs1:?}"
    );

    // 2. Replaced with chain -> final order CANCELED
    client
        .modify_order(ModifyOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from(ALPACA_VENUE)),
            StrategyId::from("STRAT-001"),
            InstrumentId::from("MSFT.ALPACA"),
            ClientOrderId::from("CID-MOD-CANC"),
            Some(VenueOrderId::from("VEN-MOD2-A")),
            Some(Quantity::from("5")),
            Some(Price::from("402.00")),
            None,
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            None,
            None,
        ))
        .unwrap();
    let evs2 = drain_events(&mut rx, 250).await;
    assert!(
        evs2.iter().any(|e| matches!(
            e,
            OrderEventAny::Updated(u)
                if u.client_order_id == ClientOrderId::from("CID-MOD-CANC")
                    && u.venue_order_id == Some(VenueOrderId::from("VEN-MOD2-C"))
        )),
        "modify_order must emit OrderUpdated(VEN-MOD2-C), got {evs2:?}"
    );
    assert!(
        evs2.iter().any(|e| matches!(
            e,
            OrderEventAny::Canceled(c)
                if c.client_order_id == ClientOrderId::from("CID-MOD-CANC")
                    && c.venue_order_id == Some(VenueOrderId::from("VEN-MOD2-C"))
        )),
        "modify_order must reconcile terminal canceled state on final VEN-MOD2-C, got {evs2:?}"
    );

    // 3. Alpaca PATCH sets status="replaced" on the NEW order itself with NO replaced_by:
    //    MUST emit OrderUpdated(VEN-MOD3-NEW) and MUST NOT emit spurious OrderCanceled!
    client
        .modify_order(ModifyOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from(ALPACA_VENUE)),
            StrategyId::from("STRAT-001"),
            InstrumentId::from("NVDA.ALPACA"),
            ClientOrderId::from("CID-MOD-NOSPURIOUS"),
            Some(VenueOrderId::from("VEN-MOD3-OLD")),
            Some(Quantity::from("8")),
            Some(Price::from("142.00")),
            None,
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            None,
            None,
        ))
        .unwrap();
    let evs3 = drain_events(&mut rx, 250).await;
    assert!(
        evs3.iter().any(|e| matches!(
            e,
            OrderEventAny::Updated(u)
                if u.client_order_id == ClientOrderId::from("CID-MOD-NOSPURIOUS")
                    && u.venue_order_id == Some(VenueOrderId::from("VEN-MOD3-NEW"))
        )),
        "modify_order with status='replaced' & replaced_by=None MUST emit OrderUpdated(VEN-MOD3-NEW), got {evs3:?}"
    );
    assert!(
        !evs3.iter().any(|e| matches!(e, OrderEventAny::Canceled(_))),
        "modify_order with status='replaced' & replaced_by=None MUST NOT emit spurious OrderCanceled, got {evs3:?}"
    );
    assert!(
        !evs3.iter().any(|e| matches!(e, OrderEventAny::ModifyRejected(_))),
        "must not emit ModifyRejected, got {evs3:?}"
    );

    assert_eq!(get_calls.load(Ordering::SeqCst), 2);
    let _ = stop_tx.send(());
    server.await.unwrap();
}
