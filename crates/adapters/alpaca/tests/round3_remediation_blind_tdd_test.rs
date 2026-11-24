// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
// -------------------------------------------------------------------------------------------------

//! Blind behavioral regression tests for Round 3 `nautilus-alpaca` fixes (`P1-22` & execution invariants):
//! - `P1-22`: `cancel_all_orders` when `list_open_orders` omits locally tracked
//!   submitted/accepted/venue-ID orders STILL queries `get_order` / `get_order_by_client_order_id`
//!   for unmatched live `fallback_orders` (testing `filled`, still-open active cancel, and
//!   lookup-error -> `OrderCancelRejected`), while NOT querying purely unsubmitted
//!   `OrderStatus::Initialized` local orders with no `VenueOrderId`.
//! - OCO `submit_order_list` with `[StopMarket, Limit]` leg ordering (`top_idx = 1`) maps
//!   top-level `client_order_id` and `OrderAccepted` events to the TP `Limit` leg and child
//!   leg to the SL `StopMarket` leg.
//! - Unparented `is_reduce_only` stop order without an open cached position is rejected
//!   fail-closed without submitting to HTTP `POST /v2/orders`.

#![allow(linker_messages)]

use std::{cell::RefCell, rc::Rc, sync::Arc, time::Duration};

use nautilus_alpaca::{
    ALPACA_VENUE, AlpacaCredential, AlpacaEnvironment, AlpacaExecClientConfig,
    AlpacaExecutionClient,
};
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    live::runner::replace_exec_event_sender,
    messages::{
        ExecutionEvent,
        execution::{CancelAllOrders, SubmitOrder, SubmitOrderList},
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    enums::{
        ContingencyType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType, TimeInForce,
        TriggerType,
    },
    events::{OrderAccepted, OrderEventAny, OrderFilled, OrderSubmitted},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, PositionId, StrategyId,
        Symbol, TradeId, TraderId, VenueOrderId,
    },
    instruments::{Equity, InstrumentAny},
    orders::{LimitOrder, Order, OrderAny, OrderList, StopMarketOrder},
    position::Position,
    types::{Currency, Money, Price, Quantity},
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

async fn write_http_response(
    stream: &mut tokio::net::TcpStream,
    status_line: &str,
    body: &str,
) {
    let resp = format!(
        "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(resp.as_bytes()).await.unwrap();
    let _ = stream.shutdown().await;
}

fn make_test_config(http_base_url: &str) -> AlpacaExecClientConfig {
    let cred = AlpacaCredential::new_checked("PKR3TESTKEY12345", "SKR3TESTSECRET87654")
        .expect("valid test credential");
    AlpacaExecClientConfig::new_checked(
        AlpacaEnvironment::Paper,
        http_base_url,
        "ws://127.0.0.1:9/stream",
        cred,
        5,
        0,
        25,
        200,
        100,
        ALPACA_VENUE,
    )
    .expect("valid test config")
}

fn build_limit_order(
    cid: &str,
    symbol: &str,
    side: OrderSide,
    qty: &str,
    price: &str,
    contingency: Option<ContingencyType>,
    order_list_id: Option<OrderListId>,
    linked: Option<Vec<ClientOrderId>>,
) -> OrderAny {
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
            contingency,
            order_list_id,
            linked,
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

fn build_stop_order(
    cid: &str,
    symbol: &str,
    side: OrderSide,
    qty: &str,
    stop_price: &str,
    reduce_only: bool,
    contingency: Option<ContingencyType>,
    order_list_id: Option<OrderListId>,
    linked: Option<Vec<ClientOrderId>>,
) -> OrderAny {
    OrderAny::StopMarket(
        StopMarketOrder::new_checked(
            TraderId::from("TRADER-001"),
            StrategyId::from("STRAT-001"),
            InstrumentId::from(format!("{symbol}.ALPACA").as_str()),
            ClientOrderId::from(cid),
            side,
            Quantity::from(qty),
            Price::from(stop_price),
            TriggerType::Default,
            TimeInForce::Day,
            None,
            reduce_only,
            false,
            None,
            None,
            None,
            contingency,
            order_list_id,
            linked,
            None,
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
        )
        .expect("valid StopMarketOrder"),
    )
}

fn mark_submitted(order: &mut OrderAny) {
    let ev = OrderEventAny::Submitted(OrderSubmitted::new(
        order.trader_id(),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        AccountId::from("ALPACA-001"),
        UUID4::new(),
        UnixNanos::from(1_700_000_000_000_000_001u64),
        UnixNanos::from(1_700_000_000_000_000_001u64),
    ));
    order.apply(ev).expect("apply OrderSubmitted");
}

fn mark_accepted(order: &mut OrderAny, vid: &str) {
    mark_submitted(order);
    let ev = OrderEventAny::Accepted(OrderAccepted::new(
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
    order.apply(ev).expect("apply OrderAccepted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_round3_p1_22_cancel_all_orders_fallback_reconciliation_and_initialized_skip() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let recorded_requests: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let recorded_srv = recorded_requests.clone();
    let open_cancel_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let open_cancel_srv = open_cancel_count.clone();
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                accept_res = listener.accept() => {
                    let Ok((mut s, _)) = accept_res else { break; };
                    let (m, p, _) = read_http_request(&mut s).await;
                    recorded_srv.lock().push((m.clone(), p.clone()));
                    let route_p = if m == "GET" && p.starts_with("/v2/orders/") {
                        p.split('?').next().unwrap_or(p.as_str())
                    } else {
                        p.as_str()
                    };
                    match (m.as_str(), route_p) {
                        // list_open_orders omits all AAPL local orders (returns empty list)
                        ("GET", path) if path.starts_with("/v2/orders?") => {
                            write_http_response(&mut s, "200 OK", "[]").await;
                        }
                        // 1) Submitted order with NO VenueOrderId -> filled on venue
                        ("GET", path) if path == "/v2/orders:by_client_order_id?client_order_id=CID-FB-SUB-FILL&nested=true" => {
                            write_http_response(
                                &mut s,
                                "200 OK",
                                r#"{"id":"VEN-FB-SUB-FILL","client_order_id":"CID-FB-SUB-FILL","created_at":"2026-03-31T15:00:00Z","filled_at":"2026-03-31T15:00:05Z","symbol":"AAPL","qty":"10","filled_qty":"10","filled_avg_price":"215.50","type":"limit","side":"buy","time_in_force":"day","limit_price":"216.00","status":"filled"}"#,
                            )
                            .await;
                        }
                        // 2) Accepted order WITH VenueOrderId -> still open on venue -> cancel & confirm canceled
                        ("GET", "/v2/orders/VEN-FB-ACC-OPEN") => {
                            let body = if open_cancel_srv.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                                r#"{"id":"VEN-FB-ACC-OPEN","client_order_id":"CID-FB-ACC-OPEN","created_at":"2026-03-31T15:00:01Z","symbol":"AAPL","qty":"15","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"210.00","status":"accepted"}"#
                            } else {
                                r#"{"id":"VEN-FB-ACC-OPEN","client_order_id":"CID-FB-ACC-OPEN","created_at":"2026-03-31T15:00:01Z","canceled_at":"2026-03-31T15:00:06Z","symbol":"AAPL","qty":"15","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"210.00","status":"canceled"}"#
                            };
                            write_http_response(&mut s, "200 OK", body).await;
                        }
                        ("DELETE", "/v2/orders/VEN-FB-ACC-OPEN") => {
                            open_cancel_srv.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            write_http_response(&mut s, "204 No Content", "").await;
                        }
                        // 3) Submitted order with NO VenueOrderId -> lookup error (404) -> OrderCancelRejected
                        ("GET", path) if path == "/v2/orders:by_client_order_id?client_order_id=CID-FB-SUB-ERR&nested=true" => {
                            write_http_response(
                                &mut s,
                                "404 Not Found",
                                r#"{"message":"order not found"}"#,
                            )
                            .await;
                        }
                        // 4) Accepted order WITH VenueOrderId -> lookup error (404) -> OrderCancelRejected
                        ("GET", "/v2/orders/VEN-FB-ACC-ERR") => {
                            write_http_response(
                                &mut s,
                                "404 Not Found",
                                r#"{"message":"order not found"}"#,
                            )
                            .await;
                        }
                        // 5) Initialized order WITH recorded VenueOrderId -> still queried & canceled
                        ("GET", "/v2/orders/VEN-FB-INIT-VID") => {
                            write_http_response(
                                &mut s,
                                "200 OK",
                                r#"{"id":"VEN-FB-INIT-VID","client_order_id":"CID-FB-INIT-VID","created_at":"2026-03-31T15:00:02Z","canceled_at":"2026-03-31T15:00:07Z","symbol":"AAPL","qty":"5","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"205.00","status":"canceled"}"#,
                            )
                            .await;
                        }
                        // 6) Purely unsubmitted Initialized order with NO VenueOrderId MUST NOT be queried
                        (_, path) if path.contains("CID-FB-INIT-LOCAL") => {
                            panic!("purely unsubmitted OrderStatus::Initialized order without VenueOrderId must NOT trigger HTTP lookup: {m} {path}");
                        }
                        (_, path) if path.contains("MSFT") => {
                            panic!("other instrument MSFT order must NOT be queried: {m} {path}");
                        }
                        (other_m, other_p) => {
                            panic!("unexpected HTTP request {other_m} {other_p}");
                        }
                    }
                }
            }
        }
    });

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
    replace_exec_event_sender(tx.clone());

    let cache = Rc::new(RefCell::new(Cache::default()));
    let mut client = AlpacaExecutionClient::new(
        TraderId::from("TRADER-001"),
        ClientId::from(ALPACA_VENUE),
        AccountId::from("ALPACA-001"),
        make_test_config(&base_url),
        cache.clone(),
    )
    .unwrap()
    .with_ws_enabled(false);
    client.set_sender(tx);

    let mut ord_sub_fill = build_limit_order(
        "CID-FB-SUB-FILL",
        "AAPL",
        OrderSide::Buy,
        "10",
        "216.00",
        None,
        None,
        None,
    );
    mark_submitted(&mut ord_sub_fill);
    assert_eq!(ord_sub_fill.status(), OrderStatus::Submitted);
    assert!(ord_sub_fill.venue_order_id().is_none());

    let mut ord_acc_open = build_limit_order(
        "CID-FB-ACC-OPEN",
        "AAPL",
        OrderSide::Buy,
        "15",
        "210.00",
        None,
        None,
        None,
    );
    mark_accepted(&mut ord_acc_open, "VEN-FB-ACC-OPEN");

    let mut ord_sub_err = build_limit_order(
        "CID-FB-SUB-ERR",
        "AAPL",
        OrderSide::Buy,
        "8",
        "212.00",
        None,
        None,
        None,
    );
    mark_submitted(&mut ord_sub_err);

    let mut ord_acc_err = build_limit_order(
        "CID-FB-ACC-ERR",
        "AAPL",
        OrderSide::Buy,
        "12",
        "211.00",
        None,
        None,
        None,
    );
    mark_accepted(&mut ord_acc_err, "VEN-FB-ACC-ERR");

    let ord_init_with_vid = build_limit_order(
        "CID-FB-INIT-VID",
        "AAPL",
        OrderSide::Buy,
        "5",
        "205.00",
        None,
        None,
        None,
    );
    assert_eq!(ord_init_with_vid.status(), OrderStatus::Initialized);

    let ord_init_local = build_limit_order(
        "CID-FB-INIT-LOCAL",
        "AAPL",
        OrderSide::Buy,
        "20",
        "200.00",
        None,
        None,
        None,
    );
    assert_eq!(ord_init_local.status(), OrderStatus::Initialized);
    assert!(ord_init_local.venue_order_id().is_none());

    let mut ord_msft = build_limit_order(
        "CID-FB-MSFT",
        "MSFT",
        OrderSide::Buy,
        "10",
        "400.00",
        None,
        None,
        None,
    );
    mark_submitted(&mut ord_msft);

    {
        let mut c = cache.borrow_mut();
        for o in [
            ord_sub_fill,
            ord_acc_open,
            ord_sub_err,
            ord_acc_err,
            ord_init_with_vid,
            ord_init_local,
            ord_msft,
        ] {
            c.add_order(o, None, Some(ClientId::from(ALPACA_VENUE)), false)
                .unwrap();
        }
    }
    client.record_venue_order_id(
        ClientOrderId::from("CID-FB-INIT-VID"),
        VenueOrderId::from("VEN-FB-INIT-VID"),
    );

    client
        .cancel_all_orders(CancelAllOrders::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from(ALPACA_VENUE)),
            StrategyId::from("STRAT-001"),
            InstrumentId::from("AAPL.ALPACA"),
            None,
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            None,
            None,
        ))
        .unwrap();

    // Expected events (6 total):
    // 1. CID-FB-SUB-FILL -> OrderFilled + OrderCancelRejected("order already filled on venue before cancel completed")
    // 2. CID-FB-ACC-OPEN -> OrderCanceled
    // 3. CID-FB-SUB-ERR -> OrderCancelRejected("cancel_all_orders unmatched local order reconciliation failed: ...")
    // 4. CID-FB-ACC-ERR -> OrderCancelRejected("cancel_all_orders unmatched local order reconciliation failed: ...")
    // 5. CID-FB-INIT-VID -> OrderCanceled
    // (CID-FB-INIT-LOCAL -> 0 events, 0 HTTP queries)
    let mut filled_cids = Vec::new();
    let mut canceled_cids = Vec::new();
    let mut cancel_rejected: Vec<(String, Option<VenueOrderId>, String)> = Vec::new();

    for _ in 0..6 {
        let ev = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for unmatched fallback cancel_all_orders events")
            .unwrap();
        match ev {
            ExecutionEvent::Order(OrderEventAny::Filled(f)) => {
                filled_cids.push(f.client_order_id.to_string());
            }
            ExecutionEvent::Order(OrderEventAny::Canceled(c)) => {
                canceled_cids.push(c.client_order_id.to_string());
            }
            ExecutionEvent::Order(OrderEventAny::CancelRejected(r)) => {
                cancel_rejected.push((
                    r.client_order_id.to_string(),
                    r.venue_order_id,
                    r.reason.to_string(),
                ));
            }
            other => panic!("unexpected ExecutionEvent: {other:?}"),
        }
    }
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(
        rx.try_recv().is_err(),
        "unexpected extra event (Initialized local-only order must emit zero events)"
    );

    filled_cids.sort();
    assert_eq!(filled_cids, vec!["CID-FB-SUB-FILL".to_string()]);

    canceled_cids.sort();
    assert_eq!(
        canceled_cids,
        vec![
            "CID-FB-ACC-OPEN".to_string(),
            "CID-FB-INIT-VID".to_string()
        ]
    );

    cancel_rejected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(cancel_rejected.len(), 3);
    assert_eq!(cancel_rejected[0].0, "CID-FB-ACC-ERR");
    assert_eq!(
        cancel_rejected[0].1,
        Some(VenueOrderId::from("VEN-FB-ACC-ERR"))
    );
    assert!(
        cancel_rejected[0]
            .2
            .contains("cancel_all_orders unmatched local order reconciliation failed"),
        "unexpected acc_err reason: {}",
        cancel_rejected[0].2
    );

    assert_eq!(cancel_rejected[1].0, "CID-FB-SUB-ERR");
    assert_eq!(cancel_rejected[1].1, None);
    assert!(
        cancel_rejected[1]
            .2
            .contains("cancel_all_orders unmatched local order reconciliation failed"),
        "unexpected sub_err reason: {}",
        cancel_rejected[1].2
    );

    assert_eq!(cancel_rejected[2].0, "CID-FB-SUB-FILL");
    assert_eq!(
        cancel_rejected[2].1,
        Some(VenueOrderId::from("VEN-FB-SUB-FILL"))
    );
    assert!(
        cancel_rejected[2]
            .2
            .contains("order already filled on venue before cancel completed"),
        "unexpected sub_fill reason: {}",
        cancel_rejected[2].2
    );

    let reqs = recorded_requests.lock().clone();
    assert!(
        !reqs.iter().any(|(_, p)| p.contains("CID-FB-INIT-LOCAL")),
        "purely unsubmitted OrderStatus::Initialized order must never trigger HTTP lookup: {reqs:?}"
    );
    assert!(
        reqs.iter().any(|(m, p)| m == "DELETE" && p == "/v2/orders/VEN-FB-ACC-OPEN"),
        "unmatched still-open Accepted order must be actively cancelled via DELETE: {reqs:?}"
    );

    let _ = stop_tx.send(());
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_round3_oco_sl_then_tp_leg_order_and_unparented_reduce_only_stop_guard() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let post_bodies: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let post_bodies_srv = post_bodies.clone();
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                accept_res = listener.accept() => {
                    let Ok((mut s, _)) = accept_res else { break; };
                    let (m, p, body) = read_http_request(&mut s).await;
                    assert_eq!(m, "POST");
                    assert_eq!(p, "/v2/orders");
                    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
                    post_bodies_srv.lock().push(parsed);
                    write_http_response(
                        &mut s,
                        "200 OK",
                        r#"{
                            "id":"VEN-OCO-TP-TOP",
                            "client_order_id":"CID-OCO-TP",
                            "created_at":"2026-03-31T15:00:00Z",
                            "submitted_at":"2026-03-31T15:00:00Z",
                            "symbol":"NVDA",
                            "qty":"25",
                            "filled_qty":"0",
                            "order_class":"oco",
                            "type":"limit",
                            "side":"sell",
                            "time_in_force":"day",
                            "limit_price":"160.00",
                            "status":"accepted",
                            "legs":[
                                {
                                    "id":"VEN-OCO-SL-LEG",
                                    "client_order_id":"CID-OCO-SL",
                                    "created_at":"2026-03-31T15:00:00Z",
                                    "submitted_at":"2026-03-31T15:00:00Z",
                                    "symbol":"NVDA",
                                    "qty":"25",
                                    "filled_qty":"0",
                                    "order_class":"oco",
                                    "type":"stop",
                                    "side":"sell",
                                    "time_in_force":"day",
                                    "stop_price":"135.00",
                                    "status":"held"
                                }
                            ]
                        }"#,
                    )
                    .await;
                }
            }
        }
    });

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
    replace_exec_event_sender(tx.clone());

    let cache = Rc::new(RefCell::new(Cache::default()));
    let mut client = AlpacaExecutionClient::new(
        TraderId::from("TRADER-001"),
        ClientId::from(ALPACA_VENUE),
        AccountId::from("ALPACA-001"),
        make_test_config(&base_url),
        cache.clone(),
    )
    .unwrap()
    .with_ws_enabled(false);
    client.set_sender(tx);

    // 1. Unparented reduce_only StopMarket when NVDA.ALPACA has a closed position in Cache
    //    -> MUST emit OrderDenied locally without touching HTTP POST /v2/orders.
    {
        let mut c = cache.borrow_mut();
        let nvda_inst = InstrumentAny::Equity(
            Equity::builder()
                .instrument_id(InstrumentId::from("NVDA.ALPACA"))
                .raw_symbol(Symbol::from("NVDA"))
                .currency(Currency::USD())
                .price_precision(2)
                .price_increment(Price::from("0.01"))
                .lot_size(Quantity::from("1"))
                .min_quantity(Quantity::from("1"))
                .ts_event(UnixNanos::from(1_700_000_000_000_000_000u64))
                .ts_init(UnixNanos::from(1_700_000_000_000_000_000u64))
                .build()
                .expect("valid Equity instrument"),
        );
        c.add_instrument(nvda_inst.clone()).unwrap();

        let make_fill = |cid: &str, vid: &str, tid: &str, side: OrderSide, px: &str| {
            OrderFilled::new(
                TraderId::from("TRADER-001"),
                StrategyId::from("STRAT-001"),
                InstrumentId::from("NVDA.ALPACA"),
                ClientOrderId::from(cid),
                VenueOrderId::from(vid),
                AccountId::from("ALPACA-001"),
                TradeId::from(tid),
                side,
                OrderType::Market,
                Quantity::from("10"),
                Price::from(px),
                Currency::USD(),
                LiquiditySide::Taker,
                UUID4::new(),
                UnixNanos::from(1_700_000_000_000_000_000u64),
                UnixNanos::from(1_700_000_000_000_000_000u64),
                false,
                Some(PositionId::new("POS-NVDA-CLOSED")),
                Some(Money::from("0.00 USD")),
                None,
            )
        };
        let mut pos = Position::new(
            &nvda_inst,
            make_fill("CID-NVDA-OPEN", "VEN-NVDA-OPEN", "TRD-1", OrderSide::Buy, "140.00"),
        );
        c.add_position_without_order(&pos, OmsType::Netting).unwrap();
        pos.apply(&make_fill(
            "CID-NVDA-CLOSE",
            "VEN-NVDA-CLOSE",
            "TRD-2",
            OrderSide::Sell,
            "145.00",
        ));
        c.update_position(&pos).unwrap();
    }

    let orphan_stop = build_stop_order(
        "CID-ORPHAN-STOP",
        "NVDA",
        OrderSide::Sell,
        "10",
        "135.00",
        true,
        None,
        None,
        None,
    );
    {
        cache
            .borrow_mut()
            .add_order(
                orphan_stop.clone(),
                None,
                Some(ClientId::from(ALPACA_VENUE)),
                false,
            )
            .unwrap();
    }
    client
        .submit_order(SubmitOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from(ALPACA_VENUE)),
            StrategyId::from("STRAT-001"),
            InstrumentId::from("NVDA.ALPACA"),
            ClientOrderId::from("CID-ORPHAN-STOP"),
            orphan_stop.init_event().clone(),
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            None,
        ))
        .unwrap();

    let ev_denied = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("expected OrderDenied for unparented reduce-only stop on closed position")
        .unwrap();
    match ev_denied {
        ExecutionEvent::Order(OrderEventAny::Denied(d)) => {
            assert_eq!(d.client_order_id, ClientOrderId::from("CID-ORPHAN-STOP"));
            assert!(
                d.reason.as_str().contains("REDUCE_ONLY_POSITION_CLOSED"),
                "unexpected denial reason: {}",
                d.reason
            );
        }
        other => panic!("expected OrderDenied, got {other:?}"),
    }
    assert!(
        post_bodies.lock().is_empty(),
        "unparented reduce-only stop guard must deny before HTTP POST /v2/orders"
    );

    // 2. OCO OrderList passed as [SL StopMarket, TP Limit] -> top-level payload & VenueOrderId
    //    must map to TP Limit leg (`CID-OCO-TP`), and child leg to SL StopMarket (`CID-OCO-SL`)!
    let olid = OrderListId::from("OL-OCO-SL-FIRST");
    let cid_sl = ClientOrderId::from("CID-OCO-SL");
    let cid_tp = ClientOrderId::from("CID-OCO-TP");
    let sl_leg = build_stop_order(
        "CID-OCO-SL",
        "NVDA",
        OrderSide::Sell,
        "25",
        "135.00",
        false,
        Some(ContingencyType::Oco),
        Some(olid),
        Some(vec![cid_tp]),
    );
    let tp_leg = build_limit_order(
        "CID-OCO-TP",
        "NVDA",
        OrderSide::Sell,
        "25",
        "160.00",
        Some(ContingencyType::Oco),
        Some(olid),
        Some(vec![cid_sl]),
    );
    let init_events = vec![sl_leg.init_event().clone(), tp_leg.init_event().clone()];
    let order_list = OrderList::new(
        olid,
        InstrumentId::from("NVDA.ALPACA"),
        StrategyId::from("STRAT-001"),
        vec![cid_sl, cid_tp],
        UnixNanos::from(1_700_000_000_000_000_000u64),
    );
    {
        let mut c = cache.borrow_mut();
        c.add_order(sl_leg, None, Some(ClientId::from(ALPACA_VENUE)), false)
            .unwrap();
        c.add_order(tp_leg, None, Some(ClientId::from(ALPACA_VENUE)), false)
            .unwrap();
        c.add_order_list(order_list.clone()).unwrap();
    }
    client
        .submit_order_list(SubmitOrderList::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from(ALPACA_VENUE)),
            StrategyId::from("STRAT-001"),
            order_list,
            init_events,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            None,
        ))
        .unwrap();

    // Expect 2 OrderSubmitted + 2 OrderAccepted
    let mut accepted = Vec::new();
    for _ in 0..4 {
        let ev = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("expected OCO order lifecycle event")
            .unwrap();
        if let ExecutionEvent::Order(OrderEventAny::Accepted(a)) = ev {
            accepted.push((a.client_order_id.to_string(), a.venue_order_id.to_string()));
        }
    }
    accepted.sort();
    assert_eq!(
        accepted,
        vec![
            ("CID-OCO-SL".to_string(), "VEN-OCO-SL-LEG".to_string()),
            ("CID-OCO-TP".to_string(), "VEN-OCO-TP-TOP".to_string()),
        ],
        "OCO [StopMarket, Limit] list must map top-level response to TP Limit leg and leg[0] to SL StopMarket leg"
    );

    let bodies = post_bodies.lock().clone();
    assert_eq!(bodies.len(), 1);
    assert_eq!(bodies[0]["order_class"], "oco");
    assert_eq!(
        bodies[0]["client_order_id"], "CID-OCO-TP",
        "OCO HTTP payload must use TP Limit leg's client_order_id even when SL is passed first"
    );

    let _ = stop_tx.send(());
    server.await.unwrap();
}
