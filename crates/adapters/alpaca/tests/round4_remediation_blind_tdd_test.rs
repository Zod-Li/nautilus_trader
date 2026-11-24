// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
// -------------------------------------------------------------------------------------------------

//! Blind behavioral regression tests for Round 4 `nautilus-alpaca` fixes (`D5-P1-7` & `D5-P1-8`):
//! - `D5-P1-7`: Multi-hop `replaced_by` chain (`A -> B -> C` and cycle guard) across
//!   `reconcile_tracked_orders_once` (`reconcile_open_orders`), `modify_order`, `cancel_order`
//!   (`DeterministicReject` + transport error), and `cancel_all_orders` (including skipping
//!   duplicate `DELETE` when terminal `new_vid` is already in `matched_fallback_vids`).
//! - `D5-P1-8`: `get_order_by_client_order_id` passes `nested=true`; `submit_order_list` probes
//!   `GET /v2/orders/{id}` once when `legs` are missing/short, and if a leg is still absent
//!   leaves the child in `Submitted` state without fabricating `"{id}-LEG-{idx}"` VenueOrderIds.

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
    messages::{
        ExecutionEvent,
        execution::{CancelAllOrders, CancelOrder, ModifyOrder, SubmitOrder, SubmitOrderList},
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    enums::{ContingencyType, OrderSide, TimeInForce, TriggerType},
    events::{OrderAccepted, OrderEventAny, OrderSubmitted},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, StrategyId, TraderId,
        VenueOrderId,
    },
    orders::{LimitOrder, Order, OrderAny, OrderList, StopMarketOrder},
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
    let cred = AlpacaCredential::new_checked("PKR4TESTKEY12345", "SKR4TESTSECRET87654")
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
        .expect("valid StopMarketOrder"),
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
async fn test_round4_d5_p1_7_multihop_replaced_by_and_cycle_guard_across_reconcile_modify_cancel() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let cycle_gets = Arc::new(AtomicUsize::new(0));
    let cycle_gets_srv = cycle_gets.clone();
    let can2_canceled = Arc::new(AtomicUsize::new(0));
    let can2_srv = can2_canceled.clone();
    let can3_canceled = Arc::new(AtomicUsize::new(0));
    let can3_srv = can3_canceled.clone();
    let delete_paths: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let del_srv = delete_paths.clone();
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                accept_res = listener.accept() => {
                    let Ok((mut s, _)) = accept_res else { break; };
                    let (m, p, _) = read_http_request(&mut s).await;
                    if m == "DELETE" {
                        del_srv.lock().push(p.clone());
                    }
                    let route_p = if m == "GET" && p.starts_with("/v2/orders/") {
                        p.split('?').next().unwrap_or(p.as_str())
                    } else {
                        p.as_str()
                    };
                    match (m.as_str(), route_p) {
                        // 1) reconcile_tracked_orders_once: A -> B -> C (accepted)
                        ("POST", "/v2/orders") => {
                            write_http_response(
                                &mut s,
                                "200 OK",
                                r#"{"id":"VEN-REC-A","client_order_id":"CID-REC-CHAIN","created_at":"2026-03-31T15:00:00Z","symbol":"AAPL","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"210.00","status":"accepted"}"#,
                            ).await;
                        }
                        ("GET", "/v2/orders/VEN-REC-A") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-REC-A","replaced_by":"VEN-REC-B","client_order_id":"CID-REC-CHAIN","created_at":"2026-03-31T15:00:00Z","symbol":"AAPL","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"210.00","status":"replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-REC-B") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-REC-B","replaces":"VEN-REC-A","replaced_by":"VEN-REC-C","client_order_id":"CID-REC-CHAIN-2","created_at":"2026-03-31T15:00:01Z","symbol":"AAPL","qty":"11","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"211.00","status":"replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-REC-C") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-REC-C","replaces":"VEN-REC-B","client_order_id":"CID-REC-CHAIN-3","created_at":"2026-03-31T15:00:02Z","symbol":"AAPL","qty":"12","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"212.00","status":"accepted"}"#).await;
                        }

                        // 2) modify_order: VEN-MOD-A (422) -> B -> C (accepted)
                        ("PATCH", "/v2/orders/VEN-MOD-A") => {
                            write_http_response(&mut s, "422 Unprocessable Entity", r#"{"message":"order cannot be replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-MOD-A") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-MOD-A","replaced_by":"VEN-MOD-B","client_order_id":"CID-MOD-CHAIN","created_at":"2026-03-31T15:01:00Z","symbol":"MSFT","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"400.00","status":"replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-MOD-B") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-MOD-B","replaced_by":"VEN-MOD-C","client_order_id":"CID-MOD-CHAIN-2","created_at":"2026-03-31T15:01:01Z","symbol":"MSFT","qty":"15","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"405.00","status":"replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-MOD-C") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-MOD-C","client_order_id":"CID-MOD-CHAIN-3","created_at":"2026-03-31T15:01:02Z","symbol":"MSFT","qty":"15","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"405.00","status":"accepted"}"#).await;
                        }

                        // 2b) cycle guard on modify_order: VEN-CYC-A -> VEN-CYC-B -> VEN-CYC-A
                        ("PATCH", "/v2/orders/VEN-CYC-A") => {
                            write_http_response(&mut s, "422 Unprocessable Entity", r#"{"message":"order cannot be replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-CYC-A") => {
                            cycle_gets_srv.fetch_add(1, Ordering::SeqCst);
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-CYC-A","replaced_by":"VEN-CYC-B","client_order_id":"CID-MOD-CYCLE","created_at":"2026-03-31T15:01:10Z","symbol":"NVDA","qty":"5","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"130.00","status":"replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-CYC-B") => {
                            cycle_gets_srv.fetch_add(1, Ordering::SeqCst);
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-CYC-B","replaced_by":"VEN-CYC-A","client_order_id":"CID-MOD-CYCLE-2","created_at":"2026-03-31T15:01:11Z","symbol":"NVDA","qty":"5","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"131.00","status":"replaced"}"#).await;
                        }

                        // 3a) cancel_order DeterministicReject (422), A -> B -> C (filled)
                        ("DELETE", "/v2/orders/VEN-CAN1-A") => {
                            write_http_response(&mut s, "422 Unprocessable Entity", r#"{"message":"cannot cancel replaced order"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-CAN1-A") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-CAN1-A","replaced_by":"VEN-CAN1-B","client_order_id":"CID-CAN-422-TERM","created_at":"2026-03-31T15:02:00Z","symbol":"AMD","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"150.00","status":"replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-CAN1-B") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-CAN1-B","replaced_by":"VEN-CAN1-C","client_order_id":"CID-CAN-422-TERM-2","created_at":"2026-03-31T15:02:01Z","symbol":"AMD","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"151.00","status":"replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-CAN1-C") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-CAN1-C","client_order_id":"CID-CAN-422-TERM-3","created_at":"2026-03-31T15:02:02Z","filled_at":"2026-03-31T15:02:03Z","symbol":"AMD","qty":"10","filled_qty":"10","filled_avg_price":"151.00","type":"limit","side":"buy","time_in_force":"day","limit_price":"151.00","status":"filled"}"#).await;
                        }

                        // 3b) cancel_order DeterministicReject (422), A -> B -> C (open -> canceled)
                        ("DELETE", "/v2/orders/VEN-CAN2-A") => {
                            write_http_response(&mut s, "422 Unprocessable Entity", r#"{"message":"cannot cancel replaced order"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-CAN2-A") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-CAN2-A","replaced_by":"VEN-CAN2-B","client_order_id":"CID-CAN-422-OPEN","created_at":"2026-03-31T15:03:00Z","symbol":"TSLA","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"240.00","status":"replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-CAN2-B") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-CAN2-B","replaced_by":"VEN-CAN2-C","client_order_id":"CID-CAN-422-OPEN-2","created_at":"2026-03-31T15:03:01Z","symbol":"TSLA","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"241.00","status":"replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-CAN2-C") => {
                            let body = if can2_srv.load(Ordering::SeqCst) == 0 {
                                r#"{"id":"VEN-CAN2-C","client_order_id":"CID-CAN-422-OPEN-3","created_at":"2026-03-31T15:03:02Z","symbol":"TSLA","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"241.00","status":"accepted"}"#
                            } else {
                                r#"{"id":"VEN-CAN2-C","client_order_id":"CID-CAN-422-OPEN-3","created_at":"2026-03-31T15:03:02Z","canceled_at":"2026-03-31T15:03:05Z","symbol":"TSLA","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"241.00","status":"canceled"}"#
                            };
                            write_http_response(&mut s, "200 OK", body).await;
                        }
                        ("DELETE", "/v2/orders/VEN-CAN2-C") => {
                            can2_srv.fetch_add(1, Ordering::SeqCst);
                            write_http_response(&mut s, "204 No Content", "").await;
                        }

                        // 3c) cancel_order transport error (500), A -> B -> C (open -> canceled)
                        ("DELETE", "/v2/orders/VEN-CAN3-A") => {
                            write_http_response(&mut s, "500 Internal Server Error", r#"{"message":"transient 500"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-CAN3-A") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-CAN3-A","replaced_by":"VEN-CAN3-B","client_order_id":"CID-CAN-500","created_at":"2026-03-31T15:04:00Z","symbol":"META","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"500.00","status":"replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-CAN3-B") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-CAN3-B","replaced_by":"VEN-CAN3-C","client_order_id":"CID-CAN-500-2","created_at":"2026-03-31T15:04:01Z","symbol":"META","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"501.00","status":"replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-CAN3-C") => {
                            let body = if can3_srv.load(Ordering::SeqCst) == 0 {
                                r#"{"id":"VEN-CAN3-C","client_order_id":"CID-CAN-500-3","created_at":"2026-03-31T15:04:02Z","symbol":"META","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"501.00","status":"accepted"}"#
                            } else {
                                r#"{"id":"VEN-CAN3-C","client_order_id":"CID-CAN-500-3","created_at":"2026-03-31T15:04:02Z","canceled_at":"2026-03-31T15:04:05Z","symbol":"META","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"501.00","status":"canceled"}"#
                            };
                            write_http_response(&mut s, "200 OK", body).await;
                        }
                        ("DELETE", "/v2/orders/VEN-CAN3-C") => {
                            can3_srv.fetch_add(1, Ordering::SeqCst);
                            write_http_response(&mut s, "204 No Content", "").await;
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

    // 1. reconcile_open_orders follows multi-hop A -> B -> C
    let ord_rec = build_limit_order(
        "CID-REC-CHAIN",
        "AAPL",
        OrderSide::Buy,
        "10",
        "210.00",
        None,
        None,
        None,
    );
    cache
        .borrow_mut()
        .add_order(
            ord_rec.clone(),
            None,
            Some(ClientId::from(ALPACA_VENUE)),
            false,
        )
        .unwrap();
    client
        .submit_order(SubmitOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from(ALPACA_VENUE)),
            StrategyId::from("STRAT-001"),
            InstrumentId::from("AAPL.ALPACA"),
            ClientOrderId::from("CID-REC-CHAIN"),
            ord_rec.init_event().clone(),
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            None,
        ))
        .unwrap();
    let sub_evs = drain_events(&mut rx, 200).await;
    assert!(sub_evs
        .iter()
        .any(|e| matches!(e, OrderEventAny::Accepted(a) if a.venue_order_id == VenueOrderId::from("VEN-REC-A"))));

    client.reconcile_open_orders().await;
    let rec_evs = drain_events(&mut rx, 200).await;
    assert!(
        rec_evs.iter().any(|e| matches!(
            e,
            OrderEventAny::Updated(u)
                if u.client_order_id == ClientOrderId::from("CID-REC-CHAIN")
                    && u.venue_order_id == Some(VenueOrderId::from("VEN-REC-C"))
        )),
        "reconcile_open_orders must follow A->B->C and emit OrderUpdated(VEN-REC-C), got {rec_evs:?}"
    );

    // 2. modify_order follows multi-hop A -> B -> C, and stops cleanly on cycle A -> B -> A
    let mut ord_mod = build_limit_order(
        "CID-MOD-CHAIN",
        "MSFT",
        OrderSide::Buy,
        "10",
        "400.00",
        None,
        None,
        None,
    );
    mark_accepted(&mut ord_mod, "VEN-MOD-A");
    let mut ord_cyc = build_limit_order(
        "CID-MOD-CYCLE",
        "NVDA",
        OrderSide::Buy,
        "5",
        "130.00",
        None,
        None,
        None,
    );
    mark_accepted(&mut ord_cyc, "VEN-CYC-A");
    {
        let mut c = cache.borrow_mut();
        c.add_order(ord_mod, None, Some(ClientId::from(ALPACA_VENUE)), false)
            .unwrap();
        c.add_order(ord_cyc, None, Some(ClientId::from(ALPACA_VENUE)), false)
            .unwrap();
    }

    client
        .modify_order(ModifyOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from(ALPACA_VENUE)),
            StrategyId::from("STRAT-001"),
            InstrumentId::from("MSFT.ALPACA"),
            ClientOrderId::from("CID-MOD-CHAIN"),
            Some(VenueOrderId::from("VEN-MOD-A")),
            Some(Quantity::from("15")),
            Some(Price::from("405.00")),
            None,
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            None,
            None,
        ))
        .unwrap();
    let mod_evs = drain_events(&mut rx, 250).await;
    assert!(
        mod_evs.iter().any(|e| matches!(
            e,
            OrderEventAny::Updated(u)
                if u.client_order_id == ClientOrderId::from("CID-MOD-CHAIN")
                    && u.venue_order_id == Some(VenueOrderId::from("VEN-MOD-C"))
        )),
        "modify_order must follow A->B->C and emit OrderUpdated(VEN-MOD-C), got {mod_evs:?}"
    );
    assert!(
        !mod_evs
            .iter()
            .any(|e| matches!(e, OrderEventAny::ModifyRejected(_))),
        "reconciled replace chain must not emit ModifyRejected"
    );

    client
        .modify_order(ModifyOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from(ALPACA_VENUE)),
            StrategyId::from("STRAT-001"),
            InstrumentId::from("NVDA.ALPACA"),
            ClientOrderId::from("CID-MOD-CYCLE"),
            Some(VenueOrderId::from("VEN-CYC-A")),
            Some(Quantity::from("5")),
            Some(Price::from("131.00")),
            None,
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            None,
            None,
        ))
        .unwrap();
    let cyc_evs = drain_events(&mut rx, 250).await;
    assert!(!cyc_evs.is_empty());
    assert!(
        cycle_gets.load(Ordering::SeqCst) <= 3,
        "cycle guard must stop A->B->A immediately without spinning MAX_REPLACE_HOPS; got {} GETs",
        cycle_gets.load(Ordering::SeqCst)
    );

    // 3. cancel_order: DeterministicReject (terminal + open) AND transport error (500)
    let mut ord_c1 = build_limit_order(
        "CID-CAN-422-TERM",
        "AMD",
        OrderSide::Buy,
        "10",
        "150.00",
        None,
        None,
        None,
    );
    mark_accepted(&mut ord_c1, "VEN-CAN1-A");
    let mut ord_c2 = build_limit_order(
        "CID-CAN-422-OPEN",
        "TSLA",
        OrderSide::Buy,
        "10",
        "240.00",
        None,
        None,
        None,
    );
    mark_accepted(&mut ord_c2, "VEN-CAN2-A");
    let mut ord_c3 = build_limit_order(
        "CID-CAN-500",
        "META",
        OrderSide::Buy,
        "10",
        "500.00",
        None,
        None,
        None,
    );
    mark_accepted(&mut ord_c3, "VEN-CAN3-A");
    {
        let mut c = cache.borrow_mut();
        for o in [ord_c1, ord_c2, ord_c3] {
            c.add_order(o, None, Some(ClientId::from(ALPACA_VENUE)), false)
                .unwrap();
        }
    }

    for (cid, sym, vid) in [
        ("CID-CAN-422-TERM", "AMD.ALPACA", "VEN-CAN1-A"),
        ("CID-CAN-422-OPEN", "TSLA.ALPACA", "VEN-CAN2-A"),
        ("CID-CAN-500", "META.ALPACA", "VEN-CAN3-A"),
    ] {
        client
            .cancel_order(CancelOrder::new(
                TraderId::from("TRADER-001"),
                Some(ClientId::from(ALPACA_VENUE)),
                StrategyId::from("STRAT-001"),
                InstrumentId::from(sym),
                ClientOrderId::from(cid),
                Some(VenueOrderId::from(vid)),
                UUID4::new(),
                UnixNanos::from(1_700_000_000_000_000_000u64),
                None,
                None,
            ))
            .unwrap();
    }

    let can_evs = drain_events(&mut rx, 350).await;
    assert!(can_evs.iter().any(|e| matches!(
        e,
        OrderEventAny::Updated(u)
            if u.client_order_id == ClientOrderId::from("CID-CAN-422-TERM")
                && u.venue_order_id == Some(VenueOrderId::from("VEN-CAN1-C"))
    )));
    assert!(can_evs.iter().any(|e| matches!(
        e,
        OrderEventAny::Filled(f)
            if f.client_order_id == ClientOrderId::from("CID-CAN-422-TERM")
                && f.venue_order_id == VenueOrderId::from("VEN-CAN1-C")
    )));
    assert!(
        !delete_paths
            .lock()
            .iter()
            .any(|p| p == "/v2/orders/VEN-CAN1-C"),
        "terminal C must NOT receive DELETE"
    );

    assert!(can_evs.iter().any(|e| matches!(
        e,
        OrderEventAny::Canceled(c)
            if c.client_order_id == ClientOrderId::from("CID-CAN-422-OPEN")
                && c.venue_order_id == Some(VenueOrderId::from("VEN-CAN2-C"))
    )));
    assert!(can_evs.iter().any(|e| matches!(
        e,
        OrderEventAny::Canceled(c)
            if c.client_order_id == ClientOrderId::from("CID-CAN-500")
                && c.venue_order_id == Some(VenueOrderId::from("VEN-CAN3-C"))
    )));

    let _ = stop_tx.send(());
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_round4_d5_p1_7_cancel_all_orders_multihop_chain_and_skips_duplicate_delete() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let delete_c_count = Arc::new(AtomicUsize::new(0));
    let del_c_srv = delete_c_count.clone();
    let fb_c_canceled = Arc::new(AtomicUsize::new(0));
    let fb_c_srv = fb_c_canceled.clone();
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                accept_res = listener.accept() => {
                    let Ok((mut s, _)) = accept_res else { break; };
                    let (m, p, _) = read_http_request(&mut s).await;
                    let route_p = if m == "GET" && p.starts_with("/v2/orders/") {
                        p.split('?').next().unwrap_or(p.as_str())
                    } else {
                        p.as_str()
                    };
                    match (m.as_str(), route_p) {
                        // Open orders returns VEN-ALL-A (with unmatched client_order_id)
                        ("GET", path) if path.starts_with("/v2/orders?") => {
                            write_http_response(
                                &mut s,
                                "200 OK",
                                r#"[{"id":"VEN-ALL-A","client_order_id":"CID-ALL-EXT","created_at":"2026-03-31T15:10:00Z","symbol":"AAPL","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"210.00","status":"accepted"}]"#,
                            ).await;
                        }
                        // First loop cancel of VEN-ALL-A fails -> follows A -> B -> C (already filled terminal!)
                        ("DELETE", "/v2/orders/VEN-ALL-A") => {
                            write_http_response(&mut s, "422 Unprocessable Entity", r#"{"message":"order replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-ALL-A") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-ALL-A","replaced_by":"VEN-ALL-B","client_order_id":"CID-ALL-EXT","created_at":"2026-03-31T15:10:00Z","symbol":"AAPL","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"210.00","status":"replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-ALL-B") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-ALL-B","replaced_by":"VEN-ALL-C","client_order_id":"CID-ALL-EXT-2","created_at":"2026-03-31T15:10:01Z","symbol":"AAPL","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"211.00","status":"replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-ALL-C") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-ALL-C","client_order_id":"CID-ALL-EXT-3","created_at":"2026-03-31T15:10:02Z","filled_at":"2026-03-31T15:10:03Z","symbol":"AAPL","qty":"10","filled_qty":"10","filled_avg_price":"211.00","type":"limit","side":"buy","time_in_force":"day","limit_price":"211.00","status":"filled"}"#).await;
                        }
                        ("DELETE", "/v2/orders/VEN-ALL-C") => {
                            del_c_srv.fetch_add(1, Ordering::SeqCst);
                            write_http_response(&mut s, "422 Unprocessable Entity", r#"{"message":"duplicate delete!"}"#).await;
                        }
                        // Local fallback order `CID-LOCAL-DUP` has VenueOrderId `VEN-OLD-0` which replaced -> VEN-ALL-C
                        // Since VEN-ALL-C is already in `matched_fallback_vids`, cancel_all_orders MUST skip DELETE!
                        ("GET", "/v2/orders/VEN-OLD-0") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-OLD-0","replaced_by":"VEN-ALL-C","client_order_id":"CID-LOCAL-DUP","created_at":"2026-03-31T15:09:50Z","symbol":"AAPL","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"209.00","status":"replaced"}"#).await;
                        }

                        // Second fallback order `CID-LOCAL-FB-CHAIN`: VEN-FB-A -> VEN-FB-B -> VEN-FB-C (accepted -> canceled)
                        ("GET", "/v2/orders/VEN-FB-A") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-FB-A","replaced_by":"VEN-FB-B","client_order_id":"CID-LOCAL-FB-CHAIN","created_at":"2026-03-31T15:11:00Z","symbol":"AAPL","qty":"5","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"205.00","status":"replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-FB-B") => {
                            write_http_response(&mut s, "200 OK", r#"{"id":"VEN-FB-B","replaced_by":"VEN-FB-C","client_order_id":"CID-LOCAL-FB-CHAIN-2","created_at":"2026-03-31T15:11:01Z","symbol":"AAPL","qty":"5","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"206.00","status":"replaced"}"#).await;
                        }
                        ("GET", "/v2/orders/VEN-FB-C") => {
                            let body = if fb_c_srv.load(Ordering::SeqCst) == 0 {
                                r#"{"id":"VEN-FB-C","client_order_id":"CID-LOCAL-FB-CHAIN-3","created_at":"2026-03-31T15:11:02Z","symbol":"AAPL","qty":"5","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"206.00","status":"accepted"}"#
                            } else {
                                r#"{"id":"VEN-FB-C","client_order_id":"CID-LOCAL-FB-CHAIN-3","created_at":"2026-03-31T15:11:02Z","canceled_at":"2026-03-31T15:11:05Z","symbol":"AAPL","qty":"5","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"206.00","status":"canceled"}"#
                            };
                            write_http_response(&mut s, "200 OK", body).await;
                        }
                        ("DELETE", "/v2/orders/VEN-FB-C") => {
                            fb_c_srv.fetch_add(1, Ordering::SeqCst);
                            write_http_response(&mut s, "204 No Content", "").await;
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

    let mut ord_dup = build_limit_order(
        "CID-LOCAL-DUP",
        "AAPL",
        OrderSide::Buy,
        "10",
        "209.00",
        None,
        None,
        None,
    );
    mark_accepted(&mut ord_dup, "VEN-OLD-0");
    let mut ord_fb = build_limit_order(
        "CID-LOCAL-FB-CHAIN",
        "AAPL",
        OrderSide::Buy,
        "5",
        "205.00",
        None,
        None,
        None,
    );
    mark_accepted(&mut ord_fb, "VEN-FB-A");
    {
        let mut c = cache.borrow_mut();
        c.add_order(ord_dup, None, Some(ClientId::from(ALPACA_VENUE)), false)
            .unwrap();
        c.add_order(ord_fb, None, Some(ClientId::from(ALPACA_VENUE)), false)
            .unwrap();
    }

    client
        .cancel_all_orders(CancelAllOrders::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from(ALPACA_VENUE)),
            StrategyId::from("STRAT-001"),
            InstrumentId::from("AAPL.ALPACA"),
            Some(OrderSide::Buy),
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            None,
            None,
        ))
        .unwrap();

    let evs = drain_events(&mut rx, 350).await;
    assert_eq!(
        delete_c_count.load(Ordering::SeqCst),
        0,
        "terminal VEN-ALL-C already in matched_fallback_vids must NOT receive duplicate DELETE"
    );
    assert!(
        evs.iter().any(|e| matches!(
            e,
            OrderEventAny::Updated(u)
                if u.client_order_id == ClientOrderId::from("CID-LOCAL-DUP")
                    && u.venue_order_id == Some(VenueOrderId::from("VEN-ALL-C"))
        )),
        "expected OrderUpdated(CID-LOCAL-DUP, VEN-ALL-C), got evs={evs:?}"
    );
    assert!(
        evs.iter().any(|e| matches!(
            e,
            OrderEventAny::CancelRejected(r)
                if r.client_order_id == ClientOrderId::from("CID-LOCAL-DUP")
                    && r.venue_order_id == Some(VenueOrderId::from("VEN-ALL-C"))
                    && r.reason.as_str().contains("already filled")
        )),
        "expected OrderCancelRejected(CID-LOCAL-DUP, VEN-ALL-C, already filled), got evs={evs:?}"
    );
    assert!(evs.iter().any(|e| matches!(
        e,
        OrderEventAny::Updated(u)
            if u.client_order_id == ClientOrderId::from("CID-LOCAL-FB-CHAIN")
                && u.venue_order_id == Some(VenueOrderId::from("VEN-FB-C"))
    )));
    assert!(evs.iter().any(|e| matches!(
        e,
        OrderEventAny::Canceled(c)
            if c.client_order_id == ClientOrderId::from("CID-LOCAL-FB-CHAIN")
                && c.venue_order_id == Some(VenueOrderId::from("VEN-FB-C"))
    )));
    assert_eq!(fb_c_canceled.load(Ordering::SeqCst), 1);

    let _ = stop_tx.send(());
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_round4_d5_p1_8_nested_query_and_submit_order_list_single_probe_no_fabricated_vid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let nested_query_seen = Arc::new(Mutex::new(String::new()));
    let nested_srv = nested_query_seen.clone();
    let probe1_count = Arc::new(AtomicUsize::new(0));
    let probe1_srv = probe1_count.clone();
    let probe2_count = Arc::new(AtomicUsize::new(0));
    let probe2_srv = probe2_count.clone();
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        let mut post_seq = 0usize;
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                accept_res = listener.accept() => {
                    let Ok((mut s, _)) = accept_res else { break; };
                    let (m, p, _) = read_http_request(&mut s).await;
                    let route_p = if m == "GET" && p.starts_with("/v2/orders/") {
                        p.split('?').next().unwrap_or(p.as_str())
                    } else {
                        p.as_str()
                    };
                    match (m.as_str(), route_p) {
                        ("GET", path) if path.starts_with("/v2/orders:by_client_order_id") => {
                            *nested_srv.lock() = path.to_string();
                            write_http_response(
                                &mut s,
                                "200 OK",
                                r#"{"id":"VEN-NEST-1","client_order_id":"CID-NEST-CHK","created_at":"2026-03-31T15:20:00Z","symbol":"AAPL","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"210.00","status":"accepted"}"#,
                            ).await;
                        }
                        ("POST", "/v2/orders") => {
                            post_seq += 1;
                            if post_seq == 1 {
                                // Bracket 1: initial POST returns legs: null -> probed once and recovered!
                                write_http_response(
                                    &mut s,
                                    "200 OK",
                                    r#"{"id":"VEN-BRK-1","client_order_id":"CID-B1-ENTRY","created_at":"2026-03-31T15:21:00Z","symbol":"NVDA","qty":"10","filled_qty":"0","order_class":"bracket","type":"limit","side":"buy","time_in_force":"day","limit_price":"140.00","status":"accepted","legs":null}"#,
                                ).await;
                            } else {
                                // Bracket 2: initial POST returns only 1 of 2 child legs -> probed once, SL leg still missing!
                                write_http_response(
                                    &mut s,
                                    "200 OK",
                                    r#"{"id":"VEN-BRK-2","client_order_id":"CID-B2-ENTRY","created_at":"2026-03-31T15:22:00Z","symbol":"MSFT","qty":"10","filled_qty":"0","order_class":"bracket","type":"limit","side":"buy","time_in_force":"day","limit_price":"400.00","status":"accepted","legs":[{"id":"VEN-B2-TP","client_order_id":"CID-B2-TP","created_at":"2026-03-31T15:22:00Z","symbol":"MSFT","qty":"10","filled_qty":"0","order_class":"bracket","type":"limit","side":"sell","time_in_force":"day","limit_price":"430.00","status":"held"}]}"#,
                                ).await;
                            }
                        }
                        ("GET", "/v2/orders/VEN-BRK-1") => {
                            probe1_srv.fetch_add(1, Ordering::SeqCst);
                            write_http_response(
                                &mut s,
                                "200 OK",
                                r#"{"id":"VEN-BRK-1","client_order_id":"CID-B1-ENTRY","created_at":"2026-03-31T15:21:00Z","symbol":"NVDA","qty":"10","filled_qty":"0","order_class":"bracket","type":"limit","side":"buy","time_in_force":"day","limit_price":"140.00","status":"accepted","legs":[{"id":"VEN-B1-TP","client_order_id":"CID-B1-TP","created_at":"2026-03-31T15:21:00Z","symbol":"NVDA","qty":"10","filled_qty":"0","order_class":"bracket","type":"limit","side":"sell","time_in_force":"day","limit_price":"160.00","status":"held"},{"id":"VEN-B1-SL","client_order_id":"CID-B1-SL","created_at":"2026-03-31T15:21:00Z","symbol":"NVDA","qty":"10","filled_qty":"0","order_class":"bracket","type":"stop","side":"sell","time_in_force":"day","stop_price":"130.00","status":"held"}]}"#,
                            ).await;
                        }
                        ("GET", "/v2/orders/VEN-BRK-2") => {
                            probe2_srv.fetch_add(1, Ordering::SeqCst);
                            write_http_response(
                                &mut s,
                                "200 OK",
                                r#"{"id":"VEN-BRK-2","client_order_id":"CID-B2-ENTRY","created_at":"2026-03-31T15:22:00Z","symbol":"MSFT","qty":"10","filled_qty":"0","order_class":"bracket","type":"limit","side":"buy","time_in_force":"day","limit_price":"400.00","status":"accepted","legs":[{"id":"VEN-B2-TP","client_order_id":"CID-B2-TP","created_at":"2026-03-31T15:22:00Z","symbol":"MSFT","qty":"10","filled_qty":"0","order_class":"bracket","type":"limit","side":"sell","time_in_force":"day","limit_price":"430.00","status":"held"}]}"#,
                            ).await;
                        }
                        (other_m, other_p) => {
                            panic!("unexpected HTTP request {other_m} {other_p}");
                        }
                    }
                }
            }
        }
    });

    // 1. `get_order_by_client_order_id` sends `nested=true`
    let cred = AlpacaCredential::new_checked("PKR4HTTPKEY12345", "SKR4HTTPSECRET8765").unwrap();
    let http_cfg = AlpacaConfig::new_checked(
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
    let http_client = AlpacaHttpClient::new(http_cfg).unwrap();
    let by_cid = http_client
        .get_order_by_client_order_id("CID-NEST-CHK")
        .await
        .unwrap();
    assert_eq!(by_cid.id, "VEN-NEST-1");
    assert_eq!(
        nested_query_seen.lock().as_str(),
        "/v2/orders:by_client_order_id?client_order_id=CID-NEST-CHK&nested=true"
    );

    // 2. `submit_order_list` single GET probe & zero fabricated `"{id}-LEG-{idx}"` IDs
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

    // Bracket 1 (recovered on single probe)
    let olid1 = OrderListId::from("OL-B1");
    let b1_entry = build_limit_order(
        "CID-B1-ENTRY",
        "NVDA",
        OrderSide::Buy,
        "10",
        "140.00",
        Some(ContingencyType::Oto),
        Some(olid1),
        Some(vec![ClientOrderId::from("CID-B1-TP"), ClientOrderId::from("CID-B1-SL")]),
    );
    let b1_tp = build_limit_order(
        "CID-B1-TP",
        "NVDA",
        OrderSide::Sell,
        "10",
        "160.00",
        Some(ContingencyType::Oco),
        Some(olid1),
        Some(vec![ClientOrderId::from("CID-B1-SL")]),
    );
    let b1_sl = build_stop_order(
        "CID-B1-SL",
        "NVDA",
        OrderSide::Sell,
        "10",
        "130.00",
        Some(ContingencyType::Oco),
        Some(olid1),
        Some(vec![ClientOrderId::from("CID-B1-TP")]),
    );
    let b1_inits = vec![
        b1_entry.init_event().clone(),
        b1_tp.init_event().clone(),
        b1_sl.init_event().clone(),
    ];
    let b1_list = OrderList::new(
        olid1,
        InstrumentId::from("NVDA.ALPACA"),
        StrategyId::from("STRAT-001"),
        vec![
            ClientOrderId::from("CID-B1-ENTRY"),
            ClientOrderId::from("CID-B1-TP"),
            ClientOrderId::from("CID-B1-SL"),
        ],
        UnixNanos::from(1_700_000_000_000_000_000u64),
    );
    {
        let mut c = cache.borrow_mut();
        for o in [b1_entry, b1_tp, b1_sl] {
            c.add_order(o, None, Some(ClientId::from(ALPACA_VENUE)), false)
                .unwrap();
        }
        c.add_order_list(b1_list.clone()).unwrap();
    }
    client
        .submit_order_list(SubmitOrderList::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from(ALPACA_VENUE)),
            StrategyId::from("STRAT-001"),
            b1_list,
            b1_inits,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            None,
        ))
        .unwrap();

    let evs1 = drain_events(&mut rx, 300).await;
    assert_eq!(probe1_count.load(Ordering::SeqCst), 1);
    let acc1: Vec<(String, String)> = evs1
        .iter()
        .filter_map(|e| match e {
            OrderEventAny::Accepted(a) => {
                Some((a.client_order_id.to_string(), a.venue_order_id.to_string()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(acc1.len(), 3, "expected all 3 bracket legs accepted via single probe: {acc1:?}");
    assert!(acc1.contains(&("CID-B1-ENTRY".to_string(), "VEN-BRK-1".to_string())));
    assert!(acc1.contains(&("CID-B1-TP".to_string(), "VEN-B1-TP".to_string())));
    assert!(acc1.contains(&("CID-B1-SL".to_string(), "VEN-B1-SL".to_string())));

    // Bracket 2 (SL leg still missing after single probe -> stays Submitted without fabricated VenueOrderId)
    let olid2 = OrderListId::from("OL-B2");
    let b2_entry = build_limit_order(
        "CID-B2-ENTRY",
        "MSFT",
        OrderSide::Buy,
        "10",
        "400.00",
        Some(ContingencyType::Oto),
        Some(olid2),
        Some(vec![ClientOrderId::from("CID-B2-TP"), ClientOrderId::from("CID-B2-SL")]),
    );
    let b2_tp = build_limit_order(
        "CID-B2-TP",
        "MSFT",
        OrderSide::Sell,
        "10",
        "430.00",
        Some(ContingencyType::Oco),
        Some(olid2),
        Some(vec![ClientOrderId::from("CID-B2-SL")]),
    );
    let b2_sl = build_stop_order(
        "CID-B2-SL",
        "MSFT",
        OrderSide::Sell,
        "10",
        "380.00",
        Some(ContingencyType::Oco),
        Some(olid2),
        Some(vec![ClientOrderId::from("CID-B2-TP")]),
    );
    let b2_inits = vec![
        b2_entry.init_event().clone(),
        b2_tp.init_event().clone(),
        b2_sl.init_event().clone(),
    ];
    let b2_list = OrderList::new(
        olid2,
        InstrumentId::from("MSFT.ALPACA"),
        StrategyId::from("STRAT-001"),
        vec![
            ClientOrderId::from("CID-B2-ENTRY"),
            ClientOrderId::from("CID-B2-TP"),
            ClientOrderId::from("CID-B2-SL"),
        ],
        UnixNanos::from(1_700_000_000_000_000_000u64),
    );
    {
        let mut c = cache.borrow_mut();
        for o in [b2_entry, b2_tp, b2_sl] {
            c.add_order(o, None, Some(ClientId::from(ALPACA_VENUE)), false)
                .unwrap();
        }
        c.add_order_list(b2_list.clone()).unwrap();
    }
    client
        .submit_order_list(SubmitOrderList::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from(ALPACA_VENUE)),
            StrategyId::from("STRAT-001"),
            b2_list,
            b2_inits,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            None,
        ))
        .unwrap();

    let evs2 = drain_events(&mut rx, 300).await;
    assert_eq!(probe2_count.load(Ordering::SeqCst), 1);

    assert!(evs2.iter().any(|e| matches!(
        e,
        OrderEventAny::Submitted(s) if s.client_order_id == ClientOrderId::from("CID-B2-SL")
    )));
    assert!(
        !evs2.iter().any(|e| match e {
            OrderEventAny::Accepted(a) => a.client_order_id == ClientOrderId::from("CID-B2-SL")
                || a.venue_order_id.as_str().contains("-LEG-"),
            OrderEventAny::Rejected(r) => r.client_order_id == ClientOrderId::from("CID-B2-SL"),
            _ => false,
        }),
        "missing SL child leg must remain in Submitted state without fabricating -LEG- VenueOrderId; got {evs2:?}"
    );

    let _ = stop_tx.send(());
    server.await.unwrap();
}
