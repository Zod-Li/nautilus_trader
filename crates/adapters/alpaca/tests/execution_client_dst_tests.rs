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

//! Deterministic Simulation Test (DST) suite for `nautilus-alpaca` Ticket #08:
//! - `AlpacaExecutionClientFactory::create(...)` creating an `AlpacaExecutionClient`.
//! - Single-leg `submit_order` (`Market`, `Limit`, `StopLimit`) -> `OrderSubmitted` + `OrderAccepted`
//!   emitted, plus `FifoCache` deduplication when WebSocket `trade_updates` also arrives.
//! - Bracket/OCO/OTO `submit_order_list` (`ContingencyType::Oto` / `Oco`) -> mapped to Alpaca
//!   `order_class: "bracket" | "oco" | "oto"` with `take_profit` and `stop_loss` legs, verifying
//!   parent and child orders are tracked and accepted.
//! - `modify_order`, `cancel_order`, `cancel_all_orders`, and `batch_cancel_orders`.
//! - WebSocket `trade_updates` fill (`partial_fill` / `fill`) emitting `OrderFilled` with accurate
//!   qty, price, and commission.
//! - Startup & periodic reconciliation reports: `generate_order_status_reports`,
//!   `generate_fill_reports`, `generate_position_status_reports`, and `generate_mass_status`.

#![allow(linker_messages)]

use std::{cell::RefCell, rc::Rc, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use nautilus_alpaca::{
    ALPACA_VENUE, AlpacaCredential, AlpacaEnvironment, AlpacaExecClientConfig,
    AlpacaExecutionClient, AlpacaExecutionClientFactory,
};
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    clock::{Clock, VirtualClock},
    factories::{ClientConfig, ExecutionClientFactory},
    live::runner::replace_exec_event_sender,
    messages::{
        ExecutionEvent,
        execution::{
            BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReportsBuilder,
            GenerateOrderStatusReportsBuilder, GeneratePositionStatusReportsBuilder, ModifyOrder,
            SubmitOrder, SubmitOrderList,
        },
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    enums::{
        AccountType, ContingencyType, LiquiditySide, OmsType, OrderSide, OrderStatus, PositionSide,
        TimeInForce, TriggerType,
    },
    events::OrderEventAny,
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, StrategyId, TradeId,
        TraderId, Venue, VenueOrderId,
    },
    orders::{LimitOrder, MarketOrder, Order, OrderAny, OrderList, StopLimitOrder, StopMarketOrder},
    types::{Currency, Money, Price, Quantity},
};
use parking_lot::Mutex;
use rust_decimal_macros::dec;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc::UnboundedReceiver,
};
use tokio_tungstenite::{accept_async, tungstenite::Message as WsMessage};

// =================================================================================================
// Ephemeral HTTP + WebSocket Mock Broker Harness
// =================================================================================================

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

fn make_test_config(http_base_url: &str, ws_url: &str) -> AlpacaExecClientConfig {
    let cred = AlpacaCredential::new_checked("PKDSTKEY12345678", "SKDSTSECRET87654321")
        .expect("valid test credential");
    AlpacaExecClientConfig::new_checked(
        AlpacaEnvironment::Paper,
        http_base_url,
        ws_url,
        cred,
        5,
        2,
        25,
        200,
        100,
        ALPACA_VENUE,
    )
    .expect("valid test config")
}

fn drain_events(rx: &mut UnboundedReceiver<ExecutionEvent>) -> Vec<ExecutionEvent> {
    let mut out = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        out.push(ev);
    }
    out
}

#[expect(clippy::too_many_arguments)]
fn build_ws_trade_update_frame(
    event: &str,
    venue_order_id: &str,
    client_order_id: &str,
    symbol: &str,
    side: &str,
    order_type: &str,
    status: &str,
    qty: &str,
    filled_qty: &str,
    filled_avg_price: Option<&str>,
    event_price: Option<&str>,
    event_qty: Option<&str>,
    position_qty: Option<&str>,
    execution_id: Option<&str>,
) -> String {
    serde_json::json!({
        "stream": "trade_updates",
        "data": {
            "event": event,
            "execution_id": execution_id,
            "price": event_price,
            "qty": event_qty,
            "position_qty": position_qty,
            "timestamp": "2026-03-31T14:30:02Z",
            "order": {
                "id": venue_order_id,
                "client_order_id": client_order_id,
                "created_at": "2026-03-31T14:30:00Z",
                "updated_at": "2026-03-31T14:30:02Z",
                "submitted_at": "2026-03-31T14:30:00.100Z",
                "filled_at": if filled_qty != "0" { Some("2026-03-31T14:30:02Z") } else { None },
                "symbol": symbol,
                "asset_class": "us_equity",
                "qty": qty,
                "filled_qty": filled_qty,
                "filled_avg_price": filled_avg_price,
                "order_class": "simple",
                "type": order_type,
                "side": side,
                "time_in_force": "day",
                "limit_price": event_price,
                "status": status,
                "extended_hours": false
            }
        }
    })
    .to_string()
}

// =================================================================================================
//Order Construction Helpers
// =================================================================================================

fn make_market_order(
    cid: &str,
    symbol: &str,
    side: OrderSide,
    qty: &str,
    contingency: Option<ContingencyType>,
    order_list_id: Option<OrderListId>,
    linked: Option<Vec<ClientOrderId>>,
    parent: Option<ClientOrderId>,
) -> OrderAny {
    OrderAny::Market(
        MarketOrder::new_checked(
            TraderId::from("TRADER-001"),
            StrategyId::from("STRAT-001"),
            InstrumentId::from(format!("{symbol}.ALPACA").as_str()),
            ClientOrderId::from(cid),
            side,
            Quantity::from(qty),
            TimeInForce::Day,
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            false,
            false,
            contingency,
            order_list_id,
            linked,
            parent,
            None,
            None,
            None,
            None,
        )
        .expect("valid MarketOrder"),
    )
}

#[expect(clippy::too_many_arguments)]
fn make_limit_order(
    cid: &str,
    symbol: &str,
    side: OrderSide,
    qty: &str,
    price: &str,
    contingency: Option<ContingencyType>,
    order_list_id: Option<OrderListId>,
    linked: Option<Vec<ClientOrderId>>,
    parent: Option<ClientOrderId>,
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
            parent,
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

#[expect(clippy::too_many_arguments)]
fn make_stop_market_order(
    cid: &str,
    symbol: &str,
    side: OrderSide,
    qty: &str,
    trigger_price: &str,
    contingency: Option<ContingencyType>,
    order_list_id: Option<OrderListId>,
    linked: Option<Vec<ClientOrderId>>,
    parent: Option<ClientOrderId>,
) -> OrderAny {
    OrderAny::StopMarket(
        StopMarketOrder::new_checked(
            TraderId::from("TRADER-001"),
            StrategyId::from("STRAT-001"),
            InstrumentId::from(format!("{symbol}.ALPACA").as_str()),
            ClientOrderId::from(cid),
            side,
            Quantity::from(qty),
            Price::from(trigger_price),
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
            parent,
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

#[expect(clippy::too_many_arguments)]
fn make_stop_limit_order(
    cid: &str,
    symbol: &str,
    side: OrderSide,
    qty: &str,
    price: &str,
    trigger_price: &str,
    contingency: Option<ContingencyType>,
    order_list_id: Option<OrderListId>,
    linked: Option<Vec<ClientOrderId>>,
    parent: Option<ClientOrderId>,
) -> OrderAny {
    OrderAny::StopLimit(
        StopLimitOrder::new_checked(
            TraderId::from("TRADER-001"),
            StrategyId::from("STRAT-001"),
            InstrumentId::from(format!("{symbol}.ALPACA").as_str()),
            ClientOrderId::from(cid),
            side,
            Quantity::from(qty),
            Price::from(price),
            Price::from(trigger_price),
            TriggerType::Default,
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
            parent,
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
        )
        .expect("valid StopLimitOrder"),
    )
}

fn make_submit_order_cmd(order: &OrderAny) -> SubmitOrder {
    SubmitOrder::new(
        order.trader_id(),
        Some(ClientId::from(ALPACA_VENUE)),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(1_700_000_000_000_000_000u64),
        None,
    )
}

// =================================================================================================
// 1. Factory Creation + Connect/Disconnect + WebSocket Fill Stream DST
// =================================================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_factory_create_connect_and_ws_trade_updates_fills_dst() {
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    let http_base_url = format!("http://{http_addr}");

    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = ws_listener.local_addr().unwrap();
    let ws_url = format!("ws://{ws_addr}/stream");

    let (ws_trigger_tx, mut ws_trigger_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    let http_server = tokio::spawn(async move {
        // 1. GET /v2/account on connect()
        let (mut s1, _) = http_listener.accept().await.unwrap();
        let (method, path, _) = read_http_request(&mut s1).await;
        assert_eq!(method, "GET");
        assert_eq!(path, "/v2/account");
        write_http_response(
            &mut s1,
            "200 OK",
            r#"{"id":"acc-dst-1","account_number":"PA777","status":"ACTIVE","currency":"USD","cash":"100000.00","equity":"120000.00","buying_power":"200000.00","initial_margin":"10000.00","maintenance_margin":"5000.00"}"#,
        )
        .await;

        // 2. POST /v2/orders for Market order CID-MKT-1
        let (mut s2, _) = http_listener.accept().await.unwrap();
        let (method2, path2, body2) = read_http_request(&mut s2).await;
        assert_eq!(method2, "POST");
        assert_eq!(path2, "/v2/orders");
        let parsed2: serde_json::Value = serde_json::from_str(&body2).unwrap();
        assert_eq!(parsed2["client_order_id"], "CID-MKT-1");
        assert_eq!(parsed2["type"], "market");
        assert_eq!(parsed2["order_class"], "simple");
        write_http_response(
            &mut s2,
            "200 OK",
            r#"{"id":"VEN-MKT-1","client_order_id":"CID-MKT-1","created_at":"2026-03-31T14:30:00Z","submitted_at":"2026-03-31T14:30:00.100Z","symbol":"AAPL","qty":"100","filled_qty":"0","order_class":"simple","type":"market","side":"buy","time_in_force":"day","status":"accepted"}"#,
        )
        .await;
    });

    let ws_server = tokio::spawn(async move {
        let (tcp, _) = ws_listener.accept().await.unwrap();
        let mut ws = accept_async(tcp).await.unwrap();

        // Expect authenticate
        let auth_msg = ws.next().await.unwrap().unwrap();
        let auth_json: serde_json::Value =
            serde_json::from_str(auth_msg.to_text().unwrap()).unwrap();
        assert_eq!(auth_json["action"], "authenticate");
        ws.send(WsMessage::Text(
            r#"{"stream":"authorization","data":{"status":"authorized","action":"authenticate"}}"#
                .into(),
        ))
        .await
        .unwrap();

        // Expect listen
        let listen_msg = ws.next().await.unwrap().unwrap();
        let listen_json: serde_json::Value =
            serde_json::from_str(listen_msg.to_text().unwrap()).unwrap();
        assert_eq!(listen_json["action"], "listen");
        ws.send(WsMessage::Text(
            r#"{"stream":"listening","data":{"streams":["trade_updates"]}}"#.into(),
        ))
        .await
        .unwrap();

        while let Some(frame) = ws_trigger_rx.recv().await {
            if ws.send(WsMessage::Text(frame.into())).await.is_err() {
                break;
            }
        }
    });

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
    replace_exec_event_sender(tx);

    let cache = Rc::new(RefCell::new(Cache::default()));
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::default()));
    let config = make_test_config(&http_base_url, &ws_url);

    let factory = AlpacaExecutionClientFactory::new();
    assert_eq!(factory.name(), ALPACA_VENUE);
    assert_eq!(factory.config_type(), "AlpacaExecClientConfig");
    assert!(config.as_any().downcast_ref::<AlpacaExecClientConfig>().is_some());

    let mut client = factory
        .create(
            TraderId::from("TRADER-001"),
            ALPACA_VENUE,
            &config,
            cache.clone().into(),
            clock,
        )
        .expect("factory.create must succeed");

    assert_eq!(client.client_id(), ClientId::from(ALPACA_VENUE));
    assert_eq!(client.account_id(), AccountId::from("ALPACA-001"));
    assert_eq!(client.venue(), Venue::from(ALPACA_VENUE));
    assert_eq!(client.oms_type(), OmsType::Netting);
    assert!(client.retain_unresolved_submissions());
    assert!(client.handles_order_venue(Venue::from("ALPACA")));
    assert!(client.handles_order_venue(Venue::from("XNAS")));
    assert!(client.handles_order_venue(Venue::from("XNYS")));
    assert!(!client.is_connected());

    client.start().unwrap();
    client.connect().await.expect("connect must succeed");
    assert!(client.is_connected());

    // First event must be AccountState from GET /v2/account
    let acct_ev = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    match acct_ev {
        ExecutionEvent::Account(state) => {
            assert_eq!(state.account_id, AccountId::from("ALPACA-001"));
            assert_eq!(state.account_type, AccountType::Margin);
            assert_eq!(state.balances[0].total.as_decimal(), dec!(120000.00));
        }
        other => panic!("expected Account event on connect, got {other:?}"),
    }

    // Add MarketOrder to cache and submit
    let mkt_order = make_market_order(
        "CID-MKT-1",
        "AAPL",
        OrderSide::Buy,
        "100",
        None,
        None,
        None,
        None,
    );
    cache
        .borrow_mut()
        .add_order(mkt_order.clone(), None, Some(ClientId::from(ALPACA_VENUE)), false)
        .unwrap();

    client.submit_order(make_submit_order_cmd(&mkt_order)).unwrap();

    // Expect OrderSubmitted + OrderAccepted
    let ev_sub = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        ev_sub,
        ExecutionEvent::Order(OrderEventAny::Submitted(ref s))
            if s.client_order_id == ClientOrderId::from("CID-MKT-1")
    ));

    let ev_acc = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        ev_acc,
        ExecutionEvent::Order(OrderEventAny::Accepted(ref a))
            if a.client_order_id == ClientOrderId::from("CID-MKT-1")
                && a.venue_order_id == VenueOrderId::from("VEN-MKT-1")
    ));

    // Duplicate submit_order call for the same ClientOrderId must be deduplicated by FifoCache
    client.submit_order(make_submit_order_cmd(&mkt_order)).unwrap();
    assert!(
        rx.try_recv().is_err(),
        "Duplicate submit_order must be suppressed by FifoCache"
    );

    // WebSocket sends `new` for the same VEN-MKT-1 -> FifoCache must deduplicate OrderAccepted!
    let ws_new = build_ws_trade_update_frame(
        "new",
        "VEN-MKT-1",
        "CID-MKT-1",
        "AAPL",
        "buy",
        "market",
        "new",
        "100",
        "0",
        None,
        None,
        None,
        Some("0"),
        None,
    );
    ws_trigger_tx.send(ws_new).unwrap();

    // WebSocket sends `partial_fill` (40 @ 210.25) and `fill` (60 @ 210.50), plus a duplicate `fill`
    let ws_partial = build_ws_trade_update_frame(
        "partial_fill",
        "VEN-MKT-1",
        "CID-MKT-1",
        "AAPL",
        "buy",
        "market",
        "partially_filled",
        "100",
        "40",
        Some("210.25"),
        Some("210.25"),
        Some("40"),
        Some("40"),
        Some("EXEC-FILL-1"),
    );
    ws_trigger_tx.send(ws_partial).unwrap();

    let ws_fill = build_ws_trade_update_frame(
        "fill",
        "VEN-MKT-1",
        "CID-MKT-1",
        "AAPL",
        "buy",
        "market",
        "filled",
        "100",
        "100",
        Some("210.40"),
        Some("210.50"),
        Some("60"),
        Some("100"),
        Some("EXEC-FILL-2"),
    );
    ws_trigger_tx.send(ws_fill.clone()).unwrap();
    // Send duplicate fill with same execution_id
    ws_trigger_tx.send(ws_fill).unwrap();

    // Receive partial_fill -> OrderFilled
    let ev_fill_1 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    match ev_fill_1 {
        ExecutionEvent::Order(OrderEventAny::Filled(f)) => {
            assert_eq!(f.client_order_id, ClientOrderId::from("CID-MKT-1"));
            assert_eq!(f.venue_order_id, VenueOrderId::from("VEN-MKT-1"));
            assert_eq!(f.trade_id, TradeId::from("EXEC-FILL-1"));
            assert_eq!(f.last_qty, Quantity::from("40"));
            assert_eq!(f.last_px, Price::from("210.25"));
            assert_eq!(
                f.commission,
                Some(Money::from_decimal(dec!(0), Currency::USD()).unwrap())
            );
            assert_eq!(f.liquidity_side, LiquiditySide::Taker);
        }
        other => panic!("expected first OrderFilled (partial_fill) without duplicate OrderAccepted, got {other:?}"),
    }

    // Receive final fill -> OrderFilled
    let ev_fill_2 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    match ev_fill_2 {
        ExecutionEvent::Order(OrderEventAny::Filled(f)) => {
            assert_eq!(f.client_order_id, ClientOrderId::from("CID-MKT-1"));
            assert_eq!(f.venue_order_id, VenueOrderId::from("VEN-MKT-1"));
            assert_eq!(f.trade_id, TradeId::from("EXEC-FILL-2"));
            assert_eq!(f.last_qty, Quantity::from("60"));
            assert_eq!(f.last_px, Price::from("210.50"));
            assert_eq!(
                f.commission,
                Some(Money::from_decimal(dec!(0), Currency::USD()).unwrap())
            );
        }
        other => panic!("expected second OrderFilled (fill), got {other:?}"),
    }

    // Give a brief window and verify the duplicate EXEC-FILL-2 was suppressed
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        rx.try_recv().is_err(),
        "Duplicate trade_updates fill with same execution_id must be suppressed by FifoCache"
    );

    drop(ws_trigger_tx);
    client.disconnect().await.unwrap();
    assert!(!client.is_connected());
    client.stop().unwrap();
    client.reset().unwrap();
    client.dispose().unwrap();

    http_server.await.unwrap();
    ws_server.await.unwrap();
}

// =================================================================================================
// 2. Single-Leg Limit & StopLimit + Bracket / OCO / OTO OrderList Submissions DST
// =================================================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_single_leg_limit_stoplimit_and_bracket_oco_oto_order_lists_dst() {
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    let http_base_url = format!("http://{http_addr}");

    let recorded_requests: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let recorded_srv = Arc::clone(&recorded_requests);

    let http_server = tokio::spawn(async move {
        // 1. Limit order CID-LMT-1
        let (mut s1, _) = http_listener.accept().await.unwrap();
        let (_, path1, body1) = read_http_request(&mut s1).await;
        assert_eq!(path1, "/v2/orders");
        let j1: serde_json::Value = serde_json::from_str(&body1).unwrap();
        recorded_srv.lock().push(j1);
        write_http_response(
            &mut s1,
            "200 OK",
            r#"{"id":"VEN-LMT-1","client_order_id":"CID-LMT-1","created_at":"2026-03-31T14:31:00Z","symbol":"NVDA","qty":"25","filled_qty":"0","order_class":"simple","type":"limit","side":"buy","time_in_force":"day","limit_price":"135.50","status":"accepted"}"#,
        )
        .await;

        // 2. StopLimit order CID-SLMT-1
        let (mut s2, _) = http_listener.accept().await.unwrap();
        let (_, path2, body2) = read_http_request(&mut s2).await;
        assert_eq!(path2, "/v2/orders");
        let j2: serde_json::Value = serde_json::from_str(&body2).unwrap();
        recorded_srv.lock().push(j2);
        write_http_response(
            &mut s2,
            "200 OK",
            r#"{"id":"VEN-SLMT-1","client_order_id":"CID-SLMT-1","created_at":"2026-03-31T14:31:05Z","symbol":"NVDA","qty":"25","filled_qty":"0","order_class":"simple","type":"stop_limit","side":"sell","time_in_force":"day","limit_price":"129.50","stop_price":"130.00","status":"accepted"}"#,
        )
        .await;

        // 3. Bracket OrderList (Parent Limit Buy + TP Limit Sell + SL StopMarket Sell)
        let (mut s3, _) = http_listener.accept().await.unwrap();
        let (_, path3, body3) = read_http_request(&mut s3).await;
        assert_eq!(path3, "/v2/orders");
        let j3: serde_json::Value = serde_json::from_str(&body3).unwrap();
        recorded_srv.lock().push(j3);
        write_http_response(
            &mut s3,
            "200 OK",
            r#"{
                "id":"VEN-BRK-PARENT",
                "client_order_id":"CID-BRK-PARENT",
                "created_at":"2026-03-31T14:32:00Z",
                "symbol":"AAPL",
                "qty":"50",
                "filled_qty":"0",
                "order_class":"bracket",
                "type":"limit",
                "side":"buy",
                "time_in_force":"day",
                "limit_price":"200.00",
                "status":"accepted",
                "legs":[
                    {
                        "id":"VEN-BRK-TP",
                        "client_order_id":"CID-BRK-TP",
                        "created_at":"2026-03-31T14:32:00Z",
                        "symbol":"AAPL",
                        "qty":"50",
                        "filled_qty":"0",
                        "order_class":"bracket",
                        "type":"limit",
                        "side":"sell",
                        "time_in_force":"day",
                        "limit_price":"215.00",
                        "status":"held"
                    },
                    {
                        "id":"VEN-BRK-SL",
                        "client_order_id":"CID-BRK-SL",
                        "created_at":"2026-03-31T14:32:00Z",
                        "symbol":"AAPL",
                        "qty":"50",
                        "filled_qty":"0",
                        "order_class":"bracket",
                        "type":"stop",
                        "side":"sell",
                        "time_in_force":"day",
                        "stop_price":"192.00",
                        "status":"held"
                    }
                ]
            }"#,
        )
        .await;

        // 4. OCO OrderList (2 exit legs of same side: Limit Sell TP + StopLimit Sell SL)
        let (mut s4, _) = http_listener.accept().await.unwrap();
        let (_, path4, body4) = read_http_request(&mut s4).await;
        assert_eq!(path4, "/v2/orders");
        let j4: serde_json::Value = serde_json::from_str(&body4).unwrap();
        recorded_srv.lock().push(j4);
        write_http_response(
            &mut s4,
            "200 OK",
            r#"{
                "id":"VEN-OCO-TP",
                "client_order_id":"CID-OCO-TP",
                "created_at":"2026-03-31T14:33:00Z",
                "symbol":"MSFT",
                "qty":"30",
                "filled_qty":"0",
                "order_class":"oco",
                "type":"limit",
                "side":"sell",
                "time_in_force":"day",
                "limit_price":"430.00",
                "status":"accepted",
                "legs":[
                    {
                        "id":"VEN-OCO-SL",
                        "client_order_id":"CID-OCO-SL",
                        "created_at":"2026-03-31T14:33:00Z",
                        "symbol":"MSFT",
                        "qty":"30",
                        "filled_qty":"0",
                        "order_class":"oco",
                        "type":"stop_limit",
                        "side":"sell",
                        "time_in_force":"day",
                        "limit_price":"399.00",
                        "stop_price":"400.00",
                        "status":"accepted"
                    }
                ]
            }"#,
        )
        .await;

        // 5. OTO OrderList (Parent Market Buy + Contingent StopMarket Sell SL)
        let (mut s5, _) = http_listener.accept().await.unwrap();
        let (_, path5, body5) = read_http_request(&mut s5).await;
        assert_eq!(path5, "/v2/orders");
        let j5: serde_json::Value = serde_json::from_str(&body5).unwrap();
        recorded_srv.lock().push(j5);
        write_http_response(
            &mut s5,
            "200 OK",
            r#"{
                "id":"VEN-OTO-PARENT",
                "client_order_id":"CID-OTO-PARENT",
                "created_at":"2026-03-31T14:34:00Z",
                "symbol":"TSLA",
                "qty":"20",
                "filled_qty":"0",
                "order_class":"oto",
                "type":"market",
                "side":"buy",
                "time_in_force":"day",
                "status":"accepted",
                "legs":[
                    {
                        "id":"VEN-OTO-SL",
                        "client_order_id":"CID-OTO-SL",
                        "created_at":"2026-03-31T14:34:00Z",
                        "symbol":"TSLA",
                        "qty":"20",
                        "filled_qty":"0",
                        "order_class":"oto",
                        "type":"stop",
                        "side":"sell",
                        "time_in_force":"day",
                        "stop_price":"240.00",
                        "status":"held"
                    }
                ]
            }"#,
        )
        .await;
    });

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
    replace_exec_event_sender(tx);

    let cache = Rc::new(RefCell::new(Cache::default()));
    let config = make_test_config(&http_base_url, "ws://127.0.0.1:9/stream");
    let client = AlpacaExecutionClient::new(
        TraderId::from("TRADER-001"),
        ClientId::from(ALPACA_VENUE),
        AccountId::from("ALPACA-001"),
        config,
        cache.clone(),
    )
    .unwrap()
    .with_ws_enabled(false);

    // 1. Submit LimitOrder
    let lmt_order = make_limit_order(
        "CID-LMT-1",
        "NVDA",
        OrderSide::Buy,
        "25",
        "135.50",
        None,
        None,
        None,
        None,
    );
    cache
        .borrow_mut()
        .add_order(lmt_order.clone(), None, Some(ClientId::from(ALPACA_VENUE)), false)
        .unwrap();
    client.submit_order(make_submit_order_cmd(&lmt_order)).unwrap();

    // 2. Submit StopLimitOrder
    let slmt_order = make_stop_limit_order(
        "CID-SLMT-1",
        "NVDA",
        OrderSide::Sell,
        "25",
        "129.50",
        "130.00",
        None,
        None,
        None,
        None,
    );
    cache
        .borrow_mut()
        .add_order(slmt_order.clone(), None, Some(ClientId::from(ALPACA_VENUE)), false)
        .unwrap();
    client.submit_order(make_submit_order_cmd(&slmt_order)).unwrap();

    let single_events = drain_events(&mut rx);
    assert_eq!(single_events.len(), 4);
    assert!(matches!(
        single_events[1],
        ExecutionEvent::Order(OrderEventAny::Accepted(ref a))
            if a.client_order_id == ClientOrderId::from("CID-LMT-1")
                && a.venue_order_id == VenueOrderId::from("VEN-LMT-1")
    ));
    assert!(matches!(
        single_events[3],
        ExecutionEvent::Order(OrderEventAny::Accepted(ref a))
            if a.client_order_id == ClientOrderId::from("CID-SLMT-1")
                && a.venue_order_id == VenueOrderId::from("VEN-SLMT-1")
    ));

    // 3. Submit Bracket OrderList (ContingencyType::Oto on parent, Oco on children)
    let brk_list_id = OrderListId::from("OL-BRACKET-1");
    let brk_parent = make_limit_order(
        "CID-BRK-PARENT",
        "AAPL",
        OrderSide::Buy,
        "50",
        "200.00",
        Some(ContingencyType::Oto),
        Some(brk_list_id),
        Some(vec![
            ClientOrderId::from("CID-BRK-TP"),
            ClientOrderId::from("CID-BRK-SL"),
        ]),
        None,
    );
    let brk_tp = make_limit_order(
        "CID-BRK-TP",
        "AAPL",
        OrderSide::Sell,
        "50",
        "215.00",
        Some(ContingencyType::Oco),
        Some(brk_list_id),
        Some(vec![ClientOrderId::from("CID-BRK-SL")]),
        Some(ClientOrderId::from("CID-BRK-PARENT")),
    );
    let brk_sl = make_stop_market_order(
        "CID-BRK-SL",
        "AAPL",
        OrderSide::Sell,
        "50",
        "192.00",
        Some(ContingencyType::Oco),
        Some(brk_list_id),
        Some(vec![ClientOrderId::from("CID-BRK-TP")]),
        Some(ClientOrderId::from("CID-BRK-PARENT")),
    );
    let brk_orders = vec![brk_parent, brk_tp, brk_sl];
    let brk_order_list = OrderList::new(
        brk_list_id,
        InstrumentId::from("AAPL.ALPACA"),
        StrategyId::from("STRAT-001"),
        vec![
            ClientOrderId::from("CID-BRK-PARENT"),
            ClientOrderId::from("CID-BRK-TP"),
            ClientOrderId::from("CID-BRK-SL"),
        ],
        UnixNanos::from(1_700_000_000_000_000_000u64),
    );
    {
        let mut c = cache.borrow_mut();
        for o in &brk_orders {
            c.add_order(o.clone(), None, Some(ClientId::from(ALPACA_VENUE)), false)
                .unwrap();
        }
        c.add_order_list(brk_order_list.clone()).unwrap();
    }
    let brk_cmd = SubmitOrderList::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from(ALPACA_VENUE)),
        StrategyId::from("STRAT-001"),
        brk_order_list,
        brk_orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(1_700_000_000_000_000_000u64),
        None,
    );
    client.submit_order_list(brk_cmd).unwrap();

    let brk_events = drain_events(&mut rx);
    // 3 Submitted + 3 Accepted = 6 events
    assert_eq!(brk_events.len(), 6);
    assert!(matches!(
        brk_events[3],
        ExecutionEvent::Order(OrderEventAny::Accepted(ref a))
            if a.client_order_id == ClientOrderId::from("CID-BRK-PARENT")
                && a.venue_order_id == VenueOrderId::from("VEN-BRK-PARENT")
    ));
    assert!(matches!(
        brk_events[4],
        ExecutionEvent::Order(OrderEventAny::Accepted(ref a))
            if a.client_order_id == ClientOrderId::from("CID-BRK-TP")
                && a.venue_order_id == VenueOrderId::from("VEN-BRK-TP")
    ));
    assert!(matches!(
        brk_events[5],
        ExecutionEvent::Order(OrderEventAny::Accepted(ref a))
            if a.client_order_id == ClientOrderId::from("CID-BRK-SL")
                && a.venue_order_id == VenueOrderId::from("VEN-BRK-SL")
    ));

    // 4. Submit OCO OrderList (ContingencyType::Oco)
    let oco_list_id = OrderListId::from("OL-OCO-1");
    let oco_tp = make_limit_order(
        "CID-OCO-TP",
        "MSFT",
        OrderSide::Sell,
        "30",
        "430.00",
        Some(ContingencyType::Oco),
        Some(oco_list_id),
        Some(vec![ClientOrderId::from("CID-OCO-SL")]),
        None,
    );
    let oco_sl = make_stop_limit_order(
        "CID-OCO-SL",
        "MSFT",
        OrderSide::Sell,
        "30",
        "399.00",
        "400.00",
        Some(ContingencyType::Oco),
        Some(oco_list_id),
        Some(vec![ClientOrderId::from("CID-OCO-TP")]),
        None,
    );
    let oco_orders = vec![oco_tp, oco_sl];
    let oco_order_list = OrderList::new(
        oco_list_id,
        InstrumentId::from("MSFT.ALPACA"),
        StrategyId::from("STRAT-001"),
        vec![
            ClientOrderId::from("CID-OCO-TP"),
            ClientOrderId::from("CID-OCO-SL"),
        ],
        UnixNanos::from(1_700_000_000_000_000_000u64),
    );
    {
        let mut c = cache.borrow_mut();
        for o in &oco_orders {
            c.add_order(o.clone(), None, Some(ClientId::from(ALPACA_VENUE)), false)
                .unwrap();
        }
        c.add_order_list(oco_order_list.clone()).unwrap();
    }
    let oco_cmd = SubmitOrderList::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from(ALPACA_VENUE)),
        StrategyId::from("STRAT-001"),
        oco_order_list,
        oco_orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(1_700_000_000_000_000_000u64),
        None,
    );
    client.submit_order_list(oco_cmd).unwrap();

    let oco_events = drain_events(&mut rx);
    assert_eq!(oco_events.len(), 4);
    assert!(matches!(
        oco_events[2],
        ExecutionEvent::Order(OrderEventAny::Accepted(ref a))
            if a.client_order_id == ClientOrderId::from("CID-OCO-TP")
                && a.venue_order_id == VenueOrderId::from("VEN-OCO-TP")
    ));
    assert!(matches!(
        oco_events[3],
        ExecutionEvent::Order(OrderEventAny::Accepted(ref a))
            if a.client_order_id == ClientOrderId::from("CID-OCO-SL")
                && a.venue_order_id == VenueOrderId::from("VEN-OCO-SL")
    ));

    // 5. Submit OTO OrderList (ContingencyType::Oto)
    let oto_list_id = OrderListId::from("OL-OTO-1");
    let oto_parent = make_market_order(
        "CID-OTO-PARENT",
        "TSLA",
        OrderSide::Buy,
        "20",
        Some(ContingencyType::Oto),
        Some(oto_list_id),
        Some(vec![ClientOrderId::from("CID-OTO-SL")]),
        None,
    );
    let oto_sl = make_stop_market_order(
        "CID-OTO-SL",
        "TSLA",
        OrderSide::Sell,
        "20",
        "240.00",
        None,
        Some(oto_list_id),
        None,
        Some(ClientOrderId::from("CID-OTO-PARENT")),
    );
    let oto_orders = vec![oto_parent, oto_sl];
    let oto_order_list = OrderList::new(
        oto_list_id,
        InstrumentId::from("TSLA.ALPACA"),
        StrategyId::from("STRAT-001"),
        vec![
            ClientOrderId::from("CID-OTO-PARENT"),
            ClientOrderId::from("CID-OTO-SL"),
        ],
        UnixNanos::from(1_700_000_000_000_000_000u64),
    );
    {
        let mut c = cache.borrow_mut();
        for o in &oto_orders {
            c.add_order(o.clone(), None, Some(ClientId::from(ALPACA_VENUE)), false)
                .unwrap();
        }
        c.add_order_list(oto_order_list.clone()).unwrap();
    }
    let oto_cmd = SubmitOrderList::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from(ALPACA_VENUE)),
        StrategyId::from("STRAT-001"),
        oto_order_list,
        oto_orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(1_700_000_000_000_000_000u64),
        None,
    );
    client.submit_order_list(oto_cmd).unwrap();

    let oto_events = drain_events(&mut rx);
    assert_eq!(oto_events.len(), 4);
    assert!(matches!(
        oto_events[2],
        ExecutionEvent::Order(OrderEventAny::Accepted(ref a))
            if a.client_order_id == ClientOrderId::from("CID-OTO-PARENT")
                && a.venue_order_id == VenueOrderId::from("VEN-OTO-PARENT")
    ));
    assert!(matches!(
        oto_events[3],
        ExecutionEvent::Order(OrderEventAny::Accepted(ref a))
            if a.client_order_id == ClientOrderId::from("CID-OTO-SL")
                && a.venue_order_id == VenueOrderId::from("VEN-OTO-SL")
    ));

    http_server.await.unwrap();

    // Verify wire payloads sent to Alpaca POST /v2/orders
    let reqs = recorded_requests.lock();
    assert_eq!(reqs.len(), 5);
    assert_eq!(reqs[0]["order_class"], "simple");
    assert_eq!(reqs[0]["type"], "limit");
    assert_eq!(reqs[0]["limit_price"], "135.50");

    assert_eq!(reqs[1]["order_class"], "simple");
    assert_eq!(reqs[1]["type"], "stop_limit");
    assert_eq!(reqs[1]["limit_price"], "129.50");
    assert_eq!(reqs[1]["stop_price"], "130.00");

    assert_eq!(reqs[2]["order_class"], "bracket");
    assert_eq!(reqs[2]["take_profit"]["limit_price"], "215.00");
    assert_eq!(reqs[2]["stop_loss"]["stop_price"], "192.00");

    assert_eq!(reqs[3]["order_class"], "oco");
    assert_eq!(reqs[3]["take_profit"]["limit_price"], "430.00");
    assert_eq!(reqs[3]["stop_loss"]["stop_price"], "400.00");
    assert_eq!(reqs[3]["stop_loss"]["limit_price"], "399.00");

    assert_eq!(reqs[4]["order_class"], "oto");
    assert_eq!(reqs[4]["stop_loss"]["stop_price"], "240.00");
}

// =================================================================================================
// 3. ModifyOrder, CancelOrder, CancelAllOrders, and BatchCancelOrders DST
// =================================================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_modify_cancel_cancel_all_and_batch_cancel_orders_dst() {
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    let http_base_url = format!("http://{http_addr}");

    let http_server = tokio::spawn(async move {
        // 1. Submit order CID-MOD-1 -> VEN-MOD-1
        let (mut s1, _) = http_listener.accept().await.unwrap();
        let (m1, p1, _) = read_http_request(&mut s1).await;
        assert_eq!((m1.as_str(), p1.as_str()), ("POST", "/v2/orders"));
        write_http_response(
            &mut s1,
            "200 OK",
            r#"{"id":"VEN-MOD-1","client_order_id":"CID-MOD-1","created_at":"2026-03-31T14:40:00Z","symbol":"AAPL","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"200.00","status":"accepted"}"#,
        )
        .await;

        // 2. ModifyOrder PATCH /v2/orders/VEN-MOD-1 -> VEN-MOD-2
        let (mut s2, _) = http_listener.accept().await.unwrap();
        let (m2, p2, body2) = read_http_request(&mut s2).await;
        assert_eq!((m2.as_str(), p2.as_str()), ("PATCH", "/v2/orders/VEN-MOD-1"));
        let mod_json: serde_json::Value = serde_json::from_str(&body2).unwrap();
        assert_eq!(mod_json["qty"], "15");
        assert_eq!(mod_json["limit_price"], "202.50");
        write_http_response(
            &mut s2,
            "200 OK",
            r#"{"id":"VEN-MOD-2","replaces":"VEN-MOD-1","client_order_id":"CID-MOD-1","created_at":"2026-03-31T14:40:05Z","symbol":"AAPL","qty":"15","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"202.50","status":"replaced"}"#,
        )
        .await;

        // 3. CancelOrder DELETE /v2/orders/VEN-MOD-2
        let (mut s3, _) = http_listener.accept().await.unwrap();
        let (m3, p3, _) = read_http_request(&mut s3).await;
        assert_eq!((m3.as_str(), p3.as_str()), ("DELETE", "/v2/orders/VEN-MOD-2"));
        write_http_response(&mut s3, "204 No Content", "").await;

        // 4. CancelAllOrders: GET /v2/orders?status=open followed by DELETE /v2/orders/VEN-ALL-1
        let (mut s4, _) = http_listener.accept().await.unwrap();
        let (m4, p4, _) = read_http_request(&mut s4).await;
        assert_eq!(
            (m4.as_str(), p4.as_str()),
            ("GET", "/v2/orders?status=open")
        );
        write_http_response(
            &mut s4,
            "200 OK",
            r#"[
                {"id":"VEN-ALL-1","client_order_id":"CID-ALL-1","created_at":"2026-03-31T14:41:00Z","symbol":"NVDA","qty":"20","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"130.00","status":"new"},
                {"id":"VEN-OTHER-SYM","client_order_id":"CID-OTHER","created_at":"2026-03-31T14:41:00Z","symbol":"MSFT","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"400.00","status":"new"}
            ]"#,
        )
        .await;

        let (mut s5, _) = http_listener.accept().await.unwrap();
        let (m5, p5, _) = read_http_request(&mut s5).await;
        assert_eq!((m5.as_str(), p5.as_str()), ("DELETE", "/v2/orders/VEN-ALL-1"));
        write_http_response(&mut s5, "204 No Content", "").await;

        // 5. BatchCancelOrders: DELETE /v2/orders/VEN-BATCH-1 and DELETE /v2/orders/VEN-BATCH-2
        let (mut s6, _) = http_listener.accept().await.unwrap();
        let (m6, p6, _) = read_http_request(&mut s6).await;
        assert_eq!(
            (m6.as_str(), p6.as_str()),
            ("DELETE", "/v2/orders/VEN-BATCH-1")
        );
        write_http_response(&mut s6, "204 No Content", "").await;

        let (mut s7, _) = http_listener.accept().await.unwrap();
        let (m7, p7, _) = read_http_request(&mut s7).await;
        assert_eq!(
            (m7.as_str(), p7.as_str()),
            ("DELETE", "/v2/orders/VEN-BATCH-2")
        );
        write_http_response(&mut s7, "204 No Content", "").await;
    });

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
    replace_exec_event_sender(tx);

    let cache = Rc::new(RefCell::new(Cache::default()));
    let config = make_test_config(&http_base_url, "ws://127.0.0.1:9/stream");
    let client = AlpacaExecutionClient::new(
        TraderId::from("TRADER-001"),
        ClientId::from(ALPACA_VENUE),
        AccountId::from("ALPACA-001"),
        config,
        cache.clone(),
    )
    .unwrap()
    .with_ws_enabled(false);

    // Add orders to cache
    let mod_order = make_limit_order(
        "CID-MOD-1",
        "AAPL",
        OrderSide::Buy,
        "10",
        "200.00",
        None,
        None,
        None,
        None,
    );
    let all_order = make_limit_order(
        "CID-ALL-1",
        "NVDA",
        OrderSide::Buy,
        "20",
        "130.00",
        None,
        None,
        None,
        None,
    );
    let batch_order_1 = make_limit_order(
        "CID-BATCH-1",
        "NVDA",
        OrderSide::Buy,
        "10",
        "131.00",
        None,
        None,
        None,
        None,
    );
    let batch_order_2 = make_limit_order(
        "CID-BATCH-2",
        "NVDA",
        OrderSide::Sell,
        "10",
        "145.00",
        None,
        None,
        None,
        None,
    );
    {
        let mut c = cache.borrow_mut();
        for o in [&mod_order, &all_order, &batch_order_1, &batch_order_2] {
            c.add_order(o.clone(), None, Some(ClientId::from(ALPACA_VENUE)), false)
                .unwrap();
        }
    }

    // 1. Submit CID-MOD-1
    client.submit_order(make_submit_order_cmd(&mod_order)).unwrap();
    let _ = drain_events(&mut rx);

    // 2. Modify CID-MOD-1 (uses tracked VenueOrderId VEN-MOD-1 -> replaces with VEN-MOD-2)
    let modify_cmd = ModifyOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from(ALPACA_VENUE)),
        StrategyId::from("STRAT-001"),
        InstrumentId::from("AAPL.ALPACA"),
        ClientOrderId::from("CID-MOD-1"),
        None,
        Some(Quantity::from("15")),
        Some(Price::from("202.50")),
        None,
        UUID4::new(),
        UnixNanos::from(1_700_000_000_000_000_000u64),
        None,
        None,
    );
    client.modify_order(modify_cmd).unwrap();

    let mod_events = drain_events(&mut rx);
    assert_eq!(mod_events.len(), 1);
    match &mod_events[0] {
        ExecutionEvent::Order(OrderEventAny::Updated(u)) => {
            assert_eq!(u.client_order_id, ClientOrderId::from("CID-MOD-1"));
            assert_eq!(u.venue_order_id, Some(VenueOrderId::from("VEN-MOD-2")));
            assert_eq!(u.quantity, Quantity::from("15"));
            assert_eq!(u.price, Some(Price::from("202.50")));
        }
        other => panic!("expected OrderUpdated event, got {other:?}"),
    }

    // 3. Cancel CID-MOD-1 (should automatically resolve current VenueOrderId VEN-MOD-2)
    let cancel_cmd = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from(ALPACA_VENUE)),
        StrategyId::from("STRAT-001"),
        InstrumentId::from("AAPL.ALPACA"),
        ClientOrderId::from("CID-MOD-1"),
        None,
        UUID4::new(),
        UnixNanos::from(1_700_000_000_000_000_000u64),
        None,
        None,
    );
    client.cancel_order(cancel_cmd).unwrap();

    let cancel_events = drain_events(&mut rx);
    assert_eq!(cancel_events.len(), 1);
    assert!(matches!(
        cancel_events[0],
        ExecutionEvent::Order(OrderEventAny::Canceled(ref c))
            if c.client_order_id == ClientOrderId::from("CID-MOD-1")
                && c.venue_order_id == Some(VenueOrderId::from("VEN-MOD-2"))
    ));

    // 4. CancelAllOrders for NVDA.ALPACA
    let cancel_all_cmd = CancelAllOrders::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from(ALPACA_VENUE)),
        StrategyId::from("STRAT-001"),
        InstrumentId::from("NVDA.ALPACA"),
        Some(OrderSide::Buy),
        UUID4::new(),
        UnixNanos::from(1_700_000_000_000_000_000u64),
        None,
        None,
    );
    client.cancel_all_orders(cancel_all_cmd).unwrap();

    let cancel_all_events = drain_events(&mut rx);
    assert_eq!(cancel_all_events.len(), 1);
    assert!(matches!(
        cancel_all_events[0],
        ExecutionEvent::Order(OrderEventAny::Canceled(ref c))
            if c.client_order_id == ClientOrderId::from("CID-ALL-1")
                && c.venue_order_id == Some(VenueOrderId::from("VEN-ALL-1"))
    ));

    // 5. BatchCancelOrders for CID-BATCH-1 and CID-BATCH-2
    let batch_cancel_cmd = BatchCancelOrders::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from(ALPACA_VENUE)),
        StrategyId::from("STRAT-001"),
        InstrumentId::from("NVDA.ALPACA"),
        vec![
            CancelOrder::new(
                TraderId::from("TRADER-001"),
                Some(ClientId::from(ALPACA_VENUE)),
                StrategyId::from("STRAT-001"),
                InstrumentId::from("NVDA.ALPACA"),
                ClientOrderId::from("CID-BATCH-1"),
                Some(VenueOrderId::from("VEN-BATCH-1")),
                UUID4::new(),
                UnixNanos::from(1_700_000_000_000_000_000u64),
                None,
                None,
            ),
            CancelOrder::new(
                TraderId::from("TRADER-001"),
                Some(ClientId::from(ALPACA_VENUE)),
                StrategyId::from("STRAT-001"),
                InstrumentId::from("NVDA.ALPACA"),
                ClientOrderId::from("CID-BATCH-2"),
                Some(VenueOrderId::from("VEN-BATCH-2")),
                UUID4::new(),
                UnixNanos::from(1_700_000_000_000_000_000u64),
                None,
                None,
            ),
        ],
        UUID4::new(),
        UnixNanos::from(1_700_000_000_000_000_000u64),
        None,
        None,
    );
    client.batch_cancel_orders(batch_cancel_cmd).unwrap();

    let batch_cancel_events = drain_events(&mut rx);
    assert_eq!(
        batch_cancel_events.len(),
        2,
        "batch_cancel_orders must cancel each order in the batch and emit OrderCanceled"
    );
    assert!(matches!(
        batch_cancel_events[0],
        ExecutionEvent::Order(OrderEventAny::Canceled(ref c))
            if c.client_order_id == ClientOrderId::from("CID-BATCH-1")
                && c.venue_order_id == Some(VenueOrderId::from("VEN-BATCH-1"))
    ));
    assert!(matches!(
        batch_cancel_events[1],
        ExecutionEvent::Order(OrderEventAny::Canceled(ref c))
            if c.client_order_id == ClientOrderId::from("CID-BATCH-2")
                && c.venue_order_id == Some(VenueOrderId::from("VEN-BATCH-2"))
    ));

    http_server.await.unwrap();
}

// =================================================================================================
// 4. Startup & Periodic Reconciliation Reports + generate_mass_status DST
// =================================================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_reconciliation_order_fill_position_reports_and_mass_status_dst() {
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    let http_base_url = format!("http://{http_addr}");

    let orders_payload = r#"[
        {
            "id":"VEN-REC-BRK",
            "client_order_id":"CID-REC-BRK",
            "created_at":"2026-03-31T13:30:00Z",
            "submitted_at":"2026-03-31T13:30:01Z",
            "filled_at":"2026-03-31T13:30:05Z",
            "symbol":"AAPL",
            "qty":"40",
            "filled_qty":"40",
            "filled_avg_price":"210.50",
            "order_class":"bracket",
            "type":"limit",
            "side":"buy",
            "time_in_force":"day",
            "limit_price":"211.00",
            "status":"filled",
            "legs":[
                {
                    "id":"VEN-REC-TP",
                    "client_order_id":"CID-REC-TP",
                    "created_at":"2026-03-31T13:30:00Z",
                    "submitted_at":"2026-03-31T13:30:05Z",
                    "symbol":"AAPL",
                    "qty":"40",
                    "filled_qty":"0",
                    "order_class":"bracket",
                    "type":"limit",
                    "side":"sell",
                    "time_in_force":"gtc",
                    "limit_price":"225.00",
                    "status":"new"
                },
                {
                    "id":"VEN-REC-SL",
                    "client_order_id":"CID-REC-SL",
                    "created_at":"2026-03-31T13:30:00Z",
                    "submitted_at":"2026-03-31T13:30:05Z",
                    "symbol":"AAPL",
                    "qty":"40",
                    "filled_qty":"0",
                    "order_class":"bracket",
                    "type":"stop",
                    "side":"sell",
                    "time_in_force":"gtc",
                    "stop_price":"200.00",
                    "status":"held"
                }
            ]
        },
        {
            "id":"VEN-REC-REPL-2",
            "replaces":"VEN-REC-REPL-1",
            "client_order_id":"CID-REC-REPL",
            "created_at":"2026-03-31T13:35:00Z",
            "submitted_at":"2026-03-31T13:35:01Z",
            "symbol":"NVDA",
            "qty":"15",
            "filled_qty":"0",
            "order_class":"simple",
            "type":"limit",
            "side":"buy",
            "time_in_force":"day",
            "limit_price":"135.00",
            "status":"accepted"
        }
    ]"#;

    let positions_payload = r#"[
        {
            "asset_id":"asset-aapl",
            "symbol":"AAPL",
            "exchange":"NASDAQ",
            "asset_class":"us_equity",
            "avg_entry_price":"210.50",
            "qty":"40",
            "qty_available":"0",
            "side":"long",
            "market_value":"8480.00",
            "cost_basis":"8420.00",
            "unrealized_pl":"60.00",
            "current_price":"212.00"
        },
        {
            "asset_id":"asset-tsla",
            "symbol":"TSLA",
            "exchange":"NASDAQ",
            "asset_class":"us_equity",
            "avg_entry_price":"255.00",
            "qty":"-10",
            "qty_available":"-10",
            "side":"short",
            "market_value":"-2500.00",
            "cost_basis":"-2550.00",
            "unrealized_pl":"50.00",
            "current_price":"250.00"
        }
    ]"#;

    let http_server = tokio::spawn(async move {
        // Serve 6 requests:
        // 1. generate_order_status_reports -> GET /v2/orders?status=all
        // 2. generate_fill_reports -> GET /v2/orders?status=all
        // 3. generate_position_status_reports -> GET /v2/positions
        // 4..=6. generate_mass_status -> concurrent GET /v2/orders?status=all (x2) + GET /v2/positions (x1)
        for _ in 0..6 {
            let (mut sock, _) = http_listener.accept().await.unwrap();
            let (_method, path, _) = read_http_request(&mut sock).await;
            if path.starts_with("/v2/orders") {
                write_http_response(&mut sock, "200 OK", orders_payload).await;
            } else if path.starts_with("/v2/positions") {
                write_http_response(&mut sock, "200 OK", positions_payload).await;
            } else {
                write_http_response(&mut sock, "404 Not Found", "{}").await;
            }
        }
    });

    let cache = Rc::new(RefCell::new(Cache::default()));
    let config = make_test_config(&http_base_url, "ws://127.0.0.1:9/stream");
    let client = AlpacaExecutionClient::new(
        TraderId::from("TRADER-001"),
        ClientId::from(ALPACA_VENUE),
        AccountId::from("ALPACA-001"),
        config,
        cache,
    )
    .unwrap()
    .with_ws_enabled(false);

    let ts_init = UnixNanos::from(1_700_000_000_000_000_000u64);

    // 1. generate_order_status_reports (flattens parent + 2 bracket legs + 1 replaced order = 4 reports)
    let order_cmd = GenerateOrderStatusReportsBuilder::default()
        .ts_init(ts_init)
        .open_only(false)
        .build()
        .unwrap();
    let order_reports = client
        .generate_order_status_reports(&order_cmd)
        .await
        .expect("generate_order_status_reports must succeed");
    assert_eq!(order_reports.len(), 4);
    assert_eq!(
        order_reports[0].venue_order_id,
        VenueOrderId::from("VEN-REC-BRK")
    );
    assert_eq!(order_reports[0].order_status, OrderStatus::Filled);
    assert_eq!(
        order_reports[1].venue_order_id,
        VenueOrderId::from("VEN-REC-TP")
    );
    assert_eq!(order_reports[1].order_status, OrderStatus::Accepted);
    assert_eq!(
        order_reports[2].venue_order_id,
        VenueOrderId::from("VEN-REC-SL")
    );
    assert_eq!(order_reports[2].order_status, OrderStatus::Accepted);
    assert_eq!(
        order_reports[3].venue_order_id,
        VenueOrderId::from("VEN-REC-REPL-2")
    );

    // 2. generate_fill_reports
    let fill_cmd = GenerateFillReportsBuilder::default()
        .ts_init(ts_init)
        .build()
        .unwrap();
    let fill_reports = client
        .generate_fill_reports(fill_cmd)
        .await
        .expect("generate_fill_reports must succeed");
    assert_eq!(fill_reports.len(), 1);
    assert_eq!(
        fill_reports[0].venue_order_id,
        VenueOrderId::from("VEN-REC-BRK")
    );
    assert_eq!(
        fill_reports[0].client_order_id,
        Some(ClientOrderId::from("CID-REC-BRK"))
    );
    assert_eq!(fill_reports[0].last_qty, Quantity::from("40"));
    assert_eq!(fill_reports[0].last_px, Price::from("210.50"));
    assert_eq!(fill_reports[0].trade_id, TradeId::from("FILL-VEN-REC-BRK"));

    // 3. generate_position_status_reports
    let pos_cmd = GeneratePositionStatusReportsBuilder::default()
        .ts_init(ts_init)
        .build()
        .unwrap();
    let pos_reports = client
        .generate_position_status_reports(&pos_cmd)
        .await
        .expect("generate_position_status_reports must succeed");
    assert_eq!(pos_reports.len(), 2);
    assert_eq!(
        pos_reports[0].instrument_id,
        InstrumentId::from("AAPL.ALPACA")
    );
    assert_eq!(pos_reports[0].position_side, PositionSide::Long);
    assert_eq!(pos_reports[0].quantity, Quantity::from("40"));
    assert_eq!(
        pos_reports[1].instrument_id,
        InstrumentId::from("TSLA.ALPACA")
    );
    assert_eq!(pos_reports[1].position_side, PositionSide::Short);
    assert_eq!(pos_reports[1].quantity, Quantity::from("10"));

    // 4. generate_mass_status
    let mass_status = client
        .generate_mass_status(None)
        .await
        .expect("generate_mass_status must succeed")
        .expect("ExecutionMassStatus must be Some");
    assert_eq!(mass_status.client_id, ClientId::from(ALPACA_VENUE));
    assert_eq!(mass_status.account_id, AccountId::from("ALPACA-001"));
    assert_eq!(mass_status.venue, Venue::from(ALPACA_VENUE));
    assert_eq!(mass_status.order_reports().len(), 4);
    assert_eq!(mass_status.fill_reports().len(), 1);
    assert_eq!(mass_status.position_reports().len(), 2);

    // Verify venue_order_ids replacement history recorded from `replaces` field
    let vid_history = client
        .venue_order_ids(&mass_status)
        .expect("replaced order must be tracked in venue_order_ids");
    assert_eq!(
        vid_history.get(&ClientOrderId::from("CID-REC-REPL")),
        Some(&vec![
            VenueOrderId::from("VEN-REC-REPL-1"),
            VenueOrderId::from("VEN-REC-REPL-2"),
        ])
    );

    http_server.await.unwrap();
}
