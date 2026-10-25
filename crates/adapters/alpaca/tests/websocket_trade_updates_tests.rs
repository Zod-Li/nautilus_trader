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

//! Integration and unit verification suite for `nautilus-alpaca` Ticket #07:
//! - Physical OOD separation between session control signals (`AlpacaWsControlSignal`) and
//!   business execution events (`AlpacaExecutionUpdate`).
//! - `classify_alpaca_ws_frame` across JSON and binary frames (`authorization`, `listening`,
//!   `trade_updates`, and v2 array-wrapped control frames) plus fail-closed error handling.
//! - `parse_trade_update_payload` across all order lifecycle events (`new`, `partial_fill`,
//!   `fill`, `canceled`, `expired`, `rejected`, `replaced`, `done_for_day`, `pending_new`,
//!   `pending_cancel`, `pending_replace`) with exact decimal conversion and fail-closed validation.
//! - End-to-end `AlpacaWsClient` handshake, live event streaming, fail-closed auth rejection,
//!   and automatic WebSocket drop & reconnect recovery against a local `tokio_tungstenite` peer.

#![allow(linker_messages)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use nautilus_alpaca::{
    ALPACA_PAPER_HTTP_URL, ALPACA_VENUE, AlpacaConfig, AlpacaCredential, AlpacaEnvironment,
    AlpacaError, AlpacaExecutionUpdate, AlpacaOrderClass, AlpacaOrderSide, AlpacaOrderType,
    AlpacaTimeInForce, AlpacaTradeEventKind, AlpacaTradeUpdatePayload, AlpacaWsClient,
    AlpacaWsLifecycleObserver, AlpacaWsSessionState,
    http::AlpacaOrderResponse,
    websocket::{
        AlpacaWsAuthenticateCommand, AlpacaWsControlSignal, AlpacaWsFrameClassification,
        AlpacaWsListenCommand, TRADE_UPDATES_STREAM, classify_alpaca_ws_frame,
        parse_trade_update_payload,
    },
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{LiquiditySide, OrderSide, OrderStatus, OrderType},
    identifiers::{AccountId, ClientOrderId, InstrumentId, TradeId, VenueOrderId},
    types::{Price, Quantity},
};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::Message as WsMessage};

// =================================================================================================
// Test Helpers & Lifecycle Observer
// =================================================================================================

#[derive(Debug, Default)]
struct RecordingLifecycleObserver {
    authenticated_count: AtomicU32,
    listening_streams: Mutex<Vec<Vec<String>>>,
    reconnect_attempts: Mutex<Vec<u32>>,
    auth_failures: Mutex<Vec<String>>,
}

impl AlpacaWsLifecycleObserver for RecordingLifecycleObserver {
    fn on_ws_authenticated(&self) {
        self.authenticated_count.fetch_add(1, Ordering::SeqCst);
    }

    fn on_ws_listening(&self, streams: &[String]) {
        self.listening_streams.lock().push(streams.to_vec());
    }

    fn on_ws_reconnecting(&self, reconnect_count: u32) {
        self.reconnect_attempts.lock().push(reconnect_count);
    }

    fn on_ws_auth_failed(&self, reason: &str) {
        self.auth_failures.lock().push(reason.to_string());
    }
}

fn sample_order_response(
    id: &str,
    client_order_id: &str,
    symbol: &str,
    side: AlpacaOrderSide,
    order_type: AlpacaOrderType,
    status: &str,
    qty: Decimal,
    filled_qty: Decimal,
    filled_avg_price: Option<Decimal>,
    limit_price: Option<Decimal>,
) -> AlpacaOrderResponse {
    AlpacaOrderResponse {
        id: id.to_string(),
        client_order_id: client_order_id.to_string(),
        created_at: "2026-03-31T13:30:00Z".to_string(),
        updated_at: Some("2026-03-31T13:30:01Z".to_string()),
        submitted_at: Some("2026-03-31T13:30:00.100Z".to_string()),
        filled_at: if filled_qty > Decimal::ZERO {
            Some("2026-03-31T13:30:02Z".to_string())
        } else {
            None
        },
        expired_at: None,
        canceled_at: None,
        failed_at: None,
        replaced_at: None,
        replaced_by: None,
        replaces: None,
        asset_id: Some("b28f4066-5c6d-479b-a2af-85dc1a8f16fb".to_string()),
        symbol: symbol.to_string(),
        asset_class: Some("us_equity".to_string()),
        qty: Some(qty),
        filled_qty,
        filled_avg_price,
        order_class: Some(AlpacaOrderClass::Simple),
        order_type,
        side,
        time_in_force: AlpacaTimeInForce::Day,
        limit_price,
        stop_price: None,
        status: status.to_string(),
        extended_hours: false,
        legs: None,
        trail_price: None,
        trail_percent: None,
        hwm: None,
    }
}

fn make_ws_test_config(ws_url: &str) -> AlpacaConfig {
    let cred = AlpacaCredential::new_checked("PKWSKEY123456789", "SKWSSECRET987654321")
        .expect("valid test credential");
    AlpacaConfig::new_checked(
        AlpacaEnvironment::Paper,
        ALPACA_PAPER_HTTP_URL,
        ws_url,
        cred,
        5,
        3,
        50,
        250,
        20,
        ALPACA_VENUE,
    )
    .expect("valid test config")
}

// =================================================================================================
// 1. Wire Frame Classification & Control Signal Separation Tests
// =================================================================================================

#[test]
fn test_outbound_authenticate_and_listen_command_serialization() {
    let auth_cmd = AlpacaWsAuthenticateCommand::new("PK_ID_01", "SK_SEC_01");
    let auth_json: serde_json::Value =
        serde_json::to_value(&auth_cmd).expect("serialize auth command");
    assert_eq!(auth_json["action"], "authenticate");
    assert_eq!(auth_json["data"]["key_id"], "PK_ID_01");
    assert_eq!(auth_json["data"]["secret_key"], "SK_SEC_01");

    let listen_cmd = AlpacaWsListenCommand::trade_updates();
    let listen_json: serde_json::Value =
        serde_json::to_value(&listen_cmd).expect("serialize listen command");
    assert_eq!(listen_json["action"], "listen");
    assert_eq!(
        listen_json["data"]["streams"],
        serde_json::json!(["trade_updates"])
    );
}

#[test]
fn test_classify_authorization_and_listening_control_frames() {
    let auth_ok = br#"{"stream":"authorization","data":{"status":"authorized","action":"authenticate"}}"#;
    let classified = classify_alpaca_ws_frame(auth_ok).expect("valid authorization frame");
    assert_eq!(
        classified,
        AlpacaWsFrameClassification::Control(vec![AlpacaWsControlSignal::Authorization {
            authorized: true,
            message: Some("authenticate".to_string()),
        }])
    );

    let auth_fail = br#"{"stream":"authorization","data":{"status":"unauthorized","action":"authenticate","message":"invalid credentials"}}"#;
    let classified_fail =
        classify_alpaca_ws_frame(auth_fail).expect("valid unauthorized control frame");
    assert_eq!(
        classified_fail,
        AlpacaWsFrameClassification::Control(vec![AlpacaWsControlSignal::Authorization {
            authorized: false,
            message: Some("invalid credentials".to_string()),
        }])
    );

    let listening = br#"{"stream":"listening","data":{"streams":["trade_updates"]}}"#;
    let classified_listen = classify_alpaca_ws_frame(listening).expect("valid listening frame");
    assert_eq!(
        classified_listen,
        AlpacaWsFrameClassification::Control(vec![AlpacaWsControlSignal::Listening {
            streams: vec![TRADE_UPDATES_STREAM.to_string()],
        }])
    );
}

#[test]
fn test_classify_v2_array_control_frames() {
    let array_connected_and_auth =
        br#"[{"T":"success","msg":"connected"},{"T":"success","msg":"authenticated"}]"#;
    let classified =
        classify_alpaca_ws_frame(array_connected_and_auth).expect("valid v2 array frame");
    assert_eq!(
        classified,
        AlpacaWsFrameClassification::Control(vec![
            AlpacaWsControlSignal::Connected,
            AlpacaWsControlSignal::Authorization {
                authorized: true,
                message: None,
            },
        ])
    );

    let array_error = br#"[{"T":"error","code":402,"msg":"auth failed"}]"#;
    let classified_err = classify_alpaca_ws_frame(array_error).expect("valid v2 error frame");
    assert_eq!(
        classified_err,
        AlpacaWsFrameClassification::Control(vec![AlpacaWsControlSignal::Authorization {
            authorized: false,
            message: Some("code=402: auth failed".to_string()),
        }])
    );

    let array_sub = br#"[{"T":"subscription","trades":["trade_updates"]}]"#;
    let classified_sub = classify_alpaca_ws_frame(array_sub).expect("valid v2 subscription frame");
    assert_eq!(
        classified_sub,
        AlpacaWsFrameClassification::Control(vec![AlpacaWsControlSignal::Listening {
            streams: vec![TRADE_UPDATES_STREAM.to_string()],
        }])
    );
}

#[test]
fn test_classify_trade_updates_business_frame_and_fail_closed_errors() {
    let trade_frame = br#"{
        "stream": "trade_updates",
        "data": {
            "event": "fill",
            "execution_id": "exec-998877",
            "price": "185.25",
            "qty": "50",
            "position_qty": "50",
            "timestamp": "2026-03-31T14:00:00Z",
            "order": {
                "id": "ord-111",
                "client_order_id": "cid-111",
                "created_at": "2026-03-31T13:59:59Z",
                "submitted_at": "2026-03-31T13:59:59.100Z",
                "filled_at": "2026-03-31T14:00:00Z",
                "symbol": "AAPL",
                "qty": "50",
                "filled_qty": "50",
                "filled_avg_price": "185.25",
                "type": "limit",
                "side": "buy",
                "time_in_force": "day",
                "limit_price": "185.50",
                "status": "filled"
            }
        }
    }"#;

    let classified = classify_alpaca_ws_frame(trade_frame).expect("valid trade_updates frame");
    match classified {
        AlpacaWsFrameClassification::TradeUpdate(payload) => {
            assert_eq!(payload.event, AlpacaTradeEventKind::Fill);
            assert_eq!(payload.execution_id.as_deref(), Some("exec-998877"));
            assert_eq!(payload.price, Some(dec!(185.25)));
            assert_eq!(payload.qty, Some(dec!(50)));
            assert_eq!(payload.position_qty, Some(dec!(50)));
            assert_eq!(payload.order.symbol, "AAPL");
        }
        other => panic!("expected TradeUpdate classification, got {other:?}"),
    }

    assert!(matches!(
        classify_alpaca_ws_frame(b"   "),
        Err(AlpacaError::Parse(_))
    ));
    assert!(matches!(
        classify_alpaca_ws_frame(br#"{"stream":"unknown_stream","data":{}}"#),
        Err(AlpacaError::Parse(_))
    ));
    assert!(matches!(
        classify_alpaca_ws_frame(br#"[{"T":"mystery"}]"#),
        Err(AlpacaError::Parse(_))
    ));
    assert!(matches!(
        classify_alpaca_ws_frame(b"{malformed json"),
        Err(AlpacaError::Parse(_))
    ));
}

// =================================================================================================
// 2. Domain Parsing (`parse_trade_update_payload`) Across All Lifecycle Events
// =================================================================================================

#[test]
fn test_parse_trade_update_lifecycle_events_and_decimal_precision() {
    let account_id = AccountId::new("ALPACA-PA123456");
    let ts_init = UnixNanos::from(1_775_000_000_000_000_000u64);

    // 1. New (Accepted)
    let new_payload = AlpacaTradeUpdatePayload {
        event: AlpacaTradeEventKind::New,
        execution_id: None,
        price: None,
        qty: None,
        position_qty: Some(dec!(0)),
        timestamp: Some("2026-03-31T13:30:00.500Z".to_string()),
        order: sample_order_response(
            "venue-ord-1",
            "client-ord-1",
            "NVDA",
            AlpacaOrderSide::Buy,
            AlpacaOrderType::Limit,
            "new",
            dec!(100),
            dec!(0),
            None,
            Some(dec!(125.50)),
        ),
    };
    let new_update =
        parse_trade_update_payload(&new_payload, account_id, ALPACA_VENUE, 2, 0, ts_init)
            .expect("parse new event");
    assert_eq!(new_update.event_kind, AlpacaTradeEventKind::New);
    assert_eq!(new_update.client_order_id, ClientOrderId::new("client-ord-1"));
    assert_eq!(new_update.venue_order_id, VenueOrderId::new("venue-ord-1"));
    assert_eq!(new_update.instrument_id, InstrumentId::from("NVDA.ALPACA"));
    assert_eq!(new_update.order_side, OrderSide::Buy);
    assert_eq!(new_update.order_type, OrderType::Limit);
    assert_eq!(new_update.order_status, OrderStatus::Accepted);
    assert_eq!(new_update.order_qty, Quantity::from("100"));
    assert_eq!(new_update.cum_qty, Quantity::from("0"));
    assert_eq!(new_update.leaves_qty, Quantity::from("100"));
    assert!(new_update.last_qty.is_none());
    assert!(new_update.last_px.is_none());
    assert!(new_update.fill_report.is_none());

    // 2. PartialFill (Limit -> Maker)
    let partial_payload = AlpacaTradeUpdatePayload {
        event: AlpacaTradeEventKind::PartialFill,
        execution_id: Some("exec-pf-1".to_string()),
        price: Some(dec!(125.40)),
        qty: Some(dec!(40)),
        position_qty: Some(dec!(40)),
        timestamp: Some("2026-03-31T13:30:01.000Z".to_string()),
        order: sample_order_response(
            "venue-ord-1",
            "client-ord-1",
            "NVDA",
            AlpacaOrderSide::Buy,
            AlpacaOrderType::Limit,
            "partially_filled",
            dec!(100),
            dec!(40),
            Some(dec!(125.40)),
            Some(dec!(125.50)),
        ),
    };
    let partial_update =
        parse_trade_update_payload(&partial_payload, account_id, ALPACA_VENUE, 2, 0, ts_init)
            .expect("parse partial_fill event");
    assert_eq!(partial_update.order_status, OrderStatus::PartiallyFilled);
    assert_eq!(partial_update.cum_qty, Quantity::from("40"));
    assert_eq!(partial_update.leaves_qty, Quantity::from("60"));
    assert_eq!(partial_update.last_qty, Some(Quantity::from("40")));
    assert_eq!(partial_update.last_px, Some(Price::from("125.40")));
    assert_eq!(partial_update.avg_px, Some(dec!(125.40)));
    assert_eq!(partial_update.position_qty, Some(dec!(40)));
    assert_eq!(partial_update.trade_id, Some(TradeId::new("exec-pf-1")));
    let pf_report = partial_update
        .fill_report
        .expect("partial_fill must produce FillReport");
    assert_eq!(pf_report.last_qty, Quantity::from("40"));
    assert_eq!(pf_report.last_px, Price::from("125.40"));
    assert_eq!(pf_report.liquidity_side, LiquiditySide::Maker);
    assert_eq!(pf_report.avg_px, Some(dec!(125.40)));

    // 3. Fill (Market -> Taker, fallback execution_id when None)
    let fill_payload = AlpacaTradeUpdatePayload {
        event: AlpacaTradeEventKind::Fill,
        execution_id: None,
        price: Some(dec!(125.50)),
        qty: Some(dec!(60)),
        position_qty: Some(dec!(100)),
        timestamp: Some("2026-03-31T13:30:02.000Z".to_string()),
        order: sample_order_response(
            "venue-ord-1",
            "client-ord-1",
            "NVDA",
            AlpacaOrderSide::Buy,
            AlpacaOrderType::Market,
            "filled",
            dec!(100),
            dec!(100),
            Some(dec!(125.46)),
            None,
        ),
    };
    let fill_update =
        parse_trade_update_payload(&fill_payload, account_id, ALPACA_VENUE, 2, 0, ts_init)
            .expect("parse fill event");
    assert_eq!(fill_update.order_status, OrderStatus::Filled);
    assert_eq!(fill_update.cum_qty, Quantity::from("100"));
    assert_eq!(fill_update.leaves_qty, Quantity::from("0"));
    assert_eq!(fill_update.last_qty, Some(Quantity::from("60")));
    assert_eq!(fill_update.last_px, Some(Price::from("125.50")));
    assert_eq!(fill_update.avg_px, Some(dec!(125.46)));
    assert_eq!(fill_update.position_qty, Some(dec!(100)));
    let f_report = fill_update.fill_report.expect("fill must produce FillReport");
    assert_eq!(f_report.liquidity_side, LiquiditySide::Taker);
    assert!(f_report.trade_id.as_str().starts_with("venue-ord-1-"));

    // 4. Other non-fill lifecycle events: Canceled, DoneForDay, Expired, Rejected, Replaced, PendingNew, PendingCancel, PendingReplace
    let lifecycle_cases = [
        (AlpacaTradeEventKind::Canceled, "canceled", OrderStatus::Canceled),
        (AlpacaTradeEventKind::DoneForDay, "done_for_day", OrderStatus::Canceled),
        (AlpacaTradeEventKind::Expired, "expired", OrderStatus::Expired),
        (AlpacaTradeEventKind::Rejected, "rejected", OrderStatus::Rejected),
        (AlpacaTradeEventKind::Replaced, "replaced", OrderStatus::Accepted),
        (AlpacaTradeEventKind::PendingNew, "pending_new", OrderStatus::Submitted),
        (AlpacaTradeEventKind::PendingCancel, "pending_cancel", OrderStatus::PendingCancel),
        (AlpacaTradeEventKind::PendingReplace, "pending_replace", OrderStatus::PendingUpdate),
    ];

    for (event_kind, raw_status, expected_status) in lifecycle_cases {
        let payload = AlpacaTradeUpdatePayload {
            event: event_kind,
            execution_id: None,
            price: None,
            qty: None,
            position_qty: None,
            timestamp: Some("2026-03-31T15:00:00Z".to_string()),
            order: sample_order_response(
                "venue-ord-lifecycle",
                "client-ord-lifecycle",
                "MSFT",
                AlpacaOrderSide::Sell,
                AlpacaOrderType::Limit,
                raw_status,
                dec!(25),
                dec!(5),
                Some(dec!(410.00)),
                Some(dec!(412.00)),
            ),
        };
        let update = parse_trade_update_payload(&payload, account_id, ALPACA_VENUE, 2, 0, ts_init)
            .unwrap_or_else(|e| panic!("failed to parse {event_kind:?}: {e}"));
        assert_eq!(update.event_kind, event_kind);
        assert_eq!(update.order_status, expected_status);
        assert_eq!(update.order_report.order_status, expected_status);
        assert_eq!(update.cum_qty, Quantity::from("5"));
        assert_eq!(update.leaves_qty, Quantity::from("20"));
        assert!(update.fill_report.is_none());
    }
}

#[test]
fn test_parse_trade_update_fail_closed_validation() {
    let account_id = AccountId::new("ALPACA-PA123456");
    let ts_init = UnixNanos::from(1_775_000_000_000_000_000u64);

    // Missing client_order_id -> fail-closed Parse error
    let missing_cid_payload = AlpacaTradeUpdatePayload {
        event: AlpacaTradeEventKind::New,
        execution_id: None,
        price: None,
        qty: None,
        position_qty: None,
        timestamp: Some("2026-03-31T13:30:00Z".to_string()),
        order: sample_order_response(
            "venue-ord-err-1",
            "   ",
            "NVDA",
            AlpacaOrderSide::Buy,
            AlpacaOrderType::Market,
            "new",
            dec!(10),
            dec!(0),
            None,
            None,
        ),
    };
    assert!(matches!(
        parse_trade_update_payload(&missing_cid_payload, account_id, ALPACA_VENUE, 2, 0, ts_init),
        Err(AlpacaError::Parse(_))
    ));

    // Fill event missing positive fill qty -> fail-closed Parse error
    let missing_qty_fill = AlpacaTradeUpdatePayload {
        event: AlpacaTradeEventKind::Fill,
        execution_id: Some("exec-err-qty".to_string()),
        price: Some(dec!(125.00)),
        qty: None,
        position_qty: None,
        timestamp: Some("2026-03-31T13:30:00Z".to_string()),
        order: sample_order_response(
            "venue-ord-err-2",
            "client-ord-err-2",
            "NVDA",
            AlpacaOrderSide::Buy,
            AlpacaOrderType::Market,
            "filled",
            dec!(10),
            dec!(0),
            Some(dec!(125.00)),
            None,
        ),
    };
    assert!(matches!(
        parse_trade_update_payload(&missing_qty_fill, account_id, ALPACA_VENUE, 2, 0, ts_init),
        Err(AlpacaError::Parse(_))
    ));

    // Fill event missing positive fill price -> fail-closed Parse error
    let missing_px_fill = AlpacaTradeUpdatePayload {
        event: AlpacaTradeEventKind::PartialFill,
        execution_id: Some("exec-err-px".to_string()),
        price: None,
        qty: Some(dec!(10)),
        position_qty: None,
        timestamp: Some("2026-03-31T13:30:00Z".to_string()),
        order: sample_order_response(
            "venue-ord-err-3",
            "client-ord-err-3",
            "NVDA",
            AlpacaOrderSide::Buy,
            AlpacaOrderType::Market,
            "partially_filled",
            dec!(20),
            dec!(10),
            None,
            None,
        ),
    };
    assert!(matches!(
        parse_trade_update_payload(&missing_px_fill, account_id, ALPACA_VENUE, 2, 0, ts_init),
        Err(AlpacaError::Parse(_))
    ));
}

// =================================================================================================
// 3. End-to-End Local WebSocket Peer Simulation Tests (`AlpacaWsClient`)
// =================================================================================================

fn build_trade_update_json(
    event: &str,
    venue_order_id: &str,
    client_order_id: &str,
    symbol: &str,
    status: &str,
    order_qty: &str,
    filled_qty: &str,
    filled_avg_price: Option<&str>,
    last_px: Option<&str>,
    last_qty: Option<&str>,
    position_qty: Option<&str>,
    execution_id: Option<&str>,
) -> String {
    serde_json::json!({
        "stream": "trade_updates",
        "data": {
            "event": event,
            "execution_id": execution_id,
            "price": last_px,
            "qty": last_qty,
            "position_qty": position_qty,
            "timestamp": "2026-03-31T14:30:00Z",
            "order": {
                "id": venue_order_id,
                "client_order_id": client_order_id,
                "created_at": "2026-03-31T14:29:59Z",
                "submitted_at": "2026-03-31T14:29:59.100Z",
                "updated_at": "2026-03-31T14:30:00Z",
                "symbol": symbol,
                "qty": order_qty,
                "filled_qty": filled_qty,
                "filled_avg_price": filled_avg_price,
                "type": "limit",
                "side": "buy",
                "time_in_force": "day",
                "limit_price": "150.00",
                "status": status
            }
        }
    })
    .to_string()
}

#[tokio::test]
async fn test_ws_client_handshake_and_trade_updates_streaming_with_zero_control_leakage() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind local WS listener");
    let addr = listener.local_addr().expect("local addr");
    let ws_url = format!("ws://{addr}/v2/test");

    let server_task = tokio::spawn(async move {
        let (tcp_stream, _) = listener.accept().await.expect("accept ws client");
        let mut ws = accept_async(tcp_stream).await.expect("ws handshake");

        // 1. Expect outbound authenticate command from AlpacaWsClient
        let first_msg = ws
            .next()
            .await
            .expect("client must send frame")
            .expect("valid ws frame");
        let auth_val: serde_json::Value =
            serde_json::from_str(first_msg.to_text().expect("text frame")).expect("json");
        assert_eq!(auth_val["action"], "authenticate");
        assert_eq!(auth_val["data"]["key_id"], "PKWSKEY123456789");
        assert_eq!(auth_val["data"]["secret_key"], "SKWSSECRET987654321");

        // Send greeting + authorization control frames
        ws.send(WsMessage::Text(
            r#"[{"T":"success","msg":"connected"}]"#.into(),
        ))
        .await
        .expect("send connected");
        ws.send(WsMessage::Text(
            r#"{"stream":"authorization","data":{"status":"authorized","action":"authenticate"}}"#
                .into(),
        ))
        .await
        .expect("send authorized");

        // 2. Expect outbound listen command for trade_updates
        let second_msg = ws
            .next()
            .await
            .expect("client must send listen frame")
            .expect("valid ws frame");
        let listen_val: serde_json::Value =
            serde_json::from_str(second_msg.to_text().expect("text frame")).expect("json");
        assert_eq!(listen_val["action"], "listen");
        assert_eq!(
            listen_val["data"]["streams"],
            serde_json::json!(["trade_updates"])
        );

        // Confirm listening
        ws.send(WsMessage::Text(
            r#"{"stream":"listening","data":{"streams":["trade_updates"]}}"#.into(),
        ))
        .await
        .expect("send listening");

        // 3. Stream 5 lifecycle events (mixing Text and Binary WebSocket frames)
        let f_new = build_trade_update_json(
            "new",
            "v-ord-100",
            "c-ord-100",
            "NVDA",
            "new",
            "100",
            "0",
            None,
            None,
            None,
            Some("0"),
            None,
        );
        ws.send(WsMessage::Text(f_new.into()))
            .await
            .expect("send new");

        let f_partial = build_trade_update_json(
            "partial_fill",
            "v-ord-100",
            "c-ord-100",
            "NVDA",
            "partially_filled",
            "100",
            "40",
            Some("149.80"),
            Some("149.80"),
            Some("40"),
            Some("40"),
            Some("exec-100-1"),
        );
        // Send as Binary frame to verify binary frame handling in AlpacaWsClient
        ws.send(WsMessage::Binary(f_partial.into_bytes().into()))
            .await
            .expect("send partial_fill binary");

        let f_fill = build_trade_update_json(
            "fill",
            "v-ord-100",
            "c-ord-100",
            "NVDA",
            "filled",
            "100",
            "100",
            Some("149.92"),
            Some("150.00"),
            Some("60"),
            Some("100"),
            Some("exec-100-2"),
        );
        ws.send(WsMessage::Text(f_fill.into()))
            .await
            .expect("send fill");

        let f_canceled = build_trade_update_json(
            "canceled",
            "v-ord-101",
            "c-ord-101",
            "AAPL",
            "canceled",
            "50",
            "0",
            None,
            None,
            None,
            Some("0"),
            None,
        );
        ws.send(WsMessage::Text(f_canceled.into()))
            .await
            .expect("send canceled");

        let f_rejected = build_trade_update_json(
            "rejected",
            "v-ord-102",
            "c-ord-102",
            "TSLA",
            "rejected",
            "25",
            "0",
            None,
            None,
            None,
            Some("0"),
            None,
        );
        ws.send(WsMessage::Text(f_rejected.into()))
            .await
            .expect("send rejected");

        // Keep connection open briefly until client disconnects
        let _ = ws.next().await;
    });

    let observer = Arc::new(RecordingLifecycleObserver::default());
    let obs_dyn: Arc<dyn AlpacaWsLifecycleObserver> = observer.clone();
    let config = make_ws_test_config(&ws_url);
    let mut client = AlpacaWsClient::new(
        config,
        AccountId::new("ALPACA-PA999"),
        2,
        0,
        Some(obs_dyn),
    )
    .expect("create AlpacaWsClient");

    assert_eq!(client.session_state(), AlpacaWsSessionState::Disconnected);
    assert_eq!(client.reconnect_count(), 0);

    let mut rx = client.connect().await.expect("connect and complete handshake");
    assert_eq!(client.session_state(), AlpacaWsSessionState::Listening);
    assert_eq!(observer.authenticated_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        *observer.listening_streams.lock(),
        vec![vec![TRADE_UPDATES_STREAM.to_string()]]
    );
    assert!(
        client
            .subscriptions()
            .all_topics()
            .contains(&TRADE_UPDATES_STREAM.to_string())
    );

    let mut received: Vec<AlpacaExecutionUpdate> = Vec::with_capacity(5);
    for _ in 0..5 {
        let ev = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("receive event before timeout")
            .expect("channel open");
        received.push(ev);
    }

    assert_eq!(received[0].event_kind, AlpacaTradeEventKind::New);
    assert_eq!(received[0].order_status, OrderStatus::Accepted);
    assert_eq!(received[0].client_order_id, ClientOrderId::new("c-ord-100"));
    assert!(received[0].fill_report.is_none());

    assert_eq!(received[1].event_kind, AlpacaTradeEventKind::PartialFill);
    assert_eq!(received[1].order_status, OrderStatus::PartiallyFilled);
    assert_eq!(received[1].last_qty, Some(Quantity::from("40")));
    assert_eq!(received[1].last_px, Some(Price::from("149.80")));
    assert_eq!(received[1].cum_qty, Quantity::from("40"));
    assert_eq!(received[1].leaves_qty, Quantity::from("60"));
    assert!(received[1].fill_report.is_some());

    assert_eq!(received[2].event_kind, AlpacaTradeEventKind::Fill);
    assert_eq!(received[2].order_status, OrderStatus::Filled);
    assert_eq!(received[2].last_qty, Some(Quantity::from("60")));
    assert_eq!(received[2].last_px, Some(Price::from("150.00")));
    assert_eq!(received[2].cum_qty, Quantity::from("100"));
    assert_eq!(received[2].leaves_qty, Quantity::from("0"));
    assert_eq!(received[2].avg_px, Some(dec!(149.92)));
    assert_eq!(received[2].position_qty, Some(dec!(100)));
    assert!(received[2].fill_report.is_some());

    assert_eq!(received[3].event_kind, AlpacaTradeEventKind::Canceled);
    assert_eq!(received[3].order_status, OrderStatus::Canceled);
    assert_eq!(received[3].client_order_id, ClientOrderId::new("c-ord-101"));
    assert!(received[3].fill_report.is_none());

    assert_eq!(received[4].event_kind, AlpacaTradeEventKind::Rejected);
    assert_eq!(received[4].order_status, OrderStatus::Rejected);
    assert_eq!(received[4].client_order_id, ClientOrderId::new("c-ord-102"));
    assert!(received[4].fill_report.is_none());

    client.disconnect().await;
    assert_eq!(client.session_state(), AlpacaWsSessionState::Disconnected);
    let _ = server_task.await;
}

#[tokio::test]
async fn test_ws_client_fail_closed_on_unauthorized_handshake() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind local WS listener");
    let addr = listener.local_addr().expect("local addr");
    let ws_url = format!("ws://{addr}/v2/test");

    let server_task = tokio::spawn(async move {
        let (tcp_stream, _) = listener.accept().await.expect("accept ws client");
        let mut ws = accept_async(tcp_stream).await.expect("ws handshake");

        let _auth_msg = ws.next().await;
        ws.send(WsMessage::Text(
            r#"{"stream":"authorization","data":{"status":"unauthorized","action":"authenticate","message":"invalid api credentials"}}"#.into(),
        ))
        .await
        .expect("send unauthorized");
        let _ = ws.next().await;
    });

    let observer = Arc::new(RecordingLifecycleObserver::default());
    let obs_dyn: Arc<dyn AlpacaWsLifecycleObserver> = observer.clone();
    let config = make_ws_test_config(&ws_url);
    let mut client = AlpacaWsClient::new(
        config,
        AccountId::new("ALPACA-PA999"),
        2,
        0,
        Some(obs_dyn),
    )
    .expect("create AlpacaWsClient");

    let res = client.connect().await;
    match res {
        Err(AlpacaError::AuthError { status, message }) => {
            assert_eq!(status, 401);
            assert!(
                message.contains("invalid api credentials"),
                "unexpected auth error message: {message}"
            );
        }
        other => panic!("expected Err(AlpacaError::AuthError), got {other:?}"),
    }

    assert_eq!(
        *observer.auth_failures.lock(),
        vec!["invalid api credentials".to_string()]
    );
    let _ = server_task.await;
}

#[tokio::test]
async fn test_ws_client_automatic_reconnect_resubscribes_and_resumes_stream() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind local WS listener");
    let addr = listener.local_addr().expect("local addr");
    let ws_url = format!("ws://{addr}/v2/test");

    let server_task = tokio::spawn(async move {
        // --- Connection #1 ---
        {
            let (tcp_stream_1, _) = listener.accept().await.expect("accept connection #1");
            let mut ws1 = accept_async(tcp_stream_1).await.expect("ws1 handshake");

            // Receive authenticate
            let msg1 = ws1.next().await.expect("msg1").expect("ok");
            let auth1: serde_json::Value =
                serde_json::from_str(msg1.to_text().expect("text")).expect("json");
            assert_eq!(auth1["action"], "authenticate");

            ws1.send(WsMessage::Text(
                r#"{"stream":"authorization","data":{"status":"authorized","action":"authenticate"}}"#
                    .into(),
            ))
            .await
            .expect("send auth1");

            // Receive listen
            let msg2 = ws1.next().await.expect("msg2").expect("ok");
            let listen1: serde_json::Value =
                serde_json::from_str(msg2.to_text().expect("text")).expect("json");
            assert_eq!(listen1["action"], "listen");

            ws1.send(WsMessage::Text(
                r#"{"stream":"listening","data":{"streams":["trade_updates"]}}"#.into(),
            ))
            .await
            .expect("send listening1");

            // Send first event (`new`) before dropping connection #1
            let ev1 = build_trade_update_json(
                "new",
                "v-reconn-1",
                "c-reconn-1",
                "NVDA",
                "new",
                "50",
                "0",
                None,
                None,
                None,
                Some("0"),
                None,
            );
            ws1.send(WsMessage::Text(ev1.into()))
                .await
                .expect("send ev1");

            // Abruptly close Connection #1 to trigger automatic reconnection
            let _ = ws1.close(None).await;
        }

        // --- Connection #2 (automatic reconnect) ---
        {
            let (tcp_stream_2, _) = listener.accept().await.expect("accept connection #2");
            let mut ws2 = accept_async(tcp_stream_2).await.expect("ws2 handshake");

            // Client must automatically re-send authenticate on reconnect
            let msg1 = ws2.next().await.expect("re-auth msg").expect("ok");
            let auth2: serde_json::Value =
                serde_json::from_str(msg1.to_text().expect("text")).expect("json");
            assert_eq!(auth2["action"], "authenticate");

            ws2.send(WsMessage::Text(
                r#"{"stream":"authorization","data":{"status":"authorized","action":"authenticate"}}"#
                    .into(),
            ))
            .await
            .expect("send auth2");

            // Client must automatically re-send listen on reconnect
            let msg2 = ws2.next().await.expect("re-listen msg").expect("ok");
            let listen2: serde_json::Value =
                serde_json::from_str(msg2.to_text().expect("text")).expect("json");
            assert_eq!(listen2["action"], "listen");

            ws2.send(WsMessage::Text(
                r#"{"stream":"listening","data":{"streams":["trade_updates"]}}"#.into(),
            ))
            .await
            .expect("send listening2");

            // Send second event (`fill`) over the recovered connection
            let ev2 = build_trade_update_json(
                "fill",
                "v-reconn-1",
                "c-reconn-1",
                "NVDA",
                "filled",
                "50",
                "50",
                Some("150.00"),
                Some("150.00"),
                Some("50"),
                Some("50"),
                Some("exec-reconn-1"),
            );
            ws2.send(WsMessage::Text(ev2.into()))
                .await
                .expect("send ev2");

            let _ = ws2.next().await;
        }
    });

    let observer = Arc::new(RecordingLifecycleObserver::default());
    let obs_dyn: Arc<dyn AlpacaWsLifecycleObserver> = observer.clone();
    let config = make_ws_test_config(&ws_url);
    let mut client = AlpacaWsClient::new(
        config,
        AccountId::new("ALPACA-PA999"),
        2,
        0,
        Some(obs_dyn),
    )
    .expect("create AlpacaWsClient");

    let mut rx = client.connect().await.expect("initial connect");

    // 1. Receive event from Connection #1
    let first_update = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .expect("recv first update")
        .expect("channel open");
    assert_eq!(first_update.event_kind, AlpacaTradeEventKind::New);
    assert_eq!(first_update.client_order_id, ClientOrderId::new("c-reconn-1"));

    // 2. Receive event from Connection #2 after automatic reconnect + re-auth + re-subscribe
    let second_update = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("recv second update after reconnect")
        .expect("channel open");
    assert_eq!(second_update.event_kind, AlpacaTradeEventKind::Fill);
    assert_eq!(second_update.order_status, OrderStatus::Filled);
    assert_eq!(second_update.client_order_id, ClientOrderId::new("c-reconn-1"));
    assert_eq!(second_update.cum_qty, Quantity::from("50"));
    assert!(second_update.fill_report.is_some());

    // Verify reconnect metrics, observer callbacks, and subscription state
    assert!(
        client.reconnect_count() >= 1,
        "expected reconnect_count >= 1, got {}",
        client.reconnect_count()
    );
    assert_eq!(client.session_state(), AlpacaWsSessionState::Listening);
    assert!(observer.authenticated_count.load(Ordering::SeqCst) >= 2);
    assert_eq!(*observer.reconnect_attempts.lock(), vec![1]);
    assert_eq!(observer.listening_streams.lock().len(), 2);
    assert!(
        client
            .subscriptions()
            .all_topics()
            .contains(&TRADE_UPDATES_STREAM.to_string())
    );

    client.disconnect().await;
    let _ = server_task.await;
}
