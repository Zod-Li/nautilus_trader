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

//! Integration and unit verification suite for `nautilus-alpaca` Ticket #06:
//! - `AlpacaCredential` secret redaction, masking, and fail-closed validation.
//! - `AlpacaConfig` environment URL switching and bounds validation.
//! - Pure decimal-to-domain parsing (`AccountState`, `PositionStatusReport`, `OrderStatusReport`, `Equity`).
//! - Byte-level HTTP client operations, retry recovery (`429`/`503`), and deterministic fail-closed non-retry (`401`/`403`/`422`).

#![allow(linker_messages)]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use nautilus_alpaca::{
    ALPACA_LIVE_HTTP_URL, ALPACA_PAPER_HTTP_URL, ALPACA_VENUE, AlpacaConfig, AlpacaCredential,
    AlpacaEnvironment, AlpacaError, AlpacaHttpClient, AlpacaOrderClass, AlpacaOrderSide,
    AlpacaOrderType, AlpacaTimeInForce,
    common::{HEADER_APCA_API_KEY_ID, HEADER_APCA_API_SECRET_KEY},
    http::{
        AlpacaAccountResponse, AlpacaAssetResponse, AlpacaOrderRequest, AlpacaOrderResponse,
        AlpacaPositionResponse, AlpacaReplaceOrderRequest, AlpacaStopLossSpec,
        AlpacaTakeProfitSpec, decimal_to_money, decimal_to_price, decimal_to_quantity,
        parse_account_state, parse_equity_instrument, parse_order_status_report,
        parse_position_status_report,
    },
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{AccountType, ContingencyType, OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce},
    identifiers::{AccountId, ClientOrderId, InstrumentId},
    instruments::{Instrument, InstrumentAny},
    types::Currency,
};
use rust_decimal_macros::dec;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

// =================================================================================================
// 1. Credential & Config Validation Tests
// =================================================================================================

#[test]
fn test_credential_redacts_debug_and_validates_non_empty() {
    let cred = AlpacaCredential::new_checked("PKTEST1234567890", "SKSECRET9876543210")
        .expect("valid credential must succeed");

    let dbg = format!("{cred:?}");
    assert!(
        !dbg.contains("SKSECRET9876543210"),
        "Debug output must never expose api_secret_key: {dbg}"
    );
    assert!(
        !dbg.contains("PKTEST1234567890"),
        "Debug output must mask full api_key_id: {dbg}"
    );
    assert!(
        dbg.contains("PKTE...7890"),
        "Debug output must contain masked key ID: {dbg}"
    );

    let headers = cred.to_http_headers();
    assert_eq!(
        headers.get(HEADER_APCA_API_KEY_ID).map(String::as_str),
        Some("PKTEST1234567890")
    );
    assert_eq!(
        headers.get(HEADER_APCA_API_SECRET_KEY).map(String::as_str),
        Some("SKSECRET9876543210")
    );

    assert!(matches!(
        AlpacaCredential::new_checked("", "secret"),
        Err(AlpacaError::InvalidConfig(_))
    ));
    assert!(matches!(
        AlpacaCredential::new_checked("key", "   "),
        Err(AlpacaError::InvalidConfig(_))
    ));
    assert!(matches!(
        AlpacaCredential::new_checked("key with space", "secret"),
        Err(AlpacaError::InvalidConfig(_))
    ));
}

#[test]
fn test_config_environment_switching_and_fail_closed_validation() {
    let cred = AlpacaCredential::new_checked("PK12345678", "SK12345678").unwrap();

    let paper_cfg =
        AlpacaConfig::new_for_environment(AlpacaEnvironment::Paper, cred.clone(), 10, 3).unwrap();
    assert_eq!(paper_cfg.http_base_url, ALPACA_PAPER_HTTP_URL);
    assert_eq!(paper_cfg.venue, ALPACA_VENUE);

    let live_cfg =
        AlpacaConfig::new_for_environment(AlpacaEnvironment::Live, cred.clone(), 10, 3).unwrap();
    assert_eq!(live_cfg.http_base_url, ALPACA_LIVE_HTTP_URL);

    // Invalid HTTP scheme (ftp)
    assert!(matches!(
        AlpacaConfig::new_checked(
            AlpacaEnvironment::Paper,
            "ftp://paper-api.alpaca.markets",
            "wss://paper-api.alpaca.markets/stream",
            cred.clone(),
            10,
            3,
            250,
            5000,
            10,
            "ALPACA",
        ),
        Err(AlpacaError::InvalidConfig(_))
    ));

    // Invalid WS scheme (https instead of ws/wss)
    assert!(matches!(
        AlpacaConfig::new_checked(
            AlpacaEnvironment::Paper,
            "https://paper-api.alpaca.markets",
            "https://paper-api.alpaca.markets/stream",
            cred.clone(),
            10,
            3,
            250,
            5000,
            10,
            "ALPACA",
        ),
        Err(AlpacaError::InvalidConfig(_))
    ));

    // Invalid timeout (0s)
    assert!(matches!(
        AlpacaConfig::new_for_environment(AlpacaEnvironment::Paper, cred.clone(), 0, 3),
        Err(AlpacaError::InvalidConfig(_))
    ));

    // Invalid max_retries (> 10)
    assert!(matches!(
        AlpacaConfig::new_for_environment(AlpacaEnvironment::Paper, cred.clone(), 10, 11),
        Err(AlpacaError::InvalidConfig(_))
    ));

    // Invalid retry backoff bounds (initial > max)
    assert!(matches!(
        AlpacaConfig::new_checked(
            AlpacaEnvironment::Paper,
            ALPACA_PAPER_HTTP_URL,
            "wss://paper-api.alpaca.markets/stream",
            cred.clone(),
            10,
            3,
            6000,
            5000,
            10,
            "ALPACA",
        ),
        Err(AlpacaError::InvalidConfig(_))
    ));

    // Invalid rate limit (0 rps)
    assert!(matches!(
        AlpacaConfig::new_checked(
            AlpacaEnvironment::Paper,
            ALPACA_PAPER_HTTP_URL,
            "wss://paper-api.alpaca.markets/stream",
            cred,
            10,
            3,
            250,
            5000,
            0,
            "ALPACA",
        ),
        Err(AlpacaError::InvalidConfig(_))
    ));
}

// =================================================================================================
// 2. Pure Decimal-to-Domain Conversion Tests
// =================================================================================================

#[test]
fn test_decimal_primitives_and_account_position_order_asset_parsing() {
    let px = decimal_to_price(dec!(189.5425), 2).unwrap();
    assert_eq!(px.precision, 4);
    assert_eq!(px.as_decimal(), dec!(189.5425));

    let qty = decimal_to_quantity(dec!(150), 0).unwrap();
    assert_eq!(qty.precision, 0);
    assert_eq!(qty.as_decimal(), dec!(150));

    let money = decimal_to_money(dec!(105432.18), Currency::USD()).unwrap();
    assert_eq!(money.as_decimal(), dec!(105432.18));

    let ts_init = UnixNanos::from(1_700_000_000_000_000_000u64);

    // 1. AccountState parsing
    let raw_account = r#"{
        "id": "904837e3-3b76-47ec-b432-046db621571b",
        "account_number": "PA31415926",
        "status": "ACTIVE",
        "currency": "USD",
        "cash": "85000.50",
        "equity": "125000.75",
        "buying_power": "250001.50",
        "initial_margin": "20000.25",
        "maintenance_margin": "12000.00",
        "pattern_day_trader": false,
        "daytrade_count": 1,
        "trading_blocked": false,
        "account_blocked": false,
        "created_at": "2026-01-15T14:30:00Z"
    }"#;
    let acct_dto: AlpacaAccountResponse = serde_json::from_str(raw_account).unwrap();
    let acct_state = parse_account_state(&acct_dto, ALPACA_VENUE, ts_init).unwrap();
    assert_eq!(acct_state.account_id, AccountId::from("ALPACA-PA31415926"));
    assert_eq!(acct_state.account_type, AccountType::Margin);
    assert_eq!(acct_state.balances.len(), 1);
    assert_eq!(acct_state.balances[0].total.as_decimal(), dec!(125000.75));
    assert_eq!(acct_state.balances[0].locked.as_decimal(), dec!(20000.25));
    assert_eq!(acct_state.balances[0].free.as_decimal(), dec!(105000.50));

    // 2. PositionStatusReport parsing (Long, Short, Flat)
    let account_id = AccountId::from("ALPACA-PA31415926");
    let raw_short_pos = r#"{
        "asset_id": "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415",
        "symbol": "NVDA",
        "exchange": "NASDAQ",
        "asset_class": "us_equity",
        "avg_entry_price": "132.45",
        "qty": "-40",
        "qty_available": "-40",
        "side": "short",
        "market_value": "-5298.00",
        "cost_basis": "-5298.00",
        "unrealized_pl": "120.00",
        "current_price": "129.45"
    }"#;
    let pos_dto: AlpacaPositionResponse = serde_json::from_str(raw_short_pos).unwrap();
    let pos_report =
        parse_position_status_report(&pos_dto, account_id, ALPACA_VENUE, ts_init).unwrap();
    assert_eq!(pos_report.instrument_id, InstrumentId::from("NVDA.ALPACA"));
    assert_eq!(pos_report.position_side, PositionSide::Short);
    assert_eq!(pos_report.quantity.as_decimal(), dec!(40));
    assert_eq!(pos_report.avg_px_open, Some(dec!(132.45)));

    let mut long_pos_dto = pos_dto.clone();
    long_pos_dto.qty = dec!(25);
    long_pos_dto.side = "long".to_string();
    let long_report =
        parse_position_status_report(&long_pos_dto, account_id, ALPACA_VENUE, ts_init).unwrap();
    assert_eq!(long_report.position_side, PositionSide::Long);
    assert_eq!(long_report.quantity.as_decimal(), dec!(25));

    let mut flat_pos_dto = pos_dto;
    flat_pos_dto.qty = dec!(0);
    let flat_report =
        parse_position_status_report(&flat_pos_dto, account_id, ALPACA_VENUE, ts_init).unwrap();
    assert_eq!(flat_report.position_side, PositionSide::Flat);
    assert_eq!(flat_report.quantity.as_decimal(), dec!(0));

    // 3. OrderStatusReport parsing for Bracket order with nested OCO legs
    let raw_bracket_order = r#"{
        "id": "61e69015-8549-4bfd-b9c3-01e75843f47d",
        "client_order_id": "CID-ENTRY-001",
        "created_at": "2026-03-30T13:35:00Z",
        "submitted_at": "2026-03-30T13:35:01Z",
        "filled_at": "2026-03-30T13:35:02Z",
        "symbol": "AAPL",
        "asset_class": "us_equity",
        "qty": "25",
        "filled_qty": "25",
        "filled_avg_price": "210.15",
        "order_class": "bracket",
        "type": "limit",
        "side": "buy",
        "time_in_force": "day",
        "limit_price": "210.50",
        "status": "filled",
        "extended_hours": false,
        "legs": [
            {
                "id": "leg-tp-001",
                "client_order_id": "CID-TP-001",
                "created_at": "2026-03-30T13:35:00Z",
                "symbol": "AAPL",
                "qty": "25",
                "filled_qty": "0",
                "order_class": "bracket",
                "type": "limit",
                "side": "sell",
                "time_in_force": "gtc",
                "limit_price": "225.00",
                "status": "new"
            },
            {
                "id": "leg-sl-001",
                "client_order_id": "CID-SL-001",
                "created_at": "2026-03-30T13:35:00Z",
                "symbol": "AAPL",
                "qty": "25",
                "filled_qty": "0",
                "order_class": "bracket",
                "type": "stop",
                "side": "sell",
                "time_in_force": "gtc",
                "stop_price": "202.00",
                "status": "held"
            }
        ]
    }"#;
    let order_dto: AlpacaOrderResponse = serde_json::from_str(raw_bracket_order).unwrap();
    let order_report =
        parse_order_status_report(&order_dto, account_id, ALPACA_VENUE, ts_init).unwrap();
    assert_eq!(order_report.instrument_id, InstrumentId::from("AAPL.ALPACA"));
    assert_eq!(order_report.order_side, Some(OrderSide::Buy));
    assert_eq!(order_report.order_type, OrderType::Limit);
    assert_eq!(order_report.time_in_force, TimeInForce::Day);
    assert_eq!(order_report.order_status, OrderStatus::Filled);
    assert_eq!(order_report.quantity.as_decimal(), dec!(25));
    assert_eq!(order_report.filled_qty.as_decimal(), dec!(25));
    assert_eq!(order_report.price.unwrap().as_decimal(), dec!(210.50));
    assert_eq!(order_report.avg_px, Some(dec!(210.15)));
    assert_eq!(order_report.contingency_type, Some(ContingencyType::Oto));
    assert_eq!(
        order_report.linked_order_ids,
        Some(vec![
            ClientOrderId::from("CID-TP-001"),
            ClientOrderId::from("CID-SL-001"),
        ])
    );

    // 4. Equity instrument parsing
    let raw_asset = r#"{
        "id": "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415",
        "class": "us_equity",
        "exchange": "NASDAQ",
        "symbol": "NVDA",
        "name": "NVIDIA Corporation",
        "status": "active",
        "tradable": true,
        "marginable": true,
        "shortable": true,
        "easy_to_borrow": true,
        "fractionable": false,
        "min_order_size": "1",
        "price_increment": "0.01"
    }"#;
    let asset_dto: AlpacaAssetResponse = serde_json::from_str(raw_asset).unwrap();
    let instrument = parse_equity_instrument(&asset_dto, ALPACA_VENUE, ts_init).unwrap();
    match instrument {
        InstrumentAny::Equity(eq) => {
            assert_eq!(eq.id(), InstrumentId::from("NVDA.ALPACA"));
            assert_eq!(eq.price_precision(), 2);
            assert_eq!(eq.price_increment().as_decimal(), dec!(0.01));
            assert_eq!(eq.size_precision(), 0);
        }
        other => panic!("expected InstrumentAny::Equity, got {other:?}"),
    }
}

// =================================================================================================
// 3. Ephemeral TCP HTTP Peer & Byte-Level REST Client Tests
// =================================================================================================

async fn read_http_request(stream: &mut tokio::net::TcpStream) -> String {
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
    String::from_utf8_lossy(&buf).into_owned()
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

fn build_test_client(base_url: &str, max_retries: u32) -> AlpacaHttpClient {
    let cred = AlpacaCredential::new_checked("PKLIVEKEY999", "SKLIVESECRET888").unwrap();
    let cfg = AlpacaConfig::new_checked(
        AlpacaEnvironment::Paper,
        base_url,
        "wss://paper-api.alpaca.markets/stream",
        cred,
        5,
        max_retries,
        20,
        100,
        100,
        ALPACA_VENUE,
    )
    .unwrap();
    AlpacaHttpClient::new(cfg).unwrap()
}

#[tokio::test]
async fn test_http_client_endpoints_and_auth_headers_on_wire() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let server_handle = tokio::spawn(async move {
        // 1. GET /v2/account
        let (mut socket, _) = listener.accept().await.unwrap();
        let req1 = read_http_request(&mut socket).await;
        let req1_lower = req1.to_ascii_lowercase();
        assert!(req1.starts_with("GET /v2/account HTTP/1.1"));
        assert!(req1_lower.contains("apca-api-key-id: pklivekey999"));
        assert!(req1_lower.contains("apca-api-secret-key: sklivesecret888"));
        write_http_response(
            &mut socket,
            "200 OK",
            r#"{"id":"acc-1","account_number":"PA999","status":"ACTIVE","currency":"USD","cash":"50000.00","equity":"50000.00","buying_power":"100000.00"}"#,
        )
        .await;

        // 2. POST /v2/orders (Bracket order)
        let (mut socket, _) = listener.accept().await.unwrap();
        let req2 = read_http_request(&mut socket).await;
        assert!(req2.starts_with("POST /v2/orders HTTP/1.1"));
        assert!(req2.contains(r#""order_class":"bracket""#));
        assert!(req2.contains(r#""take_profit":{"limit_price":"220.00"}"#));
        assert!(req2.contains(r#""stop_loss":{"stop_price":"195.00"}"#));
        write_http_response(
            &mut socket,
            "200 OK",
            r#"{"id":"ord-100","client_order_id":"CID-BRACKET-1","created_at":"2026-03-30T14:00:00Z","symbol":"NVDA","qty":"10","filled_qty":"0","order_class":"bracket","type":"limit","side":"buy","time_in_force":"day","limit_price":"205.00","status":"accepted"}"#,
        )
        .await;

        // 3. PATCH /v2/orders/ord-100
        let (mut socket, _) = listener.accept().await.unwrap();
        let req3 = read_http_request(&mut socket).await;
        assert!(req3.starts_with("PATCH /v2/orders/ord-100 HTTP/1.1"));
        assert!(req3.contains(r#""limit_price":"206.50""#));
        write_http_response(
            &mut socket,
            "200 OK",
            r#"{"id":"ord-101","replaces":"ord-100","client_order_id":"CID-BRACKET-2","created_at":"2026-03-30T14:01:00Z","symbol":"NVDA","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"206.50","status":"replaced"}"#,
        )
        .await;

        // 4. DELETE /v2/orders/ord-101
        let (mut socket, _) = listener.accept().await.unwrap();
        let req4 = read_http_request(&mut socket).await;
        assert!(req4.starts_with("DELETE /v2/orders/ord-101 HTTP/1.1"));
        write_http_response(&mut socket, "204 No Content", "").await;

        // 5. GET /v2/orders:by_client_order_id?client_order_id=CID-BRACKET-2
        let (mut socket, _) = listener.accept().await.unwrap();
        let req5 = read_http_request(&mut socket).await;
        assert!(req5.starts_with(
            "GET /v2/orders:by_client_order_id?client_order_id=CID-BRACKET-2 HTTP/1.1"
        ));
        write_http_response(
            &mut socket,
            "200 OK",
            r#"{"id":"ord-101","client_order_id":"CID-BRACKET-2","created_at":"2026-03-30T14:01:00Z","symbol":"NVDA","qty":"10","filled_qty":"0","type":"limit","side":"buy","time_in_force":"day","limit_price":"206.50","status":"canceled"}"#,
        )
        .await;

        // 6. GET /v2/assets?status=active&asset_class=us_equity
        let (mut socket, _) = listener.accept().await.unwrap();
        let req6 = read_http_request(&mut socket).await;
        assert!(req6.starts_with("GET /v2/assets?status=active&asset_class=us_equity HTTP/1.1"));
        write_http_response(
            &mut socket,
            "200 OK",
            r#"[{"id":"asset-nvda","class":"us_equity","exchange":"NASDAQ","symbol":"NVDA","name":"NVIDIA Corp","status":"active","tradable":true,"marginable":true,"shortable":true,"easy_to_borrow":true,"fractionable":true,"min_order_size":"0.0001","price_increment":"0.01"}]"#,
        )
        .await;
    });

    let client = build_test_client(&base_url, 2);

    let acct = client.get_account().await.unwrap();
    assert_eq!(acct.account_number, "PA999");
    assert_eq!(acct.equity, dec!(50000.00));

    let order_req = AlpacaOrderRequest {
        symbol: "NVDA".to_string(),
        qty: dec!(10),
        side: AlpacaOrderSide::Buy,
        order_type: AlpacaOrderType::Limit,
        time_in_force: AlpacaTimeInForce::Day,
        limit_price: Some(dec!(205.00)),
        stop_price: None,
        trail_price: None,
        trail_percent: None,
        extended_hours: false,
        client_order_id: "CID-BRACKET-1".to_string(),
        order_class: Some(AlpacaOrderClass::Bracket),
        take_profit: Some(AlpacaTakeProfitSpec {
            limit_price: dec!(220.00),
        }),
        stop_loss: Some(AlpacaStopLossSpec {
            stop_price: dec!(195.00),
            limit_price: None,
        }),
    };
    let submitted = client.submit_order(&order_req).await.unwrap();
    assert_eq!(submitted.id, "ord-100");
    assert_eq!(submitted.order_class, Some(AlpacaOrderClass::Bracket));

    let replace_req = AlpacaReplaceOrderRequest {
        qty: Some(dec!(10)),
        time_in_force: Some(AlpacaTimeInForce::Day),
        limit_price: Some(dec!(206.50)),
        stop_price: None,
        trail: None,
        client_order_id: Some("CID-BRACKET-2".to_string()),
    };
    let replaced = client.replace_order("ord-100", &replace_req).await.unwrap();
    assert_eq!(replaced.id, "ord-101");
    assert_eq!(replaced.replaces.as_deref(), Some("ord-100"));

    client.cancel_order("ord-101").await.unwrap();

    let queried = client
        .get_order_by_client_order_id("CID-BRACKET-2")
        .await
        .unwrap();
    assert_eq!(queried.id, "ord-101");
    assert_eq!(queried.status, "canceled");

    let assets = client.list_assets().await.unwrap();
    assert_eq!(assets.len(), 1);
    assert_eq!(assets[0].symbol, "NVDA");
    assert!(assets[0].fractionable);

    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_http_client_retries_on_429_and_503_then_recovers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_srv = Arc::clone(&attempts);

    let server_handle = tokio::spawn(async move {
        // Attempt 1: 429 Too Many Requests
        let (mut s1, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut s1).await;
        attempts_srv.fetch_add(1, Ordering::SeqCst);
        write_http_response(
            &mut s1,
            "429 Too Many Requests",
            r#"{"code":42910000,"message":"rate limit exceeded"}"#,
        )
        .await;

        // Attempt 2: 503 Service Unavailable
        let (mut s2, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut s2).await;
        attempts_srv.fetch_add(1, Ordering::SeqCst);
        write_http_response(
            &mut s2,
            "503 Service Unavailable",
            r#"{"message":"upstream gateway timeout"}"#,
        )
        .await;

        // Attempt 3: 200 OK
        let (mut s3, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut s3).await;
        attempts_srv.fetch_add(1, Ordering::SeqCst);
        write_http_response(
            &mut s3,
            "200 OK",
            r#"[{"asset_id":"a1","symbol":"MSFT","exchange":"NASDAQ","asset_class":"us_equity","avg_entry_price":"410.00","qty":"15","side":"long","market_value":"6150.00","cost_basis":"6150.00"}]"#,
        )
        .await;
    });

    let client = build_test_client(&base_url, 3);
    let positions = client.list_positions().await.unwrap();
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].symbol, "MSFT");
    assert_eq!(attempts.load(Ordering::SeqCst), 3);

    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_http_client_does_not_retry_deterministic_422_or_403_rejects() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_srv = Arc::clone(&attempts);

    let server_handle = tokio::spawn(async move {
        // 1. 422 Unprocessable Entity (Wash trade / invalid price) -> must NOT retry
        let (mut s1, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut s1).await;
        attempts_srv.fetch_add(1, Ordering::SeqCst);
        write_http_response(
            &mut s1,
            "422 Unprocessable Entity",
            r#"{"code":42210000,"message":"existing open order would result in wash trade"}"#,
        )
        .await;

        // 2. 403 Forbidden with numeric code 40310000 (insufficient buying power / PDT) -> must NOT retry
        let (mut s2, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut s2).await;
        attempts_srv.fetch_add(1, Ordering::SeqCst);
        write_http_response(
            &mut s2,
            "403 Forbidden",
            r#"{"code":40310000,"message":"insufficient buying power"}"#,
        )
        .await;
    });

    let client = build_test_client(&base_url, 3);
    let dummy_req = AlpacaOrderRequest {
        symbol: "NVDA".to_string(),
        qty: dec!(100),
        side: AlpacaOrderSide::Buy,
        order_type: AlpacaOrderType::Market,
        time_in_force: AlpacaTimeInForce::Day,
        limit_price: None,
        stop_price: None,
        trail_price: None,
        trail_percent: None,
        extended_hours: false,
        client_order_id: "CID-WASH-1".to_string(),
        order_class: Some(AlpacaOrderClass::Simple),
        take_profit: None,
        stop_loss: None,
    };

    let err1 = client.submit_order(&dummy_req).await.unwrap_err();
    assert_eq!(
        err1,
        AlpacaError::DeterministicReject {
            status: 422,
            code: Some(42210000),
            message: "existing open order would result in wash trade".to_string(),
        }
    );
    assert!(
        !err1.is_retryable(),
        "422 DeterministicReject must never be retryable"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "422 rejection must fail closed on first attempt without retrying"
    );

    let err2 = client.submit_order(&dummy_req).await.unwrap_err();
    assert_eq!(
        err2,
        AlpacaError::DeterministicReject {
            status: 403,
            code: Some(40310000),
            message: "insufficient buying power".to_string(),
        }
    );
    assert!(!err2.is_retryable());
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "403 business rejection must fail closed on first attempt without retrying"
    );

    server_handle.await.unwrap();
}
