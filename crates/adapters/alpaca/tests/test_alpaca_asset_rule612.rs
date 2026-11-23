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

//! Blind-TDD integration tests for `AlpacaHttpClient::get_asset` and Reg NMS Rule 612 parsing.

#![allow(linker_messages)]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use nautilus_alpaca::{
    ALPACA_VENUE, AlpacaConfig, AlpacaCredential, AlpacaEnvironment, AlpacaError, AlpacaHttpClient,
    common::{HEADER_APCA_API_KEY_ID, HEADER_APCA_API_SECRET_KEY},
    http::{AlpacaAssetResponse, parse_equity_instrument_with_ref_price},
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn read_http_request(stream: &mut tokio::net::TcpStream) -> String {
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 1024];
    loop {
        let n = stream.read(&mut tmp).await.unwrap_or(0);
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
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
    let cred = AlpacaCredential::new_checked("PKASSETKEY1234", "SKASSETSECRET5678").unwrap();
    let cfg = AlpacaConfig::new_checked(
        AlpacaEnvironment::Paper,
        base_url,
        "wss://paper-api.alpaca.markets/stream",
        cred,
        5,
        max_retries,
        10,
        50,
        100,
        ALPACA_VENUE,
    )
    .unwrap();
    AlpacaHttpClient::new(cfg).unwrap()
}

fn make_asset_dto(
    symbol: &str,
    asset_class: &str,
    status: &str,
    tradable: bool,
    fractionable: bool,
    min_order_size: Option<Decimal>,
    price_increment: Option<Decimal>,
    min_trade_increment: Option<Decimal>,
) -> AlpacaAssetResponse {
    AlpacaAssetResponse {
        id: "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415".to_string(),
        asset_class: asset_class.to_string(),
        exchange: "NASDAQ".to_string(),
        symbol: symbol.to_string(),
        name: format!("{symbol} Corp"),
        status: status.to_string(),
        tradable,
        marginable: true,
        shortable: true,
        easy_to_borrow: true,
        fractionable,
        min_order_size,
        price_increment,
        min_trade_increment,
    }
}

#[tokio::test]
async fn test_http_get_asset_parses_min_trade_increment_and_auth_headers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_srv = Arc::clone(&call_count);

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        call_count_srv.fetch_add(1, Ordering::SeqCst);
        let req = read_http_request(&mut stream).await;
        assert!(
            req.starts_with("GET /v2/assets/AAPL "),
            "unexpected request line: {req}"
        );
        let req_lower = req.to_ascii_lowercase();
        assert!(req_lower.contains(&format!("{}: pkassetkey1234", HEADER_APCA_API_KEY_ID.to_ascii_lowercase())));
        assert!(req_lower.contains(&format!("{}: skassetsecret5678", HEADER_APCA_API_SECRET_KEY.to_ascii_lowercase())));

        let body = r#"{
            "id": "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415",
            "class": "us_equity",
            "exchange": "NASDAQ",
            "symbol": "AAPL",
            "name": "Apple Inc.",
            "status": "active",
            "tradable": true,
            "marginable": true,
            "shortable": true,
            "easy_to_borrow": true,
            "fractionable": true,
            "min_order_size": "0.0001",
            "min_trade_increment": "1",
            "price_increment": "0.01"
        }"#;
        write_http_response(&mut stream, "200 OK", body).await;
    });

    let client = build_test_client(&format!("http://{addr}"), 0);
    let asset = client.get_asset("AAPL").await.unwrap();

    server.await.unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert_eq!(asset.symbol, "AAPL");
    assert_eq!(asset.asset_class, "us_equity");
    assert_eq!(asset.status, "active");
    assert!(asset.tradable);
    assert!(asset.fractionable);
    assert_eq!(asset.min_order_size, Some(dec!(0.0001)));
    assert_eq!(asset.min_trade_increment, Some(dec!(1)));
    assert_eq!(asset.price_increment, Some(dec!(0.01)));
}

#[tokio::test]
async fn test_http_get_asset_fails_closed_on_404_500_and_invalid_symbol() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_srv = Arc::clone(&call_count);

    let server = tokio::spawn(async move {
        // Request 1: 404
        let (mut stream1, _) = listener.accept().await.unwrap();
        call_count_srv.fetch_add(1, Ordering::SeqCst);
        let req1 = read_http_request(&mut stream1).await;
        assert!(req1.starts_with("GET /v2/assets/UNKNOWN "));
        write_http_response(&mut stream1, "404 Not Found", r#"{"message":"asset not found"}"#).await;

        // Request 2: 500
        let (mut stream2, _) = listener.accept().await.unwrap();
        call_count_srv.fetch_add(1, Ordering::SeqCst);
        let req2 = read_http_request(&mut stream2).await;
        assert!(req2.starts_with("GET /v2/assets/BROKEN "));
        write_http_response(
            &mut stream2,
            "500 Internal Server Error",
            r#"{"message":"internal error"}"#,
        )
        .await;
    });

    let client = build_test_client(&format!("http://{addr}"), 0);

    let err_404 = client.get_asset("UNKNOWN").await.unwrap_err();
    assert!(
        matches!(err_404, AlpacaError::DeterministicReject { status: 404, .. }),
        "expected DeterministicReject 404, got {err_404:?}"
    );

    let err_500 = client.get_asset("BROKEN").await.unwrap_err();
    assert!(
        matches!(err_500, AlpacaError::RetryableServer { status: 500, .. }),
        "expected RetryableServer 500, got {err_500:?}"
    );

    // Invalid / path-traversal symbols fail before hitting HTTP.
    assert!(client.get_asset("").await.is_err());
    assert!(client.get_asset("../secrets").await.is_err());

    server.await.unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[test]
fn test_rule612_tick_precision_and_whole_share_lot_size() {
    let ts_init = UnixNanos::from(1_700_000_000_000_000_000u64);
    let fractionable_penny = make_asset_dto(
        "PENNY",
        "us_equity",
        "active",
        true,
        true,
        Some(dec!(0.0001)),
        None,
        None,
    );

    // Sub-$1.00 reference price => Rule 612 tick = $0.0001 (precision 4), lot_size = 1, size_precision = 0.
    let sub_dollar =
        parse_equity_instrument_with_ref_price(&fractionable_penny, ALPACA_VENUE, ts_init, Some(dec!(0.85)))
            .unwrap();
    match sub_dollar {
        InstrumentAny::Equity(eq) => {
            assert_eq!(eq.id(), InstrumentId::from("PENNY.ALPACA"));
            assert_eq!(eq.price_precision(), 4);
            assert_eq!(eq.price_increment().as_decimal(), dec!(0.0001));
            assert_eq!(eq.size_precision(), 0);
            assert_eq!(eq.lot_size().unwrap().as_decimal(), dec!(1));
        }
        other => panic!("expected Equity, got {other:?}"),
    }

    // Exact $1.00 boundary => Rule 612 tick = $0.01 (precision 2), whole-share lot_size = 1.
    let at_one_dollar =
        parse_equity_instrument_with_ref_price(&fractionable_penny, ALPACA_VENUE, ts_init, Some(dec!(1.00)))
            .unwrap();
    match at_one_dollar {
        InstrumentAny::Equity(eq) => {
            assert_eq!(eq.price_precision(), 2);
            assert_eq!(eq.price_increment().as_decimal(), dec!(0.01));
            assert_eq!(eq.size_precision(), 0);
            assert_eq!(eq.lot_size().unwrap().as_decimal(), dec!(1));
        }
        other => panic!("expected Equity, got {other:?}"),
    }

    // >= $1.00 and None fallback => tick = $0.01 (precision 2), whole-share lot_size = 1.
    for ref_px in [Some(dec!(215.43)), None] {
        let inst = parse_equity_instrument_with_ref_price(
            &fractionable_penny,
            ALPACA_VENUE,
            ts_init,
            ref_px,
        )
        .unwrap();
        match inst {
            InstrumentAny::Equity(eq) => {
                assert_eq!(eq.price_precision(), 2);
                assert_eq!(eq.price_increment().as_decimal(), dec!(0.01));
                assert_eq!(eq.size_precision(), 0);
                assert_eq!(eq.lot_size().unwrap().as_decimal(), dec!(1));
            }
            other => panic!("expected Equity, got {other:?}"),
        }
    }
}

#[test]
fn test_parse_equity_instrument_with_ref_price_fails_closed_on_invalid_assets() {
    let ts_init = UnixNanos::from(1_700_000_000_000_000_000u64);

    let untradable = make_asset_dto("HALT", "us_equity", "active", false, false, None, None, None);
    assert!(matches!(
        parse_equity_instrument_with_ref_price(&untradable, ALPACA_VENUE, ts_init, Some(dec!(50))),
        Err(AlpacaError::Parse(_))
    ));

    let inactive = make_asset_dto("DELIST", "us_equity", "inactive", true, false, None, None, None);
    assert!(matches!(
        parse_equity_instrument_with_ref_price(&inactive, ALPACA_VENUE, ts_init, Some(dec!(50))),
        Err(AlpacaError::Parse(_))
    ));

    let crypto = make_asset_dto("BTCUSD", "crypto", "active", true, true, None, None, None);
    assert!(matches!(
        parse_equity_instrument_with_ref_price(&crypto, ALPACA_VENUE, ts_init, Some(dec!(50000))),
        Err(AlpacaError::Parse(_))
    ));

    let active_eq = make_asset_dto("AAPL", "us_equity", "active", true, false, None, None, None);
    assert!(
        parse_equity_instrument_with_ref_price(&active_eq, ALPACA_VENUE, ts_init, Some(dec!(0)))
            .is_err()
    );
    assert!(
        parse_equity_instrument_with_ref_price(&active_eq, ALPACA_VENUE, ts_init, Some(dec!(-1)))
            .is_err()
    );
}

#[tokio::test]
async fn test_http_get_asset_strict_symbol_allowlist_blocks_pre_http() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_srv = Arc::clone(&call_count);

    let server = tokio::spawn(async move {
        for expected_symbol in ["AAPL", "BRK.B", "BF-B"] {
            let (mut stream, _) = listener.accept().await.unwrap();
            call_count_srv.fetch_add(1, Ordering::SeqCst);
            let req = read_http_request(&mut stream).await;
            let expected_prefix = format!("GET /v2/assets/{expected_symbol} ");
            assert!(
                req.starts_with(&expected_prefix),
                "unexpected request line: {req}"
            );
            let body = format!(
                r#"{{
                    "id": "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415",
                    "class": "us_equity",
                    "exchange": "NYSE",
                    "symbol": "{expected_symbol}",
                    "name": "{expected_symbol} Corp",
                    "status": "active",
                    "tradable": true,
                    "marginable": true,
                    "shortable": true,
                    "easy_to_borrow": true,
                    "fractionable": false,
                    "min_order_size": "1",
                    "min_trade_increment": "1",
                    "price_increment": "0.01"
                }}"#
            );
            write_http_response(&mut stream, "200 OK", &body).await;
        }
    });

    let client = build_test_client(&format!("http://{addr}"), 0);

    for valid in ["AAPL", "BRK.B", "BF-B"] {
        let asset = client.get_asset(valid).await.unwrap();
        assert_eq!(asset.symbol, valid);
    }

    let blocked_symbols = [
        ".",
        "..",
        "../orders",
        "%2e%2e%2forders",
        "AAPL&foo=1",
        "AAPL\0bar",
        "AAPL?x=1",
        "AAPL#x",
        "",
        "   ",
    ];
    for bad_symbol in blocked_symbols {
        let err = client.get_asset(bad_symbol).await.unwrap_err();
        assert!(
            matches!(err, AlpacaError::Parse(_)),
            "expected AlpacaError::Parse for {bad_symbol:?}, got {err:?}"
        );
    }

    server.await.unwrap();
    assert_eq!(
        call_count.load(Ordering::SeqCst),
        3,
        "blocked symbols must fail pre-HTTP without hitting server"
    );
}
