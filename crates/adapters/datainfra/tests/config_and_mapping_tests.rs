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

//! Integration tests for `nautilus-datainfra` configuration validation, identifier/timeframe
//! mapping, and US equity instrument metadata construction (Ticket #01).

#![allow(linker_messages)]

use std::{num::NonZeroUsize, str::FromStr};

use nautilus_core::UnixNanos;
use nautilus_datainfra::{
    DataInfraConfig, DataInfraEquityMetadata, DataInfraError, DataInfraTimeframe,
    config::{MAX_ALLOWED_RETRIES, MAX_REQUEST_TIMEOUT_MS, MIN_REQUEST_TIMEOUT_MS},
    from_bar_type, from_instrument_id, parse_datainfra_bar_type, to_bar_type, to_instrument_id,
};
use nautilus_model::{
    data::bar::{BarSpecification, BarType},
    enums::{AggregationSource, BarAggregation, PriceType},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use rstest::rstest;
use rust_decimal_macros::dec;

fn valid_config() -> DataInfraConfig {
    DataInfraConfig::new_checked(
        "http://127.0.0.1:8123",
        "market_data",
        5_000,
        3,
        vec![
            "127.0.0.1:9092".to_string(),
            "kafka-broker-2.internal:9092".to_string(),
        ],
        "nautilus-datainfra-consumer",
        "http://127.0.0.1:8012",
        "ALPACA",
    )
    .expect("valid DataInfraConfig should succeed")
}

// ================================================================================================
// 1. DataInfraConfig Normal & Boundary Validation Tests
// ================================================================================================

#[rstest]
fn test_datainfra_config_valid_construction_and_roundtrip_serde() {
    let cfg = valid_config();
    assert_eq!(cfg.clickhouse_url, "http://127.0.0.1:8123");
    assert_eq!(cfg.clickhouse_database, "market_data");
    assert_eq!(cfg.request_timeout_ms, 5_000);
    assert_eq!(cfg.max_retries, 3);
    assert_eq!(cfg.kafka_brokers.len(), 2);
    assert_eq!(cfg.kafka_group_id, "nautilus-datainfra-consumer");
    assert_eq!(cfg.calendar_service_url, "http://127.0.0.1:8012");
    assert_eq!(cfg.default_venue, "ALPACA");
    assert!(cfg.validate().is_ok());

    let json = serde_json::to_string(&cfg).expect("serialize config");
    let decoded: DataInfraConfig = serde_json::from_str(&json).expect("deserialize config");
    assert_eq!(decoded, cfg);
    assert!(decoded.validate().is_ok());
}

#[rstest]
fn test_datainfra_config_boundary_timeout_and_retries_accepted() {
    let min_cfg = DataInfraConfig::new_checked(
        "https://clickhouse.prod.local:8443",
        "_private_market_data_v2",
        MIN_REQUEST_TIMEOUT_MS,
        0,
        vec!["localhost:9092".to_string()],
        "cg_min",
        "https://calendar.prod.local:8443",
        "NASDAQ",
    );
    assert!(min_cfg.is_ok());

    let max_cfg = DataInfraConfig::new_checked(
        "http://192.168.3.100:8010",
        "market_data_2026",
        MAX_REQUEST_TIMEOUT_MS,
        MAX_ALLOWED_RETRIES,
        vec!["192.168.3.100:65535".to_string()],
        "cg_max",
        "http://192.168.3.100:8012",
        "ARCA",
    );
    assert!(max_cfg.is_ok());
}

#[rstest]
#[case::empty("")]
#[case::whitespace("   ")]
#[case::non_http_tcp("tcp://127.0.0.1:9000")]
#[case::non_http_ws("ws://127.0.0.1:8123")]
#[case::non_http_ftp("ftp://127.0.0.1:21")]
#[case::missing_scheme("127.0.0.1:8123")]
#[case::port_zero("http://127.0.0.1:0")]
#[case::port_overflow("http://127.0.0.1:70000")]
fn test_datainfra_config_rejects_invalid_clickhouse_url(#[case] bad_url: &str) {
    let mut cfg = valid_config();
    cfg.clickhouse_url = bad_url.to_string();
    let err = cfg.validate().expect_err("must reject invalid clickhouse_url");
    assert!(matches!(err, DataInfraError::InvalidConfig(_)));
}

#[rstest]
#[case::empty("")]
#[case::non_http("grpc://127.0.0.1:8012")]
#[case::port_zero("http://localhost:0")]
#[case::port_overflow("http://localhost:70000")]
fn test_datainfra_config_rejects_invalid_calendar_service_url(#[case] bad_url: &str) {
    let mut cfg = valid_config();
    cfg.calendar_service_url = bad_url.to_string();
    let err = cfg
        .validate()
        .expect_err("must reject invalid calendar_service_url");
    assert!(matches!(err, DataInfraError::InvalidConfig(_)));
}

#[rstest]
#[case::empty("")]
#[case::whitespace("   ")]
#[case::sql_injection("market_data; DROP TABLE ohlcv_bars--")]
#[case::starts_with_digit("1market_data")]
#[case::contains_hyphen("market-data")]
#[case::contains_dot("default.market_data")]
#[case::contains_quote("market_data' OR '1'='1")]
fn test_datainfra_config_rejects_invalid_database_identifier(#[case] bad_db: &str) {
    let mut cfg = valid_config();
    cfg.clickhouse_database = bad_db.to_string();
    let err = cfg
        .validate()
        .expect_err("must reject invalid SQL database identifier");
    assert!(matches!(err, DataInfraError::InvalidConfig(_)));
}

#[rstest]
#[case::zero(0)]
#[case::below_min(MIN_REQUEST_TIMEOUT_MS - 1)]
#[case::above_max(MAX_REQUEST_TIMEOUT_MS + 1)]
#[case::u64_max(u64::MAX)]
fn test_datainfra_config_rejects_out_of_bounds_timeout(#[case] bad_timeout: u64) {
    let mut cfg = valid_config();
    cfg.request_timeout_ms = bad_timeout;
    let err = cfg
        .validate()
        .expect_err("must reject out-of-bounds request_timeout_ms");
    assert!(matches!(err, DataInfraError::InvalidConfig(_)));
}

#[rstest]
#[case::eleven(MAX_ALLOWED_RETRIES + 1)]
#[case::hundred(100)]
fn test_datainfra_config_rejects_excessive_retries(#[case] bad_retries: u32) {
    let mut cfg = valid_config();
    cfg.max_retries = bad_retries;
    let err = cfg
        .validate()
        .expect_err("must reject max_retries > MAX_ALLOWED_RETRIES");
    assert!(matches!(err, DataInfraError::InvalidConfig(_)));
}

#[rstest]
#[case::empty_list(vec![])]
#[case::empty_broker_entry(vec!["".to_string()])]
#[case::missing_port(vec!["localhost".to_string()])]
#[case::non_numeric_port(vec!["localhost:abc".to_string()])]
#[case::zero_port(vec!["localhost:0".to_string()])]
#[case::overflow_port(vec!["localhost:70000".to_string()])]
#[case::empty_host(vec![":9092".to_string()])]
#[case::url_with_scheme(vec!["http://localhost:9092".to_string()])]
#[case::whitespace_in_host(vec!["local host:9092".to_string()])]
fn test_datainfra_config_rejects_invalid_kafka_brokers(#[case] bad_brokers: Vec<String>) {
    let mut cfg = valid_config();
    cfg.kafka_brokers = bad_brokers;
    let err = cfg
        .validate()
        .expect_err("must reject invalid kafka_brokers");
    assert!(matches!(err, DataInfraError::InvalidConfig(_)));
}

#[rstest]
fn test_datainfra_config_rejects_empty_group_id_or_invalid_venue() {
    let mut cfg = valid_config();
    cfg.kafka_group_id = "   ".to_string();
    assert!(matches!(
        cfg.validate(),
        Err(DataInfraError::InvalidConfig(_))
    ));

    let mut cfg2 = valid_config();
    cfg2.default_venue = "".to_string();
    assert!(matches!(
        cfg2.validate(),
        Err(DataInfraError::InvalidConfig(_))
    ));

    let mut cfg3 = valid_config();
    cfg3.default_venue = "BAD VENUE!".to_string();
    assert!(matches!(
        cfg3.validate(),
        Err(DataInfraError::InvalidConfig(_))
    ));
}

// ================================================================================================
// 2. Bidirectional InstrumentId & Timeframe / BarType Mapping Tests
// ================================================================================================

#[rstest]
#[case("NVDA", "ALPACA", "NVDA.ALPACA")]
#[case("SPY", "ARCA", "SPY.ARCA")]
#[case("AAPL", "NASDAQ", "AAPL.NASDAQ")]
#[case("BRK/B", "NYSE", "BRK/B.NYSE")]
fn test_instrument_id_bidirectional_roundtrip(
    #[case] symbol: &str,
    #[case] venue: &str,
    #[case] expected_full: &str,
) {
    let instrument_id = to_instrument_id(symbol, venue).expect("valid InstrumentId");
    assert_eq!(instrument_id.to_string(), expected_full);
    assert_eq!(
        instrument_id,
        InstrumentId::from_str(expected_full).expect("parse InstrumentId")
    );

    let (roundtrip_symbol, roundtrip_venue) = from_instrument_id(instrument_id);
    assert_eq!(roundtrip_symbol, symbol);
    assert_eq!(roundtrip_venue, venue);
}

#[rstest]
#[case::empty_symbol("", "ALPACA")]
#[case::whitespace_symbol("   ", "ALPACA")]
#[case::empty_venue("NVDA", "")]
#[case::whitespace_venue("NVDA", "   ")]
#[case::space_in_symbol("NV DA", "ALPACA")]
#[case::space_in_venue("NVDA", "AL PACA")]
fn test_to_instrument_id_rejects_invalid_identifiers(#[case] symbol: &str, #[case] venue: &str) {
    let err = to_instrument_id(symbol, venue).expect_err("must reject invalid symbol/venue");
    assert!(matches!(err, DataInfraError::InvalidIdentifier(_)));
}

#[rstest]
#[case(DataInfraTimeframe::M1, "1m", 1, BarAggregation::Minute, "NVDA.ALPACA-1-MINUTE-LAST-EXTERNAL")]
#[case(DataInfraTimeframe::M15, "15m", 15, BarAggregation::Minute, "NVDA.ALPACA-15-MINUTE-LAST-EXTERNAL")]
#[case(DataInfraTimeframe::H1, "1h", 1, BarAggregation::Hour, "NVDA.ALPACA-1-HOUR-LAST-EXTERNAL")]
#[case(DataInfraTimeframe::H4, "4h", 4, BarAggregation::Hour, "NVDA.ALPACA-4-HOUR-LAST-EXTERNAL")]
#[case(DataInfraTimeframe::D1, "1d", 1, BarAggregation::Day, "NVDA.ALPACA-1-DAY-LAST-EXTERNAL")]
#[case(DataInfraTimeframe::W1, "1w", 1, BarAggregation::Week, "NVDA.ALPACA-1-WEEK-LAST-EXTERNAL")]
fn test_all_six_timeframes_roundtrip_bar_spec_and_bar_type(
    #[case] tf: DataInfraTimeframe,
    #[case] canonical_str: &str,
    #[case] expected_step: usize,
    #[case] expected_agg: BarAggregation,
    #[case] expected_bar_type_str: &str,
) {
    assert_eq!(tf.as_str(), canonical_str);
    assert_eq!(tf.to_string(), canonical_str);
    assert_eq!(
        DataInfraTimeframe::from_str(canonical_str).expect("parse canonical timeframe"),
        tf
    );

    let spec = tf.to_bar_specification();
    assert_eq!(spec.step.get(), expected_step);
    assert_eq!(spec.aggregation, expected_agg);
    assert_eq!(spec.price_type, PriceType::Last);

    let roundtrip_tf =
        DataInfraTimeframe::from_bar_specification(spec).expect("roundtrip BarSpecification");
    assert_eq!(roundtrip_tf, tf);

    let bar_type = to_bar_type("NVDA", "ALPACA", tf).expect("to_bar_type");
    assert_eq!(bar_type.to_string(), expected_bar_type_str);
    assert_eq!(bar_type.aggregation_source(), AggregationSource::External);
    assert_eq!(bar_type.spec(), spec);

    let parsed_bar_type =
        parse_datainfra_bar_type("NVDA", "ALPACA", canonical_str).expect("parse_datainfra_bar_type");
    assert_eq!(parsed_bar_type, bar_type);

    let (sym, ven, extracted_tf) = from_bar_type(&bar_type).expect("from_bar_type");
    assert_eq!(sym, "NVDA");
    assert_eq!(ven, "ALPACA");
    assert_eq!(extracted_tf, tf);
}

#[rstest]
#[case("1Min", DataInfraTimeframe::M1)]
#[case("15Min", DataInfraTimeframe::M15)]
#[case("1Hour", DataInfraTimeframe::H1)]
#[case("4Hour", DataInfraTimeframe::H4)]
#[case("1Day", DataInfraTimeframe::D1)]
#[case("1Week", DataInfraTimeframe::W1)]
fn test_timeframe_aliases_parse_to_canonical_enum(
    #[case] alias: &str,
    #[case] expected: DataInfraTimeframe,
) {
    let tf = DataInfraTimeframe::from_str(alias).expect("alias should parse");
    assert_eq!(tf, expected);
}

#[rstest]
#[case::empty("")]
#[case::two_minutes("2m")]
#[case::five_minutes("5m")]
#[case::thirty_minutes("30m")]
#[case::two_hours("2h")]
#[case::three_hours("3h")]
#[case::one_second("1s")]
#[case::tick("tick")]
fn test_unsupported_timeframe_strings_fail_closed(#[case] bad_tf: &str) {
    let err = DataInfraTimeframe::from_str(bad_tf).expect_err("must reject unsupported timeframe");
    assert!(matches!(err, DataInfraError::UnsupportedTimeframe(_)));

    let err2 = parse_datainfra_bar_type("NVDA", "ALPACA", bad_tf)
        .expect_err("parse_datainfra_bar_type must fail closed");
    assert!(matches!(err2, DataInfraError::UnsupportedTimeframe(_)));
}

#[rstest]
fn test_from_bar_type_rejects_internal_aggregation_non_last_price_and_composite() {
    let instrument_id = to_instrument_id("NVDA", "ALPACA").unwrap();

    // 1. Internal aggregation source must be rejected
    let internal_bar_type = BarType::new(
        instrument_id,
        DataInfraTimeframe::M15.to_bar_specification(),
        AggregationSource::Internal,
    );
    let err_internal =
        from_bar_type(&internal_bar_type).expect_err("must reject INTERNAL aggregation");
    assert!(matches!(
        err_internal,
        DataInfraError::UnsupportedTimeframe(_)
    ));

    // 2. PriceType::Mid must be rejected
    let mid_spec = BarSpecification {
        step: NonZeroUsize::new(15).unwrap(),
        aggregation: BarAggregation::Minute,
        price_type: PriceType::Mid,
    };
    let mid_bar_type = BarType::new(instrument_id, mid_spec, AggregationSource::External);
    let err_mid = from_bar_type(&mid_bar_type).expect_err("must reject MID price type");
    assert!(matches!(err_mid, DataInfraError::UnsupportedTimeframe(_)));

    // 3. Unsupported step (e.g. 2-MINUTE) must be rejected
    let two_min_spec = BarSpecification {
        step: NonZeroUsize::new(2).unwrap(),
        aggregation: BarAggregation::Minute,
        price_type: PriceType::Last,
    };
    let two_min_bar_type = BarType::new(instrument_id, two_min_spec, AggregationSource::External);
    let err_step = from_bar_type(&two_min_bar_type).expect_err("must reject 2-MINUTE bar spec");
    assert!(matches!(err_step, DataInfraError::UnsupportedTimeframe(_)));

    // 4. Composite BarType must be rejected
    let composite_bar_type = BarType::new_composite(
        instrument_id,
        DataInfraTimeframe::M15.to_bar_specification(),
        AggregationSource::External,
        15,
        BarAggregation::Minute,
        AggregationSource::External,
    );
    let err_comp =
        from_bar_type(&composite_bar_type).expect_err("must reject Composite BarType");
    assert!(matches!(err_comp, DataInfraError::UnsupportedTimeframe(_)));
}

// ================================================================================================
// 3. DataInfraEquityMetadata Construction & Fail-Closed Validation Tests
// ================================================================================================

#[rstest]
fn test_us_equity_metadata_to_instrument_any_valid() {
    let ts_event = UnixNanos::from(1_700_000_000_000_000_000_u64);
    let ts_init = UnixNanos::from(1_700_000_000_100_000_000_u64);

    let mut meta = DataInfraEquityMetadata::us_equity("NVDA", "ALPACA", ts_event, ts_init);
    meta.isin = Some("US67066G1040".to_string());
    meta.max_quantity = Some(100_000);
    meta.margin_init = Some(dec!(0.50));
    meta.margin_maint = Some(dec!(0.25));

    let instrument_any = meta
        .to_instrument_any()
        .expect("valid US equity should build InstrumentAny::Equity");

    match &instrument_any {
        InstrumentAny::Equity(eq) => {
            assert_eq!(eq.id.to_string(), "NVDA.ALPACA");
            assert_eq!(eq.raw_symbol.as_str(), "NVDA");
            assert_eq!(eq.isin.map(|u| u.as_str()), Some("US67066G1040"));
            assert_eq!(eq.currency.code.as_str(), "USD");
            assert_eq!(eq.price_precision, 2);
            assert_eq!(eq.price_increment, Price::from("0.01"));
            assert_eq!(eq.lot_size, Some(Quantity::from("1")));
            assert_eq!(eq.min_quantity, Some(Quantity::from("1")));
            assert_eq!(eq.max_quantity, Some(Quantity::from("100000")));
            assert_eq!(eq.ts_event, ts_event);
            assert_eq!(eq.ts_init, ts_init);
        }
        other => panic!("expected InstrumentAny::Equity, got {other:?}"),
    }

    assert_eq!(instrument_any.id().to_string(), "NVDA.ALPACA");
    assert_eq!(instrument_any.price_precision(), 2);
    assert_eq!(instrument_any.size_precision(), 0);
}

#[rstest]
fn test_equity_metadata_rejects_invalid_precision_lot_size_and_quantity_bounds() {
    let ts = UnixNanos::from(1_700_000_000_000_000_000_u64);

    // 1. price_precision == 0
    let mut bad_prec_zero = DataInfraEquityMetadata::us_equity("NVDA", "ALPACA", ts, ts);
    bad_prec_zero.price_precision = 0;
    assert!(matches!(
        bad_prec_zero.to_instrument_any(),
        Err(DataInfraError::InvalidInstrument(_))
    ));

    // 2. price_precision > 9
    let mut bad_prec_high = DataInfraEquityMetadata::us_equity("NVDA", "ALPACA", ts, ts);
    bad_prec_high.price_precision = 10;
    assert!(matches!(
        bad_prec_high.to_instrument_any(),
        Err(DataInfraError::InvalidInstrument(_))
    ));

    // 3. size_precision > 9
    let mut bad_size_prec = DataInfraEquityMetadata::us_equity("NVDA", "ALPACA", ts, ts);
    bad_size_prec.size_precision = 10;
    assert!(matches!(
        bad_size_prec.to_instrument_any(),
        Err(DataInfraError::InvalidInstrument(_))
    ));

    // 4. lot_size == 0
    let mut bad_lot = DataInfraEquityMetadata::us_equity("NVDA", "ALPACA", ts, ts);
    bad_lot.lot_size = 0;
    assert!(matches!(
        bad_lot.to_instrument_any(),
        Err(DataInfraError::InvalidInstrument(_))
    ));

    // 5. min_quantity == Some(0)
    let mut bad_min_zero = DataInfraEquityMetadata::us_equity("NVDA", "ALPACA", ts, ts);
    bad_min_zero.min_quantity = Some(0);
    assert!(matches!(
        bad_min_zero.to_instrument_any(),
        Err(DataInfraError::InvalidInstrument(_))
    ));

    // 6. min_quantity > max_quantity
    let mut bad_bounds = DataInfraEquityMetadata::us_equity("NVDA", "ALPACA", ts, ts);
    bad_bounds.min_quantity = Some(500);
    bad_bounds.max_quantity = Some(100);
    assert!(matches!(
        bad_bounds.to_instrument_any(),
        Err(DataInfraError::InvalidInstrument(_))
    ));

    // 7. Empty ISIN when Some
    let mut bad_isin = DataInfraEquityMetadata::us_equity("NVDA", "ALPACA", ts, ts);
    bad_isin.isin = Some("   ".to_string());
    assert!(matches!(
        bad_isin.to_instrument_any(),
        Err(DataInfraError::InvalidInstrument(_))
    ));

    // 8. Invalid currency code
    let mut bad_curr = DataInfraEquityMetadata::us_equity("NVDA", "ALPACA", ts, ts);
    bad_curr.currency_code = "NOT_A_CURRENCY".to_string();
    assert!(matches!(
        bad_curr.to_instrument_any(),
        Err(DataInfraError::InvalidInstrument(_))
    ));

    // 9. Invalid symbol
    let bad_sym = DataInfraEquityMetadata::us_equity("", "ALPACA", ts, ts);
    assert!(matches!(
        bad_sym.to_instrument_any(),
        Err(DataInfraError::InvalidInstrument(_))
    ));
}
