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

//! Integration tests for the `DataInfra` Kafka real-time stream decoder seam (`stream.rs`).

#![allow(linker_messages)]

use std::sync::Arc;

use nautilus_core::UnixNanos;
use nautilus_datainfra::{
    BarStreamVersionTracker, CanonicalBarWire, CanonicalQuoteWire, CanonicalStreamDecoder,
    CanonicalTradingStatusWire, CanonicalWireFrame, DEFAULT_SIP_KAFKA_TOPICS, DataInfraConfig,
    DataInfraError, DataInfraStreamEvent, DataInfraTimeframe, KafkaBrokerEnvelope,
    KafkaEnvelopePayload, KafkaPayloadFrame, KafkaPayloadSource, KafkaStreamSessionCoordinator,
    StreamDecodeOutcome, StreamLifecycleListener, TOPIC_SIP_BARS_1M, TOPIC_SIP_QUOTES,
    TOPIC_SIP_STATUSES, TcpKafkaPayloadSource, VersionEvaluation, to_instrument_id,
};
use nautilus_model::{
    enums::MarketStatusAction,
    types::{Price, Quantity},
};
use parking_lot::Mutex;
use rskafka::{
    client::{
        ClientBuilder,
        partition::{Compression, UnknownTopicHandling},
    },
    record::Record,
};
use tokio::net::TcpListener;
use ustr::Ustr;

#[derive(Debug, Clone, PartialEq, Eq)]
enum LifecycleTransition {
    Connected { endpoint: String },
    Disconnected { endpoint: String, reason: String },
    Reconnected { endpoint: String, attempt: u32 },
}

#[derive(Debug, Default)]
struct RecordingLifecycleListener {
    transitions: Mutex<Vec<LifecycleTransition>>,
}

impl RecordingLifecycleListener {
    fn snapshot(&self) -> Vec<LifecycleTransition> {
        self.transitions.lock().clone()
    }
}

impl StreamLifecycleListener for RecordingLifecycleListener {
    fn on_stream_connected(&self, endpoint: &str) {
        self.transitions.lock().push(LifecycleTransition::Connected {
            endpoint: endpoint.to_string(),
        });
    }

    fn on_stream_disconnected(&self, endpoint: &str, reason: &str) {
        self.transitions
            .lock()
            .push(LifecycleTransition::Disconnected {
                endpoint: endpoint.to_string(),
                reason: reason.to_string(),
            });
    }

    fn on_stream_reconnected(&self, endpoint: &str, attempt: u32) {
        self.transitions
            .lock()
            .push(LifecycleTransition::Reconnected {
                endpoint: endpoint.to_string(),
                attempt,
            });
    }
}

#[test]
fn test_decode_canonical_bar_quote_and_status_wire_frames() {
    let decoder = CanonicalStreamDecoder::new("NASDAQ", 2, 0, 128);

    // 1. Valid 1m SIP bar frame on md.sip.bars.1m
    let bar_json = br#"{
        "schema_version": 1,
        "symbol": "AAPL",
        "timeframe": "1m",
        "bar_ts": 1711980000,
        "open": 170.10,
        "high": 171.25,
        "low": 169.85,
        "close": 170.95,
        "volume": 48200.0,
        "vwap": 170.62,
        "trade_count": 310,
        "version": 1,
        "is_synthetic": false,
        "is_correction": false,
        "is_suspect_spike": false,
        "session": "REGULAR",
        "provider": "alpaca_sip",
        "source_ts_ms": 1711980060000,
        "ingest_ts_ms": 1711980060125
    }"#;

    let bar_wire_legacy: CanonicalBarWire =
        serde_json::from_slice(bar_json).expect("CanonicalBarWire v1 JSON should deserialize");
    assert_eq!(bar_wire_legacy.market, "US");
    assert_eq!(bar_wire_legacy.origin, "REALTIME");

    let bar_outcome = decoder
        .decode_topic_payload(TOPIC_SIP_BARS_1M, bar_json)
        .expect("valid CanonicalBarWire JSON should decode");

    match bar_outcome {
        StreamDecodeOutcome::Emitted(DataInfraStreamEvent::Bar(bar)) => {
            assert_eq!(bar.bar_type.to_string(), "AAPL.NASDAQ-1-MINUTE-LAST-EXTERNAL");
            assert_eq!(bar.open, Price::new(170.10, 2));
            assert_eq!(bar.high, Price::new(171.25, 2));
            assert_eq!(bar.low, Price::new(169.85, 2));
            assert_eq!(bar.close, Price::new(170.95, 2));
            assert_eq!(bar.volume, Quantity::new(48_200.0, 0));
            assert_eq!(bar.ts_event, UnixNanos::from(1_711_980_060_000_000_000u64));
            assert_eq!(bar.ts_init, UnixNanos::from(1_711_980_060_125_000_000u64));
        }
        other => panic!("expected Emitted(Bar), got {other:?}"),
    }

    // 1b. Explicit `market` and `origin` fields on initial Bar and UpdatedBar
    let explicit_bar_v1_json = br#"{
        "schema_version": 1,
        "market": "US",
        "symbol": "TSLA",
        "timeframe": "1m",
        "bar_ts": 1711980060,
        "open": 175.00,
        "high": 176.50,
        "low": 174.80,
        "close": 176.10,
        "volume": 32100.0,
        "vwap": 175.85,
        "trade_count": 210,
        "version": 1,
        "is_synthetic": false,
        "is_correction": false,
        "is_suspect_spike": false,
        "session": "REGULAR",
        "origin": "REALTIME",
        "provider": "alpaca_sip",
        "source_ts_ms": 1711980120000,
        "ingest_ts_ms": 1711980120050
    }"#;
    let explicit_bar_v1_wire: CanonicalBarWire = serde_json::from_slice(explicit_bar_v1_json)
        .expect("explicit CanonicalBarWire v1 should deserialize");
    assert_eq!(explicit_bar_v1_wire.market, "US");
    assert_eq!(explicit_bar_v1_wire.origin, "REALTIME");

    let explicit_v1_outcome = decoder
        .decode_topic_payload(TOPIC_SIP_BARS_1M, explicit_bar_v1_json)
        .expect("explicit CanonicalBarWire v1 should decode");
    match explicit_v1_outcome {
        StreamDecodeOutcome::Emitted(DataInfraStreamEvent::Bar(bar)) => {
            assert_eq!(bar.bar_type.to_string(), "TSLA.NASDAQ-1-MINUTE-LAST-EXTERNAL");
            assert_eq!(bar.close, Price::new(176.10, 2));
            assert_eq!(bar.volume, Quantity::new(32_100.0, 0));
        }
        other => panic!("expected Emitted(Bar) for explicit v1, got {other:?}"),
    }

    let explicit_bar_v2_json = br#"{
        "schema_version": 1,
        "market": "US",
        "symbol": "TSLA",
        "timeframe": "1m",
        "bar_ts": 1711980060,
        "open": 175.00,
        "high": 176.80,
        "low": 174.80,
        "close": 176.45,
        "volume": 33500.0,
        "vwap": 175.95,
        "trade_count": 225,
        "version": 2,
        "is_synthetic": false,
        "is_correction": true,
        "is_suspect_spike": false,
        "session": "REGULAR",
        "origin": "EOD_SYNC",
        "provider": "alpaca_sip",
        "source_ts_ms": 1711980125000,
        "ingest_ts_ms": 1711980125050
    }"#;
    let explicit_bar_v2_wire: CanonicalBarWire = serde_json::from_slice(explicit_bar_v2_json)
        .expect("explicit CanonicalBarWire v2 should deserialize");
    assert_eq!(explicit_bar_v2_wire.market, "US");
    assert_eq!(explicit_bar_v2_wire.origin, "EOD_SYNC");

    let explicit_v2_outcome = decoder
        .decode_topic_payload("md.sip.bars.1m.updated.v1", explicit_bar_v2_json)
        .expect("explicit CanonicalBarWire v2 should decode");
    match explicit_v2_outcome {
        StreamDecodeOutcome::Emitted(DataInfraStreamEvent::UpdatedBar {
            bar,
            version,
            is_correction,
        }) => {
            assert_eq!(version, 2);
            assert!(is_correction);
            assert_eq!(bar.bar_type.to_string(), "TSLA.NASDAQ-1-MINUTE-LAST-EXTERNAL");
            assert_eq!(bar.high, Price::new(176.80, 2));
            assert_eq!(bar.close, Price::new(176.45, 2));
            assert_eq!(bar.volume, Quantity::new(33_500.0, 0));
        }
        other => panic!("expected Emitted(UpdatedBar) for explicit v2, got {other:?}"),
    }

    // 2. Valid L1 SIP quote frame on md.sip.quotes
    let quote_json = br#"{
        "schema_version": 1,
        "symbol": "MSFT",
        "quote_ts_ms": 1711980005250,
        "ingest_ts_ms": 1711980005255,
        "bid_price": 420.15,
        "bid_size": 300.0,
        "ask_price": 420.20,
        "ask_size": 500.0,
        "mid_price": 420.175,
        "spread_bps": 1.19,
        "provider": "alpaca_sip"
    }"#;

    let quote_wire: CanonicalQuoteWire =
        serde_json::from_slice(quote_json).expect("CanonicalQuoteWire should deserialize");
    assert_eq!(quote_wire.market, "US");
    let explicit_quote_wire = CanonicalQuoteWire {
        schema_version: 1,
        market: "US".to_string(),
        symbol: "MSFT".to_string(),
        quote_ts_ms: 1_711_980_005_250,
        ingest_ts_ms: 1_711_980_005_255,
        bid_price: 420.15,
        bid_size: 300.0,
        ask_price: 420.20,
        ask_size: 500.0,
        mid_price: 420.175,
        spread_bps: 1.19,
        provider: "alpaca_sip".to_string(),
    };
    assert_eq!(quote_wire, explicit_quote_wire);
    let direct_quote = quote_wire
        .to_nautilus_quote("NASDAQ", 2, 0)
        .expect("direct quote conversion should succeed");

    let quote_outcome = decoder
        .decode_topic_payload(TOPIC_SIP_QUOTES, quote_json)
        .expect("valid CanonicalQuoteWire JSON should decode");

    match quote_outcome {
        StreamDecodeOutcome::Emitted(DataInfraStreamEvent::Quote(quote)) => {
            assert_eq!(quote, direct_quote);
            assert_eq!(quote.instrument_id.to_string(), "MSFT.NASDAQ");
            assert_eq!(quote.bid_price, Price::new(420.15, 2));
            assert_eq!(quote.ask_price, Price::new(420.20, 2));
            assert_eq!(quote.bid_size, Quantity::new(300.0, 0));
            assert_eq!(quote.ask_size, Quantity::new(500.0, 0));
            assert_eq!(
                quote.ts_event,
                UnixNanos::from(1_711_980_005_250_000_000u64)
            );
            assert_eq!(quote.ts_init, UnixNanos::from(1_711_980_005_255_000_000u64));
        }
        other => panic!("expected Emitted(Quote), got {other:?}"),
    }

    // 3. Valid SIP trading status frames on md.sip.statuses (LULD_PAUSE & SHORT_SALE_RESTRICTED)
    let pause_json = br#"{
        "schema_version": 1,
        "symbol": "NVDA",
        "status_ts_ms": 1711980010000,
        "ingest_ts_ms": 1711980010005,
        "state": "LULD_PAUSE",
        "reason_code": "LULD_T5",
        "provider": "alpaca_sip"
    }"#;

    let pause_wire_legacy: CanonicalTradingStatusWire = serde_json::from_slice(pause_json)
        .expect("legacy CanonicalTradingStatusWire JSON should deserialize");
    assert_eq!(pause_wire_legacy.market, "US");

    let pause_outcome = decoder
        .decode_topic_payload(TOPIC_SIP_STATUSES, pause_json)
        .expect("valid CanonicalTradingStatusWire JSON should decode");

    match pause_outcome {
        StreamDecodeOutcome::Emitted(DataInfraStreamEvent::TradingStatus(status)) => {
            assert_eq!(status.instrument_id.to_string(), "NVDA.NASDAQ");
            assert_eq!(status.action, MarketStatusAction::Pause);
            assert_eq!(status.reason, Some(Ustr::from("LULD_T5")));
            assert_eq!(status.trading_event, Some(Ustr::from("LULD_PAUSE")));
            assert_eq!(status.is_trading, Some(false));
            assert_eq!(status.is_quoting, Some(false));
            assert_eq!(status.is_short_sell_restricted, Some(false));
            assert_eq!(
                status.ts_event,
                UnixNanos::from(1_711_980_010_000_000_000u64)
            );
            assert_eq!(
                status.ts_init,
                UnixNanos::from(1_711_980_010_005_000_000u64)
            );
        }
        other => panic!("expected Emitted(TradingStatus), got {other:?}"),
    }

    let ssr_wire = CanonicalTradingStatusWire {
        schema_version: 1,
        market: "US".to_string(),
        symbol: "AMD".to_string(),
        status_ts_ms: 1_711_980_020_000,
        ingest_ts_ms: 0,
        state: "SHORT_SALE_RESTRICTED".to_string(),
        reason_code: None,
        provider: "alpaca_sip".to_string(),
    };
    let ssr_status = ssr_wire
        .to_nautilus_status("NASDAQ")
        .expect("SSR status should convert");
    assert_eq!(ssr_status.action, MarketStatusAction::Trading);
    assert_eq!(ssr_status.is_trading, Some(true));
    assert_eq!(ssr_status.is_short_sell_restricted, Some(true));
    assert_eq!(ssr_status.reason, None);
    assert_eq!(ssr_status.ts_init, ssr_status.ts_event);
}

#[test]
fn test_quality_guards_filter_synthetic_and_suspect_spike_and_reject_corrupted_frames() {
    let decoder = CanonicalStreamDecoder::new("NASDAQ", 2, 0, 128);
    let aapl_id = to_instrument_id("AAPL", "NASDAQ").unwrap();

    // 1. Synthetic +3s zero-volume placeholder bar -> FilteredSynthetic (must NOT record version)
    let synthetic_bar = CanonicalBarWire {
        schema_version: 1,
        market: "US".to_string(),
        symbol: "aapl".to_string(),
        timeframe: "1m".to_string(),
        bar_ts: 1_711_980_120,
        open: 170.00,
        high: 170.00,
        low: 170.00,
        close: 170.00,
        volume: 0.0,
        vwap: None,
        trade_count: Some(0),
        version: 1,
        is_synthetic: true,
        is_correction: false,
        is_suspect_spike: false,
        session: "REGULAR".to_string(),
        origin: "REALTIME".to_string(),
        provider: "alpaca_sip".to_string(),
        source_ts_ms: 1_711_980_183_000,
        ingest_ts_ms: 1_711_980_183_010,
    };

    let outcome_synth = decoder
        .decode_bar_wire(&synthetic_bar)
        .expect("synthetic bar should be cleanly filtered");
    assert_eq!(
        outcome_synth,
        StreamDecodeOutcome::FilteredSynthetic {
            symbol: "AAPL".to_string(),
            bar_ts: 1_711_980_120,
        }
    );
    assert_eq!(
        decoder
            .version_tracker()
            .current_version(aapl_id, DataInfraTimeframe::M1, 1_711_980_120),
        None,
        "filtered synthetic bar must not pollute BarStreamVersionTracker"
    );

    // 2. Suspect spike bar -> FilteredSuspectSpike (must NOT record version)
    let spike_bar = CanonicalBarWire {
        is_synthetic: false,
        is_suspect_spike: true,
        high: 250.00,
        volume: 100.0,
        ..synthetic_bar.clone()
    };

    let outcome_spike = decoder
        .decode_bar_wire(&spike_bar)
        .expect("suspect spike bar should be cleanly filtered");
    assert_eq!(
        outcome_spike,
        StreamDecodeOutcome::FilteredSuspectSpike {
            symbol: "AAPL".to_string(),
            bar_ts: 1_711_980_120,
        }
    );
    assert_eq!(
        decoder
            .version_tracker()
            .current_version(aapl_id, DataInfraTimeframe::M1, 1_711_980_120),
        None,
        "filtered suspect spike bar must not pollute BarStreamVersionTracker"
    );

    // 3. Version == 0 fails closed with ProvenanceViolation
    let zero_version_bar = CanonicalBarWire {
        is_synthetic: false,
        is_suspect_spike: false,
        version: 0,
        volume: 1500.0,
        ..synthetic_bar.clone()
    };
    let err_zero_ver = decoder
        .decode_bar_wire(&zero_version_bar)
        .expect_err("version == 0 must fail closed");
    assert!(
        matches!(err_zero_ver, DataInfraError::ProvenanceViolation(_)),
        "expected ProvenanceViolation for version == 0, got {err_zero_ver:?}"
    );

    // 4. Corrupted OHLC (high < low) fails closed with ProvenanceViolation
    let corrupted_ohlc_bar = CanonicalBarWire {
        is_synthetic: false,
        is_suspect_spike: false,
        version: 1,
        open: 170.00,
        high: 168.50,
        low: 169.50,
        close: 169.00,
        volume: 1500.0,
        ..synthetic_bar
    };
    let err_ohlc = decoder
        .decode_bar_wire(&corrupted_ohlc_bar)
        .expect_err("corrupted OHLC (high < low) must fail closed");
    assert!(
        matches!(err_ohlc, DataInfraError::ProvenanceViolation(_)),
        "expected ProvenanceViolation for high < low, got {err_ohlc:?}"
    );

    // 5. Crossed quote (bid_price > ask_price) fails closed with ProvenanceViolation
    let crossed_quote_json = br#"{
        "schema_version": 1,
        "symbol": "AAPL",
        "quote_ts_ms": 1711980125000,
        "ingest_ts_ms": 1711980125005,
        "bid_price": 170.50,
        "bid_size": 100.0,
        "ask_price": 170.40,
        "ask_size": 100.0
    }"#;
    let err_crossed = decoder
        .decode_quote_json(crossed_quote_json)
        .expect_err("crossed quote must fail closed");
    assert!(
        matches!(err_crossed, DataInfraError::ProvenanceViolation(_)),
        "expected ProvenanceViolation for crossed quote, got {err_crossed:?}"
    );

    // 6. Unrecognized topic fails closed with InvalidIdentifier
    let err_topic = decoder
        .decode_topic_payload("md.sip.trades.raw", b"{}")
        .expect_err("unknown Kafka topic must fail closed");
    assert!(
        matches!(err_topic, DataInfraError::InvalidIdentifier(_)),
        "expected InvalidIdentifier for unknown topic, got {err_topic:?}"
    );
}

#[test]
fn test_same_window_version_deduplication_and_upgrade_tracking() {
    let decoder = CanonicalStreamDecoder::new("NASDAQ", 2, 0, 64);
    let aapl_id = to_instrument_id("AAPL", "NASDAQ").unwrap();
    let bar_ts = 1_711_980_900_i64;

    let v1_wire = CanonicalBarWire {
        schema_version: 1,
        market: "US".to_string(),
        symbol: "AAPL".to_string(),
        timeframe: "15m".to_string(),
        bar_ts,
        open: 170.00,
        high: 171.00,
        low: 169.50,
        close: 170.50,
        volume: 120_000.0,
        vwap: Some(170.30),
        trade_count: Some(850),
        version: 1,
        is_synthetic: false,
        is_correction: false,
        is_suspect_spike: false,
        session: "REGULAR".to_string(),
        origin: "REALTIME".to_string(),
        provider: "alpaca_sip".to_string(),
        source_ts_ms: 1_711_981_800_000,
        ingest_ts_ms: 1_711_981_800_100,
    };

    // Step 1: Initial version = 1 -> emits DataInfraStreamEvent::Bar
    let outcome_v1 = decoder
        .decode_bar_wire(&v1_wire)
        .expect("version 1 should succeed");
    match outcome_v1 {
        StreamDecodeOutcome::Emitted(DataInfraStreamEvent::Bar(bar)) => {
            assert_eq!(bar.close, Price::new(170.50, 2));
            assert_eq!(bar.volume, Quantity::new(120_000.0, 0));
        }
        other => panic!("expected Emitted(Bar) for v1, got {other:?}"),
    }
    assert_eq!(
        decoder
            .version_tracker()
            .current_version(aapl_id, DataInfraTimeframe::M15, bar_ts),
        Some(1)
    );

    // Step 2: Re-delivery of version = 1 -> DuplicateOrStaleVersion
    let dup_v1 = decoder
        .decode_bar_wire(&v1_wire)
        .expect("duplicate v1 should not error");
    assert_eq!(
        dup_v1,
        StreamDecodeOutcome::DuplicateOrStaleVersion {
            symbol: "AAPL".to_string(),
            bar_ts,
            current_version: 1,
            incoming_version: 1,
        }
    );

    // Step 3: Late correction frame version = 2 -> emits DataInfraStreamEvent::UpdatedBar
    let v2_wire = CanonicalBarWire {
        high: 171.40,
        close: 170.85,
        volume: 124_500.0,
        version: 2,
        is_correction: true,
        ingest_ts_ms: 1_711_981_815_000,
        ..v1_wire.clone()
    };
    let outcome_v2 = decoder
        .decode_bar_wire(&v2_wire)
        .expect("version 2 upgrade should succeed");
    match outcome_v2 {
        StreamDecodeOutcome::Emitted(DataInfraStreamEvent::UpdatedBar {
            bar,
            version,
            is_correction,
        }) => {
            assert_eq!(version, 2);
            assert!(is_correction);
            assert_eq!(bar.high, Price::new(171.40, 2));
            assert_eq!(bar.close, Price::new(170.85, 2));
            assert_eq!(bar.volume, Quantity::new(124_500.0, 0));
        }
        other => panic!("expected Emitted(UpdatedBar) for v2, got {other:?}"),
    }
    assert_eq!(
        decoder
            .version_tracker()
            .current_version(aapl_id, DataInfraTimeframe::M15, bar_ts),
        Some(2)
    );

    // Step 4: Out-of-order stale version = 1 and duplicate version = 2 -> DuplicateOrStaleVersion
    let stale_v1 = decoder
        .decode_bar_wire(&v1_wire)
        .expect("stale v1 should be suppressed");
    assert_eq!(
        stale_v1,
        StreamDecodeOutcome::DuplicateOrStaleVersion {
            symbol: "AAPL".to_string(),
            bar_ts,
            current_version: 2,
            incoming_version: 1,
        }
    );

    let dup_v2 = decoder
        .decode_bar_wire(&v2_wire)
        .expect("duplicate v2 should be suppressed");
    assert_eq!(
        dup_v2,
        StreamDecodeOutcome::DuplicateOrStaleVersion {
            symbol: "AAPL".to_string(),
            bar_ts,
            current_version: 2,
            incoming_version: 2,
        }
    );

    // Step 5: Bounded capacity eviction check on BarStreamVersionTracker
    let tracker = BarStreamVersionTracker::new(64);
    for idx in 0..64_i64 {
        let eval = tracker.check_and_record(
            aapl_id,
            DataInfraTimeframe::M1,
            1_711_980_000 + idx * 60,
            1,
        );
        assert_eq!(eval, VersionEvaluation::Initial);
    }
    // Inserting 65th window evicts the oldest timestamp (1_711_980_000)
    assert_eq!(
        tracker.check_and_record(
            aapl_id,
            DataInfraTimeframe::M1,
            1_711_980_000 + 64 * 60,
            1
        ),
        VersionEvaluation::Initial
    );
    assert_eq!(
        tracker.current_version(aapl_id, DataInfraTimeframe::M1, 1_711_980_000),
        None
    );
}

#[test]
fn test_kafka_stream_session_coordinator_isolates_lifecycle_and_fails_closed_when_disconnected() {
    let listener = Arc::new(RecordingLifecycleListener::default());
    let decoder = CanonicalStreamDecoder::new("NASDAQ", 2, 0, 128);
    let coordinator = KafkaStreamSessionCoordinator::new(
        "kafka://192.168.3.100:9092",
        decoder,
        listener.clone(),
    );

    let sample_quote = br#"{
        "schema_version": 1,
        "symbol": "AAPL",
        "quote_ts_ms": 1711980005000,
        "ingest_ts_ms": 1711980005002,
        "bid_price": 170.10,
        "bid_size": 200.0,
        "ask_price": 170.12,
        "ask_size": 400.0
    }"#;

    // 1. Initially disconnected -> process_kafka_message must fail closed
    assert!(!coordinator.is_connected());
    let err_initial = coordinator
        .process_kafka_message(TOPIC_SIP_QUOTES, sample_quote)
        .expect_err("processing while initially disconnected must fail closed");
    assert!(
        matches!(err_initial, DataInfraError::Transport(_)),
        "expected Transport error while disconnected, got {err_initial:?}"
    );
    assert!(listener.snapshot().is_empty());

    // 2. First connection -> notifies on_stream_connected and unlocks message processing
    coordinator.notify_connected();
    assert!(coordinator.is_connected());
    assert_eq!(
        listener.snapshot(),
        vec![LifecycleTransition::Connected {
            endpoint: "kafka://192.168.3.100:9092".to_string(),
        }]
    );

    let emitted = coordinator
        .process_kafka_message(TOPIC_SIP_QUOTES, sample_quote)
        .expect("processing while connected must succeed");
    assert!(matches!(
        emitted,
        StreamDecodeOutcome::Emitted(DataInfraStreamEvent::Quote(_))
    ));

    // 3. Broker disconnect -> notifies on_stream_disconnected and fails closed on messages
    coordinator.notify_disconnected("broker heartbeat timeout");
    assert!(!coordinator.is_connected());
    let err_after_disc = coordinator
        .process_kafka_message(TOPIC_SIP_QUOTES, sample_quote)
        .expect_err("processing after disconnect must fail closed");
    assert!(matches!(err_after_disc, DataInfraError::Transport(_)));

    // 4. Reconnect #1 -> notifies on_stream_reconnected with attempt = 1
    coordinator.notify_connected();
    assert!(coordinator.is_connected());

    // 5. Disconnect #2 & Reconnect #2 -> notifies on_stream_reconnected with attempt = 2
    coordinator.notify_disconnected("leader election");
    assert!(!coordinator.is_connected());
    coordinator.notify_connected();
    assert!(coordinator.is_connected());

    assert_eq!(
        listener.snapshot(),
        vec![
            LifecycleTransition::Connected {
                endpoint: "kafka://192.168.3.100:9092".to_string(),
            },
            LifecycleTransition::Disconnected {
                endpoint: "kafka://192.168.3.100:9092".to_string(),
                reason: "broker heartbeat timeout".to_string(),
            },
            LifecycleTransition::Reconnected {
                endpoint: "kafka://192.168.3.100:9092".to_string(),
                attempt: 1,
            },
            LifecycleTransition::Disconnected {
                endpoint: "kafka://192.168.3.100:9092".to_string(),
                reason: "leader election".to_string(),
            },
            LifecycleTransition::Reconnected {
                endpoint: "kafka://192.168.3.100:9092".to_string(),
                attempt: 2,
            },
        ]
    );
}

#[test]
fn test_kafka_broker_envelope_and_canonical_wire_frame_serialization() {
    assert_eq!(
        DEFAULT_SIP_KAFKA_TOPICS,
        [TOPIC_SIP_BARS_1M, TOPIC_SIP_QUOTES, TOPIC_SIP_STATUSES]
    );

    let bar_wire = CanonicalBarWire {
        schema_version: 1,
        market: "US".to_string(),
        symbol: "AAPL".to_string(),
        timeframe: "1m".to_string(),
        bar_ts: 1_711_980_000,
        open: 170.00,
        high: 171.00,
        low: 169.50,
        close: 170.50,
        volume: 15_000.0,
        vwap: Some(170.25),
        trade_count: Some(120),
        version: 1,
        is_synthetic: false,
        is_correction: false,
        is_suspect_spike: false,
        session: "REGULAR".to_string(),
        origin: "REALTIME".to_string(),
        provider: "alpaca_sip".to_string(),
        source_ts_ms: 1_711_980_060_000,
        ingest_ts_ms: 1_711_980_060_050,
    };

    // 1. Strongly typed KafkaBrokerEnvelope with Structured, JsonString (`value`), and JsonBytes (`data`)
    let env_structured = KafkaBrokerEnvelope {
        topic: TOPIC_SIP_BARS_1M.to_string(),
        payload: KafkaEnvelopePayload::Structured(CanonicalWireFrame::Bar(bar_wire.clone())),
    };
    let raw_env_struct = serde_json::to_vec(&env_structured).unwrap();
    let parsed_env_struct: KafkaBrokerEnvelope =
        serde_json::from_slice(&raw_env_struct).expect("parse structured envelope");
    assert_eq!(parsed_env_struct.topic, TOPIC_SIP_BARS_1M);
    let payload_bytes = parsed_env_struct
        .payload
        .into_payload_bytes()
        .expect("serialize structured payload");
    let decoded_struct: CanonicalBarWire = serde_json::from_slice(&payload_bytes).unwrap();
    assert_eq!(decoded_struct, bar_wire);

    let raw_env_value = serde_json::json!({
        "topic": TOPIC_SIP_QUOTES,
        "value": r#"{"symbol":"AAPL","quote_ts_ms":1711980005000,"bid_price":170.1,"bid_size":100.0,"ask_price":170.2,"ask_size":200.0}"#
    })
    .to_string();
    let parsed_env_val: KafkaBrokerEnvelope =
        serde_json::from_str(&raw_env_value).expect("parse value-aliased envelope");
    assert_eq!(parsed_env_val.topic, TOPIC_SIP_QUOTES);

    let status_bytes =
        br#"{"symbol":"AAPL","status_ts_ms":1711980010000,"state":"TRADING"}"#.to_vec();
    let raw_env_data = serde_json::json!({
        "topic": TOPIC_SIP_STATUSES,
        "data": status_bytes
    })
    .to_string();
    let parsed_env_data: KafkaBrokerEnvelope =
        serde_json::from_str(&raw_env_data).expect("parse data-aliased byte envelope");
    assert_eq!(parsed_env_data.topic, TOPIC_SIP_STATUSES);
    assert_eq!(
        parsed_env_data.payload.into_payload_bytes().unwrap(),
        status_bytes
    );
}

#[tokio::test]
async fn test_tcp_kafka_payload_source_ephemeral_broker_streaming_failover_and_close() {
    let dead_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = dead_listener.local_addr().unwrap().to_string();
    drop(dead_listener);

    // 1. Unreachable broker must fail closed with Err(DataInfraError::Transport) on first next_frame()
    let dead_cfg = DataInfraConfig::new_checked(
        "http://127.0.0.1:8123",
        "market_data",
        300,
        0,
        vec![dead_addr.clone()],
        "nautilus-datainfra-dead-test",
        "http://127.0.0.1:8012",
        "ALPACA",
    )
    .expect("valid DataInfraConfig for unreachable broker");
    let dead_source =
        TcpKafkaPayloadSource::from_config(&dead_cfg).expect("create dead TcpKafkaPayloadSource");
    let dead_err = dead_source
        .next_frame()
        .await
        .expect_err("unreachable broker must fail closed on initial next_frame");
    assert!(matches!(dead_err, DataInfraError::Transport(_)));

    let live_addr = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let bar_topic = format!("md.sip.bars.1m.test.{run_id}");
    let quote_topic = format!("md.sip.quotes.test.{run_id}");
    let status_topic = format!("md.sip.statuses.test.{run_id}");

    // 2. Non-existent topic must fail closed with Err(DataInfraError::Transport)
    let cfg = DataInfraConfig::new_checked(
        "http://127.0.0.1:8123",
        "market_data",
        2_000,
        0,
        vec![dead_addr.clone(), live_addr.clone()],
        "nautilus-datainfra-tcp-test",
        "http://127.0.0.1:8012",
        "ALPACA",
    )
    .expect("valid DataInfraConfig");

    let missing_topic_source =
        TcpKafkaPayloadSource::with_topics(&cfg, vec![format!("missing.topic.{run_id}")])
            .expect("create TcpKafkaPayloadSource with missing topic");
    let missing_err = missing_topic_source
        .next_frame()
        .await
        .expect_err("missing topic on broker must fail closed");
    assert!(matches!(missing_err, DataInfraError::Transport(_)));

    // 3. Create topics and verify OffsetAt::Latest initialization + offset persistence across reconnects
    let producer_client = ClientBuilder::new(vec![live_addr.clone()])
        .build()
        .await
        .expect("connect to local Kafka broker on 127.0.0.1:9092");
    if let Ok(ctrl) = producer_client.controller_client() {
        for t in [&bar_topic, &quote_topic, &status_topic] {
            let _ = ctrl.create_topic(t.as_str(), 1, 1, 5_000).await;
        }
    }

    let source = TcpKafkaPayloadSource::with_topics(
        &cfg,
        vec![bar_topic.clone(), quote_topic.clone(), status_topic.clone()],
    )
    .expect("create TcpKafkaPayloadSource");
    assert_eq!(source.group_id(), "nautilus-datainfra-tcp-test");
    assert_eq!(source.brokers(), &[dead_addr, live_addr]);

    // First next_frame() establishes the session and records OffsetAt::Latest in `offsets`
    let initial_poll = source
        .next_frame()
        .await
        .expect("initial next_frame on empty topics should succeed");
    assert!(initial_poll.is_none());

    // Produce bar record (as a {"topic", "payload"} envelope) while connected
    let ts = rskafka::chrono::Utc::now();
    let bar_pc = producer_client
        .partition_client(bar_topic.clone(), 0, UnknownTopicHandling::Retry)
        .await
        .expect("bar partition client");
    bar_pc
        .produce(
            vec![Record {
                key: Some(b"AAPL".to_vec()),
                value: Some(
                    br#"{"topic":"md.sip.bars.1m","payload":{"schema_version":1,"symbol":"AAPL","timeframe":"1m","bar_ts":1777896600,"open":185.0,"high":186.0,"low":184.5,"close":185.5,"volume":42000.0,"version":1,"is_synthetic":false,"is_suspect_spike":false}}"#
                        .to_vec(),
                ),
                headers: std::collections::BTreeMap::new(),
                timestamp: ts,
            }],
            Compression::NoCompression,
        )
        .await
        .expect("produce bar record");

    let first_frame = source
        .next_frame()
        .await
        .expect("poll bar frame")
        .expect("bar frame should be present");
    assert_eq!(first_frame.topic, TOPIC_SIP_BARS_1M);

    // Simulate disconnect via close(), then produce quote & status records (direct JSON without outer envelope)
    // to verify reconnect resumes from persisted offsets and preserves authoritative `cursor.topic`
    source.close().await.expect("close before reconnect");

    for (topic, json_payload) in [
        (
            &quote_topic,
            r#"{"symbol":"AAPL","quote_ts_ms":1777896605000,"bid_price":185.45,"bid_size":200.0,"ask_price":185.55,"ask_size":300.0}"#,
        ),
        (
            &status_topic,
            r#"{"symbol":"AAPL","status_ts_ms":1777896610000,"state":"TRADING"}"#,
        ),
    ] {
        let pc = producer_client
            .partition_client(topic.clone(), 0, UnknownTopicHandling::Retry)
            .await
            .expect("partition client");
        pc.produce(
            vec![Record {
                key: Some(b"AAPL".to_vec()),
                value: Some(json_payload.as_bytes().to_vec()),
                headers: std::collections::BTreeMap::new(),
                timestamp: ts,
            }],
            Compression::NoCompression,
        )
        .await
        .expect("produce record");
    }

    let mut received: Vec<KafkaPayloadFrame> = Vec::new();
    while received.len() < 2 {
        if let Some(frame) = source
            .next_frame()
            .await
            .expect("next_frame after reconnect should resume from saved offsets")
        {
            received.push(frame);
        }
    }

    assert_eq!(received[0].topic, quote_topic);
    assert_eq!(received[1].topic, status_topic);

    // 4. Malformed non-JSON record on broker partition must fail closed
    bar_pc
        .produce(
            vec![Record {
                key: Some(b"AAPL".to_vec()),
                value: Some(b"MALFORMED_NOT_JSON".to_vec()),
                headers: std::collections::BTreeMap::new(),
                timestamp: ts,
            }],
            Compression::NoCompression,
        )
        .await
        .expect("produce malformed record");

    let bad_err = source
        .next_frame()
        .await
        .expect_err("malformed non-JSON Kafka record must fail closed");
    assert!(matches!(bad_err, DataInfraError::Transport(_)));

    source.close().await.expect("close TcpKafkaPayloadSource");
}

#[tokio::test]
async fn test_kafka_partition_polling_priority_batch_cap_and_poison_pill_offset_advance() {
    let live_addr = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "127.0.0.1:9092".to_string());

    // Use the canonical SIP topics so `topic_poll_priority` assigns:
    // - `md.sip.statuses` -> 0
    // - `md.sip.bars.1m`  -> 1
    // - `md.sip.quotes`   -> 2
    let quote_topic = TOPIC_SIP_QUOTES.to_string();
    let bar_topic = TOPIC_SIP_BARS_1M.to_string();
    let status_topic = TOPIC_SIP_STATUSES.to_string();

    let producer_client = ClientBuilder::new(vec![live_addr.clone()])
        .build()
        .await
        .expect("connect to local Kafka broker on 127.0.0.1:9092");
    if let Ok(ctrl) = producer_client.controller_client() {
        for t in [&quote_topic, &bar_topic, &status_topic] {
            let _ = ctrl.create_topic(t.as_str(), 1, 1, 5_000).await;
        }
    }

    let cfg = DataInfraConfig::new_checked(
        "http://127.0.0.1:8123",
        "market_data",
        2_000,
        0,
        vec![live_addr],
        "nautilus-datainfra-fairness-test",
        "http://127.0.0.1:8012",
        "ALPACA",
    )
    .expect("valid DataInfraConfig");

    // Deliberately pass `quote_topic` first in the configured topics list to verify `ensure_session`
    // sorts cursors by `topic_poll_priority` (`statuses` -> `bars` -> `quotes`).
    let source = TcpKafkaPayloadSource::with_topics(
        &cfg,
        vec![quote_topic.clone(), bar_topic.clone(), status_topic.clone()],
    )
    .expect("create TcpKafkaPayloadSource");

    // Establish initial session (`OffsetAt::Latest`)
    assert!(source.next_frame().await.expect("initial poll").is_none());

    let quote_pc = producer_client
        .partition_client(quote_topic.clone(), 0, UnknownTopicHandling::Retry)
        .await
        .expect("quote partition client");
    let bar_pc = producer_client
        .partition_client(bar_topic.clone(), 0, UnknownTopicHandling::Retry)
        .await
        .expect("bar partition client");
    let status_pc = producer_client
        .partition_client(status_topic.clone(), 0, UnknownTopicHandling::Retry)
        .await
        .expect("status partition client");

    let ts = rskafka::chrono::Utc::now();

    // 1. Produce 80 quotes (> MAX_RECORDS_PER_PARTITION_POLL = 64), 1 bar, and 1 status simultaneously.
    //    Because `statuses` (priority 0) and `bars` (priority 1) are polled before `quotes` (priority 2),
    //    the first two frames returned by `next_frame()` MUST be the status frame and the bar frame,
    //    followed by at most 64 quote frames from the first poll cycle!
    let mut quote_records = Vec::with_capacity(80);
    for idx in 0..80_u64 {
        let q_json = format!(
            r#"{{"schema_version":1,"symbol":"AAPL","quote_ts_ms":{},"bid_price":185.0,"bid_size":100.0,"ask_price":185.1,"ask_size":100.0}}"#,
            1_777_896_600_000_u64 + idx
        );
        quote_records.push(Record {
            key: Some(b"AAPL".to_vec()),
            value: Some(q_json.into_bytes()),
            headers: std::collections::BTreeMap::new(),
            timestamp: ts,
        });
    }
    quote_pc
        .produce(quote_records, Compression::NoCompression)
        .await
        .expect("produce 80 quotes");

    bar_pc
        .produce(
            vec![Record {
                key: Some(b"AAPL".to_vec()),
                value: Some(
                    br#"{"schema_version":1,"symbol":"AAPL","timeframe":"1m","bar_ts":1777896600,"open":185.0,"high":186.0,"low":184.5,"close":185.5,"volume":42000.0,"version":1,"is_synthetic":false,"is_suspect_spike":false}"#
                        .to_vec(),
                ),
                headers: std::collections::BTreeMap::new(),
                timestamp: ts,
            }],
            Compression::NoCompression,
        )
        .await
        .expect("produce 1 bar");

    status_pc
        .produce(
            vec![Record {
                key: Some(b"AAPL".to_vec()),
                value: Some(
                    br#"{"schema_version":1,"symbol":"AAPL","status_ts_ms":1777896600000,"state":"HALTED","reason":"LULD_PAUSE"}"#
                        .to_vec(),
                ),
                headers: std::collections::BTreeMap::new(),
                timestamp: ts,
            }],
            Compression::NoCompression,
        )
        .await
        .expect("produce 1 status");

    let first = source
        .next_frame()
        .await
        .expect("poll #1")
        .expect("frame #1");
    let second = source
        .next_frame()
        .await
        .expect("poll #2")
        .expect("frame #2");
    assert_eq!(
        first.topic, status_topic,
        "md.sip.statuses must be polled with highest priority (before bars and quotes)"
    );
    assert_eq!(
        second.topic, bar_topic,
        "md.sip.bars.1m must be polled before md.sip.quotes"
    );

    // Drain the 64 quotes queued from the first poll batch (`MAX_RECORDS_PER_PARTITION_POLL = 64`),
    // then produce a new status event while 16 quotes remain unpolled on the broker partition.
    // On the next poll cycle (65th call after the 2 status/bar frames), the new status event must be
    // prioritized ahead of the remaining 16 quotes!
    for _ in 0..64 {
        let q_frame = source
            .next_frame()
            .await
            .expect("poll queued quote")
            .expect("quote frame");
        assert_eq!(q_frame.topic, quote_topic);
    }

    status_pc
        .produce(
            vec![Record {
                key: Some(b"AAPL".to_vec()),
                value: Some(
                    br#"{"schema_version":1,"symbol":"AAPL","status_ts_ms":1777896660000,"state":"TRADING"}"#
                        .to_vec(),
                ),
                headers: std::collections::BTreeMap::new(),
                timestamp: ts,
            }],
            Compression::NoCompression,
        )
        .await
        .expect("produce 2nd status while 16 quotes remain on broker");

    let next_cycle_first = source
        .next_frame()
        .await
        .expect("poll next cycle")
        .expect("frame after 64-quote cap");
    assert_eq!(
        next_cycle_first.topic, status_topic,
        "per-partition batch cap (64) must prevent quote flood from starving subsequent status events"
    );

    // Drain the remaining 16 quotes
    for _ in 0..16 {
        let q_frame = source
            .next_frame()
            .await
            .expect("poll remaining quote")
            .expect("remaining quote frame");
        assert_eq!(q_frame.topic, quote_topic);
    }

    // 2. Poison-pill offset advance: produce a corrupt non-JSON record followed by a valid JSON record
    //    on `bar_topic`. The first `next_frame()` must return `Err(DataInfraError::Transport)` AND
    //    advance `cursor.next_offset` so the very next `next_frame()` returns `Ok(Some(valid_frame))`
    //    rather than getting stuck on the poison pill forever.
    bar_pc
        .produce(
            vec![
                Record {
                    key: Some(b"AAPL".to_vec()),
                    value: Some(b"POISON_PILL_CORRUPT_PAYLOAD".to_vec()),
                    headers: std::collections::BTreeMap::new(),
                    timestamp: ts,
                },
                Record {
                    key: Some(b"AAPL".to_vec()),
                    value: Some(
                        br#"{"schema_version":1,"symbol":"AAPL","timeframe":"1m","bar_ts":1777896660,"open":185.5,"high":186.5,"low":185.0,"close":186.0,"volume":31000.0,"version":1,"is_synthetic":false,"is_suspect_spike":false}"#
                            .to_vec(),
                    ),
                    headers: std::collections::BTreeMap::new(),
                    timestamp: ts,
                },
            ],
            Compression::NoCompression,
        )
        .await
        .expect("produce poison pill + valid bar");

    let poison_err = source
        .next_frame()
        .await
        .expect_err("corrupt record must return Err");
    assert!(matches!(poison_err, DataInfraError::Transport(_)));

    let recovered_frame = source
        .next_frame()
        .await
        .expect("next_frame after poison pill must not re-read corrupt offset")
        .expect("valid bar record after poison pill must be returned");
    assert_eq!(recovered_frame.topic, bar_topic);

    source.close().await.expect("close TcpKafkaPayloadSource");
}
