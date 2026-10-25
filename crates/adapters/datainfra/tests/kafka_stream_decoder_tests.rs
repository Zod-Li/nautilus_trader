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
    CanonicalTradingStatusWire, DataInfraError, DataInfraStreamEvent, DataInfraTimeframe,
    KafkaStreamSessionCoordinator, StreamDecodeOutcome, StreamLifecycleListener, TOPIC_SIP_BARS_1M,
    TOPIC_SIP_QUOTES, TOPIC_SIP_STATUSES, VersionEvaluation, to_instrument_id,
};
use nautilus_model::{
    enums::MarketStatusAction,
    types::{Price, Quantity},
};
use parking_lot::Mutex;
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
            assert_eq!(bar.ts_event, UnixNanos::from(1_711_980_000_000_000_000u64));
            assert_eq!(bar.ts_init, UnixNanos::from(1_711_980_060_125_000_000u64));
        }
        other => panic!("expected Emitted(Bar), got {other:?}"),
    }

    // 2. Valid L1 SIP quote frame on md.sip.quotes
    let quote_json = br#"{
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
        "symbol": "NVDA",
        "status_ts_ms": 1711980010000,
        "ingest_ts_ms": 1711980010005,
        "state": "LULD_PAUSE",
        "reason_code": "LULD_T5",
        "provider": "alpaca_sip"
    }"#;

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
