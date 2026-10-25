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

//! Real-time Kafka market stream decoding seam (`md.sip.bars.1m`, `md.sip.quotes`, `md.sip.statuses`).
//!
//! Strictly separates:
//! 1. Strongly typed wire frames (`CanonicalBarWire`, `CanonicalQuoteWire`, `CanonicalTradingStatusWire`)
//! 2. Pure business market-data domain events (`DataInfraStreamEvent`)
//! 3. Connection lifecycle observer notifications (`StreamLifecycleListener`)
//! 4. Single-responsibility collaborators (`BarStreamVersionTracker`, `CanonicalStreamDecoder`,
//!    `KafkaStreamSessionCoordinator`).

use std::sync::Arc;

use ahash::AHashMap;
use nautilus_model::{
    data::{Bar, InstrumentStatus, QuoteTick},
    identifiers::InstrumentId,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use ustr::Ustr;

use crate::{
    arrow_decoder::{
        f64_to_price, f64_to_quantity, map_trading_status_state, normalize_epoch_to_unix_nanos,
    },
    error::{DataInfraError, DataInfraResult},
    mapping::{DataInfraTimeframe, parse_datainfra_bar_type, to_instrument_id},
};

/// Canonical Kafka topic for 1-minute SIP bars (`sip-feeder`).
pub const TOPIC_SIP_BARS_1M: &str = "md.sip.bars.1m";
/// Canonical Kafka topic prefix for SIP bars (`sip-feeder`).
pub const TOPIC_SIP_BARS_PREFIX: &str = "md.sip.bars.";
/// Canonical Kafka topic for L1 SIP quotes (`sip-feeder`).
pub const TOPIC_SIP_QUOTES: &str = "md.sip.quotes";
/// Canonical Kafka topic for SIP trading statuses / halts (`sip-feeder`).
pub const TOPIC_SIP_STATUSES: &str = "md.sip.statuses";

/// Strongly typed wire representation of `sip-feeder` `CanonicalBar` JSON frames.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalBarWire {
    /// Ticker symbol (e.g. `"AAPL"`).
    pub symbol: String,
    /// Timeframe string (e.g. `"1m"`, `"15m"`).
    pub timeframe: String,
    /// Bar Open UTC timestamp in seconds (`bar_ts`).
    pub bar_ts: i64,
    /// Bar open price.
    pub open: f64,
    /// Bar high price.
    pub high: f64,
    /// Bar low price.
    pub low: f64,
    /// Bar close price.
    pub close: f64,
    /// Bar volume.
    pub volume: f64,
    /// Optional volume-weighted average price.
    #[serde(default)]
    pub vwap: Option<f64>,
    /// Optional trade count.
    #[serde(default)]
    pub trade_count: Option<u64>,
    /// Monotonically increasing bar version within the same `(symbol, timeframe, bar_ts)` window.
    #[serde(default = "default_bar_version")]
    pub version: u32,
    /// True if this bar is a `+3s` watermark zero-volume synthetic placeholder.
    #[serde(default)]
    pub is_synthetic: bool,
    /// True if this bar is a late correction frame.
    #[serde(default)]
    pub is_correction: bool,
    /// True if this bar was flagged as a suspect price spike.
    #[serde(default)]
    pub is_suspect_spike: bool,
    /// Market session (`"PRE_MARKET"`, `"REGULAR"`, `"POST_MARKET"`).
    #[serde(default)]
    pub session: String,
    /// Upstream data provider identifier.
    #[serde(default)]
    pub provider: String,
    /// Source event timestamp in milliseconds (`source_ts_ms`).
    #[serde(default)]
    pub source_ts_ms: i64,
    /// Ingestion timestamp in milliseconds (`ingest_ts_ms`).
    #[serde(default)]
    pub ingest_ts_ms: i64,
}

const fn default_bar_version() -> u32 {
    1
}

impl CanonicalBarWire {
    /// Converts this strongly typed wire bar into a validated Nautilus [`Bar`].
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if the symbol/timeframe is invalid, timestamps are non-positive,
    /// or OHLCV values fail fixed-point conversion or OHLC invariants.
    pub fn to_nautilus_bar(
        &self,
        venue: &str,
        price_precision: u8,
        size_precision: u8,
    ) -> DataInfraResult<Bar> {
        let bar_type = parse_datainfra_bar_type(&self.symbol, venue, &self.timeframe)?;
        let ts_event = normalize_epoch_to_unix_nanos(self.bar_ts, "bar_ts")?;
        let ts_init = if self.ingest_ts_ms > 0 {
            let init_ns = normalize_epoch_to_unix_nanos(self.ingest_ts_ms, "ingest_ts_ms")?;
            if init_ns < ts_event {
                ts_event
            } else {
                init_ns
            }
        } else {
            ts_event
        };

        let open = f64_to_price(self.open, price_precision, "open")?;
        let high = f64_to_price(self.high, price_precision, "high")?;
        let low = f64_to_price(self.low, price_precision, "low")?;
        let close = f64_to_price(self.close, price_precision, "close")?;
        let volume = f64_to_quantity(self.volume, size_precision, "volume")?;

        Bar::new_checked(
            bar_type, open, high, low, close, volume, ts_event, ts_init,
        )
        .map_err(|e| {
            DataInfraError::ProvenanceViolation(format!(
                "CanonicalBarWire OHLC sanity violation for {} at bar_ts={}: {e}",
                self.symbol, self.bar_ts
            ))
        })
    }
}

/// Strongly typed wire representation of `sip-feeder` `CanonicalQuote` JSON frames.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalQuoteWire {
    /// Ticker symbol (e.g. `"AAPL"`).
    pub symbol: String,
    /// Quote event timestamp in milliseconds (`quote_ts_ms`).
    pub quote_ts_ms: i64,
    /// Ingestion timestamp in milliseconds (`ingest_ts_ms`).
    #[serde(default)]
    pub ingest_ts_ms: i64,
    /// Top-of-book bid price.
    pub bid_price: f64,
    /// Top-of-book bid size.
    pub bid_size: f64,
    /// Top-of-book ask price.
    pub ask_price: f64,
    /// Top-of-book ask size.
    pub ask_size: f64,
    /// Mid-price (`0.5 * (bid_price + ask_price)`).
    #[serde(default)]
    pub mid_price: f64,
    /// Bid-ask spread in basis points.
    #[serde(default)]
    pub spread_bps: f64,
    /// Upstream provider identifier.
    #[serde(default)]
    pub provider: String,
}

impl CanonicalQuoteWire {
    /// Converts this strongly typed wire quote into a validated Nautilus [`QuoteTick`].
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if the symbol is invalid, timestamps are non-positive,
    /// `ask_price < bid_price`, or prices/sizes are invalid.
    pub fn to_nautilus_quote(
        &self,
        venue: &str,
        price_precision: u8,
        size_precision: u8,
    ) -> DataInfraResult<QuoteTick> {
        let instrument_id = to_instrument_id(&self.symbol, venue)?;
        let ts_event = normalize_epoch_to_unix_nanos(self.quote_ts_ms, "quote_ts_ms")?;
        let ts_init = if self.ingest_ts_ms > 0 {
            let init_ns = normalize_epoch_to_unix_nanos(self.ingest_ts_ms, "ingest_ts_ms")?;
            if init_ns < ts_event {
                ts_event
            } else {
                init_ns
            }
        } else {
            ts_event
        };

        if self.ask_price < self.bid_price {
            return Err(DataInfraError::ProvenanceViolation(format!(
                "CanonicalQuoteWire crossed quote for {} at quote_ts_ms={}: bid={} > ask={}",
                self.symbol, self.quote_ts_ms, self.bid_price, self.ask_price
            )));
        }

        let bid_price = f64_to_price(self.bid_price, price_precision, "bid_price")?;
        let ask_price = f64_to_price(self.ask_price, price_precision, "ask_price")?;
        let bid_size = f64_to_quantity(self.bid_size, size_precision, "bid_size")?;
        let ask_size = f64_to_quantity(self.ask_size, size_precision, "ask_size")?;

        QuoteTick::new_checked(
            instrument_id,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            ts_event,
            ts_init,
        )
        .map_err(|e| {
            DataInfraError::ProvenanceViolation(format!(
                "CanonicalQuoteWire invalid QuoteTick for {}: {e}",
                self.symbol
            ))
        })
    }
}

/// Strongly typed wire representation of `sip-feeder` `CanonicalTradingStatus` JSON frames.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalTradingStatusWire {
    /// Ticker symbol (e.g. `"AAPL"`).
    pub symbol: String,
    /// Status event timestamp in milliseconds (`status_ts_ms`).
    pub status_ts_ms: i64,
    /// Ingestion timestamp in milliseconds (`ingest_ts_ms`).
    #[serde(default)]
    pub ingest_ts_ms: i64,
    /// Trading halt state string (`"TRADING"`, `"HALTED"`, `"LULD_PAUSE"`, `"SHORT_SALE_RESTRICTED"`).
    pub state: String,
    /// Optional exchange halt reason code.
    #[serde(default)]
    pub reason_code: Option<String>,
    /// Upstream provider identifier.
    #[serde(default)]
    pub provider: String,
}

impl CanonicalTradingStatusWire {
    /// Converts this strongly typed wire status frame into a Nautilus [`InstrumentStatus`].
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if the symbol, timestamp, or `state` code is invalid.
    pub fn to_nautilus_status(&self, venue: &str) -> DataInfraResult<InstrumentStatus> {
        let instrument_id = to_instrument_id(&self.symbol, venue)?;
        let ts_event = normalize_epoch_to_unix_nanos(self.status_ts_ms, "status_ts_ms")?;
        let ts_init = if self.ingest_ts_ms > 0 {
            let init_ns = normalize_epoch_to_unix_nanos(self.ingest_ts_ms, "ingest_ts_ms")?;
            if init_ns < ts_event {
                ts_event
            } else {
                init_ns
            }
        } else {
            ts_event
        };

        let (action, is_trading, is_ssr) = map_trading_status_state(&self.state)?;
        let reason = self
            .reason_code
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(Ustr::from);

        Ok(InstrumentStatus::new(
            instrument_id,
            action,
            ts_event,
            ts_init,
            reason,
            Some(Ustr::from(self.state.trim())),
            Some(is_trading),
            Some(is_trading),
            Some(is_ssr),
        ))
    }
}

/// Pure business market-data domain events emitted by the `DataInfra` real-time stream decoder.
///
/// Strictly excludes handshake, heartbeat, subscription, or disconnect/reconnect control states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataInfraStreamEvent {
    /// First validated bar for a `(bar_type, bar_ts)` window (`version >= 1`).
    Bar(Bar),
    /// Version-upgraded bar (`version > previous_version`) for an already emitted `(bar_type, bar_ts)` window.
    UpdatedBar {
        /// Updated Nautilus [`Bar`].
        bar: Bar,
        /// New bar version (`> previous_version`).
        version: u32,
        /// Whether the upstream frame marked this update as an exchange correction.
        is_correction: bool,
    },
    /// Validated L1 quote tick.
    Quote(QuoteTick),
    /// Validated instrument trading status / halt event.
    TradingStatus(InstrumentStatus),
}

/// Outcome of decoding and filtering a single Kafka stream message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamDecodeOutcome {
    /// Frame passed all quality and version guards and produced a pure domain event.
    Emitted(DataInfraStreamEvent),
    /// Frame was dropped because `is_synthetic == true` (`+3s` zero-volume placeholder).
    FilteredSynthetic {
        /// Symbol of the dropped synthetic bar.
        symbol: String,
        /// Bar open timestamp (`bar_ts`) in seconds.
        bar_ts: i64,
    },
    /// Frame was dropped because `is_suspect_spike == true` (abnormal price spike).
    FilteredSuspectSpike {
        /// Symbol of the dropped spike bar.
        symbol: String,
        /// Bar open timestamp (`bar_ts`) in seconds.
        bar_ts: i64,
    },
    /// Frame was dropped because its `version` was `<=` the already-processed version for that window.
    DuplicateOrStaleVersion {
        /// Symbol of the bar.
        symbol: String,
        /// Bar open timestamp (`bar_ts`) in seconds.
        bar_ts: i64,
        /// Currently recorded version in the window tracker.
        current_version: u32,
        /// Incoming frame's version (`<= current_version`).
        incoming_version: u32,
    },
}

/// Physically isolated observer interface for stream connection lifecycle transitions.
///
/// Keeps connection state notifications (`connected`, `disconnected`, `reconnected`) completely
/// separate from [`DataInfraStreamEvent`].
pub trait StreamLifecycleListener: Send + Sync + std::fmt::Debug {
    /// Invoked when the Kafka/stream consumer establishes an active broker connection.
    fn on_stream_connected(&self, endpoint: &str);

    /// Invoked when the Kafka/stream consumer loses connectivity or encounters a transport fault.
    fn on_stream_disconnected(&self, endpoint: &str, reason: &str);

    /// Invoked when the Kafka/stream consumer successfully recovers connectivity after a disconnect.
    fn on_stream_reconnected(&self, endpoint: &str, attempt: u32);
}

/// Single-responsibility collaborator tracking the highest processed `version` per
/// `(InstrumentId, DataInfraTimeframe, bar_ts)` window.
#[derive(Debug, Default)]
pub struct BarStreamVersionTracker {
    versions: Mutex<AHashMap<(InstrumentId, DataInfraTimeframe, i64), u32>>,
    max_entries_per_symbol: usize,
}

impl BarStreamVersionTracker {
    /// Creates a new [`BarStreamVersionTracker`] retaining up to `max_entries` window keys.
    #[must_use]
    pub fn new(max_entries: usize) -> Self {
        Self {
            versions: Mutex::new(AHashMap::new()),
            max_entries_per_symbol: max_entries.max(64),
        }
    }

    /// Evaluates whether an incoming `(instrument_id, timeframe, bar_ts, version)` is new,
    /// a version upgrade, or a duplicate/stale frame.
    pub fn check_and_record(
        &self,
        instrument_id: InstrumentId,
        timeframe: DataInfraTimeframe,
        bar_ts: i64,
        incoming_version: u32,
    ) -> VersionEvaluation {
        let mut guard = self.versions.lock();
        let key = (instrument_id, timeframe, bar_ts);
        match guard.get(&key).copied() {
            None => {
                if guard.len() >= self.max_entries_per_symbol {
                    if let Some(oldest_key) = guard
                        .keys()
                        .min_by_key(|(_, _, ts)| *ts)
                        .copied()
                    {
                        guard.remove(&oldest_key);
                    }
                }
                guard.insert(key, incoming_version);
                VersionEvaluation::Initial
            }
            Some(current_version) if incoming_version > current_version => {
                guard.insert(key, incoming_version);
                VersionEvaluation::Upgraded {
                    previous_version: current_version,
                    new_version: incoming_version,
                }
            }
            Some(current_version) => VersionEvaluation::DuplicateOrStale {
                current_version,
                incoming_version,
            },
        }
    }

    /// Returns the currently recorded version for `(instrument_id, timeframe, bar_ts)`, if any.
    #[must_use]
    pub fn current_version(
        &self,
        instrument_id: InstrumentId,
        timeframe: DataInfraTimeframe,
        bar_ts: i64,
    ) -> Option<u32> {
        self.versions
            .lock()
            .get(&(instrument_id, timeframe, bar_ts))
            .copied()
    }
}

/// Result of comparing an incoming bar version against [`BarStreamVersionTracker`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionEvaluation {
    /// First time seeing this `(instrument_id, timeframe, bar_ts)` window.
    Initial,
    /// Incoming version is strictly greater than `previous_version`.
    Upgraded {
        /// Previously seen version.
        previous_version: u32,
        /// Newly recorded version.
        new_version: u32,
    },
    /// Incoming version is `<= current_version` and must be ignored.
    DuplicateOrStale {
        /// Currently recorded version.
        current_version: u32,
        /// Incoming duplicate or stale version.
        incoming_version: u32,
    },
}

/// Real-time Kafka payload decoder with built-in synthetic/spike quality guard and version deduplication.
#[derive(Debug)]
pub struct CanonicalStreamDecoder {
    venue: String,
    price_precision: u8,
    size_precision: u8,
    version_tracker: BarStreamVersionTracker,
}

impl CanonicalStreamDecoder {
    /// Creates a new [`CanonicalStreamDecoder`].
    #[must_use]
    pub fn new(
        venue: impl Into<String>,
        price_precision: u8,
        size_precision: u8,
        max_tracked_windows: usize,
    ) -> Self {
        Self {
            venue: venue.into(),
            price_precision,
            size_precision,
            version_tracker: BarStreamVersionTracker::new(max_tracked_windows),
        }
    }

    /// Returns a reference to the underlying [`BarStreamVersionTracker`].
    #[must_use]
    pub const fn version_tracker(&self) -> &BarStreamVersionTracker {
        &self.version_tracker
    }

    /// Decodes a `CanonicalBarWire` JSON payload from `md.sip.bars.*` with quality and version guards.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if JSON deserialization fails or a non-filtered bar violates OHLCV invariants.
    pub fn decode_bar_json(&self, payload: &[u8]) -> DataInfraResult<StreamDecodeOutcome> {
        let wire: CanonicalBarWire = serde_json::from_slice(payload).map_err(|e| {
            DataInfraError::Transport(format!("failed to deserialize CanonicalBarWire JSON: {e}"))
        })?;
        self.decode_bar_wire(&wire)
    }

    /// Evaluates a [`CanonicalBarWire`] against quality guards (`is_synthetic`, `is_suspect_spike`)
    /// and version tracking, converting valid frames into [`DataInfraStreamEvent::Bar`] or
    /// [`DataInfraStreamEvent::UpdatedBar`].
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if a non-filtered bar has invalid symbol/timeframe or violates OHLCV sanity.
    pub fn decode_bar_wire(&self, wire: &CanonicalBarWire) -> DataInfraResult<StreamDecodeOutcome> {
        if wire.is_synthetic {
            return Ok(StreamDecodeOutcome::FilteredSynthetic {
                symbol: wire.symbol.trim().to_uppercase(),
                bar_ts: wire.bar_ts,
            });
        }
        if wire.is_suspect_spike {
            return Ok(StreamDecodeOutcome::FilteredSuspectSpike {
                symbol: wire.symbol.trim().to_uppercase(),
                bar_ts: wire.bar_ts,
            });
        }
        if wire.version == 0 {
            return Err(DataInfraError::ProvenanceViolation(format!(
                "CanonicalBarWire version must be >= 1 for {} at bar_ts={}",
                wire.symbol, wire.bar_ts
            )));
        }

        let bar = wire.to_nautilus_bar(&self.venue, self.price_precision, self.size_precision)?;
        let tf: DataInfraTimeframe = wire.timeframe.parse()?;
        let instrument_id = bar.bar_type.instrument_id();

        match self
            .version_tracker
            .check_and_record(instrument_id, tf, wire.bar_ts, wire.version)
        {
            VersionEvaluation::Initial => {
                Ok(StreamDecodeOutcome::Emitted(DataInfraStreamEvent::Bar(bar)))
            }
            VersionEvaluation::Upgraded { new_version, .. } => Ok(StreamDecodeOutcome::Emitted(
                DataInfraStreamEvent::UpdatedBar {
                    bar,
                    version: new_version,
                    is_correction: wire.is_correction,
                },
            )),
            VersionEvaluation::DuplicateOrStale {
                current_version,
                incoming_version,
            } => Ok(StreamDecodeOutcome::DuplicateOrStaleVersion {
                symbol: wire.symbol.trim().to_uppercase(),
                bar_ts: wire.bar_ts,
                current_version,
                incoming_version,
            }),
        }
    }

    /// Decodes a `CanonicalQuoteWire` JSON payload from `md.sip.quotes`.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if JSON deserialization or quote validation fails.
    pub fn decode_quote_json(&self, payload: &[u8]) -> DataInfraResult<StreamDecodeOutcome> {
        let wire: CanonicalQuoteWire = serde_json::from_slice(payload).map_err(|e| {
            DataInfraError::Transport(format!("failed to deserialize CanonicalQuoteWire JSON: {e}"))
        })?;
        let quote =
            wire.to_nautilus_quote(&self.venue, self.price_precision, self.size_precision)?;
        Ok(StreamDecodeOutcome::Emitted(DataInfraStreamEvent::Quote(
            quote,
        )))
    }

    /// Decodes a `CanonicalTradingStatusWire` JSON payload from `md.sip.statuses`.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if JSON deserialization or status validation fails.
    pub fn decode_status_json(&self, payload: &[u8]) -> DataInfraResult<StreamDecodeOutcome> {
        let wire: CanonicalTradingStatusWire = serde_json::from_slice(payload).map_err(|e| {
            DataInfraError::Transport(format!(
                "failed to deserialize CanonicalTradingStatusWire JSON: {e}"
            ))
        })?;
        let status = wire.to_nautilus_status(&self.venue)?;
        Ok(StreamDecodeOutcome::Emitted(
            DataInfraStreamEvent::TradingStatus(status),
        ))
    }

    /// Routes a Kafka `(topic, payload)` pair to the corresponding strongly typed decoder.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if the topic is unrecognized or payload decoding/validation fails.
    pub fn decode_topic_payload(
        &self,
        topic: &str,
        payload: &[u8],
    ) -> DataInfraResult<StreamDecodeOutcome> {
        let topic_trimmed = topic.trim();
        if topic_trimmed == TOPIC_SIP_BARS_1M || topic_trimmed.starts_with(TOPIC_SIP_BARS_PREFIX) {
            self.decode_bar_json(payload)
        } else if topic_trimmed == TOPIC_SIP_QUOTES {
            self.decode_quote_json(payload)
        } else if topic_trimmed == TOPIC_SIP_STATUSES {
            self.decode_status_json(payload)
        } else {
            Err(DataInfraError::InvalidIdentifier(format!(
                "unrecognized DataInfra Kafka topic '{topic_trimmed}'"
            )))
        }
    }
}

/// Coordinates Kafka stream session lifecycle transitions (`connected`, `disconnected`, `reconnected`)
/// via a dedicated [`StreamLifecycleListener`] without polluting [`DataInfraStreamEvent`].
#[derive(Debug)]
pub struct KafkaStreamSessionCoordinator {
    endpoint: String,
    decoder: CanonicalStreamDecoder,
    listener: Arc<dyn StreamLifecycleListener>,
    connected: Mutex<bool>,
    reconnect_attempts: Mutex<u32>,
}

impl KafkaStreamSessionCoordinator {
    /// Creates a new [`KafkaStreamSessionCoordinator`].
    #[must_use]
    pub fn new(
        endpoint: impl Into<String>,
        decoder: CanonicalStreamDecoder,
        listener: Arc<dyn StreamLifecycleListener>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            decoder,
            listener,
            connected: Mutex::new(false),
            reconnect_attempts: Mutex::new(0),
        }
    }

    /// Marks the stream session as connected and notifies [`StreamLifecycleListener`].
    pub fn notify_connected(&self) {
        let mut is_conn = self.connected.lock();
        let attempts = self.reconnect_attempts.lock();
        *is_conn = true;
        if *attempts > 0 {
            self.listener
                .on_stream_reconnected(&self.endpoint, *attempts);
        } else {
            self.listener.on_stream_connected(&self.endpoint);
        }
    }

    /// Marks the stream session as disconnected and notifies [`StreamLifecycleListener`].
    pub fn notify_disconnected(&self, reason: &str) {
        let mut is_conn = self.connected.lock();
        let mut attempts = self.reconnect_attempts.lock();
        *is_conn = false;
        *attempts = attempts.saturating_add(1);
        self.listener.on_stream_disconnected(&self.endpoint, reason);
    }

    /// Returns whether the stream session is currently marked connected.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        *self.connected.lock()
    }

    /// Decodes a Kafka message payload when connected, failing closed if the session is disconnected.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::Transport`] if the stream is currently disconnected, or propagates
    /// any decoding/validation error from [`CanonicalStreamDecoder`].
    pub fn process_kafka_message(
        &self,
        topic: &str,
        payload: &[u8],
    ) -> DataInfraResult<StreamDecodeOutcome> {
        if !self.is_connected() {
            return Err(DataInfraError::Transport(format!(
                "cannot process message on topic '{topic}': Kafka stream '{}' is disconnected",
                self.endpoint
            )));
        }
        self.decoder.decode_topic_payload(topic, payload)
    }
}
