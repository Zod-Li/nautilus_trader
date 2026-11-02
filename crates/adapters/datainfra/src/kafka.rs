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

//! Asynchronous Kafka stream consumer transport (`KafkaPayloadSource` & `TcpKafkaPayloadSource`)
//! and strongly typed wire framing for `DataInfra` real-time topics (`md.sip.bars.1m`,
//! `md.sip.quotes`, `md.sip.statuses`).

use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpStream,
    sync::Mutex as AsyncMutex,
};

use crate::{
    config::DataInfraConfig,
    error::{DataInfraError, DataInfraResult},
    stream::{
        CanonicalBarWire, CanonicalQuoteWire, CanonicalStreamDecoder, CanonicalTradingStatusWire,
        TOPIC_SIP_BARS_1M, TOPIC_SIP_BARS_PREFIX, TOPIC_SIP_QUOTES, TOPIC_SIP_STATUSES,
    },
};

pub use crate::stream::{
    BarStreamVersionTracker, DataInfraStreamEvent, KafkaStreamSessionCoordinator,
    StreamDecodeOutcome, StreamLifecycleListener, VersionEvaluation,
};

/// Canonical alias for the real-time Kafka market stream decoder.
pub type KafkaMarketStreamDecoder = CanonicalStreamDecoder;

/// Default initial read readiness probe window after establishing a TCP broker connection.
const INITIAL_READ_PROBE_MS: u64 = 20;

/// Canonical list of SIP real-time Kafka topics consumed by `DataInfraDataClient`.
pub const DEFAULT_SIP_KAFKA_TOPICS: [&str; 3] =
    [TOPIC_SIP_BARS_1M, TOPIC_SIP_QUOTES, TOPIC_SIP_STATUSES];

/// Raw `(topic, payload)` frame delivered by a [`KafkaPayloadSource`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaPayloadFrame {
    /// Target Kafka topic (e.g. `md.sip.bars.1m`, `md.sip.quotes`, `md.sip.statuses`).
    pub topic: String,
    /// Raw UTF-8 JSON bytes representing the canonical wire payload.
    pub payload: Vec<u8>,
}

impl KafkaPayloadFrame {
    /// Creates a new [`KafkaPayloadFrame`].
    #[must_use]
    pub fn new(topic: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            topic: topic.into(),
            payload: payload.into(),
        }
    }
}

/// Strongly typed union of `sip-feeder` canonical wire objects for topic inference and envelope decoding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CanonicalWireFrame {
    /// 1m/15m/1h/4h/1d bar frame (`md.sip.bars.*`).
    Bar(CanonicalBarWire),
    /// L1 quote frame (`md.sip.quotes`).
    Quote(CanonicalQuoteWire),
    /// Trading halt / status frame (`md.sip.statuses`).
    Status(CanonicalTradingStatusWire),
}

impl CanonicalWireFrame {
    /// Returns the canonical Kafka topic corresponding to this wire frame.
    #[must_use]
    pub fn canonical_topic(&self) -> String {
        match self {
            Self::Bar(bar) => {
                let tf = bar.timeframe.trim();
                if tf.is_empty() || tf == "1m" {
                    TOPIC_SIP_BARS_1M.to_string()
                } else {
                    format!("{TOPIC_SIP_BARS_PREFIX}{tf}")
                }
            }
            Self::Quote(_) => TOPIC_SIP_QUOTES.to_string(),
            Self::Status(_) => TOPIC_SIP_STATUSES.to_string(),
        }
    }

    /// Serializes this wire frame back into canonical JSON bytes.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::Transport`] if JSON serialization fails.
    pub fn to_json_bytes(&self) -> DataInfraResult<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| {
            DataInfraError::Transport(format!("failed to serialize CanonicalWireFrame: {e}"))
        })
    }
}

/// Strongly typed payload representation inside a [`KafkaBrokerEnvelope`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum KafkaEnvelopePayload {
    /// Pre-serialized JSON string payload.
    JsonString(String),
    /// Raw byte array payload.
    JsonBytes(Vec<u8>),
    /// Embedded strongly typed canonical wire object.
    Structured(CanonicalWireFrame),
}

impl KafkaEnvelopePayload {
    /// Converts the envelope payload into raw UTF-8 JSON bytes for downstream decoding.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::Transport`] if serialization fails.
    pub fn into_payload_bytes(self) -> DataInfraResult<Vec<u8>> {
        match self {
            Self::JsonString(s) => Ok(s.into_bytes()),
            Self::JsonBytes(b) => Ok(b),
            Self::Structured(wire) => wire.to_json_bytes(),
        }
    }
}

/// Strongly typed JSON envelope carrying an explicit `topic` and `payload` over a broker stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KafkaBrokerEnvelope {
    /// Kafka topic name.
    pub topic: String,
    /// Canonical wire payload.
    #[serde(alias = "value", alias = "data")]
    pub payload: KafkaEnvelopePayload,
}

/// Parses a single broker wire line or frame into a [`KafkaPayloadFrame`].
///
/// Supported wire formats:
/// 1. Topic-prefixed text frames: `<topic>\t<json>`, `<topic>|<json>`, `<topic> <json>`, or `<topic>:<json>`
/// 2. Strongly typed JSON envelope: `{"topic": "md.sip.bars.1m", "payload": {...}}`
/// 3. Direct canonical JSON frame (`CanonicalBarWire`, `CanonicalQuoteWire`, `CanonicalTradingStatusWire`)
///
/// # Errors
///
/// Returns [`DataInfraError::Transport`] if the wire frame is empty or cannot be parsed.
pub fn parse_broker_wire_frame(raw: &[u8]) -> DataInfraResult<KafkaPayloadFrame> {
    let text = std::str::from_utf8(raw).map_err(|e| {
        DataInfraError::Transport(format!("Kafka broker frame is not valid UTF-8: {e}"))
    })?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(DataInfraError::Transport(
            "cannot parse empty Kafka broker frame".to_string(),
        ));
    }

    if !trimmed.starts_with('{') {
        if let Some(brace_idx) = trimmed.find('{') {
            let prefix = trimmed[..brace_idx]
                .trim_matches(|c: char| c.is_whitespace() || c == '|' || c == ':');
            let json_part = trimmed[brace_idx..].trim();
            if !prefix.is_empty() && !json_part.is_empty() {
                return Ok(KafkaPayloadFrame::new(
                    prefix.to_string(),
                    json_part.as_bytes().to_vec(),
                ));
            }
        }
        return Err(DataInfraError::Transport(format!(
            "unrecognized non-JSON Kafka broker frame: '{trimmed}'"
        )));
    }

    if let Ok(envelope) = serde_json::from_str::<KafkaBrokerEnvelope>(trimmed) {
        let payload_bytes = envelope.payload.into_payload_bytes()?;
        return Ok(KafkaPayloadFrame::new(
            envelope.topic.trim().to_string(),
            payload_bytes,
        ));
    }

    let wire: CanonicalWireFrame = serde_json::from_str(trimmed).map_err(|e| {
        DataInfraError::Transport(format!("failed to parse Kafka broker JSON frame: {e}"))
    })?;
    let topic = wire.canonical_topic();
    Ok(KafkaPayloadFrame::new(topic, trimmed.as_bytes().to_vec()))
}

/// Asynchronous stream transport port for reading `(topic, payload)` frames from `DataInfra` Kafka brokers.
#[async_trait]
pub trait KafkaPayloadSource: Send + Sync + std::fmt::Debug {
    /// Polls the next [`KafkaPayloadFrame`] from the broker stream.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::Transport`] on broker disconnect, EOF on an active session,
    /// or malformed wire framing.
    async fn next_frame(&self) -> DataInfraResult<Option<KafkaPayloadFrame>>;

    /// Closes any active broker socket or stream handle.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::Transport`] if closing the underlying resource fails.
    async fn close(&self) -> DataInfraResult<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct ActiveTcpSession {
    broker: String,
    reader: BufReader<TcpStream>,
}

/// Production TCP broker stream reader connecting to `config.kafka_brokers`.
#[derive(Debug)]
pub struct TcpKafkaPayloadSource {
    brokers: Vec<String>,
    group_id: String,
    connect_timeout: Duration,
    next_broker_idx: AtomicUsize,
    has_established_session: AtomicBool,
    session: AsyncMutex<Option<ActiveTcpSession>>,
}

impl TcpKafkaPayloadSource {
    /// Creates a new [`TcpKafkaPayloadSource`] from [`DataInfraConfig`].
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::InvalidConfig`] if `config.validate()` fails.
    pub fn from_config(config: &DataInfraConfig) -> DataInfraResult<Self> {
        config.validate()?;
        let connect_timeout_ms = config.request_timeout_ms.clamp(100, 2_000);
        Ok(Self {
            brokers: config.kafka_brokers.clone(),
            group_id: config.kafka_group_id.clone(),
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            next_broker_idx: AtomicUsize::new(0),
            has_established_session: AtomicBool::new(false),
            session: AsyncMutex::new(None),
        })
    }

    /// Returns the configured Kafka consumer group ID.
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the configured seed broker endpoints.
    #[must_use]
    pub fn brokers(&self) -> &[String] {
        &self.brokers
    }

    async fn read_line_frame(
        session: &mut ActiveTcpSession,
    ) -> DataInfraResult<Option<KafkaPayloadFrame>> {
        let mut line = String::new();
        loop {
            line.clear();
            match session.reader.read_line(&mut line).await {
                Ok(0) => {
                    return Err(DataInfraError::Transport(format!(
                        "Kafka broker {} closed TCP stream (EOF)",
                        session.broker
                    )));
                }
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let frame = parse_broker_wire_frame(trimmed.as_bytes())?;
                    return Ok(Some(frame));
                }
                Err(e) => {
                    return Err(DataInfraError::Transport(format!(
                        "Kafka broker {} stream read error: {e}",
                        session.broker
                    )));
                }
            }
        }
    }
}

#[async_trait]
impl KafkaPayloadSource for TcpKafkaPayloadSource {
    async fn next_frame(&self) -> DataInfraResult<Option<KafkaPayloadFrame>> {
        let mut guard = self.session.lock().await;

        if guard.is_none() {
            let mut last_connect_err: Option<String> = None;
            let count = self.brokers.len();
            for _ in 0..count {
                let idx = self.next_broker_idx.fetch_add(1, Ordering::Relaxed) % count;
                let broker = &self.brokers[idx];
                match tokio::time::timeout(self.connect_timeout, TcpStream::connect(broker)).await {
                    Ok(Ok(stream)) => {
                        let _ = stream.set_nodelay(true);
                        self.has_established_session.store(true, Ordering::Release);
                        let mut active = ActiveTcpSession {
                            broker: broker.clone(),
                            reader: BufReader::new(stream),
                        };
                        let probe = tokio::time::timeout(
                            Duration::from_millis(INITIAL_READ_PROBE_MS),
                            active.reader.fill_buf(),
                        )
                        .await;
                        match probe {
                            Err(_) => {
                                *guard = Some(active);
                                return Ok(None);
                            }
                            Ok(Ok(buf)) if buf.is_empty() => {
                                return Err(DataInfraError::Transport(format!(
                                    "Kafka broker {broker} closed stream immediately after connect"
                                )));
                            }
                            Ok(Ok(_)) => {
                                let res = Self::read_line_frame(&mut active).await;
                                if res.is_ok() {
                                    *guard = Some(active);
                                }
                                return res;
                            }
                            Ok(Err(e)) => {
                                return Err(DataInfraError::Transport(format!(
                                    "Kafka broker {broker} read probe error: {e}"
                                )));
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        last_connect_err =
                            Some(format!("Kafka broker {broker} connect error: {e}"));
                    }
                    Err(_) => {
                        last_connect_err = Some(format!(
                            "Kafka broker {broker} connect timed out after {:?}",
                            self.connect_timeout
                        ));
                    }
                }
            }

            if self.has_established_session.load(Ordering::Acquire) {
                return Err(DataInfraError::Transport(last_connect_err.unwrap_or_else(
                    || "all configured Kafka brokers unreachable".to_string(),
                )));
            }
            return Ok(None);
        }

        let Some(active) = guard.as_mut() else {
            return Ok(None);
        };
        let res = Self::read_line_frame(active).await;
        if res.is_err() {
            *guard = None;
        }
        res
    }

    async fn close(&self) -> DataInfraResult<()> {
        let mut guard = self.session.lock().await;
        *guard = None;
        self.has_established_session.store(false, Ordering::Release);
        Ok(())
    }
}
