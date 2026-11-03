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
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use rskafka::{
    BackoffConfig,
    client::{
        Client, ClientBuilder,
        partition::{OffsetAt, PartitionClient, UnknownTopicHandling},
    },
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;

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
struct PartitionCursor {
    topic: String,
    partition: i32,
    client: Arc<PartitionClient>,
    next_offset: i64,
}

#[derive(Debug)]
struct ActiveKafkaSession {
    _client: Arc<Client>,
    cursors: Vec<PartitionCursor>,
    buffered: VecDeque<KafkaPayloadFrame>,
    cursor_idx: usize,
}

/// Production Kafka broker partition consumer connecting to `config.kafka_brokers` via `rskafka`.
#[derive(Debug)]
pub struct TcpKafkaPayloadSource {
    brokers: Vec<String>,
    group_id: String,
    topics: Vec<String>,
    connect_timeout: Duration,
    has_established_session: AtomicBool,
    session: AsyncMutex<Option<ActiveKafkaSession>>,
    offsets: AsyncMutex<HashMap<(String, i32), i64>>,
}

impl TcpKafkaPayloadSource {
    /// Creates a new [`TcpKafkaPayloadSource`] from [`DataInfraConfig`].
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::InvalidConfig`] if `config.validate()` fails.
    pub fn from_config(config: &DataInfraConfig) -> DataInfraResult<Self> {
        let topics = DEFAULT_SIP_KAFKA_TOPICS
            .iter()
            .map(|t| (*t).to_string())
            .collect();
        Self::with_topics(config, topics)
    }

    /// Creates a [`TcpKafkaPayloadSource`] subscribed to an explicit list of Kafka topics.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::InvalidConfig`] if `config.validate()` fails or `topics` is empty.
    pub fn with_topics(config: &DataInfraConfig, topics: Vec<String>) -> DataInfraResult<Self> {
        config.validate()?;
        if topics.is_empty() {
            return Err(DataInfraError::InvalidConfig(
                "Kafka topic list cannot be empty".to_string(),
            ));
        }
        let connect_timeout_ms = config.request_timeout_ms.clamp(100, 2_000);
        Ok(Self {
            brokers: config.kafka_brokers.clone(),
            group_id: config.kafka_group_id.clone(),
            topics,
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            has_established_session: AtomicBool::new(false),
            session: AsyncMutex::new(None),
            offsets: AsyncMutex::new(HashMap::new()),
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

    async fn connect_kafka_session(&self) -> DataInfraResult<ActiveKafkaSession> {
        let backoff = BackoffConfig {
            init_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(80),
            base: 1.5,
            deadline: Some(self.connect_timeout),
        };
        let client = tokio::time::timeout(
            self.connect_timeout,
            ClientBuilder::new(self.brokers.clone())
                .client_id(self.group_id.clone())
                .backoff_config(backoff)
                .build(),
        )
        .await
        .map_err(|_| {
            DataInfraError::Transport(format!(
                "Kafka broker connect timed out after {:?}",
                self.connect_timeout
            ))
        })?
        .map_err(|e| DataInfraError::Transport(format!("Kafka broker connect error: {e}")))?;

        let client = Arc::new(client);
        let topics_meta = tokio::time::timeout(self.connect_timeout, client.list_topics())
            .await
            .map_err(|_| DataInfraError::Transport("Kafka list_topics timed out".to_string()))?
            .map_err(|e| DataInfraError::Transport(format!("Kafka list_topics error: {e}")))?;

        let mut offsets_guard = self.offsets.lock().await;
        let mut cursors = Vec::new();
        for target_topic in &self.topics {
            let topic_meta = topics_meta
                .iter()
                .find(|t| &t.name == target_topic)
                .ok_or_else(|| {
                    DataInfraError::Transport(format!(
                        "Kafka required topic '{target_topic}' does not exist on broker"
                    ))
                })?;
            if topic_meta.partitions.is_empty() {
                return Err(DataInfraError::Transport(format!(
                    "Kafka topic '{target_topic}' has 0 partitions"
                )));
            }
            let partitions: Vec<i32> = topic_meta.partitions.iter().copied().collect();

            for part_id in partitions {
                let p_client = tokio::time::timeout(
                    self.connect_timeout,
                    client.partition_client(
                        target_topic.clone(),
                        part_id,
                        UnknownTopicHandling::Retry,
                    ),
                )
                .await
                .map_err(|_| {
                    DataInfraError::Transport(format!(
                        "Kafka partition_client({target_topic}[{part_id}]) timed out"
                    ))
                })?
                .map_err(|e| {
                    DataInfraError::Transport(format!(
                        "Kafka partition_client({target_topic}[{part_id}]) error: {e}"
                    ))
                })?;

                let key = (target_topic.clone(), part_id);
                let start_offset = if let Some(&saved_offset) = offsets_guard.get(&key) {
                    saved_offset
                } else {
                    let latest = tokio::time::timeout(
                        self.connect_timeout,
                        p_client.get_offset(OffsetAt::Latest),
                    )
                    .await
                    .map_err(|_| {
                        DataInfraError::Transport(format!(
                            "Kafka get_offset(Latest) timed out for {target_topic}[{part_id}]"
                        ))
                    })?
                    .map_err(|e| {
                        DataInfraError::Transport(format!(
                            "Kafka get_offset(Latest) failed for {target_topic}[{part_id}]: {e}"
                        ))
                    })?;
                    offsets_guard.insert(key, latest);
                    latest
                };

                cursors.push(PartitionCursor {
                    topic: target_topic.clone(),
                    partition: part_id,
                    client: Arc::new(p_client),
                    next_offset: start_offset,
                });
            }
        }

        Ok(ActiveKafkaSession {
            _client: client,
            cursors,
            buffered: VecDeque::new(),
            cursor_idx: 0,
        })
    }

    async fn poll_session_frames(
        &self,
        session: &mut ActiveKafkaSession,
        fetch_timeout: Duration,
    ) -> DataInfraResult<Option<KafkaPayloadFrame>> {
        if let Some(frame) = session.buffered.pop_front() {
            return Ok(Some(frame));
        }
        let num_cursors = session.cursors.len();
        if num_cursors == 0 {
            return Ok(None);
        }

        for _ in 0..num_cursors {
            let idx = session.cursor_idx % num_cursors;
            session.cursor_idx = session.cursor_idx.wrapping_add(1);
            let cursor = &mut session.cursors[idx];

            let fetch_res = tokio::time::timeout(
                fetch_timeout,
                cursor
                    .client
                    .fetch_records(cursor.next_offset, 1..1_048_576, 15),
            )
            .await
            .map_err(|_| {
                DataInfraError::Transport(format!(
                    "Kafka fetch_records timed out on {}[{}] (EOF/disconnect)",
                    cursor.topic, cursor.partition
                ))
            })?
            .map_err(|e| {
                DataInfraError::Transport(format!(
                    "Kafka fetch_records failed on {}[{}] (EOF): {e}",
                    cursor.topic, cursor.partition
                ))
            })?;

            let (records, _high_watermark) = fetch_res;
            for rec_and_off in records {
                cursor.next_offset = rec_and_off.offset.saturating_add(1);
                self.offsets
                    .lock()
                    .await
                    .insert((cursor.topic.clone(), cursor.partition), cursor.next_offset);
                if let Some(raw_bytes) = rec_and_off.record.value {
                    let text = std::str::from_utf8(&raw_bytes).map_err(|e| {
                        DataInfraError::Transport(format!(
                            "Kafka record on {}[{}] is not valid UTF-8: {e}",
                            cursor.topic, cursor.partition
                        ))
                    })?;
                    let trimmed = text.trim();
                    if trimmed.is_empty() {
                        return Err(DataInfraError::Transport(format!(
                            "Kafka record on {}[{}] is empty",
                            cursor.topic, cursor.partition
                        )));
                    }
                    let parsed: serde_json::Value = serde_json::from_str(trimmed).map_err(|e| {
                        DataInfraError::Transport(format!(
                            "Kafka record on {}[{}] is not valid JSON: {e}",
                            cursor.topic, cursor.partition
                        ))
                    })?;
                    let frame = if parsed
                        .as_object()
                        .is_some_and(|obj| obj.contains_key("topic") && obj.contains_key("payload"))
                    {
                        let envelope: KafkaBrokerEnvelope =
                            serde_json::from_value(parsed).map_err(|e| {
                                DataInfraError::Transport(format!(
                                    "Kafka envelope on {}[{}] failed validation: {e}",
                                    cursor.topic, cursor.partition
                                ))
                            })?;
                        let payload_bytes = envelope.payload.into_payload_bytes()?;
                        KafkaPayloadFrame::new(envelope.topic.trim().to_string(), payload_bytes)
                    } else {
                        KafkaPayloadFrame::new(cursor.topic.clone(), trimmed.as_bytes().to_vec())
                    };
                    session.buffered.push_back(frame);
                }
            }

            if let Some(frame) = session.buffered.pop_front() {
                return Ok(Some(frame));
            }
        }

        Ok(None)
    }
}

#[async_trait]
impl KafkaPayloadSource for TcpKafkaPayloadSource {
    async fn next_frame(&self) -> DataInfraResult<Option<KafkaPayloadFrame>> {
        let mut guard = self.session.lock().await;

        if guard.is_none() {
            let mut active = self.connect_kafka_session().await?;
            self.has_established_session.store(true, Ordering::Release);
            let res = self
                .poll_session_frames(&mut active, self.connect_timeout)
                .await;
            if res.is_ok() {
                *guard = Some(active);
            }
            return res;
        }

        let Some(active) = guard.as_mut() else {
            return Ok(None);
        };
        let res = self
            .poll_session_frames(active, self.connect_timeout)
            .await;
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
