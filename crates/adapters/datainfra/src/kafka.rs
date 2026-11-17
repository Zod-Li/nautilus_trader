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
        CanonicalBarWire, CanonicalQuoteWire, CanonicalTradingStatusWire, TOPIC_SIP_BARS_1M,
        TOPIC_SIP_QUOTES, TOPIC_SIP_STATUSES,
    },
};

pub use crate::stream::{
    BarStreamVersionTracker, DataInfraStreamEvent, KafkaStreamSessionCoordinator,
    StreamDecodeOutcome, StreamLifecycleListener, VersionEvaluation,
};

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
    cursor_idx: usize,
}

fn decode_kafka_record_value(
    topic: &str,
    partition: i32,
    raw_bytes: &[u8],
) -> DataInfraResult<KafkaPayloadFrame> {
    let text = std::str::from_utf8(raw_bytes).map_err(|e| {
        DataInfraError::Transport(format!(
            "Kafka record on {topic}[{partition}] is not valid UTF-8: {e}"
        ))
    })?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(DataInfraError::Transport(format!(
            "Kafka record on {topic}[{partition}] is empty"
        )));
    }
    let parsed: serde_json::Value = serde_json::from_str(trimmed).map_err(|e| {
        DataInfraError::Transport(format!(
            "Kafka record on {topic}[{partition}] is not valid JSON: {e}"
        ))
    })?;
    if parsed
        .as_object()
        .is_some_and(|obj| obj.contains_key("topic") && obj.contains_key("payload"))
    {
        let envelope: KafkaBrokerEnvelope = serde_json::from_value(parsed).map_err(|e| {
            DataInfraError::Transport(format!(
                "Kafka envelope on {topic}[{partition}] failed validation: {e}"
            ))
        })?;
        let payload_bytes = envelope.payload.into_payload_bytes()?;
        Ok(KafkaPayloadFrame::new(
            envelope.topic.trim().to_string(),
            payload_bytes,
        ))
    } else {
        Ok(KafkaPayloadFrame::new(
            topic.to_string(),
            trimmed.as_bytes().to_vec(),
        ))
    }
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
    buffered: AsyncMutex<VecDeque<KafkaPayloadFrame>>,
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
            buffered: AsyncMutex::new(VecDeque::new()),
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
        cursors.sort_by_key(|c| (topic_poll_priority(&c.topic), c.partition));

        Ok(ActiveKafkaSession {
            _client: client,
            cursors,
            cursor_idx: 0,
        })
    }

    async fn poll_session_frames(
        &self,
        session: &mut ActiveKafkaSession,
        fetch_timeout: Duration,
    ) -> DataInfraResult<Option<KafkaPayloadFrame>> {
        if let Some(frame) = self.buffered.lock().await.pop_front() {
            return Ok(Some(frame));
        }
        let num_cursors = session.cursors.len();
        if num_cursors == 0 {
            return Ok(None);
        }

        for step in 0..num_cursors {
            let idx = session.cursor_idx % num_cursors;
            session.cursor_idx = session.cursor_idx.wrapping_add(1);
            let cursor = &mut session.cursors[idx];
            let max_wait_ms = if step == 0 { 15 } else { 1 };

            let fetch_res = tokio::time::timeout(
                fetch_timeout,
                cursor
                    .client
                    .fetch_records(cursor.next_offset, 1..1_048_576, max_wait_ms),
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
            let mut buffered_guard = self.buffered.lock().await;
            for rec_and_off in records.into_iter().take(MAX_RECORDS_PER_PARTITION_POLL) {
                let next_offset = rec_and_off.offset.saturating_add(1);
                cursor.next_offset = next_offset;
                self.offsets
                    .lock()
                    .await
                    .insert((cursor.topic.clone(), cursor.partition), next_offset);
                if let Some(raw_bytes) = rec_and_off.record.value {
                    match decode_kafka_record_value(&cursor.topic, cursor.partition, &raw_bytes) {
                        Ok(frame) => {
                            buffered_guard.push_back(frame);
                        }
                        Err(err) => {
                            if let Some(first_valid) = buffered_guard.pop_front() {
                                return Ok(Some(first_valid));
                            }
                            return Err(err);
                        }
                    }
                }
            }

            if let Some(frame) = buffered_guard.pop_front() {
                return Ok(Some(frame));
            }
        }

        Ok(None)
    }
}

const MAX_RECORDS_PER_PARTITION_POLL: usize = 64;

fn topic_poll_priority(topic: &str) -> u8 {
    let trimmed = topic.trim();
    if trimmed == TOPIC_SIP_STATUSES {
        0
    } else if trimmed == TOPIC_SIP_BARS_1M || trimmed.starts_with("md.sip.bars.") {
        1
    } else if trimmed == TOPIC_SIP_QUOTES {
        2
    } else {
        3
    }
}

#[async_trait]
impl KafkaPayloadSource for TcpKafkaPayloadSource {
    async fn next_frame(&self) -> DataInfraResult<Option<KafkaPayloadFrame>> {
        if let Some(frame) = self.buffered.lock().await.pop_front() {
            return Ok(Some(frame));
        }

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
        self.buffered.lock().await.clear();
        self.has_established_session.store(false, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_kafka_record_value_validates_raw_and_envelope_frames() {
        let raw_bar = br#"{"symbol":"AAPL","timeframe":"1m","bar_ts":1700000000,"open":150.0,"high":151.0,"low":149.5,"close":150.5,"volume":1000,"trade_count":10,"vwap":150.25,"bar_version":1,"is_synthetic":false,"is_suspect_spike":false}"#;
        let frame = decode_kafka_record_value(TOPIC_SIP_BARS_1M, 0, raw_bar)
            .expect("valid raw bar JSON should decode");
        assert_eq!(frame.topic, TOPIC_SIP_BARS_1M);
        assert_eq!(frame.payload, raw_bar);

        let envelope = br#"{"topic":"md.sip.quotes","payload":{"symbol":"AAPL","quote_ts_ms":1700000000000,"bid_price":150.0,"bid_size":100,"ask_price":150.1,"ask_size":200}}"#;
        let env_frame = decode_kafka_record_value(TOPIC_SIP_BARS_1M, 1, envelope)
            .expect("valid envelope JSON should decode");
        assert_eq!(env_frame.topic, TOPIC_SIP_QUOTES);
        assert!(std::str::from_utf8(&env_frame.payload)
            .expect("utf8")
            .contains("\"quote_ts_ms\""));

        assert!(decode_kafka_record_value(TOPIC_SIP_BARS_1M, 0, b"   ").is_err());
        assert!(decode_kafka_record_value(TOPIC_SIP_BARS_1M, 0, &[0xff, 0xfe]).is_err());
        assert!(decode_kafka_record_value(TOPIC_SIP_BARS_1M, 0, br#"{"symbol":"AAPL""#).is_err());
        assert!(
            decode_kafka_record_value(
                TOPIC_SIP_BARS_1M,
                0,
                br#"{"topic":"md.sip.bars.1m","payload":12345}"#,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn test_tcp_kafka_payload_source_preserves_buffered_frames_across_session_drop() {
        let mut cfg = DataInfraConfig::default();
        cfg.kafka_brokers = vec!["127.0.0.1:1".to_string()];
        cfg.request_timeout_ms = 100;

        let source = TcpKafkaPayloadSource::from_config(&cfg).expect("valid config");
        let f1 = KafkaPayloadFrame::new(TOPIC_SIP_BARS_1M, br#"{"symbol":"AAPL"}"#.to_vec());
        let f2 = KafkaPayloadFrame::new(TOPIC_SIP_QUOTES, br#"{"symbol":"NVDA"}"#.to_vec());

        {
            let mut buf = source.buffered.lock().await;
            buf.push_back(f1.clone());
            buf.push_back(f2.clone());
        }

        // Even with session = None and an unreachable broker endpoint, buffered frames are drained first
        let out1 = source
            .next_frame()
            .await
            .expect("first buffered frame should succeed")
            .expect("frame 1 present");
        assert_eq!(out1, f1);

        let out2 = source
            .next_frame()
            .await
            .expect("second buffered frame should succeed")
            .expect("frame 2 present");
        assert_eq!(out2, f2);

        // Once the buffer is empty, next_frame attempts to connect to 127.0.0.1:1 and returns Err
        assert!(source.next_frame().await.is_err());

        // Re-buffering and calling close() clears the buffer
        source.buffered.lock().await.push_back(f1);
        source.close().await.expect("close succeeds");
        assert!(source.buffered.lock().await.is_empty());
    }

    #[test]
    fn test_topic_poll_priority_and_max_records_per_partition_poll_fairness_invariants() {
        assert_eq!(MAX_RECORDS_PER_PARTITION_POLL, 64);
        assert_eq!(topic_poll_priority(TOPIC_SIP_STATUSES), 0);
        assert_eq!(topic_poll_priority(TOPIC_SIP_BARS_1M), 1);
        assert_eq!(topic_poll_priority("md.sip.bars.15m"), 1);
        assert_eq!(topic_poll_priority(TOPIC_SIP_QUOTES), 2);
        assert_eq!(topic_poll_priority("md.sip.unknown"), 3);

        let mut topics = vec![
            (TOPIC_SIP_QUOTES.to_string(), 0_i32),
            (TOPIC_SIP_BARS_1M.to_string(), 1_i32),
            (TOPIC_SIP_STATUSES.to_string(), 0_i32),
            (TOPIC_SIP_BARS_1M.to_string(), 0_i32),
        ];
        topics.sort_by_key(|(t, p)| (topic_poll_priority(t), *p));
        assert_eq!(
            topics,
            vec![
                (TOPIC_SIP_STATUSES.to_string(), 0),
                (TOPIC_SIP_BARS_1M.to_string(), 0),
                (TOPIC_SIP_BARS_1M.to_string(), 1),
                (TOPIC_SIP_QUOTES.to_string(), 0),
            ]
        );
    }
}
