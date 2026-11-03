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

//! `DataInfraDataClient` implementation of [`DataClient`] for real-time Kafka stream
//! subscriptions (`Bar`, `QuoteTick`, `InstrumentStatus`), `CalendarSessionGate` RTH
//! session validation, and ClickHouse historical `RequestBars` / `RequestInstrument` backfill.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use indexmap::{IndexMap, IndexSet};
use nautilus_common::{
    clients::{DataClient, execute_bar_subscriptions, execute_instrument_subscriptions},
    live::{
        runner::try_get_data_event_sender,
        runtime::get_runtime,
        sender::EventSender,
    },
    messages::{
        DataEvent, DataResponse,
        data::{
            BarsResponse, InstrumentResponse, InstrumentsResponse, QuotesResponse, RequestBars,
            RequestInstrument, RequestInstruments, RequestQuotes, SubscribeBars,
            SubscribeInstrument, SubscribeInstrumentStatus, SubscribeInstruments, SubscribeQuotes,
            UnsubscribeBars, UnsubscribeInstrument, UnsubscribeInstrumentStatus,
            UnsubscribeInstruments, UnsubscribeQuotes,
        },
    },
};
use nautilus_core::{
    datetime::datetime_to_unix_nanos,
    nanos::UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_model::{
    data::{BarType, Data},
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};
use parking_lot::{Mutex, RwLock};
use tokio::{sync::watch, task::JoinHandle};

use crate::{
    arrow_decoder::PriceAdjustmentMode,
    calendar::{CalendarSessionGate, CalendarSessionStatus},
    catalog::block_on_catalog_future,
    clickhouse::{
        BarsQueryRequest, ClickHouseArrowTransport, HttpClickHouseArrowTransport,
        QuotesQueryRequest,
    },
    config::DataInfraConfig,
    error::{DataInfraError, DataInfraResult},
    instrument::DataInfraEquityMetadata,
    kafka::{KafkaPayloadSource, TcpKafkaPayloadSource},
    mapping::{from_bar_type, from_instrument_id, timeframe_step_ns},
    stream::{
        CanonicalStreamDecoder, DEFAULT_DECODER_PRICE_PRECISION, DEFAULT_DECODER_SIZE_PRECISION,
        DEFAULT_VERSION_TRACKER_CAPACITY, DataInfraStreamEvent, KafkaStreamSessionCoordinator,
        StreamDecodeOutcome, StreamLifecycleListener,
    },
};

/// Canonical client name for the `DataInfra` market data adapter.
pub const DATAINFRA: &str = "DATAINFRA";

/// Backoff interval when the Kafka stream source reports no frame immediately ready.
const STREAM_IDLE_BACKOFF_MS: u64 = 15;
/// Backoff interval before attempting a broker reconnect after a stream transport error.
const STREAM_RECONNECT_BACKOFF_MS: u64 = 50;
/// Maximum buffered background stream events awaiting an in-flight subscription command.
const MAX_PENDING_STREAM_EVENTS: usize = 256;

/// Default [`StreamLifecycleListener`] for [`DataInfraDataClient`] that logs stream transitions.
#[derive(Debug, Default)]
pub struct TracingStreamLifecycleListener;

impl StreamLifecycleListener for TracingStreamLifecycleListener {
    fn on_stream_connected(&self, endpoint: &str) {
        log::info!("DataInfra Kafka stream connected: {endpoint}");
    }

    fn on_stream_disconnected(&self, endpoint: &str, reason: &str) {
        log::warn!("DataInfra Kafka stream disconnected ({endpoint}): {reason}");
    }

    fn on_stream_reconnected(&self, endpoint: &str, attempt: u32) {
        log::info!("DataInfra Kafka stream reconnected ({endpoint}) on attempt {attempt}");
    }
}

#[derive(Debug, Default)]
struct SubscriptionRegistry {
    bars: RwLock<IndexSet<BarType>>,
    quotes: RwLock<IndexSet<InstrumentId>>,
    statuses: RwLock<IndexSet<InstrumentId>>,
    pending_events: Mutex<Vec<DataInfraStreamEvent>>,
}

impl SubscriptionRegistry {
    fn contains_bar(&self, bar_type: &BarType) -> bool {
        self.bars.read().contains(bar_type)
    }

    fn contains_quote(&self, instrument_id: &InstrumentId) -> bool {
        self.quotes.read().contains(instrument_id)
    }

    fn contains_status(&self, instrument_id: &InstrumentId) -> bool {
        self.statuses.read().contains(instrument_id)
    }

    fn buffer_unsubscribed_event(&self, event: DataInfraStreamEvent) {
        let mut guard = self.pending_events.lock();
        if guard.len() >= MAX_PENDING_STREAM_EVENTS {
            guard.remove(0);
        }
        guard.push(event);
    }

    fn drain_matching_bar(&self, bar_type: &BarType) -> Vec<DataInfraStreamEvent> {
        let mut guard = self.pending_events.lock();
        let mut matched = Vec::new();
        guard.retain(|ev| match ev {
            DataInfraStreamEvent::Bar(b) | DataInfraStreamEvent::UpdatedBar { bar: b, .. }
                if &b.bar_type == bar_type =>
            {
                matched.push(ev.clone());
                false
            }
            _ => true,
        });
        matched
    }

    fn drain_matching_quote(&self, instrument_id: &InstrumentId) -> Vec<DataInfraStreamEvent> {
        let mut guard = self.pending_events.lock();
        let mut matched = Vec::new();
        guard.retain(|ev| match ev {
            DataInfraStreamEvent::Quote(q) if &q.instrument_id == instrument_id => {
                matched.push(ev.clone());
                false
            }
            _ => true,
        });
        matched
    }

    fn drain_matching_status(&self, instrument_id: &InstrumentId) -> Vec<DataInfraStreamEvent> {
        let mut guard = self.pending_events.lock();
        let mut matched = Vec::new();
        guard.retain(|ev| match ev {
            DataInfraStreamEvent::TradingStatus(s) if &s.instrument_id == instrument_id => {
                matched.push(ev.clone());
                false
            }
            _ => true,
        });
        matched
    }

    fn clear_all(&self) {
        self.bars.write().clear();
        self.quotes.write().clear();
        self.statuses.write().clear();
        self.pending_events.lock().clear();
    }
}

#[derive(Debug)]
struct ConsumerTaskHandle {
    shutdown_tx: watch::Sender<bool>,
    join_handle: JoinHandle<()>,
}

impl ConsumerTaskHandle {
    fn signal_and_abort(&self) {
        let _ = self.shutdown_tx.send(true);
        self.join_handle.abort();
    }

    fn is_running(&self) -> bool {
        !self.join_handle.is_finished()
    }
}

fn send_event_via_shared_sender(
    shared_sender: &RwLock<Option<EventSender<DataEvent>>>,
    event: DataEvent,
) -> DataInfraResult<()> {
    let guard = shared_sender.read();
    let Some(ref sender) = *guard else {
        return Err(DataInfraError::Transport(
            "DataInfraDataClient has no bound DataEvent sender".to_string(),
        ));
    };
    sender.send(event).map_err(|e| {
        DataInfraError::Transport(format!("Failed to send DataEvent to DataEngine: {e}"))
    })
}

fn dispatch_stream_event(
    event: &DataInfraStreamEvent,
    subscriptions: &SubscriptionRegistry,
    shared_sender: &RwLock<Option<EventSender<DataEvent>>>,
    buffer_if_unsubscribed: bool,
) -> DataInfraResult<bool> {
    match event {
        DataInfraStreamEvent::Bar(bar) | DataInfraStreamEvent::UpdatedBar { bar, .. } => {
            if !subscriptions.contains_bar(&bar.bar_type) {
                if buffer_if_unsubscribed {
                    subscriptions.buffer_unsubscribed_event(event.clone());
                }
                return Ok(false);
            }
            send_event_via_shared_sender(shared_sender, DataEvent::Data(Data::Bar(*bar)))?;
            Ok(true)
        }
        DataInfraStreamEvent::Quote(quote) => {
            if !subscriptions.contains_quote(&quote.instrument_id) {
                if buffer_if_unsubscribed {
                    subscriptions.buffer_unsubscribed_event(event.clone());
                }
                return Ok(false);
            }
            send_event_via_shared_sender(shared_sender, DataEvent::Data(Data::Quote(*quote)))?;
            Ok(true)
        }
        DataInfraStreamEvent::TradingStatus(status) => {
            if !subscriptions.contains_status(&status.instrument_id) {
                if buffer_if_unsubscribed {
                    subscriptions.buffer_unsubscribed_event(event.clone());
                }
                return Ok(false);
            }
            send_event_via_shared_sender(shared_sender, DataEvent::InstrumentStatus(*status))?;
            Ok(true)
        }
    }
}

/// Real-time and historical [`DataClient`] for `DataInfra` (ClickHouse + Kafka + `calendar-service`).
#[derive(Debug)]
pub struct DataInfraDataClient {
    client_id: ClientId,
    venue: Venue,
    config: DataInfraConfig,
    clickhouse_transport: Arc<dyn ClickHouseArrowTransport>,
    calendar_gate: CalendarSessionGate,
    kafka_source: Arc<dyn KafkaPayloadSource>,
    coordinator: Arc<KafkaStreamSessionCoordinator>,
    is_connected: Arc<AtomicBool>,
    subscribed_bars: IndexSet<BarType>,
    subscribed_quotes: IndexSet<InstrumentId>,
    subscribed_statuses: IndexSet<InstrumentId>,
    subscriptions: Arc<SubscriptionRegistry>,
    instruments: IndexMap<InstrumentId, InstrumentAny>,
    data_sender: Option<EventSender<DataEvent>>,
    shared_sender: Arc<RwLock<Option<EventSender<DataEvent>>>>,
    consumer_task: Option<ConsumerTaskHandle>,
    clock: &'static AtomicTime,
    enforce_calendar_on_connect: bool,
}

impl Drop for DataInfraDataClient {
    fn drop(&mut self) {
        if let Some(ref handle) = self.consumer_task {
            handle.signal_and_abort();
        }
    }
}

impl DataInfraDataClient {
    /// Creates a production [`DataInfraDataClient`] from [`DataInfraConfig`].
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if configuration validation or HTTP transport construction fails.
    pub fn new(client_id: ClientId, config: DataInfraConfig) -> DataInfraResult<Self> {
        config.validate()?;
        let clickhouse_transport = Arc::new(HttpClickHouseArrowTransport::new(config.clone())?);
        let calendar_gate = CalendarSessionGate::from_config(&config)?;
        Self::with_transports(
            client_id,
            config,
            clickhouse_transport,
            calendar_gate,
            try_get_data_event_sender(),
        )
    }

    /// Creates a [`DataInfraDataClient`] with explicit ClickHouse, Calendar, and `DataEvent` transports.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if `config.validate()` fails.
    pub fn with_transports(
        client_id: ClientId,
        config: DataInfraConfig,
        clickhouse_transport: Arc<dyn ClickHouseArrowTransport>,
        calendar_gate: CalendarSessionGate,
        data_sender: Option<EventSender<DataEvent>>,
    ) -> DataInfraResult<Self> {
        Self::with_transports_and_listener(
            client_id,
            config,
            clickhouse_transport,
            calendar_gate,
            data_sender,
            Arc::new(TracingStreamLifecycleListener),
        )
    }

    /// Creates a [`DataInfraDataClient`] with explicit transports and a custom [`StreamLifecycleListener`].
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if `config.validate()` fails.
    pub fn with_transports_and_listener(
        client_id: ClientId,
        config: DataInfraConfig,
        clickhouse_transport: Arc<dyn ClickHouseArrowTransport>,
        calendar_gate: CalendarSessionGate,
        data_sender: Option<EventSender<DataEvent>>,
        lifecycle_listener: Arc<dyn StreamLifecycleListener>,
    ) -> DataInfraResult<Self> {
        let kafka_source = Arc::new(TcpKafkaPayloadSource::from_config(&config)?);
        Self::with_transports_and_kafka_source(
            client_id,
            config,
            clickhouse_transport,
            calendar_gate,
            kafka_source,
            data_sender,
            lifecycle_listener,
        )
    }

    /// Creates a [`DataInfraDataClient`] with explicit ClickHouse, Calendar, Kafka stream source,
    /// `DataEvent` sender, and [`StreamLifecycleListener`].
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if `config.validate()` fails.
    pub fn with_transports_and_kafka_source(
        client_id: ClientId,
        config: DataInfraConfig,
        clickhouse_transport: Arc<dyn ClickHouseArrowTransport>,
        calendar_gate: CalendarSessionGate,
        kafka_source: Arc<dyn KafkaPayloadSource>,
        data_sender: Option<EventSender<DataEvent>>,
        lifecycle_listener: Arc<dyn StreamLifecycleListener>,
    ) -> DataInfraResult<Self> {
        config.validate()?;
        let venue = Venue::from(config.default_venue.as_str());
        let decoder = CanonicalStreamDecoder::new(
            &config.default_venue,
            DEFAULT_DECODER_PRICE_PRECISION,
            DEFAULT_DECODER_SIZE_PRECISION,
            DEFAULT_VERSION_TRACKER_CAPACITY,
        );
        let endpoint = config.kafka_brokers.join(",");
        let coordinator = Arc::new(KafkaStreamSessionCoordinator::new(
            endpoint,
            decoder,
            lifecycle_listener,
        ));
        let shared_sender = Arc::new(RwLock::new(data_sender.clone()));
        Ok(Self {
            client_id,
            venue,
            config,
            clickhouse_transport,
            calendar_gate,
            kafka_source,
            coordinator,
            is_connected: Arc::new(AtomicBool::new(false)),
            subscribed_bars: IndexSet::new(),
            subscribed_quotes: IndexSet::new(),
            subscribed_statuses: IndexSet::new(),
            subscriptions: Arc::new(SubscriptionRegistry::default()),
            instruments: IndexMap::new(),
            data_sender,
            shared_sender,
            consumer_task: None,
            clock: get_atomic_clock_realtime(),
            enforce_calendar_on_connect: true,
        })
    }

    /// Replaces the [`KafkaPayloadSource`] used by the background stream consumer task.
    #[must_use]
    pub fn with_kafka_source(mut self, kafka_source: Arc<dyn KafkaPayloadSource>) -> Self {
        self.kafka_source = kafka_source;
        self
    }

    /// Replaces the [`KafkaPayloadSource`] used by the background stream consumer task.
    #[must_use]
    pub fn with_payload_source(mut self, kafka_source: Arc<dyn KafkaPayloadSource>) -> Self {
        self.kafka_source = kafka_source;
        self
    }

    /// Binds or replaces the `DataEvent` channel sender toward `DataEngine`.
    pub fn set_data_sender(&mut self, sender: EventSender<DataEvent>) {
        *self.shared_sender.write() = Some(sender.clone());
        self.data_sender = Some(sender);
    }

    /// Configures whether `connect()` must query `calendar-service` before marking the client connected.
    #[must_use]
    pub const fn with_calendar_check_on_connect(mut self, enforce: bool) -> Self {
        self.enforce_calendar_on_connect = enforce;
        self
    }

    /// Returns whether the background Tokio Kafka consumer loop is currently active.
    #[must_use]
    pub fn is_consumer_task_running(&self) -> bool {
        self.consumer_task
            .as_ref()
            .is_some_and(ConsumerTaskHandle::is_running)
    }

    /// Returns a reference to the [`CalendarSessionGate`].
    #[must_use]
    pub const fn calendar_gate(&self) -> &CalendarSessionGate {
        &self.calendar_gate
    }

    /// Returns a reference to the active [`BarType`] subscriptions.
    #[must_use]
    pub const fn subscribed_bars(&self) -> &IndexSet<BarType> {
        &self.subscribed_bars
    }

    /// Returns a reference to the active [`QuoteTick`](nautilus_model::data::QuoteTick) subscriptions.
    #[must_use]
    pub const fn subscribed_quotes(&self) -> &IndexSet<InstrumentId> {
        &self.subscribed_quotes
    }

    /// Returns a reference to the active [`InstrumentStatus`](nautilus_model::data::InstrumentStatus) subscriptions.
    #[must_use]
    pub const fn subscribed_statuses(&self) -> &IndexSet<InstrumentId> {
        &self.subscribed_statuses
    }

    /// Registers an [`InstrumentAny`] into the client's local instrument cache and optionally
    /// forwards it to `DataEngine` if a sender is bound.
    pub fn register_instrument(&mut self, instrument: InstrumentAny) {
        self.instruments.insert(instrument.id(), instrument.clone());
        if let Some(ref sender) = self.data_sender {
            let _ = sender.send(DataEvent::Instrument(instrument));
        }
    }

    /// Queries `calendar-service` for the current session status (fail-closed).
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::CalendarGate`] if `calendar-service` is unreachable or returns `UNSYNCED`.
    pub async fn verify_session_status(
        &self,
        at_ts_utc: Option<i64>,
    ) -> DataInfraResult<CalendarSessionStatus> {
        self.calendar_gate.refresh_session_status(at_ts_utc).await
    }

    /// Decodes a raw Kafka message payload on `topic`, applies version/sanity filtering via
    /// [`KafkaStreamSessionCoordinator`], checks active subscriptions, and forwards emitted
    /// [`DataEvent`]s into `DataEngine`.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] on malformed JSON, sanity failure, or channel send error.
    pub fn ingest_kafka_payload(
        &mut self,
        topic: &str,
        payload: &[u8],
        _ts_init: UnixNanos,
    ) -> DataInfraResult<StreamDecodeOutcome> {
        let outcome = self.coordinator.process_kafka_message(topic, payload)?;
        if let StreamDecodeOutcome::Emitted(ref event) = outcome {
            self.forward_stream_event(event)?;
        }
        Ok(outcome)
    }

    /// Forwards a pre-decoded [`DataInfraStreamEvent`] into `DataEngine` if its instrument or
    /// bar type is currently subscribed.
    ///
    /// Returns `Ok(true)` if forwarded to `DataEngine`, or `Ok(false)` if not subscribed.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::Transport`] if no `DataEvent` sender is bound or the channel is closed,
    /// or [`DataInfraError::CalendarGate`] if a bar falls outside the cached RTH session window.
    pub fn forward_stream_event(&self, event: &DataInfraStreamEvent) -> DataInfraResult<bool> {
        if let DataInfraStreamEvent::Bar(bar) | DataInfraStreamEvent::UpdatedBar { bar, .. } = event
            && (self.enforce_calendar_on_connect || self.calendar_gate.cached_status().is_some())
        {
            let bar_open_ns = UnixNanos::from(
                bar.ts_event
                    .as_u64()
                    .saturating_sub(timeframe_step_ns(&bar.bar_type)),
            );
            if !self.calendar_gate.is_within_cached_rth(bar_open_ns)? {
                return Err(DataInfraError::CalendarGate(format!(
                    "live bar {} at bar_open_ns={bar_open_ns} (ts_event={}) is outside cached RTH session window",
                    bar.bar_type, bar.ts_event
                )));
            }
        }
        dispatch_stream_event(event, &self.subscriptions, &self.shared_sender, false)
    }

    fn send_data_event(&self, event: DataEvent) -> DataInfraResult<()> {
        send_event_via_shared_sender(&self.shared_sender, event)
    }

    fn spawn_consumer_task(&mut self) {
        self.abort_consumer_task();

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let kafka_source = Arc::clone(&self.kafka_source);
        let coordinator = Arc::clone(&self.coordinator);
        let subscriptions = Arc::clone(&self.subscriptions);
        let shared_sender = Arc::clone(&self.shared_sender);
        let calendar_gate = self.calendar_gate.clone();
        let enforce_calendar = self.enforce_calendar_on_connect;

        let handle = tokio::runtime::Handle::try_current()
            .unwrap_or_else(|_| get_runtime().handle().clone());

        let join_handle = handle.spawn(async move {
            let mut stream_faulted = false;
            loop {
                if *shutdown_rx.borrow() {
                    break;
                }
                tokio::select! {
                    biased;
                    _ = shutdown_rx.changed() => {
                        break;
                    }
                    frame_res = kafka_source.next_frame() => {
                        match frame_res {
                            Ok(Some(frame)) => {
                                if stream_faulted {
                                    coordinator.notify_connected();
                                    stream_faulted = false;
                                }
                                match coordinator.process_kafka_message(&frame.topic, &frame.payload) {
                                    Ok(StreamDecodeOutcome::Emitted(ref event)) => {
                                        if let DataInfraStreamEvent::Bar(bar)
                                        | DataInfraStreamEvent::UpdatedBar { bar, .. } = event
                                            && (enforce_calendar || calendar_gate.cached_status().is_some())
                                        {
                                            let bar_open_ns = UnixNanos::from(
                                                bar.ts_event
                                                    .as_u64()
                                                    .saturating_sub(timeframe_step_ns(&bar.bar_type)),
                                            );
                                            match calendar_gate.is_within_cached_rth(bar_open_ns) {
                                                Ok(true) => {}
                                                Ok(false) => {
                                                    log::warn!(
                                                        "DataInfra background consumer dropped non-RTH bar {} at bar_open_ns={bar_open_ns}",
                                                        bar.bar_type
                                                    );
                                                    continue;
                                                }
                                                Err(e) => {
                                                    log::warn!(
                                                        "DataInfra background consumer calendar gate rejected bar {}: {e}",
                                                        bar.bar_type
                                                    );
                                                    continue;
                                                }
                                            }
                                        }
                                        if let Err(e) = dispatch_stream_event(
                                            event,
                                            &subscriptions,
                                            &shared_sender,
                                            true,
                                        ) {
                                            log::error!("DataInfra background consumer failed to dispatch DataEvent: {e}");
                                        }
                                    }
                                    Ok(
                                        StreamDecodeOutcome::FilteredSynthetic { .. }
                                        | StreamDecodeOutcome::FilteredSuspectSpike { .. }
                                        | StreamDecodeOutcome::DuplicateOrStaleVersion { .. },
                                    ) => {}
                                    Err(e) => {
                                        log::warn!(
                                            "DataInfra background consumer dropped invalid frame on topic '{}': {e}",
                                            frame.topic
                                        );
                                    }
                                }
                            }
                            Ok(None) => {
                                if stream_faulted {
                                    coordinator.notify_connected();
                                    stream_faulted = false;
                                }
                                tokio::select! {
                                    _ = shutdown_rx.changed() => break,
                                    () = tokio::time::sleep(Duration::from_millis(STREAM_IDLE_BACKOFF_MS)) => {}
                                }
                            }
                            Err(e) => {
                                if !stream_faulted {
                                    coordinator.notify_disconnected(&e.to_string());
                                    stream_faulted = true;
                                }
                                tokio::select! {
                                    _ = shutdown_rx.changed() => break,
                                    () = tokio::time::sleep(Duration::from_millis(STREAM_RECONNECT_BACKOFF_MS)) => {}
                                }
                            }
                        }
                    }
                }
            }
            let _ = kafka_source.close().await;
        });

        self.consumer_task = Some(ConsumerTaskHandle {
            shutdown_tx,
            join_handle,
        });
    }

    fn abort_consumer_task(&mut self) {
        if let Some(handle) = self.consumer_task.take() {
            handle.signal_and_abort();
        }
    }

    async fn stop_consumer_task(&mut self) {
        if let Some(handle) = self.consumer_task.take() {
            let _ = handle.shutdown_tx.send(true);
            handle.join_handle.abort();
            let _ = handle.join_handle.await;
        }
        let _ = self.kafka_source.close().await;
    }

    fn resolve_or_create_instrument(
        &mut self,
        instrument_id: InstrumentId,
        ts_init: UnixNanos,
    ) -> anyhow::Result<InstrumentAny> {
        if let Some(existing) = self.instruments.get(&instrument_id) {
            return Ok(existing.clone());
        }
        let (symbol, venue) = from_instrument_id(instrument_id);
        let metadata = DataInfraEquityMetadata::new(symbol, venue);
        let equity = metadata.to_equity_any(ts_init)?;
        let inst = InstrumentAny::Equity(equity);
        self.instruments.insert(instrument_id, inst.clone());
        Ok(inst)
    }
}

#[async_trait(?Send)]
impl DataClient for DataInfraDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(self.venue)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.abort_consumer_task();
        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.abort_consumer_task();
        self.subscribed_bars.clear();
        self.subscribed_quotes.clear();
        self.subscribed_statuses.clear();
        self.subscriptions.clear_all();
        self.coordinator.notify_disconnected("client reset");
        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.reset()
    }

    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.enforce_calendar_on_connect {
            self.calendar_gate.refresh_session_status(None).await?;
        }
        self.stop_consumer_task().await;
        self.coordinator.notify_connected();
        self.is_connected.store(true, Ordering::Release);
        self.spawn_consumer_task();
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.stop_consumer_task().await;
        self.subscriptions.pending_events.lock().clear();
        if self.coordinator.is_connected() || self.is_connected.load(Ordering::Acquire) {
            self.coordinator.notify_disconnected("client disconnect");
        }
        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }

    fn subscribe_instruments(&mut self, _cmd: SubscribeInstruments) -> anyhow::Result<()> {
        Ok(())
    }

    fn subscribe_instrument(&mut self, cmd: SubscribeInstrument) -> anyhow::Result<()> {
        let ts_init = self.clock.get_time_ns();
        let mut resolved = Vec::new();
        execute_instrument_subscriptions(&cmd.instrument_ids, |inst_id| {
            let inst = self.resolve_or_create_instrument(inst_id, ts_init)?;
            resolved.push(inst);
            Ok(())
        })?;
        for inst in resolved {
            if let Some(ref sender) = self.data_sender {
                let _ = sender.send(DataEvent::Instrument(inst));
            }
        }
        Ok(())
    }

    fn subscribe_bars(&mut self, cmd: SubscribeBars) -> anyhow::Result<()> {
        execute_bar_subscriptions(&cmd.bar_types, |bar_type| {
            from_bar_type(&bar_type)?;
            self.subscribed_bars.insert(bar_type);
            self.subscriptions.bars.write().insert(bar_type);
            let pending = self.subscriptions.drain_matching_bar(&bar_type);
            for ev in &pending {
                let _ = self.forward_stream_event(ev);
            }
            Ok(())
        })
    }

    fn unsubscribe_bars(&mut self, cmd: &UnsubscribeBars) -> anyhow::Result<()> {
        execute_bar_subscriptions(&cmd.bar_types, |bar_type| {
            self.subscribed_bars.shift_remove(&bar_type);
            self.subscriptions.bars.write().shift_remove(&bar_type);
            Ok(())
        })
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |inst_id| {
            let _ = from_instrument_id(inst_id);
            self.subscribed_quotes.insert(inst_id);
            self.subscriptions.quotes.write().insert(inst_id);
            let pending = self.subscriptions.drain_matching_quote(&inst_id);
            for ev in &pending {
                let _ = self.forward_stream_event(ev);
            }
            Ok(())
        })
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |inst_id| {
            self.subscribed_quotes.shift_remove(&inst_id);
            self.subscriptions.quotes.write().shift_remove(&inst_id);
            Ok(())
        })
    }

    fn subscribe_instrument_status(&mut self, cmd: SubscribeInstrumentStatus) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |inst_id| {
            let _ = from_instrument_id(inst_id);
            self.subscribed_statuses.insert(inst_id);
            self.subscriptions.statuses.write().insert(inst_id);
            let pending = self.subscriptions.drain_matching_status(&inst_id);
            for ev in &pending {
                let _ = self.forward_stream_event(ev);
            }
            Ok(())
        })
    }

    fn unsubscribe_instrument_status(
        &mut self,
        cmd: &UnsubscribeInstrumentStatus,
    ) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |inst_id| {
            self.subscribed_statuses.shift_remove(&inst_id);
            self.subscriptions.statuses.write().shift_remove(&inst_id);
            Ok(())
        })
    }

    fn unsubscribe_instruments(&mut self, _cmd: &UnsubscribeInstruments) -> anyhow::Result<()> {
        Ok(())
    }

    fn unsubscribe_instrument(&mut self, _cmd: &UnsubscribeInstrument) -> anyhow::Result<()> {
        Ok(())
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        let ts_init = self.clock.get_time_ns();
        let start_ns = datetime_to_unix_nanos(request.start);
        let end_ns = datetime_to_unix_nanos(request.end);

        for inst_id in &request.instrument_ids {
            let inst = if let Some(existing) = self.instruments.get(inst_id) {
                existing.clone()
            } else {
                let (symbol, venue) = from_instrument_id(*inst_id);
                let metadata = DataInfraEquityMetadata::new(symbol, venue);
                InstrumentAny::Equity(metadata.to_equity_any(ts_init)?)
            };
            let resp = InstrumentResponse::new(
                request.request_id,
                self.client_id,
                *inst_id,
                inst,
                start_ns,
                end_ns,
                ts_init,
                request.params.clone(),
            );
            self.send_data_event(DataEvent::Response(DataResponse::Instrument(Box::new(resp))))?;
        }
        Ok(())
    }

    fn request_instruments(&self, request: RequestInstruments) -> anyhow::Result<()> {
        let ts_init = self.clock.get_time_ns();
        let start_ns = datetime_to_unix_nanos(request.start);
        let end_ns = datetime_to_unix_nanos(request.end);
        let venue = request.venue.unwrap_or(self.venue);
        let instruments: Vec<InstrumentAny> = self
            .instruments
            .values()
            .filter(|i| i.id().venue == venue)
            .cloned()
            .collect();
        let resp = InstrumentsResponse::new(
            request.request_id,
            self.client_id,
            venue,
            instruments,
            start_ns,
            end_ns,
            ts_init,
            request.params,
        );
        self.send_data_event(DataEvent::Response(DataResponse::Instruments(resp)))?;
        Ok(())
    }

    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        let ts_init = self.clock.get_time_ns();
        let start_ns = datetime_to_unix_nanos(request.start);
        let end_ns = datetime_to_unix_nanos(request.end);
        let limit_u32 = request.limit.map(|n| n.get() as u32);

        for bar_type in &request.bar_types {
            let (symbol, venue, tf) = from_bar_type(bar_type)?;
            let mut req =
                BarsQueryRequest::new(vec![symbol], tf, venue, PriceAdjustmentMode::Adjusted)
                    .with_time_range(start_ns, end_ns);
            if let Some(lim) = limit_u32 {
                req = req.with_limit_per_symbol(lim);
            }
            let transport = Arc::clone(&self.clickhouse_transport);
            let mut bars =
                block_on_catalog_future(async move { transport.fetch_bars(&req).await })?;
            bars.sort_by_key(|b| b.ts_init);

            let resp = BarsResponse::new(
                request.request_id,
                self.client_id,
                *bar_type,
                bars,
                start_ns,
                end_ns,
                ts_init,
                request.params.clone(),
            );
            self.send_data_event(DataEvent::Response(DataResponse::Bars(resp)))?;
        }
        Ok(())
    }

    fn request_quotes(&self, request: RequestQuotes) -> anyhow::Result<()> {
        let ts_init = self.clock.get_time_ns();
        let start_ns = datetime_to_unix_nanos(request.start);
        let end_ns = datetime_to_unix_nanos(request.end);
        let limit_u32 = request.limit.map(|n| n.get() as u32);

        let mut symbols = Vec::with_capacity(request.instrument_ids.len());
        let mut venue = self.config.default_venue.clone();
        for inst_id in &request.instrument_ids {
            let (sym, ven) = from_instrument_id(*inst_id);
            symbols.push(sym);
            venue = ven;
        }
        let mut req = QuotesQueryRequest::new(symbols, venue);
        req.start_ts_ns = start_ns;
        req.end_ts_ns = end_ns;
        req.limit = limit_u32;

        let transport = Arc::clone(&self.clickhouse_transport);
        let mut quotes =
            block_on_catalog_future(async move { transport.fetch_quotes(&req).await })?;
        quotes.sort_by_key(|q| q.ts_init);

        let resp = QuotesResponse::new(
            request.request_id,
            self.client_id,
            request.instrument_ids,
            quotes,
            start_ns,
            end_ns,
            ts_init,
            request.params,
        );
        self.send_data_event(DataEvent::Response(DataResponse::Quotes(resp)))?;
        Ok(())
    }
}
