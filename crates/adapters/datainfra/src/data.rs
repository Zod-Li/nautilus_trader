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
    collections::BTreeMap,
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
    data::{Bar, BarType, Data},
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};
use parking_lot::{Mutex, RwLock};
use tokio::{sync::watch, task::JoinHandle};

use crate::{
    arrow_decoder::{PriceAdjustmentMode, f64_to_quantity},
    calendar::CalendarSessionGate,
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
/// Canonical 1-minute bar duration in nanoseconds (`60s`).
const M1_STEP_NS: u64 = 60_000_000_000;

fn can_rollup_from_1m(source_bar_type: &BarType, target_bar_type: &BarType) -> bool {
    if source_bar_type.instrument_id() != target_bar_type.instrument_id() {
        return false;
    }
    let src_step = timeframe_step_ns(source_bar_type);
    let dst_step = timeframe_step_ns(target_bar_type);
    src_step == M1_STEP_NS && dst_step > M1_STEP_NS && dst_step.is_multiple_of(M1_STEP_NS)
}

fn rth_day_bounds_ns(bar_open_ns: u64) -> Option<(u64, u64)> {
    crate::mapping::rth_day_bounds_ns(bar_open_ns)
}

fn compute_bucket_bounds_ns(target_bar_type: BarType, bar_open_ns: u64) -> (u64, u64) {
    let step_ns = timeframe_step_ns(&target_bar_type);
    if step_ns >= 3_600_000_000_000
        && let Some((rth_open_ns, rth_close_ns)) = rth_day_bounds_ns(bar_open_ns)
    {
        if step_ns >= 86_400_000_000_000 {
            let bucket_open_ns = rth_open_ns;
            let flush_close_ns = rth_close_ns.max(bucket_open_ns.saturating_add(M1_STEP_NS));
            return (bucket_open_ns, flush_close_ns);
        }
        let bucket_open_ns = if bar_open_ns >= rth_open_ns {
            rth_open_ns + ((bar_open_ns - rth_open_ns) / step_ns) * step_ns
        } else {
            let delta = rth_open_ns - bar_open_ns;
            let steps_back = delta.div_ceil(step_ns);
            rth_open_ns.saturating_sub(steps_back * step_ns)
        };
        let natural_close_ns = bucket_open_ns.saturating_add(step_ns);
        let flush_close_ns = if bucket_open_ns >= rth_open_ns && bucket_open_ns < rth_close_ns {
            natural_close_ns.min(rth_close_ns)
        } else {
            natural_close_ns
        };
        return (bucket_open_ns, flush_close_ns);
    }
    let bucket_open_ns = (bar_open_ns / step_ns) * step_ns;
    (bucket_open_ns, bucket_open_ns.saturating_add(step_ns))
}

#[derive(Debug, Default)]
struct LiveBarRollupBucket {
    bucket_open_ns: u64,
    flush_close_ns: u64,
    minutes: BTreeMap<u64, Bar>,
    emitted: bool,
    last_emitted_bar: Option<Bar>,
    prev_bucket_open_ns: u64,
    prev_minutes: BTreeMap<u64, Bar>,
    prev_last_emitted_bar: Option<Bar>,
}

impl LiveBarRollupBucket {
    fn same_bar_ohlcv(a: &Bar, b: &Bar) -> bool {
        a.open == b.open
            && a.high == b.high
            && a.low == b.low
            && a.close == b.close
            && a.volume == b.volume
    }

    fn build_aggregated_from_map(
        minutes: &BTreeMap<u64, Bar>,
        bucket_open_ns: u64,
        target_bar_type: BarType,
        _step_ns: u64,
        latest_init_ns: u64,
    ) -> DataInfraResult<Option<Bar>> {
        let Some((&first_open_ns, first_bar)) = minutes.iter().next() else {
            return Ok(None);
        };
        let (_, last_bar) = minutes
            .iter()
            .next_back()
            .unwrap_or((&first_open_ns, first_bar));
        let open = first_bar.open;
        let close = last_bar.close;
        let mut high = first_bar.high;
        let mut low = first_bar.low;
        let mut total_vol = 0.0_f64;
        for b in minutes.values() {
            if b.high > high {
                high = b.high;
            }
            if b.low < low {
                low = b.low;
            }
            total_vol += b.volume.as_f64();
        }
        let volume = f64_to_quantity(total_vol, first_bar.volume.precision, "volume")?;
        let (_, bucket_close_ns) = compute_bucket_bounds_ns(target_bar_type, bucket_open_ns);
        let ts_event = UnixNanos::from(bucket_close_ns);
        let ts_init = UnixNanos::from(bucket_close_ns.max(latest_init_ns));
        let bar = Bar::new_checked(
            target_bar_type,
            open,
            high,
            low,
            close,
            volume,
            ts_event,
            ts_init,
        )
        .map_err(|e| {
            DataInfraError::ProvenanceViolation(format!(
                "LiveBarRollupBucket OHLC sanity violation for {target_bar_type}: {e}"
            ))
        })?;
        Ok(Some(bar))
    }

    fn build_aggregated_bar(
        &self,
        target_bar_type: BarType,
        step_ns: u64,
        latest_init_ns: u64,
    ) -> DataInfraResult<Option<Bar>> {
        Self::build_aggregated_from_map(
            &self.minutes,
            self.bucket_open_ns,
            target_bar_type,
            step_ns,
            latest_init_ns,
        )
    }

    fn advance_watermark_ts(
        &mut self,
        target_bar_type: BarType,
        watermark_close_ns: u64,
    ) -> DataInfraResult<Option<Bar>> {
        let step_ns = timeframe_step_ns(&target_bar_type);
        let close_threshold = if self.flush_close_ns > 0 {
            self.flush_close_ns
        } else {
            self.bucket_open_ns.saturating_add(step_ns)
        };
        if self.bucket_open_ns != 0
            && !self.minutes.is_empty()
            && !self.emitted
            && watermark_close_ns >= close_threshold
            && let Some(agg_bar) =
                self.build_aggregated_bar(target_bar_type, step_ns, watermark_close_ns)?
        {
            self.emitted = true;
            self.last_emitted_bar = Some(agg_bar);
            return Ok(Some(agg_bar));
        }
        Ok(None)
    }

    fn ingest_1m_bar(&mut self, target_bar_type: BarType, bar: &Bar) -> DataInfraResult<Vec<Bar>> {
        let step_ns = timeframe_step_ns(&target_bar_type);
        let bar_open_ns = bar.ts_event.as_u64().saturating_sub(M1_STEP_NS);
        let (bucket_open_ns, flush_close_ns) = compute_bucket_bounds_ns(target_bar_type, bar_open_ns);
        let mut emitted_bars = Vec::new();

        if self.bucket_open_ns != 0 && bucket_open_ns > self.bucket_open_ns {
            if !self.emitted
                && let Some(prev_bar) =
                    self.build_aggregated_bar(target_bar_type, step_ns, bar.ts_init.as_u64())?
            {
                self.last_emitted_bar = Some(prev_bar);
                emitted_bars.push(prev_bar);
            }
            self.prev_bucket_open_ns = self.bucket_open_ns;
            self.prev_minutes = std::mem::take(&mut self.minutes);
            self.prev_last_emitted_bar = self.last_emitted_bar.take();
            self.bucket_open_ns = bucket_open_ns;
            self.flush_close_ns = flush_close_ns;
            self.emitted = false;
        } else if self.bucket_open_ns != 0 && bucket_open_ns < self.bucket_open_ns {
            if bucket_open_ns == self.prev_bucket_open_ns && !self.prev_minutes.is_empty() {
                self.prev_minutes.insert(bar_open_ns, *bar);
                if let Some(revised_prev) = Self::build_aggregated_from_map(
                    &self.prev_minutes,
                    self.prev_bucket_open_ns,
                    target_bar_type,
                    step_ns,
                    bar.ts_init.as_u64(),
                )? && self
                    .prev_last_emitted_bar
                    .as_ref()
                    .is_none_or(|prev| !Self::same_bar_ohlcv(prev, &revised_prev))
                {
                    self.prev_last_emitted_bar = Some(revised_prev);
                    emitted_bars.push(revised_prev);
                }
            }
            return Ok(emitted_bars);
        } else if self.bucket_open_ns == 0 {
            self.bucket_open_ns = bucket_open_ns;
            self.flush_close_ns = flush_close_ns;
        }

        self.minutes.insert(bar_open_ns, *bar);

        if self.emitted || bar.ts_event.as_u64() >= self.flush_close_ns {
            if let Some(agg_bar) =
                self.build_aggregated_bar(target_bar_type, step_ns, bar.ts_init.as_u64())?
            {
                let should_emit = !self.emitted
                    || self
                        .last_emitted_bar
                        .as_ref()
                        .is_none_or(|prev| !Self::same_bar_ohlcv(prev, &agg_bar));
                self.emitted = true;
                self.last_emitted_bar = Some(agg_bar);
                if should_emit {
                    emitted_bars.push(agg_bar);
                }
            }
        }

        Ok(emitted_bars)
    }
}

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
    rollups: Mutex<IndexMap<BarType, LiveBarRollupBucket>>,
    quotes: RwLock<IndexSet<InstrumentId>>,
    statuses: RwLock<IndexSet<InstrumentId>>,
    pending_events: Mutex<Vec<DataInfraStreamEvent>>,
}

impl SubscriptionRegistry {
    fn contains_bar(&self, bar_type: &BarType) -> bool {
        self.bars.read().contains(bar_type)
    }

    fn matching_rollup_targets(&self, source_bar_type: &BarType) -> Vec<BarType> {
        self.bars
            .read()
            .iter()
            .copied()
            .filter(|target| can_rollup_from_1m(source_bar_type, target))
            .collect()
    }

    fn ingest_rollup_bar(&self, target_bar_type: BarType, bar: &Bar) -> DataInfraResult<Vec<Bar>> {
        let mut guard = self.rollups.lock();
        guard
            .entry(target_bar_type)
            .or_default()
            .ingest_1m_bar(target_bar_type, bar)
    }

    fn advance_synthetic_watermarks(
        &self,
        symbol: &str,
        watermark_close_ns: u64,
    ) -> DataInfraResult<Vec<Bar>> {
        let targets: Vec<BarType> = self
            .bars
            .read()
            .iter()
            .copied()
            .filter(|bt| {
                bt.instrument_id().symbol.as_str().eq_ignore_ascii_case(symbol)
                    && timeframe_step_ns(bt) > M1_STEP_NS
            })
            .collect();
        let mut emitted = Vec::new();
        let mut guard = self.rollups.lock();
        for target_bt in targets {
            if let Some(bucket) = guard.get_mut(&target_bt)
                && let Some(agg_bar) =
                    bucket.advance_watermark_ts(target_bt, watermark_close_ns)?
            {
                emitted.push(agg_bar);
            }
        }
        Ok(emitted)
    }

    fn subscribe_status(&self, instrument_id: InstrumentId) {
        self.statuses.write().insert(instrument_id);
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
                if &b.bar_type == bar_type || can_rollup_from_1m(&b.bar_type, bar_type) =>
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
        self.rollups.lock().clear();
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
            let direct_match = subscriptions.contains_bar(&bar.bar_type);
            let rollup_targets = subscriptions.matching_rollup_targets(&bar.bar_type);
            if !direct_match && rollup_targets.is_empty() {
                if buffer_if_unsubscribed {
                    subscriptions.buffer_unsubscribed_event(event.clone());
                }
                return Ok(false);
            }
            let mut dispatched = false;
            if direct_match {
                send_event_via_shared_sender(shared_sender, DataEvent::Data(Data::Bar(*bar)))?;
                dispatched = true;
            }
            for target_bar_type in rollup_targets {
                for agg_bar in subscriptions.ingest_rollup_bar(target_bar_type, bar)? {
                    send_event_via_shared_sender(
                        shared_sender,
                        DataEvent::Data(Data::Bar(agg_bar)),
                    )?;
                    dispatched = true;
                }
            }
            Ok(dispatched)
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
        match outcome {
            StreamDecodeOutcome::Emitted(ref event) => {
                self.forward_stream_event(event)?;
            }
            StreamDecodeOutcome::FilteredSynthetic { ref symbol, bar_ts } => {
                let bar_open_ns =
                    crate::arrow_decoder::normalize_epoch_to_unix_nanos(bar_ts, "bar_ts")?;
                let watermark_close_ns = bar_open_ns.as_u64().saturating_add(M1_STEP_NS);
                for agg_bar in self
                    .subscriptions
                    .advance_synthetic_watermarks(symbol, watermark_close_ns)?
                {
                    self.send_data_event(DataEvent::Data(Data::Bar(agg_bar)))?;
                }
            }
            _ => {}
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
            if self.calendar_gate.should_refresh_for_bar_open(bar_open_ns) {
                let gate = self.calendar_gate.clone();
                let at_ts = Some((bar_open_ns.as_u64() / 1_000_000_000) as i64);
                block_on_catalog_future(async move {
                    gate.refresh_session_status(at_ts).await
                })?;
            }
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
                                            if calendar_gate.should_refresh_for_bar_open(bar_open_ns) {
                                                let at_ts = Some((bar_open_ns.as_u64() / 1_000_000_000) as i64);
                                                if let Err(e) = calendar_gate.refresh_session_status(at_ts).await {
                                                    log::warn!(
                                                        "DataInfra background consumer calendar refresh failed for bar {}: {e}",
                                                        bar.bar_type
                                                    );
                                                    continue;
                                                }
                                            }
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
                                    Ok(StreamDecodeOutcome::FilteredSynthetic {
                                        ref symbol,
                                        bar_ts,
                                    }) => {
                                        if let Ok(bar_open_ns) =
                                            crate::arrow_decoder::normalize_epoch_to_unix_nanos(
                                                bar_ts, "bar_ts",
                                            )
                                        {
                                            let watermark_close_ns =
                                                bar_open_ns.as_u64().saturating_add(M1_STEP_NS);
                                            match subscriptions.advance_synthetic_watermarks(
                                                symbol,
                                                watermark_close_ns,
                                            ) {
                                                Ok(agg_bars) => {
                                                    for agg_bar in agg_bars {
                                                        if let Err(e) =
                                                            send_event_via_shared_sender(
                                                                &shared_sender,
                                                                DataEvent::Data(Data::Bar(agg_bar)),
                                                            )
                                                        {
                                                            log::error!(
                                                                "DataInfra background consumer failed to dispatch synthetic-flushed rollup Bar: {e}"
                                                            );
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    log::error!(
                                                        "DataInfra background consumer failed synthetic watermark rollup for {symbol}: {e}"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    Ok(
                                        StreamDecodeOutcome::FilteredSuspectSpike { .. }
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
            let inst_id = bar_type.instrument_id();
            self.subscribed_bars.insert(bar_type);
            self.subscriptions.bars.write().insert(bar_type);
            self.subscribed_statuses.insert(inst_id);
            self.subscriptions.subscribe_status(inst_id);
            let pending = self.subscriptions.drain_matching_bar(&bar_type);
            for ev in &pending {
                let _ = self.forward_stream_event(ev);
            }
            let pending_status = self.subscriptions.drain_matching_status(&inst_id);
            for ev in &pending_status {
                let _ = self.forward_stream_event(ev);
            }
            Ok(())
        })
    }

    fn unsubscribe_bars(&mut self, cmd: &UnsubscribeBars) -> anyhow::Result<()> {
        execute_bar_subscriptions(&cmd.bar_types, |bar_type| {
            self.subscribed_bars.shift_remove(&bar_type);
            self.subscriptions.bars.write().shift_remove(&bar_type);
            self.subscriptions.rollups.lock().shift_remove(&bar_type);
            Ok(())
        })
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |inst_id| {
            let _ = from_instrument_id(inst_id);
            self.subscribed_quotes.insert(inst_id);
            self.subscriptions.quotes.write().insert(inst_id);
            self.subscribed_statuses.insert(inst_id);
            self.subscriptions.subscribe_status(inst_id);
            let pending = self.subscriptions.drain_matching_quote(&inst_id);
            for ev in &pending {
                let _ = self.forward_stream_event(ev);
            }
            let pending_status = self.subscriptions.drain_matching_status(&inst_id);
            for ev in &pending_status {
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
            self.subscriptions.subscribe_status(inst_id);
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
            if end_ns.is_none() {
                let now_u64 = ts_init.as_u64();
                bars.retain(|b| {
                    let close_ns = b.ts_event.as_u64();
                    let open_ns = close_ns.saturating_sub(timeframe_step_ns(&b.bar_type));
                    !(open_ns < now_u64 && now_u64 < close_ns)
                });
            }
            bars.sort_by_key(|b| b.ts_init);
            if let Some(lim) = limit_u32 {
                let lim_usize = lim as usize;
                if bars.len() > lim_usize {
                    bars.drain(0..(bars.len() - lim_usize));
                }
            }

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
        if let Some(lim) = limit_u32 {
            let lim_usize = lim as usize;
            if quotes.len() > lim_usize {
                quotes.drain(0..(quotes.len() - lim_usize));
            }
        }

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

#[cfg(test)]
mod tests {
    use nautilus_core::UUID4;
    use nautilus_model::{
        enums::MarketStatusAction,
        types::{Price, Quantity},
    };

    use super::*;
    use crate::{
        mapping::{parse_datainfra_bar_type, to_instrument_id},
        stream::{TOPIC_SIP_BARS_1M, TOPIC_SIP_STATUSES},
    };

    #[test]
    fn test_datainfra_data_client_rolls_up_1m_kafka_bars_into_subscribed_15m_bars() {
        let config = DataInfraConfig {
            default_venue: "ALPACA".to_string(),
            ..DataInfraConfig::default()
        };
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        let mut client = DataInfraDataClient::new(ClientId::from(DATAINFRA), config)
            .expect("create DataInfraDataClient")
            .with_calendar_check_on_connect(false);
        client.set_data_sender(EventSender::from(tx));
        client.coordinator.notify_connected();

        let aapl_15m = parse_datainfra_bar_type("AAPL", "ALPACA", "15m").expect("AAPL 15m BarType");
        assert_eq!(aapl_15m.to_string(), "AAPL.ALPACA-15-MINUTE-LAST-EXTERNAL");

        client
            .subscribe_bars(SubscribeBars::new(
                vec![aapl_15m],
                Some(ClientId::from(DATAINFRA)),
                Some(Venue::from("ALPACA")),
                UUID4::new(),
                UnixNanos::from(1_u64),
                None,
                None,
            ))
            .expect("subscribe 15m bars");

        let make_1m_json = |bar_ts: i64, open: f64, high: f64, low: f64, close: f64, vol: f64| {
            format!(
                r#"{{"symbol":"AAPL","timeframe":"1m","bar_ts":{bar_ts},"open":{open:.2},"high":{high:.2},"low":{low:.2},"close":{close:.2},"volume":{vol:.1},"version":1,"is_synthetic":false,"is_correction":false,"is_suspect_spike":false,"session":"REGULAR","provider":"alpaca_sip","source_ts_ms":{0},"ingest_ts_ms":{0}}}"#,
                (bar_ts + 60) * 1000
            )
        };

        // Bucket 1: [1777896000 (14:00:00), 1777896900 (14:15:00))
        // 14:00 (open) and 14:05 (mid-bucket) must not emit yet.
        client
            .ingest_kafka_payload(
                TOPIC_SIP_BARS_1M,
                make_1m_json(1_777_896_000, 185.00, 185.80, 184.90, 185.50, 10_000.0).as_bytes(),
                UnixNanos::default(),
            )
            .expect("ingest 14:00 1m bar");
        assert!(rx.try_recv().is_err());

        client
            .ingest_kafka_payload(
                TOPIC_SIP_BARS_1M,
                make_1m_json(1_777_896_300, 185.50, 186.40, 184.70, 186.10, 15_000.0).as_bytes(),
                UnixNanos::default(),
            )
            .expect("ingest 14:05 1m bar");
        assert!(rx.try_recv().is_err());

        // 14:14 (bar_ts=1777896840, ts_event=1777896900 == 14:15:00) closes Bucket 1 immediately.
        client
            .ingest_kafka_payload(
                TOPIC_SIP_BARS_1M,
                make_1m_json(1_777_896_840, 186.10, 186.20, 185.90, 186.05, 12_000.0).as_bytes(),
                UnixNanos::default(),
            )
            .expect("ingest 14:14 1m bar");

        let evt1 = rx.try_recv().expect("expected closed 14:00..14:15 15m bar");
        let DataEvent::Data(Data::Bar(bar1)) = evt1 else {
            panic!("expected DataEvent::Data(Data::Bar), got {evt1:?}");
        };
        assert_eq!(bar1.bar_type, aapl_15m);
        assert_eq!(bar1.open, Price::new(185.00, 2));
        assert_eq!(bar1.high, Price::new(186.40, 2));
        assert_eq!(bar1.low, Price::new(184.70, 2));
        assert_eq!(bar1.close, Price::new(186.05, 2));
        assert_eq!(bar1.volume, Quantity::new(37_000.0, 0));
        assert_eq!(
            bar1.ts_event,
            UnixNanos::from(1_777_896_900_000_000_000_u64)
        );
        assert!(rx.try_recv().is_err());

        // Bucket 2: [1777896900 (14:15:00), 1777897800 (14:30:00)) with missing :29 minute,
        // closed on rollover when 14:30:00 (1777897800) arrives.
        client
            .ingest_kafka_payload(
                TOPIC_SIP_BARS_1M,
                make_1m_json(1_777_896_900, 186.05, 186.50, 185.80, 186.30, 8_000.0).as_bytes(),
                UnixNanos::default(),
            )
            .expect("ingest 14:15 1m bar");
        client
            .ingest_kafka_payload(
                TOPIC_SIP_BARS_1M,
                make_1m_json(1_777_897_200, 186.30, 186.90, 186.10, 186.75, 11_000.0).as_bytes(),
                UnixNanos::default(),
            )
            .expect("ingest 14:20 1m bar");
        assert!(rx.try_recv().is_err());

        client
            .ingest_kafka_payload(
                TOPIC_SIP_BARS_1M,
                make_1m_json(1_777_897_800, 186.75, 187.00, 186.60, 186.90, 9_000.0).as_bytes(),
                UnixNanos::default(),
            )
            .expect("ingest 14:30 rollover 1m bar");

        let evt2 = rx
            .try_recv()
            .expect("expected rollover-flushed 14:15..14:30 15m bar");
        let DataEvent::Data(Data::Bar(bar2)) = evt2 else {
            panic!("expected DataEvent::Data(Data::Bar), got {evt2:?}");
        };
        assert_eq!(bar2.bar_type, aapl_15m);
        assert_eq!(bar2.open, Price::new(186.05, 2));
        assert_eq!(bar2.high, Price::new(186.90, 2));
        assert_eq!(bar2.low, Price::new(185.80, 2));
        assert_eq!(bar2.close, Price::new(186.75, 2));
        assert_eq!(bar2.volume, Quantity::new(19_000.0, 0));
        assert_eq!(
            bar2.ts_event,
            UnixNanos::from(1_777_897_800_000_000_000_u64)
        );
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_live_bar_rollup_synthetic_end_of_bucket_flush_and_all_synthetic_window_suppression() {
        let config = DataInfraConfig {
            default_venue: "ALPACA".to_string(),
            ..DataInfraConfig::default()
        };
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        let mut client = DataInfraDataClient::new(ClientId::from(DATAINFRA), config)
            .expect("create DataInfraDataClient")
            .with_calendar_check_on_connect(false);
        client.set_data_sender(EventSender::from(tx));
        client.coordinator.notify_connected();

        let aapl_1m = parse_datainfra_bar_type("AAPL", "ALPACA", "1m").expect("AAPL 1m BarType");
        let aapl_15m = parse_datainfra_bar_type("AAPL", "ALPACA", "15m").expect("AAPL 15m BarType");

        // Subscribe to BOTH 1m and 15m bars to verify that synthetic 1m bars are NEVER emitted
        // to 1m subscribers while still advancing the 15m rollup watermark!
        client
            .subscribe_bars(SubscribeBars::new(
                vec![aapl_1m, aapl_15m],
                Some(ClientId::from(DATAINFRA)),
                Some(Venue::from("ALPACA")),
                UUID4::new(),
                UnixNanos::from(1_u64),
                None,
                None,
            ))
            .expect("subscribe 1m and 15m bars");

        let make_bar_json = |bar_ts: i64,
                             open: f64,
                             high: f64,
                             low: f64,
                             close: f64,
                             vol: f64,
                             is_synthetic: bool| {
            format!(
                r#"{{"symbol":"AAPL","timeframe":"1m","bar_ts":{bar_ts},"open":{open:.2},"high":{high:.2},"low":{low:.2},"close":{close:.2},"volume":{vol:.1},"version":1,"is_synthetic":{is_synthetic},"is_correction":false,"is_suspect_spike":false,"session":"REGULAR","provider":"alpaca_sip","source_ts_ms":{0},"ingest_ts_ms":{0}}}"#,
                (bar_ts + 60) * 1000
            )
        };

        // 1. All-synthetic 15m window [14:00 (1777903200), 14:15 (1777904100)):
        //    Every minute :00..:14 is `is_synthetic = true` (0 real trades).
        //    Must NOT emit any 1m bar OR any empty/all-synthetic 15m bar!
        for i in 0..15_i64 {
            let bar_ts = 1_777_903_200 + i * 60;
            let outcome = client
                .ingest_kafka_payload(
                    TOPIC_SIP_BARS_1M,
                    make_bar_json(bar_ts, 185.00, 185.00, 185.00, 185.00, 0.0, true).as_bytes(),
                    UnixNanos::default(),
                )
                .expect("ingest synthetic 1m bar");
            assert_eq!(
                outcome,
                StreamDecodeOutcome::FilteredSynthetic {
                    symbol: "AAPL".to_string(),
                    bar_ts,
                }
            );
            assert!(
                rx.try_recv().is_err(),
                "all-synthetic window must not emit 1m or 15m bars at minute offset {i}"
            );
        }

        // 2. Final RTH 15m bucket [15:45 ET (1777923900), 16:00 ET (1777924800)):
        //    Minutes :45..:58 (14 minutes) are real 1m bars; minute :59 (15:59 ET = 1777924740)
        //    is a synthetic watermark bar (`is_synthetic = true, volume = 0`).
        for i in 0..14_i64 {
            let bar_ts = 1_777_923_900 + i * 60;
            let open: f64 = if i == 0 { 190.00 } else { 191.00 };
            let close: f64 = if i == 13 { 193.80 } else { 191.50 };
            let high: f64 = if i == 5 { 194.50 } else { open.max(close) + 0.50 };
            let low: f64 = if i == 2 { 189.25 } else { open.min(close) - 0.25 };
            client
                .ingest_kafka_payload(
                    TOPIC_SIP_BARS_1M,
                    make_bar_json(bar_ts, open, high, low, close, 1_000.0, false).as_bytes(),
                    UnixNanos::default(),
                )
                .expect("ingest real 1m bar");
            // Drain the 1m bar event; verify no 15m bar is emitted yet
            let evt = rx.try_recv().expect("expected 1m bar");
            let DataEvent::Data(Data::Bar(b1m)) = evt else {
                panic!("expected 1m bar, got {evt:?}");
            };
            assert_eq!(b1m.bar_type, aapl_1m);
            assert!(
                rx.try_recv().is_err(),
                "15m bar must not emit before minute :59"
            );
        }

        // Minute :59 (15:59 ET = 1777924740, close = 1777924800 == 16:00 ET) arrives as a synthetic watermark bar!
        let syn_outcome = client
            .ingest_kafka_payload(
                TOPIC_SIP_BARS_1M,
                make_bar_json(1_777_924_740, 193.80, 193.80, 193.80, 193.80, 0.0, true).as_bytes(),
                UnixNanos::default(),
            )
            .expect("ingest 15:59 synthetic watermark bar");
        assert_eq!(
            syn_outcome,
            StreamDecodeOutcome::FilteredSynthetic {
                symbol: "AAPL".to_string(),
                bar_ts: 1_777_924_740,
            }
        );

        // Exactly ONE event must be emitted: the closed 15m bar (NO 1m synthetic bar!)
        let flushed_evt = rx
            .try_recv()
            .expect("synthetic watermark at :59 must immediately flush the 15:45..16:00 15m bar");
        let DataEvent::Data(Data::Bar(flushed_15m)) = flushed_evt else {
            panic!("expected flushed 15m Bar, got {flushed_evt:?}");
        };
        assert_eq!(flushed_15m.bar_type, aapl_15m);
        assert_eq!(flushed_15m.open, Price::new(190.00, 2));
        assert_eq!(flushed_15m.high, Price::new(194.50, 2));
        assert_eq!(flushed_15m.low, Price::new(189.25, 2));
        assert_eq!(flushed_15m.close, Price::new(193.80, 2));
        assert_eq!(flushed_15m.volume, Quantity::new(14_000.0, 0));
        assert_eq!(
            flushed_15m.ts_event,
            UnixNanos::from(1_777_924_800_000_000_000_u64)
        );
        assert!(
            rx.try_recv().is_err(),
            "1m synthetic bar must NOT be emitted to 1m subscribers"
        );
    }

    #[test]
    fn test_live_bar_rollup_updated_bar_revisions_in_current_and_previous_bucket() {
        let config = DataInfraConfig {
            default_venue: "ALPACA".to_string(),
            ..DataInfraConfig::default()
        };
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        let mut client = DataInfraDataClient::new(ClientId::from(DATAINFRA), config)
            .expect("create DataInfraDataClient")
            .with_calendar_check_on_connect(false);
        client.set_data_sender(EventSender::from(tx));
        client.coordinator.notify_connected();

        let aapl_15m = parse_datainfra_bar_type("AAPL", "ALPACA", "15m").expect("AAPL 15m BarType");
        client
            .subscribe_bars(SubscribeBars::new(
                vec![aapl_15m],
                Some(ClientId::from(DATAINFRA)),
                Some(Venue::from("ALPACA")),
                UUID4::new(),
                UnixNanos::from(1_u64),
                None,
                None,
            ))
            .expect("subscribe 15m bars");

        let make_1m_ver_json =
            |bar_ts: i64, open: f64, high: f64, low: f64, close: f64, vol: f64, ver: u32| {
                let is_corr = ver > 1;
                format!(
                    r#"{{"symbol":"AAPL","timeframe":"1m","bar_ts":{bar_ts},"open":{open:.2},"high":{high:.2},"low":{low:.2},"close":{close:.2},"volume":{vol:.1},"version":{ver},"is_synthetic":false,"is_correction":{is_corr},"is_suspect_spike":false,"session":"REGULAR","provider":"alpaca_sip","source_ts_ms":{0},"ingest_ts_ms":{0}}}"#,
                    (bar_ts + 60) * 1000 + i64::from(ver)
                )
            };

        // Bucket 1: [1777896000 (14:00), 1777896900 (14:15))
        client
            .ingest_kafka_payload(
                TOPIC_SIP_BARS_1M,
                make_1m_ver_json(1_777_896_000, 185.00, 185.80, 184.90, 185.50, 10_000.0, 1)
                    .as_bytes(),
                UnixNanos::default(),
            )
            .unwrap();
        client
            .ingest_kafka_payload(
                TOPIC_SIP_BARS_1M,
                make_1m_ver_json(1_777_896_840, 185.50, 186.20, 185.10, 186.00, 12_000.0, 1)
                    .as_bytes(),
                UnixNanos::default(),
            )
            .unwrap();

        let DataEvent::Data(Data::Bar(initial_15m)) = rx.try_recv().expect("initial 15m bar")
        else {
            panic!("expected initial 15m bar");
        };
        assert_eq!(initial_15m.high, Price::new(186.20, 2));
        assert_eq!(initial_15m.close, Price::new(186.00, 2));
        assert_eq!(initial_15m.volume, Quantity::new(22_000.0, 0));

        // Case A: Late correction (`version = 2`) for 14:14 (`1_777_896_840`) arrives BEFORE 14:15 rollover
        client
            .ingest_kafka_payload(
                TOPIC_SIP_BARS_1M,
                make_1m_ver_json(1_777_896_840, 185.50, 187.40, 185.10, 187.10, 15_000.0, 2)
                    .as_bytes(),
                UnixNanos::default(),
            )
            .expect("ingest v2 correction in current emitted bucket");

        let DataEvent::Data(Data::Bar(revised_same_bucket)) = rx
            .try_recv()
            .expect("UpdatedBar in current emitted bucket must re-emit revised 15m bar")
        else {
            panic!("expected revised 15m bar");
        };
        assert_eq!(revised_same_bucket.open, Price::new(185.00, 2));
        assert_eq!(revised_same_bucket.high, Price::new(187.40, 2));
        assert_eq!(revised_same_bucket.close, Price::new(187.10, 2));
        assert_eq!(revised_same_bucket.volume, Quantity::new(25_000.0, 0));
        assert_eq!(
            revised_same_bucket.ts_event,
            UnixNanos::from(1_777_896_900_000_000_000_u64)
        );

        // Case B: Roll over to Bucket 2 (`1_777_896_900` = 14:15), then receive a late `version = 2`
        // correction for `1_777_896_000` (14:00, inside `prev_bucket_open_ns`).
        client
            .ingest_kafka_payload(
                TOPIC_SIP_BARS_1M,
                make_1m_ver_json(1_777_896_900, 187.10, 187.50, 186.90, 187.30, 8_000.0, 1)
                    .as_bytes(),
                UnixNanos::default(),
            )
            .expect("ingest 14:15 bar in next bucket");
        // Bucket 1 was already emitted, so rollover does NOT duplicate-emit Bucket 1
        assert!(rx.try_recv().is_err());

        client
            .ingest_kafka_payload(
                TOPIC_SIP_BARS_1M,
                make_1m_ver_json(1_777_896_000, 184.50, 188.00, 184.20, 185.50, 13_000.0, 2)
                    .as_bytes(),
                UnixNanos::default(),
            )
            .expect("ingest v2 correction for previous bucket");

        let DataEvent::Data(Data::Bar(revised_prev_bucket)) = rx
            .try_recv()
            .expect("UpdatedBar for previous bucket must re-emit revised previous 15m bar")
        else {
            panic!("expected revised previous 15m bar");
        };
        assert_eq!(revised_prev_bucket.open, Price::new(184.50, 2));
        assert_eq!(revised_prev_bucket.high, Price::new(188.00, 2));
        assert_eq!(revised_prev_bucket.low, Price::new(184.20, 2));
        assert_eq!(revised_prev_bucket.close, Price::new(187.10, 2));
        assert_eq!(revised_prev_bucket.volume, Quantity::new(28_000.0, 0));
        assert_eq!(
            revised_prev_bucket.ts_event,
            UnixNanos::from(1_777_896_900_000_000_000_u64)
        );
    }

    #[test]
    fn test_live_bar_rollup_1h_4h_1d_rth_0930_et_session_anchoring() {
        // 2026-05-04 (EDT, UTC-4):
        //   09:30 EDT = 13:30 UTC = 1_777_901_400
        //   10:00 EDT = 14:00 UTC = 1_777_903_200
        //   10:30 EDT = 14:30 UTC = 1_777_905_000
        //   13:30 EDT = 17:30 UTC = 1_777_915_800
        //   15:30 EDT = 19:30 UTC = 1_777_923_000
        //   16:00 EDT = 20:00 UTC = 1_777_924_800
        let aapl_1h = parse_datainfra_bar_type("AAPL", "ALPACA", "1h").unwrap();
        let aapl_4h = parse_datainfra_bar_type("AAPL", "ALPACA", "4h").unwrap();
        let aapl_1d = parse_datainfra_bar_type("AAPL", "ALPACA", "1d").unwrap();

        // 1. Verify `compute_bucket_bounds_ns` anchors `1h`, `4h`, and `1d` to `09:30 America/New_York`
        //    (NOT `09:00..10:00` Unix epoch hours!)
        let (h1_open_0930, h1_close_0930) =
            compute_bucket_bounds_ns(aapl_1h, 1_777_901_400_000_000_000);
        let (h1_open_0959, h1_close_0959) =
            compute_bucket_bounds_ns(aapl_1h, 1_777_903_140_000_000_000);
        let (h1_open_1029, h1_close_1029) =
            compute_bucket_bounds_ns(aapl_1h, 1_777_904_940_000_000_000);
        assert_eq!(h1_open_0930, 1_777_901_400_000_000_000);
        assert_eq!(h1_close_0930, 1_777_905_000_000_000_000);
        assert_eq!(h1_open_0959, 1_777_901_400_000_000_000);
        assert_eq!(h1_close_0959, 1_777_905_000_000_000_000);
        assert_eq!(h1_open_1029, 1_777_901_400_000_000_000);
        assert_eq!(h1_close_1029, 1_777_905_000_000_000_000);

        // Final RTH 1h bucket `15:30..16:00 ET`: `bucket_open = 15:30 ET`, `flush_close = 16:00 ET`
        let (h1_open_1530, h1_close_1530) =
            compute_bucket_bounds_ns(aapl_1h, 1_777_923_000_000_000_000);
        assert_eq!(h1_open_1530, 1_777_923_000_000_000_000);
        assert_eq!(h1_close_1530, 1_777_924_800_000_000_000);

        // `4h` buckets: `09:30..13:30 ET` and `13:30..16:00 ET` (clamped to `16:00 ET` RTH close)
        let (h4_open_1, h4_close_1) =
            compute_bucket_bounds_ns(aapl_4h, 1_777_901_400_000_000_000);
        let (h4_open_2, h4_close_2) =
            compute_bucket_bounds_ns(aapl_4h, 1_777_915_800_000_000_000);
        assert_eq!(h4_open_1, 1_777_901_400_000_000_000);
        assert_eq!(h4_close_1, 1_777_915_800_000_000_000);
        assert_eq!(h4_open_2, 1_777_915_800_000_000_000);
        assert_eq!(h4_close_2, 1_777_924_800_000_000_000);

        // `1d` bucket: `09:30..16:00 ET`
        let (d1_open, d1_close) = compute_bucket_bounds_ns(aapl_1d, 1_777_910_000_000_000_000);
        assert_eq!(d1_open, 1_777_901_400_000_000_000);
        assert_eq!(d1_close, 1_777_924_800_000_000_000);

        // 2. End-to-end client test: 09:59 ET (`ts_event = 10:00 ET`) must NOT flush the `1h` bar;
        //    `10:29 ET` (`ts_event = 10:30 ET`) flushes the `09:30..10:30 ET` `1h` bar!
        let config = DataInfraConfig {
            default_venue: "ALPACA".to_string(),
            ..DataInfraConfig::default()
        };
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        let mut client = DataInfraDataClient::new(ClientId::from(DATAINFRA), config)
            .unwrap()
            .with_calendar_check_on_connect(false);
        client.set_data_sender(EventSender::from(tx));
        client.coordinator.notify_connected();

        client
            .subscribe_bars(SubscribeBars::new(
                vec![aapl_1h, aapl_4h],
                Some(ClientId::from(DATAINFRA)),
                Some(Venue::from("ALPACA")),
                UUID4::new(),
                UnixNanos::from(1_u64),
                None,
                None,
            ))
            .unwrap();

        let make_1m_json = |bar_ts: i64, open: f64, high: f64, low: f64, close: f64, vol: f64| {
            format!(
                r#"{{"symbol":"AAPL","timeframe":"1m","bar_ts":{bar_ts},"open":{open:.2},"high":{high:.2},"low":{low:.2},"close":{close:.2},"volume":{vol:.1},"version":1,"is_synthetic":false,"is_correction":false,"is_suspect_spike":false,"session":"REGULAR","provider":"alpaca_sip","source_ts_ms":{0},"ingest_ts_ms":{0}}}"#,
                (bar_ts + 60) * 1000
            )
        };

        // 09:30 ET (`1_777_901_400`) and 09:59 ET (`1_777_903_140`, close = 10:00 ET): must NOT emit 1h bar at 10:00 ET
        client
            .ingest_kafka_payload(
                TOPIC_SIP_BARS_1M,
                make_1m_json(1_777_901_400, 180.00, 181.00, 179.50, 180.50, 5_000.0).as_bytes(),
                UnixNanos::default(),
            )
            .unwrap();
        client
            .ingest_kafka_payload(
                TOPIC_SIP_BARS_1M,
                make_1m_json(1_777_903_140, 180.50, 182.00, 180.20, 181.75, 6_000.0).as_bytes(),
                UnixNanos::default(),
            )
            .unwrap();
        assert!(
            rx.try_recv().is_err(),
            "1h bar anchored to 09:30 ET must NOT flush at 10:00 ET"
        );

        // 10:29 ET (`1_777_904_940`, close = 10:30 ET = `1_777_905_000`): flushes `09:30..10:30 ET` 1h bar!
        client
            .ingest_kafka_payload(
                TOPIC_SIP_BARS_1M,
                make_1m_json(1_777_904_940, 181.75, 183.00, 181.50, 182.50, 7_000.0).as_bytes(),
                UnixNanos::default(),
            )
            .unwrap();
        let DataEvent::Data(Data::Bar(bar_1h)) = rx
            .try_recv()
            .expect("expected 09:30..10:30 ET 1h bar at 10:30 ET")
        else {
            panic!("expected 1h Bar");
        };
        assert_eq!(bar_1h.bar_type, aapl_1h);
        assert_eq!(bar_1h.open, Price::new(180.00, 2));
        assert_eq!(bar_1h.high, Price::new(183.00, 2));
        assert_eq!(bar_1h.low, Price::new(179.50, 2));
        assert_eq!(bar_1h.close, Price::new(182.50, 2));
        assert_eq!(bar_1h.volume, Quantity::new(18_000.0, 0));
        assert_eq!(
            bar_1h.ts_event,
            UnixNanos::from(1_777_905_000_000_000_000_u64)
        );
        assert!(rx.try_recv().is_err());

        // 13:29 ET (`1_777_915_740`, close = 13:30 ET = `1_777_915_800`): flushes both `12:30..13:30 ET` 1h
        // and `09:30..13:30 ET` 4h bar!
        client
            .ingest_kafka_payload(
                TOPIC_SIP_BARS_1M,
                make_1m_json(1_777_915_740, 182.50, 184.00, 182.00, 183.90, 10_000.0).as_bytes(),
                UnixNanos::default(),
            )
            .unwrap();
        let mut emitted_at_1330 = Vec::new();
        while let Ok(DataEvent::Data(Data::Bar(b))) = rx.try_recv() {
            emitted_at_1330.push(b);
        }
        assert_eq!(emitted_at_1330.len(), 2);
        let bar_4h = emitted_at_1330
            .iter()
            .find(|b| b.bar_type == aapl_4h)
            .expect("09:30..13:30 ET 4h bar emitted at 13:30 ET");
        assert_eq!(bar_4h.open, Price::new(180.00, 2));
        assert_eq!(bar_4h.high, Price::new(184.00, 2));
        assert_eq!(bar_4h.low, Price::new(179.50, 2));
        assert_eq!(bar_4h.close, Price::new(183.90, 2));
        assert_eq!(bar_4h.volume, Quantity::new(28_000.0, 0));
        assert_eq!(
            bar_4h.ts_event,
            UnixNanos::from(1_777_915_800_000_000_000_u64)
        );
    }

    #[test]
    fn test_subscribe_bars_and_subscribe_quotes_automatically_register_instrument_status() {
        let config = DataInfraConfig {
            default_venue: "ALPACA".to_string(),
            ..DataInfraConfig::default()
        };
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        let mut client = DataInfraDataClient::new(ClientId::from(DATAINFRA), config)
            .expect("create DataInfraDataClient")
            .with_calendar_check_on_connect(false);
        client.set_data_sender(EventSender::from(tx));
        client.coordinator.notify_connected();

        let aapl_id = to_instrument_id("AAPL", "ALPACA").unwrap();
        let nvda_id = to_instrument_id("NVDA", "ALPACA").unwrap();
        let aapl_15m = parse_datainfra_bar_type("AAPL", "ALPACA", "15m").unwrap();

        assert!(client.subscribed_statuses().is_empty());

        // 1. Calling `subscribe_bars` for AAPL automatically registers `AAPL.ALPACA` in `subscribed_statuses`
        client
            .subscribe_bars(SubscribeBars::new(
                vec![aapl_15m],
                Some(ClientId::from(DATAINFRA)),
                Some(Venue::from("ALPACA")),
                UUID4::new(),
                UnixNanos::from(1_u64),
                None,
                None,
            ))
            .expect("subscribe_bars AAPL");
        assert!(client.subscribed_statuses().contains(&aapl_id));

        // Emit a halt status on `md.sip.statuses` for AAPL without ever calling `subscribe_instrument_status`
        let aapl_halt_json = br#"{"symbol":"AAPL","status_ts_ms":1777903200000,"ingest_ts_ms":1777903200005,"state":"HALTED","reason_code":"LULD_PAUSE","provider":"alpaca_sip"}"#;
        client
            .ingest_kafka_payload(TOPIC_SIP_STATUSES, aapl_halt_json, UnixNanos::default())
            .expect("ingest AAPL halt status");

        let evt_aapl = rx
            .try_recv()
            .expect("AAPL halt status must be emitted after subscribe_bars");
        let DataEvent::InstrumentStatus(status_aapl) = evt_aapl else {
            panic!("expected DataEvent::InstrumentStatus, got {evt_aapl:?}");
        };
        assert_eq!(status_aapl.instrument_id, aapl_id);
        assert_eq!(status_aapl.action, MarketStatusAction::Halt);
        assert_eq!(status_aapl.is_trading, Some(false));

        // 2. Buffer a status event for NVDA before subscription, then call `subscribe_quotes` for NVDA
        //    (without calling `subscribe_instrument_status`) -> automatically registers status & drains buffer!
        let nvda_status_outcome = client
            .coordinator
            .process_kafka_message(
                TOPIC_SIP_STATUSES,
                br#"{"symbol":"NVDA","status_ts_ms":1777903205000,"ingest_ts_ms":1777903205005,"state":"LULD_PAUSE","reason_code":"LULD_T5","provider":"alpaca_sip"}"#,
            )
            .unwrap();
        let StreamDecodeOutcome::Emitted(nvda_status_ev) = nvda_status_outcome else {
            panic!("expected Emitted status");
        };
        assert!(
            !dispatch_stream_event(
                &nvda_status_ev,
                &client.subscriptions,
                &client.shared_sender,
                true,
            )
            .unwrap()
        );
        assert!(rx.try_recv().is_err());

        client
            .subscribe_quotes(SubscribeQuotes::new(
                vec![nvda_id],
                Some(ClientId::from(DATAINFRA)),
                Some(Venue::from("ALPACA")),
                UUID4::new(),
                UnixNanos::from(2_u64),
                None,
                None,
            ))
            .expect("subscribe_quotes NVDA");
        assert!(client.subscribed_statuses().contains(&nvda_id));

        let evt_nvda = rx
            .try_recv()
            .expect("buffered NVDA halt status must be drained upon subscribe_quotes");
        let DataEvent::InstrumentStatus(status_nvda) = evt_nvda else {
            panic!("expected DataEvent::InstrumentStatus, got {evt_nvda:?}");
        };
        assert_eq!(status_nvda.instrument_id, nvda_id);
        assert_eq!(status_nvda.action, MarketStatusAction::Pause);
    }
}
