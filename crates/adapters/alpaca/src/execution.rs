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

//! `AlpacaExecutionClient` implementation of [`ExecutionClient`] for Alpaca Markets REST v2
//! and `trade_updates` WebSocket streaming.
//!
//! Supports single-leg orders (`Market`, `Limit`, `StopMarket`, `StopLimit`, `TrailingStopMarket`),
//! multi-leg `OrderList` submissions mapped to Alpaca `order_class: "bracket" | "oco" | "oto"`,
//! order modification (`PATCH /v2/orders/{id}`), order cancellation (`DELETE /v2/orders/{id}`),
//! `FifoCache` event deduplication, and startup/periodic reconciliation reports
//! (`OrderStatusReport`, `FillReport`, `PositionStatusReport`).

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use indexmap::{IndexMap, IndexSet};
use nautilus_common::{
    cache::{
        CacheView,
        fifo::{FifoCache, FifoCacheMap},
    },
    clients::ExecutionClient,
    live::{
        runner::try_get_exec_event_sender,
        runtime::get_runtime,
        sender::EventSender,
    },
    messages::{
        ExecutionEvent,
        execution::{
            BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
            GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
            ModifyOrder, QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList,
        },
    },
};
use nautilus_core::{
    Params, UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_execution::client::core::ExecutionClientCore;
use nautilus_live::{ExecutionEventEmitter, execution::context::OrderContext};
use nautilus_model::{
    accounts::AccountAny,
    enums::{
        AccountType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType, PositionSide,
        TrailingOffsetType,
    },
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TradeId, TraderId, Venue,
        VenueOrderId,
    },
    orders::{Order, OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use tokio::task::JoinHandle;

use crate::{
    common::{
        consts::ALPACA_VENUE,
        enums::{
            AlpacaOrderClass, AlpacaOrderSide, AlpacaOrderType, AlpacaTimeInForce,
            parse_alpaca_order_status,
        },
    },
    config::AlpacaConfig,
    error::{AlpacaError, AlpacaResult},
    http::{
        client::AlpacaHttpClient,
        models::{
            AlpacaOrderRequest, AlpacaOrderResponse, AlpacaReplaceOrderRequest,
            AlpacaStopLossSpec, AlpacaTakeProfitSpec,
        },
        parse::{
            DEFAULT_EQUITY_PRICE_PRECISION, DEFAULT_EQUITY_SIZE_PRECISION,
            build_fill_report_with_qty, decimal_to_price, decimal_to_quantity,
            derive_deterministic_fill_trade_id, parse_account_state, parse_instrument_id,
            parse_order_status_report, parse_position_status_report, parse_rfc3339_to_unix_nanos,
        },
    },
    websocket::{
        client::{AlpacaWsClient, AlpacaWsLifecycleObserver},
        messages::AlpacaTradeEventKind,
        parse::AlpacaExecutionUpdate,
    },
};

const DEDUP_CACHE_CAPACITY: usize = 10_000;
const MAX_FILL_DEDUP_CAPACITY: usize = 50_000;
const MAX_VENUE_ORDER_ID_CAPACITY: usize = 16_384;

/// Spawns an asynchronous order execution task onto the global Nautilus Tokio runtime
/// without blocking the single-threaded execution loop.
fn spawn_alpaca_order_task<F>(fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    get_runtime().spawn(fut);
}

struct QueuedOrderTask(Pin<Box<dyn Future<Output = ()> + Send + 'static>>);

impl std::fmt::Debug for QueuedOrderTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QueuedOrderTask(..)")
    }
}

/// Runs a `Future` to completion from synchronous code without panicking whether or not
/// the current thread is already inside a Tokio runtime.
pub(crate) fn block_on_alpaca_future<F, T>(fut: F) -> T
where
    F: Future<Output = T> + Send,
    T: Send,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::scope(|scope| {
            scope
                .spawn(move || get_runtime().block_on(fut))
                .join()
                .expect("AlpacaExecutionClient async worker thread panicked")
        })
    } else {
        get_runtime().block_on(fut)
    }
}

#[derive(Debug, Default)]
struct OrderFillDedupState {
    seen_trade_ids: IndexSet<TradeId>,
    emitted_cum_qty_by_venue: IndexMap<VenueOrderId, Decimal>,
    emitted_cum_qty_by_client: IndexMap<ClientOrderId, Decimal>,
    emitted_cum_notional_by_venue: IndexMap<VenueOrderId, Decimal>,
    emitted_cum_notional_by_client: IndexMap<ClientOrderId, Decimal>,
    rest_reported_cum_qty_by_venue: IndexMap<VenueOrderId, Decimal>,
    rest_reported_cum_qty_by_client: IndexMap<ClientOrderId, Decimal>,
    rest_reported_cum_notional_by_venue: IndexMap<VenueOrderId, Decimal>,
    rest_reported_cum_notional_by_client: IndexMap<ClientOrderId, Decimal>,
}

impl OrderFillDedupState {
    fn emitted_cum_qty(
        &self,
        venue_order_id: VenueOrderId,
        client_order_id: Option<ClientOrderId>,
    ) -> Decimal {
        let by_venue = self
            .emitted_cum_qty_by_venue
            .get(&venue_order_id)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let by_client = client_order_id
            .and_then(|cid| self.emitted_cum_qty_by_client.get(&cid).copied())
            .unwrap_or(Decimal::ZERO);
        by_venue.max(by_client)
    }

    fn emitted_cum_notional(
        &self,
        venue_order_id: VenueOrderId,
        client_order_id: Option<ClientOrderId>,
    ) -> Decimal {
        let by_venue = self
            .emitted_cum_notional_by_venue
            .get(&venue_order_id)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let by_client = client_order_id
            .and_then(|cid| self.emitted_cum_notional_by_client.get(&cid).copied())
            .unwrap_or(Decimal::ZERO);
        by_venue.max(by_client)
    }

    fn effective_cum_qty(
        &self,
        venue_order_id: VenueOrderId,
        client_order_id: Option<ClientOrderId>,
    ) -> Decimal {
        let emitted = self.emitted_cum_qty(venue_order_id, client_order_id);
        let rest_venue = self
            .rest_reported_cum_qty_by_venue
            .get(&venue_order_id)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let rest_client = client_order_id
            .and_then(|cid| self.rest_reported_cum_qty_by_client.get(&cid).copied())
            .unwrap_or(Decimal::ZERO);
        emitted.max(rest_venue).max(rest_client)
    }

    fn effective_cum_notional(
        &self,
        venue_order_id: VenueOrderId,
        client_order_id: Option<ClientOrderId>,
    ) -> Decimal {
        let emitted = self.emitted_cum_notional(venue_order_id, client_order_id);
        let rest_venue = self
            .rest_reported_cum_notional_by_venue
            .get(&venue_order_id)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let rest_client = client_order_id
            .and_then(|cid| self.rest_reported_cum_notional_by_client.get(&cid).copied())
            .unwrap_or(Decimal::ZERO);
        emitted.max(rest_venue).max(rest_client)
    }

    fn prune_map<K: std::hash::Hash + Eq>(map: &mut IndexMap<K, Decimal>) {
        if map.len() >= MAX_FILL_DEDUP_CAPACITY {
            let drain_count = map.len() / 2;
            map.drain(..drain_count);
        }
    }

    fn prune_if_needed(&mut self) {
        if self.seen_trade_ids.len() >= MAX_FILL_DEDUP_CAPACITY {
            let drain_count = self.seen_trade_ids.len() / 2;
            self.seen_trade_ids.drain(..drain_count);
        }
        Self::prune_map(&mut self.emitted_cum_qty_by_venue);
        Self::prune_map(&mut self.emitted_cum_notional_by_venue);
        Self::prune_map(&mut self.emitted_cum_qty_by_client);
        Self::prune_map(&mut self.emitted_cum_notional_by_client);
        Self::prune_map(&mut self.rest_reported_cum_qty_by_venue);
        Self::prune_map(&mut self.rest_reported_cum_notional_by_venue);
        Self::prune_map(&mut self.rest_reported_cum_qty_by_client);
        Self::prune_map(&mut self.rest_reported_cum_notional_by_client);
    }

    fn record_emitted_fill(
        &mut self,
        venue_order_id: VenueOrderId,
        client_order_id: Option<ClientOrderId>,
        new_cum_qty: Decimal,
        new_cum_notional: Decimal,
    ) {
        self.prune_if_needed();

        let prev_venue_qty = self
            .emitted_cum_qty_by_venue
            .get(&venue_order_id)
            .copied()
            .unwrap_or(Decimal::ZERO);
        self.emitted_cum_qty_by_venue
            .insert(venue_order_id, prev_venue_qty.max(new_cum_qty));

        let prev_venue_notional = self
            .emitted_cum_notional_by_venue
            .get(&venue_order_id)
            .copied()
            .unwrap_or(Decimal::ZERO);
        self.emitted_cum_notional_by_venue
            .insert(venue_order_id, prev_venue_notional.max(new_cum_notional));

        if let Some(cid) = client_order_id {
            let prev_client_qty = self
                .emitted_cum_qty_by_client
                .get(&cid)
                .copied()
                .unwrap_or(Decimal::ZERO);
            self.emitted_cum_qty_by_client
                .insert(cid, prev_client_qty.max(new_cum_qty));

            let prev_client_notional = self
                .emitted_cum_notional_by_client
                .get(&cid)
                .copied()
                .unwrap_or(Decimal::ZERO);
            self.emitted_cum_notional_by_client
                .insert(cid, prev_client_notional.max(new_cum_notional));
        }

        self.record_rest_reported_fill(
            venue_order_id,
            client_order_id,
            new_cum_qty,
            new_cum_notional,
        );
    }

    fn seed_order_baseline(
        &mut self,
        client_order_id: ClientOrderId,
        venue_order_id: Option<VenueOrderId>,
        filled_qty: Decimal,
        filled_notional: Decimal,
    ) {
        if filled_qty <= Decimal::ZERO {
            return;
        }
        self.prune_if_needed();
        let prev_client_qty = self
            .emitted_cum_qty_by_client
            .get(&client_order_id)
            .copied()
            .unwrap_or(Decimal::ZERO);
        self.emitted_cum_qty_by_client
            .insert(client_order_id, prev_client_qty.max(filled_qty));
        if filled_notional > Decimal::ZERO {
            let prev_client_notional = self
                .emitted_cum_notional_by_client
                .get(&client_order_id)
                .copied()
                .unwrap_or(Decimal::ZERO);
            self.emitted_cum_notional_by_client
                .insert(client_order_id, prev_client_notional.max(filled_notional));
        }
        if let Some(vid) = venue_order_id {
            self.record_emitted_fill(vid, Some(client_order_id), filled_qty, filled_notional);
        }
    }

    fn record_rest_reported_fill(
        &mut self,
        venue_order_id: VenueOrderId,
        client_order_id: Option<ClientOrderId>,
        new_cum_qty: Decimal,
        new_cum_notional: Decimal,
    ) {
        self.prune_if_needed();

        let prev_venue_qty = self
            .rest_reported_cum_qty_by_venue
            .get(&venue_order_id)
            .copied()
            .unwrap_or(Decimal::ZERO);
        self.rest_reported_cum_qty_by_venue
            .insert(venue_order_id, prev_venue_qty.max(new_cum_qty));

        let prev_venue_notional = self
            .rest_reported_cum_notional_by_venue
            .get(&venue_order_id)
            .copied()
            .unwrap_or(Decimal::ZERO);
        self.rest_reported_cum_notional_by_venue
            .insert(venue_order_id, prev_venue_notional.max(new_cum_notional));

        if let Some(cid) = client_order_id {
            let prev_client_qty = self
                .rest_reported_cum_qty_by_client
                .get(&cid)
                .copied()
                .unwrap_or(Decimal::ZERO);
            self.rest_reported_cum_qty_by_client
                .insert(cid, prev_client_qty.max(new_cum_qty));

            let prev_client_notional = self
                .rest_reported_cum_notional_by_client
                .get(&cid)
                .copied()
                .unwrap_or(Decimal::ZERO);
            self.rest_reported_cum_notional_by_client
                .insert(cid, prev_client_notional.max(new_cum_notional));
        }
    }

    fn clear(&mut self) {
        self.seen_trade_ids.clear();
        self.emitted_cum_qty_by_venue.clear();
        self.emitted_cum_qty_by_client.clear();
        self.emitted_cum_notional_by_venue.clear();
        self.emitted_cum_notional_by_client.clear();
        self.rest_reported_cum_qty_by_venue.clear();
        self.rest_reported_cum_qty_by_client.clear();
        self.rest_reported_cum_notional_by_venue.clear();
        self.rest_reported_cum_notional_by_client.clear();
    }
}

#[derive(Debug, Clone)]
struct SharedExecutionTracking {
    cached_orders: Arc<Mutex<FifoCacheMap<ClientOrderId, OrderAny, DEDUP_CACHE_CAPACITY>>>,
    order_contexts: Arc<Mutex<FifoCacheMap<ClientOrderId, OrderContext, DEDUP_CACHE_CAPACITY>>>,
    venue_order_ids: Arc<Mutex<IndexMap<ClientOrderId, Vec<VenueOrderId>>>>,
    client_order_id_by_venue: Arc<Mutex<IndexMap<VenueOrderId, ClientOrderId>>>,
    canonical_client_order_ids: Arc<Mutex<IndexMap<ClientOrderId, ClientOrderId>>>,
    submitted_orders: Arc<Mutex<FifoCache<ClientOrderId, DEDUP_CACHE_CAPACITY>>>,
    pending_modifies:
        Arc<Mutex<IndexMap<ClientOrderId, (Option<Quantity>, Option<Price>, Option<Price>)>>>,
    emitted_updates:
        Arc<Mutex<IndexMap<VenueOrderId, (Quantity, Option<Price>, Option<Price>)>>>,
    fill_dedup: Arc<Mutex<OrderFillDedupState>>,
    terminal_events:
        Arc<Mutex<FifoCache<(VenueOrderId, AlpacaTradeEventKind), DEDUP_CACHE_CAPACITY>>>,
    instrument_locks: Arc<Mutex<IndexMap<InstrumentId, Arc<tokio::sync::Mutex<()>>>>>,
    instrument_queues:
        Arc<Mutex<IndexMap<InstrumentId, tokio::sync::mpsc::UnboundedSender<QueuedOrderTask>>>>,
}

impl Default for SharedExecutionTracking {
    fn default() -> Self {
        Self {
            cached_orders: Arc::new(Mutex::new(FifoCacheMap::new())),
            order_contexts: Arc::new(Mutex::new(FifoCacheMap::new())),
            venue_order_ids: Arc::new(Mutex::new(IndexMap::new())),
            client_order_id_by_venue: Arc::new(Mutex::new(IndexMap::new())),
            canonical_client_order_ids: Arc::new(Mutex::new(IndexMap::new())),
            submitted_orders: Arc::new(Mutex::new(FifoCache::new())),
            pending_modifies: Arc::new(Mutex::new(IndexMap::new())),
            emitted_updates: Arc::new(Mutex::new(IndexMap::new())),
            fill_dedup: Arc::new(Mutex::new(OrderFillDedupState::default())),
            terminal_events: Arc::new(Mutex::new(FifoCache::new())),
            instrument_locks: Arc::new(Mutex::new(IndexMap::new())),
            instrument_queues: Arc::new(Mutex::new(IndexMap::new())),
        }
    }
}

impl SharedExecutionTracking {
    fn track_order(&self, order: &OrderAny) {
        let cid = order.client_order_id();
        self.cached_orders.lock().insert(cid, order.clone());
        self.order_contexts
            .lock()
            .insert(cid, OrderContext::from(order));
        let filled_dec = order.filled_qty().as_decimal();
        if let Some(vid) = order.venue_order_id() {
            self.record_venue_order_id(cid, vid);
        }
        if filled_dec > Decimal::ZERO {
            let avg_px_dec = order.avg_px().unwrap_or(Decimal::ZERO);
            let filled_notional = if avg_px_dec > Decimal::ZERO {
                filled_dec * avg_px_dec
            } else {
                Decimal::ZERO
            };
            self.fill_dedup.lock().seed_order_baseline(
                cid,
                order.venue_order_id(),
                filled_dec,
                filled_notional,
            );
        }
    }

    fn record_client_order_id_alias(
        &self,
        alias_client_order_id: ClientOrderId,
        canonical_client_order_id: ClientOrderId,
    ) {
        if alias_client_order_id == canonical_client_order_id {
            return;
        }
        let mut guard = self.canonical_client_order_ids.lock();
        if !guard.contains_key(&alias_client_order_id) && guard.len() >= MAX_VENUE_ORDER_ID_CAPACITY
        {
            let drain_count = guard.len() / 2;
            guard.drain(..drain_count);
        }
        let root = guard
            .get(&canonical_client_order_id)
            .copied()
            .unwrap_or(canonical_client_order_id);
        guard.insert(alias_client_order_id, root);
    }

    fn resolve_client_order_id(&self, client_order_id: ClientOrderId) -> ClientOrderId {
        self.canonical_client_order_ids
            .lock()
            .get(&client_order_id)
            .copied()
            .unwrap_or(client_order_id)
    }

    fn instrument_lock(&self, instrument_id: InstrumentId) -> Arc<tokio::sync::Mutex<()>> {
        let mut guard = self.instrument_locks.lock();
        Arc::clone(
            guard
                .entry(instrument_id)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    fn spawn_instrument_order_task<F>(&self, instrument_id: InstrumentId, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut boxed_task = Some(QueuedOrderTask(Box::pin(fut)));
        let mut queues = self.instrument_queues.lock();
        if let Some(tx) = queues.get(&instrument_id) {
            match tx.send(boxed_task.take().expect("task initialized")) {
                Ok(()) => return,
                Err(err) => {
                    boxed_task = Some(err.0);
                    queues.shift_remove(&instrument_id);
                }
            }
        }
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<QueuedOrderTask>();
        if let Some(task) = boxed_task.take() {
            let _ = tx.send(task);
        }
        queues.insert(instrument_id, tx);
        drop(queues);

        let inst_lock = self.instrument_lock(instrument_id);
        spawn_alpaca_order_task(async move {
            while let Some(QueuedOrderTask(task)) = rx.recv().await {
                let _guard = inst_lock.lock().await;
                task.await;
            }
        });
    }

    fn record_venue_order_id(&self, client_order_id: ClientOrderId, venue_order_id: VenueOrderId) {
        let client_order_id = self.resolve_client_order_id(client_order_id);
        let is_known_nautilus_order = |cid: &ClientOrderId| -> bool {
            self.cached_orders.lock().contains_key(cid)
                || self.submitted_orders.lock().contains(cid)
        };
        {
            let mut guard = self.venue_order_ids.lock();
            if !guard.contains_key(&client_order_id) && guard.len() >= MAX_VENUE_ORDER_ID_CAPACITY {
                let drain_count = guard.len() / 2;
                guard.drain(..drain_count);
            }
            let entry = guard.entry(client_order_id).or_default();
            if !entry.contains(&venue_order_id) {
                entry.push(venue_order_id);
            }
        }
        {
            let mut by_venue = self.client_order_id_by_venue.lock();
            if !by_venue.contains_key(&venue_order_id)
                && by_venue.len() >= MAX_VENUE_ORDER_ID_CAPACITY
            {
                let drain_count = by_venue.len() / 2;
                by_venue.drain(..drain_count);
            }
            let should_insert = match by_venue.get(&venue_order_id) {
                Some(existing) if *existing != client_order_id => {
                    if is_known_nautilus_order(existing)
                        && !is_known_nautilus_order(&client_order_id)
                    {
                        false
                    } else if existing.as_str() != venue_order_id.as_str() {
                        client_order_id.as_str() != venue_order_id.as_str()
                    } else {
                        true
                    }
                }
                _ => true,
            };
            if should_insert {
                by_venue.insert(venue_order_id, client_order_id);
            }
        }
    }

    fn current_venue_order_id(&self, client_order_id: &ClientOrderId) -> Option<VenueOrderId> {
        let resolved = self.resolve_client_order_id(*client_order_id);
        self.venue_order_ids
            .lock()
            .get(&resolved)
            .and_then(|ids| ids.last().copied())
    }

    fn client_order_id_for_venue(&self, venue_order_id: &VenueOrderId) -> Option<ClientOrderId> {
        self.client_order_id_by_venue
            .lock()
            .get(venue_order_id)
            .copied()
            .map(|cid| self.resolve_client_order_id(cid))
    }

    #[expect(clippy::too_many_arguments)]
    fn claim_ws_fill(
        &self,
        venue_order_id: VenueOrderId,
        client_order_id: ClientOrderId,
        trade_id: Option<TradeId>,
        last_qty: Option<Quantity>,
        last_px: Option<Price>,
        cum_qty: Quantity,
        order_qty: Quantity,
        size_precision: u8,
    ) -> Option<(TradeId, Quantity)> {
        let mut dedup = self.fill_dedup.lock();
        dedup.prune_if_needed();
        let already_reported = dedup.effective_cum_qty(venue_order_id, Some(client_order_id));
        let already_notional = dedup.effective_cum_notional(venue_order_id, Some(client_order_id));
        let order_cum_dec = cum_qty.as_decimal();
        let last_qty_dec = last_qty
            .map(|q| q.as_decimal())
            .unwrap_or(order_cum_dec)
            .max(Decimal::ZERO);
        let slice_px_dec = last_px
            .map(|p| p.as_decimal())
            .unwrap_or(Decimal::ZERO)
            .max(Decimal::ZERO);

        let is_delta = already_reported > Decimal::ZERO;
        let target_cum = if order_cum_dec > already_reported {
            order_cum_dec
        } else {
            already_reported + last_qty_dec
        };

        let resolved_trade_id = match trade_id {
            Some(tid) => {
                let base_tid =
                    derive_deterministic_fill_trade_id(venue_order_id.as_str(), target_cum, false)
                        .ok();
                if is_delta && base_tid == Some(tid) {
                    derive_deterministic_fill_trade_id(venue_order_id.as_str(), target_cum, true)
                        .ok()?
                } else {
                    tid
                }
            }
            None => {
                derive_deterministic_fill_trade_id(venue_order_id.as_str(), target_cum, is_delta)
                    .ok()?
            }
        };

        if dedup.seen_trade_ids.contains(&resolved_trade_id) {
            return None;
        }

        if order_cum_dec > Decimal::ZERO && order_cum_dec <= already_reported {
            dedup.seen_trade_ids.insert(resolved_trade_id);
            return None;
        }

        let max_unreported = if order_cum_dec > already_reported {
            order_cum_dec - already_reported
        } else if order_cum_dec == Decimal::ZERO && already_reported > Decimal::ZERO {
            let order_total_dec = order_qty.as_decimal();
            if order_total_dec > Decimal::ZERO {
                (order_total_dec - already_reported).max(Decimal::ZERO)
            } else {
                Decimal::ZERO
            }
        } else {
            last_qty_dec
        };
        let emit_qty_dec = last_qty_dec.min(max_unreported);
        if emit_qty_dec <= Decimal::ZERO {
            dedup.seen_trade_ids.insert(resolved_trade_id);
            return None;
        }

        let emit_qty = decimal_to_quantity(emit_qty_dec, size_precision).ok()?;
        let new_cum_qty = already_reported + emit_qty_dec;
        let new_cum_notional = already_notional + (emit_qty_dec * slice_px_dec);

        dedup.seen_trade_ids.insert(resolved_trade_id);
        dedup.record_emitted_fill(
            venue_order_id,
            Some(client_order_id),
            new_cum_qty,
            new_cum_notional,
        );
        if let Ok(det_base) =
            derive_deterministic_fill_trade_id(venue_order_id.as_str(), new_cum_qty, false)
        {
            dedup.seen_trade_ids.insert(det_base);
        }
        if let Ok(det_delta) =
            derive_deterministic_fill_trade_id(venue_order_id.as_str(), new_cum_qty, true)
        {
            dedup.seen_trade_ids.insert(det_delta);
        }

        Some((resolved_trade_id, emit_qty))
    }

    fn claim_submitted_order_fill(
        &self,
        venue_order_id: VenueOrderId,
        client_order_id: Option<ClientOrderId>,
        filled_qty: Decimal,
        filled_avg_price: Decimal,
        price_precision: u8,
        size_precision: u8,
    ) -> AlpacaResult<Option<(TradeId, Quantity, Price)>> {
        if filled_qty <= Decimal::ZERO || filled_avg_price <= Decimal::ZERO {
            return Ok(None);
        }

        let mut dedup = self.fill_dedup.lock();
        dedup.prune_if_needed();
        let already_known = dedup.effective_cum_qty(venue_order_id, client_order_id);
        if filled_qty <= already_known {
            return Ok(None);
        }

        let already_notional = dedup.effective_cum_notional(venue_order_id, client_order_id);
        let delta_dec = filled_qty - already_known;
        let total_notional = filled_qty * filled_avg_price;
        let delta_notional = total_notional - already_notional;
        let is_delta = already_known > Decimal::ZERO;
        let trade_id =
            derive_deterministic_fill_trade_id(venue_order_id.as_str(), filled_qty, is_delta)?;

        if dedup.seen_trade_ids.contains(&trade_id) {
            dedup.record_emitted_fill(venue_order_id, client_order_id, filled_qty, total_notional);
            return Ok(None);
        }

        let delta_px_dec =
            if is_delta && delta_notional > Decimal::ZERO && delta_dec > Decimal::ZERO {
                let dp = u32::from(price_precision.max(DEFAULT_EQUITY_PRICE_PRECISION).min(9));
                (delta_notional / delta_dec).round_dp(dp)
            } else {
                filled_avg_price
            };

        let delta_qty = decimal_to_quantity(delta_dec, size_precision)?;
        let delta_px = decimal_to_price(delta_px_dec, price_precision)?;
        dedup.seen_trade_ids.insert(trade_id);
        dedup.record_emitted_fill(venue_order_id, client_order_id, filled_qty, total_notional);
        if let Ok(det_base) =
            derive_deterministic_fill_trade_id(venue_order_id.as_str(), filled_qty, false)
        {
            dedup.seen_trade_ids.insert(det_base);
        }
        if let Ok(det_delta) =
            derive_deterministic_fill_trade_id(venue_order_id.as_str(), filled_qty, true)
        {
            dedup.seen_trade_ids.insert(det_delta);
        }

        Ok(Some((trade_id, delta_qty, delta_px)))
    }

    fn claim_rest_fill(
        &self,
        venue_order_id: VenueOrderId,
        client_order_id: Option<ClientOrderId>,
        filled_qty: Decimal,
        filled_avg_price: Decimal,
        price_precision: u8,
        size_precision: u8,
    ) -> AlpacaResult<Option<(TradeId, Quantity, Price)>> {
        if filled_qty <= Decimal::ZERO || filled_avg_price <= Decimal::ZERO {
            return Ok(None);
        }

        let mut dedup = self.fill_dedup.lock();
        dedup.prune_if_needed();
        let already_emitted = dedup.emitted_cum_qty(venue_order_id, client_order_id);
        if filled_qty <= already_emitted {
            return Ok(None);
        }

        let already_notional = dedup.emitted_cum_notional(venue_order_id, client_order_id);
        let delta_dec = filled_qty - already_emitted;
        let total_notional = filled_qty * filled_avg_price;
        let delta_notional = total_notional - already_notional;
        let is_delta = already_emitted > Decimal::ZERO;
        let trade_id =
            derive_deterministic_fill_trade_id(venue_order_id.as_str(), filled_qty, is_delta)?;

        if dedup.seen_trade_ids.contains(&trade_id) {
            dedup.record_emitted_fill(venue_order_id, client_order_id, filled_qty, total_notional);
            return Ok(None);
        }

        let delta_px_dec =
            if is_delta && delta_notional > Decimal::ZERO && delta_dec > Decimal::ZERO {
                let dp = u32::from(price_precision.max(DEFAULT_EQUITY_PRICE_PRECISION).min(9));
                (delta_notional / delta_dec).round_dp(dp)
            } else {
                filled_avg_price
            };

        let delta_qty = decimal_to_quantity(delta_dec, size_precision)?;
        let delta_px = decimal_to_price(delta_px_dec, price_precision)?;
        dedup.seen_trade_ids.insert(trade_id);
        dedup.record_emitted_fill(
            venue_order_id,
            client_order_id,
            filled_qty,
            total_notional,
        );
        if let Ok(det_base) =
            derive_deterministic_fill_trade_id(venue_order_id.as_str(), filled_qty, false)
        {
            dedup.seen_trade_ids.insert(det_base);
        }
        if let Ok(det_delta) =
            derive_deterministic_fill_trade_id(venue_order_id.as_str(), filled_qty, true)
        {
            dedup.seen_trade_ids.insert(det_delta);
        }

        Ok(Some((trade_id, delta_qty, delta_px)))
    }

    fn has_terminal_order_event(&self, venue_order_id: VenueOrderId) -> bool {
        let term = self.terminal_events.lock();
        term.contains(&(venue_order_id, AlpacaTradeEventKind::Canceled))
            || term.contains(&(venue_order_id, AlpacaTradeEventKind::Fill))
            || term.contains(&(venue_order_id, AlpacaTradeEventKind::Expired))
            || term.contains(&(venue_order_id, AlpacaTradeEventKind::Rejected))
    }

    fn claim_order_updated(
        &self,
        venue_order_id: VenueOrderId,
        qty: Quantity,
        price: Option<Price>,
        trigger_price: Option<Price>,
    ) -> bool {
        let mut guard = self.emitted_updates.lock();
        if !guard.contains_key(&venue_order_id) && guard.len() >= MAX_VENUE_ORDER_ID_CAPACITY {
            let drain_count = guard.len() / 2;
            guard.drain(..drain_count);
        }
        let target = (qty, price, trigger_price);
        if guard.get(&venue_order_id) == Some(&target) {
            return false;
        }
        guard.insert(venue_order_id, target);
        true
    }

    fn clear(&self) {
        self.cached_orders.lock().clear();
        self.order_contexts.lock().clear();
        self.venue_order_ids.lock().clear();
        self.client_order_id_by_venue.lock().clear();
        self.canonical_client_order_ids.lock().clear();
        self.submitted_orders.lock().clear();
        self.pending_modifies.lock().clear();
        self.emitted_updates.lock().clear();
        self.fill_dedup.lock().clear();
        self.terminal_events.lock().clear();
        self.instrument_queues.lock().clear();
    }
}

#[derive(Debug)]
struct AlpacaExecutionWsObserver {
    http_client: Arc<AlpacaHttpClient>,
    emitter: ExecutionEventEmitter,
    tracking: SharedExecutionTracking,
    clock: &'static AtomicTime,
    reconnecting: AtomicBool,
}

impl AlpacaWsLifecycleObserver for AlpacaExecutionWsObserver {
    fn on_ws_authenticated(&self) {}

    fn on_ws_listening(&self, _streams: &[String]) {
        if self.reconnecting.swap(false, Ordering::AcqRel) {
            let http = Arc::clone(&self.http_client);
            let emitter = self.emitter.clone();
            let tracking = self.tracking.clone();
            let clock = self.clock;
            spawn_alpaca_order_task(async move {
                AlpacaExecutionClient::reconcile_tracked_orders_once(
                    &http, &emitter, &tracking, clock,
                )
                .await;
            });
        }
    }

    fn on_ws_reconnecting(&self, _reconnect_count: u32) {
        self.reconnecting.store(true, Ordering::Release);
    }

    fn on_ws_auth_failed(&self, reason: &str) {
        log::error!("Alpaca trade_updates WebSocket authentication failed: {reason}");
    }
}

/// Alpaca Markets execution client implementing [`ExecutionClient`].
#[derive(Debug)]
pub struct AlpacaExecutionClient {
    core: ExecutionClientCore,
    config: AlpacaConfig,
    http_client: Arc<AlpacaHttpClient>,
    ws_client: Option<AlpacaWsClient>,
    ws_dispatch_task: Option<JoinHandle<()>>,
    emitter: ExecutionEventEmitter,
    clock: &'static AtomicTime,
    tracking: SharedExecutionTracking,
    connect_ws_on_connect: bool,
}

impl AlpacaExecutionClient {
    /// Creates a new [`AlpacaExecutionClient`] from a validated [`AlpacaConfig`].
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError`] if configuration validation or HTTP/WS client construction fails.
    pub fn new(
        trader_id: TraderId,
        client_id: ClientId,
        account_id: AccountId,
        config: AlpacaConfig,
        cache: impl Into<CacheView>,
    ) -> AlpacaResult<Self> {
        config.validate()?;
        let http_client = Arc::new(AlpacaHttpClient::new(config.clone())?);
        let venue = Venue::from(config.venue.as_str());
        let clock = get_atomic_clock_realtime();
        let core = ExecutionClientCore::new(
            trader_id,
            client_id,
            venue,
            OmsType::Netting,
            account_id,
            AccountType::Margin,
            Some(Currency::USD()),
            cache,
        );
        let mut emitter = ExecutionEventEmitter::new(
            clock,
            trader_id,
            account_id,
            AccountType::Margin,
            Some(Currency::USD()),
        );
        if let Some(sender) = try_get_exec_event_sender() {
            emitter.set_sender(sender);
        }
        let tracking = SharedExecutionTracking::default();
        let ws_observer: Arc<dyn AlpacaWsLifecycleObserver> = Arc::new(AlpacaExecutionWsObserver {
            http_client: Arc::clone(&http_client),
            emitter: emitter.clone(),
            tracking: tracking.clone(),
            clock,
            reconnecting: AtomicBool::new(false),
        });
        let ws_client = AlpacaWsClient::new(
            config.clone(),
            account_id,
            DEFAULT_EQUITY_PRICE_PRECISION,
            DEFAULT_EQUITY_SIZE_PRECISION,
            Some(ws_observer),
        )?;

        Ok(Self {
            core,
            config,
            http_client,
            ws_client: Some(ws_client),
            ws_dispatch_task: None,
            emitter,
            clock,
            tracking,
            connect_ws_on_connect: true,
        })
    }

    /// Installs or replaces the execution event sender on the client's [`ExecutionEventEmitter`].
    pub fn set_sender(&mut self, sender: impl Into<EventSender<ExecutionEvent>>) {
        self.emitter.set_sender(sender);
    }

    /// Configures whether `connect()` dials the WebSocket `trade_updates` stream in addition
    /// to verifying the REST `/v2/account` endpoint.
    #[must_use]
    pub const fn with_ws_enabled(mut self, enabled: bool) -> Self {
        self.connect_ws_on_connect = enabled;
        self
    }

    /// Returns a reference to the underlying [`AlpacaHttpClient`].
    #[must_use]
    pub fn http_client(&self) -> &AlpacaHttpClient {
        &self.http_client
    }

    /// Returns a reference to the [`ExecutionEventEmitter`].
    #[must_use]
    pub const fn emitter(&self) -> &ExecutionEventEmitter {
        &self.emitter
    }

    /// Returns the canonical [`ClientOrderId`] mapped to `venue_order_id`, if known.
    #[must_use]
    pub fn client_order_id_for_venue(&self, venue_order_id: &VenueOrderId) -> Option<ClientOrderId> {
        self.tracking.client_order_id_for_venue(venue_order_id)
    }

    /// Records a [`VenueOrderId`] mapping for a canonical [`ClientOrderId`].
    pub fn record_venue_order_id(
        &self,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
    ) {
        self.tracking
            .record_venue_order_id(client_order_id, venue_order_id);
    }

    /// Processes a decoded [`AlpacaExecutionUpdate`] (from `trade_updates` WebSocket or DST harness),
    /// applying `FifoCache` deduplication and emitting normalized order/fill events.
    pub fn ingest_execution_update(&self, update: AlpacaExecutionUpdate) {
        if let Some(replaces_vid) = update.replaces
            && self.tracking.client_order_id_for_venue(&replaces_vid).is_none()
        {
            if let Some(cid) = self.core.cache().client_order_id(&replaces_vid).copied() {
                self.tracking.record_venue_order_id(cid, replaces_vid);
                if let Ok(order) = self.core.get_order(&cid) {
                    self.tracking.track_order(&order);
                }
            } else {
                for order in self.core.cache().orders(None, None, None, None, None) {
                    if order.venue_order_id() == Some(replaces_vid) {
                        let cid = order.client_order_id();
                        self.tracking.track_order(&order);
                        self.tracking.record_venue_order_id(cid, replaces_vid);
                        break;
                    }
                }
            }
        }
        if self
            .tracking
            .client_order_id_for_venue(&update.venue_order_id)
            .is_none()
            && let Some(cid) = self
                .core
                .cache()
                .client_order_id(&update.venue_order_id)
                .copied()
        {
            self.tracking
                .record_venue_order_id(cid, update.venue_order_id);
            if let Ok(order) = self.core.get_order(&cid) {
                self.tracking.track_order(&order);
            }
        }
        let lookup_cid = update
            .replaces
            .and_then(|r_vid| self.tracking.client_order_id_for_venue(&r_vid))
            .or_else(|| self.tracking.client_order_id_for_venue(&update.venue_order_id))
            .unwrap_or_else(|| self.tracking.resolve_client_order_id(update.client_order_id));
        if let Ok(order) = self.core.get_order(&lookup_cid) {
            self.tracking.track_order(&order);
        }
        Self::dispatch_ws_update(&self.emitter, &self.tracking, update);
    }

    fn dispatch_ws_update(
        emitter: &ExecutionEventEmitter,
        tracking: &SharedExecutionTracking,
        mut update: AlpacaExecutionUpdate,
    ) {
        let resolved_cid = update
            .replaces
            .and_then(|replaces_vid| tracking.client_order_id_for_venue(&replaces_vid))
            .or_else(|| tracking.client_order_id_for_venue(&update.venue_order_id))
            .or_else(|| {
                let mapped = tracking.resolve_client_order_id(update.client_order_id);
                if mapped != update.client_order_id {
                    Some(mapped)
                } else {
                    None
                }
            });

        if let Some(canonical_cid) = resolved_cid {
            tracking.record_client_order_id_alias(update.client_order_id, canonical_cid);
            if let Some(replaces_vid) = update.replaces {
                tracking.record_venue_order_id(canonical_cid, replaces_vid);
            }
            tracking.record_venue_order_id(canonical_cid, update.venue_order_id);
            update.client_order_id = canonical_cid;
            update.order_report.client_order_id = Some(canonical_cid);
            if let Some(ref mut fill) = update.fill_report {
                fill.client_order_id = Some(canonical_cid);
            }
        } else {
            if let Some(replaces_vid) = update.replaces {
                tracking.record_venue_order_id(update.client_order_id, replaces_vid);
            }
            tracking.record_venue_order_id(update.client_order_id, update.venue_order_id);
        }
        let cached_order = tracking
            .cached_orders
            .lock()
            .get(&update.client_order_id)
            .cloned();

        match update.event_kind {
            AlpacaTradeEventKind::New
            | AlpacaTradeEventKind::Calculated
            | AlpacaTradeEventKind::Stopped => {
                if update.replaces.is_some()
                    && tracking
                        .emitted_updates
                        .lock()
                        .contains_key(&update.venue_order_id)
                {
                    tracking
                        .terminal_events
                        .lock()
                        .add((update.venue_order_id, AlpacaTradeEventKind::New));
                    if tracking.claim_order_updated(
                        update.venue_order_id,
                        update.order_qty,
                        update.order_report.price,
                        update.order_report.trigger_price,
                    ) {
                        if let Some(ref order) = cached_order {
                            emitter.emit_order_updated(
                                order,
                                update.venue_order_id,
                                update.order_qty,
                                update.order_report.price,
                                update.order_report.trigger_price,
                                None,
                                update.ts_event,
                            );
                        } else {
                            emitter.send_order_status_report(update.order_report);
                        }
                    }
                    return;
                }
                let key = (update.venue_order_id, AlpacaTradeEventKind::New);
                if !tracking.terminal_events.lock().insert(key) {
                    log::debug!(
                        "Skipping duplicate Alpaca OrderAccepted venue_order_id={}",
                        update.venue_order_id
                    );
                    return;
                }
                if let Some(ref order) = cached_order {
                    emitter.emit_order_accepted(order, update.venue_order_id, update.ts_event);
                } else {
                    emitter.send_order_status_report(update.order_report);
                }
            }
            AlpacaTradeEventKind::Fill | AlpacaTradeEventKind::PartialFill => {
                let Some((trade_id, emit_qty)) = tracking.claim_ws_fill(
                    update.venue_order_id,
                    update.client_order_id,
                    update.trade_id,
                    update.last_qty,
                    update.last_px,
                    update.cum_qty,
                    update.order_qty,
                    DEFAULT_EQUITY_SIZE_PRECISION,
                ) else {
                    log::debug!(
                        "Skipping duplicate or already-reconciled Alpaca fill for venue_order_id={} trade_id={:?}",
                        update.venue_order_id,
                        update.trade_id
                    );
                    return;
                };

                {
                    let mut term = tracking.terminal_events.lock();
                    term.add((update.venue_order_id, AlpacaTradeEventKind::New));
                    if update.event_kind == AlpacaTradeEventKind::Fill {
                        term.add((update.venue_order_id, AlpacaTradeEventKind::Fill));
                    }
                }

                if let (Some(order), Some(last_px)) = (cached_order.as_ref(), update.last_px) {
                    let commission = Money::from_decimal(Decimal::ZERO, Currency::USD()).ok();
                    emitter.emit_order_filled(
                        order,
                        update.venue_order_id,
                        None,
                        trade_id,
                        emit_qty,
                        last_px,
                        Currency::USD(),
                        commission,
                        LiquiditySide::Taker,
                        update.ts_event,
                    );
                } else if let Some(mut fill_report) = update.fill_report {
                    fill_report.trade_id = trade_id;
                    fill_report.last_qty = emit_qty;
                    emitter.send_order_with_fills(update.order_report, vec![fill_report]);
                } else {
                    emitter.send_order_status_report(update.order_report);
                }
            }
            AlpacaTradeEventKind::Canceled | AlpacaTradeEventKind::DoneForDay => {
                let key = (update.venue_order_id, AlpacaTradeEventKind::Canceled);
                {
                    let mut term = tracking.terminal_events.lock();
                    term.add((update.venue_order_id, AlpacaTradeEventKind::New));
                    if !term.insert(key) {
                        log::debug!(
                            "Skipping duplicate Alpaca cancel event for venue_order_id={}",
                            update.venue_order_id
                        );
                        return;
                    }
                }
                if let Some(ref order) = cached_order {
                    emitter.emit_order_canceled(
                        order,
                        Some(update.venue_order_id),
                        update.ts_event,
                    );
                } else {
                    emitter.send_order_status_report(update.order_report);
                }
            }
            AlpacaTradeEventKind::Expired => {
                let key = (update.venue_order_id, AlpacaTradeEventKind::Expired);
                {
                    let mut term = tracking.terminal_events.lock();
                    term.add((update.venue_order_id, AlpacaTradeEventKind::New));
                    if !term.insert(key) {
                        return;
                    }
                }
                if let Some(ref order) = cached_order {
                    emitter.emit_order_expired(
                        order,
                        Some(update.venue_order_id),
                        update.ts_event,
                    );
                } else {
                    emitter.send_order_status_report(update.order_report);
                }
            }
            AlpacaTradeEventKind::Rejected | AlpacaTradeEventKind::Suspended => {
                let key = (update.venue_order_id, AlpacaTradeEventKind::Rejected);
                {
                    let mut term = tracking.terminal_events.lock();
                    term.add((update.venue_order_id, AlpacaTradeEventKind::New));
                    if !term.insert(key) {
                        return;
                    }
                }
                if let Some(ref order) = cached_order {
                    emitter.emit_order_rejected(
                        order,
                        "alpaca_ws_rejected",
                        update.ts_event,
                        false,
                    );
                } else if let Some(ctx) = tracking
                    .order_contexts
                    .lock()
                    .get(&update.client_order_id)
                    .copied()
                {
                    emitter.emit_order_rejected_event(
                        ctx.identity.strategy_id,
                        ctx.identity.instrument_id,
                        ctx.identity.client_order_id,
                        "alpaca_ws_rejected",
                        update.ts_event,
                        false,
                    );
                } else {
                    emitter.send_order_status_report(update.order_report);
                }
            }
            AlpacaTradeEventKind::Replaced => {
                if let Some(replaces_vid) = update.replaces {
                    tracking
                        .terminal_events
                        .lock()
                        .add((replaces_vid, AlpacaTradeEventKind::Canceled));
                }
                // On Alpaca's `trade_updates` WebSocket, `event: "replaced"` carries the OLD
                // replaced order snapshot (`id = old_vid`, `replaced_by = new_vid`). If the
                // replacement order `new_vid` has already emitted its authoritative `OrderUpdated`
                // (e.g. from the HTTP `PATCH` response), do NOT emit a rollback `OrderUpdated`
                // with the old order's stale `qty` / `stop_price`.
                if tracking
                    .emitted_updates
                    .lock()
                    .contains_key(&update.venue_order_id)
                {
                    return;
                }
                let pending = tracking
                    .pending_modifies
                    .lock()
                    .shift_remove(&update.client_order_id);
                let (eff_qty, eff_price, eff_trigger) = match pending {
                    Some((req_qty, req_px, req_trig)) => (
                        req_qty.unwrap_or(update.order_qty),
                        req_px.or(update.order_report.price),
                        req_trig.or(update.order_report.trigger_price),
                    ),
                    None => (
                        update.order_qty,
                        update.order_report.price,
                        update.order_report.trigger_price,
                    ),
                };
                if !tracking.claim_order_updated(
                    update.venue_order_id,
                    eff_qty,
                    eff_price,
                    eff_trigger,
                ) {
                    return;
                }
                if let Some(ref order) = cached_order {
                    emitter.emit_order_updated(
                        order,
                        update.venue_order_id,
                        eff_qty,
                        eff_price,
                        eff_trigger,
                        None,
                        update.ts_event,
                    );
                } else {
                    update.order_report.quantity = eff_qty;
                    update.order_report.price = eff_price;
                    update.order_report.trigger_price = eff_trigger;
                    emitter.send_order_status_report(update.order_report);
                }
            }
            AlpacaTradeEventKind::OrderCancelRejected => {
                if let Some(ref order) = cached_order {
                    emitter.emit_order_cancel_rejected(
                        order,
                        Some(update.venue_order_id),
                        "alpaca_ws_order_cancel_rejected",
                        update.ts_event,
                    );
                } else if let Some(ctx) = tracking
                    .order_contexts
                    .lock()
                    .get(&update.client_order_id)
                    .copied()
                {
                    emitter.emit_order_cancel_rejected_event(
                        ctx.identity.strategy_id,
                        ctx.identity.instrument_id,
                        ctx.identity.client_order_id,
                        Some(update.venue_order_id),
                        "alpaca_ws_order_cancel_rejected",
                        update.ts_event,
                    );
                } else {
                    emitter.send_order_status_report(update.order_report);
                }
            }
            AlpacaTradeEventKind::OrderReplaceRejected => {
                if let Some(ref order) = cached_order {
                    emitter.emit_order_modify_rejected(
                        order,
                        Some(update.venue_order_id),
                        "alpaca_ws_order_replace_rejected",
                        update.ts_event,
                    );
                } else if let Some(ctx) = tracking
                    .order_contexts
                    .lock()
                    .get(&update.client_order_id)
                    .copied()
                {
                    emitter.emit_order_modify_rejected_event(
                        ctx.identity.strategy_id,
                        ctx.identity.instrument_id,
                        ctx.identity.client_order_id,
                        Some(update.venue_order_id),
                        "alpaca_ws_order_replace_rejected",
                        update.ts_event,
                    );
                } else {
                    emitter.send_order_status_report(update.order_report);
                }
            }
            AlpacaTradeEventKind::PendingNew
            | AlpacaTradeEventKind::PendingCancel
            | AlpacaTradeEventKind::PendingReplace => {}
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_order_response_after_cancel_attempt(
        emitter: &ExecutionEventEmitter,
        tracking: &SharedExecutionTracking,
        clock: &'static AtomicTime,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        active_vid: VenueOrderId,
        cached_order: Option<&OrderAny>,
        resp: &AlpacaOrderResponse,
    ) -> bool {
        let ts_done = clock.get_time_ns();
        if resp.filled_qty > Decimal::ZERO
            && let Some(avg_px) = resp
                .filled_avg_price
                .filter(|p| *p > Decimal::ZERO)
                .or_else(|| resp.limit_price.filter(|p| *p > Decimal::ZERO))
                .or_else(|| resp.stop_price.filter(|p| *p > Decimal::ZERO))
            && let Ok(Some((trade_id, delta_qty, delta_px))) = tracking.claim_submitted_order_fill(
                active_vid,
                Some(client_order_id),
                resp.filled_qty,
                avg_px,
                DEFAULT_EQUITY_PRICE_PRECISION,
                DEFAULT_EQUITY_SIZE_PRECISION,
            )
            && let Some(order) = cached_order
        {
            let commission = Money::from_decimal(Decimal::ZERO, Currency::USD()).ok();
            emitter.emit_order_filled(
                order,
                active_vid,
                None,
                trade_id,
                delta_qty,
                delta_px,
                Currency::USD(),
                commission,
                LiquiditySide::Taker,
                ts_done,
            );
        }

        match resp.status.trim().to_ascii_lowercase().as_str() {
            "canceled" | "cancelled" | "done_for_day" => {
                let key = (active_vid, AlpacaTradeEventKind::Canceled);
                if tracking.terminal_events.lock().insert(key)
                    && let Some(order) = cached_order
                {
                    emitter.emit_order_canceled(order, Some(active_vid), ts_done);
                }
                true
            }
            "filled" => {
                tracking
                    .terminal_events
                    .lock()
                    .add((active_vid, AlpacaTradeEventKind::Fill));
                emitter.emit_order_cancel_rejected_event(
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    Some(active_vid),
                    "order already filled on venue before cancel completed",
                    ts_done,
                );
                true
            }
            "expired" => {
                let key = (active_vid, AlpacaTradeEventKind::Expired);
                if tracking.terminal_events.lock().insert(key)
                    && let Some(order) = cached_order
                {
                    emitter.emit_order_expired(order, Some(active_vid), ts_done);
                }
                true
            }
            "rejected" | "suspended" => {
                tracking
                    .terminal_events
                    .lock()
                    .add((active_vid, AlpacaTradeEventKind::Rejected));
                emitter.emit_order_cancel_rejected_event(
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    Some(active_vid),
                    "order already rejected or suspended on venue",
                    ts_done,
                );
                true
            }
            _ => false,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn reconcile_post_cancel_order(
        http: &Arc<AlpacaHttpClient>,
        emitter: &ExecutionEventEmitter,
        tracking: &SharedExecutionTracking,
        clock: &'static AtomicTime,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        active_vid: VenueOrderId,
        cached_order: Option<OrderAny>,
        _ws_active: bool,
    ) {
        if tracking.has_terminal_order_event(active_vid) {
            return;
        }
        let vid_str = active_vid.as_str().to_string();
        match http.get_order(&vid_str).await {
            Ok(resp) => {
                if Self::apply_order_response_after_cancel_attempt(
                    emitter,
                    tracking,
                    clock,
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    active_vid,
                    cached_order.as_ref(),
                    &resp,
                ) {
                    return;
                }
            }
            Err(e) => {
                log::warn!(
                    "Alpaca post-cancel get_order({vid_str}) failed for {client_order_id}: {e}; scheduling bounded confirmation retry without optimistic OrderCanceled"
                );
            }
        }
        // Order is in `pending_cancel` or initial `get_order` failed transiently.
        // Wait for WebSocket `trade_updates` with a bounded REST confirmation safety net.
        let http_bg = Arc::clone(http);
        let emitter_bg = emitter.clone();
        let tracking_bg = tracking.clone();
        get_runtime().spawn(async move {
            for _ in 0..10 {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                if tracking_bg.has_terminal_order_event(active_vid) {
                    return;
                }
                if let Ok(poll_resp) = http_bg.get_order(&vid_str).await
                    && Self::apply_order_response_after_cancel_attempt(
                        &emitter_bg,
                        &tracking_bg,
                        clock,
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        active_vid,
                        cached_order.as_ref(),
                        &poll_resp,
                    )
                {
                    return;
                }
            }
        });
    }

    fn build_single_order_request(order: &OrderAny) -> AlpacaResult<AlpacaOrderRequest> {
        let side = AlpacaOrderSide::try_from(order.order_side())?;
        let order_type = AlpacaOrderType::try_from(order.order_type())?;
        let time_in_force = AlpacaTimeInForce::try_from(order.time_in_force())?;
        let qty = order.quantity().as_decimal();

        let limit_price = order.price().map(|p| p.as_decimal());
        let stop_price = order.trigger_price().map(|p| p.as_decimal());

        let (trail_price, trail_percent) = if order.order_type() == OrderType::TrailingStopMarket {
            let offset = order.trailing_offset().ok_or_else(|| {
                AlpacaError::Parse(format!(
                    "trailing stop order {} requires a trailing_offset",
                    order.client_order_id()
                ))
            })?;
            match order.trailing_offset_type() {
                Some(TrailingOffsetType::Price) | None => (Some(offset), None),
                Some(TrailingOffsetType::BasisPoints) => {
                    (None, Some(offset / Decimal::from(100)))
                }
                Some(other) => {
                    return Err(AlpacaError::UnsupportedOrderType(format!(
                        "unsupported trailing_offset_type {other:?} for order {}",
                        order.client_order_id()
                    )));
                }
            }
        } else {
            (None, None)
        };

        Ok(AlpacaOrderRequest {
            symbol: order.instrument_id().symbol.as_str().to_string(),
            qty,
            side,
            order_type,
            time_in_force,
            limit_price,
            stop_price,
            trail_price,
            trail_percent,
            extended_hours: false,
            client_order_id: order.client_order_id().as_str().to_string(),
            order_class: Some(AlpacaOrderClass::Simple),
            take_profit: None,
            stop_loss: None,
        })
    }

    fn extract_take_profit_spec(leg: &OrderAny) -> AlpacaResult<AlpacaTakeProfitSpec> {
        let limit_px = leg.price().ok_or_else(|| {
            AlpacaError::Parse(format!(
                "take-profit leg {} requires a limit price",
                leg.client_order_id()
            ))
        })?;
        Ok(AlpacaTakeProfitSpec {
            limit_price: limit_px.as_decimal(),
        })
    }

    fn extract_stop_loss_spec(leg: &OrderAny) -> AlpacaResult<AlpacaStopLossSpec> {
        let stop_px = leg.trigger_price().ok_or_else(|| {
            AlpacaError::Parse(format!(
                "stop-loss leg {} requires a trigger_price",
                leg.client_order_id()
            ))
        })?;
        let limit_price = leg.price().map(|p| p.as_decimal());
        Ok(AlpacaStopLossSpec {
            stop_price: stop_px.as_decimal(),
            limit_price,
        })
    }

    fn build_order_list_request(orders: &[OrderAny]) -> AlpacaResult<AlpacaOrderRequest> {
        match orders.len() {
            1 => Self::build_single_order_request(&orders[0]),
            2 => {
                let first = &orders[0];
                let second = &orders[1];
                if first.order_side() == second.order_side() {
                    // OCO exit pair: one Limit take-profit leg + one Stop/StopLimit stop-loss leg
                    let (tp_leg, sl_leg) = match (first.order_type(), second.order_type()) {
                        (OrderType::Limit, OrderType::StopMarket | OrderType::StopLimit) => {
                            (first, second)
                        }
                        (OrderType::StopMarket | OrderType::StopLimit, OrderType::Limit) => {
                            (second, first)
                        }
                        (t1, t2) => {
                            return Err(AlpacaError::Parse(format!(
                                "OCO OrderList requires one Limit leg and one StopMarket/StopLimit leg, got ({t1}, {t2})"
                            )));
                        }
                    };
                    let tp_spec = Self::extract_take_profit_spec(tp_leg)?;
                    let sl_spec = Self::extract_stop_loss_spec(sl_leg)?;
                    let side = AlpacaOrderSide::try_from(first.order_side())?;
                    let time_in_force = AlpacaTimeInForce::try_from(first.time_in_force())?;
                    Ok(AlpacaOrderRequest {
                        symbol: first.instrument_id().symbol.as_str().to_string(),
                        qty: first.quantity().as_decimal(),
                        side,
                        order_type: AlpacaOrderType::Limit,
                        time_in_force,
                        limit_price: None,
                        stop_price: None,
                        trail_price: None,
                        trail_percent: None,
                        extended_hours: false,
                        client_order_id: first.client_order_id().as_str().to_string(),
                        order_class: Some(AlpacaOrderClass::Oco),
                        take_profit: Some(tp_spec),
                        stop_loss: Some(sl_spec),
                    })
                } else {
                    // OTO pair: parent entry order + contingent exit leg (take-profit or stop-loss)
                    let mut req = Self::build_single_order_request(first)?;
                    req.order_class = Some(AlpacaOrderClass::Oto);
                    match second.order_type() {
                        OrderType::Limit => {
                            req.take_profit = Some(Self::extract_take_profit_spec(second)?);
                        }
                        OrderType::StopMarket | OrderType::StopLimit => {
                            req.stop_loss = Some(Self::extract_stop_loss_spec(second)?);
                        }
                        other => {
                            return Err(AlpacaError::Parse(format!(
                                "OTO child leg must be Limit, StopMarket, or StopLimit, got {other}"
                            )));
                        }
                    }
                    Ok(req)
                }
            }
            3 => {
                // Bracket order: parent entry order + take-profit Limit leg + stop-loss Stop/StopLimit leg
                let parent = &orders[0];
                let leg_a = &orders[1];
                let leg_b = &orders[2];
                let (tp_leg, sl_leg) = match (leg_a.order_type(), leg_b.order_type()) {
                    (OrderType::Limit, OrderType::StopMarket | OrderType::StopLimit) => {
                        (leg_a, leg_b)
                    }
                    (OrderType::StopMarket | OrderType::StopLimit, OrderType::Limit) => {
                        (leg_b, leg_a)
                    }
                    (t1, t2) => {
                        return Err(AlpacaError::Parse(format!(
                            "Bracket OrderList child legs must be one Limit and one StopMarket/StopLimit, got ({t1}, {t2})"
                        )));
                    }
                };
                let mut req = Self::build_single_order_request(parent)?;
                req.order_class = Some(AlpacaOrderClass::Bracket);
                req.take_profit = Some(Self::extract_take_profit_spec(tp_leg)?);
                req.stop_loss = Some(Self::extract_stop_loss_spec(sl_leg)?);
                Ok(req)
            }
            n => Err(AlpacaError::Parse(format!(
                "Alpaca OrderList supports 1..=3 orders (Simple/OCO/OTO/Bracket), got {n} orders"
            ))),
        }
    }

    fn flatten_order_responses(orders: Vec<AlpacaOrderResponse>) -> Vec<AlpacaOrderResponse> {
        let mut flat = Vec::with_capacity(orders.len());
        for order in orders {
            let legs = order.legs.clone();
            flat.push(order);
            if let Some(child_legs) = legs {
                for leg in child_legs {
                    flat.push(leg);
                }
            }
        }
        flat
    }

    fn seed_order_responses_lineage(&self, flat_orders: &[AlpacaOrderResponse]) {
        for resp in flat_orders.iter().rev() {
            if resp.replaces.is_none()
                && !resp.client_order_id.trim().is_empty()
                && let Ok(cid) = ClientOrderId::new_checked(resp.client_order_id.trim())
            {
                let vid = VenueOrderId::new(resp.id.trim());
                self.tracking.record_venue_order_id(cid, vid);
                if let Ok(order) = self.core.get_order(&cid) {
                    self.tracking.track_order(&order);
                }
            }
        }
        for resp in flat_orders.iter().rev() {
            if let Some(ref prev_id) = resp.replaces
                && !prev_id.trim().is_empty()
            {
                let prev_vid = VenueOrderId::new(prev_id.trim());
                let vid = VenueOrderId::new(resp.id.trim());
                let canonical_cid = self
                    .tracking
                    .client_order_id_for_venue(&prev_vid)
                    .or_else(|| self.core.cache().client_order_id(&prev_vid).copied())
                    .or_else(|| {
                        let raw = resp.client_order_id.trim();
                        if raw.is_empty() {
                            None
                        } else {
                            ClientOrderId::new_checked(raw).ok()
                        }
                    });
                if let Some(cid) = canonical_cid {
                    self.tracking.record_venue_order_id(cid, prev_vid);
                    self.tracking.record_venue_order_id(cid, vid);
                    if let Ok(order) = self.core.get_order(&cid) {
                        self.tracking.track_order(&order);
                    }
                    if !resp.client_order_id.trim().is_empty()
                        && let Ok(alias_cid) =
                            ClientOrderId::new_checked(resp.client_order_id.trim())
                    {
                        self.tracking.record_client_order_id_alias(alias_cid, cid);
                    }
                }
            }
        }
    }

    pub async fn reconcile_open_orders(&self) {
        Self::reconcile_tracked_orders_once(
            &self.http_client,
            &self.emitter,
            &self.tracking,
            self.clock,
        )
        .await;
    }

    async fn reconcile_tracked_orders_once(
        http: &Arc<AlpacaHttpClient>,
        emitter: &ExecutionEventEmitter,
        tracking: &SharedExecutionTracking,
        clock: &'static AtomicTime,
    ) {
        let tracked: Vec<(ClientOrderId, OrderAny)> = tracking
            .cached_orders
            .lock()
            .iter()
            .map(|(cid, order)| (*cid, order.clone()))
            .collect();
        for (cid, order) in tracked {
            let Some(vid) = tracking.current_venue_order_id(&cid) else {
                continue;
            };
            if tracking.has_terminal_order_event(vid) {
                continue;
            }
            let Ok(resp) = http.get_order(vid.as_str()).await else {
                continue;
            };
            let ts_now = clock.get_time_ns();
            if resp.status.trim().eq_ignore_ascii_case("replaced")
                && let Some(ref new_id) = resp.replaced_by
                && !new_id.trim().is_empty()
            {
                tracking
                    .terminal_events
                    .lock()
                    .add((vid, AlpacaTradeEventKind::Canceled));
                let new_vid = VenueOrderId::new(new_id.trim());
                tracking.record_venue_order_id(cid, new_vid);
                if let Ok(new_resp) = http.get_order(new_vid.as_str()).await {
                    let new_qty = new_resp
                        .qty
                        .and_then(|d| decimal_to_quantity(d, DEFAULT_EQUITY_SIZE_PRECISION).ok())
                        .unwrap_or_else(|| order.quantity());
                    let new_px = new_resp
                        .limit_price
                        .and_then(|d| decimal_to_price(d, DEFAULT_EQUITY_PRICE_PRECISION).ok())
                        .or_else(|| order.price());
                    let new_trigger = new_resp
                        .stop_price
                        .and_then(|d| decimal_to_price(d, DEFAULT_EQUITY_PRICE_PRECISION).ok())
                        .or_else(|| order.trigger_price());
                    if tracking.claim_order_updated(new_vid, new_qty, new_px, new_trigger) {
                        emitter.emit_order_updated(
                            &order,
                            new_vid,
                            new_qty,
                            new_px,
                            new_trigger,
                            None,
                            ts_now,
                        );
                    }
                    Self::reconcile_submitted_order_response_with(
                        emitter,
                        tracking,
                        &order,
                        &new_resp,
                        ts_now,
                    );
                    continue;
                }
            }
            Self::reconcile_submitted_order_response_with(emitter, tracking, &order, &resp, ts_now);
        }
    }

    fn resolve_order_response_lineage(
        &self,
        resp: &mut AlpacaOrderResponse,
    ) -> Option<ClientOrderId> {
        let vid = VenueOrderId::new(resp.id.trim());
        let replaces_vid = resp
            .replaces
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(VenueOrderId::new);

        let canonical_cid = replaces_vid
            .and_then(|r_vid| {
                self.tracking
                    .client_order_id_for_venue(&r_vid)
                    .or_else(|| self.core.cache().client_order_id(&r_vid).copied())
            })
            .or_else(|| {
                self.tracking
                    .client_order_id_for_venue(&vid)
                    .or_else(|| self.core.cache().client_order_id(&vid).copied())
            })
            .or_else(|| {
                let raw = resp.client_order_id.trim();
                if raw.is_empty() {
                    None
                } else {
                    ClientOrderId::new_checked(raw)
                        .ok()
                        .map(|cid| self.tracking.resolve_client_order_id(cid))
                }
            });

        if let Some(cid) = canonical_cid {
            if !resp.client_order_id.trim().is_empty()
                && let Ok(alias_cid) = ClientOrderId::new_checked(resp.client_order_id.trim())
            {
                self.tracking.record_client_order_id_alias(alias_cid, cid);
            }
            if let Some(r_vid) = replaces_vid {
                self.tracking.record_venue_order_id(cid, r_vid);
            }
            self.tracking.record_venue_order_id(cid, vid);
            resp.client_order_id = cid.as_str().to_string();
        }
        canonical_cid
    }

    fn reconcile_submitted_order_response_with(
        emitter: &ExecutionEventEmitter,
        tracking: &SharedExecutionTracking,
        order: &OrderAny,
        resp: &AlpacaOrderResponse,
        ts_now: UnixNanos,
    ) {
        let cid = order.client_order_id();
        let venue_order_id = VenueOrderId::new(resp.id.trim());
        tracking.record_venue_order_id(cid, venue_order_id);

        let ts_accepted = resp
            .submitted_at
            .as_deref()
            .or(Some(resp.created_at.as_str()))
            .and_then(|s| parse_rfc3339_to_unix_nanos(s).ok())
            .unwrap_or(ts_now);

        let status = parse_alpaca_order_status(&resp.status).unwrap_or(OrderStatus::Accepted);

        if status == OrderStatus::Rejected {
            if tracking
                .terminal_events
                .lock()
                .insert((venue_order_id, AlpacaTradeEventKind::Rejected))
            {
                emitter.emit_order_rejected(
                    order,
                    &format!("Broker order {} status is rejected", resp.id.trim()),
                    ts_accepted,
                    false,
                );
            }
            return;
        }

        if tracking
            .terminal_events
            .lock()
            .insert((venue_order_id, AlpacaTradeEventKind::New))
        {
            emitter.emit_order_accepted(order, venue_order_id, ts_accepted);
        }

        let resolved_avg_px = resp
            .filled_avg_price
            .filter(|p| *p > Decimal::ZERO)
            .or_else(|| resp.limit_price.filter(|p| *p > Decimal::ZERO))
            .or_else(|| resp.stop_price.filter(|p| *p > Decimal::ZERO))
            .or_else(|| order.price().map(|p| p.as_decimal()).filter(|p| *p > Decimal::ZERO))
            .or_else(|| {
                order
                    .trigger_price()
                    .map(|p| p.as_decimal())
                    .filter(|p| *p > Decimal::ZERO)
            });

        if resp.filled_qty > Decimal::ZERO
            && let Some(avg_px_dec) = resolved_avg_px
            && let Ok(Some((trade_id, fill_qty, fill_px))) = tracking.claim_submitted_order_fill(
                venue_order_id,
                Some(cid),
                resp.filled_qty,
                avg_px_dec,
                DEFAULT_EQUITY_PRICE_PRECISION,
                DEFAULT_EQUITY_SIZE_PRECISION,
            )
        {
            let ts_filled = resp
                .filled_at
                .as_deref()
                .or(resp.updated_at.as_deref())
                .or(Some(resp.created_at.as_str()))
                .and_then(|s| parse_rfc3339_to_unix_nanos(s).ok())
                .unwrap_or(ts_now);
            if status == OrderStatus::Filled {
                tracking
                    .terminal_events
                    .lock()
                    .add((venue_order_id, AlpacaTradeEventKind::Fill));
            }
            let commission = Money::from_decimal(Decimal::ZERO, Currency::USD()).ok();
            let liquidity_side = match order.order_type() {
                OrderType::Limit => LiquiditySide::Maker,
                _ => LiquiditySide::Taker,
            };
            emitter.emit_order_filled(
                order,
                venue_order_id,
                None,
                trade_id,
                fill_qty,
                fill_px,
                Currency::USD(),
                commission,
                liquidity_side,
                ts_filled,
            );
        }

        match status {
            OrderStatus::Canceled => {
                if tracking
                    .terminal_events
                    .lock()
                    .insert((venue_order_id, AlpacaTradeEventKind::Canceled))
                {
                    let ts_canceled = resp
                        .canceled_at
                        .as_deref()
                        .or(resp.updated_at.as_deref())
                        .and_then(|s| parse_rfc3339_to_unix_nanos(s).ok())
                        .unwrap_or(ts_now);
                    emitter.emit_order_canceled(order, Some(venue_order_id), ts_canceled);
                }
            }
            OrderStatus::Expired => {
                if tracking
                    .terminal_events
                    .lock()
                    .insert((venue_order_id, AlpacaTradeEventKind::Expired))
                {
                    let ts_expired = resp
                        .expired_at
                        .as_deref()
                        .or(resp.updated_at.as_deref())
                        .and_then(|s| parse_rfc3339_to_unix_nanos(s).ok())
                        .unwrap_or(ts_now);
                    emitter.emit_order_expired(order, Some(venue_order_id), ts_expired);
                }
            }
            _ => {}
        }
    }
}

#[async_trait(?Send)]
impl ExecutionClient for AlpacaExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        self.core.venue
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core
            .cache()
            .account(&self.core.account_id)
            .map(|a| (*a).clone())
    }

    fn retain_unresolved_submissions(&self) -> bool {
        true
    }

    fn venue_order_ids(
        &self,
        _mass_status: &ExecutionMassStatus,
    ) -> Option<IndexMap<ClientOrderId, Vec<VenueOrderId>>> {
        let guard = self.tracking.venue_order_ids.lock();
        let multi: IndexMap<ClientOrderId, Vec<VenueOrderId>> = guard
            .iter()
            .filter(|(_, ids)| ids.len() >= 2)
            .map(|(cid, ids)| (*cid, ids.clone()))
            .collect();
        if multi.is_empty() {
            None
        } else {
            Some(multi)
        }
    }

    fn handles_order_venue(&self, venue: Venue) -> bool {
        venue == self.core.venue
            || venue.as_str() == "XNAS"
            || venue.as_str() == "XNYS"
            || venue.as_str() == "ARCX"
            || venue.as_str() == ALPACA_VENUE
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
        info: Option<Params>,
    ) -> anyhow::Result<()> {
        self.emitter
            .emit_account_state(balances, margins, reported, ts_event, info);
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }
        if let Some(sender) = try_get_exec_event_sender() {
            self.emitter.set_sender(sender);
        }
        self.core.set_started();
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }
        if let Some(task) = self.ws_dispatch_task.take() {
            task.abort();
        }
        self.core.set_disconnected();
        self.core.set_stopped();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.tracking.clear();
        self.core.set_disconnected();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.tracking.clear();
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.core.is_connected() {
            return Ok(());
        }

        let acct_resp = self.http_client.get_account().await?;
        let ts_init = self.clock.get_time_ns();
        let mut account_state = parse_account_state(&acct_resp, &self.config.venue, ts_init)?;
        account_state.account_id = self.core.account_id;
        self.emitter.send_account_state(account_state);

        for order in self.core.cache().orders(None, None, None, None, None) {
            self.tracking.track_order(&order);
        }

        if self.connect_ws_on_connect
            && let Some(ref mut ws) = self.ws_client
        {
            let mut rx = ws.connect().await?;
            let emitter_clone = self.emitter.clone();
            let tracking_clone = self.tracking.clone();
            let handle = get_runtime().spawn(async move {
                while let Some(update) = rx.recv().await {
                    Self::dispatch_ws_update(&emitter_clone, &tracking_clone, update);
                }
            });
            self.ws_dispatch_task = Some(handle);
        }

        self.core.set_connected();
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if let Some(task) = self.ws_dispatch_task.take() {
            task.abort();
        }
        if let Some(ref mut ws) = self.ws_client {
            ws.disconnect().await;
        }
        self.core.set_disconnected();
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = self.core.get_order(&cmd.client_order_id)?;
        let cid = order.client_order_id();
        let instrument_id = order.instrument_id();
        self.tracking.track_order(&order);

        if order.is_reduce_only()
            && matches!(
                order.order_type(),
                OrderType::Market | OrderType::Limit | OrderType::MarketToLimit
            )
        {
            let cache = self.core.cache();
            let open_positions = cache.positions_open(None, Some(&instrument_id), None, None, None);
            if !open_positions.is_empty() {
                let mut net_signed = Decimal::ZERO;
                for pos in open_positions {
                    match pos.side {
                        PositionSide::Long => net_signed += pos.quantity.as_decimal(),
                        PositionSide::Short => net_signed -= pos.quantity.as_decimal(),
                        _ => {}
                    }
                }
                let order_qty = order.quantity().as_decimal();
                let valid_reduce = match order.order_side() {
                    OrderSide::Sell => net_signed > Decimal::ZERO && order_qty <= net_signed,
                    OrderSide::Buy => net_signed < Decimal::ZERO && order_qty <= net_signed.abs(),
                };
                if !valid_reduce {
                    self.emitter.emit_order_denied(
                        &order,
                        "REDUCE_ONLY_POSITION_MISMATCH: reduce_only exit order exceeds or opposes open position",
                    );
                    return Ok(());
                }
            } else if !cache
                .positions_closed(None, Some(&instrument_id), None, None, None)
                .is_empty()
            {
                self.emitter.emit_order_denied(
                    &order,
                    "REDUCE_ONLY_POSITION_CLOSED: position already closed before reduce_only exit submission",
                );
                return Ok(());
            }
        }

        if !self.tracking.submitted_orders.lock().insert(cid) {
            log::warn!("Ignoring duplicate submit_order for client_order_id={cid}");
            return Ok(());
        }

        let req = match Self::build_single_order_request(&order) {
            Ok(r) => r,
            Err(e) => {
                self.emitter.emit_order_denied(&order, &e.to_string());
                return Ok(());
            }
        };

        self.emitter.emit_order_submitted(&order);

        let http = Arc::clone(&self.http_client);
        let emitter = self.emitter.clone();
        let tracking = self.tracking.clone();
        let clock = self.clock;
        let order_clone = order.clone();
        self.tracking
            .spawn_instrument_order_task(instrument_id, async move {
                let result = http.submit_order(&req).await;
                let ts_now = clock.get_time_ns();
                match result {
                    Ok(resp) => {
                        Self::reconcile_submitted_order_response_with(
                            &emitter,
                            &tracking,
                            &order_clone,
                            &resp,
                            ts_now,
                        );
                    }
                    Err(AlpacaError::DeterministicReject { message, .. }) => {
                        emitter.emit_order_rejected(&order_clone, &message, ts_now, false);
                    }
                    Err(other_err) => {
                        // Fail-closed: retain order in Submitted state so reconciliation resolves venue truth
                        log::error!(
                            "Alpaca submit_order transport/auth error for {cid} (retaining unresolved submission): {other_err}"
                        );
                    }
                }
            });
        Ok(())
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        let orders = self.core.get_orders_for_list(&cmd.order_list)?;
        if orders.is_empty() {
            return Ok(());
        }

        let instrument_id = orders[0].instrument_id();
        for order in &orders {
            let cid = order.client_order_id();
            self.tracking.track_order(order);
            if !self.tracking.submitted_orders.lock().insert(cid) {
                anyhow::bail!("Duplicate client_order_id {cid} in order list");
            }
        }

        let req = match Self::build_order_list_request(&orders) {
            Ok(r) => r,
            Err(e) => {
                let reason = e.to_string();
                for order in &orders {
                    self.emitter.emit_order_denied(order, &reason);
                }
                return Ok(());
            }
        };

        for order in &orders {
            self.emitter.emit_order_submitted(order);
        }

        let http = Arc::clone(&self.http_client);
        let emitter = self.emitter.clone();
        let tracking = self.tracking.clone();
        let clock = self.clock;
        let orders_clone = orders.clone();
        let order_list_id = cmd.order_list.id;
        self.tracking
            .spawn_instrument_order_task(instrument_id, async move {
                let result = http.submit_order(&req).await;
                let ts_now = clock.get_time_ns();
                match result {
                    Ok(resp) => {
                        let parent_order = &orders_clone[0];
                        let ts_accepted = resp
                            .submitted_at
                            .as_deref()
                            .or(Some(resp.created_at.as_str()))
                            .and_then(|s| parse_rfc3339_to_unix_nanos(s).ok())
                            .unwrap_or(ts_now);
                        let resp_legs = resp.legs.clone().unwrap_or_default();
                        Self::reconcile_submitted_order_response_with(
                            &emitter,
                            &tracking,
                            parent_order,
                            &resp,
                            ts_now,
                        );

                        let mut used_leg_indices = vec![false; resp_legs.len()];
                        for (idx, child_order) in orders_clone.iter().enumerate().skip(1) {
                            let child_ot = child_order.order_type();
                            let matched_pos = resp_legs
                                .iter()
                                .enumerate()
                                .find(|(leg_idx, leg)| {
                                    !used_leg_indices[*leg_idx]
                                        && OrderType::from(leg.order_type) == child_ot
                                })
                                .map(|(leg_idx, _)| leg_idx)
                                .or_else(|| {
                                    let is_stop_child = matches!(
                                        child_ot,
                                        OrderType::StopMarket
                                            | OrderType::StopLimit
                                            | OrderType::TrailingStopMarket
                                    );
                                    if is_stop_child {
                                        resp_legs
                                            .iter()
                                            .enumerate()
                                            .find(|(leg_idx, leg)| {
                                                !used_leg_indices[*leg_idx]
                                                    && matches!(
                                                        leg.order_type,
                                                        AlpacaOrderType::Stop
                                                            | AlpacaOrderType::StopLimit
                                                            | AlpacaOrderType::TrailingStop
                                                    )
                                            })
                                            .map(|(leg_idx, _)| leg_idx)
                                    } else {
                                        None
                                    }
                                })
                                .or_else(|| {
                                    let fallback_idx = idx - 1;
                                    if fallback_idx < resp_legs.len()
                                        && !used_leg_indices[fallback_idx]
                                    {
                                        Some(fallback_idx)
                                    } else {
                                        resp_legs
                                            .iter()
                                            .enumerate()
                                            .find(|(leg_idx, _)| !used_leg_indices[*leg_idx])
                                            .map(|(leg_idx, _)| leg_idx)
                                    }
                                });

                            if let Some(leg_idx) = matched_pos {
                                used_leg_indices[leg_idx] = true;
                                let leg = &resp_legs[leg_idx];
                                let child_vid = VenueOrderId::new(leg.id.trim());
                                tracking
                                    .record_venue_order_id(child_order.client_order_id(), child_vid);
                                Self::reconcile_submitted_order_response_with(
                                    &emitter,
                                    &tracking,
                                    child_order,
                                    leg,
                                    ts_now,
                                );
                            } else {
                                let child_vid =
                                    VenueOrderId::new(format!("{}-LEG-{idx}", resp.id.trim()));
                                tracking
                                    .record_venue_order_id(child_order.client_order_id(), child_vid);
                                if tracking
                                    .terminal_events
                                    .lock()
                                    .insert((child_vid, AlpacaTradeEventKind::New))
                                {
                                    emitter.emit_order_accepted(
                                        child_order,
                                        child_vid,
                                        ts_accepted,
                                    );
                                }
                            }
                        }
                    }
                    Err(AlpacaError::DeterministicReject { message, .. }) => {
                        for order in &orders_clone {
                            emitter.emit_order_rejected(order, &message, ts_now, false);
                        }
                    }
                    Err(other_err) => {
                        log::error!(
                            "Alpaca submit_order_list transport/auth error for {order_list_id} (retaining unresolved submissions): {other_err}"
                        );
                    }
                }
            });
        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let order = self.core.get_order(&cmd.client_order_id)?;
        let ts_now = self.clock.get_time_ns();
        self.tracking.track_order(&order);

        let venue_order_id = cmd
            .venue_order_id
            .or_else(|| order.venue_order_id())
            .or_else(|| self.tracking.current_venue_order_id(&cmd.client_order_id));

        if let Some(vid) = venue_order_id {
            self.tracking
                .record_venue_order_id(cmd.client_order_id, vid);
        } else if !self
            .tracking
            .submitted_orders
            .lock()
            .contains(&cmd.client_order_id)
        {
            self.emitter.emit_order_modify_rejected(
                &order,
                None,
                "cannot modify order without VenueOrderId",
                ts_now,
            );
            return Ok(());
        }

        let replace_req = AlpacaReplaceOrderRequest {
            qty: cmd.quantity.map(|q| q.as_decimal()),
            time_in_force: None,
            limit_price: cmd.price.map(|p| p.as_decimal()),
            stop_price: cmd.trigger_price.map(|p| p.as_decimal()),
            trail: None,
            client_order_id: None,
        };

        let http = Arc::clone(&self.http_client);
        let emitter = self.emitter.clone();
        let tracking = self.tracking.clone();
        let clock = self.clock;
        let order_clone = order.clone();
        let cid = cmd.client_order_id;
        let instrument_id = order.instrument_id();
        let cmd_qty = cmd.quantity;
        let cmd_price = cmd.price;
        let cmd_trigger_price = cmd.trigger_price;
        self.tracking
            .pending_modifies
            .lock()
            .insert(cid, (cmd_qty, cmd_price, cmd_trigger_price));
        self.tracking
            .spawn_instrument_order_task(instrument_id, async move {
                let resolved_vid = tracking.current_venue_order_id(&cid).or(venue_order_id);
                let Some(active_vid) = resolved_vid else {
                    tracking.pending_modifies.lock().swap_remove(&cid);
                    let ts_done = clock.get_time_ns();
                    emitter.emit_order_modify_rejected(
                        &order_clone,
                        None,
                        "cannot modify order without VenueOrderId",
                        ts_done,
                    );
                    return;
                };
                let vid_str = active_vid.as_str().to_string();
                let result = http.replace_order(&vid_str, &replace_req).await;
                let ts_done = clock.get_time_ns();
                tracking.pending_modifies.lock().swap_remove(&cid);
                match result {
                    Ok(resp) => {
                        let new_vid = VenueOrderId::new(resp.id.trim());
                        tracking
                            .terminal_events
                            .lock()
                            .add((active_vid, AlpacaTradeEventKind::Canceled));
                        if let Some(ref prev_id) = resp.replaces
                            && !prev_id.trim().is_empty()
                        {
                            let prev_vid = VenueOrderId::new(prev_id.trim());
                            tracking
                                .terminal_events
                                .lock()
                                .add((prev_vid, AlpacaTradeEventKind::Canceled));
                            tracking.record_venue_order_id(cid, prev_vid);
                        }
                        tracking.record_venue_order_id(cid, new_vid);
                        if !resp.client_order_id.trim().is_empty()
                            && let Ok(resp_cid) =
                                ClientOrderId::new_checked(resp.client_order_id.trim())
                        {
                            tracking.record_client_order_id_alias(resp_cid, cid);
                        }
                        let new_qty = resp
                            .qty
                            .and_then(|d| {
                                decimal_to_quantity(d, DEFAULT_EQUITY_SIZE_PRECISION).ok()
                            })
                            .or(cmd_qty)
                            .unwrap_or_else(|| order_clone.quantity());
                        let new_px = resp
                            .limit_price
                            .and_then(|d| decimal_to_price(d, DEFAULT_EQUITY_PRICE_PRECISION).ok())
                            .or(cmd_price)
                            .or_else(|| order_clone.price());
                        let new_trigger = resp
                            .stop_price
                            .and_then(|d| decimal_to_price(d, DEFAULT_EQUITY_PRICE_PRECISION).ok())
                            .or(cmd_trigger_price)
                            .or_else(|| order_clone.trigger_price());
                        if tracking.claim_order_updated(new_vid, new_qty, new_px, new_trigger) {
                            emitter.emit_order_updated(
                                &order_clone,
                                new_vid,
                                new_qty,
                                new_px,
                                new_trigger,
                                None,
                                ts_done,
                            );
                        }
                    }
                    Err(e) => {
                        if let Ok(resp) = http.get_order(&vid_str).await {
                            if resp.status.trim().eq_ignore_ascii_case("replaced")
                                && let Some(ref new_id) = resp.replaced_by
                                && !new_id.trim().is_empty()
                            {
                                tracking
                                    .terminal_events
                                    .lock()
                                    .add((active_vid, AlpacaTradeEventKind::Canceled));
                                let new_vid = VenueOrderId::new(new_id.trim());
                                tracking.record_venue_order_id(cid, new_vid);
                                let new_resp_opt = http.get_order(new_vid.as_str()).await.ok();
                                let new_qty = new_resp_opt
                                    .as_ref()
                                    .and_then(|r| r.qty)
                                    .and_then(|d| {
                                        decimal_to_quantity(d, DEFAULT_EQUITY_SIZE_PRECISION).ok()
                                    })
                                    .or(cmd_qty)
                                    .unwrap_or_else(|| order_clone.quantity());
                                let new_px = new_resp_opt
                                    .as_ref()
                                    .and_then(|r| r.limit_price)
                                    .and_then(|d| {
                                        decimal_to_price(d, DEFAULT_EQUITY_PRICE_PRECISION).ok()
                                    })
                                    .or(cmd_price)
                                    .or_else(|| order_clone.price());
                                let new_trigger = new_resp_opt
                                    .as_ref()
                                    .and_then(|r| r.stop_price)
                                    .and_then(|d| {
                                        decimal_to_price(d, DEFAULT_EQUITY_PRICE_PRECISION).ok()
                                    })
                                    .or(cmd_trigger_price)
                                    .or_else(|| order_clone.trigger_price());
                                if tracking
                                    .claim_order_updated(new_vid, new_qty, new_px, new_trigger)
                                {
                                    emitter.emit_order_updated(
                                        &order_clone,
                                        new_vid,
                                        new_qty,
                                        new_px,
                                        new_trigger,
                                        None,
                                        ts_done,
                                    );
                                }
                                if let Some(ref new_resp) = new_resp_opt {
                                    Self::reconcile_submitted_order_response_with(
                                        &emitter,
                                        &tracking,
                                        &order_clone,
                                        new_resp,
                                        ts_done,
                                    );
                                }
                                return;
                            }
                            if matches!(
                                resp.status.trim().to_ascii_lowercase().as_str(),
                                "filled"
                                    | "canceled"
                                    | "cancelled"
                                    | "expired"
                                    | "done_for_day"
                                    | "rejected"
                            ) {
                                Self::reconcile_submitted_order_response_with(
                                    &emitter,
                                    &tracking,
                                    &order_clone,
                                    &resp,
                                    ts_done,
                                );
                            }
                        }
                        emitter.emit_order_modify_rejected(
                            &order_clone,
                            Some(active_vid),
                            &e.to_string(),
                            ts_done,
                        );
                    }
                }
            });
        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let cached_order = self
            .core
            .get_order(&cmd.client_order_id)
            .ok()
            .or_else(|| {
                self.tracking
                    .cached_orders
                    .lock()
                    .get(&cmd.client_order_id)
                    .cloned()
            });
        if let Some(ref order) = cached_order {
            self.tracking.track_order(order);
        }
        let ts_now = self.clock.get_time_ns();

        let venue_order_id = cmd
            .venue_order_id
            .or_else(|| cached_order.as_ref().and_then(Order::venue_order_id))
            .or_else(|| self.tracking.current_venue_order_id(&cmd.client_order_id));

        if let Some(vid) = venue_order_id {
            self.tracking
                .record_venue_order_id(cmd.client_order_id, vid);
        } else if !self
            .tracking
            .submitted_orders
            .lock()
            .contains(&cmd.client_order_id)
        {
            self.emitter.emit_order_cancel_rejected_event(
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
                None,
                "cannot cancel order without VenueOrderId",
                ts_now,
            );
            return Ok(());
        }

        let http = Arc::clone(&self.http_client);
        let emitter = self.emitter.clone();
        let tracking = self.tracking.clone();
        let clock = self.clock;
        let strategy_id = cmd.strategy_id;
        let instrument_id = cmd.instrument_id;
        let client_order_id = cmd.client_order_id;
        let ws_active = self.ws_dispatch_task.is_some();
        self.tracking
            .spawn_instrument_order_task(instrument_id, async move {
                let resolved_vid = tracking
                    .current_venue_order_id(&client_order_id)
                    .or(venue_order_id);
                let Some(active_vid) = resolved_vid else {
                    let ts_done = clock.get_time_ns();
                    emitter.emit_order_cancel_rejected_event(
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        None,
                        "cannot cancel order without VenueOrderId",
                        ts_done,
                    );
                    return;
                };
                let vid_str = active_vid.as_str().to_string();
                let result = http.cancel_order(&vid_str).await;
                let ts_done = clock.get_time_ns();
                match result {
                    Ok(()) => {
                        Self::reconcile_post_cancel_order(
                            &http,
                            &emitter,
                            &tracking,
                            clock,
                            strategy_id,
                            instrument_id,
                            client_order_id,
                            active_vid,
                            cached_order,
                            ws_active,
                        )
                        .await;
                    }
                    Err(AlpacaError::DeterministicReject { message, .. }) => {
                        let mut reconciled = false;
                        if let Ok(resp) = http.get_order(&vid_str).await {
                            reconciled = Self::apply_order_response_after_cancel_attempt(
                                &emitter,
                                &tracking,
                                clock,
                                strategy_id,
                                instrument_id,
                                client_order_id,
                                active_vid,
                                cached_order.as_ref(),
                                &resp,
                            );
                        }
                        if !reconciled {
                            emitter.emit_order_cancel_rejected_event(
                                strategy_id,
                                instrument_id,
                                client_order_id,
                                Some(active_vid),
                                &message,
                                ts_done,
                            );
                        }
                    }
                    Err(other_err) => {
                        // Fail-closed: never optimistically mark local orders CANCELED on transport error,
                        // but always reconcile or emit OrderCancelRejected so Two-Phase Safe Exit cannot deadlock.
                        log::error!(
                            "Alpaca cancel_order transport error for {client_order_id} (venue_order_id={active_vid}): {other_err}"
                        );
                        let mut reconciled = false;
                        if let Ok(resp) = http.get_order(&vid_str).await {
                            reconciled = Self::apply_order_response_after_cancel_attempt(
                                &emitter,
                                &tracking,
                                clock,
                                strategy_id,
                                instrument_id,
                                client_order_id,
                                active_vid,
                                cached_order.as_ref(),
                                &resp,
                            );
                        }
                        if !reconciled {
                            emitter.emit_order_cancel_rejected_event(
                                strategy_id,
                                instrument_id,
                                client_order_id,
                                Some(active_vid),
                                &format!("cancel_order transport error: {other_err}"),
                                ts_done,
                            );
                        }
                    }
                }
            });
        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        for order in self
            .core
            .cache()
            .orders(None, Some(&cmd.instrument_id), None, None, None)
        {
            self.tracking.track_order(&order);
        }
        let http = Arc::clone(&self.http_client);
        let emitter = self.emitter.clone();
        let tracking = self.tracking.clone();
        let clock = self.clock;
        let strategy_id = cmd.strategy_id;
        let instrument_id = cmd.instrument_id;
        let target_symbol = cmd.instrument_id.symbol.as_str().to_string();
        let side_filter = cmd.order_side;
        let ws_active = self.ws_dispatch_task.is_some();
        self.tracking
            .spawn_instrument_order_task(instrument_id, async move {
                let open_orders = match http.list_open_orders().await {
                    Ok(orders) => orders,
                    Err(e) => {
                        log::error!("Alpaca cancel_all_orders list_open_orders failed: {e}");
                        return;
                    }
                };
                for open_order in open_orders {
                    if open_order.symbol.trim() != target_symbol {
                        continue;
                    }
                    let order_side = OrderSide::from(open_order.side);
                    if let Some(filter) = side_filter
                        && order_side != filter
                    {
                        continue;
                    }
                    let vid = VenueOrderId::new(open_order.id.trim());
                    let vid_str = vid.as_str().to_string();
                    if let Ok(()) = http.cancel_order(&vid_str).await {
                        let resolved_cid = tracking
                            .client_order_id_for_venue(&vid)
                            .or_else(|| {
                                open_order
                                    .replaces
                                    .as_deref()
                                    .map(str::trim)
                                    .filter(|s| !s.is_empty())
                                    .map(VenueOrderId::new)
                                    .and_then(|r_vid| tracking.client_order_id_for_venue(&r_vid))
                            })
                            .or_else(|| {
                                ClientOrderId::new_checked(open_order.client_order_id.trim())
                                    .ok()
                                    .map(|cid| tracking.resolve_client_order_id(cid))
                            });
                        if let Some(cid) = resolved_cid {
                            let cached_order = tracking.cached_orders.lock().get(&cid).cloned();
                            let eff_strategy_id = cached_order
                                .as_ref()
                                .map(Order::strategy_id)
                                .unwrap_or(strategy_id);
                            Self::reconcile_post_cancel_order(
                                &http,
                                &emitter,
                                &tracking,
                                clock,
                                eff_strategy_id,
                                instrument_id,
                                cid,
                                vid,
                                cached_order,
                                ws_active,
                            )
                            .await;
                        }
                    }
                }
            });
        Ok(())
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        for cancel in cmd.cancels {
            self.cancel_order(cancel)?;
        }
        Ok(())
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        let http = Arc::clone(&self.http_client);
        let acct_resp = block_on_alpaca_future(async move { http.get_account().await })?;
        let ts_init = self.clock.get_time_ns();
        let mut state = parse_account_state(&acct_resp, &self.config.venue, ts_init)?;
        state.account_id = self.core.account_id;
        self.emitter.send_account_state(state);
        Ok(())
    }

    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        let http = Arc::clone(&self.http_client);
        let cid_str = cmd.client_order_id.as_str().to_string();
        let tracked_vid = cmd
            .venue_order_id
            .or_else(|| self.tracking.current_venue_order_id(&cmd.client_order_id))
            .map(|v| v.as_str().to_string());
        let mut resp = block_on_alpaca_future(async move {
            if let Some(ref vid_str) = tracked_vid
                && let Ok(r) = http.get_order(vid_str).await
            {
                return Ok(r);
            }
            http.get_order_by_client_order_id(&cid_str).await
        })?;
        self.resolve_order_response_lineage(&mut resp);
        let ts_init = self.clock.get_time_ns();
        let report =
            parse_order_status_report(&resp, self.core.account_id, &self.config.venue, ts_init)?;
        self.emitter.send_order_status_report(report);
        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let ts_init = self.clock.get_time_ns();
        if let Some(cid) = cmd.client_order_id {
            match self
                .http_client
                .get_order_by_client_order_id(cid.as_str())
                .await
            {
                Ok(mut resp) => {
                    self.resolve_order_response_lineage(&mut resp);
                    let report = parse_order_status_report(
                        &resp,
                        self.core.account_id,
                        &self.config.venue,
                        ts_init,
                    )?;
                    return Ok(Some(report));
                }
                Err(AlpacaError::DeterministicReject { status: 404, .. }) => {
                    if cmd.venue_order_id.is_none()
                        && self.tracking.current_venue_order_id(&cid).is_none()
                    {
                        return Ok(None);
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }

        let target_vid = cmd.venue_order_id.or_else(|| {
            cmd.client_order_id
                .and_then(|cid| self.tracking.current_venue_order_id(&cid))
        });

        if let Some(vid) = target_vid {
            match self.http_client.get_order(vid.as_str()).await {
                Ok(mut resp) => {
                    self.resolve_order_response_lineage(&mut resp);
                    let report = parse_order_status_report(
                        &resp,
                        self.core.account_id,
                        &self.config.venue,
                        ts_init,
                    )?;
                    return Ok(Some(report));
                }
                Err(AlpacaError::DeterministicReject { status: 404, .. }) => return Ok(None),
                Err(e) => return Err(e.into()),
            }
        }
        Ok(None)
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let status_filter = if cmd.open_only {
            Some("open")
        } else {
            Some("all")
        };
        let raw_orders = self.http_client.list_orders(status_filter).await?;
        let flat_orders = Self::flatten_order_responses(raw_orders);
        self.seed_order_responses_lineage(&flat_orders);
        let ts_init = self.clock.get_time_ns();

        let mut reports = Vec::with_capacity(flat_orders.len());
        for mut resp in flat_orders {
            self.resolve_order_response_lineage(&mut resp);

            let report = parse_order_status_report(
                &resp,
                self.core.account_id,
                &self.config.venue,
                ts_init,
            )?;

            if let Some(inst_filter) = cmd.instrument_id
                && report.instrument_id != inst_filter
            {
                continue;
            }
            if let Some(start) = cmd.start
                && report.ts_last < start
            {
                continue;
            }
            if let Some(end) = cmd.end
                && report.ts_accepted > end
            {
                continue;
            }
            reports.push(report);
        }
        Ok(reports)
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let raw_orders = if let Some(vid) = cmd.venue_order_id {
            match self.http_client.get_order(vid.as_str()).await {
                Ok(order) => vec![order],
                Err(AlpacaError::DeterministicReject { status: 404, .. }) => Vec::new(),
                Err(e) => return Err(e.into()),
            }
        } else {
            self.http_client.list_orders(Some("all")).await?
        };
        let flat_orders = Self::flatten_order_responses(raw_orders);
        self.seed_order_responses_lineage(&flat_orders);
        let ts_init = self.clock.get_time_ns();

        let mut fills = Vec::new();
        for mut resp in flat_orders {
            let resolved_cid = self.resolve_order_response_lineage(&mut resp);
            if resp.filled_qty <= Decimal::ZERO {
                continue;
            }
            let Some(avg_px_dec) = resp
                .filled_avg_price
                .filter(|p| *p > Decimal::ZERO)
                .or_else(|| resp.limit_price.filter(|p| *p > Decimal::ZERO))
                .or_else(|| resp.stop_price.filter(|p| *p > Decimal::ZERO))
            else {
                continue;
            };

            let instrument_id = parse_instrument_id(&resp.symbol, &self.config.venue)?;
            if let Some(inst_filter) = cmd.instrument_id
                && instrument_id != inst_filter
            {
                continue;
            }

            let venue_order_id = VenueOrderId::new_checked(resp.id.trim())?;
            if let Some(vid_filter) = cmd.venue_order_id
                && venue_order_id != vid_filter
            {
                continue;
            }

            let client_order_id = resolved_cid.or_else(|| {
                if resp.client_order_id.trim().is_empty() {
                    None
                } else {
                    ClientOrderId::new_checked(resp.client_order_id.trim()).ok()
                }
            });

            let ts_event = resp
                .filled_at
                .as_deref()
                .or(resp.updated_at.as_deref())
                .unwrap_or(&resp.created_at);
            let ts_event_ns = parse_rfc3339_to_unix_nanos(ts_event)?;

            if let Some(start) = cmd.start
                && ts_event_ns < start
            {
                continue;
            }
            if let Some(end) = cmd.end
                && ts_event_ns > end
            {
                continue;
            }

            let Some((trade_id, last_qty, last_px)) = self.tracking.claim_rest_fill(
                venue_order_id,
                client_order_id,
                resp.filled_qty,
                avg_px_dec,
                DEFAULT_EQUITY_PRICE_PRECISION,
                DEFAULT_EQUITY_SIZE_PRECISION,
            )?
            else {
                log::debug!(
                    "Skipping already-reported Alpaca REST fill for venue_order_id={} filled_qty={}",
                    venue_order_id,
                    resp.filled_qty
                );
                continue;
            };

            if let Some(report) = build_fill_report_with_qty(
                &resp,
                self.core.account_id,
                instrument_id,
                DEFAULT_EQUITY_PRICE_PRECISION,
                DEFAULT_EQUITY_SIZE_PRECISION,
                ts_init,
                last_qty.as_decimal(),
                Some(trade_id),
                Some(last_px),
            )? {
                fills.push(report);
            }
        }

        Ok(fills)
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let raw_positions = self.http_client.list_positions().await?;
        let ts_init = self.clock.get_time_ns();

        let mut reports = Vec::with_capacity(raw_positions.len());
        for pos in raw_positions {
            let report = parse_position_status_report(
                &pos,
                self.core.account_id,
                &self.config.venue,
                ts_init,
            )?;
            if let Some(inst_filter) = cmd.instrument_id
                && report.instrument_id != inst_filter
            {
                continue;
            }
            reports.push(report);
        }
        Ok(reports)
    }

    fn register_external_order(
        &self,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        _instrument_id: InstrumentId,
        _strategy_id: StrategyId,
        _ts_init: UnixNanos,
    ) {
        self.tracking
            .record_venue_order_id(client_order_id, venue_order_id);
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc, time::Duration};

    use nautilus_common::{
        cache::Cache,
        clients::ExecutionClient,
        live::runner::replace_exec_event_sender,
        messages::{
            ExecutionEvent,
            execution::{CancelOrder, ModifyOrder, SubmitOrder},
        },
    };
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        enums::{
            LiquiditySide, OmsType, OrderSide, OrderType, TimeInForce, TriggerType,
        },
        events::{OrderEventAny, OrderFilled},
        identifiers::{
            AccountId, ClientId, ClientOrderId, InstrumentId, PositionId, StrategyId, Symbol,
            TradeId, TraderId, VenueOrderId,
        },
        instruments::{Equity, InstrumentAny},
        orders::{LimitOrder, MarketOrder, OrderAny, StopMarketOrder},
        position::Position,
        types::{Currency, Money, Price, Quantity},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::mpsc::UnboundedReceiver,
    };

    use super::*;
    use crate::{
        common::{ALPACA_VENUE, AlpacaCredential, AlpacaEnvironment},
        websocket::{
            AlpacaWsFrameClassification, classify_alpaca_ws_frame, parse_trade_update_payload,
        },
    };

    async fn read_test_http_request(stream: &mut TcpStream) -> (String, String) {
        let mut buf = Vec::with_capacity(2048);
        let mut tmp = [0u8; 512];
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
        let raw = String::from_utf8_lossy(&buf);
        let first_line = raw.lines().next().unwrap_or("");
        let mut parts = first_line.split_whitespace();
        let method = parts.next().unwrap_or("").to_string();
        let path = parts.next().unwrap_or("").to_string();
        (method, path)
    }

    async fn write_test_http_response(stream: &mut TcpStream, status_line: &str, body: &str) {
        let resp = format!(
            "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(resp.as_bytes()).await.unwrap();
        let _ = stream.shutdown().await;
    }

    fn build_test_stop_order(cid: &str, symbol: &str, qty: &str, trigger_px: &str) -> OrderAny {
        OrderAny::StopMarket(
            StopMarketOrder::new_checked(
                TraderId::from("TRADER-001"),
                StrategyId::from("STRAT-001"),
                InstrumentId::from(format!("{symbol}.ALPACA").as_str()),
                ClientOrderId::from(cid),
                OrderSide::Sell,
                Quantity::from(qty),
                Price::from(trigger_px),
                TriggerType::Default,
                TimeInForce::Gtc,
                None,
                false,
                true,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                UUID4::new(),
                UnixNanos::from(1_700_000_000_000_000_000u64),
            )
            .expect("valid StopMarketOrder"),
        )
    }

    fn build_test_client(
        http_base_url: &str,
        cache: Rc<RefCell<Cache>>,
    ) -> (AlpacaExecutionClient, UnboundedReceiver<ExecutionEvent>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
        replace_exec_event_sender(tx.clone());
        let cred = AlpacaCredential::new_checked("PKTESTKEY12345678", "SKTESTSECRET87654321")
            .expect("valid test credential");
        let cfg = AlpacaConfig::new_checked(
            AlpacaEnvironment::Paper,
            http_base_url,
            "ws://127.0.0.1:9/stream",
            cred,
            5,
            1,
            10,
            50,
            100,
            ALPACA_VENUE,
        )
        .expect("valid AlpacaConfig");
        let mut client = AlpacaExecutionClient::new(
            TraderId::from("TRADER-001"),
            ClientId::from(ALPACA_VENUE),
            AccountId::from("ALPACA-001"),
            cfg,
            cache,
        )
        .expect("valid AlpacaExecutionClient")
        .with_ws_enabled(false);
        client.set_sender(tx);
        (client, rx)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cancel_order_204_pending_cancel_does_not_emit_premature_canceled_and_reconciles_fill() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");

        let (pending_seen_tx, pending_seen_rx) = tokio::sync::oneshot::channel::<()>();
        let (allow_fill_tx, allow_fill_rx) = tokio::sync::oneshot::channel::<()>();

        let server = tokio::spawn(async move {
            // 1. DELETE /v2/orders/VEN-PEND-FILL-1 -> 204 No Content
            let (mut s1, _) = listener.accept().await.unwrap();
            let (m1, p1) = read_test_http_request(&mut s1).await;
            assert_eq!((m1.as_str(), p1.as_str()), ("DELETE", "/v2/orders/VEN-PEND-FILL-1"));
            write_test_http_response(&mut s1, "204 No Content", "").await;

            // 2. Immediate GET /v2/orders/VEN-PEND-FILL-1 -> status: "pending_cancel"
            let (mut s2, _) = listener.accept().await.unwrap();
            let (m2, p2) = read_test_http_request(&mut s2).await;
            assert_eq!((m2.as_str(), p2.as_str()), ("GET", "/v2/orders/VEN-PEND-FILL-1"));
            write_test_http_response(
                &mut s2,
                "200 OK",
                r#"{"id":"VEN-PEND-FILL-1","client_order_id":"CID-PEND-FILL-1","created_at":"2026-03-31T14:40:00Z","symbol":"AAPL","qty":"50","filled_qty":"0","type":"stop","side":"sell","time_in_force":"gtc","stop_price":"190.00","status":"pending_cancel"}"#,
            )
            .await;
            let _ = pending_seen_tx.send(());

            // Wait until test asserts zero premature events were emitted during pending_cancel
            let _ = allow_fill_rx.await;

            // 3. Follow-up GET /v2/orders/VEN-PEND-FILL-1 -> status: "filled" (stop triggered before cancel took effect)
            let (mut s3, _) = listener.accept().await.unwrap();
            let (m3, p3) = read_test_http_request(&mut s3).await;
            assert_eq!((m3.as_str(), p3.as_str()), ("GET", "/v2/orders/VEN-PEND-FILL-1"));
            write_test_http_response(
                &mut s3,
                "200 OK",
                r#"{"id":"VEN-PEND-FILL-1","client_order_id":"CID-PEND-FILL-1","created_at":"2026-03-31T14:40:00Z","filled_at":"2026-03-31T14:40:01Z","symbol":"AAPL","qty":"50","filled_qty":"50","filled_avg_price":"189.80","type":"stop","side":"sell","time_in_force":"gtc","stop_price":"190.00","status":"filled"}"#,
            )
            .await;
        });

        let cache = Rc::new(RefCell::new(Cache::default()));
        let (client, mut rx) = build_test_client(&base_url, cache.clone());
        let stop_order = build_test_stop_order("CID-PEND-FILL-1", "AAPL", "50", "190.00");
        cache
            .borrow_mut()
            .add_order(stop_order, None, Some(ClientId::from(ALPACA_VENUE)), false)
            .unwrap();
        client.record_venue_order_id(
            ClientOrderId::from("CID-PEND-FILL-1"),
            VenueOrderId::from("VEN-PEND-FILL-1"),
        );

        let cancel_cmd = CancelOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from(ALPACA_VENUE)),
            StrategyId::from("STRAT-001"),
            InstrumentId::from("AAPL.ALPACA"),
            ClientOrderId::from("CID-PEND-FILL-1"),
            Some(VenueOrderId::from("VEN-PEND-FILL-1")),
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            None,
            None,
        );
        client.cancel_order(cancel_cmd).unwrap();

        tokio::time::timeout(Duration::from_secs(2), pending_seen_rx)
            .await
            .expect("timed out waiting for initial pending_cancel GET")
            .unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(
            rx.try_recv().is_err(),
            "Must NOT emit OrderCanceled while order status is pending_cancel"
        );

        let _ = allow_fill_tx.send(());

        let ev1 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("expected OrderFilled event")
            .unwrap();
        let ev2 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("expected OrderCancelRejected event")
            .unwrap();

        match ev1 {
            ExecutionEvent::Order(OrderEventAny::Filled(f)) => {
                assert_eq!(f.client_order_id, ClientOrderId::from("CID-PEND-FILL-1"));
                assert_eq!(f.venue_order_id, VenueOrderId::from("VEN-PEND-FILL-1"));
                assert_eq!(f.last_qty, Quantity::from("50"));
                assert_eq!(f.last_px, Price::from("189.80"));
            }
            other => panic!("expected OrderFilled, got {other:?}"),
        }
        match ev2 {
            ExecutionEvent::Order(OrderEventAny::CancelRejected(r)) => {
                assert_eq!(r.client_order_id, ClientOrderId::from("CID-PEND-FILL-1"));
                assert_eq!(r.venue_order_id, Some(VenueOrderId::from("VEN-PEND-FILL-1")));
                assert!(
                    r.reason.as_str().contains("already filled"),
                    "unexpected cancel reject reason: {}",
                    r.reason
                );
            }
            other => panic!("expected OrderCancelRejected, got {other:?}"),
        }
        assert!(
            rx.try_recv().is_err(),
            "Must never emit OrderCanceled after order filled during pending_cancel"
        );

        server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cancel_order_204_pending_cancel_then_canceled_and_422_reject_reconciliation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");

        let server = tokio::spawn(async move {
            // Case A: DELETE -> 204, first GET -> pending_cancel, second GET -> canceled (with partial fill)
            let (mut s1, _) = listener.accept().await.unwrap();
            let (m1, p1) = read_test_http_request(&mut s1).await;
            assert_eq!((m1.as_str(), p1.as_str()), ("DELETE", "/v2/orders/VEN-PEND-CANC-1"));
            write_test_http_response(&mut s1, "204 No Content", "").await;

            let (mut s2, _) = listener.accept().await.unwrap();
            let (m2, p2) = read_test_http_request(&mut s2).await;
            assert_eq!((m2.as_str(), p2.as_str()), ("GET", "/v2/orders/VEN-PEND-CANC-1"));
            write_test_http_response(
                &mut s2,
                "200 OK",
                r#"{"id":"VEN-PEND-CANC-1","client_order_id":"CID-PEND-CANC-1","created_at":"2026-03-31T14:40:00Z","symbol":"AAPL","qty":"50","filled_qty":"0","type":"stop","side":"sell","time_in_force":"gtc","stop_price":"190.00","status":"pending_cancel"}"#,
            )
            .await;

            let (mut s3, _) = listener.accept().await.unwrap();
            let (m3, p3) = read_test_http_request(&mut s3).await;
            assert_eq!((m3.as_str(), p3.as_str()), ("GET", "/v2/orders/VEN-PEND-CANC-1"));
            write_test_http_response(
                &mut s3,
                "200 OK",
                r#"{"id":"VEN-PEND-CANC-1","client_order_id":"CID-PEND-CANC-1","created_at":"2026-03-31T14:40:00Z","canceled_at":"2026-03-31T14:40:01Z","symbol":"AAPL","qty":"50","filled_qty":"20","filled_avg_price":"190.00","type":"stop","side":"sell","time_in_force":"gtc","stop_price":"190.00","status":"canceled"}"#,
            )
            .await;

            // Case B: DELETE -> 422 DeterministicReject, followed by GET -> filled
            let (mut s4, _) = listener.accept().await.unwrap();
            let (m4, p4) = read_test_http_request(&mut s4).await;
            assert_eq!((m4.as_str(), p4.as_str()), ("DELETE", "/v2/orders/VEN-422-FILL-1"));
            write_test_http_response(
                &mut s4,
                "422 Unprocessable Entity",
                r#"{"code":42210000,"message":"order is already in filled state"}"#,
            )
            .await;

            let (mut s5, _) = listener.accept().await.unwrap();
            let (m5, p5) = read_test_http_request(&mut s5).await;
            assert_eq!((m5.as_str(), p5.as_str()), ("GET", "/v2/orders/VEN-422-FILL-1"));
            write_test_http_response(
                &mut s5,
                "200 OK",
                r#"{"id":"VEN-422-FILL-1","client_order_id":"CID-422-FILL-1","created_at":"2026-03-31T14:41:00Z","filled_at":"2026-03-31T14:41:01Z","symbol":"MSFT","qty":"30","filled_qty":"30","filled_avg_price":"410.25","type":"stop","side":"sell","time_in_force":"gtc","stop_price":"410.50","status":"filled"}"#,
            )
            .await;
        });

        let cache = Rc::new(RefCell::new(Cache::default()));
        let (client, mut rx) = build_test_client(&base_url, cache.clone());
        let order_a = build_test_stop_order("CID-PEND-CANC-1", "AAPL", "50", "190.00");
        let order_b = build_test_stop_order("CID-422-FILL-1", "MSFT", "30", "410.50");
        {
            let mut c = cache.borrow_mut();
            c.add_order(order_a, None, Some(ClientId::from(ALPACA_VENUE)), false)
                .unwrap();
            c.add_order(order_b, None, Some(ClientId::from(ALPACA_VENUE)), false)
                .unwrap();
        }
        client.record_venue_order_id(
            ClientOrderId::from("CID-PEND-CANC-1"),
            VenueOrderId::from("VEN-PEND-CANC-1"),
        );
        client.record_venue_order_id(
            ClientOrderId::from("CID-422-FILL-1"),
            VenueOrderId::from("VEN-422-FILL-1"),
        );

        // Execute Case A
        client
            .cancel_order(CancelOrder::new(
                TraderId::from("TRADER-001"),
                Some(ClientId::from(ALPACA_VENUE)),
                StrategyId::from("STRAT-001"),
                InstrumentId::from("AAPL.ALPACA"),
                ClientOrderId::from("CID-PEND-CANC-1"),
                Some(VenueOrderId::from("VEN-PEND-CANC-1")),
                UUID4::new(),
                UnixNanos::from(1_700_000_000_000_000_000u64),
                None,
                None,
            ))
            .unwrap();

        let a_ev1 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("expected partial OrderFilled before cancel")
            .unwrap();
        let a_ev2 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("expected OrderCanceled after pending_cancel resolved")
            .unwrap();
        match a_ev1 {
            ExecutionEvent::Order(OrderEventAny::Filled(f)) => {
                assert_eq!(f.client_order_id, ClientOrderId::from("CID-PEND-CANC-1"));
                assert_eq!(f.last_qty, Quantity::from("20"));
                assert_eq!(f.last_px, Price::from("190.00"));
            }
            other => panic!("expected partial OrderFilled, got {other:?}"),
        }
        match a_ev2 {
            ExecutionEvent::Order(OrderEventAny::Canceled(c)) => {
                assert_eq!(c.client_order_id, ClientOrderId::from("CID-PEND-CANC-1"));
                assert_eq!(c.venue_order_id, Some(VenueOrderId::from("VEN-PEND-CANC-1")));
            }
            other => panic!("expected OrderCanceled, got {other:?}"),
        }

        // Execute Case B
        client
            .cancel_order(CancelOrder::new(
                TraderId::from("TRADER-001"),
                Some(ClientId::from(ALPACA_VENUE)),
                StrategyId::from("STRAT-001"),
                InstrumentId::from("MSFT.ALPACA"),
                ClientOrderId::from("CID-422-FILL-1"),
                Some(VenueOrderId::from("VEN-422-FILL-1")),
                UUID4::new(),
                UnixNanos::from(1_700_000_000_000_000_000u64),
                None,
                None,
            ))
            .unwrap();

        let b_ev1 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("expected OrderFilled on 422 cancel reconciliation")
            .unwrap();
        let b_ev2 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("expected OrderCancelRejected on 422 cancel reconciliation")
            .unwrap();
        match b_ev1 {
            ExecutionEvent::Order(OrderEventAny::Filled(f)) => {
                assert_eq!(f.client_order_id, ClientOrderId::from("CID-422-FILL-1"));
                assert_eq!(f.last_qty, Quantity::from("30"));
                assert_eq!(f.last_px, Price::from("410.25"));
            }
            other => panic!("expected OrderFilled, got {other:?}"),
        }
        match b_ev2 {
            ExecutionEvent::Order(OrderEventAny::CancelRejected(r)) => {
                assert_eq!(r.client_order_id, ClientOrderId::from("CID-422-FILL-1"));
                assert_eq!(r.venue_order_id, Some(VenueOrderId::from("VEN-422-FILL-1")));
            }
            other => panic!("expected OrderCancelRejected, got {other:?}"),
        }

        server.await.unwrap();
    }

    fn decode_test_ws_trade_update(raw_json: &str) -> AlpacaExecutionUpdate {
        let classified = classify_alpaca_ws_frame(raw_json.as_bytes())
            .expect("valid Alpaca WS trade_updates JSON frame");
        match classified {
            AlpacaWsFrameClassification::TradeUpdate(payload) => parse_trade_update_payload(
                &payload,
                AccountId::from("ALPACA-001"),
                ALPACA_VENUE,
                DEFAULT_EQUITY_PRICE_PRECISION,
                DEFAULT_EQUITY_SIZE_PRECISION,
                UnixNanos::from(1_700_000_000_000_000_000u64),
            )
            .expect("valid AlpacaExecutionUpdate"),
            other => panic!("expected TradeUpdate frame, got {other:?}"),
        }
    }

    fn build_test_market_order(
        cid: &str,
        symbol: &str,
        side: OrderSide,
        qty: &str,
        reduce_only: bool,
    ) -> OrderAny {
        OrderAny::Market(
            MarketOrder::new_checked(
                TraderId::from("TRADER-001"),
                StrategyId::from("STRAT-001"),
                InstrumentId::from(format!("{symbol}.ALPACA").as_str()),
                ClientOrderId::from(cid),
                side,
                Quantity::from(qty),
                TimeInForce::Day,
                UUID4::new(),
                UnixNanos::from(1_700_000_000_000_000_000u64),
                reduce_only,
                false,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("valid MarketOrder"),
        )
    }

    fn build_test_limit_order(
        cid: &str,
        symbol: &str,
        side: OrderSide,
        qty: &str,
        price: &str,
        reduce_only: bool,
    ) -> OrderAny {
        OrderAny::Limit(
            LimitOrder::new_checked(
                TraderId::from("TRADER-001"),
                StrategyId::from("STRAT-001"),
                InstrumentId::from(format!("{symbol}.ALPACA").as_str()),
                ClientOrderId::from(cid),
                side,
                Quantity::from(qty),
                Price::from(price),
                TimeInForce::Day,
                None,
                false,
                reduce_only,
                false,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                UUID4::new(),
                UnixNanos::from(1_700_000_000_000_000_000u64),
            )
            .expect("valid LimitOrder"),
        )
    }

    fn build_test_equity_instrument(symbol: &str) -> InstrumentAny {
        InstrumentAny::Equity(
            Equity::builder()
                .instrument_id(InstrumentId::from(format!("{symbol}.ALPACA").as_str()))
                .raw_symbol(Symbol::from(symbol))
                .currency(Currency::USD())
                .price_precision(2)
                .price_increment(Price::from("0.01"))
                .lot_size(Quantity::from("1"))
                .min_quantity(Quantity::from("1"))
                .ts_event(UnixNanos::from(1_700_000_000_000_000_000u64))
                .ts_init(UnixNanos::from(1_700_000_000_000_000_000u64))
                .build()
                .expect("valid Equity instrument"),
        )
    }

    #[expect(clippy::too_many_arguments)]
    fn build_test_fill_event(
        symbol: &str,
        cid: &str,
        vid: &str,
        trade_id: &str,
        pos_id: &str,
        side: OrderSide,
        qty: &str,
        px: &str,
    ) -> OrderFilled {
        OrderFilled::new(
            TraderId::from("TRADER-001"),
            StrategyId::from("STRAT-001"),
            InstrumentId::from(format!("{symbol}.ALPACA").as_str()),
            ClientOrderId::from(cid),
            VenueOrderId::from(vid),
            AccountId::from("ALPACA-001"),
            TradeId::from(trade_id),
            side,
            OrderType::Market,
            Quantity::from(qty),
            Price::from(px),
            Currency::USD(),
            LiquiditySide::Taker,
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            false,
            Some(PositionId::new(pos_id)),
            Some(Money::from("0.00 USD")),
            None,
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_modify_order_patch_then_ws_replaced_does_not_rollback_and_marks_old_vid_canceled() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");

        let server = tokio::spawn(async move {
            let (mut s1, _) = listener.accept().await.unwrap();
            let (m1, p1) = read_test_http_request(&mut s1).await;
            assert_eq!(
                (m1.as_str(), p1.as_str()),
                ("PATCH", "/v2/orders/VEN-PATCH-OLD-1")
            );
            write_test_http_response(
                &mut s1,
                "200 OK",
                r#"{"id":"VEN-PATCH-NEW-1","replaces":"VEN-PATCH-OLD-1","client_order_id":"alpaca-auto-cid-1","created_at":"2026-03-31T15:00:00Z","submitted_at":"2026-03-31T15:00:00Z","symbol":"AAPL","qty":"50","filled_qty":"0","type":"stop","side":"sell","time_in_force":"gtc","stop_price":"195.00","status":"replaced"}"#,
            )
            .await;
        });

        let cache = Rc::new(RefCell::new(Cache::default()));
        let (client, mut rx) = build_test_client(&base_url, cache.clone());
        let stop_order = build_test_stop_order("CID-PATCH-1", "AAPL", "50", "190.00");
        cache
            .borrow_mut()
            .add_order(stop_order, None, Some(ClientId::from(ALPACA_VENUE)), false)
            .unwrap();
        client.record_venue_order_id(
            ClientOrderId::from("CID-PATCH-1"),
            VenueOrderId::from("VEN-PATCH-OLD-1"),
        );

        let mod_cmd = ModifyOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from(ALPACA_VENUE)),
            StrategyId::from("STRAT-001"),
            InstrumentId::from("AAPL.ALPACA"),
            ClientOrderId::from("CID-PATCH-1"),
            Some(VenueOrderId::from("VEN-PATCH-OLD-1")),
            Some(Quantity::from("50")),
            None,
            Some(Price::from("195.00")),
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            None,
            None,
        );
        client.modify_order(mod_cmd).unwrap();

        let ev = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("expected OrderUpdated from HTTP PATCH")
            .unwrap();
        match ev {
            ExecutionEvent::Order(OrderEventAny::Updated(u)) => {
                assert_eq!(u.client_order_id, ClientOrderId::from("CID-PATCH-1"));
                assert_eq!(u.venue_order_id, Some(VenueOrderId::from("VEN-PATCH-NEW-1")));
                assert_eq!(u.quantity, Quantity::from("50"));
                assert_eq!(u.trigger_price, Some(Price::from("195.00")));
            }
            other => panic!("expected OrderUpdated, got {other:?}"),
        }

        // Now Alpaca's `trade_updates` WebSocket sends `"event": "replaced"` carrying the OLD order
        // (`id = VEN-PATCH-OLD-1`, `replaced_by = VEN-PATCH-NEW-1`, old `stop_price = 190.00`).
        let ws_replaced_frame = r#"{
            "stream": "trade_updates",
            "data": {
                "event": "replaced",
                "timestamp": "2026-03-31T15:00:00.050Z",
                "order": {
                    "id": "VEN-PATCH-OLD-1",
                    "replaced_by": "VEN-PATCH-NEW-1",
                    "client_order_id": "CID-PATCH-1",
                    "created_at": "2026-03-31T14:55:00Z",
                    "updated_at": "2026-03-31T15:00:00.050Z",
                    "symbol": "AAPL",
                    "qty": "50",
                    "filled_qty": "0",
                    "type": "stop",
                    "side": "sell",
                    "time_in_force": "gtc",
                    "stop_price": "190.00",
                    "status": "replaced"
                }
            }
        }"#;
        client.ingest_execution_update(decode_test_ws_trade_update(ws_replaced_frame));

        assert!(
            rx.try_recv().is_err(),
            "Subsequent WS Replaced frame carrying old stop_price must NOT emit a rollback OrderUpdated"
        );
        assert!(
            client
                .tracking
                .has_terminal_order_event(VenueOrderId::from("VEN-PATCH-OLD-1")),
            "Old replaced venue_order_id must be marked Canceled in terminal_events"
        );

        server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ws_replaced_before_modify_order_patch_uses_pending_modifies_and_deduplicates_http_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");

        let (patch_arrived_tx, patch_arrived_rx) = tokio::sync::oneshot::channel::<()>();
        let (patch_release_tx, patch_release_rx) = tokio::sync::oneshot::channel::<()>();

        let server = tokio::spawn(async move {
            let (mut s1, _) = listener.accept().await.unwrap();
            let (m1, p1) = read_test_http_request(&mut s1).await;
            assert_eq!(
                (m1.as_str(), p1.as_str()),
                ("PATCH", "/v2/orders/VEN-RACE-OLD-1")
            );
            let _ = patch_arrived_tx.send(());
            let _ = patch_release_rx.await;
            write_test_http_response(
                &mut s1,
                "200 OK",
                r#"{"id":"VEN-RACE-NEW-1","replaces":"VEN-RACE-OLD-1","client_order_id":"alpaca-auto-cid-race","created_at":"2026-03-31T15:01:00Z","submitted_at":"2026-03-31T15:01:00Z","symbol":"AAPL","qty":"40","filled_qty":"0","type":"stop","side":"sell","time_in_force":"gtc","stop_price":"196.50","status":"replaced"}"#,
            )
            .await;
        });

        let cache = Rc::new(RefCell::new(Cache::default()));
        let (client, mut rx) = build_test_client(&base_url, cache.clone());
        let stop_order = build_test_stop_order("CID-RACE-1", "AAPL", "50", "190.00");
        cache
            .borrow_mut()
            .add_order(stop_order, None, Some(ClientId::from(ALPACA_VENUE)), false)
            .unwrap();
        client.record_venue_order_id(
            ClientOrderId::from("CID-RACE-1"),
            VenueOrderId::from("VEN-RACE-OLD-1"),
        );

        let mod_cmd = ModifyOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from(ALPACA_VENUE)),
            StrategyId::from("STRAT-001"),
            InstrumentId::from("AAPL.ALPACA"),
            ClientOrderId::from("CID-RACE-1"),
            Some(VenueOrderId::from("VEN-RACE-OLD-1")),
            Some(Quantity::from("40")),
            None,
            Some(Price::from("196.50")),
            UUID4::new(),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            None,
            None,
        );
        client.modify_order(mod_cmd).unwrap();

        tokio::time::timeout(Duration::from_secs(2), patch_arrived_rx)
            .await
            .expect("timed out waiting for PATCH request")
            .unwrap();

        // WS `Replaced` arrives BEFORE HTTP `PATCH` returns; `payload.order` carries the OLD
        // `qty: "50"` and `stop_price: "190.00"`, so `pending_modifies` must supply `40` and `196.50`.
        let ws_replaced_frame = r#"{
            "stream": "trade_updates",
            "data": {
                "event": "replaced",
                "timestamp": "2026-03-31T15:01:00.010Z",
                "order": {
                    "id": "VEN-RACE-OLD-1",
                    "replaced_by": "VEN-RACE-NEW-1",
                    "client_order_id": "CID-RACE-1",
                    "created_at": "2026-03-31T14:55:00Z",
                    "updated_at": "2026-03-31T15:01:00.010Z",
                    "symbol": "AAPL",
                    "qty": "50",
                    "filled_qty": "0",
                    "type": "stop",
                    "side": "sell",
                    "time_in_force": "gtc",
                    "stop_price": "190.00",
                    "status": "replaced"
                }
            }
        }"#;
        client.ingest_execution_update(decode_test_ws_trade_update(ws_replaced_frame));

        let ev = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("expected OrderUpdated from WS Replaced + pending_modifies")
            .unwrap();
        match ev {
            ExecutionEvent::Order(OrderEventAny::Updated(u)) => {
                assert_eq!(u.client_order_id, ClientOrderId::from("CID-RACE-1"));
                assert_eq!(u.venue_order_id, Some(VenueOrderId::from("VEN-RACE-NEW-1")));
                assert_eq!(u.quantity, Quantity::from("40"));
                assert_eq!(u.trigger_price, Some(Price::from("196.50")));
            }
            other => panic!("expected OrderUpdated with pending_modifies params, got {other:?}"),
        }
        assert!(
            client
                .tracking
                .has_terminal_order_event(VenueOrderId::from("VEN-RACE-OLD-1"))
        );

        // Now allow HTTP PATCH to complete; `claim_order_updated` must deduplicate it.
        let _ = patch_release_tx.send(());
        server.await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            rx.try_recv().is_err(),
            "HTTP PATCH completion after WS Replaced must be deduplicated via claim_order_updated"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_external_ws_replaced_then_ws_new_with_replaces_emits_updated_parameters() {
        let cache = Rc::new(RefCell::new(Cache::default()));
        let (client, mut rx) = build_test_client("http://127.0.0.1:9", cache.clone());
        let stop_order = build_test_stop_order("CID-EXT-1", "AAPL", "50", "190.00");
        cache
            .borrow_mut()
            .add_order(stop_order, None, Some(ClientId::from(ALPACA_VENUE)), false)
            .unwrap();
        client.record_venue_order_id(
            ClientOrderId::from("CID-EXT-1"),
            VenueOrderId::from("VEN-EXT-OLD-1"),
        );

        // 1. External replace (no local `pending_modifies`): WS `Replaced` arrives first with old params
        let ws_replaced_frame = r#"{
            "stream": "trade_updates",
            "data": {
                "event": "replaced",
                "timestamp": "2026-03-31T15:02:00.010Z",
                "order": {
                    "id": "VEN-EXT-OLD-1",
                    "replaced_by": "VEN-EXT-NEW-1",
                    "client_order_id": "CID-EXT-1",
                    "created_at": "2026-03-31T14:55:00Z",
                    "updated_at": "2026-03-31T15:02:00.010Z",
                    "symbol": "AAPL",
                    "qty": "50",
                    "filled_qty": "0",
                    "type": "stop",
                    "side": "sell",
                    "time_in_force": "gtc",
                    "stop_price": "190.00",
                    "status": "replaced"
                }
            }
        }"#;
        client.ingest_execution_update(decode_test_ws_trade_update(ws_replaced_frame));

        let ev1 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("expected initial OrderUpdated from external WS Replaced")
            .unwrap();
        match ev1 {
            ExecutionEvent::Order(OrderEventAny::Updated(u)) => {
                assert_eq!(u.client_order_id, ClientOrderId::from("CID-EXT-1"));
                assert_eq!(u.venue_order_id, Some(VenueOrderId::from("VEN-EXT-NEW-1")));
                assert_eq!(u.quantity, Quantity::from("50"));
                assert_eq!(u.trigger_price, Some(Price::from("190.00")));
            }
            other => panic!("expected OrderUpdated, got {other:?}"),
        }

        // 2. Subsequent WS `New` frame arrives with `replaces: "VEN-EXT-OLD-1"` and updated parameters
        let ws_new_frame = r#"{
            "stream": "trade_updates",
            "data": {
                "event": "new",
                "timestamp": "2026-03-31T15:02:00.020Z",
                "order": {
                    "id": "VEN-EXT-NEW-1",
                    "replaces": "VEN-EXT-OLD-1",
                    "client_order_id": "alpaca-ext-cid-2",
                    "created_at": "2026-03-31T15:02:00Z",
                    "updated_at": "2026-03-31T15:02:00.020Z",
                    "symbol": "AAPL",
                    "qty": "60",
                    "filled_qty": "0",
                    "type": "stop",
                    "side": "sell",
                    "time_in_force": "gtc",
                    "stop_price": "194.25",
                    "status": "new"
                }
            }
        }"#;
        client.ingest_execution_update(decode_test_ws_trade_update(ws_new_frame));

        let ev2 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("expected second OrderUpdated with updated external parameters")
            .unwrap();
        match ev2 {
            ExecutionEvent::Order(OrderEventAny::Updated(u)) => {
                assert_eq!(u.client_order_id, ClientOrderId::from("CID-EXT-1"));
                assert_eq!(u.venue_order_id, Some(VenueOrderId::from("VEN-EXT-NEW-1")));
                assert_eq!(u.quantity, Quantity::from("60"));
                assert_eq!(u.trigger_price, Some(Price::from("194.25")));
            }
            other => panic!("expected OrderUpdated with updated parameters, got {other:?}"),
        }
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ws_observer_reconnect_and_reconcile_open_orders_emits_missed_fill_cancel_and_replace() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");

        let server = tokio::spawn(async move {
            // 5 HTTP GET requests in total (4 during reconnect reconciliation + 1 during follow-up `reconcile_open_orders()`):
            for _ in 0..5 {
                let (mut s, _) = listener.accept().await.unwrap();
                let (m, p) = read_test_http_request(&mut s).await;
                assert_eq!(m, "GET");
                match p.as_str() {
                    "/v2/orders/VEN-REC-FILL-1" => {
                        write_test_http_response(
                            &mut s,
                            "200 OK",
                            r#"{"id":"VEN-REC-FILL-1","client_order_id":"CID-REC-FILL-1","created_at":"2026-03-31T15:05:00Z","filled_at":"2026-03-31T15:05:02Z","symbol":"AAPL","qty":"50","filled_qty":"50","filled_avg_price":"189.90","type":"stop","side":"sell","time_in_force":"gtc","stop_price":"190.00","status":"filled"}"#,
                        )
                        .await;
                    }
                    "/v2/orders/VEN-REC-CANC-1" => {
                        write_test_http_response(
                            &mut s,
                            "200 OK",
                            r#"{"id":"VEN-REC-CANC-1","client_order_id":"CID-REC-CANC-1","created_at":"2026-03-31T15:05:00Z","canceled_at":"2026-03-31T15:05:03Z","symbol":"MSFT","qty":"30","filled_qty":"0","type":"stop","side":"sell","time_in_force":"gtc","stop_price":"410.00","status":"canceled"}"#,
                        )
                        .await;
                    }
                    "/v2/orders/VEN-REC-REPL-OLD" => {
                        write_test_http_response(
                            &mut s,
                            "200 OK",
                            r#"{"id":"VEN-REC-REPL-OLD","replaced_by":"VEN-REC-REPL-NEW","client_order_id":"CID-REC-REPL-1","created_at":"2026-03-31T15:05:00Z","symbol":"NVDA","qty":"25","filled_qty":"0","type":"stop","side":"sell","time_in_force":"gtc","stop_price":"120.00","status":"replaced"}"#,
                        )
                        .await;
                    }
                    "/v2/orders/VEN-REC-REPL-NEW" => {
                        write_test_http_response(
                            &mut s,
                            "200 OK",
                            r#"{"id":"VEN-REC-REPL-NEW","replaces":"VEN-REC-REPL-OLD","client_order_id":"alpaca-repl-cid","created_at":"2026-03-31T15:05:04Z","submitted_at":"2026-03-31T15:05:04Z","symbol":"NVDA","qty":"25","filled_qty":"0","type":"stop","side":"sell","time_in_force":"gtc","stop_price":"125.50","status":"accepted"}"#,
                        )
                        .await;
                    }
                    other => panic!("unexpected GET path during reconciliation: {other}"),
                }
            }
        });

        let cache = Rc::new(RefCell::new(Cache::default()));
        let (client, mut rx) = build_test_client(&base_url, cache.clone());

        let o_fill = build_test_stop_order("CID-REC-FILL-1", "AAPL", "50", "190.00");
        let o_canc = build_test_stop_order("CID-REC-CANC-1", "MSFT", "30", "410.00");
        let o_repl = build_test_stop_order("CID-REC-REPL-1", "NVDA", "25", "120.00");

        client.tracking.track_order(&o_fill);
        client.tracking.track_order(&o_canc);
        client.tracking.track_order(&o_repl);

        client.record_venue_order_id(
            ClientOrderId::from("CID-REC-FILL-1"),
            VenueOrderId::from("VEN-REC-FILL-1"),
        );
        client.record_venue_order_id(
            ClientOrderId::from("CID-REC-CANC-1"),
            VenueOrderId::from("VEN-REC-CANC-1"),
        );
        client.record_venue_order_id(
            ClientOrderId::from("CID-REC-REPL-1"),
            VenueOrderId::from("VEN-REC-REPL-OLD"),
        );

        // Pre-mark `New` accepted so reconciliation only emits the missed delta transitions
        {
            let mut term = client.tracking.terminal_events.lock();
            term.add((VenueOrderId::from("VEN-REC-FILL-1"), AlpacaTradeEventKind::New));
            term.add((VenueOrderId::from("VEN-REC-CANC-1"), AlpacaTradeEventKind::New));
            term.add((VenueOrderId::from("VEN-REC-REPL-OLD"), AlpacaTradeEventKind::New));
        }

        let observer = AlpacaExecutionWsObserver {
            http_client: Arc::clone(&client.http_client),
            emitter: client.emitter.clone(),
            tracking: client.tracking.clone(),
            clock: client.clock,
            reconnecting: AtomicBool::new(false),
        };

        // Initial `on_ws_listening` without prior `on_ws_reconnecting` must not trigger reconciliation
        observer.on_ws_listening(&["trade_updates".to_string()]);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(rx.try_recv().is_err());

        // Simulate WebSocket disconnect -> reconnect -> listening
        observer.on_ws_reconnecting(1);
        observer.on_ws_listening(&["trade_updates".to_string()]);

        // Expect:
        // 1. OrderFilled for CID-REC-FILL-1
        // 2. OrderCanceled for CID-REC-CANC-1
        // 3. OrderUpdated for CID-REC-REPL-1 (VEN-REC-REPL-NEW @ 125.50)
        // 4. OrderAccepted for VEN-REC-REPL-NEW
        let mut events = Vec::new();
        for _ in 0..4 {
            let ev = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("expected reconciliation event after WS reconnect")
                .unwrap();
            events.push(ev);
        }

        assert!(events.iter().any(|ev| matches!(
            ev,
            ExecutionEvent::Order(OrderEventAny::Filled(f))
                if f.client_order_id == ClientOrderId::from("CID-REC-FILL-1")
                    && f.venue_order_id == VenueOrderId::from("VEN-REC-FILL-1")
                    && f.last_qty == Quantity::from("50")
                    && f.last_px == Price::from("189.90")
        )));
        assert!(events.iter().any(|ev| matches!(
            ev,
            ExecutionEvent::Order(OrderEventAny::Canceled(c))
                if c.client_order_id == ClientOrderId::from("CID-REC-CANC-1")
                    && c.venue_order_id == Some(VenueOrderId::from("VEN-REC-CANC-1"))
        )));
        assert!(events.iter().any(|ev| matches!(
            ev,
            ExecutionEvent::Order(OrderEventAny::Updated(u))
                if u.client_order_id == ClientOrderId::from("CID-REC-REPL-1")
                    && u.venue_order_id == Some(VenueOrderId::from("VEN-REC-REPL-NEW"))
                    && u.quantity == Quantity::from("25")
                    && u.trigger_price == Some(Price::from("125.50"))
        )));

        // Direct call to `reconcile_open_orders()` must skip terminal orders and deduplicate already-updated VEN-REC-REPL-NEW
        client.reconcile_open_orders().await;
        assert!(rx.try_recv().is_err());

        server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_submit_order_naked_reduce_only_exit_guard_denies_closed_or_mismatched_and_allows_valid() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");

        let server = tokio::spawn(async move {
            // Only 2 orders should reach the HTTP server:
            // 1. Standalone reduce_only order on NVDA (no position history in cache)
            let (mut s1, _) = listener.accept().await.unwrap();
            let (m1, p1) = read_test_http_request(&mut s1).await;
            assert_eq!((m1.as_str(), p1.as_str()), ("POST", "/v2/orders"));
            write_test_http_response(
                &mut s1,
                "200 OK",
                r#"{"id":"VEN-RO-NVDA-1","client_order_id":"CID-RO-NVDA-1","created_at":"2026-03-31T15:10:00Z","submitted_at":"2026-03-31T15:10:00Z","symbol":"NVDA","qty":"10","filled_qty":"0","type":"market","side":"sell","time_in_force":"day","status":"accepted"}"#,
            )
            .await;

            // 2. Valid reduce_only Sell 50 on MSFT (against open Long 50 position)
            let (mut s2, _) = listener.accept().await.unwrap();
            let (m2, p2) = read_test_http_request(&mut s2).await;
            assert_eq!((m2.as_str(), p2.as_str()), ("POST", "/v2/orders"));
            write_test_http_response(
                &mut s2,
                "200 OK",
                r#"{"id":"VEN-RO-MSFT-OK","client_order_id":"CID-RO-MSFT-OK","created_at":"2026-03-31T15:10:01Z","submitted_at":"2026-03-31T15:10:01Z","symbol":"MSFT","qty":"50","filled_qty":"0","type":"market","side":"sell","time_in_force":"day","status":"accepted"}"#,
            )
            .await;
        });

        let cache = Rc::new(RefCell::new(Cache::default()));
        let (client, mut rx) = build_test_client(&base_url, cache.clone());

        // Seed cache with:
        // - Closed position for AAPL.ALPACA (Long 50 opened then closed by stop fill)
        // - Open Long position of 50 shares for MSFT.ALPACA
        // - Zero position history for NVDA.ALPACA
        {
            let mut c = cache.borrow_mut();
            let aapl_inst = build_test_equity_instrument("AAPL");
            let msft_inst = build_test_equity_instrument("MSFT");
            c.add_instrument(aapl_inst.clone()).unwrap();
            c.add_instrument(msft_inst.clone()).unwrap();

            let aapl_open_fill = build_test_fill_event(
                "AAPL",
                "CID-AAPL-ENTRY",
                "VEN-AAPL-ENTRY",
                "TRD-AAPL-1",
                "P-AAPL-1",
                OrderSide::Buy,
                "50",
                "195.00",
            );
            let mut aapl_pos = Position::new(&aapl_inst, aapl_open_fill);
            c.add_position_without_order(&aapl_pos, OmsType::Netting)
                .unwrap();
            let aapl_close_fill = build_test_fill_event(
                "AAPL",
                "CID-AAPL-STOP",
                "VEN-AAPL-STOP",
                "TRD-AAPL-2",
                "P-AAPL-1",
                OrderSide::Sell,
                "50",
                "190.00",
            );
            aapl_pos.apply(&aapl_close_fill);
            c.update_position(&aapl_pos).unwrap();

            let msft_open_fill = build_test_fill_event(
                "MSFT",
                "CID-MSFT-ENTRY",
                "VEN-MSFT-ENTRY",
                "TRD-MSFT-1",
                "P-MSFT-1",
                OrderSide::Buy,
                "50",
                "415.00",
            );
            let msft_pos = Position::new(&msft_inst, msft_open_fill);
            c.add_position_without_order(&msft_pos, OmsType::Netting)
                .unwrap();
        }

        let make_submit = |order: &OrderAny| -> SubmitOrder {
            SubmitOrder::new(
                TraderId::from("TRADER-001"),
                Some(ClientId::from(ALPACA_VENUE)),
                StrategyId::from("STRAT-001"),
                order.instrument_id(),
                order.client_order_id(),
                order.init_event().clone(),
                None,
                None,
                None,
                UUID4::new(),
                UnixNanos::from(1_700_000_000_000_000_000u64),
                None,
            )
        };

        // 1. Standalone reduce_only order when cache has NO position history at all -> ALLOWED
        let nvda_order = build_test_market_order("CID-RO-NVDA-1", "NVDA", OrderSide::Sell, "10", true);
        cache
            .borrow_mut()
            .add_order(
                nvda_order.clone(),
                None,
                Some(ClientId::from(ALPACA_VENUE)),
                false,
            )
            .unwrap();
        client.submit_order(make_submit(&nvda_order)).unwrap();
        let nvda_ev1 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let nvda_ev2 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            nvda_ev1,
            ExecutionEvent::Order(OrderEventAny::Submitted(s))
                if s.client_order_id == ClientOrderId::from("CID-RO-NVDA-1")
        ));
        assert!(matches!(
            nvda_ev2,
            ExecutionEvent::Order(OrderEventAny::Accepted(a))
                if a.client_order_id == ClientOrderId::from("CID-RO-NVDA-1")
        ));

        // 2. Reduce_only order on AAPL where position was ALREADY CLOSED -> DENIED (REDUCE_ONLY_POSITION_CLOSED)
        let aapl_exit = build_test_market_order("CID-RO-AAPL-CLOSED", "AAPL", OrderSide::Sell, "50", true);
        cache
            .borrow_mut()
            .add_order(
                aapl_exit.clone(),
                None,
                Some(ClientId::from(ALPACA_VENUE)),
                false,
            )
            .unwrap();
        client.submit_order(make_submit(&aapl_exit)).unwrap();
        let aapl_ev = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        match aapl_ev {
            ExecutionEvent::Order(OrderEventAny::Denied(d)) => {
                assert_eq!(d.client_order_id, ClientOrderId::from("CID-RO-AAPL-CLOSED"));
                assert!(
                    d.reason.as_str().contains("REDUCE_ONLY_POSITION_CLOSED"),
                    "expected REDUCE_ONLY_POSITION_CLOSED, got {}",
                    d.reason
                );
            }
            other => panic!("expected OrderDenied for closed position, got {other:?}"),
        }

        // 3a. Reduce_only Limit Sell 60 on MSFT (exceeds open Long 50) -> DENIED (REDUCE_ONLY_POSITION_MISMATCH)
        let msft_exceed =
            build_test_limit_order("CID-RO-MSFT-EXCEED", "MSFT", OrderSide::Sell, "60", "420.00", true);
        cache
            .borrow_mut()
            .add_order(
                msft_exceed.clone(),
                None,
                Some(ClientId::from(ALPACA_VENUE)),
                false,
            )
            .unwrap();
        client.submit_order(make_submit(&msft_exceed)).unwrap();
        let msft_ex_ev = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        match msft_ex_ev {
            ExecutionEvent::Order(OrderEventAny::Denied(d)) => {
                assert_eq!(d.client_order_id, ClientOrderId::from("CID-RO-MSFT-EXCEED"));
                assert!(
                    d.reason.as_str().contains("REDUCE_ONLY_POSITION_MISMATCH"),
                    "expected REDUCE_ONLY_POSITION_MISMATCH, got {}",
                    d.reason
                );
            }
            other => panic!("expected OrderDenied for excess qty, got {other:?}"),
        }

        // 3b. Reduce_only Market Buy 10 on MSFT (opposes open Long 50) -> DENIED (REDUCE_ONLY_POSITION_MISMATCH)
        let msft_oppose =
            build_test_market_order("CID-RO-MSFT-OPPOSE", "MSFT", OrderSide::Buy, "10", true);
        cache
            .borrow_mut()
            .add_order(
                msft_oppose.clone(),
                None,
                Some(ClientId::from(ALPACA_VENUE)),
                false,
            )
            .unwrap();
        client.submit_order(make_submit(&msft_oppose)).unwrap();
        let msft_op_ev = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        match msft_op_ev {
            ExecutionEvent::Order(OrderEventAny::Denied(d)) => {
                assert_eq!(d.client_order_id, ClientOrderId::from("CID-RO-MSFT-OPPOSE"));
                assert!(
                    d.reason.as_str().contains("REDUCE_ONLY_POSITION_MISMATCH"),
                    "expected REDUCE_ONLY_POSITION_MISMATCH, got {}",
                    d.reason
                );
            }
            other => panic!("expected OrderDenied for opposing side, got {other:?}"),
        }

        // 3c. Valid reduce_only Market Sell 50 on MSFT -> ALLOWED
        let msft_valid =
            build_test_market_order("CID-RO-MSFT-OK", "MSFT", OrderSide::Sell, "50", true);
        cache
            .borrow_mut()
            .add_order(
                msft_valid.clone(),
                None,
                Some(ClientId::from(ALPACA_VENUE)),
                false,
            )
            .unwrap();
        client.submit_order(make_submit(&msft_valid)).unwrap();
        let msft_ok_ev1 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let msft_ok_ev2 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            msft_ok_ev1,
            ExecutionEvent::Order(OrderEventAny::Submitted(s))
                if s.client_order_id == ClientOrderId::from("CID-RO-MSFT-OK")
        ));
        assert!(matches!(
            msft_ok_ev2,
            ExecutionEvent::Order(OrderEventAny::Accepted(a))
                if a.client_order_id == ClientOrderId::from("CID-RO-MSFT-OK")
        ));

        server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_modify_order_and_cancel_order_transport_error_reconciliation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");

        let server = tokio::spawn(async move {
            // Part A: `modify_order` (`PATCH /v2/orders/VEN-MOD-ERR-OLD`) fails on transport error
            // (`max_retries = 1` means initial attempt + 1 retry = 2 connections closed without response).
            for _ in 0..2 {
                let (mut s_patch, _) = listener.accept().await.unwrap();
                let (m, p) = read_test_http_request(&mut s_patch).await;
                assert_eq!(
                    (m.as_str(), p.as_str()),
                    ("PATCH", "/v2/orders/VEN-MOD-ERR-OLD")
                );
                drop(s_patch);
            }

            // Reconciliation GET /v2/orders/VEN-MOD-ERR-OLD -> status: "replaced", replaced_by: "VEN-MOD-ERR-NEW"
            let (mut s_get_old, _) = listener.accept().await.unwrap();
            let (m_old, p_old) = read_test_http_request(&mut s_get_old).await;
            assert_eq!(
                (m_old.as_str(), p_old.as_str()),
                ("GET", "/v2/orders/VEN-MOD-ERR-OLD")
            );
            write_test_http_response(
                &mut s_get_old,
                "200 OK",
                r#"{"id":"VEN-MOD-ERR-OLD","replaced_by":"VEN-MOD-ERR-NEW","client_order_id":"CID-MOD-ERR-1","created_at":"2026-03-31T15:15:00Z","symbol":"AAPL","qty":"50","filled_qty":"0","type":"stop","side":"sell","time_in_force":"gtc","stop_price":"190.00","status":"replaced"}"#,
            )
            .await;

            // Reconciliation GET /v2/orders/VEN-MOD-ERR-NEW -> status: "accepted", stop_price: "194.00"
            let (mut s_get_new, _) = listener.accept().await.unwrap();
            let (m_new, p_new) = read_test_http_request(&mut s_get_new).await;
            assert_eq!(
                (m_new.as_str(), p_new.as_str()),
                ("GET", "/v2/orders/VEN-MOD-ERR-NEW")
            );
            write_test_http_response(
                &mut s_get_new,
                "200 OK",
                r#"{"id":"VEN-MOD-ERR-NEW","replaces":"VEN-MOD-ERR-OLD","client_order_id":"alpaca-mod-err-new","created_at":"2026-03-31T15:15:01Z","submitted_at":"2026-03-31T15:15:01Z","symbol":"AAPL","qty":"50","filled_qty":"0","type":"stop","side":"sell","time_in_force":"gtc","stop_price":"194.00","status":"accepted"}"#,
            )
            .await;

            // Part B1: `cancel_order` (`DELETE /v2/orders/VEN-CANC-ERR-1`) fails on transport error
            // (2 attempts dropped), and `GET /v2/orders/VEN-CANC-ERR-1` returns `status: "accepted"` (still open).
            for _ in 0..2 {
                let (mut s_del, _) = listener.accept().await.unwrap();
                let (m, p) = read_test_http_request(&mut s_del).await;
                assert_eq!(
                    (m.as_str(), p.as_str()),
                    ("DELETE", "/v2/orders/VEN-CANC-ERR-1")
                );
                drop(s_del);
            }
            let (mut s_get_canc1, _) = listener.accept().await.unwrap();
            let (m_c1, p_c1) = read_test_http_request(&mut s_get_canc1).await;
            assert_eq!(
                (m_c1.as_str(), p_c1.as_str()),
                ("GET", "/v2/orders/VEN-CANC-ERR-1")
            );
            write_test_http_response(
                &mut s_get_canc1,
                "200 OK",
                r#"{"id":"VEN-CANC-ERR-1","client_order_id":"CID-CANC-ERR-1","created_at":"2026-03-31T15:16:00Z","symbol":"MSFT","qty":"30","filled_qty":"0","type":"stop","side":"sell","time_in_force":"gtc","stop_price":"410.00","status":"accepted"}"#,
            )
            .await;

            // Part B2: `cancel_order` (`DELETE /v2/orders/VEN-CANC-422-OPEN`) fails with 422 DeterministicReject
            // while order is still open (`status: "accepted"`).
            let (mut s_del_422, _) = listener.accept().await.unwrap();
            let (m_d422, p_d422) = read_test_http_request(&mut s_del_422).await;
            assert_eq!(
                (m_d422.as_str(), p_d422.as_str()),
                ("DELETE", "/v2/orders/VEN-CANC-422-OPEN")
            );
            write_test_http_response(
                &mut s_del_422,
                "422 Unprocessable Entity",
                r#"{"code":42210000,"message":"order cannot be canceled right now"}"#,
            )
            .await;

            let (mut s_get_422, _) = listener.accept().await.unwrap();
            let (m_g422, p_g422) = read_test_http_request(&mut s_get_422).await;
            assert_eq!(
                (m_g422.as_str(), p_g422.as_str()),
                ("GET", "/v2/orders/VEN-CANC-422-OPEN")
            );
            write_test_http_response(
                &mut s_get_422,
                "200 OK",
                r#"{"id":"VEN-CANC-422-OPEN","client_order_id":"CID-CANC-422-OPEN","created_at":"2026-03-31T15:16:05Z","symbol":"NVDA","qty":"20","filled_qty":"0","type":"stop","side":"sell","time_in_force":"gtc","stop_price":"120.00","status":"accepted"}"#,
            )
            .await;
        });

        let cache = Rc::new(RefCell::new(Cache::default()));
        let (client, mut rx) = build_test_client(&base_url, cache.clone());

        let mod_order = build_test_stop_order("CID-MOD-ERR-1", "AAPL", "50", "190.00");
        let canc_order_1 = build_test_stop_order("CID-CANC-ERR-1", "MSFT", "30", "410.00");
        let canc_order_2 = build_test_stop_order("CID-CANC-422-OPEN", "NVDA", "20", "120.00");
        {
            let mut c = cache.borrow_mut();
            c.add_order(mod_order, None, Some(ClientId::from(ALPACA_VENUE)), false)
                .unwrap();
            c.add_order(canc_order_1, None, Some(ClientId::from(ALPACA_VENUE)), false)
                .unwrap();
            c.add_order(canc_order_2, None, Some(ClientId::from(ALPACA_VENUE)), false)
                .unwrap();
        }
        client.record_venue_order_id(
            ClientOrderId::from("CID-MOD-ERR-1"),
            VenueOrderId::from("VEN-MOD-ERR-OLD"),
        );
        client.record_venue_order_id(
            ClientOrderId::from("CID-CANC-ERR-1"),
            VenueOrderId::from("VEN-CANC-ERR-1"),
        );
        client.record_venue_order_id(
            ClientOrderId::from("CID-CANC-422-OPEN"),
            VenueOrderId::from("VEN-CANC-422-OPEN"),
        );

        // Execute Part A: `modify_order` transport error where venue already replaced the order
        client
            .modify_order(ModifyOrder::new(
                TraderId::from("TRADER-001"),
                Some(ClientId::from(ALPACA_VENUE)),
                StrategyId::from("STRAT-001"),
                InstrumentId::from("AAPL.ALPACA"),
                ClientOrderId::from("CID-MOD-ERR-1"),
                Some(VenueOrderId::from("VEN-MOD-ERR-OLD")),
                Some(Quantity::from("50")),
                None,
                Some(Price::from("194.00")),
                UUID4::new(),
                UnixNanos::from(1_700_000_000_000_000_000u64),
                None,
                None,
            ))
            .unwrap();

        let mod_ev1 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("expected OrderUpdated after modify_order transport error reconciliation")
            .unwrap();
        let mod_ev2 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("expected OrderAccepted for reconciled replacement order")
            .unwrap();
        match mod_ev1 {
            ExecutionEvent::Order(OrderEventAny::Updated(u)) => {
                assert_eq!(u.client_order_id, ClientOrderId::from("CID-MOD-ERR-1"));
                assert_eq!(
                    u.venue_order_id,
                    Some(VenueOrderId::from("VEN-MOD-ERR-NEW"))
                );
                assert_eq!(u.quantity, Quantity::from("50"));
                assert_eq!(u.trigger_price, Some(Price::from("194.00")));
            }
            other => panic!("expected OrderUpdated, got {other:?}"),
        }
        assert!(
            !matches!(mod_ev2, ExecutionEvent::Order(OrderEventAny::ModifyRejected(_))),
            "Must NOT emit a false OrderModifyRejected when order was already replaced on broker"
        );
        assert_eq!(
            client
                .tracking
                .current_venue_order_id(&ClientOrderId::from("CID-MOD-ERR-1")),
            Some(VenueOrderId::from("VEN-MOD-ERR-NEW"))
        );

        // Execute Part B1: `cancel_order` transport error where order is still open on venue
        client
            .cancel_order(CancelOrder::new(
                TraderId::from("TRADER-001"),
                Some(ClientId::from(ALPACA_VENUE)),
                StrategyId::from("STRAT-001"),
                InstrumentId::from("MSFT.ALPACA"),
                ClientOrderId::from("CID-CANC-ERR-1"),
                Some(VenueOrderId::from("VEN-CANC-ERR-1")),
                UUID4::new(),
                UnixNanos::from(1_700_000_000_000_000_000u64),
                None,
                None,
            ))
            .unwrap();

        let canc_ev1 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("expected OrderCancelRejected on cancel_order transport error")
            .unwrap();
        match canc_ev1 {
            ExecutionEvent::Order(OrderEventAny::CancelRejected(r)) => {
                assert_eq!(r.client_order_id, ClientOrderId::from("CID-CANC-ERR-1"));
                assert_eq!(r.venue_order_id, Some(VenueOrderId::from("VEN-CANC-ERR-1")));
                assert!(
                    r.reason.as_str().contains("cancel_order transport error"),
                    "unexpected cancel reject reason: {}",
                    r.reason
                );
            }
            other => panic!("expected OrderCancelRejected, got {other:?}"),
        }

        // Execute Part B2: `cancel_order` 422 DeterministicReject where order is still open on venue
        client
            .cancel_order(CancelOrder::new(
                TraderId::from("TRADER-001"),
                Some(ClientId::from(ALPACA_VENUE)),
                StrategyId::from("STRAT-001"),
                InstrumentId::from("NVDA.ALPACA"),
                ClientOrderId::from("CID-CANC-422-OPEN"),
                Some(VenueOrderId::from("VEN-CANC-422-OPEN")),
                UUID4::new(),
                UnixNanos::from(1_700_000_000_000_000_000u64),
                None,
                None,
            ))
            .unwrap();

        let canc_ev2 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("expected OrderCancelRejected on 422 cancel error")
            .unwrap();
        match canc_ev2 {
            ExecutionEvent::Order(OrderEventAny::CancelRejected(r)) => {
                assert_eq!(r.client_order_id, ClientOrderId::from("CID-CANC-422-OPEN"));
                assert_eq!(
                    r.venue_order_id,
                    Some(VenueOrderId::from("VEN-CANC-422-OPEN"))
                );
                assert!(
                    r.reason.as_str().contains("cannot be canceled"),
                    "unexpected cancel reject reason: {}",
                    r.reason
                );
            }
            other => panic!("expected OrderCancelRejected, got {other:?}"),
        }

        server.await.unwrap();
    }
}
