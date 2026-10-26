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

use std::{future::Future, sync::Arc};

use async_trait::async_trait;
use indexmap::IndexMap;
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
    Params, UUID4, UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_execution::client::core::ExecutionClientCore;
use nautilus_live::{ExecutionEventEmitter, execution::context::OrderContext};
use nautilus_model::{
    accounts::AccountAny,
    enums::{AccountType, LiquiditySide, OmsType, OrderSide, OrderType},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TradeId, TraderId, Venue,
        VenueOrderId,
    },
    orders::{Order, OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money},
};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use tokio::task::JoinHandle;

use crate::{
    common::{
        consts::ALPACA_VENUE,
        enums::{AlpacaOrderClass, AlpacaOrderSide, AlpacaOrderType, AlpacaTimeInForce},
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
            DEFAULT_EQUITY_PRICE_PRECISION, DEFAULT_EQUITY_SIZE_PRECISION, decimal_to_price,
            decimal_to_quantity, parse_account_state, parse_alpaca_trade_id, parse_instrument_id,
            parse_order_status_report, parse_position_status_report, parse_rfc3339_to_unix_nanos,
        },
    },
    websocket::{
        client::AlpacaWsClient,
        messages::AlpacaTradeEventKind,
        parse::AlpacaExecutionUpdate,
    },
};

const DEDUP_CACHE_CAPACITY: usize = 10_000;

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

#[derive(Debug, Clone)]
struct SharedExecutionTracking {
    cached_orders: Arc<Mutex<FifoCacheMap<ClientOrderId, OrderAny, DEDUP_CACHE_CAPACITY>>>,
    order_contexts: Arc<Mutex<FifoCacheMap<ClientOrderId, OrderContext, DEDUP_CACHE_CAPACITY>>>,
    venue_order_ids: Arc<Mutex<IndexMap<ClientOrderId, Vec<VenueOrderId>>>>,
    submitted_orders: Arc<Mutex<FifoCache<ClientOrderId, DEDUP_CACHE_CAPACITY>>>,
    processed_trades: Arc<Mutex<FifoCache<TradeId, DEDUP_CACHE_CAPACITY>>>,
    terminal_events:
        Arc<Mutex<FifoCache<(VenueOrderId, AlpacaTradeEventKind), DEDUP_CACHE_CAPACITY>>>,
}

impl Default for SharedExecutionTracking {
    fn default() -> Self {
        Self {
            cached_orders: Arc::new(Mutex::new(FifoCacheMap::new())),
            order_contexts: Arc::new(Mutex::new(FifoCacheMap::new())),
            venue_order_ids: Arc::new(Mutex::new(IndexMap::new())),
            submitted_orders: Arc::new(Mutex::new(FifoCache::new())),
            processed_trades: Arc::new(Mutex::new(FifoCache::new())),
            terminal_events: Arc::new(Mutex::new(FifoCache::new())),
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
    }

    fn record_venue_order_id(&self, client_order_id: ClientOrderId, venue_order_id: VenueOrderId) {
        let mut guard = self.venue_order_ids.lock();
        let entry = guard.entry(client_order_id).or_default();
        if !entry.contains(&venue_order_id) {
            entry.push(venue_order_id);
        }
    }

    fn current_venue_order_id(&self, client_order_id: &ClientOrderId) -> Option<VenueOrderId> {
        self.venue_order_ids
            .lock()
            .get(client_order_id)
            .and_then(|ids| ids.last().copied())
    }

    fn clear(&self) {
        self.cached_orders.lock().clear();
        self.order_contexts.lock().clear();
        self.venue_order_ids.lock().clear();
        self.submitted_orders.lock().clear();
        self.processed_trades.lock().clear();
        self.terminal_events.lock().clear();
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
        let ws_client = AlpacaWsClient::new(
            config.clone(),
            account_id,
            DEFAULT_EQUITY_PRICE_PRECISION,
            DEFAULT_EQUITY_SIZE_PRECISION,
            None,
        )?;
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

        Ok(Self {
            core,
            config,
            http_client,
            ws_client: Some(ws_client),
            ws_dispatch_task: None,
            emitter,
            clock,
            tracking: SharedExecutionTracking::default(),
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

    /// Processes a decoded [`AlpacaExecutionUpdate`] (from `trade_updates` WebSocket or DST harness),
    /// applying `FifoCache` deduplication and emitting normalized order/fill events.
    pub fn ingest_execution_update(&self, update: AlpacaExecutionUpdate) {
        if let Ok(order) = self.core.get_order(&update.client_order_id) {
            self.tracking.track_order(&order);
        }
        Self::dispatch_ws_update(&self.emitter, &self.tracking, update);
    }

    fn dispatch_ws_update(
        emitter: &ExecutionEventEmitter,
        tracking: &SharedExecutionTracking,
        update: AlpacaExecutionUpdate,
    ) {
        tracking.record_venue_order_id(update.client_order_id, update.venue_order_id);
        let cached_order = tracking
            .cached_orders
            .lock()
            .get(&update.client_order_id)
            .cloned();

        match update.event_kind {
            AlpacaTradeEventKind::New | AlpacaTradeEventKind::Calculated => {
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
                if let Some(trade_id) = update.trade_id
                    && !tracking.processed_trades.lock().insert(trade_id)
                {
                    log::debug!("Skipping duplicate Alpaca fill trade_id={trade_id}");
                    return;
                }
                if update.event_kind == AlpacaTradeEventKind::Fill {
                    tracking
                        .terminal_events
                        .lock()
                        .add((update.venue_order_id, AlpacaTradeEventKind::Fill));
                }

                if let (Some(order), Some(trade_id), Some(last_qty), Some(last_px)) = (
                    cached_order.as_ref(),
                    update.trade_id,
                    update.last_qty,
                    update.last_px,
                ) {
                    let commission = Money::from_decimal(Decimal::ZERO, Currency::USD()).ok();
                    emitter.emit_order_filled(
                        order,
                        update.venue_order_id,
                        None,
                        trade_id,
                        last_qty,
                        last_px,
                        Currency::USD(),
                        commission,
                        LiquiditySide::Taker,
                        update.ts_event,
                    );
                } else if let Some(fill_report) = update.fill_report {
                    emitter.send_order_with_fills(update.order_report, vec![fill_report]);
                } else {
                    emitter.send_order_status_report(update.order_report);
                }
            }
            AlpacaTradeEventKind::Canceled
            | AlpacaTradeEventKind::DoneForDay
            | AlpacaTradeEventKind::Stopped => {
                let key = (update.venue_order_id, AlpacaTradeEventKind::Canceled);
                if !tracking.terminal_events.lock().insert(key) {
                    log::debug!(
                        "Skipping duplicate Alpaca cancel event for venue_order_id={}",
                        update.venue_order_id
                    );
                    return;
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
                if !tracking.terminal_events.lock().insert(key) {
                    return;
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
                if !tracking.terminal_events.lock().insert(key) {
                    return;
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
                }
            }
            AlpacaTradeEventKind::PendingNew
            | AlpacaTradeEventKind::PendingCancel
            | AlpacaTradeEventKind::PendingReplace => {}
        }
    }

    fn build_single_order_request(order: &OrderAny) -> AlpacaResult<AlpacaOrderRequest> {
        let side = AlpacaOrderSide::try_from(order.order_side())?;
        let order_type = AlpacaOrderType::try_from(order.order_type())?;
        let time_in_force = AlpacaTimeInForce::try_from(order.time_in_force())?;
        let qty = order.quantity().as_decimal();

        let limit_price = order.price().map(|p| p.as_decimal());
        let stop_price = order.trigger_price().map(|p| p.as_decimal());

        let (trail_price, trail_percent) = if order.order_type() == OrderType::TrailingStopMarket {
            let offset = order.trailing_offset();
            (offset, None)
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
        self.tracking.track_order(&order);

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
        let result = block_on_alpaca_future(async move { http.submit_order(&req).await });
        let ts_now = self.clock.get_time_ns();

        match result {
            Ok(resp) => {
                let venue_order_id = VenueOrderId::new(resp.id.trim());
                self.tracking.record_venue_order_id(cid, venue_order_id);
                let ts_accepted = resp
                    .submitted_at
                    .as_deref()
                    .or(Some(resp.created_at.as_str()))
                    .and_then(|s| parse_rfc3339_to_unix_nanos(s).ok())
                    .unwrap_or(ts_now);
                if self
                    .tracking
                    .terminal_events
                    .lock()
                    .insert((venue_order_id, AlpacaTradeEventKind::New))
                {
                    self.emitter
                        .emit_order_accepted(&order, venue_order_id, ts_accepted);
                }
            }
            Err(AlpacaError::DeterministicReject { message, .. }) => {
                self.emitter
                    .emit_order_rejected(&order, &message, ts_now, false);
            }
            Err(other_err) => {
                // Fail-closed: retain order in Submitted state so reconciliation resolves venue truth
                log::error!(
                    "Alpaca submit_order transport/auth error for {cid} (retaining unresolved submission): {other_err}"
                );
            }
        }
        Ok(())
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        let orders = self.core.get_orders_for_list(&cmd.order_list)?;
        if orders.is_empty() {
            return Ok(());
        }

        for order in &orders {
            let cid = order.client_order_id();
            self.tracking.track_order(order);
            self.tracking.submitted_orders.lock().add(cid);
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
        let result = block_on_alpaca_future(async move { http.submit_order(&req).await });
        let ts_now = self.clock.get_time_ns();

        match result {
            Ok(resp) => {
                let parent_order = &orders[0];
                let parent_vid = VenueOrderId::new(resp.id.trim());
                self.tracking
                    .record_venue_order_id(parent_order.client_order_id(), parent_vid);
                let ts_accepted = resp
                    .submitted_at
                    .as_deref()
                    .or(Some(resp.created_at.as_str()))
                    .and_then(|s| parse_rfc3339_to_unix_nanos(s).ok())
                    .unwrap_or(ts_now);
                if self
                    .tracking
                    .terminal_events
                    .lock()
                    .insert((parent_vid, AlpacaTradeEventKind::New))
                {
                    self.emitter
                        .emit_order_accepted(parent_order, parent_vid, ts_accepted);
                }

                let resp_legs = resp.legs.unwrap_or_default();
                for (idx, child_order) in orders.iter().enumerate().skip(1) {
                    // Match child leg by OrderType first, then by positional index
                    let matched_leg = resp_legs
                        .iter()
                        .find(|leg| OrderType::from(leg.order_type) == child_order.order_type())
                        .or_else(|| resp_legs.get(idx - 1));

                    let child_vid = match matched_leg {
                        Some(leg) => VenueOrderId::new(leg.id.trim()),
                        None => VenueOrderId::new(format!("{}-LEG-{idx}", resp.id.trim())),
                    };
                    self.tracking
                        .record_venue_order_id(child_order.client_order_id(), child_vid);
                    if self
                        .tracking
                        .terminal_events
                        .lock()
                        .insert((child_vid, AlpacaTradeEventKind::New))
                    {
                        self.emitter
                            .emit_order_accepted(child_order, child_vid, ts_accepted);
                    }
                }
            }
            Err(AlpacaError::DeterministicReject { message, .. }) => {
                for order in &orders {
                    self.emitter
                        .emit_order_rejected(order, &message, ts_now, false);
                }
            }
            Err(other_err) => {
                log::error!(
                    "Alpaca submit_order_list transport/auth error for {} (retaining unresolved submissions): {other_err}",
                    cmd.order_list.id
                );
            }
        }
        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let order = self.core.get_order(&cmd.client_order_id)?;
        let ts_now = self.clock.get_time_ns();

        let venue_order_id = cmd
            .venue_order_id
            .or_else(|| order.venue_order_id())
            .or_else(|| self.tracking.current_venue_order_id(&cmd.client_order_id));

        let Some(vid) = venue_order_id else {
            self.emitter.emit_order_modify_rejected(
                &order,
                None,
                "cannot modify order without VenueOrderId",
                ts_now,
            );
            return Ok(());
        };

        let replace_req = AlpacaReplaceOrderRequest {
            qty: cmd.quantity.map(|q| q.as_decimal()),
            time_in_force: None,
            limit_price: cmd.price.map(|p| p.as_decimal()),
            stop_price: cmd.trigger_price.map(|p| p.as_decimal()),
            trail: None,
            client_order_id: None,
        };

        let http = Arc::clone(&self.http_client);
        let vid_str = vid.as_str().to_string();
        let result =
            block_on_alpaca_future(async move { http.replace_order(&vid_str, &replace_req).await });

        match result {
            Ok(resp) => {
                let new_vid = VenueOrderId::new(resp.id.trim());
                self.tracking
                    .record_venue_order_id(cmd.client_order_id, new_vid);
                let new_qty = resp
                    .qty
                    .and_then(|d| decimal_to_quantity(d, DEFAULT_EQUITY_SIZE_PRECISION).ok())
                    .or(cmd.quantity)
                    .unwrap_or_else(|| order.quantity());
                let new_px = resp
                    .limit_price
                    .and_then(|d| decimal_to_price(d, DEFAULT_EQUITY_PRICE_PRECISION).ok())
                    .or(cmd.price)
                    .or_else(|| order.price());
                let new_trigger = resp
                    .stop_price
                    .and_then(|d| decimal_to_price(d, DEFAULT_EQUITY_PRICE_PRECISION).ok())
                    .or(cmd.trigger_price)
                    .or_else(|| order.trigger_price());
                self.emitter.emit_order_updated(
                    &order,
                    new_vid,
                    new_qty,
                    new_px,
                    new_trigger,
                    None,
                    ts_now,
                );
            }
            Err(e) => {
                self.emitter.emit_order_modify_rejected(
                    &order,
                    Some(vid),
                    &e.to_string(),
                    ts_now,
                );
            }
        }
        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let cached_order = self.core.get_order(&cmd.client_order_id).ok();
        let ts_now = self.clock.get_time_ns();

        let venue_order_id = cmd
            .venue_order_id
            .or_else(|| cached_order.as_ref().and_then(Order::venue_order_id))
            .or_else(|| self.tracking.current_venue_order_id(&cmd.client_order_id));

        let Some(vid) = venue_order_id else {
            self.emitter.emit_order_cancel_rejected_event(
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
                None,
                "cannot cancel order without VenueOrderId",
                ts_now,
            );
            return Ok(());
        };

        let http = Arc::clone(&self.http_client);
        let vid_str = vid.as_str().to_string();
        let result = block_on_alpaca_future(async move { http.cancel_order(&vid_str).await });

        match result {
            Ok(()) => {
                let key = (vid, AlpacaTradeEventKind::Canceled);
                if self.tracking.terminal_events.lock().insert(key)
                    && let Some(ref order) = cached_order
                {
                    self.emitter
                        .emit_order_canceled(order, Some(vid), ts_now);
                }
            }
            Err(AlpacaError::DeterministicReject { message, .. }) => {
                self.emitter.emit_order_cancel_rejected_event(
                    cmd.strategy_id,
                    cmd.instrument_id,
                    cmd.client_order_id,
                    Some(vid),
                    &message,
                    ts_now,
                );
            }
            Err(other_err) => {
                // Fail-closed: never optimistically mark local orders CANCELED on transport error
                log::error!(
                    "Alpaca cancel_order transport error for {} (venue_order_id={vid}): {other_err}",
                    cmd.client_order_id
                );
            }
        }
        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let http = Arc::clone(&self.http_client);
        let open_orders =
            block_on_alpaca_future(async move { http.list_orders(Some("open")).await })?;
        let ts_now = self.clock.get_time_ns();
        let target_symbol = cmd.instrument_id.symbol.as_str();

        for open_order in open_orders {
            if open_order.symbol.trim() != target_symbol {
                continue;
            }
            let order_side = OrderSide::from(open_order.side);
            if let Some(side_filter) = cmd.order_side
                && order_side != side_filter
            {
                continue;
            }
            let vid = VenueOrderId::new(open_order.id.trim());
            let http_cancel = Arc::clone(&self.http_client);
            let vid_str = vid.as_str().to_string();
            if let Ok(()) =
                block_on_alpaca_future(async move { http_cancel.cancel_order(&vid_str).await })
            {
                let key = (vid, AlpacaTradeEventKind::Canceled);
                if self.tracking.terminal_events.lock().insert(key)
                    && let Ok(cid) = ClientOrderId::new_checked(open_order.client_order_id.trim())
                    && let Ok(order) = self.core.get_order(&cid)
                {
                    self.emitter.emit_order_canceled(&order, Some(vid), ts_now);
                }
            }
        }
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
        let resp = block_on_alpaca_future(async move {
            http.get_order_by_client_order_id(&cid_str).await
        })?;
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
                Ok(resp) => {
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

        if let Some(vid) = cmd.venue_order_id {
            let orders = self.http_client.list_orders(Some("all")).await?;
            for resp in Self::flatten_order_responses(orders) {
                if resp.id.trim() == vid.as_str() {
                    let report = parse_order_status_report(
                        &resp,
                        self.core.account_id,
                        &self.config.venue,
                        ts_init,
                    )?;
                    return Ok(Some(report));
                }
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
        let ts_init = self.clock.get_time_ns();

        let mut reports = Vec::with_capacity(flat_orders.len());
        for resp in flat_orders {
            if let Some(ref prev_id) = resp.replaces
                && !resp.client_order_id.trim().is_empty()
                && let Ok(cid) = ClientOrderId::new_checked(resp.client_order_id.trim())
            {
                self.tracking
                    .record_venue_order_id(cid, VenueOrderId::new(prev_id.trim()));
                self.tracking
                    .record_venue_order_id(cid, VenueOrderId::new(resp.id.trim()));
            }

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
        let raw_orders = self.http_client.list_orders(Some("all")).await?;
        let flat_orders = Self::flatten_order_responses(raw_orders);
        let ts_init = self.clock.get_time_ns();

        let mut fills = Vec::new();
        for resp in flat_orders {
            if resp.filled_qty <= Decimal::ZERO {
                continue;
            }
            let Some(avg_px_dec) = resp.filled_avg_price else {
                continue;
            };
            if avg_px_dec <= Decimal::ZERO {
                continue;
            }

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

            let client_order_id = if resp.client_order_id.trim().is_empty() {
                None
            } else {
                ClientOrderId::new_checked(resp.client_order_id.trim()).ok()
            };

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

            let last_qty = decimal_to_quantity(resp.filled_qty, DEFAULT_EQUITY_SIZE_PRECISION)?;
            let last_px = decimal_to_price(avg_px_dec, DEFAULT_EQUITY_PRICE_PRECISION)?;
            let commission = Money::from_decimal(Decimal::ZERO, Currency::USD())?;
            let trade_id = parse_alpaca_trade_id("FILL-", resp.id.trim())?;

            fills.push(FillReport::new(
                self.core.account_id,
                instrument_id,
                venue_order_id,
                trade_id,
                OrderSide::from(resp.side),
                last_qty,
                last_px,
                commission,
                LiquiditySide::Taker,
                client_order_id,
                None,
                ts_event_ns,
                ts_init,
                Some(UUID4::new()),
            ));
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
