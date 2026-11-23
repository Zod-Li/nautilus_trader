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

//! Blind-TDD integration tests for the `PreTradeRiskGate` micro-seam in `nautilus-risk`.

use std::{
    cell::RefCell,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use ahash::{AHashMap, AHashSet};
use nautilus_common::{
    cache::Cache,
    clock::{Clock, VirtualClock},
    messages::execution::{
        BatchModifyOrders, ModifyOrder, SubmitOrder, SubmitOrderList, TradingCommand,
    },
    msgbus::{
        self, MessagingSwitchboard,
        stubs::{TypedIntoMessageSavingHandler, get_typed_into_message_saving_handler},
    },
    throttler::RateLimit,
};
use nautilus_core::{DurationNanos, UUID4, UnixNanos};
use nautilus_model::{
    accounts::{AccountAny, stubs::{cash_account, margin_account}},
    data::stubs::quote_audusd,
    enums::{LiquiditySide, OmsType, OrderSide, OrderType, TimeInForce, TradingState},
    events::{
        OrderAccepted, OrderEventAny, OrderFilled, OrderSubmitted,
        account::stubs::{cash_account_state_million_usd, margin_account_state},
        order::spec::OrderUpdatedSpec,
    },
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, PositionId, StrategyId,
        TradeId, TraderId, VenueOrderId,
        stubs::{client_id_binance, strategy_id_ema_cross, trader_id, uuid4},
    },
    instruments::{Instrument, InstrumentAny, stubs::audusd_sim},
    orders::{Order, OrderAny, OrderList, OrderTestBuilder},
    position::Position,
    types::{Currency, Money, Price, Quantity},
};
use nautilus_portfolio::Portfolio;
use nautilus_risk::{
    PreTradeRiskGate,
    engine::{RiskEngine, config::RiskEngineConfig},
};
use rust_decimal::Decimal;
use rstest::rstest;
use ustr::Ustr;

#[derive(Debug, Clone)]
enum GateBehavior {
    Allow,
    Deny(String),
}

#[derive(Debug)]
struct RecordingPreTradeGate {
    behavior: Mutex<GateBehavior>,
    check_calls: AtomicUsize,
    observed_cache_orders: AtomicUsize,
    denied_callbacks: Mutex<Vec<ClientOrderId>>,
    updated_callbacks: Mutex<Vec<(ClientOrderId, Decimal)>>,
}

impl RecordingPreTradeGate {
    fn new(behavior: GateBehavior) -> Self {
        Self {
            behavior: Mutex::new(behavior),
            check_calls: AtomicUsize::new(0),
            observed_cache_orders: AtomicUsize::new(0),
            denied_callbacks: Mutex::new(Vec::new()),
            updated_callbacks: Mutex::new(Vec::new()),
        }
    }

    fn set_behavior(&self, behavior: GateBehavior) {
        *self.behavior.lock().unwrap() = behavior;
    }

    fn check_calls(&self) -> usize {
        self.check_calls.load(Ordering::SeqCst)
    }

    fn observed_cache_orders(&self) -> usize {
        self.observed_cache_orders.load(Ordering::SeqCst)
    }

    fn denied_callbacks(&self) -> Vec<ClientOrderId> {
        self.denied_callbacks.lock().unwrap().clone()
    }

    fn updated_callbacks(&self) -> Vec<(ClientOrderId, Decimal)> {
        self.updated_callbacks.lock().unwrap().clone()
    }
}

impl PreTradeRiskGate for RecordingPreTradeGate {
    fn check_command(
        &self,
        command: &TradingCommand,
        cache: &Cache,
        _ts_now: UnixNanos,
    ) -> Result<(), String> {
        self.check_calls.fetch_add(1, Ordering::SeqCst);

        // Actively exercise read-only Cache borrows inside check_command so any
        // missing drop(cache_ref) before deny_command/reject_modify_order would
        // trigger a RefCell BorrowMutError panic.
        let count = cache.orders(None, None, None, None, None).len();
        self.observed_cache_orders.store(count, Ordering::SeqCst);
        if let TradingCommand::SubmitOrder(cmd) = command {
            let _ = cache.order(&cmd.client_order_id);
        }

        match &*self.behavior.lock().unwrap() {
            GateBehavior::Allow => Ok(()),
            GateBehavior::Deny(reason) => Err(reason.clone()),
        }
    }

    fn on_order_denied(&self, client_order_id: &ClientOrderId) {
        self.denied_callbacks
            .lock()
            .unwrap()
            .push(*client_order_id);
    }

    fn on_order_updated(&self, client_order_id: &ClientOrderId, new_qty: Decimal) {
        self.updated_callbacks
            .lock()
            .unwrap()
            .push((*client_order_id, new_qty));
    }
}

fn register_process_handler() -> TypedIntoMessageSavingHandler<OrderEventAny> {
    let (handler, saving_handler) =
        get_typed_into_message_saving_handler::<OrderEventAny>(Some(Ustr::from("ExecEngine.process")));
    msgbus::register_order_event_endpoint(MessagingSwitchboard::exec_engine_process(), handler);
    saving_handler
}

fn register_execute_handler() -> TypedIntoMessageSavingHandler<TradingCommand> {
    let (handler, saving_handler) = get_typed_into_message_saving_handler::<TradingCommand>(Some(
        Ustr::from("ExecEngine.queue_execute"),
    ));
    msgbus::register_trading_command_endpoint(
        MessagingSwitchboard::exec_engine_queue_execute(),
        handler,
    );
    saving_handler
}

fn build_risk_engine(
    cache: Cache,
    bypass: bool,
    max_notional_per_order: AHashMap<InstrumentId, Decimal>,
) -> RiskEngine {
    let cache = Rc::new(RefCell::new(cache));
    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    let portfolio = Portfolio::new(
        Rc::clone(&clock) as Rc<RefCell<dyn Clock>>,
        Rc::clone(&cache),
        None,
    );
    let config = RiskEngineConfig {
        debug: true,
        bypass,
        max_order_submit: RateLimit::new(100, DurationNanos::new(1_000_000_000)),
        max_order_modify: RateLimit::new(100, DurationNanos::new(1_000_000_000)),
        max_notional_per_order,
        full_position_exit_venues: AHashSet::new(),
    };
    RiskEngine::new(config, portfolio, clock, cache)
}

fn setup_audusd_cache() -> (Cache, InstrumentAny) {
    let instrument = InstrumentAny::CurrencyPair(audusd_sim());
    let mut cache = Cache::default();
    cache.add_instrument(instrument.clone()).unwrap();
    cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd("1000000 USD", "0 USD", "1000000 USD"),
        )))
        .unwrap();
    cache.add_quote(quote_audusd()).unwrap();
    (cache, instrument)
}

fn make_accepted_limit_order(
    instrument_id: InstrumentId,
    cid: &str,
    qty: &str,
    price: &str,
    account_id: AccountId,
) -> OrderAny {
    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_id)
        .client_order_id(ClientOrderId::from(cid))
        .side(OrderSide::Buy)
        .quantity(Quantity::from(qty))
        .price(Price::from(price))
        .time_in_force(TimeInForce::Gtc)
        .build();
    let submitted = OrderSubmitted::new(
        order.trader_id(),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        account_id,
        UUID4::new(),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    order.apply(OrderEventAny::Submitted(submitted)).unwrap();
    let accepted = OrderAccepted::new(
        order.trader_id(),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        VenueOrderId::from(cid),
        account_id,
        UUID4::new(),
        UnixNanos::default(),
        UnixNanos::default(),
        false,
    );
    order.apply(OrderEventAny::Accepted(accepted)).unwrap();
    order
}

#[rstest]
fn test_pre_trade_gate_denies_submit_order_without_refcell_panic(
    trader_id: TraderId,
    client_id_binance: ClientId,
    strategy_id_ema_cross: StrategyId,
) {
    let process_handler = register_process_handler();
    let execute_handler = register_execute_handler();
    let (mut cache, instrument) = setup_audusd_cache();

    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-GATE-SUBMIT-1"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.00000"))
        .build();
    cache
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let mut risk_engine = build_risk_engine(cache, false, AHashMap::new());
    let gate = Arc::new(RecordingPreTradeGate::new(GateBehavior::Deny(
        "PLAN_GATE_CAP_EXCEEDED".to_string(),
    )));
    risk_engine.set_pre_trade_gate(Some(gate.clone()));

    let submit = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        uuid4(),
        UnixNanos::from(100),
        None,
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit));

    assert_eq!(gate.check_calls(), 1);
    assert_eq!(gate.observed_cache_orders(), 1);
    assert!(execute_handler.get_messages().is_empty());

    let events = process_handler.get_messages();
    assert_eq!(events.len(), 1);
    match &events[0] {
        OrderEventAny::Denied(denied) => {
            assert_eq!(denied.client_order_id, order.client_order_id());
            assert!(
                denied.reason.as_str().contains("PLAN_GATE_CAP_EXCEEDED"),
                "unexpected denial reason: {}",
                denied.reason
            );
        }
        other => panic!("expected OrderEventAny::Denied, got {other:?}"),
    }
}

#[rstest]
fn test_pre_trade_gate_denies_submit_order_list_without_refcell_panic(
    trader_id: TraderId,
    client_id_binance: ClientId,
    strategy_id_ema_cross: StrategyId,
) {
    let process_handler = register_process_handler();
    let execute_handler = register_execute_handler();
    let (mut cache, instrument) = setup_audusd_cache();

    let list_id = OrderListId::from("OL-GATE-1");
    let order1 = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-LIST-1"))
        .order_list_id(list_id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from("50"))
        .price(Price::from("1.00000"))
        .build();
    let order2 = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-LIST-2"))
        .order_list_id(list_id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from("50"))
        .price(Price::from("1.00000"))
        .build();
    cache
        .add_order(order1.clone(), None, Some(client_id_binance), false)
        .unwrap();
    cache
        .add_order(order2.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let order_list = OrderList::new(
        list_id,
        instrument.id(),
        strategy_id_ema_cross,
        vec![order1.client_order_id(), order2.client_order_id()],
        UnixNanos::from(100),
    );
    let submit_list = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        order_list,
        vec![order1.init_event().clone(), order2.init_event().clone()],
        None,
        None,
        None,
        uuid4(),
        UnixNanos::from(100),
        None,
    );

    let mut risk_engine = build_risk_engine(cache, false, AHashMap::new());
    let gate = Arc::new(RecordingPreTradeGate::new(GateBehavior::Deny(
        "ATOMIC_LIST_DENIED".to_string(),
    )));
    risk_engine.set_pre_trade_gate(Some(gate.clone()));

    risk_engine.execute(TradingCommand::SubmitOrderList(submit_list));

    assert_eq!(gate.check_calls(), 1);
    assert_eq!(gate.observed_cache_orders(), 2);
    assert!(execute_handler.get_messages().is_empty());

    let events = process_handler.get_messages();
    assert_eq!(events.len(), 2);
    for (idx, event) in events.iter().enumerate() {
        match event {
            OrderEventAny::Denied(denied) => {
                assert!(denied.reason.as_str().contains("ATOMIC_LIST_DENIED"));
                let expected_cid = if idx == 0 {
                    order1.client_order_id()
                } else {
                    order2.client_order_id()
                };
                assert_eq!(denied.client_order_id, expected_cid);
            }
            other => panic!("expected OrderEventAny::Denied, got {other:?}"),
        }
    }
}

#[rstest]
fn test_pre_trade_gate_rejects_modify_order_and_modify_orders_without_refcell_panic(
    client_id_binance: ClientId,
) {
    let process_handler = register_process_handler();
    let execute_handler = register_execute_handler();
    let (mut cache, instrument) = setup_audusd_cache();

    let account_id = AccountId::from("SIM-001");
    let order1 = make_accepted_limit_order(
        instrument.id(),
        "O-MOD-1",
        "100",
        "1.00000",
        account_id,
    );
    let order2 = make_accepted_limit_order(
        instrument.id(),
        "O-MOD-2",
        "100",
        "1.00000",
        account_id,
    );
    cache
        .add_order(order1.clone(), None, Some(client_id_binance), false)
        .unwrap();
    cache
        .add_order(order2.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let mut risk_engine = build_risk_engine(cache, false, AHashMap::new());
    let gate = Arc::new(RecordingPreTradeGate::new(GateBehavior::Deny(
        "ENTRY_UPWARD_MODIFY_PROHIBITED".to_string(),
    )));
    risk_engine.set_pre_trade_gate(Some(gate.clone()));

    let mod1 = ModifyOrder::new(
        order1.trader_id(),
        Some(client_id_binance),
        order1.strategy_id(),
        order1.instrument_id(),
        order1.client_order_id(),
        order1.venue_order_id(),
        Some(Quantity::from("150")),
        None,
        None,
        UUID4::new(),
        UnixNanos::from(200),
        None,
        None,
    );
    risk_engine.execute(TradingCommand::ModifyOrder(mod1.clone()));

    let mod2 = ModifyOrder::new(
        order2.trader_id(),
        Some(client_id_binance),
        order2.strategy_id(),
        order2.instrument_id(),
        order2.client_order_id(),
        order2.venue_order_id(),
        Some(Quantity::from("120")),
        None,
        None,
        UUID4::new(),
        UnixNanos::from(201),
        None,
        None,
    );
    let batch = BatchModifyOrders::new(
        order1.trader_id(),
        Some(client_id_binance),
        order1.strategy_id(),
        order1.instrument_id(),
        vec![mod1, mod2],
        UUID4::new(),
        UnixNanos::from(202),
        None,
        None,
    );
    risk_engine.execute(TradingCommand::ModifyOrders(batch));

    assert_eq!(gate.check_calls(), 2);
    assert!(execute_handler.get_messages().is_empty());

    let events = process_handler.get_messages();
    assert_eq!(events.len(), 3);
    for event in &events {
        match event {
            OrderEventAny::ModifyRejected(rejected) => {
                assert!(
                    rejected
                        .reason
                        .as_str()
                        .contains("ENTRY_UPWARD_MODIFY_PROHIBITED"),
                    "unexpected reject reason: {}",
                    rejected.reason
                );
            }
            other => panic!("expected OrderEventAny::ModifyRejected, got {other:?}"),
        }
    }
}

#[rstest]
fn test_pre_trade_gate_enforces_denials_even_when_risk_engine_bypass_is_true(
    trader_id: TraderId,
    client_id_binance: ClientId,
    strategy_id_ema_cross: StrategyId,
) {
    let process_handler = register_process_handler();
    let execute_handler = register_execute_handler();
    let (mut cache, instrument) = setup_audusd_cache();

    let order_submit = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-BYPASS-1"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("25"))
        .price(Price::from("1.00000"))
        .build();
    let order_mod = make_accepted_limit_order(
        instrument.id(),
        "O-BYPASS-MOD",
        "25",
        "1.00000",
        AccountId::from("SIM-001"),
    );
    cache
        .add_order(order_submit.clone(), None, Some(client_id_binance), false)
        .unwrap();
    cache
        .add_order(order_mod.clone(), None, Some(client_id_binance), false)
        .unwrap();

    // RiskEngineConfig.bypass = true!
    let mut risk_engine = build_risk_engine(cache, true, AHashMap::new());
    let gate = Arc::new(RecordingPreTradeGate::new(GateBehavior::Deny(
        "BYPASS_RESILIENT_GATE".to_string(),
    )));
    risk_engine.set_pre_trade_gate(Some(gate.clone()));

    let submit_cmd = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument.id(),
        order_submit.client_order_id(),
        order_submit.init_event().clone(),
        None,
        None,
        None,
        uuid4(),
        UnixNanos::from(300),
        None,
    );
    risk_engine.execute(TradingCommand::SubmitOrder(submit_cmd.clone()));

    let modify_cmd = ModifyOrder::new(
        order_mod.trader_id(),
        Some(client_id_binance),
        order_mod.strategy_id(),
        order_mod.instrument_id(),
        order_mod.client_order_id(),
        order_mod.venue_order_id(),
        Some(Quantity::from("50")),
        None,
        None,
        UUID4::new(),
        UnixNanos::from(301),
        None,
        None,
    );
    risk_engine.execute(TradingCommand::ModifyOrder(modify_cmd));

    assert_eq!(gate.check_calls(), 2);
    assert!(
        execute_handler.get_messages().is_empty(),
        "gate denial must prevent command execution even with RiskEngineConfig.bypass=true"
    );
    assert_eq!(process_handler.get_messages().len(), 2);

    // Once gate allows, bypass forwards cleanly.
    gate.set_behavior(GateBehavior::Allow);
    risk_engine.execute(TradingCommand::SubmitOrder(submit_cmd));
    assert_eq!(execute_handler.get_messages().len(), 1);
}

#[rstest]
fn test_pre_trade_gate_on_order_denied_invoked_on_downstream_risk_denial(
    trader_id: TraderId,
    client_id_binance: ClientId,
    strategy_id_ema_cross: StrategyId,
) {
    let process_handler = register_process_handler();
    let execute_handler = register_execute_handler();
    let (mut cache, instrument) = setup_audusd_cache();

    let order_halted = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-DOWNSTREAM-HALTED"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("10"))
        .price(Price::from("1.00000"))
        .build();
    let list_id = OrderListId::from("OL-DOWNSTREAM");
    let list_leg1 = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-DOWNSTREAM-L1"))
        .order_list_id(list_id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from("10"))
        .price(Price::from("1.00000"))
        .build();
    let list_leg2 = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-DOWNSTREAM-L2"))
        .order_list_id(list_id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from("10"))
        .price(Price::from("1.00000"))
        .build();

    for o in [&order_halted, &list_leg1, &list_leg2] {
        cache
            .add_order(o.clone(), None, Some(client_id_binance), false)
            .unwrap();
    }

    let mut risk_engine = build_risk_engine(cache, false, AHashMap::new());
    risk_engine.set_trading_state(TradingState::Halted);

    let gate = Arc::new(RecordingPreTradeGate::new(GateBehavior::Allow));
    risk_engine.set_pre_trade_gate(Some(gate.clone()));

    let submit = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument.id(),
        order_halted.client_order_id(),
        order_halted.init_event().clone(),
        None,
        None,
        None,
        uuid4(),
        UnixNanos::from(400),
        None,
    );
    risk_engine.execute(TradingCommand::SubmitOrder(submit));

    let order_list = OrderList::new(
        list_id,
        instrument.id(),
        strategy_id_ema_cross,
        vec![list_leg1.client_order_id(), list_leg2.client_order_id()],
        UnixNanos::from(401),
    );
    let submit_list = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        order_list,
        vec![
            list_leg1.init_event().clone(),
            list_leg2.init_event().clone(),
        ],
        None,
        None,
        None,
        uuid4(),
        UnixNanos::from(401),
        None,
    );
    risk_engine.execute(TradingCommand::SubmitOrderList(submit_list));

    assert!(execute_handler.get_messages().is_empty());
    assert_eq!(process_handler.get_messages().len(), 3);
    assert_eq!(
        gate.denied_callbacks(),
        vec![
            order_halted.client_order_id(),
            list_leg1.client_order_id(),
            list_leg2.client_order_id(),
        ]
    );
}

#[rstest]
fn test_pre_trade_gate_can_be_cleared_with_none(
    trader_id: TraderId,
    client_id_binance: ClientId,
    strategy_id_ema_cross: StrategyId,
) {
    let process_handler = register_process_handler();
    let execute_handler = register_execute_handler();
    let (mut cache, instrument) = setup_audusd_cache();

    let order1 = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-CLR-1"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1000"))
        .price(Price::from("1.00000"))
        .build();
    let order2 = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-CLR-2"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1000"))
        .price(Price::from("1.00000"))
        .build();
    cache
        .add_order(order1.clone(), None, Some(client_id_binance), false)
        .unwrap();
    cache
        .add_order(order2.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let mut risk_engine = build_risk_engine(cache, false, AHashMap::new());
    let gate = Arc::new(RecordingPreTradeGate::new(GateBehavior::Deny(
        "TEMPORARY_BLOCK".to_string(),
    )));
    risk_engine.set_pre_trade_gate(Some(gate));

    let cmd1 = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument.id(),
        order1.client_order_id(),
        order1.init_event().clone(),
        None,
        None,
        None,
        uuid4(),
        UnixNanos::from(500),
        None,
    );
    risk_engine.execute(TradingCommand::SubmitOrder(cmd1));
    assert_eq!(process_handler.get_messages().len(), 1);
    assert!(execute_handler.get_messages().is_empty());

    // Clearing the gate allows normal RiskEngine execution.
    risk_engine.set_pre_trade_gate(None);
    let cmd2 = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument.id(),
        order2.client_order_id(),
        order2.init_event().clone(),
        None,
        None,
        None,
        uuid4(),
        UnixNanos::from(501),
        None,
    );
    risk_engine.execute(TradingCommand::SubmitOrder(cmd2));
    assert_eq!(execute_handler.get_messages().len(), 1);
}

#[rstest]
fn test_pre_trade_gate_on_order_updated_invoked_on_order_updated_event(
    client_id_binance: ClientId,
) {
    let _process_handler = register_process_handler();
    let _execute_handler = register_execute_handler();
    let (mut cache, instrument) = setup_audusd_cache();

    let account_id = AccountId::from("SIM-001");
    let order = make_accepted_limit_order(
        instrument.id(),
        "O-GATE-UPD-1",
        "80",
        "1.00000",
        account_id,
    );
    cache
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let mut risk_engine = build_risk_engine(cache, false, AHashMap::new());
    let gate = Arc::new(RecordingPreTradeGate::new(GateBehavior::Allow));
    risk_engine.set_pre_trade_gate(Some(gate.clone()));

    let submitted = OrderSubmitted::new(
        order.trader_id(),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        account_id,
        UUID4::new(),
        UnixNanos::from(600),
        UnixNanos::from(600),
    );
    risk_engine.process(OrderEventAny::Submitted(submitted));
    assert!(
        gate.updated_callbacks().is_empty(),
        "non-Updated event must not invoke on_order_updated"
    );

    let updated = OrderUpdatedSpec::builder()
        .trader_id(order.trader_id())
        .strategy_id(order.strategy_id())
        .instrument_id(order.instrument_id())
        .client_order_id(order.client_order_id())
        .quantity(Quantity::from("30"))
        .maybe_venue_order_id(order.venue_order_id())
        .maybe_account_id(Some(account_id))
        .maybe_price(Some(Price::from("1.00000")))
        .build();
    risk_engine.process(OrderEventAny::Updated(updated));

    assert_eq!(
        gate.updated_callbacks(),
        vec![(order.client_order_id(), Decimal::from(30))]
    );
}

#[rstest]
fn test_pre_trade_gate_rejects_batch_modify_orders_deduplicates_repeated_cids(
    client_id_binance: ClientId,
) {
    let process_handler = register_process_handler();
    let execute_handler = register_execute_handler();
    let (mut cache, instrument) = setup_audusd_cache();

    let account_id = AccountId::from("SIM-001");
    let order1 = make_accepted_limit_order(
        instrument.id(),
        "O-DEDUP-1",
        "100",
        "1.00000",
        account_id,
    );
    let order2 = make_accepted_limit_order(
        instrument.id(),
        "O-DEDUP-2",
        "100",
        "1.00000",
        account_id,
    );
    cache
        .add_order(order1.clone(), None, Some(client_id_binance), false)
        .unwrap();
    cache
        .add_order(order2.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let mut risk_engine = build_risk_engine(cache, false, AHashMap::new());
    let gate = Arc::new(RecordingPreTradeGate::new(GateBehavior::Deny(
        "ENTRY_UPWARD_MODIFY_PROHIBITED".to_string(),
    )));
    risk_engine.set_pre_trade_gate(Some(gate.clone()));

    let make_mod = |order: &OrderAny, qty: &str, ts: u64| {
        ModifyOrder::new(
            order.trader_id(),
            Some(client_id_binance),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            order.venue_order_id(),
            Some(Quantity::from(qty)),
            None,
            None,
            UUID4::new(),
            UnixNanos::from(ts),
            None,
            None,
        )
    };

    let batch = BatchModifyOrders::new(
        order1.trader_id(),
        Some(client_id_binance),
        order1.strategy_id(),
        order1.instrument_id(),
        vec![
            make_mod(&order1, "110", 700),
            make_mod(&order1, "120", 701),
            make_mod(&order2, "130", 702),
            make_mod(&order1, "140", 703),
        ],
        UUID4::new(),
        UnixNanos::from(704),
        None,
        None,
    );
    risk_engine.execute(TradingCommand::ModifyOrders(batch));

    assert_eq!(gate.check_calls(), 1);
    assert!(execute_handler.get_messages().is_empty());

    let events = process_handler.get_messages();
    assert_eq!(
        events.len(),
        2,
        "repeated CIDs in ModifyOrders must emit only 1 OrderModifyRejected per unique CID, got {events:?}"
    );
    let mut rejected_cids = Vec::new();
    for event in &events {
        match event {
            OrderEventAny::ModifyRejected(rejected) => {
                assert!(
                    rejected
                        .reason
                        .as_str()
                        .contains("ENTRY_UPWARD_MODIFY_PROHIBITED")
                );
                rejected_cids.push(rejected.client_order_id);
            }
            other => panic!("expected OrderEventAny::ModifyRejected, got {other:?}"),
        }
    }
    assert_eq!(
        rejected_cids,
        vec![order1.client_order_id(), order2.client_order_id()]
    );
}

#[derive(Debug)]
struct CapacityTrackingPreTradeGate {
    reserved_by_cid: Mutex<AHashMap<ClientOrderId, Decimal>>,
    check_calls: AtomicUsize,
    denied_callbacks: Mutex<Vec<ClientOrderId>>,
}

impl CapacityTrackingPreTradeGate {
    fn new() -> Self {
        Self {
            reserved_by_cid: Mutex::new(AHashMap::new()),
            check_calls: AtomicUsize::new(0),
            denied_callbacks: Mutex::new(Vec::new()),
        }
    }

    fn check_calls(&self) -> usize {
        self.check_calls.load(Ordering::SeqCst)
    }

    fn reserved_total(&self) -> Decimal {
        self.reserved_by_cid.lock().unwrap().values().copied().sum()
    }

    fn denied_callbacks(&self) -> Vec<ClientOrderId> {
        self.denied_callbacks.lock().unwrap().clone()
    }
}

impl PreTradeRiskGate for CapacityTrackingPreTradeGate {
    fn check_command(
        &self,
        command: &TradingCommand,
        cache: &Cache,
        _ts_now: UnixNanos,
    ) -> Result<(), String> {
        self.check_calls.fetch_add(1, Ordering::SeqCst);
        if let TradingCommand::SubmitOrder(cmd) = command
            && let Some(order) = cache.order(&cmd.client_order_id)
            && !order.is_reduce_only()
        {
            self.reserved_by_cid
                .lock()
                .unwrap()
                .insert(cmd.client_order_id, order.quantity().as_decimal());
        }
        Ok(())
    }

    fn on_order_denied(&self, client_order_id: &ClientOrderId) {
        self.denied_callbacks
            .lock()
            .unwrap()
            .push(*client_order_id);
        self.reserved_by_cid
            .lock()
            .unwrap()
            .remove(client_order_id);
    }
}

#[rstest]
fn test_pre_trade_gate_on_order_denied_invoked_on_trading_state_reducing_and_permits_reduce_only_exit(
    trader_id: TraderId,
    client_id_binance: ClientId,
    strategy_id_ema_cross: StrategyId,
) {
    let process_handler = register_process_handler();
    let execute_handler = register_execute_handler();
    let instrument = InstrumentAny::CurrencyPair(audusd_sim());
    let mut cache = Cache::default();
    cache.add_instrument(instrument.clone()).unwrap();
    cache
        .add_account(AccountAny::Margin(margin_account(margin_account_state())))
        .unwrap();
    cache.add_quote(quote_audusd()).unwrap();

    // Open a LONG 100 position (audusd_sim min_quantity=100) so SELL reduce_only=true is a valid
    // position-reducing exit and BUY reduce_only=false is unambiguously a non-reducing entry.
    let position_id = PositionId::from("P-REDUCING-1");
    let open_fill = OrderFilled::new(
        trader_id,
        strategy_id_ema_cross,
        instrument.id(),
        ClientOrderId::from("O-REDUCING-OPEN"),
        VenueOrderId::from("V-REDUCING-OPEN"),
        AccountId::from("SIM-001"),
        TradeId::from("T-REDUCING-OPEN"),
        OrderSide::Buy,
        OrderType::Limit,
        Quantity::from("100"),
        Price::from("1.00000"),
        Currency::USD(),
        LiquiditySide::Taker,
        UUID4::new(),
        UnixNanos::from(799),
        UnixNanos::from(799),
        false,
        Some(position_id),
        Some(Money::from("0.00 USD")),
        None,
    );
    cache
        .add_position(&Position::new(&instrument, open_fill), OmsType::Netting)
        .unwrap();

    let entry_order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-REDUCING-ENTRY"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.00000"))
        .reduce_only(false)
        .build();
    let exit_order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-REDUCING-EXIT"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.00000"))
        .reduce_only(true)
        .build();

    cache
        .add_order(entry_order.clone(), None, Some(client_id_binance), false)
        .unwrap();
    cache
        .add_order(
            exit_order.clone(),
            Some(position_id),
            Some(client_id_binance),
            false,
        )
        .unwrap();

    let mut risk_engine = build_risk_engine(cache, false, AHashMap::new());
    risk_engine.set_trading_state(TradingState::Reducing);

    let gate = Arc::new(CapacityTrackingPreTradeGate::new());
    risk_engine.set_pre_trade_gate(Some(gate.clone()));

    let submit_entry = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument.id(),
        entry_order.client_order_id(),
        entry_order.init_event().clone(),
        None,
        None,
        None,
        uuid4(),
        UnixNanos::from(800),
        None,
    );
    risk_engine.execute(TradingCommand::SubmitOrder(submit_entry));

    assert_eq!(gate.check_calls(), 1);
    assert_eq!(
        gate.denied_callbacks(),
        vec![entry_order.client_order_id()],
        "TradingState::Reducing denial must invoke on_order_denied rollback"
    );
    assert_eq!(
        gate.reserved_total(),
        Decimal::ZERO,
        "TradingState::Reducing denial must leave 0 leaked capacity"
    );
    assert!(execute_handler.get_messages().is_empty());

    let denied_events = process_handler.get_messages();
    assert_eq!(denied_events.len(), 1);
    match &denied_events[0] {
        OrderEventAny::Denied(denied) => {
            assert_eq!(denied.client_order_id, entry_order.client_order_id());
            assert!(
                denied.reason.as_str().contains("REDUCING")
                    || denied.reason.as_str().to_lowercase().contains("reducing")
                    || denied.reason.as_str().to_lowercase().contains("reduce"),
                "expected TradingState::Reducing denial reason, got: {}",
                denied.reason
            );
        }
        other => panic!("expected OrderEventAny::Denied, got {other:?}"),
    }

    // Reducing state must still permit a reduce_only exit order.
    let submit_exit = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument.id(),
        exit_order.client_order_id(),
        exit_order.init_event().clone(),
        None,
        Some(position_id),
        None,
        uuid4(),
        UnixNanos::from(801),
        None,
    );
    risk_engine.execute(TradingCommand::SubmitOrder(submit_exit));

    assert_eq!(gate.check_calls(), 2);
    assert_eq!(gate.reserved_total(), Decimal::ZERO);
    let post_exit_events = process_handler.get_messages();
    assert_eq!(
        gate.denied_callbacks(),
        vec![entry_order.client_order_id()],
        "reduce_only exit order must not trigger on_order_denied (events={post_exit_events:?})"
    );
    let executed = execute_handler.get_messages();
    assert_eq!(executed.len(), 1, "reduce_only exit must be forwarded to execution");
    match &executed[0] {
        TradingCommand::SubmitOrder(cmd) => {
            assert_eq!(cmd.client_order_id, exit_order.client_order_id());
        }
        other => panic!("expected TradingCommand::SubmitOrder, got {other:?}"),
    }
}
