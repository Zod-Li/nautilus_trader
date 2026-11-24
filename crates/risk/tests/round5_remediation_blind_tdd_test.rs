// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
// -------------------------------------------------------------------------------------------------

//! Blind TDD regression tests for Round 5 `nautilus-risk` (`D5-R5-P1-1`):
//! Verify `PreTradeRiskGate::on_order_denied` is invoked **EXACTLY ONCE** per denied client order ID
//! (never 0 times, never twice) across:
//! 1. `SubmitOrder` dropped because order is missing from cache,
//! 2. `SubmitOrderList` rejected due to incomplete order list (`OrderListIncomplete`) or
//!    missing instrument (`InstrumentNotFound`, covering ALL sibling CIDs in the list),
//! 3. `TradingState::Halted` (`SubmitOrder` and `SubmitOrderList`), and
//! 4. `TradingState::Reducing` (`SubmitOrder` non-reducing entry denied once while reduce-only exit
//!    passes, and `SubmitOrderList` denied once per CID).

#![allow(linker_messages)]

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
    messages::execution::{SubmitOrder, SubmitOrderList, TradingCommand},
    msgbus::{
        self, MessagingSwitchboard,
        stubs::{TypedIntoMessageSavingHandler, get_typed_into_message_saving_handler},
    },
    throttler::RateLimit,
};
use nautilus_core::{DurationNanos, UUID4, UnixNanos};
use nautilus_model::{
    accounts::{
        AccountAny,
        stubs::{cash_account, margin_account},
    },
    data::stubs::quote_audusd,
    enums::{LiquiditySide, OmsType, OrderSide, OrderType, TradingState},
    events::{
        OrderEventAny, OrderFilled,
        account::stubs::{cash_account_state_million_usd, margin_account_state},
    },
    identifiers::{
        AccountId, ClientId, ClientOrderId, OrderListId, PositionId, StrategyId,
        TradeId, TraderId, VenueOrderId,
        stubs::{client_id_binance, strategy_id_ema_cross, trader_id, uuid4},
    },
    instruments::{Instrument, InstrumentAny, stubs::audusd_sim},
    orders::{Order, OrderList, OrderTestBuilder},
    position::Position,
    types::{Currency, Money, Price, Quantity},
};
use nautilus_portfolio::Portfolio;
use nautilus_risk::{
    PreTradeRiskGate,
    engine::{RiskEngine, config::RiskEngineConfig},
};
use rstest::rstest;
use rust_decimal::Decimal;
use ustr::Ustr;

#[derive(Debug)]
struct ExactOncePreTradeGate {
    reserved_by_cid: Mutex<AHashMap<ClientOrderId, Decimal>>,
    check_calls: AtomicUsize,
    denied_callbacks: Mutex<Vec<ClientOrderId>>,
}

impl ExactOncePreTradeGate {
    fn new() -> Self {
        Self {
            reserved_by_cid: Mutex::new(AHashMap::new()),
            check_calls: AtomicUsize::new(0),
            denied_callbacks: Mutex::new(Vec::new()),
        }
    }

    fn denied_callbacks(&self) -> Vec<ClientOrderId> {
        self.denied_callbacks.lock().unwrap().clone()
    }

    fn denied_count_for(&self, cid: ClientOrderId) -> usize {
        self.denied_callbacks
            .lock()
            .unwrap()
            .iter()
            .filter(|c| **c == cid)
            .count()
    }

    fn reserved_total(&self) -> Decimal {
        self.reserved_by_cid.lock().unwrap().values().copied().sum()
    }
}

impl PreTradeRiskGate for ExactOncePreTradeGate {
    fn check_command(
        &self,
        command: &TradingCommand,
        cache: &Cache,
        _ts_now: UnixNanos,
    ) -> Result<(), String> {
        self.check_calls.fetch_add(1, Ordering::SeqCst);
        match command {
            TradingCommand::SubmitOrder(cmd) => {
                let qty = cache
                    .order(&cmd.client_order_id)
                    .map(|o| {
                        if o.is_reduce_only() {
                            Decimal::ZERO
                        } else {
                            o.quantity().as_decimal()
                        }
                    })
                    .unwrap_or_else(|| cmd.order_init.quantity.as_decimal());
                if qty > Decimal::ZERO {
                    self.reserved_by_cid
                        .lock()
                        .unwrap()
                        .insert(cmd.client_order_id, qty);
                }
            }
            TradingCommand::SubmitOrderList(cmd) => {
                for init in &cmd.order_inits {
                    let qty = cache
                        .order(&init.client_order_id)
                        .map(|o| {
                            if o.is_reduce_only() {
                                Decimal::ZERO
                            } else {
                                o.quantity().as_decimal()
                            }
                        })
                        .unwrap_or_else(|| init.quantity.as_decimal());
                    if qty > Decimal::ZERO {
                        self.reserved_by_cid
                            .lock()
                            .unwrap()
                            .insert(init.client_order_id, qty);
                    }
                }
            }
            _ => {}
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

fn build_risk_engine(cache: Cache) -> RiskEngine {
    let cache = Rc::new(RefCell::new(cache));
    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    let portfolio = Portfolio::new(
        Rc::clone(&clock) as Rc<RefCell<dyn Clock>>,
        Rc::clone(&cache),
        None,
    );
    let config = RiskEngineConfig {
        debug: true,
        bypass: false,
        max_order_submit: RateLimit::new(100, DurationNanos::new(1_000_000_000)),
        max_order_modify: RateLimit::new(100, DurationNanos::new(1_000_000_000)),
        max_notional_per_order: AHashMap::new(),
        full_position_exit_venues: AHashSet::new(),
    };
    RiskEngine::new(config, portfolio, clock, cache)
}

fn setup_audusd_cache(with_instrument: bool) -> (Cache, InstrumentAny) {
    let instrument = InstrumentAny::CurrencyPair(audusd_sim());
    let mut cache = Cache::default();
    if with_instrument {
        cache.add_instrument(instrument.clone()).unwrap();
    }
    cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd("1000000 USD", "0 USD", "1000000 USD"),
        )))
        .unwrap();
    cache.add_quote(quote_audusd()).unwrap();
    (cache, instrument)
}

#[rstest]
fn test_round5_d5_p1_1_submit_order_missing_from_cache_invokes_on_order_denied_exactly_once(
    trader_id: TraderId,
    client_id_binance: ClientId,
    strategy_id_ema_cross: StrategyId,
) {
    let _process_handler = register_process_handler();
    let execute_handler = register_execute_handler();
    let (cache, instrument) = setup_audusd_cache(true);

    let missing_order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-R5-MISSING-CACHE"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.00000"))
        .build();
    // Deliberately DO NOT add `missing_order` to `cache`!

    let mut risk_engine = build_risk_engine(cache);
    let gate = Arc::new(ExactOncePreTradeGate::new());
    risk_engine.set_pre_trade_gate(Some(gate.clone()));

    let submit = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument.id(),
        missing_order.client_order_id(),
        missing_order.init_event().clone(),
        None,
        None,
        None,
        uuid4(),
        UnixNanos::from(100),
        None,
    );
    risk_engine.execute(TradingCommand::SubmitOrder(submit));

    assert!(execute_handler.get_messages().is_empty());
    assert_eq!(
        gate.denied_count_for(missing_order.client_order_id()),
        1,
        "SubmitOrder missing from cache MUST invoke on_order_denied EXACTLY ONCE; callbacks={:?}",
        gate.denied_callbacks()
    );
    assert_eq!(gate.reserved_total(), Decimal::ZERO);
}

#[rstest]
fn test_round5_d5_p1_1_submit_order_list_incomplete_invokes_on_order_denied_exactly_once_per_cid(
    trader_id: TraderId,
    client_id_binance: ClientId,
    strategy_id_ema_cross: StrategyId,
) {
    let _process_handler = register_process_handler();
    let execute_handler = register_execute_handler();
    let (mut cache, instrument) = setup_audusd_cache(true);

    let leg1 = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-R5-INC-1"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.00000"))
        .build();
    let leg2 = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-R5-INC-2"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.05000"))
        .build();

    // Only leg1 is added to cache; leg2 is absent -> OrderListIncomplete!
    cache
        .add_order(leg1.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let mut risk_engine = build_risk_engine(cache);
    let gate = Arc::new(ExactOncePreTradeGate::new());
    risk_engine.set_pre_trade_gate(Some(gate.clone()));

    let order_list = OrderList::new(
        OrderListId::from("OL-R5-INCOMPLETE"),
        instrument.id(),
        strategy_id_ema_cross,
        vec![leg1.client_order_id(), leg2.client_order_id()],
        UnixNanos::from(200),
    );
    let submit_list = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        order_list,
        vec![leg1.init_event().clone(), leg2.init_event().clone()],
        None,
        None,
        None,
        uuid4(),
        UnixNanos::from(200),
        None,
    );
    risk_engine.execute(TradingCommand::SubmitOrderList(submit_list));

    assert!(execute_handler.get_messages().is_empty());
    assert_eq!(
        gate.denied_count_for(leg1.client_order_id()),
        1,
        "OrderListIncomplete must deny sibling leg1 EXACTLY ONCE; callbacks={:?}",
        gate.denied_callbacks()
    );
    assert_eq!(
        gate.denied_count_for(leg2.client_order_id()),
        1,
        "OrderListIncomplete must deny missing leg2 EXACTLY ONCE; callbacks={:?}",
        gate.denied_callbacks()
    );
    assert_eq!(gate.reserved_total(), Decimal::ZERO);
}

#[rstest]
fn test_round5_d5_p1_1_submit_order_list_instrument_not_found_invokes_on_order_denied_exactly_once_for_all_siblings(
    trader_id: TraderId,
    client_id_binance: ClientId,
    strategy_id_ema_cross: StrategyId,
) {
    let process_handler = register_process_handler();
    let execute_handler = register_execute_handler();
    let (mut cache, instrument) = setup_audusd_cache(false); // Instrument NOT in cache!

    let leg1 = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-R5-NOINST-1"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.00000"))
        .build();
    let leg2 = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-R5-NOINST-2"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.05000"))
        .build();
    let leg3 = OrderTestBuilder::new(OrderType::StopMarket)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-R5-NOINST-3"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("100"))
        .trigger_price(Price::from("0.95000"))
        .build();

    for o in [&leg1, &leg2, &leg3] {
        cache
            .add_order(o.clone(), None, Some(client_id_binance), false)
            .unwrap();
    }

    let mut risk_engine = build_risk_engine(cache);
    let gate = Arc::new(ExactOncePreTradeGate::new());
    risk_engine.set_pre_trade_gate(Some(gate.clone()));

    let order_list = OrderList::new(
        OrderListId::from("OL-R5-NOINST"),
        instrument.id(),
        strategy_id_ema_cross,
        vec![
            leg1.client_order_id(),
            leg2.client_order_id(),
            leg3.client_order_id(),
        ],
        UnixNanos::from(300),
    );
    let submit_list = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        order_list,
        vec![
            leg1.init_event().clone(),
            leg2.init_event().clone(),
            leg3.init_event().clone(),
        ],
        None,
        None,
        None,
        uuid4(),
        UnixNanos::from(300),
        None,
    );
    risk_engine.execute(TradingCommand::SubmitOrderList(submit_list));

    assert!(execute_handler.get_messages().is_empty());
    assert_eq!(process_handler.get_messages().len(), 3);
    for cid in [
        leg1.client_order_id(),
        leg2.client_order_id(),
        leg3.client_order_id(),
    ] {
        assert_eq!(
            gate.denied_count_for(cid),
            1,
            "InstrumentNotFound MUST invoke on_order_denied EXACTLY ONCE for sibling {cid}; callbacks={:?}",
            gate.denied_callbacks()
        );
    }
    assert_eq!(gate.denied_callbacks().len(), 3);
    assert_eq!(gate.reserved_total(), Decimal::ZERO);
}

#[rstest]
fn test_round5_d5_p1_1_halted_state_invokes_on_order_denied_exactly_once_for_submit_order_and_list(
    trader_id: TraderId,
    client_id_binance: ClientId,
    strategy_id_ema_cross: StrategyId,
) {
    let process_handler = register_process_handler();
    let execute_handler = register_execute_handler();
    let (mut cache, instrument) = setup_audusd_cache(true);

    let single = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-R5-HALT-SINGLE"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.00000"))
        .build();
    let l1 = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-R5-HALT-L1"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.00000"))
        .build();
    let l2 = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-R5-HALT-L2"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.05000"))
        .build();

    for o in [&single, &l1, &l2] {
        cache
            .add_order(o.clone(), None, Some(client_id_binance), false)
            .unwrap();
    }

    let mut risk_engine = build_risk_engine(cache);
    risk_engine.set_trading_state(TradingState::Halted);
    let gate = Arc::new(ExactOncePreTradeGate::new());
    risk_engine.set_pre_trade_gate(Some(gate.clone()));

    risk_engine.execute(TradingCommand::SubmitOrder(SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument.id(),
        single.client_order_id(),
        single.init_event().clone(),
        None,
        None,
        None,
        uuid4(),
        UnixNanos::from(400),
        None,
    )));

    let order_list = OrderList::new(
        OrderListId::from("OL-R5-HALT"),
        instrument.id(),
        strategy_id_ema_cross,
        vec![l1.client_order_id(), l2.client_order_id()],
        UnixNanos::from(401),
    );
    risk_engine.execute(TradingCommand::SubmitOrderList(SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        order_list,
        vec![l1.init_event().clone(), l2.init_event().clone()],
        None,
        None,
        None,
        uuid4(),
        UnixNanos::from(401),
        None,
    )));

    assert!(execute_handler.get_messages().is_empty());
    assert_eq!(process_handler.get_messages().len(), 3);
    for cid in [
        single.client_order_id(),
        l1.client_order_id(),
        l2.client_order_id(),
    ] {
        assert_eq!(
            gate.denied_count_for(cid),
            1,
            "TradingState::Halted MUST invoke on_order_denied EXACTLY ONCE for {cid}; callbacks={:?}",
            gate.denied_callbacks()
        );
    }
    assert_eq!(gate.denied_callbacks().len(), 3);
    assert_eq!(gate.reserved_total(), Decimal::ZERO);
}

#[rstest]
fn test_round5_d5_p1_1_reducing_state_invokes_on_order_denied_once_per_cid_and_allows_reduce_only_exit(
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

    let position_id = PositionId::from("P-R5-REDUCING");
    let open_fill = OrderFilled::new(
        trader_id,
        strategy_id_ema_cross,
        instrument.id(),
        ClientOrderId::from("O-R5-RED-OPEN"),
        VenueOrderId::from("V-R5-RED-OPEN"),
        AccountId::from("SIM-001"),
        TradeId::from("T-R5-RED-OPEN"),
        OrderSide::Buy,
        OrderType::Limit,
        Quantity::from("100"),
        Price::from("1.00000"),
        Currency::USD(),
        LiquiditySide::Taker,
        UUID4::new(),
        UnixNanos::from(500),
        UnixNanos::from(500),
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
        .client_order_id(ClientOrderId::from("O-R5-RED-ENTRY"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.00000"))
        .reduce_only(false)
        .build();
    let exit_order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-R5-RED-EXIT"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.00000"))
        .reduce_only(true)
        .build();
    let list_entry = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-R5-RED-LIST-1"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.00000"))
        .reduce_only(false)
        .build();
    let list_tp = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-R5-RED-LIST-2"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.05000"))
        .reduce_only(false)
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
    cache
        .add_order(list_entry.clone(), None, Some(client_id_binance), false)
        .unwrap();
    cache
        .add_order(list_tp.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let mut risk_engine = build_risk_engine(cache);
    risk_engine.set_trading_state(TradingState::Reducing);
    let gate = Arc::new(ExactOncePreTradeGate::new());
    risk_engine.set_pre_trade_gate(Some(gate.clone()));

    // 1. Non-reducing single entry -> denied ONCE
    risk_engine.execute(TradingCommand::SubmitOrder(SubmitOrder::new(
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
        UnixNanos::from(600),
        None,
    )));

    // 2. Reduce-only exit -> permitted (0 denied callbacks for exit_order!)
    risk_engine.execute(TradingCommand::SubmitOrder(SubmitOrder::new(
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
        UnixNanos::from(601),
        None,
    )));

    // 3. SubmitOrderList in Reducing state -> denied ONCE per CID
    let order_list = OrderList::new(
        OrderListId::from("OL-R5-REDUCING"),
        instrument.id(),
        strategy_id_ema_cross,
        vec![list_entry.client_order_id(), list_tp.client_order_id()],
        UnixNanos::from(602),
    );
    risk_engine.execute(TradingCommand::SubmitOrderList(SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        order_list,
        vec![
            list_entry.init_event().clone(),
            list_tp.init_event().clone(),
        ],
        None,
        None,
        None,
        uuid4(),
        UnixNanos::from(602),
        None,
    )));

    assert_eq!(execute_handler.get_messages().len(), 1);
    assert_eq!(
        gate.denied_count_for(entry_order.client_order_id()),
        1,
        "Reducing state single entry MUST invoke on_order_denied EXACTLY ONCE"
    );
    assert_eq!(
        gate.denied_count_for(exit_order.client_order_id()),
        0,
        "Reduce-only exit MUST pass without on_order_denied"
    );
    assert_eq!(
        gate.denied_count_for(list_entry.client_order_id()),
        1,
        "Reducing state SubmitOrderList leg1 MUST invoke on_order_denied EXACTLY ONCE"
    );
    assert_eq!(
        gate.denied_count_for(list_tp.client_order_id()),
        1,
        "Reducing state SubmitOrderList leg2 MUST invoke on_order_denied EXACTLY ONCE"
    );
    assert_eq!(gate.denied_callbacks().len(), 3);
    assert_eq!(process_handler.get_messages().len(), 3);
    assert_eq!(gate.reserved_total(), Decimal::ZERO);
}
