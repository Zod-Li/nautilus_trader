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

//! Blind-TDD integration tests for `RustPlanGateEngine`'s `PreTradeRiskGate` implementation.

use nautilus_common::{
    cache::Cache,
    messages::execution::{
        BatchModifyOrders, ModifyOrder, SubmitOrder, SubmitOrderList, TradingCommand,
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    data::QuoteTick,
    enums::{
        ContingencyType, LiquiditySide, OrderSide, OrderStatus, OrderType, TimeInForce,
        TrailingOffsetType, TriggerType,
    },
    events::{
        OrderAccepted, OrderCanceled, OrderEmulated, OrderEventAny, OrderExpired, OrderFilled,
        OrderInitialized, OrderPendingCancel, OrderPendingUpdate, OrderReleased, OrderSubmitted,
        OrderUpdated,
    },
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, PositionId, StrategyId,
        Symbol, TradeId, TraderId, VenueOrderId,
    },
    instruments::{Equity, Instrument, InstrumentAny},
    orders::{
        LimitIfTouchedOrder, LimitOrder, MarketIfTouchedOrder, MarketOrder, Order, OrderAny,
        OrderList, StopLimitOrder, StopMarketOrder, TrailingStopLimitOrder,
        TrailingStopMarketOrder,
    },
    position::Position,
    types::{Currency, Money, Price, Quantity},
};
use nautilus_quant_execution::{RustPlanGateEngine, RustPlanItemEnvelope};
use nautilus_risk::PreTradeRiskGate;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

const TS_NOW: u64 = 1_700_000_000_000_000_000;
const TS_FUTURE: i64 = 1_900_000_000_000_000_000;
const TS_PAST: i64 = 1_600_000_000_000_000_000;

fn make_equity(symbol: &str) -> InstrumentAny {
    let inst_id = InstrumentId::from(format!("{symbol}.ALPACA").as_str());
    let eq = Equity::builder()
        .instrument_id(inst_id)
        .raw_symbol(Symbol::from(symbol))
        .currency(Currency::USD())
        .price_precision(2)
        .price_increment(Price::from("0.01"))
        .lot_size(Quantity::from("1"))
        .ts_event(UnixNanos::from(TS_NOW))
        .ts_init(UnixNanos::from(TS_NOW))
        .build()
        .unwrap();
    eq.into_any()
}

fn make_quote(inst_id: InstrumentId, bid: &str, ask: &str) -> QuoteTick {
    QuoteTick::new(
        inst_id,
        Price::from(bid),
        Price::from(ask),
        Quantity::from("1000"),
        Quantity::from("1000"),
        UnixNanos::from(TS_NOW),
        UnixNanos::from(TS_NOW),
    )
}

fn make_plan_item(
    symbol: &str,
    side: &str,
    approved_qty_cap: Decimal,
    initial_stop_price: Decimal,
    valid_until_ns: i64,
) -> RustPlanItemEnvelope {
    RustPlanItemEnvelope {
        item_id: format!("item-{symbol}"),
        plan_id: "plan-001".to_string(),
        symbol: symbol.to_string(),
        side: side.to_string(),
        status: "ARMED".to_string(),
        target_qty: approved_qty_cap,
        approved_qty_cap,
        target_notional_usd: approved_qty_cap * dec!(150),
        filled_qty: dec!(0),
        valid_until_ns,
        valid_until_iso: "2030-01-01T00:00:00Z".to_string(),
        limit_collar_pct: dec!(0.50),
        limit_collar_atr: dec!(0.50),
        initial_stop_price,
        max_risk_budget_usd: dec!(500),
        daily_atr_14: Some(dec!(2.00)),
    }
}

fn make_limit_order(
    inst_id: InstrumentId,
    cid: &str,
    side: OrderSide,
    qty: &str,
    price: &str,
    reduce_only: bool,
) -> OrderAny {
    OrderAny::Limit(LimitOrder::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst_id,
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
        UnixNanos::from(TS_NOW),
    ))
}

fn make_market_order(
    inst_id: InstrumentId,
    cid: &str,
    side: OrderSide,
    qty: &str,
    reduce_only: bool,
) -> OrderAny {
    OrderAny::Market(MarketOrder::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst_id,
        ClientOrderId::from(cid),
        side,
        Quantity::from(qty),
        TimeInForce::Day,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
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
    ))
}

fn make_stop_market_order(
    inst_id: InstrumentId,
    cid: &str,
    side: OrderSide,
    qty: &str,
    trigger_price: &str,
    reduce_only: bool,
) -> OrderAny {
    OrderAny::StopMarket(StopMarketOrder::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst_id,
        ClientOrderId::from(cid),
        side,
        Quantity::from(qty),
        Price::from(trigger_price),
        TriggerType::LastPrice,
        TimeInForce::Gtc,
        None,
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
        UnixNanos::from(TS_NOW),
    ))
}

fn accept_order(mut order: OrderAny) -> OrderAny {
    let account_id = AccountId::from("ALPACA-001");
    let submitted = OrderSubmitted::new(
        order.trader_id(),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        account_id,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
        UnixNanos::from(TS_NOW),
    );
    order.apply(OrderEventAny::Submitted(submitted)).unwrap();
    let accepted = OrderAccepted::new(
        order.trader_id(),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        VenueOrderId::from(order.client_order_id().as_str()),
        account_id,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
        UnixNanos::from(TS_NOW),
        false,
    );
    order.apply(OrderEventAny::Accepted(accepted)).unwrap();
    order
}

fn submit_cmd(order: &OrderAny) -> TradingCommand {
    TradingCommand::SubmitOrder(SubmitOrder::new(
        order.trader_id(),
        Some(ClientId::from("ALPACA")),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
        None,
    ))
}

fn modify_cmd(
    order: &OrderAny,
    qty: Option<&str>,
    price: Option<&str>,
    trigger_price: Option<&str>,
) -> ModifyOrder {
    ModifyOrder::new(
        order.trader_id(),
        Some(ClientId::from("ALPACA")),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        order.venue_order_id(),
        qty.map(Quantity::from),
        price.map(Price::from),
        trigger_price.map(Price::from),
        UUID4::new(),
        UnixNanos::from(TS_NOW),
        None,
        None,
    )
}

fn open_position_in_cache(
    cache: &mut Cache,
    instrument: &InstrumentAny,
    side: OrderSide,
    qty: &str,
    px: &str,
    pos_id: &str,
) {
    let fill = OrderFilled::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        instrument.id(),
        ClientOrderId::from(format!("OPEN-{pos_id}").as_str()),
        VenueOrderId::from(format!("V-{pos_id}").as_str()),
        AccountId::from("ALPACA-001"),
        TradeId::from(format!("T-{pos_id}").as_str()),
        side,
        OrderType::Limit,
        Quantity::from(qty),
        Price::from(px),
        Currency::USD(),
        LiquiditySide::Taker,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
        UnixNanos::from(TS_NOW),
        false,
        Some(PositionId::from(pos_id)),
        Some(Money::from("0.00 USD")),
        None,
    );
    let position = Position::new(instrument, fill);
    cache
        .add_position(&position, nautilus_model::enums::OmsType::Netting)
        .unwrap();
}

#[test]
fn test_absent_cache_order_skips_reservation_without_panic() {
    let inst = make_equity("AAPL");
    let mut cache = Cache::default();
    cache.add_instrument(inst.clone()).unwrap();
    cache
        .add_quote(make_quote(inst.id(), "149.90", "150.00"))
        .unwrap();

    let gate = RustPlanGateEngine::new(false, String::new());
    gate.upsert_plan_item(
        make_plan_item("AAPL", "LONG", dec!(100), dec!(145.00), TS_FUTURE),
        false,
    );

    // Order NOT added to Cache.
    let absent_order = make_limit_order(
        inst.id(),
        "O-ABSENT-1",
        OrderSide::Buy,
        "25",
        "150.10",
        false,
    );
    let res = gate.check_command(&submit_cmd(&absent_order), &cache, UnixNanos::from(TS_NOW));
    assert!(res.is_ok(), "absent cache order should not panic or reserve: {res:?}");
    assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
    assert!(!gate.has_admitted_reservation("O-ABSENT-1"));

    // ModifyOrder for absent cache order also must not panic or reserve.
    let absent_mod = ModifyOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("ALPACA")),
        StrategyId::from("STRAT-001"),
        inst.id(),
        ClientOrderId::from("O-ABSENT-MOD"),
        None,
        Some(Quantity::from("10")),
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
        None,
        None,
    );
    let mod_res = gate.check_command(
        &TradingCommand::ModifyOrder(absent_mod),
        &cache,
        UnixNanos::from(TS_NOW),
    );
    assert!(mod_res.is_ok());
    assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
}

#[test]
fn test_exit_bypass_reduce_only_admitted_under_freeze_unavailable_and_expired() {
    let inst_aapl = make_equity("AAPL");
    let inst_msft = make_equity("MSFT");
    let mut cache = Cache::default();
    cache.add_instrument(inst_aapl.clone()).unwrap();
    cache.add_instrument(inst_msft.clone()).unwrap();
    open_position_in_cache(
        &mut cache,
        &inst_aapl,
        OrderSide::Buy,
        "100",
        "150.00",
        "P-EXIT-AAPL",
    );

    let exit_aapl = make_market_order(
        inst_aapl.id(),
        "O-EXIT-AAPL",
        OrderSide::Sell,
        "50",
        true,
    );
    let stop_aapl = make_stop_market_order(
        inst_aapl.id(),
        "O-STOP-AAPL",
        OrderSide::Sell,
        "50",
        "145.00",
        true,
    );
    let exit_msft_no_plan = make_market_order(
        inst_msft.id(),
        "O-EXIT-MSFT",
        OrderSide::Sell,
        "20",
        true,
    );
    cache.add_order(exit_aapl.clone(), None, None, false).unwrap();
    cache.add_order(stop_aapl.clone(), None, None, false).unwrap();
    cache
        .add_order(exit_msft_no_plan.clone(), None, None, false)
        .unwrap();

    let gate = RustPlanGateEngine::new(true, "GLOBAL_FREEZE".to_string());
    gate.set_backend_unavailable(true, "PG_DOWN".to_string());
    gate.upsert_plan_item(
        make_plan_item("AAPL", "LONG", dec!(100), dec!(145.00), TS_PAST),
        false,
    );

    assert!(
        gate.check_command(&submit_cmd(&exit_aapl), &cache, UnixNanos::from(TS_NOW))
            .is_ok()
    );
    assert!(
        gate.check_command(&submit_cmd(&stop_aapl), &cache, UnixNanos::from(TS_NOW))
            .is_ok()
    );
    assert!(
        gate.check_command(
            &submit_cmd(&exit_msft_no_plan),
            &cache,
            UnixNanos::from(TS_NOW)
        )
        .is_ok()
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
    assert_eq!(gate.reserved_qty("MSFT"), dec!(0));
    assert!(!gate.has_admitted_reservation("O-EXIT-AAPL"));
    assert!(!gate.has_admitted_reservation("O-STOP-AAPL"));
}

#[test]
fn test_monotonic_stop_modification_long_and_short_under_entry_freeze() {
    let inst_aapl = make_equity("AAPL");
    let inst_nvda = make_equity("NVDA");
    let mut cache = Cache::default();
    cache.add_instrument(inst_aapl.clone()).unwrap();
    cache.add_instrument(inst_nvda.clone()).unwrap();

    // Open LONG position of 50 AAPL and SHORT position of 40 NVDA.
    open_position_in_cache(&mut cache, &inst_aapl, OrderSide::Buy, "50", "150.00", "P-AAPL");
    open_position_in_cache(&mut cache, &inst_nvda, OrderSide::Sell, "40", "125.00", "P-NVDA");

    let long_stop = accept_order(make_stop_market_order(
        inst_aapl.id(),
        "O-LONG-STOP",
        OrderSide::Sell,
        "50",
        "145.00",
        true,
    ));
    let short_stop = accept_order(make_stop_market_order(
        inst_nvda.id(),
        "O-SHORT-STOP",
        OrderSide::Buy,
        "40",
        "130.00",
        true,
    ));
    cache.add_order(long_stop.clone(), None, None, true).unwrap();
    cache
        .add_order(short_stop.clone(), None, None, true)
        .unwrap();

    let gate = RustPlanGateEngine::new(true, "FREEZE_ACTIVE".to_string());
    gate.upsert_plan_item(
        make_plan_item("AAPL", "LONG", dec!(50), dec!(145.00), TS_FUTURE),
        false,
    );
    gate.upsert_plan_item(
        make_plan_item("NVDA", "SHORT", dec!(40), dec!(130.00), TS_FUTURE),
        false,
    );

    // LONG: raising stop (145 -> 148) succeeds under entry_freeze.
    assert!(
        gate.check_command(
            &TradingCommand::ModifyOrder(modify_cmd(&long_stop, Some("50"), None, Some("148.00"))),
            &cache,
            UnixNanos::from(TS_NOW),
        )
        .is_ok()
    );
    // LONG: reducing qty (50 -> 30) while tightening stop succeeds.
    assert!(
        gate.check_command(
            &TradingCommand::ModifyOrder(modify_cmd(&long_stop, Some("30"), None, Some("149.00"))),
            &cache,
            UnixNanos::from(TS_NOW),
        )
        .is_ok()
    );
    // LONG: loosening stop (145 -> 142) DENIED.
    assert!(
        gate.check_command(
            &TradingCommand::ModifyOrder(modify_cmd(&long_stop, Some("50"), None, Some("142.00"))),
            &cache,
            UnixNanos::from(TS_NOW),
        )
        .is_err()
    );
    // LONG: exceeding open position qty (60 > 50) DENIED.
    assert!(
        gate.check_command(
            &TradingCommand::ModifyOrder(modify_cmd(&long_stop, Some("60"), None, Some("148.00"))),
            &cache,
            UnixNanos::from(TS_NOW),
        )
        .is_err()
    );

    // SHORT: lowering stop (130 -> 127) succeeds under entry_freeze.
    assert!(
        gate.check_command(
            &TradingCommand::ModifyOrder(modify_cmd(&short_stop, Some("40"), None, Some("127.00"))),
            &cache,
            UnixNanos::from(TS_NOW),
        )
        .is_ok()
    );
    // SHORT: raising stop (130 -> 133) DENIED.
    assert!(
        gate.check_command(
            &TradingCommand::ModifyOrder(modify_cmd(&short_stop, Some("40"), None, Some("133.00"))),
            &cache,
            UnixNanos::from(TS_NOW),
        )
        .is_err()
    );
    // SHORT: exceeding open position qty (50 > 40) DENIED.
    assert!(
        gate.check_command(
            &TradingCommand::ModifyOrder(modify_cmd(&short_stop, Some("50"), None, Some("127.00"))),
            &cache,
            UnixNanos::from(TS_NOW),
        )
        .is_err()
    );
}

#[test]
fn test_upward_entry_modify_and_non_reduce_only_batch_modify_denied() {
    let inst = make_equity("AAPL");
    let mut cache = Cache::default();
    cache.add_instrument(inst.clone()).unwrap();
    cache
        .add_quote(make_quote(inst.id(), "149.95", "150.00"))
        .unwrap();

    let entry1 = accept_order(make_limit_order(
        inst.id(),
        "O-ENTRY-1",
        OrderSide::Buy,
        "40",
        "150.00",
        false,
    ));
    let entry2 = accept_order(make_limit_order(
        inst.id(),
        "O-ENTRY-2",
        OrderSide::Buy,
        "20",
        "150.00",
        false,
    ));
    cache.add_order(entry1.clone(), None, None, true).unwrap();
    cache.add_order(entry2.clone(), None, None, true).unwrap();

    let gate = RustPlanGateEngine::new(false, String::new());
    gate.upsert_plan_item(
        make_plan_item("AAPL", "LONG", dec!(100), dec!(145.00), TS_FUTURE),
        false,
    );

    // Upward quantity modify (40 -> 60) must be denied with ENTRY_UPWARD_MODIFY_PROHIBITED.
    let err_upward = gate
        .check_command(
            &TradingCommand::ModifyOrder(modify_cmd(&entry1, Some("60"), None, None)),
            &cache,
            UnixNanos::from(TS_NOW),
        )
        .unwrap_err();
    assert!(
        err_upward.contains("ENTRY_UPWARD_MODIFY_PROHIBITED"),
        "expected ENTRY_UPWARD_MODIFY_PROHIBITED, got: {err_upward}"
    );

    // Non-reduce_only BatchModifyOrders must be denied with ENTRY_UPWARD_MODIFY_PROHIBITED.
    let batch = BatchModifyOrders::new(
        entry1.trader_id(),
        Some(ClientId::from("ALPACA")),
        entry1.strategy_id(),
        inst.id(),
        vec![
            modify_cmd(&entry1, Some("30"), Some("150.05"), None),
            modify_cmd(&entry2, Some("15"), Some("150.05"), None),
        ],
        UUID4::new(),
        UnixNanos::from(TS_NOW),
        None,
        None,
    );
    let err_batch = gate
        .check_command(
            &TradingCommand::ModifyOrders(batch),
            &cache,
            UnixNanos::from(TS_NOW),
        )
        .unwrap_err();
    assert!(
        err_batch.contains("ENTRY_UPWARD_MODIFY_PROHIBITED"),
        "expected ENTRY_UPWARD_MODIFY_PROHIBITED for batch modify, got: {err_batch}"
    );

    // Non-upward entry modify within collar (40 -> 30 at 150.10) is permitted.
    assert!(
        gate.check_command(
            &TradingCommand::ModifyOrder(modify_cmd(&entry1, Some("30"), Some("150.10"), None)),
            &cache,
            UnixNanos::from(TS_NOW),
        )
        .is_ok()
    );
}

#[test]
fn test_uncollared_market_entry_denied() {
    let inst = make_equity("AAPL");
    let mut cache = Cache::default();
    cache.add_instrument(inst.clone()).unwrap();
    cache
        .add_quote(make_quote(inst.id(), "149.95", "150.00"))
        .unwrap();

    let mkt_entry = make_market_order(inst.id(), "O-MKT-ENTRY", OrderSide::Buy, "25", false);
    cache.add_order(mkt_entry.clone(), None, None, false).unwrap();

    let gate = RustPlanGateEngine::new(false, String::new());
    gate.upsert_plan_item(
        make_plan_item("AAPL", "LONG", dec!(100), dec!(145.00), TS_FUTURE),
        false,
    );

    let err = gate
        .check_command(&submit_cmd(&mkt_entry), &cache, UnixNanos::from(TS_NOW))
        .unwrap_err();
    assert!(
        err.contains("UNCOLLARED_MARKET_ENTRY_DENIED"),
        "expected UNCOLLARED_MARKET_ENTRY_DENIED, got: {err}"
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
    assert!(!gate.has_admitted_reservation("O-MKT-ENTRY"));
}

#[test]
fn test_submit_order_list_atomic_two_phase_and_zero_leakage() {
    let inst = make_equity("AAPL");
    let mut cache = Cache::default();
    cache.add_instrument(inst.clone()).unwrap();
    cache
        .add_quote(make_quote(inst.id(), "149.95", "150.00"))
        .unwrap();

    let gate = RustPlanGateEngine::new(false, String::new());
    gate.upsert_plan_item(
        make_plan_item("AAPL", "LONG", dec!(100), dec!(145.00), TS_FUTURE),
        false,
    );

    // Case A: Leg 1 (60) fits cap 100, Leg 2 (50) pushes total to 110 > 100 -> atomic deny, 0 leaked.
    let leg1 = make_limit_order(inst.id(), "OL-A-1", OrderSide::Buy, "60", "150.10", false);
    let leg2 = make_limit_order(inst.id(), "OL-A-2", OrderSide::Buy, "50", "150.10", false);
    cache.add_order(leg1.clone(), None, None, false).unwrap();
    cache.add_order(leg2.clone(), None, None, false).unwrap();

    let list_a = OrderList::new(
        OrderListId::from("OL-A"),
        inst.id(),
        leg1.strategy_id(),
        vec![leg1.client_order_id(), leg2.client_order_id()],
        UnixNanos::from(TS_NOW),
    );
    let cmd_a = TradingCommand::SubmitOrderList(SubmitOrderList::new(
        leg1.trader_id(),
        Some(ClientId::from("ALPACA")),
        leg1.strategy_id(),
        list_a,
        vec![leg1.init_event().clone(), leg2.init_event().clone()],
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
        None,
    ));
    assert!(
        gate.check_command(&cmd_a, &cache, UnixNanos::from(TS_NOW))
            .is_err()
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
    assert!(!gate.has_admitted_reservation("OL-A-1"));
    assert!(!gate.has_admitted_reservation("OL-A-2"));

    // Case B: Leg 1 fits (60), Leg 2 is uncollared market entry -> atomic deny, 0 leaked.
    let leg1_b = make_limit_order(inst.id(), "OL-B-1", OrderSide::Buy, "60", "150.10", false);
    let leg2_b = make_market_order(inst.id(), "OL-B-2", OrderSide::Buy, "10", false);
    cache.add_order(leg1_b.clone(), None, None, false).unwrap();
    cache.add_order(leg2_b.clone(), None, None, false).unwrap();
    let list_b = OrderList::new(
        OrderListId::from("OL-B"),
        inst.id(),
        leg1_b.strategy_id(),
        vec![leg1_b.client_order_id(), leg2_b.client_order_id()],
        UnixNanos::from(TS_NOW),
    );
    let cmd_b = TradingCommand::SubmitOrderList(SubmitOrderList::new(
        leg1_b.trader_id(),
        Some(ClientId::from("ALPACA")),
        leg1_b.strategy_id(),
        list_b,
        vec![leg1_b.init_event().clone(), leg2_b.init_event().clone()],
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
        None,
    ));
    assert!(
        gate.check_command(&cmd_b, &cache, UnixNanos::from(TS_NOW))
            .is_err()
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
    assert!(!gate.has_admitted_reservation("OL-B-1"));

    // Case C: Leg 1 is valid entry (60), Leg 2 is reduce_only protective stop -> admitted with 60 reserved.
    let leg1_c = make_limit_order(inst.id(), "OL-C-1", OrderSide::Buy, "60", "150.10", false);
    let leg2_c = make_stop_market_order(
        inst.id(),
        "OL-C-2",
        OrderSide::Sell,
        "60",
        "145.00",
        true,
    );
    cache.add_order(leg1_c.clone(), None, None, false).unwrap();
    cache.add_order(leg2_c.clone(), None, None, false).unwrap();
    let list_c = OrderList::new(
        OrderListId::from("OL-C"),
        inst.id(),
        leg1_c.strategy_id(),
        vec![leg1_c.client_order_id(), leg2_c.client_order_id()],
        UnixNanos::from(TS_NOW),
    );
    let cmd_c = TradingCommand::SubmitOrderList(SubmitOrderList::new(
        leg1_c.trader_id(),
        Some(ClientId::from("ALPACA")),
        leg1_c.strategy_id(),
        list_c,
        vec![leg1_c.init_event().clone(), leg2_c.init_event().clone()],
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
        None,
    ));
    assert!(
        gate.check_command(&cmd_c, &cache, UnixNanos::from(TS_NOW))
            .is_ok()
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(60));
    assert!(gate.has_admitted_reservation("OL-C-1"));
    assert!(!gate.has_admitted_reservation("OL-C-2"));
}

#[test]
fn test_on_order_denied_and_release_order_reservation_idempotency() {
    let inst = make_equity("AAPL");
    let mut cache = Cache::default();
    cache.add_instrument(inst.clone()).unwrap();
    cache
        .add_quote(make_quote(inst.id(), "149.95", "150.00"))
        .unwrap();

    let order1 = make_limit_order(inst.id(), "O-REL-1", OrderSide::Buy, "40", "150.10", false);
    let order2 = make_limit_order(inst.id(), "O-REL-2", OrderSide::Buy, "40", "150.10", false);
    cache.add_order(order1.clone(), None, None, false).unwrap();
    cache.add_order(order2.clone(), None, None, false).unwrap();

    let gate = RustPlanGateEngine::new(false, String::new());
    gate.upsert_plan_item(
        make_plan_item("AAPL", "LONG", dec!(100), dec!(145.00), TS_FUTURE),
        false,
    );

    assert!(
        gate.check_command(&submit_cmd(&order1), &cache, UnixNanos::from(TS_NOW))
            .is_ok()
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(40));
    assert!(gate.has_admitted_reservation("O-REL-1"));

    // Downstream RiskEngine denial rollback via PreTradeRiskGate::on_order_denied.
    gate.on_order_denied(&order1.client_order_id());
    assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
    assert!(!gate.has_admitted_reservation("O-REL-1"));

    // Second on_order_denied is idempotent.
    gate.on_order_denied(&order1.client_order_id());
    assert_eq!(gate.reserved_qty("AAPL"), dec!(0));

    // Admit order2, then release_order_reservation twice -> idempotent.
    assert!(
        gate.check_command(&submit_cmd(&order2), &cache, UnixNanos::from(TS_NOW))
            .is_ok()
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(40));
    assert!(gate.has_admitted_reservation("O-REL-2"));

    // Partial release 15 of 40 -> remaining 25, reservation remains active.
    let remaining_after_partial = gate.release_order_reservation("O-REL-2", "AAPL", dec!(15));
    assert_eq!(remaining_after_partial, dec!(25));
    assert_eq!(gate.reserved_qty("AAPL"), dec!(25));
    assert!(gate.has_admitted_reservation("O-REL-2"));

    // Release remaining 25 -> 0, reservation cleared; subsequent release is idempotent (0).
    let remaining_after_full = gate.release_order_reservation("O-REL-2", "AAPL", dec!(25));
    assert_eq!(remaining_after_full, dec!(0));
    assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
    assert!(!gate.has_admitted_reservation("O-REL-2"));

    let remaining_idempotent = gate.release_order_reservation("O-REL-2", "AAPL", dec!(40));
    assert_eq!(remaining_idempotent, dec!(0));
    assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
}

#[test]
fn test_downward_entry_modify_reconcile_and_zero_leakage() {
    let inst = make_equity("AAPL");
    let mut cache = Cache::default();
    cache.add_instrument(inst.clone()).unwrap();
    cache
        .add_quote(make_quote(inst.id(), "149.95", "150.00"))
        .unwrap();

    let order_a = accept_order(make_limit_order(
        inst.id(),
        "O-DOWN-A",
        OrderSide::Buy,
        "80",
        "150.10",
        false,
    ));
    cache.add_order(order_a.clone(), None, None, true).unwrap();

    let gate = RustPlanGateEngine::new(false, String::new());
    gate.upsert_plan_item(
        make_plan_item("AAPL", "LONG", dec!(100), dec!(145.00), TS_FUTURE),
        false,
    );

    // Admit initial 80-share entry.
    assert!(
        gate.check_command(&submit_cmd(&order_a), &cache, UnixNanos::from(TS_NOW))
            .is_ok()
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(80));
    assert!(gate.has_admitted_reservation("O-DOWN-A"));

    // Rejected downward modify (e.g. collar violation at 155.00 > 150.75) preserves reserved_qty == 80.
    assert!(
        gate.check_command(
            &TradingCommand::ModifyOrder(modify_cmd(&order_a, Some("30"), Some("155.00"), None)),
            &cache,
            UnixNanos::from(TS_NOW),
        )
        .is_err()
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(80));

    // Valid downward modify (80 -> 30 at 150.10) passes pre-trade check WITHOUT pre-releasing
    // before venue confirmation (`OrderUpdated`).
    assert!(
        gate.check_command(
            &TradingCommand::ModifyOrder(modify_cmd(&order_a, Some("30"), Some("150.10"), None)),
            &cache,
            UnixNanos::from(TS_NOW),
        )
        .is_ok()
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(80));

    // Confirmed OrderUpdated shrinks reservation via PreTradeRiskGate::on_order_updated to 30.
    let updated_a = OrderUpdated::new(
        order_a.trader_id(),
        order_a.strategy_id(),
        order_a.instrument_id(),
        order_a.client_order_id(),
        Quantity::from("30"),
        UUID4::new(),
        UnixNanos::from(TS_NOW + 1),
        UnixNanos::from(TS_NOW + 1),
        false,
        order_a.venue_order_id(),
        order_a.account_id(),
        Some(Price::from("150.10")),
        None,
        None,
        false,
    );
    cache
        .update_order(&OrderEventAny::Updated(updated_a))
        .unwrap();
    gate.on_order_updated(&order_a.client_order_id(), dec!(30));
    assert_eq!(gate.reserved_qty("AAPL"), dec!(30));
    assert!(gate.has_admitted_reservation("O-DOWN-A"));

    // Branch (a): Cancel + release_order_reservation("30") drops reserved_qty to 0 and clears reservation,
    // allowing subsequent 80-share entry.
    let canceled_a = OrderCanceled::new(
        order_a.trader_id(),
        order_a.strategy_id(),
        order_a.instrument_id(),
        order_a.client_order_id(),
        UUID4::new(),
        UnixNanos::from(TS_NOW + 2),
        UnixNanos::from(TS_NOW + 2),
        false,
        order_a.venue_order_id(),
        order_a.account_id(),
        None,
    );
    cache
        .update_order(&OrderEventAny::Canceled(canceled_a))
        .unwrap();
    assert_eq!(
        gate.release_order_reservation("O-DOWN-A", "AAPL", dec!(30)),
        dec!(0)
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
    assert!(!gate.has_admitted_reservation("O-DOWN-A"));

    let order_b = accept_order(make_limit_order(
        inst.id(),
        "O-DOWN-B",
        OrderSide::Buy,
        "80",
        "150.10",
        false,
    ));
    cache.add_order(order_b.clone(), None, None, true).unwrap();
    let res_b = gate.check_command(&submit_cmd(&order_b), &cache, UnixNanos::from(TS_NOW));
    assert!(res_b.is_ok(), "order_b submit failed: {res_b:?}");
    assert_eq!(gate.reserved_qty("AAPL"), dec!(80));

    // Branch (b): Reconcile downward modify 80 -> 30 via reconcile_order_updated -> reserved_qty == 30,
    // then fill 30 shares -> subsequent 70-share entry (30 + 70 = 100 <= cap) is ADMITTED.
    let updated_b = OrderUpdated::new(
        order_b.trader_id(),
        order_b.strategy_id(),
        order_b.instrument_id(),
        order_b.client_order_id(),
        Quantity::from("30"),
        UUID4::new(),
        UnixNanos::from(TS_NOW + 3),
        UnixNanos::from(TS_NOW + 3),
        false,
        order_b.venue_order_id(),
        order_b.account_id(),
        Some(Price::from("150.10")),
        None,
        None,
        false,
    );
    cache
        .update_order(&OrderEventAny::Updated(updated_b))
        .unwrap();
    let updated_reserved = gate.reconcile_order_updated("O-DOWN-B", "AAPL", dec!(30));
    assert_eq!(updated_reserved, dec!(30));
    assert_eq!(gate.reserved_qty("AAPL"), dec!(30));
    // Idempotent repeat call & unknown CID must not corrupt reservation.
    assert_eq!(
        gate.reconcile_order_updated("O-DOWN-B", "AAPL", dec!(30)),
        dec!(30)
    );
    assert_eq!(
        gate.reconcile_order_updated("O-UNKNOWN", "AAPL", dec!(10)),
        dec!(30)
    );

    let order_c = make_limit_order(
        inst.id(),
        "O-DOWN-C",
        OrderSide::Buy,
        "70",
        "150.10",
        false,
    );
    cache.add_order(order_c.clone(), None, None, false).unwrap();
    assert!(
        gate.check_command(&submit_cmd(&order_c), &cache, UnixNanos::from(TS_NOW))
            .is_ok()
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(100));
}

#[test]
fn test_prefill_oto_contingent_reduce_only_modify_after_prior_closed_position() {
    let inst = make_equity("AAPL");
    let mut cache = Cache::default();
    cache.add_instrument(inst.clone()).unwrap();
    cache
        .add_quote(make_quote(inst.id(), "149.95", "150.00"))
        .unwrap();

    // Open and then close a prior 50-share position on AAPL so positions_closed is non-empty
    // while open_position_qty == 0.
    open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "50", "140.00", "P-CLOSED-1");
    let close_fill = OrderFilled::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst.id(),
        ClientOrderId::from("CLOSE-P-CLOSED-1"),
        VenueOrderId::from("V-CLOSE-P-CLOSED-1"),
        AccountId::from("ALPACA-001"),
        TradeId::from("T-CLOSE-P-CLOSED-1"),
        OrderSide::Sell,
        OrderType::Limit,
        Quantity::from("50"),
        Price::from("145.00"),
        Currency::USD(),
        LiquiditySide::Taker,
        UUID4::new(),
        UnixNanos::from(TS_NOW + 1),
        UnixNanos::from(TS_NOW + 1),
        false,
        Some(PositionId::from("P-CLOSED-1")),
        Some(Money::from("0.00 USD")),
        None,
    );
    cache
        .update_position_from_fill(PositionId::from("P-CLOSED-1"), &close_fill)
        .unwrap();
    assert_eq!(
        cache
            .positions_closed(None, Some(&inst.id()), None, None, None)
            .len(),
        1
    );
    assert!(
        cache
            .positions_open(None, Some(&inst.id()), None, None, None)
            .is_empty()
    );

    // Create a new unfilled OTO bracket: resting BUY 30 entry + contingent reduce_only SL & TP.
    let cid_entry = ClientOrderId::from("O-OTO-ENT");
    let cid_sl = ClientOrderId::from("O-OTO-SL");
    let cid_tp = ClientOrderId::from("O-OTO-TP");
    let list_id = OrderListId::from("OL-OTO-1");

    let entry = accept_order(OrderAny::Limit(LimitOrder::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst.id(),
        cid_entry,
        OrderSide::Buy,
        Quantity::from("30"),
        Price::from("148.00"),
        TimeInForce::Day,
        None,
        false,
        false,
        false,
        None,
        None,
        None,
        Some(ContingencyType::Oto),
        Some(list_id),
        Some(vec![cid_sl, cid_tp]),
        None,
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
    )));
    let sl = accept_order(OrderAny::StopMarket(StopMarketOrder::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst.id(),
        cid_sl,
        OrderSide::Sell,
        Quantity::from("30"),
        Price::from("145.00"),
        TriggerType::LastPrice,
        TimeInForce::Gtc,
        None,
        true,
        false,
        None,
        None,
        None,
        Some(ContingencyType::Ouo),
        Some(list_id),
        Some(vec![cid_tp]),
        Some(cid_entry),
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
    )));
    let tp = accept_order(OrderAny::Limit(LimitOrder::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst.id(),
        cid_tp,
        OrderSide::Sell,
        Quantity::from("30"),
        Price::from("155.00"),
        TimeInForce::Gtc,
        None,
        false,
        true,
        false,
        None,
        None,
        None,
        Some(ContingencyType::Ouo),
        Some(list_id),
        Some(vec![cid_sl]),
        Some(cid_entry),
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
    )));

    cache.add_order(entry.clone(), None, None, true).unwrap();
    cache.add_order(sl.clone(), None, None, true).unwrap();
    cache.add_order(tp.clone(), None, None, true).unwrap();

    let gate = RustPlanGateEngine::new(false, String::new());
    gate.upsert_plan_item(
        make_plan_item("AAPL", "LONG", dec!(100), dec!(145.00), TS_FUTURE),
        false,
    );

    let order_list = OrderList::new(
        list_id,
        inst.id(),
        entry.strategy_id(),
        vec![cid_entry, cid_sl, cid_tp],
        UnixNanos::from(TS_NOW),
    );
    let submit_list = TradingCommand::SubmitOrderList(SubmitOrderList::new(
        entry.trader_id(),
        Some(ClientId::from("ALPACA")),
        entry.strategy_id(),
        order_list,
        vec![
            entry.init_event().clone(),
            sl.init_event().clone(),
            tp.init_event().clone(),
        ],
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
        None,
    ));
    assert!(
        gate.check_command(&submit_list, &cache, UnixNanos::from(TS_NOW))
            .is_ok()
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(30));

    // Downward modify of contingent reduce_only SL (30 -> 25) while OTO entry is still unfilled
    // and prior closed position exists MUST SUCCEED.
    assert!(
        gate.check_command(
            &TradingCommand::ModifyOrder(modify_cmd(&sl, Some("25"), None, Some("145.50"))),
            &cache,
            UnixNanos::from(TS_NOW),
        )
        .is_ok(),
        "downward modify of pre-fill OTO reduce_only SL (30 -> 25) must succeed"
    );

    // Upward modify of contingent reduce_only SL (to 35 > resting 30, even though closed pos was 50)
    // MUST BE DENIED.
    assert!(
        gate.check_command(
            &TradingCommand::ModifyOrder(modify_cmd(&sl, Some("35"), None, Some("145.50"))),
            &cache,
            UnixNanos::from(TS_NOW),
        )
        .is_err(),
        "upward modify of pre-fill OTO reduce_only SL (to 35) must be denied"
    );
    assert!(
        gate.check_command(
            &TradingCommand::ModifyOrder(modify_cmd(&tp, Some("35"), Some("155.00"), None)),
            &cache,
            UnixNanos::from(TS_NOW),
        )
        .is_err(),
        "upward modify of pre-fill OTO reduce_only TP (to 35) must be denied"
    );
}

#[test]
fn test_compute_worst_limit_price_reg_nms_rule_612_boundary_crossovers() {
    let make_collar_item = |side: &str, collar_pct: Decimal| RustPlanItemEnvelope {
        item_id: "item-rule612".to_string(),
        plan_id: "plan-rule612".to_string(),
        symbol: "PENNY".to_string(),
        side: side.to_string(),
        status: "ARMED".to_string(),
        target_qty: dec!(100),
        approved_qty_cap: dec!(100),
        target_notional_usd: dec!(100),
        filled_qty: dec!(0),
        valid_until_ns: TS_FUTURE,
        valid_until_iso: "2030-01-01T00:00:00Z".to_string(),
        limit_collar_pct: collar_pct,
        limit_collar_atr: dec!(0),
        initial_stop_price: dec!(0.50),
        max_risk_budget_usd: dec!(50),
        daily_atr_14: None,
    };

    // 1. LONG 0.9950 + 2.0% => 1.0149 >= $1.00 => 1.01 (2 dp, NOT 1.0149)
    let p1 = make_collar_item("LONG", dec!(2.0)).compute_worst_limit_price(dec!(0.9950), None);
    assert_eq!(p1, dec!(1.01));
    assert_eq!(p1.scale(), 2);

    // 2. LONG 0.9999 + 0.5% => 1.0048995 >= $1.00 => 1.00 (2 dp, NOT 1.0049)
    let p2 = make_collar_item("LONG", dec!(0.5)).compute_worst_limit_price(dec!(0.9999), None);
    assert_eq!(p2, dec!(1.00));
    assert_eq!(p2.scale(), 2);

    // 3. LONG 0.9999 + 0.006% => crosses $1.00 => 1.00 (2 dp)
    let p3 = make_collar_item("LONG", dec!(0.006)).compute_worst_limit_price(dec!(0.9999), None);
    assert_eq!(p3, dec!(1.00));
    assert_eq!(p3.scale(), 2);

    // 4. SHORT 1.0000 - 1.5% => 0.9850 < $1.00 => 0.9850 (4 dp)
    let p4 = make_collar_item("SHORT", dec!(1.5)).compute_worst_limit_price(dec!(1.0000), None);
    assert_eq!(p4, dec!(0.9850));
    assert_eq!(p4.scale(), 4);

    // 5. LONG 0.9999 + 0.0% => 0.9999 < $1.00 => 0.9999 (4 dp)
    let p5 = make_collar_item("LONG", dec!(0.0)).compute_worst_limit_price(dec!(0.9999), None);
    assert_eq!(p5, dec!(0.9999));
    assert_eq!(p5.scale(), 4);
}

fn make_mixed_order_list_cmd(list_id: &str, orders: &[OrderAny]) -> TradingCommand {
    let first = &orders[0];
    let order_list = OrderList::new(
        OrderListId::from(list_id),
        first.instrument_id(),
        first.strategy_id(),
        orders.iter().map(Order::client_order_id).collect(),
        UnixNanos::from(TS_NOW),
    );
    TradingCommand::SubmitOrderList(SubmitOrderList::new(
        first.trader_id(),
        Some(ClientId::from("ALPACA")),
        first.strategy_id(),
        order_list,
        orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
        None,
    ))
}

fn make_plan_item_with_notional(
    symbol: &str,
    side: &str,
    approved_qty_cap: Decimal,
    ref_price: Decimal,
    initial_stop_price: Decimal,
    daily_atr_14: Decimal,
) -> RustPlanItemEnvelope {
    RustPlanItemEnvelope {
        item_id: format!("item-{symbol}"),
        plan_id: "plan-mixed-001".to_string(),
        symbol: symbol.to_string(),
        side: side.to_string(),
        status: "ARMED".to_string(),
        target_qty: approved_qty_cap,
        approved_qty_cap,
        target_notional_usd: approved_qty_cap * ref_price,
        filled_qty: dec!(0),
        valid_until_ns: TS_FUTURE,
        valid_until_iso: "2030-01-01T00:00:00Z".to_string(),
        limit_collar_pct: dec!(0.50),
        limit_collar_atr: dec!(0.50),
        initial_stop_price,
        max_risk_budget_usd: dec!(1000),
        daily_atr_14: Some(daily_atr_14),
    }
}

#[test]
fn test_mixed_instrument_submit_order_list_stop_scoping_both_orderings() {
    let inst_msft = make_equity("MSFT");
    let inst_aapl = make_equity("AAPL");

    let run_case = |list_id: &str, msft_first: bool, interleaved: bool| {
        let mut cache = Cache::default();
        cache.add_instrument(inst_msft.clone()).unwrap();
        cache.add_instrument(inst_aapl.clone()).unwrap();
        cache
            .add_quote(make_quote(inst_msft.id(), "299.90", "299.95"))
            .unwrap();
        cache
            .add_quote(make_quote(inst_aapl.id(), "149.90", "149.95"))
            .unwrap();

        let msft_entry =
            make_limit_order(inst_msft.id(), "OL-MSFT-ENT", OrderSide::Buy, "20", "300.00", false);
        let msft_stop = make_stop_market_order(
            inst_msft.id(),
            "OL-MSFT-SL",
            OrderSide::Sell,
            "20",
            "290.00",
            true,
        );
        let aapl_entry =
            make_limit_order(inst_aapl.id(), "OL-AAPL-ENT", OrderSide::Buy, "30", "150.00", false);
        let aapl_stop = make_stop_market_order(
            inst_aapl.id(),
            "OL-AAPL-SL",
            OrderSide::Sell,
            "30",
            "145.00",
            true,
        );

        let ordered = if interleaved {
            vec![
                msft_entry.clone(),
                aapl_entry.clone(),
                msft_stop.clone(),
                aapl_stop.clone(),
            ]
        } else if msft_first {
            vec![
                msft_entry.clone(),
                msft_stop.clone(),
                aapl_entry.clone(),
                aapl_stop.clone(),
            ]
        } else {
            vec![
                aapl_entry.clone(),
                aapl_stop.clone(),
                msft_entry.clone(),
                msft_stop.clone(),
            ]
        };

        for o in &ordered {
            cache.add_order(o.clone(), None, None, false).unwrap();
        }

        let gate = RustPlanGateEngine::new(false, String::new());
        gate.upsert_plan_item(
            make_plan_item_with_notional(
                "MSFT",
                "LONG",
                dec!(50),
                dec!(300),
                dec!(290.00),
                dec!(4.00),
            ),
            false,
        );
        gate.upsert_plan_item(
            make_plan_item_with_notional(
                "AAPL",
                "LONG",
                dec!(50),
                dec!(150),
                dec!(145.00),
                dec!(2.00),
            ),
            false,
        );

        let cmd = make_mixed_order_list_cmd(list_id, &ordered);
        let res = gate.check_command(&cmd, &cache, UnixNanos::from(TS_NOW));
        assert!(
            res.is_ok(),
            "mixed-instrument OrderList ({list_id}) should be admitted without cross-symbol stop leakage: {res:?}"
        );
        assert_eq!(gate.reserved_qty("MSFT"), dec!(20));
        assert_eq!(gate.reserved_qty("AAPL"), dec!(30));
        assert!(gate.has_admitted_reservation("OL-MSFT-ENT"));
        assert!(gate.has_admitted_reservation("OL-AAPL-ENT"));
        assert!(!gate.has_admitted_reservation("OL-MSFT-SL"));
        assert!(!gate.has_admitted_reservation("OL-AAPL-SL"));
    };

    run_case("OL-MIXED-MSFT-FIRST", true, false);
    run_case("OL-MIXED-AAPL-FIRST", false, false);
    run_case("OL-MIXED-INTERLEAVED", true, true);
}

#[test]
fn test_mixed_instrument_submit_order_list_one_with_stop_one_without_stop() {
    let inst_msft = make_equity("MSFT");
    let inst_aapl = make_equity("AAPL");

    let run_case = |list_id: &str, aapl_side: &str, msft_first: bool| {
        let mut cache = Cache::default();
        cache.add_instrument(inst_msft.clone()).unwrap();
        cache.add_instrument(inst_aapl.clone()).unwrap();
        cache
            .add_quote(make_quote(inst_msft.id(), "299.90", "299.95"))
            .unwrap();
        cache
            .add_quote(make_quote(inst_aapl.id(), "149.95", "150.05"))
            .unwrap();

        let msft_entry =
            make_limit_order(inst_msft.id(), "OL-NS-MSFT-ENT", OrderSide::Buy, "20", "300.00", false);
        let msft_stop = make_stop_market_order(
            inst_msft.id(),
            "OL-NS-MSFT-SL",
            OrderSide::Sell,
            "20",
            "290.00",
            true,
        );
        let aapl_order_side = if aapl_side == "LONG" {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        let aapl_entry = make_limit_order(
            inst_aapl.id(),
            "OL-NS-AAPL-ENT",
            aapl_order_side,
            "30",
            "150.00",
            false,
        );

        let ordered = if msft_first {
            vec![msft_entry.clone(), msft_stop.clone(), aapl_entry.clone()]
        } else {
            vec![aapl_entry.clone(), msft_entry.clone(), msft_stop.clone()]
        };
        for o in &ordered {
            cache.add_order(o.clone(), None, None, false).unwrap();
        }

        let gate = RustPlanGateEngine::new(false, String::new());
        gate.upsert_plan_item(
            make_plan_item_with_notional(
                "MSFT",
                "LONG",
                dec!(50),
                dec!(300),
                dec!(290.00),
                dec!(4.00),
            ),
            false,
        );
        // For SHORT AAPL, initial_stop_price=155.00: if MSFT's 290.00 stop leaked onto AAPL,
        // 290.00 > 155.00 would falsely violate AAPL's SHORT stop ceiling!
        let aapl_floor = if aapl_side == "LONG" {
            dec!(145.00)
        } else {
            dec!(155.00)
        };
        gate.upsert_plan_item(
            make_plan_item_with_notional(
                "AAPL",
                aapl_side,
                dec!(50),
                dec!(150),
                aapl_floor,
                dec!(2.00),
            ),
            false,
        );

        let cmd = make_mixed_order_list_cmd(list_id, &ordered);
        let res = gate.check_command(&cmd, &cache, UnixNanos::from(TS_NOW));
        assert!(
            res.is_ok(),
            "MSFT stop must not leak onto stop-less AAPL entry ({list_id}): {res:?}"
        );
        assert_eq!(gate.reserved_qty("MSFT"), dec!(20));
        assert_eq!(gate.reserved_qty("AAPL"), dec!(30));
        assert!(gate.has_admitted_reservation("OL-NS-MSFT-ENT"));
        assert!(gate.has_admitted_reservation("OL-NS-AAPL-ENT"));
        assert!(!gate.has_admitted_reservation("OL-NS-MSFT-SL"));
    };

    run_case("OL-NOSTOP-LONG-MSFT-FIRST", "LONG", true);
    run_case("OL-NOSTOP-LONG-AAPL-FIRST", "LONG", false);
    run_case("OL-NOSTOP-SHORT-MSFT-FIRST", "SHORT", true);
    run_case("OL-NOSTOP-SHORT-AAPL-FIRST", "SHORT", false);
}

#[test]
fn test_mixed_instrument_submit_order_list_stop_floor_violation_atomic_zero_leakage() {
    let inst_msft = make_equity("MSFT");
    let inst_aapl = make_equity("AAPL");

    let run_negative = |list_id: &str, msft_stop_px: &str, aapl_stop_px: &str| {
        let mut cache = Cache::default();
        cache.add_instrument(inst_msft.clone()).unwrap();
        cache.add_instrument(inst_aapl.clone()).unwrap();
        cache
            .add_quote(make_quote(inst_msft.id(), "299.90", "299.95"))
            .unwrap();
        cache
            .add_quote(make_quote(inst_aapl.id(), "149.90", "149.95"))
            .unwrap();

        let msft_entry =
            make_limit_order(inst_msft.id(), "OL-NEG-MSFT-ENT", OrderSide::Buy, "20", "300.00", false);
        let msft_stop = make_stop_market_order(
            inst_msft.id(),
            "OL-NEG-MSFT-SL",
            OrderSide::Sell,
            "20",
            msft_stop_px,
            true,
        );
        let aapl_entry =
            make_limit_order(inst_aapl.id(), "OL-NEG-AAPL-ENT", OrderSide::Buy, "30", "150.00", false);
        let aapl_stop = make_stop_market_order(
            inst_aapl.id(),
            "OL-NEG-AAPL-SL",
            OrderSide::Sell,
            "30",
            aapl_stop_px,
            true,
        );

        let orders = vec![
            msft_entry.clone(),
            msft_stop.clone(),
            aapl_entry.clone(),
            aapl_stop.clone(),
        ];
        for o in &orders {
            cache.add_order(o.clone(), None, None, false).unwrap();
        }

        let gate = RustPlanGateEngine::new(false, String::new());
        gate.upsert_plan_item(
            make_plan_item_with_notional(
                "MSFT",
                "LONG",
                dec!(50),
                dec!(300),
                dec!(290.00),
                dec!(4.00),
            ),
            false,
        );
        gate.upsert_plan_item(
            make_plan_item_with_notional(
                "AAPL",
                "LONG",
                dec!(50),
                dec!(150),
                dec!(145.00),
                dec!(2.00),
            ),
            false,
        );

        let cmd = make_mixed_order_list_cmd(list_id, &orders);
        let res = gate.check_command(&cmd, &cache, UnixNanos::from(TS_NOW));
        assert!(
            res.is_err(),
            "mixed-instrument OrderList ({list_id}) with stop violation must fail closed: {res:?}"
        );
        assert_eq!(gate.reserved_qty("MSFT"), dec!(0));
        assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
        assert!(!gate.has_admitted_reservation("OL-NEG-MSFT-ENT"));
        assert!(!gate.has_admitted_reservation("OL-NEG-AAPL-ENT"));
        assert!(!gate.has_admitted_reservation("OL-NEG-MSFT-SL"));
        assert!(!gate.has_admitted_reservation("OL-NEG-AAPL-SL"));
    };

    // Case A: MSFT valid (290 >= 290), AAPL violates own floor (140 < 145).
    run_negative("OL-NEG-AAPL-BAD", "290.00", "140.00");
    // Case B: MSFT violates own floor (280 < 290, even though 280 >= AAPL's 145), AAPL valid (145 >= 145).
    run_negative("OL-NEG-MSFT-BAD", "280.00", "145.00");
}

#[allow(clippy::too_many_arguments)]
fn make_contingent_limit_order(
    inst_id: InstrumentId,
    cid: &str,
    side: OrderSide,
    qty: &str,
    price: &str,
    reduce_only: bool,
    contingency: Option<ContingencyType>,
    list_id: Option<&str>,
    linked: Option<Vec<&str>>,
    parent: Option<&str>,
) -> OrderAny {
    OrderAny::Limit(LimitOrder::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst_id,
        ClientOrderId::from(cid),
        side,
        Quantity::from(qty),
        Price::from(price),
        TimeInForce::Gtc,
        None,
        false,
        reduce_only,
        false,
        None,
        None,
        None,
        contingency,
        list_id.map(OrderListId::from),
        linked.map(|ids| ids.into_iter().map(ClientOrderId::from).collect()),
        parent.map(ClientOrderId::from),
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
    ))
}

#[allow(clippy::too_many_arguments)]
fn make_contingent_stop_market_order(
    inst_id: InstrumentId,
    cid: &str,
    side: OrderSide,
    qty: &str,
    trigger_price: &str,
    reduce_only: bool,
    contingency: Option<ContingencyType>,
    list_id: Option<&str>,
    linked: Option<Vec<&str>>,
    parent: Option<&str>,
) -> OrderAny {
    OrderAny::StopMarket(StopMarketOrder::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst_id,
        ClientOrderId::from(cid),
        side,
        Quantity::from(qty),
        Price::from(trigger_price),
        TriggerType::LastPrice,
        TimeInForce::Gtc,
        None,
        reduce_only,
        false,
        None,
        None,
        None,
        contingency,
        list_id.map(OrderListId::from),
        linked.map(|ids| ids.into_iter().map(ClientOrderId::from).collect()),
        parent.map(ClientOrderId::from),
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
    ))
}

#[allow(clippy::too_many_arguments)]
fn make_limit_if_touched_order(
    inst_id: InstrumentId,
    cid: &str,
    side: OrderSide,
    qty: &str,
    price: &str,
    trigger_price: &str,
    reduce_only: bool,
    contingency: Option<ContingencyType>,
    list_id: Option<&str>,
    linked: Option<Vec<&str>>,
    parent: Option<&str>,
) -> OrderAny {
    OrderAny::LimitIfTouched(LimitIfTouchedOrder::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst_id,
        ClientOrderId::from(cid),
        side,
        Quantity::from(qty),
        Price::from(price),
        Price::from(trigger_price),
        TriggerType::LastPrice,
        TimeInForce::Gtc,
        None,
        false,
        reduce_only,
        false,
        None,
        None,
        None,
        contingency,
        list_id.map(OrderListId::from),
        linked.map(|ids| ids.into_iter().map(ClientOrderId::from).collect()),
        parent.map(ClientOrderId::from),
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
    ))
}

#[allow(clippy::too_many_arguments)]
fn make_market_if_touched_order(
    inst_id: InstrumentId,
    cid: &str,
    side: OrderSide,
    qty: &str,
    trigger_price: &str,
    reduce_only: bool,
    contingency: Option<ContingencyType>,
    list_id: Option<&str>,
    linked: Option<Vec<&str>>,
    parent: Option<&str>,
) -> OrderAny {
    OrderAny::MarketIfTouched(MarketIfTouchedOrder::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst_id,
        ClientOrderId::from(cid),
        side,
        Quantity::from(qty),
        Price::from(trigger_price),
        TriggerType::LastPrice,
        TimeInForce::Gtc,
        None,
        reduce_only,
        false,
        None,
        None,
        contingency,
        list_id.map(OrderListId::from),
        linked.map(|ids| ids.into_iter().map(ClientOrderId::from).collect()),
        parent.map(ClientOrderId::from),
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
    ))
}

#[allow(clippy::too_many_arguments)]
fn make_trailing_stop_tp_order(
    inst_id: InstrumentId,
    cid: &str,
    side: OrderSide,
    qty: &str,
    activation_price: Option<&str>,
    trigger_price: &str,
    trailing_offset: Decimal,
    reduce_only: bool,
    contingency: Option<ContingencyType>,
    list_id: Option<&str>,
    linked: Option<Vec<&str>>,
    parent: Option<&str>,
    tag: Option<&str>,
    intent_param: Option<&str>,
) -> OrderAny {
    let base = OrderAny::TrailingStopMarket(TrailingStopMarketOrder::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst_id,
        ClientOrderId::from(cid),
        side,
        Quantity::from(qty),
        activation_price.map(Price::from),
        Price::from(trigger_price),
        TriggerType::LastPrice,
        trailing_offset,
        TrailingOffsetType::Price,
        TimeInForce::Gtc,
        None,
        reduce_only,
        false,
        None,
        None,
        None,
        contingency,
        list_id.map(OrderListId::from),
        linked.map(|ids| ids.into_iter().map(ClientOrderId::from).collect()),
        parent.map(ClientOrderId::from),
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
    ));
    let mut init_val = serde_json::to_value(base.init_event()).unwrap();
    if let Some(t) = tag {
        init_val["tags"] = serde_json::json!([t]);
    }
    if let Some(intent) = intent_param {
        init_val["exec_algorithm_params"] = serde_json::json!({"intent": intent});
    }
    let patched_init: OrderInitialized = serde_json::from_value(init_val).unwrap();
    OrderAny::try_from(patched_init).unwrap()
}

fn setup_aapl_cache_and_gate(
    side: &str,
    cap: Decimal,
    initial_stop: Decimal,
) -> (InstrumentAny, Cache, RustPlanGateEngine) {
    let inst = make_equity("AAPL");
    let mut cache = Cache::default();
    cache.add_instrument(inst.clone()).unwrap();
    cache
        .add_quote(make_quote(inst.id(), "149.95", "150.00"))
        .unwrap();
    let gate = RustPlanGateEngine::new(false, String::new());
    gate.upsert_plan_item(
        make_plan_item("AAPL", side, cap, initial_stop, TS_FUTURE),
        false,
    );
    (inst, cache, gate)
}

#[test]
fn test_oto_take_profit_lit_and_mit_without_sl_admitted_long_and_short() {
    for (plan_side, entry_side, exit_side, initial_stop, tp_px) in [
        ("LONG", OrderSide::Buy, OrderSide::Sell, dec!(145.00), "160.00"),
        ("SHORT", OrderSide::Sell, OrderSide::Buy, dec!(155.00), "140.00"),
    ] {
        for tp_kind in ["LIT", "MIT"] {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
            let list_id = format!("OL-OTO-{plan_side}-{tp_kind}");
            let ent_cid = format!("O-ENT-{plan_side}-{tp_kind}");
            let tp_cid = format!("O-TP-{plan_side}-{tp_kind}");

            let entry = make_contingent_limit_order(
                inst.id(),
                &ent_cid,
                entry_side,
                "20",
                "150.00",
                false,
                Some(ContingencyType::Oto),
                Some(&list_id),
                Some(vec![&tp_cid]),
                None,
            );
            let tp = if tp_kind == "LIT" {
                make_limit_if_touched_order(
                    inst.id(),
                    &tp_cid,
                    exit_side,
                    "20",
                    tp_px,
                    tp_px,
                    true,
                    None,
                    Some(&list_id),
                    None,
                    Some(&ent_cid),
                )
            } else {
                make_market_if_touched_order(
                    inst.id(),
                    &tp_cid,
                    exit_side,
                    "20",
                    tp_px,
                    true,
                    None,
                    Some(&list_id),
                    None,
                    Some(&ent_cid),
                )
            };

            let orders = vec![entry, tp];
            for o in &orders {
                cache.add_order(o.clone(), None, None, false).unwrap();
            }

            let cmd = make_mixed_order_list_cmd(&list_id, &orders);
            let res = gate.check_command(&cmd, &cache, UnixNanos::from(TS_NOW));
            assert!(
                res.is_ok(),
                "{plan_side} OTO with {tp_kind} TP and no SL must be admitted (never INVALID_STOP_LOSS_PRICE): {res:?}"
            );
            assert_eq!(gate.reserved_qty("AAPL"), dec!(20));
            assert!(gate.has_admitted_reservation(&ent_cid));
            assert!(!gate.has_admitted_reservation(&tp_cid));
        }
    }
}

#[test]
fn test_lit_tp_with_unparented_or_unlinked_stop_loss_validated_for_long_and_short() {
    for (plan_side, entry_side, exit_side, initial_stop, tp_px, valid_sl, bad_sl) in [
        ("LONG", OrderSide::Buy, OrderSide::Sell, dec!(145.00), "160.00", "146.00", "140.00"),
        ("SHORT", OrderSide::Sell, OrderSide::Buy, dec!(155.00), "140.00", "154.00", "160.00"),
    ] {
        for shape in ["UNPARENTED_SL", "UNLINKED_PARENTED_SL"] {
            for exit_cont in [ContingencyType::Oco, ContingencyType::Ouo] {
                for (is_valid, sl_px) in [(true, valid_sl), (false, bad_sl)] {
                    let (inst, mut cache, gate) =
                        setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                    let tag = if is_valid { "VALID" } else { "BAD" };
                    let cont_tag = match exit_cont {
                        ContingencyType::Oco => "OCO",
                        ContingencyType::Ouo => "OUO",
                        _ => unreachable!(),
                    };
                    let list_id = format!("OL-{plan_side}-{shape}-{cont_tag}-{tag}");
                    let ent_cid = format!("O-ENT-{plan_side}-{shape}-{cont_tag}-{tag}");
                    let tp_cid = format!("O-TP-{plan_side}-{shape}-{cont_tag}-{tag}");
                    let sl_cid = format!("O-SL-{plan_side}-{shape}-{cont_tag}-{tag}");

                    // Notice entry ONLY links [tp_cid] in both shapes; tp and sl are mutually
                    // OCO/OUO-linked so unlinked exit capacity collapses to 20 <= 20 while
                    // keeping sl omitted from entry.linked_order_ids (and unparented in UNPARENTED_SL).
                    let entry = make_contingent_limit_order(
                        inst.id(),
                        &ent_cid,
                        entry_side,
                        "20",
                        "150.00",
                        false,
                        Some(ContingencyType::Oto),
                        Some(&list_id),
                        Some(vec![&tp_cid]),
                        None,
                    );
                    let tp = make_limit_if_touched_order(
                        inst.id(),
                        &tp_cid,
                        exit_side,
                        "20",
                        tp_px,
                        tp_px,
                        true,
                        Some(exit_cont),
                        Some(&list_id),
                        Some(vec![&sl_cid]),
                        Some(&ent_cid),
                    );
                    let sl_parent = if shape == "UNLINKED_PARENTED_SL" {
                        Some(ent_cid.as_str())
                    } else {
                        None
                    };
                    let sl = make_contingent_stop_market_order(
                        inst.id(),
                        &sl_cid,
                        exit_side,
                        "20",
                        sl_px,
                        true,
                        Some(exit_cont),
                        Some(&list_id),
                        Some(vec![&tp_cid]),
                        sl_parent,
                    );

                    let orders = vec![entry, tp, sl];
                    for o in &orders {
                        cache.add_order(o.clone(), None, None, false).unwrap();
                    }

                    let cmd = make_mixed_order_list_cmd(&list_id, &orders);
                    let res = gate.check_command(&cmd, &cache, UnixNanos::from(TS_NOW));
                    if is_valid {
                        assert!(
                            res.is_ok(),
                            "{plan_side} {shape} ({cont_tag}) with valid SL {sl_px} must be admitted: {res:?}"
                        );
                        assert_eq!(gate.reserved_qty("AAPL"), dec!(20));
                        assert!(gate.has_admitted_reservation(&ent_cid));
                    } else {
                        let err = res.unwrap_err();
                        assert!(
                            err.contains("STOP_LOSS_FLOOR_VIOLATION"),
                            "{plan_side} {shape} ({cont_tag}) with violating SL {sl_px} must fail with STOP_LOSS_FLOOR_VIOLATION, got: {err}"
                        );
                        assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
                        assert!(!gate.has_admitted_reservation(&ent_cid));
                    }
                }
            }
        }
    }
}

#[test]
fn test_trailing_stop_market_tp_ignored_before_sl_both_tag_and_param() {
    for tp_marker in ["TAG", "PARAM"] {
        for (plan_side, entry_side, exit_side, initial_stop, tp_trigger, valid_sl, bad_sl) in [
            ("LONG", OrderSide::Buy, OrderSide::Sell, dec!(145.00), "160.00", "146.00", "140.00"),
            ("SHORT", OrderSide::Sell, OrderSide::Buy, dec!(155.00), "140.00", "154.00", "160.00"),
            // Also verify a low-trigger TrailingStopMarket TP for LONG (140 < 145) is ignored when SL is valid (146 >= 145).
            ("LONG", OrderSide::Buy, OrderSide::Sell, dec!(145.00), "140.00", "146.00", "140.00"),
        ] {
            for (is_valid, sl_px) in [(true, valid_sl), (false, bad_sl)] {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                let list_id = format!("OL-TRAIL-{tp_marker}-{plan_side}-{tp_trigger}-{sl_px}");
                let ent_cid = format!("O-ENT-{list_id}");
                let tp_cid = format!("O-TP-{list_id}");
                let sl_cid = format!("O-SL-{list_id}");

                let entry = make_contingent_limit_order(
                    inst.id(),
                    &ent_cid,
                    entry_side,
                    "20",
                    "150.00",
                    false,
                    Some(ContingencyType::Oto),
                    Some(&list_id),
                    Some(vec![&tp_cid, &sl_cid]),
                    None,
                );
                let (tag_opt, param_opt) = if tp_marker == "TAG" {
                    (Some("TAKE_PROFIT"), None)
                } else {
                    (None, Some("TAKE_PROFIT"))
                };
                let tp = make_trailing_stop_tp_order(
                    inst.id(),
                    &tp_cid,
                    exit_side,
                    "20",
                    Some(tp_trigger),
                    tp_trigger,
                    dec!(1.00),
                    true,
                    Some(ContingencyType::Ouo),
                    Some(&list_id),
                    Some(vec![&sl_cid]),
                    Some(&ent_cid),
                    tag_opt,
                    param_opt,
                );
                let sl = make_contingent_stop_market_order(
                    inst.id(),
                    &sl_cid,
                    exit_side,
                    "20",
                    sl_px,
                    true,
                    Some(ContingencyType::Ouo),
                    Some(&list_id),
                    Some(vec![&tp_cid]),
                    Some(&ent_cid),
                );

                // Ordered with TrailingStopMarket TP BEFORE StopMarket SL.
                let orders = vec![entry, tp, sl];
                for o in &orders {
                    cache.add_order(o.clone(), None, None, false).unwrap();
                }

                let cmd = make_mixed_order_list_cmd(&list_id, &orders);
                let res = gate.check_command(&cmd, &cache, UnixNanos::from(TS_NOW));
                if is_valid {
                    assert!(
                        res.is_ok(),
                        "{list_id}: TrailingStopMarket TP must be ignored and valid SL {sl_px} admitted: {res:?}"
                    );
                    assert_eq!(gate.reserved_qty("AAPL"), dec!(20));
                    assert!(gate.has_admitted_reservation(&ent_cid));
                } else {
                    let err = res.unwrap_err();
                    assert!(
                        err.contains("STOP_LOSS_FLOOR_VIOLATION"),
                        "{list_id}: TrailingStopMarket TP must NOT shadow violating SL {sl_px}, got: {err}"
                    );
                    assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
                    assert!(!gate.has_admitted_reservation(&ent_cid));
                }
            }
        }
    }
}

#[test]
fn test_same_symbol_multi_entry_unowned_stop_floor_enforcement() {
    // Negative cases: unowned [e1, s1(valid), e2, s2(violating)] and permutations must fail closed atomically.
    for (case_name, plan_side, entry_side, exit_side, initial_stop, s1_px, s2_px, grouped_entries) in [
        ("LONG_E1_VALID_E2_BAD", "LONG", OrderSide::Buy, OrderSide::Sell, dec!(145.00), "146.00", "140.00", false),
        ("LONG_E1_BAD_E2_VALID", "LONG", OrderSide::Buy, OrderSide::Sell, dec!(145.00), "140.00", "146.00", false),
        ("LONG_GROUPED_ENTRIES", "LONG", OrderSide::Buy, OrderSide::Sell, dec!(145.00), "146.00", "140.00", true),
        ("SHORT_E1_VALID_E2_BAD", "SHORT", OrderSide::Sell, OrderSide::Buy, dec!(155.00), "154.00", "160.00", false),
    ] {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate(plan_side, dec!(100), initial_stop);
        open_position_in_cache(
            &mut cache,
            &inst,
            entry_side,
            "40",
            "150.00",
            &format!("P-ME-NEG-{case_name}"),
        );
        let e1 = make_limit_order(inst.id(), "O-ME-E1", entry_side, "20", "150.00", false);
        let s1 = make_stop_market_order(inst.id(), "O-ME-S1", exit_side, "20", s1_px, true);
        let e2 = make_limit_order(inst.id(), "O-ME-E2", entry_side, "20", "150.00", false);
        let s2 = make_stop_market_order(inst.id(), "O-ME-S2", exit_side, "20", s2_px, true);

        let orders = if grouped_entries {
            vec![e1, e2, s1, s2]
        } else {
            vec![e1, s1, e2, s2]
        };
        for o in &orders {
            cache.add_order(o.clone(), None, None, false).unwrap();
        }

        let cmd = make_mixed_order_list_cmd(&format!("OL-{case_name}"), &orders);
        let err = gate
            .check_command(&cmd, &cache, UnixNanos::from(TS_NOW))
            .unwrap_err();
        assert!(
            err.contains("STOP_LOSS_FLOOR_VIOLATION"),
            "{case_name}: expected STOP_LOSS_FLOOR_VIOLATION, got: {err}"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
        assert!(!gate.has_admitted_reservation("O-ME-E1"));
        assert!(!gate.has_admitted_reservation("O-ME-E2"));
    }

    // Positive Case 1: [e1, s1(valid), e2, s2(valid)] admitted cleanly with 40 reserved.
    {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(100), dec!(145.00));
        open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "40", "150.00", "P-ME-POS1");
        let e1 = make_limit_order(inst.id(), "O-OK-E1", OrderSide::Buy, "20", "150.00", false);
        let s1 = make_stop_market_order(inst.id(), "O-OK-S1", OrderSide::Sell, "20", "146.00", true);
        let e2 = make_limit_order(inst.id(), "O-OK-E2", OrderSide::Buy, "20", "150.00", false);
        let s2 = make_stop_market_order(inst.id(), "O-OK-S2", OrderSide::Sell, "20", "145.00", true);
        let orders = vec![e1, s1, e2, s2];
        for o in &orders {
            cache.add_order(o.clone(), None, None, false).unwrap();
        }
        let res = gate.check_command(
            &make_mixed_order_list_cmd("OL-ME-BOTH-VALID", &orders),
            &cache,
            UnixNanos::from(TS_NOW),
        );
        assert!(res.is_ok(), "both valid unowned pairs must be admitted: {res:?}");
        assert_eq!(gate.reserved_qty("AAPL"), dec!(40));
        assert!(gate.has_admitted_reservation("O-OK-E1"));
        assert!(gate.has_admitted_reservation("O-OK-E2"));
        assert!(!gate.has_admitted_reservation("O-OK-S1"));
        assert!(!gate.has_admitted_reservation("O-OK-S2"));
    }

    // Positive Case 2: [e1 (parented bracket with valid SL), e2 (unparented no-stop entry)] in both orderings.
    for bracket_first in [true, false] {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(100), dec!(145.00));
        let list_id = if bracket_first {
            "OL-BRK-FIRST"
        } else {
            "OL-UNP-FIRST"
        };
        let e1 = make_contingent_limit_order(
            inst.id(),
            "O-BRK-E1",
            OrderSide::Buy,
            "20",
            "150.00",
            false,
            Some(ContingencyType::Oto),
            Some(list_id),
            Some(vec!["O-BRK-S1", "O-BRK-TP1"]),
            None,
        );
        let s1 = make_contingent_stop_market_order(
            inst.id(),
            "O-BRK-S1",
            OrderSide::Sell,
            "20",
            "146.00",
            true,
            Some(ContingencyType::Ouo),
            Some(list_id),
            Some(vec!["O-BRK-TP1"]),
            Some("O-BRK-E1"),
        );
        let tp1 = make_contingent_limit_order(
            inst.id(),
            "O-BRK-TP1",
            OrderSide::Sell,
            "20",
            "160.00",
            true,
            Some(ContingencyType::Ouo),
            Some(list_id),
            Some(vec!["O-BRK-S1"]),
            Some("O-BRK-E1"),
        );
        let e2 = make_limit_order(inst.id(), "O-UNP-E2", OrderSide::Buy, "20", "150.00", false);

        let orders = if bracket_first {
            vec![e1, s1, tp1, e2]
        } else {
            vec![e2, e1, s1, tp1]
        };
        for o in &orders {
            cache.add_order(o.clone(), None, None, false).unwrap();
        }

        let res = gate.check_command(
            &make_mixed_order_list_cmd(list_id, &orders),
            &cache,
            UnixNanos::from(TS_NOW),
        );
        assert!(
            res.is_ok(),
            "{list_id}: parented bracket + unparented no-stop entry must be admitted: {res:?}"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(40));
        assert!(gate.has_admitted_reservation("O-BRK-E1"));
        assert!(gate.has_admitted_reservation("O-UNP-E2"));
        assert!(!gate.has_admitted_reservation("O-BRK-S1"));
        assert!(!gate.has_admitted_reservation("O-BRK-TP1"));
    }
}

#[test]
fn test_non_exit_opposite_side_stop_order_not_accepted_as_protective_sl() {
    // Subcase A: unowned opposite-side StopMarket with reduce_only=False (no exit tag/param)
    {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(100), dec!(145.00));
        let e1 = make_limit_order(inst.id(), "O-NE-E1", OrderSide::Buy, "20", "150.00", false);
        let s_non_exit =
            make_stop_market_order(inst.id(), "O-NE-S1", OrderSide::Sell, "20", "146.00", false);
        let orders = vec![e1, s_non_exit];
        for o in &orders {
            cache.add_order(o.clone(), None, None, false).unwrap();
        }
        let res = gate.check_command(
            &make_mixed_order_list_cmd("OL-NE-A", &orders),
            &cache,
            UnixNanos::from(TS_NOW),
        );
        assert!(
            res.is_err(),
            "opposite-side StopMarket with reduce_only=False must not be accepted: {res:?}"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
        assert!(!gate.has_admitted_reservation("O-NE-E1"));
        assert!(!gate.has_admitted_reservation("O-NE-S1"));
    }

    // Subcase B: parented opposite-side StopMarket with reduce_only=False (no exit tag/param)
    {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(100), dec!(145.00));
        let e1 = make_contingent_limit_order(
            inst.id(),
            "O-NE-B-E1",
            OrderSide::Buy,
            "20",
            "150.00",
            false,
            Some(ContingencyType::Oto),
            Some("OL-NE-B"),
            Some(vec!["O-NE-B-S1"]),
            None,
        );
        let s_non_exit = make_contingent_stop_market_order(
            inst.id(),
            "O-NE-B-S1",
            OrderSide::Sell,
            "20",
            "146.00",
            false,
            None,
            Some("OL-NE-B"),
            None,
            Some("O-NE-B-E1"),
        );
        let orders = vec![e1, s_non_exit];
        for o in &orders {
            cache.add_order(o.clone(), None, None, false).unwrap();
        }
        let res = gate.check_command(
            &make_mixed_order_list_cmd("OL-NE-B", &orders),
            &cache,
            UnixNanos::from(TS_NOW),
        );
        assert!(
            res.is_err(),
            "parented opposite-side StopMarket with reduce_only=False must not be accepted: {res:?}"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
        assert!(!gate.has_admitted_reservation("O-NE-B-E1"));
    }

    // Subcase C: non-exit StopMarket at 146.00 must NOT shadow a violating reduce_only SL at 140.00.
    {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(100), dec!(145.00));
        let e1 = make_limit_order(inst.id(), "O-NE-C-E1", OrderSide::Buy, "20", "150.00", false);
        let s_non_exit =
            make_stop_market_order(inst.id(), "O-NE-C-S1", OrderSide::Sell, "20", "146.00", false);
        let s_bad_sl =
            make_stop_market_order(inst.id(), "O-NE-C-SL", OrderSide::Sell, "20", "140.00", true);
        let orders = vec![e1, s_non_exit, s_bad_sl];
        for o in &orders {
            cache.add_order(o.clone(), None, None, false).unwrap();
        }
        let res = gate.check_command(
            &make_mixed_order_list_cmd("OL-NE-C", &orders),
            &cache,
            UnixNanos::from(TS_NOW),
        );
        assert!(
            res.is_err(),
            "non-exit StopMarket must not satisfy entry stop or shadow violating SL: {res:?}"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
        assert!(!gate.has_admitted_reservation("O-NE-C-E1"));
    }

    // Subcase D: standalone SubmitOrder of opposite-side StopMarket with reduce_only=False is denied.
    {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(100), dec!(145.00));
        let s_non_exit =
            make_stop_market_order(inst.id(), "O-NE-D-S1", OrderSide::Sell, "20", "146.00", false);
        cache
            .add_order(s_non_exit.clone(), None, None, false)
            .unwrap();
        let res = gate.check_command(&submit_cmd(&s_non_exit), &cache, UnixNanos::from(TS_NOW));
        assert!(
            res.is_err(),
            "standalone opposite-side StopMarket with reduce_only=False must be denied: {res:?}"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
        assert!(!gate.has_admitted_reservation("O-NE-D-S1"));
    }
}

#[allow(clippy::too_many_arguments)]
fn make_stop_limit_order(
    inst_id: InstrumentId,
    cid: &str,
    side: OrderSide,
    qty: &str,
    price: &str,
    trigger_price: &str,
    reduce_only: bool,
    tag: Option<&str>,
) -> OrderAny {
    let base = OrderAny::StopLimit(StopLimitOrder::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst_id,
        ClientOrderId::from(cid),
        side,
        Quantity::from(qty),
        Price::from(price),
        Price::from(trigger_price),
        TriggerType::LastPrice,
        TimeInForce::Gtc,
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
        UnixNanos::from(TS_NOW),
    ));
    if let Some(t) = tag {
        let mut init_val = serde_json::to_value(base.init_event()).unwrap();
        init_val["tags"] = serde_json::json!([t]);
        let patched_init: OrderInitialized = serde_json::from_value(init_val).unwrap();
        OrderAny::try_from(patched_init).unwrap()
    } else {
        base
    }
}

#[allow(clippy::too_many_arguments)]
fn make_trailing_stop_limit_order(
    inst_id: InstrumentId,
    cid: &str,
    side: OrderSide,
    qty: &str,
    activation_price: Option<&str>,
    price: &str,
    trigger_price: &str,
    limit_offset: Decimal,
    trailing_offset: Decimal,
    reduce_only: bool,
    tag: Option<&str>,
) -> OrderAny {
    let base = OrderAny::TrailingStopLimit(TrailingStopLimitOrder::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst_id,
        ClientOrderId::from(cid),
        side,
        Quantity::from(qty),
        activation_price.map(Price::from),
        Price::from(price),
        Price::from(trigger_price),
        TriggerType::LastPrice,
        limit_offset,
        trailing_offset,
        TrailingOffsetType::Price,
        TimeInForce::Gtc,
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
        UnixNanos::from(TS_NOW),
    ));
    if let Some(t) = tag {
        let mut init_val = serde_json::to_value(base.init_event()).unwrap();
        init_val["tags"] = serde_json::json!([t]);
        let patched_init: OrderInitialized = serde_json::from_value(init_val).unwrap();
        OrderAny::try_from(patched_init).unwrap()
    } else {
        base
    }
}

#[test]
fn test_cross_batch_parent_order_id_spoofing_denied_atomically() {
    for (plan_side, entry_side, exit_side, initial_stop, valid_sl_px, bad_sl_px) in [
        ("LONG", OrderSide::Buy, OrderSide::Sell, dec!(145.00), "146.00", "140.00"),
        ("SHORT", OrderSide::Sell, OrderSide::Buy, dec!(155.00), "154.00", "160.00"),
    ] {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate(plan_side, dec!(100), initial_stop);
        // Non-exhausted resting exit capacity (open_pos=40) + s1(10)/s_bad(10) so Batch 2
        // does not hit exit-capacity limits first and directly enforces STOP_LOSS_FLOOR_VIOLATION.
        open_position_in_cache(
            &mut cache,
            &inst,
            entry_side,
            "40",
            "150.00",
            &format!("P-CB-{plan_side}"),
        );

        // Batch 1: submit and accept initial OTO bracket [e1, s1 (parent=e1)].
        let list_1 = format!("OL-CB-1-{plan_side}");
        let e1_cid = format!("O-CB-E1-{plan_side}");
        let s1_cid = format!("O-CB-S1-{plan_side}");

        let e1 = make_contingent_limit_order(
            inst.id(),
            &e1_cid,
            entry_side,
            "20",
            "150.00",
            false,
            Some(ContingencyType::Oto),
            Some(&list_1),
            Some(vec![&s1_cid]),
            None,
        );
        let s1 = make_contingent_stop_market_order(
            inst.id(),
            &s1_cid,
            exit_side,
            "10",
            valid_sl_px,
            true,
            None,
            Some(&list_1),
            None,
            Some(&e1_cid),
        );
        cache.add_order(e1.clone(), None, None, false).unwrap();
        cache.add_order(s1.clone(), None, None, false).unwrap();

        let cmd_1 = make_mixed_order_list_cmd(&list_1, &[e1.clone(), s1.clone()]);
        assert!(
            gate.check_command(&cmd_1, &cache, UnixNanos::from(TS_NOW)).is_ok(),
            "{plan_side}: initial OTO bracket must be admitted"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(20));
        assert!(gate.has_admitted_reservation(&e1_cid));

        // Simulate e1 and s1 accepted into cache (so e1 exists in cache as accepted order).
        cache.update_order(&OrderEventAny::Submitted(OrderSubmitted::new(
            e1.trader_id(),
            e1.strategy_id(),
            e1.instrument_id(),
            e1.client_order_id(),
            AccountId::from("ALPACA-001"),
            UUID4::new(),
            UnixNanos::from(TS_NOW),
            UnixNanos::from(TS_NOW),
        ))).unwrap();
        cache.update_order(&OrderEventAny::Accepted(OrderAccepted::new(
            e1.trader_id(),
            e1.strategy_id(),
            e1.instrument_id(),
            e1.client_order_id(),
            VenueOrderId::from(e1_cid.as_str()),
            AccountId::from("ALPACA-001"),
            UUID4::new(),
            UnixNanos::from(TS_NOW),
            UnixNanos::from(TS_NOW),
            false,
        ))).unwrap();

        // Batch 2 (spoofing attack): [e2, s_bad (parent=e1, trigger violating plan floor, reduce_only=true)].
        let list_2 = format!("OL-CB-2-SPOOF-{plan_side}");
        let e2_cid = format!("O-CB-E2-{plan_side}");
        let s_bad_cid = format!("O-CB-SBAD-{plan_side}");
        let e2 = make_contingent_limit_order(
            inst.id(),
            &e2_cid,
            entry_side,
            "20",
            "150.00",
            false,
            Some(ContingencyType::Oto),
            Some(&list_2),
            Some(vec![&s_bad_cid]),
            None,
        );
        let s_bad = make_contingent_stop_market_order(
            inst.id(),
            &s_bad_cid,
            exit_side,
            "10",
            bad_sl_px,
            true,
            None,
            Some(&list_2),
            None,
            Some(&e1_cid),
        );
        cache.add_order(e2.clone(), None, None, false).unwrap();
        cache.add_order(s_bad.clone(), None, None, false).unwrap();

        let cmd_2 = make_mixed_order_list_cmd(&list_2, &[e2, s_bad]);
        let err_2 = gate
            .check_command(&cmd_2, &cache, UnixNanos::from(TS_NOW))
            .unwrap_err();
        assert!(
            err_2.contains("STOP_LOSS_FLOOR_VIOLATION"),
            "{plan_side}: cross-batch parent_order_id spoof must fail with STOP_LOSS_FLOOR_VIOLATION, got: {err_2}"
        );
        assert_eq!(
            gate.reserved_qty("AAPL"),
            dec!(20),
            "{plan_side}: reserved_qty must remain unchanged at e1's 20 shares"
        );
        assert!(gate.has_admitted_reservation(&e1_cid));
        assert!(!gate.has_admitted_reservation(&e2_cid));
        assert!(!gate.has_admitted_reservation(&s_bad_cid));

        // Batch 2b: even if Batch 2 also includes a valid s_good (parent=e2b), spoofed s_bad (parent=e1)
        // must still trigger atomic STOP_LOSS_FLOOR_VIOLATION.
        let list_2b = format!("OL-CB-2B-SPOOF-{plan_side}");
        let e2b_cid = format!("O-CB-E2B-{plan_side}");
        let s_good_cid = format!("O-CB-SGOOD-{plan_side}");
        let s_bad_2b_cid = format!("O-CB-SBAD2B-{plan_side}");
        let e2b = make_contingent_limit_order(
            inst.id(),
            &e2b_cid,
            entry_side,
            "20",
            "150.00",
            false,
            Some(ContingencyType::Oto),
            Some(&list_2b),
            Some(vec![&s_good_cid]),
            None,
        );
        let s_good = make_contingent_stop_market_order(
            inst.id(),
            &s_good_cid,
            exit_side,
            "20",
            valid_sl_px,
            true,
            None,
            Some(&list_2b),
            None,
            Some(&e2b_cid),
        );
        let s_bad_2b = make_contingent_stop_market_order(
            inst.id(),
            &s_bad_2b_cid,
            exit_side,
            "10",
            bad_sl_px,
            true,
            None,
            Some(&list_2b),
            None,
            Some(&e1_cid),
        );
        cache.add_order(e2b.clone(), None, None, false).unwrap();
        cache.add_order(s_good.clone(), None, None, false).unwrap();
        cache.add_order(s_bad_2b.clone(), None, None, false).unwrap();

        let cmd_2b = make_mixed_order_list_cmd(&list_2b, &[e2b, s_good, s_bad_2b]);
        let err_2b = gate
            .check_command(&cmd_2b, &cache, UnixNanos::from(TS_NOW))
            .unwrap_err();
        assert!(
            err_2b.contains("STOP_LOSS_FLOOR_VIOLATION"),
            "{plan_side}: spoofed s_bad alongside valid s_good must fail with STOP_LOSS_FLOOR_VIOLATION, got: {err_2b}"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(20));
        assert!(!gate.has_admitted_reservation(&e2b_cid));

        // Batch 3 (legitimate): [e3, s3 (parent=e3, valid_sl_px)] must be admitted cleanly (total 40).
        let list_3 = format!("OL-CB-3-OK-{plan_side}");
        let e3_cid = format!("O-CB-E3-{plan_side}");
        let s3_cid = format!("O-CB-S3-{plan_side}");
        let e3 = make_contingent_limit_order(
            inst.id(),
            &e3_cid,
            entry_side,
            "20",
            "150.00",
            false,
            Some(ContingencyType::Oto),
            Some(&list_3),
            Some(vec![&s3_cid]),
            None,
        );
        let s3 = make_contingent_stop_market_order(
            inst.id(),
            &s3_cid,
            exit_side,
            "20",
            valid_sl_px,
            true,
            None,
            Some(&list_3),
            None,
            Some(&e3_cid),
        );
        cache.add_order(e3.clone(), None, None, false).unwrap();
        cache.add_order(s3.clone(), None, None, false).unwrap();
        let cmd_3 = make_mixed_order_list_cmd(&list_3, &[e3, s3]);
        assert!(
            gate.check_command(&cmd_3, &cache, UnixNanos::from(TS_NOW)).is_ok(),
            "{plan_side}: legitimate second OTO batch must be admitted"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(40));
        assert!(gate.has_admitted_reservation(&e3_cid));
    }
}

#[test]
fn test_tp_trigger_tightening_modify_order_accepted_long_and_short() {
    for entry_freeze in [false, true] {
        for (plan_side, pos_side, exit_side, initial_stop, tp_init, tp_tighter, tp_limit_tighter) in [
            ("LONG", OrderSide::Buy, OrderSide::Sell, dec!(145.00), "165.00", "160.00", "159.90"),
            ("SHORT", OrderSide::Sell, OrderSide::Buy, dec!(155.00), "135.00", "140.00", "140.10"),
        ] {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate(plan_side, dec!(100), initial_stop);
            if entry_freeze {
                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            }
            open_position_in_cache(
                &mut cache,
                &inst,
                pos_side,
                "40",
                "150.00",
                &format!("P-TP-{plan_side}"),
            );

            // 1. LimitIfTouched TP tightening (LONG 165 -> 160, SHORT 135 -> 140)
            let lit_tp = accept_order(make_limit_if_touched_order(
                inst.id(),
                &format!("O-LIT-TP-{plan_side}-{entry_freeze}"),
                exit_side,
                "40",
                tp_init,
                tp_init,
                true,
                None,
                None,
                None,
                None,
            ));
            // 2. MarketIfTouched TP tightening (LONG 165 -> 160, SHORT 135 -> 140)
            let mit_tp = accept_order(make_market_if_touched_order(
                inst.id(),
                &format!("O-MIT-TP-{plan_side}-{entry_freeze}"),
                exit_side,
                "40",
                tp_init,
                true,
                None,
                None,
                None,
                None,
            ));
            // 3. TrailingStopLimit TP with tags=["TAKE_PROFIT"] tightening
            let tsl_init_limit = if plan_side == "LONG" { "164.90" } else { "135.10" };
            let tsl_tp = accept_order(make_trailing_stop_limit_order(
                inst.id(),
                &format!("O-TSL-TP-{plan_side}-{entry_freeze}"),
                exit_side,
                "40",
                Some(tp_init),
                tsl_init_limit,
                tp_init,
                dec!(0.10),
                dec!(1.00),
                true,
                Some("TAKE_PROFIT"),
            ));

            cache.add_order(lit_tp.clone(), None, None, true).unwrap();
            cache.add_order(mit_tp.clone(), None, None, true).unwrap();
            cache.add_order(tsl_tp.clone(), None, None, true).unwrap();

            let res_lit = gate.check_command(
                &TradingCommand::ModifyOrder(modify_cmd(
                    &lit_tp,
                    None,
                    Some(tp_tighter),
                    Some(tp_tighter),
                )),
                &cache,
                UnixNanos::from(TS_NOW),
            );
            assert!(
                res_lit.is_ok(),
                "{plan_side} (freeze={entry_freeze}) LimitIfTouched TP tightening {tp_init}->{tp_tighter} must be accepted: {res_lit:?}"
            );

            let res_mit = gate.check_command(
                &TradingCommand::ModifyOrder(modify_cmd(
                    &mit_tp,
                    None,
                    None,
                    Some(tp_tighter),
                )),
                &cache,
                UnixNanos::from(TS_NOW),
            );
            assert!(
                res_mit.is_ok(),
                "{plan_side} (freeze={entry_freeze}) MarketIfTouched TP tightening {tp_init}->{tp_tighter} must be accepted: {res_mit:?}"
            );

            let res_tsl = gate.check_command(
                &TradingCommand::ModifyOrder(modify_cmd(
                    &tsl_tp,
                    None,
                    Some(tp_limit_tighter),
                    Some(tp_tighter),
                )),
                &cache,
                UnixNanos::from(TS_NOW),
            );
            assert!(
                res_tsl.is_ok(),
                "{plan_side} (freeze={entry_freeze}) TrailingStopLimit(tags=[TAKE_PROFIT]) tightening {tp_init}->{tp_tighter} must be accepted: {res_tsl:?}"
            );
        }
    }
}

#[test]
fn test_genuine_and_exit_tagged_stop_loss_orders_enforce_monotonic_floor() {
    for (plan_side, pos_side, exit_side, initial_stop, tighter_stop, looser_stop) in [
        ("LONG", OrderSide::Buy, OrderSide::Sell, dec!(145.00), "147.00", "142.00"),
        ("SHORT", OrderSide::Sell, OrderSide::Buy, dec!(155.00), "153.00", "158.00"),
    ] {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate(plan_side, dec!(100), initial_stop);
        gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
        open_position_in_cache(
            &mut cache,
            &inst,
            pos_side,
            "40",
            "150.00",
            &format!("P-SL-{plan_side}"),
        );

        let sl_init = initial_stop.to_string();
        let sl_init_lim = if plan_side == "LONG" { "144.90" } else { "155.10" };
        let sl_tight_lim = if plan_side == "LONG" { "146.90" } else { "153.10" };
        let sl_loose_lim = if plan_side == "LONG" { "141.90" } else { "158.10" };

        let cases = vec![
            (
                "StopMarket",
                accept_order(make_stop_market_order(
                    inst.id(),
                    &format!("O-SL-SM-{plan_side}"),
                    exit_side,
                    "40",
                    &sl_init,
                    true,
                )),
                false,
            ),
            (
                "StopLimit",
                accept_order(make_stop_limit_order(
                    inst.id(),
                    &format!("O-SL-SL-{plan_side}"),
                    exit_side,
                    "40",
                    sl_init_lim,
                    &sl_init,
                    true,
                    None,
                )),
                true,
            ),
            (
                "TrailingStopLimit_no_tag",
                accept_order(make_trailing_stop_limit_order(
                    inst.id(),
                    &format!("O-SL-TSL-{plan_side}"),
                    exit_side,
                    "40",
                    None,
                    sl_init_lim,
                    &sl_init,
                    dec!(0.10),
                    dec!(1.00),
                    true,
                    None,
                )),
                true,
            ),
            (
                "TrailingStopLimit_STOP_LOSS_tag",
                accept_order(make_trailing_stop_limit_order(
                    inst.id(),
                    &format!("O-SL-TSL-TAG-{plan_side}"),
                    exit_side,
                    "40",
                    None,
                    sl_init_lim,
                    &sl_init,
                    dec!(0.10),
                    dec!(1.00),
                    true,
                    Some("STOP_LOSS"),
                )),
                true,
            ),
            (
                "StopLimit_STOP_LOSS_tag",
                accept_order(make_stop_limit_order(
                    inst.id(),
                    &format!("O-SL-SL-TAG-{plan_side}"),
                    exit_side,
                    "40",
                    sl_init_lim,
                    &sl_init,
                    true,
                    Some("STOP_LOSS"),
                )),
                true,
            ),
        ];

        for (label, order, has_limit_px) in cases {
            cache.add_order(order.clone(), None, None, true).unwrap();

            let tighten_res = gate.check_command(
                &TradingCommand::ModifyOrder(modify_cmd(
                    &order,
                    None,
                    if has_limit_px { Some(sl_tight_lim) } else { None },
                    Some(tighter_stop),
                )),
                &cache,
                UnixNanos::from(TS_NOW),
            );
            assert!(
                tighten_res.is_ok(),
                "{plan_side} {label}: tightening stop {sl_init}->{tighter_stop} must succeed: {tighten_res:?}"
            );

            let loosen_err = gate
                .check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(
                        &order,
                        None,
                        if has_limit_px { Some(sl_loose_lim) } else { None },
                        Some(looser_stop),
                    )),
                    &cache,
                    UnixNanos::from(TS_NOW),
                )
                .unwrap_err();
            assert!(
                loosen_err.contains("STOP_LOSS_LOOSENING_PROHIBITED"),
                "{plan_side} {label}: loosening stop {sl_init}->{looser_stop} must fail with STOP_LOSS_LOOSENING_PROHIBITED, got: {loosen_err}"
            );
        }
    }
}

#[test]
fn test_non_reduce_only_batch_modify_orders_still_denied_with_entry_upward_modify_prohibited() {
    let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(100), dec!(145.00));
    open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "20", "150.00", "P-BM-1");

    let entry = accept_order(make_limit_order(
        inst.id(),
        "O-BM-ENTRY",
        OrderSide::Buy,
        "20",
        "149.50",
        false,
    ));
    let stop = accept_order(make_stop_market_order(
        inst.id(),
        "O-BM-STOP",
        OrderSide::Sell,
        "20",
        "145.00",
        true,
    ));
    cache.add_order(entry.clone(), None, None, true).unwrap();
    cache.add_order(stop.clone(), None, None, true).unwrap();

    // Mixed BatchModifyOrders containing a reduce_only stop AND a non-reduce_only entry
    // must still be denied with ENTRY_UPWARD_MODIFY_PROHIBITED.
    let mixed_batch = BatchModifyOrders::new(
        entry.trader_id(),
        Some(ClientId::from("ALPACA")),
        entry.strategy_id(),
        inst.id(),
        vec![
            modify_cmd(&stop, None, None, Some("146.00")),
            modify_cmd(&entry, Some("15"), Some("149.60"), None),
        ],
        UUID4::new(),
        UnixNanos::from(TS_NOW),
        None,
        None,
    );
    let err = gate
        .check_command(
            &TradingCommand::ModifyOrders(mixed_batch),
            &cache,
            UnixNanos::from(TS_NOW),
        )
        .unwrap_err();
    assert!(
        err.contains("ENTRY_UPWARD_MODIFY_PROHIBITED"),
        "expected ENTRY_UPWARD_MODIFY_PROHIBITED for mixed non-reduce_only ModifyOrders, got: {err}"
    );
}

fn with_exit_marker(base: OrderAny, tag: Option<&str>, intent_param: Option<&str>) -> OrderAny {
    let mut init_val = serde_json::to_value(base.init_event()).unwrap();
    if let Some(t) = tag {
        init_val["tags"] = serde_json::json!([t]);
    }
    if let Some(intent) = intent_param {
        init_val["exec_algorithm_params"] = serde_json::json!({"intent": intent});
    }
    let patched_init: OrderInitialized = serde_json::from_value(init_val).unwrap();
    OrderAny::try_from(patched_init).unwrap()
}

fn make_trailing_stop_market_tp(
    inst_id: InstrumentId,
    cid: &str,
    side: OrderSide,
    qty: &str,
    activation_price: Option<&str>,
    trigger_price: &str,
    trailing_offset: Decimal,
    reduce_only: bool,
    tag: Option<&str>,
    intent_param: Option<&str>,
) -> OrderAny {
    make_trailing_stop_tp_order(
        inst_id,
        cid,
        side,
        qty,
        activation_price,
        trigger_price,
        trailing_offset,
        reduce_only,
        None,
        None,
        None,
        None,
        tag,
        intent_param,
    )
}

#[test]
fn test_spoofed_non_reduce_only_exit_tags_denied_on_flat_same_side_and_unplanned_submit() {
    let markers: [(&str, Option<&str>, Option<&str>); 3] = [
        ("TAG_TP", Some("TAKE_PROFIT"), None),
        ("TAG_EXIT", Some("EXIT"), None),
        ("PARAM_TP", None, Some("TAKE_PROFIT")),
    ];

    for (label, tag, intent_param) in markers {
        // 1. entry_freeze=True, flat position, LONG plan: BUY 50 MarketOrder reduce_only=False -> ENTRY_FREEZE_ACTIVE
        {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(100), dec!(145.00));
            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            let spoof_buy = with_exit_marker(
                make_market_order(inst.id(), &format!("O-SP-FLAT-BUY-{label}"), OrderSide::Buy, "50", false),
                tag,
                intent_param,
            );
            cache.add_order(spoof_buy.clone(), None, None, false).unwrap();
            let err = gate
                .check_command(&submit_cmd(&spoof_buy), &cache, UnixNanos::from(TS_NOW))
                .unwrap_err();
            assert!(
                err.contains("ENTRY_FREEZE_ACTIVE"),
                "{label} flat same-side BUY under freeze must fail with ENTRY_FREEZE_ACTIVE, got: {err}"
            );
            assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
            assert!(!gate.has_admitted_reservation(spoof_buy.client_order_id().as_str()));
        }

        // 2. entry_freeze=True, flat position, LONG plan: SELL 50 MarketOrder reduce_only=False -> ENTRY_FREEZE_ACTIVE
        {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(100), dec!(145.00));
            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            let spoof_sell_flat = with_exit_marker(
                make_market_order(inst.id(), &format!("O-SP-FLAT-SELL-{label}"), OrderSide::Sell, "50", false),
                tag,
                intent_param,
            );
            cache.add_order(spoof_sell_flat.clone(), None, None, false).unwrap();
            let err = gate
                .check_command(&submit_cmd(&spoof_sell_flat), &cache, UnixNanos::from(TS_NOW))
                .unwrap_err();
            assert!(
                err.contains("ENTRY_FREEZE_ACTIVE"),
                "{label} flat opposite-side SELL under freeze must fail with ENTRY_FREEZE_ACTIVE, got: {err}"
            );
            assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
        }

        // 3. entry_freeze=True, open LONG 10 position, LONG plan: same-side BUY 50 MarketOrder reduce_only=False -> ENTRY_FREEZE_ACTIVE
        {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(100), dec!(145.00));
            open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-SP-SAME");
            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            let spoof_same_side = with_exit_marker(
                make_market_order(inst.id(), &format!("O-SP-OPEN-BUY-{label}"), OrderSide::Buy, "50", false),
                tag,
                intent_param,
            );
            cache.add_order(spoof_same_side.clone(), None, None, false).unwrap();
            let err = gate
                .check_command(&submit_cmd(&spoof_same_side), &cache, UnixNanos::from(TS_NOW))
                .unwrap_err();
            assert!(
                err.contains("ENTRY_FREEZE_ACTIVE"),
                "{label} same-side BUY with open LONG under freeze must fail with ENTRY_FREEZE_ACTIVE, got: {err}"
            );
            assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
        }

        // 4. Unplanned symbol (MSFT has no locked plan item, flat position): BUY and SELL reduce_only=False -> NO_LOCKED_PLAN_ITEM
        for side in [OrderSide::Buy, OrderSide::Sell] {
            let (_inst_aapl, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(100), dec!(145.00));
            let inst_msft = make_equity("MSFT");
            cache.add_instrument(inst_msft.clone()).unwrap();
            cache
                .add_quote(make_quote(inst_msft.id(), "299.95", "300.00"))
                .unwrap();
            let unplanned = with_exit_marker(
                make_market_order(inst_msft.id(), &format!("O-SP-UNP-{label}-{side:?}"), side, "10", false),
                tag,
                intent_param,
            );
            cache.add_order(unplanned.clone(), None, None, false).unwrap();
            let err = gate
                .check_command(&submit_cmd(&unplanned), &cache, UnixNanos::from(TS_NOW))
                .unwrap_err();
            assert!(
                err.contains("NO_LOCKED_PLAN_ITEM"),
                "{label} unplanned {side:?} must fail with NO_LOCKED_PLAN_ITEM, got: {err}"
            );
            assert_eq!(gate.reserved_qty("MSFT"), dec!(0));
            assert!(!gate.has_admitted_reservation(unplanned.client_order_id().as_str()));
        }
    }
}

#[test]
fn test_spoofed_non_reduce_only_exit_tags_denied_atomically_in_submit_order_list() {
    let markers: [(&str, Option<&str>, Option<&str>); 3] = [
        ("TAG_TP", Some("TAKE_PROFIT"), None),
        ("TAG_EXIT", Some("EXIT"), None),
        ("PARAM_TP", None, Some("TAKE_PROFIT")),
    ];

    for (label, tag, intent_param) in markers {
        for spoof_side in [OrderSide::Buy, OrderSide::Sell] {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(10), dec!(145.00));
            let list_id = format!("OL-SP-{label}-{spoof_side:?}");
            let e_cid = format!("O-SP-E-{label}-{spoof_side:?}");
            let m_cid = format!("O-SP-M-{label}-{spoof_side:?}");

            let e1 = make_limit_order(inst.id(), &e_cid, OrderSide::Buy, "10", "150.00", false);
            let spoof_leg = with_exit_marker(
                make_market_order(inst.id(), &m_cid, spoof_side, "50", false),
                tag,
                intent_param,
            );
            let orders = vec![e1, spoof_leg];
            for o in &orders {
                cache.add_order(o.clone(), None, None, false).unwrap();
            }
            let cmd = make_mixed_order_list_cmd(&list_id, &orders);
            let res = gate.check_command(&cmd, &cache, UnixNanos::from(TS_NOW));
            assert!(
                res.is_err(),
                "{list_id}: [BUY 10, MarketOrder {spoof_side:?} 50 reduce_only=False] must be denied atomically: {res:?}"
            );
            assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
            assert!(!gate.has_admitted_reservation(&e_cid));
            assert!(!gate.has_admitted_reservation(&m_cid));
        }
    }
}

#[test]
fn test_spoofed_non_reduce_only_exit_tags_rejected_on_modify_order_under_freeze() {
    let markers: [(&str, Option<&str>, Option<&str>); 3] = [
        ("TAG_TP", Some("TAKE_PROFIT"), None),
        ("TAG_EXIT", Some("EXIT"), None),
        ("PARAM_TP", None, Some("TAKE_PROFIT")),
    ];

    for (label, tag, intent_param) in markers {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
        let entry = accept_order(with_exit_marker(
            make_limit_order(
                inst.id(),
                &format!("O-SP-MOD-{label}"),
                OrderSide::Buy,
                "10",
                "148.00",
                false,
            ),
            tag,
            intent_param,
        ));
        cache.add_order(entry.clone(), None, None, true).unwrap();
        gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());

        let err = gate
            .check_command(
                &TradingCommand::ModifyOrder(modify_cmd(&entry, None, Some("250.00"), None)),
                &cache,
                UnixNanos::from(TS_NOW),
            )
            .unwrap_err();
        assert!(
            err.contains("ENTRY_FREEZE_ACTIVE") || err.contains("COLLAR_LIMIT_VIOLATION"),
            "{label} ModifyOrder on reduce_only=False entry-side order must be rejected, got: {err}"
        );
    }
}

#[test]
fn test_two_unlinked_non_reduce_only_exit_orders_cannot_exceed_open_position() {
    // Subcase A: Sequential SubmitOrder (first SELL 6 admitted and resting in cache; second unlinked SELL 6 denied)
    for (label, tag, intent_param) in [
        ("TAG_TP", Some("TAKE_PROFIT"), None),
        ("TAG_EXIT", Some("EXIT"), None),
        ("PARAM_TP", None, Some("TAKE_PROFIT")),
    ] {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
        open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-UNL-1");
        gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());

        let sell1 = accept_order(with_exit_marker(
            make_limit_order(
                inst.id(),
                &format!("O-UNL-S1-{label}"),
                OrderSide::Sell,
                "6",
                "160.00",
                false,
            ),
            tag,
            intent_param,
        ));
        cache.add_order(sell1.clone(), None, None, true).unwrap();
        assert!(
            gate.check_command(&submit_cmd(&sell1), &cache, UnixNanos::from(TS_NOW))
                .is_ok(),
            "{label}: first SELL 6 against open LONG 10 must be admitted"
        );

        let sell2 = with_exit_marker(
            make_limit_order(
                inst.id(),
                &format!("O-UNL-S2-{label}"),
                OrderSide::Sell,
                "6",
                "161.00",
                false,
            ),
            tag,
            intent_param,
        );
        cache.add_order(sell2.clone(), None, None, false).unwrap();
        let res2 = gate.check_command(&submit_cmd(&sell2), &cache, UnixNanos::from(TS_NOW));
        assert!(
            res2.is_err(),
            "{label}: second unlinked SELL 6 (6+6=12 > 10 open) must be denied: {res2:?}"
        );
    }

    // Subcase B: Same-batch unlinked [SELL 6, SELL 6] SubmitOrderList against open LONG 10 -> denied atomically
    {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
        open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-UNL-2");
        gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());

        let s1 = with_exit_marker(
            make_limit_order(inst.id(), "O-UNL-B1", OrderSide::Sell, "6", "160.00", false),
            Some("TAKE_PROFIT"),
            None,
        );
        let s2 = with_exit_marker(
            make_limit_order(inst.id(), "O-UNL-B2", OrderSide::Sell, "6", "161.00", false),
            Some("TAKE_PROFIT"),
            None,
        );
        let orders = vec![s1, s2];
        for o in &orders {
            cache.add_order(o.clone(), None, None, false).unwrap();
        }
        let res = gate.check_command(
            &make_mixed_order_list_cmd("OL-UNL-2X6", &orders),
            &cache,
            UnixNanos::from(TS_NOW),
        );
        assert!(
            res.is_err(),
            "batch of two unlinked reduce_only=False SELL 6 against LONG 10 must be denied: {res:?}"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
    }
}

#[test]
fn test_genuine_non_reduce_only_exit_tagged_orders_and_3leg_bracket_admitted_and_upward_modify_blocked() {
    // Subcase A: Open LONG 10 under entry_freeze=True -> genuine SELL 10 StopMarket(tags=["STOP_LOSS"], reduce_only=False)
    // admitted, and upward ModifyOrder qty 10 -> 15 rejected with REDUCE_ONLY_MODIFY_EXCEEDS_POSITION.
    {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
        open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-GEN-SM");
        gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());

        let sl = accept_order(with_exit_marker(
            make_stop_market_order(inst.id(), "O-GEN-SL", OrderSide::Sell, "10", "146.00", false),
            Some("STOP_LOSS"),
            None,
        ));
        cache.add_order(sl.clone(), None, None, true).unwrap();
        assert!(
            gate.check_command(&submit_cmd(&sl), &cache, UnixNanos::from(TS_NOW))
                .is_ok(),
            "genuine open-backed StopMarket(tags=[STOP_LOSS], reduce_only=False) must be admitted"
        );

        let err_up = gate
            .check_command(
                &TradingCommand::ModifyOrder(modify_cmd(&sl, Some("15"), None, Some("146.50"))),
                &cache,
                UnixNanos::from(TS_NOW),
            )
            .unwrap_err();
        assert!(
            err_up.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
            "upward qty modify (10->15) on exit-tagged StopMarket must fail with REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, got: {err_up}"
        );
    }

    // Subcase B: Open LONG 10 under entry_freeze=True -> genuine SELL 10 Limit(tags=["TAKE_PROFIT"], reduce_only=False)
    // admitted, and upward ModifyOrder qty 10 -> 15 rejected with REDUCE_ONLY_MODIFY_EXCEEDS_POSITION.
    {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
        open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-GEN-TP");
        gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());

        let tp = accept_order(with_exit_marker(
            make_limit_order(inst.id(), "O-GEN-TP", OrderSide::Sell, "10", "160.00", false),
            Some("TAKE_PROFIT"),
            None,
        ));
        cache.add_order(tp.clone(), None, None, true).unwrap();
        assert!(
            gate.check_command(&submit_cmd(&tp), &cache, UnixNanos::from(TS_NOW))
                .is_ok(),
            "genuine open-backed Limit(tags=[TAKE_PROFIT], reduce_only=False) must be admitted"
        );

        let err_up = gate
            .check_command(
                &TradingCommand::ModifyOrder(modify_cmd(&tp, Some("15"), Some("159.00"), None)),
                &cache,
                UnixNanos::from(TS_NOW),
            )
            .unwrap_err();
        assert!(
            err_up.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
            "upward qty modify (10->15) on exit-tagged Limit TP must fail with REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, got: {err_up}"
        );
    }

    // Subcase C: Legitimate 3-leg OTO/OCO (OUO) bracket [entry, SL(reduce_only=False, tags=[STOP_LOSS]), TP(reduce_only=False, tags=[TAKE_PROFIT])]
    // admitted cleanly with 10 reserved, and pre-fill upward ModifyOrder qty (10 -> 15) on SL/TP rejected.
    {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
        let list_id = "OL-GEN-3LEG";
        let e_cid = "O-3L-ENT";
        let sl_cid = "O-3L-SL";
        let tp_cid = "O-3L-TP";

        let entry = accept_order(make_contingent_limit_order(
            inst.id(),
            e_cid,
            OrderSide::Buy,
            "10",
            "150.00",
            false,
            Some(ContingencyType::Oto),
            Some(list_id),
            Some(vec![sl_cid, tp_cid]),
            None,
        ));
        let sl = accept_order(with_exit_marker(
            make_contingent_stop_market_order(
                inst.id(),
                sl_cid,
                OrderSide::Sell,
                "10",
                "146.00",
                false,
                Some(ContingencyType::Ouo),
                Some(list_id),
                Some(vec![tp_cid]),
                Some(e_cid),
            ),
            Some("STOP_LOSS"),
            None,
        ));
        let tp = accept_order(with_exit_marker(
            make_contingent_limit_order(
                inst.id(),
                tp_cid,
                OrderSide::Sell,
                "10",
                "160.00",
                false,
                Some(ContingencyType::Ouo),
                Some(list_id),
                Some(vec![sl_cid]),
                Some(e_cid),
            ),
            Some("TAKE_PROFIT"),
            None,
        ));
        let orders = vec![entry, sl.clone(), tp.clone()];
        for o in &orders {
            cache.add_order(o.clone(), None, None, true).unwrap();
        }
        let res = gate.check_command(
            &make_mixed_order_list_cmd(list_id, &orders),
            &cache,
            UnixNanos::from(TS_NOW),
        );
        assert!(
            res.is_ok(),
            "legitimate 3-leg OTO/OCO bracket with reduce_only=False exit tags must be admitted: {res:?}"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(10));
        assert!(gate.has_admitted_reservation(e_cid));
        assert!(!gate.has_admitted_reservation(sl_cid));
        assert!(!gate.has_admitted_reservation(tp_cid));

        let err_sl_up = gate
            .check_command(
                &TradingCommand::ModifyOrder(modify_cmd(&sl, Some("15"), None, Some("146.50"))),
                &cache,
                UnixNanos::from(TS_NOW),
            )
            .unwrap_err();
        assert!(
            err_sl_up.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
            "3-leg bracket SL upward qty modify (10->15) must fail with REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, got: {err_sl_up}"
        );
    }
}

#[test]
fn test_disguised_stop_market_and_stop_limit_with_take_profit_intent_enforced_as_stops() {
    let markers: [(&str, Option<&str>, Option<&str>); 2] = [
        ("TAG_TP", Some("TAKE_PROFIT"), None),
        ("PARAM_TP", None, Some("TAKE_PROFIT")),
    ];

    for (label, tag, intent_param) in markers {
        for (plan_side, pos_side, entry_side, exit_side, initial_stop, bad_sl, tight_sl, loose_sl) in [
            ("LONG", OrderSide::Buy, OrderSide::Buy, OrderSide::Sell, dec!(145.00), "140.00", "147.00", "142.00"),
            ("SHORT", OrderSide::Sell, OrderSide::Sell, OrderSide::Buy, dec!(155.00), "160.00", "153.00", "158.00"),
        ] {
            for stop_kind in ["StopMarket", "StopLimit"] {
                // Part A: SubmitOrderList violating initial_stop_price with TAKE_PROFIT marker -> STOP_LOSS_FLOOR_VIOLATION
                {
                    let (inst, mut cache, gate) =
                        setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                    let list_id = format!("OL-DISG-{stop_kind}-{label}-{plan_side}");
                    let ent_cid = format!("O-DISG-E-{stop_kind}-{label}-{plan_side}");
                    let stp_cid = format!("O-DISG-S-{stop_kind}-{label}-{plan_side}");

                    let entry = make_limit_order(inst.id(), &ent_cid, entry_side, "20", "150.00", false);
                    let bad_stop_base = if stop_kind == "StopMarket" {
                        make_stop_market_order(inst.id(), &stp_cid, exit_side, "20", bad_sl, true)
                    } else {
                        make_stop_limit_order(
                            inst.id(),
                            &stp_cid,
                            exit_side,
                            "20",
                            bad_sl,
                            bad_sl,
                            true,
                            None,
                        )
                    };
                    let disguised_stop = with_exit_marker(bad_stop_base, tag, intent_param);
                    let orders = vec![entry, disguised_stop];
                    for o in &orders {
                        cache.add_order(o.clone(), None, None, false).unwrap();
                    }

                    let err = gate
                        .check_command(
                            &make_mixed_order_list_cmd(&list_id, &orders),
                            &cache,
                            UnixNanos::from(TS_NOW),
                        )
                        .unwrap_err();
                    assert!(
                        err.contains("STOP_LOSS_FLOOR_VIOLATION"),
                        "{list_id}: disguised {stop_kind} must fail with STOP_LOSS_FLOOR_VIOLATION, got: {err}"
                    );
                    assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
                    assert!(!gate.has_admitted_reservation(&ent_cid));
                }

                // Part B: ModifyOrder loosening disguised StopMarket / StopLimit -> STOP_LOSS_LOOSENING_PROHIBITED;
                // tightening must succeed under entry_freeze=True.
                {
                    let (inst, mut cache, gate) =
                        setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                    open_position_in_cache(
                        &mut cache,
                        &inst,
                        pos_side,
                        "20",
                        "150.00",
                        &format!("P-DISG-{stop_kind}-{label}-{plan_side}"),
                    );

                    let init_px = initial_stop.to_string();
                    let base = if stop_kind == "StopMarket" {
                        make_stop_market_order(
                            inst.id(),
                            &format!("O-DISG-MOD-{stop_kind}-{label}-{plan_side}"),
                            exit_side,
                            "20",
                            &init_px,
                            true,
                        )
                    } else {
                        make_stop_limit_order(
                            inst.id(),
                            &format!("O-DISG-MOD-{stop_kind}-{label}-{plan_side}"),
                            exit_side,
                            "20",
                            &init_px,
                            &init_px,
                            true,
                            None,
                        )
                    };
                    let order = accept_order(with_exit_marker(base, tag, intent_param));
                    cache.add_order(order.clone(), None, None, true).unwrap();

                    let has_limit = stop_kind == "StopLimit";
                    let err_loose = gate
                        .check_command(
                            &TradingCommand::ModifyOrder(modify_cmd(
                                &order,
                                None,
                                if has_limit { Some(loose_sl) } else { None },
                                Some(loose_sl),
                            )),
                            &cache,
                            UnixNanos::from(TS_NOW),
                        )
                        .unwrap_err();
                    assert!(
                        err_loose.contains("STOP_LOSS_LOOSENING_PROHIBITED"),
                        "{plan_side} {stop_kind} {label}: loosening disguised TP stop must fail with STOP_LOSS_LOOSENING_PROHIBITED, got: {err_loose}"
                    );

                    let res_tight = gate.check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(
                            &order,
                            None,
                            if has_limit { Some(tight_sl) } else { None },
                            Some(tight_sl),
                        )),
                        &cache,
                        UnixNanos::from(TS_NOW),
                    );
                    assert!(
                        res_tight.is_ok(),
                        "{plan_side} {stop_kind} {label}: tightening stop must succeed: {res_tight:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn test_tp_trigger_tightening_with_trailing_stop_market_and_limit_both_tag_and_param() {
    for (label, tag, intent_param) in [
        ("TAG_TP", Some("TAKE_PROFIT"), None),
        ("PARAM_TP", None, Some("TAKE_PROFIT")),
    ] {
        for (plan_side, pos_side, exit_side, initial_stop, tp_init, tp_tight, tp_lim_tight) in [
            ("LONG", OrderSide::Buy, OrderSide::Sell, dec!(145.00), "165.00", "160.00", "159.90"),
            ("SHORT", OrderSide::Sell, OrderSide::Buy, dec!(155.00), "135.00", "140.00", "140.10"),
        ] {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate(plan_side, dec!(100), initial_stop);
            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            open_position_in_cache(
                &mut cache,
                &inst,
                pos_side,
                "40",
                "150.00",
                &format!("P-TSM-TP-{label}-{plan_side}"),
            );

            let tsm_tp = accept_order(make_trailing_stop_market_tp(
                inst.id(),
                &format!("O-TSM-TP-{label}-{plan_side}"),
                exit_side,
                "40",
                Some(tp_init),
                tp_init,
                dec!(1.00),
                true,
                tag,
                intent_param,
            ));
            let tsl_init_lim = if plan_side == "LONG" { "164.90" } else { "135.10" };
            let tsl_tp = accept_order(with_exit_marker(
                make_trailing_stop_limit_order(
                    inst.id(),
                    &format!("O-TSL-TP2-{label}-{plan_side}"),
                    exit_side,
                    "40",
                    Some(tp_init),
                    tsl_init_lim,
                    tp_init,
                    dec!(0.10),
                    dec!(1.00),
                    true,
                    None,
                ),
                tag,
                intent_param,
            ));
            cache.add_order(tsm_tp.clone(), None, None, true).unwrap();
            cache.add_order(tsl_tp.clone(), None, None, true).unwrap();

            assert!(
                gate.check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(&tsm_tp, None, None, Some(tp_tight))),
                    &cache,
                    UnixNanos::from(TS_NOW),
                )
                .is_ok(),
                "{label} {plan_side}: TrailingStopMarket TP trigger tightening must succeed"
            );
            assert!(
                gate.check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(
                        &tsl_tp,
                        None,
                        Some(tp_lim_tight),
                        Some(tp_tight),
                    )),
                    &cache,
                    UnixNanos::from(TS_NOW),
                )
                .is_ok(),
                "{label} {plan_side}: TrailingStopLimit TP trigger tightening must succeed"
            );
        }
    }
}

#[test]
fn test_partial_fill_plus_terminal_cancel_and_expire_reservation_reconciliation() {
    for terminal_kind in ["CANCELED", "EXPIRED"] {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(20), dec!(145.00));

        // E1 for 20 shares -> admitted (reserved_qty == 20).
        let e1 = accept_order(make_limit_order(
            inst.id(),
            &format!("O-PF-E1-{terminal_kind}"),
            OrderSide::Buy,
            "20",
            "150.00",
            false,
        ));
        cache.add_order(e1.clone(), None, None, true).unwrap();
        assert!(
            gate.check_command(&submit_cmd(&e1), &cache, UnixNanos::from(TS_NOW))
                .is_ok()
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(20));

        // E1 partially fills 8 shares (leaves 12 unfilled), then reaches terminal Canceled or Expired.
        let partial_fill = OrderFilled::new(
            e1.trader_id(),
            e1.strategy_id(),
            e1.instrument_id(),
            e1.client_order_id(),
            e1.venue_order_id().unwrap(),
            AccountId::from("ALPACA-001"),
            TradeId::from(format!("T-PF-{terminal_kind}").as_str()),
            OrderSide::Buy,
            OrderType::Limit,
            Quantity::from("8"),
            Price::from("150.00"),
            Currency::USD(),
            LiquiditySide::Taker,
            UUID4::new(),
            UnixNanos::from(TS_NOW + 1),
            UnixNanos::from(TS_NOW + 1),
            false,
            Some(PositionId::from(format!("P-PF-{terminal_kind}").as_str())),
            Some(Money::from("0.00 USD")),
            None,
        );
        cache
            .update_order(&OrderEventAny::Filled(partial_fill))
            .unwrap();
        let terminal_event = if terminal_kind == "CANCELED" {
            OrderEventAny::Canceled(OrderCanceled::new(
                e1.trader_id(),
                e1.strategy_id(),
                e1.instrument_id(),
                e1.client_order_id(),
                UUID4::new(),
                UnixNanos::from(TS_NOW + 2),
                UnixNanos::from(TS_NOW + 2),
                false,
                e1.venue_order_id(),
                e1.account_id(),
                None,
            ))
        } else {
            OrderEventAny::Expired(OrderExpired::new(
                e1.trader_id(),
                e1.strategy_id(),
                e1.instrument_id(),
                e1.client_order_id(),
                UUID4::new(),
                UnixNanos::from(TS_NOW + 2),
                UnixNanos::from(TS_NOW + 2),
                false,
                e1.venue_order_id(),
                e1.account_id(),
            ))
        };
        cache.update_order(&terminal_event).unwrap();

        let unfilled_leaves = {
            let e1_cached = cache.order(&e1.client_order_id()).unwrap();
            assert_eq!(e1_cached.filled_qty(), Quantity::from("8"));
            assert_eq!(e1_cached.leaves_qty(), Quantity::from("12"));
            e1_cached.leaves_qty().as_decimal()
        };
        assert_eq!(unfilled_leaves, dec!(12));
        let remaining_after_release = gate.release_order_reservation(
            e1.client_order_id().as_str(),
            "AAPL",
            unfilled_leaves,
        );
        assert_eq!(remaining_after_release, dec!(8));
        assert_eq!(gate.reserved_qty("AAPL"), dec!(8));

        // Subsequent E2 for 12 shares (8 + 12 = 20) MUST be admitted -> reserved_qty == 20.
        let e2 = make_limit_order(
            inst.id(),
            &format!("O-PF-E2-{terminal_kind}"),
            OrderSide::Buy,
            "12",
            "150.00",
            false,
        );
        cache.add_order(e2.clone(), None, None, false).unwrap();
        assert!(
            gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW))
                .is_ok(),
            "{terminal_kind}: E2 for 12 shares (8 + 12 = 20) must be admitted"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(20));
        assert!(gate.has_admitted_reservation(e2.client_order_id().as_str()));

        // Subsequent E3 for 1 additional share MUST be denied with QTY_CAP_EXCEEDED.
        let e3 = make_limit_order(
            inst.id(),
            &format!("O-PF-E3-{terminal_kind}"),
            OrderSide::Buy,
            "1",
            "150.00",
            false,
        );
        cache.add_order(e3.clone(), None, None, false).unwrap();
        let err_e3 = gate
            .check_command(&submit_cmd(&e3), &cache, UnixNanos::from(TS_NOW))
            .unwrap_err();
        assert!(
            err_e3.contains("QTY_CAP_EXCEEDED"),
            "{terminal_kind}: E3 for 1 extra share must fail with QTY_CAP_EXCEEDED, got: {err_e3}"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(20));
        assert!(!gate.has_admitted_reservation(e3.client_order_id().as_str()));
    }
}

fn make_contingent_market_order_with_marker(
    inst_id: InstrumentId,
    cid: &str,
    side: OrderSide,
    qty: &str,
    reduce_only: bool,
    contingency: Option<ContingencyType>,
    list_id: Option<&str>,
    linked: Option<Vec<&str>>,
    parent: Option<&str>,
    tag: Option<&str>,
    intent_param: Option<&str>,
) -> OrderAny {
    let base = OrderAny::Market(MarketOrder::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst_id,
        ClientOrderId::from(cid),
        side,
        Quantity::from(qty),
        TimeInForce::Day,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
        reduce_only,
        false,
        contingency,
        list_id.map(OrderListId::from),
        linked.map(|ids| ids.into_iter().map(ClientOrderId::from).collect()),
        parent.map(ClientOrderId::from),
        None,
        None,
        None,
        None,
    ));
    with_exit_marker(base, tag, intent_param)
}

#[test]
fn test_bug10_unmasked_cache_reconcile_late_release_and_reconcile_terminal_order() {
    // Subcase A: Unmasked reconcile_cache_reservations (NO release_order_reservation before E2)
    // followed by late release_order_reservation and late reconcile_terminal_order.
    for terminal_kind in ["CANCELED", "EXPIRED"] {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(20), dec!(145.00));
        let e1_cid = format!("O-B10-E1-{terminal_kind}");
        let e2_cid = format!("O-B10-E2-{terminal_kind}");
        let e3_cid = format!("O-B10-E3-{terminal_kind}");

        let e1 = accept_order(make_limit_order(
            inst.id(),
            &e1_cid,
            OrderSide::Buy,
            "20",
            "150.00",
            false,
        ));
        cache.add_order(e1.clone(), None, None, true).unwrap();
        assert!(
            gate.check_command(&submit_cmd(&e1), &cache, UnixNanos::from(TS_NOW))
                .is_ok()
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(20));

        // E1 partially fills 8 shares (leaves=12), then reaches terminal Canceled or Expired.
        let partial_fill = OrderFilled::new(
            e1.trader_id(),
            e1.strategy_id(),
            e1.instrument_id(),
            e1.client_order_id(),
            e1.venue_order_id().unwrap(),
            AccountId::from("ALPACA-001"),
            TradeId::from(format!("T-B10-{terminal_kind}").as_str()),
            OrderSide::Buy,
            OrderType::Limit,
            Quantity::from("8"),
            Price::from("150.00"),
            Currency::USD(),
            LiquiditySide::Taker,
            UUID4::new(),
            UnixNanos::from(TS_NOW + 1),
            UnixNanos::from(TS_NOW + 1),
            false,
            Some(PositionId::from(format!("P-B10-{terminal_kind}").as_str())),
            Some(Money::from("0.00 USD")),
            None,
        );
        cache
            .update_order(&OrderEventAny::Filled(partial_fill))
            .unwrap();
        let terminal_event = if terminal_kind == "CANCELED" {
            OrderEventAny::Canceled(OrderCanceled::new(
                e1.trader_id(),
                e1.strategy_id(),
                e1.instrument_id(),
                e1.client_order_id(),
                UUID4::new(),
                UnixNanos::from(TS_NOW + 2),
                UnixNanos::from(TS_NOW + 2),
                false,
                e1.venue_order_id(),
                e1.account_id(),
                None,
            ))
        } else {
            OrderEventAny::Expired(OrderExpired::new(
                e1.trader_id(),
                e1.strategy_id(),
                e1.instrument_id(),
                e1.client_order_id(),
                UUID4::new(),
                UnixNanos::from(TS_NOW + 2),
                UnixNanos::from(TS_NOW + 2),
                false,
                e1.venue_order_id(),
                e1.account_id(),
            ))
        };
        cache.update_order(&terminal_event).unwrap();

        // CRITICAL: DO NOT call release_order_reservation before submitting E2!
        let e2 = make_limit_order(inst.id(), &e2_cid, OrderSide::Buy, "12", "150.00", false);
        cache.add_order(e2.clone(), None, None, false).unwrap();
        assert!(
            gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW + 3))
                .is_ok(),
            "{terminal_kind}: E2(12) must be admitted via automatic cache reconciliation"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(20));
        assert!(!gate.has_admitted_reservation(&e1_cid));
        assert!(gate.has_admitted_reservation(&e2_cid));

        // Simulate late PostgresTelemetryActor release_order_reservation(E1, "AAPL", 12) arriving after E2.
        let after_late_rel = gate.release_order_reservation(&e1_cid, "AAPL", dec!(12));
        assert_eq!(
            after_late_rel,
            dec!(20),
            "{terminal_kind}: late release_order_reservation must be an idempotent no-op and NEVER double-release down to 12"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(20));

        // Also verify late reconcile_terminal_order(E1, "AAPL", 8) is an idempotent no-op.
        let after_late_rec = gate.reconcile_terminal_order(&e1_cid, "AAPL", dec!(8));
        assert_eq!(after_late_rec, dec!(20));
        assert_eq!(gate.reserved_qty("AAPL"), dec!(20));

        // E3(1) MUST be denied with QTY_CAP_EXCEEDED.
        let e3 = make_limit_order(inst.id(), &e3_cid, OrderSide::Buy, "1", "150.00", false);
        cache.add_order(e3.clone(), None, None, false).unwrap();
        let err_e3 = gate
            .check_command(&submit_cmd(&e3), &cache, UnixNanos::from(TS_NOW + 4))
            .unwrap_err();
        assert!(
            err_e3.contains("QTY_CAP_EXCEEDED"),
            "{terminal_kind}: E3(1) must fail with QTY_CAP_EXCEEDED, got: {err_e3}"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(20));
    }

    // Subcase B: Direct reconcile_terminal_order clamping (> tracked_qty, < 0, partial fill, unknown CID)
    // and idempotency with release_order_reservation.
    {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(100), dec!(145.00));
        let o_norm = make_limit_order(inst.id(), "O-RTO-NORM", OrderSide::Buy, "20", "150.00", false);
        let o_over = make_limit_order(inst.id(), "O-RTO-OVER", OrderSide::Buy, "20", "150.00", false);
        let o_neg = make_limit_order(inst.id(), "O-RTO-NEG", OrderSide::Buy, "20", "150.00", false);
        for o in [&o_norm, &o_over, &o_neg] {
            cache.add_order(o.clone(), None, None, false).unwrap();
            assert!(gate.check_command(&submit_cmd(o), &cache, UnixNanos::from(TS_NOW)).is_ok());
        }
        assert_eq!(gate.reserved_qty("AAPL"), dec!(60));

        // Partial fill: 8 filled out of 20 -> releases 12 -> 48 remaining.
        assert_eq!(gate.reconcile_terminal_order("O-RTO-NORM", "AAPL", dec!(8)), dec!(48));
        assert!(!gate.has_admitted_reservation("O-RTO-NORM"));
        assert_eq!(gate.reconcile_terminal_order("O-RTO-NORM", "AAPL", dec!(8)), dec!(48));
        assert_eq!(gate.release_order_reservation("O-RTO-NORM", "AAPL", dec!(12)), dec!(48));

        // Over-filled clamp: filled=35 > tracked=20 -> clamped to 20 -> releases 0 -> 48 remaining.
        assert_eq!(gate.reconcile_terminal_order("O-RTO-OVER", "AAPL", dec!(35)), dec!(48));
        assert!(!gate.has_admitted_reservation("O-RTO-OVER"));
        assert_eq!(gate.release_order_reservation("O-RTO-OVER", "AAPL", dec!(20)), dec!(48));

        // Negative fill clamp: filled=-5 < 0 -> clamped to 0 -> releases 20 -> 28 remaining.
        assert_eq!(gate.reconcile_terminal_order("O-RTO-NEG", "AAPL", dec!(-5)), dec!(28));
        assert!(!gate.has_admitted_reservation("O-RTO-NEG"));
        assert_eq!(gate.release_order_reservation("O-RTO-NEG", "AAPL", dec!(20)), dec!(28));

        // Unknown CID is a safe no-op.
        assert_eq!(gate.reconcile_terminal_order("O-RTO-MISSING", "AAPL", dec!(5)), dec!(28));
        assert_eq!(gate.reserved_qty("AAPL"), dec!(28));
    }

    // Subcase C: Reverse-order (release_order_reservation before reconcile_cache_reservations + duplicate release).
    {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(20), dec!(145.00));
        let e1 = accept_order(make_limit_order(
            inst.id(),
            "O-B10-REV-E1",
            OrderSide::Buy,
            "20",
            "150.00",
            false,
        ));
        cache.add_order(e1.clone(), None, None, true).unwrap();
        assert!(gate.check_command(&submit_cmd(&e1), &cache, UnixNanos::from(TS_NOW)).is_ok());
        let fill = OrderFilled::new(
            e1.trader_id(),
            e1.strategy_id(),
            e1.instrument_id(),
            e1.client_order_id(),
            e1.venue_order_id().unwrap(),
            AccountId::from("ALPACA-001"),
            TradeId::from("T-B10-REV"),
            OrderSide::Buy,
            OrderType::Limit,
            Quantity::from("8"),
            Price::from("150.00"),
            Currency::USD(),
            LiquiditySide::Taker,
            UUID4::new(),
            UnixNanos::from(TS_NOW + 1),
            UnixNanos::from(TS_NOW + 1),
            false,
            Some(PositionId::from("P-B10-REV")),
            Some(Money::from("0.00 USD")),
            None,
        );
        cache.update_order(&OrderEventAny::Filled(fill)).unwrap();
        cache
            .update_order(&OrderEventAny::Canceled(OrderCanceled::new(
                e1.trader_id(),
                e1.strategy_id(),
                e1.instrument_id(),
                e1.client_order_id(),
                UUID4::new(),
                UnixNanos::from(TS_NOW + 2),
                UnixNanos::from(TS_NOW + 2),
                false,
                e1.venue_order_id(),
                e1.account_id(),
                None,
            )))
            .unwrap();

        // Actor releases leaves=12 before E2 triggers reconcile_cache_reservations.
        assert_eq!(gate.release_order_reservation("O-B10-REV-E1", "AAPL", dec!(12)), dec!(8));
        // Submitting E2(12) invokes reconcile_cache_reservations -> must NOT double-release, admits E2 -> 20.
        let e2 = make_limit_order(inst.id(), "O-B10-REV-E2", OrderSide::Buy, "12", "150.00", false);
        cache.add_order(e2.clone(), None, None, false).unwrap();
        assert!(gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW + 3)).is_ok());
        assert_eq!(gate.reserved_qty("AAPL"), dec!(20));
        assert!(!gate.has_admitted_reservation("O-B10-REV-E1"));

        // Duplicate release after reconciliation must be a no-op -> stays 20.
        assert_eq!(gate.release_order_reservation("O-B10-REV-E1", "AAPL", dec!(12)), dec!(20));
        assert_eq!(gate.reserved_qty("AAPL"), dec!(20));

        let e3 = make_limit_order(inst.id(), "O-B10-REV-E3", OrderSide::Buy, "1", "150.00", false);
        cache.add_order(e3.clone(), None, None, false).unwrap();
        let err_e3 = gate
            .check_command(&submit_cmd(&e3), &cache, UnixNanos::from(TS_NOW + 4))
            .unwrap_err();
        assert!(err_e3.contains("QTY_CAP_EXCEEDED"), "got: {err_e3}");
    }
}

#[test]
fn test_bug8c_symmetric_mutual_oco_and_ouo_linkage() {
    for entry_freeze in [false, true] {
        // 1. Unilateral OCO / OUO spoofing in SubmitOrder:
        //    O1 has contingency_type=None, O2 declares ContingencyType::Oco (or Ouo) + linked_order_ids=[O1].
        for spoof_cont in [ContingencyType::Oco, ContingencyType::Ouo] {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
            open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-8C-UNI-SUB");
            if entry_freeze {
                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            }

            let o1 = accept_order(with_exit_marker(
                make_stop_market_order(inst.id(), "O-8C-UNI-1", OrderSide::Sell, "10", "146.00", false),
                Some("STOP_LOSS"),
                None,
            ));
            cache.add_order(o1.clone(), None, None, true).unwrap();
            assert!(gate.check_command(&submit_cmd(&o1), &cache, UnixNanos::from(TS_NOW)).is_ok());

            let o2 = with_exit_marker(
                make_contingent_limit_order(
                    inst.id(),
                    "O-8C-UNI-2",
                    OrderSide::Sell,
                    "10",
                    "160.00",
                    false,
                    Some(spoof_cont),
                    None,
                    Some(vec!["O-8C-UNI-1"]),
                    None,
                ),
                Some("TAKE_PROFIT"),
                None,
            );
            cache.add_order(o2.clone(), None, None, false).unwrap();
            let err = gate
                .check_command(&submit_cmd(&o2), &cache, UnixNanos::from(TS_NOW))
                .unwrap_err();
            assert!(
                err.contains("ENTRY_FREEZE_ACTIVE") || err.contains("SIDE_MISMATCH"),
                "freeze={entry_freeze} {spoof_cont:?}: unilateral OCO/OUO spoof in SubmitOrder must fail, got: {err}"
            );
        }

        // 2. Unilateral & non-reciprocal OCO spoofing in SubmitOrderList.
        for list_shape in ["none_then_oco", "oco_then_none", "missing_reciprocal_link"] {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
            open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-8C-UNI-LIST");
            if entry_freeze {
                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            }
            let list_id = format!("OL-8C-UNI-{list_shape}");
            let (o1_cont, o1_link, o2_cont, o2_link) = match list_shape {
                "none_then_oco" => (None, None, Some(ContingencyType::Oco), Some(vec!["O-8C-L1"])),
                "oco_then_none" => (Some(ContingencyType::Oco), Some(vec!["O-8C-L2"]), None, None),
                _ => (
                    Some(ContingencyType::Oco),
                    Some(vec!["O-8C-L2"]),
                    Some(ContingencyType::Oco),
                    Some(vec!["O-8C-UNRELATED"]),
                ),
            };
            let o1 = with_exit_marker(
                make_contingent_stop_market_order(
                    inst.id(),
                    "O-8C-L1",
                    OrderSide::Sell,
                    "10",
                    "146.00",
                    false,
                    o1_cont,
                    Some(&list_id),
                    o1_link,
                    None,
                ),
                Some("STOP_LOSS"),
                None,
            );
            let o2 = with_exit_marker(
                make_contingent_limit_order(
                    inst.id(),
                    "O-8C-L2",
                    OrderSide::Sell,
                    "10",
                    "160.00",
                    false,
                    o2_cont,
                    Some(&list_id),
                    o2_link,
                    None,
                ),
                Some("TAKE_PROFIT"),
                None,
            );
            let orders = vec![o1, o2];
            for o in &orders {
                cache.add_order(o.clone(), None, None, false).unwrap();
            }
            let res = gate.check_command(
                &make_mixed_order_list_cmd(&list_id, &orders),
                &cache,
                UnixNanos::from(TS_NOW),
            );
            assert!(
                res.is_err(),
                "freeze={entry_freeze} {list_shape}: unilateral OCO in SubmitOrderList must be denied: {res:?}"
            );
        }

        // 3. Mismatched contingency (O1 Oco vs O2 Ouo) denied in SubmitOrder and SubmitOrderList.
        {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
            open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-8C-MIS-SUB");
            if entry_freeze {
                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            }
            let o1 = accept_order(with_exit_marker(
                make_contingent_stop_market_order(
                    inst.id(),
                    "O-8C-M1",
                    OrderSide::Sell,
                    "10",
                    "146.00",
                    false,
                    Some(ContingencyType::Oco),
                    Some("OL-8C-MIS"),
                    Some(vec!["O-8C-M2"]),
                    None,
                ),
                Some("STOP_LOSS"),
                None,
            ));
            let o2 = with_exit_marker(
                make_contingent_limit_order(
                    inst.id(),
                    "O-8C-M2",
                    OrderSide::Sell,
                    "10",
                    "160.00",
                    false,
                    Some(ContingencyType::Ouo),
                    Some("OL-8C-MIS"),
                    Some(vec!["O-8C-M1"]),
                    None,
                ),
                Some("TAKE_PROFIT"),
                None,
            );
            cache.add_order(o1.clone(), None, None, true).unwrap();
            assert!(gate.check_command(&submit_cmd(&o1), &cache, UnixNanos::from(TS_NOW)).is_ok());
            cache.add_order(o2.clone(), None, None, false).unwrap();
            assert!(
                gate.check_command(&submit_cmd(&o2), &cache, UnixNanos::from(TS_NOW))
                    .is_err(),
                "freeze={entry_freeze}: mismatched Oco vs Ouo in SubmitOrder must be denied"
            );
        }
        {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
            open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-8C-MIS-LST");
            if entry_freeze {
                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            }
            let o1 = with_exit_marker(
                make_contingent_stop_market_order(
                    inst.id(),
                    "O-8C-ML1",
                    OrderSide::Sell,
                    "10",
                    "146.00",
                    false,
                    Some(ContingencyType::Oco),
                    Some("OL-8C-MISL"),
                    Some(vec!["O-8C-ML2"]),
                    None,
                ),
                Some("STOP_LOSS"),
                None,
            );
            let o2 = with_exit_marker(
                make_contingent_limit_order(
                    inst.id(),
                    "O-8C-ML2",
                    OrderSide::Sell,
                    "10",
                    "160.00",
                    false,
                    Some(ContingencyType::Ouo),
                    Some("OL-8C-MISL"),
                    Some(vec!["O-8C-ML1"]),
                    None,
                ),
                Some("TAKE_PROFIT"),
                None,
            );
            let orders = vec![o1, o2];
            for o in &orders {
                cache.add_order(o.clone(), None, None, false).unwrap();
            }
            assert!(
                gate.check_command(
                    &make_mixed_order_list_cmd("OL-8C-MISL", &orders),
                    &cache,
                    UnixNanos::from(TS_NOW)
                )
                .is_err(),
                "freeze={entry_freeze}: mismatched Oco vs Ouo in SubmitOrderList must be denied"
            );
        }

        // 4. Symmetric mutual Oco and symmetric mutual Ouo (SubmitOrderList + sequential SubmitOrder) ADMITTED.
        for sym_cont in [ContingencyType::Oco, ContingencyType::Ouo] {
            // SubmitOrderList
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
            open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-8C-OK-LST");
            if entry_freeze {
                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            }
            let list_id = format!("OL-8C-OK-{sym_cont:?}");
            let o1 = with_exit_marker(
                make_contingent_stop_market_order(
                    inst.id(),
                    "O-8C-OK1",
                    OrderSide::Sell,
                    "10",
                    "146.00",
                    false,
                    Some(sym_cont),
                    Some(&list_id),
                    Some(vec!["O-8C-OK2"]),
                    None,
                ),
                Some("STOP_LOSS"),
                None,
            );
            let o2 = with_exit_marker(
                make_contingent_limit_order(
                    inst.id(),
                    "O-8C-OK2",
                    OrderSide::Sell,
                    "10",
                    "160.00",
                    false,
                    Some(sym_cont),
                    Some(&list_id),
                    Some(vec!["O-8C-OK1"]),
                    None,
                ),
                Some("TAKE_PROFIT"),
                None,
            );
            let orders = vec![o1.clone(), o2.clone()];
            for o in &orders {
                cache.add_order(o.clone(), None, None, false).unwrap();
            }
            let res = gate.check_command(
                &make_mixed_order_list_cmd(&list_id, &orders),
                &cache,
                UnixNanos::from(TS_NOW),
            );
            assert!(
                res.is_ok(),
                "freeze={entry_freeze} {sym_cont:?}: symmetric mutual SubmitOrderList must be admitted: {res:?}"
            );

            // Sequential SubmitOrder
            let (inst_s, mut cache_s, gate_s) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
            open_position_in_cache(&mut cache_s, &inst_s, OrderSide::Buy, "10", "150.00", "P-8C-OK-SEQ");
            if entry_freeze {
                gate_s.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            }
            let s1 = accept_order(with_exit_marker(
                make_contingent_stop_market_order(
                    inst_s.id(),
                    "O-8C-SEQ1",
                    OrderSide::Sell,
                    "10",
                    "146.00",
                    false,
                    Some(sym_cont),
                    Some("OL-8C-SEQ"),
                    Some(vec!["O-8C-SEQ2"]),
                    None,
                ),
                Some("STOP_LOSS"),
                None,
            ));
            let s2 = with_exit_marker(
                make_contingent_limit_order(
                    inst_s.id(),
                    "O-8C-SEQ2",
                    OrderSide::Sell,
                    "10",
                    "160.00",
                    false,
                    Some(sym_cont),
                    Some("OL-8C-SEQ"),
                    Some(vec!["O-8C-SEQ1"]),
                    None,
                ),
                Some("TAKE_PROFIT"),
                None,
            );
            cache_s.add_order(s1.clone(), None, None, true).unwrap();
            assert!(gate_s.check_command(&submit_cmd(&s1), &cache_s, UnixNanos::from(TS_NOW)).is_ok());
            cache_s.add_order(s2.clone(), None, None, false).unwrap();
            let res_s2 = gate_s.check_command(&submit_cmd(&s2), &cache_s, UnixNanos::from(TS_NOW));
            assert!(
                res_s2.is_ok(),
                "freeze={entry_freeze} {sym_cont:?}: symmetric mutual sequential SubmitOrder must be admitted: {res_s2:?}"
            );
        }
    }
}

#[test]
fn test_bug8b_and_p3_cross_bucket_cumulative_unlinked_exit_exposure() {
    for entry_freeze in [false, true] {
        // 1. Mixing reduce_only=True (SELL 6) + reduce_only=False (SELL 6) against LONG 10.
        //    (a) SubmitOrder: rest reduce_only=True SELL 6, then submit reduce_only=False SELL 6 -> DENIED.
        {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
            open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-8B-MIX-SUB");
            if entry_freeze {
                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            }

            let ro_true = accept_order(make_limit_order(
                inst.id(),
                "O-8B-ROT",
                OrderSide::Sell,
                "6",
                "159.00",
                true,
            ));
            cache.add_order(ro_true.clone(), None, None, true).unwrap();
            assert!(gate.check_command(&submit_cmd(&ro_true), &cache, UnixNanos::from(TS_NOW)).is_ok());

            let ro_false = with_exit_marker(
                make_limit_order(inst.id(), "O-8B-ROF", OrderSide::Sell, "6", "160.00", false),
                Some("TAKE_PROFIT"),
                None,
            );
            cache.add_order(ro_false.clone(), None, None, false).unwrap();
            let res = gate.check_command(&submit_cmd(&ro_false), &cache, UnixNanos::from(TS_NOW));
            assert!(
                res.is_err(),
                "freeze={entry_freeze}: reduce_only=True(6) + reduce_only=False(6) > LONG 10 must deny excess reduce_only=False exit: {res:?}"
            );
        }

        //    (b) SubmitOrderList: [ro=True SELL 6, ro=False SELL 6] and [ro=False SELL 6, ro=True SELL 6] -> DENIED atomically.
        for ro_true_first in [true, false] {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
            open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-8B-MIX-LST");
            if entry_freeze {
                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            }
            let ro_true = make_stop_market_order(inst.id(), "O-8B-L-ROT", OrderSide::Sell, "6", "146.00", true);
            let ro_false = with_exit_marker(
                make_limit_order(inst.id(), "O-8B-L-ROF", OrderSide::Sell, "6", "160.00", false),
                Some("TAKE_PROFIT"),
                None,
            );
            let orders = if ro_true_first {
                vec![ro_true, ro_false]
            } else {
                vec![ro_false, ro_true]
            };
            for o in &orders {
                cache.add_order(o.clone(), None, None, false).unwrap();
            }
            let res = gate.check_command(
                &make_mixed_order_list_cmd("OL-8B-MIX", &orders),
                &cache,
                UnixNanos::from(TS_NOW),
            );
            assert!(
                res.is_err(),
                "freeze={entry_freeze} ro_true_first={ro_true_first}: mixed SubmitOrderList 6+6 > 10 must deny atomically: {res:?}"
            );
        }

        // 2. Unlinked cross-bucket exits (StopMarket SELL 10 ro=False + Limit SELL 10 ro=False) against LONG 10.
        //    (a) SubmitOrder (both orders: SL first, then TP; and TP first, then SL) -> second DENIED.
        for sl_first in [true, false] {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
            open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-P3-CB-SUB");
            if entry_freeze {
                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            }
            let sl = accept_order(with_exit_marker(
                make_stop_market_order(inst.id(), "O-P3-SL", OrderSide::Sell, "10", "146.00", false),
                Some("STOP_LOSS"),
                None,
            ));
            let tp = accept_order(with_exit_marker(
                make_limit_order(inst.id(), "O-P3-TP", OrderSide::Sell, "10", "160.00", false),
                Some("TAKE_PROFIT"),
                None,
            ));
            let (first, second) = if sl_first { (sl, tp) } else { (tp, sl) };
            cache.add_order(first.clone(), None, None, true).unwrap();
            assert!(gate.check_command(&submit_cmd(&first), &cache, UnixNanos::from(TS_NOW)).is_ok());

            cache.add_order(second.clone(), None, None, false).unwrap();
            let res2 = gate.check_command(&submit_cmd(&second), &cache, UnixNanos::from(TS_NOW));
            assert!(
                res2.is_err(),
                "freeze={entry_freeze} sl_first={sl_first}: unlinked cross-bucket 10+10 > LONG 10 must deny second exit: {res2:?}"
            );
        }

        //    (b) SubmitOrderList: unlinked [StopMarket SELL 10 ro=False, Limit SELL 10 ro=False] -> DENIED atomically.
        {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
            open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-P3-CB-LST");
            if entry_freeze {
                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            }
            let sl = with_exit_marker(
                make_stop_market_order(inst.id(), "O-P3-L-SL", OrderSide::Sell, "10", "146.00", false),
                Some("STOP_LOSS"),
                None,
            );
            let tp = with_exit_marker(
                make_limit_order(inst.id(), "O-P3-L-TP", OrderSide::Sell, "10", "160.00", false),
                Some("TAKE_PROFIT"),
                None,
            );
            let orders = vec![sl, tp];
            for o in &orders {
                cache.add_order(o.clone(), None, None, false).unwrap();
            }
            let res = gate.check_command(
                &make_mixed_order_list_cmd("OL-P3-CB", &orders),
                &cache,
                UnixNanos::from(TS_NOW),
            );
            assert!(
                res.is_err(),
                "freeze={entry_freeze}: unlinked cross-bucket SubmitOrderList 10+10 > LONG 10 must deny atomically: {res:?}"
            );
        }
    }
}

#[test]
fn test_bug8d_strict_oto_parentage_in_submit_order_list() {
    for case_name in [
        "no_oto_at_all",
        "spoof_parent_id_without_oto",
        "oto_missing_child_in_linked_ids",
        "oco_with_linked_ids_instead_of_oto",
    ] {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
        let list_id = format!("OL-8D-{case_name}");
        let e_cid = format!("O-8D-E-{case_name}");
        let m_cid = format!("O-8D-M-{case_name}");

        let (e_cont, e_links, m_parent) = match case_name {
            "no_oto_at_all" => (None, None, None),
            "spoof_parent_id_without_oto" => (None, None, Some(e_cid.as_str())),
            "oto_missing_child_in_linked_ids" => (
                Some(ContingencyType::Oto),
                Some(vec!["O-8D-UNRELATED"]),
                Some(e_cid.as_str()),
            ),
            _ => (Some(ContingencyType::Oco), Some(vec![m_cid.as_str()]), Some(e_cid.as_str())),
        };

        let entry = make_contingent_limit_order(
            inst.id(),
            &e_cid,
            OrderSide::Buy,
            "10",
            "100.00",
            false,
            e_cont,
            Some(&list_id),
            e_links,
            None,
        );
        let mkt_sell = make_contingent_market_order_with_marker(
            inst.id(),
            &m_cid,
            OrderSide::Sell,
            "10",
            false,
            None,
            Some(&list_id),
            None,
            m_parent,
            Some("TAKE_PROFIT"),
            None,
        );
        let orders = vec![entry, mkt_sell];
        for o in &orders {
            cache.add_order(o.clone(), None, None, false).unwrap();
        }
        let err = gate
            .check_command(
                &make_mixed_order_list_cmd(&list_id, &orders),
                &cache,
                UnixNanos::from(TS_NOW),
            )
            .unwrap_err();
        assert!(
            err.contains("SIDE_MISMATCH"),
            "{case_name}: non-OTO SELL 10 on flat LONG plan must deny atomically with SIDE_MISMATCH, got: {err}"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
        assert!(!gate.has_admitted_reservation(&e_cid));
        assert!(!gate.has_admitted_reservation(&m_cid));
    }

    // Contrast: legitimate OTO parentage [Limit BUY 10 (Oto, linked=[tp]), Limit SELL 10 (parent=entry)] admitted.
    {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
        let entry = make_contingent_limit_order(
            inst.id(),
            "O-8D-OK-E",
            OrderSide::Buy,
            "10",
            "149.50",
            false,
            Some(ContingencyType::Oto),
            Some("OL-8D-OK"),
            Some(vec!["O-8D-OK-TP"]),
            None,
        );
        let tp = with_exit_marker(
            make_contingent_limit_order(
                inst.id(),
                "O-8D-OK-TP",
                OrderSide::Sell,
                "10",
                "160.00",
                false,
                None,
                Some("OL-8D-OK"),
                None,
                Some("O-8D-OK-E"),
            ),
            Some("TAKE_PROFIT"),
            None,
        );
        let orders = vec![entry, tp];
        for o in &orders {
            cache.add_order(o.clone(), None, None, false).unwrap();
        }
        let res = gate.check_command(
            &make_mixed_order_list_cmd("OL-8D-OK", &orders),
            &cache,
            UnixNanos::from(TS_NOW),
        );
        assert!(res.is_ok(), "legitimate OTO parentage must be admitted: {res:?}");
        assert_eq!(gate.reserved_qty("AAPL"), dec!(10));
        assert!(gate.has_admitted_reservation("O-8D-OK-E"));
    }
}

#[test]
fn test_bug8a_modify_order_cumulative_non_reduce_only_exit_cap() {
    for entry_freeze in [false, true] {
        // 1. Open LONG 10, rest two unlinked SELL 5 (ro=False) exits across SL StopMarket & TP Limit buckets,
        //    then ModifyOrder either one to quantity=10 -> MUST reject with REDUCE_ONLY_MODIFY_EXCEEDS_POSITION.
        {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
            open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-8A-XB");
            if entry_freeze {
                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            }
            let sl = accept_order(with_exit_marker(
                make_stop_market_order(inst.id(), "O-8A-SL", OrderSide::Sell, "5", "146.00", false),
                Some("STOP_LOSS"),
                None,
            ));
            let tp = accept_order(with_exit_marker(
                make_limit_order(inst.id(), "O-8A-TP", OrderSide::Sell, "5", "160.00", false),
                Some("TAKE_PROFIT"),
                None,
            ));
            cache.add_order(sl.clone(), None, None, true).unwrap();
            assert!(gate.check_command(&submit_cmd(&sl), &cache, UnixNanos::from(TS_NOW)).is_ok());
            cache.add_order(tp.clone(), None, None, true).unwrap();
            assert!(gate.check_command(&submit_cmd(&tp), &cache, UnixNanos::from(TS_NOW)).is_ok());

            let err_tp = gate
                .check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(&tp, Some("10"), Some("159.00"), None)),
                    &cache,
                    UnixNanos::from(TS_NOW),
                )
                .unwrap_err();
            assert!(
                err_tp.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                "freeze={entry_freeze}: modifying TP 5->10 alongside resting SL 5 must fail with REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, got: {err_tp}"
            );

            let err_sl = gate
                .check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(&sl, Some("10"), None, Some("146.50"))),
                    &cache,
                    UnixNanos::from(TS_NOW),
                )
                .unwrap_err();
            assert!(
                err_sl.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                "freeze={entry_freeze}: modifying SL 5->10 alongside resting TP 5 must fail with REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, got: {err_sl}"
            );
        }

        // 1b. Same-bucket two unlinked Limit SELL 5 (ro=False) exits -> ModifyOrder one to qty=10 -> REJECTED.
        {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
            open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-8A-SB");
            if entry_freeze {
                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            }
            let tp1 = accept_order(with_exit_marker(
                make_limit_order(inst.id(), "O-8A-SB1", OrderSide::Sell, "5", "159.00", false),
                Some("TAKE_PROFIT"),
                None,
            ));
            let tp2 = accept_order(with_exit_marker(
                make_limit_order(inst.id(), "O-8A-SB2", OrderSide::Sell, "5", "160.00", false),
                Some("TAKE_PROFIT"),
                None,
            ));
            cache.add_order(tp1.clone(), None, None, true).unwrap();
            cache.add_order(tp2.clone(), None, None, true).unwrap();
            assert!(gate.check_command(&submit_cmd(&tp1), &cache, UnixNanos::from(TS_NOW)).is_ok());
            assert!(gate.check_command(&submit_cmd(&tp2), &cache, UnixNanos::from(TS_NOW)).is_ok());

            let err_sb = gate
                .check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(&tp2, Some("10"), Some("161.00"), None)),
                    &cache,
                    UnixNanos::from(TS_NOW),
                )
                .unwrap_err();
            assert!(
                err_sb.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                "freeze={entry_freeze}: same-bucket Limit SELL 5->10 alongside resting Limit SELL 5 must fail, got: {err_sb}"
            );
        }

        // 2. Pre-fill OTO-linked non-reduce_only exit ModifyOrder exceeding linked entry's remaining leaves qty
        //    must reject with REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, while trigger tightening on linked Ouo/Oco succeeds.
        for linked_cont in [ContingencyType::Ouo, ContingencyType::Oco] {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
            let list_id = format!("OL-8A-OTO-{linked_cont:?}");
            let e_cid = format!("O-8A-E-{linked_cont:?}");
            let sl_cid = format!("O-8A-SL-{linked_cont:?}");
            let tp_cid = format!("O-8A-TP-{linked_cont:?}");

            let entry = accept_order(make_contingent_limit_order(
                inst.id(),
                &e_cid,
                OrderSide::Buy,
                "10",
                "148.00",
                false,
                Some(ContingencyType::Oto),
                Some(&list_id),
                Some(vec![&sl_cid, &tp_cid]),
                None,
            ));
            let sl = accept_order(with_exit_marker(
                make_contingent_stop_market_order(
                    inst.id(),
                    &sl_cid,
                    OrderSide::Sell,
                    "10",
                    "146.00",
                    false,
                    Some(linked_cont),
                    Some(&list_id),
                    Some(vec![&tp_cid]),
                    Some(&e_cid),
                ),
                Some("STOP_LOSS"),
                None,
            ));
            let tp = accept_order(with_exit_marker(
                make_contingent_limit_order(
                    inst.id(),
                    &tp_cid,
                    OrderSide::Sell,
                    "10",
                    "160.00",
                    false,
                    Some(linked_cont),
                    Some(&list_id),
                    Some(vec![&sl_cid]),
                    Some(&e_cid),
                ),
                Some("TAKE_PROFIT"),
                None,
            ));
            let orders = vec![entry.clone(), sl.clone(), tp.clone()];
            for o in &orders {
                cache.add_order(o.clone(), None, None, true).unwrap();
            }
            assert!(
                gate.check_command(
                    &make_mixed_order_list_cmd(&list_id, &orders),
                    &cache,
                    UnixNanos::from(TS_NOW),
                )
                .is_ok()
            );

            if entry_freeze {
                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            }

            // Exceeding entry leaves (10 -> 12) must reject.
            let err_over = gate
                .check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(&tp, Some("12"), Some("159.00"), None)),
                    &cache,
                    UnixNanos::from(TS_NOW),
                )
                .unwrap_err();
            assert!(
                err_over.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                "freeze={entry_freeze} {linked_cont:?}: pre-fill OTO TP 10->12 must fail with REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, got: {err_over}"
            );

            // Downward-modify entry 10 -> 6 (remaining leaves = 6), then modify TP to 8 (> 6) -> MUST reject.
            let updated_entry = OrderUpdated::new(
                entry.trader_id(),
                entry.strategy_id(),
                entry.instrument_id(),
                entry.client_order_id(),
                Quantity::from("6"),
                UUID4::new(),
                UnixNanos::from(TS_NOW + 1),
                UnixNanos::from(TS_NOW + 1),
                false,
                entry.venue_order_id(),
                entry.account_id(),
                Some(Price::from("148.00")),
                None,
                None,
                false,
            );
            cache
                .update_order(&OrderEventAny::Updated(updated_entry))
                .unwrap();
            gate.on_order_updated(&entry.client_order_id(), dec!(6));

            let err_after_shrink = gate
                .check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(&sl, Some("8"), None, Some("146.50"))),
                    &cache,
                    UnixNanos::from(TS_NOW + 2),
                )
                .unwrap_err();
            assert!(
                err_after_shrink.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                "freeze={entry_freeze} {linked_cont:?}: pre-fill OTO SL qty 8 > entry leaves 6 must fail, got: {err_after_shrink}"
            );

            // Legitimate tightening on linked Ouo/Oco bracket legs within entry leaves (qty=6) MUST be accepted.
            let res_tight_sl = gate.check_command(
                &TradingCommand::ModifyOrder(modify_cmd(&sl, Some("6"), None, Some("147.00"))),
                &cache,
                UnixNanos::from(TS_NOW + 3),
            );
            assert!(
                res_tight_sl.is_ok(),
                "freeze={entry_freeze} {linked_cont:?}: linked SL tightening within entry leaves must succeed: {res_tight_sl:?}"
            );
            let res_tight_tp = gate.check_command(
                &TradingCommand::ModifyOrder(modify_cmd(&tp, Some("6"), Some("158.00"), None)),
                &cache,
                UnixNanos::from(TS_NOW + 4),
            );
            assert!(
                res_tight_tp.is_ok(),
                "freeze={entry_freeze} {linked_cont:?}: linked TP tightening within entry leaves must succeed: {res_tight_tp:?}"
            );
        }
    }
}

#[test]
fn test_iter8_strict_oto_parentage_in_linked_entry_capacity_prefill_and_post_flatten() {
    // 1. Pre-fill OTO SubmitOrderList([E1(BUY 5, Oto, linked=[TP]), E2(BUY 5, None), TP(SELL 5, ro=False, parent=E1, linked=[E2])])
    //    while flat -> ModifyOrder(TP, quantity=10) MUST reject with REDUCE_ONLY_MODIFY_EXCEEDS_POSITION,
    //    counting only genuine OTO parent E1(5), never unlinked E2(5).
    for entry_freeze_after_submit in [false, true] {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
        let list_id = "OL-I8-OTO";
        let e1 = accept_order(make_contingent_limit_order(
            inst.id(),
            "O-I8-E1",
            OrderSide::Buy,
            "5",
            "148.00",
            false,
            Some(ContingencyType::Oto),
            Some(list_id),
            Some(vec!["O-I8-TP"]),
            None,
        ));
        let e2 = accept_order(make_contingent_limit_order(
            inst.id(),
            "O-I8-E2",
            OrderSide::Buy,
            "5",
            "148.00",
            false,
            None,
            Some(list_id),
            None,
            None,
        ));
        let tp = accept_order(with_exit_marker(
            make_contingent_limit_order(
                inst.id(),
                "O-I8-TP",
                OrderSide::Sell,
                "5",
                "160.00",
                false,
                None,
                Some(list_id),
                Some(vec!["O-I8-E2"]),
                Some("O-I8-E1"),
            ),
            Some("TAKE_PROFIT"),
            None,
        ));
        let orders = vec![e1, e2, tp.clone()];
        for o in &orders {
            cache.add_order(o.clone(), None, None, true).unwrap();
        }
        assert!(
            gate.check_command(
                &make_mixed_order_list_cmd(list_id, &orders),
                &cache,
                UnixNanos::from(TS_NOW),
            )
            .is_ok(),
            "freeze_after={entry_freeze_after_submit}: initial SubmitOrderList should be admitted"
        );
        assert_eq!(gate.reserved_qty("AAPL"), dec!(10));

        if entry_freeze_after_submit {
            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
        }

        let err_up = gate
            .check_command(
                &TradingCommand::ModifyOrder(modify_cmd(&tp, Some("10"), Some("159.00"), None)),
                &cache,
                UnixNanos::from(TS_NOW + 1),
            )
            .unwrap_err();
        assert!(
            err_up.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
            "freeze_after={entry_freeze_after_submit}: TP modify to 10 must reject (only genuine OTO parent E1(5) counts, not E2(5)), got: {err_up}"
        );

        let ok_same_qty = gate.check_command(
            &TradingCommand::ModifyOrder(modify_cmd(&tp, Some("5"), Some("159.00"), None)),
            &cache,
            UnixNanos::from(TS_NOW + 2),
        );
        assert!(
            ok_same_qty.is_ok(),
            "freeze_after={entry_freeze_after_submit}: TP modify within genuine OTO parent E1(5) must succeed: {ok_same_qty:?}"
        );
    }

    // 2. Post-flatten spoofed linked_order_ids (and spoofed parent without Oto) under ENTRY_FREEZE=True:
    //    Open LONG 10 with resting E2(BUY 10), submit X1(SELL 10 ro=False, linked=[E2]),
    //    flatten LONG 10 with X2(SELL 10 ro=True), then ModifyOrder(X1) under ENTRY_FREEZE=True MUST reject.
    for spoof_parent_too in [false, true] {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
        open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-I8-FLAT");

        let e2 = accept_order(make_limit_order(
            inst.id(),
            "O-I8-REST-E2",
            OrderSide::Buy,
            "10",
            "148.00",
            false,
        ));
        cache.add_order(e2.clone(), None, None, true).unwrap();
        assert!(
            gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW))
                .is_ok()
        );

        let x1 = accept_order(with_exit_marker(
            make_contingent_limit_order(
                inst.id(),
                "O-I8-X1",
                OrderSide::Sell,
                "10",
                "160.00",
                false,
                None,
                None,
                Some(vec!["O-I8-REST-E2"]),
                if spoof_parent_too {
                    Some("O-I8-REST-E2")
                } else {
                    None
                },
            ),
            Some("TAKE_PROFIT"),
            None,
        ));
        cache.add_order(x1.clone(), None, None, true).unwrap();
        assert!(
            gate.check_command(&submit_cmd(&x1), &cache, UnixNanos::from(TS_NOW + 1))
                .is_ok()
        );

        // Flatten LONG 10 with X2(SELL 10 ro=True).
        let x2 = accept_order(make_market_order(
            inst.id(),
            "O-I8-X2",
            OrderSide::Sell,
            "10",
            true,
        ));
        cache.add_order(x2.clone(), None, None, true).unwrap();
        // Cancel/expire or flatten? Wait: X2 is submitted, then fills and closes position.
        // Note: while X1 is resting, X2(10) + X1(10) > 10 if X1 is ro=False! Wait: X2 is submitted to flatten,
        // or position is flattened by X2 fill. Let's simulate X2 filling directly on the position!
        let close_fill = OrderFilled::new(
            x2.trader_id(),
            x2.strategy_id(),
            inst.id(),
            x2.client_order_id(),
            x2.venue_order_id().unwrap(),
            AccountId::from("ALPACA-001"),
            TradeId::from("T-I8-CLOSE"),
            OrderSide::Sell,
            OrderType::Market,
            Quantity::from("10"),
            Price::from("150.00"),
            Currency::USD(),
            LiquiditySide::Taker,
            UUID4::new(),
            UnixNanos::from(TS_NOW + 2),
            UnixNanos::from(TS_NOW + 2),
            false,
            Some(PositionId::from("P-I8-FLAT")),
            Some(Money::from("0.00 USD")),
            None,
        );
        cache
            .update_order(&OrderEventAny::Filled(close_fill.clone()))
            .unwrap();
        cache
            .update_position_from_fill(PositionId::from("P-I8-FLAT"), &close_fill)
            .unwrap();
        assert!(
            cache
                .positions_open(None, Some(&inst.id()), None, None, None)
                .is_empty()
        );

        gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());

        let err_mod = gate
            .check_command(
                &TradingCommand::ModifyOrder(modify_cmd(&x1, Some("10"), Some("149.90"), None)),
                &cache,
                UnixNanos::from(TS_NOW + 3),
            )
            .unwrap_err();
        assert!(
            err_mod.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION")
                || err_mod.contains("ENTRY_FREEZE_ACTIVE")
                || err_mod.contains("SIDE_MISMATCH"),
            "spoof_parent_too={spoof_parent_too}: post-flatten ModifyOrder(X1) under ENTRY_FREEZE=True must reject (never count non-OTO E2), got: {err_mod}"
        );
    }
}

#[test]
fn test_iter8_reverse_order_ro_false_then_ro_true_exit_cumulative_cap() {
    for (plan_side, pos_side, exit_side, initial_stop, tp_px, tp_px2, sl_px) in [
        ("LONG", OrderSide::Buy, OrderSide::Sell, dec!(145.00), "160.00", "161.00", "146.00"),
        ("SHORT", OrderSide::Sell, OrderSide::Buy, dec!(155.00), "140.00", "139.00", "154.00"),
    ] {
        // 1. Sequential Submit: resting X1(6 ro=False) then X2(10 ro=True) -> DENIED (REDUCE_ONLY_EXIT_EXCEEDS_POSITION).
        {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
            open_position_in_cache(&mut cache, &inst, pos_side, "10", "150.00", "P-I8-REV-1");
            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());

            let x1 = accept_order(with_exit_marker(
                make_limit_order(inst.id(), "O-I8-R1-X1", exit_side, "6", tp_px, false),
                Some("TAKE_PROFIT"),
                None,
            ));
            cache.add_order(x1.clone(), None, None, true).unwrap();
            assert!(
                gate.check_command(&submit_cmd(&x1), &cache, UnixNanos::from(TS_NOW))
                    .is_ok(),
                "{plan_side}: X1(6 ro=False) against 10 must be admitted"
            );

            let x2 = make_limit_order(inst.id(), "O-I8-R1-X2", exit_side, "10", tp_px2, true);
            cache.add_order(x2.clone(), None, None, false).unwrap();
            let err = gate
                .check_command(&submit_cmd(&x2), &cache, UnixNanos::from(TS_NOW + 1))
                .unwrap_err();
            assert!(
                err.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                "{plan_side}: X2(10 ro=True) after resting X1(6 ro=False) must deny with REDUCE_ONLY_EXIT_EXCEEDS_POSITION, got: {err}"
            );
        }

        // 2. Resting X1(6 ro=False) + X2(4 ro=True) against 10, then X3(4 ro=True) -> DENIED (6 + 4 + 4 > 10).
        {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
            open_position_in_cache(&mut cache, &inst, pos_side, "10", "150.00", "P-I8-REV-2");
            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());

            let x1 = accept_order(with_exit_marker(
                make_limit_order(inst.id(), "O-I8-R2-X1", exit_side, "6", tp_px, false),
                Some("TAKE_PROFIT"),
                None,
            ));
            let x2 = accept_order(make_limit_order(
                inst.id(),
                "O-I8-R2-X2",
                exit_side,
                "4",
                tp_px2,
                true,
            ));
            cache.add_order(x1.clone(), None, None, true).unwrap();
            assert!(gate.check_command(&submit_cmd(&x1), &cache, UnixNanos::from(TS_NOW)).is_ok());
            cache.add_order(x2.clone(), None, None, true).unwrap();
            assert!(gate.check_command(&submit_cmd(&x2), &cache, UnixNanos::from(TS_NOW + 1)).is_ok());

            let x3 = make_stop_market_order(inst.id(), "O-I8-R2-X3", exit_side, "4", sl_px, true);
            cache.add_order(x3.clone(), None, None, false).unwrap();
            let err = gate
                .check_command(&submit_cmd(&x3), &cache, UnixNanos::from(TS_NOW + 2))
                .unwrap_err();
            assert!(
                err.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                "{plan_side}: X3(4 ro=True) after resting X1(6 ro=False) + X2(4 ro=True) must deny (6+4+4 > 10), got: {err}"
            );
        }

        // 3. Resting X1(6 ro=False) + X2(4 ro=True) against 10 -> ModifyOrder(X2, quantity=10) -> REJECTED.
        {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
            open_position_in_cache(&mut cache, &inst, pos_side, "10", "150.00", "P-I8-REV-3");
            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());

            let x1 = accept_order(with_exit_marker(
                make_limit_order(inst.id(), "O-I8-R3-X1", exit_side, "6", tp_px, false),
                Some("TAKE_PROFIT"),
                None,
            ));
            let x2 = accept_order(make_limit_order(
                inst.id(),
                "O-I8-R3-X2",
                exit_side,
                "4",
                tp_px2,
                true,
            ));
            cache.add_order(x1.clone(), None, None, true).unwrap();
            assert!(gate.check_command(&submit_cmd(&x1), &cache, UnixNanos::from(TS_NOW)).is_ok());
            cache.add_order(x2.clone(), None, None, true).unwrap();
            assert!(gate.check_command(&submit_cmd(&x2), &cache, UnixNanos::from(TS_NOW + 1)).is_ok());

            let err = gate
                .check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(&x2, Some("10"), None, None)),
                    &cache,
                    UnixNanos::from(TS_NOW + 2),
                )
                .unwrap_err();
            assert!(
                err.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                "{plan_side}: ModifyOrder(X2, qty=10) alongside resting X1(6 ro=False) must reject, got: {err}"
            );
        }

        // 4. SubmitOrderList([X1(6 ro=False), X2(10 ro=True)]) against 10 -> DENIED atomically.
        {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
            open_position_in_cache(&mut cache, &inst, pos_side, "10", "150.00", "P-I8-REV-4");
            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());

            let x1 = with_exit_marker(
                make_limit_order(inst.id(), "O-I8-R4-X1", exit_side, "6", tp_px, false),
                Some("TAKE_PROFIT"),
                None,
            );
            let x2 = make_stop_market_order(inst.id(), "O-I8-R4-X2", exit_side, "10", sl_px, true);
            let orders = vec![x1, x2];
            for o in &orders {
                cache.add_order(o.clone(), None, None, false).unwrap();
            }
            let err = gate
                .check_command(
                    &make_mixed_order_list_cmd(&format!("OL-I8-R4-{plan_side}"), &orders),
                    &cache,
                    UnixNanos::from(TS_NOW),
                )
                .unwrap_err();
            assert!(
                err.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                "{plan_side}: SubmitOrderList([X1(6 ro=False), X2(10 ro=True)]) must deny atomically, got: {err}"
            );
        }

        // 5. Iteration 15 Invariant 2: unlinked SL(10, ro=True) + TP(10, ro=True) (20 > 10)
        //    without safe_exit MUST be denied (REDUCE_ONLY_EXIT_EXCEEDS_POSITION), whereas
        //    mutually OCO/OUO-linked SL(10, ro=True) + TP(10, ro=True) collapse to 10 <= 10 and succeed.
        {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
            open_position_in_cache(&mut cache, &inst, pos_side, "10", "150.00", "P-I8-REV-5");
            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());

            let sl = make_stop_market_order(
                inst.id(),
                "O-I8-R5-SL",
                exit_side,
                "10",
                sl_px,
                true,
            );
            let tp = make_limit_order(
                inst.id(),
                "O-I8-R5-TP",
                exit_side,
                "10",
                tp_px,
                true,
            );
            cache.add_order(sl.clone(), None, None, false).unwrap();
            assert!(
                gate.check_command(&submit_cmd(&sl), &cache, UnixNanos::from(TS_NOW))
                    .is_ok(),
                "{plan_side}: initial ro=True SL(10) must succeed"
            );
            cache.update_order(&OrderEventAny::Submitted(OrderSubmitted::new(
                sl.trader_id(),
                sl.strategy_id(),
                sl.instrument_id(),
                sl.client_order_id(),
                AccountId::from("ALPACA-001"),
                UUID4::new(),
                UnixNanos::from(TS_NOW),
                UnixNanos::from(TS_NOW),
            ))).unwrap();
            cache.update_order(&OrderEventAny::Accepted(OrderAccepted::new(
                sl.trader_id(),
                sl.strategy_id(),
                sl.instrument_id(),
                sl.client_order_id(),
                VenueOrderId::from("O-I8-R5-SL"),
                AccountId::from("ALPACA-001"),
                UUID4::new(),
                UnixNanos::from(TS_NOW),
                UnixNanos::from(TS_NOW),
                false,
            ))).unwrap();

            cache.add_order(tp.clone(), None, None, false).unwrap();
            let err = gate
                .check_command(&submit_cmd(&tp), &cache, UnixNanos::from(TS_NOW + 1))
                .unwrap_err();
            assert!(
                err.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                "{plan_side}: unlinked ro=True TP(10) alongside unlinked ro=True SL(10) must deny (20 > 10), got: {err}"
            );
        }
        for exit_cont in [ContingencyType::Oco, ContingencyType::Ouo] {
            let (inst, mut cache, gate) = setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
            open_position_in_cache(&mut cache, &inst, pos_side, "10", "150.00", "P-I8-REV-5-LNK");
            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());

            let list_id = format!("OL-I8-R5-{plan_side}-{exit_cont:?}");
            let sl_cid = format!("O-I8-R5-LNK-SL-{exit_cont:?}");
            let tp_cid = format!("O-I8-R5-LNK-TP-{exit_cont:?}");
            let sl = make_contingent_stop_market_order(
                inst.id(),
                &sl_cid,
                exit_side,
                "10",
                sl_px,
                true,
                Some(exit_cont),
                Some(&list_id),
                Some(vec![&tp_cid]),
                None,
            );
            let tp = make_contingent_limit_order(
                inst.id(),
                &tp_cid,
                exit_side,
                "10",
                tp_px,
                true,
                Some(exit_cont),
                Some(&list_id),
                Some(vec![&sl_cid]),
                None,
            );
            let orders = vec![sl, tp];
            for o in &orders {
                cache.add_order(o.clone(), None, None, false).unwrap();
            }
            let res = gate.check_command(
                &make_mixed_order_list_cmd(&list_id, &orders),
                &cache,
                UnixNanos::from(TS_NOW + 2),
            );
            assert!(
                res.is_ok(),
                "{plan_side}: {exit_cont:?}-linked ro=True SL(10) + TP(10) must collapse to 10 <= 10 and succeed: {res:?}"
            );
        }
    }
}

#[test]
fn test_iter8_partial_fill_reduce_only_modify_uses_remaining_new_leaves() {
    for entry_freeze in [false, true] {
        for (plan_side, pos_side, exit_side, initial_stop, lim_init, lim_mod1, lim_mod2, sl_tight) in [
            ("LONG", OrderSide::Buy, OrderSide::Sell, dec!(145.00), "160.00", "159.00", "158.50", "147.00"),
            ("SHORT", OrderSide::Sell, OrderSide::Buy, dec!(155.00), "140.00", "141.00", "141.50", "153.00"),
        ] {
            // Part A: Partially filled Limit(10, ro=True) -> filled=3, leaves=7, open_pos_qty=7.
            {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                let pos_id = format!("P-I8-PF-{plan_side}-{entry_freeze}");
                open_position_in_cache(&mut cache, &inst, pos_side, "10", "150.00", &pos_id);

                let exit_lim = accept_order(make_limit_order(
                    inst.id(),
                    &format!("O-I8-PF-LIM-{plan_side}"),
                    exit_side,
                    "10",
                    lim_init,
                    true,
                ));
                cache.add_order(exit_lim.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(&submit_cmd(&exit_lim), &cache, UnixNanos::from(TS_NOW))
                        .is_ok()
                );

                // Partial fill of 3 shares reduces open position 10 -> 7 and order leaves 10 -> 7.
                let partial_fill = OrderFilled::new(
                    exit_lim.trader_id(),
                    exit_lim.strategy_id(),
                    inst.id(),
                    exit_lim.client_order_id(),
                    exit_lim.venue_order_id().unwrap(),
                    AccountId::from("ALPACA-001"),
                    TradeId::from(format!("T-I8-PF-{plan_side}-{entry_freeze}").as_str()),
                    exit_side,
                    OrderType::Limit,
                    Quantity::from("3"),
                    Price::from(lim_init),
                    Currency::USD(),
                    LiquiditySide::Maker,
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 1),
                    UnixNanos::from(TS_NOW + 1),
                    false,
                    Some(PositionId::from(pos_id.as_str())),
                    Some(Money::from("0.00 USD")),
                    None,
                );
                cache
                    .update_order(&OrderEventAny::Filled(partial_fill.clone()))
                    .unwrap();
                cache
                    .update_position_from_fill(PositionId::from(pos_id.as_str()), &partial_fill)
                    .unwrap();

                if entry_freeze {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                // Price-only modify (quantity=None) -> new_leaves=7 <= open_pos_qty=7 -> MUST succeed.
                let res_price_only = gate.check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(
                        &exit_lim,
                        None,
                        Some(lim_mod1),
                        None,
                    )),
                    &cache,
                    UnixNanos::from(TS_NOW + 2),
                );
                assert!(
                    res_price_only.is_ok(),
                    "{plan_side} freeze={entry_freeze}: price-only ModifyOrder on partially-filled ro=True order (leaves=7, pos=7) must succeed: {res_price_only:?}"
                );

                // Explicit quantity=10 -> new_leaves = 10 - 3 = 7 <= 7 -> MUST succeed.
                let res_same_total = gate.check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(
                        &exit_lim,
                        Some("10"),
                        Some(lim_mod2),
                        None,
                    )),
                    &cache,
                    UnixNanos::from(TS_NOW + 3),
                );
                assert!(
                    res_same_total.is_ok(),
                    "{plan_side} freeze={entry_freeze}: ModifyOrder(qty=10) on partially-filled ro=True order (new_leaves=7, pos=7) must succeed: {res_same_total:?}"
                );

                // quantity=11 -> new_leaves = 11 - 3 = 8 > 7 -> MUST reject with REDUCE_ONLY_MODIFY_EXCEEDS_POSITION.
                let err_exceeds = gate
                    .check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(
                            &exit_lim,
                            Some("11"),
                            Some(lim_mod2),
                            None,
                        )),
                        &cache,
                        UnixNanos::from(TS_NOW + 4),
                    )
                    .unwrap_err();
                assert!(
                    err_exceeds.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                    "{plan_side} freeze={entry_freeze}: ModifyOrder(qty=11) (new_leaves=8 > pos=7) must reject, got: {err_exceeds}"
                );
            }

            // Part B: Partially filled StopMarket(10, ro=True) stop-tightening (qty=None / qty=10) succeeds, qty=11 rejects.
            {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                let pos_id = format!("P-I8-PF-SL-{plan_side}-{entry_freeze}");
                open_position_in_cache(&mut cache, &inst, pos_side, "10", "150.00", &pos_id);

                let sl_px = initial_stop.to_string();
                let sl = accept_order(make_stop_market_order(
                    inst.id(),
                    &format!("O-I8-PF-SL-{plan_side}"),
                    exit_side,
                    "10",
                    &sl_px,
                    true,
                ));
                cache.add_order(sl.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(&submit_cmd(&sl), &cache, UnixNanos::from(TS_NOW))
                        .is_ok()
                );

                let partial_fill = OrderFilled::new(
                    sl.trader_id(),
                    sl.strategy_id(),
                    inst.id(),
                    sl.client_order_id(),
                    sl.venue_order_id().unwrap(),
                    AccountId::from("ALPACA-001"),
                    TradeId::from(format!("T-I8-PF-SL-{plan_side}-{entry_freeze}").as_str()),
                    exit_side,
                    OrderType::StopMarket,
                    Quantity::from("3"),
                    Price::from(sl_px.as_str()),
                    Currency::USD(),
                    LiquiditySide::Taker,
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 1),
                    UnixNanos::from(TS_NOW + 1),
                    false,
                    Some(PositionId::from(pos_id.as_str())),
                    Some(Money::from("0.00 USD")),
                    None,
                );
                cache
                    .update_order(&OrderEventAny::Filled(partial_fill.clone()))
                    .unwrap();
                cache
                    .update_position_from_fill(PositionId::from(pos_id.as_str()), &partial_fill)
                    .unwrap();

                if entry_freeze {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                assert!(
                    gate.check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(&sl, None, None, Some(sl_tight))),
                        &cache,
                        UnixNanos::from(TS_NOW + 2),
                    )
                    .is_ok(),
                    "{plan_side} freeze={entry_freeze}: stop-tightening with qty=None on partially-filled StopMarket must succeed"
                );
                assert!(
                    gate.check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(&sl, Some("10"), None, Some(sl_tight))),
                        &cache,
                        UnixNanos::from(TS_NOW + 3),
                    )
                    .is_ok(),
                    "{plan_side} freeze={entry_freeze}: stop-tightening with qty=10 (new_leaves=7) must succeed"
                );
                let err_sl_over = gate
                    .check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(&sl, Some("11"), None, Some(sl_tight))),
                        &cache,
                        UnixNanos::from(TS_NOW + 4),
                    )
                    .unwrap_err();
                assert!(
                    err_sl_over.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                    "{plan_side} freeze={entry_freeze}: StopMarket qty=11 (new_leaves=8 > 7) must reject, got: {err_sl_over}"
                );
            }
        }
    }
}

#[test]
fn test_iter8_release_order_reservation_symbol_mismatch_guard() {
    let inst_aapl = make_equity("AAPL");
    let inst_msft = make_equity("MSFT");
    let mut cache = Cache::default();
    cache.add_instrument(inst_aapl.clone()).unwrap();
    cache.add_instrument(inst_msft.clone()).unwrap();
    cache
        .add_quote(make_quote(inst_aapl.id(), "149.95", "150.00"))
        .unwrap();
    cache
        .add_quote(make_quote(inst_msft.id(), "299.95", "300.00"))
        .unwrap();

    let e_aapl = make_limit_order(
        inst_aapl.id(),
        "E-AAPL",
        OrderSide::Buy,
        "10",
        "150.00",
        false,
    );
    let e_msft = make_limit_order(
        inst_msft.id(),
        "E-MSFT",
        OrderSide::Buy,
        "10",
        "300.00",
        false,
    );
    cache.add_order(e_aapl.clone(), None, None, false).unwrap();
    cache.add_order(e_msft.clone(), None, None, false).unwrap();

    let gate = RustPlanGateEngine::new(false, String::new());
    gate.upsert_plan_item(
        make_plan_item("AAPL", "LONG", dec!(50), dec!(145.00), TS_FUTURE),
        false,
    );
    gate.upsert_plan_item(
        make_plan_item("MSFT", "LONG", dec!(50), dec!(290.00), TS_FUTURE),
        false,
    );

    assert!(
        gate.check_command(&submit_cmd(&e_aapl), &cache, UnixNanos::from(TS_NOW))
            .is_ok()
    );
    assert!(
        gate.check_command(&submit_cmd(&e_msft), &cache, UnixNanos::from(TS_NOW))
            .is_ok()
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(10));
    assert_eq!(gate.reserved_qty("MSFT"), dec!(10));
    assert!(gate.has_admitted_reservation("E-AAPL"));
    assert!(gate.has_admitted_reservation("E-MSFT"));

    // Mismatched symbol ("MSFT" or "NVDA") MUST be a no-op.
    let _ = gate.release_order_reservation("E-AAPL", "MSFT", dec!(10));
    assert_eq!(gate.reserved_qty("AAPL"), dec!(10));
    assert_eq!(gate.reserved_qty("MSFT"), dec!(10));
    assert!(gate.has_admitted_reservation("E-AAPL"));
    assert!(gate.has_admitted_reservation("E-MSFT"));

    let _ = gate.release_order_reservation("E-AAPL", "NVDA", dec!(10));
    assert_eq!(gate.reserved_qty("AAPL"), dec!(10));
    assert_eq!(gate.reserved_qty("MSFT"), dec!(10));
    assert!(gate.has_admitted_reservation("E-AAPL"));

    // Empty symbol "" releases from tracked symbol (AAPL).
    assert_eq!(
        gate.release_order_reservation("E-AAPL", "", dec!(4)),
        dec!(6)
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(6));
    assert_eq!(gate.reserved_qty("MSFT"), dec!(10));
    assert!(gate.has_admitted_reservation("E-AAPL"));

    // Case-insensitive + whitespace-trimmed matching symbol " aapl " releases remaining 6 from AAPL.
    assert_eq!(
        gate.release_order_reservation("E-AAPL", " aapl ", dec!(6)),
        dec!(0)
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
    assert_eq!(gate.reserved_qty("MSFT"), dec!(10));
    assert!(!gate.has_admitted_reservation("E-AAPL"));
    assert!(gate.has_admitted_reservation("E-MSFT"));
}

fn make_emulated_stop_market_order(
    inst_id: InstrumentId,
    cid: &str,
    side: OrderSide,
    qty: &str,
    trigger_price: &str,
    reduce_only: bool,
    emulation_trigger: TriggerType,
) -> OrderAny {
    OrderAny::StopMarket(StopMarketOrder::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst_id,
        ClientOrderId::from(cid),
        side,
        Quantity::from(qty),
        Price::from(trigger_price),
        TriggerType::LastPrice,
        TimeInForce::Gtc,
        None,
        reduce_only,
        false,
        None,
        Some(emulation_trigger),
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
        UnixNanos::from(TS_NOW),
    ))
}

#[test]
fn test_iter9_batch_modify_orders_cumulative_unlinked_exit_cap() {
    for entry_freeze in [false, true] {
        for (plan_side, pos_side, exit_side, initial_stop, px_init, px_1, px_2, px_ok) in [
            (
                "LONG",
                OrderSide::Buy,
                OrderSide::Sell,
                dec!(145.00),
                "160.00",
                "154.00",
                "153.00",
                "155.00",
            ),
            (
                "SHORT",
                OrderSide::Sell,
                OrderSide::Buy,
                dec!(155.00),
                "140.00",
                "146.00",
                "147.00",
                "145.00",
            ),
        ] {
            // Case A: Open 10 + unlinked x3(2, ro=False) + x1(2, ro=True) + x2(2, ro=True).
            // Duplicate-CID BatchModifyOrders [(x1, qty=6), (x1, qty=None, px_2), (x2, qty=6)]
            // -> post-batch cumulative unlinked exit = 6 + 6 + 2 = 14 > 10 -> REJECTED.
            // BatchModifyOrders [(x1, qty=6), (x1, qty=None, px_2), (x2, qty=2)]
            // -> post-batch cumulative unlinked exit = 6 + 2 + 2 = 10 <= 10 -> ADMITTED.
            {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                open_position_in_cache(
                    &mut cache,
                    &inst,
                    pos_side,
                    "10",
                    "150.00",
                    &format!("P-I9-BM-A-{plan_side}"),
                );
                if entry_freeze {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                let x3 = accept_order(with_exit_marker(
                    make_limit_order(inst.id(), "O-I9-A-X3", exit_side, "2", px_init, false),
                    Some("TAKE_PROFIT"),
                    None,
                ));
                let x1 = accept_order(make_limit_order(
                    inst.id(),
                    "O-I9-A-X1",
                    exit_side,
                    "2",
                    px_init,
                    true,
                ));
                let x2 = accept_order(make_limit_order(
                    inst.id(),
                    "O-I9-A-X2",
                    exit_side,
                    "2",
                    px_init,
                    true,
                ));
                for o in [&x3, &x1, &x2] {
                    cache.add_order(o.clone(), None, None, true).unwrap();
                    assert!(
                        gate.check_command(&submit_cmd(o), &cache, UnixNanos::from(TS_NOW))
                            .is_ok()
                    );
                }

                let over_batch = BatchModifyOrders::new(
                    x1.trader_id(),
                    Some(ClientId::from("ALPACA")),
                    x1.strategy_id(),
                    inst.id(),
                    vec![
                        modify_cmd(&x1, Some("6"), Some(px_1), None),
                        modify_cmd(&x1, None, Some(px_2), None),
                        modify_cmd(&x2, Some("6"), Some(px_ok), None),
                    ],
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 1),
                    None,
                    None,
                );
                let err = gate
                    .check_command(
                        &TradingCommand::ModifyOrders(over_batch),
                        &cache,
                        UnixNanos::from(TS_NOW + 1),
                    )
                    .unwrap_err();
                assert!(
                    err.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                    "{plan_side} freeze={entry_freeze}: duplicate-CID ModifyOrders (6+6+2=14 > 10) must reject, got: {err}"
                );

                let ok_batch = BatchModifyOrders::new(
                    x1.trader_id(),
                    Some(ClientId::from("ALPACA")),
                    x1.strategy_id(),
                    inst.id(),
                    vec![
                        modify_cmd(&x1, Some("6"), Some(px_1), None),
                        modify_cmd(&x1, None, Some(px_2), None),
                        modify_cmd(&x2, Some("2"), Some(px_ok), None),
                    ],
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 2),
                    None,
                    None,
                );
                let res_ok = gate.check_command(
                    &TradingCommand::ModifyOrders(ok_batch),
                    &cache,
                    UnixNanos::from(TS_NOW + 2),
                );
                assert!(
                    res_ok.is_ok(),
                    "{plan_side} freeze={entry_freeze}: duplicate-CID ModifyOrders (6+2+2=10 <= 10) must succeed: {res_ok:?}"
                );
            }

            // Case B: X1(6, ro=False) + x2(2, ro=True) + x3(2, ro=True) -> batch [(x2, 3), (x3, 3)]
            // -> 6 + 3 + 3 = 12 > 10 -> REJECTED.
            {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                open_position_in_cache(
                    &mut cache,
                    &inst,
                    pos_side,
                    "10",
                    "150.00",
                    &format!("P-I9-BM-B-{plan_side}"),
                );
                if entry_freeze {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                let x1 = accept_order(with_exit_marker(
                    make_limit_order(inst.id(), "O-I9-B-X1", exit_side, "6", px_init, false),
                    Some("EXIT"),
                    None,
                ));
                let x2 = accept_order(make_limit_order(
                    inst.id(),
                    "O-I9-B-X2",
                    exit_side,
                    "2",
                    px_init,
                    true,
                ));
                let x3 = accept_order(make_limit_order(
                    inst.id(),
                    "O-I9-B-X3",
                    exit_side,
                    "2",
                    px_init,
                    true,
                ));
                for o in [&x1, &x2, &x3] {
                    cache.add_order(o.clone(), None, None, true).unwrap();
                    assert!(
                        gate.check_command(&submit_cmd(o), &cache, UnixNanos::from(TS_NOW))
                            .is_ok()
                    );
                }

                let batch = BatchModifyOrders::new(
                    x2.trader_id(),
                    Some(ClientId::from("ALPACA")),
                    x2.strategy_id(),
                    inst.id(),
                    vec![
                        modify_cmd(&x2, Some("3"), Some(px_1), None),
                        modify_cmd(&x3, Some("3"), Some(px_ok), None),
                    ],
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 1),
                    None,
                    None,
                );
                let err = gate
                    .check_command(
                        &TradingCommand::ModifyOrders(batch),
                        &cache,
                        UnixNanos::from(TS_NOW + 1),
                    )
                    .unwrap_err();
                assert!(
                    err.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                    "{plan_side} freeze={entry_freeze}: X1(6 ro=False) + batch [(x2,3), (x3,3)] = 12 > 10 must reject, got: {err}"
                );
            }

            // Case C: Partial-fill leaves_qty preservation in ModifyOrders (qty=None).
            {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                let pos_id = format!("P-I9-BM-C-{plan_side}-{entry_freeze}");
                open_position_in_cache(&mut cache, &inst, pos_side, "10", "150.00", &pos_id);

                let x3 = accept_order(with_exit_marker(
                    make_limit_order(inst.id(), "O-I9-C-X3", exit_side, "2", px_init, false),
                    Some("TAKE_PROFIT"),
                    None,
                ));
                let x1 = accept_order(make_limit_order(
                    inst.id(),
                    "O-I9-C-X1",
                    exit_side,
                    "4",
                    px_init,
                    true,
                ));
                let x2 = accept_order(make_limit_order(
                    inst.id(),
                    "O-I9-C-X2",
                    exit_side,
                    "2",
                    px_init,
                    true,
                ));
                for o in [&x3, &x1, &x2] {
                    cache.add_order(o.clone(), None, None, true).unwrap();
                    assert!(
                        gate.check_command(&submit_cmd(o), &cache, UnixNanos::from(TS_NOW))
                            .is_ok()
                    );
                }

                // Partial fill 1 share on x1 -> open_pos=9, x1.leaves=3.
                let pf = OrderFilled::new(
                    x1.trader_id(),
                    x1.strategy_id(),
                    inst.id(),
                    x1.client_order_id(),
                    x1.venue_order_id().unwrap(),
                    AccountId::from("ALPACA-001"),
                    TradeId::from(format!("T-I9-BM-C-{plan_side}-{entry_freeze}").as_str()),
                    exit_side,
                    OrderType::Limit,
                    Quantity::from("1"),
                    Price::from(px_init),
                    Currency::USD(),
                    LiquiditySide::Maker,
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 1),
                    UnixNanos::from(TS_NOW + 1),
                    false,
                    Some(PositionId::from(pos_id.as_str())),
                    Some(Money::from("0.00 USD")),
                    None,
                );
                cache.update_order(&OrderEventAny::Filled(pf.clone())).unwrap();
                cache
                    .update_position_from_fill(PositionId::from(pos_id.as_str()), &pf)
                    .unwrap();

                if entry_freeze {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                // x1(qty=None -> leaves=3) + x2(qty=5) + x3(2) = 10 > 9 -> REJECTED.
                let over_pf = BatchModifyOrders::new(
                    x1.trader_id(),
                    Some(ClientId::from("ALPACA")),
                    x1.strategy_id(),
                    inst.id(),
                    vec![
                        modify_cmd(&x1, None, Some(px_1), None),
                        modify_cmd(&x2, Some("5"), Some(px_2), None),
                    ],
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 2),
                    None,
                    None,
                );
                let err_pf = gate
                    .check_command(
                        &TradingCommand::ModifyOrders(over_pf),
                        &cache,
                        UnixNanos::from(TS_NOW + 2),
                    )
                    .unwrap_err();
                assert!(
                    err_pf.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                    "{plan_side} freeze={entry_freeze}: partial-fill ModifyOrders (3+5+2=10 > 9) must reject, got: {err_pf}"
                );

                // x1(qty=None -> leaves=3) + x2(qty=4) + x3(2) = 9 <= 9 -> ADMITTED.
                let ok_pf = BatchModifyOrders::new(
                    x1.trader_id(),
                    Some(ClientId::from("ALPACA")),
                    x1.strategy_id(),
                    inst.id(),
                    vec![
                        modify_cmd(&x1, None, Some(px_1), None),
                        modify_cmd(&x2, Some("4"), Some(px_2), None),
                    ],
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 3),
                    None,
                    None,
                );
                assert!(
                    gate.check_command(
                        &TradingCommand::ModifyOrders(ok_pf),
                        &cache,
                        UnixNanos::from(TS_NOW + 3),
                    )
                    .is_ok()
                );
            }
        }
    }

    // Non-reduce_only entry in ModifyOrders continues to reject with ENTRY_UPWARD_MODIFY_PROHIBITED.
    {
        let (inst, mut cache, gate) = setup_aapl_cache_and_gate("LONG", dec!(50), dec!(145.00));
        open_position_in_cache(&mut cache, &inst, OrderSide::Buy, "10", "150.00", "P-I9-NRO");
        let entry = accept_order(make_limit_order(
            inst.id(),
            "O-I9-ENT",
            OrderSide::Buy,
            "10",
            "148.00",
            false,
        ));
        let exit_ro = accept_order(make_limit_order(
            inst.id(),
            "O-I9-RO",
            OrderSide::Sell,
            "5",
            "160.00",
            true,
        ));
        cache.add_order(entry.clone(), None, None, true).unwrap();
        cache.add_order(exit_ro.clone(), None, None, true).unwrap();

        let batch = BatchModifyOrders::new(
            entry.trader_id(),
            Some(ClientId::from("ALPACA")),
            entry.strategy_id(),
            inst.id(),
            vec![
                modify_cmd(&exit_ro, Some("5"), Some("159.00"), None),
                modify_cmd(&entry, Some("5"), Some("148.50"), None),
            ],
            UUID4::new(),
            UnixNanos::from(TS_NOW + 1),
            None,
            None,
        );
        let err = gate
            .check_command(
                &TradingCommand::ModifyOrders(batch),
                &cache,
                UnixNanos::from(TS_NOW + 1),
            )
            .unwrap_err();
        assert!(
            err.contains("ENTRY_UPWARD_MODIFY_PROHIBITED"),
            "non-reduce_only entry in ModifyOrders must reject with ENTRY_UPWARD_MODIFY_PROHIBITED, got: {err}"
        );
    }
}

#[test]
fn test_iter9_emulated_and_released_exit_visibility() {
    for entry_freeze in [false, true] {
        for (plan_side, pos_side, exit_side, initial_stop, sl_px, tp_px) in [
            ("LONG", OrderSide::Buy, OrderSide::Sell, dec!(145.00), "146.00", "160.00"),
            ("SHORT", OrderSide::Sell, OrderSide::Buy, dec!(155.00), "154.00", "140.00"),
        ] {
            let (inst, mut cache, gate) =
                setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
            open_position_in_cache(
                &mut cache,
                &inst,
                pos_side,
                "10",
                "150.00",
                &format!("P-I9-EMUL-{plan_side}"),
            );
            if entry_freeze {
                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            }

            let mut emul = with_exit_marker(
                make_emulated_stop_market_order(
                    inst.id(),
                    "O-I9-EMUL-SL",
                    exit_side,
                    "6",
                    sl_px,
                    false,
                    TriggerType::BidAsk,
                ),
                Some("EXIT"),
                None,
            );
            let emulated_ev = OrderEmulated::new(
                emul.trader_id(),
                emul.strategy_id(),
                emul.instrument_id(),
                emul.client_order_id(),
                UUID4::new(),
                UnixNanos::from(TS_NOW + 1),
                UnixNanos::from(TS_NOW + 1),
            );
            emul.apply(OrderEventAny::Emulated(emulated_ev)).unwrap();
            assert_eq!(emul.status(), OrderStatus::Emulated);
            assert!(!emul.is_open(), "Emulated order has is_open() == false");
            cache.add_order(emul.clone(), None, None, true).unwrap();

            // Emulated SELL 6 + Limit SELL 10 (ro=True) = 16 > 10 -> DENIED (use separate OrderDenied transition or fresh order).
            let ro_10 = make_limit_order(inst.id(), "O-I9-EMUL-L10", exit_side, "10", tp_px, true);
            cache.add_order(ro_10.clone(), None, None, false).unwrap();
            let err_10 = gate
                .check_command(&submit_cmd(&ro_10), &cache, UnixNanos::from(TS_NOW + 2))
                .unwrap_err();
            assert!(
                err_10.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                "{plan_side} freeze={entry_freeze}: Emulated 6 + ro=True 10 > 10 must deny, got: {err_10}"
            );
            // RiskEngine::deny_command marks denied order as Denied (closed) in Cache.
            let denied_10 = nautilus_model::events::OrderDenied::new(
                ro_10.trader_id(),
                ro_10.strategy_id(),
                ro_10.instrument_id(),
                ro_10.client_order_id(),
                err_10.into(),
                UUID4::new(),
                UnixNanos::from(TS_NOW + 2),
                UnixNanos::from(TS_NOW + 2),
            );
            cache.update_order(&OrderEventAny::Denied(denied_10)).unwrap();

            // Emulated SELL 6 + Limit SELL 4 (ro=True) = 10 <= 10 -> ADMITTED.
            let ro_4 = accept_order(make_limit_order(
                inst.id(),
                "O-I9-EMUL-L4",
                exit_side,
                "4",
                tp_px,
                true,
            ));
            cache.add_order(ro_4.clone(), None, None, true).unwrap();
            let ok_4 = gate.check_command(&submit_cmd(&ro_4), &cache, UnixNanos::from(TS_NOW + 3));
            assert!(
                ok_4.is_ok(),
                "{plan_side} freeze={entry_freeze}: Emulated 6 + ro=True 4 = 10 <= 10 must be admitted: {ok_4:?}"
            );

            // Transition emulated order to OrderStatus::Released and test visibility & self-submission.
            let released_ev = OrderReleased::new(
                emul.trader_id(),
                emul.strategy_id(),
                emul.instrument_id(),
                emul.client_order_id(),
                Price::from(sl_px),
                UUID4::new(),
                UnixNanos::from(TS_NOW + 4),
                UnixNanos::from(TS_NOW + 4),
            );
            emul.apply(OrderEventAny::Released(released_ev.clone())).unwrap();
            assert_eq!(emul.status(), OrderStatus::Released);
            assert!(!emul.is_open(), "Released order still has is_open() == false until submitted");
            cache.update_order(&OrderEventAny::Released(released_ev)).unwrap();

            // Released 6 + ro_4(4) + another ro_1(1) = 11 > 10 -> DENIED.
            let ro_1 = make_limit_order(inst.id(), "O-I9-REL-L1", exit_side, "1", tp_px, true);
            cache.add_order(ro_1.clone(), None, None, false).unwrap();
            let err_rel = gate
                .check_command(&submit_cmd(&ro_1), &cache, UnixNanos::from(TS_NOW + 5))
                .unwrap_err();
            assert!(
                err_rel.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                "{plan_side} freeze={entry_freeze}: Released 6 + ro=True 4 + ro=True 1 = 11 > 10 must deny, got: {err_rel}"
            );
            let denied_1 = nautilus_model::events::OrderDenied::new(
                ro_1.trader_id(),
                ro_1.strategy_id(),
                ro_1.instrument_id(),
                ro_1.client_order_id(),
                err_rel.into(),
                UUID4::new(),
                UnixNanos::from(TS_NOW + 5),
                UnixNanos::from(TS_NOW + 5),
            );
            cache.update_order(&OrderEventAny::Denied(denied_1)).unwrap();

            // Releasing SubmitOrder for the Released order itself alongside ro_4(4) (6 + 4 = 10 <= 10)
            // MUST succeed without double-counting itself or denying with SIDE_MISMATCH.
            let res_self =
                gate.check_command(&submit_cmd(&emul), &cache, UnixNanos::from(TS_NOW + 6));
            assert!(
                res_self.is_ok(),
                "{plan_side} freeze={entry_freeze}: SubmitOrder for Released exit itself (6 + 4 = 10) must succeed: {res_self:?}"
            );
        }
    }
}

#[test]
fn test_iter9_two_separate_otos_pre_fill_modify_order_isolation() {
    for entry_freeze_after_submit in [false, true] {
        for (plan_side, entry_side, exit_side, initial_stop, px_e, px_x1, px_x1_mod, px_x2, px_x2_mod, sl_px2) in [
            (
                "LONG",
                OrderSide::Buy,
                OrderSide::Sell,
                dec!(145.00),
                "148.00",
                "160.00",
                "159.00",
                "162.00",
                "158.00",
                "146.00",
            ),
            (
                "SHORT",
                OrderSide::Sell,
                OrderSide::Buy,
                dec!(155.00),
                "152.00",
                "140.00",
                "141.00",
                "138.00",
                "142.00",
                "154.00",
            ),
        ] {
            // Shape A: [e1(10, Oto) -> x1(10, ro=True, parent=e1)] + [e2(15, Oto) -> x2(5, ro=False, parent=e2)]
            {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);

                let e1 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I9-OTO1-E1",
                    entry_side,
                    "10",
                    px_e,
                    false,
                    Some(ContingencyType::Oto),
                    Some("OL-I9-1"),
                    Some(vec!["O-I9-OTO1-X1"]),
                    None,
                ));
                let x1 = make_contingent_limit_order(
                    inst.id(),
                    "O-I9-OTO1-X1",
                    exit_side,
                    "10",
                    px_x1,
                    true,
                    None,
                    Some("OL-I9-1"),
                    None,
                    Some("O-I9-OTO1-E1"),
                );
                let e2 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I9-OTO2-E2",
                    entry_side,
                    "15",
                    px_e,
                    false,
                    Some(ContingencyType::Oto),
                    Some("OL-I9-2"),
                    Some(vec!["O-I9-OTO2-X2"]),
                    None,
                ));
                let x2 = with_exit_marker(
                    make_contingent_limit_order(
                        inst.id(),
                        "O-I9-OTO2-X2",
                        exit_side,
                        "5",
                        px_x2,
                        false,
                        None,
                        Some("OL-I9-2"),
                        None,
                        Some("O-I9-OTO2-E2"),
                    ),
                    Some("TAKE_PROFIT"),
                    None,
                );

                for o in [&e1, &x1] {
                    cache.add_order(o.clone(), None, None, true).unwrap();
                }
                let res_ol1 = gate.check_command(
                    &make_mixed_order_list_cmd("OL-I9-1", &[e1.clone(), x1.clone()]),
                    &cache,
                    UnixNanos::from(TS_NOW),
                );
                assert!(res_ol1.is_ok(), "OL-I9-1 failed: {res_ol1:?}");

                for o in [&e2, &x2] {
                    cache.add_order(o.clone(), None, None, true).unwrap();
                }
                let res_ol2 = gate.check_command(
                    &make_mixed_order_list_cmd("OL-I9-2", &[e2.clone(), x2.clone()]),
                    &cache,
                    UnixNanos::from(TS_NOW + 1),
                );
                assert!(res_ol2.is_ok(), "OL-I9-2 failed: {res_ol2:?}");
                assert_eq!(gate.reserved_qty("AAPL"), dec!(25));

                if entry_freeze_after_submit {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                // Price-only ModifyOrder(x1) MUST succeed (must NOT be rejected by x2 backed by e2).
                let ok_price_only = gate.check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(&x1, None, Some(px_x1_mod), None)),
                    &cache,
                    UnixNanos::from(TS_NOW + 2),
                );
                assert!(
                    ok_price_only.is_ok(),
                    "{plan_side} freeze={entry_freeze_after_submit}: TwoSeparateOtos price-only ModifyOrder(x1) must be admitted: {ok_price_only:?}"
                );

                // Upward ModifyOrder(x1, qty=12) exceeds e1(10) -> MUST reject.
                let err_up = gate
                    .check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(
                            &x1,
                            Some("12"),
                            Some(px_x1_mod),
                            None,
                        )),
                        &cache,
                        UnixNanos::from(TS_NOW + 3),
                    )
                    .unwrap_err();
                assert!(
                    err_up.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                    "{plan_side} freeze={entry_freeze_after_submit}: ModifyOrder(x1, qty=12 > e1=10) must reject, got: {err_up}"
                );

                // Price-only and same-qty ModifyOrder(x2) MUST be admitted; any upward qty
                // modification on pre-fill OTO child x2(qty=5) (even <= parent e2=15) MUST reject.
                let ok_x2_price_only = gate.check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(&x2, None, Some(px_x2_mod), None)),
                    &cache,
                    UnixNanos::from(TS_NOW + 4),
                );
                assert!(
                    ok_x2_price_only.is_ok(),
                    "{plan_side} freeze={entry_freeze_after_submit}: TwoSeparateOtos price-only ModifyOrder(x2) must be admitted: {ok_x2_price_only:?}"
                );
                let ok_x2_same_qty = gate.check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(
                        &x2,
                        Some("5"),
                        Some(px_x2_mod),
                        None,
                    )),
                    &cache,
                    UnixNanos::from(TS_NOW + 5),
                );
                assert!(
                    ok_x2_same_qty.is_ok(),
                    "{plan_side} freeze={entry_freeze_after_submit}: TwoSeparateOtos same-qty ModifyOrder(x2, qty=5) must be admitted: {ok_x2_same_qty:?}"
                );
                let err_x2_up = gate
                    .check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(
                            &x2,
                            Some("15"),
                            Some(px_x2_mod),
                            None,
                        )),
                        &cache,
                        UnixNanos::from(TS_NOW + 6),
                    )
                    .unwrap_err();
                assert!(
                    err_x2_up.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                    "{plan_side} freeze={entry_freeze_after_submit}: ModifyOrder(x2, qty=15 > x2=5) must reject with REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, got: {err_x2_up}"
                );
                let err_x2_over = gate
                    .check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(
                            &x2,
                            Some("16"),
                            Some(px_x2_mod),
                            None,
                        )),
                        &cache,
                        UnixNanos::from(TS_NOW + 7),
                    )
                    .unwrap_err();
                assert!(
                    err_x2_over.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                    "{plan_side} freeze={entry_freeze_after_submit}: ModifyOrder(x2, qty=16 > x2=5) must reject with REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, got: {err_x2_over}"
                );
            }

            // Shape B: [e1(10, Oto) -> x1(10, ro=True, parent=e1)] + [e2(15, Oto) -> tp2(15, ro=False, Oco), sl2(15, ro=True, Oco)]
            {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);

                let e1 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I9-SHB-E1",
                    entry_side,
                    "10",
                    px_e,
                    false,
                    Some(ContingencyType::Oto),
                    Some("OL-I9-SHB1"),
                    Some(vec!["O-I9-SHB-X1"]),
                    None,
                ));
                let x1 = make_contingent_limit_order(
                    inst.id(),
                    "O-I9-SHB-X1",
                    exit_side,
                    "10",
                    px_x1,
                    true,
                    None,
                    Some("OL-I9-SHB1"),
                    None,
                    Some("O-I9-SHB-E1"),
                );
                let e2 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I9-SHB-E2",
                    entry_side,
                    "15",
                    px_e,
                    false,
                    Some(ContingencyType::Oto),
                    Some("OL-I9-SHB2"),
                    Some(vec!["O-I9-SHB-TP2", "O-I9-SHB-SL2"]),
                    None,
                ));
                let tp2 = with_exit_marker(
                    make_contingent_limit_order(
                        inst.id(),
                        "O-I9-SHB-TP2",
                        exit_side,
                        "15",
                        px_x2,
                        false,
                        Some(ContingencyType::Oco),
                        Some("OL-I9-SHB2"),
                        Some(vec!["O-I9-SHB-SL2"]),
                        Some("O-I9-SHB-E2"),
                    ),
                    Some("TAKE_PROFIT"),
                    None,
                );
                let sl2 = make_contingent_stop_market_order(
                    inst.id(),
                    "O-I9-SHB-SL2",
                    exit_side,
                    "15",
                    sl_px2,
                    true,
                    Some(ContingencyType::Oco),
                    Some("OL-I9-SHB2"),
                    Some(vec!["O-I9-SHB-TP2"]),
                    Some("O-I9-SHB-E2"),
                );

                for o in [&e1, &x1] {
                    cache.add_order(o.clone(), None, None, true).unwrap();
                }
                let res_shb1 = gate.check_command(
                    &make_mixed_order_list_cmd("OL-I9-SHB1", &[e1, x1.clone()]),
                    &cache,
                    UnixNanos::from(TS_NOW),
                );
                assert!(res_shb1.is_ok(), "OL-I9-SHB1 failed: {res_shb1:?}");

                for o in [&e2, &tp2, &sl2] {
                    cache.add_order(o.clone(), None, None, true).unwrap();
                }
                let res_shb2 = gate.check_command(
                    &make_mixed_order_list_cmd("OL-I9-SHB2", &[e2, tp2.clone(), sl2]),
                    &cache,
                    UnixNanos::from(TS_NOW + 1),
                );
                assert!(res_shb2.is_ok(), "OL-I9-SHB2 failed: {res_shb2:?}");

                if entry_freeze_after_submit {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                assert!(
                    gate.check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(&x1, None, Some(px_x1_mod), None)),
                        &cache,
                        UnixNanos::from(TS_NOW + 2),
                    )
                    .is_ok()
                );
                let err_x1 = gate
                    .check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(
                            &x1,
                            Some("12"),
                            Some(px_x1_mod),
                            None,
                        )),
                        &cache,
                        UnixNanos::from(TS_NOW + 3),
                    )
                    .unwrap_err();
                assert!(err_x1.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"));

                assert!(
                    gate.check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(&tp2, None, Some(px_x2_mod), None)),
                        &cache,
                        UnixNanos::from(TS_NOW + 4),
                    )
                    .is_ok()
                );
                let err_tp2 = gate
                    .check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(
                            &tp2,
                            Some("16"),
                            Some(px_x2_mod),
                            None,
                        )),
                        &cache,
                        UnixNanos::from(TS_NOW + 5),
                    )
                    .unwrap_err();
                assert!(err_tp2.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"));
            }
        }
    }
}

#[test]
fn test_iter9_normalized_symbol_matching_in_reconcile_methods() {
    let inst_aapl = make_equity("AAPL");
    let inst_msft = make_equity("MSFT");
    let mut cache = Cache::default();
    cache.add_instrument(inst_aapl.clone()).unwrap();
    cache.add_instrument(inst_msft.clone()).unwrap();
    cache
        .add_quote(make_quote(inst_aapl.id(), "149.95", "150.00"))
        .unwrap();
    cache
        .add_quote(make_quote(inst_msft.id(), "299.95", "300.00"))
        .unwrap();

    let e_aapl = make_limit_order(
        inst_aapl.id(),
        "E-AAPL",
        OrderSide::Buy,
        "20",
        "150.00",
        false,
    );
    let e_aapl_2 = make_limit_order(
        inst_aapl.id(),
        "E-AAPL-2",
        OrderSide::Buy,
        "10",
        "150.00",
        false,
    );
    let e_msft = make_limit_order(
        inst_msft.id(),
        "E-MSFT",
        OrderSide::Buy,
        "20",
        "300.00",
        false,
    );
    cache.add_order(e_aapl.clone(), None, None, false).unwrap();
    cache.add_order(e_aapl_2.clone(), None, None, false).unwrap();
    cache.add_order(e_msft.clone(), None, None, false).unwrap();

    let gate = RustPlanGateEngine::new(false, String::new());
    gate.upsert_plan_item(
        make_plan_item("AAPL", "LONG", dec!(50), dec!(145.00), TS_FUTURE),
        false,
    );
    gate.upsert_plan_item(
        make_plan_item("MSFT", "LONG", dec!(50), dec!(290.00), TS_FUTURE),
        false,
    );

    assert!(
        gate.check_command(&submit_cmd(&e_aapl), &cache, UnixNanos::from(TS_NOW))
            .is_ok()
    );
    assert!(
        gate.check_command(&submit_cmd(&e_msft), &cache, UnixNanos::from(TS_NOW))
            .is_ok()
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(20));
    assert_eq!(gate.reserved_qty("MSFT"), dec!(20));

    // Mismatched symbols with whitespace/lowercase must be ignored without releasing
    // (returns unchanged reserved_qty for the queried symbol).
    assert_eq!(gate.reconcile_order_updated("E-AAPL", " nvda ", dec!(5)), dec!(0));
    assert_eq!(gate.reserved_qty("AAPL"), dec!(20));
    assert!(gate.has_admitted_reservation("E-AAPL"));

    assert_eq!(gate.reconcile_terminal_order("E-MSFT", " nvda ", dec!(5)), dec!(0));
    assert_eq!(gate.reserved_qty("MSFT"), dec!(20));
    assert!(gate.has_admitted_reservation("E-MSFT"));

    assert_eq!(gate.reconcile_terminal_order("E-AAPL", " msft ", dec!(5)), dec!(20));
    assert_eq!(gate.reserved_qty("AAPL"), dec!(20));
    assert_eq!(gate.reserved_qty("MSFT"), dec!(20));
    assert!(gate.has_admitted_reservation("E-AAPL"));
    assert!(gate.has_admitted_reservation("E-MSFT"));

    // Normalized matching symbols (" aapl ", "msft", "  msft  ") must reconcile identically to uppercase.
    assert_eq!(gate.reconcile_order_updated("E-AAPL", " aapl ", dec!(12)), dec!(12));
    assert_eq!(gate.reserved_qty("AAPL"), dec!(12));
    assert!(gate.has_admitted_reservation("E-AAPL"));

    assert_eq!(gate.reconcile_terminal_order("E-AAPL", " aapl ", dec!(7)), dec!(7));
    assert_eq!(gate.reserved_qty("AAPL"), dec!(7));
    assert!(!gate.has_admitted_reservation("E-AAPL"));

    assert_eq!(gate.reconcile_order_updated("E-MSFT", "msft", dec!(15)), dec!(15));
    assert_eq!(gate.reserved_qty("MSFT"), dec!(15));
    assert!(gate.has_admitted_reservation("E-MSFT"));

    assert_eq!(gate.reconcile_terminal_order("E-MSFT", "  msft  ", dec!(10)), dec!(10));
    assert_eq!(gate.reserved_qty("MSFT"), dec!(10));
    assert!(!gate.has_admitted_reservation("E-MSFT"));

    // Whitespace-only / empty symbol falls back to tracked symbol (consistent with release_order_reservation).
    assert!(
        gate.check_command(&submit_cmd(&e_aapl_2), &cache, UnixNanos::from(TS_NOW + 1))
            .is_ok()
    );
    assert_eq!(gate.reserved_qty("AAPL"), dec!(17));
    assert_eq!(gate.reconcile_order_updated("E-AAPL-2", "   ", dec!(6)), dec!(13));
    assert_eq!(gate.reserved_qty("AAPL"), dec!(13));
    assert_eq!(gate.reconcile_terminal_order("E-AAPL-2", "", dec!(4)), dec!(11));
    assert_eq!(gate.reserved_qty("AAPL"), dec!(11));
    assert!(!gate.has_admitted_reservation("E-AAPL-2"));
}

#[test]
fn test_iter10_mixed_oco_ouo_bracket_plus_unlinked_exits_no_double_count() {
    for entry_freeze in [false, true] {
        for contingency in [ContingencyType::Oco, ContingencyType::Ouo] {
            for (
                plan_side,
                pos_side,
                exit_side,
                initial_stop,
                px_x1,
                px_x2,
                px_x3,
                px_x1_mod,
                px_x3_mod,
            ) in [
                (
                    "LONG",
                    OrderSide::Buy,
                    OrderSide::Sell,
                    dec!(145.00),
                    "159.00",
                    "160.00",
                    "161.00",
                    "154.00",
                    "155.50",
                ),
                (
                    "SHORT",
                    OrderSide::Sell,
                    OrderSide::Buy,
                    dec!(155.00),
                    "141.00",
                    "140.00",
                    "139.00",
                    "146.00",
                    "144.50",
                ),
            ] {
                for order_seq in [
                    "bracket_then_x3",
                    "x3_then_bracket_list",
                    "x3_then_bracket_seq",
                ] {
                    let (inst, mut cache, gate) =
                        setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                    open_position_in_cache(
                        &mut cache,
                        &inst,
                        pos_side,
                        "10",
                        "150.00",
                        &format!("P-I10-MX-{plan_side}"),
                    );
                    if entry_freeze {
                        gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                    }

                    let list_id = format!("OL-I10-{plan_side}-{contingency:?}-{order_seq}");
                    let mut x1 = make_contingent_limit_order(
                        inst.id(),
                        "O-I10-MX-X1",
                        exit_side,
                        "6",
                        px_x1,
                        true,
                        Some(contingency),
                        Some(&list_id),
                        Some(vec!["O-I10-MX-X2"]),
                        None,
                    );
                    let mut x2 = with_exit_marker(
                        make_contingent_limit_order(
                            inst.id(),
                            "O-I10-MX-X2",
                            exit_side,
                            "6",
                            px_x2,
                            false,
                            Some(contingency),
                            Some(&list_id),
                            Some(vec!["O-I10-MX-X1"]),
                            None,
                        ),
                        Some("EXIT"),
                        None,
                    );
                    let mut x3 = make_limit_order(
                        inst.id(),
                        "O-I10-MX-X3",
                        exit_side,
                        "4",
                        px_x3,
                        true,
                    );

                    if order_seq == "bracket_then_x3" {
                        cache.add_order(x1.clone(), None, None, false).unwrap();
                        cache.add_order(x2.clone(), None, None, false).unwrap();
                        let res_list = gate.check_command(
                            &make_mixed_order_list_cmd(&list_id, &[x1.clone(), x2.clone()]),
                            &cache,
                            UnixNanos::from(TS_NOW),
                        );
                        assert!(
                            res_list.is_ok(),
                            "{plan_side} {contingency:?} freeze={entry_freeze} {order_seq}: bracket list must be admitted: {res_list:?}"
                        );
                        x1 = accept_order(x1);
                        x2 = accept_order(x2);
                        cache.update_order(x1.last_event()).unwrap();
                        cache.update_order(x2.last_event()).unwrap();

                        cache.add_order(x3.clone(), None, None, false).unwrap();
                        let res_x3 = gate.check_command(
                            &submit_cmd(&x3),
                            &cache,
                            UnixNanos::from(TS_NOW + 1),
                        );
                        assert!(
                            res_x3.is_ok(),
                            "{plan_side} {contingency:?} freeze={entry_freeze} {order_seq}: unlinked x3(4) alongside max(6,6)=6 must be admitted: {res_x3:?}"
                        );
                        x3 = accept_order(x3);
                        cache.update_order(x3.last_event()).unwrap();
                    } else {
                        cache.add_order(x3.clone(), None, None, false).unwrap();
                        let res_x3 = gate.check_command(
                            &submit_cmd(&x3),
                            &cache,
                            UnixNanos::from(TS_NOW),
                        );
                        assert!(res_x3.is_ok());
                        x3 = accept_order(x3);
                        cache.update_order(x3.last_event()).unwrap();

                        cache.add_order(x1.clone(), None, None, false).unwrap();
                        cache.add_order(x2.clone(), None, None, false).unwrap();
                        if order_seq == "x3_then_bracket_list" {
                            let res_list = gate.check_command(
                                &make_mixed_order_list_cmd(&list_id, &[x1.clone(), x2.clone()]),
                                &cache,
                                UnixNanos::from(TS_NOW + 1),
                            );
                            assert!(
                                res_list.is_ok(),
                                "{plan_side} {contingency:?} freeze={entry_freeze} {order_seq}: bracket list after x3(4) must be admitted (4 + max(6,6) = 10 <= 10): {res_list:?}"
                            );
                            x1 = accept_order(x1);
                            x2 = accept_order(x2);
                            cache.update_order(x1.last_event()).unwrap();
                            cache.update_order(x2.last_event()).unwrap();
                        } else {
                            assert!(
                                gate.check_command(
                                    &submit_cmd(&x1),
                                    &cache,
                                    UnixNanos::from(TS_NOW + 1)
                                )
                                .is_ok()
                            );
                            x1 = accept_order(x1);
                            cache.update_order(x1.last_event()).unwrap();
                            let res_x2 = gate.check_command(
                                &submit_cmd(&x2),
                                &cache,
                                UnixNanos::from(TS_NOW + 2),
                            );
                            assert!(
                                res_x2.is_ok(),
                                "{plan_side} {contingency:?} freeze={entry_freeze} {order_seq}: sequential x2(6) linked to x1(6) after x3(4) must be admitted: {res_x2:?}"
                            );
                            x2 = accept_order(x2);
                            cache.update_order(x2.last_event()).unwrap();
                        }
                    }

                    // Batch ModifyOrders([(x1, qty=6, px=px_x1_mod), (x3, qty=4, px=px_x3_mod)]) -> ADMITTED.
                    let ok_batch = BatchModifyOrders::new(
                        x1.trader_id(),
                        Some(ClientId::from("ALPACA")),
                        x1.strategy_id(),
                        inst.id(),
                        vec![
                            modify_cmd(&x1, Some("6"), Some(px_x1_mod), None),
                            modify_cmd(&x3, Some("4"), Some(px_x3_mod), None),
                        ],
                        UUID4::new(),
                        UnixNanos::from(TS_NOW + 3),
                        None,
                        None,
                    );
                    let res_ok_batch = gate.check_command(
                        &TradingCommand::ModifyOrders(ok_batch),
                        &cache,
                        UnixNanos::from(TS_NOW + 3),
                    );
                    assert!(
                        res_ok_batch.is_ok(),
                        "{plan_side} {contingency:?} freeze={entry_freeze} {order_seq}: BatchModifyOrders(x1=6, x3=4) with max(6,6)+4=10 must succeed: {res_ok_batch:?}"
                    );

                    // Contrast 1: ModifyOrder(x3, qty=5) pushes 6 + 5 = 11 > 10 -> REJECTED.
                    let err_mod_single = gate
                        .check_command(
                            &TradingCommand::ModifyOrder(modify_cmd(
                                &x3,
                                Some("5"),
                                Some(px_x3_mod),
                                None,
                            )),
                            &cache,
                            UnixNanos::from(TS_NOW + 4),
                        )
                        .unwrap_err();
                    assert!(
                        err_mod_single.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                        "expected REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, got: {err_mod_single}"
                    );

                    // Contrast 2: BatchModifyOrders([(x1, qty=6), (x3, qty=5)]) pushes 6 + 5 = 11 > 10 -> REJECTED.
                    let over_batch = BatchModifyOrders::new(
                        x1.trader_id(),
                        Some(ClientId::from("ALPACA")),
                        x1.strategy_id(),
                        inst.id(),
                        vec![
                            modify_cmd(&x1, Some("6"), Some(px_x1_mod), None),
                            modify_cmd(&x3, Some("5"), Some(px_x3_mod), None),
                        ],
                        UUID4::new(),
                        UnixNanos::from(TS_NOW + 5),
                        None,
                        None,
                    );
                    let err_mod_batch = gate
                        .check_command(
                            &TradingCommand::ModifyOrders(over_batch),
                            &cache,
                            UnixNanos::from(TS_NOW + 5),
                        )
                        .unwrap_err();
                    assert!(
                        err_mod_batch.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                        "expected REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, got: {err_mod_batch}"
                    );
                }

                // Contrast 3: Submitting unlinked x_over(5, ro=True) while [x1(6), x2(6)] active -> DENIED.
                {
                    let (inst, mut cache, gate) =
                        setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                    open_position_in_cache(
                        &mut cache,
                        &inst,
                        pos_side,
                        "10",
                        "150.00",
                        &format!("P-I10-OVR-{plan_side}"),
                    );
                    if entry_freeze {
                        gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                    }
                    let list_id = format!("OL-I10-OVR-{plan_side}-{contingency:?}");
                    let x1 = accept_order(make_contingent_limit_order(
                        inst.id(),
                        "O-I10-OVR-X1",
                        exit_side,
                        "6",
                        px_x1,
                        true,
                        Some(contingency),
                        Some(&list_id),
                        Some(vec!["O-I10-OVR-X2"]),
                        None,
                    ));
                    let x2 = accept_order(with_exit_marker(
                        make_contingent_limit_order(
                            inst.id(),
                            "O-I10-OVR-X2",
                            exit_side,
                            "6",
                            px_x2,
                            false,
                            Some(contingency),
                            Some(&list_id),
                            Some(vec!["O-I10-OVR-X1"]),
                            None,
                        ),
                        Some("EXIT"),
                        None,
                    ));
                    cache.add_order(x1.clone(), None, None, true).unwrap();
                    cache.add_order(x2.clone(), None, None, true).unwrap();
                    assert!(
                        gate.check_command(
                            &make_mixed_order_list_cmd(&list_id, &[x1, x2]),
                            &cache,
                            UnixNanos::from(TS_NOW),
                        )
                        .is_ok()
                    );

                    let x_over = make_limit_order(
                        inst.id(),
                        "O-I10-OVR-X3",
                        exit_side,
                        "5",
                        px_x3,
                        true,
                    );
                    cache.add_order(x_over.clone(), None, None, false).unwrap();
                    let err_sub = gate
                        .check_command(&submit_cmd(&x_over), &cache, UnixNanos::from(TS_NOW + 1))
                        .unwrap_err();
                    assert!(
                        err_sub.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                        "{plan_side} {contingency:?} freeze={entry_freeze}: 6 + 5 = 11 > 10 must deny, got: {err_sub}"
                    );
                }
            }
        }
    }
}

#[test]
fn test_iter10_two_separate_otos_parent2_full_and_partial_fill_transitions() {
    for entry_freeze_after_submit in [false, true] {
        for (plan_side, entry_side, exit_side, initial_stop, px_e1, px_e2, px_x1, px_x1_mod, px_x2, px_x3) in [
            (
                "LONG",
                OrderSide::Buy,
                OrderSide::Sell,
                dec!(145.00),
                "147.00",
                "149.00",
                "160.00",
                "159.00",
                "162.00",
                "163.00",
            ),
            (
                "SHORT",
                OrderSide::Sell,
                OrderSide::Buy,
                dec!(155.00),
                "153.00",
                "151.00",
                "140.00",
                "141.00",
                "138.00",
                "137.00",
            ),
        ] {
            // Case A: Full fill of e2 (10/10) -> open_pos_qty=10, e2.leaves=0, x2 Accepted(leaves=10),
            // while e1 remains unfilled (leaves=10, filled=0).
            {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);

                let e1 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I10-A-E1",
                    entry_side,
                    "10",
                    px_e1,
                    false,
                    Some(ContingencyType::Oto),
                    Some("OL-I10-A1"),
                    Some(vec!["O-I10-A-X1"]),
                    None,
                ));
                let x1 = make_contingent_limit_order(
                    inst.id(),
                    "O-I10-A-X1",
                    exit_side,
                    "10",
                    px_x1,
                    true,
                    None,
                    Some("OL-I10-A1"),
                    None,
                    Some("O-I10-A-E1"),
                );
                let e2 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I10-A-E2",
                    entry_side,
                    "10",
                    px_e2,
                    false,
                    Some(ContingencyType::Oto),
                    Some("OL-I10-A2"),
                    Some(vec!["O-I10-A-X2"]),
                    None,
                ));
                let x2 = with_exit_marker(
                    make_contingent_limit_order(
                        inst.id(),
                        "O-I10-A-X2",
                        exit_side,
                        "10",
                        px_x2,
                        false,
                        None,
                        Some("OL-I10-A2"),
                        None,
                        Some("O-I10-A-E2"),
                    ),
                    Some("EXIT"),
                    None,
                );

                cache.add_order(e1.clone(), None, None, true).unwrap();
                cache.add_order(x1.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(
                        &make_mixed_order_list_cmd("OL-I10-A1", &[e1, x1.clone()]),
                        &cache,
                        UnixNanos::from(TS_NOW),
                    )
                    .is_ok()
                );

                cache.add_order(e2.clone(), None, None, true).unwrap();
                cache.add_order(x2.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(
                        &make_mixed_order_list_cmd("OL-I10-A2", &[e2.clone(), x2.clone()]),
                        &cache,
                        UnixNanos::from(TS_NOW + 1),
                    )
                    .is_ok()
                );

                // Full fill of e2 (10 shares) + release/accept x2 + open position of 10.
                let pos_id = PositionId::from(format!("P-I10-A-{plan_side}").as_str());
                let fill_e2 = OrderFilled::new(
                    e2.trader_id(),
                    e2.strategy_id(),
                    inst.id(),
                    e2.client_order_id(),
                    e2.venue_order_id().unwrap(),
                    AccountId::from("ALPACA-001"),
                    TradeId::from(format!("T-I10-A-{plan_side}").as_str()),
                    entry_side,
                    OrderType::Limit,
                    Quantity::from("10"),
                    Price::from(px_e2),
                    Currency::USD(),
                    LiquiditySide::Taker,
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 2),
                    UnixNanos::from(TS_NOW + 2),
                    false,
                    Some(pos_id),
                    Some(Money::from("0.00 USD")),
                    None,
                );
                cache
                    .update_order(&OrderEventAny::Filled(fill_e2.clone()))
                    .unwrap();
                let pos = Position::new(&inst, fill_e2);
                cache
                    .add_position(&pos, nautilus_model::enums::OmsType::Netting)
                    .unwrap();

                let x2_acc = accept_order(x2);
                cache.update_order(x2_acc.last_event()).unwrap();

                if entry_freeze_after_submit {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                // Price-only ModifyOrder(x1) MUST succeed while e1 is still unfilled.
                let res_ok = gate.check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(&x1, None, Some(px_x1_mod), None)),
                    &cache,
                    UnixNanos::from(TS_NOW + 3),
                );
                assert!(
                    res_ok.is_ok(),
                    "{plan_side} freeze={entry_freeze_after_submit}: Case A full-fill e2 -> price-only ModifyOrder(x1) must succeed: {res_ok:?}"
                );

                // Upward ModifyOrder(x1, quantity=12) MUST be rejected.
                let err_up = gate
                    .check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(
                            &x1,
                            Some("12"),
                            Some(px_x1_mod),
                            None,
                        )),
                        &cache,
                        UnixNanos::from(TS_NOW + 4),
                    )
                    .unwrap_err();
                assert!(
                    err_up.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                    "Case A upward ModifyOrder(x1, 12) must reject, got: {err_up}"
                );
            }

            // Case B: Partial fill of e2 (4/10) -> open_pos_qty=4, e2.leaves=6, e2.filled=4, x2 Accepted,
            // while e1 remains unfilled (leaves=10, filled=0). Also exercise x1 status transitions
            // (Initialized, Submitted [previous=Initialized], PendingUpdate [previous=Submitted]).
            for x1_phase in ["initialized", "submitted", "pending_update"] {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);

                let e1 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I10-B-E1",
                    entry_side,
                    "10",
                    px_e1,
                    false,
                    Some(ContingencyType::Oto),
                    Some("OL-I10-B1"),
                    Some(vec!["O-I10-B-X1"]),
                    None,
                ));
                let mut x1 = make_contingent_limit_order(
                    inst.id(),
                    "O-I10-B-X1",
                    exit_side,
                    "10",
                    px_x1,
                    true,
                    None,
                    Some("OL-I10-B1"),
                    None,
                    Some("O-I10-B-E1"),
                );
                let e2 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I10-B-E2",
                    entry_side,
                    "10",
                    px_e2,
                    false,
                    Some(ContingencyType::Oto),
                    Some("OL-I10-B2"),
                    Some(vec!["O-I10-B-X2"]),
                    None,
                ));
                let x2 = with_exit_marker(
                    make_contingent_limit_order(
                        inst.id(),
                        "O-I10-B-X2",
                        exit_side,
                        "10",
                        px_x2,
                        false,
                        None,
                        Some("OL-I10-B2"),
                        None,
                        Some("O-I10-B-E2"),
                    ),
                    Some("EXIT"),
                    None,
                );

                cache.add_order(e1.clone(), None, None, true).unwrap();
                cache.add_order(x1.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(
                        &make_mixed_order_list_cmd("OL-I10-B1", &[e1, x1.clone()]),
                        &cache,
                        UnixNanos::from(TS_NOW),
                    )
                    .is_ok()
                );

                cache.add_order(e2.clone(), None, None, true).unwrap();
                cache.add_order(x2.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(
                        &make_mixed_order_list_cmd("OL-I10-B2", &[e2.clone(), x2.clone()]),
                        &cache,
                        UnixNanos::from(TS_NOW + 1),
                    )
                    .is_ok()
                );

                // Partial fill 4/10 on e2 -> open_pos=4, e2.leaves=6, e2.filled=4.
                let pos_id = PositionId::from(format!("P-I10-B-{plan_side}-{x1_phase}").as_str());
                let fill_e2 = OrderFilled::new(
                    e2.trader_id(),
                    e2.strategy_id(),
                    inst.id(),
                    e2.client_order_id(),
                    e2.venue_order_id().unwrap(),
                    AccountId::from("ALPACA-001"),
                    TradeId::from(format!("T-I10-B-{plan_side}-{x1_phase}").as_str()),
                    entry_side,
                    OrderType::Limit,
                    Quantity::from("4"),
                    Price::from(px_e2),
                    Currency::USD(),
                    LiquiditySide::Taker,
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 2),
                    UnixNanos::from(TS_NOW + 2),
                    false,
                    Some(pos_id),
                    Some(Money::from("0.00 USD")),
                    None,
                );
                cache
                    .update_order(&OrderEventAny::Filled(fill_e2.clone()))
                    .unwrap();
                let pos = Position::new(&inst, fill_e2);
                cache
                    .add_position(&pos, nautilus_model::enums::OmsType::Netting)
                    .unwrap();

                let x2_acc = accept_order(x2);
                cache.update_order(x2_acc.last_event()).unwrap();

                if x1_phase == "submitted" || x1_phase == "pending_update" {
                    let sub_ev = OrderSubmitted::new(
                        x1.trader_id(),
                        x1.strategy_id(),
                        x1.instrument_id(),
                        x1.client_order_id(),
                        AccountId::from("ALPACA-001"),
                        UUID4::new(),
                        UnixNanos::from(TS_NOW + 2),
                        UnixNanos::from(TS_NOW + 2),
                    );
                    x1.apply(OrderEventAny::Submitted(sub_ev.clone())).unwrap();
                    cache.update_order(&OrderEventAny::Submitted(sub_ev)).unwrap();
                    assert_eq!(x1.status(), OrderStatus::Submitted);
                    assert_eq!(x1.previous_status(), Some(OrderStatus::Initialized));
                }
                if x1_phase == "pending_update" {
                    let pu_ev = OrderPendingUpdate::new(
                        x1.trader_id(),
                        x1.strategy_id(),
                        x1.instrument_id(),
                        x1.client_order_id(),
                        Some(AccountId::from("ALPACA-001")),
                        UUID4::new(),
                        UnixNanos::from(TS_NOW + 3),
                        UnixNanos::from(TS_NOW + 3),
                        false,
                        None,
                    );
                    x1.apply(OrderEventAny::PendingUpdate(pu_ev.clone()))
                        .unwrap();
                    cache
                        .update_order(&OrderEventAny::PendingUpdate(pu_ev))
                        .unwrap();
                    assert_eq!(x1.status(), OrderStatus::PendingUpdate);
                    assert_eq!(x1.previous_status(), Some(OrderStatus::Submitted));
                }

                if entry_freeze_after_submit {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                let res_ok = gate.check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(&x1, None, Some(px_x1_mod), None)),
                    &cache,
                    UnixNanos::from(TS_NOW + 4),
                );
                assert!(
                    res_ok.is_ok(),
                    "{plan_side} freeze={entry_freeze_after_submit} {x1_phase}: Case B partial-fill e2 -> price-only ModifyOrder(x1) must succeed: {res_ok:?}"
                );

                let err_up = gate
                    .check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(
                            &x1,
                            Some("12"),
                            Some(px_x1_mod),
                            None,
                        )),
                        &cache,
                        UnixNanos::from(TS_NOW + 5),
                    )
                    .unwrap_err();
                assert!(
                    err_up.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                    "Case B ({x1_phase}) upward ModifyOrder(x1, 12) must reject, got: {err_up}"
                );
            }

            // Case C: Adversarial unlinked exit x3(4, ro=True) during e2 partial fill (4/10)
            // when already-released Accepted x2(4 or 6, ro=False, tags=["EXIT"]) sits on book -> DENIED.
            for x2_released_qty in ["4", "6"] {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);

                let e1 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I10-C-E1",
                    entry_side,
                    "10",
                    px_e1,
                    false,
                    Some(ContingencyType::Oto),
                    Some("OL-I10-C1"),
                    Some(vec!["O-I10-C-X1"]),
                    None,
                ));
                let x1 = make_contingent_limit_order(
                    inst.id(),
                    "O-I10-C-X1",
                    exit_side,
                    "10",
                    px_x1,
                    true,
                    None,
                    Some("OL-I10-C1"),
                    None,
                    Some("O-I10-C-E1"),
                );
                let e2 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I10-C-E2",
                    entry_side,
                    "10",
                    px_e2,
                    false,
                    Some(ContingencyType::Oto),
                    Some("OL-I10-C2"),
                    Some(vec!["O-I10-C-X2"]),
                    None,
                ));
                let x2 = with_exit_marker(
                    make_contingent_limit_order(
                        inst.id(),
                        "O-I10-C-X2",
                        exit_side,
                        x2_released_qty,
                        px_x2,
                        false,
                        None,
                        Some("OL-I10-C2"),
                        None,
                        Some("O-I10-C-E2"),
                    ),
                    Some("EXIT"),
                    None,
                );

                cache.add_order(e1.clone(), None, None, true).unwrap();
                cache.add_order(x1.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(
                        &make_mixed_order_list_cmd("OL-I10-C1", &[e1, x1]),
                        &cache,
                        UnixNanos::from(TS_NOW),
                    )
                    .is_ok()
                );

                cache.add_order(e2.clone(), None, None, true).unwrap();
                cache.add_order(x2.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(
                        &make_mixed_order_list_cmd("OL-I10-C2", &[e2.clone(), x2.clone()]),
                        &cache,
                        UnixNanos::from(TS_NOW + 1),
                    )
                    .is_ok()
                );

                let pos_id =
                    PositionId::from(format!("P-I10-C-{plan_side}-{x2_released_qty}").as_str());
                let fill_e2 = OrderFilled::new(
                    e2.trader_id(),
                    e2.strategy_id(),
                    inst.id(),
                    e2.client_order_id(),
                    e2.venue_order_id().unwrap(),
                    AccountId::from("ALPACA-001"),
                    TradeId::from(format!("T-I10-C-{plan_side}-{x2_released_qty}").as_str()),
                    entry_side,
                    OrderType::Limit,
                    Quantity::from("4"),
                    Price::from(px_e2),
                    Currency::USD(),
                    LiquiditySide::Taker,
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 2),
                    UnixNanos::from(TS_NOW + 2),
                    false,
                    Some(pos_id),
                    Some(Money::from("0.00 USD")),
                    None,
                );
                cache
                    .update_order(&OrderEventAny::Filled(fill_e2.clone()))
                    .unwrap();
                let pos = Position::new(&inst, fill_e2);
                cache
                    .add_position(&pos, nautilus_model::enums::OmsType::Netting)
                    .unwrap();

                let x2_acc = accept_order(x2);
                cache.update_order(x2_acc.last_event()).unwrap();

                if entry_freeze_after_submit {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                let x3 = make_limit_order(
                    inst.id(),
                    "O-I10-C-X3",
                    exit_side,
                    "4",
                    px_x3,
                    true,
                );
                cache.add_order(x3.clone(), None, None, false).unwrap();
                let err_x3 = gate
                    .check_command(&submit_cmd(&x3), &cache, UnixNanos::from(TS_NOW + 3))
                    .unwrap_err();
                assert!(
                    err_x3.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                    "{plan_side} freeze={entry_freeze_after_submit} x2={x2_released_qty}: adversarial unlinked x3(4) alongside live Accepted x2 must be denied, got: {err_x3}"
                );
            }
        }
    }
}

#[test]
fn test_iter11_prefill_oto_child_upward_modify_guard_symmetry_ro_true_and_false_exit() {
    for x1_reduce_only in [true, false] {
        for entry_freeze_after_submit in [false, true] {
            for e2_fill_qty in ["4", "10"] {
                for x1_phase in ["initialized", "submitted", "pending_update"] {
                    for (
                        plan_side,
                        entry_side,
                        exit_side,
                        initial_stop,
                        px_e1,
                        px_e2,
                        px_x1,
                        px_x1_mod,
                        px_x2,
                    ) in [
                        (
                            "LONG",
                            OrderSide::Buy,
                            OrderSide::Sell,
                            dec!(145.00),
                            "147.00",
                            "149.00",
                            "160.00",
                            "159.00",
                            "162.00",
                        ),
                        (
                            "SHORT",
                            OrderSide::Sell,
                            OrderSide::Buy,
                            dec!(155.00),
                            "153.00",
                            "151.00",
                            "140.00",
                            "141.00",
                            "138.00",
                        ),
                    ] {
                        let (inst, mut cache, gate) =
                            setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);

                        let e1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I11-SYM-E1",
                            entry_side,
                            "10",
                            px_e1,
                            false,
                            Some(ContingencyType::Oto),
                            Some("OL-I11-SYM-1"),
                            Some(vec!["O-I11-SYM-X1"]),
                            None,
                        ));
                        let mut x1 = make_contingent_limit_order(
                            inst.id(),
                            "O-I11-SYM-X1",
                            exit_side,
                            "10",
                            px_x1,
                            x1_reduce_only,
                            None,
                            Some("OL-I11-SYM-1"),
                            None,
                            Some("O-I11-SYM-E1"),
                        );
                        if !x1_reduce_only {
                            x1 = with_exit_marker(x1, Some("EXIT"), None);
                        }

                        let e2 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I11-SYM-E2",
                            entry_side,
                            "10",
                            px_e2,
                            false,
                            Some(ContingencyType::Oto),
                            Some("OL-I11-SYM-2"),
                            Some(vec!["O-I11-SYM-X2"]),
                            None,
                        ));
                        let x2 = with_exit_marker(
                            make_contingent_limit_order(
                                inst.id(),
                                "O-I11-SYM-X2",
                                exit_side,
                                e2_fill_qty,
                                px_x2,
                                false,
                                None,
                                Some("OL-I11-SYM-2"),
                                None,
                                Some("O-I11-SYM-E2"),
                            ),
                            Some("EXIT"),
                            None,
                        );

                        cache.add_order(e1.clone(), None, None, true).unwrap();
                        cache.add_order(x1.clone(), None, None, true).unwrap();
                        assert!(
                            gate.check_command(
                                &make_mixed_order_list_cmd("OL-I11-SYM-1", &[e1, x1.clone()]),
                                &cache,
                                UnixNanos::from(TS_NOW),
                            )
                            .is_ok()
                        );

                        cache.add_order(e2.clone(), None, None, true).unwrap();
                        cache.add_order(x2.clone(), None, None, true).unwrap();
                        assert!(
                            gate.check_command(
                                &make_mixed_order_list_cmd(
                                    "OL-I11-SYM-2",
                                    &[e2.clone(), x2.clone()]
                                ),
                                &cache,
                                UnixNanos::from(TS_NOW + 1),
                            )
                            .is_ok()
                        );

                        let ro_tag = if x1_reduce_only { "T" } else { "F" };
                        let phase_tag = match x1_phase {
                            "initialized" => "I",
                            "submitted" => "S",
                            _ => "P",
                        };
                        let pos_id = PositionId::from(
                            format!("P-11S-{plan_side}-{ro_tag}-{e2_fill_qty}-{phase_tag}")
                                .as_str(),
                        );
                        let fill_e2 = OrderFilled::new(
                            e2.trader_id(),
                            e2.strategy_id(),
                            inst.id(),
                            e2.client_order_id(),
                            e2.venue_order_id().unwrap(),
                            AccountId::from("ALPACA-001"),
                            TradeId::from(
                                format!("T-11S-{plan_side}-{ro_tag}-{e2_fill_qty}-{phase_tag}")
                                    .as_str(),
                            ),
                            entry_side,
                            OrderType::Limit,
                            Quantity::from(e2_fill_qty),
                            Price::from(px_e2),
                            Currency::USD(),
                            LiquiditySide::Taker,
                            UUID4::new(),
                            UnixNanos::from(TS_NOW + 2),
                            UnixNanos::from(TS_NOW + 2),
                            false,
                            Some(pos_id),
                            Some(Money::from("0.00 USD")),
                            None,
                        );
                        cache
                            .update_order(&OrderEventAny::Filled(fill_e2.clone()))
                            .unwrap();
                        let pos = Position::new(&inst, fill_e2);
                        cache
                            .add_position(&pos, nautilus_model::enums::OmsType::Netting)
                            .unwrap();

                        let x2_acc = accept_order(x2);
                        cache.update_order(x2_acc.last_event()).unwrap();

                        if x1_phase == "submitted" || x1_phase == "pending_update" {
                            let sub_ev = OrderSubmitted::new(
                                x1.trader_id(),
                                x1.strategy_id(),
                                x1.instrument_id(),
                                x1.client_order_id(),
                                AccountId::from("ALPACA-001"),
                                UUID4::new(),
                                UnixNanos::from(TS_NOW + 2),
                                UnixNanos::from(TS_NOW + 2),
                            );
                            x1.apply(OrderEventAny::Submitted(sub_ev.clone())).unwrap();
                            cache.update_order(&OrderEventAny::Submitted(sub_ev)).unwrap();
                        }
                        if x1_phase == "pending_update" {
                            let pu_ev = OrderPendingUpdate::new(
                                x1.trader_id(),
                                x1.strategy_id(),
                                x1.instrument_id(),
                                x1.client_order_id(),
                                Some(AccountId::from("ALPACA-001")),
                                UUID4::new(),
                                UnixNanos::from(TS_NOW + 3),
                                UnixNanos::from(TS_NOW + 3),
                                false,
                                None,
                            );
                            x1.apply(OrderEventAny::PendingUpdate(pu_ev.clone()))
                                .unwrap();
                            cache
                                .update_order(&OrderEventAny::PendingUpdate(pu_ev))
                                .unwrap();
                        }

                        if entry_freeze_after_submit {
                            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                        }

                        // Price-only, same-qty, and downward-qty ModifyOrder(x1) MUST succeed.
                        assert!(
                            gate.check_command(
                                &TradingCommand::ModifyOrder(modify_cmd(
                                    &x1,
                                    None,
                                    Some(px_x1_mod),
                                    None
                                )),
                                &cache,
                                UnixNanos::from(TS_NOW + 4),
                            )
                            .is_ok(),
                            "{plan_side} ro={x1_reduce_only} freeze={entry_freeze_after_submit} fill={e2_fill_qty} phase={x1_phase}: price-only modify must succeed"
                        );
                        assert!(
                            gate.check_command(
                                &TradingCommand::ModifyOrder(modify_cmd(
                                    &x1,
                                    Some("10"),
                                    Some(px_x1_mod),
                                    None
                                )),
                                &cache,
                                UnixNanos::from(TS_NOW + 5),
                            )
                            .is_ok(),
                            "{plan_side} ro={x1_reduce_only} freeze={entry_freeze_after_submit} fill={e2_fill_qty} phase={x1_phase}: same-qty modify must succeed"
                        );
                        assert!(
                            gate.check_command(
                                &TradingCommand::ModifyOrder(modify_cmd(
                                    &x1,
                                    Some("8"),
                                    Some(px_x1_mod),
                                    None
                                )),
                                &cache,
                                UnixNanos::from(TS_NOW + 6),
                            )
                            .is_ok(),
                            "{plan_side} ro={x1_reduce_only} freeze={entry_freeze_after_submit} fill={e2_fill_qty} phase={x1_phase}: downward-qty modify must succeed"
                        );

                        // Upward ModifyOrder(x1, qty=12|14|15|20) MUST be rejected symmetrically.
                        for up_qty in ["12", "14", "15", "20"] {
                            let err = gate
                                .check_command(
                                    &TradingCommand::ModifyOrder(modify_cmd(
                                        &x1,
                                        Some(up_qty),
                                        Some(px_x1_mod),
                                        None,
                                    )),
                                    &cache,
                                    UnixNanos::from(TS_NOW + 7),
                                )
                                .unwrap_err();
                            assert!(
                                err.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                                "{plan_side} ro={x1_reduce_only} freeze={entry_freeze_after_submit} fill={e2_fill_qty} phase={x1_phase} up_qty={up_qty}: expected REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, got: {err}"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn test_iter11_prefill_oto_full_lifecycle_partial_fill_cancel_e2_fill_e1_flat_no_spillover() {
    for x1_reduce_only in [false, true] {
        for (
            plan_side,
            entry_side,
            exit_side,
            initial_stop,
            px_e1,
            px_e2,
            px_x1,
            px_x1_mod,
            px_x2,
            px_x2_mod,
            px_x3,
        ) in [
            (
                "LONG",
                OrderSide::Buy,
                OrderSide::Sell,
                dec!(145.00),
                "147.00",
                "149.00",
                "160.00",
                "159.00",
                "162.00",
                "161.50",
                "163.00",
            ),
            (
                "SHORT",
                OrderSide::Sell,
                OrderSide::Buy,
                dec!(155.00),
                "153.00",
                "151.00",
                "140.00",
                "141.00",
                "138.00",
                "138.50",
                "137.00",
            ),
        ] {
            for post_x1_action in ["x2_resize_to_10", "submit_x3_10_ro_true"] {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);

                let e2 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I11-LC-E2",
                    entry_side,
                    "10",
                    px_e2,
                    false,
                    Some(ContingencyType::Oto),
                    Some("OL-I11-LC-2"),
                    Some(vec!["O-I11-LC-X2"]),
                    None,
                ));
                let x2 = with_exit_marker(
                    make_contingent_limit_order(
                        inst.id(),
                        "O-I11-LC-X2",
                        exit_side,
                        "4",
                        px_x2,
                        false,
                        None,
                        Some("OL-I11-LC-2"),
                        None,
                        Some("O-I11-LC-E2"),
                    ),
                    Some("EXIT"),
                    None,
                );
                let e1 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I11-LC-E1",
                    entry_side,
                    "10",
                    px_e1,
                    false,
                    Some(ContingencyType::Oto),
                    Some("OL-I11-LC-1"),
                    Some(vec!["O-I11-LC-X1"]),
                    None,
                ));
                let mut x1 = make_contingent_limit_order(
                    inst.id(),
                    "O-I11-LC-X1",
                    exit_side,
                    "10",
                    px_x1,
                    x1_reduce_only,
                    None,
                    Some("OL-I11-LC-1"),
                    None,
                    Some("O-I11-LC-E1"),
                );
                if !x1_reduce_only {
                    x1 = with_exit_marker(x1, Some("EXIT"), None);
                }

                cache.add_order(e2.clone(), None, None, true).unwrap();
                cache.add_order(x2.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(
                        &make_mixed_order_list_cmd("OL-I11-LC-2", &[e2.clone(), x2.clone()]),
                        &cache,
                        UnixNanos::from(TS_NOW),
                    )
                    .is_ok()
                );

                cache.add_order(e1.clone(), None, None, true).unwrap();
                cache.add_order(x1.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(
                        &make_mixed_order_list_cmd("OL-I11-LC-1", &[e1.clone(), x1.clone()]),
                        &cache,
                        UnixNanos::from(TS_NOW + 1),
                    )
                    .is_ok()
                );

                // Partial fill e2 (4/10) -> open_pos=4, x2 Accepted(4), e1 still unfilled (0/10).
                let ro_tag = if x1_reduce_only { "T" } else { "F" };
                let act_tag = if post_x1_action == "x2_resize_to_10" {
                    "R"
                } else {
                    "X3"
                };
                let pos_id =
                    PositionId::from(format!("P-11L-{plan_side}-{ro_tag}-{act_tag}").as_str());
                let fill_e2 = OrderFilled::new(
                    e2.trader_id(),
                    e2.strategy_id(),
                    inst.id(),
                    e2.client_order_id(),
                    e2.venue_order_id().unwrap(),
                    AccountId::from("ALPACA-001"),
                    TradeId::from(format!("T-11L2-{plan_side}-{ro_tag}-{act_tag}").as_str()),
                    entry_side,
                    OrderType::Limit,
                    Quantity::from("4"),
                    Price::from(px_e2),
                    Currency::USD(),
                    LiquiditySide::Taker,
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 2),
                    UnixNanos::from(TS_NOW + 2),
                    false,
                    Some(pos_id),
                    Some(Money::from("0.00 USD")),
                    None,
                );
                cache
                    .update_order(&OrderEventAny::Filled(fill_e2.clone()))
                    .unwrap();
                let pos = Position::new(&inst, fill_e2);
                cache
                    .add_position(&pos, nautilus_model::enums::OmsType::Netting)
                    .unwrap();

                let x2_acc = accept_order(x2);
                cache.update_order(x2_acc.last_event()).unwrap();

                // Attempt upward ModifyOrder(x1, quantity=12) -> MUST be rejected!
                let err_up = gate
                    .check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(
                            &x1,
                            Some("12"),
                            Some(px_x1_mod),
                            None,
                        )),
                        &cache,
                        UnixNanos::from(TS_NOW + 3),
                    )
                    .unwrap_err();
                assert!(
                    err_up.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                    "{plan_side} ro={x1_reduce_only}: upward ModifyOrder(x1, 12) must reject, got: {err_up}"
                );

                // Cancel e2 (stops at 4/10 filled, 0 leaves).
                let cancel_e2 = OrderCanceled::new(
                    e2.trader_id(),
                    e2.strategy_id(),
                    e2.instrument_id(),
                    e2.client_order_id(),
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 4),
                    UnixNanos::from(TS_NOW + 4),
                    false,
                    e2.venue_order_id(),
                    Some(AccountId::from("ALPACA-001")),
                    None,
                );
                cache
                    .update_order(&OrderEventAny::Canceled(cancel_e2))
                    .unwrap();
                gate.release_order_reservation(e2.client_order_id().as_str(), "AAPL", dec!(6));

                // Fill e1 (10/10) -> open_pos becomes 14, x1 Accepted(10).
                let fill_e1 = OrderFilled::new(
                    e1.trader_id(),
                    e1.strategy_id(),
                    inst.id(),
                    e1.client_order_id(),
                    e1.venue_order_id().unwrap(),
                    AccountId::from("ALPACA-001"),
                    TradeId::from(format!("T-11L1-{plan_side}-{ro_tag}-{act_tag}").as_str()),
                    entry_side,
                    OrderType::Limit,
                    Quantity::from("10"),
                    Price::from(px_e1),
                    Currency::USD(),
                    LiquiditySide::Taker,
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 5),
                    UnixNanos::from(TS_NOW + 5),
                    false,
                    Some(pos_id),
                    Some(Money::from("0.00 USD")),
                    None,
                );
                cache
                    .update_order(&OrderEventAny::Filled(fill_e1.clone()))
                    .unwrap();
                cache.update_position_from_fill(pos_id, &fill_e1).unwrap();
                let x1_acc = accept_order(x1);
                cache.update_order(x1_acc.last_event()).unwrap();

                // While x1(10) and x2(4) are both open against open_pos=14, price-only ModifyOrder(x2)
                // succeeds (10 + 4 = 14 <= 14; no phantom x1=12 spillover!).
                assert!(
                    gate.check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(
                            &x2_acc,
                            None,
                            Some(px_x2_mod),
                            None
                        )),
                        &cache,
                        UnixNanos::from(TS_NOW + 6),
                    )
                    .is_ok(),
                    "{plan_side} ro={x1_reduce_only}: ModifyOrder(x2) alongside active x1(10) must not suffer phantom x1(12) spillover"
                );

                // Cancel x1 -> open_pos=14, x2=4; verify x2 resize (4->10) OR x3(10, ro=True) is NOT blocked.
                let cancel_x1 = OrderCanceled::new(
                    x1_acc.trader_id(),
                    x1_acc.strategy_id(),
                    x1_acc.instrument_id(),
                    x1_acc.client_order_id(),
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 7),
                    UnixNanos::from(TS_NOW + 7),
                    false,
                    x1_acc.venue_order_id(),
                    Some(AccountId::from("ALPACA-001")),
                    None,
                );
                cache
                    .update_order(&OrderEventAny::Canceled(cancel_x1))
                    .unwrap();

                if post_x1_action == "x2_resize_to_10" {
                    let res_x2 = gate.check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(
                            &x2_acc,
                            Some("10"),
                            Some(px_x2_mod),
                            None,
                        )),
                        &cache,
                        UnixNanos::from(TS_NOW + 8),
                    );
                    assert!(
                        res_x2.is_ok(),
                        "{plan_side} ro={x1_reduce_only}: x2 resize (4 -> 10) against open_pos=14 after x1 cancel must not be blocked by phantom x1 spillover: {res_x2:?}"
                    );
                } else {
                    let x3 = make_limit_order(
                        inst.id(),
                        "O-I11-LC-X3",
                        exit_side,
                        "10",
                        px_x3,
                        true,
                    );
                    cache.add_order(x3.clone(), None, None, false).unwrap();
                    let res_x3 = gate.check_command(
                        &submit_cmd(&x3),
                        &cache,
                        UnixNanos::from(TS_NOW + 8),
                    );
                    assert!(
                        res_x3.is_ok(),
                        "{plan_side} ro={x1_reduce_only}: valid x3(10, ro=True) alongside x2(4) against open_pos=14 must not be blocked by phantom x1 spillover: {res_x3:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn test_iter11_prefill_oto_zero_open_pos_smaller_child_upward_modify_rejected() {
    for x1_reduce_only in [false, true] {
        for entry_freeze_after_submit in [false, true] {
            for x1_phase in ["initialized", "submitted", "pending_update"] {
                for (plan_side, entry_side, exit_side, initial_stop, px_e1, px_x1, px_x1_mod) in [
                    (
                        "LONG",
                        OrderSide::Buy,
                        OrderSide::Sell,
                        dec!(145.00),
                        "147.00",
                        "160.00",
                        "159.00",
                    ),
                    (
                        "SHORT",
                        OrderSide::Sell,
                        OrderSide::Buy,
                        dec!(155.00),
                        "153.00",
                        "140.00",
                        "141.00",
                    ),
                ] {
                    let (inst, mut cache, gate) =
                        setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);

                    let e1 = accept_order(make_contingent_limit_order(
                        inst.id(),
                        "O-I11-Z-E1",
                        entry_side,
                        "10",
                        px_e1,
                        false,
                        Some(ContingencyType::Oto),
                        Some("OL-I11-Z-1"),
                        Some(vec!["O-I11-Z-X1"]),
                        None,
                    ));
                    let mut x1 = make_contingent_limit_order(
                        inst.id(),
                        "O-I11-Z-X1",
                        exit_side,
                        "5",
                        px_x1,
                        x1_reduce_only,
                        None,
                        Some("OL-I11-Z-1"),
                        None,
                        Some("O-I11-Z-E1"),
                    );
                    if !x1_reduce_only {
                        x1 = with_exit_marker(x1, Some("EXIT"), None);
                    }

                    cache.add_order(e1.clone(), None, None, true).unwrap();
                    cache.add_order(x1.clone(), None, None, true).unwrap();
                    assert!(
                        gate.check_command(
                            &make_mixed_order_list_cmd("OL-I11-Z-1", &[e1, x1.clone()]),
                            &cache,
                            UnixNanos::from(TS_NOW),
                        )
                        .is_ok()
                    );

                    if x1_phase == "submitted" || x1_phase == "pending_update" {
                        let sub_ev = OrderSubmitted::new(
                            x1.trader_id(),
                            x1.strategy_id(),
                            x1.instrument_id(),
                            x1.client_order_id(),
                            AccountId::from("ALPACA-001"),
                            UUID4::new(),
                            UnixNanos::from(TS_NOW + 1),
                            UnixNanos::from(TS_NOW + 1),
                        );
                        x1.apply(OrderEventAny::Submitted(sub_ev.clone())).unwrap();
                        cache.update_order(&OrderEventAny::Submitted(sub_ev)).unwrap();
                    }
                    if x1_phase == "pending_update" {
                        let pu_ev = OrderPendingUpdate::new(
                            x1.trader_id(),
                            x1.strategy_id(),
                            x1.instrument_id(),
                            x1.client_order_id(),
                            Some(AccountId::from("ALPACA-001")),
                            UUID4::new(),
                            UnixNanos::from(TS_NOW + 2),
                            UnixNanos::from(TS_NOW + 2),
                            false,
                            None,
                        );
                        x1.apply(OrderEventAny::PendingUpdate(pu_ev.clone()))
                            .unwrap();
                        cache
                            .update_order(&OrderEventAny::PendingUpdate(pu_ev))
                            .unwrap();
                    }

                    if entry_freeze_after_submit {
                        gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                    }

                    // Price-only, same-qty (5), and downward (3) ModifyOrder(x1) MUST succeed at open_pos=0.
                    assert!(
                        gate.check_command(
                            &TradingCommand::ModifyOrder(modify_cmd(
                                &x1,
                                None,
                                Some(px_x1_mod),
                                None
                            )),
                            &cache,
                            UnixNanos::from(TS_NOW + 3),
                        )
                        .is_ok(),
                        "{plan_side} ro={x1_reduce_only} freeze={entry_freeze_after_submit} phase={x1_phase}: price-only ModifyOrder(x1) must succeed at open_pos=0"
                    );
                    assert!(
                        gate.check_command(
                            &TradingCommand::ModifyOrder(modify_cmd(
                                &x1,
                                Some("5"),
                                Some(px_x1_mod),
                                None
                            )),
                            &cache,
                            UnixNanos::from(TS_NOW + 4),
                        )
                        .is_ok()
                    );
                    assert!(
                        gate.check_command(
                            &TradingCommand::ModifyOrder(modify_cmd(
                                &x1,
                                Some("3"),
                                Some(px_x1_mod),
                                None
                            )),
                            &cache,
                            UnixNanos::from(TS_NOW + 5),
                        )
                        .is_ok()
                    );

                    // Upward ModifyOrder(x1, qty=8) (even though 8 <= e1(10)) and qty=12 MUST be rejected!
                    for up_qty in ["8", "12"] {
                        let err_up = gate
                            .check_command(
                                &TradingCommand::ModifyOrder(modify_cmd(
                                    &x1,
                                    Some(up_qty),
                                    Some(px_x1_mod),
                                    None,
                                )),
                                &cache,
                                UnixNanos::from(TS_NOW + 6),
                            )
                            .unwrap_err();
                        assert!(
                            err_up.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                            "{plan_side} ro={x1_reduce_only} freeze={entry_freeze_after_submit} phase={x1_phase} up_qty={up_qty}: expected REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, got: {err_up}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn test_iter12_prefill_oto_submit_order_list_child_exceeds_parent_atomic_and_close_flat() {
    for unrelated_open_e2 in [false, true] {
        for x3_reduce_only in [true, false] {
            for freeze_before_x3 in [false, true] {
                for bad_shape in [
                    "single_ro_false_exit",
                    "single_ro_true",
                    "oco_bracket_20",
                    "ouo_bracket_20",
                    "unlinked_multi_child",
                ] {
                    for (
                        plan_side,
                        entry_side,
                        exit_side,
                        initial_stop,
                        px_e2,
                        px_e1,
                        px_sl,
                        px_tp,
                        px_x3,
                    ) in [
                        (
                            "LONG",
                            OrderSide::Buy,
                            OrderSide::Sell,
                            dec!(145.00),
                            "150.00",
                            "147.00",
                            "145.50",
                            "160.00",
                            "150.05",
                        ),
                        (
                            "SHORT",
                            OrderSide::Sell,
                            OrderSide::Buy,
                            dec!(155.00),
                            "150.00",
                            "153.00",
                            "154.50",
                            "140.00",
                            "149.95",
                        ),
                    ] {
                        let (inst, mut cache, gate) =
                            setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);

                        let pos_id = PositionId::from(format!("P-I12-L-{plan_side}").as_str());
                        if unrelated_open_e2 {
                            let e2 = accept_order(make_limit_order(
                                inst.id(),
                                "O-I12-L-E2",
                                entry_side,
                                "10",
                                px_e2,
                                false,
                            ));
                            cache.add_order(e2.clone(), None, None, true).unwrap();
                            assert!(
                                gate.check_command(
                                    &submit_cmd(&e2),
                                    &cache,
                                    UnixNanos::from(TS_NOW),
                                )
                                .is_ok()
                            );
                            let fill_e2 = OrderFilled::new(
                                e2.trader_id(),
                                e2.strategy_id(),
                                inst.id(),
                                e2.client_order_id(),
                                e2.venue_order_id().unwrap(),
                                AccountId::from("ALPACA-001"),
                                TradeId::from(format!("T-I12-L-E2-{plan_side}").as_str()),
                                entry_side,
                                OrderType::Limit,
                                Quantity::from("10"),
                                Price::from(px_e2),
                                Currency::USD(),
                                LiquiditySide::Taker,
                                UUID4::new(),
                                UnixNanos::from(TS_NOW + 1),
                                UnixNanos::from(TS_NOW + 1),
                                false,
                                Some(pos_id),
                                Some(Money::from("0.00 USD")),
                                None,
                            );
                            cache
                                .update_order(&OrderEventAny::Filled(fill_e2.clone()))
                                .unwrap();
                            let pos = Position::new(&inst, fill_e2);
                            cache
                                .add_position(&pos, nautilus_model::enums::OmsType::Netting)
                                .unwrap();
                        }

                        let expected_reserved_before =
                            if unrelated_open_e2 { dec!(10) } else { dec!(0) };
                        assert_eq!(gate.reserved_qty("AAPL"), expected_reserved_before);

                        let list_id = format!("OL-I12-{plan_side}-{bad_shape}");
                        let e1_cid = "O-I12-L-E1";
                        let mut orders: Vec<OrderAny> = Vec::new();
                        let mut child_cids: Vec<&str> = Vec::new();

                        match bad_shape {
                            "single_ro_false_exit" => {
                                let e1 = make_contingent_limit_order(
                                    inst.id(),
                                    e1_cid,
                                    entry_side,
                                    "10",
                                    px_e1,
                                    false,
                                    Some(ContingencyType::Oto),
                                    Some(&list_id),
                                    Some(vec!["O-I12-L-X1"]),
                                    None,
                                );
                                let x1 = with_exit_marker(
                                    make_contingent_limit_order(
                                        inst.id(),
                                        "O-I12-L-X1",
                                        exit_side,
                                        "20",
                                        px_tp,
                                        false,
                                        None,
                                        Some(&list_id),
                                        None,
                                        Some(e1_cid),
                                    ),
                                    Some("EXIT"),
                                    None,
                                );
                                orders.push(e1);
                                orders.push(x1);
                                child_cids.push("O-I12-L-X1");
                            }
                            "single_ro_true" => {
                                let e1 = make_contingent_limit_order(
                                    inst.id(),
                                    e1_cid,
                                    entry_side,
                                    "10",
                                    px_e1,
                                    false,
                                    Some(ContingencyType::Oto),
                                    Some(&list_id),
                                    Some(vec!["O-I12-L-X1"]),
                                    None,
                                );
                                let x1 = make_contingent_limit_order(
                                    inst.id(),
                                    "O-I12-L-X1",
                                    exit_side,
                                    "20",
                                    px_tp,
                                    true,
                                    None,
                                    Some(&list_id),
                                    None,
                                    Some(e1_cid),
                                );
                                orders.push(e1);
                                orders.push(x1);
                                child_cids.push("O-I12-L-X1");
                            }
                            "oco_bracket_20" | "ouo_bracket_20" => {
                                let cont = if bad_shape == "oco_bracket_20" {
                                    ContingencyType::Oco
                                } else {
                                    ContingencyType::Ouo
                                };
                                let e1 = make_contingent_limit_order(
                                    inst.id(),
                                    e1_cid,
                                    entry_side,
                                    "10",
                                    px_e1,
                                    false,
                                    Some(ContingencyType::Oto),
                                    Some(&list_id),
                                    Some(vec!["O-I12-L-SL", "O-I12-L-TP"]),
                                    None,
                                );
                                let sl = make_contingent_stop_market_order(
                                    inst.id(),
                                    "O-I12-L-SL",
                                    exit_side,
                                    "20",
                                    px_sl,
                                    true,
                                    Some(cont),
                                    Some(&list_id),
                                    Some(vec!["O-I12-L-TP"]),
                                    Some(e1_cid),
                                );
                                let tp = with_exit_marker(
                                    make_contingent_limit_order(
                                        inst.id(),
                                        "O-I12-L-TP",
                                        exit_side,
                                        "20",
                                        px_tp,
                                        false,
                                        Some(cont),
                                        Some(&list_id),
                                        Some(vec!["O-I12-L-SL"]),
                                        Some(e1_cid),
                                    ),
                                    Some("TAKE_PROFIT"),
                                    None,
                                );
                                orders.push(e1);
                                orders.push(sl);
                                orders.push(tp);
                                child_cids.extend(["O-I12-L-SL", "O-I12-L-TP"]);
                            }
                            _ => {
                                // Unlinked multi-child [E-1(10, Oto), X-1(10), X-2(10)]: raw sum 20 > 10.
                                let e1 = make_contingent_limit_order(
                                    inst.id(),
                                    e1_cid,
                                    entry_side,
                                    "10",
                                    px_e1,
                                    false,
                                    Some(ContingencyType::Oto),
                                    Some(&list_id),
                                    Some(vec!["O-I12-L-X1", "O-I12-L-X2"]),
                                    None,
                                );
                                let x1 = make_contingent_limit_order(
                                    inst.id(),
                                    "O-I12-L-X1",
                                    exit_side,
                                    "10",
                                    px_tp,
                                    true,
                                    None,
                                    Some(&list_id),
                                    None,
                                    Some(e1_cid),
                                );
                                let x2 = with_exit_marker(
                                    make_contingent_limit_order(
                                        inst.id(),
                                        "O-I12-L-X2",
                                        exit_side,
                                        "10",
                                        px_tp,
                                        false,
                                        None,
                                        Some(&list_id),
                                        None,
                                        Some(e1_cid),
                                    ),
                                    Some("EXIT"),
                                    None,
                                );
                                orders.push(e1);
                                orders.push(x1);
                                orders.push(x2);
                                child_cids.extend(["O-I12-L-X1", "O-I12-L-X2"]);
                            }
                        }

                        for o in &orders {
                            cache.add_order(o.clone(), None, None, false).unwrap();
                        }
                        let err = gate
                            .check_command(
                                &make_mixed_order_list_cmd(&list_id, &orders),
                                &cache,
                                UnixNanos::from(TS_NOW + 2),
                            )
                            .unwrap_err();
                        assert!(
                            err.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                            "{plan_side} open_e2={unrelated_open_e2} {bad_shape}: pre-fill OTO child exceeding parent must reject with REDUCE_ONLY_EXIT_EXCEEDS_POSITION, got: {err}"
                        );
                        assert_eq!(
                            gate.reserved_qty("AAPL"),
                            expected_reserved_before,
                            "{plan_side} {bad_shape}: rejection must be atomic with zero capacity reserved for E-1"
                        );
                        assert!(!gate.has_admitted_reservation(e1_cid));
                        for cid in child_cids {
                            assert!(!gate.has_admitted_reservation(cid));
                        }
                        // RiskEngine::deny_command marks all denied orders as Denied (closed) in Cache.
                        for o in &orders {
                            let denied_ev = nautilus_model::events::OrderDenied::new(
                                o.trader_id(),
                                o.strategy_id(),
                                o.instrument_id(),
                                o.client_order_id(),
                                err.clone().into(),
                                UUID4::new(),
                                UnixNanos::from(TS_NOW + 2),
                                UnixNanos::from(TS_NOW + 2),
                            );
                            cache.update_order(&OrderEventAny::Denied(denied_ev)).unwrap();
                        }

                        if unrelated_open_e2 {
                            if freeze_before_x3 {
                                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                            }
                            let mut x3 = make_limit_order(
                                inst.id(),
                                "O-I12-L-X3",
                                exit_side,
                                "10",
                                px_x3,
                                x3_reduce_only,
                            );
                            if !x3_reduce_only {
                                x3 = with_exit_marker(x3, Some("EXIT"), None);
                            }
                            let x3 = accept_order(x3);
                            cache.add_order(x3.clone(), None, None, true).unwrap();
                            let res_x3 = gate.check_command(
                                &submit_cmd(&x3),
                                &cache,
                                UnixNanos::from(TS_NOW + 3),
                            );
                            assert!(
                                res_x3.is_ok(),
                                "{plan_side} {bad_shape} x3_ro={x3_reduce_only} freeze={freeze_before_x3}: subsequent valid exit X-3(10) for live E-2 must be admitted: {res_x3:?}"
                            );
                            let fill_x3 = OrderFilled::new(
                                x3.trader_id(),
                                x3.strategy_id(),
                                inst.id(),
                                x3.client_order_id(),
                                x3.venue_order_id().unwrap(),
                                AccountId::from("ALPACA-001"),
                                TradeId::from(format!("T-I12-L-X3-{plan_side}").as_str()),
                                exit_side,
                                OrderType::Limit,
                                Quantity::from("10"),
                                Price::from(px_x3),
                                Currency::USD(),
                                LiquiditySide::Taker,
                                UUID4::new(),
                                UnixNanos::from(TS_NOW + 4),
                                UnixNanos::from(TS_NOW + 4),
                                false,
                                Some(pos_id),
                                Some(Money::from("0.00 USD")),
                                None,
                            );
                            cache
                                .update_order(&OrderEventAny::Filled(fill_x3.clone()))
                                .unwrap();
                            cache.update_position_from_fill(pos_id, &fill_x3).unwrap();
                            assert!(
                                cache
                                    .positions_open(None, Some(&inst.id()), None, None, None)
                                    .is_empty(),
                                "{plan_side} {bad_shape}: E-2 must close cleanly to FLAT(0) with zero flip"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn test_iter12_prefill_oto_single_submit_order_child_exceeds_parent_and_freeze_symmetry() {
    for unrelated_open_e2 in [false, true] {
        for entry_freeze_for_child in [false, true] {
            for child_reduce_only in [true, false] {
                for parent_accepted in [false, true] {
                    for (
                        plan_side,
                        entry_side,
                        exit_side,
                        initial_stop,
                        px_e2,
                        px_e1,
                        px_x1,
                        px_x3,
                    ) in [
                        (
                            "LONG",
                            OrderSide::Buy,
                            OrderSide::Sell,
                            dec!(145.00),
                            "150.00",
                            "147.00",
                            "160.00",
                            "150.05",
                        ),
                        (
                            "SHORT",
                            OrderSide::Sell,
                            OrderSide::Buy,
                            dec!(155.00),
                            "150.00",
                            "153.00",
                            "140.00",
                            "149.95",
                        ),
                    ] {
                        // Case A: Single SubmitOrder(X-1(20, parent=E-1)) > E-1(10) -> REJECTED.
                        {
                            let (inst, mut cache, gate) =
                                setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                            let pos_id =
                                PositionId::from(format!("P-I12-S1-{plan_side}").as_str());
                            if unrelated_open_e2 {
                                let e2 = accept_order(make_limit_order(
                                    inst.id(),
                                    "O-I12-S1-E2",
                                    entry_side,
                                    "10",
                                    px_e2,
                                    false,
                                ));
                                cache.add_order(e2.clone(), None, None, true).unwrap();
                                assert!(
                                    gate.check_command(
                                        &submit_cmd(&e2),
                                        &cache,
                                        UnixNanos::from(TS_NOW),
                                    )
                                    .is_ok()
                                );
                                let fill_e2 = OrderFilled::new(
                                    e2.trader_id(),
                                    e2.strategy_id(),
                                    inst.id(),
                                    e2.client_order_id(),
                                    e2.venue_order_id().unwrap(),
                                    AccountId::from("ALPACA-001"),
                                    TradeId::from(format!("T-I12-S1-E2-{plan_side}").as_str()),
                                    entry_side,
                                    OrderType::Limit,
                                    Quantity::from("10"),
                                    Price::from(px_e2),
                                    Currency::USD(),
                                    LiquiditySide::Taker,
                                    UUID4::new(),
                                    UnixNanos::from(TS_NOW + 1),
                                    UnixNanos::from(TS_NOW + 1),
                                    false,
                                    Some(pos_id),
                                    Some(Money::from("0.00 USD")),
                                    None,
                                );
                                cache
                                    .update_order(&OrderEventAny::Filled(fill_e2.clone()))
                                    .unwrap();
                                let pos = Position::new(&inst, fill_e2);
                                cache
                                    .add_position(&pos, nautilus_model::enums::OmsType::Netting)
                                    .unwrap();
                            }

                            let e1_raw = make_contingent_limit_order(
                                inst.id(),
                                "O-I12-S1-E1",
                                entry_side,
                                "10",
                                px_e1,
                                false,
                                Some(ContingencyType::Oto),
                                None,
                                Some(vec!["O-I12-S1-X1"]),
                                None,
                            );
                            let e1 = if parent_accepted {
                                accept_order(e1_raw)
                            } else {
                                e1_raw
                            };
                            cache.add_order(e1.clone(), None, None, true).unwrap();
                            assert!(
                                gate.check_command(
                                    &submit_cmd(&e1),
                                    &cache,
                                    UnixNanos::from(TS_NOW + 2),
                                )
                                .is_ok()
                            );
                            let expected_reserved =
                                if unrelated_open_e2 { dec!(20) } else { dec!(10) };
                            assert_eq!(gate.reserved_qty("AAPL"), expected_reserved);

                            if entry_freeze_for_child {
                                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                            }

                            let mut x1 = make_contingent_limit_order(
                                inst.id(),
                                "O-I12-S1-X1",
                                exit_side,
                                "20",
                                px_x1,
                                child_reduce_only,
                                None,
                                None,
                                None,
                                Some("O-I12-S1-E1"),
                            );
                            if !child_reduce_only {
                                x1 = with_exit_marker(x1, Some("EXIT"), None);
                            }
                            cache.add_order(x1.clone(), None, None, false).unwrap();
                            let err = gate
                                .check_command(
                                    &submit_cmd(&x1),
                                    &cache,
                                    UnixNanos::from(TS_NOW + 3),
                                )
                                .unwrap_err();
                            assert!(
                                err.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                                "{plan_side} open_e2={unrelated_open_e2} freeze={entry_freeze_for_child} ro={child_reduce_only} acc={parent_accepted}: single SubmitOrder(X-1(20, parent=E-1)) must reject, got: {err}"
                            );
                            assert_eq!(gate.reserved_qty("AAPL"), expected_reserved);
                            assert!(!gate.has_admitted_reservation("O-I12-S1-X1"));
                            let denied_x1 = nautilus_model::events::OrderDenied::new(
                                x1.trader_id(),
                                x1.strategy_id(),
                                x1.instrument_id(),
                                x1.client_order_id(),
                                err.into(),
                                UUID4::new(),
                                UnixNanos::from(TS_NOW + 3),
                                UnixNanos::from(TS_NOW + 3),
                            );
                            cache.update_order(&OrderEventAny::Denied(denied_x1)).unwrap();

                            // Valid exit X-3(10) for E-2 must still succeed and flatten E-2.
                            if unrelated_open_e2 {
                                let mut x3 = make_limit_order(
                                    inst.id(),
                                    "O-I12-S1-X3",
                                    exit_side,
                                    "10",
                                    px_x3,
                                    child_reduce_only,
                                );
                                if !child_reduce_only {
                                    x3 = with_exit_marker(x3, Some("EXIT"), None);
                                }
                                let x3 = accept_order(x3);
                                cache.add_order(x3.clone(), None, None, true).unwrap();
                                assert!(
                                    gate.check_command(
                                        &submit_cmd(&x3),
                                        &cache,
                                        UnixNanos::from(TS_NOW + 4),
                                    )
                                    .is_ok()
                                );
                                let fill_x3 = OrderFilled::new(
                                    x3.trader_id(),
                                    x3.strategy_id(),
                                    inst.id(),
                                    x3.client_order_id(),
                                    x3.venue_order_id().unwrap(),
                                    AccountId::from("ALPACA-001"),
                                    TradeId::from(format!("T-I12-S1-X3-{plan_side}").as_str()),
                                    exit_side,
                                    OrderType::Limit,
                                    Quantity::from("10"),
                                    Price::from(px_x3),
                                    Currency::USD(),
                                    LiquiditySide::Taker,
                                    UUID4::new(),
                                    UnixNanos::from(TS_NOW + 5),
                                    UnixNanos::from(TS_NOW + 5),
                                    false,
                                    Some(pos_id),
                                    Some(Money::from("0.00 USD")),
                                    None,
                                );
                                cache
                                    .update_order(&OrderEventAny::Filled(fill_x3.clone()))
                                    .unwrap();
                                cache.update_position_from_fill(pos_id, &fill_x3).unwrap();
                                assert!(
                                    cache
                                        .positions_open(None, Some(&inst.id()), None, None, None)
                                        .is_empty()
                                );
                            }
                        }

                        // Case B: Sequential single SubmitOrder multi-child [X-1(6), X-2(6)] against E-1(10) -> X-2 REJECTED (6+6=12>10).
                        {
                            let (inst, mut cache, gate) =
                                setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                            if unrelated_open_e2 {
                                open_position_in_cache(
                                    &mut cache,
                                    &inst,
                                    entry_side,
                                    "10",
                                    px_e2,
                                    &format!("P-I12-S2-{plan_side}"),
                                );
                            }
                            let e1_raw = make_contingent_limit_order(
                                inst.id(),
                                "O-I12-S2-E1",
                                entry_side,
                                "10",
                                px_e1,
                                false,
                                Some(ContingencyType::Oto),
                                None,
                                Some(vec!["O-I12-S2-X1", "O-I12-S2-X2"]),
                                None,
                            );
                            let e1 = if parent_accepted {
                                accept_order(e1_raw)
                            } else {
                                e1_raw
                            };
                            cache.add_order(e1.clone(), None, None, true).unwrap();
                            assert!(
                                gate.check_command(
                                    &submit_cmd(&e1),
                                    &cache,
                                    UnixNanos::from(TS_NOW),
                                )
                                .is_ok()
                            );

                            if entry_freeze_for_child {
                                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                            }

                            let x1 = accept_order(make_contingent_limit_order(
                                inst.id(),
                                "O-I12-S2-X1",
                                exit_side,
                                "6",
                                px_x1,
                                true,
                                None,
                                None,
                                None,
                                Some("O-I12-S2-E1"),
                            ));
                            cache.add_order(x1.clone(), None, None, true).unwrap();
                            assert!(
                                gate.check_command(
                                    &submit_cmd(&x1),
                                    &cache,
                                    UnixNanos::from(TS_NOW + 1),
                                )
                                .is_ok()
                            );

                            let mut x2 = make_contingent_limit_order(
                                inst.id(),
                                "O-I12-S2-X2",
                                exit_side,
                                "6",
                                px_x1,
                                child_reduce_only,
                                None,
                                None,
                                None,
                                Some("O-I12-S2-E1"),
                            );
                            if !child_reduce_only {
                                x2 = with_exit_marker(x2, Some("TAKE_PROFIT"), None);
                            }
                            cache.add_order(x2.clone(), None, None, false).unwrap();
                            let err2 = gate
                                .check_command(
                                    &submit_cmd(&x2),
                                    &cache,
                                    UnixNanos::from(TS_NOW + 2),
                                )
                                .unwrap_err();
                            assert!(
                                err2.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                                "{plan_side} open_e2={unrelated_open_e2} freeze={entry_freeze_for_child} ro={child_reduce_only}: cumulative unlinked OTO child 6+6=12>10 must reject, got: {err2}"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn test_iter12_valid_3leg_oto_oco_ouo_bracket_contrast_with_and_without_open_e2() {
    for e2_open in [false, true] {
        for contingency in [ContingencyType::Oco, ContingencyType::Ouo] {
            for submission_mode in ["order_list", "sequential_submit_order"] {
                for x3_ro in [true, false] {
                    for freeze_for_x3 in [false, true] {
                        for (
                            plan_side,
                            entry_side,
                            exit_side,
                            initial_stop,
                            px_e2,
                            px_e1,
                            px_sl,
                            px_tp,
                            px_x3,
                        ) in [
                            (
                                "LONG",
                                OrderSide::Buy,
                                OrderSide::Sell,
                                dec!(145.00),
                                "150.00",
                                "147.00",
                                "145.50",
                                "160.00",
                                "150.05",
                            ),
                            (
                                "SHORT",
                                OrderSide::Sell,
                                OrderSide::Buy,
                                dec!(155.00),
                                "150.00",
                                "153.00",
                                "154.50",
                                "140.00",
                                "149.95",
                            ),
                        ] {
                            let (inst, mut cache, gate) =
                                setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                            let pos_id =
                                PositionId::from(format!("P-I12-V-{plan_side}").as_str());

                            if e2_open {
                                let e2 = accept_order(make_limit_order(
                                    inst.id(),
                                    "O-I12-V-E2",
                                    entry_side,
                                    "10",
                                    px_e2,
                                    false,
                                ));
                                cache.add_order(e2.clone(), None, None, true).unwrap();
                                assert!(
                                    gate.check_command(
                                        &submit_cmd(&e2),
                                        &cache,
                                        UnixNanos::from(TS_NOW),
                                    )
                                    .is_ok()
                                );
                                let fill_e2 = OrderFilled::new(
                                    e2.trader_id(),
                                    e2.strategy_id(),
                                    inst.id(),
                                    e2.client_order_id(),
                                    e2.venue_order_id().unwrap(),
                                    AccountId::from("ALPACA-001"),
                                    TradeId::from(format!("T-I12-V-E2-{plan_side}").as_str()),
                                    entry_side,
                                    OrderType::Limit,
                                    Quantity::from("10"),
                                    Price::from(px_e2),
                                    Currency::USD(),
                                    LiquiditySide::Taker,
                                    UUID4::new(),
                                    UnixNanos::from(TS_NOW + 1),
                                    UnixNanos::from(TS_NOW + 1),
                                    false,
                                    Some(pos_id),
                                    Some(Money::from("0.00 USD")),
                                    None,
                                );
                                cache
                                    .update_order(&OrderEventAny::Filled(fill_e2.clone()))
                                    .unwrap();
                                let pos = Position::new(&inst, fill_e2);
                                cache
                                    .add_position(&pos, nautilus_model::enums::OmsType::Netting)
                                    .unwrap();
                            }

                            let list_id = format!("OL-I12-V-{plan_side}-{contingency:?}");
                            let e1 = accept_order(make_contingent_limit_order(
                                inst.id(),
                                "O-I12-V-E1",
                                entry_side,
                                "10",
                                px_e1,
                                false,
                                Some(ContingencyType::Oto),
                                Some(&list_id),
                                Some(vec!["O-I12-V-SL", "O-I12-V-TP"]),
                                None,
                            ));
                            let sl = make_contingent_stop_market_order(
                                inst.id(),
                                "O-I12-V-SL",
                                exit_side,
                                "10",
                                px_sl,
                                true,
                                Some(contingency),
                                Some(&list_id),
                                Some(vec!["O-I12-V-TP"]),
                                Some("O-I12-V-E1"),
                            );
                            let tp = with_exit_marker(
                                make_contingent_limit_order(
                                    inst.id(),
                                    "O-I12-V-TP",
                                    exit_side,
                                    "10",
                                    px_tp,
                                    false,
                                    Some(contingency),
                                    Some(&list_id),
                                    Some(vec!["O-I12-V-SL"]),
                                    Some("O-I12-V-E1"),
                                ),
                                Some("TAKE_PROFIT"),
                                None,
                            );

                            if submission_mode == "order_list" {
                                let orders = vec![e1, sl, tp];
                                for o in &orders {
                                    cache.add_order(o.clone(), None, None, true).unwrap();
                                }
                                let res = gate.check_command(
                                    &make_mixed_order_list_cmd(&list_id, &orders),
                                    &cache,
                                    UnixNanos::from(TS_NOW + 2),
                                );
                                assert!(
                                    res.is_ok(),
                                    "{plan_side} {contingency:?} e2_open={e2_open} {submission_mode}: valid 3-leg OTO/OCO/OUO bracket (collapsed 10 <= 10) must be admitted: {res:?}"
                                );
                            } else {
                                cache.add_order(e1.clone(), None, None, true).unwrap();
                                assert!(
                                    gate.check_command(
                                        &submit_cmd(&e1),
                                        &cache,
                                        UnixNanos::from(TS_NOW + 2),
                                    )
                                    .is_ok()
                                );
                                cache.add_order(sl.clone(), None, None, true).unwrap();
                                let res_sl = gate.check_command(
                                    &submit_cmd(&sl),
                                    &cache,
                                    UnixNanos::from(TS_NOW + 3),
                                );
                                assert!(
                                    res_sl.is_ok(),
                                    "{plan_side} {contingency:?} e2_open={e2_open} {submission_mode}: SL(10) must be admitted: {res_sl:?}"
                                );
                                cache.add_order(tp.clone(), None, None, true).unwrap();
                                let res_tp = gate.check_command(
                                    &submit_cmd(&tp),
                                    &cache,
                                    UnixNanos::from(TS_NOW + 4),
                                );
                                assert!(
                                    res_tp.is_ok(),
                                    "{plan_side} {contingency:?} e2_open={e2_open} {submission_mode}: TP(10) linked via {contingency:?} to SL(10) must collapse to 10 <= 10 and be admitted: {res_tp:?}"
                                );
                            }

                            let expected_reserved = if e2_open { dec!(20) } else { dec!(10) };
                            assert_eq!(gate.reserved_qty("AAPL"), expected_reserved);
                            assert!(gate.has_admitted_reservation("O-I12-V-E1"));
                            assert!(!gate.has_admitted_reservation("O-I12-V-SL"));
                            assert!(!gate.has_admitted_reservation("O-I12-V-TP"));

                            if e2_open {
                                if freeze_for_x3 {
                                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                                }
                                let mut x3 = make_limit_order(
                                    inst.id(),
                                    "O-I12-V-X3",
                                    exit_side,
                                    "10",
                                    px_x3,
                                    x3_ro,
                                );
                                if !x3_ro {
                                    x3 = with_exit_marker(x3, Some("EXIT"), None);
                                }
                                let x3 = accept_order(x3);
                                cache.add_order(x3.clone(), None, None, true).unwrap();
                                let res_x3 = gate.check_command(
                                    &submit_cmd(&x3),
                                    &cache,
                                    UnixNanos::from(TS_NOW + 5),
                                );
                                assert!(
                                    res_x3.is_ok(),
                                    "{plan_side} {contingency:?} {submission_mode} x3_ro={x3_ro} freeze={freeze_for_x3}: X-3(10) closing E-2 while pre-fill 3-leg bracket rests must be admitted: {res_x3:?}"
                                );
                                let fill_x3 = OrderFilled::new(
                                    x3.trader_id(),
                                    x3.strategy_id(),
                                    inst.id(),
                                    x3.client_order_id(),
                                    x3.venue_order_id().unwrap(),
                                    AccountId::from("ALPACA-001"),
                                    TradeId::from(format!("T-I12-V-X3-{plan_side}").as_str()),
                                    exit_side,
                                    OrderType::Limit,
                                    Quantity::from("10"),
                                    Price::from(px_x3),
                                    Currency::USD(),
                                    LiquiditySide::Taker,
                                    UUID4::new(),
                                    UnixNanos::from(TS_NOW + 6),
                                    UnixNanos::from(TS_NOW + 6),
                                    false,
                                    Some(pos_id),
                                    Some(Money::from("0.00 USD")),
                                    None,
                                );
                                cache
                                    .update_order(&OrderEventAny::Filled(fill_x3.clone()))
                                    .unwrap();
                                cache.update_position_from_fill(pos_id, &fill_x3).unwrap();
                                assert!(
                                    cache
                                        .positions_open(None, Some(&inst.id()), None, None, None)
                                        .is_empty()
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn test_iter12_single_submit_order_accepted_sibling_oco_collapse_contrast() {
    for parent_accepted in [false, true] {
        for freeze_for_children in [false, true] {
            for contingency in [ContingencyType::Oco, ContingencyType::Ouo] {
                for (plan_side, entry_side, exit_side, initial_stop, px_e1, px_sl, px_tp) in [
                    (
                        "LONG",
                        OrderSide::Buy,
                        OrderSide::Sell,
                        dec!(145.00),
                        "147.00",
                        "145.50",
                        "160.00",
                    ),
                    (
                        "SHORT",
                        OrderSide::Sell,
                        OrderSide::Buy,
                        dec!(155.00),
                        "153.00",
                        "154.50",
                        "140.00",
                    ),
                ] {
                    // Positive contrast: SL(10, Accepted) + TP(10, Initialized) via OCO/OUO collapses to 10 <= 10.
                    {
                        let (inst, mut cache, gate) =
                            setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                        let list_id = format!("OL-I12-ACC-{plan_side}-{contingency:?}");
                        let e1_raw = make_contingent_limit_order(
                            inst.id(),
                            "O-I12-ACC-E1",
                            entry_side,
                            "10",
                            px_e1,
                            false,
                            Some(ContingencyType::Oto),
                            Some(&list_id),
                            Some(vec!["O-I12-ACC-SL", "O-I12-ACC-TP"]),
                            None,
                        );
                        let e1 = if parent_accepted {
                            accept_order(e1_raw)
                        } else {
                            e1_raw
                        };
                        cache.add_order(e1.clone(), None, None, true).unwrap();
                        assert!(
                            gate.check_command(&submit_cmd(&e1), &cache, UnixNanos::from(TS_NOW))
                                .is_ok()
                        );

                        if freeze_for_children {
                            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                        }

                        let sl = accept_order(make_contingent_stop_market_order(
                            inst.id(),
                            "O-I12-ACC-SL",
                            exit_side,
                            "10",
                            px_sl,
                            true,
                            Some(contingency),
                            Some(&list_id),
                            Some(vec!["O-I12-ACC-TP"]),
                            Some("O-I12-ACC-E1"),
                        ));
                        cache.add_order(sl.clone(), None, None, true).unwrap();
                        assert!(
                            gate.check_command(
                                &submit_cmd(&sl),
                                &cache,
                                UnixNanos::from(TS_NOW + 1)
                            )
                            .is_ok()
                        );

                        let tp = with_exit_marker(
                            make_contingent_limit_order(
                                inst.id(),
                                "O-I12-ACC-TP",
                                exit_side,
                                "10",
                                px_tp,
                                false,
                                Some(contingency),
                                Some(&list_id),
                                Some(vec!["O-I12-ACC-SL"]),
                                Some("O-I12-ACC-E1"),
                            ),
                            Some("TAKE_PROFIT"),
                            None,
                        );
                        cache.add_order(tp.clone(), None, None, false).unwrap();
                        let res_tp = gate.check_command(
                            &submit_cmd(&tp),
                            &cache,
                            UnixNanos::from(TS_NOW + 2),
                        );
                        assert!(
                            res_tp.is_ok(),
                            "{plan_side} {contingency:?} acc={parent_accepted} freeze={freeze_for_children}: SL(10, Accepted) + TP(10, Initialized) via {contingency:?} must collapse to 10 <= 10 and admit: {res_tp:?}"
                        );
                        assert_eq!(gate.reserved_qty("AAPL"), dec!(10));
                        assert!(!gate.has_admitted_reservation("O-I12-ACC-SL"));
                        assert!(!gate.has_admitted_reservation("O-I12-ACC-TP"));
                    }

                    // Negative contrast: unlinked X-1(6, Accepted) + X-2(6, Initialized) -> REJECTED (6+6=12>10).
                    {
                        let (inst, mut cache, gate) =
                            setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                        let e1_raw = make_contingent_limit_order(
                            inst.id(),
                            "O-I12-UNL-E1",
                            entry_side,
                            "10",
                            px_e1,
                            false,
                            Some(ContingencyType::Oto),
                            None,
                            Some(vec!["O-I12-UNL-X1", "O-I12-UNL-X2"]),
                            None,
                        );
                        let e1 = if parent_accepted {
                            accept_order(e1_raw)
                        } else {
                            e1_raw
                        };
                        cache.add_order(e1.clone(), None, None, true).unwrap();
                        assert!(
                            gate.check_command(&submit_cmd(&e1), &cache, UnixNanos::from(TS_NOW))
                                .is_ok()
                        );

                        if freeze_for_children {
                            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                        }

                        let x1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I12-UNL-X1",
                            exit_side,
                            "6",
                            px_tp,
                            true,
                            None,
                            None,
                            None,
                            Some("O-I12-UNL-E1"),
                        ));
                        cache.add_order(x1.clone(), None, None, true).unwrap();
                        assert!(
                            gate.check_command(
                                &submit_cmd(&x1),
                                &cache,
                                UnixNanos::from(TS_NOW + 1)
                            )
                            .is_ok()
                        );

                        let x2 = with_exit_marker(
                            make_contingent_limit_order(
                                inst.id(),
                                "O-I12-UNL-X2",
                                exit_side,
                                "6",
                                px_tp,
                                false,
                                None,
                                None,
                                None,
                                Some("O-I12-UNL-E1"),
                            ),
                            Some("TAKE_PROFIT"),
                            None,
                        );
                        cache.add_order(x2.clone(), None, None, false).unwrap();
                        let err_x2 = gate
                            .check_command(
                                &submit_cmd(&x2),
                                &cache,
                                UnixNanos::from(TS_NOW + 2),
                            )
                            .unwrap_err();
                        assert!(
                            err_x2.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                            "{plan_side} acc={parent_accepted} freeze={freeze_for_children}: unlinked X-1(6, Accepted) + X-2(6) must reject with REDUCE_ONLY_EXIT_EXCEEDS_POSITION, got: {err_x2}"
                        );
                        assert_eq!(gate.reserved_qty("AAPL"), dec!(10));
                        assert!(!gate.has_admitted_reservation("O-I12-UNL-X2"));
                    }
                }
            }
        }
    }
}

#[test]
fn test_iter12_single_submit_order_ro_false_exit_parent_vs_orphan_side_mismatch() {
    for entry_freeze_for_child in [false, true] {
        for tag in ["EXIT", "TAKE_PROFIT"] {
            for (plan_side, entry_side, exit_side, initial_stop, px_e1, px_tp) in [
                (
                    "LONG",
                    OrderSide::Buy,
                    OrderSide::Sell,
                    dec!(145.00),
                    "147.00",
                    "160.00",
                ),
                (
                    "SHORT",
                    OrderSide::Sell,
                    OrderSide::Buy,
                    dec!(155.00),
                    "153.00",
                    "140.00",
                ),
            ] {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                let e1 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I12-ORPH-E1",
                    entry_side,
                    "10",
                    px_e1,
                    false,
                    Some(ContingencyType::Oto),
                    None,
                    Some(vec!["O-I12-ORPH-CHILD"]),
                    None,
                ));
                cache.add_order(e1.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(&submit_cmd(&e1), &cache, UnixNanos::from(TS_NOW))
                        .is_ok()
                );

                if entry_freeze_for_child {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                // Contrast A: ro=False exit-tagged child WITH parent=E-1 when flat -> ADMITTED.
                let valid_child = with_exit_marker(
                    make_contingent_limit_order(
                        inst.id(),
                        "O-I12-ORPH-CHILD",
                        exit_side,
                        "10",
                        px_tp,
                        false,
                        None,
                        None,
                        None,
                        Some("O-I12-ORPH-E1"),
                    ),
                    Some(tag),
                    None,
                );
                cache.add_order(valid_child.clone(), None, None, false).unwrap();
                let res_valid = gate.check_command(
                    &submit_cmd(&valid_child),
                    &cache,
                    UnixNanos::from(TS_NOW + 1),
                );
                assert!(
                    res_valid.is_ok(),
                    "{plan_side} freeze={entry_freeze_for_child} tag={tag}: single SubmitOrder ro=False exit-tagged child with parent=E-1 when flat must be admitted: {res_valid:?}"
                );

                // Contrast B: orphan ro=False exit-tagged order WITHOUT parent=E-1 when flat -> REJECTED.
                let orphan = with_exit_marker(
                    make_limit_order(
                        inst.id(),
                        "O-I12-ORPH-NONE",
                        exit_side,
                        "10",
                        px_tp,
                        false,
                    ),
                    Some(tag),
                    None,
                );
                cache.add_order(orphan.clone(), None, None, false).unwrap();
                let err_orphan = gate
                    .check_command(
                        &submit_cmd(&orphan),
                        &cache,
                        UnixNanos::from(TS_NOW + 2),
                    )
                    .unwrap_err();
                if entry_freeze_for_child {
                    assert!(
                        err_orphan.contains("SIDE_MISMATCH")
                            || err_orphan.contains("ENTRY_FREEZE_ACTIVE"),
                        "{plan_side} freeze=true tag={tag}: orphan without parent=E-1 must be denied, got: {err_orphan}"
                    );
                } else {
                    assert!(
                        err_orphan.contains("SIDE_MISMATCH"),
                        "{plan_side} freeze=false tag={tag}: orphan without parent=E-1 when flat must reject with SIDE_MISMATCH, got: {err_orphan}"
                    );
                }
                assert_eq!(gate.reserved_qty("AAPL"), dec!(10));
                assert!(!gate.has_admitted_reservation("O-I12-ORPH-NONE"));
            }
        }
    }
}

fn mark_submitted(cache: &mut Cache, order: &mut OrderAny, ts: u64) {
    let sub = OrderSubmitted::new(
        order.trader_id(),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        AccountId::from("ALPACA-001"),
        UUID4::new(),
        UnixNanos::from(ts),
        UnixNanos::from(ts),
    );
    order.apply(OrderEventAny::Submitted(sub.clone())).unwrap();
    cache.update_order(&OrderEventAny::Submitted(sub)).unwrap();
}

fn mark_accepted(cache: &mut Cache, order: &mut OrderAny, ts: u64) {
    if order.status() == OrderStatus::Initialized {
        mark_submitted(cache, order, ts);
    }
    let acc = OrderAccepted::new(
        order.trader_id(),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        VenueOrderId::from(order.client_order_id().as_str()),
        AccountId::from("ALPACA-001"),
        UUID4::new(),
        UnixNanos::from(ts + 1),
        UnixNanos::from(ts + 1),
        false,
    );
    order.apply(OrderEventAny::Accepted(acc.clone())).unwrap();
    cache.update_order(&OrderEventAny::Accepted(acc)).unwrap();
}

#[allow(clippy::too_many_arguments)]
fn apply_fill_to_cache_and_pos(
    cache: &mut Cache,
    inst: &InstrumentAny,
    order: &mut OrderAny,
    side: OrderSide,
    qty: &str,
    px: &str,
    pos_id: PositionId,
    trade_id: &str,
    ts: u64,
    pos_already_exists: bool,
) {
    let fill = OrderFilled::new(
        order.trader_id(),
        order.strategy_id(),
        inst.id(),
        order.client_order_id(),
        order
            .venue_order_id()
            .unwrap_or_else(|| VenueOrderId::from(order.client_order_id().as_str())),
        AccountId::from("ALPACA-001"),
        TradeId::from(trade_id),
        side,
        order.order_type(),
        Quantity::from(qty),
        Price::from(px),
        Currency::USD(),
        LiquiditySide::Taker,
        UUID4::new(),
        UnixNanos::from(ts),
        UnixNanos::from(ts),
        false,
        Some(pos_id),
        Some(Money::from("0.00 USD")),
        None,
    );
    order.apply(OrderEventAny::Filled(fill.clone())).unwrap();
    cache
        .update_order(&OrderEventAny::Filled(fill.clone()))
        .unwrap();
    if pos_already_exists {
        cache.update_position_from_fill(pos_id, &fill).unwrap();
    } else {
        let pos = Position::new(inst, fill);
        cache
            .add_position(&pos, nautilus_model::enums::OmsType::Netting)
            .unwrap();
    }
}

#[test]
fn test_iter13_invariant1_accepted_prefill_oto_sibling_vs_unlinked_exits() {
    for entry_freeze in [false, true] {
        for (plan_side, entry_side, exit_side, initial_stop, px_e2, px_e1, px_tp, px_sl, px_sl_tight, px_exit) in [
            (
                "LONG",
                OrderSide::Buy,
                OrderSide::Sell,
                dec!(145.00),
                "150.00",
                "147.00",
                "160.00",
                "145.50",
                "146.50",
                "149.95",
            ),
            (
                "SHORT",
                OrderSide::Sell,
                OrderSide::Buy,
                dec!(155.00),
                "150.00",
                "153.00",
                "140.00",
                "154.50",
                "153.50",
                "150.05",
            ),
        ] {
            // Non-contingent book (X-1 Accepted while OTO parent E-1 unfilled):
            // 1) SubmitOrder(X-3, 10, ro=True) MUST be ADMITTED and flatten E-2 cleanly.
            {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                let pos_id = PositionId::from(format!("P-I13-1A-{plan_side}-{entry_freeze}").as_str());

                let mut e2 = accept_order(make_limit_order(
                    inst.id(),
                    "O-I13-1A-E2",
                    entry_side,
                    "10",
                    px_e2,
                    false,
                ));
                cache.add_order(e2.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW))
                        .is_ok()
                );
                apply_fill_to_cache_and_pos(
                    &mut cache,
                    &inst,
                    &mut e2,
                    entry_side,
                    "10",
                    px_e2,
                    pos_id,
                    &format!("T-I13-1A-E2-{plan_side}"),
                    TS_NOW + 1,
                    false,
                );

                let e1 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I13-1A-E1",
                    entry_side,
                    "10",
                    px_e1,
                    false,
                    Some(ContingencyType::Oto),
                    Some("OL-I13-1A"),
                    Some(vec!["O-I13-1A-X1"]),
                    None,
                ));
                let mut x1 = with_exit_marker(
                    make_contingent_limit_order(
                        inst.id(),
                        "O-I13-1A-X1",
                        exit_side,
                        "10",
                        px_tp,
                        false,
                        None,
                        Some("OL-I13-1A"),
                        None,
                        Some("O-I13-1A-E1"),
                    ),
                    Some("EXIT"),
                    None,
                );
                cache.add_order(e1.clone(), None, None, true).unwrap();
                cache.add_order(x1.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(
                        &make_mixed_order_list_cmd("OL-I13-1A", &[e1.clone(), x1.clone()]),
                        &cache,
                        UnixNanos::from(TS_NOW + 2),
                    )
                    .is_ok()
                );
                // Non-contingent venue immediately accepts X-1 while E-1 remains unfilled (0/10).
                mark_accepted(&mut cache, &mut x1, TS_NOW + 3);
                assert_eq!(cache.order(&e1.client_order_id()).unwrap().filled_qty(), Quantity::from("0"));
                assert_eq!(cache.order(&x1.client_order_id()).unwrap().status(), OrderStatus::Accepted);

                if entry_freeze {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                let mut x3 = accept_order(make_limit_order(
                    inst.id(),
                    "O-I13-1A-X3",
                    exit_side,
                    "10",
                    px_exit,
                    true,
                ));
                cache.add_order(x3.clone(), None, None, true).unwrap();
                let res_x3 = gate.check_command(
                    &submit_cmd(&x3),
                    &cache,
                    UnixNanos::from(TS_NOW + 5),
                );
                assert!(
                    res_x3.is_ok(),
                    "{plan_side} freeze={entry_freeze}: SubmitOrder(X-3, 10, ro=True) alongside Accepted pre-fill OTO child X-1 must be ADMITTED: {res_x3:?}"
                );
                apply_fill_to_cache_and_pos(
                    &mut cache,
                    &inst,
                    &mut x3,
                    exit_side,
                    "10",
                    px_exit,
                    pos_id,
                    &format!("T-I13-1A-X3-{plan_side}"),
                    TS_NOW + 6,
                    true,
                );
                assert!(
                    cache
                        .positions_open(None, Some(&inst.id()), None, None, None)
                        .is_empty(),
                    "{plan_side} freeze={entry_freeze}: E-2 must flatten cleanly to FLAT(0)"
                );
            }

            // 2) Non-contingent book (X-1 Accepted while OTO parent E-1 unfilled):
            //    SubmitOrder(X-2, 10, ro=False, EXIT) MUST be DENIED, preventing X-1 + X-2 from flipping E-2.
            {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                let pos_id = PositionId::from(format!("P-I13-1B-{plan_side}-{entry_freeze}").as_str());

                let mut e2 = accept_order(make_limit_order(
                    inst.id(),
                    "O-I13-1B-E2",
                    entry_side,
                    "10",
                    px_e2,
                    false,
                ));
                cache.add_order(e2.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW))
                        .is_ok()
                );
                apply_fill_to_cache_and_pos(
                    &mut cache,
                    &inst,
                    &mut e2,
                    entry_side,
                    "10",
                    px_e2,
                    pos_id,
                    &format!("T-I13-1B-E2-{plan_side}"),
                    TS_NOW + 1,
                    false,
                );

                let e1 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I13-1B-E1",
                    entry_side,
                    "10",
                    px_e1,
                    false,
                    Some(ContingencyType::Oto),
                    Some("OL-I13-1B"),
                    Some(vec!["O-I13-1B-X1"]),
                    None,
                ));
                let mut x1 = with_exit_marker(
                    make_contingent_limit_order(
                        inst.id(),
                        "O-I13-1B-X1",
                        exit_side,
                        "10",
                        px_tp,
                        false,
                        None,
                        Some("OL-I13-1B"),
                        None,
                        Some("O-I13-1B-E1"),
                    ),
                    Some("EXIT"),
                    None,
                );
                cache.add_order(e1.clone(), None, None, true).unwrap();
                cache.add_order(x1.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(
                        &make_mixed_order_list_cmd("OL-I13-1B", &[e1, x1.clone()]),
                        &cache,
                        UnixNanos::from(TS_NOW + 2),
                    )
                    .is_ok()
                );
                mark_accepted(&mut cache, &mut x1, TS_NOW + 3);

                if entry_freeze {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                let x2 = with_exit_marker(
                    make_limit_order(inst.id(), "O-I13-1B-X2", exit_side, "10", px_exit, false),
                    Some("EXIT"),
                    None,
                );
                cache.add_order(x2.clone(), None, None, false).unwrap();
                let err_x2 = gate
                    .check_command(&submit_cmd(&x2), &cache, UnixNanos::from(TS_NOW + 5))
                    .unwrap_err();
                assert!(
                    err_x2.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION")
                        || err_x2.contains("SIDE_MISMATCH")
                        || (entry_freeze && err_x2.contains("ENTRY_FREEZE_ACTIVE")),
                    "{plan_side} freeze={entry_freeze}: SubmitOrder(X-2, 10, ro=False, EXIT) alongside Accepted pre-fill OTO X-1 must be DENIED, got: {err_x2}"
                );
                assert!(!gate.has_admitted_reservation("O-I13-1B-X2"));
            }

            // 3) Contrast (support_contingent_orders=True where X-1 stays Submitted):
            //    X-3(10, ro=True), X-3(10, ro=False, EXIT), and ModifyOrder(SL-2) MUST ALL be ADMITTED.
            for x3_ro in [true, false] {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                let pos_id = PositionId::from(
                    format!("P-I13-1C-{plan_side}-{entry_freeze}-{x3_ro}").as_str(),
                );

                let mut e2 = accept_order(make_limit_order(
                    inst.id(),
                    "O-I13-1C-E2",
                    entry_side,
                    "10",
                    px_e2,
                    false,
                ));
                cache.add_order(e2.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW))
                        .is_ok()
                );
                apply_fill_to_cache_and_pos(
                    &mut cache,
                    &inst,
                    &mut e2,
                    entry_side,
                    "10",
                    px_e2,
                    pos_id,
                    &format!("T-I13-1C-E2-{plan_side}-{x3_ro}"),
                    TS_NOW + 1,
                    false,
                );

                let sl2 = accept_order(make_stop_market_order(
                    inst.id(),
                    "O-I13-1C-SL2",
                    exit_side,
                    "10",
                    px_sl,
                    true,
                ));
                cache.add_order(sl2.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(&submit_cmd(&sl2), &cache, UnixNanos::from(TS_NOW + 2))
                        .is_ok()
                );

                let e1 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I13-1C-E1",
                    entry_side,
                    "10",
                    px_e1,
                    false,
                    Some(ContingencyType::Oto),
                    Some("OL-I13-1C"),
                    Some(vec!["O-I13-1C-X1"]),
                    None,
                ));
                let mut x1 = with_exit_marker(
                    make_contingent_limit_order(
                        inst.id(),
                        "O-I13-1C-X1",
                        exit_side,
                        "10",
                        px_tp,
                        false,
                        None,
                        Some("OL-I13-1C"),
                        None,
                        Some("O-I13-1C-E1"),
                    ),
                    Some("EXIT"),
                    None,
                );
                cache.add_order(e1.clone(), None, None, true).unwrap();
                cache.add_order(x1.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(
                        &make_mixed_order_list_cmd("OL-I13-1C", &[e1, x1.clone()]),
                        &cache,
                        UnixNanos::from(TS_NOW + 3),
                    )
                    .is_ok()
                );
                // Contingent venue keeps X-1 in Submitted while E-1 is unfilled.
                mark_submitted(&mut cache, &mut x1, TS_NOW + 4);
                assert_eq!(cache.order(&x1.client_order_id()).unwrap().status(), OrderStatus::Submitted);

                if entry_freeze {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                // ModifyOrder(SL-2) stop tighten MUST be ADMITTED.
                let res_mod = gate.check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(&sl2, None, None, Some(px_sl_tight))),
                    &cache,
                    UnixNanos::from(TS_NOW + 5),
                );
                assert!(
                    res_mod.is_ok(),
                    "{plan_side} freeze={entry_freeze} x3_ro={x3_ro}: ModifyOrder(SL-2) with Submitted pre-fill X-1 must be ADMITTED: {res_mod:?}"
                );

                // Cancel SL-2 before submitting unlinked X-3(10) so cumulative exit exposure
                // against E-2(10) is evaluated cleanly alongside Submitted pre-fill X-1(10).
                let cancel_sl2 = OrderCanceled::new(
                    sl2.trader_id(),
                    sl2.strategy_id(),
                    sl2.instrument_id(),
                    sl2.client_order_id(),
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 6),
                    UnixNanos::from(TS_NOW + 6),
                    false,
                    sl2.venue_order_id(),
                    Some(AccountId::from("ALPACA-001")),
                    None,
                );
                cache.update_order(&OrderEventAny::Canceled(cancel_sl2)).unwrap();

                let mut x3 = make_limit_order(
                    inst.id(),
                    "O-I13-1C-X3",
                    exit_side,
                    "10",
                    px_exit,
                    x3_ro,
                );
                if !x3_ro {
                    x3 = with_exit_marker(x3, Some("EXIT"), None);
                }
                let mut x3 = accept_order(x3);
                cache.add_order(x3.clone(), None, None, true).unwrap();
                let res_x3 = gate.check_command(
                    &submit_cmd(&x3),
                    &cache,
                    UnixNanos::from(TS_NOW + 7),
                );
                assert!(
                    res_x3.is_ok(),
                    "{plan_side} freeze={entry_freeze} x3_ro={x3_ro}: SubmitOrder(X-3, 10, ro={x3_ro}) with Submitted pre-fill X-1 must be ADMITTED: {res_x3:?}"
                );
                apply_fill_to_cache_and_pos(
                    &mut cache,
                    &inst,
                    &mut x3,
                    exit_side,
                    "10",
                    px_exit,
                    pos_id,
                    &format!("T-I13-1C-X3-{plan_side}-{x3_ro}"),
                    TS_NOW + 8,
                    true,
                );
                assert!(
                    cache
                        .positions_open(None, Some(&inst.id()), None, None, None)
                        .is_empty()
                );
            }
        }
    }
}

#[test]
fn test_iter13_invariant2_non_upward_modify_and_batch_on_resting_exits_with_accepted_prefill_oto() {
    for entry_freeze in [false, true] {
        for (
            plan_side,
            entry_side,
            exit_side,
            initial_stop,
            px_e2,
            px_e1,
            px_tp,
            px_sl,
            px_sl_tight,
            px_sl_tight2,
            px_sl_loose,
            px_x2,
            px_x2_mod,
            px_x2_mod2,
        ) in [
            (
                "LONG",
                OrderSide::Buy,
                OrderSide::Sell,
                dec!(145.00),
                "150.00",
                "147.00",
                "160.00",
                "145.50",
                "146.50",
                "147.00",
                "144.00",
                "162.00",
                "161.00",
                "160.50",
            ),
            (
                "SHORT",
                OrderSide::Sell,
                OrderSide::Buy,
                dec!(155.00),
                "150.00",
                "153.00",
                "140.00",
                "154.50",
                "153.50",
                "153.00",
                "156.00",
                "138.00",
                "139.00",
                "139.50",
            ),
        ] {
            // Case A: Standalone resting SL-2(10, ro=True) alongside Accepted pre-fill OTO [E-1(10), X-1(10, ro=False, Accepted)].
            {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                let pos_id = PositionId::from(format!("P-I13-2A-{plan_side}").as_str());
                let mut e2 = accept_order(make_limit_order(
                    inst.id(),
                    "O-I13-2A-E2",
                    entry_side,
                    "10",
                    px_e2,
                    false,
                ));
                cache.add_order(e2.clone(), None, None, true).unwrap();
                assert!(gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW)).is_ok());
                apply_fill_to_cache_and_pos(
                    &mut cache,
                    &inst,
                    &mut e2,
                    entry_side,
                    "10",
                    px_e2,
                    pos_id,
                    &format!("T-I13-2A-E2-{plan_side}"),
                    TS_NOW + 1,
                    false,
                );

                let e1 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I13-2A-E1",
                    entry_side,
                    "10",
                    px_e1,
                    false,
                    Some(ContingencyType::Oto),
                    Some("OL-I13-2A"),
                    Some(vec!["O-I13-2A-X1"]),
                    None,
                ));
                let mut x1 = with_exit_marker(
                    make_contingent_limit_order(
                        inst.id(),
                        "O-I13-2A-X1",
                        exit_side,
                        "10",
                        px_tp,
                        false,
                        None,
                        Some("OL-I13-2A"),
                        None,
                        Some("O-I13-2A-E1"),
                    ),
                    Some("EXIT"),
                    None,
                );
                cache.add_order(e1.clone(), None, None, true).unwrap();
                cache.add_order(x1.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(
                        &make_mixed_order_list_cmd("OL-I13-2A", &[e1, x1.clone()]),
                        &cache,
                        UnixNanos::from(TS_NOW + 2),
                    )
                    .is_ok()
                );
                mark_accepted(&mut cache, &mut x1, TS_NOW + 3);

                let sl2 = accept_order(make_stop_market_order(
                    inst.id(),
                    "O-I13-2A-SL2",
                    exit_side,
                    "10",
                    px_sl,
                    true,
                ));
                cache.add_order(sl2.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(&submit_cmd(&sl2), &cache, UnixNanos::from(TS_NOW + 5))
                        .is_ok()
                );

                if entry_freeze {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                // Non-upward ModifyOrder(SL-2): trigger tighten (qty=None and qty=8) -> ADMITTED.
                let res_tight = gate.check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(&sl2, None, None, Some(px_sl_tight))),
                    &cache,
                    UnixNanos::from(TS_NOW + 6),
                );
                assert!(
                    res_tight.is_ok(),
                    "{plan_side} freeze={entry_freeze}: non-upward ModifyOrder(SL-2) tighten must be ADMITTED: {res_tight:?}"
                );
                let res_down = gate.check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(
                        &sl2,
                        Some("8"),
                        None,
                        Some(px_sl_tight2),
                    )),
                    &cache,
                    UnixNanos::from(TS_NOW + 7),
                );
                assert!(
                    res_down.is_ok(),
                    "{plan_side} freeze={entry_freeze}: downward ModifyOrder(SL-2, 8) must be ADMITTED: {res_down:?}"
                );

                // Stop loosening -> REJECTED.
                let err_loose = gate
                    .check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(&sl2, None, None, Some(px_sl_loose))),
                        &cache,
                        UnixNanos::from(TS_NOW + 8),
                    )
                    .unwrap_err();
                assert!(
                    err_loose.contains("STOP_LOSS_LOOSENING_PROHIBITED")
                        || err_loose.contains("STOP_LOSS_MONOTONICITY_VIOLATION")
                        || err_loose.contains("STOP_LOSS_FLOOR_VIOLATION"),
                    "{plan_side}: stop loosening must reject, got: {err_loose}"
                );

                // Upward qty -> REJECTED.
                let err_up = gate
                    .check_command(
                        &TradingCommand::ModifyOrder(modify_cmd(&sl2, Some("12"), None, Some(px_sl_tight))),
                        &cache,
                        UnixNanos::from(TS_NOW + 9),
                    )
                    .unwrap_err();
                assert!(
                    err_up.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                    "{plan_side}: upward ModifyOrder(SL-2, 12) must reject, got: {err_up}"
                );
            }

            // Case B: Standalone resting X-2(10, ro=True) + ModifyOrders batch on [SL-2(5, ro=True), X-2(5, ro=True)]
            // alongside Accepted pre-fill OTO [E-1(10), X-1(10, ro=False, Accepted)].
            {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                let pos_id = PositionId::from(format!("P-I13-2B-{plan_side}").as_str());
                let mut e2 = accept_order(make_limit_order(
                    inst.id(),
                    "O-I13-2B-E2",
                    entry_side,
                    "10",
                    px_e2,
                    false,
                ));
                cache.add_order(e2.clone(), None, None, true).unwrap();
                assert!(gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW)).is_ok());
                apply_fill_to_cache_and_pos(
                    &mut cache,
                    &inst,
                    &mut e2,
                    entry_side,
                    "10",
                    px_e2,
                    pos_id,
                    &format!("T-I13-2B-E2-{plan_side}"),
                    TS_NOW + 1,
                    false,
                );

                let e1 = accept_order(make_contingent_limit_order(
                    inst.id(),
                    "O-I13-2B-E1",
                    entry_side,
                    "10",
                    px_e1,
                    false,
                    Some(ContingencyType::Oto),
                    Some("OL-I13-2B"),
                    Some(vec!["O-I13-2B-X1"]),
                    None,
                ));
                let mut x1 = with_exit_marker(
                    make_contingent_limit_order(
                        inst.id(),
                        "O-I13-2B-X1",
                        exit_side,
                        "10",
                        px_tp,
                        false,
                        None,
                        Some("OL-I13-2B"),
                        None,
                        Some("O-I13-2B-E1"),
                    ),
                    Some("EXIT"),
                    None,
                );
                cache.add_order(e1.clone(), None, None, true).unwrap();
                cache.add_order(x1.clone(), None, None, true).unwrap();
                assert!(
                    gate.check_command(
                        &make_mixed_order_list_cmd("OL-I13-2B", &[e1, x1.clone()]),
                        &cache,
                        UnixNanos::from(TS_NOW + 2),
                    )
                    .is_ok()
                );
                mark_accepted(&mut cache, &mut x1, TS_NOW + 3);

                let sl2 = accept_order(make_stop_market_order(
                    inst.id(),
                    "O-I13-2B-SL2",
                    exit_side,
                    "5",
                    px_sl,
                    true,
                ));
                let x2 = accept_order(make_limit_order(
                    inst.id(),
                    "O-I13-2B-X2",
                    exit_side,
                    "5",
                    px_x2,
                    true,
                ));
                cache.add_order(sl2.clone(), None, None, true).unwrap();
                cache.add_order(x2.clone(), None, None, true).unwrap();
                assert!(gate.check_command(&submit_cmd(&sl2), &cache, UnixNanos::from(TS_NOW + 5)).is_ok());
                assert!(gate.check_command(&submit_cmd(&x2), &cache, UnixNanos::from(TS_NOW + 6)).is_ok());

                if entry_freeze {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                // Price-only ModifyOrder(X-2) -> ADMITTED.
                let res_x2_px = gate.check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(&x2, None, Some(px_x2_mod), None)),
                    &cache,
                    UnixNanos::from(TS_NOW + 7),
                );
                assert!(
                    res_x2_px.is_ok(),
                    "{plan_side} freeze={entry_freeze}: price-only ModifyOrder(X-2) must be ADMITTED: {res_x2_px:?}"
                );

                // Non-upward ModifyOrders batch -> ADMITTED.
                let batch_ok = BatchModifyOrders::new(
                    sl2.trader_id(),
                    Some(ClientId::from("ALPACA")),
                    sl2.strategy_id(),
                    inst.id(),
                    vec![
                        modify_cmd(&sl2, Some("5"), None, Some(px_sl_tight)),
                        modify_cmd(&x2, Some("4"), Some(px_x2_mod2), None),
                    ],
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 8),
                    None,
                    None,
                );
                let res_batch_ok = gate.check_command(
                    &TradingCommand::ModifyOrders(batch_ok),
                    &cache,
                    UnixNanos::from(TS_NOW + 8),
                );
                assert!(
                    res_batch_ok.is_ok(),
                    "{plan_side} freeze={entry_freeze}: non-upward ModifyOrders([SL-2, X-2]) must be ADMITTED: {res_batch_ok:?}"
                );

                // Upward batch -> REJECTED.
                let batch_up = BatchModifyOrders::new(
                    sl2.trader_id(),
                    Some(ClientId::from("ALPACA")),
                    sl2.strategy_id(),
                    inst.id(),
                    vec![
                        modify_cmd(&sl2, Some("6"), None, Some(px_sl_tight)),
                        modify_cmd(&x2, Some("6"), Some(px_x2_mod2), None),
                    ],
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 9),
                    None,
                    None,
                );
                let err_batch_up = gate
                    .check_command(
                        &TradingCommand::ModifyOrders(batch_up),
                        &cache,
                        UnixNanos::from(TS_NOW + 9),
                    )
                    .unwrap_err();
                assert!(
                    err_batch_up.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                    "{plan_side}: upward ModifyOrders(6+6=12 > 10) must reject, got: {err_batch_up}"
                );

                // Stop-loosening batch -> REJECTED.
                let batch_loose = BatchModifyOrders::new(
                    sl2.trader_id(),
                    Some(ClientId::from("ALPACA")),
                    sl2.strategy_id(),
                    inst.id(),
                    vec![
                        modify_cmd(&sl2, Some("5"), None, Some(px_sl_loose)),
                        modify_cmd(&x2, Some("5"), Some(px_x2_mod2), None),
                    ],
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 10),
                    None,
                    None,
                );
                let err_batch_loose = gate
                    .check_command(
                        &TradingCommand::ModifyOrders(batch_loose),
                        &cache,
                        UnixNanos::from(TS_NOW + 10),
                    )
                    .unwrap_err();
                assert!(
                    err_batch_loose.contains("STOP_LOSS_LOOSENING_PROHIBITED")
                        || err_batch_loose.contains("STOP_LOSS_MONOTONICITY_VIOLATION")
                        || err_batch_loose.contains("STOP_LOSS_FLOOR_VIOLATION"),
                    "{plan_side}: stop-loosening ModifyOrders must reject, got: {err_batch_loose}"
                );
            }
        }
    }
}

#[test]
fn test_iter13_invariant3_active_oto_child_group_cap_prefill_and_partial_fill() {
    for open_e2 in [false, true] {
        for entry_freeze in [false, true] {
            for (plan_side, entry_side, exit_side, initial_stop, px_e2, px_e1, px_tp, px_sl, px_x2_mod) in [
                (
                    "LONG",
                    OrderSide::Buy,
                    OrderSide::Sell,
                    dec!(145.00),
                    "150.00",
                    "147.00",
                    "160.00",
                    "145.50",
                    "159.00",
                ),
                (
                    "SHORT",
                    OrderSide::Sell,
                    OrderSide::Buy,
                    dec!(155.00),
                    "150.00",
                    "153.00",
                    "140.00",
                    "154.50",
                    "141.00",
                ),
            ] {
                // Case A: E-1(0/10 pre-fill & 4/10 partial-fill) + X-1(4, parent=E-1) + SubmitOrder(X-2, 16, ro=False, EXIT, parent=E-1) -> DENIED (4+16=20>10),
                //         and ModifyOrder(X-1, 14) (even with E-2(10) open) -> DENIED (14 > 10),
                //         while X-2(6, parent=E-1) -> ADMITTED (4+6=10 <= 10).
                for parent_fill_qty in ["0", "4"] {
                    let (inst, mut cache, gate) =
                        setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                    let pos_id = PositionId::from(
                        format!("P-I13-3A-{plan_side}-{open_e2}-{entry_freeze}-{parent_fill_qty}").as_str(),
                    );
                    if open_e2 {
                        let mut e2 = accept_order(make_limit_order(
                            inst.id(),
                            "O-I13-3A-E2",
                            entry_side,
                            "10",
                            px_e2,
                            false,
                        ));
                        cache.add_order(e2.clone(), None, None, true).unwrap();
                        assert!(gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW)).is_ok());
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut e2,
                            entry_side,
                            "10",
                            px_e2,
                            pos_id,
                            &format!("T-I13-3A-E2-{plan_side}-{parent_fill_qty}"),
                            TS_NOW + 1,
                            false,
                        );
                    }

                    let mut e1 = accept_order(make_contingent_limit_order(
                        inst.id(),
                        "O-I13-3A-E1",
                        entry_side,
                        "10",
                        px_e1,
                        false,
                        Some(ContingencyType::Oto),
                        None,
                        Some(vec!["O-I13-3A-X1", "O-I13-3A-X2-OVR", "O-I13-3A-X2-OK"]),
                        None,
                    ));
                    cache.add_order(e1.clone(), None, None, true).unwrap();
                    assert!(
                        gate.check_command(&submit_cmd(&e1), &cache, UnixNanos::from(TS_NOW + 2))
                            .is_ok()
                    );
                    if parent_fill_qty == "4" {
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut e1,
                            entry_side,
                            "4",
                            px_e1,
                            pos_id,
                            &format!("T-I13-3A-E1-{plan_side}"),
                            TS_NOW + 3,
                            open_e2,
                        );
                        assert_eq!(cache.order(&e1.client_order_id()).unwrap().leaves_qty(), Quantity::from("6"));
                    } else {
                        assert_eq!(cache.order(&e1.client_order_id()).unwrap().leaves_qty(), Quantity::from("10"));
                    }

                    if entry_freeze {
                        gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                    }

                    let x1 = accept_order(make_contingent_limit_order(
                        inst.id(),
                        "O-I13-3A-X1",
                        exit_side,
                        "4",
                        px_tp,
                        true,
                        None,
                        None,
                        None,
                        Some("O-I13-3A-E1"),
                    ));
                    cache.add_order(x1.clone(), None, None, true).unwrap();
                    assert!(
                        gate.check_command(&submit_cmd(&x1), &cache, UnixNanos::from(TS_NOW + 4))
                            .is_ok()
                    );

                    let x2_over = with_exit_marker(
                        make_contingent_limit_order(
                            inst.id(),
                            "O-I13-3A-X2-OVR",
                            exit_side,
                            "16",
                            px_tp,
                            false,
                            None,
                            None,
                            None,
                            Some("O-I13-3A-E1"),
                        ),
                        Some("EXIT"),
                        None,
                    );
                    cache.add_order(x2_over.clone(), None, None, false).unwrap();
                    let err_sub = gate
                        .check_command(&submit_cmd(&x2_over), &cache, UnixNanos::from(TS_NOW + 5))
                        .unwrap_err();
                    assert!(
                        err_sub.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                        "{plan_side} open_e2={open_e2} freeze={entry_freeze} pfill={parent_fill_qty}: X-2(16, parent=E-1) alongside X-1(4) must reject with REDUCE_ONLY_EXIT_EXCEEDS_POSITION, got: {err_sub}"
                    );
                    let denied_x2_over = nautilus_model::events::OrderDenied::new(
                        x2_over.trader_id(),
                        x2_over.strategy_id(),
                        x2_over.instrument_id(),
                        x2_over.client_order_id(),
                        err_sub.into(),
                        UUID4::new(),
                        UnixNanos::from(TS_NOW + 5),
                        UnixNanos::from(TS_NOW + 5),
                    );
                    cache.update_order(&OrderEventAny::Denied(denied_x2_over)).unwrap();

                    // ModifyOrder(X-1, 14) -> DENIED even if E-2(10) is open (14 > E-1.qty=10).
                    let err_mod = gate
                        .check_command(
                            &TradingCommand::ModifyOrder(modify_cmd(
                                &x1,
                                Some("14"),
                                Some(px_x2_mod),
                                None,
                            )),
                            &cache,
                            UnixNanos::from(TS_NOW + 6),
                        )
                        .unwrap_err();
                    assert!(
                        err_mod.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION")
                            || err_mod.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                        "{plan_side} open_e2={open_e2} freeze={entry_freeze} pfill={parent_fill_qty}: ModifyOrder(X-1, 14) when E-1 has leaves>0 must reject, got: {err_mod}"
                    );

                    // Valid X-2(6, parent=E-1) when open_pos >= 10 (open_e2=true): 4+6=10 <= 10 -> ADMITTED.
                    if open_e2 {
                        let x2_ok = with_exit_marker(
                            make_contingent_limit_order(
                                inst.id(),
                                "O-I13-3A-X2-OK",
                                exit_side,
                                "6",
                                px_tp,
                                false,
                                None,
                                None,
                                None,
                                Some("O-I13-3A-E1"),
                            ),
                            Some("EXIT"),
                            None,
                        );
                        cache.add_order(x2_ok.clone(), None, None, false).unwrap();
                        let res_ok = gate.check_command(
                            &submit_cmd(&x2_ok),
                            &cache,
                            UnixNanos::from(TS_NOW + 7),
                        );
                        assert!(
                            res_ok.is_ok(),
                            "{plan_side} open_e2=true freeze={entry_freeze} pfill={parent_fill_qty}: X-2(6, parent=E-1) alongside X-1(4) (4+6=10 <= 10) must be ADMITTED: {res_ok:?}"
                        );
                    }
                }

                // Case B: Closed already-filled OTO child X-1(4, filled=4, parent=E-1) + SubmitOrder(X-2, 7, parent=E-1)
                //         -> DENIED (4+7=11 > 10), while X-2(6, parent=E-1) -> ADMITTED (4+6=10 <= 10) when open_pos >= 6.
                //         Also verify ModifyOrders batch splitting children across E-1 exceeding 10 -> DENIED.
                {
                    let (inst, mut cache, gate) =
                        setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                    let pos_id = PositionId::from(
                        format!("P-I13-3B-{plan_side}-{open_e2}-{entry_freeze}").as_str(),
                    );
                    if open_e2 {
                        let mut e2 = accept_order(make_limit_order(
                            inst.id(),
                            "O-I13-3B-E2",
                            entry_side,
                            "10",
                            px_e2,
                            false,
                        ));
                        cache.add_order(e2.clone(), None, None, true).unwrap();
                        assert!(gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW)).is_ok());
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut e2,
                            entry_side,
                            "10",
                            px_e2,
                            pos_id,
                            &format!("T-I13-3B-E2-{plan_side}"),
                            TS_NOW + 1,
                            false,
                        );
                    }

                    let mut e1 = accept_order(make_contingent_limit_order(
                        inst.id(),
                        "O-I13-3B-E1",
                        entry_side,
                        "10",
                        px_e1,
                        false,
                        Some(ContingencyType::Oto),
                        None,
                        Some(vec!["O-I13-3B-X1", "O-I13-3B-X2-OVR", "O-I13-3B-X2-OK"]),
                        None,
                    ));
                    cache.add_order(e1.clone(), None, None, true).unwrap();
                    assert!(
                        gate.check_command(&submit_cmd(&e1), &cache, UnixNanos::from(TS_NOW + 2))
                            .is_ok()
                    );
                    // Partial fill E-1: 4/10 (leaves=6).
                    apply_fill_to_cache_and_pos(
                        &mut cache,
                        &inst,
                        &mut e1,
                        entry_side,
                        "4",
                        px_e1,
                        pos_id,
                        &format!("T-I13-3B-E1-{plan_side}"),
                        TS_NOW + 3,
                        open_e2,
                    );

                    // X-1(4, parent=E-1) fills completely (status=FILLED, filled=4, leaves=0).
                    let mut x1 = accept_order(make_contingent_limit_order(
                        inst.id(),
                        "O-I13-3B-X1",
                        exit_side,
                        "4",
                        px_tp,
                        true,
                        None,
                        None,
                        None,
                        Some("O-I13-3B-E1"),
                    ));
                    cache.add_order(x1.clone(), None, None, true).unwrap();
                    assert!(gate.check_command(&submit_cmd(&x1), &cache, UnixNanos::from(TS_NOW + 4)).is_ok());
                    apply_fill_to_cache_and_pos(
                        &mut cache,
                        &inst,
                        &mut x1,
                        exit_side,
                        "4",
                        px_tp,
                        pos_id,
                        &format!("T-I13-3B-X1-{plan_side}"),
                        TS_NOW + 5,
                        true,
                    );
                    assert_eq!(cache.order(&x1.client_order_id()).unwrap().status(), OrderStatus::Filled);

                    if entry_freeze {
                        gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                    }

                    let x2_over = with_exit_marker(
                        make_contingent_limit_order(
                            inst.id(),
                            "O-I13-3B-X2-OVR",
                            exit_side,
                            "7",
                            px_tp,
                            false,
                            None,
                            None,
                            None,
                            Some("O-I13-3B-E1"),
                        ),
                        Some("EXIT"),
                        None,
                    );
                    cache.add_order(x2_over.clone(), None, None, false).unwrap();
                    let err_closed = gate
                        .check_command(&submit_cmd(&x2_over), &cache, UnixNanos::from(TS_NOW + 6))
                        .unwrap_err();
                    assert!(
                        err_closed.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                        "{plan_side} open_e2={open_e2} freeze={entry_freeze}: closed X-1(4, filled=4) + X-2(7) = 11 > 10 must reject with REDUCE_ONLY_EXIT_EXCEEDS_POSITION, got: {err_closed}"
                    );
                    let denied_x2 = nautilus_model::events::OrderDenied::new(
                        x2_over.trader_id(),
                        x2_over.strategy_id(),
                        x2_over.instrument_id(),
                        x2_over.client_order_id(),
                        err_closed.into(),
                        UUID4::new(),
                        UnixNanos::from(TS_NOW + 6),
                        UnixNanos::from(TS_NOW + 6),
                    );
                    cache.update_order(&OrderEventAny::Denied(denied_x2)).unwrap();

                    // Contrast while E-1 still has leaves=6: X-2(6, parent=E-1) -> 4+6=10 <= 10 -> ADMITTED when open_e2=true (open_pos=10).
                    if open_e2 {
                        let x2_ok = with_exit_marker(
                            make_contingent_limit_order(
                                inst.id(),
                                "O-I13-3B-X2-OK",
                                exit_side,
                                "6",
                                px_tp,
                                false,
                                None,
                                None,
                                None,
                                Some("O-I13-3B-E1"),
                            ),
                            Some("EXIT"),
                            None,
                        );
                        cache.add_order(x2_ok.clone(), None, None, false).unwrap();
                        let res_x2_ok = gate.check_command(
                            &submit_cmd(&x2_ok),
                            &cache,
                            UnixNanos::from(TS_NOW + 6),
                        );
                        assert!(
                            res_x2_ok.is_ok(),
                            "{plan_side} open_e2=true freeze={entry_freeze}: closed X-1(4) + X-2(6, parent=E-1) = 10 <= 10 must be ADMITTED: {res_x2_ok:?}"
                        );
                        // Cancel x2_ok before testing post-fill X-3(16) below.
                        let cancel_x2_ok = OrderCanceled::new(
                            x2_ok.trader_id(),
                            x2_ok.strategy_id(),
                            x2_ok.instrument_id(),
                            x2_ok.client_order_id(),
                            UUID4::new(),
                            UnixNanos::from(TS_NOW + 6),
                            UnixNanos::from(TS_NOW + 6),
                            false,
                            None,
                            Some(AccountId::from("ALPACA-001")),
                            None,
                        );
                        cache.update_order(&OrderEventAny::Canceled(cancel_x2_ok)).unwrap();
                    }

                    // Once E-1 fills remaining 6/10 (parent_leaves == 0), post-fill exit for full remaining position
                    // (6 if !open_e2, 16 if open_e2) MUST be ADMITTED!
                    apply_fill_to_cache_and_pos(
                        &mut cache,
                        &inst,
                        &mut e1,
                        entry_side,
                        "6",
                        px_e1,
                        pos_id,
                        &format!("T-I13-3B-E1F-{plan_side}"),
                        TS_NOW + 7,
                        open_e2,
                    );
                    assert_eq!(cache.order(&e1.client_order_id()).unwrap().leaves_qty(), Quantity::from("0"));
                    let remaining_pos = if open_e2 { "16" } else { "6" };
                    let mut x3_post = accept_order(make_limit_order(
                        inst.id(),
                        "O-I13-3B-X3-POST",
                        exit_side,
                        remaining_pos,
                        px_e2,
                        true,
                    ));
                    cache.add_order(x3_post.clone(), None, None, true).unwrap();
                    let res_post = gate.check_command(
                        &submit_cmd(&x3_post),
                        &cache,
                        UnixNanos::from(TS_NOW + 8),
                    );
                    assert!(
                        res_post.is_ok(),
                        "{plan_side} open_e2={open_e2} freeze={entry_freeze}: post-fill exit X-3({remaining_pos}) after E-1 leaves==0 must be ADMITTED: {res_post:?}"
                    );
                    apply_fill_to_cache_and_pos(
                        &mut cache,
                        &inst,
                        &mut x3_post,
                        exit_side,
                        remaining_pos,
                        px_e2,
                        pos_id,
                        &format!("T-I13-3B-X3F-{plan_side}"),
                        TS_NOW + 9,
                        true,
                    );
                    assert!(cache.positions_open(None, Some(&inst.id()), None, None, None).is_empty());
                }

                // Case C: ModifyOrders batch splitting children across open E-1(4/10) exceeding 10 -> DENIED (even with E-2(10) open, 6+5=11 <= 14 open_pos, > 10 E-1.qty!),
                //         while valid partial-fill resizes (4 -> 8 -> 10 <= 10) and OCO/OUO bracket (SL-1(10)+TP-1(10)) -> ADMITTED.
                {
                    let (inst, mut cache, gate) =
                        setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                    let pos_id = PositionId::from(
                        format!("P-I13-3C-{plan_side}-{open_e2}-{entry_freeze}").as_str(),
                    );
                    if open_e2 {
                        let mut e2 = accept_order(make_limit_order(
                            inst.id(),
                            "O-I13-3C-E2",
                            entry_side,
                            "10",
                            px_e2,
                            false,
                        ));
                        cache.add_order(e2.clone(), None, None, true).unwrap();
                        assert!(gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW)).is_ok());
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut e2,
                            entry_side,
                            "10",
                            px_e2,
                            pos_id,
                            &format!("T-I13-3C-E2-{plan_side}"),
                            TS_NOW,
                            false,
                        );
                    }
                    let mut e1 = accept_order(make_contingent_limit_order(
                        inst.id(),
                        "O-I13-3C-E1",
                        entry_side,
                        "10",
                        px_e1,
                        false,
                        Some(ContingencyType::Oto),
                        None,
                        Some(vec!["O-I13-3C-X1", "O-I13-3C-X2"]),
                        None,
                    ));
                    cache.add_order(e1.clone(), None, None, true).unwrap();
                    assert!(gate.check_command(&submit_cmd(&e1), &cache, UnixNanos::from(TS_NOW + 1)).is_ok());
                    apply_fill_to_cache_and_pos(
                        &mut cache,
                        &inst,
                        &mut e1,
                        entry_side,
                        "4",
                        px_e1,
                        pos_id,
                        &format!("T-I13-3C-E1-{plan_side}"),
                        TS_NOW + 1,
                        open_e2,
                    );
                    if entry_freeze {
                        gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                    }

                    let mut x1 = accept_order(make_contingent_limit_order(
                        inst.id(),
                        "O-I13-3C-X1",
                        exit_side,
                        "2",
                        px_tp,
                        true,
                        None,
                        None,
                        None,
                        Some("O-I13-3C-E1"),
                    ));
                    let x2 = accept_order(make_contingent_limit_order(
                        inst.id(),
                        "O-I13-3C-X2",
                        exit_side,
                        "2",
                        px_tp,
                        true,
                        None,
                        None,
                        None,
                        Some("O-I13-3C-E1"),
                    ));
                    cache.add_order(x1.clone(), None, None, true).unwrap();
                    cache.add_order(x2.clone(), None, None, true).unwrap();
                    assert!(gate.check_command(&submit_cmd(&x1), &cache, UnixNanos::from(TS_NOW + 2)).is_ok());
                    assert!(gate.check_command(&submit_cmd(&x2), &cache, UnixNanos::from(TS_NOW + 3)).is_ok());

                    // BatchModifyOrders([(X-1, 6), (X-2, 5)]) -> 6 + 5 = 11 > 10 -> DENIED.
                    let over_batch = BatchModifyOrders::new(
                        x1.trader_id(),
                        Some(ClientId::from("ALPACA")),
                        x1.strategy_id(),
                        inst.id(),
                        vec![
                            modify_cmd(&x1, Some("6"), Some(px_x2_mod), None),
                            modify_cmd(&x2, Some("5"), Some(px_x2_mod), None),
                        ],
                        UUID4::new(),
                        UnixNanos::from(TS_NOW + 4),
                        None,
                        None,
                    );
                    let err_batch = gate
                        .check_command(
                            &TradingCommand::ModifyOrders(over_batch),
                            &cache,
                            UnixNanos::from(TS_NOW + 4),
                        )
                        .unwrap_err();
                    assert!(
                        err_batch.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION")
                            || err_batch.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                        "{plan_side} open_e2={open_e2}: ModifyOrders(6+5=11 > 10) across E-1 must reject, got: {err_batch}"
                    );

                    // Valid BatchModifyOrders([(X-1, 5), (X-2, 5)]) when open_e2=true (5+5=10 <= 10 and <= 14 open_pos) -> ADMITTED.
                    if open_e2 {
                        let ok_batch = BatchModifyOrders::new(
                            x1.trader_id(),
                            Some(ClientId::from("ALPACA")),
                            x1.strategy_id(),
                            inst.id(),
                            vec![
                                modify_cmd(&x1, Some("5"), Some(px_x2_mod), None),
                                modify_cmd(&x2, Some("5"), Some(px_x2_mod), None),
                            ],
                            UUID4::new(),
                            UnixNanos::from(TS_NOW + 4),
                            None,
                            None,
                        );
                        let res_ok_batch = gate.check_command(
                            &TradingCommand::ModifyOrders(ok_batch),
                            &cache,
                            UnixNanos::from(TS_NOW + 4),
                        );
                        assert!(
                            res_ok_batch.is_ok(),
                            "{plan_side} open_e2=true: ModifyOrders(5+5=10 <= 10) across E-1 must be ADMITTED: {res_ok_batch:?}"
                        );
                    }

                    // Cancel X-2, resize X-1 along OrderManager partial-fill sequence: 2 -> 4 -> 8 -> 10 <= 10.
                    let cancel_x2 = OrderCanceled::new(
                        x2.trader_id(),
                        x2.strategy_id(),
                        x2.instrument_id(),
                        x2.client_order_id(),
                        UUID4::new(),
                        UnixNanos::from(TS_NOW + 5),
                        UnixNanos::from(TS_NOW + 5),
                        false,
                        x2.venue_order_id(),
                        Some(AccountId::from("ALPACA-001")),
                        None,
                    );
                    cache.update_order(&OrderEventAny::Canceled(cancel_x2)).unwrap();

                    let upd_4 = OrderUpdated::new(
                        x1.trader_id(),
                        x1.strategy_id(),
                        x1.instrument_id(),
                        x1.client_order_id(),
                        Quantity::from("4"),
                        UUID4::new(),
                        UnixNanos::from(TS_NOW + 6),
                        UnixNanos::from(TS_NOW + 6),
                        false,
                        x1.venue_order_id(),
                        Some(AccountId::from("ALPACA-001")),
                        None,
                        None,
                        None,
                        false,
                    );
                    assert!(
                        gate.check_command(
                            &TradingCommand::ModifyOrder(modify_cmd(&x1, Some("4"), None, None)),
                            &cache,
                            UnixNanos::from(TS_NOW + 6),
                        )
                        .is_ok()
                    );
                    x1.apply(OrderEventAny::Updated(upd_4.clone())).unwrap();
                    cache.update_order(&OrderEventAny::Updated(upd_4)).unwrap();

                    // E-1 fills +4 -> 8/10.
                    apply_fill_to_cache_and_pos(
                        &mut cache,
                        &inst,
                        &mut e1,
                        entry_side,
                        "4",
                        px_e1,
                        pos_id,
                        &format!("T-I13-3C-E1-8-{plan_side}"),
                        TS_NOW + 7,
                        true,
                    );
                    assert!(
                        gate.check_command(
                            &TradingCommand::ModifyOrder(modify_cmd(&x1, Some("8"), None, None)),
                            &cache,
                            UnixNanos::from(TS_NOW + 8),
                        )
                        .is_ok()
                    );
                    let upd_8 = OrderUpdated::new(
                        x1.trader_id(),
                        x1.strategy_id(),
                        x1.instrument_id(),
                        x1.client_order_id(),
                        Quantity::from("8"),
                        UUID4::new(),
                        UnixNanos::from(TS_NOW + 8),
                        UnixNanos::from(TS_NOW + 8),
                        false,
                        x1.venue_order_id(),
                        Some(AccountId::from("ALPACA-001")),
                        None,
                        None,
                        None,
                        false,
                    );
                    x1.apply(OrderEventAny::Updated(upd_8.clone())).unwrap();
                    cache.update_order(&OrderEventAny::Updated(upd_8)).unwrap();

                    // E-1 fills +2 -> 10/10.
                    apply_fill_to_cache_and_pos(
                        &mut cache,
                        &inst,
                        &mut e1,
                        entry_side,
                        "2",
                        px_e1,
                        pos_id,
                        &format!("T-I13-3C-E1-10-{plan_side}"),
                        TS_NOW + 9,
                        true,
                    );
                    assert!(
                        gate.check_command(
                            &TradingCommand::ModifyOrder(modify_cmd(&x1, Some("10"), None, None)),
                            &cache,
                            UnixNanos::from(TS_NOW + 10),
                        )
                        .is_ok()
                    );
                }

                // Case D: Valid OCO/OUO bracket children (SL-1(10) + TP-1(10) collapsing to 10 <= 10)
                //         while E-1(4/10) is partially filled (both with and without open E-2(10)).
                for cont in [ContingencyType::Oco, ContingencyType::Ouo] {
                    let (inst, mut cache, gate) =
                        setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                    let pos_id = PositionId::from(
                        format!("P-I13-3D-{plan_side}-{cont:?}-{open_e2}-{entry_freeze}").as_str(),
                    );
                    if open_e2 {
                        let mut e2 = accept_order(make_limit_order(
                            inst.id(),
                            "O-I13-3D-E2",
                            entry_side,
                            "10",
                            px_e2,
                            false,
                        ));
                        cache.add_order(e2.clone(), None, None, true).unwrap();
                        assert!(gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW)).is_ok());
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut e2,
                            entry_side,
                            "10",
                            px_e2,
                            pos_id,
                            &format!("T-I13-3D-E2-{plan_side}-{cont:?}"),
                            TS_NOW,
                            false,
                        );
                    }
                    let mut e1 = accept_order(make_contingent_limit_order(
                        inst.id(),
                        "O-I13-3D-E1",
                        entry_side,
                        "10",
                        px_e1,
                        false,
                        Some(ContingencyType::Oto),
                        Some("OL-I13-3D"),
                        Some(vec!["O-I13-3D-SL1", "O-I13-3D-TP1"]),
                        None,
                    ));
                    cache.add_order(e1.clone(), None, None, true).unwrap();
                    assert!(gate.check_command(&submit_cmd(&e1), &cache, UnixNanos::from(TS_NOW + 1)).is_ok());
                    apply_fill_to_cache_and_pos(
                        &mut cache,
                        &inst,
                        &mut e1,
                        entry_side,
                        "4",
                        px_e1,
                        pos_id,
                        &format!("T-I13-3D-E1-{plan_side}-{cont:?}"),
                        TS_NOW + 1,
                        open_e2,
                    );
                    if entry_freeze {
                        gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                    }

                    let sl1 = make_contingent_stop_market_order(
                        inst.id(),
                        "O-I13-3D-SL1",
                        exit_side,
                        "10",
                        px_sl,
                        true,
                        Some(cont),
                        Some("OL-I13-3D"),
                        Some(vec!["O-I13-3D-TP1"]),
                        Some("O-I13-3D-E1"),
                    );
                    let tp1 = with_exit_marker(
                        make_contingent_limit_order(
                            inst.id(),
                            "O-I13-3D-TP1",
                            exit_side,
                            "10",
                            px_tp,
                            false,
                            Some(cont),
                            Some("OL-I13-3D"),
                            Some(vec!["O-I13-3D-SL1"]),
                            Some("O-I13-3D-E1"),
                        ),
                        Some("TAKE_PROFIT"),
                        None,
                    );
                    cache.add_order(sl1.clone(), None, None, true).unwrap();
                    cache.add_order(tp1.clone(), None, None, true).unwrap();
                    let res_sl = gate.check_command(&submit_cmd(&sl1), &cache, UnixNanos::from(TS_NOW + 2));
                    assert!(
                        res_sl.is_ok(),
                        "{plan_side} {cont:?}: SL-1(10, parent=E-1(4/10)) must be ADMITTED: {res_sl:?}"
                    );
                    let res_tp = gate.check_command(&submit_cmd(&tp1), &cache, UnixNanos::from(TS_NOW + 3));
                    assert!(
                        res_tp.is_ok(),
                        "{plan_side} {cont:?}: TP-1(10, parent=E-1(4/10), linked {cont:?} to SL-1) must collapse to 10 <= 10 and be ADMITTED: {res_tp:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn test_iter14_closed_oco_ouo_oto_child_additive_not_collapsed_while_parent_open() {
    for open_e2 in [false, true] {
        for entry_freeze in [false, true] {
            for cont in [ContingencyType::Oco, ContingencyType::Ouo] {
                for (
                    plan_side,
                    entry_side,
                    exit_side,
                    initial_stop,
                    px_e2,
                    px_e1,
                    px_x1,
                    px_x2,
                    px_x2_mod,
                ) in [
                    (
                        "LONG",
                        OrderSide::Buy,
                        OrderSide::Sell,
                        dec!(145.00),
                        "150.00",
                        "149.00",
                        "152.00",
                        "160.00",
                        "159.00",
                    ),
                    (
                        "SHORT",
                        OrderSide::Sell,
                        OrderSide::Buy,
                        dec!(155.00),
                        "150.00",
                        "151.00",
                        "148.00",
                        "140.00",
                        "141.00",
                    ),
                ] {
                    // Cases 1 & 4: Closed FILLED OCO/OUO sibling X-1(6, FILLED, parent=E-1(8/10))
                    // + SubmitOrder(X-2, 10, ro=True/False, parent=E-1, OCO/OUO with X-1)
                    //   -> DENIED (REDUCE_ONLY_EXIT_EXCEEDS_POSITION, 6 + 10 = 16 > 10),
                    // whereas Valid Contrast SubmitOrder(X-2-OK, 4, ro=True/False, parent=E-1, OCO/OUO with X-1)
                    //   -> ADMITTED (6 + 4 = 10 <= 10).
                    for x2_ro in [true, false] {
                        let (inst, mut cache, gate) =
                            setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                        let pos_id = PositionId::from(
                            format!(
                                "P-I14-C14-{plan_side}-{cont:?}-{open_e2}-{entry_freeze}-{x2_ro}"
                            )
                            .as_str(),
                        );
                        if open_e2 {
                            let mut e2 = accept_order(make_limit_order(
                                inst.id(),
                                "O-I14-C14-E2",
                                entry_side,
                                "10",
                                px_e2,
                                false,
                            ));
                            cache.add_order(e2.clone(), None, None, true).unwrap();
                            assert!(
                                gate.check_command(
                                    &submit_cmd(&e2),
                                    &cache,
                                    UnixNanos::from(TS_NOW),
                                )
                                .is_ok()
                            );
                            apply_fill_to_cache_and_pos(
                                &mut cache,
                                &inst,
                                &mut e2,
                                entry_side,
                                "10",
                                px_e2,
                                pos_id,
                                &format!("T-I14-C14-E2-{plan_side}-{cont:?}-{x2_ro}"),
                                TS_NOW + 1,
                                false,
                            );
                        }

                        let list_id = format!("OL-I14-C14-{plan_side}-{cont:?}-{x2_ro}");
                        let mut e1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I14-C14-E1",
                            entry_side,
                            "10",
                            px_e1,
                            false,
                            Some(ContingencyType::Oto),
                            Some(&list_id),
                            Some(vec!["O-I14-C14-X1", "O-I14-C14-X2", "O-I14-C14-X2-OK"]),
                            None,
                        ));
                        cache.add_order(e1.clone(), None, None, true).unwrap();
                        assert!(
                            gate.check_command(
                                &submit_cmd(&e1),
                                &cache,
                                UnixNanos::from(TS_NOW + 2),
                            )
                            .is_ok()
                        );
                        // Partial fill E-1: 8/10 (parent_leaves = 2 > 0).
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut e1,
                            entry_side,
                            "8",
                            px_e1,
                            pos_id,
                            &format!("T-I14-C14-E1-{plan_side}-{cont:?}-{x2_ro}"),
                            TS_NOW + 3,
                            open_e2,
                        );
                        assert_eq!(
                            cache.order(&e1.client_order_id()).unwrap().leaves_qty(),
                            Quantity::from("2")
                        );

                        // X-1(6, parent=E-1, OCO/OUO with X-2) fills completely (status=FILLED).
                        let mut x1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I14-C14-X1",
                            exit_side,
                            "6",
                            px_x1,
                            true,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I14-C14-X2"]),
                            Some("O-I14-C14-E1"),
                        ));
                        cache.add_order(x1.clone(), None, None, true).unwrap();
                        assert!(
                            gate.check_command(
                                &submit_cmd(&x1),
                                &cache,
                                UnixNanos::from(TS_NOW + 4),
                            )
                            .is_ok()
                        );
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut x1,
                            exit_side,
                            "6",
                            px_x1,
                            pos_id,
                            &format!("T-I14-C14-X1-{plan_side}-{cont:?}-{x2_ro}"),
                            TS_NOW + 5,
                            true,
                        );
                        assert_eq!(
                            cache.order(&x1.client_order_id()).unwrap().status(),
                            OrderStatus::Filled
                        );

                        if entry_freeze {
                            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                        }

                        // Case 1: SubmitOrder(X-2, 10, parent=E-1, OCO/OUO with X-1) -> DENIED (6 + 10 = 16 > 10).
                        let mut x2_over = make_contingent_limit_order(
                            inst.id(),
                            "O-I14-C14-X2",
                            exit_side,
                            "10",
                            px_x2,
                            x2_ro,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I14-C14-X1"]),
                            Some("O-I14-C14-E1"),
                        );
                        if !x2_ro {
                            x2_over = with_exit_marker(x2_over, Some("TAKE_PROFIT"), None);
                        }
                        cache.add_order(x2_over.clone(), None, None, false).unwrap();
                        let err_c1 = gate
                            .check_command(
                                &submit_cmd(&x2_over),
                                &cache,
                                UnixNanos::from(TS_NOW + 6),
                            )
                            .unwrap_err();
                        assert!(
                            err_c1.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                            "{plan_side} {cont:?} open_e2={open_e2} freeze={entry_freeze} ro={x2_ro}: closed FILLED X-1(6) + X-2(10) = 16 > 10 must reject with REDUCE_ONLY_EXIT_EXCEEDS_POSITION, got: {err_c1}"
                        );
                        assert!(!gate.has_admitted_reservation("O-I14-C14-X2"));
                        let denied_x2 = nautilus_model::events::OrderDenied::new(
                            x2_over.trader_id(),
                            x2_over.strategy_id(),
                            x2_over.instrument_id(),
                            x2_over.client_order_id(),
                            err_c1.into(),
                            UUID4::new(),
                            UnixNanos::from(TS_NOW + 6),
                            UnixNanos::from(TS_NOW + 6),
                        );
                        cache.update_order(&OrderEventAny::Denied(denied_x2)).unwrap();

                        // Case 4 (Valid Contrast): SubmitOrder(X-2-OK, 4, parent=E-1, OCO/OUO with X-1)
                        // -> 6 + 4 = 10 <= 10 (and <= open_pos + parent_leaves) -> ADMITTED.
                        let mut x2_ok = make_contingent_limit_order(
                            inst.id(),
                            "O-I14-C14-X2-OK",
                            exit_side,
                            "4",
                            px_x2,
                            x2_ro,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I14-C14-X1"]),
                            Some("O-I14-C14-E1"),
                        );
                        if !x2_ro {
                            x2_ok = with_exit_marker(x2_ok, Some("TAKE_PROFIT"), None);
                        }
                        cache.add_order(x2_ok.clone(), None, None, false).unwrap();
                        let res_c4 = gate.check_command(
                            &submit_cmd(&x2_ok),
                            &cache,
                            UnixNanos::from(TS_NOW + 7),
                        );
                        assert!(
                            res_c4.is_ok(),
                            "{plan_side} {cont:?} open_e2={open_e2} freeze={entry_freeze} ro={x2_ro}: closed FILLED X-1(6) + X-2-OK(4) = 10 <= 10 must be ADMITTED: {res_c4:?}"
                        );
                    }

                    // Case 2: Closed FILLED X-1(6, parent=E-1, OCO/OUO with X-2) + active X-2(6, OCO/OUO with X-1)
                    // + SubmitOrder(X-3, 4, ro=True, parent=E-1) (unlinked) -> DENIED (6 + 6 + 4 = 16 > 10).
                    {
                        let (inst, mut cache, gate) =
                            setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                        let pos_id = PositionId::from(
                            format!("P-I14-C2-{plan_side}-{cont:?}-{open_e2}-{entry_freeze}")
                                .as_str(),
                        );
                        if open_e2 {
                            let mut e2 = accept_order(make_limit_order(
                                inst.id(),
                                "O-I14-C2-E2",
                                entry_side,
                                "10",
                                px_e2,
                                false,
                            ));
                            cache.add_order(e2.clone(), None, None, true).unwrap();
                            assert!(
                                gate.check_command(
                                    &submit_cmd(&e2),
                                    &cache,
                                    UnixNanos::from(TS_NOW),
                                )
                                .is_ok()
                            );
                            apply_fill_to_cache_and_pos(
                                &mut cache,
                                &inst,
                                &mut e2,
                                entry_side,
                                "10",
                                px_e2,
                                pos_id,
                                &format!("T-I14-C2-E2-{plan_side}-{cont:?}"),
                                TS_NOW + 1,
                                false,
                            );
                        }

                        let list_id = format!("OL-I14-C2-{plan_side}-{cont:?}");
                        let mut e1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I14-C2-E1",
                            entry_side,
                            "10",
                            px_e1,
                            false,
                            Some(ContingencyType::Oto),
                            Some(&list_id),
                            Some(vec!["O-I14-C2-X1", "O-I14-C2-X2", "O-I14-C2-X3"]),
                            None,
                        ));
                        cache.add_order(e1.clone(), None, None, true).unwrap();
                        assert!(
                            gate.check_command(
                                &submit_cmd(&e1),
                                &cache,
                                UnixNanos::from(TS_NOW + 2),
                            )
                            .is_ok()
                        );
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut e1,
                            entry_side,
                            "8",
                            px_e1,
                            pos_id,
                            &format!("T-I14-C2-E1-{plan_side}-{cont:?}"),
                            TS_NOW + 3,
                            open_e2,
                        );

                        let mut x1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I14-C2-X1",
                            exit_side,
                            "6",
                            px_x1,
                            true,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I14-C2-X2"]),
                            Some("O-I14-C2-E1"),
                        ));
                        let x2 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I14-C2-X2",
                            exit_side,
                            "6",
                            px_x2,
                            true,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I14-C2-X1"]),
                            Some("O-I14-C2-E1"),
                        ));
                        cache.add_order(x1.clone(), None, None, true).unwrap();
                        cache.add_order(x2.clone(), None, None, true).unwrap();
                        assert!(
                            gate.check_command(
                                &submit_cmd(&x1),
                                &cache,
                                UnixNanos::from(TS_NOW + 4),
                            )
                            .is_ok()
                        );
                        assert!(
                            gate.check_command(
                                &submit_cmd(&x2),
                                &cache,
                                UnixNanos::from(TS_NOW + 5),
                            )
                            .is_ok()
                        );

                        // X-1 fills 6/6 (FILLED) while X-2(6) remains Accepted.
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut x1,
                            exit_side,
                            "6",
                            px_x1,
                            pos_id,
                            &format!("T-I14-C2-X1-{plan_side}-{cont:?}"),
                            TS_NOW + 6,
                            true,
                        );
                        assert_eq!(
                            cache.order(&x1.client_order_id()).unwrap().status(),
                            OrderStatus::Filled
                        );
                        assert_eq!(
                            cache.order(&x2.client_order_id()).unwrap().status(),
                            OrderStatus::Accepted
                        );

                        if entry_freeze {
                            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                        }

                        // SubmitOrder(X-3, 4, ro=True, parent=E-1) (unlinked) -> DENIED (6 + 6 + 4 = 16 > 10).
                        let x3 = make_contingent_limit_order(
                            inst.id(),
                            "O-I14-C2-X3",
                            exit_side,
                            "4",
                            px_x2,
                            true,
                            None,
                            None,
                            None,
                            Some("O-I14-C2-E1"),
                        );
                        cache.add_order(x3.clone(), None, None, false).unwrap();
                        let err_c2 = gate
                            .check_command(
                                &submit_cmd(&x3),
                                &cache,
                                UnixNanos::from(TS_NOW + 7),
                            )
                            .unwrap_err();
                        assert!(
                            err_c2.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                            "{plan_side} {cont:?} open_e2={open_e2} freeze={entry_freeze}: closed FILLED X-1(6) + active X-2(6) + unlinked X-3(4) = 16 > 10 must reject with REDUCE_ONLY_EXIT_EXCEEDS_POSITION, got: {err_c2}"
                        );
                        assert!(!gate.has_admitted_reservation("O-I14-C2-X3"));
                    }

                    // Case 3: Closed CANCELED partially-filled X-1(10, CANCELED, filled=6, parent=E-1, OCO/OUO with X-2)
                    // + active X-2(10, OCO/OUO with X-1) + ModifyOrder(X-2, qty=8) (ro=True/False)
                    // -> REJECTED (REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, 6 + 8 = 14 > 10),
                    // while ModifyOrder(X-2, qty=4) -> ADMITTED (6 + 4 = 10 <= 10).
                    for x2_ro in [true, false] {
                        let (inst, mut cache, gate) =
                            setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                        let pos_id = PositionId::from(
                            format!(
                                "P-I14-C3-{plan_side}-{cont:?}-{open_e2}-{entry_freeze}-{x2_ro}"
                            )
                            .as_str(),
                        );
                        if open_e2 {
                            let mut e2 = accept_order(make_limit_order(
                                inst.id(),
                                "O-I14-C3-E2",
                                entry_side,
                                "10",
                                px_e2,
                                false,
                            ));
                            cache.add_order(e2.clone(), None, None, true).unwrap();
                            assert!(
                                gate.check_command(
                                    &submit_cmd(&e2),
                                    &cache,
                                    UnixNanos::from(TS_NOW),
                                )
                                .is_ok()
                            );
                            apply_fill_to_cache_and_pos(
                                &mut cache,
                                &inst,
                                &mut e2,
                                entry_side,
                                "10",
                                px_e2,
                                pos_id,
                                &format!("T-I14-C3-E2-{plan_side}-{cont:?}-{x2_ro}"),
                                TS_NOW + 1,
                                false,
                            );
                        }

                        let list_id = format!("OL-I14-C3-{plan_side}-{cont:?}-{x2_ro}");
                        let mut e1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I14-C3-E1",
                            entry_side,
                            "10",
                            px_e1,
                            false,
                            Some(ContingencyType::Oto),
                            Some(&list_id),
                            Some(vec!["O-I14-C3-X1", "O-I14-C3-X2"]),
                            None,
                        ));
                        cache.add_order(e1.clone(), None, None, true).unwrap();
                        assert!(
                            gate.check_command(
                                &submit_cmd(&e1),
                                &cache,
                                UnixNanos::from(TS_NOW + 2),
                            )
                            .is_ok()
                        );
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut e1,
                            entry_side,
                            "8",
                            px_e1,
                            pos_id,
                            &format!("T-I14-C3-E1-{plan_side}-{cont:?}-{x2_ro}"),
                            TS_NOW + 3,
                            open_e2,
                        );

                        let mut x1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I14-C3-X1",
                            exit_side,
                            "10",
                            px_x1,
                            true,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I14-C3-X2"]),
                            Some("O-I14-C3-E1"),
                        ));
                        let mut x2_raw = make_contingent_limit_order(
                            inst.id(),
                            "O-I14-C3-X2",
                            exit_side,
                            "10",
                            px_x2,
                            x2_ro,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I14-C3-X1"]),
                            Some("O-I14-C3-E1"),
                        );
                        if !x2_ro {
                            x2_raw = with_exit_marker(x2_raw, Some("TAKE_PROFIT"), None);
                        }
                        let x2 = accept_order(x2_raw);
                        cache.add_order(x1.clone(), None, None, true).unwrap();
                        cache.add_order(x2.clone(), None, None, true).unwrap();
                        assert!(
                            gate.check_command(
                                &submit_cmd(&x1),
                                &cache,
                                UnixNanos::from(TS_NOW + 4),
                            )
                            .is_ok()
                        );
                        assert!(
                            gate.check_command(
                                &submit_cmd(&x2),
                                &cache,
                                UnixNanos::from(TS_NOW + 5),
                            )
                            .is_ok()
                        );

                        // X-1 partially fills 6/10, then is CANCELED (closed with filled_qty=6).
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut x1,
                            exit_side,
                            "6",
                            px_x1,
                            pos_id,
                            &format!("T-I14-C3-X1-{plan_side}-{cont:?}-{x2_ro}"),
                            TS_NOW + 6,
                            true,
                        );
                        assert_eq!(
                            cache.order(&x1.client_order_id()).unwrap().status(),
                            OrderStatus::PartiallyFilled
                        );
                        let cancel_x1 = OrderCanceled::new(
                            x1.trader_id(),
                            x1.strategy_id(),
                            x1.instrument_id(),
                            x1.client_order_id(),
                            UUID4::new(),
                            UnixNanos::from(TS_NOW + 7),
                            UnixNanos::from(TS_NOW + 7),
                            false,
                            x1.venue_order_id(),
                            Some(AccountId::from("ALPACA-001")),
                            None,
                        );
                        x1.apply(OrderEventAny::Canceled(cancel_x1.clone())).unwrap();
                        cache.update_order(&OrderEventAny::Canceled(cancel_x1)).unwrap();
                        assert_eq!(
                            cache.order(&x1.client_order_id()).unwrap().status(),
                            OrderStatus::Canceled
                        );
                        assert_eq!(
                            cache.order(&x1.client_order_id()).unwrap().filled_qty(),
                            Quantity::from("6")
                        );
                        assert_eq!(
                            cache.order(&x2.client_order_id()).unwrap().status(),
                            OrderStatus::Accepted
                        );

                        if entry_freeze {
                            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                        }

                        // ModifyOrder(X-2, qty=8) -> REJECTED (REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, 6 + 8 = 14 > 10).
                        let err_c3 = gate
                            .check_command(
                                &TradingCommand::ModifyOrder(modify_cmd(
                                    &x2,
                                    Some("8"),
                                    Some(px_x2_mod),
                                    None,
                                )),
                                &cache,
                                UnixNanos::from(TS_NOW + 8),
                            )
                            .unwrap_err();
                        assert!(
                            err_c3.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                            "{plan_side} {cont:?} open_e2={open_e2} freeze={entry_freeze} ro={x2_ro}: closed CANCELED X-1(filled=6) + ModifyOrder(X-2, 8) = 14 > 10 must reject with REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, got: {err_c3}"
                        );

                        // Contrast: ModifyOrder(X-2, qty=4) when open_e2=true (open_pos=12, 6+4=10<=10)
                        // or ModifyOrder(X-2, qty=2) when open_e2=false (open_pos=2, 6+2=8<=10) -> ADMITTED.
                        let ok_mod_qty = if open_e2 { "4" } else { "2" };
                        let res_c3_ok = gate.check_command(
                            &TradingCommand::ModifyOrder(modify_cmd(
                                &x2,
                                Some(ok_mod_qty),
                                Some(px_x2_mod),
                                None,
                            )),
                            &cache,
                            UnixNanos::from(TS_NOW + 9),
                        );
                        assert!(
                            res_c3_ok.is_ok(),
                            "{plan_side} {cont:?} open_e2={open_e2} freeze={entry_freeze} ro={x2_ro}: closed CANCELED X-1(filled=6) + ModifyOrder(X-2, {ok_mod_qty}) <= 10 must be ADMITTED: {res_c3_ok:?}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn test_iter15_partially_filled_oco_ouo_oto_child_irreversible_filled_qty() {
    for open_e2 in [false, true] {
        for entry_freeze in [false, true] {
            for cont in [ContingencyType::Oco, ContingencyType::Ouo] {
                for (
                    plan_side,
                    entry_side,
                    exit_side,
                    initial_stop,
                    px_e2,
                    px_e1,
                    px_x1,
                    px_x2,
                    px_x2_mod,
                    px_sl,
                    px_sl_tight,
                ) in [
                    (
                        "LONG",
                        OrderSide::Buy,
                        OrderSide::Sell,
                        dec!(145.00),
                        "150.00",
                        "149.00",
                        "152.00",
                        "160.00",
                        "159.00",
                        "145.00",
                        "146.00",
                    ),
                    (
                        "SHORT",
                        OrderSide::Sell,
                        OrderSide::Buy,
                        dec!(155.00),
                        "150.00",
                        "151.00",
                        "148.00",
                        "140.00",
                        "141.00",
                        "155.00",
                        "154.00",
                    ),
                ] {
                    // Subcase 1: PartiallyFilled OCO/OUO sibling X-1(6/10, leaves=4, parent=E-1(8/10))
                    // + SubmitOrder(X-2, 10, ro=True/False, parent=E-1, OCO/OUO with X-1)
                    //   -> DENIED (REDUCE_ONLY_EXIT_EXCEEDS_POSITION, 6 + max(4, 10) = 16 > 10),
                    // Contrast: SubmitOrder(X-2-OK, 4, ro=True/False, parent=E-1, OCO/OUO with X-1)
                    //   -> ADMITTED (6 + max(4, 4) = 10 <= 10).
                    for x2_ro in [true, false] {
                        let (inst, mut cache, gate) =
                            setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                        let pos_id = PositionId::from(
                            format!(
                                "P-I15-S1-{plan_side}-{cont:?}-{open_e2}-{entry_freeze}-{x2_ro}"
                            )
                            .as_str(),
                        );
                        if open_e2 {
                            let mut e2 = accept_order(make_limit_order(
                                inst.id(),
                                "O-I15-S1-E2",
                                entry_side,
                                "10",
                                px_e2,
                                false,
                            ));
                            cache.add_order(e2.clone(), None, None, true).unwrap();
                            assert!(
                                gate.check_command(
                                    &submit_cmd(&e2),
                                    &cache,
                                    UnixNanos::from(TS_NOW),
                                )
                                .is_ok()
                            );
                            apply_fill_to_cache_and_pos(
                                &mut cache,
                                &inst,
                                &mut e2,
                                entry_side,
                                "10",
                                px_e2,
                                pos_id,
                                &format!("T-I15-S1-E2-{plan_side}-{cont:?}-{x2_ro}"),
                                TS_NOW + 1,
                                false,
                            );
                        }

                        let list_id = format!("OL-I15-S1-{plan_side}-{cont:?}-{x2_ro}");
                        let mut e1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S1-E1",
                            entry_side,
                            "10",
                            px_e1,
                            false,
                            Some(ContingencyType::Oto),
                            Some(&list_id),
                            Some(vec!["O-I15-S1-X1", "O-I15-S1-X2", "O-I15-S1-X2-OK"]),
                            None,
                        ));
                        cache.add_order(e1.clone(), None, None, true).unwrap();
                        assert!(
                            gate.check_command(
                                &submit_cmd(&e1),
                                &cache,
                                UnixNanos::from(TS_NOW + 2),
                            )
                            .is_ok()
                        );
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut e1,
                            entry_side,
                            "8",
                            px_e1,
                            pos_id,
                            &format!("T-I15-S1-E1-{plan_side}-{cont:?}-{x2_ro}"),
                            TS_NOW + 3,
                            open_e2,
                        );

                        let mut x1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S1-X1",
                            exit_side,
                            "10",
                            px_x1,
                            true,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I15-S1-X2", "O-I15-S1-X2-OK"]),
                            Some("O-I15-S1-E1"),
                        ));
                        cache.add_order(x1.clone(), None, None, true).unwrap();
                        assert!(
                            gate.check_command(
                                &submit_cmd(&x1),
                                &cache,
                                UnixNanos::from(TS_NOW + 4),
                            )
                            .is_ok()
                        );
                        // X-1 partially fills 6/10 -> leaves=4.
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut x1,
                            exit_side,
                            "6",
                            px_x1,
                            pos_id,
                            &format!("T-I15-S1-X1-{plan_side}-{cont:?}-{x2_ro}"),
                            TS_NOW + 5,
                            true,
                        );
                        assert_eq!(
                            cache.order(&x1.client_order_id()).unwrap().status(),
                            OrderStatus::PartiallyFilled
                        );
                        assert_eq!(
                            cache.order(&x1.client_order_id()).unwrap().filled_qty(),
                            Quantity::from("6")
                        );
                        assert_eq!(
                            cache.order(&x1.client_order_id()).unwrap().leaves_qty(),
                            Quantity::from("4")
                        );

                        if entry_freeze {
                            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                        }

                        // SubmitOrder(X-2, 10) -> DENIED (6 + max(4, 10) = 16 > 10).
                        let mut x2_over = make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S1-X2",
                            exit_side,
                            "10",
                            px_x2,
                            x2_ro,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I15-S1-X1"]),
                            Some("O-I15-S1-E1"),
                        );
                        if !x2_ro {
                            x2_over = with_exit_marker(x2_over, Some("TAKE_PROFIT"), None);
                        }
                        cache.add_order(x2_over.clone(), None, None, false).unwrap();
                        let err_s1 = gate
                            .check_command(
                                &submit_cmd(&x2_over),
                                &cache,
                                UnixNanos::from(TS_NOW + 6),
                            )
                            .unwrap_err();
                        assert!(
                            err_s1.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                            "{plan_side} {cont:?} open_e2={open_e2} freeze={entry_freeze} ro={x2_ro}: PartiallyFilled X-1(6/10) + X-2(10) = 16 > 10 must reject with REDUCE_ONLY_EXIT_EXCEEDS_POSITION, got: {err_s1}"
                        );
                        assert!(!gate.has_admitted_reservation("O-I15-S1-X2"));
                        let denied_x2 = nautilus_model::events::OrderDenied::new(
                            x2_over.trader_id(),
                            x2_over.strategy_id(),
                            x2_over.instrument_id(),
                            x2_over.client_order_id(),
                            err_s1.into(),
                            UUID4::new(),
                            UnixNanos::from(TS_NOW + 6),
                            UnixNanos::from(TS_NOW + 6),
                        );
                        cache.update_order(&OrderEventAny::Denied(denied_x2)).unwrap();

                        // Contrast: SubmitOrder(X-2-OK, 4) -> ADMITTED (6 + max(4, 4) = 10 <= 10).
                        let mut x2_ok = make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S1-X2-OK",
                            exit_side,
                            "4",
                            px_x2,
                            x2_ro,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I15-S1-X1"]),
                            Some("O-I15-S1-E1"),
                        );
                        if !x2_ro {
                            x2_ok = with_exit_marker(x2_ok, Some("TAKE_PROFIT"), None);
                        }
                        cache.add_order(x2_ok.clone(), None, None, false).unwrap();
                        let res_s1_ok = gate.check_command(
                            &submit_cmd(&x2_ok),
                            &cache,
                            UnixNanos::from(TS_NOW + 7),
                        );
                        assert!(
                            res_s1_ok.is_ok(),
                            "{plan_side} {cont:?} open_e2={open_e2} freeze={entry_freeze} ro={x2_ro}: PartiallyFilled X-1(6/10) + X-2-OK(4) = 10 <= 10 must be ADMITTED: {res_s1_ok:?}"
                        );
                    }

                    // Subcase 2: Resting X-2(10) OCO/OUO with PartiallyFilled X-1(6/10) (ro=True/False)
                    // + ModifyOrder(X-2, qty=8) -> REJECTED (REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, 6+8=14>10),
                    // Contrast: ModifyOrder(X-2, qty=4) -> ADMITTED (6+4=10<=10).
                    for x2_ro in [true, false] {
                        let (inst, mut cache, gate) =
                            setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                        let pos_id = PositionId::from(
                            format!(
                                "P-I15-S2-{plan_side}-{cont:?}-{open_e2}-{entry_freeze}-{x2_ro}"
                            )
                            .as_str(),
                        );
                        if open_e2 {
                            let mut e2 = accept_order(make_limit_order(
                                inst.id(),
                                "O-I15-S2-E2",
                                entry_side,
                                "10",
                                px_e2,
                                false,
                            ));
                            cache.add_order(e2.clone(), None, None, true).unwrap();
                            assert!(gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW)).is_ok());
                            apply_fill_to_cache_and_pos(
                                &mut cache,
                                &inst,
                                &mut e2,
                                entry_side,
                                "10",
                                px_e2,
                                pos_id,
                                &format!("T-I15-S2-E2-{plan_side}-{cont:?}-{x2_ro}"),
                                TS_NOW + 1,
                                false,
                            );
                        }

                        let list_id = format!("OL-I15-S2-{plan_side}-{cont:?}-{x2_ro}");
                        let mut e1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S2-E1",
                            entry_side,
                            "10",
                            px_e1,
                            false,
                            Some(ContingencyType::Oto),
                            Some(&list_id),
                            Some(vec!["O-I15-S2-X1", "O-I15-S2-X2"]),
                            None,
                        ));
                        cache.add_order(e1.clone(), None, None, true).unwrap();
                        assert!(gate.check_command(&submit_cmd(&e1), &cache, UnixNanos::from(TS_NOW + 2)).is_ok());
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut e1,
                            entry_side,
                            "8",
                            px_e1,
                            pos_id,
                            &format!("T-I15-S2-E1-{plan_side}-{cont:?}-{x2_ro}"),
                            TS_NOW + 3,
                            open_e2,
                        );

                        let mut x1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S2-X1",
                            exit_side,
                            "10",
                            px_x1,
                            true,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I15-S2-X2"]),
                            Some("O-I15-S2-E1"),
                        ));
                        let mut x2_raw = make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S2-X2",
                            exit_side,
                            "10",
                            px_x2,
                            x2_ro,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I15-S2-X1"]),
                            Some("O-I15-S2-E1"),
                        );
                        if !x2_ro {
                            x2_raw = with_exit_marker(x2_raw, Some("TAKE_PROFIT"), None);
                        }
                        let x2 = accept_order(x2_raw);
                        cache.add_order(x1.clone(), None, None, true).unwrap();
                        cache.add_order(x2.clone(), None, None, true).unwrap();
                        assert!(gate.check_command(&submit_cmd(&x1), &cache, UnixNanos::from(TS_NOW + 4)).is_ok());
                        assert!(gate.check_command(&submit_cmd(&x2), &cache, UnixNanos::from(TS_NOW + 5)).is_ok());

                        // X-1 partially fills 6/10 while X-2(10) stays Accepted.
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut x1,
                            exit_side,
                            "6",
                            px_x1,
                            pos_id,
                            &format!("T-I15-S2-X1-{plan_side}-{cont:?}-{x2_ro}"),
                            TS_NOW + 6,
                            true,
                        );
                        assert_eq!(cache.order(&x1.client_order_id()).unwrap().status(), OrderStatus::PartiallyFilled);

                        if entry_freeze {
                            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                        }

                        // ModifyOrder(X-2, qty=8) -> REJECTED (6 + max(4, 8) = 14 > 10).
                        let err_s2 = gate
                            .check_command(
                                &TradingCommand::ModifyOrder(modify_cmd(&x2, Some("8"), Some(px_x2_mod), None)),
                                &cache,
                                UnixNanos::from(TS_NOW + 7),
                            )
                            .unwrap_err();
                        assert!(
                            err_s2.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                            "{plan_side} {cont:?} open_e2={open_e2} freeze={entry_freeze} ro={x2_ro}: PartiallyFilled X-1(6/10) + ModifyOrder(X-2, 8) = 14 > 10 must reject, got: {err_s2}"
                        );

                        // Contrast: ModifyOrder(X-2, qty=4) when open_e2=true (open_pos=12, 6+4=10<=10)
                        // or ModifyOrder(X-2, qty=2) when open_e2=false (open_pos=2, 6+2=8<=10) -> ADMITTED.
                        let ok_mod_qty = if open_e2 { "4" } else { "2" };
                        let res_s2_ok = gate.check_command(
                            &TradingCommand::ModifyOrder(modify_cmd(&x2, Some(ok_mod_qty), Some(px_x2_mod), None)),
                            &cache,
                            UnixNanos::from(TS_NOW + 8),
                        );
                        assert!(
                            res_s2_ok.is_ok(),
                            "{plan_side} {cont:?} open_e2={open_e2} freeze={entry_freeze} ro={x2_ro}: PartiallyFilled X-1(6/10) + ModifyOrder(X-2, {ok_mod_qty}) <= 10 must be ADMITTED: {res_s2_ok:?}"
                        );
                    }

                    // Subcase 3: ModifyOrder(X-1, qty=8) on PartiallyFilled X-1(6/10) alongside resting OCO/OUO X-2(6)
                    // -> REJECTED (6 + max(2, 6) = 12 > 10). Contrast: with X-2(4), ModifyOrder(X-1, qty=8) -> ADMITTED (6 + max(2, 4) = 10).
                    {
                        let (inst, mut cache, gate) =
                            setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                        let pos_id = PositionId::from(
                            format!("P-I15-S3-{plan_side}-{cont:?}-{open_e2}-{entry_freeze}")
                                .as_str(),
                        );
                        if open_e2 {
                            let mut e2 = accept_order(make_limit_order(
                                inst.id(),
                                "O-I15-S3-E2",
                                entry_side,
                                "10",
                                px_e2,
                                false,
                            ));
                            cache.add_order(e2.clone(), None, None, true).unwrap();
                            assert!(gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW)).is_ok());
                            apply_fill_to_cache_and_pos(
                                &mut cache,
                                &inst,
                                &mut e2,
                                entry_side,
                                "10",
                                px_e2,
                                pos_id,
                                &format!("T-I15-S3-E2-{plan_side}-{cont:?}"),
                                TS_NOW + 1,
                                false,
                            );
                        }

                        let list_id = format!("OL-I15-S3-{plan_side}-{cont:?}");
                        let mut e1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S3-E1",
                            entry_side,
                            "10",
                            px_e1,
                            false,
                            Some(ContingencyType::Oto),
                            Some(&list_id),
                            Some(vec!["O-I15-S3-X1", "O-I15-S3-X2"]),
                            None,
                        ));
                        cache.add_order(e1.clone(), None, None, true).unwrap();
                        assert!(gate.check_command(&submit_cmd(&e1), &cache, UnixNanos::from(TS_NOW + 2)).is_ok());
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut e1,
                            entry_side,
                            "8",
                            px_e1,
                            pos_id,
                            &format!("T-I15-S3-E1-{plan_side}-{cont:?}"),
                            TS_NOW + 3,
                            open_e2,
                        );

                        let mut x1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S3-X1",
                            exit_side,
                            "10",
                            px_x1,
                            true,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I15-S3-X2"]),
                            Some("O-I15-S3-E1"),
                        ));
                        let mut x2 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S3-X2",
                            exit_side,
                            "6",
                            px_x2,
                            true,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I15-S3-X1"]),
                            Some("O-I15-S3-E1"),
                        ));
                        cache.add_order(x1.clone(), None, None, true).unwrap();
                        cache.add_order(x2.clone(), None, None, true).unwrap();
                        assert!(gate.check_command(&submit_cmd(&x1), &cache, UnixNanos::from(TS_NOW + 4)).is_ok());
                        assert!(gate.check_command(&submit_cmd(&x2), &cache, UnixNanos::from(TS_NOW + 5)).is_ok());

                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut x1,
                            exit_side,
                            "6",
                            px_x1,
                            pos_id,
                            &format!("T-I15-S3-X1-{plan_side}-{cont:?}"),
                            TS_NOW + 6,
                            true,
                        );

                        if entry_freeze {
                            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                        }

                        // ModifyOrder(X-1, qty=8) with X-2(6) -> REJECTED (6 + max(2, 6) = 12 > 10).
                        let err_s3 = gate
                            .check_command(
                                &TradingCommand::ModifyOrder(modify_cmd(&x1, Some("8"), Some(px_x1), None)),
                                &cache,
                                UnixNanos::from(TS_NOW + 7),
                            )
                            .unwrap_err();
                        assert!(
                            err_s3.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                            "{plan_side} {cont:?} open_e2={open_e2} freeze={entry_freeze}: ModifyOrder(X-1, 8) alongside X-2(6) must yield 6+max(2,6)=12>10 -> REJECTED, got: {err_s3}"
                        );

                        // Downsize X-2 (4 when open_e2=true, 2 when open_e2=false), then ModifyOrder(X-1, qty=8) -> ADMITTED (6 + max(2, 4|2) <= 10).
                        let s3_x2_ok_qty = if open_e2 { "4" } else { "2" };
                        assert!(
                            gate.check_command(
                                &TradingCommand::ModifyOrder(modify_cmd(&x2, Some(s3_x2_ok_qty), Some(px_x2_mod), None)),
                                &cache,
                                UnixNanos::from(TS_NOW + 8),
                            )
                            .is_ok()
                        );
                        let upd_x2 = OrderUpdated::new(
                            x2.trader_id(),
                            x2.strategy_id(),
                            x2.instrument_id(),
                            x2.client_order_id(),
                            Quantity::from(s3_x2_ok_qty),
                            UUID4::new(),
                            UnixNanos::from(TS_NOW + 8),
                            UnixNanos::from(TS_NOW + 8),
                            false,
                            x2.venue_order_id(),
                            Some(AccountId::from("ALPACA-001")),
                            Some(Price::from(px_x2_mod)),
                            None,
                            None,
                            false,
                        );
                        x2.apply(OrderEventAny::Updated(upd_x2.clone())).unwrap();
                        cache.update_order(&OrderEventAny::Updated(upd_x2)).unwrap();
                        assert!(
                            gate.check_command(
                                &TradingCommand::ModifyOrder(modify_cmd(&x1, Some("8"), Some(px_x1), None)),
                                &cache,
                                UnixNanos::from(TS_NOW + 9),
                            )
                            .is_ok()
                        );
                    }

                    // Subcase 4: ModifyOrders batch modifying (X-1, 10) and (X-2, 8) (and (X-2, 10))
                    // when X-1 is PartiallyFilled(6/10) -> REJECTED (REDUCE_ONLY_MODIFY_EXCEEDS_POSITION).
                    // Contrast: ModifyOrders([(X-1, 10), (X-2, 4)]) -> ADMITTED (6 + max(4, 4) = 10 <= 10).
                    {
                        let (inst, mut cache, gate) =
                            setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                        let pos_id = PositionId::from(
                            format!("P-I15-S4-{plan_side}-{cont:?}-{open_e2}-{entry_freeze}")
                                .as_str(),
                        );
                        if open_e2 {
                            let mut e2 = accept_order(make_limit_order(
                                inst.id(),
                                "O-I15-S4-E2",
                                entry_side,
                                "10",
                                px_e2,
                                false,
                            ));
                            cache.add_order(e2.clone(), None, None, true).unwrap();
                            assert!(gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW)).is_ok());
                            apply_fill_to_cache_and_pos(
                                &mut cache,
                                &inst,
                                &mut e2,
                                entry_side,
                                "10",
                                px_e2,
                                pos_id,
                                &format!("T-I15-S4-E2-{plan_side}-{cont:?}"),
                                TS_NOW + 1,
                                false,
                            );
                        }

                        let list_id = format!("OL-I15-S4-{plan_side}-{cont:?}");
                        let mut e1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S4-E1",
                            entry_side,
                            "10",
                            px_e1,
                            false,
                            Some(ContingencyType::Oto),
                            Some(&list_id),
                            Some(vec!["O-I15-S4-X1", "O-I15-S4-X2"]),
                            None,
                        ));
                        cache.add_order(e1.clone(), None, None, true).unwrap();
                        assert!(gate.check_command(&submit_cmd(&e1), &cache, UnixNanos::from(TS_NOW + 2)).is_ok());
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut e1,
                            entry_side,
                            "8",
                            px_e1,
                            pos_id,
                            &format!("T-I15-S4-E1-{plan_side}-{cont:?}"),
                            TS_NOW + 3,
                            open_e2,
                        );

                        let mut x1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S4-X1",
                            exit_side,
                            "10",
                            px_x1,
                            true,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I15-S4-X2"]),
                            Some("O-I15-S4-E1"),
                        ));
                        let x2 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S4-X2",
                            exit_side,
                            "10",
                            px_x2,
                            true,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I15-S4-X1"]),
                            Some("O-I15-S4-E1"),
                        ));
                        cache.add_order(x1.clone(), None, None, true).unwrap();
                        cache.add_order(x2.clone(), None, None, true).unwrap();
                        assert!(gate.check_command(&submit_cmd(&x1), &cache, UnixNanos::from(TS_NOW + 4)).is_ok());
                        assert!(gate.check_command(&submit_cmd(&x2), &cache, UnixNanos::from(TS_NOW + 5)).is_ok());

                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut x1,
                            exit_side,
                            "6",
                            px_x1,
                            pos_id,
                            &format!("T-I15-S4-X1-{plan_side}-{cont:?}"),
                            TS_NOW + 6,
                            true,
                        );

                        if entry_freeze {
                            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                        }

                        for x2_bad_qty in ["8", "10"] {
                            let batch_bad = BatchModifyOrders::new(
                                x1.trader_id(),
                                Some(ClientId::from("ALPACA")),
                                x1.strategy_id(),
                                inst.id(),
                                vec![
                                    modify_cmd(&x1, Some("10"), Some(px_x1), None),
                                    modify_cmd(&x2, Some(x2_bad_qty), Some(px_x2_mod), None),
                                ],
                                UUID4::new(),
                                UnixNanos::from(TS_NOW + 7),
                                None,
                                None,
                            );
                            let err_s4 = gate
                                .check_command(
                                    &TradingCommand::ModifyOrders(batch_bad),
                                    &cache,
                                    UnixNanos::from(TS_NOW + 7),
                                )
                                .unwrap_err();
                            assert!(
                                err_s4.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                                "{plan_side} {cont:?} open_e2={open_e2} freeze={entry_freeze}: ModifyOrders((X-1, 10), (X-2, {x2_bad_qty})) with X-1(6/10) must reject, got: {err_s4}"
                            );
                        }

                        // Contrast: ModifyOrders([(X-1, 10), (X-2, 4)]) when open_e2=true (open_pos=12, 6+4=10<=10)
                        // or ModifyOrders([(X-1, 8), (X-2, 2)]) when open_e2=false (open_pos=2, 6+2=8<=10) -> ADMITTED.
                        let (s4_x1_ok, s4_x2_ok) = if open_e2 { ("10", "4") } else { ("8", "2") };
                        let batch_ok = BatchModifyOrders::new(
                            x1.trader_id(),
                            Some(ClientId::from("ALPACA")),
                            x1.strategy_id(),
                            inst.id(),
                            vec![
                                modify_cmd(&x1, Some(s4_x1_ok), Some(px_x1), None),
                                modify_cmd(&x2, Some(s4_x2_ok), Some(px_x2_mod), None),
                            ],
                            UUID4::new(),
                            UnixNanos::from(TS_NOW + 8),
                            None,
                            None,
                        );
                        let res_s4_ok = gate.check_command(
                            &TradingCommand::ModifyOrders(batch_ok),
                            &cache,
                            UnixNanos::from(TS_NOW + 8),
                        );
                        assert!(
                            res_s4_ok.is_ok(),
                            "{plan_side} {cont:?} open_e2={open_e2} freeze={entry_freeze}: ModifyOrders((X-1, {s4_x1_ok}), (X-2, {s4_x2_ok})) <= 10 must be ADMITTED: {res_s4_ok:?}"
                        );
                    }

                    // Subcase 5: PartiallyFilled X-1(5/6) + Accepted OCO/OUO X-2(6)
                    // + unlinked X-3(4, parent=E-1, ro=True) -> DENIED (5 + max(1, 6) + 4 = 15 > 10).
                    // Contrast: PartiallyFilled X-1(5/6) + Accepted OCO/OUO X-2(3) + unlinked X-3(2) -> ADMITTED (5 + 3 + 2 = 10).
                    {
                        let (inst, mut cache, gate) =
                            setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                        let pos_id = PositionId::from(
                            format!("P-I15-S5-{plan_side}-{cont:?}-{open_e2}-{entry_freeze}")
                                .as_str(),
                        );
                        if open_e2 {
                            let mut e2 = accept_order(make_limit_order(
                                inst.id(),
                                "O-I15-S5-E2",
                                entry_side,
                                "10",
                                px_e2,
                                false,
                            ));
                            cache.add_order(e2.clone(), None, None, true).unwrap();
                            assert!(gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW)).is_ok());
                            apply_fill_to_cache_and_pos(
                                &mut cache,
                                &inst,
                                &mut e2,
                                entry_side,
                                "10",
                                px_e2,
                                pos_id,
                                &format!("T-I15-S5-E2-{plan_side}-{cont:?}"),
                                TS_NOW + 1,
                                false,
                            );
                        }

                        let list_id = format!("OL-I15-S5-{plan_side}-{cont:?}");
                        let mut e1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S5-E1",
                            entry_side,
                            "10",
                            px_e1,
                            false,
                            Some(ContingencyType::Oto),
                            Some(&list_id),
                            Some(vec!["O-I15-S5-X1", "O-I15-S5-X2", "O-I15-S5-X3"]),
                            None,
                        ));
                        cache.add_order(e1.clone(), None, None, true).unwrap();
                        assert!(gate.check_command(&submit_cmd(&e1), &cache, UnixNanos::from(TS_NOW + 2)).is_ok());
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut e1,
                            entry_side,
                            "8",
                            px_e1,
                            pos_id,
                            &format!("T-I15-S5-E1-{plan_side}-{cont:?}"),
                            TS_NOW + 3,
                            open_e2,
                        );

                        let mut x1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S5-X1",
                            exit_side,
                            "6",
                            px_x1,
                            true,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I15-S5-X2"]),
                            Some("O-I15-S5-E1"),
                        ));
                        let x2 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S5-X2",
                            exit_side,
                            "6",
                            px_x2,
                            true,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I15-S5-X1"]),
                            Some("O-I15-S5-E1"),
                        ));
                        cache.add_order(x1.clone(), None, None, true).unwrap();
                        cache.add_order(x2.clone(), None, None, true).unwrap();
                        assert!(gate.check_command(&submit_cmd(&x1), &cache, UnixNanos::from(TS_NOW + 4)).is_ok());
                        assert!(gate.check_command(&submit_cmd(&x2), &cache, UnixNanos::from(TS_NOW + 5)).is_ok());

                        // X-1 fills 5/6 (PartiallyFilled, leaves=1) while X-2(6) stays Accepted.
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut x1,
                            exit_side,
                            "5",
                            px_x1,
                            pos_id,
                            &format!("T-I15-S5-X1-{plan_side}-{cont:?}"),
                            TS_NOW + 6,
                            true,
                        );
                        assert_eq!(cache.order(&x1.client_order_id()).unwrap().status(), OrderStatus::PartiallyFilled);

                        if entry_freeze {
                            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                        }

                        let x3 = make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S5-X3",
                            exit_side,
                            "4",
                            px_x2,
                            true,
                            None,
                            None,
                            None,
                            Some("O-I15-S5-E1"),
                        );
                        cache.add_order(x3.clone(), None, None, false).unwrap();
                        let err_s5 = gate
                            .check_command(&submit_cmd(&x3), &cache, UnixNanos::from(TS_NOW + 7))
                            .unwrap_err();
                        assert!(
                            err_s5.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                            "{plan_side} {cont:?} open_e2={open_e2} freeze={entry_freeze}: X-1(5/6) + X-2(6) + X-3(4) = 15 > 10 must reject, got: {err_s5}"
                        );
                        assert!(!gate.has_admitted_reservation("O-I15-S5-X3"));
                    }

                    // Valid contrast for Subcase 5: X-1(5/6) + X-2(3) + unlinked X-3(2) = 5 + max(1, 3) + 2 = 10 <= 10.
                    {
                        let (inst, mut cache, gate) =
                            setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                        let pos_id = PositionId::from(
                            format!("P-I15-S5OK-{plan_side}-{cont:?}-{open_e2}-{entry_freeze}")
                                .as_str(),
                        );
                        if open_e2 {
                            let mut e2 = accept_order(make_limit_order(
                                inst.id(),
                                "O-I15-S5OK-E2",
                                entry_side,
                                "10",
                                px_e2,
                                false,
                            ));
                            cache.add_order(e2.clone(), None, None, true).unwrap();
                            assert!(gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW)).is_ok());
                            apply_fill_to_cache_and_pos(
                                &mut cache,
                                &inst,
                                &mut e2,
                                entry_side,
                                "10",
                                px_e2,
                                pos_id,
                                &format!("T-I15-S5OK-E2-{plan_side}-{cont:?}"),
                                TS_NOW + 1,
                                false,
                            );
                        }
                        let list_id = format!("OL-I15-S5OK-{plan_side}-{cont:?}");
                        let mut e1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S5OK-E1",
                            entry_side,
                            "10",
                            px_e1,
                            false,
                            Some(ContingencyType::Oto),
                            Some(&list_id),
                            Some(vec!["O-I15-S5OK-X1", "O-I15-S5OK-X2", "O-I15-S5OK-X3"]),
                            None,
                        ));
                        cache.add_order(e1.clone(), None, None, true).unwrap();
                        assert!(gate.check_command(&submit_cmd(&e1), &cache, UnixNanos::from(TS_NOW + 2)).is_ok());
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut e1,
                            entry_side,
                            "8",
                            px_e1,
                            pos_id,
                            &format!("T-I15-S5OK-E1-{plan_side}-{cont:?}"),
                            TS_NOW + 3,
                            open_e2,
                        );
                        let mut x1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S5OK-X1",
                            exit_side,
                            "6",
                            px_x1,
                            true,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I15-S5OK-X2"]),
                            Some("O-I15-S5OK-E1"),
                        ));
                        let x2 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S5OK-X2",
                            exit_side,
                            "3",
                            px_x2,
                            true,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I15-S5OK-X1"]),
                            Some("O-I15-S5OK-E1"),
                        ));
                        cache.add_order(x1.clone(), None, None, true).unwrap();
                        cache.add_order(x2.clone(), None, None, true).unwrap();
                        assert!(gate.check_command(&submit_cmd(&x1), &cache, UnixNanos::from(TS_NOW + 4)).is_ok());
                        assert!(gate.check_command(&submit_cmd(&x2), &cache, UnixNanos::from(TS_NOW + 5)).is_ok());
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut x1,
                            exit_side,
                            "5",
                            px_x1,
                            pos_id,
                            &format!("T-I15-S5OK-X1-{plan_side}-{cont:?}"),
                            TS_NOW + 6,
                            true,
                        );
                        if entry_freeze {
                            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                        }
                        let x3 = make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S5OK-X3",
                            exit_side,
                            "2",
                            px_x2,
                            true,
                            None,
                            None,
                            None,
                            Some("O-I15-S5OK-E1"),
                        );
                        cache.add_order(x3.clone(), None, None, false).unwrap();
                        let res_s5_ok = gate.check_command(&submit_cmd(&x3), &cache, UnixNanos::from(TS_NOW + 7));
                        assert!(
                            res_s5_ok.is_ok(),
                            "{plan_side} {cont:?} open_e2={open_e2} freeze={entry_freeze}: X-1(5/6) + X-2(3) + X-3(2) = 10 <= 10 must be ADMITTED: {res_s5_ok:?}"
                        );
                    }

                    // Subcase 6: Contrast — sequential unfilled (filled_qty == 0) OCO/OUO bracket downsizing
                    // SL(10)->6, TP(10)->6 remains ACCEPTED while E-1(8/10) is open.
                    {
                        let (inst, mut cache, gate) =
                            setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                        let pos_id = PositionId::from(
                            format!("P-I15-S6-{plan_side}-{cont:?}-{open_e2}-{entry_freeze}")
                                .as_str(),
                        );
                        if open_e2 {
                            let mut e2 = accept_order(make_limit_order(
                                inst.id(),
                                "O-I15-S6-E2",
                                entry_side,
                                "10",
                                px_e2,
                                false,
                            ));
                            cache.add_order(e2.clone(), None, None, true).unwrap();
                            assert!(gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW)).is_ok());
                            apply_fill_to_cache_and_pos(
                                &mut cache,
                                &inst,
                                &mut e2,
                                entry_side,
                                "10",
                                px_e2,
                                pos_id,
                                &format!("T-I15-S6-E2-{plan_side}-{cont:?}"),
                                TS_NOW + 1,
                                false,
                            );
                        }
                        let list_id = format!("OL-I15-S6-{plan_side}-{cont:?}");
                        let mut e1 = accept_order(make_contingent_limit_order(
                            inst.id(),
                            "O-I15-S6-E1",
                            entry_side,
                            "10",
                            px_e1,
                            false,
                            Some(ContingencyType::Oto),
                            Some(&list_id),
                            Some(vec!["O-I15-S6-SL", "O-I15-S6-TP"]),
                            None,
                        ));
                        cache.add_order(e1.clone(), None, None, true).unwrap();
                        assert!(gate.check_command(&submit_cmd(&e1), &cache, UnixNanos::from(TS_NOW + 2)).is_ok());
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut e1,
                            entry_side,
                            "8",
                            px_e1,
                            pos_id,
                            &format!("T-I15-S6-E1-{plan_side}-{cont:?}"),
                            TS_NOW + 3,
                            open_e2,
                        );
                        let mut sl = accept_order(make_contingent_stop_market_order(
                            inst.id(),
                            "O-I15-S6-SL",
                            exit_side,
                            "10",
                            px_sl,
                            true,
                            Some(cont),
                            Some(&list_id),
                            Some(vec!["O-I15-S6-TP"]),
                            Some("O-I15-S6-E1"),
                        ));
                        let tp = accept_order(with_exit_marker(
                            make_contingent_limit_order(
                                inst.id(),
                                "O-I15-S6-TP",
                                exit_side,
                                "10",
                                px_x2,
                                false,
                                Some(cont),
                                Some(&list_id),
                                Some(vec!["O-I15-S6-SL"]),
                                Some("O-I15-S6-E1"),
                            ),
                            Some("TAKE_PROFIT"),
                            None,
                        ));
                        cache.add_order(sl.clone(), None, None, true).unwrap();
                        cache.add_order(tp.clone(), None, None, true).unwrap();
                        assert!(gate.check_command(&submit_cmd(&sl), &cache, UnixNanos::from(TS_NOW + 4)).is_ok());
                        assert!(gate.check_command(&submit_cmd(&tp), &cache, UnixNanos::from(TS_NOW + 5)).is_ok());

                        if entry_freeze {
                            gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                        }

                        // Downsize SL 10->6 while TP(10) is unfilled -> ADMITTED.
                        let res_sl = gate.check_command(
                            &TradingCommand::ModifyOrder(modify_cmd(&sl, Some("6"), None, Some(px_sl_tight))),
                            &cache,
                            UnixNanos::from(TS_NOW + 6),
                        );
                        assert!(
                            res_sl.is_ok(),
                            "{plan_side} {cont:?} open_e2={open_e2} freeze={entry_freeze}: sequential unfilled SL(10)->6 must be ADMITTED: {res_sl:?}"
                        );
                        let upd_sl = OrderUpdated::new(
                            sl.trader_id(),
                            sl.strategy_id(),
                            sl.instrument_id(),
                            sl.client_order_id(),
                            Quantity::from("6"),
                            UUID4::new(),
                            UnixNanos::from(TS_NOW + 6),
                            UnixNanos::from(TS_NOW + 6),
                            false,
                            sl.venue_order_id(),
                            Some(AccountId::from("ALPACA-001")),
                            None,
                            Some(Price::from(px_sl_tight)),
                            None,
                            false,
                        );
                        sl.apply(OrderEventAny::Updated(upd_sl.clone())).unwrap();
                        cache.update_order(&OrderEventAny::Updated(upd_sl)).unwrap();

                        // Downsize TP 10->6 -> ADMITTED.
                        let res_tp = gate.check_command(
                            &TradingCommand::ModifyOrder(modify_cmd(&tp, Some("6"), Some(px_x2_mod), None)),
                            &cache,
                            UnixNanos::from(TS_NOW + 7),
                        );
                        assert!(
                            res_tp.is_ok(),
                            "{plan_side} {cont:?} open_e2={open_e2} freeze={entry_freeze}: sequential unfilled TP(10)->6 must be ADMITTED: {res_tp:?}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn test_iter15_pending_cancel_pre_ack_fail_closed_capacity_retention() {
    for entry_freeze in [false, true] {
        for (plan_side, entry_side, exit_side, initial_stop, px_e1, px_x1, px_x2) in [
            (
                "LONG",
                OrderSide::Buy,
                OrderSide::Sell,
                dec!(145.00),
                "149.00",
                "160.00",
                "161.00",
            ),
            (
                "SHORT",
                OrderSide::Sell,
                OrderSide::Buy,
                dec!(155.00),
                "151.00",
                "140.00",
                "139.00",
            ),
        ] {
            // (a) OTO child exposure cap during PendingCancel (both E-1 0/10 pre-fill and 8/10 partial-fill).
            for partial_fill_e1 in [false, true] {
                for x2_ro in [true, false] {
                    let (inst, mut cache, gate) =
                        setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                    let pos_id = PositionId::from(
                        format!("P-I15-PC-OTO-{plan_side}-{partial_fill_e1}-{entry_freeze}-{x2_ro}")
                            .as_str(),
                    );
                    let list_id = format!("OL-I15-PC-{plan_side}-{partial_fill_e1}-{x2_ro}");
                    let mut e1 = accept_order(make_contingent_limit_order(
                        inst.id(),
                        "O-I15-PC-E1",
                        entry_side,
                        "10",
                        px_e1,
                        false,
                        Some(ContingencyType::Oto),
                        Some(&list_id),
                        Some(vec!["O-I15-PC-X1", "O-I15-PC-X2", "O-I15-PC-X3"]),
                        None,
                    ));
                    cache.add_order(e1.clone(), None, None, true).unwrap();
                    assert!(gate.check_command(&submit_cmd(&e1), &cache, UnixNanos::from(TS_NOW)).is_ok());

                    if partial_fill_e1 {
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut e1,
                            entry_side,
                            "8",
                            px_e1,
                            pos_id,
                            &format!("T-I15-PC-E1-{plan_side}-{x2_ro}"),
                            TS_NOW + 1,
                            false,
                        );
                    }

                    let mut x1 = accept_order(make_contingent_limit_order(
                        inst.id(),
                        "O-I15-PC-X1",
                        exit_side,
                        "10",
                        px_x1,
                        true,
                        None,
                        Some(&list_id),
                        None,
                        Some("O-I15-PC-E1"),
                    ));
                    cache.add_order(x1.clone(), None, None, true).unwrap();
                    assert!(gate.check_command(&submit_cmd(&x1), &cache, UnixNanos::from(TS_NOW + 2)).is_ok());

                    // Transition X-1 to PendingCancel (pre-ack).
                    let pending_cancel = OrderPendingCancel::new(
                        x1.trader_id(),
                        x1.strategy_id(),
                        x1.instrument_id(),
                        x1.client_order_id(),
                        Some(AccountId::from("ALPACA-001")),
                        UUID4::new(),
                        UnixNanos::from(TS_NOW + 3),
                        UnixNanos::from(TS_NOW + 3),
                        false,
                        x1.venue_order_id(),
                    );
                    x1.apply(OrderEventAny::PendingCancel(pending_cancel.clone()))
                        .unwrap();
                    cache
                        .update_order(&OrderEventAny::PendingCancel(pending_cancel))
                        .unwrap();
                    {
                        let c_x1 = cache.order(&x1.client_order_id()).unwrap();
                        assert_eq!(c_x1.status(), OrderStatus::PendingCancel);
                        assert_eq!(c_x1.leaves_qty(), Quantity::from("10"));
                        assert!(!c_x1.is_closed());
                    }

                    if entry_freeze {
                        gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                    }

                    // While X-1 is PendingCancel, SubmitOrder(X-2, 10, parent=E-1) MUST be DENIED.
                    let mut x2 = make_contingent_limit_order(
                        inst.id(),
                        "O-I15-PC-X2",
                        exit_side,
                        "10",
                        px_x2,
                        x2_ro,
                        None,
                        Some(&list_id),
                        None,
                        Some("O-I15-PC-E1"),
                    );
                    if !x2_ro {
                        x2 = with_exit_marker(x2, Some("TAKE_PROFIT"), None);
                    }
                    cache.add_order(x2.clone(), None, None, false).unwrap();
                    let err_pc = gate
                        .check_command(&submit_cmd(&x2), &cache, UnixNanos::from(TS_NOW + 4))
                        .unwrap_err();
                    assert!(
                        err_pc.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                        "{plan_side} partial={partial_fill_e1} freeze={entry_freeze} ro={x2_ro}: PendingCancel X-1(10) + X-2(10) must reject with REDUCE_ONLY_EXIT_EXCEEDS_POSITION, got: {err_pc}"
                    );
                    assert!(!gate.has_admitted_reservation("O-I15-PC-X2"));
                    let denied_x2 = nautilus_model::events::OrderDenied::new(
                        x2.trader_id(),
                        x2.strategy_id(),
                        x2.instrument_id(),
                        x2.client_order_id(),
                        err_pc.into(),
                        UUID4::new(),
                        UnixNanos::from(TS_NOW + 4),
                        UnixNanos::from(TS_NOW + 4),
                    );
                    cache.update_order(&OrderEventAny::Denied(denied_x2)).unwrap();

                    // Once X-1 receives broker cancel ack -> Canceled (is_closed()), X-3(10) is ACCEPTED.
                    let canceled = OrderCanceled::new(
                        x1.trader_id(),
                        x1.strategy_id(),
                        x1.instrument_id(),
                        x1.client_order_id(),
                        UUID4::new(),
                        UnixNanos::from(TS_NOW + 5),
                        UnixNanos::from(TS_NOW + 5),
                        false,
                        x1.venue_order_id(),
                        Some(AccountId::from("ALPACA-001")),
                        None,
                    );
                    x1.apply(OrderEventAny::Canceled(canceled.clone())).unwrap();
                    cache.update_order(&OrderEventAny::Canceled(canceled)).unwrap();
                    assert_eq!(
                        cache.order(&x1.client_order_id()).unwrap().status(),
                        OrderStatus::Canceled
                    );
                    assert!(cache.order(&x1.client_order_id()).unwrap().is_closed());

                    let mut x3 = make_contingent_limit_order(
                        inst.id(),
                        "O-I15-PC-X3",
                        exit_side,
                        "10",
                        px_x2,
                        x2_ro,
                        None,
                        Some(&list_id),
                        None,
                        Some("O-I15-PC-E1"),
                    );
                    if !x2_ro {
                        x3 = with_exit_marker(x3, Some("TAKE_PROFIT"), None);
                    }
                    cache.add_order(x3.clone(), None, None, false).unwrap();
                    let res_x3 =
                        gate.check_command(&submit_cmd(&x3), &cache, UnixNanos::from(TS_NOW + 6));
                    assert!(
                        res_x3.is_ok(),
                        "{plan_side} partial={partial_fill_e1} freeze={entry_freeze} ro={x2_ro}: after X-1 Canceled, X-3(10) must be ADMITTED: {res_x3:?}"
                    );
                }
            }

            // (b) Unlinked open-position exit capacity during PendingCancel.
            {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                let pos_id = PositionId::from(
                    format!("P-I15-PC-UNL-{plan_side}-{entry_freeze}").as_str(),
                );
                let mut e2 = accept_order(make_limit_order(
                    inst.id(),
                    "O-I15-PC-UNL-E2",
                    entry_side,
                    "10",
                    px_e1,
                    false,
                ));
                cache.add_order(e2.clone(), None, None, true).unwrap();
                assert!(gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW)).is_ok());
                apply_fill_to_cache_and_pos(
                    &mut cache,
                    &inst,
                    &mut e2,
                    entry_side,
                    "10",
                    px_e1,
                    pos_id,
                    &format!("T-I15-PC-UNL-E2-{plan_side}"),
                    TS_NOW + 1,
                    false,
                );

                let mut x1 = accept_order(make_limit_order(
                    inst.id(),
                    "O-I15-PC-UNL-X1",
                    exit_side,
                    "10",
                    px_x1,
                    true,
                ));
                cache.add_order(x1.clone(), None, None, true).unwrap();
                assert!(gate.check_command(&submit_cmd(&x1), &cache, UnixNanos::from(TS_NOW + 2)).is_ok());

                let pending_cancel = OrderPendingCancel::new(
                    x1.trader_id(),
                    x1.strategy_id(),
                    x1.instrument_id(),
                    x1.client_order_id(),
                    Some(AccountId::from("ALPACA-001")),
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 3),
                    UnixNanos::from(TS_NOW + 3),
                    false,
                    x1.venue_order_id(),
                );
                x1.apply(OrderEventAny::PendingCancel(pending_cancel.clone()))
                    .unwrap();
                cache
                    .update_order(&OrderEventAny::PendingCancel(pending_cancel))
                    .unwrap();
                assert_eq!(
                    cache.order(&x1.client_order_id()).unwrap().status(),
                    OrderStatus::PendingCancel
                );

                if entry_freeze {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                // While X-1 is PendingCancel, SubmitOrder(X-2, 10, ro=True) MUST be DENIED.
                let x2 = make_limit_order(
                    inst.id(),
                    "O-I15-PC-UNL-X2",
                    exit_side,
                    "10",
                    px_x2,
                    true,
                );
                cache.add_order(x2.clone(), None, None, false).unwrap();
                let err_unl = gate
                    .check_command(&submit_cmd(&x2), &cache, UnixNanos::from(TS_NOW + 4))
                    .unwrap_err();
                assert!(
                    err_unl.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                    "{plan_side} freeze={entry_freeze}: unlinked PendingCancel X-1(10) + X-2(10, ro=True) must reject with REDUCE_ONLY_EXIT_EXCEEDS_POSITION, got: {err_unl}"
                );
                assert!(!gate.has_admitted_reservation("O-I15-PC-UNL-X2"));
                let denied_x2 = nautilus_model::events::OrderDenied::new(
                    x2.trader_id(),
                    x2.strategy_id(),
                    x2.instrument_id(),
                    x2.client_order_id(),
                    err_unl.into(),
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 4),
                    UnixNanos::from(TS_NOW + 4),
                );
                cache.update_order(&OrderEventAny::Denied(denied_x2)).unwrap();

                // Once X-1 transitions to Canceled, replacement X-3(10, ro=True) is ACCEPTED.
                let canceled = OrderCanceled::new(
                    x1.trader_id(),
                    x1.strategy_id(),
                    x1.instrument_id(),
                    x1.client_order_id(),
                    UUID4::new(),
                    UnixNanos::from(TS_NOW + 5),
                    UnixNanos::from(TS_NOW + 5),
                    false,
                    x1.venue_order_id(),
                    Some(AccountId::from("ALPACA-001")),
                    None,
                );
                x1.apply(OrderEventAny::Canceled(canceled.clone())).unwrap();
                cache.update_order(&OrderEventAny::Canceled(canceled)).unwrap();
                assert!(cache.order(&x1.client_order_id()).unwrap().is_closed());

                let x3 = make_limit_order(
                    inst.id(),
                    "O-I15-PC-UNL-X3",
                    exit_side,
                    "10",
                    px_x2,
                    true,
                );
                cache.add_order(x3.clone(), None, None, false).unwrap();
                let res_unl_x3 =
                    gate.check_command(&submit_cmd(&x3), &cache, UnixNanos::from(TS_NOW + 6));
                assert!(
                    res_unl_x3.is_ok(),
                    "{plan_side} freeze={entry_freeze}: after unlinked X-1 Canceled, X-3(10, ro=True) must be ADMITTED: {res_unl_x3:?}"
                );
            }
        }
    }
}

#[test]
fn test_iter15_pending_cancel_unlinked_ro_sibling_upward_modify_rejected() {
    for entry_freeze in [false, true] {
        for (plan_side, entry_side, exit_side, initial_stop, px_e, px_x1, px_x2, px_x2_mod, px_x2_mod2) in [
            (
                "LONG",
                OrderSide::Buy,
                OrderSide::Sell,
                dec!(145.00),
                "150.00",
                "160.00",
                "161.00",
                "162.00",
                "163.00",
            ),
            (
                "SHORT",
                OrderSide::Sell,
                OrderSide::Buy,
                dec!(155.00),
                "150.00",
                "140.00",
                "139.00",
                "138.00",
                "137.00",
            ),
        ] {
            let (inst, mut cache, gate) =
                setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
            let pos_id = PositionId::from(
                format!("P-I15-PC-MOD-{plan_side}-{entry_freeze}").as_str(),
            );
            let mut e2 = accept_order(make_limit_order(
                inst.id(),
                "O-I15-PCM-E2",
                entry_side,
                "10",
                px_e,
                false,
            ));
            cache.add_order(e2.clone(), None, None, true).unwrap();
            assert!(gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW)).is_ok());
            apply_fill_to_cache_and_pos(
                &mut cache,
                &inst,
                &mut e2,
                entry_side,
                "10",
                px_e,
                pos_id,
                &format!("T-I15-PCM-E2-{plan_side}-{entry_freeze}"),
                TS_NOW + 1,
                false,
            );

            // Submit & accept unlinked X-1(6, ro=True) and X-2(4, ro=True) against open_pos = 10.
            let mut x1 = accept_order(make_limit_order(
                inst.id(),
                "O-I15-PCM-X1",
                exit_side,
                "6",
                px_x1,
                true,
            ));
            let mut x2 = accept_order(make_limit_order(
                inst.id(),
                "O-I15-PCM-X2",
                exit_side,
                "4",
                px_x2,
                true,
            ));
            cache.add_order(x1.clone(), None, None, true).unwrap();
            cache.add_order(x2.clone(), None, None, true).unwrap();
            assert!(gate.check_command(&submit_cmd(&x1), &cache, UnixNanos::from(TS_NOW + 2)).is_ok());
            assert!(gate.check_command(&submit_cmd(&x2), &cache, UnixNanos::from(TS_NOW + 3)).is_ok());

            // Transition X-1 to PendingCancel (pre-ack).
            let pending_cancel = OrderPendingCancel::new(
                x1.trader_id(),
                x1.strategy_id(),
                x1.instrument_id(),
                x1.client_order_id(),
                Some(AccountId::from("ALPACA-001")),
                UUID4::new(),
                UnixNanos::from(TS_NOW + 4),
                UnixNanos::from(TS_NOW + 4),
                false,
                x1.venue_order_id(),
            );
            x1.apply(OrderEventAny::PendingCancel(pending_cancel.clone()))
                .unwrap();
            cache
                .update_order(&OrderEventAny::PendingCancel(pending_cancel))
                .unwrap();
            assert_eq!(
                cache.order(&x1.client_order_id()).unwrap().status(),
                OrderStatus::PendingCancel
            );

            if entry_freeze {
                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            }

            // While X-1(6, ro=True) is PendingCancel, upward ModifyOrder(X-2, qty=8) MUST be REJECTED (6 + 8 = 14 > 10).
            let err_up = gate
                .check_command(
                    &TradingCommand::ModifyOrder(modify_cmd(&x2, Some("8"), Some(px_x2_mod), None)),
                    &cache,
                    UnixNanos::from(TS_NOW + 5),
                )
                .unwrap_err();
            assert!(
                err_up.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                "{plan_side} freeze={entry_freeze}: PendingCancel X-1(6) + ModifyOrder(X-2, 8) = 14 > 10 must reject with REDUCE_ONLY_MODIFY_EXCEEDS_POSITION, got: {err_up}"
            );

            // Same-qty price modify ModifyOrder(X-2, qty=4) while X-1(6) is PendingCancel MUST be ACCEPTED (6 + 4 = 10 <= 10).
            let res_same = gate.check_command(
                &TradingCommand::ModifyOrder(modify_cmd(&x2, Some("4"), Some(px_x2_mod), None)),
                &cache,
                UnixNanos::from(TS_NOW + 6),
            );
            assert!(
                res_same.is_ok(),
                "{plan_side} freeze={entry_freeze}: PendingCancel X-1(6) + ModifyOrder(X-2, 4) = 10 <= 10 must be ACCEPTED: {res_same:?}"
            );
            let upd_x2 = OrderUpdated::new(
                x2.trader_id(),
                x2.strategy_id(),
                x2.instrument_id(),
                x2.client_order_id(),
                Quantity::from("4"),
                UUID4::new(),
                UnixNanos::from(TS_NOW + 6),
                UnixNanos::from(TS_NOW + 6),
                false,
                x2.venue_order_id(),
                Some(AccountId::from("ALPACA-001")),
                Some(Price::from(px_x2_mod)),
                None,
                None,
                false,
            );
            x2.apply(OrderEventAny::Updated(upd_x2.clone())).unwrap();
            cache.update_order(&OrderEventAny::Updated(upd_x2)).unwrap();

            // Once X-1 transitions to Canceled, ModifyOrder(X-2, qty=8) MUST be ACCEPTED (0 + 8 = 8 <= 10).
            let canceled = OrderCanceled::new(
                x1.trader_id(),
                x1.strategy_id(),
                x1.instrument_id(),
                x1.client_order_id(),
                UUID4::new(),
                UnixNanos::from(TS_NOW + 7),
                UnixNanos::from(TS_NOW + 7),
                false,
                x1.venue_order_id(),
                Some(AccountId::from("ALPACA-001")),
                None,
            );
            x1.apply(OrderEventAny::Canceled(canceled.clone())).unwrap();
            cache.update_order(&OrderEventAny::Canceled(canceled)).unwrap();
            assert!(cache.order(&x1.client_order_id()).unwrap().is_closed());

            let res_after = gate.check_command(
                &TradingCommand::ModifyOrder(modify_cmd(&x2, Some("8"), Some(px_x2_mod2), None)),
                &cache,
                UnixNanos::from(TS_NOW + 8),
            );
            assert!(
                res_after.is_ok(),
                "{plan_side} freeze={entry_freeze}: after X-1 Canceled, ModifyOrder(X-2, 8) = 8 <= 10 must be ACCEPTED: {res_after:?}"
            );
        }
    }
}

#[test]
fn test_iter15_modify_orders_atomic_unlinked_ro_exit_batch_rebalance() {
    for entry_freeze in [false, true] {
        for (
            plan_side,
            entry_side,
            exit_side,
            initial_stop,
            px_e,
            px_x1,
            px_x2,
            px_x1_mod,
            px_x2_mod,
        ) in [
            (
                "LONG",
                OrderSide::Buy,
                OrderSide::Sell,
                dec!(145.00),
                "150.00",
                "160.00",
                "161.00",
                "159.00",
                "162.00",
            ),
            (
                "SHORT",
                OrderSide::Sell,
                OrderSide::Buy,
                dec!(155.00),
                "150.00",
                "140.00",
                "139.00",
                "141.00",
                "138.00",
            ),
        ] {
            let (inst, mut cache, gate) =
                setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
            let pos_id = PositionId::from(
                format!("P-I15-UB-{plan_side}-{entry_freeze}").as_str(),
            );
            let mut e2 = accept_order(make_limit_order(
                inst.id(),
                "O-I15-UB-E2",
                entry_side,
                "10",
                px_e,
                false,
            ));
            cache.add_order(e2.clone(), None, None, true).unwrap();
            assert!(gate.check_command(&submit_cmd(&e2), &cache, UnixNanos::from(TS_NOW)).is_ok());
            apply_fill_to_cache_and_pos(
                &mut cache,
                &inst,
                &mut e2,
                entry_side,
                "10",
                px_e,
                pos_id,
                &format!("T-I15-UB-E2-{plan_side}-{entry_freeze}"),
                TS_NOW + 1,
                false,
            );

            // Unlinked X-1(6, ro=True) + X-2(4, ro=True) against open_pos = 10.
            let x1 = accept_order(make_limit_order(
                inst.id(),
                "O-I15-UB-X1",
                exit_side,
                "6",
                px_x1,
                true,
            ));
            let x2 = accept_order(make_limit_order(
                inst.id(),
                "O-I15-UB-X2",
                exit_side,
                "4",
                px_x2,
                true,
            ));
            cache.add_order(x1.clone(), None, None, true).unwrap();
            cache.add_order(x2.clone(), None, None, true).unwrap();
            assert!(gate.check_command(&submit_cmd(&x1), &cache, UnixNanos::from(TS_NOW + 2)).is_ok());
            assert!(gate.check_command(&submit_cmd(&x2), &cache, UnixNanos::from(TS_NOW + 3)).is_ok());

            if entry_freeze {
                gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
            }

            // ModifyOrders([(X-1, 8), (X-2, 8)]) -> REJECTED (8 + 8 = 16 > 10).
            let batch_over = BatchModifyOrders::new(
                x1.trader_id(),
                Some(ClientId::from("ALPACA")),
                x1.strategy_id(),
                inst.id(),
                vec![
                    modify_cmd(&x1, Some("8"), Some(px_x1_mod), None),
                    modify_cmd(&x2, Some("8"), Some(px_x2_mod), None),
                ],
                UUID4::new(),
                UnixNanos::from(TS_NOW + 4),
                None,
                None,
            );
            let err_over = gate
                .check_command(
                    &TradingCommand::ModifyOrders(batch_over),
                    &cache,
                    UnixNanos::from(TS_NOW + 4),
                )
                .unwrap_err();
            assert!(
                err_over.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                "{plan_side} freeze={entry_freeze}: unlinked ro=True ModifyOrders((X-1, 8), (X-2, 8)) = 16 > 10 must reject, got: {err_over}"
            );

            // Simultaneous atomic rebalancing ModifyOrders([(X-1, 2), (X-2, 8)]) -> ADMITTED (2 + 8 = 10 <= 10).
            let batch_rebalance = BatchModifyOrders::new(
                x1.trader_id(),
                Some(ClientId::from("ALPACA")),
                x1.strategy_id(),
                inst.id(),
                vec![
                    modify_cmd(&x1, Some("2"), Some(px_x1_mod), None),
                    modify_cmd(&x2, Some("8"), Some(px_x2_mod), None),
                ],
                UUID4::new(),
                UnixNanos::from(TS_NOW + 5),
                None,
                None,
            );
            let res_rebalance = gate.check_command(
                &TradingCommand::ModifyOrders(batch_rebalance),
                &cache,
                UnixNanos::from(TS_NOW + 5),
            );
            assert!(
                res_rebalance.is_ok(),
                "{plan_side} freeze={entry_freeze}: simultaneous atomic rebalancing ModifyOrders((X-1, 2), (X-2, 8)) = 10 <= 10 must be ADMITTED: {res_rebalance:?}"
            );
        }
    }
}

#[test]
fn test_iter15_modify_orders_partial_oto_parent_6_10_partial_child_4_6_open_pos_vs_parent_qty() {
    for open_e2 in [false, true] {
        for entry_freeze in [false, true] {
            for cont in [ContingencyType::Oco, ContingencyType::Ouo] {
                for (
                    plan_side,
                    entry_side,
                    exit_side,
                    initial_stop,
                    px_e2,
                    px_e1,
                    px_x1,
                    px_x2,
                    px_x1_mod,
                    px_x2_mod,
                ) in [
                    (
                        "LONG",
                        OrderSide::Buy,
                        OrderSide::Sell,
                        dec!(145.00),
                        "150.00",
                        "149.00",
                        "152.00",
                        "160.00",
                        "153.00",
                        "159.00",
                    ),
                    (
                        "SHORT",
                        OrderSide::Sell,
                        OrderSide::Buy,
                        dec!(155.00),
                        "150.00",
                        "151.00",
                        "148.00",
                        "140.00",
                        "147.00",
                        "141.00",
                    ),
                ] {
                    let (inst, mut cache, gate) =
                        setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                    let pos_id = PositionId::from(
                        format!(
                            "P-I15-PO-{plan_side}-{cont:?}-{open_e2}-{entry_freeze}"
                        )
                        .as_str(),
                    );
                    if open_e2 {
                        let mut e2 = accept_order(make_limit_order(
                            inst.id(),
                            "O-I15-PO-E2",
                            entry_side,
                            "10",
                            px_e2,
                            false,
                        ));
                        cache.add_order(e2.clone(), None, None, true).unwrap();
                        assert!(
                            gate.check_command(
                                &submit_cmd(&e2),
                                &cache,
                                UnixNanos::from(TS_NOW),
                            )
                            .is_ok()
                        );
                        apply_fill_to_cache_and_pos(
                            &mut cache,
                            &inst,
                            &mut e2,
                            entry_side,
                            "10",
                            px_e2,
                            pos_id,
                            &format!("T-I15-PO-E2-{plan_side}-{cont:?}"),
                            TS_NOW + 1,
                            false,
                        );
                    }

                    // PartiallyFilled OTO parent E-1 (6/10 -> filled=6, leaves=4, total=10).
                    let list_id = format!("OL-I15-PO-{plan_side}-{cont:?}");
                    let mut e1 = accept_order(make_contingent_limit_order(
                        inst.id(),
                        "O-I15-PO-E1",
                        entry_side,
                        "10",
                        px_e1,
                        false,
                        Some(ContingencyType::Oto),
                        Some(&list_id),
                        Some(vec!["O-I15-PO-X1", "O-I15-PO-X2"]),
                        None,
                    ));
                    cache.add_order(e1.clone(), None, None, true).unwrap();
                    assert!(
                        gate.check_command(
                            &submit_cmd(&e1),
                            &cache,
                            UnixNanos::from(TS_NOW + 2),
                        )
                        .is_ok()
                    );
                    apply_fill_to_cache_and_pos(
                        &mut cache,
                        &inst,
                        &mut e1,
                        entry_side,
                        "6",
                        px_e1,
                        pos_id,
                        &format!("T-I15-PO-E1-{plan_side}-{cont:?}"),
                        TS_NOW + 3,
                        open_e2,
                    );
                    assert_eq!(
                        cache.order(&e1.client_order_id()).unwrap().status(),
                        OrderStatus::PartiallyFilled
                    );

                    // OTO child X-1(6, ro=True) linked OCO/OUO with X-2(6, ro=True).
                    let mut x1 = accept_order(make_contingent_limit_order(
                        inst.id(),
                        "O-I15-PO-X1",
                        exit_side,
                        "6",
                        px_x1,
                        true,
                        Some(cont),
                        Some(&list_id),
                        Some(vec!["O-I15-PO-X2"]),
                        Some("O-I15-PO-E1"),
                    ));
                    let x2 = accept_order(make_contingent_limit_order(
                        inst.id(),
                        "O-I15-PO-X2",
                        exit_side,
                        "6",
                        px_x2,
                        true,
                        Some(cont),
                        Some(&list_id),
                        Some(vec!["O-I15-PO-X1"]),
                        Some("O-I15-PO-E1"),
                    ));
                    cache.add_order(x1.clone(), None, None, true).unwrap();
                    cache.add_order(x2.clone(), None, None, true).unwrap();
                    assert!(
                        gate.check_command(
                            &submit_cmd(&x1),
                            &cache,
                            UnixNanos::from(TS_NOW + 4),
                        )
                        .is_ok()
                    );
                    assert!(
                        gate.check_command(
                            &submit_cmd(&x2),
                            &cache,
                            UnixNanos::from(TS_NOW + 5),
                        )
                        .is_ok()
                    );

                    // X-1 partially fills 4/6 -> filled=4, leaves=2; open_pos = 2 (or 12 if open_e2).
                    apply_fill_to_cache_and_pos(
                        &mut cache,
                        &inst,
                        &mut x1,
                        exit_side,
                        "4",
                        px_x1,
                        pos_id,
                        &format!("T-I15-PO-X1-{plan_side}-{cont:?}"),
                        TS_NOW + 6,
                        true,
                    );
                    assert_eq!(
                        cache.order(&x1.client_order_id()).unwrap().status(),
                        OrderStatus::PartiallyFilled
                    );
                    assert_eq!(
                        cache.order(&x1.client_order_id()).unwrap().filled_qty(),
                        Quantity::from("4")
                    );
                    assert_eq!(
                        cache.order(&x1.client_order_id()).unwrap().leaves_qty(),
                        Quantity::from("2")
                    );

                    if entry_freeze {
                        gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                    }

                    // ModifyOrders([(X-1, 6), (X-2, 2)]) -> 4 + max(2, 2) = 6 <= 10 (and <= open_pos=2|12) -> ADMITTED.
                    let batch_6_2 = BatchModifyOrders::new(
                        x1.trader_id(),
                        Some(ClientId::from("ALPACA")),
                        x1.strategy_id(),
                        inst.id(),
                        vec![
                            modify_cmd(&x1, Some("6"), Some(px_x1_mod), None),
                            modify_cmd(&x2, Some("2"), Some(px_x2_mod), None),
                        ],
                        UUID4::new(),
                        UnixNanos::from(TS_NOW + 7),
                        None,
                        None,
                    );
                    let res_6_2 = gate.check_command(
                        &TradingCommand::ModifyOrders(batch_6_2),
                        &cache,
                        UnixNanos::from(TS_NOW + 7),
                    );
                    assert!(
                        res_6_2.is_ok(),
                        "{plan_side} {cont:?} open_e2={open_e2} freeze={entry_freeze}: ModifyOrders((X-1, 6), (X-2, 2)) -> 4+max(2,2)=6<=10 must be ADMITTED: {res_6_2:?}"
                    );

                    // ModifyOrders([(X-1, 8), (X-2, 4)]): new_leaves=4 -> 4 + max(4, 4) = 8 <= E-1.qty(10),
                    // but when open_e2=false (open_pos=2 < 4) open_pos constraint MUST reject,
                    // whereas when open_e2=true (open_pos=12 >= 4) both open_pos and parent_total_qty pass -> ADMITTED.
                    let batch_8_4 = BatchModifyOrders::new(
                        x1.trader_id(),
                        Some(ClientId::from("ALPACA")),
                        x1.strategy_id(),
                        inst.id(),
                        vec![
                            modify_cmd(&x1, Some("8"), Some(px_x1_mod), None),
                            modify_cmd(&x2, Some("4"), Some(px_x2_mod), None),
                        ],
                        UUID4::new(),
                        UnixNanos::from(TS_NOW + 8),
                        None,
                        None,
                    );
                    let res_8_4 = gate.check_command(
                        &TradingCommand::ModifyOrders(batch_8_4),
                        &cache,
                        UnixNanos::from(TS_NOW + 8),
                    );
                    if open_e2 {
                        assert!(
                            res_8_4.is_ok(),
                            "{plan_side} {cont:?} open_e2=true freeze={entry_freeze}: ModifyOrders((X-1, 8), (X-2, 4)) -> 4+4=8<=10 & <=12 open_pos must be ADMITTED: {res_8_4:?}"
                        );
                        // Exceeding parent_total_qty=10 when open_pos=12: ModifyOrders([(X-1, 8), (X-2, 8)]) -> 4+max(4,8)=12 > 10 -> REJECTED.
                        let batch_over_parent = BatchModifyOrders::new(
                            x1.trader_id(),
                            Some(ClientId::from("ALPACA")),
                            x1.strategy_id(),
                            inst.id(),
                            vec![
                                modify_cmd(&x1, Some("8"), Some(px_x1_mod), None),
                                modify_cmd(&x2, Some("8"), Some(px_x2_mod), None),
                            ],
                            UUID4::new(),
                            UnixNanos::from(TS_NOW + 9),
                            None,
                            None,
                        );
                        let err_over_parent = gate
                            .check_command(
                                &TradingCommand::ModifyOrders(batch_over_parent),
                                &cache,
                                UnixNanos::from(TS_NOW + 9),
                            )
                            .unwrap_err();
                        assert!(
                            err_over_parent.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                            "{plan_side} {cont:?} open_e2=true freeze={entry_freeze}: ModifyOrders((X-1, 8), (X-2, 8)) -> 4+8=12>10 must reject, got: {err_over_parent}"
                        );
                    } else {
                        let err_open_pos = res_8_4.unwrap_err();
                        assert!(
                            err_open_pos.contains("REDUCE_ONLY_MODIFY_EXCEEDS_POSITION"),
                            "{plan_side} {cont:?} open_e2=false freeze={entry_freeze}: ModifyOrders((X-1, 8), (X-2, 4)) -> new_leaves=4 > open_pos=2 must reject, got: {err_open_pos}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn test_iter15_reduce_only_exit_downstream_denial_rollback_does_not_block_same_callback_exit() {
    for entry_freeze in [false, true] {
        for (plan_side, pos_side, exit_side, initial_stop, px_bad, px_good) in [
            ("LONG", OrderSide::Buy, OrderSide::Sell, dec!(145.00), "160.00", "152.00"),
            ("SHORT", OrderSide::Sell, OrderSide::Buy, dec!(155.00), "160.00", "145.00"),
        ] {
            // Case 1: Single SubmitOrder(X-bad, 10, ro=True) admitted in Initialized,
            // then rolled back via on_order_denied (even while still Initialized in cache)
            // -> subsequent X-good(10, ro=True) in the same callback MUST be admitted.
            {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                open_position_in_cache(
                    &mut cache,
                    &inst,
                    pos_side,
                    "10",
                    "150.00",
                    &format!("P-I15-RO-RB-{plan_side}-{entry_freeze}"),
                );
                if entry_freeze {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                let x_bad = make_limit_order(
                    inst.id(),
                    "O-I15-RO-XBAD",
                    exit_side,
                    "10",
                    px_bad,
                    true,
                );
                let x_good = make_limit_order(
                    inst.id(),
                    "O-I15-RO-XGOOD",
                    exit_side,
                    "10",
                    px_good,
                    true,
                );
                cache.add_order(x_bad.clone(), None, None, false).unwrap();
                cache.add_order(x_good.clone(), None, None, false).unwrap();

                assert!(
                    gate.check_command(&submit_cmd(&x_bad), &cache, UnixNanos::from(TS_NOW))
                        .is_ok(),
                    "{plan_side} freeze={entry_freeze}: initial X-bad(10, ro=True) must be admitted by gate"
                );
                assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
                assert!(!gate.has_admitted_reservation("O-I15-RO-XBAD"));

                // While X-bad is still admitted (pre-rollback), an unlinked X-good(10, ro=True) is blocked.
                let err_before_rollback = gate
                    .check_command(&submit_cmd(&x_good), &cache, UnixNanos::from(TS_NOW))
                    .unwrap_err();
                assert!(
                    err_before_rollback.contains("REDUCE_ONLY_EXIT_EXCEEDS_POSITION"),
                    "{plan_side} freeze={entry_freeze}: pre-rollback X-good(10, ro=True) must see X-bad(10), got: {err_before_rollback}"
                );

                // Downstream RiskEngine denial invokes on_order_denied (idempotent), while X-bad is still Initialized in cache.
                gate.on_order_denied(&x_bad.client_order_id());
                gate.on_order_denied(&x_bad.client_order_id());

                let res_good = gate.check_command(
                    &submit_cmd(&x_good),
                    &cache,
                    UnixNanos::from(TS_NOW + 1),
                );
                assert!(
                    res_good.is_ok(),
                    "{plan_side} freeze={entry_freeze}: after on_order_denied(X-bad), X-good(10, ro=True) must be ADMITTED: {res_good:?}"
                );
                assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
                assert!(!gate.has_admitted_reservation("O-I15-RO-XGOOD"));
            }

            // Case 2: SubmitOrderList([X-bad1(5, ro=True), X-bad2(5, ro=True)]) admitted,
            // then rolled back via on_order_denied for both legs -> X-good(10, ro=True) admitted.
            {
                let (inst, mut cache, gate) =
                    setup_aapl_cache_and_gate(plan_side, dec!(50), initial_stop);
                open_position_in_cache(
                    &mut cache,
                    &inst,
                    pos_side,
                    "10",
                    "150.00",
                    &format!("P-I15-RO-RB-LST-{plan_side}-{entry_freeze}"),
                );
                if entry_freeze {
                    gate.set_entry_freeze(true, "FREEZE_ACTIVE".to_string());
                }

                let x_bad1 = make_limit_order(
                    inst.id(),
                    "O-I15-RO-LB1",
                    exit_side,
                    "5",
                    px_bad,
                    true,
                );
                let x_bad2 = make_limit_order(
                    inst.id(),
                    "O-I15-RO-LB2",
                    exit_side,
                    "5",
                    px_bad,
                    true,
                );
                let x_good = make_limit_order(
                    inst.id(),
                    "O-I15-RO-LGOOD",
                    exit_side,
                    "10",
                    px_good,
                    true,
                );
                cache.add_order(x_bad1.clone(), None, None, false).unwrap();
                cache.add_order(x_bad2.clone(), None, None, false).unwrap();
                cache.add_order(x_good.clone(), None, None, false).unwrap();

                let batch = vec![x_bad1.clone(), x_bad2.clone()];
                assert!(
                    gate.check_command(
                        &make_mixed_order_list_cmd(&format!("OL-I15-RB-{plan_side}"), &batch),
                        &cache,
                        UnixNanos::from(TS_NOW),
                    )
                    .is_ok(),
                    "{plan_side} freeze={entry_freeze}: batch [X-bad1(5), X-bad2(5)] ro=True must be admitted by gate"
                );

                gate.on_order_denied(&x_bad1.client_order_id());
                gate.on_order_denied(&x_bad2.client_order_id());

                let res_good = gate.check_command(
                    &submit_cmd(&x_good),
                    &cache,
                    UnixNanos::from(TS_NOW + 1),
                );
                assert!(
                    res_good.is_ok(),
                    "{plan_side} freeze={entry_freeze}: after SubmitOrderList on_order_denied rollback, X-good(10, ro=True) must be ADMITTED: {res_good:?}"
                );
                assert_eq!(gate.reserved_qty("AAPL"), dec!(0));
            }
        }
    }
}
