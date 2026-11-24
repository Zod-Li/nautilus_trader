// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
// -------------------------------------------------------------------------------------------------

//! Blind behavioral regression tests for Round 4 `nautilus-quant-execution` fixes (`D5-P1-9`):
//! - `validate_entry_modify` enforces `stop_loss_price > 0`, LONG (`stop < entry_ref`) / SHORT
//!   (`stop > entry_ref`) directional ordering, and `STOP_LOSS_FLOOR_VIOLATION`
//!   (`effective_stop_floor` / `effective_stop_ceiling`), even when modifying ONLY
//!   `stop_loss_price` or ONLY `new_qty`.

use nautilus_common::{
    cache::Cache,
    messages::execution::{ModifyOrder, TradingCommand},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    data::QuoteTick,
    enums::{ContingencyType, OrderSide, OrderStatus, TimeInForce, TriggerType},
    events::{OrderAccepted, OrderEventAny, OrderSubmitted},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, StrategyId, Symbol,
        TraderId, VenueOrderId,
    },
    instruments::{Equity, Instrument, InstrumentAny},
    orders::{LimitOrder, Order, OrderAny, StopMarketOrder},
    types::{Currency, Price, Quantity},
};
use nautilus_quant_execution::{RustPlanGateEngine, RustPlanItemEnvelope};
use nautilus_risk::PreTradeRiskGate;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

const TS_NOW: u64 = 1_700_000_000_000_000_000;
const TS_FUTURE: i64 = 1_900_000_000_000_000_000;

fn make_equity(symbol: &str) -> InstrumentAny {
    Equity::builder()
        .instrument_id(InstrumentId::from(format!("{symbol}.ALPACA").as_str()))
        .raw_symbol(Symbol::from(symbol))
        .currency(Currency::USD())
        .price_precision(2)
        .price_increment(Price::from("0.01"))
        .lot_size(Quantity::from("1"))
        .ts_event(UnixNanos::from(TS_NOW))
        .ts_init(UnixNanos::from(TS_NOW))
        .build()
        .unwrap()
        .into_any()
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

fn make_item(
    symbol: &str,
    side: &str,
    target_qty: Decimal,
    target_notional: Decimal,
    initial_stop_price: Decimal,
    max_risk_budget_usd: Decimal,
) -> RustPlanItemEnvelope {
    RustPlanItemEnvelope {
        item_id: format!("item-{symbol}"),
        plan_id: "plan-r4".to_string(),
        symbol: symbol.to_string(),
        side: side.to_string(),
        status: "ARMED".to_string(),
        target_qty,
        approved_qty_cap: target_qty,
        target_notional_usd: target_notional,
        filled_qty: dec!(0),
        valid_until_ns: TS_FUTURE,
        valid_until_iso: "2030-01-01T00:00:00Z".to_string(),
        limit_collar_pct: dec!(0.50),
        limit_collar_atr: dec!(0.50),
        initial_stop_price,
        max_risk_budget_usd,
        daily_atr_14: Some(dec!(2.00)),
    }
}

#[test]
fn test_round4_d5_p1_9_validate_entry_modify_enforces_stop_loss_when_only_stop_or_qty_modified() {
    let engine = RustPlanGateEngine::new(false, String::new());
    // LONG: implied_plan_ref = 100.00, initial_stop_price = 90.00, max_risk_budget_usd = 500
    engine.upsert_plan_item(
        make_item("AAPL", "LONG", dec!(10), dec!(1000), dec!(90.00), dec!(500)),
        false,
    );
    // SHORT: implied_plan_ref = 100.00, initial_stop_price = 110.00, max_risk_budget_usd = 500
    engine.upsert_plan_item(
        make_item("TSLA", "SHORT", dec!(10), dec!(1000), dec!(110.00), dec!(500)),
        false,
    );

    // 1) Modifying ONLY `stop_loss_price` (new_qty=None, new_limit_price=None, new_trigger_price=None)
    // 1a. LONG stop <= 0 -> INVALID_STOP_LOSS_PRICE
    for bad_stop in [dec!(0), dec!(-5.00)] {
        let err = engine
            .validate_entry_modify(
                "AAPL",
                None,
                dec!(10),
                None,
                Some(dec!(100.00)),
                None,
                Some(bad_stop),
                None,
                None,
                TS_NOW as i64,
                "2026-10-22T14:30:00Z",
            )
            .expect_err("zero/negative stop must fail");
        assert!(err.contains("INVALID_STOP_LOSS_PRICE"), "got: {err}");
    }

    // 1b. LONG stop >= entry_ref -> INVALID_STOP_LOSS_PRICE
    for bad_long_stop in [dec!(100.00), dec!(102.00)] {
        let err = engine
            .validate_entry_modify(
                "AAPL",
                None,
                dec!(10),
                None,
                Some(dec!(100.00)),
                None,
                Some(bad_long_stop),
                None,
                None,
                TS_NOW as i64,
                "2026-10-22T14:30:00Z",
            )
            .expect_err("LONG stop >= entry_ref must fail");
        assert!(err.contains("INVALID_STOP_LOSS_PRICE"), "got: {err}");
    }

    // 1c. LONG stop < effective_stop_floor (90.00) -> STOP_LOSS_FLOOR_VIOLATION
    let err_floor = engine
        .validate_entry_modify(
            "AAPL",
            None,
            dec!(10),
            None,
            Some(dec!(100.00)),
            None,
            Some(dec!(89.50)),
            None,
            None,
            TS_NOW as i64,
            "2026-10-22T14:30:00Z",
        )
        .expect_err("LONG stop below initial_stop_price must fail");
    assert!(
        err_floor.contains("STOP_LOSS_FLOOR_VIOLATION"),
        "got: {err_floor}"
    );

    // 1d. LONG gap-down branch (`entry_ref <= initial_stop_price`):
    //     implied_plan_ref=100, initial_stop=90 => max_plan_stop_dist=10.
    //     With current_limit_price=88 (<=90), effective_stop_floor = 88 - 10 = 78.
    let err_gap_floor = engine
        .validate_entry_modify(
            "AAPL",
            None,
            dec!(10),
            None,
            Some(dec!(88.00)),
            None,
            Some(dec!(77.50)),
            None,
            None,
            TS_NOW as i64,
            "2026-10-22T14:30:00Z",
        )
        .expect_err("LONG gap-down stop below effective_stop_floor must fail");
    assert!(
        err_gap_floor.contains("STOP_LOSS_FLOOR_VIOLATION"),
        "got: {err_gap_floor}"
    );
    assert!(
        engine
            .validate_entry_modify(
                "AAPL",
                None,
                dec!(10),
                None,
                Some(dec!(88.00)),
                None,
                Some(dec!(79.00)),
                None,
                None,
                TS_NOW as i64,
                "2026-10-22T14:30:00Z",
            )
            .is_ok()
    );

    // 1e. SHORT stop <= 0 / stop <= entry_ref / stop > effective_stop_ceiling
    let err_short_dir = engine
        .validate_entry_modify(
            "TSLA",
            None,
            dec!(10),
            None,
            Some(dec!(100.00)),
            None,
            Some(dec!(99.00)),
            None,
            None,
            TS_NOW as i64,
            "2026-10-22T14:30:00Z",
        )
        .expect_err("SHORT stop <= entry_ref must fail");
    assert!(
        err_short_dir.contains("INVALID_STOP_LOSS_PRICE"),
        "got: {err_short_dir}"
    );

    let err_short_ceil = engine
        .validate_entry_modify(
            "TSLA",
            None,
            dec!(10),
            None,
            Some(dec!(100.00)),
            None,
            Some(dec!(111.00)),
            None,
            None,
            TS_NOW as i64,
            "2026-10-22T14:30:00Z",
        )
        .expect_err("SHORT stop > effective_stop_ceiling must fail");
    assert!(
        err_short_ceil.contains("STOP_LOSS_FLOOR_VIOLATION"),
        "got: {err_short_ceil}"
    );

    // SHORT gap-up branch (`entry_ref >= initial_stop_price`):
    // implied_plan_ref=100, initial_stop=110 => max_plan_stop_dist=10.
    // With current_limit_price=112 (>=110), effective_stop_ceiling = 112 + 10 = 122.
    let err_short_gap_ceil = engine
        .validate_entry_modify(
            "TSLA",
            None,
            dec!(10),
            None,
            Some(dec!(112.00)),
            None,
            Some(dec!(123.00)),
            None,
            None,
            TS_NOW as i64,
            "2026-10-22T14:30:00Z",
        )
        .expect_err("SHORT gap-up stop above effective_stop_ceiling must fail");
    assert!(
        err_short_gap_ceil.contains("STOP_LOSS_FLOOR_VIOLATION"),
        "got: {err_short_gap_ceil}"
    );
    assert!(
        engine
            .validate_entry_modify(
                "TSLA",
                None,
                dec!(10),
                None,
                Some(dec!(112.00)),
                None,
                Some(dec!(120.00)),
                None,
                None,
                TS_NOW as i64,
                "2026-10-22T14:30:00Z",
            )
            .is_ok()
    );

    // 2) Modifying ONLY `new_qty` (new_limit_price=None, new_trigger_price=None)
    //    must STILL validate stop_loss_price & risk budget!
    let err_qty_only_floor = engine
        .validate_entry_modify(
            "AAPL",
            Some(dec!(8)),
            dec!(10),
            None,
            Some(dec!(100.00)),
            None,
            Some(dec!(85.00)),
            None,
            None,
            TS_NOW as i64,
            "2026-10-22T14:30:00Z",
        )
        .expect_err("qty-only modify with stop below floor must fail");
    assert!(
        err_qty_only_floor.contains("STOP_LOSS_FLOOR_VIOLATION"),
        "got: {err_qty_only_floor}"
    );

    let err_qty_only_dir = engine
        .validate_entry_modify(
            "AAPL",
            Some(dec!(8)),
            dec!(10),
            None,
            Some(dec!(100.00)),
            None,
            Some(dec!(101.00)),
            None,
            None,
            TS_NOW as i64,
            "2026-10-22T14:30:00Z",
        )
        .expect_err("qty-only modify with LONG stop >= entry_ref must fail");
    assert!(
        err_qty_only_dir.contains("INVALID_STOP_LOSS_PRICE"),
        "got: {err_qty_only_dir}"
    );

    // Budget enforcement when modifying ONLY `new_qty` (`stop_loss_price = None`, uses `initial_stop_price`)
    let tight_budget_engine = RustPlanGateEngine::new(false, String::new());
    tight_budget_engine.upsert_plan_item(
        make_item("NVDA", "LONG", dec!(10), dec!(1000), dec!(90.00), dec!(50)),
        false,
    );
    let err_qty_budget = tight_budget_engine
        .validate_entry_modify(
            "NVDA",
            Some(dec!(8)),
            dec!(10),
            None,
            Some(dec!(100.00)),
            None,
            None,
            None,
            None,
            TS_NOW as i64,
            "2026-10-22T14:30:00Z",
        )
        .expect_err("qty-only modify exceeding risk budget must fail");
    assert!(
        err_qty_budget.contains("RISK_BUDGET_EXCEEDED"),
        "got: {err_qty_budget}"
    );
    assert!(
        tight_budget_engine
            .validate_entry_modify(
                "NVDA",
                Some(dec!(5)),
                dec!(10),
                None,
                Some(dec!(100.00)),
                None,
                None,
                None,
                None,
                TS_NOW as i64,
                "2026-10-22T14:30:00Z",
            )
            .is_ok()
    );
}

#[test]
fn test_round4_d5_p1_9_pretrade_risk_gate_qty_only_modify_command_enforces_stop_floor() {
    let inst = make_equity("AAPL");
    let mut cache = Cache::default();
    cache.add_instrument(inst.clone()).unwrap();
    cache
        .add_quote(make_quote(inst.id(), "99.90", "100.00"))
        .unwrap();

    let gate = RustPlanGateEngine::new(false, String::new());
    gate.upsert_plan_item(
        make_item("AAPL", "LONG", dec!(10), dec!(1000), dec!(90.00), dec!(500)),
        false,
    );

    let mut entry = OrderAny::Limit(LimitOrder::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst.id(),
        ClientOrderId::from("CID-R4-QTYMOD"),
        OrderSide::Buy,
        Quantity::from("10"),
        Price::from("100.00"),
        TimeInForce::Day,
        None,
        false,
        false,
        false,
        None,
        None,
        None,
        Some(ContingencyType::Oto),
        Some(OrderListId::from("OL-R4-QTYMOD")),
        Some(vec![ClientOrderId::from("CID-R4-SL")]),
        None,
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
    ));
    let sub = OrderSubmitted::new(
        entry.trader_id(),
        entry.strategy_id(),
        entry.instrument_id(),
        entry.client_order_id(),
        AccountId::from("ALPACA-001"),
        UUID4::new(),
        UnixNanos::from(TS_NOW),
        UnixNanos::from(TS_NOW),
    );
    entry.apply(OrderEventAny::Submitted(sub)).unwrap();
    let acc = OrderAccepted::new(
        entry.trader_id(),
        entry.strategy_id(),
        entry.instrument_id(),
        entry.client_order_id(),
        VenueOrderId::from("VEN-R4-QTYMOD"),
        AccountId::from("ALPACA-001"),
        UUID4::new(),
        UnixNanos::from(TS_NOW),
        UnixNanos::from(TS_NOW),
        false,
    );
    entry.apply(OrderEventAny::Accepted(acc)).unwrap();
    assert_eq!(entry.status(), OrderStatus::Accepted);

    let sl = OrderAny::StopMarket(StopMarketOrder::new(
        TraderId::from("TRADER-001"),
        StrategyId::from("STRAT-001"),
        inst.id(),
        ClientOrderId::from("CID-R4-SL"),
        OrderSide::Sell,
        Quantity::from("10"),
        Price::from("85.00"),
        TriggerType::LastPrice,
        TimeInForce::Gtc,
        None,
        true,
        false,
        None,
        None,
        None,
        None,
        Some(OrderListId::from("OL-R4-QTYMOD")),
        None,
        Some(ClientOrderId::from("CID-R4-QTYMOD")),
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
    ));
    cache
        .add_order(entry.clone(), None, Some(ClientId::from("ALPACA")), false)
        .unwrap();
    cache
        .add_order(sl, None, Some(ClientId::from("ALPACA")), false)
        .unwrap();

    // Modify ONLY quantity (price=None, trigger_price=None) via PreTradeRiskGate::check_command
    let mod_cmd = TradingCommand::ModifyOrder(ModifyOrder::new(
        entry.trader_id(),
        Some(ClientId::from("ALPACA")),
        entry.strategy_id(),
        entry.instrument_id(),
        entry.client_order_id(),
        entry.venue_order_id(),
        Some(Quantity::from("8")),
        None,
        None,
        UUID4::new(),
        UnixNanos::from(TS_NOW),
        None,
        None,
    ));
    let err = gate
        .check_command(&mod_cmd, &cache, UnixNanos::from(TS_NOW))
        .expect_err("qty-only ModifyOrder with violating stop_loss_price must be denied");
    assert!(err.contains("STOP_LOSS_FLOOR_VIOLATION"), "got: {err}");
}
