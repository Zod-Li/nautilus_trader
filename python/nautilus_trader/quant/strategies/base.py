"""Shared base classes and PlanGate order routing helpers for pre-audited Nautilus v2 strategies."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional

from nautilus_trader.quant.contracts.models import (
    ArchetypeStrategyConfig,
    PlanItemContract,
    PlanSide,
    StrategyArchetypeId,
)
from nautilus_trader.quant.governance.plan_gate import PLAN_GATE_EXEC_ALGORITHM_ID
from nautilus_trader.model import (
    Bar,
    BarType,
    ExecAlgorithmId,
    InstrumentId,
    OrderSide,
    Price,
    Quantity,
    StrategyId,
    TimeInForce,
)
from nautilus_trader.trading import Strategy, StrategyConfig

TIMEFRAME_TO_BAR_SPEC: Dict[str, str] = {
    "1m": "1-MINUTE-LAST-EXTERNAL",
    "5m": "5-MINUTE-LAST-EXTERNAL",
    "15m": "15-MINUTE-LAST-EXTERNAL",
    "1h": "1-HOUR-LAST-EXTERNAL",
    "4h": "4-HOUR-LAST-EXTERNAL",
    "1d": "1-DAY-LAST-EXTERNAL",
    "1w": "1-WEEK-LAST-EXTERNAL",
}


def format_datainfra_bar_type(instrument_id: str | InstrumentId, bar_timeframe: str) -> BarType:
    """Formats a canonical Nautilus `BarType` for `instrument_id` and `bar_timeframe`."""
    inst_str = str(instrument_id).strip()
    tf = str(bar_timeframe).strip()
    spec = TIMEFRAME_TO_BAR_SPEC.get(tf)
    if spec is None:
        raise ValueError(
            f"Unsupported bar_timeframe '{bar_timeframe}', expected one of {tuple(TIMEFRAME_TO_BAR_SPEC)}"
        )
    return BarType.from_str(f"{inst_str}-{spec}")


def _to_decimal(val: Any, default: str = "0") -> Decimal:
    if val is None:
        return Decimal(default)
    if isinstance(val, Decimal):
        return val
    text = str(val).strip()
    if not text:
        return Decimal(default)
    token = text.split()[0]
    return Decimal(token)


class BaseArchetypeStrategyConfig(StrategyConfig):
    """Base `StrategyConfig` for pre-audited empirical-distribution-driven strategies."""

    ARCHETYPE_ID: StrategyArchetypeId = StrategyArchetypeId.TREND_MOMENTUM

    def __new__(
        cls,
        instrument_id: str | InstrumentId,
        bar_timeframe: str = "15m",
        lookback_window: int = 30,
        fast_window: int = 5,
        slow_window: int = 20,
        entry_quantile_threshold: Decimal | float | str = Decimal("0.80"),
        exit_quantile_threshold: Decimal | float | str = Decimal("0.35"),
        atr_stop_multiplier: Decimal | float | str = Decimal("2.0"),
        trade_qty: Decimal | float | int | str = Decimal("100"),
        plan_side: str | PlanSide = PlanSide.LONG,
        secondary_instrument_id: Optional[str | InstrumentId] = None,
        use_limit_orders: bool = True,
        submit_protective_stop: bool = False,
        exec_algorithm_id: str | ExecAlgorithmId = PLAN_GATE_EXEC_ALGORITHM_ID,
        strategy_id: Optional[str | StrategyId] = None,
        order_id_tag: Optional[str] = None,
        manage_contingent_orders: bool = False,
        log_events: bool = True,
        log_commands: bool = True,
    ):
        inst_str = str(instrument_id).strip()
        sym_clean = inst_str.split(".", 1)[0].replace("/", "")
        default_tag = str(order_id_tag or sym_clean[:8] or "001")
        resolved_strategy_id = (
            strategy_id
            if isinstance(strategy_id, StrategyId)
            else StrategyId(
                str(strategy_id or f"{cls.ARCHETYPE_ID.value}-{default_tag}")
            )
        )
        return super().__new__(
            cls,
            strategy_id=resolved_strategy_id,
            order_id_tag=default_tag,
            manage_contingent_orders=manage_contingent_orders,
            log_events=log_events,
            log_commands=log_commands,
        )

    def __init__(
        self,
        instrument_id: str | InstrumentId,
        bar_timeframe: str = "15m",
        lookback_window: int = 30,
        fast_window: int = 5,
        slow_window: int = 20,
        entry_quantile_threshold: Decimal | float | str = Decimal("0.80"),
        exit_quantile_threshold: Decimal | float | str = Decimal("0.35"),
        atr_stop_multiplier: Decimal | float | str = Decimal("2.0"),
        trade_qty: Decimal | float | int | str = Decimal("100"),
        plan_side: str | PlanSide = PlanSide.LONG,
        secondary_instrument_id: Optional[str | InstrumentId] = None,
        use_limit_orders: bool = True,
        submit_protective_stop: bool = False,
        exec_algorithm_id: str | ExecAlgorithmId = PLAN_GATE_EXEC_ALGORITHM_ID,
        strategy_id: Optional[str | StrategyId] = None,
        order_id_tag: Optional[str] = None,
        manage_contingent_orders: bool = False,
        log_events: bool = True,
        log_commands: bool = True,
    ) -> None:
        inst_str = str(instrument_id).strip()
        sec_str = (
            str(secondary_instrument_id).strip()
            if secondary_instrument_id is not None
            else None
        )
        self.archetype_contract = ArchetypeStrategyConfig(
            archetype_id=self.ARCHETYPE_ID,
            instrument_id=inst_str,
            bar_timeframe=str(bar_timeframe).strip(),
            lookback_window=int(lookback_window),
            fast_window=int(fast_window),
            slow_window=int(slow_window),
            entry_quantile_threshold=_to_decimal(entry_quantile_threshold),
            exit_quantile_threshold=_to_decimal(exit_quantile_threshold),
            atr_stop_multiplier=_to_decimal(atr_stop_multiplier),
            secondary_instrument_id=sec_str,
        )
        self.instrument_id = InstrumentId.from_str(inst_str)
        self.secondary_instrument_id = (
            InstrumentId.from_str(sec_str) if sec_str else None
        )
        self.bar_timeframe = self.archetype_contract.bar_timeframe
        self.bar_type = format_datainfra_bar_type(inst_str, self.bar_timeframe)
        self.secondary_bar_type = (
            format_datainfra_bar_type(sec_str, self.bar_timeframe)
            if sec_str
            else None
        )
        self.lookback_window = self.archetype_contract.lookback_window
        self.fast_window = self.archetype_contract.fast_window
        self.slow_window = self.archetype_contract.slow_window
        self.entry_quantile_threshold = (
            self.archetype_contract.entry_quantile_threshold
        )
        self.exit_quantile_threshold = self.archetype_contract.exit_quantile_threshold
        self.atr_stop_multiplier = self.archetype_contract.atr_stop_multiplier
        self.trade_qty = _to_decimal(trade_qty, "100")
        if self.trade_qty <= Decimal("0"):
            raise ValueError(f"trade_qty must be > 0, got {self.trade_qty}")
        self.plan_side = (
            plan_side
            if isinstance(plan_side, PlanSide)
            else PlanSide(str(plan_side).strip().lower())
        )
        self.use_limit_orders = bool(use_limit_orders)
        self.submit_protective_stop = bool(submit_protective_stop)
        self.exec_algorithm_id = (
            exec_algorithm_id
            if isinstance(exec_algorithm_id, ExecAlgorithmId)
            else ExecAlgorithmId(str(exec_algorithm_id or PLAN_GATE_EXEC_ALGORITHM_ID))
        )

    @classmethod
    def from_plan_item(
        cls,
        plan_item: PlanItemContract,
        use_limit_orders: bool = True,
        submit_protective_stop: bool = False,
    ) -> "BaseArchetypeStrategyConfig":
        cfg = plan_item.strategy_config
        return cls(
            instrument_id=cfg.instrument_id,
            bar_timeframe=cfg.bar_timeframe,
            lookback_window=cfg.lookback_window,
            fast_window=cfg.fast_window,
            slow_window=cfg.slow_window,
            entry_quantile_threshold=cfg.entry_quantile_threshold,
            exit_quantile_threshold=cfg.exit_quantile_threshold,
            atr_stop_multiplier=cfg.atr_stop_multiplier,
            trade_qty=plan_item.risk_envelope.target_qty,
            plan_side=plan_item.risk_envelope.side,
            secondary_instrument_id=cfg.secondary_instrument_id,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            order_id_tag=plan_item.symbol[:8],
        )


class BaseArchetypeStrategy(Strategy):
    """Base pre-audited Nautilus `Strategy` routing all orders through `PLAN_GATE`."""

    def __new__(cls, config: BaseArchetypeStrategyConfig) -> "BaseArchetypeStrategy":
        return super().__new__(cls, config)

    def __init__(self, config: BaseArchetypeStrategyConfig) -> None:
        super().__init__(config)
        self._archetype_config = config
        self._highs: List[float] = []
        self._lows: List[float] = []
        self._closes: List[float] = []
        self._volumes: List[float] = []
        self._trailing_stop_price: Optional[float] = None
        self._submitted_order_ids: List[str] = []

    @property
    def archetype_config(self) -> BaseArchetypeStrategyConfig:
        return self._archetype_config

    @property
    def trailing_stop_price(self) -> Optional[float]:
        return self._trailing_stop_price

    @property
    def submitted_order_ids(self) -> List[str]:
        return list(self._submitted_order_ids)

    def on_start(self) -> None:
        self.subscribe_bars(self._archetype_config.bar_type)
        if self._archetype_config.secondary_bar_type is not None:
            self.subscribe_bars(self._archetype_config.secondary_bar_type)

    def _record_primary_bar(self, bar: Bar) -> None:
        self._highs.append(float(bar.high.as_double()))
        self._lows.append(float(bar.low.as_double()))
        self._closes.append(float(bar.close.as_double()))
        self._volumes.append(float(bar.volume.as_double()))
        max_keep = max(self._archetype_config.lookback_window * 3, 120)
        if len(self._closes) > max_keep:
            del self._highs[:-max_keep]
            del self._lows[:-max_keep]
            del self._closes[:-max_keep]
            del self._volumes[:-max_keep]

    def _open_position_signed_qty(
        self, instrument_id: Optional[InstrumentId] = None
    ) -> Decimal:
        target_inst = instrument_id or self._archetype_config.instrument_id
        total = Decimal("0")
        try:
            positions = self.cache.positions_open(instrument_id=target_inst)
        except Exception:
            positions = []
        for pos in positions or []:
            qty = _to_decimal(getattr(pos, "quantity", 0))
            side_str = str(getattr(pos, "side", "LONG")).upper()
            if "SHORT" in side_str:
                total -= qty
            elif "LONG" in side_str:
                total += qty
        return total

    def _has_inflight_or_open_entry_order(
        self, instrument_id: Optional[InstrumentId] = None
    ) -> bool:
        target_inst = instrument_id or self._archetype_config.instrument_id
        try:
            open_orders = self.cache.orders_open(instrument_id=target_inst)
            inflight_orders = self.cache.orders_inflight(instrument_id=target_inst)
        except Exception:
            open_orders = []
            inflight_orders = []
        for order in list(open_orders or []) + list(inflight_orders or []):
            if not bool(getattr(order, "is_reduce_only", False)):
                return True
        return False

    def _format_quantity(
        self, raw_qty: Decimal | float, instrument_id: Optional[InstrumentId] = None
    ) -> Quantity:
        target_inst = instrument_id or self._archetype_config.instrument_id
        dec_qty = _to_decimal(raw_qty, "1")
        try:
            inst = self.cache.instrument(target_inst)
            if inst is not None:
                prec = int(getattr(inst, "size_precision", 0) or 0)
                if prec <= 0:
                    return Quantity.from_int(max(1, int(round(float(dec_qty)))))
                return Quantity(float(dec_qty), prec)
        except Exception:
            pass
        if dec_qty == dec_qty.to_integral_value():
            return Quantity.from_int(max(1, int(dec_qty)))
        return Quantity.from_str(str(dec_qty))

    def _format_price(
        self, raw_price: Decimal | float, instrument_id: Optional[InstrumentId] = None
    ) -> Price:
        target_inst = instrument_id or self._archetype_config.instrument_id
        val = float(raw_price)
        try:
            inst = self.cache.instrument(target_inst)
            if inst is not None:
                prec = int(getattr(inst, "price_precision", 2) or 2)
                return Price(round(val, prec), prec)
        except Exception:
            pass
        return Price(round(val, 2), 2)

    def _submit_plan_gated_entry(
        self,
        order_side: OrderSide,
        reference_price: float,
        atr: float,
        limit_price: Optional[float] = None,
        quantity: Optional[Decimal | float] = None,
        instrument_id: Optional[InstrumentId] = None,
        stop_loss_price: Optional[float] = None,
        tags: Optional[List[str]] = None,
    ) -> str:
        """Creates and submits a PlanGate-routed entry order (`exec_algorithm_id='PLAN_GATE'`)."""
        target_inst = instrument_id or self._archetype_config.instrument_id
        qty_obj = self._format_quantity(
            quantity if quantity is not None else self._archetype_config.trade_qty,
            target_inst,
        )
        exec_params = {
            "intent": "ENTRY",
            "reference_price": f"{reference_price:.6f}",
            "atr": f"{max(0.0, atr):.6f}",
            "archetype_id": self._archetype_config.ARCHETYPE_ID.value,
        }
        if stop_loss_price is not None and stop_loss_price > 0.0:
            exec_params["stop_loss_price"] = f"{stop_loss_price:.6f}"

        eff_limit = limit_price if limit_price is not None else reference_price
        if self._archetype_config.use_limit_orders and eff_limit > 0.0:
            order = self.order_factory.limit(
                instrument_id=target_inst,
                order_side=order_side,
                quantity=qty_obj,
                price=self._format_price(eff_limit, target_inst),
                time_in_force=TimeInForce.DAY,
                reduce_only=False,
                exec_algorithm_id=self._archetype_config.exec_algorithm_id,
                exec_algorithm_params=exec_params,
                tags=tags,
            )
        else:
            order = self.order_factory.market(
                instrument_id=target_inst,
                order_side=order_side,
                quantity=qty_obj,
                time_in_force=TimeInForce.GTC,
                reduce_only=False,
                exec_algorithm_id=self._archetype_config.exec_algorithm_id,
                exec_algorithm_params=exec_params,
                tags=tags,
            )

        cid = str(order.client_order_id)
        self._submitted_order_ids.append(cid)
        self.submit_order(order)
        return cid

    def _submit_plan_gated_exit(
        self,
        exit_side: OrderSide,
        quantity: Decimal | float,
        reference_price: float,
        reason_tag: str = "SIGNAL_EXIT",
        instrument_id: Optional[InstrumentId] = None,
    ) -> str:
        """Creates and submits a reduce-only exit order via `PLAN_GATE`."""
        target_inst = instrument_id or self._archetype_config.instrument_id
        qty_obj = self._format_quantity(abs(float(quantity)), target_inst)
        exec_params = {
            "intent": "EXIT",
            "reduce_only": "true",
            "reference_price": f"{reference_price:.6f}",
            "exit_reason": reason_tag,
            "archetype_id": self._archetype_config.ARCHETYPE_ID.value,
        }
        order = self.order_factory.market(
            instrument_id=target_inst,
            order_side=exit_side,
            quantity=qty_obj,
            time_in_force=TimeInForce.GTC,
            reduce_only=True,
            exec_algorithm_id=self._archetype_config.exec_algorithm_id,
            exec_algorithm_params=exec_params,
            tags=[reason_tag],
        )
        cid = str(order.client_order_id)
        self._submitted_order_ids.append(cid)
        self.submit_order(order)
        self._trailing_stop_price = None
        return cid
