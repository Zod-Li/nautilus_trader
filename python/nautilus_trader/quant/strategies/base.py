"""Shared base classes and PlanGate order routing helpers for pre-audited Nautilus v2 strategies."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
import logging
import threading
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from nautilus_trader.common import Clock, OrderFactory
from nautilus_trader.quant.contracts.models import (
    ArchetypeStrategyConfig,
    MarketableLimitCollar,
    PlanItemContract,
    PlanRiskEnvelope,
    PlanSide,
    StrategyArchetypeId,
)
from nautilus_trader.quant.governance.plan_gate import PLAN_GATE_EXEC_ALGORITHM_ID
from nautilus_trader.model import (
    Bar,
    BarType,
    ClientOrderId,
    ExecAlgorithmId,
    InstrumentId,
    OrderSide,
    Price,
    Quantity,
    StrategyId,
    TimeInForce,
    TraderId,
)
from nautilus_trader.trading import Strategy, StrategyConfig

logger = logging.getLogger(__name__)

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


def required_warmup_bars(lookback_window: int) -> int:
    """Computes the canonical historical warmup bar count `max(lookback_window * 3, 120)`."""
    if int(lookback_window) <= 0:
        raise ValueError(f"lookback_window must be > 0, got {lookback_window}")
    return max(int(lookback_window) * 3, 120)


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
        execution_delay_bars: int = 0,
        exec_algorithm_id: str | ExecAlgorithmId = PLAN_GATE_EXEC_ALGORITHM_ID,
        strategy_id: Optional[str | StrategyId] = None,
        order_id_tag: Optional[str] = None,
        manage_contingent_orders: bool = False,
        log_events: bool = True,
        log_commands: bool = True,
    ):
        _ = execution_delay_bars
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
        execution_delay_bars: int = 0,
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
        self.execution_delay_bars = max(0, int(execution_delay_bars))
        self.exec_algorithm_id = (
            exec_algorithm_id
            if isinstance(exec_algorithm_id, ExecAlgorithmId)
            else ExecAlgorithmId(str(exec_algorithm_id or PLAN_GATE_EXEC_ALGORITHM_ID))
        )
        sym_clean = inst_str.split(".", 1)[0].replace("/", "")
        default_tag = str(order_id_tag or sym_clean[:8] or "001")
        self._resolved_order_id_tag: str = default_tag
        self._resolved_strategy_id: StrategyId = (
            strategy_id
            if isinstance(strategy_id, StrategyId)
            else StrategyId(
                str(strategy_id or f"{self.ARCHETYPE_ID.value}-{default_tag}")
            )
        )
        self._plan_risk_envelope: Optional[PlanRiskEnvelope] = None
        self._plan_initial_stop_price: Optional[Decimal] = None
        self._plan_max_risk_budget_usd: Optional[Decimal] = None

    @property
    def required_warmup_bars(self) -> int:
        return required_warmup_bars(self.lookback_window)

    @classmethod
    def from_plan_item(
        cls,
        plan_item: PlanItemContract,
        use_limit_orders: bool = True,
        submit_protective_stop: bool = True,
        execution_delay_bars: int = 0,
    ) -> "BaseArchetypeStrategyConfig":
        cfg = plan_item.strategy_config
        instance = cls(
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
            execution_delay_bars=execution_delay_bars,
            order_id_tag=plan_item.symbol[:8],
        )
        instance._plan_risk_envelope = plan_item.risk_envelope
        instance._plan_initial_stop_price = plan_item.risk_envelope.initial_stop_price
        instance._plan_max_risk_budget_usd = plan_item.risk_envelope.max_risk_budget_usd
        return instance


_CROSS_THREAD_STRATEGY_LOCK = threading.Lock()
_CROSS_THREAD_STRATEGY_ANCHORS: List[Any] = []
_CROSS_THREAD_STRATEGY_ANCHOR_EVENT = threading.Event()
_CROSS_THREAD_STRATEGY_ANCHOR_THREAD: Optional[threading.Thread] = None


def _anchor_strategy_if_non_main_thread(strategy: Any) -> None:
    """Prevents PyO3 `unsendable` `PyStrategy` instances created on background threads
    from being deallocated on `MainThread` during garbage collection.
    """
    global _CROSS_THREAD_STRATEGY_ANCHOR_THREAD
    if threading.current_thread() is threading.main_thread():
        return
    with _CROSS_THREAD_STRATEGY_LOCK:
        _CROSS_THREAD_STRATEGY_ANCHORS.append(strategy)
        if (
            _CROSS_THREAD_STRATEGY_ANCHOR_THREAD is None
            or not _CROSS_THREAD_STRATEGY_ANCHOR_THREAD.is_alive()
        ):
            def _hold_refs() -> None:
                local_refs = _CROSS_THREAD_STRATEGY_ANCHORS
                _CROSS_THREAD_STRATEGY_ANCHOR_EVENT.wait()
                _ = len(local_refs)

            _CROSS_THREAD_STRATEGY_ANCHOR_THREAD = threading.Thread(
                target=_hold_refs,
                name="nautilus-pystrategy-anchor",
                daemon=True,
            )
            _CROSS_THREAD_STRATEGY_ANCHOR_THREAD.start()


class BaseArchetypeStrategy(Strategy):
    """Base pre-audited Nautilus `Strategy` routing all orders through `PLAN_GATE`."""

    def __new__(cls, config: BaseArchetypeStrategyConfig) -> "BaseArchetypeStrategy":
        return super().__new__(cls, config)

    def __init__(self, config: BaseArchetypeStrategyConfig) -> None:
        super().__init__(config)
        self._owner_thread_id: int = threading.get_ident()
        _anchor_strategy_if_non_main_thread(self)
        self._archetype_config = config
        self._risk_envelope: Optional[PlanRiskEnvelope] = getattr(
            config, "_plan_risk_envelope", None
        )
        self._highs: List[float] = []
        self._lows: List[float] = []
        self._closes: List[float] = []
        self._volumes: List[float] = []
        self._trailing_stop_price: Optional[float] = None
        self._submitted_order_ids: List[str] = []
        self._protective_stop_order_ids: List[str] = []
        self._pending_protective_stops: Dict[str, Dict[str, Any]] = {}
        self._active_protective_stops_by_inst: Dict[str, List[str]] = {}
        self._stop_orders_by_cid: Dict[str, Any] = {}
        self._stop_qty_by_cid: Dict[str, Decimal] = {}
        self._pending_safe_exits: Dict[str, Dict[str, Any]] = {}
        self._active_exit_orders_by_inst: Dict[str, str] = {}
        self._active_entry_orders_by_inst: Dict[str, Dict[str, Any]] = {}
        self._telemetry_actor_ref: Optional[Any] = None
        self._plan_gate_ref: Optional[Any] = None
        self._last_bar_ts_ns: int = 0
        self._in_warmup: bool = False
        self._warmup_bar_count: int = 0
        self._warmup_loader: Optional[Callable[[], Sequence[Bar]]] = None
        self._exit_only_mode: bool = False
        self._thesis_invalidated: bool = False
        self._pending_thesis_exit: bool = False
        self._pending_thesis_exit_reason: str = "THESIS_INVALIDATED_EXIT"
        self._tracked_signed_qty_by_inst: Dict[str, Decimal] = {}
        self._order_side_by_cid: Dict[str, Tuple[str, OrderSide]] = {}
        self._submitted_orders_by_cid: Dict[str, Any] = {}
        self._submitted_orders_audit: List[Any] = []
        self._canceled_order_ids_audit: List[str] = []
        self._pending_cross_thread_submits: List[Any] = []
        self._pending_cross_thread_cancels: List[Any] = []
        self._in_cancel_order_call: bool = False
        self._in_submit_order_call: bool = False
        self._pending_reentrant_orders: List[Any] = []
        self._pending_reentrant_cancels: List[Any] = []
        self._last_stop_exit_ts_ns: int = 0
        self._post_warmup_bar_seq: int = 0
        self._delayed_order_queue: List[Dict[str, Any]] = []
        self._flushing_delayed_orders: bool = False
        self._standalone_clock: Optional[Any] = None
        self._cached_order_factory: Optional[OrderFactory] = None
        self._cached_clock: Optional[Any] = None
        self._cached_trader_registered: bool = False
        self._standalone_order_seq: int = 0
        self._denied_order_ids: set[str] = set()
        self._rejected_order_ids: set[str] = set()
        self._processed_terminal_events: set[Tuple[str, str]] = set()

    @property
    def _canceled_order_ids(self) -> List[str]:
        return self._canceled_order_ids_audit

    @staticmethod
    def _event_get(event: Any, key: str, default: Any = None) -> Any:
        if isinstance(event, dict):
            return event.get(key, default)
        return getattr(event, key, default)

    def _is_owner_thread(self) -> bool:
        return threading.get_ident() == getattr(
            self, "_owner_thread_id", threading.get_ident()
        )

    @property
    def archetype_config(self) -> BaseArchetypeStrategyConfig:
        return self._archetype_config

    @property
    def risk_envelope(self) -> Optional[PlanRiskEnvelope]:
        return self._risk_envelope

    @property
    def is_warmed_up(self) -> bool:
        return len(self._closes) >= self._archetype_config.lookback_window

    @property
    def warmup_bar_count(self) -> int:
        return self._warmup_bar_count

    @property
    def exit_only_mode(self) -> bool:
        return self._exit_only_mode

    @property
    def thesis_invalidated(self) -> bool:
        return self._thesis_invalidated

    def bind_warmup_loader(self, loader: Callable[[], Sequence[Bar]]) -> None:
        """Binds a zero-argument historical bar loader invoked automatically on `on_start()`."""
        self._warmup_loader = loader

    def warmup_historical_bars(self, bars: Sequence[Bar]) -> int:
        """Feeds historical bars chronologically into rolling indicator windows under `_in_warmup=True`
        without emitting any entry or exit orders. When multiple instruments share identical bar
        timestamps (e.g. `StatArbPairsStrategy`), secondary-leg bars are ordered before primary-leg bars.
        """
        if not bars:
            return 0
        sorted_bars = sorted(
            bars,
            key=lambda b: (
                int(getattr(b, "ts_init", 0) or getattr(b, "ts_event", 0) or 0),
                1 if b.bar_type == self._archetype_config.bar_type else 0,
            ),
        )
        fed_primary = 0
        prev_warmup = self._in_warmup
        self._in_warmup = True
        try:
            for b in sorted_bars:
                prev_last_ts = self._last_bar_ts_ns
                prev_len = len(self._closes)
                self.on_bar(b)
                if b.bar_type == self._archetype_config.bar_type and (
                    self._last_bar_ts_ns > prev_last_ts or len(self._closes) > prev_len
                ):
                    fed_primary += 1
                    self._warmup_bar_count += 1
        finally:
            self._in_warmup = prev_warmup
        return fed_primary

    def on_historical_data(self, data: Any) -> None:
        """Handles historical `Bar` callbacks from `request_bars` by routing through `warmup_historical_bars`."""
        if isinstance(data, Bar) or (
            hasattr(data, "bar_type") and hasattr(data, "close")
        ):
            self.warmup_historical_bars([data])

    def reset_rolling_windows(self) -> None:
        """Clears rolling bar and indicator windows when a flat strategy is re-warmed up."""
        self._highs.clear()
        self._lows.clear()
        self._closes.clear()
        self._volumes.clear()
        self._last_bar_ts_ns = 0
        self._warmup_bar_count = 0

    def set_tracked_position_signed_qty(
        self,
        signed_qty: Decimal | float | int | str,
        instrument_id: Optional[InstrumentId] = None,
    ) -> None:
        """Sets the fallback tracked signed position quantity for `instrument_id`."""
        target_inst = instrument_id or self._archetype_config.instrument_id
        inst_key = str(target_inst)
        qty_dec = _to_decimal(signed_qty, "0")
        if qty_dec == Decimal("0"):
            self._tracked_signed_qty_by_inst.pop(inst_key, None)
        else:
            self._tracked_signed_qty_by_inst[inst_key] = qty_dec

    @property
    def trailing_stop_price(self) -> Optional[float]:
        return self._trailing_stop_price

    @property
    def submitted_order_ids(self) -> List[str]:
        return list(self._submitted_order_ids)

    @property
    def submitted_orders(self) -> List[Any]:
        return [
            self._submitted_orders_by_cid[cid]
            for cid in self._submitted_order_ids
            if cid in self._submitted_orders_by_cid
        ]

    def _resolve_strategy_id_str(self) -> str:
        if self._is_owner_thread():
            try:
                sid = getattr(self, "id", None)
                if sid is not None:
                    return str(sid)
            except Exception:
                pass
        resolved_sid = getattr(self._archetype_config, "_resolved_strategy_id", None)
        if resolved_sid is not None:
            return str(resolved_sid)
        return f"{self._archetype_config.ARCHETYPE_ID.value}-001"

    def _sync_engine_handles(self) -> None:
        if not self._is_owner_thread():
            return
        try:
            if getattr(self, "trader_id", None) is not None:
                self._cached_trader_registered = True
        except Exception:
            pass
        try:
            of = getattr(self, "order_factory", None)
            if of is not None and self._cached_order_factory is None:
                self._cached_order_factory = of
        except Exception:
            pass
        try:
            clk = getattr(self, "clock", None)
            if clk is not None:
                self._cached_clock = clk
        except Exception:
            pass

    def _get_order_factory(self) -> OrderFactory:
        if self._cached_order_factory is not None:
            return self._cached_order_factory
        if self._is_owner_thread():
            try:
                of = self.order_factory
                if of is not None:
                    self._cached_order_factory = of
                    try:
                        clk = getattr(self, "clock", None)
                        if clk is not None:
                            self._cached_clock = clk
                    except Exception:
                        pass
                    return of
            except Exception:
                pass
        return self.order_factory

    _resolve_order_factory = _get_order_factory

    def _submit_order_safely(self, order: Any) -> None:
        cid = str(order.client_order_id)
        self._submitted_orders_by_cid[cid] = order
        if order not in self._submitted_orders_audit:
            self._submitted_orders_audit.append(order)
        if not self._is_owner_thread():
            self._pending_cross_thread_submits.append(order)
            return
        if self._in_cancel_order_call or self._in_submit_order_call:
            self._pending_reentrant_orders.append(order)
            return
        self._sync_engine_handles()
        prev_in_submit = self._in_submit_order_call
        self._in_submit_order_call = True
        try:
            self.submit_order(order)
        except RuntimeError as exc:
            msg = str(exc).lower()
            if "already borrowed" in msg:
                self._pending_reentrant_orders.append(order)
                return
            if "not registered" not in msg:
                raise
        finally:
            self._in_submit_order_call = prev_in_submit
            if not self._in_submit_order_call:
                if self._pending_reentrant_cancels:
                    pending_cancels = list(self._pending_reentrant_cancels)
                    self._pending_reentrant_cancels.clear()
                    for queued_cancel in pending_cancels:
                        self._cancel_single_order_safely(queued_cancel)
                if not self._in_cancel_order_call and self._pending_reentrant_orders:
                    pending_orders = list(self._pending_reentrant_orders)
                    self._pending_reentrant_orders.clear()
                    for queued_order in pending_orders:
                        self._submit_order_safely(queued_order)

    _dispatch_submit_order = _submit_order_safely

    def _flush_cross_thread_orders(self) -> None:
        if not self._is_owner_thread():
            return
        if (
            self._pending_cross_thread_cancels
            or self._pending_cross_thread_submits
        ) and (
            self._thesis_invalidated
            or self._active_exit_orders_by_inst
            or self._pending_safe_exits
        ):
            sym = self._symbol_from_instrument_id(
                self._archetype_config.instrument_id
            )
            self._notify_safe_exit_begin(sym)
        while self._pending_cross_thread_cancels:
            order_or_cid = self._pending_cross_thread_cancels.pop(0)
            self._cancel_single_order_safely(order_or_cid)
        while self._pending_cross_thread_submits:
            order = self._pending_cross_thread_submits.pop(0)
            self._submit_order_safely(order)

    @property
    def protective_stop_order_ids(self) -> List[str]:
        return list(self._protective_stop_order_ids)

    def bind_telemetry_actor(self, telemetry_actor: Any) -> None:
        """Optionally binds a `PostgresTelemetryActor` reference for direct safe-exit / resize coordination."""
        self._telemetry_actor_ref = telemetry_actor

    def bind_plan_gate(self, plan_gate: Any) -> None:
        """Optionally binds a `PlanGateExecAlgorithm` reference for closed-cycle capacity release."""
        self._plan_gate_ref = plan_gate
        if plan_gate is not None and hasattr(plan_gate, "register_strategy"):
            try:
                plan_gate.register_strategy(self)
            except Exception:
                pass

    def _resolve_telemetry_actor(self) -> Optional[Any]:
        return self._telemetry_actor_ref

    def _symbol_from_instrument_id(
        self, instrument_id: Optional[InstrumentId] = None
    ) -> str:
        target_inst = instrument_id or self._archetype_config.instrument_id
        return str(target_inst).strip().upper().split(".", 1)[0]

    def _resolve_tick_size(
        self, instrument_id: Optional[InstrumentId] = None
    ) -> Decimal:
        target_inst = instrument_id or self._archetype_config.instrument_id
        if self._is_owner_thread():
            try:
                inst = self.cache.instrument(target_inst)
                if inst is not None:
                    px_inc = getattr(inst, "price_increment", None)
                    if px_inc is not None:
                        inc_dec = (
                            Decimal(str(px_inc.as_decimal()))
                            if hasattr(px_inc, "as_decimal")
                            else _to_decimal(px_inc, "0.01")
                        )
                        if inc_dec > Decimal("0"):
                            return inc_dec
            except Exception:
                pass
        return Decimal("0.01")

    def _resolve_entry_stop_price(
        self,
        reference_price: float,
        raw_stop_price: float,
        quantity: Optional[Decimal] = None,
        instrument_id: Optional[InstrumentId] = None,
    ) -> float:
        """Formats the strategy's ATR-derived stop-loss price without clamping, delegating floor/budget enforcement to `PlanGate`."""
        _ = (reference_price, quantity, instrument_id)
        return float(Decimal(f"{float(raw_stop_price):.4f}"))

    def _resolve_protective_stop_price(
        self,
        reference_price: Optional[float] = None,
        raw_stop_price: Optional[float] = None,
        *,
        side: Optional[OrderSide] = None,
        entry_price: Optional[float] = None,
        raw_stop_distance: Optional[float] = None,
        quantity: Optional[Decimal] = None,
        instrument_id: Optional[InstrumentId] = None,
    ) -> float:
        """Resolves protective stop-loss price bounded by the locked `PlanRiskEnvelope`."""
        eff_ref = float(
            reference_price
            if reference_price is not None
            else (entry_price if entry_price is not None else 0.0)
        )
        if raw_stop_price is not None:
            eff_raw_stop = float(raw_stop_price)
        else:
            dist = max(0.0, float(raw_stop_distance or 0.0))
            is_buy = (
                side == OrderSide.BUY
                if side is not None
                else self._archetype_config.plan_side == PlanSide.LONG
            )
            eff_raw_stop = eff_ref - dist if is_buy else eff_ref + dist
        return self._resolve_entry_stop_price(
            eff_ref,
            eff_raw_stop,
            quantity=quantity,
            instrument_id=instrument_id,
        )

    def _notify_safe_exit_begin(self, symbol: str) -> None:
        norm_sym = str(symbol or "").strip().upper().split(".", 1)[0]
        if not norm_sym:
            return
        actor = self._resolve_telemetry_actor()
        if actor is not None and hasattr(actor, "begin_safe_exit"):
            try:
                actor.begin_safe_exit(norm_sym)
            except Exception:
                pass
        if self._is_owner_thread():
            try:
                if hasattr(self, "cache") and self.cache is not None and hasattr(self.cache, "add"):
                    self.cache.add(f"__nautilus_safe_exit__{norm_sym}", b"1")
            except Exception:
                pass

    def _notify_safe_exit_end(self, symbol: str) -> None:
        norm_sym = str(symbol or "").strip().upper().split(".", 1)[0]
        if not norm_sym:
            return
        actor = self._resolve_telemetry_actor()
        if actor is not None and hasattr(actor, "end_safe_exit"):
            try:
                actor.end_safe_exit(norm_sym)
            except Exception:
                pass
        if self._is_owner_thread():
            try:
                if hasattr(self, "cache") and self.cache is not None and hasattr(self.cache, "add"):
                    self.cache.add(f"__nautilus_safe_exit__{norm_sym}", b"0")
            except Exception:
                pass

    def _notify_stop_leg_replaced(
        self,
        symbol: str,
        old_cid: str,
        new_cid: str,
        new_covered_qty: Any = None,
        stop_price: Any = None,
    ) -> None:
        actor = self._resolve_telemetry_actor()
        if actor is not None and hasattr(actor, "mark_stop_leg_replaced"):
            try:
                actor.mark_stop_leg_replaced(
                    symbol,
                    old_cid,
                    new_cid,
                    new_covered_qty=new_covered_qty,
                    stop_price=stop_price,
                )
            except TypeError:
                try:
                    actor.mark_stop_leg_replaced(symbol, old_cid, new_cid)
                except Exception:
                    pass
            except Exception:
                pass

    def _should_submit_protective_stop(self) -> bool:
        if bool(getattr(self._archetype_config, "submit_protective_stop", False)):
            return True
        if self._is_owner_thread():
            try:
                cfg_obj = getattr(self, "config", None)
                if cfg_obj is not None and bool(getattr(cfg_obj, "submit_protective_stop", False)):
                    return True
            except Exception:
                pass
        return False

    def _is_trader_registered(self) -> bool:
        if not self._is_owner_thread():
            return False
        try:
            if getattr(self, "trader_id", None) is not None:
                self._cached_trader_registered = True
                return True
        except RuntimeError as exc:
            if "already borrowed" in str(exc).lower():
                return self._cached_trader_registered
        except Exception:
            pass
        return self._cached_trader_registered

    def on_start(self) -> None:
        self._sync_engine_handles()
        if (
            self._warmup_loader is not None
            and self._warmup_bar_count == 0
            and len(self._closes) < self._archetype_config.lookback_window
        ):
            loaded_bars = self._warmup_loader()
            if loaded_bars:
                self.warmup_historical_bars(loaded_bars)
        if self._is_owner_thread():
            self.subscribe_bars(self._archetype_config.bar_type)
            if self._archetype_config.secondary_bar_type is not None:
                self.subscribe_bars(self._archetype_config.secondary_bar_type)

    def _resolve_entry_limit_price(
        self,
        order_side: OrderSide,
        reference_price: float,
        atr: float,
        limit_price: Optional[float] = None,
    ) -> float:
        """Resolves the marketable limit entry price bounded by `MarketableLimitCollar`."""
        if (
            limit_price is not None
            and float(limit_price) > 0.0
            and abs(float(limit_price) - float(reference_price)) > 1e-9
        ):
            return float(limit_price)
        if reference_price <= 0.0:
            return float(limit_price or 0.0)
        cfg = self._archetype_config
        env = self._risk_envelope or getattr(cfg, "_plan_risk_envelope", None)
        plan_side = (
            PlanSide.LONG
            if "BUY" in str(order_side).upper()
            else PlanSide.SHORT
        )
        ref_dec = Decimal(f"{float(reference_price):.4f}")
        atr_dec = Decimal(f"{max(0.0, float(atr)):.4f}")
        collar = (
            env.collar
            if env is not None and getattr(env, "collar", None) is not None
            else MarketableLimitCollar(
                limit_collar_pct=Decimal("0.35"),
                limit_collar_atr=Decimal("0.25"),
            )
        )
        worst_limit_dec = collar.compute_worst_limit_price(
            plan_side,
            ref_dec,
            atr_dec,
        )
        return float(worst_limit_dec)

    def _flush_delayed_orders(self, current_bar: Bar) -> None:
        if not self._delayed_order_queue or self._in_warmup:
            return
        due = [
            item
            for item in self._delayed_order_queue
            if int(item.get("due_bar_seq", 0)) <= self._post_warmup_bar_seq
        ]
        if not due:
            return
        self._delayed_order_queue = [
            item
            for item in self._delayed_order_queue
            if int(item.get("due_bar_seq", 0)) > self._post_warmup_bar_seq
        ]
        prev_flushing = self._flushing_delayed_orders
        self._flushing_delayed_orders = True
        try:
            for item in due:
                action = item.get("action")
                kwargs = dict(item.get("kwargs", {}))
                target_inst = kwargs.get("instrument_id") or self._archetype_config.instrument_id
                cur_px = float(current_bar.close.as_double())
                sec_inst = getattr(self._archetype_config, "secondary_instrument_id", None)
                if sec_inst is not None and str(target_inst) == str(sec_inst):
                    sec_closes = getattr(self, "_secondary_closes", None)
                    if sec_closes:
                        cur_px = float(sec_closes[-1])
                if action == "entry":
                    orig_ref = float(kwargs.get("reference_price") or cur_px)
                    orig_stop = kwargs.get("stop_loss_price")
                    if cur_px > 0.0:
                        kwargs["reference_price"] = cur_px
                        kwargs["limit_price"] = cur_px
                        if orig_stop is not None and float(orig_stop) > 0.0:
                            stop_dist = abs(orig_ref - float(orig_stop))
                            order_side = kwargs.get("order_side", OrderSide.BUY)
                            is_buy = "BUY" in str(order_side).upper()
                            raw_stop = (
                                max(0.01, cur_px - stop_dist)
                                if is_buy
                                else cur_px + stop_dist
                            )
                            kwargs["stop_loss_price"] = self._resolve_entry_stop_price(
                                cur_px,
                                raw_stop,
                                quantity=kwargs.get("quantity"),
                                instrument_id=target_inst,
                            )
                    self._submit_plan_gated_entry(**kwargs)
                elif action == "exit":
                    if cur_px > 0.0:
                        kwargs["reference_price"] = cur_px
                    self._submit_plan_gated_exit(**kwargs)
        finally:
            self._flushing_delayed_orders = prev_flushing

    def _record_primary_bar(self, bar: Bar) -> bool:
        self._sync_engine_handles()
        if not self._in_warmup and self._is_owner_thread():
            self._flush_cross_thread_orders()
        bar_ts = int(getattr(bar, "ts_init", 0) or getattr(bar, "ts_event", 0) or 0)
        if bar_ts > 0 and self._last_bar_ts_ns > 0 and bar_ts <= self._last_bar_ts_ns:
            return False
        if bar_ts > 0:
            self._last_bar_ts_ns = bar_ts
            if self._standalone_clock is not None:
                try:
                    self._standalone_clock.set_time(bar_ts)
                except Exception:
                    pass
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
        if not self._in_warmup:
            self._post_warmup_bar_seq += 1
            self._expire_stale_unfilled_entry_orders()
            self._flush_delayed_orders(bar)
        return True

    def _expire_stale_unfilled_entry_orders(self) -> None:
        if not self._active_entry_orders_by_inst:
            return
        for inst_key, entry_spec in list(self._active_entry_orders_by_inst.items()):
            if entry_spec.get("canceling"):
                continue
            submitted_seq = int(entry_spec.get("submitted_bar_seq", 0))
            if submitted_seq >= self._post_warmup_bar_seq:
                continue
            target_inst = entry_spec.get("instrument_id") or InstrumentId.from_str(inst_key)
            if self._open_position_signed_qty(target_inst) != Decimal("0"):
                self._active_entry_orders_by_inst.pop(inst_key, None)
                continue
            order_obj = entry_spec.get("order")
            cid = str(entry_spec.get("cid", "") or "")
            if self._is_trader_registered() and cid:
                try:
                    cached = self.cache.order(
                        getattr(order_obj, "client_order_id", cid)
                    )
                    if cached is not None:
                        order_obj = cached
                except Exception:
                    pass
            if order_obj is None:
                self._active_entry_orders_by_inst.pop(inst_key, None)
                continue
            status_str = self._normalized_order_status(order_obj)
            if status_str in ("DENIED", "REJECTED", "CANCELED", "EXPIRED", "FILLED"):
                self._active_entry_orders_by_inst.pop(inst_key, None)
                continue
            filled_qty_dec = _to_decimal(getattr(order_obj, "filled_qty", 0), "0")
            if filled_qty_dec > Decimal("0"):
                self._active_entry_orders_by_inst.pop(inst_key, None)
                continue
            self._active_entry_orders_by_inst.pop(inst_key, None)
            self._cancel_single_order_safely(order_obj)

    def _open_position_signed_qty(
        self, instrument_id: Optional[InstrumentId] = None
    ) -> Decimal:
        target_inst = instrument_id or self._archetype_config.instrument_id
        inst_key = str(target_inst)
        if not self._is_owner_thread():
            return self._tracked_signed_qty_by_inst.get(inst_key, Decimal("0"))
        total = Decimal("0")
        try:
            positions = list(self.cache.positions_open(instrument_id=target_inst) or [])
        except Exception:
            positions = []
        if positions:
            for pos in positions:
                qty = _to_decimal(getattr(pos, "quantity", 0))
                side_str = str(getattr(pos, "side", "LONG")).upper()
                if "SHORT" in side_str:
                    total -= qty
                elif "LONG" in side_str:
                    total += qty
            if total == Decimal("0"):
                self._tracked_signed_qty_by_inst.pop(inst_key, None)
            else:
                self._tracked_signed_qty_by_inst[inst_key] = total
            return total
        try:
            all_positions = list(self.cache.positions(instrument_id=target_inst) or [])
        except Exception:
            all_positions = []
        if all_positions:
            self._tracked_signed_qty_by_inst.pop(inst_key, None)
            return Decimal("0")
        return self._tracked_signed_qty_by_inst.get(inst_key, Decimal("0"))

    def _has_inflight_or_open_entry_order(
        self, instrument_id: Optional[InstrumentId] = None
    ) -> bool:
        target_inst = instrument_id or self._archetype_config.instrument_id
        inst_key = str(target_inst)
        for item in self._delayed_order_queue:
            if item.get("action") == "entry":
                queued_inst = (
                    item.get("kwargs", {}).get("instrument_id")
                    or self._archetype_config.instrument_id
                )
                if str(queued_inst) == inst_key:
                    return True
        tracked_entry = self._active_entry_orders_by_inst.get(inst_key)
        if tracked_entry is not None:
            order_obj = tracked_entry.get("order")
            status_str = (
                self._normalized_order_status(order_obj)
                if order_obj is not None
                else ""
            )
            if status_str not in ("DENIED", "REJECTED", "CANCELED", "EXPIRED", "FILLED"):
                return True
            self._active_entry_orders_by_inst.pop(inst_key, None)
        if not self._is_owner_thread():
            return False
        try:
            open_orders = self.cache.orders_open(instrument_id=target_inst)
            inflight_orders = self.cache.orders_inflight(instrument_id=target_inst)
        except Exception:
            open_orders = []
            inflight_orders = []
        for order in list(open_orders or []) + list(inflight_orders or []):
            cid = str(getattr(order, "client_order_id", "") or "")
            if cid in self._canceled_order_ids_audit or self._is_order_denied_or_rejected(cid):
                continue
            if not bool(getattr(order, "is_reduce_only", False)):
                return True
        return False

    def _format_quantity(
        self, raw_qty: Decimal | float, instrument_id: Optional[InstrumentId] = None
    ) -> Quantity:
        target_inst = instrument_id or self._archetype_config.instrument_id
        dec_qty = _to_decimal(raw_qty, "1")
        if self._is_owner_thread():
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
        if self._is_owner_thread():
            try:
                inst = self.cache.instrument(target_inst)
                if inst is not None:
                    prec = int(getattr(inst, "price_precision", 2) or 2)
                    return Price(round(val, prec), prec)
            except Exception:
                pass
        return Price(round(val, 2), 2)

    @staticmethod
    def _normalized_order_status(order: Any) -> str:
        status_str = str(getattr(order, "status", "") or "").upper()
        if "." in status_str:
            status_str = status_str.rsplit(".", 1)[-1]
        return status_str

    def _is_protective_stop_order(
        self, order: Any, active_set: Optional[set[str]] = None
    ) -> bool:
        cid = str(getattr(order, "client_order_id", "") or "")
        if active_set and cid in active_set:
            return True
        tags_raw = getattr(order, "tags", None) or []
        tags_upper = {str(t).strip().upper() for t in tags_raw}
        if "PROTECTIVE_STOP" in tags_upper or "STOP_LOSS" in tags_upper:
            return True
        exec_params = getattr(order, "exec_algorithm_params", None) or {}
        intent = str(exec_params.get("intent", "") or "").strip().upper()
        if intent in ("STOP", "STOP_LOSS", "PROTECTIVE_STOP", "OCO_STOP"):
            return True
        order_type_str = str(getattr(order, "order_type", "") or "").upper()
        if "STOP" in order_type_str and bool(getattr(order, "is_reduce_only", False)):
            return True
        return False

    def _get_open_protective_stop_orders(
        self, instrument_id: Optional[InstrumentId] = None
    ) -> List[Any]:
        target_inst = instrument_id or self._archetype_config.instrument_id
        inst_key = str(target_inst)
        active_cids = list(self._active_protective_stops_by_inst.get(inst_key, []))
        active_set = set(active_cids)
        open_orders: List[Any] = []
        inflight_orders: List[Any] = []
        if self._is_owner_thread():
            try:
                open_orders = list(self.cache.orders_open(instrument_id=target_inst) or [])
            except Exception:
                open_orders = []
            try:
                inflight_orders = list(
                    self.cache.orders_inflight(instrument_id=target_inst) or []
                )
            except Exception:
                inflight_orders = []

        seen_cids: set[str] = set()
        result: List[Any] = []
        for order in open_orders + inflight_orders:
            cid = str(getattr(order, "client_order_id", "") or "")
            if not cid or cid in seen_cids or cid in self._canceled_order_ids_audit:
                continue
            status_str = self._normalized_order_status(order)
            if status_str in ("DENIED", "REJECTED", "CANCELED", "EXPIRED", "FILLED"):
                continue
            if self._is_protective_stop_order(order, active_set):
                seen_cids.add(cid)
                result.append(order)
                self._stop_orders_by_cid[cid] = order
                tracked_list = self._active_protective_stops_by_inst.setdefault(
                    inst_key, []
                )
                if cid not in tracked_list:
                    tracked_list.append(cid)

        for cid in active_cids:
            if cid in seen_cids:
                continue
            cached_order = None
            if self._is_trader_registered():
                try:
                    cached_order = self.cache.order(
                        getattr(
                            self._stop_orders_by_cid.get(cid),
                            "client_order_id",
                            cid,
                        )
                    )
                except Exception:
                    cached_order = None
            order_obj = cached_order if cached_order is not None else self._stop_orders_by_cid.get(cid)
            if order_obj is None:
                continue
            status_str = self._normalized_order_status(order_obj)
            if status_str in ("DENIED", "REJECTED", "CANCELED", "EXPIRED", "FILLED"):
                continue
            if cached_order is not None and (open_orders or inflight_orders):
                # Already checked live open/inflight sets above
                continue
            seen_cids.add(cid)
            result.append(order_obj)

        return result

    def _cancel_single_order_safely(self, order_or_cid: Any) -> None:
        cid_str = str(getattr(order_or_cid, "client_order_id", order_or_cid) or "")
        if cid_str and cid_str not in self._canceled_order_ids_audit:
            self._canceled_order_ids_audit.append(cid_str)
        if not self._is_owner_thread():
            self._pending_cross_thread_cancels.append(order_or_cid)
            return
        if self._in_submit_order_call and self._is_trader_registered():
            self._pending_reentrant_cancels.append(order_or_cid)
            return
        tracked_ord = (
            self._submitted_orders_by_cid.get(cid_str)
            or self._stop_orders_by_cid.get(cid_str)
        )
        if self._is_trader_registered() and cid_str:
            try:
                cached_ord = self.cache.order(ClientOrderId(cid_str))
                if (
                    cached_ord is not None
                    and self._normalized_order_status(cached_ord) == "INITIALIZED"
                    and (
                        cid_str in self._stop_orders_by_cid
                        or (
                            tracked_ord is not None
                            and self._normalized_order_status(tracked_ord)
                            != "INITIALIZED"
                        )
                    )
                ):
                    return
            except Exception:
                pass
        prev_in_cancel = self._in_cancel_order_call
        self._in_cancel_order_call = True
        order_arg = order_or_cid
        if not hasattr(order_arg, "client_order_id"):
            order_arg = (
                self._stop_orders_by_cid.get(cid_str)
                or self._submitted_orders_by_cid.get(cid_str)
                or order_or_cid
            )
        reentrant_borrowed = False
        try:
            self.cancel_order(order_arg)
        except TypeError:
            try:
                cid_arg = getattr(order_or_cid, "client_order_id", order_or_cid)
                self.cancel_order(cid_arg)
            except RuntimeError as exc:
                if "already borrowed" in str(exc).lower():
                    reentrant_borrowed = True
            except Exception:
                pass
        except RuntimeError as exc:
            if "already borrowed" in str(exc).lower():
                reentrant_borrowed = True
            else:
                try:
                    cid_arg = getattr(order_or_cid, "client_order_id", order_or_cid)
                    if cid_arg is not order_or_cid:
                        self.cancel_order(cid_arg)
                except Exception:
                    pass
        except Exception:
            try:
                cid_arg = getattr(order_or_cid, "client_order_id", order_or_cid)
                if cid_arg is not order_or_cid:
                    self.cancel_order(cid_arg)
            except Exception:
                pass
        finally:
            self._in_cancel_order_call = prev_in_cancel
            if reentrant_borrowed:
                self._pending_reentrant_cancels.append(order_or_cid)
            elif not self._in_cancel_order_call:
                try:
                    cid_str = str(getattr(order_or_cid, "client_order_id", order_or_cid))
                    cid_obj = ClientOrderId(cid_str)
                    if self._is_trader_registered() and self.cache is not None:
                        cached = self.cache.order(cid_obj)
                        if (
                            cached is not None
                            and self._normalized_order_status(cached) == "CANCELED"
                        ):
                            self.on_order_canceled(cached)
                except Exception:
                    pass
                if self._pending_reentrant_orders:
                    pending = list(self._pending_reentrant_orders)
                    self._pending_reentrant_orders.clear()
                    for queued_order in pending:
                        self._submit_order_safely(queued_order)

    _dispatch_cancel_order = _cancel_single_order_safely

    def _remove_tracked_stop_cid(
        self, cid: str, instrument_id: Optional[InstrumentId] = None
    ) -> None:
        self._stop_orders_by_cid.pop(cid, None)
        self._stop_qty_by_cid.pop(cid, None)
        if instrument_id is not None:
            inst_key = str(instrument_id)
            cids = self._active_protective_stops_by_inst.get(inst_key)
            if cids and cid in cids:
                cids.remove(cid)
            return
        for inst_key, cids in list(self._active_protective_stops_by_inst.items()):
            if cid in cids:
                cids.remove(cid)

    def _build_protective_stop_order(
        self,
        *,
        instrument_id: InstrumentId,
        entry_side: OrderSide,
        quantity: Quantity,
        stop_loss_price: float,
        parent_entry_cid: str,
    ) -> Optional[Any]:
        if stop_loss_price <= 0.0:
            return None
        stop_side = OrderSide.SELL if entry_side == OrderSide.BUY else OrderSide.BUY
        trigger_px = self._format_price(stop_loss_price, instrument_id)
        stop_params = {
            "intent": "STOP_LOSS",
            "reduce_only": "true",
            "reference_price": f"{stop_loss_price:.6f}",
            "stop_loss_price": f"{stop_loss_price:.6f}",
            "parent_entry_order_id": parent_entry_cid,
            "archetype_id": self._archetype_config.ARCHETYPE_ID.value,
        }
        return self._get_order_factory().stop_market(
            instrument_id=instrument_id,
            order_side=stop_side,
            quantity=quantity,
            trigger_price=trigger_px,
            time_in_force=TimeInForce.GTC,
            reduce_only=True,
            exec_algorithm_id=self._archetype_config.exec_algorithm_id,
            exec_algorithm_params=stop_params,
            tags=["PROTECTIVE_STOP", "STOP_LOSS"],
        )

    def _register_and_submit_protective_stop_order(
        self,
        stop_order: Any,
        instrument_id: InstrumentId,
        quantity: Quantity,
    ) -> str:
        stop_cid = str(stop_order.client_order_id)
        stop_side = stop_order.side
        qty_dec = (
            Decimal(str(quantity.as_decimal()))
            if hasattr(quantity, "as_decimal")
            else _to_decimal(quantity, "0")
        )
        self._submitted_order_ids.append(stop_cid)
        self._protective_stop_order_ids.append(stop_cid)
        self._stop_orders_by_cid[stop_cid] = stop_order
        self._stop_qty_by_cid[stop_cid] = qty_dec
        inst_key = str(instrument_id)
        self._order_side_by_cid[stop_cid] = (inst_key, stop_side)
        active_list = self._active_protective_stops_by_inst.setdefault(inst_key, [])
        if stop_cid not in active_list:
            active_list.append(stop_cid)
        self._submit_order_safely(stop_order)
        return stop_cid

    def _create_and_submit_protective_stop(
        self,
        *,
        instrument_id: InstrumentId,
        entry_side: OrderSide,
        quantity: Quantity,
        stop_loss_price: float,
        parent_entry_cid: str,
    ) -> Optional[str]:
        stop_order = self._build_protective_stop_order(
            instrument_id=instrument_id,
            entry_side=entry_side,
            quantity=quantity,
            stop_loss_price=stop_loss_price,
            parent_entry_cid=parent_entry_cid,
        )
        if stop_order is None:
            return None
        return self._register_and_submit_protective_stop_order(
            stop_order,
            instrument_id,
            quantity,
        )

    def _sync_protective_stop_for_entry_fill(
        self,
        *,
        spec: Dict[str, Any],
        new_covered_dec: Decimal,
    ) -> Optional[str]:
        """Synchronizes protective `STOP_MARKET` (`GTC`, `reduce_only=True`) to full open position."""
        inst_id: InstrumentId = spec["instrument_id"]
        inst_key = str(inst_id)
        sym = self._symbol_from_instrument_id(inst_id)
        initial_pos_qty_dec = _to_decimal(spec.get("initial_pos_qty_dec"), "0")
        abs_net_pos_qty = abs(self._open_position_signed_qty(inst_id))
        expected_from_order = initial_pos_qty_dec + max(Decimal("0"), new_covered_dec)
        if abs_net_pos_qty <= initial_pos_qty_dec:
            desired_stop_qty = max(abs_net_pos_qty, expected_from_order)
        else:
            desired_stop_qty = max(abs_net_pos_qty, new_covered_dec)
        if desired_stop_qty <= Decimal("0"):
            return None

        open_stops = self._get_open_protective_stop_orders(inst_id)
        if len(open_stops) == 1:
            existing_stop = open_stops[0]
            existing_cid = str(getattr(existing_stop, "client_order_id", "") or "")
            existing_qty_dec = self._stop_qty_by_cid.get(
                existing_cid,
                _to_decimal(getattr(existing_stop, "quantity", 0), "0"),
            )
            if existing_qty_dec >= desired_stop_qty:
                return existing_cid

        stop_qty_obj = self._format_quantity(desired_stop_qty, inst_id)
        try:
            replacement_stop = self._build_protective_stop_order(
                instrument_id=inst_id,
                entry_side=spec["entry_side"],
                quantity=stop_qty_obj,
                stop_loss_price=spec["stop_loss_price"],
                parent_entry_cid=spec["parent_entry_cid"],
            )
            planned_new_cid = (
                str(replacement_stop.client_order_id)
                if replacement_stop is not None
                else ""
            )
            for old_stop in open_stops:
                old_cid = str(getattr(old_stop, "client_order_id", "") or "")
                if not old_cid or old_cid == planned_new_cid:
                    continue
                self._notify_stop_leg_replaced(
                    sym,
                    old_cid,
                    planned_new_cid,
                    new_covered_qty=desired_stop_qty,
                    stop_price=spec["stop_loss_price"],
                )
                self._remove_tracked_stop_cid(old_cid, inst_id)
                self._cancel_single_order_safely(old_stop)

            new_stop_cid = (
                self._register_and_submit_protective_stop_order(
                    replacement_stop,
                    inst_id,
                    stop_qty_obj,
                )
                if replacement_stop is not None
                else None
            )
        except Exception as exc:
            logger.error(
                "Protective stop submission failed for %s (parent=%s): %s",
                inst_id,
                spec.get("parent_entry_cid"),
                exc,
            )
            new_stop_cid = None

        if new_stop_cid is None:
            actor = self._resolve_telemetry_actor()
            if actor is not None and hasattr(actor, "oco_monitor") and desired_stop_qty > Decimal("0"):
                try:
                    actor.oco_monitor.invalidate_protective_order(
                        symbol=sym,
                        reason=f"PROTECTIVE_STOP_SUBMISSION_FAILED:{spec.get('parent_entry_cid')}",
                    )
                    actor.oco_monitor.verify_position_coverage(
                        symbol=sym,
                        pos_qty=desired_stop_qty,
                        override_reason=f"PROTECTIVE_STOP_SUBMISSION_FAILED:{spec.get('parent_entry_cid')}",
                    )
                except Exception as oco_exc:
                    logger.error(
                        "Failed to trigger protective OCO verification on stop submission failure for %s: %s",
                        sym,
                        oco_exc,
                    )
            return None

        self._active_protective_stops_by_inst[inst_key] = [new_stop_cid]
        return new_stop_cid

    def _cancel_protective_stops_for(
        self, instrument_id: Optional[InstrumentId] = None
    ) -> None:
        target_inst = instrument_id or self._archetype_config.instrument_id
        inst_key = str(target_inst)
        for pending_cid, spec in list(self._pending_protective_stops.items()):
            if str(spec.get("instrument_id")) == inst_key:
                self._pending_protective_stops.pop(pending_cid, None)

        open_stops = self._get_open_protective_stop_orders(target_inst)
        tracked_cids = list(self._active_protective_stops_by_inst.pop(inst_key, []))
        for tracked_cid in tracked_cids:
            self._remove_tracked_stop_cid(tracked_cid, target_inst)
        for stop_order in open_stops:
            old_cid = str(getattr(stop_order, "client_order_id", "") or "")
            if old_cid:
                self._remove_tracked_stop_cid(old_cid, target_inst)
            self._cancel_single_order_safely(stop_order)

    def _advance_pending_safe_exit_for_stop_event(
        self,
        cid: str,
        *,
        stop_filled: bool = False,
        stop_fill_qty_dec: Decimal = Decimal("0"),
    ) -> None:
        for inst_key, spec in list(self._pending_safe_exits.items()):
            pending_cids: set[str] = spec.get("pending_stop_cids", set())
            if cid not in pending_cids:
                continue
            target_inst: InstrumentId = spec["instrument_id"]
            sym: str = spec["symbol"]
            if stop_filled:
                effective_fill = (
                    stop_fill_qty_dec
                    if stop_fill_qty_dec > Decimal("0")
                    else self._stop_qty_by_cid.get(
                        cid, _to_decimal(spec.get("quantity"), "0")
                    )
                )
                spec["stop_filled_qty_dec"] = (
                    _to_decimal(spec.get("stop_filled_qty_dec"), "0") + effective_fill
                )
                stop_total_qty = self._stop_qty_by_cid.get(
                    cid, _to_decimal(spec.get("initial_abs_pos_qty"), "0")
                )
                cached_stop = None
                if self._is_trader_registered():
                    try:
                        cached_stop = self.cache.order(
                            getattr(
                                self._stop_orders_by_cid.get(cid),
                                "client_order_id",
                                cid,
                            )
                        )
                    except Exception:
                        cached_stop = None
                status_str = (
                    self._normalized_order_status(cached_stop)
                    if cached_stop is not None
                    else ""
                )
                if (
                    stop_total_qty <= Decimal("0")
                    or spec["stop_filled_qty_dec"] >= stop_total_qty
                    or status_str in ("FILLED", "CANCELED", "EXPIRED", "REJECTED")
                ):
                    pending_cids.discard(cid)
                    self._remove_tracked_stop_cid(cid, target_inst)
            else:
                pending_cids.discard(cid)
                self._remove_tracked_stop_cid(cid, target_inst)

            if pending_cids:
                return

            self._pending_safe_exits.pop(inst_key, None)
            current_abs_pos = abs(self._open_position_signed_qty(target_inst))
            initial_abs_pos = _to_decimal(spec.get("initial_abs_pos_qty"), "0")
            total_stop_filled = _to_decimal(spec.get("stop_filled_qty_dec"), "0")
            had_cache_pos = bool(spec.get("had_open_position_in_cache", False))
            requested_qty = _to_decimal(spec.get("quantity"), "0")

            if total_stop_filled > Decimal("0"):
                if current_abs_pos > Decimal("0") and current_abs_pos < initial_abs_pos:
                    remaining_qty = current_abs_pos
                elif current_abs_pos == initial_abs_pos and initial_abs_pos > Decimal("0"):
                    remaining_qty = max(Decimal("0"), initial_abs_pos - total_stop_filled)
                elif not had_cache_pos:
                    remaining_qty = max(Decimal("0"), requested_qty - total_stop_filled)
                else:
                    remaining_qty = Decimal("0")
            else:
                if current_abs_pos > Decimal("0"):
                    remaining_qty = current_abs_pos
                elif not had_cache_pos:
                    remaining_qty = requested_qty
                else:
                    remaining_qty = Decimal("0")

            if remaining_qty <= Decimal("0"):
                self._trailing_stop_price = None
                self._notify_safe_exit_end(sym)
                return

            exit_qty = (
                min(requested_qty, remaining_qty)
                if requested_qty > Decimal("0")
                else remaining_qty
            )
            exit_cid = self._execute_exit_order_submission(
                instrument_id=target_inst,
                exit_side=spec["exit_side"],
                quantity=exit_qty,
                reference_price=spec["reference_price"],
                reason_tag=spec["reason_tag"],
                signal_tags=spec.get("signal_tags"),
            )
            spec["submitted_exit_cid"] = exit_cid
            return

    def _handle_exit_order_failure(self, cid: str, reason: str) -> None:
        for inst_key, active_exit_cid in list(self._active_exit_orders_by_inst.items()):
            if active_exit_cid != cid:
                continue
            self._active_exit_orders_by_inst.pop(inst_key, None)
            target_inst = InstrumentId.from_str(inst_key)
            sym = self._symbol_from_instrument_id(target_inst)
            signed_qty = self._open_position_signed_qty(target_inst)
            open_stops = self._get_open_protective_stop_orders(target_inst)
            if signed_qty != Decimal("0") and not open_stops:
                actor = self._resolve_telemetry_actor()
                if actor is not None and hasattr(actor, "oco_monitor"):
                    try:
                        actor.oco_monitor.invalidate_protective_order(
                            symbol=sym,
                            broker_order_id=cid,
                            reason=f"SAFE_EXIT_ORDER_FAILED:{reason}:{cid}",
                        )
                        actor.oco_monitor.verify_position_coverage(
                            symbol=sym,
                            pos_qty=abs(signed_qty),
                            override_reason=f"SAFE_EXIT_ORDER_FAILED:{reason}:{cid}",
                        )
                    except Exception as exc:
                        logger.error(
                            "Failed to trigger OCO verification on safe exit failure for %s (%s): %s",
                            sym,
                            cid,
                            exc,
                        )
            self._notify_safe_exit_end(sym)

    def on_order_filled(self, event: Any) -> None:
        cid = str(self._event_get(event, "client_order_id", "") or "")
        if not cid:
            return

        last_qty = self._event_get(event, "last_qty", None)
        if last_qty is None:
            last_qty = self._event_get(event, "fill_qty", None) or self._event_get(
                event, "quantity", None
            )
        fill_qty_dec = Decimal("0")
        if last_qty is not None:
            try:
                fill_qty_dec = Decimal(str(last_qty.as_decimal()))
            except Exception:
                fill_qty_dec = _to_decimal(last_qty, "0")

        if fill_qty_dec > Decimal("0"):
            tracked_meta = self._order_side_by_cid.get(cid)
            if tracked_meta is None and "-E" in cid:
                tracked_meta = self._order_side_by_cid.get(cid.rsplit("-E", 1)[0])
            if tracked_meta is not None:
                t_inst_key, t_side = tracked_meta
            else:
                ev_inst = self._event_get(event, "instrument_id", None) or self._archetype_config.instrument_id
                t_inst_key = str(ev_inst)
                ev_side = self._event_get(event, "order_side", None)
                t_side = (
                    OrderSide.BUY
                    if "BUY" in str(ev_side or "").upper()
                    else (
                        OrderSide.SELL
                        if "SELL" in str(ev_side or "").upper()
                        else None
                    )
                )
            if t_side is not None:
                prev_signed = self._tracked_signed_qty_by_inst.get(t_inst_key, Decimal("0"))
                next_signed = (
                    prev_signed + fill_qty_dec
                    if t_side == OrderSide.BUY
                    else prev_signed - fill_qty_dec
                )
                if next_signed == Decimal("0"):
                    self._tracked_signed_qty_by_inst.pop(t_inst_key, None)
                else:
                    self._tracked_signed_qty_by_inst[t_inst_key] = next_signed

        # 1. Check if this fill belongs to a pending Two-Phase Safe Exit stop order
        for spec in list(self._pending_safe_exits.values()):
            if cid in spec.get("pending_stop_cids", set()):
                self._advance_pending_safe_exit_for_stop_event(
                    cid,
                    stop_filled=True,
                    stop_fill_qty_dec=fill_qty_dec,
                )
                return

        # 2. Check if this fill is an active protective stop or exit order completing
        for inst_key, stop_cids in list(self._active_protective_stops_by_inst.items()):
            if cid in stop_cids:
                ev_ts = int(getattr(event, "ts_event", 0) or getattr(event, "ts_init", 0) or 0)
                self._last_stop_exit_ts_ns = max(self._last_stop_exit_ts_ns, ev_ts, self._last_bar_ts_ns)
                self._trailing_stop_price = None
                stop_total = self._stop_qty_by_cid.get(cid, Decimal("0"))
                if stop_total <= Decimal("0") or fill_qty_dec >= stop_total:
                    self._remove_tracked_stop_cid(
                        cid, InstrumentId.from_str(inst_key)
                    )
                else:
                    self._stop_qty_by_cid[cid] = max(
                        Decimal("0"), stop_total - fill_qty_dec
                    )

        for inst_key, active_exit_cid in list(self._active_exit_orders_by_inst.items()):
            if active_exit_cid == cid:
                target_inst = InstrumentId.from_str(inst_key)
                if self._open_position_signed_qty(target_inst) == Decimal("0"):
                    self._active_exit_orders_by_inst.pop(inst_key, None)
                    self._notify_safe_exit_end(
                        self._symbol_from_instrument_id(target_inst)
                    )

        # 3. Position-driven protective stop synchronization for entry partial/full fills
        lookup_cid = cid
        if "-E" in cid:
            lookup_cid = cid.rsplit("-E", 1)[0]
        leaves_obj = getattr(event, "leaves_qty", None)
        leaves_dec = (
            _to_decimal(
                leaves_obj.as_decimal() if hasattr(leaves_obj, "as_decimal") else leaves_obj,
                "0",
            )
            if leaves_obj is not None
            else None
        )
        spec = self._pending_protective_stops.get(cid)
        if spec is None and "-E" in cid:
            spec = self._pending_protective_stops.get(lookup_cid)
        target_qty_dec = (
            _to_decimal(spec.get("target_qty_dec"), "0")
            if spec is not None
            else Decimal("0")
        )
        prev_covered_dec = (
            _to_decimal(spec.get("covered_qty_dec"), "0")
            if spec is not None
            else Decimal("0")
        )
        projected_covered_dec = prev_covered_dec + max(Decimal("0"), fill_qty_dec)
        is_entry_fully_filled = (
            (leaves_dec is not None and leaves_dec <= Decimal("0"))
            or (
                leaves_dec is None
                and (
                    target_qty_dec <= Decimal("0")
                    or projected_covered_dec >= target_qty_dec
                )
            )
        )
        if is_entry_fully_filled:
            for e_inst_key, e_spec in list(self._active_entry_orders_by_inst.items()):
                if e_spec.get("cid") in (cid, lookup_cid):
                    self._active_entry_orders_by_inst.pop(e_inst_key, None)
        if spec is not None and self._should_submit_protective_stop():
            target_qty_dec = _to_decimal(spec.get("target_qty_dec"), "0")
            prev_covered_dec = _to_decimal(spec.get("covered_qty_dec"), "0")
            if fill_qty_dec <= Decimal("0"):
                fill_qty_dec = max(target_qty_dec - prev_covered_dec, target_qty_dec)
            new_covered_dec = prev_covered_dec + max(Decimal("0"), fill_qty_dec)
            if target_qty_dec > Decimal("0"):
                new_covered_dec = min(target_qty_dec, new_covered_dec)
            delta_qty_dec = max(Decimal("0"), new_covered_dec - prev_covered_dec)
            if delta_qty_dec > Decimal("0") or target_qty_dec <= Decimal("0"):
                synced_stop_cid = self._sync_protective_stop_for_entry_fill(
                    spec=spec,
                    new_covered_dec=new_covered_dec
                    if new_covered_dec > Decimal("0")
                    else target_qty_dec,
                )
                if synced_stop_cid is not None:
                    spec["covered_qty_dec"] = new_covered_dec
                    if target_qty_dec <= Decimal("0") or new_covered_dec >= target_qty_dec:
                        self._pending_protective_stops.pop(lookup_cid, None)
            elif target_qty_dec > Decimal("0") and new_covered_dec >= target_qty_dec:
                if self._active_protective_stops_by_inst.get(str(spec["instrument_id"])):
                    self._pending_protective_stops.pop(lookup_cid, None)

    def on_order_partially_filled(self, event: Any) -> None:
        self.on_order_filled(event)

    def _is_order_denied_or_rejected(self, cid: str) -> bool:
        if not cid:
            return True
        base_cid = cid.rsplit("-E", 1)[0] if "-E" in cid else cid
        if (
            cid in self._denied_order_ids
            or base_cid in self._denied_order_ids
            or cid in self._rejected_order_ids
            or base_cid in self._rejected_order_ids
        ):
            return True
        if self._plan_gate_ref is not None and hasattr(self._plan_gate_ref, "decisions"):
            try:
                decisions = self._plan_gate_ref.decisions
                dec = decisions.get(cid) or decisions.get(base_cid)
                if dec is not None and not bool(getattr(dec, "allowed", True)):
                    return True
            except Exception:
                pass
        order_obj = self._submitted_orders_by_cid.get(cid)
        if self._is_trader_registered():
            try:
                cached = self.cache.order(
                    getattr(order_obj, "client_order_id", cid)
                    if order_obj is not None
                    else cid
                )
                if cached is not None:
                    order_obj = cached
            except Exception:
                pass
        if order_obj is not None:
            status_str = self._normalized_order_status(order_obj)
            if status_str in ("DENIED", "REJECTED", "CANCELED", "EXPIRED"):
                return True
        return False

    def _on_order_terminal(self, event: Any, status_str: str) -> None:
        cid = str(self._event_get(event, "client_order_id", "") or "")
        if not cid:
            return
        base_cid = cid.rsplit("-E", 1)[0] if "-E" in cid else cid
        norm_status = str(status_str or "").strip().upper()
        if norm_status == "DENIED":
            self._denied_order_ids.add(cid)
            self._denied_order_ids.add(base_cid)
        elif norm_status == "REJECTED":
            self._rejected_order_ids.add(cid)
            self._rejected_order_ids.add(base_cid)
        dedup_key = (base_cid, norm_status)
        if dedup_key in self._processed_terminal_events:
            return
        self._processed_terminal_events.add(dedup_key)
        for e_inst_key, e_spec in list(self._active_entry_orders_by_inst.items()):
            if e_spec.get("cid") in (cid, base_cid):
                self._active_entry_orders_by_inst.pop(e_inst_key, None)
        self._pending_protective_stops.pop(cid, None)
        if "-E" in cid:
            self._pending_protective_stops.pop(base_cid, None)
        self._remove_tracked_stop_cid(cid)
        self._advance_pending_safe_exit_for_stop_event(cid, stop_filled=False)
        self._handle_exit_order_failure(cid, status_str)

    def on_order_denied(self, event: Any) -> None:
        self._on_order_terminal(event, "DENIED")

    def on_order_rejected(self, event: Any) -> None:
        self._on_order_terminal(event, "REJECTED")

    def on_order_canceled(self, event: Any) -> None:
        self._on_order_terminal(event, "CANCELED")

    def on_order_expired(self, event: Any) -> None:
        self._on_order_terminal(event, "EXPIRED")

    def _sync_tracked_position_from_event(self, event: Any) -> None:
        inst_id = self._event_get(event, "instrument_id", None)
        target_inst = inst_id or self._archetype_config.instrument_id
        inst_key = str(target_inst)
        if self._is_owner_thread():
            signed_qty = self._open_position_signed_qty(target_inst)
            if signed_qty != Decimal("0"):
                return
        signed_raw = self._event_get(event, "signed_qty", None)
        if signed_raw is not None:
            qty_dec = _to_decimal(signed_raw, "0")
            if qty_dec != Decimal("0"):
                self._tracked_signed_qty_by_inst[inst_key] = qty_dec
                return
        qty = _to_decimal(self._event_get(event, "quantity", 0), "0")
        if qty > Decimal("0"):
            side_str = str(
                self._event_get(event, "side", self._event_get(event, "entry", "LONG"))
            ).upper()
            self._tracked_signed_qty_by_inst[inst_key] = (
                -qty if ("SHORT" in side_str or "SELL" in side_str) else qty
            )

    def on_position_opened(self, event: Any) -> None:
        self._sync_tracked_position_from_event(event)

    def on_position_changed(self, event: Any) -> None:
        self._sync_tracked_position_from_event(event)

    def on_position_closed(self, event: Any) -> None:
        inst_id = self._event_get(event, "instrument_id", None)
        target_inst = inst_id or self._archetype_config.instrument_id
        inst_key = str(target_inst)
        sym = self._symbol_from_instrument_id(target_inst)
        self._tracked_signed_qty_by_inst.pop(inst_key, None)
        self._pending_safe_exits.pop(inst_key, None)
        self._active_exit_orders_by_inst.pop(inst_key, None)
        self._active_entry_orders_by_inst.pop(inst_key, None)
        self._notify_safe_exit_end(sym)
        if (
            self._plan_gate_ref is not None
            and hasattr(self._plan_gate_ref, "release_closed_position_capacity")
            and self._open_position_signed_qty(target_inst) == Decimal("0")
        ):
            item_id = (
                getattr(self._archetype_config, "plan_item_id", None)
                if str(target_inst) == str(self._archetype_config.instrument_id)
                else None
            )
            self._plan_gate_ref.release_closed_position_capacity(
                sym,
                instrument_id=target_inst,
                item_id=item_id,
            )
        if inst_id is None or str(inst_id) == str(self._archetype_config.instrument_id):
            self._trailing_stop_price = None
            self._pending_thesis_exit = False

    def _current_tightest_stop_price(
        self, instrument_id: Optional[InstrumentId] = None
    ) -> Optional[Decimal]:
        target_inst = instrument_id or self._archetype_config.instrument_id
        candidates: List[Decimal] = []
        if (
            self._risk_envelope is not None
            and self._risk_envelope.initial_stop_price > Decimal("0")
        ):
            candidates.append(self._risk_envelope.initial_stop_price)
        elif (
            getattr(self._archetype_config, "_plan_initial_stop_price", None) is not None
            and self._archetype_config._plan_initial_stop_price > Decimal("0")
        ):
            candidates.append(self._archetype_config._plan_initial_stop_price)
        if self._trailing_stop_price is not None and self._trailing_stop_price > 0.0:
            candidates.append(Decimal(f"{self._trailing_stop_price:.4f}"))
        for stop_order in self._get_open_protective_stop_orders(target_inst):
            trig_px = getattr(stop_order, "trigger_price", None)
            if trig_px is not None:
                trig_dec = (
                    Decimal(str(trig_px.as_decimal()))
                    if hasattr(trig_px, "as_decimal")
                    else _to_decimal(trig_px, "0")
                )
                if trig_dec > Decimal("0"):
                    candidates.append(trig_dec)
        if not candidates:
            return None
        if self._archetype_config.plan_side == PlanSide.LONG:
            return max(candidates)
        return min(candidates)

    def _replace_protective_stop_price(
        self,
        new_stop_price: float,
        instrument_id: Optional[InstrumentId] = None,
    ) -> Optional[str]:
        target_inst = instrument_id or self._archetype_config.instrument_id
        inst_key = str(target_inst)
        sym = self._symbol_from_instrument_id(target_inst)
        open_stops = self._get_open_protective_stop_orders(target_inst)
        signed_pos = self._open_position_signed_qty(target_inst)
        abs_pos = abs(signed_pos)
        if not open_stops and (abs_pos <= Decimal("0") or not self._should_submit_protective_stop()):
            return None

        desired_qty = abs_pos
        if desired_qty <= Decimal("0") and open_stops:
            first_cid = str(getattr(open_stops[0], "client_order_id", "") or "")
            desired_qty = self._stop_qty_by_cid.get(
                first_cid,
                _to_decimal(getattr(open_stops[0], "quantity", 0), "0"),
            )
        if desired_qty <= Decimal("0"):
            return None

        entry_side = (
            OrderSide.BUY
            if (signed_pos > Decimal("0") or self._archetype_config.plan_side == PlanSide.LONG)
            else OrderSide.SELL
        )
        stop_qty_obj = self._format_quantity(desired_qty, target_inst)
        replacement_stop = self._build_protective_stop_order(
            instrument_id=target_inst,
            entry_side=entry_side,
            quantity=stop_qty_obj,
            stop_loss_price=float(new_stop_price),
            parent_entry_cid=f"rollover-{sym}",
        )
        if replacement_stop is None:
            return None

        planned_new_cid = str(replacement_stop.client_order_id)
        for old_stop in open_stops:
            old_cid = str(getattr(old_stop, "client_order_id", "") or "")
            if not old_cid or old_cid == planned_new_cid:
                continue
            self._notify_stop_leg_replaced(
                sym,
                old_cid,
                planned_new_cid,
                new_covered_qty=desired_qty,
                stop_price=float(new_stop_price),
            )
            self._remove_tracked_stop_cid(old_cid, target_inst)
            self._cancel_single_order_safely(old_stop)

        new_stop_cid = self._register_and_submit_protective_stop_order(
            replacement_stop,
            target_inst,
            stop_qty_obj,
        )
        self._active_protective_stops_by_inst[inst_key] = [new_stop_cid]
        return new_stop_cid

    def apply_renewed_plan_item(self, new_item: PlanItemContract) -> PlanRiskEnvelope:
        """Applies a `RENEWED_ACTIVE` `PlanItemContract` in-place while enforcing monotonic
        stop-loss tightening (`new_stop >= current_stop` for LONG, `new_stop <= current_stop` for SHORT).
        """
        cfg = self._archetype_config
        new_cfg = new_item.strategy_config
        new_env = new_item.risk_envelope

        current_stop_dec = self._current_tightest_stop_price(cfg.instrument_id)
        proposed_stop_dec = new_env.initial_stop_price
        if current_stop_dec is not None and current_stop_dec > Decimal("0"):
            if cfg.plan_side == PlanSide.LONG:
                tightened_stop_dec = max(current_stop_dec, proposed_stop_dec)
            else:
                tightened_stop_dec = min(current_stop_dec, proposed_stop_dec)
        else:
            tightened_stop_dec = proposed_stop_dec

        renewed_env = replace(new_env, initial_stop_price=tightened_stop_dec)

        cfg.archetype_contract = new_cfg
        cfg.lookback_window = new_cfg.lookback_window
        cfg.fast_window = new_cfg.fast_window
        cfg.slow_window = new_cfg.slow_window
        cfg.entry_quantile_threshold = new_cfg.entry_quantile_threshold
        cfg.exit_quantile_threshold = new_cfg.exit_quantile_threshold
        cfg.atr_stop_multiplier = new_cfg.atr_stop_multiplier
        cfg.trade_qty = renewed_env.target_qty
        cfg._plan_risk_envelope = renewed_env
        cfg._plan_initial_stop_price = tightened_stop_dec
        cfg._plan_max_risk_budget_usd = renewed_env.max_risk_budget_usd

        self._risk_envelope = renewed_env
        self._exit_only_mode = False
        self._thesis_invalidated = False
        self._pending_thesis_exit = False

        tightened_float = float(tightened_stop_dec)
        if self._trailing_stop_price is not None:
            if cfg.plan_side == PlanSide.LONG:
                self._trailing_stop_price = max(self._trailing_stop_price, tightened_float)
            else:
                self._trailing_stop_price = min(self._trailing_stop_price, tightened_float)
        elif self._open_position_signed_qty(cfg.instrument_id) != Decimal("0"):
            self._trailing_stop_price = tightened_float

        if (
            current_stop_dec is None
            or (cfg.plan_side == PlanSide.LONG and tightened_stop_dec > current_stop_dec)
            or (cfg.plan_side == PlanSide.SHORT and tightened_stop_dec < current_stop_dec)
        ):
            if self._get_open_protective_stop_orders(cfg.instrument_id):
                self._replace_protective_stop_price(tightened_float, cfg.instrument_id)

        return renewed_env

    def enter_carry_over_exit_only_mode(self) -> None:
        """Transitions the strategy into `CARRY_OVER_EXIT_ONLY` mode: blocks new entries
        while keeping protective stops, trailing stop ratchets, and exit signals active.
        """
        self._exit_only_mode = True
        self._thesis_invalidated = False
        self._pending_thesis_exit = False

    def trigger_thesis_invalidated_exit(
        self,
        reference_price: Optional[float] = None,
        *,
        execute_immediately: bool = True,
        reason_tag: str = "THESIS_INVALIDATED_EXIT",
    ) -> Optional[str]:
        """Transitions the strategy into `THESIS_INVALIDATED_EXIT` mode and triggers
        Two-Phase Safe Exit (`begin_safe_exit` -> cancel old stop leg -> submit `reduce_only=True` exit).
        """
        self._exit_only_mode = True
        self._thesis_invalidated = True
        self._pending_thesis_exit = True
        self._pending_thesis_exit_reason = str(reason_tag or "THESIS_INVALIDATED_EXIT")
        target_inst = self._archetype_config.instrument_id
        sym = self._symbol_from_instrument_id(target_inst)

        if not execute_immediately or not self._is_owner_thread():
            return None

        signed_pos = self._open_position_signed_qty(target_inst)
        open_stops = self._get_open_protective_stop_orders(target_inst)
        if signed_pos == Decimal("0") and not open_stops:
            return None

        self._notify_safe_exit_begin(sym)

        eff_ref = (
            float(reference_price)
            if reference_price is not None and float(reference_price) > 0.0
            else (float(self._closes[-1]) if self._closes else 0.0)
        )
        if eff_ref <= 0.0 and self._risk_envelope is not None:
            env_ref = _to_decimal(getattr(self._risk_envelope, "reference_price", None), "0")
            if env_ref > Decimal("0"):
                eff_ref = float(env_ref)
            elif self._risk_envelope.target_qty > Decimal("0"):
                eff_ref = float(
                    self._risk_envelope.target_notional_usd / self._risk_envelope.target_qty
                )

        exit_cid = self._submit_plan_gated_exit(
            reference_price=eff_ref,
            reason_tag=self._pending_thesis_exit_reason,
            instrument_id=target_inst,
            signal_tags=["THESIS_INVALIDATED_EXIT"],
        )
        sec_inst = self._archetype_config.secondary_instrument_id
        if sec_inst is not None:
            sec_pos = self._open_position_signed_qty(sec_inst)
            if sec_pos != Decimal("0"):
                self._submit_plan_gated_exit(
                    exit_side=OrderSide.BUY if sec_pos < Decimal("0") else OrderSide.SELL,
                    quantity=abs(sec_pos),
                    reference_price=eff_ref,
                    reason_tag=self._pending_thesis_exit_reason,
                    instrument_id=sec_inst,
                    signal_tags=["THESIS_INVALIDATED_EXIT"],
                )
        if exit_cid:
            self._pending_thesis_exit = False
        return exit_cid

    def _check_and_execute_pending_thesis_exit(self, bar: Bar) -> bool:
        """Executes any pending `THESIS_INVALIDATED_EXIT` on the incoming RTH bar if not already exited."""
        if self._is_owner_thread():
            self._flush_cross_thread_orders()
        if not self._thesis_invalidated:
            return False
        target_inst = self._archetype_config.instrument_id
        inst_key = str(target_inst)
        if (
            self._pending_thesis_exit
            and inst_key not in self._active_exit_orders_by_inst
            and inst_key not in self._pending_safe_exits
        ):
            signed_pos = self._open_position_signed_qty(target_inst)
            open_stops = self._get_open_protective_stop_orders(target_inst)
            if signed_pos != Decimal("0") or open_stops:
                close_px = float(bar.close.as_double())
                self.trigger_thesis_invalidated_exit(
                    reference_price=close_px,
                    execute_immediately=True,
                    reason_tag=self._pending_thesis_exit_reason,
                )
        return True

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
        if self._in_warmup or self._exit_only_mode or self._thesis_invalidated:
            return ""
        if (
            self._last_stop_exit_ts_ns > 0
            and self._last_bar_ts_ns > 0
            and self._last_bar_ts_ns <= self._last_stop_exit_ts_ns
        ):
            return ""
        delay_bars = int(getattr(self._archetype_config, "execution_delay_bars", 0) or 0)
        if delay_bars > 0 and not self._flushing_delayed_orders:
            self._delayed_order_queue.append(
                {
                    "due_bar_seq": self._post_warmup_bar_seq + delay_bars,
                    "action": "entry",
                    "kwargs": {
                        "order_side": order_side,
                        "reference_price": reference_price,
                        "atr": atr,
                        "limit_price": limit_price,
                        "quantity": quantity,
                        "instrument_id": instrument_id,
                        "stop_loss_price": stop_loss_price,
                        "tags": list(tags) if tags is not None else None,
                    },
                }
            )
            return f"DELAYED-ENTRY-{self._post_warmup_bar_seq + delay_bars}"
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
        if self._last_bar_ts_ns > 100_000_000_000_000_000:
            exec_params["evaluation_ts"] = datetime.fromtimestamp(
                self._last_bar_ts_ns / 1e9, tz=timezone.utc
            ).isoformat()
        if stop_loss_price is not None:
            exec_params["stop_loss_price"] = f"{float(stop_loss_price):.6f}"

        eff_limit = self._resolve_entry_limit_price(
            order_side=order_side,
            reference_price=reference_price,
            atr=atr,
            limit_price=limit_price,
        )
        order_factory = self._get_order_factory()
        if self._archetype_config.use_limit_orders and eff_limit > 0.0:
            order = order_factory.limit(
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
            order = order_factory.market(
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
        initial_pos_qty_dec = abs(self._open_position_signed_qty(target_inst))
        if (
            self._should_submit_protective_stop()
            and stop_loss_price is not None
            and stop_loss_price > 0.0
        ):
            target_qty_dec = (
                Decimal(str(qty_obj.as_decimal()))
                if hasattr(qty_obj, "as_decimal")
                else _to_decimal(qty_obj, "0")
            )
            self._pending_protective_stops[cid] = {
                "instrument_id": target_inst,
                "entry_side": order_side,
                "quantity": qty_obj,
                "target_qty_dec": target_qty_dec,
                "covered_qty_dec": Decimal("0"),
                "initial_pos_qty_dec": initial_pos_qty_dec,
                "stop_loss_price": float(stop_loss_price),
                "parent_entry_cid": cid,
            }

        self._order_side_by_cid[cid] = (str(target_inst), order_side)
        self._active_entry_orders_by_inst[str(target_inst)] = {
            "cid": cid,
            "order": order,
            "instrument_id": target_inst,
            "submitted_bar_seq": self._post_warmup_bar_seq,
        }
        self._submitted_order_ids.append(cid)
        self._submit_order_safely(order)

        live_order = order
        if self._is_trader_registered():
            try:
                cached = self.cache.order(order.client_order_id)
                if cached is not None:
                    live_order = cached
            except Exception:
                pass
        status_str = self._normalized_order_status(live_order)
        is_denied_or_rejected = self._is_order_denied_or_rejected(cid)
        if is_denied_or_rejected or status_str in (
            "DENIED",
            "REJECTED",
            "CANCELED",
            "EXPIRED",
            "FILLED",
        ):
            self._active_entry_orders_by_inst.pop(str(target_inst), None)
        if is_denied_or_rejected and (cid, "DENIED") not in self._processed_terminal_events and (cid, "REJECTED") not in self._processed_terminal_events:
            self.on_order_denied(live_order)

        if cid in self._pending_protective_stops:
            if is_denied_or_rejected or status_str in (
                "DENIED",
                "REJECTED",
                "CANCELED",
                "EXPIRED",
            ):
                self._pending_protective_stops.pop(cid, None)
            else:
                spec = self._pending_protective_stops.get(cid)
                covered_qty_dec = (
                    _to_decimal(spec.get("covered_qty_dec"), "0")
                    if spec is not None
                    else Decimal("0")
                )
                order_filled_qty = _to_decimal(
                    getattr(live_order, "filled_qty", 0), "0"
                )
                current_pos_qty_dec = abs(self._open_position_signed_qty(target_inst))
                pos_delta_dec = max(
                    Decimal("0"), current_pos_qty_dec - initial_pos_qty_dec
                )
                if spec is not None and covered_qty_dec == Decimal("0") and (
                    status_str == "FILLED"
                    or order_filled_qty > Decimal("0")
                    or pos_delta_dec > Decimal("0")
                ):
                    target_qty_dec = _to_decimal(spec.get("target_qty_dec"), "0")
                    effective_fill_dec = (
                        order_filled_qty
                        if order_filled_qty > Decimal("0")
                        else (
                            pos_delta_dec
                            if pos_delta_dec > Decimal("0")
                            else target_qty_dec
                        )
                    )
                    synced_stop_cid = self._sync_protective_stop_for_entry_fill(
                        spec=spec,
                        new_covered_dec=effective_fill_dec,
                    )
                    if synced_stop_cid is not None:
                        spec["covered_qty_dec"] = effective_fill_dec
                        if (
                            status_str == "FILLED"
                            or target_qty_dec <= Decimal("0")
                            or effective_fill_dec >= target_qty_dec
                        ):
                            self._pending_protective_stops.pop(cid, None)

        return cid

    def _execute_exit_order_submission(
        self,
        *,
        instrument_id: InstrumentId,
        exit_side: OrderSide,
        quantity: Decimal | float,
        reference_price: float,
        reason_tag: str,
        signal_tags: Optional[List[str]] = None,
    ) -> str:
        qty_obj = self._format_quantity(abs(float(quantity)), instrument_id)
        exec_params = {
            "intent": "EXIT",
            "reduce_only": "true",
            "reference_price": f"{reference_price:.6f}",
            "exit_reason": reason_tag,
            "archetype_id": self._archetype_config.ARCHETYPE_ID.value,
        }
        order_tags = [reason_tag]
        if signal_tags:
            for tag in signal_tags:
                if tag and tag not in order_tags:
                    order_tags.append(str(tag))
        order = self._get_order_factory().market(
            instrument_id=instrument_id,
            order_side=exit_side,
            quantity=qty_obj,
            time_in_force=TimeInForce.GTC,
            reduce_only=True,
            exec_algorithm_id=self._archetype_config.exec_algorithm_id,
            exec_algorithm_params=exec_params,
            tags=order_tags,
        )
        cid = str(order.client_order_id)
        inst_key = str(instrument_id)
        self._active_exit_orders_by_inst[inst_key] = cid
        self._order_side_by_cid[cid] = (inst_key, exit_side)
        self._submitted_order_ids.append(cid)
        self._submit_order_safely(order)
        self._trailing_stop_price = None
        return cid

    def _submit_plan_gated_exit(
        self,
        exit_side: Optional[OrderSide] = None,
        quantity: Optional[Decimal | float] = None,
        reference_price: float = 0.0,
        reason_tag: str = "SIGNAL_EXIT",
        instrument_id: Optional[InstrumentId] = None,
        *,
        reason: Optional[str] = None,
        signal_tags: Optional[List[str]] = None,
    ) -> str:
        """Executes Two-Phase Safe Exit (or direct exit when no stop is open) via `PLAN_GATE`."""
        if self._in_warmup:
            return ""
        delay_bars = int(getattr(self._archetype_config, "execution_delay_bars", 0) or 0)
        if delay_bars > 0 and not self._flushing_delayed_orders and not self._thesis_invalidated:
            self._delayed_order_queue.append(
                {
                    "due_bar_seq": self._post_warmup_bar_seq + delay_bars,
                    "action": "exit",
                    "kwargs": {
                        "exit_side": exit_side,
                        "quantity": quantity,
                        "reference_price": reference_price,
                        "reason_tag": reason_tag,
                        "instrument_id": instrument_id,
                        "reason": reason,
                        "signal_tags": list(signal_tags) if signal_tags is not None else None,
                    },
                }
            )
            return f"DELAYED-EXIT-{self._post_warmup_bar_seq + delay_bars}"
        target_inst = instrument_id or self._archetype_config.instrument_id
        inst_key = str(target_inst)
        sym = self._symbol_from_instrument_id(target_inst)
        resolved_reason = str(reason or reason_tag or "SIGNAL_EXIT")

        signed_pos = self._open_position_signed_qty(target_inst)
        had_open_position_in_cache = signed_pos != Decimal("0")
        if exit_side is None:
            if signed_pos < Decimal("0"):
                resolved_exit_side = OrderSide.BUY
            elif signed_pos > Decimal("0"):
                resolved_exit_side = OrderSide.SELL
            else:
                resolved_exit_side = (
                    OrderSide.BUY
                    if self._archetype_config.plan_side == PlanSide.SHORT
                    else OrderSide.SELL
                )
        else:
            resolved_exit_side = exit_side

        if quantity is not None:
            resolved_qty_dec = abs(_to_decimal(quantity, "0"))
        elif signed_pos != Decimal("0"):
            resolved_qty_dec = abs(signed_pos)
        else:
            resolved_qty_dec = abs(_to_decimal(self._archetype_config.trade_qty, "1"))

        if reference_price <= 0.0 and self._closes:
            reference_price = float(self._closes[-1])

        for pending_cid, pending_spec in list(self._pending_protective_stops.items()):
            if str(pending_spec.get("instrument_id")) == inst_key:
                self._pending_protective_stops.pop(pending_cid, None)

        open_stops = self._get_open_protective_stop_orders(target_inst)
        if not open_stops:
            return self._execute_exit_order_submission(
                instrument_id=target_inst,
                exit_side=resolved_exit_side,
                quantity=resolved_qty_dec,
                reference_price=reference_price,
                reason_tag=resolved_reason,
                signal_tags=signal_tags,
            )

        existing_safe_exit = self._pending_safe_exits.get(inst_key)
        if existing_safe_exit is not None and existing_safe_exit.get("pending_stop_cids"):
            existing_safe_exit["exit_side"] = resolved_exit_side
            existing_safe_exit["quantity"] = resolved_qty_dec
            existing_safe_exit["reference_price"] = float(reference_price)
            existing_safe_exit["reason_tag"] = resolved_reason
            existing_safe_exit["signal_tags"] = list(signal_tags or [])
            return str(next(iter(existing_safe_exit["pending_stop_cids"])))

        pending_cids = {str(o.client_order_id) for o in open_stops}
        self._notify_safe_exit_begin(sym)
        safe_exit_spec: Dict[str, Any] = {
            "instrument_id": target_inst,
            "symbol": sym,
            "exit_side": resolved_exit_side,
            "quantity": resolved_qty_dec,
            "initial_abs_pos_qty": abs(signed_pos),
            "had_open_position_in_cache": had_open_position_in_cache,
            "stop_filled_qty_dec": Decimal("0"),
            "reference_price": float(reference_price),
            "reason_tag": resolved_reason,
            "signal_tags": list(signal_tags or []),
            "pending_stop_cids": pending_cids,
            "submitted_exit_cid": None,
        }
        self._pending_safe_exits[inst_key] = safe_exit_spec

        for stop_order in open_stops:
            self._cancel_single_order_safely(stop_order)

        if safe_exit_spec.get("submitted_exit_cid"):
            return str(safe_exit_spec["submitted_exit_cid"])
        return None
