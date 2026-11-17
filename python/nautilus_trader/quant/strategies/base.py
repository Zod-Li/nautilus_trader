"""Shared base classes and PlanGate order routing helpers for pre-audited Nautilus v2 strategies."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
import logging
import math
import threading
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from nautilus_trader.common import Clock, OrderFactory
from nautilus_trader.quant.contracts.models import (
    ArchetypeStrategyConfig,
    MarketableLimitCollar,
    PlanItemContract,
    PlanRiskEnvelope,
    PlanSide,
    StrategyArchetypeId,
    default_limit_collar_for_archetype,
)
from nautilus_trader.quant.governance.plan_gate import PLAN_GATE_EXEC_ALGORITHM_ID
from nautilus_trader.quant.strategies.empirical import rolling_atr
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
from nautilus_trader.quant.governance.bounded_collections import (
    MAX_TRACKED_ORDER_HISTORY,
    _BoundedFifoDict,
    _BoundedFifoList,
    _BoundedFifoSet,
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
        use_limit_orders: bool = True,
        submit_protective_stop: bool = False,
        execution_delay_bars: int = 0,
        entry_ttl_bars: Optional[int] = None,
        exec_algorithm_id: str | ExecAlgorithmId = PLAN_GATE_EXEC_ALGORITHM_ID,
        strategy_id: Optional[str | StrategyId] = None,
        order_id_tag: Optional[str] = None,
        manage_contingent_orders: bool = False,
        log_events: bool = True,
        log_commands: bool = True,
    ):
        _ = (execution_delay_bars, entry_ttl_bars)
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
        use_limit_orders: bool = True,
        submit_protective_stop: bool = False,
        execution_delay_bars: int = 0,
        entry_ttl_bars: Optional[int] = None,
        exec_algorithm_id: str | ExecAlgorithmId = PLAN_GATE_EXEC_ALGORITHM_ID,
        strategy_id: Optional[str | StrategyId] = None,
        order_id_tag: Optional[str] = None,
        manage_contingent_orders: bool = False,
        log_events: bool = True,
        log_commands: bool = True,
    ) -> None:
        inst_str = str(instrument_id).strip()
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
        )
        self.instrument_id = InstrumentId.from_str(inst_str)
        self.bar_timeframe = self.archetype_contract.bar_timeframe
        self.bar_type = format_datainfra_bar_type(inst_str, self.bar_timeframe)
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
        self.entry_ttl_bars: Optional[int] = (
            max(1, int(entry_ttl_bars)) if entry_ttl_bars is not None else None
        )
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
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            execution_delay_bars=execution_delay_bars,
            order_id_tag=plan_item.symbol[:8],
        )
        instance._plan_risk_envelope = plan_item.risk_envelope
        instance._plan_initial_stop_price = plan_item.risk_envelope.initial_stop_price
        instance._plan_max_risk_budget_usd = plan_item.risk_envelope.max_risk_budget_usd
        return instance


class BaseArchetypeStrategy(Strategy):
    """Base pre-audited Nautilus `Strategy` routing all orders through `PLAN_GATE`."""

    def __new__(cls, config: BaseArchetypeStrategyConfig) -> "BaseArchetypeStrategy":
        return super().__new__(cls, config)

    def __init__(self, config: BaseArchetypeStrategyConfig) -> None:
        super().__init__(config)
        self._owner_thread_id: int = threading.get_ident()
        self._archetype_config = config
        self._risk_envelope: Optional[PlanRiskEnvelope] = getattr(
            config, "_plan_risk_envelope", None
        )
        self._highs: List[float] = []
        self._lows: List[float] = []
        self._raw_highs: List[float] = []
        self._raw_lows: List[float] = []
        self._cleaned_bar_extremes_by_ts_ns: Dict[int, Tuple[float, float]] = _BoundedFifoDict(MAX_TRACKED_ORDER_HISTORY)
        self._raw_bar_extremes_by_ts_ns: Dict[int, Tuple[float, float]] = _BoundedFifoDict(MAX_TRACKED_ORDER_HISTORY)
        self._closes: List[float] = []
        self._volumes: List[float] = []
        self._bar_ts_ns_window: List[int] = []
        self._trailing_stop_price: Optional[float] = None
        self._submitted_order_ids: List[str] = _BoundedFifoList(MAX_TRACKED_ORDER_HISTORY)
        self._protective_stop_order_ids: List[str] = _BoundedFifoList(MAX_TRACKED_ORDER_HISTORY)
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
        self._order_side_by_cid: Dict[str, Tuple[str, OrderSide]] = _BoundedFifoDict(MAX_TRACKED_ORDER_HISTORY)
        self._submitted_orders_by_cid: Dict[str, Any] = _BoundedFifoDict(MAX_TRACKED_ORDER_HISTORY)
        self._submitted_orders_audit: List[Any] = _BoundedFifoList(MAX_TRACKED_ORDER_HISTORY)
        self._canceled_order_ids_audit: List[str] = _BoundedFifoList(MAX_TRACKED_ORDER_HISTORY)
        self._audit_trail: List[Any] = _BoundedFifoList(MAX_TRACKED_ORDER_HISTORY)
        self._decision_traces: List[Any] = _BoundedFifoList(MAX_TRACKED_ORDER_HISTORY)
        self._managed_client_order_ids: set[str] = _BoundedFifoSet(MAX_TRACKED_ORDER_HISTORY)
        self._filled_order_ids: set[str] = _BoundedFifoSet(MAX_TRACKED_ORDER_HISTORY)
        self._processed_fill_keys: set[str] = _BoundedFifoSet(MAX_TRACKED_ORDER_HISTORY)
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
        self._denied_order_ids: set[str] = _BoundedFifoSet(MAX_TRACKED_ORDER_HISTORY)
        self._rejected_order_ids: set[str] = _BoundedFifoSet(MAX_TRACKED_ORDER_HISTORY)
        self._processed_terminal_events: set[Tuple[str, str]] = _BoundedFifoSet(MAX_TRACKED_ORDER_HISTORY)
        self._protective_stop_order_id: Optional[str] = None
        self._protective_stop_qty: float = 0.0
        self._stop_price: Optional[float] = (
            float(self._risk_envelope.initial_stop_price)
            if self._risk_envelope is not None
            and self._risk_envelope.initial_stop_price > Decimal("0")
            else None
        )
        self._entry_filled_qty: float = 0.0
        self._pending_protective_stop_sync: Optional[Tuple[float, float]] = None
        self._deferred_protective_stop_submission: Optional[Tuple[float, float]] = None
        self._deferred_protective_stop_by_cid: Dict[str, Dict[str, Any]] = {}
        self._instrument_halted: bool = False
        self._halted_by_inst: Dict[str, bool] = {}
        self._accepted_order_ids: set[str] = _BoundedFifoSet(MAX_TRACKED_ORDER_HISTORY)
        self._unconfirmed_stop_cids: set[str] = _BoundedFifoSet(MAX_TRACKED_ORDER_HISTORY)
        self._stop_submitted_bar_seq_by_cid: Dict[str, int] = _BoundedFifoDict(MAX_TRACKED_ORDER_HISTORY)
        self._venue_order_id_by_cid: Dict[str, Any] = _BoundedFifoDict(MAX_TRACKED_ORDER_HISTORY)
        self._stop_price_by_cid: Dict[str, float] = {}
        self._prior_stop_qty_by_cid: Dict[str, Decimal] = {}
        self._modified_orders_audit: List[Dict[str, Any]] = _BoundedFifoList(MAX_TRACKED_ORDER_HISTORY)

    @property
    def instrument_id(self) -> InstrumentId:
        return self._archetype_config.instrument_id

    @property
    def _modified_orders(self) -> List[Dict[str, Any]]:
        return self._modified_orders_audit

    @property
    def _submitted_orders(self) -> List[Any]:
        return self._submitted_orders_audit

    @property
    def _canceled_orders(self) -> List[str]:
        return self._canceled_order_ids_audit

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
        without emitting any entry or exit orders.
        """
        if not bars:
            return 0
        sorted_bars = sorted(
            bars,
            key=lambda b: (
                int(getattr(b, "ts_event", 0) or getattr(b, "ts_init", 0) or 0),
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
        self._raw_highs.clear()
        self._raw_lows.clear()
        self._closes.clear()
        self._volumes.clear()
        self._bar_ts_ns_window.clear()
        self._last_bar_ts_ns = 0
        self._warmup_bar_count = 0

    def bind_dual_track_bar_extremes(
        self,
        *,
        cleaned_by_ts_ns: Optional[Mapping[int, Tuple[float, float]]] = None,
        raw_by_ts_ns: Optional[Mapping[int, Tuple[float, float]]] = None,
    ) -> None:
        """Binds dual-track bar extremes keyed by bar timestamp (`ts_ns`).

        Rolling indicators (`rolling_atr`, `vwap_anchor`) consume cleaned `(high, low)`,
        while stop-loss triggers inspect un-smoothed `(raw_high, raw_low)` to preserve
        1:1 backtest-to-live isomorphism with broker `STOP_MARKET` execution.
        """
        cleaned_dict: Dict[int, Tuple[float, float]] = _BoundedFifoDict(MAX_TRACKED_ORDER_HISTORY)
        if cleaned_by_ts_ns:
            for k, v in cleaned_by_ts_ns.items():
                cleaned_dict[int(k)] = (float(v[0]), float(v[1]))
        raw_dict: Dict[int, Tuple[float, float]] = _BoundedFifoDict(MAX_TRACKED_ORDER_HISTORY)
        if raw_by_ts_ns:
            for k, v in raw_by_ts_ns.items():
                raw_dict[int(k)] = (float(v[0]), float(v[1]))
        self._cleaned_bar_extremes_by_ts_ns = cleaned_dict
        self._raw_bar_extremes_by_ts_ns = raw_dict

    def _current_bar_raw_low(self, fallback: Optional[float] = None) -> float:
        if self._raw_lows:
            return float(self._raw_lows[-1])
        if fallback is not None:
            return float(fallback)
        return float(self._lows[-1]) if self._lows else 0.0

    def _current_bar_raw_high(self, fallback: Optional[float] = None) -> float:
        if self._raw_highs:
            return float(self._raw_highs[-1])
        if fallback is not None:
            return float(fallback)
        return float(self._highs[-1]) if self._highs else 0.0

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
        try:
            import asyncio

            self._owner_loop = asyncio.get_running_loop()
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
        self._managed_client_order_ids.add(cid)
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
        is_buy = (
            side == OrderSide.BUY
            if side is not None
            else self._archetype_config.plan_side == PlanSide.LONG
        )
        if raw_stop_price is not None:
            eff_raw_stop = float(raw_stop_price)
            if eff_ref > 0.0 and (
                (is_buy and eff_raw_stop >= eff_ref)
                or (not is_buy and eff_raw_stop <= eff_ref)
            ):
                dist = max(0.0, float(raw_stop_distance or 0.0))
                if (
                    dist <= 0.0
                    and self._risk_envelope is not None
                    and self._risk_envelope.target_qty > Decimal("0")
                ):
                    plan_ref = float(
                        self._risk_envelope.target_notional_usd
                        / self._risk_envelope.target_qty
                    )
                    plan_stop = float(self._risk_envelope.initial_stop_price)
                    if plan_ref > 0.0 and plan_stop > 0.0 and abs(plan_ref - plan_stop) > 0.0:
                        dist = abs(plan_ref - plan_stop)
                if dist > 0.0:
                    eff_raw_stop = max(0.01, eff_ref - dist) if is_buy else eff_ref + dist
        else:
            dist = max(0.0, float(raw_stop_distance or 0.0))
            eff_raw_stop = (
                (max(0.01, eff_ref - dist) if is_buy else eff_ref + dist)
                if eff_ref > 0.0
                else (0.0 - dist if is_buy else dist)
            )
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
        status: Optional[str] = None,
    ) -> None:
        actor = self._resolve_telemetry_actor()
        qty_dec = _to_decimal(new_covered_qty, "0")
        px_dec = _to_decimal(stop_price, "0")
        target_status = (
            str(status).strip().upper()
            if status
            else (
                "REPLACING"
                if str(new_cid or "").startswith("deferred-replace-")
                else "ACTIVE"
            )
        )
        oco_mon = (
            getattr(actor, "oco_monitor", None)
            or getattr(actor, "_oco_monitor", None)
            or (
                actor
                if hasattr(actor, "register_protective_order")
                or hasattr(actor, "on_stop_leg_replaced")
                else None
            )
        )
        if (
            oco_mon is not None
            and hasattr(oco_mon, "on_stop_leg_replaced")
            and qty_dec > Decimal("0")
            and px_dec > Decimal("0")
        ):
            try:
                oco_mon.on_stop_leg_replaced(
                    symbol=symbol,
                    old_broker_order_id=str(old_cid or ""),
                    new_broker_order_id=str(new_cid or old_cid or ""),
                    new_covered_qty=qty_dec,
                    stop_price=Decimal(f"{float(px_dec):.4f}"),
                    status=target_status,
                )
            except TypeError:
                try:
                    oco_mon.on_stop_leg_replaced(
                        symbol=symbol,
                        old_broker_order_id=str(old_cid or ""),
                        new_broker_order_id=str(new_cid or old_cid or ""),
                        new_covered_qty=qty_dec,
                        stop_price=Decimal(f"{float(px_dec):.4f}"),
                    )
                except Exception:
                    pass
            except Exception:
                pass
        if actor is not None and hasattr(actor, "mark_stop_leg_replaced"):
            try:
                actor.mark_stop_leg_replaced(
                    symbol,
                    old_cid,
                    new_cid,
                    new_covered_qty=new_covered_qty,
                    stop_price=stop_price,
                )
                return
            except TypeError:
                try:
                    actor.mark_stop_leg_replaced(symbol, old_cid, new_cid)
                    return
                except Exception:
                    pass
            except Exception:
                pass
        if oco_mon is not None and hasattr(oco_mon, "register_protective_order"):
            try:
                if qty_dec > Decimal("0") and px_dec > Decimal("0"):
                    oco_mon.register_protective_order(
                        symbol=symbol,
                        covered_qty=qty_dec,
                        stop_price=px_dec,
                        broker_order_id=str(new_cid or old_cid or ""),
                        status=target_status,
                        persist=bool(getattr(oco_mon, "_persist_to_db", False)),
                    )
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
            try:
                self.subscribe_bars(self._archetype_config.bar_type)
            except Exception as exc:
                logger.debug(
                    "Skipping subscribe_bars on_start for %s (unattached harness): %s",
                    self._archetype_config.bar_type,
                    exc,
                )
            if hasattr(self, "subscribe_quote_ticks"):
                try:
                    self.subscribe_quote_ticks(self.instrument_id)
                except Exception as exc:
                    logger.debug(
                        "Skipping subscribe_quote_ticks on_start for %s (unattached harness): %s",
                        self.instrument_id,
                        exc,
                    )
            if hasattr(self, "subscribe_instrument_status"):
                try:
                    self.subscribe_instrument_status(self.instrument_id)
                except Exception as exc:
                    logger.debug(
                        "Skipping subscribe_instrument_status on_start for %s (unattached harness): %s",
                        self.instrument_id,
                        exc,
                    )
            self._ensure_protective_stop_coverage_on_start()
            if self._thesis_invalidated and self._pending_thesis_exit:
                if self._is_rth_open_ts():
                    self._check_and_execute_pending_thesis_exit_at_price(0.0)
                else:
                    self._schedule_rth_open_thesis_exit_timer()

    def _is_rth_open_ts(self, ts_ns: Optional[int] = None) -> bool:
        resolved_ns = int(ts_ns or 0)
        if resolved_ns <= 0 and self._is_owner_thread():
            try:
                clock_obj = getattr(self, "clock", None) or self._standalone_clock
                if clock_obj is not None and hasattr(clock_obj, "timestamp_ns"):
                    resolved_ns = int(clock_obj.timestamp_ns() or 0)
            except Exception:
                resolved_ns = 0
        if resolved_ns <= 0:
            resolved_ns = int(self._last_bar_ts_ns or 0)
        if resolved_ns > 100_000_000_000_000_000:
            dt_utc = datetime.fromtimestamp(resolved_ns / 1_000_000_000.0, tz=timezone.utc)
        else:
            dt_utc = datetime.now(timezone.utc)
        ny_dt = dt_utc.astimezone(ZoneInfo("America/New_York"))
        if ny_dt.weekday() >= 5:
            return False
        minutes_of_day = ny_dt.hour * 60 + ny_dt.minute
        return (9 * 60 + 30) <= minutes_of_day < (16 * 60)

    def _schedule_rth_open_thesis_exit_timer(self) -> None:
        if not self._is_owner_thread():
            return
        try:
            clock_obj = getattr(self, "clock", None) or self._standalone_clock
            if clock_obj is None or not hasattr(clock_obj, "set_time_alert_ns"):
                return
            now_ns = int(clock_obj.timestamp_ns() or 0) if hasattr(clock_obj, "timestamp_ns") else 0
            if now_ns > 100_000_000_000_000_000:
                now_utc = datetime.fromtimestamp(now_ns / 1_000_000_000.0, tz=timezone.utc)
            else:
                now_utc = datetime.now(timezone.utc)
            ny_tz = ZoneInfo("America/New_York")
            ny_now = now_utc.astimezone(ny_tz)
            rth_open_ny = ny_now.replace(hour=9, minute=30, second=0, microsecond=0)
            if ny_now >= rth_open_ny:
                return
            alert_ns = int(rth_open_ny.astimezone(timezone.utc).timestamp() * 1_000_000_000)
            alert_name = f"thesis_exit_rth_open_{self._symbol_from_instrument_id(self._archetype_config.instrument_id)}"
            try:
                clock_obj.cancel_timer(alert_name)
            except Exception:
                pass
            clock_obj.set_time_alert_ns(
                name=alert_name,
                alert_time_ns=alert_ns,
                callback=self._on_rth_open_thesis_exit_timer,
            )
        except Exception as exc:
            logger.debug("Skipping RTH open thesis exit timer registration: %s", exc)

    def _on_rth_open_thesis_exit_timer(self, _event: Any = None) -> None:
        if self._thesis_invalidated and self._pending_thesis_exit:
            self._check_and_execute_pending_thesis_exit_at_price(0.0)

    def ensure_carry_over_protective_stop(self) -> Optional[str]:
        return self._ensure_protective_stop_coverage_on_start()

    def _ensure_protective_stop_coverage_on_start(self) -> Optional[str]:
        if (
            not self._is_owner_thread()
            or self._thesis_invalidated
            or self._pending_thesis_exit
        ):
            return None
        target_inst = self._archetype_config.instrument_id
        signed_pos = self._open_position_signed_qty(target_inst)
        if signed_pos == Decimal("0"):
            return None
        abs_pos = abs(signed_pos)
        sym = self._symbol_from_instrument_id(target_inst)
        open_stops = self._get_open_protective_stop_orders(target_inst)
        if open_stops:
            adopted = open_stops[0]
            cid = str(getattr(adopted, "client_order_id", "") or "")
            venue_oid = str(getattr(adopted, "venue_order_id", "") or "") or None
            stop_px = float(self._stop_price_by_cid.get(cid) or self._stop_price or 0.0)
            if stop_px > 0.0:
                self._sync_plan_gate_stop_price(
                    stop_px,
                    instrument_id=target_inst,
                    covered_qty=abs_pos,
                )
                actor = self._resolve_telemetry_actor()
                oco_mon = getattr(actor, "oco_monitor", None) or getattr(actor, "_oco_monitor", None)
                if oco_mon is not None and hasattr(oco_mon, "register_protective_order"):
                    try:
                        oco_mon.register_protective_order(
                            symbol=sym,
                            covered_qty=abs_pos,
                            stop_price=Decimal(f"{stop_px:.4f}"),
                            broker_order_id=venue_oid or cid or None,
                            status="ACTIVE",
                            persist=bool(getattr(oco_mon, "_persist_to_db", False)),
                        )
                    except Exception:
                        pass
            return cid
        if not self._should_submit_protective_stop() or not self._is_trader_registered():
            return None
        eff_stop = self._current_tightest_stop_price()
        if (eff_stop is None or eff_stop <= Decimal("0")) and self._risk_envelope is not None:
            eff_stop = self._risk_envelope.initial_stop_price
        if eff_stop is None or eff_stop <= Decimal("0"):
            return None
        entry_side = OrderSide.BUY if signed_pos > Decimal("0") else OrderSide.SELL
        qty_obj = self._format_quantity(abs_pos, target_inst)
        new_cid = self._create_and_submit_protective_stop(
            instrument_id=target_inst,
            entry_side=entry_side,
            quantity=qty_obj,
            stop_loss_price=float(eff_stop),
            parent_entry_cid=f"carry-over-{sym}",
        )
        actor = self._resolve_telemetry_actor()
        oco_mon = getattr(actor, "oco_monitor", None) or getattr(actor, "_oco_monitor", None)
        if new_cid:
            if oco_mon is not None and hasattr(oco_mon, "register_protective_order"):
                try:
                    oco_mon.register_protective_order(
                        symbol=sym,
                        covered_qty=abs_pos,
                        stop_price=Decimal(f"{float(eff_stop):.4f}"),
                        broker_order_id=new_cid,
                        status="ACTIVE",
                        persist=bool(getattr(oco_mon, "_persist_to_db", False)),
                    )
                except Exception:
                    pass
        elif oco_mon is not None and hasattr(oco_mon, "invalidate_protective_order"):
            try:
                oco_mon.invalidate_protective_order(
                    symbol=sym,
                    reason=f"CARRY_OVER_PROTECTIVE_STOP_SUBMISSION_FAILED:{sym}",
                )
                if hasattr(oco_mon, "verify_position_coverage"):
                    oco_mon.verify_position_coverage(
                        symbol=sym,
                        pos_qty=abs_pos,
                        override_reason=f"CARRY_OVER_PROTECTIVE_STOP_SUBMISSION_FAILED:{sym}",
                    )
            except Exception:
                pass
        return new_cid

    def on_stop(self) -> None:
        if self._is_owner_thread():
            if hasattr(self, "unsubscribe_bars"):
                try:
                    self.unsubscribe_bars(self._archetype_config.bar_type)
                except Exception as exc:
                    logger.debug(
                        "Skipping unsubscribe_bars on_stop for %s: %s",
                        self._archetype_config.bar_type,
                        exc,
                    )
            if hasattr(self, "unsubscribe_quote_ticks"):
                try:
                    self.unsubscribe_quote_ticks(self.instrument_id)
                except Exception as exc:
                    logger.debug(
                        "Skipping unsubscribe_quote_ticks on_stop for %s: %s",
                        self.instrument_id,
                        exc,
                    )
            if hasattr(self, "unsubscribe_instrument_status"):
                try:
                    self.unsubscribe_instrument_status(self.instrument_id)
                except Exception as exc:
                    logger.debug(
                        "Skipping unsubscribe_instrument_status on_stop for %s: %s",
                        self.instrument_id,
                        exc,
                    )

    def on_quote_tick(self, tick: Any) -> None:
        if tick is None or self._in_warmup:
            return
        if self._thesis_invalidated and self._pending_thesis_exit:
            tick_ts = int(
                getattr(tick, "ts_event", 0) or getattr(tick, "ts_init", 0) or 0
            )
            if tick_ts == 0 or self._is_rth_open_ts(tick_ts):
                bid_px = self._extract_price_float(getattr(tick, "bid_price", None))
                ask_px = self._extract_price_float(getattr(tick, "ask_price", None))
                if bid_px > 0.0 and ask_px > 0.0:
                    ref_px = 0.5 * (bid_px + ask_px)
                else:
                    ref_px = max(bid_px, ask_px, 0.0)
                self._check_and_execute_pending_thesis_exit_at_price(ref_px)

    @staticmethod
    def _extract_price_float(raw_px: Any) -> float:
        if raw_px is None:
            return 0.0
        if hasattr(raw_px, "as_double"):
            try:
                return float(raw_px.as_double())
            except Exception:
                pass
        try:
            return float(raw_px)
        except Exception:
            return 0.0

    def _is_instrument_halted(
        self, instrument_id: Optional[InstrumentId] = None
    ) -> bool:
        if (
            instrument_id is None
            or str(instrument_id) == str(self._archetype_config.instrument_id)
        ):
            if self._instrument_halted:
                return True
        inst_key = str(instrument_id or self._archetype_config.instrument_id)
        return bool(self._halted_by_inst.get(inst_key, self._instrument_halted))

    def on_instrument_status(self, data: Any) -> None:
        if data is None:
            return
        inst_id = self._event_get(data, "instrument_id", None)
        if inst_id is not None:
            inst_str = str(inst_id)
            prim_str = str(self._archetype_config.instrument_id)
            prim_sym = self._symbol_from_instrument_id(
                self._archetype_config.instrument_id
            )
            inst_sym = self._symbol_from_instrument_id(inst_id)
            is_primary = inst_str == prim_str or inst_sym == prim_sym
        else:
            inst_str = str(self._archetype_config.instrument_id)
            is_primary = True

        raw_action = self._event_get(data, "action", None)
        if raw_action is None:
            raw_action = self._event_get(data, "status", None)

        action_name = ""
        action_int: Optional[int] = None
        if raw_action is not None:
            if isinstance(raw_action, int) and not isinstance(raw_action, bool):
                action_int = int(raw_action)
            else:
                try:
                    action_int = int(raw_action)
                except Exception:
                    action_int = None
            name_attr = getattr(raw_action, "name", None)
            if name_attr is not None:
                action_name = str(name_attr).strip().upper()
            else:
                s = str(raw_action).strip().upper()
                if "." in s:
                    s = s.rsplit(".", 1)[-1]
                action_name = s

        is_trading_flag = self._event_get(data, "is_trading", None)

        halted: Optional[bool] = None
        if action_name in (
            "HALT",
            "HALTED",
            "PAUSE",
            "PAUSED",
            "SUSPEND",
            "SUSPENDED",
            "NOT_AVAILABLE_FOR_TRADING",
            "NOT_AVAILABLE",
            "TRADING_HALT",
            "CLOSE",
            "CLOSED",
        ) or action_int in (8, 9, 10, 12, 15):
            halted = True
        elif action_name in (
            "TRADING",
            "OPEN",
            "RESUME",
            "RESUMED",
            "NORMAL",
            "ACTIVE",
        ) or action_int == 7:
            halted = False

        if isinstance(is_trading_flag, bool):
            if not is_trading_flag:
                halted = True
            elif halted is None and is_trading_flag:
                halted = False

        if halted is not None:
            self._halted_by_inst[inst_str] = halted
            if is_primary:
                self._instrument_halted = halted

    def _check_and_execute_take_profit_or_corrected_stop(
        self,
        bar: Bar,
        *,
        include_stop_check: bool = False,
    ) -> bool:
        if self._in_warmup:
            return False
        target_inst = (
            bar.bar_type.instrument_id
            if hasattr(bar, "bar_type") and hasattr(bar.bar_type, "instrument_id")
            else self._archetype_config.instrument_id
        )
        signed_pos = self._open_position_signed_qty(target_inst)
        pos_qty = float(abs(signed_pos)) if signed_pos != Decimal("0") else self._current_position_qty(target_inst)
        if pos_qty <= 0.0:
            return False
        is_long = signed_pos > Decimal("0") if signed_pos != Decimal("0") else (self._archetype_config.plan_side == PlanSide.LONG)
        close_px = float(bar.close.as_double())
        raw_high_px = self._current_bar_raw_high(float(bar.high.as_double()))
        raw_low_px = self._current_bar_raw_low(float(bar.low.as_double()))

        if include_stop_check and self._trailing_stop_price is not None and self._trailing_stop_price > 0.0:
            stop_hit = (raw_low_px <= self._trailing_stop_price) if is_long else (raw_high_px >= self._trailing_stop_price)
            if stop_hit:
                exit_side = OrderSide.SELL if is_long else OrderSide.BUY
                self._submit_plan_gated_exit(
                    order_side=exit_side,
                    reference_price=close_px,
                    quantity=pos_qty,
                    instrument_id=target_inst,
                    reason_tag="CORRECTED_BAR_STOP_EXIT",
                )
                return True

        env = self._risk_envelope or getattr(self._archetype_config, "_plan_risk_envelope", None)
        tp_dec = getattr(env, "take_profit_price", None) if env is not None else None
        if tp_dec is not None and Decimal(str(tp_dec)) > Decimal("0"):
            tp_px = float(tp_dec)
            tp_hit = (raw_high_px >= tp_px) if is_long else (raw_low_px <= tp_px)
            if tp_hit:
                exit_side = OrderSide.SELL if is_long else OrderSide.BUY
                self._submit_plan_gated_exit(
                    order_side=exit_side,
                    reference_price=close_px,
                    quantity=pos_qty,
                    instrument_id=target_inst,
                    reason_tag="TAKE_PROFIT_REACHED",
                )
                return True
        return False

    def on_bar(self, bar: Bar) -> None:
        if hasattr(bar, "bar_type") and bar.bar_type != self._archetype_config.bar_type:
            return
        if hasattr(bar, "high") and hasattr(bar, "low") and hasattr(bar, "close"):
            if not self._record_primary_bar(bar):
                raw_ts_event = int(getattr(bar, "ts_event", 0) or 0)
                raw_ts_init = int(getattr(bar, "ts_init", 0) or 0)
                bar_ts = raw_ts_event if raw_ts_event > 0 else raw_ts_init
                if bar_ts > 0 and bar_ts == self._last_bar_ts_ns:
                    self._check_and_execute_take_profit_or_corrected_stop(
                        bar, include_stop_check=True
                    )
                return
            if self._in_warmup or self._check_and_execute_pending_thesis_exit(bar):
                return
            if self._check_and_execute_take_profit_or_corrected_stop(
                bar, include_stop_check=False
            ):
                return
        target_inst = (
            bar.bar_type.instrument_id
            if hasattr(bar, "bar_type") and hasattr(bar.bar_type, "instrument_id")
            else self._archetype_config.instrument_id
        )
        if (
            self._is_instrument_halted(target_inst)
            and self._open_position_signed_qty(target_inst) == Decimal("0")
            and self._current_position_qty(target_inst) <= 0.0
        ):
            return
        eval_fn = getattr(self, "evaluate_bar", None)
        if callable(eval_fn):
            eval_fn(bar)

    def _resolve_entry_limit_price(
        self,
        order_side: OrderSide,
        reference_price: float,
        atr: float,
        limit_price: Optional[float] = None,
    ) -> float:
        """Resolves the marketable limit entry price bounded by `MarketableLimitCollar`."""
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
            else default_limit_collar_for_archetype(self._archetype_config.ARCHETYPE_ID)
        )
        worst_limit_dec = collar.compute_worst_limit_price(
            plan_side,
            ref_dec,
            atr_dec,
        )
        worst_limit = float(worst_limit_dec)
        if (
            limit_price is not None
            and float(limit_price) > 0.0
            and abs(float(limit_price) - float(reference_price)) > 1e-9
        ):
            req_limit = float(limit_price)
            if plan_side == PlanSide.LONG:
                return min(req_limit, worst_limit)
            return max(req_limit, worst_limit)
        return worst_limit

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
        raw_ts_event = int(getattr(bar, "ts_event", 0) or 0)
        raw_ts_init = int(getattr(bar, "ts_init", 0) or 0)
        bar_ts = raw_ts_event if raw_ts_event > 0 else raw_ts_init
        bar_high = float(bar.high.as_double())
        bar_low = float(bar.low.as_double())
        cleaned_pair = self._cleaned_bar_extremes_by_ts_ns.get(bar_ts)
        if cleaned_pair is not None:
            cleaned_high, cleaned_low = float(cleaned_pair[0]), float(cleaned_pair[1])
        else:
            attr_ch = getattr(bar, "cleaned_high", None)
            attr_cl = getattr(bar, "cleaned_low", None)
            cleaned_high = float(attr_ch) if attr_ch is not None else bar_high
            cleaned_low = float(attr_cl) if attr_cl is not None else bar_low

        raw_pair = self._raw_bar_extremes_by_ts_ns.get(bar_ts)
        if raw_pair is not None:
            raw_high, raw_low = float(raw_pair[0]), float(raw_pair[1])
        else:
            attr_rh = getattr(bar, "raw_high", None)
            attr_rl = getattr(bar, "raw_low", None)
            raw_high = (
                max(bar_high, float(attr_rh))
                if attr_rh is not None
                else max(bar_high, cleaned_high)
            )
            raw_low = (
                min(bar_low, float(attr_rl))
                if attr_rl is not None
                else min(bar_low, cleaned_low)
            )

        close_val = float(bar.close.as_double())
        vol_val = float(bar.volume.as_double())

        if bar_ts > 0 and self._last_bar_ts_ns > 0 and bar_ts <= self._last_bar_ts_ns:
            if (
                len(self._bar_ts_ns_window) == len(self._closes)
                and bar_ts in self._bar_ts_ns_window
            ):
                rev_idx = (
                    len(self._bar_ts_ns_window)
                    - 1
                    - self._bar_ts_ns_window[::-1].index(bar_ts)
                )
                self._highs[rev_idx] = cleaned_high
                self._lows[rev_idx] = cleaned_low
                self._raw_highs[rev_idx] = raw_high
                self._raw_lows[rev_idx] = raw_low
                self._closes[rev_idx] = close_val
                self._volumes[rev_idx] = vol_val
            return False
        if bar_ts > 0:
            self._last_bar_ts_ns = bar_ts
            if self._standalone_clock is not None:
                try:
                    self._standalone_clock.set_time(bar_ts)
                except Exception:
                    pass

        self._highs.append(cleaned_high)
        self._lows.append(cleaned_low)
        self._raw_highs.append(raw_high)
        self._raw_lows.append(raw_low)
        self._closes.append(close_val)
        self._volumes.append(vol_val)
        self._bar_ts_ns_window.append(bar_ts)
        max_keep = max(self._archetype_config.lookback_window * 3, 120)
        if len(self._closes) > max_keep:
            del self._highs[:-max_keep]
            del self._lows[:-max_keep]
            del self._raw_highs[:-max_keep]
            del self._raw_lows[:-max_keep]
            del self._closes[:-max_keep]
            del self._volumes[:-max_keep]
            del self._bar_ts_ns_window[:-max_keep]
        if not self._in_warmup:
            self._post_warmup_bar_seq += 1
            self._expire_stale_unfilled_entry_orders()
            self._flush_delayed_orders(bar)
        return True

    def _expire_stale_unfilled_entry_orders(self) -> None:
        if not self._active_entry_orders_by_inst:
            return
        valid_until_ns: Optional[int] = None
        if (
            self._risk_envelope is not None
            and getattr(self._risk_envelope, "valid_until", None) is not None
        ):
            vu = self._risk_envelope.valid_until
            if isinstance(vu, datetime):
                vu_utc = vu if vu.tzinfo is not None else vu.replace(tzinfo=timezone.utc)
                valid_until_ns = int(vu_utc.timestamp() * 1_000_000_000)
            elif isinstance(vu, (int, float)):
                valid_until_ns = int(vu)
        cfg_ttl = getattr(self._archetype_config, "entry_ttl_bars", None)
        if cfg_ttl is None:
            cfg_ttl = getattr(self, "_entry_ttl_bars", None)

        for inst_key, entry_spec in list(self._active_entry_orders_by_inst.items()):
            if entry_spec.get("canceling"):
                continue
            submitted_seq = int(entry_spec.get("submitted_bar_seq", 0))
            if submitted_seq >= self._post_warmup_bar_seq:
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

            raw_leaves = getattr(order_obj, "leaves_qty", None)
            total_qty_dec = _to_decimal(getattr(order_obj, "quantity", 0), "0")
            filled_qty_dec = _to_decimal(getattr(order_obj, "filled_qty", 0), "0")
            if raw_leaves is not None:
                leaves_qty_dec = _to_decimal(
                    raw_leaves.as_decimal()
                    if hasattr(raw_leaves, "as_decimal")
                    else raw_leaves,
                    "0",
                )
            elif total_qty_dec > Decimal("0"):
                leaves_qty_dec = max(Decimal("0"), total_qty_dec - filled_qty_dec)
            else:
                leaves_qty_dec = Decimal("0")

            if leaves_qty_dec <= Decimal("0") and (
                raw_leaves is not None
                or total_qty_dec > Decimal("0")
                or filled_qty_dec > Decimal("0")
            ):
                self._active_entry_orders_by_inst.pop(inst_key, None)
                continue

            spec_ttl = entry_spec.get("entry_ttl_bars", cfg_ttl)
            expired_by_ttl = (
                spec_ttl is not None
                and int(spec_ttl) > 0
                and self._post_warmup_bar_seq >= (submitted_seq + int(spec_ttl))
            )
            expired_by_valid_until = (
                valid_until_ns is not None
                and valid_until_ns > 0
                and self._last_bar_ts_ns > valid_until_ns
            )
            if not (expired_by_ttl or expired_by_valid_until):
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

    def _format_order_qty(
        self, raw_qty: Decimal | float, instrument_id: Optional[InstrumentId] = None
    ) -> str:
        qty_obj = self._format_quantity(raw_qty, instrument_id)
        return str(qty_obj)

    def _current_position_qty(
        self, instrument_id: Optional[InstrumentId] = None
    ) -> float:
        target_inst = instrument_id or self._archetype_config.instrument_id
        signed_qty = abs(self._open_position_signed_qty(target_inst))
        if signed_qty > Decimal("0"):
            return float(signed_qty)
        if (
            instrument_id is None
            or str(instrument_id) == str(self._archetype_config.instrument_id)
        ) and self._entry_filled_qty > 0.0:
            return float(self._entry_filled_qty)
        return 0.0

    def _sync_plan_gate_stop_price(
        self,
        stop_price: float,
        instrument_id: Optional[InstrumentId] = None,
        covered_qty: Optional[Decimal | float] = None,
    ) -> None:
        if stop_price <= 0.0:
            return
        target_inst = instrument_id or self._archetype_config.instrument_id
        sym = self._symbol_from_instrument_id(target_inst)
        actor = self._resolve_telemetry_actor()
        if actor is not None and hasattr(actor, "oco_monitor"):
            try:
                if hasattr(actor.oco_monitor, "update_stop_price"):
                    actor.oco_monitor.update_stop_price(
                        symbol=sym, stop_price=Decimal(f"{float(stop_price):.4f}")
                    )
                elif hasattr(actor.oco_monitor, "on_stop_leg_replaced"):
                    cid_str = str(getattr(self._protective_stop_order_id, "value", self._protective_stop_order_id) or "")
                    qty_dec = (
                        _to_decimal(covered_qty, "0")
                        if covered_qty is not None
                        else Decimal(f"{max(0.0, float(self._protective_stop_qty)):.6f}")
                    )
                    if cid_str and qty_dec > Decimal("0"):
                        actor.oco_monitor.on_stop_leg_replaced(
                            symbol=sym,
                            old_broker_order_id=cid_str,
                            new_broker_order_id=cid_str,
                            new_covered_qty=qty_dec,
                            stop_price=Decimal(f"{float(stop_price):.4f}"),
                        )
            except Exception as exc:
                logger.debug("OCO stop price sync failed for %s: %s", sym, exc)
        if self._plan_gate_ref is not None and hasattr(
            self._plan_gate_ref, "update_stop_price"
        ):
            try:
                self._plan_gate_ref.update_stop_price(sym, float(stop_price))
            except Exception:
                pass

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

    def _lookup_order_in_cache(self, cid_or_order: Any) -> Optional[Any]:
        if cid_or_order is None:
            return None
        cid_str = str(getattr(cid_or_order, "client_order_id", cid_or_order) or "")
        if not cid_str:
            return None
        cache_obj = getattr(self, "cache", None)
        if cache_obj is not None and hasattr(cache_obj, "order"):
            for key in (
                getattr(cid_or_order, "client_order_id", None),
                ClientOrderId(cid_str),
                cid_str,
            ):
                if key is None:
                    continue
                try:
                    found = cache_obj.order(key)
                    if found is not None:
                        return found
                except Exception:
                    continue
        return None

    def _is_stop_order_unconfirmed_submitted(self, order: Any) -> bool:
        if order is None:
            return False
        cid = str(getattr(order, "client_order_id", "") or "")
        status_str = self._normalized_order_status(order)
        venue_id = getattr(order, "venue_order_id", None) or self._venue_order_id_by_cid.get(cid)
        if status_str in ("PENDING_CANCEL", "PENDING_UPDATE"):
            return True
        if status_str == "SUBMITTED":
            return True
        if (
            status_str == "INITIALIZED"
            and venue_id is None
            and cid not in self._accepted_order_ids
        ):
            if self._is_trader_registered():
                return True
            if (
                cid in self._unconfirmed_stop_cids
                and self._stop_submitted_bar_seq_by_cid.get(cid) == self._post_warmup_bar_seq
            ):
                return True
        return False

    def modify_order(
        self,
        order_or_client_order_id: Any,
        quantity: Optional[Quantity] = None,
        price: Optional[Price] = None,
        trigger_price: Optional[Price] = None,
        client_id: Optional[Any] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> None:
        cid_str = str(
            getattr(
                order_or_client_order_id,
                "client_order_id",
                order_or_client_order_id,
            )
            or ""
        )
        self._modified_orders_audit.append(
            {
                "order": order_or_client_order_id,
                "client_order_id": cid_str,
                "quantity": quantity,
                "price": price,
                "trigger_price": trigger_price,
                "params": params,
            }
        )
        if not self._is_owner_thread() or not self._is_trader_registered():
            return
        cid_obj = (
            order_or_client_order_id.client_order_id
            if hasattr(order_or_client_order_id, "client_order_id")
            and isinstance(
                getattr(order_or_client_order_id, "client_order_id", None),
                ClientOrderId,
            )
            else ClientOrderId(cid_str)
        )
        try:
            super().modify_order(
                cid_obj,
                quantity=quantity,
                price=price,
                trigger_price=trigger_price,
                client_id=client_id,
                params=params,
            )
        except TypeError:
            try:
                super().modify_order(
                    order_or_client_order_id,
                    quantity=quantity,
                    price=price,
                    trigger_price=trigger_price,
                )
            except Exception as exc:
                logger.warning("modify_order fallback failed for %s: %s", cid_str, exc)
        except Exception as exc:
            logger.warning("modify_order failed for %s: %s", cid_str, exc)

    def _apply_atomic_protective_stop_modify(
        self,
        existing_stop: Any,
        *,
        target_qty: Decimal | float,
        target_stop_px: float,
        instrument_id: Optional[InstrumentId] = None,
    ) -> str:
        target_inst = instrument_id or self._archetype_config.instrument_id
        existing_cid = str(
            getattr(
                existing_stop,
                "client_order_id",
                self._protective_stop_order_id or "",
            )
            or ""
        )
        qty_str = self._format_order_qty(target_qty, target_inst)
        qty_obj = Quantity.from_str(qty_str)
        rounded_stop_px = round(float(target_stop_px), 2)
        trig_obj = Price.from_str(f"{rounded_stop_px:.2f}")
        qty_dec = _to_decimal(qty_str, "0")
        if (
            existing_cid
            and existing_cid in self._stop_price_by_cid
            and abs(rounded_stop_px - round(float(self._stop_price_by_cid[existing_cid]), 2)) < 0.01
            and abs(float(qty_dec) - float(self._stop_qty_by_cid.get(existing_cid, Decimal("0")))) <= 1e-6
        ):
            self._protective_stop_qty = float(target_qty)
            self._stop_price = rounded_stop_px
            return existing_cid
        self.modify_order(
            existing_stop,
            quantity=qty_obj,
            trigger_price=trig_obj,
        )
        if existing_cid:
            prev_qty = self._stop_qty_by_cid.get(existing_cid)
            if prev_qty is not None:
                self._prior_stop_qty_by_cid[existing_cid] = prev_qty
            self._stop_qty_by_cid[existing_cid] = qty_dec
            self._stop_price_by_cid[existing_cid] = rounded_stop_px
            self._stop_orders_by_cid[existing_cid] = existing_stop
            if getattr(self._protective_stop_order_id, "value", None) != existing_cid:
                self._protective_stop_order_id = getattr(
                    existing_stop, "client_order_id", ClientOrderId(existing_cid)
                )
        self._protective_stop_qty = float(target_qty)
        self._stop_price = rounded_stop_px
        if existing_cid:
            sym = self._symbol_from_instrument_id(target_inst)
            self._notify_stop_leg_replaced(
                sym,
                existing_cid,
                existing_cid,
                new_covered_qty=qty_dec,
                stop_price=float(target_stop_px),
                status="ACTIVE",
            )
        self._sync_plan_gate_stop_price(
            float(target_stop_px),
            instrument_id=target_inst,
            covered_qty=qty_dec,
        )
        return existing_cid

    def _is_protective_stop_order(
        self, order: Any, active_set: Optional[set[str]] = None
    ) -> bool:
        cid = str(getattr(order, "client_order_id", "") or "")
        if active_set and cid in active_set:
            return True
        if cid.startswith("prot_"):
            return True
        if self._protective_stop_order_id is not None and cid == str(
            getattr(self._protective_stop_order_id, "value", self._protective_stop_order_id)
        ):
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
        if "STOP" in order_type_str:
            if bool(getattr(order, "is_reduce_only", False)):
                return True
            order_side_str = str(
                getattr(order, "side", getattr(order, "order_side", "")) or ""
            ).upper()
            order_inst = (
                getattr(order, "instrument_id", None)
                or self._archetype_config.instrument_id
            )
            pos_signed = self._open_position_signed_qty(order_inst)
            if pos_signed > Decimal("0") and "SELL" in order_side_str:
                return True
            if pos_signed < Decimal("0") and "BUY" in order_side_str:
                return True
            plan_side = getattr(self._archetype_config, "plan_side", PlanSide.LONG)
            if plan_side == PlanSide.LONG and "SELL" in order_side_str:
                return True
            if plan_side == PlanSide.SHORT and "BUY" in order_side_str:
                return True
        return False

    def _get_open_protective_stop_orders(
        self, instrument_id: Optional[InstrumentId] = None
    ) -> List[Any]:
        target_inst = instrument_id or self._archetype_config.instrument_id
        inst_key = str(target_inst)
        active_cids = list(self._active_protective_stops_by_inst.get(inst_key, []))
        if (
            instrument_id is None
            or str(instrument_id) == str(self._archetype_config.instrument_id)
        ) and self._protective_stop_order_id is not None:
            primary_stop_cid = str(
                getattr(
                    self._protective_stop_order_id,
                    "value",
                    self._protective_stop_order_id,
                )
                or ""
            )
            if primary_stop_cid and primary_stop_cid not in active_cids:
                active_cids.append(primary_stop_cid)
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
                raw_trig = getattr(order, "trigger_price", None)
                trig_px = self._extract_price_float(raw_trig)
                if trig_px > 0.0:
                    self._stop_price_by_cid[cid] = trig_px
                    if self._stop_price <= 0.0:
                        self._stop_price = trig_px
                    plan_side = getattr(self._archetype_config, "plan_side", PlanSide.LONG)
                    if self._trailing_stop_price is not None and self._trailing_stop_price > 0.0:
                        if plan_side == PlanSide.LONG and trig_px > self._trailing_stop_price:
                            self._trailing_stop_price = trig_px
                            self._stop_price = trig_px
                        elif plan_side == PlanSide.SHORT and trig_px < self._trailing_stop_price:
                            self._trailing_stop_price = trig_px
                            self._stop_price = trig_px
                    else:
                        self._trailing_stop_price = trig_px
                raw_leaves = getattr(order, "leaves_qty", None) or getattr(order, "quantity", None)
                if raw_leaves is not None and cid not in self._stop_qty_by_cid:
                    qty_dec = _to_decimal(
                        raw_leaves.as_decimal()
                        if hasattr(raw_leaves, "as_decimal")
                        else raw_leaves,
                        "0",
                    )
                    if qty_dec > Decimal("0"):
                        self._stop_qty_by_cid[cid] = qty_dec
                        if self._protective_stop_qty <= 0.0:
                            self._protective_stop_qty = float(qty_dec)
                if self._protective_stop_order_id is None:
                    self._protective_stop_order_id = getattr(
                        order, "client_order_id", ClientOrderId(cid)
                    )

        for cid in active_cids:
            if cid in seen_cids or cid in self._canceled_order_ids_audit:
                continue
            cached_order = self._lookup_order_in_cache(
                self._stop_orders_by_cid.get(cid) or cid
            )
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
        self._stop_price_by_cid.pop(cid, None)
        self._unconfirmed_stop_cids.discard(cid)
        self._stop_submitted_bar_seq_by_cid.pop(cid, None)
        if self._protective_stop_order_id is not None and cid == str(
            getattr(self._protective_stop_order_id, "value", self._protective_stop_order_id)
        ):
            self._protective_stop_order_id = None
            self._protective_stop_qty = 0.0
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
        stop_loss_price: Optional[float] = None,
    ) -> str:
        stop_cid = str(stop_order.client_order_id)
        stop_side = stop_order.side
        qty_dec = (
            Decimal(str(quantity.as_decimal()))
            if hasattr(quantity, "as_decimal")
            else _to_decimal(quantity, "0")
        )
        trig_px = getattr(stop_order, "trigger_price", None)
        resolved_stop_px = (
            float(stop_loss_price)
            if stop_loss_price is not None and float(stop_loss_price) > 0.0
            else (
                float(trig_px.as_double())
                if trig_px is not None and hasattr(trig_px, "as_double")
                else float(_to_decimal(trig_px, "0"))
            )
        )
        self._submitted_order_ids.append(stop_cid)
        self._protective_stop_order_ids.append(stop_cid)
        self._stop_orders_by_cid[stop_cid] = stop_order
        self._stop_qty_by_cid[stop_cid] = qty_dec
        if resolved_stop_px > 0.0:
            self._stop_price_by_cid[stop_cid] = resolved_stop_px
            self._stop_price = resolved_stop_px
        self._protective_stop_order_id = getattr(
            stop_order, "client_order_id", ClientOrderId(stop_cid)
        )
        self._protective_stop_qty = float(qty_dec)
        self._unconfirmed_stop_cids.add(stop_cid)
        self._stop_submitted_bar_seq_by_cid[stop_cid] = self._post_warmup_bar_seq
        inst_key = str(instrument_id)
        self._order_side_by_cid[stop_cid] = (inst_key, stop_side)
        active_list = self._active_protective_stops_by_inst.setdefault(inst_key, [])
        if stop_cid not in active_list:
            active_list.append(stop_cid)
        self._submit_order_safely(stop_order)
        if resolved_stop_px > 0.0:
            self._sync_plan_gate_stop_price(
                resolved_stop_px,
                instrument_id=instrument_id,
                covered_qty=qty_dec,
            )
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
            stop_loss_price=stop_loss_price,
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

        target_stop_px = float(spec.get("stop_loss_price") or self._stop_price or 0.0)
        if self._trailing_stop_price is not None and self._trailing_stop_price > 0.0:
            if self._archetype_config.plan_side == PlanSide.LONG:
                target_stop_px = max(target_stop_px, float(self._trailing_stop_price))
            else:
                target_stop_px = (
                    min(target_stop_px, float(self._trailing_stop_price))
                    if target_stop_px > 0.0
                    else float(self._trailing_stop_price)
                )

        open_stops = self._get_open_protective_stop_orders(inst_id)
        if len(open_stops) == 1:
            existing_stop = open_stops[0]
            existing_cid = str(getattr(existing_stop, "client_order_id", "") or "")
            existing_qty_dec = self._stop_qty_by_cid.get(
                existing_cid,
                _to_decimal(getattr(existing_stop, "quantity", 0), "0"),
            )
            existing_px = self._stop_price_by_cid.get(existing_cid, 0.0)
            if existing_px <= 0.0:
                trig_px = getattr(existing_stop, "trigger_price", None)
                if trig_px is not None:
                    existing_px = (
                        float(trig_px.as_double())
                        if hasattr(trig_px, "as_double")
                        else float(_to_decimal(trig_px, "0"))
                    )
            if existing_qty_dec >= desired_stop_qty and (
                target_stop_px <= 0.0
                or existing_px <= 0.0
                or abs(existing_px - target_stop_px) < 1e-6
            ):
                return existing_cid

            eff_stop_px = target_stop_px if target_stop_px > 0.0 else existing_px
            if self._is_stop_order_unconfirmed_submitted(existing_stop):
                pending_qty = (
                    max(
                        float(desired_stop_qty),
                        float(self._pending_protective_stop_sync[0]),
                    )
                    if self._pending_protective_stop_sync is not None
                    else float(desired_stop_qty)
                )
                self._pending_protective_stop_sync = (
                    pending_qty,
                    float(eff_stop_px),
                )
                if eff_stop_px > 0.0:
                    self._stop_price = float(eff_stop_px)
                    if existing_cid:
                        self._notify_stop_leg_replaced(
                            sym,
                            existing_cid,
                            f"deferred-replace-{existing_cid}",
                            new_covered_qty=Decimal(f"{pending_qty:.6f}"),
                            stop_price=float(eff_stop_px),
                            status="REPLACING",
                        )
                return existing_cid

            return self._apply_atomic_protective_stop_modify(
                existing_stop,
                target_qty=desired_stop_qty,
                target_stop_px=eff_stop_px,
                instrument_id=inst_id,
            )

        stop_qty_obj = self._format_quantity(desired_stop_qty, inst_id)
        try:
            replacement_stop = self._build_protective_stop_order(
                instrument_id=inst_id,
                entry_side=spec["entry_side"],
                quantity=stop_qty_obj,
                stop_loss_price=target_stop_px,
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
                    stop_price=target_stop_px,
                )
                self._remove_tracked_stop_cid(old_cid, inst_id)
                self._cancel_single_order_safely(old_stop)

            new_stop_cid = (
                self._register_and_submit_protective_stop_order(
                    replacement_stop,
                    inst_id,
                    stop_qty_obj,
                    stop_loss_price=target_stop_px,
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
                cached_stop = self._lookup_order_in_cache(
                    self._stop_orders_by_cid.get(cid) or cid
                )
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

    def on_order_accepted(self, event: Any) -> None:
        cid = str(self._event_get(event, "client_order_id", "") or "")
        if not cid:
            return
        self._accepted_order_ids.add(cid)
        self._unconfirmed_stop_cids.discard(cid)
        venue_id = self._event_get(event, "venue_order_id", None)
        if venue_id is not None:
            self._venue_order_id_by_cid[cid] = venue_id
            tracked_stop = self._stop_orders_by_cid.get(cid)
            if tracked_stop is not None and getattr(tracked_stop, "venue_order_id", None) is None:
                try:
                    setattr(tracked_stop, "venue_order_id", venue_id)
                except Exception:
                    pass

        primary_stop_cid = (
            str(
                getattr(
                    self._protective_stop_order_id,
                    "value",
                    self._protective_stop_order_id,
                )
                or ""
            )
            if self._protective_stop_order_id is not None
            else ""
        )
        is_protective_stop = (
            cid == primary_stop_cid
            or cid in self._stop_orders_by_cid
            or any(
                cid in cids
                for cids in self._active_protective_stops_by_inst.values()
            )
        )
        if not is_protective_stop:
            return

        order_obj = (
            self._lookup_order_in_cache(self._stop_orders_by_cid.get(cid) or cid)
            or self._stop_orders_by_cid.get(cid)
        )
        t_meta = self._order_side_by_cid.get(cid)
        target_inst = (
            InstrumentId.from_str(t_meta[0])
            if t_meta is not None
            else self._archetype_config.instrument_id
        )

        pending_sync = self._pending_protective_stop_sync
        self._pending_protective_stop_sync = None
        if pending_sync is not None:
            target_qty, target_stop_px = pending_sync
        else:
            pos_qty = self._current_position_qty(target_inst)
            stop_qty = float(
                self._stop_qty_by_cid.get(cid, Decimal(f"{self._protective_stop_qty:.6f}"))
            )
            fallback_stop = (
                float(self._stop_price)
                if self._stop_price is not None and self._stop_price > 0.0
                else 0.0
            )
            stop_px = float(self._stop_price_by_cid.get(cid, fallback_stop) or 0.0)
            desired_px = fallback_stop if fallback_stop > 0.0 else stop_px
            if self._trailing_stop_price is not None and self._trailing_stop_price > 0.0:
                if self._archetype_config.plan_side == PlanSide.LONG:
                    desired_px = max(desired_px, float(self._trailing_stop_price))
                else:
                    desired_px = (
                        min(desired_px, float(self._trailing_stop_price))
                        if desired_px > 0.0
                        else float(self._trailing_stop_price)
                    )
            qty_differs = pos_qty > 0.0 and abs(pos_qty - stop_qty) > 1e-6
            px_differs = (
                desired_px > 0.0
                and stop_px > 0.0
                and abs(round(desired_px, 2) - round(stop_px, 2)) >= 0.01
            )
            if not qty_differs and not px_differs:
                return
            target_qty = pos_qty if pos_qty > 0.0 else stop_qty
            target_stop_px = desired_px if desired_px > 0.0 else stop_px

        if target_qty > 0.0 and target_stop_px > 0.0 and order_obj is not None:
            self._apply_atomic_protective_stop_modify(
                order_obj,
                target_qty=target_qty,
                target_stop_px=target_stop_px,
                instrument_id=target_inst,
            )

    def on_order_updated(self, event: Any) -> None:
        cid = str(self._event_get(event, "client_order_id", "") or "")
        if not cid:
            return
        self._unconfirmed_stop_cids.discard(cid)
        self._prior_stop_qty_by_cid.pop(cid, None)
        venue_id = self._event_get(event, "venue_order_id", None)
        if venue_id is not None:
            self._venue_order_id_by_cid[cid] = venue_id
            tracked_stop = self._stop_orders_by_cid.get(cid)
            if tracked_stop is not None and getattr(tracked_stop, "venue_order_id", None) is None:
                try:
                    setattr(tracked_stop, "venue_order_id", venue_id)
                except Exception:
                    pass

        primary_stop_cid = (
            str(
                getattr(
                    self._protective_stop_order_id,
                    "value",
                    self._protective_stop_order_id,
                )
                or ""
            )
            if self._protective_stop_order_id is not None
            else ""
        )
        is_protective_stop = (
            cid == primary_stop_cid
            or cid in self._stop_orders_by_cid
            or cid in self._stop_qty_by_cid
            or any(
                cid in cids
                for cids in self._active_protective_stops_by_inst.values()
            )
        )
        qty_ev = self._event_get(event, "quantity", None)
        if qty_ev is not None and is_protective_stop:
            qty_dec = (
                Decimal(str(qty_ev.as_decimal()))
                if hasattr(qty_ev, "as_decimal")
                else _to_decimal(qty_ev, "0")
            )
            if qty_dec > Decimal("0"):
                self._stop_qty_by_cid[cid] = qty_dec
                self._protective_stop_qty = float(qty_dec)
        trig_ev = self._event_get(event, "trigger_price", None)
        if trig_ev is not None and is_protective_stop:
            trig_val = (
                float(trig_ev.as_double())
                if hasattr(trig_ev, "as_double")
                else float(_to_decimal(trig_ev, "0"))
            )
            if trig_val > 0.0:
                self._stop_price_by_cid[cid] = trig_val
                self._stop_price = trig_val

        if not is_protective_stop:
            return

        t_meta = self._order_side_by_cid.get(cid)
        target_inst = (
            InstrumentId.from_str(t_meta[0])
            if t_meta is not None
            else self._archetype_config.instrument_id
        )
        confirmed_qty = self._stop_qty_by_cid.get(cid, Decimal("0"))
        confirmed_px = float(self._stop_price_by_cid.get(cid, self._stop_price or 0.0) or 0.0)

        order_obj = (
            self._lookup_order_in_cache(self._stop_orders_by_cid.get(cid) or cid)
            or self._stop_orders_by_cid.get(cid)
        )
        safe_exit_spec = self._pending_safe_exits.get(str(target_inst))
        if safe_exit_spec is not None and cid in safe_exit_spec.get("pending_stop_cids", set()):
            self._cancel_single_order_safely(order_obj if order_obj is not None else ClientOrderId(cid))
            return

        if order_obj is not None and self._is_stop_order_unconfirmed_submitted(order_obj):
            if confirmed_px > 0.0 and confirmed_qty > Decimal("0"):
                self._sync_plan_gate_stop_price(
                    confirmed_px,
                    instrument_id=target_inst,
                    covered_qty=confirmed_qty,
                )
            return

        pending_sync = self._pending_protective_stop_sync
        self._pending_protective_stop_sync = None
        if pending_sync is not None:
            target_qty, target_stop_px = pending_sync
        else:
            pos_qty = self._current_position_qty(target_inst)
            stop_qty = float(
                self._stop_qty_by_cid.get(cid, Decimal(f"{self._protective_stop_qty:.6f}"))
            )
            fallback_stop = (
                float(self._stop_price)
                if self._stop_price is not None and self._stop_price > 0.0
                else 0.0
            )
            stop_px = float(self._stop_price_by_cid.get(cid, fallback_stop) or 0.0)
            desired_px = fallback_stop if fallback_stop > 0.0 else stop_px
            if self._trailing_stop_price is not None and self._trailing_stop_price > 0.0:
                if self._archetype_config.plan_side == PlanSide.LONG:
                    desired_px = max(desired_px, float(self._trailing_stop_price))
                else:
                    desired_px = (
                        min(desired_px, float(self._trailing_stop_price))
                        if desired_px > 0.0
                        else float(self._trailing_stop_price)
                    )
            qty_differs = pos_qty > 0.0 and abs(pos_qty - stop_qty) > 1e-6
            px_differs = (
                desired_px > 0.0
                and stop_px > 0.0
                and abs(round(desired_px, 2) - round(stop_px, 2)) >= 0.01
            )
            if not qty_differs and not px_differs:
                if confirmed_px > 0.0 and confirmed_qty > Decimal("0"):
                    sym = self._symbol_from_instrument_id(target_inst)
                    self._notify_stop_leg_replaced(
                        sym,
                        cid,
                        cid,
                        new_covered_qty=confirmed_qty,
                        stop_price=Decimal(f"{confirmed_px:.4f}"),
                    )
                    self._sync_plan_gate_stop_price(
                        confirmed_px,
                        instrument_id=target_inst,
                        covered_qty=confirmed_qty,
                    )
                return
            target_qty = pos_qty if pos_qty > 0.0 else stop_qty
            target_stop_px = desired_px if desired_px > 0.0 else stop_px

        current_stop_qty = float(self._stop_qty_by_cid.get(cid, Decimal("0")))
        current_stop_px = float(self._stop_price_by_cid.get(cid, 0.0) or 0.0)
        if (
            target_qty > 0.0
            and target_stop_px > 0.0
            and order_obj is not None
            and (
                abs(target_qty - current_stop_qty) > 1e-6
                or (current_stop_px > 0.0 and abs(round(target_stop_px, 2) - round(current_stop_px, 2)) >= 0.01)
            )
        ):
            self._apply_atomic_protective_stop_modify(
                order_obj,
                target_qty=target_qty,
                target_stop_px=target_stop_px,
                instrument_id=target_inst,
            )
        elif confirmed_px > 0.0 and confirmed_qty > Decimal("0"):
            sym = self._symbol_from_instrument_id(target_inst)
            self._notify_stop_leg_replaced(
                sym,
                cid,
                cid,
                new_covered_qty=confirmed_qty,
                stop_price=Decimal(f"{confirmed_px:.4f}"),
            )
            self._sync_plan_gate_stop_price(
                confirmed_px,
                instrument_id=target_inst,
                covered_qty=confirmed_qty,
            )

    def _resolve_latest_market_price(
        self, instrument_id: Optional[InstrumentId] = None
    ) -> float:
        target_inst = instrument_id or self._archetype_config.instrument_id
        if self._is_owner_thread():
            cache_obj = getattr(self, "cache", None)
            if cache_obj is not None and hasattr(cache_obj, "quote_tick"):
                try:
                    qt = cache_obj.quote_tick(target_inst)
                    if qt is not None:
                        bid_obj = getattr(qt, "bid_price", None)
                        ask_obj = getattr(qt, "ask_price", None)
                        bid_val = (
                            float(bid_obj.as_double())
                            if hasattr(bid_obj, "as_double")
                            else float(_to_decimal(bid_obj, "0"))
                        )
                        ask_val = (
                            float(ask_obj.as_double())
                            if hasattr(ask_obj, "as_double")
                            else float(_to_decimal(ask_obj, "0"))
                        )
                        if self._archetype_config.plan_side == PlanSide.LONG and bid_val > 0.0:
                            return bid_val
                        if self._archetype_config.plan_side == PlanSide.SHORT and ask_val > 0.0:
                            return ask_val
                        if bid_val > 0.0 and ask_val > 0.0:
                            return 0.5 * (bid_val + ask_val)
                except Exception:
                    pass
        if self._closes:
            return float(self._closes[-1])
        return 0.0

    def on_order_modify_rejected(self, event: Any) -> None:
        cid = str(self._event_get(event, "client_order_id", "") or "")
        primary_stop_cid = (
            str(
                getattr(
                    self._protective_stop_order_id,
                    "value",
                    self._protective_stop_order_id,
                )
                or ""
            )
            if self._protective_stop_order_id is not None
            else ""
        )
        is_protective_stop = (
            (cid and cid == primary_stop_cid)
            or cid in self._stop_orders_by_cid
            or any(
                cid in cids
                for cids in self._active_protective_stops_by_inst.values()
            )
        )
        if not is_protective_stop:
            return

        if cid and cid in self._filled_order_ids:
            return

        t_meta = self._order_side_by_cid.get(cid)
        target_inst = (
            InstrumentId.from_str(t_meta[0])
            if t_meta is not None
            else self._archetype_config.instrument_id
        )
        existing_stop = (
            self._lookup_order_in_cache(self._stop_orders_by_cid.get(cid) or cid)
            or self._stop_orders_by_cid.get(cid)
        )
        if existing_stop is not None and self._normalized_order_status(existing_stop) == "FILLED":
            return

        if cid and cid in self._prior_stop_qty_by_cid:
            restored_qty = self._prior_stop_qty_by_cid.pop(cid)
            self._stop_qty_by_cid[cid] = restored_qty
            self._protective_stop_qty = float(restored_qty)

        safe_exit_spec = self._pending_safe_exits.get(str(target_inst))
        if safe_exit_spec is not None and cid in safe_exit_spec.get("pending_stop_cids", set()):
            self._cancel_single_order_safely(
                existing_stop if existing_stop is not None else ClientOrderId(cid)
            )
            return

        pos_qty = self._current_position_qty(target_inst)
        target_qty = (
            pos_qty
            if pos_qty > 0.0
            else float(
                self._stop_qty_by_cid.get(
                    cid, Decimal(f"{max(0.0, self._protective_stop_qty):.6f}")
                )
            )
        )
        if target_qty <= 0.0:
            return

        target_stop_px = float(
            self._stop_price
            if self._stop_price is not None and self._stop_price > 0.0
            else (self._stop_price_by_cid.get(cid, 0.0) or 0.0)
        )
        if self._trailing_stop_price is not None and self._trailing_stop_price > 0.0:
            if self._archetype_config.plan_side == PlanSide.LONG:
                target_stop_px = max(target_stop_px, float(self._trailing_stop_price))
            else:
                target_stop_px = (
                    min(target_stop_px, float(self._trailing_stop_price))
                    if target_stop_px > 0.0
                    else float(self._trailing_stop_price)
                )
        if target_stop_px <= 0.0 and self._risk_envelope is not None:
            target_stop_px = float(self._risk_envelope.initial_stop_price)
        if target_stop_px <= 0.0:
            return

        latest_px = self._resolve_latest_market_price(target_inst)
        reason_str = str(self._event_get(event, "reason", "") or "").lower()
        signed_pos = self._open_position_signed_qty(target_inst)
        is_long_pos = (
            signed_pos > Decimal("0")
            if signed_pos != Decimal("0")
            else self._archetype_config.plan_side == PlanSide.LONG
        )
        price_breached = (
            (is_long_pos and latest_px > 0.0 and latest_px <= target_stop_px)
            or (not is_long_pos and latest_px > 0.0 and latest_px >= target_stop_px)
            or "stop price" in reason_str
            or "stop_price" in reason_str
            or "below market" in reason_str
            or "above market" in reason_str
        )
        if price_breached and (pos_qty > 0.0 or signed_pos != Decimal("0")):
            logger.warning(
                "Protective stop modify rejected for %s (%s) with stop_px=%.4f breached by market_px=%.4f (reason=%s); "
                "triggering immediate fail-closed safe exit",
                target_inst,
                cid,
                target_stop_px,
                latest_px,
                reason_str,
            )
            self._submit_exit(
                reason="PROTECTIVE_STOP_MODIFY_BREACHED_FAIL_CLOSED",
                reference_price=latest_px if latest_px > 0.0 else target_stop_px,
                instrument_id=target_inst,
            )
            return

        sym = self._symbol_from_instrument_id(target_inst)
        if cid:
            self._notify_stop_leg_replaced(
                sym,
                cid,
                f"deferred-replace-{cid}",
                new_covered_qty=Decimal(f"{target_qty:.6f}"),
                stop_price=target_stop_px,
            )

        self._deferred_protective_stop_submission = (
            float(target_qty),
            float(target_stop_px),
        )
        if cid:
            self._deferred_protective_stop_by_cid[cid] = {
                "instrument_id": target_inst,
                "target_qty": float(target_qty),
                "target_stop_px": float(target_stop_px),
            }

        if existing_stop is not None:
            self._cancel_single_order_safely(existing_stop)
        elif cid:
            self._cancel_single_order_safely(ClientOrderId(cid))

    def on_order_filled(self, event: Any) -> None:
        cid = str(self._event_get(event, "client_order_id", "") or "")
        if not cid:
            return
        self._filled_order_ids.add(cid)
        event_id = self._event_get(event, "event_id", None) or self._event_get(event, "id", None)
        trade_id = self._event_get(event, "trade_id", None)
        ts_event = self._event_get(event, "ts_event", None)
        fill_key = f"id:{event_id}" if event_id is not None else f"fill:{cid}:{trade_id}:{ts_event}"
        self._processed_fill_keys.add(fill_key)

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
                    if t_inst_key == str(self._archetype_config.instrument_id):
                        self._entry_filled_qty = 0.0
                else:
                    self._tracked_signed_qty_by_inst[t_inst_key] = next_signed
                    if t_inst_key == str(self._archetype_config.instrument_id):
                        self._entry_filled_qty = float(abs(next_signed))

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
                self._entry_filled_qty = float(
                    new_covered_dec if new_covered_dec > Decimal("0") else target_qty_dec
                )
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
        cid = str(self._event_get(event, "client_order_id", "") or "")
        primary_stop_cid = (
            str(
                getattr(
                    self._protective_stop_order_id,
                    "value",
                    self._protective_stop_order_id,
                )
                or ""
            )
            if self._protective_stop_order_id is not None
            else ""
        )
        t_meta = self._order_side_by_cid.get(cid)
        target_inst = (
            InstrumentId.from_str(t_meta[0])
            if t_meta is not None
            else self._archetype_config.instrument_id
        )
        is_protective_stop = (
            (cid and cid == primary_stop_cid)
            or cid in self._stop_orders_by_cid
            or any(
                cid in cids
                for cids in self._active_protective_stops_by_inst.values()
            )
        )
        self._on_order_terminal(event, "REJECTED")
        if is_protective_stop:
            pos_qty = self._current_position_qty(target_inst)
            if pos_qty > 0.0 or self._entry_filled_qty > 0.0:
                reason_msg = str(self._event_get(event, "reason", "UNKNOWN") or "UNKNOWN")
                logger.error(
                    "Protective stop order %s rejected for %s while holding position (qty=%s, reason=%s); triggering emergency fail-closed exit",
                    cid,
                    target_inst,
                    pos_qty or self._entry_filled_qty,
                    reason_msg,
                )
                self._submit_exit(
                    reason="PROTECTIVE_STOP_REJECTED_FAIL_CLOSED",
                    instrument_id=target_inst,
                )

    def on_order_canceled(self, event: Any) -> None:
        cid = str(self._event_get(event, "client_order_id", "") or "")
        deferred_meta = self._deferred_protective_stop_by_cid.pop(cid, None) if cid else None
        deferred_tuple = self._deferred_protective_stop_submission
        primary_stop_cid = (
            str(
                getattr(
                    self._protective_stop_order_id,
                    "value",
                    self._protective_stop_order_id,
                )
                or ""
            )
            if self._protective_stop_order_id is not None
            else ""
        )
        is_deferred_stop_cancel = deferred_meta is not None or (
            deferred_tuple is not None
            and (
                not cid
                or cid == primary_stop_cid
                or cid in self._stop_orders_by_cid
                or any(
                    cid in cids
                    for cids in self._active_protective_stops_by_inst.values()
                )
            )
        )
        self._on_order_terminal(event, "CANCELED")
        if is_deferred_stop_cancel:
            self._deferred_protective_stop_submission = None
            target_inst = (
                deferred_meta["instrument_id"]
                if deferred_meta is not None
                else self._archetype_config.instrument_id
            )
            if deferred_tuple is not None:
                target_qty, target_stop_px = deferred_tuple
            else:
                target_qty = float(deferred_meta["target_qty"])
                target_stop_px = float(deferred_meta["target_stop_px"])
            pos_qty = self._current_position_qty(target_inst)
            eff_qty = pos_qty if pos_qty > 0.0 else target_qty
            if eff_qty > 0.0 and target_stop_px > 0.0:
                signed_pos = self._open_position_signed_qty(target_inst)
                entry_side = (
                    OrderSide.BUY
                    if (
                        signed_pos > Decimal("0")
                        or self._archetype_config.plan_side == PlanSide.LONG
                    )
                    else OrderSide.SELL
                )
                stop_qty_obj = self._format_quantity(eff_qty, target_inst)
                sym = self._symbol_from_instrument_id(target_inst)
                replacement_stop = self._build_protective_stop_order(
                    instrument_id=target_inst,
                    entry_side=entry_side,
                    quantity=stop_qty_obj,
                    stop_loss_price=float(target_stop_px),
                    parent_entry_cid=f"cancel-replace-{sym}",
                )
                if replacement_stop is not None:
                    new_stop_cid = self._register_and_submit_protective_stop_order(
                        replacement_stop,
                        target_inst,
                        stop_qty_obj,
                        stop_loss_price=float(target_stop_px),
                    )
                    if new_stop_cid:
                        self._notify_stop_leg_replaced(
                            sym,
                            cid or f"deferred-replace-{sym}",
                            new_stop_cid,
                            new_covered_qty=Decimal(f"{eff_qty:.6f}"),
                            stop_price=float(target_stop_px),
                        )
                        self._active_protective_stops_by_inst[str(target_inst)] = [
                            new_stop_cid
                        ]

    def on_order_cancel_rejected(self, event: Any) -> None:
        cid = str(self._event_get(event, "client_order_id", "") or "")
        if not cid:
            return
        reason_str = str(self._event_get(event, "reason", "") or "")
        reason_lower = reason_str.lower()
        existing_stop = (
            self._lookup_order_in_cache(self._stop_orders_by_cid.get(cid) or cid)
            or self._stop_orders_by_cid.get(cid)
        )
        cached_status = (
            self._normalized_order_status(existing_stop)
            if existing_stop is not None
            else ""
        )
        is_filled = (
            cid in self._filled_order_ids
            or cached_status == "FILLED"
            or "already filled" in reason_lower
            or ("filled" in reason_lower and "not" not in reason_lower and "partial" not in reason_lower)
        )
        is_already_gone = (
            cached_status in ("CANCELED", "EXPIRED", "REJECTED", "DENIED")
            or "without venueorderid" in reason_lower
            or "not found" in reason_lower
            or "404" in reason_lower
            or "already cancel" in reason_lower
            or "already expired" in reason_lower
            or "already rejected" in reason_lower
        )

        for inst_key, spec in list(self._pending_safe_exits.items()):
            pending_cids: set[str] = spec.get("pending_stop_cids", set())
            if cid not in pending_cids:
                continue
            target_inst: InstrumentId = spec["instrument_id"]
            sym: str = spec["symbol"]
            if is_filled:
                fill_qty_dec = (
                    _to_decimal(getattr(existing_stop, "filled_qty", 0), "0")
                    if existing_stop is not None
                    else Decimal("0")
                )
                self._advance_pending_safe_exit_for_stop_event(
                    cid,
                    stop_filled=True,
                    stop_fill_qty_dec=fill_qty_dec,
                )
            elif is_already_gone:
                self._advance_pending_safe_exit_for_stop_event(
                    cid,
                    stop_filled=False,
                )
            elif (
                cached_status == "PENDING_UPDATE"
                or "pending_replace" in reason_lower
                or "pending_update" in reason_lower
                or "replace" in reason_lower
            ):
                while cid in self._canceled_order_ids_audit:
                    self._canceled_order_ids_audit.remove(cid)
                return
            else:
                pending_cids.discard(cid)
                if not pending_cids:
                    self._pending_safe_exits.pop(inst_key, None)
                    self._notify_safe_exit_end(sym)
            return

        deferred_meta = self._deferred_protective_stop_by_cid.pop(cid, None)
        deferred_tuple = self._deferred_protective_stop_submission
        if deferred_meta is not None or deferred_tuple is not None:
            self._deferred_protective_stop_submission = None
            if is_already_gone and not is_filled:
                target_inst = (
                    deferred_meta["instrument_id"]
                    if deferred_meta is not None
                    else self._archetype_config.instrument_id
                )
                self._remove_tracked_stop_cid(cid, target_inst)
                if deferred_tuple is not None:
                    target_qty, target_stop_px = deferred_tuple
                else:
                    target_qty = float(deferred_meta["target_qty"])
                    target_stop_px = float(deferred_meta["target_stop_px"])
                pos_qty = self._current_position_qty(target_inst)
                eff_qty = pos_qty if pos_qty > 0.0 else target_qty
                if eff_qty > 0.0 and target_stop_px > 0.0:
                    signed_pos = self._open_position_signed_qty(target_inst)
                    entry_side = (
                        OrderSide.BUY
                        if (
                            signed_pos > Decimal("0")
                            or self._archetype_config.plan_side == PlanSide.LONG
                        )
                        else OrderSide.SELL
                    )
                    stop_qty_obj = self._format_quantity(eff_qty, target_inst)
                    sym = self._symbol_from_instrument_id(target_inst)
                    replacement_stop = self._build_protective_stop_order(
                        instrument_id=target_inst,
                        entry_side=entry_side,
                        quantity=stop_qty_obj,
                        stop_loss_price=float(target_stop_px),
                        parent_entry_cid=f"cancel-replace-{sym}",
                    )
                    if replacement_stop is not None:
                        new_stop_cid = self._register_and_submit_protective_stop_order(
                            replacement_stop,
                            target_inst,
                            stop_qty_obj,
                            stop_loss_price=float(target_stop_px),
                        )
                        if new_stop_cid:
                            self._notify_stop_leg_replaced(
                                sym,
                                cid,
                                new_stop_cid,
                                new_covered_qty=Decimal(f"{eff_qty:.6f}"),
                                stop_price=float(target_stop_px),
                            )
                            self._active_protective_stops_by_inst[str(target_inst)] = [
                                new_stop_cid
                            ]

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
            backend = getattr(self._plan_gate_ref, "backend", self._plan_gate_ref)
            if (
                not bool(getattr(backend, "_allow_multi_cycle_reentry", True))
                and (inst_id is None or str(inst_id) == str(self._archetype_config.instrument_id))
            ):
                self._exit_only_mode = True
        if inst_id is None or str(inst_id) == str(self._archetype_config.instrument_id):
            self._trailing_stop_price = None
            self._pending_thesis_exit = False
            self._entry_filled_qty = 0.0
            self._pending_protective_stop_sync = None
            self._deferred_protective_stop_submission = None

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
        if self._stop_price is not None and self._stop_price > 0.0:
            candidates.append(Decimal(f"{self._stop_price:.4f}"))
        for stop_order in self._get_open_protective_stop_orders(target_inst):
            cid = str(getattr(stop_order, "client_order_id", "") or "")
            tracked_px = self._stop_price_by_cid.get(cid, 0.0)
            if tracked_px > 0.0:
                candidates.append(Decimal(f"{tracked_px:.4f}"))
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
        if inst_key in self._pending_safe_exits or self._active_exit_orders_by_inst.get(inst_key):
            return None
        sym = self._symbol_from_instrument_id(target_inst)
        open_stops = self._get_open_protective_stop_orders(target_inst)
        signed_pos = self._open_position_signed_qty(target_inst)
        abs_pos = abs(signed_pos)
        if (
            abs_pos <= Decimal("0")
            and (
                instrument_id is None
                or str(instrument_id) == str(self._archetype_config.instrument_id)
            )
            and self._entry_filled_qty > 0.0
        ):
            abs_pos = Decimal(f"{self._entry_filled_qty:.6f}")
        if not open_stops and (abs_pos <= Decimal("0") or not self._should_submit_protective_stop()):
            return None

        desired_qty = abs_pos
        if desired_qty <= Decimal("0") and open_stops:
            first_cid = str(getattr(open_stops[0], "client_order_id", "") or "")
            desired_qty = self._stop_qty_by_cid.get(
                first_cid,
                _to_decimal(getattr(open_stops[0], "quantity", 0), "0"),
            )
        if desired_qty <= Decimal("0") and self._protective_stop_qty > 0.0:
            desired_qty = Decimal(f"{self._protective_stop_qty:.6f}")
        if desired_qty <= Decimal("0"):
            return None

        if len(open_stops) == 1:
            existing_stop = open_stops[0]
            existing_cid = str(getattr(existing_stop, "client_order_id", "") or "")
            if self._is_stop_order_unconfirmed_submitted(existing_stop):
                pending_qty = (
                    max(
                        float(desired_qty),
                        float(self._pending_protective_stop_sync[0]),
                    )
                    if self._pending_protective_stop_sync is not None
                    else float(desired_qty)
                )
                self._pending_protective_stop_sync = (
                    pending_qty,
                    float(new_stop_price),
                )
                self._stop_price = float(new_stop_price)
                return existing_cid
            return self._apply_atomic_protective_stop_modify(
                existing_stop,
                target_qty=desired_qty,
                target_stop_px=float(new_stop_price),
                instrument_id=target_inst,
            )

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
            stop_loss_price=float(new_stop_price),
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
            if self._is_owner_thread():
                self._schedule_rth_open_thesis_exit_timer()
            else:
                owner_loop = getattr(self, "_owner_loop", None)
                if owner_loop is not None and not owner_loop.is_closed():
                    try:
                        owner_loop.call_soon_threadsafe(
                            lambda: self.trigger_thesis_invalidated_exit(
                                reference_price=reference_price,
                                execute_immediately=execute_immediately,
                                reason_tag=reason_tag,
                            )
                        )
                    except Exception:
                        pass
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
        if exit_cid:
            self._pending_thesis_exit = False
        return exit_cid

    def _check_and_execute_pending_thesis_exit_at_price(
        self, reference_price: float = 0.0
    ) -> bool:
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
                self.trigger_thesis_invalidated_exit(
                    reference_price=reference_price if reference_price > 0.0 else None,
                    execute_immediately=True,
                    reason_tag=self._pending_thesis_exit_reason,
                )
        return True

    def _check_and_execute_pending_thesis_exit(self, bar: Bar) -> bool:
        """Executes any pending `THESIS_INVALIDATED_EXIT` on the incoming RTH bar if not already exited."""
        close_px = float(bar.close.as_double()) if hasattr(bar, "close") else 0.0
        return self._check_and_execute_pending_thesis_exit_at_price(close_px)

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
        if (
            self._in_warmup
            or self._exit_only_mode
            or self._thesis_invalidated
            or self._is_instrument_halted(target_inst)
        ):
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
        raw_qty_val = (
            quantity if quantity is not None else self._archetype_config.trade_qty
        )
        if self._risk_envelope is not None and reference_price > 0.0:
            plan_ref_px = (
                float(
                    self._risk_envelope.target_notional_usd
                    / self._risk_envelope.target_qty
                )
                if self._risk_envelope.target_qty > Decimal("0")
                and self._risk_envelope.target_notional_usd > Decimal("0")
                else 0.0
            )
            plan_stop_px = float(self._risk_envelope.initial_stop_price)
            if (
                stop_loss_price is not None
                and plan_stop_px > 0.0
                and abs(float(stop_loss_price) - plan_stop_px) <= 1e-4
                and (
                    (order_side == OrderSide.BUY and float(stop_loss_price) >= reference_price)
                    or (order_side == OrderSide.SELL and float(stop_loss_price) <= reference_price)
                )
            ):
                atr_mult = float(
                    _to_decimal(
                        getattr(self._archetype_config, "atr_stop_multiplier", None),
                        "2",
                    )
                )
                atr_dist = max(0.0, float(atr)) * max(atr_mult, 1.0)
                plan_dist = (
                    abs(plan_ref_px - plan_stop_px)
                    if plan_ref_px > 0.0 and abs(plan_ref_px - plan_stop_px) > 0.0
                    else atr_dist
                )
                eff_dist = (
                    min(atr_dist, plan_dist)
                    if (atr_dist > 0.0 and plan_dist > 0.0)
                    else max(atr_dist, plan_dist)
                )
                if eff_dist > 0.0:
                    stop_loss_price = (
                        max(0.01, reference_price - eff_dist)
                        if order_side == OrderSide.BUY
                        else reference_price + eff_dist
                    )
            raw_qty_dec = _to_decimal(raw_qty_val, "0")
            max_risk_usd = float(self._risk_envelope.max_risk_budget_usd)
            is_coherent_plan_env = (
                plan_ref_px > 0.0
                and plan_stop_px > 0.0
                and (
                    (order_side == OrderSide.BUY and plan_ref_px > plan_stop_px)
                    or (order_side == OrderSide.SELL and plan_stop_px > plan_ref_px)
                )
                and float(self._risk_envelope.target_qty) * abs(plan_ref_px - plan_stop_px)
                <= max_risk_usd + 1.0
            )
            if (
                is_coherent_plan_env
                and raw_qty_dec > Decimal("0")
                and raw_qty_dec <= self._risk_envelope.approved_qty_cap
                and abs(reference_price - plan_ref_px) > 1e-6
            ):
                scaled_qty_int = int(raw_qty_dec)
                max_notional_usd = float(self._risk_envelope.target_notional_usd)
                if (
                    max_notional_usd > 0.0
                    and float(scaled_qty_int) * reference_price > max_notional_usd + 1e-6
                ):
                    notional_capped = int(
                        math.floor((max_notional_usd + 1e-6) / reference_price)
                    )
                    if notional_capped >= 1:
                        scaled_qty_int = min(scaled_qty_int, notional_capped)
                if (
                    stop_loss_price is not None
                    and float(stop_loss_price) > 0.0
                    and max_risk_usd > 0.0
                ):
                    stop_dist_now = abs(reference_price - float(stop_loss_price))
                    if (
                        stop_dist_now > 0.0
                        and float(scaled_qty_int) * stop_dist_now > max_risk_usd + 1e-6
                    ):
                        risk_capped = int(
                            math.floor((max_risk_usd + 1e-6) / stop_dist_now)
                        )
                        if risk_capped >= 1:
                            scaled_qty_int = min(scaled_qty_int, risk_capped)
                if scaled_qty_int < int(raw_qty_dec):
                    raw_qty_val = Decimal(str(scaled_qty_int))
        qty_obj = self._format_quantity(
            raw_qty_val,
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

    def _submit_entry(
        self,
        order_side: Optional[OrderSide] = None,
        reference_price: Optional[float] = None,
        atr: Optional[float] = None,
        limit_price: Optional[float] = None,
        quantity: Optional[Decimal | float] = None,
        instrument_id: Optional[InstrumentId] = None,
        stop_loss_price: Optional[float] = None,
        tags: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> str:
        target_inst = instrument_id or self._archetype_config.instrument_id
        if self._is_instrument_halted(target_inst):
            return ""
        resolved_side = order_side or (
            OrderSide.BUY
            if self._archetype_config.plan_side == PlanSide.LONG
            else OrderSide.SELL
        )
        raw_ref = (
            reference_price
            if reference_price is not None
            else (
                kwargs.get("price")
                if kwargs.get("price") is not None
                else (
                    kwargs.get("entry_price")
                    if kwargs.get("entry_price") is not None
                    else (self._closes[-1] if self._closes else None)
                )
            )
        )
        if raw_ref is None and self._risk_envelope is not None:
            env_ref = _to_decimal(
                getattr(self._risk_envelope, "reference_price", None), "0"
            )
            if env_ref > Decimal("0"):
                raw_ref = float(env_ref)
            elif (
                getattr(self._risk_envelope, "target_qty", Decimal("0")) > Decimal("0")
                and getattr(self._risk_envelope, "target_notional_usd", Decimal("0"))
                > Decimal("0")
            ):
                raw_ref = float(
                    self._risk_envelope.target_notional_usd
                    / self._risk_envelope.target_qty
                )
        if raw_ref is None:
            return ""
        resolved_ref = float(raw_ref)
        if resolved_ref <= 0.0:
            return ""

        compute_atr_fn = getattr(self, "_compute_atr", None)
        raw_atr = (
            atr
            if atr is not None
            else (compute_atr_fn() if callable(compute_atr_fn) else None)
        )
        if (
            (raw_atr is None or float(raw_atr) <= 0.0)
            and self._highs
            and self._lows
            and self._closes
        ):
            fast_win = max(
                1, int(getattr(self._archetype_config, "fast_window", 5) or 5)
            )
            atr_candidate = rolling_atr(
                self._highs, self._lows, self._closes, fast_win
            )
            if atr_candidate > 0.0:
                raw_atr = atr_candidate
        if (raw_atr is None or float(raw_atr) <= 0.0) and len(self._closes) >= 2:
            abs_diffs = [
                abs(float(self._closes[i]) - float(self._closes[i - 1]))
                for i in range(1, len(self._closes))
            ]
            mean_diff = sum(abs_diffs) / len(abs_diffs) if abs_diffs else 0.0
            if mean_diff > 0.0:
                raw_atr = mean_diff
        if raw_atr is None or float(raw_atr) <= 0.0:
            atr_mult_cfg = float(
                _to_decimal(
                    getattr(self._archetype_config, "atr_stop_multiplier", None), "1"
                )
            )
            env_stop = (
                float(self._stop_price)
                if self._stop_price is not None and self._stop_price > 0.0
                else (
                    float(self._risk_envelope.initial_stop_price)
                    if self._risk_envelope is not None
                    and self._risk_envelope.initial_stop_price > Decimal("0")
                    else 0.0
                )
            )
            if env_stop > 0.0 and abs(resolved_ref - env_stop) > 0.0 and atr_mult_cfg > 0.0:
                raw_atr = abs(resolved_ref - env_stop) / atr_mult_cfg
        if raw_atr is None or float(raw_atr) <= 0.0:
            return ""
        resolved_atr = float(raw_atr)

        resolved_stop = (
            stop_loss_price
            if stop_loss_price is not None
            else kwargs.get("stop_price")
        )
        if resolved_stop is None:
            stop_fn = getattr(self, "_stop_loss_price_for_entry", None)
            if callable(stop_fn):
                resolved_stop = stop_fn(resolved_ref, resolved_atr)
            elif (
                self._stop_price is not None
                and self._stop_price > 0.0
                and (
                    (resolved_side == OrderSide.BUY and float(self._stop_price) < resolved_ref)
                    or (resolved_side == OrderSide.SELL and float(self._stop_price) > resolved_ref)
                )
            ):
                resolved_stop = float(self._stop_price)
            elif (
                self._risk_envelope is not None
                and self._risk_envelope.initial_stop_price > Decimal("0")
                and (
                    (resolved_side == OrderSide.BUY and float(self._risk_envelope.initial_stop_price) < resolved_ref)
                    or (resolved_side == OrderSide.SELL and float(self._risk_envelope.initial_stop_price) > resolved_ref)
                )
            ):
                resolved_stop = float(self._risk_envelope.initial_stop_price)
            else:
                atr_mult = float(
                    _to_decimal(
                        getattr(self._archetype_config, "atr_stop_multiplier", None),
                        "1",
                    )
                )
                resolved_stop = self._resolve_protective_stop_price(
                    reference_price=resolved_ref,
                    side=resolved_side,
                    raw_stop_distance=atr_mult * resolved_atr,
                    instrument_id=target_inst,
                )
            if (
                resolved_stop is not None
                and self._risk_envelope is not None
                and self._risk_envelope.target_qty > Decimal("0")
                and self._risk_envelope.target_notional_usd > Decimal("0")
                and self._risk_envelope.initial_stop_price > Decimal("0")
            ):
                plan_ref_px = float(
                    self._risk_envelope.target_notional_usd
                    / self._risk_envelope.target_qty
                )
                plan_stop_px = float(self._risk_envelope.initial_stop_price)
                if (
                    resolved_side == OrderSide.BUY
                    and plan_ref_px > plan_stop_px
                ):
                    plan_dist = plan_ref_px - plan_stop_px
                    eff_floor = (
                        plan_stop_px
                        if resolved_ref > plan_stop_px
                        else max(0.01, resolved_ref - plan_dist)
                    )
                    if float(resolved_stop) < eff_floor < resolved_ref:
                        resolved_stop = eff_floor
                elif (
                    resolved_side == OrderSide.SELL
                    and plan_stop_px > plan_ref_px
                ):
                    plan_dist = plan_stop_px - plan_ref_px
                    eff_ceil = (
                        plan_stop_px
                        if resolved_ref < plan_stop_px
                        else resolved_ref + plan_dist
                    )
                    if float(resolved_stop) > eff_ceil > resolved_ref:
                        resolved_stop = eff_ceil
        return self._submit_plan_gated_entry(
            order_side=resolved_side,
            reference_price=resolved_ref,
            atr=resolved_atr,
            limit_price=limit_price,
            quantity=quantity,
            instrument_id=target_inst,
            stop_loss_price=float(resolved_stop) if resolved_stop is not None else None,
            tags=tags,
        )

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
        resolved_reason = str(reason or reason_tag or "SIGNAL_EXIT")
        is_emergency_exit = "FAIL_CLOSED" in resolved_reason or "EMERGENCY" in resolved_reason
        if self._in_warmup and not is_emergency_exit:
            return ""
        delay_bars = int(getattr(self._archetype_config, "execution_delay_bars", 0) or 0)
        if (
            delay_bars > 0
            and not self._flushing_delayed_orders
            and not self._thesis_invalidated
            and not is_emergency_exit
        ):
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
        elif (
            instrument_id is None
            or str(instrument_id) == str(self._archetype_config.instrument_id)
        ) and self._entry_filled_qty > 0.0:
            resolved_qty_dec = Decimal(f"{self._entry_filled_qty:.6f}")
        else:
            resolved_qty_dec = abs(_to_decimal(self._archetype_config.trade_qty, "1"))

        if reference_price <= 0.0 and self._closes:
            reference_price = float(self._closes[-1])

        for pending_cid, pending_spec in list(self._pending_protective_stops.items()):
            if str(pending_spec.get("instrument_id")) == inst_key:
                self._pending_protective_stops.pop(pending_cid, None)

        if inst_key in self._pending_safe_exits or self._active_exit_orders_by_inst.get(inst_key):
            existing_safe_exit = self._pending_safe_exits.get(inst_key)
            if existing_safe_exit is not None:
                existing_safe_exit["exit_side"] = resolved_exit_side
                existing_safe_exit["quantity"] = resolved_qty_dec
                if reference_price > 0.0:
                    existing_safe_exit["reference_price"] = float(reference_price)
                existing_safe_exit["reason_tag"] = resolved_reason
                if signal_tags is not None:
                    existing_safe_exit["signal_tags"] = list(signal_tags)
            return ""

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

    def _submit_exit(
        self,
        reason: str = "SIGNAL_EXIT",
        exit_side: Optional[OrderSide] = None,
        quantity: Optional[Decimal | float] = None,
        reference_price: float = 0.0,
        instrument_id: Optional[InstrumentId] = None,
        signal_tags: Optional[List[str]] = None,
    ) -> Optional[str]:
        return self._submit_plan_gated_exit(
            exit_side=exit_side,
            quantity=quantity,
            reference_price=reference_price,
            reason_tag=reason,
            instrument_id=instrument_id,
            reason=reason,
            signal_tags=signal_tags,
        )
