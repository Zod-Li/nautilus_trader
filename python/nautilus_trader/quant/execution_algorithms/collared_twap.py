"""Event-driven, fee-aware `CollaredTwapExecAlgorithm` (`ExecAlgorithmId("COLLARED_TWAP")`).

Implements SPEC-0007 §2.2:
- Fails closed on uncollared `MarketOrder` entries (`self.deny_order`).
- Enforces IBKR Pro Fixed breakeven slice guard (`N_eff = max(1, min(N_target, floor(Q_total / breakeven_slice_shares)))`),
  collapsing `< 2 * breakeven_slice_shares` (`< 400` shares) or `DIRECT_COLLARED_LIMIT` to a single collared `LimitOrder`.
- Computes dynamic slice limit prices from non-stale `cache.quote(instrument_id)` (`mid_price` aligning with
  `RustPlanGateEngine`'s `PriceType.MID`, falling back to `ask_price`/`bid_price` if one-sided) with isomorphic
  fallback to latest closed `cache.bar(...)` `close`, clamped strictly by `primary.price` (`worst_limit_price`)
  and quantized per Reg NMS Rule 612 / `instrument.price_increment`.
- Never chases when market `ref_px` exceeds `worst_limit_price` (waits for next interval).
- Strictly event-driven reslicing: on slice TTL `TimeEvent`, cancels any open child via `cancel_order(child)`
  and awaits `on_order_canceled` / `on_order_filled` before spawning the next slice.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_FLOOR, Decimal
import logging
import threading
from typing import Any, Dict, List, Optional, Set

from nautilus_trader.model import (
    AggregationSource,
    ClientOrderId,
    ExecAlgorithmId,
    OrderSide,
    OrderStatus,
    OrderType,
    Price,
    PriceType,
    Quantity,
    TimeInForce,
)
from nautilus_trader.trading import ExecutionAlgorithm, ExecutionAlgorithmConfig

from nautilus_trader.quant.contracts.models import (
    ExecAlgorithm,
    PlanSide,
    quantize_price_rule_612,
    rule_612_tick_size,
)
from nautilus_trader.quant.execution_algorithms.friction import IbkrProFeeSchedule

logger = logging.getLogger(__name__)

COLLARED_TWAP_EXEC_ALGORITHM_ID = ExecAlgorithmId("COLLARED_TWAP")


def _to_dec(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    if hasattr(value, "as_decimal"):
        return value.as_decimal()
    if isinstance(value, Decimal):
        return value
    text = str(value).strip()
    if not text:
        return Decimal(default)
    return Decimal(text.split()[0])


def _normalize_symbol(instrument_id: Any) -> str:
    text = str(instrument_id or "").strip().upper()
    if "." in text:
        return text.split(".", 1)[0].strip()
    return text


def _is_market_order(order: Any) -> bool:
    otype = getattr(order, "order_type", None)
    if otype == OrderType.MARKET:
        return True
    otype_str = str(otype or "").strip().upper()
    if "." in otype_str:
        otype_str = otype_str.rsplit(".", 1)[-1]
    return otype_str == "MARKET"


def _is_buy_side(side: Any) -> bool:
    if side == OrderSide.BUY:
        return True
    text = str(side or "").strip().upper()
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    return text in ("BUY", "LONG")


def _is_entry_order_for_cancel(order: Any) -> bool:
    if bool(getattr(order, "is_reduce_only", False)):
        return False
    otype = str(getattr(order, "order_type", "") or "").strip().upper()
    if "." in otype:
        otype = otype.rsplit(".", 1)[-1]
    if any(tok in otype for tok in ("STOP", "STOP_MARKET", "STOP_LIMIT", "TRAILING_STOP")):
        return False
    exec_params = getattr(order, "exec_algorithm_params", None) or {}
    if isinstance(exec_params, dict):
        intent = str(
            exec_params.get("intent") or exec_params.get("action") or ""
        ).strip().upper()
        if intent in ("EXIT", "FLATTEN", "CLOSE", "PROTECTIVE_STOP", "STOP_LOSS"):
            return False
    tags = getattr(order, "tags", None) or ()
    for tag in tags:
        if str(tag).strip().upper() in (
            "EXIT",
            "FLATTEN",
            "CLOSE",
            "PROTECTIVE_STOP",
            "STOP_LOSS",
        ):
            return False
    return True


def _format_price(px: Decimal, precision: int) -> Price:
    return Price.from_str(f"{px:.{max(0, int(precision))}f}")


def _format_quantity(qty: Decimal, precision: int) -> Quantity:
    prec = max(0, int(precision))
    if prec == 0:
        return Quantity.from_str(str(int(qty.to_integral_value(rounding=ROUND_FLOOR))))
    return Quantity.from_str(f"{qty:.{prec}f}")


@dataclass
class SliceScheduleState:
    """Single-responsibility collaborator tracking slicing & collar state for one parent order."""

    primary_cid: str
    instrument_id: Any
    symbol: str
    side: OrderSide
    total_qty: Decimal
    worst_limit_price: Decimal
    tick_size: Optional[Decimal]
    price_precision: int
    size_precision: int
    min_slice_shares: int
    target_slices: int
    interval_ns: int
    collar_pct: Decimal
    collar_atr: Decimal
    daily_atr_14: Optional[Decimal]
    quote_stale_ns: int
    horizon_end_ns: Optional[int] = None
    slice_index: int = 0
    active_child_cid: Optional[str] = None
    waiting_for_cancel: bool = False
    cancel_requested: bool = False
    completed: bool = False
    timer_name: str = ""
    child_cids: List[str] = field(default_factory=list)

    def _effective_tick_size(self, price: Decimal) -> Optional[Decimal]:
        """Enforces Reg NMS Rule 612 floor (`$0.01` at or above `$1.00`) over sub-penny instrument ticks."""
        if self.tick_size is None:
            return None
        return max(self.tick_size, rule_612_tick_size(price))

    def _quantize_for_side(self, price: Decimal) -> Decimal:
        if price <= Decimal("0"):
            return price
        side = PlanSide.LONG if _is_buy_side(self.side) else PlanSide.SHORT
        return quantize_price_rule_612(
            price,
            side=side,
            tick_size=self._effective_tick_size(price),
        )

    def compute_slice_limit_price(self, ref_px: Optional[Decimal]) -> Optional[Decimal]:
        """Computes dynamic collared limit price from `ref_px`; returns `None` if market exceeds `worst_limit_price`."""
        quantized_worst = self._quantize_for_side(self.worst_limit_price)
        if ref_px is None or ref_px <= Decimal("0"):
            return quantized_worst

        is_buy = _is_buy_side(self.side)
        if is_buy and ref_px > quantized_worst:
            return None
        if not is_buy and ref_px < quantized_worst:
            return None

        pct_offset = ref_px * (self.collar_pct / Decimal("100"))
        if (
            self.daily_atr_14 is not None
            and self.daily_atr_14 > Decimal("0")
            and self.collar_atr > Decimal("0")
        ):
            allowed_offset = min(pct_offset, self.daily_atr_14 * self.collar_atr)
        else:
            allowed_offset = pct_offset

        if is_buy:
            raw_px = min(quantized_worst, ref_px + allowed_offset)
            quantized = quantize_price_rule_612(
                raw_px,
                side=PlanSide.LONG,
                tick_size=self._effective_tick_size(raw_px),
            )
            return min(quantized_worst, quantized)

        raw_px = max(quantized_worst, ref_px - allowed_offset)
        quantized = quantize_price_rule_612(
            raw_px,
            side=PlanSide.SHORT,
            tick_size=self._effective_tick_size(raw_px),
        )
        return max(quantized_worst, quantized)

    def compute_next_slice_qty(self, remaining_qty: Decimal, *, is_final: bool) -> Decimal:
        """Computes next slice size respecting `min_slice_shares` (`200`) without sub-floor dust."""
        if remaining_qty <= Decimal("0"):
            return Decimal("0")
        min_multi = Decimal(str(2 * self.min_slice_shares))
        remaining_slices = max(1, self.target_slices - self.slice_index)
        if is_final or remaining_slices <= 1 or remaining_qty < min_multi:
            return remaining_qty

        raw_slice = int(
            (remaining_qty / Decimal(str(remaining_slices))).to_integral_value(
                rounding=ROUND_FLOOR
            )
        )
        slice_int = max(self.min_slice_shares, raw_slice)
        slice_dec = Decimal(str(slice_int))
        leftover = remaining_qty - slice_dec
        if leftover < Decimal(str(self.min_slice_shares)):
            return remaining_qty
        return slice_dec


class CollaredTwapExecAlgorithmConfig(ExecutionAlgorithmConfig):
    """Configuration for `CollaredTwapExecAlgorithm`."""

    def __new__(
        cls,
        exec_algorithm_id: Optional[str | ExecAlgorithmId] = None,
        horizon_secs: float = 900.0,
        interval_secs: float = 60.0,
        quote_stale_secs: float = 15.0,
        default_collar_pct: Optional[Decimal | float | str] = None,
        default_collar_atr: Optional[Decimal | float | str] = None,
        fee_schedule: Optional[IbkrProFeeSchedule] = None,
        log_events: bool = True,
        log_commands: bool = True,
    ):
        _ = (
            horizon_secs,
            interval_secs,
            quote_stale_secs,
            default_collar_pct,
            default_collar_atr,
            fee_schedule,
        )
        resolved_id = (
            exec_algorithm_id
            if isinstance(exec_algorithm_id, ExecAlgorithmId)
            else ExecAlgorithmId(str(exec_algorithm_id or COLLARED_TWAP_EXEC_ALGORITHM_ID))
        )
        return super().__new__(
            cls,
            exec_algorithm_id=resolved_id,
            log_events=log_events,
            log_commands=log_commands,
        )

    def __init__(
        self,
        exec_algorithm_id: Optional[str | ExecAlgorithmId] = None,
        horizon_secs: float = 900.0,
        interval_secs: float = 60.0,
        quote_stale_secs: float = 15.0,
        default_collar_pct: Optional[Decimal | float | str] = None,
        default_collar_atr: Optional[Decimal | float | str] = None,
        fee_schedule: Optional[IbkrProFeeSchedule] = None,
        log_events: bool = True,
        log_commands: bool = True,
    ) -> None:
        _ = exec_algorithm_id, log_events, log_commands
        self.horizon_secs = max(1.0, float(horizon_secs))
        self.interval_secs = max(1.0, float(interval_secs))
        self.quote_stale_secs = max(0.1, float(quote_stale_secs))
        needs_pct_default = (
            default_collar_pct is None or str(default_collar_pct).strip() == ""
        )
        needs_atr_default = (
            default_collar_atr is None or str(default_collar_atr).strip() == ""
        )
        if needs_pct_default or needs_atr_default:
            from evolution_engine.config_center.schema import SystemConfig

            exec_cfg = SystemConfig().execution
        else:
            exec_cfg = None
        self.default_collar_pct = (
            Decimal(str(exec_cfg.limit_collar_pct))
            if needs_pct_default
            else _to_dec(default_collar_pct, "0")
        )
        self.default_collar_atr = (
            Decimal(str(exec_cfg.limit_collar_atr))
            if needs_atr_default
            else _to_dec(default_collar_atr, "0")
        )
        self.fee_schedule = fee_schedule or IbkrProFeeSchedule()


class CollaredTwapExecAlgorithm(ExecutionAlgorithm):
    """Event-driven, fee-aware collared TWAP execution algorithm (`ExecAlgorithmId("COLLARED_TWAP")`)."""

    def __new__(
        cls,
        config: Optional[CollaredTwapExecAlgorithmConfig] = None,
    ) -> "CollaredTwapExecAlgorithm":
        effective_config = config or CollaredTwapExecAlgorithmConfig()
        return super().__new__(cls, effective_config)

    def __init__(
        self,
        config: Optional[CollaredTwapExecAlgorithmConfig] = None,
    ) -> None:
        effective_config = config or CollaredTwapExecAlgorithmConfig()
        super().__init__(effective_config)
        self._algo_config = effective_config
        self._fee_schedule: IbkrProFeeSchedule = effective_config.fee_schedule
        self._schedules_by_primary: Dict[str, SliceScheduleState] = {}
        self._primary_by_child: Dict[str, str] = {}
        self._primary_by_timer: Dict[str, str] = {}
        self._primaries_by_symbol: Dict[str, Set[str]] = {}
        self._owner_thread_id: int = threading.get_ident()
        self._cached_clock: Optional[Any] = None

    @property
    def fee_schedule(self) -> IbkrProFeeSchedule:
        return self._fee_schedule

    def _is_owner_thread(self) -> bool:
        return threading.get_ident() == getattr(
            self, "_owner_thread_id", threading.get_ident()
        )

    def _safe_clock(self) -> Optional[Any]:
        if not self._is_owner_thread():
            return None
        try:
            clk = getattr(self, "clock", None)
            if clk is not None:
                self._cached_clock = clk
                return clk
        except Exception:
            pass
        return self._cached_clock

    def on_start(self) -> None:
        self._safe_clock()
        return None

    def on_stop(self) -> None:
        self._cancel_all_timers()

    def on_reset(self) -> None:
        self._cancel_all_timers()
        self._schedules_by_primary.clear()
        self._primary_by_child.clear()
        self._primary_by_timer.clear()
        self._primaries_by_symbol.clear()

    def on_dispose(self) -> None:
        if self._is_owner_thread():
            self._cancel_all_timers()
            self._cached_clock = None
        self._schedules_by_primary.clear()
        self._primary_by_child.clear()
        self._primary_by_timer.clear()
        self._primaries_by_symbol.clear()

    def _cancel_all_timers(self) -> None:
        clk = self._safe_clock()
        if clk is None:
            return
        for timer_name in list(self._primary_by_timer.keys()):
            self._cancel_timer_safe(timer_name)

    def _cancel_timer_safe(self, timer_name: str) -> None:
        if not timer_name:
            return
        clk = self._safe_clock()
        if clk is None:
            return
        try:
            timer_names_fn = getattr(clk, "timer_names", None)
            if callable(timer_names_fn):
                if timer_name in set(timer_names_fn()):
                    clk.cancel_timer(timer_name)
            elif hasattr(clk, "cancel_timer"):
                clk.cancel_timer(timer_name)
        except Exception:
            pass

    def _resolve_reference_price(self, state: SliceScheduleState) -> Optional[Decimal]:
        cache = getattr(self, "cache", None)
        clock = getattr(self, "clock", None)
        if cache is None:
            return None
        now_ns = int(clock.timestamp_ns()) if clock is not None else 0

        try:
            quote = cache.quote(state.instrument_id)
        except Exception:
            quote = None
        if quote is not None:
            q_ts = int(getattr(quote, "ts_init", 0) or getattr(quote, "ts_event", 0) or 0)
            is_fresh = now_ns <= 0 or q_ts <= 0 or (now_ns - q_ts) <= state.quote_stale_ns
            if is_fresh:
                bid_dec = _to_dec(getattr(quote, "bid_price", None), "0")
                ask_dec = _to_dec(getattr(quote, "ask_price", None), "0")
                if bid_dec > Decimal("0") and ask_dec > Decimal("0"):
                    return (bid_dec + ask_dec) / Decimal("2")
                raw_px = ask_dec if _is_buy_side(state.side) else bid_dec
                if raw_px > Decimal("0"):
                    return raw_px
                alt_px = bid_dec if _is_buy_side(state.side) else ask_dec
                if alt_px > Decimal("0"):
                    return alt_px

        try:
            trade = cache.trade(state.instrument_id)
        except Exception:
            trade = None
        if trade is not None:
            t_ts = int(getattr(trade, "ts_init", 0) or getattr(trade, "ts_event", 0) or 0)
            t_fresh = now_ns <= 0 or t_ts <= 0 or (now_ns - t_ts) <= state.quote_stale_ns
            if t_fresh:
                trade_px = _to_dec(getattr(trade, "price", None), "0")
                if trade_px > Decimal("0"):
                    return trade_px

        latest_bar = None
        latest_ts = -1
        bar_types: List[Any] = []
        for agg_src in (AggregationSource.EXTERNAL, AggregationSource.INTERNAL):
            try:
                found = cache.bar_types(
                    agg_src,
                    instrument_id=state.instrument_id,
                    price_type=PriceType.LAST,
                )
                if not found:
                    found = cache.bar_types(
                        agg_src,
                        instrument_id=state.instrument_id,
                    )
                if found:
                    bar_types.extend(found)
            except Exception:
                continue

        for bt in bar_types or ():
            try:
                bar = cache.bar(bt)
            except Exception:
                bar = None
            if bar is None:
                continue
            b_ts = int(getattr(bar, "ts_init", 0) or getattr(bar, "ts_event", 0) or 0)
            if b_ts >= latest_ts:
                latest_ts = b_ts
                latest_bar = bar

        if latest_bar is not None:
            close_dec = _to_dec(getattr(latest_bar, "close", None), "0")
            if close_dec > Decimal("0"):
                return close_dec
        return None

    def _build_schedule_state(self, order: Any) -> SliceScheduleState:
        exec_params = getattr(order, "exec_algorithm_params", None) or {}
        if not isinstance(exec_params, dict):
            exec_params = {}

        order_limit = _to_dec(getattr(order, "price", None), "0")
        worst_override = exec_params.get("worst_limit_price")
        if worst_override is not None and str(worst_override).strip() != "":
            cand_worst = _to_dec(worst_override, "0")
            if cand_worst > Decimal("0"):
                worst_limit_px = (
                    min(order_limit, cand_worst)
                    if _is_buy_side(order.side) and order_limit > Decimal("0")
                    else (
                        max(order_limit, cand_worst)
                        if not _is_buy_side(order.side) and order_limit > Decimal("0")
                        else cand_worst
                    )
                )
            else:
                worst_limit_px = order_limit
        else:
            worst_limit_px = order_limit

        cache = getattr(self, "cache", None)
        instrument = None
        if cache is not None:
            try:
                instrument = cache.instrument(order.instrument_id)
            except Exception:
                instrument = None

        if instrument is not None:
            inst_tick = _to_dec(getattr(instrument, "price_increment", None), "0")
            tick_size: Optional[Decimal] = inst_tick if inst_tick > Decimal("0") else None
            price_precision = int(getattr(instrument, "price_precision", 2) or 2)
            size_precision = int(getattr(instrument, "size_precision", 0) or 0)
        else:
            tick_size = rule_612_tick_size(worst_limit_px)
            price_precision = 4 if worst_limit_px < Decimal("1.00") else 2
            size_precision = 0

        horizon_secs = max(
            1.0,
            float(exec_params.get("horizon_secs") or self._algo_config.horizon_secs),
        )
        interval_secs = max(
            1.0,
            float(exec_params.get("interval_secs") or self._algo_config.interval_secs),
        )
        quote_stale_secs = max(
            0.1,
            float(exec_params.get("quote_stale_secs") or self._algo_config.quote_stale_secs),
        )
        raw_collar_pct = exec_params.get("limit_collar_pct")
        if raw_collar_pct is None or str(raw_collar_pct).strip() == "":
            raw_collar_pct = exec_params.get("collar_pct")
        collar_pct = _to_dec(
            raw_collar_pct, str(self._algo_config.default_collar_pct)
        )
        raw_collar_atr = exec_params.get("limit_collar_atr")
        if raw_collar_atr is None or str(raw_collar_atr).strip() == "":
            raw_collar_atr = exec_params.get("collar_atr")
        collar_atr = _to_dec(
            raw_collar_atr, str(self._algo_config.default_collar_atr)
        )
        raw_atr = exec_params.get("atr")
        atr_dec = (
            _to_dec(raw_atr, "0")
            if raw_atr is not None and str(raw_atr).strip() != ""
            else None
        )
        raw_daily_atr = exec_params.get("daily_atr_14")
        daily_atr_dec = (
            _to_dec(raw_daily_atr, "0")
            if raw_daily_atr is not None and str(raw_daily_atr).strip() != ""
            else None
        )
        daily_atr_14 = (
            atr_dec
            if atr_dec is not None and atr_dec > Decimal("0")
            else (
                daily_atr_dec
                if daily_atr_dec is not None and daily_atr_dec > Decimal("0")
                else None
            )
        )

        total_qty = _to_dec(getattr(order, "quantity", None), "0")
        min_slice = self._fee_schedule.breakeven_slice_shares
        exec_mode = str(
            exec_params.get("exec_algorithm") or ExecAlgorithm.COLLARED_TWAP.value
        ).strip().upper()
        n_target = max(1, int(round(horizon_secs / interval_secs)))
        if (
            exec_mode == ExecAlgorithm.DIRECT_COLLARED_LIMIT.value
            or total_qty < Decimal(str(2 * min_slice))
        ):
            n_eff = 1
        else:
            n_eff = max(1, min(n_target, int(total_qty // Decimal(str(min_slice)))))

        clk = self._safe_clock()
        horizon_end_ns: Optional[int] = (
            int(clk.timestamp_ns()) + int(round(horizon_secs * 1_000_000_000))
            if clk is not None and horizon_secs > 0
            else None
        )
        primary_cid = str(order.client_order_id)
        sym = _normalize_symbol(order.instrument_id)
        return SliceScheduleState(
            primary_cid=primary_cid,
            instrument_id=order.instrument_id,
            symbol=sym,
            side=order.side,
            total_qty=total_qty,
            worst_limit_price=worst_limit_px,
            tick_size=tick_size,
            price_precision=price_precision,
            size_precision=size_precision,
            min_slice_shares=min_slice,
            target_slices=n_eff,
            interval_ns=int(round(interval_secs * 1_000_000_000)),
            collar_pct=collar_pct,
            collar_atr=collar_atr,
            daily_atr_14=daily_atr_14,
            quote_stale_ns=int(round(quote_stale_secs * 1_000_000_000)),
            horizon_end_ns=horizon_end_ns,
            timer_name=f"collared_twap_{primary_cid}",
        )

    def _schedule_next_interval(self, state: SliceScheduleState) -> None:
        clk = self._safe_clock()
        if clk is None or state.completed or state.cancel_requested:
            return
        self._cancel_timer_safe(state.timer_name)
        now_ns = int(clk.timestamp_ns())
        alert_ns = now_ns + state.interval_ns
        self._primary_by_timer[state.timer_name] = state.primary_cid
        clk.set_time_alert_ns(
            name=state.timer_name,
            alert_time_ns=alert_ns,
            allow_past=True,
        )

    def _cleanup_schedule(self, primary_cid: str) -> None:
        state = self._schedules_by_primary.pop(primary_cid, None)
        if state is None:
            return
        self._primary_by_timer.pop(state.timer_name, None)
        self._cancel_timer_safe(state.timer_name)
        for ch_cid in state.child_cids:
            self._primary_by_child.pop(ch_cid, None)
        sym_set = self._primaries_by_symbol.get(state.symbol)
        if sym_set is not None:
            sym_set.discard(primary_cid)
            if not sym_set:
                self._primaries_by_symbol.pop(state.symbol, None)

    def _resolve_gate_reference_price(self, state: SliceScheduleState) -> Optional[Decimal]:
        """Mirrors `RustPlanGateEngine`'s cache reference-price precedence (`Mid` -> `Trade`)."""
        cache = getattr(self, "cache", None)
        if cache is None:
            return None
        price_fn = getattr(cache, "price", None)
        if callable(price_fn):
            try:
                mid_obj = price_fn(state.instrument_id, PriceType.MID)
                mid_dec = _to_dec(mid_obj, "0")
                if mid_dec > Decimal("0"):
                    return mid_dec
            except Exception:
                pass
        trade_fn = getattr(cache, "trade", None)
        if callable(trade_fn):
            try:
                trade_obj = trade_fn(state.instrument_id)
                trade_px = _to_dec(getattr(trade_obj, "price", None), "0")
                if trade_px > Decimal("0"):
                    return trade_px
            except Exception:
                pass
        return None

    def _resolve_bounded_primary_price(
        self,
        state: SliceScheduleState,
        *,
        cur_px: Decimal,
        dyn_px: Optional[Decimal],
    ) -> Optional[Decimal]:
        """Returns the collar-bounded price for direct `primary` submission, or `None` if unchanged."""
        is_buy = _is_buy_side(state.side)
        candidate: Optional[Decimal] = dyn_px

        quantized_worst = state._quantize_for_side(state.worst_limit_price)
        if candidate is None and quantized_worst > Decimal("0"):
            if is_buy and cur_px > quantized_worst:
                candidate = quantized_worst
            elif not is_buy and (cur_px <= Decimal("0") or cur_px < quantized_worst):
                candidate = quantized_worst

        gate_ref_px = self._resolve_gate_reference_price(state)
        if gate_ref_px is not None and gate_ref_px > Decimal("0"):
            gate_px = state.compute_slice_limit_price(gate_ref_px)
            if gate_px is not None and gate_px > Decimal("0"):
                effective = candidate if candidate is not None else cur_px
                if is_buy and (effective <= Decimal("0") or gate_px < effective):
                    candidate = gate_px
                elif not is_buy and (effective <= Decimal("0") or gate_px > effective):
                    candidate = gate_px

        if candidate is not None and cur_px > Decimal("0"):
            candidate = min(candidate, cur_px) if is_buy else max(candidate, cur_px)
            candidate = state._quantize_for_side(candidate)
        elif candidate is None and cur_px >= Decimal("1.00"):
            q_cur = state._quantize_for_side(cur_px)
            if q_cur != cur_px:
                candidate = q_cur

        return candidate

    def _submit_final_primary(self, state: SliceScheduleState, primary: Any) -> None:
        if hasattr(primary, "price"):
            cur_price_obj = getattr(primary, "price", None)
            cur_px = _to_dec(cur_price_obj, "0")
            ref_px = self._resolve_reference_price(state)
            dyn_px = state.compute_slice_limit_price(ref_px)
            target_px = self._resolve_bounded_primary_price(
                state,
                cur_px=cur_px,
                dyn_px=dyn_px,
            )
            if (
                target_px is not None
                and target_px > Decimal("0")
                and getattr(primary, "status", None)
                in (OrderStatus.INITIALIZED, OrderStatus.RELEASED)
            ):
                new_price = _format_price(target_px, state.price_precision)
                if cur_price_obj is None or new_price != cur_price_obj:
                    self.modify_order_in_place(primary, price=new_price)
                    # `modify_order_in_place` updates Cache in Rust without mutating the Python handle;
                    # re-fetch from Cache so `submit_order(primary)` does not overwrite Cache with stale price.
                    cache = getattr(self, "cache", None)
                    if cache is not None:
                        cid = getattr(primary, "client_order_id", None) or ClientOrderId(
                            state.primary_cid
                        )
                        refreshed = cache.order(cid)
                        if refreshed is not None:
                            primary = refreshed
        state.completed = True
        self._primary_by_timer.pop(state.timer_name, None)
        self._cancel_timer_safe(state.timer_name)
        self.submit_order(primary)

    def _spawn_or_advance_slice(self, state: SliceScheduleState, primary: Any) -> None:
        if state.completed or state.cancel_requested:
            return
        if primary is None or getattr(primary, "is_closed", False):
            state.completed = True
            self._cleanup_schedule(state.primary_cid)
            return

        rem_qty = _to_dec(getattr(primary, "leaves_qty", None), "0")
        if rem_qty <= Decimal("0"):
            state.completed = True
            self._cleanup_schedule(state.primary_cid)
            return

        clk = self._safe_clock()
        horizon_expired = (
            state.horizon_end_ns is not None
            and clk is not None
            and int(clk.timestamp_ns()) >= state.horizon_end_ns
        )
        is_final = (
            horizon_expired
            or state.slice_index >= (state.target_slices - 1)
            or rem_qty < Decimal(str(2 * state.min_slice_shares))
        )
        if is_final:
            self._submit_final_primary(state, primary)
            return

        ref_px = self._resolve_reference_price(state)
        dyn_px = state.compute_slice_limit_price(ref_px)
        if dyn_px is None:
            # Market price currently exceeds worst_limit_price: wait until next interval without chasing.
            self._schedule_next_interval(state)
            return

        gate_ref_px = self._resolve_gate_reference_price(state)
        if gate_ref_px is not None and gate_ref_px > Decimal("0"):
            gate_px = state.compute_slice_limit_price(gate_ref_px)
            if gate_px is not None and gate_px > Decimal("0"):
                dyn_px = (
                    min(dyn_px, gate_px)
                    if _is_buy_side(state.side)
                    else max(dyn_px, gate_px)
                )

        slice_qty = state.compute_next_slice_qty(rem_qty, is_final=False)
        if slice_qty >= rem_qty:
            self._submit_final_primary(state, primary)
            return

        child = self.spawn_limit(
            primary,
            quantity=_format_quantity(slice_qty, state.size_precision),
            price=_format_price(dyn_px, state.price_precision),
            time_in_force=TimeInForce.DAY,
            reduce_only=bool(getattr(primary, "is_reduce_only", False)),
            tags=getattr(primary, "tags", None),
            reduce_primary=True,
        )
        child_cid = str(child.client_order_id)
        state.active_child_cid = child_cid
        state.waiting_for_cancel = False
        state.child_cids.append(child_cid)
        self._primary_by_child[child_cid] = state.primary_cid
        self.submit_order(child)
        self._schedule_next_interval(state)

    def on_order(self, order: Any) -> None:
        if order is None:
            return
        if _is_market_order(order):
            self.deny_order(
                order,
                "UNCOLLARED_MARKET_ENTRY_DENIED: CollaredTwapExecAlgorithm requires a collared LimitOrder",
            )
            return

        state = self._build_schedule_state(order)
        if state.worst_limit_price <= Decimal("0"):
            self.deny_order(
                order,
                "INVALID_LIMIT_PRICE: CollaredTwapExecAlgorithm requires positive limit price",
            )
            return

        if state.target_slices <= 1:
            self._submit_final_primary(state, order)
            return

        self._schedules_by_primary[state.primary_cid] = state
        self._primaries_by_symbol.setdefault(state.symbol, set()).add(state.primary_cid)
        self._spawn_or_advance_slice(state, order)

    def on_time_event(self, event: Any) -> None:
        timer_name = str(getattr(event, "name", "") or "")
        primary_cid = self._primary_by_timer.pop(timer_name, None)
        if not primary_cid:
            return
        state = self._schedules_by_primary.get(primary_cid)
        if state is None or state.completed or state.cancel_requested:
            return

        cache = getattr(self, "cache", None)
        primary = cache.order(ClientOrderId(primary_cid)) if cache is not None else None
        if primary is None or getattr(primary, "is_closed", False):
            self._cleanup_schedule(primary_cid)
            return

        if state.active_child_cid is not None:
            child = (
                cache.order(ClientOrderId(state.active_child_cid))
                if cache is not None
                else None
            )
            if child is not None:
                if bool(getattr(child, "is_pending_cancel", False)):
                    self._schedule_next_interval(state)
                    return
                if bool(getattr(child, "is_inflight", False)) and not bool(
                    getattr(child, "is_open", False)
                ):
                    self._schedule_next_interval(state)
                    return
                if bool(getattr(child, "is_open", False)):
                    state.waiting_for_cancel = True
                    self.cancel_order(child)
                    return
                if not bool(getattr(child, "is_closed", False)):
                    self._schedule_next_interval(state)
                    return
            self._primary_by_child.pop(state.active_child_cid, None)
            state.active_child_cid = None
            state.waiting_for_cancel = False

        self._spawn_or_advance_slice(state, primary)

    def _deny_held_primary_if_open(self, primary_cid: str, reason: str) -> None:
        cache = getattr(self, "cache", None)
        if cache is None or not primary_cid:
            return
        primary = cache.order(ClientOrderId(primary_cid))
        if primary is None or bool(getattr(primary, "is_closed", False)):
            return
        status = getattr(primary, "status", None)
        if status in (OrderStatus.INITIALIZED, OrderStatus.RELEASED):
            self.deny_order(primary, reason)
        elif self._is_cancelable_order(primary):
            self.cancel_order(primary)

    def on_order_canceled(self, event: Any) -> None:
        cid = str(getattr(event, "client_order_id", "") or "")
        if not cid:
            return

        if cid in self._schedules_by_primary:
            state = self._schedules_by_primary[cid]
            state.cancel_requested = True
            state.completed = True
            self._cancel_active_child(state)
            self._cleanup_schedule(cid)
            return

        primary_cid = self._primary_by_child.pop(cid, None)
        if not primary_cid:
            return
        state = self._schedules_by_primary.get(primary_cid)
        if state is None:
            return
        if state.active_child_cid == cid:
            state.active_child_cid = None
            state.waiting_for_cancel = False
        if state.cancel_requested or state.completed:
            self._deny_held_primary_if_open(
                primary_cid,
                f"CollaredTwap child slice {cid} canceled during aborted schedule",
            )
            self._cleanup_schedule(primary_cid)
            return

        cache = getattr(self, "cache", None)
        primary = cache.order(ClientOrderId(primary_cid)) if cache is not None else None
        if primary is None or getattr(primary, "is_closed", False):
            self._cleanup_schedule(primary_cid)
            return

        state.slice_index += 1
        self._spawn_or_advance_slice(state, primary)

    def on_order_cancel_rejected(self, event: Any) -> None:
        cid = str(getattr(event, "client_order_id", "") or "")
        if not cid:
            return

        if cid in self._schedules_by_primary:
            state = self._schedules_by_primary[cid]
            state.waiting_for_cancel = False
            if state.cancel_requested or state.completed:
                cache = getattr(self, "cache", None)
                primary = cache.order(ClientOrderId(cid)) if cache is not None else None
                if primary is None or bool(getattr(primary, "is_closed", False)):
                    self._cleanup_schedule(cid)
            return

        primary_cid = self._primary_by_child.get(cid)
        if not primary_cid:
            return
        state = self._schedules_by_primary.get(primary_cid)
        if state is None:
            return
        if state.active_child_cid == cid:
            state.waiting_for_cancel = False

        cache = getattr(self, "cache", None)
        primary = cache.order(ClientOrderId(primary_cid)) if cache is not None else None
        if primary is None or bool(getattr(primary, "is_closed", False)):
            self._cleanup_schedule(primary_cid)
            return

        child = cache.order(ClientOrderId(cid)) if cache is not None else None
        if state.cancel_requested or state.completed:
            if child is None or bool(getattr(child, "is_closed", False)):
                self._primary_by_child.pop(cid, None)
                if state.active_child_cid == cid:
                    state.active_child_cid = None
                self._cleanup_schedule(primary_cid)
            return

        if child is not None and not bool(getattr(child, "is_closed", False)):
            # Child is still open/working on the venue after cancel rejection; reschedule next check.
            self._schedule_next_interval(state)
            return

        self._primary_by_child.pop(cid, None)
        if state.active_child_cid == cid:
            state.active_child_cid = None
        if _to_dec(getattr(primary, "leaves_qty", None), "0") <= Decimal("0"):
            self._cleanup_schedule(primary_cid)
            return
        state.slice_index += 1
        self._spawn_or_advance_slice(state, primary)

    def on_order_filled(self, event: Any) -> None:
        cid = str(getattr(event, "client_order_id", "") or "")
        if not cid:
            return

        if cid in self._schedules_by_primary:
            cache = getattr(self, "cache", None)
            primary = cache.order(ClientOrderId(cid)) if cache is not None else None
            if primary is None or _to_dec(getattr(primary, "leaves_qty", None), "0") <= Decimal("0"):
                self._cleanup_schedule(cid)
            return

        primary_cid = self._primary_by_child.get(cid)
        if not primary_cid:
            return
        state = self._schedules_by_primary.get(primary_cid)
        if state is None:
            return
        cache = getattr(self, "cache", None)
        child = cache.order(ClientOrderId(cid)) if cache is not None else None
        child_leaves = (
            _to_dec(getattr(child, "leaves_qty", None), "0")
            if child is not None
            else Decimal("0")
        )
        if child_leaves > Decimal("0") and bool(getattr(child, "is_open", False)):
            return

        was_waiting = False
        self._primary_by_child.pop(cid, None)
        if state.active_child_cid == cid:
            was_waiting = bool(state.waiting_for_cancel)
            state.active_child_cid = None
            state.waiting_for_cancel = False
        if state.cancel_requested or state.completed:
            self._deny_held_primary_if_open(
                primary_cid,
                f"CollaredTwap child slice {cid} filled on completed/canceled schedule",
            )
            self._cleanup_schedule(primary_cid)
            return

        primary = cache.order(ClientOrderId(primary_cid)) if cache is not None else None
        if primary is None or _to_dec(getattr(primary, "leaves_qty", None), "0") <= Decimal("0"):
            self._cleanup_schedule(primary_cid)
            return

        state.slice_index += 1
        if was_waiting:
            self._spawn_or_advance_slice(state, primary)
        elif state.timer_name not in self._primary_by_timer:
            self._schedule_next_interval(state)

    def on_order_denied(self, event: Any) -> None:
        self._handle_child_or_parent_failure(event, terminal_abort=True)

    def on_order_rejected(self, event: Any) -> None:
        self._handle_child_or_parent_failure(event, terminal_abort=True)

    def on_order_expired(self, event: Any) -> None:
        self._handle_child_or_parent_failure(event, terminal_abort=False)

    def _handle_child_or_parent_failure(
        self,
        event: Any,
        *,
        terminal_abort: bool,
    ) -> None:
        cid = str(getattr(event, "client_order_id", "") or "")
        if not cid:
            return
        if cid in self._schedules_by_primary:
            state = self._schedules_by_primary[cid]
            state.cancel_requested = True
            state.completed = True
            self._cancel_active_child(state)
            self._cleanup_schedule(cid)
            return

        primary_cid = self._primary_by_child.pop(cid, None)
        if not primary_cid:
            return
        state = self._schedules_by_primary.get(primary_cid)
        if state is None:
            return
        if state.active_child_cid == cid:
            state.active_child_cid = None
            state.waiting_for_cancel = False

        if terminal_abort or state.cancel_requested or state.completed:
            state.cancel_requested = True
            state.completed = True
            self._deny_held_primary_if_open(
                primary_cid,
                f"CollaredTwap child slice {cid} aborted ({'terminal' if terminal_abort else 'canceled'})",
            )
            self._cleanup_schedule(primary_cid)
            return

        cache = getattr(self, "cache", None)
        primary = cache.order(ClientOrderId(primary_cid)) if cache is not None else None
        if primary is None or getattr(primary, "is_closed", False):
            self._cleanup_schedule(primary_cid)
            return
        state.slice_index += 1
        self._spawn_or_advance_slice(state, primary)

    @staticmethod
    def _is_cancelable_order(order_obj: Any) -> bool:
        if order_obj is None:
            return False
        if bool(getattr(order_obj, "is_closed", False)):
            return False
        if bool(getattr(order_obj, "is_pending_cancel", False)):
            return False
        status = getattr(order_obj, "status", None)
        if status == OrderStatus.INITIALIZED:
            return False
        status_str = str(status or "").upper()
        if "." in status_str:
            status_str = status_str.rsplit(".", 1)[-1]
        if status_str in {
            "INITIALIZED",
            "PENDING_CANCEL",
            "DENIED",
            "REJECTED",
            "CANCELED",
            "CANCELLED",
            "EXPIRED",
            "FILLED",
            "VOIDED",
        }:
            return False
        return bool(
            getattr(order_obj, "is_open", False)
            or getattr(order_obj, "is_inflight", False)
            or status_str in {"SUBMITTED", "ACCEPTED", "TRIGGERED", "PENDING_UPDATE", "PARTIALLY_FILLED"}
        )

    def _cancel_active_child(self, state: SliceScheduleState) -> None:
        cache = getattr(self, "cache", None)
        if cache is None:
            return
        sent_cids: Set[str] = set()
        if state.active_child_cid:
            child = cache.order(ClientOrderId(state.active_child_cid))
            if self._is_cancelable_order(child):
                sent_cids.add(state.active_child_cid)
                self.cancel_order(child)
        try:
            spawned = cache.orders_for_exec_spawn(ClientOrderId(state.primary_cid)) or ()
        except Exception:
            spawned = ()
        for order_obj in spawned:
            cid = str(getattr(order_obj, "client_order_id", "") or "")
            if cid == state.primary_cid or cid in sent_cids:
                continue
            if self._is_cancelable_order(order_obj):
                if cid:
                    sent_cids.add(cid)
                self.cancel_order(order_obj)

    def cancel_symbol_entries(self, symbol: str) -> int:
        """Cascade-cancels active slice timers and all open/in-flight entry orders for `symbol`."""
        norm_sym = _normalize_symbol(symbol)
        if not norm_sym:
            return 0
        primary_cids = list(self._primaries_by_symbol.get(norm_sym, ()))
        canceled = 0
        canceled_cids: Set[str] = set()
        cache = getattr(self, "cache", None)

        for primary_cid in primary_cids:
            state = self._schedules_by_primary.get(primary_cid)
            if state is not None:
                state.cancel_requested = True
                state.completed = True
                self._cancel_timer_safe(state.timer_name)
                self._primary_by_timer.pop(state.timer_name, None)
                self._cancel_active_child(state)
                if state.active_child_cid:
                    canceled_cids.add(state.active_child_cid)
                canceled += 1
            if cache is not None:
                primary = cache.order(ClientOrderId(primary_cid))
                if self._is_cancelable_order(primary):
                    self.cancel_order(primary)
                    canceled_cids.add(primary_cid)
                    canceled += 1
                elif (
                    primary is not None
                    and not bool(getattr(primary, "is_closed", False))
                    and getattr(primary, "status", None)
                    in (OrderStatus.INITIALIZED, OrderStatus.RELEASED)
                ):
                    self.deny_order(
                        primary,
                        f"CollaredTwap symbol entry canceled for {norm_sym}",
                    )
                    canceled_cids.add(primary_cid)
                    canceled += 1
            self._cleanup_schedule(primary_cid)

        # Cancel all open and in-flight non-reduce_only entry orders for `symbol` regardless of
        # `exec_algorithm_id` (covering both `COLLARED_TWAP` and `DIRECT_COLLARED_LIMIT`).
        if cache is not None:
            candidates: List[Any] = []
            seen_keys: Set[str] = set()
            for fn_name in ("orders_open", "orders_inflight", "orders"):
                fn = getattr(cache, fn_name, None)
                if not callable(fn):
                    continue
                try:
                    ord_seq = fn() or ()
                except Exception:
                    continue
                for ord_obj in ord_seq:
                    cid = str(getattr(ord_obj, "client_order_id", "") or "")
                    key = cid if cid else f"id:{id(ord_obj)}"
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    candidates.append(ord_obj)

            for ord_obj in candidates:
                if _normalize_symbol(getattr(ord_obj, "instrument_id", "")) != norm_sym:
                    continue
                if not _is_entry_order_for_cancel(ord_obj):
                    continue
                cid = str(getattr(ord_obj, "client_order_id", "") or "")
                if cid and cid in canceled_cids:
                    continue
                if self._is_cancelable_order(ord_obj):
                    self.cancel_order(ord_obj)
                    if cid:
                        canceled_cids.add(cid)
                    canceled += 1

        return canceled
