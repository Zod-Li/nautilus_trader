"""Pre-trade PlanGateExecAlgorithm (`ExecAlgorithmId("PLAN_GATE")`) backed by Rust in-memory O(1) admission and async write-behind PostgreSQL persistence."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal
import json
import logging
import queue
import select
import threading
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import RealDictCursor, register_uuid

from nautilus_trader._libnautilus.quant_execution import (
    RustPlanGateDecision,
    RustPlanGateEngine,
    RustPlanGateOrderRequest,
    RustPlanItemEnvelope,
)
from nautilus_trader.model import (
    ExecAlgorithmId,
    OrderSide,
    OrderType,
    Price,
    TimeInForce,
)
from nautilus_trader.trading import ExecutionAlgorithm, ExecutionAlgorithmConfig

from evolution_engine.db.dsn import resolve_database_dsn
from nautilus_trader.quant.contracts.models import (
    PlanItemContract,
    PlanSide,
    StrategyArchetypeId,
    _to_decimal,
)

register_uuid()

logger = logging.getLogger(__name__)

PLAN_GATE_EXEC_ALGORITHM_ID = ExecAlgorithmId("PLAN_GATE")
ADMISSIBLE_ENTRY_ITEM_STATUSES = ("ARMED", "TRIGGERED", "PARTIALLY_FILLED")
EXIT_INTENT_TAGS = frozenset(
    {
        "EXIT",
        "STOP_LOSS",
        "TAKE_PROFIT",
        "PROTECTIVE_OCO",
        "PROTECTIVE_STOP",
        "STOP",
        "OCO_STOP",
        "REDUCE_ONLY",
        "THESIS_INVALIDATED_EXIT",
        "CARRY_OVER_EXIT",
    }
)
NOTIFY_CHANNEL = "nautilus_plan_gate_sync"


@dataclass(frozen=True)
class PlanGateOrderRequest:
    """Normalized pre-trade order admission request evaluated by a `PlanGateBackend`."""

    client_order_id: str
    strategy_id: str
    instrument_id: str
    symbol: str
    venue: str
    order_side: OrderSide
    order_type: OrderType
    quantity: Decimal
    is_reduce_only: bool
    evaluation_ts: datetime
    limit_price: Optional[Decimal] = None
    reference_price: Optional[Decimal] = None
    stop_loss_price: Optional[Decimal] = None
    atr: Optional[Decimal] = None
    collar_market_order: bool = True
    allow_uncollared_market_order: bool = False
    is_exit_intent: bool = False
    cache_open_entry_qty: Decimal = Decimal("0")
    cache_filled_entry_qty: Decimal = Decimal("0")

    def to_rust_request(self) -> RustPlanGateOrderRequest:
        eval_ts = (
            self.evaluation_ts
            if self.evaluation_ts.tzinfo is not None
            else self.evaluation_ts.replace(tzinfo=timezone.utc)
        )
        eval_ns = int(eval_ts.timestamp() * 1_000_000_000)
        effective_order_type = (
            "UNCOLLARED_ALLOWED"
            if (
                self.allow_uncollared_market_order
                and not self.collar_market_order
                and self.limit_price is None
            )
            else str(self.order_type)
        )
        return RustPlanGateOrderRequest(
            client_order_id=self.client_order_id,
            strategy_id=self.strategy_id,
            instrument_id=self.instrument_id,
            symbol=self.symbol,
            venue=self.venue,
            order_side=str(self.order_side),
            order_type=effective_order_type,
            quantity=str(self.quantity),
            is_reduce_only=bool(self.is_reduce_only),
            evaluation_ts_ns=eval_ns,
            evaluation_ts_iso=eval_ts.isoformat(),
            limit_price=str(self.limit_price) if self.limit_price is not None else None,
            reference_price=(
                str(self.reference_price) if self.reference_price is not None else None
            ),
            atr=str(self.atr) if self.atr is not None else None,
            collar_market_order=bool(self.collar_market_order),
            is_exit_intent=bool(self.is_exit_intent),
            cache_open_entry_qty=str(self.cache_open_entry_qty),
            cache_filled_entry_qty=str(self.cache_filled_entry_qty),
            stop_loss_price=(
                str(self.stop_loss_price)
                if self.stop_loss_price is not None
                else None
            ),
        )


@dataclass(frozen=True)
class PlanGateDecision:
    """Authoritative pre-trade admission decision returned by a `PlanGateBackend`."""

    allowed: bool
    reason: Optional[str] = None
    is_exit_bypass: bool = False
    derived_limit_price: Optional[Decimal] = None
    item_id: Optional[UUID] = None
    plan_id: Optional[UUID] = None

    @classmethod
    def from_rust_decision(cls, decision: RustPlanGateDecision) -> PlanGateDecision:
        return cls(
            allowed=bool(decision.allowed),
            reason=decision.reason,
            is_exit_bypass=bool(decision.is_exit_bypass),
            derived_limit_price=(
                Decimal(decision.derived_limit_price)
                if decision.derived_limit_price is not None
                else None
            ),
            item_id=UUID(decision.item_id) if decision.item_id is not None else None,
            plan_id=UUID(decision.plan_id) if decision.plan_id is not None else None,
        )


def build_stat_arb_secondary_plan_item(
    primary_item: PlanItemContract,
) -> Optional[PlanItemContract]:
    """Derives the paired secondary hedge `PlanItemContract` envelope for a `STAT_ARB_PAIRS` primary item."""
    cfg = primary_item.strategy_config
    if cfg.archetype_id != StrategyArchetypeId.STAT_ARB_PAIRS or not cfg.secondary_instrument_id:
        return None
    trace = primary_item.sizing_trace or {}
    if (
        str(trace.get("role", "")).strip() == "secondary_leg"
        or bool(trace.get("is_secondary_leg", False))
        or bool(trace.get("disable_secondary_hedge_envelope", False))
        or trace.get("register_secondary_hedge_envelope") is False
    ):
        return None
    sec_inst = str(cfg.secondary_instrument_id).strip()
    sec_sym = sec_inst.split(".", 1)[0].strip().upper()
    if not sec_sym or sec_sym == primary_item.symbol:
        return None

    env = primary_item.risk_envelope
    sec_side = (
        PlanSide.SHORT
        if env.side == PlanSide.LONG
        else PlanSide.LONG
    )
    sec_item_id = uuid5(
        NAMESPACE_URL,
        f"nautilus-secondary-leg:{primary_item.plan_id}:{primary_item.item_id}:{sec_sym}",
    )
    window_scale = Decimal(cfg.slow_window) / Decimal(max(cfg.fast_window, 1))
    beta_bound = max(Decimal("1"), cfg.atr_stop_multiplier, window_scale)
    raw_trace_beta = trace.get("hedge_beta", trace.get("beta"))
    if raw_trace_beta is not None:
        beta_bound = max(beta_bound, abs(_to_decimal(raw_trace_beta, "hedge_beta")))

    raw_sec_cap = trace.get("secondary_approved_qty_cap")
    sec_cap = (
        _to_decimal(raw_sec_cap, "approved_qty_cap")
        if raw_sec_cap is not None
        else (env.approved_qty_cap * beta_bound).to_integral_value()
    )
    sec_target_qty = max(env.target_qty, sec_cap)

    prim_ref_px = env.reference_price
    raw_sec_ref = trace.get("secondary_reference_price", trace.get("secondary_price"))
    sec_ref_px = (
        _to_decimal(raw_sec_ref, "secondary_reference_price")
        if raw_sec_ref is not None
        else prim_ref_px
    )
    prim_stop_dist = abs(prim_ref_px - env.initial_stop_price)
    prim_stop_ratio = (
        prim_stop_dist / prim_ref_px
        if prim_ref_px > Decimal("0")
        else (Decimal("1") - cfg.entry_quantile_threshold)
    )
    sec_stop_ratio = min(
        cfg.entry_quantile_threshold,
        max(
            prim_stop_ratio * cfg.atr_stop_multiplier,
            Decimal("1") - cfg.exit_quantile_threshold,
        ),
    )
    raw_sec_stop = trace.get("secondary_initial_stop_price")
    if raw_sec_stop is not None:
        sec_stop = _to_decimal(raw_sec_stop, "initial_stop_price")
    elif sec_side == PlanSide.SHORT:
        sec_stop = Decimal(f"{float(sec_ref_px * (Decimal('1') + sec_stop_ratio)):.2f}")
    else:
        sec_stop = Decimal(f"{float(sec_ref_px * (Decimal('1') - sec_stop_ratio)):.2f}")

    sec_stop_offset = abs(sec_ref_px - sec_stop)
    sec_notional = max(
        env.target_notional_usd,
        Decimal(f"{float(sec_target_qty * sec_ref_px):.2f}"),
    )
    raw_sec_budget = trace.get("secondary_max_risk_budget_usd")
    sec_risk_budget = (
        _to_decimal(raw_sec_budget, "max_risk_budget_usd")
        if raw_sec_budget is not None
        else min(
            env.max_risk_budget_usd,
            sec_notional,
        )
    )
    sec_status = str(trace.get("secondary_status", primary_item.status))
    sec_take_profit = (
        Decimal(f"{float(sec_ref_px * (Decimal('1') - prim_stop_ratio)):.2f}")
        if sec_side == PlanSide.SHORT
        else Decimal(f"{float(sec_ref_px * (Decimal('1') + sec_stop_ratio)):.2f}")
    )
    sec_envelope = replace(
        env,
        side=sec_side,
        target_qty=sec_target_qty,
        approved_qty_cap=sec_cap,
        target_notional_usd=sec_notional,
        max_risk_budget_usd=sec_risk_budget,
        initial_stop_price=sec_stop,
        take_profit_price=(
            sec_take_profit
            if env.take_profit_price is not None
            else None
        ),
    )
    sec_cfg = replace(
        cfg,
        instrument_id=sec_inst,
        secondary_instrument_id=cfg.instrument_id,
    )
    sec_trace = {
        **dict(trace),
        "role": "secondary_leg",
        "is_secondary_leg": True,
        "parent_plan_item_id": str(primary_item.item_id),
    }
    sec_ven = (
        sec_inst.split(".", 1)[1].strip().upper()
        if "." in sec_inst
        else primary_item.venue
    )
    return replace(
        primary_item,
        item_id=sec_item_id,
        symbol=sec_sym,
        venue=sec_ven,
        strategy_id=f"{primary_item.strategy_id}-{sec_sym}",
        status=sec_status,
        filled_qty=Decimal("0"),
        reserved_notional_usd=Decimal("0"),
        risk_envelope=sec_envelope,
        strategy_config=sec_cfg,
        sizing_trace=sec_trace,
    )


def _plan_item_to_rust_envelope(item: PlanItemContract) -> RustPlanItemEnvelope:
    valid_until = (
        item.risk_envelope.valid_until
        if item.risk_envelope.valid_until.tzinfo is not None
        else item.risk_envelope.valid_until.replace(tzinfo=timezone.utc)
    )
    return RustPlanItemEnvelope(
        item_id=str(item.item_id),
        plan_id=str(item.plan_id),
        symbol=item.symbol,
        side=item.risk_envelope.side.value,
        status=item.status,
        target_qty=str(item.risk_envelope.target_qty),
        approved_qty_cap=str(item.risk_envelope.approved_qty_cap),
        target_notional_usd=str(item.risk_envelope.target_notional_usd),
        filled_qty=str(item.filled_qty),
        valid_until_ns=int(valid_until.timestamp() * 1_000_000_000),
        valid_until_iso=valid_until.isoformat(),
        limit_collar_pct=str(item.risk_envelope.collar.limit_collar_pct),
        limit_collar_atr=str(item.risk_envelope.collar.limit_collar_atr),
        initial_stop_price=str(item.risk_envelope.initial_stop_price),
        max_risk_budget_usd=str(item.risk_envelope.max_risk_budget_usd),
    )


@dataclass
class _TrackedEntryReservation:
    """Tracks an active entry order reservation in `PlanGateExecAlgorithm`."""

    client_order_id: str
    plan_id: Optional[UUID]
    item_id: Optional[UUID]
    symbol: str
    reserved_qty: Decimal
    filled_qty: Decimal = Decimal("0")


class PlanGateBackend(Protocol):
    """Protocol implemented by `PostgresPlanGateBackend` and `SnapshotPlanGateBackend`."""

    def evaluate_and_reserve(self, request: PlanGateOrderRequest) -> PlanGateDecision:
        """Evaluate pre-trade admission and atomically reserve capacity on approval."""
        ...

    def release_reservation(
        self,
        symbol: str = "",
        quantity: Optional[Decimal | int | float | str] = None,
        *,
        release_qty: Optional[Decimal | int | float | str] = None,
        plan_id: Optional[UUID | str] = None,
        item_id: Optional[UUID | str] = None,
        reason: str = "",
    ) -> None:
        """Release reserved entry quantity when a previously admitted order is canceled or rejected."""
        ...

    def set_entry_freeze(
        self,
        active: bool,
        reason: str = "",
        updated_by: str = "nautilus:plan_gate",
    ) -> None:
        """Update the `ENTRY_FREEZE` state in the admission gate."""
        ...


def evaluate_entry_against_envelope(
    request: PlanGateOrderRequest,
    plan_item: Optional[PlanItemContract],
    entry_freeze_active: bool,
    entry_freeze_reason: str = "",
    prior_reserved_or_filled_qty: Decimal = Decimal("0"),
) -> PlanGateDecision:
    """Pure, deterministic 1:1 isomorphic admission evaluation backed by `RustPlanGateEngine`."""
    adjusted_item = (
        replace(plan_item, filled_qty=prior_reserved_or_filled_qty)
        if plan_item is not None
        else None
    )
    engine = RustPlanGateEngine(entry_freeze_active, entry_freeze_reason)
    if adjusted_item is not None:
        engine.upsert_plan_item(_plan_item_to_rust_envelope(adjusted_item), False)
    rust_decision = engine.evaluate_and_reserve(request.to_rust_request())
    return PlanGateDecision.from_rust_decision(rust_decision)


class SnapshotPlanGateBackend:
    """High-speed deterministic backend backed by `RustPlanGateEngine` for `BacktestEngine`."""

    def __init__(
        self,
        plan_items: Sequence[PlanItemContract] = (),
        entry_freeze: bool = False,
        entry_freeze_reason: str = "",
        trading_date: Optional[date] = None,
        allow_multi_cycle_reentry: bool = True,
        auto_register_stat_arb_secondary: bool = True,
    ) -> None:
        self._rust_engine = RustPlanGateEngine(bool(entry_freeze), str(entry_freeze_reason))
        self._trading_date: Optional[date] = trading_date
        self._allow_multi_cycle_reentry: bool = bool(allow_multi_cycle_reentry)
        self._auto_register_stat_arb_secondary: bool = bool(
            auto_register_stat_arb_secondary
        )
        self._removed_symbols: set[str] = set()
        self._items_by_symbol: Dict[str, PlanItemContract] = {}
        self._alerts: List[Dict[str, Any]] = []
        explicit_symbols = {item.symbol for item in plan_items}
        for item in plan_items:
            self.upsert_plan_item(item, _skip_auto_secondary=True)
        if self._auto_register_stat_arb_secondary:
            for item in plan_items:
                sec_item = build_stat_arb_secondary_plan_item(item)
                if sec_item is not None and sec_item.symbol not in explicit_symbols:
                    self.upsert_plan_item(sec_item, _skip_auto_secondary=True)

    @property
    def trading_date(self) -> Optional[date]:
        return self._trading_date

    def set_trading_date(
        self,
        trading_date: date,
        *,
        clear_runtime_items: bool = True,
        sync: bool = True,
    ) -> None:
        _ = (clear_runtime_items, sync)
        self._trading_date = trading_date

    @property
    def rust_engine(self) -> RustPlanGateEngine:
        return self._rust_engine

    @property
    def entry_freeze(self) -> bool:
        return self._rust_engine.entry_freeze

    def set_entry_freeze(
        self,
        active: bool,
        reason: str = "",
        updated_by: str = "nautilus:plan_gate",
    ) -> None:
        _ = updated_by
        self._rust_engine.set_entry_freeze(bool(active), str(reason))

    def upsert_plan_item(
        self,
        item: PlanItemContract,
        *,
        _skip_auto_secondary: bool = False,
    ) -> None:
        self._removed_symbols.discard(item.symbol)
        self._items_by_symbol[item.symbol] = item
        self._rust_engine.upsert_plan_item(_plan_item_to_rust_envelope(item), False)
        if not _skip_auto_secondary and self._auto_register_stat_arb_secondary:
            sec_item = build_stat_arb_secondary_plan_item(item)
            if (
                sec_item is not None
                and sec_item.symbol not in self._removed_symbols
                and (
                    sec_item.symbol not in self._items_by_symbol
                    or str(
                        self._items_by_symbol[sec_item.symbol].sizing_trace.get(
                            "parent_plan_item_id", ""
                        )
                    )
                    == str(item.item_id)
                )
            ):
                self._items_by_symbol[sec_item.symbol] = sec_item
                self._rust_engine.upsert_plan_item(
                    _plan_item_to_rust_envelope(sec_item), False
                )

    def upsert_runtime_plan_item(
        self,
        item: PlanItemContract,
        *,
        reserved_qty: Optional[Decimal] = None,
        preserve_higher_reservation: bool = False,
    ) -> None:
        adjusted_item = (
            replace(item, filled_qty=reserved_qty)
            if reserved_qty is not None
            else item
        )
        self._removed_symbols.discard(adjusted_item.symbol)
        self._items_by_symbol[adjusted_item.symbol] = adjusted_item
        self._rust_engine.upsert_plan_item(
            _plan_item_to_rust_envelope(adjusted_item),
            bool(preserve_higher_reservation),
        )
        if self._auto_register_stat_arb_secondary:
            sec_item = build_stat_arb_secondary_plan_item(adjusted_item)
            if (
                sec_item is not None
                and sec_item.symbol not in self._removed_symbols
                and (
                    sec_item.symbol not in self._items_by_symbol
                    or str(
                        self._items_by_symbol[sec_item.symbol].sizing_trace.get(
                            "parent_plan_item_id", ""
                        )
                    )
                    == str(adjusted_item.item_id)
                )
            ):
                self._items_by_symbol[sec_item.symbol] = sec_item
                self._rust_engine.upsert_plan_item(
                    _plan_item_to_rust_envelope(sec_item),
                    bool(preserve_higher_reservation),
                )

    def remove_runtime_plan_item(self, symbol: str) -> None:
        norm_sym = str(symbol or "").split(".", 1)[0].strip().upper()
        if not norm_sym:
            return
        self._removed_symbols.add(norm_sym)
        self._items_by_symbol.pop(norm_sym, None)
        prev_res = {
            s: Decimal(self._rust_engine.reserved_qty(s))
            for s in self._items_by_symbol
        }
        self._rust_engine.clear_plan_items()
        for s, item in list(self._items_by_symbol.items()):
            preserved = max(item.filled_qty, prev_res.get(s, Decimal("0")))
            adjusted = replace(item, filled_qty=preserved)
            self._items_by_symbol[s] = adjusted
            self._rust_engine.upsert_plan_item(
                _plan_item_to_rust_envelope(adjusted),
                False,
            )

    def get_plan_item(self, symbol: str) -> Optional[PlanItemContract]:
        norm_sym = str(symbol or "").split(".", 1)[0].strip().upper()
        return self._items_by_symbol.get(norm_sym)

    get_plan_item_for_instrument = get_plan_item

    def reserved_qty(self, symbol: str) -> Decimal:
        norm_sym = str(symbol or "").split(".", 1)[0].strip().upper()
        return Decimal(self._rust_engine.reserved_qty(norm_sym))

    @property
    def alerts(self) -> List[Dict[str, Any]]:
        return list(self._alerts)

    def evaluate_and_reserve(self, request: PlanGateOrderRequest) -> PlanGateDecision:
        rust_decision = self._rust_engine.evaluate_and_reserve(request.to_rust_request())
        decision = PlanGateDecision.from_rust_decision(rust_decision)
        if decision.allowed and not decision.is_exit_bypass:
            item = self._items_by_symbol.get(request.symbol)
            if item is not None and item.status == "ARMED":
                self._items_by_symbol[request.symbol] = replace(item, status="TRIGGERED")
        elif not decision.allowed:
            self._alerts.append(
                {
                    "event_type": "PLAN_GATE_ORDER_DENIED",
                    "severity": "CRITICAL" if self.entry_freeze else "WARN",
                    "source": "nautilus:plan_gate",
                    "title": f"PlanGate denied {request.order_side} {request.symbol}",
                    "body": decision.reason or "PLAN_GATE_DENIED",
                    "payload": {
                        "client_order_id": request.client_order_id,
                        "strategy_id": request.strategy_id,
                        "instrument_id": request.instrument_id,
                        "symbol": request.symbol,
                        "quantity": str(request.quantity),
                        "reason": decision.reason,
                    },
                }
            )
        self._rust_engine.drain_pending_mutations(64)
        return decision

    def release_reservation(
        self,
        symbol: str = "",
        quantity: Optional[Decimal | int | float | str] = None,
        *,
        release_qty: Optional[Decimal | int | float | str] = None,
        plan_id: Optional[UUID | str] = None,
        item_id: Optional[UUID | str] = None,
        reason: str = "",
    ) -> None:
        _ = (plan_id, reason)
        raw_qty = release_qty if release_qty is not None else quantity
        if raw_qty is None:
            return
        qty_dec = _to_decimal(raw_qty, "quantity")
        if qty_dec <= Decimal("0"):
            return
        resolved_symbol = str(symbol or "").strip().upper()
        if not resolved_symbol and item_id is not None:
            target_id = str(item_id)
            for item in self._items_by_symbol.values():
                if str(item.item_id) == target_id:
                    resolved_symbol = item.symbol
                    break
        if not resolved_symbol:
            return
        self._rust_engine.release_reservation(resolved_symbol, str(qty_dec))
        self._rust_engine.drain_pending_mutations(64)

    def release_closed_position_capacity(
        self,
        symbol: UUID | str = "",
        *,
        item_id: Optional[UUID | str] = None,
    ) -> None:
        """Releases consumed entry capacity in the backtest snapshot sandbox when a position closes flat."""
        if not self._allow_multi_cycle_reentry:
            return
        raw_arg = str(symbol or "").strip()
        resolved_symbol = raw_arg.upper()
        if resolved_symbol and resolved_symbol not in self._items_by_symbol:
            for item in self._items_by_symbol.values():
                if str(item.item_id) == raw_arg:
                    resolved_symbol = item.symbol
                    break
        if (not resolved_symbol or resolved_symbol not in self._items_by_symbol) and item_id is not None:
            target_id = str(item_id).strip()
            for item in self._items_by_symbol.values():
                if str(item.item_id) == target_id:
                    resolved_symbol = item.symbol
                    break
        if not resolved_symbol or resolved_symbol not in self._items_by_symbol:
            return
        cur_reserved = self.reserved_qty(resolved_symbol)
        if cur_reserved > Decimal("0"):
            self._rust_engine.release_reservation(resolved_symbol, str(cur_reserved))
            self._rust_engine.drain_pending_mutations(64)
        item = self._items_by_symbol.get(resolved_symbol)
        if item is not None:
            reset_item = replace(item, filled_qty=Decimal("0"), status="ARMED")
            self._items_by_symbol[resolved_symbol] = reset_item
            self._rust_engine.upsert_plan_item(
                _plan_item_to_rust_envelope(reset_item),
                False,
            )


class _AsyncPlanGateDbWorker:
    """Dedicated background write-behind worker holding a persistent PostgreSQL connection."""

    def __init__(
        self,
        dsn: str,
        market: str,
        trading_date: Optional[date],
        connect_timeout_secs: int,
        rust_engine: RustPlanGateEngine,
        on_write_failure: Optional[Callable[[str], None]] = None,
        on_freeze_persisted: Optional[Callable[[bool], None]] = None,
    ) -> None:
        self._dsn = dsn
        self._market = market
        self._trading_date = trading_date
        self._connect_timeout_secs = connect_timeout_secs
        self._rust_engine = rust_engine
        self._on_write_failure = on_write_failure
        self._on_freeze_persisted = on_freeze_persisted
        self._state_lock = threading.Lock()
        self._retry_buffer: List[Dict[str, Any]] = []
        self._has_failed_writes: bool = False
        self._in_flight_count: int = 0
        self._consecutive_failures: int = 0
        self._next_retry_monotonic: float = 0.0
        self._queue: "queue.Queue[Optional[Dict[str, Any]]]" = queue.Queue()
        self._write_conn: Optional[Any] = None
        self._thread = threading.Thread(
            target=self._run_loop,
            name="nautilus-plangate-async-db-writer",
            daemon=True,
        )
        self._thread.start()

    @property
    def pending_count(self) -> int:
        with self._state_lock:
            return len(self._retry_buffer) + self._in_flight_count + self._queue.qsize()

    def set_trading_date(self, trading_date: Optional[date]) -> None:
        with self._state_lock:
            self._trading_date = trading_date

    @property
    def has_failed_writes(self) -> bool:
        with self._state_lock:
            return self._has_failed_writes

    @property
    def has_failed_or_pending_freeze(self) -> bool:
        with self._state_lock:
            if self._has_failed_writes:
                return True
            return any(
                m.get("kind") == "SET_ENTRY_FREEZE" and bool(m.get("active", True))
                for m in self._retry_buffer
            )

    def enqueue_mutations(self, raw_mutations: Sequence[Any]) -> None:
        for raw in raw_mutations:
            try:
                parsed = dict(raw) if isinstance(raw, Mapping) else json.loads(raw)
                self._queue.put_nowait(parsed)
            except Exception as exc:
                logger.error("Failed to enqueue PlanGate mutation: %s", exc)

    def flush(self, timeout_secs: float = 5.0) -> None:
        if not self._thread.is_alive():
            return
        done = threading.Event()
        self._queue.put({"kind": "__FLUSH__", "event": done})
        done.wait(timeout=max(0.05, float(timeout_secs)))

    def close(self) -> None:
        self._queue.put(None)
        if self._thread.is_alive() and threading.current_thread() is not self._thread:
            self._thread.join(timeout=2.0)
        self._close_write_conn()

    def _close_write_conn(self) -> None:
        conn = self._write_conn
        self._write_conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def _ensure_conn(self) -> Any:
        if self._write_conn is None or getattr(self._write_conn, "closed", 1) != 0:
            self._write_conn = psycopg2.connect(
                self._dsn,
                cursor_factory=RealDictCursor,
                connect_timeout=self._connect_timeout_secs,
            )
        return self._write_conn

    def _handle_batch_failure(
        self,
        failed_mutations: Sequence[Dict[str, Any]],
        exc: Exception,
    ) -> None:
        reason = f"ASYNC_DB_WRITER_FAILURE: {exc}"
        logger.error(
            "PlanGate async write-behind failed (triggering ENTRY_FREEZE): %s", exc
        )
        self._close_write_conn()
        freeze_mutation = {
            "kind": "SET_ENTRY_FREEZE",
            "active": True,
            "reason": reason,
            "updated_by": "nautilus:plan_gate:async_writer",
            "alert_event_type": "PLAN_GATE_ASYNC_DB_WRITE_FAILED",
        }
        with self._state_lock:
            existing_retry = [
                dict(m)
                for m in self._retry_buffer
                if m is not None and m.get("kind") != "__FLUSH__"
            ]
            for m in failed_mutations:
                if m is not None and m.get("kind") != "__FLUSH__":
                    existing_retry.append(dict(m))
            deduped: List[Dict[str, Any]] = [
                m
                for m in existing_retry
                if not (
                    m.get("kind") == "SET_ENTRY_FREEZE"
                    and bool(m.get("active", True))
                    and m.get("alert_event_type") == "PLAN_GATE_ASYNC_DB_WRITE_FAILED"
                )
            ]
            deduped.append(freeze_mutation)
            if len(deduped) > 4096:
                deduped = deduped[-4096:]
            self._retry_buffer = deduped
            self._has_failed_writes = True
            self._in_flight_count = 0
            self._consecutive_failures += 1
            backoff = min(30.0, 0.5 * (2 ** min(self._consecutive_failures, 6)))
            self._next_retry_monotonic = time.monotonic() + backoff

        self._rust_engine.set_entry_freeze(True, reason)
        self._rust_engine.set_backend_unavailable(
            True, f"ENTRY_FREEZE_ACTIVE: {reason}"
        )
        if self._on_write_failure is not None:
            try:
                self._on_write_failure(reason)
            except Exception as cb_exc:
                logger.error("PlanGate write failure callback error: %s", cb_exc)

    def _flush_batch(self, batch: Sequence[Dict[str, Any]]) -> bool:
        with self._state_lock:
            combined: List[Dict[str, Any]] = list(self._retry_buffer)
            self._retry_buffer.clear()
            for m in batch:
                if m is not None and m.get("kind") != "__FLUSH__":
                    combined.append(dict(m))
            self._in_flight_count = len(combined)

        if not combined:
            with self._state_lock:
                self._in_flight_count = 0
            return True

        try:
            conn = self._ensure_conn()
            with conn:
                with conn.cursor() as cur:
                    for mutation in combined:
                        self._apply_mutation(cur, mutation)
            with self._state_lock:
                self._has_failed_writes = False
                self._in_flight_count = 0
                self._consecutive_failures = 0
                self._next_retry_monotonic = 0.0
            if self._on_freeze_persisted is not None:
                for m in combined:
                    if m.get("kind") == "SET_ENTRY_FREEZE":
                        try:
                            self._on_freeze_persisted(bool(m.get("active", True)))
                        except Exception:
                            pass
            return True
        except Exception as exc:
            self._handle_batch_failure(combined, exc)
            return False

    def _run_loop(self) -> None:
        try:
            while True:
                try:
                    item = self._queue.get(timeout=0.5)
                except queue.Empty:
                    now_mono = time.monotonic()
                    with self._state_lock:
                        has_retries = bool(self._retry_buffer)
                        should_retry = (
                            has_retries and now_mono >= self._next_retry_monotonic
                        )
                    if should_retry:
                        try:
                            self._flush_batch([])
                        except Exception as exc:
                            self._handle_batch_failure([], exc)
                    elif not has_retries:
                        self._close_write_conn()
                    continue
                if item is None:
                    self._queue.task_done()
                    break
                if item.get("kind") == "__FLUSH__":
                    with self._state_lock:
                        has_retries = bool(self._retry_buffer)
                    if has_retries:
                        try:
                            self._flush_batch([])
                        except Exception as exc:
                            self._handle_batch_failure([], exc)
                    ev = item.get("event")
                    if ev is not None:
                        ev.set()
                    self._queue.task_done()
                    continue

                batch = [item]
                flush_events: List[threading.Event] = []
                should_exit = False
                while True:
                    try:
                        nxt = self._queue.get_nowait()
                        if nxt is None:
                            self._queue.task_done()
                            should_exit = True
                            break
                        if nxt.get("kind") == "__FLUSH__":
                            ev = nxt.get("event")
                            if ev is not None:
                                flush_events.append(ev)
                            self._queue.task_done()
                            continue
                        batch.append(nxt)
                    except queue.Empty:
                        break

                try:
                    self._flush_batch(batch)
                except Exception as exc:
                    self._handle_batch_failure(batch, exc)
                finally:
                    for _ in batch:
                        self._queue.task_done()
                    for ev in flush_events:
                        ev.set()
                if should_exit:
                    break
        finally:
            self._close_write_conn()

    def _apply_mutation(self, cur: Any, mutation: Mapping[str, Any]) -> None:
        kind = mutation.get("kind")
        if kind == "ORDER_ADMITTED":
            cur.execute(
                """
                UPDATE trading_plan_items
                SET filled_qty = %s,
                    reserved_notional_usd = reserved_notional_usd + %s,
                    status = CASE WHEN status = 'ARMED' THEN 'TRIGGERED' ELSE status END,
                    updated_at = NOW()
                WHERE item_id = %s
                """,
                (
                    Decimal(str(mutation["new_reserved_qty"])),
                    Decimal(str(mutation["added_notional_usd"])),
                    mutation["item_id"],
                ),
            )
            cur.execute(
                """
                INSERT INTO trading_plan_events (
                    plan_id, item_id, event_type, actor, reason, payload
                ) VALUES (
                    %s, (SELECT item_id FROM trading_plan_items WHERE item_id = %s), 'ORDER_ADMITTED', %s, %s, %s::jsonb
                )
                """,
                (
                    mutation["plan_id"],
                    mutation["item_id"],
                    f"nautilus:plan_gate:{mutation['strategy_id']}",
                    "Pre-trade PlanGate admission passed",
                    json.dumps(
                        {
                            "client_order_id": mutation["client_order_id"],
                            "symbol": mutation["symbol"],
                            "quantity": mutation["quantity"],
                            "reserved_qty_after": mutation["new_reserved_qty"],
                        }
                    ),
                ),
            )
        elif kind == "ORDER_DENIED":
            payload_dict = {
                "client_order_id": mutation["client_order_id"],
                "strategy_id": mutation["strategy_id"],
                "instrument_id": mutation["instrument_id"],
                "symbol": mutation["symbol"],
                "quantity": mutation["quantity"],
                "reason": mutation["reason"],
                "item_id": mutation.get("item_id"),
                "plan_id": mutation.get("plan_id"),
            }
            cur.execute(
                """
                INSERT INTO alert_outbox (
                    event_type, severity, source, title, body, payload, dedup_key, status
                ) VALUES (
                    'PLAN_GATE_ORDER_DENIED', %s, 'nautilus:plan_gate', %s, %s, %s::jsonb, %s, 'PENDING'
                )
                """,
                (
                    mutation["severity"],
                    f"PlanGate denied {mutation['order_side']} {mutation['symbol']}",
                    mutation["reason"] or "PLAN_GATE_DENIED",
                    json.dumps(payload_dict),
                    f"plan_gate_deny:{mutation['client_order_id']}",
                ),
            )
            if mutation.get("plan_id") and mutation.get("item_id"):
                cur.execute(
                    """
                    INSERT INTO trading_plan_events (
                        plan_id, item_id, event_type, actor, reason, payload
                    ) VALUES (
                        %s, (SELECT item_id FROM trading_plan_items WHERE item_id = %s), 'ORDER_DENIED', %s, %s, %s::jsonb
                    )
                    """,
                    (
                        mutation["plan_id"],
                        mutation["item_id"],
                        f"nautilus:plan_gate:{mutation['strategy_id']}",
                        mutation["reason"] or "PLAN_GATE_DENIED",
                        json.dumps(payload_dict),
                    ),
                )
        elif kind == "RELEASE_RESERVATION":
            item_id = mutation.get("item_id")
            new_reserved = Decimal(str(mutation["new_reserved_qty"]))
            if item_id:
                cur.execute(
                    """
                    UPDATE trading_plan_items
                    SET filled_qty = %s,
                        updated_at = NOW()
                    WHERE item_id = %s
                    """,
                    (new_reserved, item_id),
                )
            elif self._trading_date is not None:
                cur.execute(
                    """
                    UPDATE trading_plan_items i
                    SET filled_qty = GREATEST(0, i.filled_qty - %s),
                        updated_at = NOW()
                    FROM trading_plans p
                    WHERE p.plan_id = i.plan_id
                      AND p.trading_date = %s
                      AND p.market = %s
                      AND p.status = 'LOCKED'
                      AND i.symbol = %s
                    """,
                    (Decimal(str(mutation["quantity"])), self._trading_date, self._market, mutation["symbol"]),
                )
        elif kind == "SET_ENTRY_FREEZE":
            active_val = bool(mutation.get("active", True))
            reason_val = str(mutation.get("reason") or "")
            updated_by_val = str(mutation.get("updated_by") or "nautilus:plan_gate")
            alert_event_type = str(
                mutation.get("alert_event_type") or "PLAN_GATE_ENTRY_FREEZE"
            )
            cur.execute(
                """
                INSERT INTO system_flags (
                    flag, active, reason, set_by, updated_at
                ) VALUES (
                    'ENTRY_FREEZE', %s, %s, %s, NOW()
                )
                ON CONFLICT (flag) DO UPDATE SET
                    active = EXCLUDED.active,
                    reason = EXCLUDED.reason,
                    set_by = EXCLUDED.set_by,
                    updated_at = NOW()
                """,
                (active_val, reason_val, updated_by_val),
            )
            cur.execute(
                """
                INSERT INTO system_flag_events (
                    flag, active, reason, set_by, ts
                ) VALUES (
                    'ENTRY_FREEZE', %s, %s, %s, NOW()
                )
                """,
                (active_val, reason_val, updated_by_val),
            )
            if active_val:
                alert_payload = {
                    "flag": "ENTRY_FREEZE",
                    "active": True,
                    "reason": reason_val,
                    "updated_by": updated_by_val,
                    "event_type": alert_event_type,
                }
                cur.execute(
                    """
                    INSERT INTO alert_outbox (
                        event_type, severity, source, title, body, payload, dedup_key, status
                    ) VALUES (
                        %s, 'CRITICAL', %s, %s, %s, %s::jsonb, %s, 'PENDING'
                    )
                    """,
                    (
                        alert_event_type,
                        updated_by_val,
                        f"PlanGate ENTRY_FREEZE activated ({updated_by_val})",
                        reason_val or "ENTRY_FREEZE activated",
                        json.dumps(alert_payload),
                        f"plan_gate_freeze:{updated_by_val}:{reason_val[:64]}",
                    ),
                )


class PostgresPlanGateBackend:
    """In-memory O(1) Rust `PlanGate` backend synchronized with PostgreSQL via `LISTEN/NOTIFY` and async write-behind persistence."""

    def __init__(
        self,
        dsn: Optional[str] = None,
        trading_date: Optional[date] = None,
        market: str = "US_EQUITY",
        connect_timeout_secs: int = 3,
        eager_flush: bool = False,
    ) -> None:
        self._dsn = resolve_database_dsn(dsn)
        self._trading_date = trading_date
        self._market = market
        self._connect_timeout_secs = max(1, int(connect_timeout_secs))
        self._eager_flush = bool(eager_flush)
        self._rust_engine = RustPlanGateEngine(False, "")
        self._listen_conn: Optional[Any] = None
        self._sync_lock = threading.Lock()
        self._monotonic_freeze_latched: bool = False
        self._monotonic_freeze_reason: str = ""
        self._db_freeze_persisted: bool = False
        self._known_symbols: set[str] = set()
        self._symbol_by_item_id: Dict[str, str] = {}
        self._items_by_symbol: Dict[str, PlanItemContract] = {}
        self._runtime_items_by_symbol: Dict[str, Tuple[PlanItemContract, Optional[Decimal]]] = {}
        self._removed_runtime_symbols: set[str] = set()
        self._async_writer = _AsyncPlanGateDbWorker(
            dsn=self._dsn,
            market=self._market,
            trading_date=self._trading_date,
            connect_timeout_secs=self._connect_timeout_secs,
            rust_engine=self._rust_engine,
            on_write_failure=self._on_async_write_failure,
            on_freeze_persisted=self._on_async_freeze_persisted,
        )
        self._db_worker = self._async_writer
        self._initialize_sync_and_triggers()

    def _on_async_write_failure(self, reason: str) -> None:
        self._monotonic_freeze_latched = True
        self._monotonic_freeze_reason = str(reason or "ASYNC_DB_WRITER_FAILURE")
        self._db_freeze_persisted = False
        self._rust_engine.set_entry_freeze(True, self._monotonic_freeze_reason)

    def _on_async_freeze_persisted(self, active: bool) -> None:
        if active:
            self._db_freeze_persisted = True

    @property
    def dsn(self) -> str:
        return self._dsn

    @property
    def trading_date(self) -> Optional[date]:
        return self._trading_date

    def set_trading_date(
        self,
        trading_date: date,
        *,
        clear_runtime_items: bool = True,
        sync: bool = True,
    ) -> None:
        """Hot-reloads `trading_date` on `PostgresPlanGateBackend` during daily rollover."""
        self._trading_date = trading_date
        self._async_writer.set_trading_date(trading_date)
        if clear_runtime_items:
            self._runtime_items_by_symbol.clear()
            self._removed_runtime_symbols.clear()
        if sync:
            self.sync_from_db(preserve_higher_reservation=False)

    def upsert_runtime_plan_item(
        self,
        item: PlanItemContract,
        *,
        reserved_qty: Optional[Decimal] = None,
        preserve_higher_reservation: bool = False,
    ) -> None:
        """Upserts an in-memory runtime `PlanItemContract` (e.g. `RENEWED_ACTIVE`,
        `CARRY_OVER_EXIT_ONLY`, or `THESIS_INVALIDATED_EXIT`) into `RustPlanGateEngine`
        and preserves it across subsequent `NOTIFY` syncs on the active `trading_date`.
        """
        adjusted_item = (
            replace(item, filled_qty=reserved_qty)
            if reserved_qty is not None
            else item
        )
        norm_sym = adjusted_item.symbol
        with self._sync_lock:
            self._removed_runtime_symbols.discard(norm_sym)
            self._runtime_items_by_symbol[norm_sym] = (adjusted_item, reserved_qty)
            self._rust_engine.upsert_plan_item(
                _plan_item_to_rust_envelope(adjusted_item),
                bool(preserve_higher_reservation),
            )
            self._known_symbols.add(norm_sym)
            self._symbol_by_item_id[str(adjusted_item.item_id)] = norm_sym
            self._items_by_symbol[norm_sym] = adjusted_item
            sec_item = build_stat_arb_secondary_plan_item(adjusted_item)
            if (
                sec_item is not None
                and sec_item.symbol not in self._removed_runtime_symbols
                and (
                    sec_item.symbol not in self._items_by_symbol
                    or str(
                        self._items_by_symbol[sec_item.symbol].sizing_trace.get(
                            "parent_plan_item_id", ""
                        )
                    )
                    == str(adjusted_item.item_id)
                )
            ):
                sec_sym = sec_item.symbol
                self._runtime_items_by_symbol[sec_sym] = (sec_item, None)
                self._rust_engine.upsert_plan_item(
                    _plan_item_to_rust_envelope(sec_item),
                    bool(preserve_higher_reservation),
                )
                self._known_symbols.add(sec_sym)
                self._symbol_by_item_id[str(sec_item.item_id)] = sec_sym
                self._items_by_symbol[sec_sym] = sec_item

    def remove_runtime_plan_item(self, symbol: str) -> None:
        """Removes a runtime `PlanItemContract` overlay for `symbol` and evicts it from `RustPlanGateEngine`."""
        norm_sym = str(symbol or "").split(".", 1)[0].strip().upper()
        if not norm_sym:
            return
        with self._sync_lock:
            self._removed_runtime_symbols.add(norm_sym)
            removed_runtime = self._runtime_items_by_symbol.pop(norm_sym, None)
            removed_item = self._items_by_symbol.pop(norm_sym, None)
            self._known_symbols.discard(norm_sym)
            if removed_runtime is not None:
                self._symbol_by_item_id.pop(str(removed_runtime[0].item_id), None)
            if removed_item is not None:
                self._symbol_by_item_id.pop(str(removed_item.item_id), None)
            prev_res = {
                s: Decimal(self._rust_engine.reserved_qty(s))
                for s in self._items_by_symbol
            }
            self._rust_engine.clear_plan_items()
            for s, item in list(self._items_by_symbol.items()):
                preserved = max(item.filled_qty, prev_res.get(s, Decimal("0")))
                adjusted = replace(item, filled_qty=preserved)
                self._items_by_symbol[s] = adjusted
                self._rust_engine.upsert_plan_item(
                    _plan_item_to_rust_envelope(adjusted),
                    False,
                )

    def get_plan_item(self, symbol: str) -> Optional[PlanItemContract]:
        norm_sym = str(symbol or "").split(".", 1)[0].strip().upper()
        return self._items_by_symbol.get(norm_sym)

    get_plan_item_for_instrument = get_plan_item

    @property
    def rust_engine(self) -> RustPlanGateEngine:
        return self._rust_engine

    @property
    def entry_freeze(self) -> bool:
        return bool(self._rust_engine.entry_freeze or self._monotonic_freeze_latched)

    @property
    def monotonic_freeze_latched(self) -> bool:
        return self._monotonic_freeze_latched

    @property
    def monotonic_freeze_reason(self) -> str:
        return self._monotonic_freeze_reason

    @property
    def backend_unavailable(self) -> bool:
        return self._rust_engine.backend_unavailable

    def reserved_qty(self, symbol: str) -> Decimal:
        norm_sym = str(symbol or "").split(".", 1)[0].strip().upper()
        if not norm_sym:
            return Decimal("0")
        return Decimal(self._rust_engine.reserved_qty(norm_sym))

    def latch_entry_freeze(
        self,
        reason: str = "",
        updated_by: str = "nautilus:plan_gate",
        alert_event_type: str = "PLAN_GATE_ENTRY_FREEZE",
    ) -> None:
        """Monotonically latches `ENTRY_FREEZE` in $O(1)$ Rust memory and enqueues DB persistence."""
        reason_str = str(reason or "ENTRY_FREEZE")
        self._monotonic_freeze_latched = True
        self._monotonic_freeze_reason = reason_str
        self._db_freeze_persisted = False
        self._rust_engine.set_entry_freeze(True, reason_str)
        self._async_writer.enqueue_mutations(
            [
                {
                    "kind": "SET_ENTRY_FREEZE",
                    "active": True,
                    "reason": reason_str,
                    "updated_by": str(updated_by or "nautilus:plan_gate"),
                    "alert_event_type": str(alert_event_type or "PLAN_GATE_ENTRY_FREEZE"),
                }
            ]
        )
        if self._eager_flush:
            self._async_writer.flush(timeout_secs=2.0)

    def trigger_protective_oco_freeze(self, symbol: str, reason: str) -> None:
        """Triggered by `ProtectiveOcoMonitor` when an open position loses protective stop coverage."""
        norm_sym = str(symbol or "").strip().upper()
        self.latch_entry_freeze(
            reason=str(reason or f"PROTECTIVE_OCO_BREACH for {norm_sym}"),
            updated_by=(
                f"nautilus:protective_oco_monitor:{norm_sym}"
                if norm_sym
                else "nautilus:protective_oco_monitor"
            ),
            alert_event_type="PROTECTIVE_OCO_BREACH",
        )

    def clear_freeze_latch(
        self,
        reason: str = "Manual operator unfreeze",
        updated_by: str = "operator",
    ) -> None:
        """Explicitly clears the monotonic freeze latch when all async DB writes have succeeded."""
        self.set_entry_freeze(False, reason=reason, updated_by=updated_by)

    def set_entry_freeze(
        self,
        active: bool,
        reason: str = "",
        updated_by: str = "nautilus:plan_gate",
    ) -> None:
        """Atomically updates `ENTRY_FREEZE` in $O(1)$ Rust memory and persists to `system_flags`."""
        if bool(active):
            self.latch_entry_freeze(reason=reason, updated_by=updated_by)
            return

        if self._async_writer.has_failed_writes or self._async_writer.pending_count > 0:
            self._async_writer.flush(
                timeout_secs=min(float(self._connect_timeout_secs), 2.0)
            )
        if self._async_writer.has_failed_writes:
            self._monotonic_freeze_latched = True
            self._rust_engine.set_entry_freeze(
                True, self._monotonic_freeze_reason or "ASYNC_DB_WRITER_FAILURE"
            )
            return

        self._async_writer.enqueue_mutations(
            [
                {
                    "kind": "SET_ENTRY_FREEZE",
                    "active": False,
                    "reason": str(reason),
                    "updated_by": str(updated_by or "nautilus:plan_gate"),
                }
            ]
        )
        if self._eager_flush:
            self._async_writer.flush(
                timeout_secs=min(float(self._connect_timeout_secs), 2.0)
            )
        if not self._async_writer.has_failed_writes:
            self._monotonic_freeze_latched = False
            self._monotonic_freeze_reason = ""
            self._db_freeze_persisted = False
            self._rust_engine.set_entry_freeze(False, str(reason))
            self._rust_engine.set_backend_unavailable(False, "")

    def flush(self, timeout_secs: float = 5.0) -> None:
        """Waits for all queued asynchronous DB writes to complete."""
        pending = self._rust_engine.drain_pending_mutations(1024)
        if pending:
            self._async_writer.enqueue_mutations(pending)
        self._async_writer.flush(timeout_secs=timeout_secs)

    def close(self) -> None:
        """Flushes pending writes and closes background DB and LISTEN connections."""
        try:
            self._async_writer.close()
        except Exception:
            pass
        conn = self._listen_conn
        self._listen_conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def _initialize_sync_and_triggers(self) -> None:
        try:
            conn = psycopg2.connect(
                self._dsn,
                cursor_factory=RealDictCursor,
                connect_timeout=self._connect_timeout_secs,
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM pg_trigger
                    WHERE tgname IN (
                        'trg_notify_plan_gate_flags',
                        'trg_notify_plan_gate_plans',
                        'trg_notify_plan_gate_items'
                    )
                    """
                )
                row = cur.fetchone()
                if not row or int(row.get("cnt", 0)) < 3:
                    cur.execute(
                        f"""
                        CREATE OR REPLACE FUNCTION notify_nautilus_plan_gate_change()
                        RETURNS trigger AS $$
                        BEGIN
                            PERFORM pg_notify('{NOTIFY_CHANNEL}', TG_TABLE_NAME);
                            RETURN NULL;
                        END;
                        $$ LANGUAGE plpgsql;

                        DROP TRIGGER IF EXISTS trg_notify_plan_gate_flags ON system_flags;
                        CREATE TRIGGER trg_notify_plan_gate_flags
                        AFTER INSERT OR UPDATE OR DELETE ON system_flags
                        FOR EACH STATEMENT EXECUTE FUNCTION notify_nautilus_plan_gate_change();

                        DROP TRIGGER IF EXISTS trg_notify_plan_gate_plans ON trading_plans;
                        CREATE TRIGGER trg_notify_plan_gate_plans
                        AFTER INSERT OR UPDATE OR DELETE ON trading_plans
                        FOR EACH STATEMENT EXECUTE FUNCTION notify_nautilus_plan_gate_change();

                        DROP TRIGGER IF EXISTS trg_notify_plan_gate_items ON trading_plan_items;
                        CREATE TRIGGER trg_notify_plan_gate_items
                        AFTER INSERT OR UPDATE OR DELETE ON trading_plan_items
                        FOR EACH STATEMENT EXECUTE FUNCTION notify_nautilus_plan_gate_change();
                        """
                    )
                cur.execute(
                    f"""
                    LISTEN {NOTIFY_CHANNEL};
                    LISTEN nautilus_plan_locked;
                    LISTEN system_flags_changed;
                    """
                )
            self._listen_conn = conn
            self.sync_from_db(preserve_higher_reservation=True)
        except Exception as exc:
            self._rust_engine.set_backend_unavailable(
                True, f"PostgreSQL connection failed ({exc})"
            )

    def _poll_notifications_nonblocking(self) -> None:
        conn = self._listen_conn
        if conn is None or getattr(conn, "closed", 1) != 0:
            return
        try:
            readable, _, _ = select.select([conn], [], [], 0.0)
            if not readable:
                return
            conn.poll()
            had_notifies = bool(conn.notifies)
            conn.notifies.clear()
            if had_notifies:
                self.sync_from_db(preserve_higher_reservation=True)
        except Exception as exc:
            self._rust_engine.set_backend_unavailable(
                True, f"PostgreSQL notification poll failed ({exc})"
            )

    _poll_notify_and_sync = _poll_notifications_nonblocking

    def sync_from_db(self, preserve_higher_reservation: bool = True) -> None:
        """Synchronizes `system_flags.ENTRY_FREEZE` and locked `trading_plan_items` into `RustPlanGateEngine`."""
        with self._sync_lock:
            if (
                self._async_writer.has_failed_writes
                or self._async_writer.pending_count > 0
            ):
                self._async_writer.flush(
                    timeout_secs=min(float(self._connect_timeout_secs), 1.0)
                )

            conn = self._listen_conn
            if conn is None or getattr(conn, "closed", 1) != 0:
                try:
                    conn = psycopg2.connect(
                        self._dsn,
                        cursor_factory=RealDictCursor,
                        connect_timeout=self._connect_timeout_secs,
                    )
                    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                    with conn.cursor() as cur:
                        cur.execute(f"LISTEN {NOTIFY_CHANNEL};")
                    self._listen_conn = conn
                except Exception as exc:
                    self._rust_engine.set_backend_unavailable(
                        True, f"PostgreSQL connection failed ({exc})"
                    )
                    return

            effective_preserve = bool(
                preserve_higher_reservation
                or self._monotonic_freeze_latched
                or self._async_writer.pending_count > 0
                or self._async_writer.has_failed_or_pending_freeze
            )
            can_clear_before_query = (
                not self._async_writer.has_failed_or_pending_freeze
                and self._async_writer.pending_count == 0
                and self._db_freeze_persisted
            )

            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT active, reason
                        FROM system_flags
                        WHERE flag = 'ENTRY_FREEZE'
                        """
                    )
                    flag_row = cur.fetchone()
                    db_freeze_active = bool(flag_row["active"]) if flag_row else False
                    db_freeze_reason = str(flag_row["reason"] or "") if flag_row else ""

                    if db_freeze_active:
                        self._monotonic_freeze_latched = True
                        self._monotonic_freeze_reason = (
                            db_freeze_reason
                            or self._monotonic_freeze_reason
                            or "ENTRY_FREEZE"
                        )
                        self._db_freeze_persisted = True
                        self._rust_engine.set_entry_freeze(
                            True, self._monotonic_freeze_reason
                        )
                    elif self._monotonic_freeze_latched:
                        can_clear_latch = (
                            can_clear_before_query
                            and not self._async_writer.has_failed_or_pending_freeze
                            and self._async_writer.pending_count == 0
                            and self._db_freeze_persisted
                        )
                        if can_clear_latch:
                            self._monotonic_freeze_latched = False
                            self._monotonic_freeze_reason = ""
                            self._db_freeze_persisted = False
                            self._rust_engine.set_entry_freeze(False, "")
                        else:
                            reason_to_keep = (
                                self._monotonic_freeze_reason or "ENTRY_FREEZE"
                            )
                            self._rust_engine.set_entry_freeze(True, reason_to_keep)
                            if not self._db_freeze_persisted:
                                cur.execute(
                                    """
                                    INSERT INTO system_flags (
                                        flag, active, reason, set_by, updated_at
                                    ) VALUES (
                                        'ENTRY_FREEZE', TRUE, %s, 'nautilus:plan_gate:monotonic_latch', NOW()
                                    )
                                    ON CONFLICT (flag) DO UPDATE SET
                                        active = TRUE,
                                        reason = EXCLUDED.reason,
                                        set_by = EXCLUDED.set_by,
                                        updated_at = NOW()
                                    """,
                                    (reason_to_keep,),
                                )
                                cur.execute(
                                    """
                                    INSERT INTO system_flag_events (
                                        flag, active, reason, set_by, ts
                                    ) VALUES (
                                        'ENTRY_FREEZE', TRUE, %s, 'nautilus:plan_gate:monotonic_latch', NOW()
                                    )
                                    """,
                                    (reason_to_keep,),
                                )
                                cur.execute(
                                    """
                                    INSERT INTO alert_outbox (
                                        event_type, severity, source, title, body, payload, dedup_key, status
                                    ) VALUES (
                                        'PLAN_GATE_ENTRY_FREEZE',
                                        'CRITICAL',
                                        'nautilus:plan_gate:monotonic_latch',
                                        'PlanGate monotonic ENTRY_FREEZE latch persisted',
                                        %s,
                                        %s::jsonb,
                                        %s,
                                        'PENDING'
                                    )
                                    """,
                                    (
                                        reason_to_keep,
                                        json.dumps(
                                            {
                                                "flag": "ENTRY_FREEZE",
                                                "active": True,
                                                "reason": reason_to_keep,
                                            }
                                        ),
                                        f"plan_gate_latch:{reason_to_keep[:64]}",
                                    ),
                                )
                                self._db_freeze_persisted = True
                    else:
                        self._rust_engine.set_entry_freeze(False, "")

                    if not self._async_writer.has_failed_writes:
                        self._rust_engine.set_backend_unavailable(False, "")
                    else:
                        self._rust_engine.set_backend_unavailable(
                            True,
                            f"ENTRY_FREEZE_ACTIVE: {self._monotonic_freeze_reason or 'ASYNC_DB_WRITER_FAILURE'}",
                        )

                    if self._trading_date is not None:
                        cur.execute(
                            """
                            SELECT DISTINCT ON (i.symbol) i.*
                            FROM trading_plan_items i
                            JOIN trading_plans p ON p.plan_id = i.plan_id
                            WHERE p.trading_date = %s
                               AND p.market = %s
                               AND p.status = 'LOCKED'
                            ORDER BY i.symbol, p.locked_at DESC NULLS LAST
                            """,
                            (self._trading_date, self._market),
                        )
                    else:
                        cur.execute(
                            """
                            SELECT DISTINCT ON (i.symbol) i.*
                            FROM trading_plan_items i
                            JOIN trading_plans p ON p.plan_id = i.plan_id
                            WHERE p.market = %s
                               AND p.status = 'LOCKED'
                            ORDER BY i.symbol, p.trading_date DESC, p.locked_at DESC NULLS LAST
                            """,
                            (self._market,),
                        )
                    rows = cur.fetchall() or []
                    prev_reserved: Dict[str, Decimal] = {}
                    if effective_preserve:
                        for sym in list(self._known_symbols):
                            prev_reserved[sym] = Decimal(
                                self._rust_engine.reserved_qty(sym)
                            )
                        for row in rows:
                            row_sym = str(row.get("symbol") or "").strip().upper()
                            if row_sym and row_sym not in prev_reserved:
                                prev_reserved[row_sym] = Decimal(
                                    self._rust_engine.reserved_qty(row_sym)
                                )
                    self._rust_engine.clear_plan_items()
                    self._known_symbols.clear()
                    self._symbol_by_item_id.clear()
                    self._items_by_symbol.clear()
                    loaded_by_symbol: Dict[str, PlanItemContract] = {}
                    for row in rows:
                        try:
                            plan_item = PlanItemContract.from_db_row(row)
                            norm_sym = plan_item.symbol
                            if effective_preserve:
                                prior_reserved = prev_reserved.get(
                                    norm_sym, Decimal("0")
                                )
                                if prior_reserved > plan_item.filled_qty:
                                    plan_item = replace(
                                        plan_item,
                                        filled_qty=prior_reserved,
                                    )
                            self._rust_engine.upsert_plan_item(
                                _plan_item_to_rust_envelope(plan_item),
                                effective_preserve,
                            )
                            self._known_symbols.add(norm_sym)
                            self._symbol_by_item_id[str(plan_item.item_id)] = norm_sym
                            self._items_by_symbol[norm_sym] = plan_item
                            loaded_by_symbol[norm_sym] = plan_item
                        except Exception as item_exc:
                            logger.warning(
                                "Skipping invalid PlanItemContract row: %s", item_exc
                            )
                    for primary_item in list(loaded_by_symbol.values()):
                        sec_item = build_stat_arb_secondary_plan_item(primary_item)
                        if (
                            sec_item is not None
                            and sec_item.symbol not in loaded_by_symbol
                            and sec_item.symbol not in self._removed_runtime_symbols
                        ):
                            sec_sym = sec_item.symbol
                            if effective_preserve:
                                prior_sec_res = prev_reserved.get(sec_sym, Decimal("0"))
                                if prior_sec_res > sec_item.filled_qty:
                                    sec_item = replace(sec_item, filled_qty=prior_sec_res)
                            self._rust_engine.upsert_plan_item(
                                _plan_item_to_rust_envelope(sec_item),
                                effective_preserve,
                            )
                            self._known_symbols.add(sec_sym)
                            self._symbol_by_item_id[str(sec_item.item_id)] = sec_sym
                            self._items_by_symbol[sec_sym] = sec_item
                            loaded_by_symbol[sec_sym] = sec_item
                    for norm_sym, (runtime_item, runtime_reserved) in list(
                        self._runtime_items_by_symbol.items()
                    ):
                        prior_res = prev_reserved.get(norm_sym, Decimal("0"))
                        if norm_sym in loaded_by_symbol:
                            db_item = loaded_by_symbol[norm_sym]
                            db_stop = db_item.risk_envelope.initial_stop_price
                            rt_stop = runtime_item.risk_envelope.initial_stop_price
                            if db_item.risk_envelope.side == PlanSide.LONG:
                                merged_stop = max(db_stop, rt_stop)
                            else:
                                if db_stop > Decimal("0") and rt_stop > Decimal("0"):
                                    merged_stop = min(db_stop, rt_stop)
                                else:
                                    merged_stop = max(db_stop, rt_stop)
                            merged_env = (
                                replace(
                                    db_item.risk_envelope,
                                    initial_stop_price=merged_stop,
                                )
                                if merged_stop != db_stop
                                else db_item.risk_envelope
                            )
                            rt_res = (
                                runtime_reserved
                                if runtime_reserved is not None
                                else Decimal("0")
                            )
                            effective_res = (
                                max(prior_res, db_item.filled_qty, rt_res)
                                if effective_preserve
                                else max(db_item.filled_qty, rt_res)
                            )
                            overlaid_item = replace(
                                db_item,
                                risk_envelope=merged_env,
                                filled_qty=effective_res,
                            )
                            self._runtime_items_by_symbol[norm_sym] = (
                                overlaid_item,
                                effective_res,
                            )
                        else:
                            effective_res = (
                                max(prior_res, runtime_reserved)
                                if (effective_preserve and runtime_reserved is not None)
                                else (
                                    runtime_reserved
                                    if runtime_reserved is not None
                                    else max(prior_res, runtime_item.filled_qty)
                                )
                            )
                            overlaid_item = replace(
                                runtime_item, filled_qty=effective_res
                            )
                        self._rust_engine.upsert_plan_item(
                            _plan_item_to_rust_envelope(overlaid_item),
                            False,
                        )
                        self._known_symbols.add(norm_sym)
                        self._symbol_by_item_id[str(overlaid_item.item_id)] = norm_sym
                        self._items_by_symbol[norm_sym] = overlaid_item
            except Exception as exc:
                self._rust_engine.set_backend_unavailable(
                    True, f"PostgreSQL sync failed ({exc})"
                )

    def evaluate_and_reserve(self, request: PlanGateOrderRequest) -> PlanGateDecision:
        if request.is_reduce_only or request.is_exit_intent:
            return PlanGateDecision(
                allowed=True,
                reason=None,
                is_exit_bypass=True,
            )

        self._poll_notifications_nonblocking()
        rust_decision = self._rust_engine.evaluate_and_reserve(
            request.to_rust_request()
        )
        decision = PlanGateDecision.from_rust_decision(rust_decision)
        pending = list(self._rust_engine.drain_pending_mutations(64) or [])

        if not decision.allowed and not pending:
            reason_str = decision.reason or "PLAN_GATE_DENIED"
            severity = (
                "CRITICAL"
                if (
                    "ENTRY_FREEZE" in reason_str
                    or "BACKEND_UNAVAILABLE" in reason_str
                )
                else "WARN"
            )
            pending.append(
                {
                    "kind": "ORDER_DENIED",
                    "client_order_id": request.client_order_id,
                    "strategy_id": request.strategy_id,
                    "instrument_id": request.instrument_id,
                    "symbol": request.symbol,
                    "order_side": str(
                        getattr(request.order_side, "value", request.order_side)
                    ),
                    "quantity": str(request.quantity),
                    "reason": reason_str,
                    "severity": severity,
                    "item_id": (
                        str(decision.item_id) if decision.item_id is not None else None
                    ),
                    "plan_id": (
                        str(decision.plan_id) if decision.plan_id is not None else None
                    ),
                }
            )
        if pending:
            self._async_writer.enqueue_mutations(pending)
            if self._eager_flush:
                self._async_writer.flush(timeout_secs=2.0)

        return decision

    def release_reservation(
        self,
        symbol: str = "",
        quantity: Optional[Decimal | int | float | str] = None,
        *,
        release_qty: Optional[Decimal | int | float | str] = None,
        plan_id: Optional[UUID | str] = None,
        item_id: Optional[UUID | str] = None,
        reason: str = "",
    ) -> None:
        _ = (plan_id, reason)
        raw_qty = release_qty if release_qty is not None else quantity
        if raw_qty is None:
            return
        qty_dec = _to_decimal(raw_qty, "quantity")
        if qty_dec <= Decimal("0"):
            return
        self._poll_notifications_nonblocking()
        resolved_symbol = str(symbol or "").strip().upper()
        if not resolved_symbol and item_id is not None:
            resolved_symbol = self._symbol_by_item_id.get(str(item_id), "")
        if not resolved_symbol:
            return
        self._rust_engine.release_reservation(resolved_symbol, str(qty_dec))
        pending = self._rust_engine.drain_pending_mutations(64)
        if pending:
            self._async_writer.enqueue_mutations(pending)
            if self._eager_flush:
                self._async_writer.flush(timeout_secs=2.0)

    def release_closed_position_capacity(
        self,
        symbol: UUID | str = "",
        *,
        item_id: Optional[UUID | str] = None,
    ) -> None:
        """No-op in live `PostgresPlanGateBackend` (single-plan cumulative cap is authoritative)."""
        return


class PlanGateExecAlgorithmConfig(ExecutionAlgorithmConfig):
    """Configuration for `PlanGateExecAlgorithm`."""

    def __new__(
        cls,
        *,
        exec_algorithm_id: ExecAlgorithmId | str = PLAN_GATE_EXEC_ALGORITHM_ID,
        collar_market_orders: bool = True,
        disallow_uncollared_market_orders: bool = False,
        telemetry_actor: Optional[Any] = None,
        oco_monitor: Optional[Any] = None,
        log_events: bool = True,
        log_commands: bool = True,
        **kwargs: object,
    ) -> PlanGateExecAlgorithmConfig:
        _ = (disallow_uncollared_market_orders, telemetry_actor, oco_monitor)
        resolved_id = (
            exec_algorithm_id
            if isinstance(exec_algorithm_id, ExecAlgorithmId)
            else ExecAlgorithmId(str(exec_algorithm_id))
        )
        return super().__new__(
            cls,
            exec_algorithm_id=resolved_id,
            log_events=log_events,
            log_commands=log_commands,
            **kwargs,
        )

    def __init__(
        self,
        *,
        exec_algorithm_id: ExecAlgorithmId | str = PLAN_GATE_EXEC_ALGORITHM_ID,
        collar_market_orders: bool = True,
        disallow_uncollared_market_orders: bool = False,
        telemetry_actor: Optional[Any] = None,
        oco_monitor: Optional[Any] = None,
        log_events: bool = True,
        log_commands: bool = True,
        **_kwargs: object,
    ) -> None:
        self.collar_market_orders = bool(collar_market_orders)
        self.disallow_uncollared_market_orders = bool(disallow_uncollared_market_orders)
        self.telemetry_actor = telemetry_actor
        self.oco_monitor = oco_monitor


class _AuditTrailRecord(dict):
    """Structured PlanGate decision audit record supporting both dict key and attribute lookup."""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


class PlanGateExecAlgorithm(ExecutionAlgorithm):
    """Nautilus `ExecutionAlgorithm` (`ExecAlgorithmId("PLAN_GATE")`) enforcing `PlanRiskEnvelope` before submission.

    Invariants:
    1. Any `reduce_only=True` or position-closing (`EXIT`) order bypasses `ENTRY_FREEZE`, plan expiry,
       and backend outages 100% unconditionally in $O(1)$ memory.
    2. Any risk-increasing (`ENTRY` / `ADD`) order is validated atomically in $O(1)$ memory against the active
       `PlanItemContract` (`LOCKED` status, `system_flags.ENTRY_FREEZE == False`, side alignment,
       `approved_qty_cap`, and `MarketableLimitCollar`).
    3. Any validation failure or database outage immediately calls `self.deny_order(order, reason)` to emit
       `OrderDenied` and asynchronously records an alert in `alert_outbox`.
    4. Any admitted entry order reservation is tracked across order lifecycle events (`OrderFilled`,
       `OrderCanceled`, `OrderRejected`, `OrderExpired`) so unfilled capacity is released back to the
       backend when an order is canceled, rejected, or expired.
    5. When paired with a `PostgresTelemetryActor`, forwards all order and position lifecycle events
       through the `ExecutionAlgorithm` execution-engine seam so order/fill/position telemetry and
       `ProtectiveOcoMonitor` protective stop checks run deterministically.
    """

    def __init__(
        self,
        backend: PlanGateBackend,
        config: Optional[PlanGateExecAlgorithmConfig] = None,
        *,
        telemetry_actor: Optional[Any] = None,
        oco_monitor: Optional[Any] = None,
    ) -> None:
        resolved_cfg = config or PlanGateExecAlgorithmConfig(
            exec_algorithm_id=PLAN_GATE_EXEC_ALGORITHM_ID,
        )
        super().__init__(resolved_cfg)
        self._backend = backend
        self._collar_market_orders = bool(
            getattr(resolved_cfg, "collar_market_orders", True)
        )
        self._disallow_uncollared_market_orders = bool(
            getattr(resolved_cfg, "disallow_uncollared_market_orders", False)
        )
        self._decisions: Dict[str, PlanGateDecision] = {}
        self._audit_trail: List[_AuditTrailRecord] = []
        self._reservations: Dict[str, _TrackedEntryReservation] = {}
        self._spawn_to_primary_cid: Dict[str, str] = {}
        self._processed_fill_keys: set[str] = set()
        self._released_order_ids: set[str] = set()
        self._closed_cycle_entry_order_ids: set[str] = set()
        self._registered_strategies: List[Any] = []
        self._telemetry_actor: Optional[Any] = None
        self._is_started: bool = False
        self._cached_trader_id: Optional[Any] = None
        self._cached_cache: Optional[Any] = None
        self._cached_clock: Optional[Any] = None
        self._cached_msgbus: Optional[Any] = None
        self._oco_monitor: Optional[Any] = oco_monitor or getattr(
            resolved_cfg, "oco_monitor", None
        )
        initial_telemetry = telemetry_actor or getattr(
            resolved_cfg, "telemetry_actor", None
        )
        if initial_telemetry is not None:
            self.attach_telemetry_actor(initial_telemetry)
        elif self._oco_monitor is not None:
            self._wire_oco_monitor_callback(self._oco_monitor)

    def __new__(
        cls,
        backend: Optional[PlanGateBackend] = None,
        config: Optional[PlanGateExecAlgorithmConfig] = None,
        *,
        telemetry_actor: Optional[Any] = None,
        oco_monitor: Optional[Any] = None,
    ) -> PlanGateExecAlgorithm:
        _ = (backend, telemetry_actor, oco_monitor)
        resolved_cfg = config or PlanGateExecAlgorithmConfig(
            exec_algorithm_id=PLAN_GATE_EXEC_ALGORITHM_ID,
        )
        return super().__new__(cls, resolved_cfg)

    @property
    def backend(self) -> PlanGateBackend:
        return self._backend

    @property
    def telemetry_actor(self) -> Optional[Any]:
        return self._resolve_telemetry_actor()

    @property
    def oco_monitor(self) -> Optional[Any]:
        if self._oco_monitor is not None:
            return self._oco_monitor
        telemetry = self._resolve_telemetry_actor()
        return getattr(telemetry, "oco_monitor", None) if telemetry is not None else None

    @property
    def decisions(self) -> Mapping[str, PlanGateDecision]:
        return dict(self._decisions)

    @property
    def audit_trail(self) -> List[_AuditTrailRecord]:
        return list(self._audit_trail)

    def _record_decision_audit(
        self,
        cid: str,
        decision: PlanGateDecision,
        order: Optional[Any] = None,
        request: Optional[PlanGateOrderRequest] = None,
    ) -> None:
        self._decisions[cid] = decision
        inst_str = (
            request.instrument_id
            if request is not None
            else str(getattr(order, "instrument_id", "") or "")
        )
        sym_str = (
            request.symbol
            if request is not None
            else (inst_str.split(".", 1)[0] if inst_str else "")
        )
        side_obj = (
            request.order_side
            if request is not None
            else getattr(order, "side", None)
        )
        type_obj = (
            request.order_type
            if request is not None
            else getattr(order, "order_type", None)
        )
        qty_dec = (
            request.quantity
            if request is not None
            else self._extract_qty_decimal(getattr(order, "quantity", None))
        )
        is_reduce = (
            request.is_reduce_only
            if request is not None
            else bool(getattr(order, "is_reduce_only", False))
        )
        self._audit_trail.append(
            _AuditTrailRecord(
                client_order_id=cid,
                symbol=sym_str,
                instrument_id=inst_str,
                order_side=str(side_obj) if side_obj is not None else "",
                order_type=str(type_obj) if type_obj is not None else "",
                quantity=qty_dec,
                is_reduce_only=is_reduce,
                allowed=bool(decision.allowed),
                reason=decision.reason,
                is_exit_bypass=bool(decision.is_exit_bypass),
                derived_limit_price=decision.derived_limit_price,
                item_id=decision.item_id,
                plan_id=decision.plan_id,
                decision=decision,
            )
        )

    @property
    def active_reservations(self) -> Mapping[str, _TrackedEntryReservation]:
        return dict(self._reservations)

    def _refresh_runtime_handles(self) -> None:
        try:
            if self._safe_is_registered():
                self._is_started = True
                self._cached_trader_id = getattr(self, "trader_id", None)
                self._cached_cache = getattr(self, "cache", None)
                self._cached_clock = getattr(self, "clock", None)
                self._cached_msgbus = getattr(self, "msgbus", None)
        except RuntimeError:
            pass
        except Exception:
            pass

    def _safe_is_registered(self) -> bool:
        try:
            is_reg_fn = getattr(self, "is_registered", None)
            if callable(is_reg_fn):
                reg = bool(is_reg_fn())
            else:
                reg = getattr(self, "trader_id", None) is not None
            if reg:
                self._is_started = True
            return reg
        except RuntimeError:
            return self._is_started
        except Exception:
            return self._is_started

    def _safe_trader_id(self) -> Optional[Any]:
        try:
            tid = getattr(self, "trader_id", None)
            if tid is not None:
                self._cached_trader_id = tid
            return tid
        except RuntimeError:
            return self._cached_trader_id
        except Exception:
            return self._cached_trader_id

    def _safe_cache(self) -> Optional[Any]:
        try:
            c = getattr(self, "cache", None)
            if c is not None:
                self._cached_cache = c
            return c
        except RuntimeError:
            return self._cached_cache
        except Exception:
            return self._cached_cache

    def _safe_clock(self) -> Optional[Any]:
        try:
            clk = getattr(self, "clock", None)
            if clk is not None:
                self._cached_clock = clk
            return clk
        except RuntimeError:
            return self._cached_clock
        except Exception:
            return self._cached_clock

    def attach_telemetry_actor(self, telemetry_actor: Optional[Any]) -> None:
        """Attaches a `PostgresTelemetryActor` to receive all order and position lifecycle events."""
        self._telemetry_actor = telemetry_actor
        if telemetry_actor is not None:
            oco = getattr(telemetry_actor, "oco_monitor", None)
            if oco is not None:
                self._oco_monitor = oco
                self._wire_oco_monitor_callback(oco)

    def _wire_oco_monitor_callback(self, oco_monitor: Any) -> None:
        add_cb = getattr(oco_monitor, "add_entry_freeze_callback", None)
        if callable(add_cb):
            add_cb(self._on_protective_oco_freeze)
        elif getattr(oco_monitor, "_on_entry_freeze_callback", None) is None:
            oco_monitor._on_entry_freeze_callback = self._on_protective_oco_freeze

    def _on_protective_oco_freeze(self, symbol: str, reason: str) -> None:
        trigger_fn = getattr(self._backend, "trigger_protective_oco_freeze", None)
        if callable(trigger_fn):
            trigger_fn(symbol, reason)
            return
        set_freeze_fn = getattr(self._backend, "set_entry_freeze", None)
        if callable(set_freeze_fn):
            try:
                set_freeze_fn(
                    True,
                    reason=reason,
                    updated_by=f"nautilus:protective_oco_monitor:{symbol}",
                )
            except TypeError:
                set_freeze_fn(True, reason)

    def _resolve_telemetry_actor(self) -> Optional[Any]:
        if self._telemetry_actor is not None:
            return self._telemetry_actor
        if not self._safe_is_registered():
            return None
        self_cache = self._safe_cache()
        self_clock = self._safe_clock()
        if self_cache is None and self_clock is None:
            return None
        try:
            from nautilus_trader.quant.governance.telemetry_actor import (
                find_active_telemetry_actor,
            )

            found = find_active_telemetry_actor(
                cache=self_cache,
                clock=self_clock,
            )
            if found is not None:
                self.attach_telemetry_actor(found)
                return found
        except Exception:
            pass
        return None

    def _cache_telemetry_order(self, order: Any) -> None:
        if order is None:
            return
        telemetry = self._resolve_telemetry_actor()
        if telemetry is not None:
            sync_fn = getattr(telemetry, "_sync_order_object", None)
            if callable(sync_fn):
                try:
                    sync_fn(order)
                    return
                except Exception:
                    pass
            cache_fn = getattr(telemetry, "_cache_order_metadata", None)
            if callable(cache_fn):
                try:
                    cache_fn(order)
                except Exception:
                    pass
            return
        if self._oco_monitor is not None and self._is_standalone_exit_order(order):
            order_type_str = str(getattr(order, "order_type", "") or "").upper()
            if "STOP" in order_type_str:
                try:
                    inst_id = getattr(order, "instrument_id", None)
                    sym = (
                        str(inst_id.symbol)
                        if hasattr(inst_id, "symbol")
                        else str(inst_id or "").split(".", 1)[0]
                    )
                    qty_dec = self._extract_qty_decimal(getattr(order, "quantity", None))
                    trig_obj = getattr(order, "trigger_price", None) or getattr(
                        order, "price", None
                    )
                    stop_px = (
                        self._extract_qty_decimal(trig_obj)
                        if trig_obj is not None
                        else Decimal("0")
                    )
                    if sym and qty_dec > Decimal("0") and stop_px > Decimal("0"):
                        self._oco_monitor.register_protective_order(
                            symbol=sym,
                            covered_qty=qty_dec,
                            stop_price=stop_px,
                            broker_order_id=str(getattr(order, "client_order_id", "") or ""),
                            status="ACTIVE",
                            persist=False,
                        )
                except Exception:
                    pass

    def on_start(self) -> None:
        """Subscribe to order and position lifecycle events and initialize telemetry actor."""
        self._refresh_runtime_handles()
        if self._safe_is_registered():
            subscribed_order = False
            subscribed_pos = False
            try:
                self.subscribe_topic("events.order.*", self._on_order_event)
                subscribed_order = True
            except Exception:
                pass
            try:
                self.subscribe_topic("events.position.*", self._on_position_event)
                subscribed_pos = True
            except Exception:
                pass
            if not (subscribed_order and subscribed_pos):
                msgbus = self._cached_msgbus
                if msgbus is not None and hasattr(msgbus, "subscribe"):
                    try:
                        if not subscribed_order:
                            msgbus.subscribe("events.order.*", self._on_order_event)
                        if not subscribed_pos:
                            msgbus.subscribe("events.position.*", self._on_position_event)
                    except Exception:
                        pass
        telemetry = self._resolve_telemetry_actor()
        if telemetry is not None:
            try:
                telemetry.on_start()
            except Exception:
                pass

    def on_stop(self) -> None:
        flush_backend = getattr(self._backend, "flush", None)
        if callable(flush_backend):
            try:
                flush_backend()
            except Exception:
                pass
        if self._oco_monitor is not None:
            flush_oco = getattr(self._oco_monitor, "flush", None)
            if callable(flush_oco):
                try:
                    flush_oco()
                except Exception:
                    pass
        telemetry = self._resolve_telemetry_actor()
        if telemetry is not None:
            try:
                telemetry.on_stop()
            except Exception:
                pass

    def close(self) -> None:
        telemetry = self._telemetry_actor
        if telemetry is not None:
            close_tel = getattr(telemetry, "close", None)
            if callable(close_tel):
                try:
                    close_tel()
                except Exception:
                    pass
        close_fn = getattr(self._backend, "close", None)
        if callable(close_fn):
            close_fn()

    def on_order(self, order: Any) -> None:
        """Intercept a single routed order and either submit, spawn collared limit, or deny."""
        self._refresh_runtime_handles()
        self._cache_telemetry_order(order)
        self._evaluate_and_route_single_order(order)

    def on_order_list(
        self,
        order_list: Any,
        orders: Optional[Sequence[Any]] = None,
    ) -> None:
        """Intercept an `OrderList` (e.g., entry + protective OCO/OUO bracket legs)."""
        self._refresh_runtime_handles()
        self._resolve_telemetry_actor()
        resolved_orders = self._resolve_order_list_orders(order_list, orders)
        if not resolved_orders:
            return

        for o in resolved_orders:
            self._cache_telemetry_order(o)

        if len(resolved_orders) == 1:
            self._evaluate_and_route_single_order(resolved_orders[0])
            return

        list_cids = {str(o.client_order_id) for o in resolved_orders}
        primary_entry_order: Optional[Any] = None
        for candidate in resolved_orders:
            parent_cid = str(getattr(candidate, "parent_order_id", "") or "")
            if parent_cid and parent_cid in list_cids:
                continue
            if not self._is_standalone_exit_order(candidate):
                primary_entry_order = candidate
                break

        entry_orders: List[Any] = []
        protective_orders: List[Any] = []
        for leg in resolved_orders:
            if self._is_order_list_protective_leg(leg, primary_entry_order, list_cids):
                protective_orders.append(leg)
            else:
                entry_orders.append(leg)

        if not entry_orders:
            for leg in resolved_orders:
                self._evaluate_and_route_single_order(leg)
            return

        inferred_list_stop_px: Optional[Decimal] = None
        for prot_leg in protective_orders:
            trig_obj = getattr(prot_leg, "trigger_price", None) or getattr(
                prot_leg, "stop_price", None
            )
            if trig_obj is not None:
                try:
                    inferred_list_stop_px = (
                        Decimal(str(trig_obj.as_decimal()))
                        if hasattr(trig_obj, "as_decimal")
                        else _to_decimal(trig_obj, "stop_loss_price")
                    )
                    break
                except Exception:
                    pass

        admitted_entries: List[tuple[Any, PlanGateOrderRequest, PlanGateDecision]] = []
        for entry_order in entry_orders:
            request = self._build_order_request(entry_order)
            if request.stop_loss_price is None and inferred_list_stop_px is not None:
                request = replace(request, stop_loss_price=inferred_list_stop_px)
            decision = self._backend.evaluate_and_reserve(request)
            entry_cid = str(entry_order.client_order_id)
            self._record_decision_audit(
                entry_cid,
                decision,
                order=entry_order,
                request=request,
            )

            if not decision.allowed:
                for prev_order, prev_req, prev_dec in admitted_entries:
                    prev_cid = str(prev_order.client_order_id)
                    self._reservations.pop(prev_cid, None)
                    self._backend.release_reservation(
                        prev_req.symbol,
                        prev_req.quantity,
                        plan_id=prev_dec.plan_id,
                        item_id=prev_dec.item_id,
                        release_qty=prev_req.quantity,
                        reason="ORDER_LIST_SIBLING_DENIED",
                    )
                reason = decision.reason or "PLAN_GATE_DENIED"
                deny_decision = PlanGateDecision(
                    allowed=False,
                    reason=reason,
                    is_exit_bypass=False,
                    item_id=decision.item_id,
                    plan_id=decision.plan_id,
                )
                for child_order in resolved_orders:
                    cid = str(child_order.client_order_id)
                    if cid not in self._decisions or self._decisions[cid].allowed:
                        self._record_decision_audit(
                            cid,
                            deny_decision,
                            order=child_order,
                        )
                    self._safe_deny_order(child_order, reason)
                return

            admitted_entries.append((entry_order, request, decision))
            self._record_entry_reservation(entry_order, request, decision)

        primary_decision = admitted_entries[0][2]
        protective_cids = {str(o.client_order_id) for o in protective_orders}
        for prot_order in protective_orders:
            prot_cid = str(prot_order.client_order_id)
            self._record_decision_audit(
                prot_cid,
                PlanGateDecision(
                    allowed=True,
                    reason=None,
                    is_exit_bypass=True,
                    item_id=primary_decision.item_id,
                    plan_id=primary_decision.plan_id,
                ),
                order=prot_order,
            )

        has_contingency_children = any(
            str(getattr(o, "parent_order_id", "") or "") in list_cids
            or bool(getattr(o, "linked_order_ids", None))
            for o in resolved_orders
        )
        for child_order in resolved_orders:
            cid = str(child_order.client_order_id)
            child_decision = self._decisions[cid]
            if (
                cid not in protective_cids
                and not has_contingency_children
                and child_decision.derived_limit_price is not None
                and child_order.order_type == OrderType.MARKET
                and not child_decision.is_exit_bypass
            ):
                spawned = self.spawn_limit(
                    primary=child_order,
                    quantity=child_order.quantity,
                    price=Price.from_str(str(child_decision.derived_limit_price)),
                    time_in_force=TimeInForce.DAY,
                    reduce_only=bool(getattr(child_order, "is_reduce_only", False)),
                    reduce_primary=True,
                )
                spawn_cid = str(spawned.client_order_id)
                self._spawn_to_primary_cid[spawn_cid] = cid
                self._decisions[spawn_cid] = child_decision
                self._cache_telemetry_order(spawned)
                self._safe_submit_order(spawned)
            else:
                self._cache_telemetry_order(child_order)
                self._safe_submit_order(child_order)

    def _resolve_order_list_orders(
        self,
        order_list: Any,
        orders: Optional[Sequence[Any]] = None,
    ) -> List[Any]:
        if orders is not None:
            return [o for o in orders if o is not None]
        if order_list is None:
            return []
        list_attr = getattr(order_list, "orders", None)
        if list_attr is not None:
            return [o for o in list_attr if o is not None]
        if isinstance(order_list, (list, tuple)):
            return [o for o in order_list if o is not None]
        cids = getattr(order_list, "client_order_ids", None)
        cache = self._safe_cache()
        if cids is not None and cache is not None:
            resolved: List[Any] = []
            for cid in cids:
                try:
                    cached = cache.order(cid)
                    if cached is not None:
                        resolved.append(cached)
                except Exception:
                    pass
            return resolved
        return []

    def _is_standalone_exit_order(self, order: Any) -> bool:
        if bool(getattr(order, "is_reduce_only", False)):
            return True
        params: Mapping[str, Any] = getattr(order, "exec_algorithm_params", None) or {}
        tags_raw = getattr(order, "tags", None) or []
        tags_upper = {str(t).strip().upper() for t in tags_raw}
        intent_param = str(params.get("intent") or params.get("action") or "").strip().upper()
        reduce_param = str(params.get("reduce_only") or "").strip().lower() in ("1", "true", "yes")
        if reduce_param or intent_param in EXIT_INTENT_TAGS or bool(tags_upper & EXIT_INTENT_TAGS):
            return True
        try:
            qty_dec = self._extract_qty_decimal(order.quantity)
            return self._reduces_existing_cache_position(order.instrument_id, order.side, qty_dec)
        except Exception:
            return False

    def _is_order_list_protective_leg(
        self,
        order: Any,
        primary_entry_order: Optional[Any],
        list_cids: set[str],
    ) -> bool:
        if self._is_standalone_exit_order(order):
            return True
        parent_cid = str(getattr(order, "parent_order_id", "") or "")
        if parent_cid and parent_cid in list_cids:
            return True
        if primary_entry_order is not None and order is not primary_entry_order:
            if getattr(order, "side", None) != getattr(primary_entry_order, "side", None):
                contingency_str = str(getattr(order, "contingency_type", "") or "").upper()
                has_contingency = bool(contingency_str) and "NO_CONTINGENCY" not in contingency_str
                has_linked = bool(getattr(order, "linked_order_ids", None))
                order_type = getattr(order, "order_type", None)
                is_stop_or_limit = order_type in (
                    OrderType.STOP_MARKET,
                    OrderType.STOP_LIMIT,
                    OrderType.LIMIT,
                    OrderType.MARKET_IF_TOUCHED,
                    OrderType.LIMIT_IF_TOUCHED,
                    OrderType.TRAILING_STOP_MARKET,
                    OrderType.TRAILING_STOP_LIMIT,
                )
                if has_contingency or has_linked or is_stop_or_limit:
                    return True
        return False

    def _record_entry_reservation(
        self,
        order: Any,
        request: PlanGateOrderRequest,
        decision: PlanGateDecision,
    ) -> None:
        if not decision.allowed or decision.is_exit_bypass or request.quantity <= Decimal("0"):
            return
        cid = str(order.client_order_id)
        self._released_order_ids.discard(cid)
        self._reservations[cid] = _TrackedEntryReservation(
            client_order_id=cid,
            plan_id=decision.plan_id,
            item_id=decision.item_id,
            symbol=request.symbol,
            reserved_qty=request.quantity,
            filled_qty=Decimal("0"),
        )

    def register_strategy(self, strategy: Any) -> None:
        """Registers a strategy instance for synchronous order denial notification."""
        if strategy is not None and strategy not in self._registered_strategies:
            self._registered_strategies.append(strategy)

    def _safe_deny_order(self, order: Any, reason: str) -> None:
        try:
            self.deny_order(order, reason)
        except RuntimeError as exc:
            msg = str(exc).lower()
            if "not registered" not in msg and "already borrowed" not in msg:
                raise
        ord_strat_id = str(getattr(order, "strategy_id", "") or "")
        ord_inst_id = str(getattr(order, "instrument_id", "") or "")
        for strat in list(self._registered_strategies):
            try:
                cfg = getattr(strat, "archetype_config", None)
                strat_id = str(
                    getattr(strat, "id", None)
                    or getattr(cfg, "_resolved_strategy_id", "")
                    or ""
                )
                prim_inst = str(getattr(cfg, "instrument_id", "") or "")
                sec_inst = str(getattr(cfg, "secondary_instrument_id", "") or "")
                if (
                    (ord_strat_id and strat_id and ord_strat_id == strat_id)
                    or (ord_inst_id and ord_inst_id in (prim_inst, sec_inst))
                ):
                    on_denied = getattr(strat, "on_order_denied", None)
                    if callable(on_denied):
                        on_denied(order)
            except Exception:
                pass

    def _safe_submit_order(self, order: Any) -> None:
        try:
            self.submit_order(order)
        except RuntimeError as exc:
            msg = str(exc).lower()
            if "not registered" not in msg and "already borrowed" not in msg:
                raise

    def _evaluate_and_route_single_order(self, order: Any) -> PlanGateDecision:
        self._refresh_runtime_handles()
        self._resolve_telemetry_actor()
        self._cache_telemetry_order(order)
        request = self._build_order_request(order)
        decision = self._backend.evaluate_and_reserve(request)
        cid = str(order.client_order_id)
        self._record_decision_audit(
            cid,
            decision,
            order=order,
            request=request,
        )

        if not decision.allowed:
            self._safe_deny_order(order, decision.reason or "PLAN_GATE_DENIED")
            return decision

        self._record_entry_reservation(order, request, decision)

        if (
            decision.derived_limit_price is not None
            and order.order_type == OrderType.MARKET
            and not decision.is_exit_bypass
        ):
            spawned = self.spawn_limit(
                primary=order,
                quantity=order.quantity,
                price=Price.from_str(str(decision.derived_limit_price)),
                time_in_force=TimeInForce.DAY,
                reduce_only=bool(getattr(order, "is_reduce_only", False)),
                reduce_primary=True,
            )
            spawn_cid = str(spawned.client_order_id)
            self._spawn_to_primary_cid[spawn_cid] = cid
            self._decisions[spawn_cid] = decision
            self._cache_telemetry_order(spawned)
            self._safe_submit_order(spawned)
        else:
            self._cache_telemetry_order(order)
            self._safe_submit_order(order)
        return decision

    def on_order_event(self, event: Any) -> None:
        """Handle generic order lifecycle events forwarded to `PlanGateExecAlgorithm`."""
        self._on_order_event(event)

    def _on_order_event(self, event: Any) -> None:
        if event is None:
            return
        cls_name = type(event).__name__
        if cls_name == "OrderFilled":
            self.on_order_filled(event)
        elif cls_name in ("OrderCanceled", "OrderCancelled"):
            self.on_order_canceled(event)
        elif cls_name == "OrderRejected":
            self.on_order_rejected(event)
        elif cls_name == "OrderExpired":
            self.on_order_expired(event)
        else:
            telemetry = self._resolve_telemetry_actor()
            if telemetry is not None:
                telemetry.on_order_event(event)

    def on_position_event(self, event: Any) -> None:
        """Handle generic position lifecycle events forwarded to `PlanGateExecAlgorithm`."""
        self._on_position_event(event)

    def _on_position_event(self, event: Any) -> None:
        if event is None:
            return
        telemetry = self._resolve_telemetry_actor()
        if telemetry is not None:
            telemetry.on_position_event(event)
        if type(event).__name__ == "PositionClosed":
            inst_id = getattr(event, "instrument_id", None)
            sym = (
                str(inst_id.symbol)
                if hasattr(inst_id, "symbol")
                else str(inst_id or getattr(event, "symbol", "") or "").split(".", 1)[0]
            )
            self.release_closed_position_capacity(sym, instrument_id=inst_id)

    def release_closed_position_capacity(
        self,
        symbol: Optional[UUID | str] = "",
        *,
        instrument_id: Optional[Any] = None,
        item_id: Optional[UUID | str] = None,
    ) -> None:
        """Releases closed-cycle entry reservations when a position closes flat on a multi-cycle snapshot backend."""
        if not bool(getattr(self._backend, "_allow_multi_cycle_reentry", False)):
            return
        release_fn = getattr(self._backend, "release_closed_position_capacity", None)
        if not callable(release_fn):
            return
        raw_arg = str(symbol or "").strip()
        norm_sym = raw_arg.upper()
        backend_items = getattr(self._backend, "_items_by_symbol", {}) or {}
        if norm_sym and norm_sym not in backend_items:
            for item in backend_items.values():
                if str(item.item_id) == raw_arg:
                    norm_sym = item.symbol
                    if item_id is None:
                        item_id = item.item_id
                    break
        if (not norm_sym or norm_sym not in backend_items) and item_id is not None:
            target_id = str(item_id).strip()
            for item in backend_items.values():
                if str(item.item_id) == target_id:
                    norm_sym = item.symbol
                    break
        if not norm_sym and instrument_id is not None:
            norm_sym = (
                str(instrument_id.symbol).strip().upper()
                if hasattr(instrument_id, "symbol")
                else str(instrument_id).split(".", 1)[0].strip().upper()
            )
        if not norm_sym:
            return
        cache = self._safe_cache()
        if cache is not None and instrument_id is not None:
            try:
                open_pos = list(cache.positions_open(instrument_id=instrument_id) or [])
                if any(not bool(getattr(p, "is_closed", False)) for p in open_pos):
                    return
            except Exception:
                pass
        if cache is not None:
            try:
                orders_iter = (
                    cache.orders(instrument_id=instrument_id)
                    if instrument_id is not None
                    else cache.orders()
                )
                for cached_order in orders_iter or []:
                    cid = str(getattr(cached_order, "client_order_id", "") or "")
                    if not cid:
                        continue
                    ord_inst = str(getattr(cached_order, "instrument_id", "") or "")
                    ord_sym = ord_inst.split(".", 1)[0].strip().upper()
                    if ord_sym == norm_sym and not bool(getattr(cached_order, "is_open", False)):
                        self._closed_cycle_entry_order_ids.add(cid)
            except Exception:
                pass
        for cid, rec in list(self._reservations.items()):
            if rec.symbol == norm_sym:
                self._closed_cycle_entry_order_ids.add(cid)
                self._reservations.pop(cid, None)
        for rec in self._audit_trail:
            if rec.symbol == norm_sym and not rec.is_reduce_only:
                self._closed_cycle_entry_order_ids.add(rec.client_order_id)
        release_fn(norm_sym, item_id=item_id)

    def on_order_filled(self, event: Any) -> None:
        """Track cumulative filled quantity against an active entry reservation and forward to telemetry."""
        telemetry = self._resolve_telemetry_actor()
        if telemetry is not None:
            telemetry.on_order_filled(event)

        cid = self._resolve_event_client_order_id(event)
        if not cid:
            return
        primary_cid = self._spawn_to_primary_cid.get(cid, cid)
        reservation = self._reservations.get(primary_cid)
        if reservation is None:
            return

        event_id = getattr(event, "event_id", None) or getattr(event, "id", None)
        trade_id = getattr(event, "trade_id", None)
        ts_event = getattr(event, "ts_event", None)
        dedup_key = (
            f"id:{event_id}"
            if event_id is not None
            else f"fill:{cid}:{trade_id}:{ts_event}"
        )
        if dedup_key in self._processed_fill_keys:
            return
        self._processed_fill_keys.add(dedup_key)

        last_qty_obj = getattr(event, "last_qty", None) or getattr(event, "quantity", None)
        fill_qty = (
            self._extract_qty_decimal(last_qty_obj)
            if last_qty_obj is not None
            else Decimal("0")
        )
        if fill_qty > Decimal("0"):
            reservation.filled_qty = min(
                reservation.reserved_qty,
                reservation.filled_qty + fill_qty,
            )
        if reservation.filled_qty >= reservation.reserved_qty:
            self._reservations.pop(primary_cid, None)
            self._spawn_to_primary_cid.pop(cid, None)

    def on_order_canceled(self, event: Any) -> None:
        """Release unfilled reserved entry capacity when an admitted order is canceled."""
        telemetry = self._resolve_telemetry_actor()
        if telemetry is not None:
            telemetry.on_order_canceled(event)
        self._release_for_terminal_event(event, "ORDER_CANCELED")

    def on_order_rejected(self, event: Any) -> None:
        """Release unfilled reserved entry capacity when an admitted order is rejected."""
        telemetry = self._resolve_telemetry_actor()
        if telemetry is not None:
            telemetry.on_order_rejected(event)
        self._release_for_terminal_event(event, "ORDER_REJECTED")

    def on_order_expired(self, event: Any) -> None:
        """Release unfilled reserved entry capacity when an admitted order expires."""
        telemetry = self._resolve_telemetry_actor()
        if telemetry is not None:
            telemetry.on_order_expired(event)
        self._release_for_terminal_event(event, "ORDER_EXPIRED")

    def _release_for_terminal_event(self, event: Any, default_reason: str) -> None:
        cid = self._resolve_event_client_order_id(event)
        if not cid:
            return
        primary_cid = self._spawn_to_primary_cid.pop(cid, cid)
        self._released_order_ids.add(cid)
        self._released_order_ids.add(primary_cid)
        reservation = self._reservations.pop(primary_cid, None)
        if reservation is None:
            return

        effective_filled = reservation.filled_qty
        event_filled_obj = getattr(event, "filled_qty", None)
        if event_filled_obj is not None:
            effective_filled = max(
                effective_filled,
                self._extract_qty_decimal(event_filled_obj),
            )
        else:
            cache = self._safe_cache()
            if cache is not None:
                try:
                    cached_order = cache.order(getattr(event, "client_order_id", None))
                    if cached_order is not None and getattr(cached_order, "filled_qty", None) is not None:
                        effective_filled = max(
                            effective_filled,
                            self._extract_qty_decimal(cached_order.filled_qty),
                        )
                except Exception:
                    pass

        unfilled_qty = max(Decimal("0"), reservation.reserved_qty - effective_filled)
        if unfilled_qty > Decimal("0"):
            reason = str(getattr(event, "reason", None) or default_reason)
            self._backend.release_reservation(
                reservation.symbol,
                unfilled_qty,
                plan_id=reservation.plan_id,
                item_id=reservation.item_id,
                release_qty=unfilled_qty,
                reason=reason,
            )

    @staticmethod
    def _resolve_event_client_order_id(event: Any) -> str:
        cid_obj = getattr(event, "client_order_id", None)
        return str(cid_obj).strip() if cid_obj is not None else ""

    @staticmethod
    def _extract_qty_decimal(val: Any) -> Decimal:
        if val is None:
            return Decimal("0")
        if hasattr(val, "as_decimal"):
            return Decimal(str(val.as_decimal()))
        if hasattr(val, "as_double"):
            return Decimal(str(val.as_double()))
        return _to_decimal(val, "quantity")

    def _build_order_request(self, order: Any) -> PlanGateOrderRequest:
        inst_id = order.instrument_id
        symbol = str(inst_id.symbol)
        venue = str(inst_id.venue)
        order_side = order.side
        order_type = order.order_type
        qty_dec = Decimal(str(order.quantity.as_decimal()))
        is_reduce_only = bool(getattr(order, "is_reduce_only", False))

        params: Mapping[str, Any] = getattr(order, "exec_algorithm_params", None) or {}
        tags_raw = getattr(order, "tags", None) or []
        tags_upper = {str(t).strip().upper() for t in tags_raw}

        intent_param = str(params.get("intent") or params.get("action") or "").strip().upper()
        reduce_param = str(params.get("reduce_only") or "").strip().lower() in ("1", "true", "yes")
        is_exit_intent = (
            is_reduce_only
            or reduce_param
            or intent_param in EXIT_INTENT_TAGS
            or bool(tags_upper & EXIT_INTENT_TAGS)
            or self._reduces_existing_cache_position(inst_id, order_side, qty_dec)
        )

        limit_px_obj = getattr(order, "price", None)
        limit_price: Optional[Decimal] = (
            Decimal(str(limit_px_obj.as_decimal())) if limit_px_obj is not None else None
        )

        ref_price = self._resolve_reference_price(inst_id, params, limit_price)
        raw_stop_px = (
            params.get("stop_loss_price")
            if params.get("stop_loss_price") is not None
            else (
                params.get("initial_stop_price")
                if params.get("initial_stop_price") is not None
                else (
                    params.get("stop_price")
                    if params.get("stop_price") is not None
                    else getattr(order, "stop_loss_price", None)
                )
            )
        )
        stop_loss_price: Optional[Decimal] = (
            _to_decimal(raw_stop_px, "stop_loss_price")
            if raw_stop_px is not None and str(raw_stop_px).strip() != ""
            else None
        )
        if stop_loss_price is None:
            cache = self._safe_cache()
            linked_ids = getattr(order, "linked_order_ids", None) or []
            if cache is not None and linked_ids:
                for linked_cid in linked_ids:
                    try:
                        linked_order = cache.order(linked_cid)
                        if linked_order is not None:
                            trig_obj = getattr(
                                linked_order, "trigger_price", None
                            ) or getattr(linked_order, "stop_price", None)
                            if trig_obj is not None:
                                stop_loss_price = (
                                    Decimal(str(trig_obj.as_decimal()))
                                    if hasattr(trig_obj, "as_decimal")
                                    else _to_decimal(trig_obj, "stop_loss_price")
                                )
                                break
                    except Exception:
                        pass

        atr_val = (
            _to_decimal(params["atr"], "atr")
            if params.get("atr") is not None
            else None
        )

        collar_market_param = str(params.get("collar_market_order") or "").strip().lower()
        disallow_uncollared_param = str(
            params.get("disallow_uncollared_market_orders")
            or params.get("disallow_uncollared_market_order")
            or ""
        ).strip().lower()
        if collar_market_param in ("0", "false", "no"):
            collar_market_order = False
            allow_uncollared_market_order = False
        elif collar_market_param in ("1", "true", "yes"):
            collar_market_order = True
            allow_uncollared_market_order = False
        else:
            collar_market_order = self._collar_market_orders
            disallow_uncollared = (
                self._disallow_uncollared_market_orders
                or disallow_uncollared_param in ("1", "true", "yes")
            )
            allow_uncollared_market_order = (
                not collar_market_order and not disallow_uncollared
            )

        eval_ts = self._current_evaluation_ts(params, inst_id, order)
        cache_open_qty, cache_filled_qty = self._compute_cache_entry_quantities(
            inst_id,
            order_side,
            str(order.client_order_id),
        )

        return PlanGateOrderRequest(
            client_order_id=str(order.client_order_id),
            strategy_id=str(order.strategy_id),
            instrument_id=str(inst_id),
            symbol=symbol,
            venue=venue,
            order_side=order_side,
            order_type=order_type,
            quantity=qty_dec,
            is_reduce_only=is_reduce_only,
            evaluation_ts=eval_ts,
            limit_price=limit_price,
            reference_price=ref_price,
            stop_loss_price=stop_loss_price,
            atr=atr_val,
            collar_market_order=collar_market_order,
            allow_uncollared_market_order=allow_uncollared_market_order,
            is_exit_intent=is_exit_intent,
            cache_open_entry_qty=cache_open_qty,
            cache_filled_entry_qty=cache_filled_qty,
        )

    def _reduces_existing_cache_position(
        self,
        instrument_id: Any,
        order_side: OrderSide,
        order_qty: Decimal,
    ) -> bool:
        cache = self._safe_cache()
        if cache is None:
            return False
        try:
            positions = cache.positions_open(instrument_id=instrument_id)
            if not positions:
                return False
            net_signed = Decimal("0")
            for pos in positions:
                signed_qty = Decimal(str(pos.signed_qty))
                net_signed += signed_qty
            if net_signed > Decimal("0") and order_side == OrderSide.SELL and order_qty <= net_signed:
                return True
            if net_signed < Decimal("0") and order_side == OrderSide.BUY and order_qty <= abs(net_signed):
                return True
        except Exception:
            return False
        return False

    def _resolve_reference_price(
        self,
        instrument_id: Any,
        params: Mapping[str, Any],
        limit_price: Optional[Decimal],
    ) -> Optional[Decimal]:
        if params.get("reference_price") is not None:
            return _to_decimal(params["reference_price"], "reference_price")
        cache = self._safe_cache()
        if cache is not None:
            try:
                quote = cache.quote_tick(instrument_id)
                if quote is not None:
                    bid = Decimal(str(quote.bid_price.as_decimal()))
                    ask = Decimal(str(quote.ask_price.as_decimal()))
                    if bid > Decimal("0") and ask > Decimal("0"):
                        return ((bid + ask) / Decimal("2")).quantize(Decimal("0.0001"))
            except Exception:
                pass
            try:
                trade = cache.trade_tick(instrument_id)
                if trade is not None:
                    px = Decimal(str(trade.price.as_decimal()))
                    if px > Decimal("0"):
                        return px
            except Exception:
                pass
        return None

    def _current_evaluation_ts(
        self,
        params: Mapping[str, Any],
        instrument_id: Optional[Any] = None,
        order: Optional[Any] = None,
    ) -> datetime:
        if params.get("evaluation_ts") is not None:
            raw_ts = str(params["evaluation_ts"])
            dt = datetime.fromisoformat(raw_ts)
            return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
        cache = self._safe_cache()
        if cache is not None and instrument_id is not None:
            try:
                bar_types_fn = getattr(cache, "bar_types", None)
                bar_fn = getattr(cache, "bar", None)
                if callable(bar_types_fn) and callable(bar_fn):
                    latest_bar_ns = 0
                    for bt in bar_types_fn(instrument_id=instrument_id) or []:
                        b = bar_fn(bt)
                        if b is not None:
                            ts_ns = int(
                                getattr(b, "ts_init", 0)
                                or getattr(b, "ts_event", 0)
                                or 0
                            )
                            if ts_ns > latest_bar_ns:
                                latest_bar_ns = ts_ns
                    if latest_bar_ns > 100_000_000_000_000_000:
                        return datetime.fromtimestamp(
                            latest_bar_ns / 1e9, tz=timezone.utc
                        )
            except Exception:
                pass
        clock = self._safe_clock()
        if clock is not None:
            try:
                now_ns = int(clock.timestamp_ns())
                if now_ns > 100_000_000_000_000_000:
                    return datetime.fromtimestamp(now_ns / 1e9, tz=timezone.utc)
            except Exception:
                pass
        if order is not None:
            try:
                order_ts_ns = int(getattr(order, "ts_init", 0) or 0)
                if order_ts_ns > 100_000_000_000_000_000:
                    return datetime.fromtimestamp(order_ts_ns / 1e9, tz=timezone.utc)
            except Exception:
                pass
        return datetime.now(timezone.utc)

    def _compute_cache_entry_quantities(
        self,
        instrument_id: Any,
        order_side: OrderSide,
        current_client_order_id: str,
    ) -> tuple[Decimal, Decimal]:
        cache = self._safe_cache()
        if cache is None:
            return Decimal("0"), Decimal("0")
        open_qty = Decimal("0")
        filled_qty = Decimal("0")
        symbol = (
            str(instrument_id.symbol)
            if hasattr(instrument_id, "symbol")
            else str(instrument_id).split(".")[0]
        )
        active_item = (
            self._backend.get_plan_item(symbol)
            if hasattr(self._backend, "get_plan_item")
            else None
        )
        trading_date = getattr(self._backend, "trading_date", None)
        session_start_ns = (
            int(
                datetime(
                    trading_date.year,
                    trading_date.month,
                    trading_date.day,
                    tzinfo=timezone.utc,
                ).timestamp()
                * 1_000_000_000
            )
            if trading_date is not None
            else 0
        )
        try:
            for cached_order in cache.orders(instrument_id=instrument_id):
                cid = str(cached_order.client_order_id)
                if cid == current_client_order_id or cid in self._closed_cycle_entry_order_ids:
                    continue
                if getattr(cached_order, "is_reduce_only", False):
                    continue
                if cached_order.side != order_side:
                    continue
                if (
                    getattr(cached_order, "is_spawned", False)
                    or cid in self._spawn_to_primary_cid
                    or bool(getattr(cached_order, "exec_spawn_id", None))
                ):
                    continue
                prior_decision = self._decisions.get(cid)
                if prior_decision is not None and prior_decision.is_exit_bypass:
                    continue
                order_params: Mapping[str, Any] = (
                    getattr(cached_order, "exec_algorithm_params", None) or {}
                )
                order_tags = {
                    str(t).strip().upper()
                    for t in (getattr(cached_order, "tags", None) or [])
                }
                order_intent = (
                    str(order_params.get("intent") or order_params.get("action") or "")
                    .strip()
                    .upper()
                )
                order_reduce = str(order_params.get("reduce_only") or "").strip().lower() in (
                    "1",
                    "true",
                    "yes",
                )
                if (
                    order_reduce
                    or order_intent in EXIT_INTENT_TAGS
                    or bool(order_tags & EXIT_INTENT_TAGS)
                ):
                    continue
                if active_item is not None:
                    if prior_decision is not None:
                        if (
                            prior_decision.item_id is not None
                            and str(prior_decision.item_id) != str(active_item.item_id)
                        ):
                            continue
                        if (
                            prior_decision.plan_id is not None
                            and str(prior_decision.plan_id) != str(active_item.plan_id)
                        ):
                            continue
                    param_item_id = order_params.get("item_id")
                    if param_item_id is not None and str(param_item_id) != str(
                        active_item.item_id
                    ):
                        continue
                    param_plan_id = order_params.get("plan_id")
                    if param_plan_id is not None and str(param_plan_id) != str(
                        active_item.plan_id
                    ):
                        continue
                    if (
                        not cached_order.is_open
                        and prior_decision is None
                        and param_item_id is None
                        and param_plan_id is None
                        and session_start_ns > 0
                        and active_item.status == "ARMED"
                        and active_item.filled_qty == Decimal("0")
                    ):
                        order_ts_ns = int(
                            getattr(cached_order, "ts_init", 0)
                            or getattr(cached_order, "ts_last", 0)
                            or 0
                        )
                        if (
                            order_ts_ns > 100_000_000_000_000_000
                            and order_ts_ns < session_start_ns
                        ):
                            continue
                f_qty = Decimal(str(cached_order.filled_qty.as_decimal()))
                filled_qty += f_qty
                if cached_order.is_open and cid not in self._released_order_ids:
                    l_qty = Decimal(str(cached_order.leaves_qty.as_decimal()))
                    open_qty += l_qty
        except Exception:
            pass
        return open_qty, filled_qty
