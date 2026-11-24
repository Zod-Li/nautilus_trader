"""In-memory O(1) Rust `PlanGate` backend synchronized with PostgreSQL via `LISTEN/NOTIFY` and async write-behind persistence."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal
import json
import logging
import os
import queue
import select
import threading
import time
import types
from typing import Any, Callable, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple
from uuid import UUID, uuid5
import weakref

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import RealDictCursor, register_uuid

from nautilus_trader._libnautilus.quant_execution import (
    RustPlanGateDecision,
    RustPlanGateEngine,
    RustPlanGateOrderRequest,
    RustPlanItemEnvelope,
)
from nautilus_trader.model import OrderSide, OrderType

from evolution_engine.db.dsn import resolve_database_dsn
from nautilus_trader.quant.contracts.models import (
    PlanItemContract,
    PlanSide,
    _to_decimal,
)

register_uuid()

logger = logging.getLogger(__name__)

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


def _plan_item_to_rust_envelope(item: PlanItemContract) -> RustPlanItemEnvelope:
    valid_until = (
        item.risk_envelope.valid_until
        if item.risk_envelope.valid_until.tzinfo is not None
        else item.risk_envelope.valid_until.replace(tzinfo=timezone.utc)
    )
    eff_daily_atr = (
        item.daily_atr_14
        if item.daily_atr_14 is not None and item.daily_atr_14 > Decimal("0")
        else (
            item.risk_envelope.daily_atr_14
            if item.risk_envelope.daily_atr_14 is not None
            and item.risk_envelope.daily_atr_14 > Decimal("0")
            else None
        )
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
        daily_atr_14=str(eff_daily_atr) if eff_daily_atr is not None else None,
    )


class PlanGateBackend(Protocol):
    """Protocol implemented by `PostgresPlanGateBackend`."""

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
        self._rust_engine: Optional[RustPlanGateEngine] = rust_engine
        self._on_write_failure: Optional[Callable[[str], None]] = None
        self._on_write_failure_weak: Optional[weakref.WeakMethod[Any]] = None
        if on_write_failure is not None:
            if isinstance(on_write_failure, types.MethodType):
                self._on_write_failure_weak = weakref.WeakMethod(on_write_failure)
            else:
                self._on_write_failure = on_write_failure
        self._on_freeze_persisted: Optional[Callable[[bool], None]] = None
        self._on_freeze_persisted_weak: Optional[weakref.WeakMethod[Any]] = None
        if on_freeze_persisted is not None:
            if isinstance(on_freeze_persisted, types.MethodType):
                self._on_freeze_persisted_weak = weakref.WeakMethod(on_freeze_persisted)
            else:
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

    def _resolve_write_failure_cb(self) -> Optional[Callable[[str], None]]:
        if self._on_write_failure is not None:
            return self._on_write_failure
        if self._on_write_failure_weak is not None:
            return self._on_write_failure_weak()
        return None

    def _resolve_freeze_persisted_cb(self) -> Optional[Callable[[bool], None]]:
        if self._on_freeze_persisted is not None:
            return self._on_freeze_persisted
        if self._on_freeze_persisted_weak is not None:
            return self._on_freeze_persisted_weak()
        return None

    def close(self) -> None:
        self._queue.put(None)
        if self._thread.is_alive() and threading.current_thread() is not self._thread:
            self._thread.join(timeout=2.0)
        self._close_write_conn()
        self._on_write_failure = None
        self._on_write_failure_weak = None
        self._on_freeze_persisted = None
        self._on_freeze_persisted_weak = None
        self._rust_engine = None
        with self._state_lock:
            self._retry_buffer.clear()

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

        write_failure_cb = self._resolve_write_failure_cb()
        if write_failure_cb is not None:
            try:
                write_failure_cb(reason)
            except Exception as cb_exc:
                logger.error("PlanGate write failure callback error: %s", cb_exc)
        rust_engine = self._rust_engine
        if rust_engine is not None:
            if write_failure_cb is None:
                rust_engine.set_entry_freeze(True, reason)
            rust_engine.set_backend_unavailable(
                True, f"ENTRY_FREEZE_ACTIVE: {reason}"
            )

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
            freeze_persisted_cb = self._resolve_freeze_persisted_cb()
            if freeze_persisted_cb is not None:
                for m in combined:
                    if m.get("kind") == "SET_ENTRY_FREEZE":
                        try:
                            freeze_persisted_cb(bool(m.get("active", True)))
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
                  AND status NOT IN ('FILLED', 'EXPIRED', 'CANCELLED', 'CLOSED')
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
            qty_dec = Decimal(str(mutation.get("quantity", "0")))
            released_notional = Decimal(str(mutation.get("released_notional_usd", "0")))
            if item_id:
                cur.execute(
                    """
                    UPDATE trading_plan_items
                    SET filled_qty = %s,
                        reserved_notional_usd = CASE
                            WHEN %s = 0 THEN 0
                            ELSE GREATEST(0, COALESCE(reserved_notional_usd, 0) - %s)
                        END,
                        status = CASE
                            WHEN (GREATEST(0, COALESCE(filled_qty, 0) - %s) = 0 OR %s = 0) AND status = 'TRIGGERED' THEN 'ARMED'
                            ELSE status
                        END,
                        updated_at = NOW()
                    WHERE item_id = %s
                      AND status NOT IN ('FILLED', 'EXPIRED', 'CANCELLED', 'CLOSED')
                    """,
                    (
                        new_reserved,
                        new_reserved,
                        released_notional,
                        qty_dec,
                        new_reserved,
                        item_id,
                    ),
                )
            elif self._trading_date is not None:
                cur.execute(
                    """
                    UPDATE trading_plan_items i
                    SET filled_qty = GREATEST(0, COALESCE(i.filled_qty, 0) - %s),
                        reserved_notional_usd = CASE
                            WHEN GREATEST(0, COALESCE(i.filled_qty, 0) - %s) = 0 OR %s = 0 THEN 0
                            ELSE GREATEST(0, COALESCE(i.reserved_notional_usd, 0) - %s)
                        END,
                        status = CASE
                            WHEN GREATEST(0, COALESCE(i.filled_qty, 0) - %s) = 0 AND i.status = 'TRIGGERED' THEN 'ARMED'
                            ELSE i.status
                        END,
                        updated_at = NOW()
                    FROM trading_plans p
                    WHERE p.plan_id = i.plan_id
                      AND p.trading_date = %s
                      AND p.market = %s
                      AND p.status = 'LOCKED'
                      AND i.symbol = %s
                      AND i.status NOT IN ('FILLED', 'EXPIRED', 'CANCELLED', 'CLOSED')
                    """,
                    (
                        qty_dec,
                        qty_dec,
                        new_reserved,
                        released_notional,
                        qty_dec,
                        self._trading_date,
                        self._market,
                        mutation["symbol"],
                    ),
                )
        elif kind == "RECONCILE_RESERVATION":
            item_id = mutation.get("item_id")
            new_reserved = Decimal(str(mutation["new_reserved_qty"]))
            new_notional = Decimal(str(mutation.get("new_reserved_notional_usd", "0")))
            if item_id:
                cur.execute(
                    """
                    UPDATE trading_plan_items
                    SET filled_qty = %s,
                        reserved_notional_usd = %s,
                        updated_at = NOW()
                    WHERE item_id = %s
                      AND status NOT IN ('FILLED', 'EXPIRED', 'CANCELLED', 'CLOSED')
                    """,
                    (new_reserved, new_notional, item_id),
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
        *,
        plan_items: Sequence[PlanItemContract] = (),
        entry_freeze: bool = False,
        entry_freeze_reason: str = "",
        persist_to_db: bool = True,
        allow_multi_cycle_reentry: bool = False,
        oco_monitor: Optional[Any] = None,
    ) -> None:
        self._persist_to_db = bool(persist_to_db)
        self._allow_multi_cycle_reentry = bool(allow_multi_cycle_reentry)
        self._oco_monitor: Optional[Any] = oco_monitor
        self._dsn = (
            resolve_database_dsn(dsn)
            if (self._persist_to_db or dsn is not None)
            else ""
        )
        self._trading_date = trading_date
        self._market = market
        self._connect_timeout_secs = max(1, int(connect_timeout_secs))
        self._eager_flush = bool(eager_flush)
        init_freeze = bool(entry_freeze)
        init_reason = str(entry_freeze_reason or ("ENTRY_FREEZE" if init_freeze else ""))
        self._rust_engine = RustPlanGateEngine(init_freeze, init_reason)
        self._listen_conn: Optional[Any] = None
        self._query_conn: Optional[Any] = None
        self._listen_stop_event = threading.Event()
        self._listen_wake_r: Optional[int] = None
        self._listen_wake_w: Optional[int] = None
        self._listen_thread: Optional[threading.Thread] = None
        self._listen_poll_lock = threading.Lock()
        self._db_query_lock = threading.Lock()
        self._sync_lock = threading.RLock()
        self._freeze_lock = threading.RLock()
        self._freeze_epoch: int = 0
        self._monotonic_freeze_latched: bool = init_freeze
        self._monotonic_freeze_reason: str = init_reason
        self._db_freeze_persisted: bool = False
        self._known_symbols: set[str] = set()
        self._symbol_by_item_id: Dict[str, str] = {}
        self._items_by_symbol: Dict[str, PlanItemContract] = {}
        self._baseline_items_by_symbol: Dict[str, PlanItemContract] = {}
        self._runtime_items_by_symbol: Dict[str, Tuple[PlanItemContract, Optional[Decimal]]] = {}
        self._removed_runtime_symbols: set[str] = set()
        self._released_cap_by_symbol: Dict[str, Decimal] = {}
        self._reentry_cycle_counts: Dict[str, int] = {}
        self._closed_cycle_locked_symbols: set[str] = set()
        self._alerts: List[Dict[str, Any]] = []
        for item in plan_items:
            self.upsert_runtime_plan_item(item, reserved_qty=item.filled_qty)
        if self._persist_to_db:
            self._async_writer: Optional[_AsyncPlanGateDbWorker] = _AsyncPlanGateDbWorker(
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
        else:
            self._async_writer = None
            self._db_worker = None

    def _on_async_write_failure(self, reason: str) -> None:
        with self._freeze_lock:
            self._freeze_epoch += 1
            self._monotonic_freeze_latched = True
            self._monotonic_freeze_reason = str(reason or "ASYNC_DB_WRITER_FAILURE")
            self._db_freeze_persisted = False
            self._rust_engine.set_entry_freeze(True, self._monotonic_freeze_reason)

    def _on_async_freeze_persisted(self, active: bool) -> None:
        if active:
            with self._freeze_lock:
                if self._monotonic_freeze_latched:
                    self._db_freeze_persisted = True

    @property
    def dsn(self) -> str:
        return self._dsn

    @property
    def trading_date(self) -> Optional[date]:
        return self._trading_date

    @property
    def alerts(self) -> List[Dict[str, Any]]:
        return list(self._alerts)

    @property
    def oco_monitor(self) -> Optional[Any]:
        return self._oco_monitor

    def register_oco_monitor(self, monitor: Optional[Any]) -> None:
        """Attaches a `ProtectiveOcoMonitor` so `sync_from_db` refreshes protective stop coverage."""
        self._oco_monitor = monitor

    attach_oco_monitor = register_oco_monitor

    def as_pre_trade_risk_gate(self) -> Any:
        """Returns the PyO3 `PyPreTradeRiskGateHandle` wired into Nautilus `RiskEngine::execute`."""
        return self._rust_engine.as_pre_trade_risk_gate()

    def _drain_rust_mutations(self, limit: int = 64) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for raw in self._rust_engine.drain_pending_mutations(limit) or []:
            if isinstance(raw, Mapping):
                out.append(dict(raw))
            elif isinstance(raw, str):
                out.append(json.loads(raw))
        return out

    def _mirror_rust_mutations_to_python_items(
        self, mutations: Sequence[Mapping[str, Any]]
    ) -> None:
        with self._sync_lock:
            for mutation in mutations:
                kind = str(mutation.get("kind") or "")
                norm_sym = str(mutation.get("symbol") or "").split(".", 1)[0].strip().upper()
                if not norm_sym:
                    continue
                if kind == "ORDER_ADMITTED":
                    new_res_raw = mutation.get("new_reserved_qty")
                    if new_res_raw is None:
                        continue
                    new_res_dec = Decimal(str(new_res_raw))
                    self._released_cap_by_symbol.pop(norm_sym, None)
                    if norm_sym in self._items_by_symbol:
                        cur_item = self._items_by_symbol[norm_sym]
                        new_status = (
                            "TRIGGERED" if str(cur_item.status) == "ARMED" else cur_item.status
                        )
                        self._items_by_symbol[norm_sym] = replace(
                            cur_item, filled_qty=new_res_dec, status=new_status
                        )
                    if norm_sym in self._runtime_items_by_symbol:
                        rt_item, _ = self._runtime_items_by_symbol[norm_sym]
                        new_status = (
                            "TRIGGERED" if str(rt_item.status) == "ARMED" else rt_item.status
                        )
                        self._runtime_items_by_symbol[norm_sym] = (
                            replace(rt_item, filled_qty=new_res_dec, status=new_status),
                            new_res_dec,
                        )
                elif kind == "RELEASE_RESERVATION":
                    new_res_raw = mutation.get("new_reserved_qty")
                    if new_res_raw is None:
                        continue
                    new_res_dec = Decimal(str(new_res_raw))
                    locked_syms: set[str] = getattr(self, "_closed_cycle_locked_symbols", set())
                    if norm_sym in locked_syms:
                        continue
                    self._released_cap_by_symbol[norm_sym] = new_res_dec
                    if norm_sym in self._items_by_symbol:
                        cur_item = self._items_by_symbol[norm_sym]
                        reverted_status = (
                            "ARMED"
                            if new_res_dec == Decimal("0") and str(cur_item.status) == "TRIGGERED"
                            else cur_item.status
                        )
                        self._items_by_symbol[norm_sym] = replace(
                            cur_item, filled_qty=new_res_dec, status=reverted_status
                        )
                    if norm_sym in self._runtime_items_by_symbol:
                        rt_item, _ = self._runtime_items_by_symbol[norm_sym]
                        reverted_status = (
                            "ARMED"
                            if new_res_dec == Decimal("0") and str(rt_item.status) == "TRIGGERED"
                            else rt_item.status
                        )
                        self._runtime_items_by_symbol[norm_sym] = (
                            replace(rt_item, filled_qty=new_res_dec, status=reverted_status),
                            new_res_dec,
                        )

    def flush_rust_mutations(self, limit: int = 256) -> int:
        """Drains pending `RustPlanGateEngine` mutations, mirrors state, and enqueues DB writes."""
        pending = self._drain_rust_mutations(limit)
        if not pending:
            return 0
        self._mirror_rust_mutations_to_python_items(pending)
        self._alerts.extend(pending)
        if self._async_writer is not None:
            self._async_writer.enqueue_mutations(pending)
            if self._eager_flush:
                self._async_writer.flush(timeout_secs=2.0)
        return len(pending)

    def reconcile_terminal_order(
        self,
        client_order_id: str,
        symbol: str,
        filled_qty: Decimal | int | float | str,
    ) -> Decimal:
        """Idempotently reconciles a terminal entry order in `RustPlanGateEngine` and flushes mutations."""
        norm_sym = str(symbol or "").split(".", 1)[0].strip().upper()
        qty_dec = max(Decimal("0"), _to_decimal(filled_qty, "filled_qty"))
        updated = Decimal(
            str(
                self._rust_engine.reconcile_terminal_order(
                    str(client_order_id), norm_sym, str(qty_dec)
                )
            )
        )
        self.flush_rust_mutations()
        return updated

    def reconcile_order_updated(
        self,
        client_order_id: str,
        symbol: str,
        new_qty: Decimal | int | float | str,
    ) -> Decimal:
        """Reconciles a venue-confirmed order quantity reduction in `RustPlanGateEngine`."""
        norm_sym = str(symbol or "").split(".", 1)[0].strip().upper()
        qty_dec = max(Decimal("0"), _to_decimal(new_qty, "new_qty"))
        updated = Decimal(
            str(
                self._rust_engine.reconcile_order_updated(
                    str(client_order_id), norm_sym, str(qty_dec)
                )
            )
        )
        self.flush_rust_mutations()
        return updated

    def reconcile_symbol_reservation(
        self,
        symbol: str,
        reconciled_reserved_qty: Decimal | int | float | str,
        *,
        persist_db: bool = True,
        persist_filled_qty: Optional[Decimal | int | float | str] = None,
    ) -> Decimal:
        """Sets the authoritative startup reservation watermark in `RustPlanGateEngine` (and optionally PG)."""
        norm_sym = str(symbol or "").split(".", 1)[0].strip().upper()
        qty_dec = max(Decimal("0"), _to_decimal(reconciled_reserved_qty, "reconciled_reserved_qty"))
        persist_dec = (
            max(Decimal("0"), _to_decimal(persist_filled_qty, "persist_filled_qty"))
            if persist_filled_qty is not None
            else qty_dec
        )
        if not norm_sym or norm_sym not in self._items_by_symbol:
            return Decimal("0")
        with self._sync_lock:
            item = self._items_by_symbol[norm_sym]
            updated_item = replace(item, filled_qty=qty_dec)
            self._rust_engine.upsert_plan_item(
                _plan_item_to_rust_envelope(updated_item),
                False,
            )
            self._items_by_symbol[norm_sym] = updated_item
            if norm_sym in self._runtime_items_by_symbol:
                self._runtime_items_by_symbol[norm_sym] = (updated_item, qty_dec)
            if qty_dec < item.filled_qty:
                self._released_cap_by_symbol[norm_sym] = qty_dec
            else:
                self._released_cap_by_symbol.pop(norm_sym, None)

        if (
            persist_db
            and self._async_writer is not None
            and str(item.status or "").strip().upper()
            not in ("FILLED", "EXPIRED", "CANCELLED", "CLOSED")
        ):
            ref_px = (
                item.risk_envelope.target_notional_usd / item.risk_envelope.target_qty
                if item.risk_envelope.target_qty > Decimal("0")
                else Decimal("0")
            )
            new_notional = (ref_px * persist_dec).quantize(Decimal("0.01"))
            self._async_writer.enqueue_mutations(
                [
                    {
                        "kind": "RECONCILE_RESERVATION",
                        "item_id": str(item.item_id),
                        "symbol": norm_sym,
                        "new_reserved_qty": str(persist_dec),
                        "new_reserved_notional_usd": str(new_notional),
                    }
                ]
            )
            if self._eager_flush:
                self._async_writer.flush(timeout_secs=2.0)
        return qty_dec

    def mark_item_expired(self, symbol: str) -> Optional[PlanItemContract]:
        """Immediately marks `symbol` as `EXPIRED` in Rust $O(1)$ memory to block late entries."""
        norm_sym = str(symbol or "").split(".", 1)[0].strip().upper()
        if not norm_sym:
            return None
        with self._sync_lock:
            item = self._items_by_symbol.get(norm_sym)
            if item is None:
                return None
            if str(item.status or "").strip().upper() in ("EXPIRED", "CANCELLED", "CLOSED"):
                return item
            expired_item = replace(item, status="EXPIRED")
            self._items_by_symbol[norm_sym] = expired_item
            if norm_sym in self._runtime_items_by_symbol:
                _, rt_res = self._runtime_items_by_symbol[norm_sym]
                self._runtime_items_by_symbol[norm_sym] = (expired_item, rt_res)
            self._rust_engine.upsert_plan_item(
                _plan_item_to_rust_envelope(expired_item),
                True,
            )
            return expired_item

    def upsert_plan_item(self, item: PlanItemContract) -> None:
        self.upsert_runtime_plan_item(item, reserved_qty=item.filled_qty)

    def set_trading_date(
        self,
        trading_date: date,
        *,
        clear_runtime_items: bool = True,
        sync: bool = True,
    ) -> None:
        """Hot-reloads `trading_date` on `PostgresPlanGateBackend` during daily rollover."""
        self._trading_date = trading_date
        if self._async_writer is not None:
            self._async_writer.set_trading_date(trading_date)
        if clear_runtime_items:
            with self._sync_lock:
                self._runtime_items_by_symbol.clear()
                self._baseline_items_by_symbol.clear()
                self._removed_runtime_symbols.clear()
                self._released_cap_by_symbol.clear()
                self._closed_cycle_locked_symbols.clear()
                self._reentry_cycle_counts.clear()
        if sync and self._persist_to_db:
            self.sync_from_db(preserve_higher_reservation=False)

    def upsert_runtime_plan_item(
        self,
        item: PlanItemContract,
        *,
        reserved_qty: Optional[Decimal] = None,
        preserve_higher_reservation: bool = False,
    ) -> None:
        """Upserts an in-memory runtime `PlanItemContract` into `RustPlanGateEngine`."""
        adjusted_item = (
            replace(item, filled_qty=reserved_qty)
            if reserved_qty is not None
            else item
        )
        norm_sym = adjusted_item.symbol
        with self._sync_lock:
            self._removed_runtime_symbols.discard(norm_sym)
            self._released_cap_by_symbol.pop(norm_sym, None)
            self._runtime_items_by_symbol[norm_sym] = (adjusted_item, reserved_qty)
            self._rust_engine.upsert_plan_item(
                _plan_item_to_rust_envelope(adjusted_item),
                bool(preserve_higher_reservation),
            )
            self._known_symbols.add(norm_sym)
            self._symbol_by_item_id[str(adjusted_item.item_id)] = norm_sym
            self._items_by_symbol[norm_sym] = adjusted_item
            baseline_item = self._baseline_items_by_symbol.get(norm_sym)
            if baseline_item is None or baseline_item.item_id != adjusted_item.item_id:
                self._baseline_items_by_symbol[norm_sym] = adjusted_item

    def remove_runtime_plan_item(self, symbol: str) -> None:
        """Removes a runtime `PlanItemContract` overlay for `symbol` and evicts it from `RustPlanGateEngine`."""
        norm_sym = str(symbol or "").split(".", 1)[0].strip().upper()
        if not norm_sym:
            return
        with self._sync_lock:
            self._removed_runtime_symbols.add(norm_sym)
            self._baseline_items_by_symbol.pop(norm_sym, None)
            removed_runtime = self._runtime_items_by_symbol.pop(norm_sym, None)
            removed_item = self._items_by_symbol.pop(norm_sym, None)
            self._known_symbols.discard(norm_sym)
            if removed_runtime is not None:
                self._symbol_by_item_id.pop(str(removed_runtime[0].item_id), None)
            if removed_item is not None:
                self._symbol_by_item_id.pop(str(removed_item.item_id), None)
            self._rust_engine.remove_plan_item(norm_sym)

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
        """Monotonically latches `ENTRY_FREEZE` in O(1) Rust memory and enqueues DB persistence."""
        reason_str = str(reason or "ENTRY_FREEZE")
        with self._freeze_lock:
            self._freeze_epoch += 1
            self._monotonic_freeze_latched = True
            self._monotonic_freeze_reason = reason_str
            self._db_freeze_persisted = False
            self._rust_engine.set_entry_freeze(True, reason_str)
        mutation = {
            "kind": "SET_ENTRY_FREEZE",
            "active": True,
            "reason": reason_str,
            "updated_by": str(updated_by or "nautilus:plan_gate"),
            "alert_event_type": str(alert_event_type or "PLAN_GATE_ENTRY_FREEZE"),
        }
        self._alerts.append(mutation)
        if self._async_writer is not None:
            self._async_writer.enqueue_mutations([mutation])
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
        """Atomically updates `ENTRY_FREEZE` in O(1) Rust memory and persists to `system_flags`."""
        if bool(active):
            self.latch_entry_freeze(reason=reason, updated_by=updated_by)
            return

        if self._async_writer is None:
            with self._freeze_lock:
                self._freeze_epoch += 1
                self._monotonic_freeze_latched = False
                self._monotonic_freeze_reason = ""
                self._db_freeze_persisted = False
                self._rust_engine.set_entry_freeze(False, str(reason))
                self._rust_engine.set_backend_unavailable(False, "")
            return

        with self._freeze_lock:
            freeze_epoch_before = self._freeze_epoch

        if self._async_writer.has_failed_writes or self._async_writer.pending_count > 0:
            self._async_writer.flush(
                timeout_secs=min(float(self._connect_timeout_secs), 2.0)
            )
        if self._async_writer.has_failed_writes:
            with self._freeze_lock:
                self._freeze_epoch += 1
                self._monotonic_freeze_latched = True
                self._db_freeze_persisted = False
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
        with self._freeze_lock:
            if (
                not self._async_writer.has_failed_writes
                and self._freeze_epoch == freeze_epoch_before
            ):
                self._freeze_epoch += 1
                self._monotonic_freeze_latched = False
                self._monotonic_freeze_reason = ""
                self._db_freeze_persisted = False
                self._rust_engine.set_entry_freeze(False, str(reason))
                self._rust_engine.set_backend_unavailable(False, "")
            elif self._async_writer.has_failed_writes:
                self._freeze_epoch += 1
                self._monotonic_freeze_latched = True
                self._db_freeze_persisted = False
                self._rust_engine.set_entry_freeze(
                    True, self._monotonic_freeze_reason or "ASYNC_DB_WRITER_FAILURE"
                )

    def flush(self, timeout_secs: float = 5.0) -> None:
        """Waits for all queued asynchronous DB writes to complete."""
        self.flush_rust_mutations(1024)
        if self._async_writer is not None:
            self._async_writer.flush(timeout_secs=timeout_secs)

    def close(self) -> None:
        """Flushes pending writes and closes background DB, listener thread, pipe fds, and sockets."""
        self._listen_stop_event.set()
        wake_w = self._listen_wake_w
        if wake_w is not None:
            try:
                os.write(wake_w, b"\x01")
            except OSError:
                pass
        listen_thread = self._listen_thread
        self._listen_thread = None
        if listen_thread is not None and listen_thread.is_alive():
            if threading.current_thread() is not listen_thread:
                listen_thread.join(timeout=2.0)
        for attr in ("_listen_wake_w", "_listen_wake_r"):
            fd = getattr(self, attr, None)
            setattr(self, attr, None)
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass

        writer = self._async_writer
        if writer is not None:
            try:
                self.flush_rust_mutations(1024)
            except Exception:
                pass
            try:
                writer.close()
            except Exception:
                pass
        self._async_writer = None
        self._db_worker = None
        self._oco_monitor = None
        for attr in ("_listen_conn", "_query_conn"):
            conn = getattr(self, attr, None)
            setattr(self, attr, None)
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

    def _open_listen_conn(self) -> Any:
        conn = psycopg2.connect(
            self._dsn,
            cursor_factory=RealDictCursor,
            connect_timeout=self._connect_timeout_secs,
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                LISTEN {NOTIFY_CHANNEL};
                LISTEN nautilus_plan_locked;
                LISTEN system_flags_changed;
                """
            )
        return conn

    @staticmethod
    def _notify_listener_loop_weak(
        backend_ref: weakref.ReferenceType["PostgresPlanGateBackend"],
        stop_event: threading.Event,
    ) -> None:
        while not stop_event.is_set():
            backend = backend_ref()
            if backend is None:
                break
            conn = backend._listen_conn
            if conn is None or getattr(conn, "closed", 1) != 0:
                try:
                    with backend._listen_poll_lock:
                        conn = backend._open_listen_conn()
                        backend._listen_conn = conn
                    backend.sync_from_db(preserve_higher_reservation=True)
                except Exception as exc:
                    backend._rust_engine.set_backend_unavailable(
                        True, f"PostgreSQL LISTEN connection failed ({exc})"
                    )
                    backend = None
                    conn = None
                    if stop_event.wait(1.0):
                        break
                    continue

            r_fd = backend._listen_wake_r
            wait_fds: List[Any] = [conn]
            if r_fd is not None:
                wait_fds.append(r_fd)
            backend = None
            try:
                readable, _, _ = select.select(wait_fds, [], [], 5.0)
            except Exception as exc:
                wait_fds = []
                conn = None
                if stop_event.is_set():
                    break
                backend = backend_ref()
                if backend is None:
                    break
                backend._rust_engine.set_backend_unavailable(
                    True, f"PostgreSQL notification wait failed ({exc})"
                )
                with backend._listen_poll_lock:
                    cur_conn = backend._listen_conn
                    try:
                        if cur_conn is not None:
                            cur_conn.close()
                    except Exception:
                        pass
                    backend._listen_conn = None
                backend = None
                if stop_event.wait(0.5):
                    break
                continue

            if r_fd is not None and r_fd in readable:
                try:
                    os.read(r_fd, 1024)
                except OSError:
                    pass
                if stop_event.is_set():
                    wait_fds = []
                    conn = None
                    break

            if conn in readable:
                if stop_event.is_set():
                    wait_fds = []
                    conn = None
                    break
                backend = backend_ref()
                if backend is None:
                    wait_fds = []
                    conn = None
                    break
                try:
                    with backend._listen_poll_lock:
                        conn.poll()
                        had_notifies = bool(conn.notifies)
                        conn.notifies.clear()
                    if had_notifies and not stop_event.is_set():
                        backend.sync_from_db(preserve_higher_reservation=True)
                except Exception as exc:
                    backend._rust_engine.set_backend_unavailable(
                        True, f"PostgreSQL notification poll failed ({exc})"
                    )
                    with backend._listen_poll_lock:
                        try:
                            conn.close()
                        except Exception:
                            pass
                        backend._listen_conn = None
                    backend = None
                    wait_fds = []
                    conn = None
                    if stop_event.wait(0.5):
                        break
                    continue
                backend = None
            wait_fds = []
            conn = None

    def _notify_listener_loop(self) -> None:
        self._notify_listener_loop_weak(weakref.ref(self), self._listen_stop_event)

    def _start_notify_listener(self) -> None:
        if not self._persist_to_db:
            return
        if self._listen_thread is not None and self._listen_thread.is_alive():
            return
        if self._listen_wake_r is None or self._listen_wake_w is None:
            self._listen_wake_r, self._listen_wake_w = os.pipe()
        self._listen_stop_event.clear()
        thread = threading.Thread(
            target=PostgresPlanGateBackend._notify_listener_loop_weak,
            args=(weakref.ref(self), self._listen_stop_event),
            name="plan-gate-notify-listener",
            daemon=True,
        )
        self._listen_thread = thread
        thread.start()

    def _initialize_sync_and_triggers(self) -> None:
        try:
            self._listen_conn = self._open_listen_conn()
            self.sync_from_db(preserve_higher_reservation=True)
        except Exception as exc:
            self._rust_engine.set_backend_unavailable(
                True, f"PostgreSQL connection failed ({exc})"
            )
        finally:
            self._start_notify_listener()

    def _poll_notifications_nonblocking(self) -> None:
        if not self._persist_to_db:
            return
        if self._listen_thread is not None and self._listen_thread.is_alive():
            return
        conn = self._listen_conn
        if conn is None or getattr(conn, "closed", 1) != 0:
            return
        try:
            with self._listen_poll_lock:
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
        if not self._persist_to_db or self._async_writer is None:
            return
        with self._db_query_lock:
            self.flush_rust_mutations(1024)
            if (
                self._async_writer.has_failed_writes
                or self._async_writer.pending_count > 0
            ):
                self._async_writer.flush(
                    timeout_secs=min(float(self._connect_timeout_secs), 1.0)
                )

            conn = self._query_conn
            if conn is None or getattr(conn, "closed", 1) != 0:
                try:
                    conn = psycopg2.connect(
                        self._dsn,
                        cursor_factory=RealDictCursor,
                        connect_timeout=self._connect_timeout_secs,
                    )
                    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                    self._query_conn = conn
                except Exception as exc:
                    self._rust_engine.set_backend_unavailable(
                        True, f"PostgreSQL connection failed ({exc})"
                    )
                    return

            with self._freeze_lock:
                freeze_epoch_before = self._freeze_epoch
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

                    need_persist_latch = False
                    reason_to_persist = ""
                    with self._freeze_lock:
                        writer_freeze_pending = bool(
                            self._async_writer.has_failed_or_pending_freeze
                        )
                        if db_freeze_active or writer_freeze_pending:
                            self._monotonic_freeze_latched = True
                            self._monotonic_freeze_reason = (
                                db_freeze_reason
                                or self._monotonic_freeze_reason
                                or "ENTRY_FREEZE"
                            )
                            self._db_freeze_persisted = bool(db_freeze_active)
                            self._rust_engine.set_entry_freeze(
                                True, self._monotonic_freeze_reason
                            )
                            if not db_freeze_active:
                                need_persist_latch = True
                                reason_to_persist = self._monotonic_freeze_reason
                        elif self._monotonic_freeze_latched:
                            can_clear_latch = (
                                self._freeze_epoch == freeze_epoch_before
                                and can_clear_before_query
                                and not self._async_writer.has_failed_or_pending_freeze
                                and self._async_writer.pending_count == 0
                                and self._db_freeze_persisted
                            )
                            if can_clear_latch:
                                self._freeze_epoch += 1
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
                                    need_persist_latch = True
                                    reason_to_persist = reason_to_keep
                        elif (
                            self._freeze_epoch == freeze_epoch_before
                            and not self._async_writer.has_failed_or_pending_freeze
                        ):
                            self._rust_engine.set_entry_freeze(False, "")

                    if need_persist_latch:
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
                            (reason_to_persist,),
                        )
                        cur.execute(
                            """
                            INSERT INTO system_flag_events (
                                flag, active, reason, set_by, ts
                            ) VALUES (
                                'ENTRY_FREEZE', TRUE, %s, 'nautilus:plan_gate:monotonic_latch', NOW()
                            )
                            """,
                            (reason_to_persist,),
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
                                reason_to_persist,
                                json.dumps(
                                    {
                                        "flag": "ENTRY_FREEZE",
                                        "active": True,
                                        "reason": reason_to_persist,
                                    }
                                ),
                                f"plan_gate_latch:{reason_to_persist[:64]}",
                            ),
                        )
                        with self._freeze_lock:
                            if self._monotonic_freeze_latched:
                                self._db_freeze_persisted = True

                    with self._freeze_lock:
                        if not self._async_writer.has_failed_writes and not self._monotonic_freeze_latched:
                            self._rust_engine.set_backend_unavailable(False, "")
                        elif self._async_writer.has_failed_writes:
                            self._rust_engine.set_backend_unavailable(
                                True,
                                f"ENTRY_FREEZE_ACTIVE: {self._monotonic_freeze_reason or 'ASYNC_DB_WRITER_FAILURE'}",
                            )
                        else:
                            self._rust_engine.set_backend_unavailable(False, "")

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
                    with self._sync_lock:
                        late_pending = self._drain_rust_mutations(1024)
                        if late_pending:
                            self._mirror_rust_mutations_to_python_items(late_pending)
                            self._alerts.extend(late_pending)
                            if self._async_writer is not None:
                                self._async_writer.enqueue_mutations(late_pending)
                        with self._freeze_lock:
                            effective_preserve = bool(
                                effective_preserve
                                or bool(late_pending)
                                or self._monotonic_freeze_latched
                                or self._async_writer.pending_count > 0
                                or self._async_writer.has_failed_or_pending_freeze
                            )
                        prev_reserved: Dict[str, Decimal] = {}
                        if effective_preserve:
                            for sym in list(self._known_symbols):
                                prev_reserved[sym] = Decimal(
                                    self._rust_engine.reserved_qty(sym)
                                )
                            for sym in list(self._runtime_items_by_symbol.keys()):
                                if sym not in prev_reserved:
                                    prev_reserved[sym] = Decimal(
                                        self._rust_engine.reserved_qty(sym)
                                    )
                            for row in rows:
                                row_sym = str(row.get("symbol") or "").strip().upper()
                                if row_sym and row_sym not in prev_reserved:
                                    prev_reserved[row_sym] = Decimal(
                                        self._rust_engine.reserved_qty(row_sym)
                                    )
                        prior_known_symbols = set(self._known_symbols)
                        self._known_symbols.clear()
                        self._symbol_by_item_id.clear()
                        self._items_by_symbol.clear()
                        loaded_by_symbol: Dict[str, Tuple[PlanItemContract, bool]] = {}
                        for row in rows:
                            try:
                                plan_item = PlanItemContract.from_db_row(row)
                                norm_sym = plan_item.symbol
                                if norm_sym in self._removed_runtime_symbols:
                                    continue
                                released_cap = self._released_cap_by_symbol.get(norm_sym)
                                if released_cap is not None:
                                    capped_qty = min(plan_item.filled_qty, released_cap)
                                    capped_status = (
                                        "ARMED"
                                        if capped_qty == Decimal("0")
                                        and str(plan_item.status) == "TRIGGERED"
                                        else plan_item.status
                                    )
                                    plan_item = replace(
                                        plan_item,
                                        filled_qty=capped_qty,
                                        status=capped_status,
                                    )
                                    item_preserve = False
                                elif effective_preserve:
                                    prior_reserved = prev_reserved.get(
                                        norm_sym, Decimal("0")
                                    )
                                    if prior_reserved > plan_item.filled_qty:
                                        plan_item = replace(
                                            plan_item,
                                            filled_qty=prior_reserved,
                                        )
                                    item_preserve = True
                                else:
                                    item_preserve = False
                                loaded_by_symbol[norm_sym] = (plan_item, item_preserve)
                            except Exception as item_exc:
                                logger.warning(
                                    "Skipping invalid PlanItemContract row: %s", item_exc
                                )

                        final_upserts: Dict[str, Tuple[PlanItemContract, bool]] = {}
                        for norm_sym, (runtime_item, runtime_reserved) in list(
                            self._runtime_items_by_symbol.items()
                        ):
                            prior_res = prev_reserved.get(norm_sym, Decimal("0"))
                            released_cap = self._released_cap_by_symbol.get(norm_sym)
                            if norm_sym in loaded_by_symbol:
                                db_item, _ = loaded_by_symbol.pop(norm_sym)
                                db_stop = db_item.risk_envelope.initial_stop_price
                                rt_stop = runtime_item.risk_envelope.initial_stop_price
                                if db_item.risk_envelope.side == PlanSide.LONG:
                                    merged_stop = max(db_stop, rt_stop)
                                else:
                                    if db_stop > Decimal("0") and rt_stop > Decimal("0"):
                                        merged_stop = min(db_stop, rt_stop)
                                    else:
                                        merged_stop = max(db_stop, rt_stop)
                                merged_qty_cap = min(
                                    db_item.risk_envelope.approved_qty_cap,
                                    runtime_item.risk_envelope.approved_qty_cap,
                                )
                                merged_env = (
                                    replace(
                                        db_item.risk_envelope,
                                        initial_stop_price=merged_stop,
                                        approved_qty_cap=merged_qty_cap,
                                    )
                                    if (
                                        merged_stop != db_stop
                                        or merged_qty_cap
                                        != db_item.risk_envelope.approved_qty_cap
                                    )
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
                                if released_cap is not None:
                                    effective_res = min(effective_res, released_cap)
                                rt_status = str(runtime_item.status or "").strip().upper()
                                if rt_status in ("EXPIRED", "CANCELLED", "CLOSED"):
                                    merged_status = rt_status
                                elif effective_res == Decimal("0") and str(db_item.status) == "TRIGGERED":
                                    merged_status = "ARMED"
                                else:
                                    merged_status = db_item.status
                                overlaid_item = replace(
                                    db_item,
                                    risk_envelope=merged_env,
                                    filled_qty=effective_res,
                                    status=merged_status,
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
                                if released_cap is not None:
                                    effective_res = min(effective_res, released_cap)
                                merged_status = (
                                    "ARMED"
                                    if effective_res == Decimal("0")
                                    and str(runtime_item.status) == "TRIGGERED"
                                    else runtime_item.status
                                )
                                overlaid_item = replace(
                                    runtime_item,
                                    filled_qty=effective_res,
                                    status=merged_status,
                                )
                            overlay_preserve = bool(effective_preserve and released_cap is None)
                            final_upserts[norm_sym] = (overlaid_item, overlay_preserve)

                        for norm_sym, (plan_item, item_preserve) in loaded_by_symbol.items():
                            final_upserts[norm_sym] = (plan_item, item_preserve)

                        for norm_sym, (final_item, final_preserve) in final_upserts.items():
                            self._rust_engine.upsert_plan_item(
                                _plan_item_to_rust_envelope(final_item),
                                final_preserve,
                            )
                            if final_preserve:
                                rust_res = Decimal(self._rust_engine.reserved_qty(norm_sym))
                                if rust_res > final_item.filled_qty:
                                    final_item = replace(final_item, filled_qty=rust_res)
                                    if norm_sym in self._runtime_items_by_symbol:
                                        self._runtime_items_by_symbol[norm_sym] = (
                                            final_item,
                                            rust_res,
                                        )
                            self._known_symbols.add(norm_sym)
                            self._symbol_by_item_id[str(final_item.item_id)] = norm_sym
                            self._items_by_symbol[norm_sym] = final_item

                        for stale_sym in prior_known_symbols - self._known_symbols:
                            self._rust_engine.remove_plan_item(stale_sym)

                    if self._oco_monitor is not None:
                        try:
                            refresh_fn = getattr(
                                self._oco_monitor, "refresh_coverage_from_db", None
                            ) or getattr(
                                self._oco_monitor, "_hydrate_coverage_from_db", None
                            )
                            if callable(refresh_fn):
                                refresh_fn()
                            else:
                                refresh_sym_fn = getattr(
                                    self._oco_monitor,
                                    "refresh_symbol_coverage_from_db",
                                    None,
                                )
                                if callable(refresh_sym_fn):
                                    for sym_k in list(self._known_symbols):
                                        refresh_sym_fn(sym_k)
                            get_cov = getattr(self._oco_monitor, "get_coverage", None)
                            if callable(get_cov):
                                for sym_k, cur_item in list(
                                    self._items_by_symbol.items()
                                ):
                                    cov = get_cov(sym_k)
                                    cov_stop = getattr(
                                        cov, "stop_price", Decimal("0")
                                    )
                                    if (
                                        cov is not None
                                        and isinstance(cov_stop, Decimal)
                                        and cov_stop > Decimal("0")
                                    ):
                                        cur_stop = (
                                            cur_item.risk_envelope.initial_stop_price
                                        )
                                        tighter_stop = (
                                            max(cur_stop, cov_stop)
                                            if cur_item.risk_envelope.side
                                            == PlanSide.LONG
                                            else (
                                                min(cur_stop, cov_stop)
                                                if cur_stop > Decimal("0")
                                                else cov_stop
                                            )
                                        )
                                        if tighter_stop != cur_stop:
                                            tp_px = (
                                                cur_item.risk_envelope.take_profit_price
                                            )
                                            if tp_px is not None:
                                                if (
                                                    cur_item.risk_envelope.side
                                                    == PlanSide.LONG
                                                    and tp_px <= tighter_stop
                                                ) or (
                                                    cur_item.risk_envelope.side
                                                    == PlanSide.SHORT
                                                    and tp_px >= tighter_stop
                                                ):
                                                    tp_px = None
                                            updated_env = replace(
                                                cur_item.risk_envelope,
                                                initial_stop_price=tighter_stop,
                                                take_profit_price=tp_px,
                                            )
                                            updated_item = replace(
                                                cur_item, risk_envelope=updated_env
                                            )
                                            self._items_by_symbol[sym_k] = updated_item
                                            if sym_k in self._runtime_items_by_symbol:
                                                _, rt_res = (
                                                    self._runtime_items_by_symbol[sym_k]
                                                )
                                                self._runtime_items_by_symbol[sym_k] = (
                                                    updated_item,
                                                    rt_res,
                                                )
                                            self._rust_engine.upsert_plan_item(
                                                _plan_item_to_rust_envelope(
                                                    updated_item
                                                ),
                                                True,
                                            )
                        except Exception as oco_exc:
                            logger.error(
                                "Failed to refresh ProtectiveOcoMonitor during sync_from_db: %s",
                                oco_exc,
                                exc_info=True,
                            )
                            raise
            except Exception as exc:
                self._rust_engine.set_backend_unavailable(
                    True, f"PostgreSQL sync failed ({exc})"
                )

    def _maybe_reanchor_multi_cycle_envelope(self, request: PlanGateOrderRequest) -> None:
        """Re-anchor flat multi-day backtest envelope around current entry price while preserving risk budget."""
        if not (self._allow_multi_cycle_reentry and not self._persist_to_db):
            return
        if request.is_reduce_only or request.is_exit_intent:
            return
        norm_sym = str(request.symbol or "").strip().upper()
        if (
            not norm_sym
            or norm_sym not in self._items_by_symbol
            or self.reserved_qty(norm_sym) != Decimal("0")
            or request.cache_open_entry_qty != Decimal("0")
        ):
            return
        entry_ref = request.reference_price or request.limit_price or Decimal("0")
        cur_item = self._items_by_symbol[norm_sym]
        orig_item = self._baseline_items_by_symbol.get(norm_sym, cur_item)
        if entry_ref <= Decimal("0") or orig_item.risk_envelope.target_qty <= Decimal("0"):
            return
        orig_ref_px = (
            orig_item.risk_envelope.target_notional_usd / orig_item.risk_envelope.target_qty
        )
        cur_ref_px = (
            cur_item.risk_envelope.target_notional_usd / cur_item.risk_envelope.target_qty
            if cur_item.risk_envelope.target_qty > Decimal("0")
            else orig_ref_px
        )
        if (
            abs(entry_ref - orig_ref_px) <= Decimal("0.01")
            and abs(entry_ref - cur_ref_px) <= Decimal("0.01")
        ):
            return
        new_notional = max(
            Decimal("0.01"),
            (entry_ref * orig_item.risk_envelope.target_qty).quantize(Decimal("0.01")),
        )
        new_max_budget = max(
            Decimal("0.01"),
            min(orig_item.risk_envelope.max_risk_budget_usd, new_notional),
        )
        orig_stop_dist = abs(orig_ref_px - orig_item.risk_envelope.initial_stop_price)
        if request.atr is not None and request.atr > Decimal("0"):
            budget_dist = new_max_budget / max(Decimal("1"), request.quantity)
            eff_stop_dist = max(Decimal("0.01"), orig_stop_dist, budget_dist)
        else:
            eff_stop_dist = max(Decimal("0.01"), orig_stop_dist)
        if orig_item.risk_envelope.side == PlanSide.LONG:
            new_stop = max(
                Decimal("0.01"), (entry_ref - eff_stop_dist).quantize(Decimal("0.01"))
            )
        else:
            new_stop = max(
                Decimal("0.02"), (entry_ref + eff_stop_dist).quantize(Decimal("0.01"))
            )
        new_tp: Optional[Decimal] = None
        if orig_item.risk_envelope.take_profit_price is not None:
            orig_tp_dist = max(
                Decimal("0.01"),
                abs(orig_item.risk_envelope.take_profit_price - orig_ref_px),
            )
            if orig_item.risk_envelope.side == PlanSide.LONG:
                new_tp = max(
                    new_stop + Decimal("0.01"),
                    (entry_ref + orig_tp_dist).quantize(Decimal("0.01")),
                )
            else:
                new_tp = max(
                    Decimal("0.01"),
                    min(
                        new_stop - Decimal("0.01"),
                        (entry_ref - orig_tp_dist).quantize(Decimal("0.01")),
                    ),
                )
        reanchored = replace(
            cur_item,
            filled_qty=Decimal("0"),
            risk_envelope=replace(
                cur_item.risk_envelope,
                initial_stop_price=new_stop,
                take_profit_price=new_tp,
                target_notional_usd=new_notional,
                max_risk_budget_usd=new_max_budget,
            ),
        )
        self.upsert_runtime_plan_item(
            reanchored,
            reserved_qty=Decimal("0"),
            preserve_higher_reservation=False,
        )

    def evaluate_and_reserve(self, request: PlanGateOrderRequest) -> PlanGateDecision:
        if request.is_reduce_only or request.is_exit_intent:
            return PlanGateDecision(
                allowed=True,
                reason=None,
                is_exit_bypass=True,
            )

        self._poll_notifications_nonblocking()
        self._maybe_reanchor_multi_cycle_envelope(request)
        norm_sym = str(request.symbol or "").strip().upper()
        req_for_rust = request
        override_stop_reason: str | None = None
        if (
            not self._allow_multi_cycle_reentry
            and request.stop_loss_price is not None
            and norm_sym in self._items_by_symbol
        ):
            cur_item = self._items_by_symbol[norm_sym]
            init_stop = cur_item.risk_envelope.initial_stop_price
            entry_ref = request.limit_price or request.reference_price or Decimal("0")
            if init_stop > Decimal("0") and entry_ref > Decimal("0") and request.stop_loss_price > Decimal("0"):
                if (
                    cur_item.risk_envelope.side == PlanSide.LONG
                    and entry_ref > init_stop
                    and request.stop_loss_price < entry_ref
                    and request.stop_loss_price < init_stop
                ):
                    req_for_rust = replace(request, stop_loss_price=Decimal("0.0001"))
                    override_stop_reason = (
                        f"STOP_LOSS_FLOOR_VIOLATION (STOP_LOSS_BELOW_PLAN_FLOOR): "
                        f"LONG stop_loss_price ({request.stop_loss_price}) < effective_stop_floor ({init_stop}, plan initial_stop={init_stop})"
                    )
                elif (
                    cur_item.risk_envelope.side == PlanSide.SHORT
                    and entry_ref < init_stop
                    and request.stop_loss_price > entry_ref
                    and request.stop_loss_price > init_stop
                ):
                    req_for_rust = replace(request, stop_loss_price=init_stop + entry_ref + Decimal("1000000"))
                    override_stop_reason = (
                        f"STOP_LOSS_FLOOR_VIOLATION (STOP_LOSS_ABOVE_PLAN_CEILING): "
                        f"SHORT stop_loss_price ({request.stop_loss_price}) > effective_stop_ceiling ({init_stop}, plan initial_stop={init_stop})"
                    )
        rust_decision = self._rust_engine.evaluate_and_reserve(
            req_for_rust.to_rust_request()
        )
        decision = PlanGateDecision.from_rust_decision(rust_decision)
        if (
            not decision.allowed
            and override_stop_reason is not None
            and decision.reason is not None
            and "STOP_LOSS_FLOOR_VIOLATION" in decision.reason
        ):
            decision = replace(decision, reason=override_stop_reason)
        pending = self._drain_rust_mutations(64)
        if pending:
            self._mirror_rust_mutations_to_python_items(pending)

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
            self._alerts.extend(pending)
            if self._async_writer is not None:
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
        pending: List[Dict[str, Any]] = []
        with self._sync_lock:
            resolved_symbol = str(symbol or "").strip().upper()
            if not resolved_symbol and item_id is not None:
                resolved_symbol = self._symbol_by_item_id.get(str(item_id), "")
            if not resolved_symbol:
                return
            locked_syms: set[str] = getattr(self, "_closed_cycle_locked_symbols", set())
            was_cycle_locked = resolved_symbol in locked_syms
            prior_locked_qty = Decimal("0")
            if was_cycle_locked and resolved_symbol in self._items_by_symbol:
                cur_item_pre = self._items_by_symbol[resolved_symbol]
                prior_locked_qty = max(
                    self.reserved_qty(resolved_symbol),
                    cur_item_pre.risk_envelope.approved_qty_cap,
                )
                self._rust_engine.upsert_plan_item(
                    _plan_item_to_rust_envelope(cur_item_pre),
                    False,
                )
                self._rust_engine.drain_pending_mutations(64)
            new_res = Decimal(
                str(self._rust_engine.release_reservation(resolved_symbol, str(qty_dec)))
            )
            self._released_cap_by_symbol[resolved_symbol] = new_res
            if resolved_symbol in self._items_by_symbol:
                cur_item = self._items_by_symbol[resolved_symbol]
                reverted_status = (
                    "ARMED"
                    if new_res == Decimal("0") and str(cur_item.status) == "TRIGGERED"
                    else cur_item.status
                )
                self._items_by_symbol[resolved_symbol] = replace(
                    cur_item, filled_qty=new_res, status=reverted_status
                )
            if resolved_symbol in self._runtime_items_by_symbol:
                rt_item, _ = self._runtime_items_by_symbol[resolved_symbol]
                reverted_status = (
                    "ARMED"
                    if new_res == Decimal("0") and str(rt_item.status) == "TRIGGERED"
                    else rt_item.status
                )
                self._runtime_items_by_symbol[resolved_symbol] = (
                    replace(rt_item, filled_qty=new_res, status=reverted_status),
                    prior_locked_qty if was_cycle_locked else new_res,
                )
            pending = self._drain_rust_mutations(64)
            if was_cycle_locked and resolved_symbol in self._items_by_symbol:
                cur_item_post = self._items_by_symbol[resolved_symbol]
                relock_qty = max(
                    prior_locked_qty,
                    new_res,
                    cur_item_post.risk_envelope.approved_qty_cap,
                )
                self._rust_engine.upsert_plan_item(
                    _plan_item_to_rust_envelope(
                        replace(cur_item_post, filled_qty=relock_qty)
                    ),
                    True,
                )
                self._rust_engine.drain_pending_mutations(64)
        if pending:
            self._alerts.extend(pending)
            if self._async_writer is not None:
                self._async_writer.enqueue_mutations(pending)
                if self._eager_flush:
                    self._async_writer.flush(timeout_secs=2.0)

    def release_closed_position_capacity(
        self,
        symbol: UUID | str = "",
        *,
        item_id: Optional[UUID | str] = None,
    ) -> None:
        """Releases closed-cycle entry reservation in multi-cycle backtest mode; locks single-cycle quota in live mode."""
        raw_sym = str(symbol or "").strip()
        norm_sym = raw_sym.split(".", 1)[0].strip().upper()
        with self._sync_lock:
            if norm_sym not in self._items_by_symbol and item_id is not None:
                norm_sym = self._symbol_by_item_id.get(str(item_id), norm_sym)
            if norm_sym not in self._items_by_symbol:
                norm_sym = self._symbol_by_item_id.get(raw_sym, norm_sym)
            item = self._items_by_symbol.get(norm_sym)
            if item is None:
                return
            if not self._allow_multi_cycle_reentry:
                if not hasattr(self, "_closed_cycle_locked_symbols"):
                    self._closed_cycle_locked_symbols: set[str] = set()
                self._closed_cycle_locked_symbols.add(norm_sym)
                locked_qty = max(
                    self.reserved_qty(norm_sym),
                    item.filled_qty,
                    item.risk_envelope.approved_qty_cap,
                )
                self.upsert_runtime_plan_item(
                    item,
                    reserved_qty=locked_qty,
                    preserve_higher_reservation=True,
                )
                self._items_by_symbol[norm_sym] = item
                self._rust_engine.drain_pending_mutations(64)
                return

            cur_res = self.reserved_qty(norm_sym)
            if cur_res > Decimal("0"):
                self._rust_engine.release_reservation(norm_sym, str(cur_res))
                self._rust_engine.drain_pending_mutations(64)
            orig_baseline = self._baseline_items_by_symbol.get(norm_sym, item)
            cycle_idx = self._reentry_cycle_counts.get(norm_sym, 0) + 1
            self._reentry_cycle_counts[norm_sym] = cycle_idx
            next_item_id = uuid5(orig_baseline.item_id, f"reentry-cycle-{cycle_idx}")
            self._symbol_by_item_id.pop(str(item.item_id), None)
            reverted_status = "ARMED" if str(item.status) == "TRIGGERED" else item.status
            reset_item = replace(
                item,
                item_id=next_item_id,
                filled_qty=Decimal("0"),
                status=reverted_status,
            )
            self.upsert_runtime_plan_item(
                reset_item,
                reserved_qty=Decimal("0"),
                preserve_higher_reservation=False,
            )
            self._baseline_items_by_symbol[norm_sym] = replace(
                orig_baseline,
                item_id=next_item_id,
                filled_qty=Decimal("0"),
                status=reverted_status,
            )
            self._rust_engine.drain_pending_mutations(64)
