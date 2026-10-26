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
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence
from uuid import UUID

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
)
from nautilus_trader.trading import ExecutionAlgorithm, ExecutionAlgorithmConfig

from evolution_engine.db.dsn import resolve_database_dsn
from nautilus_trader.quant.contracts.models import (
    PlanItemContract,
    PlanSide,
    _to_decimal,
)

register_uuid()

logger = logging.getLogger(__name__)

PLAN_GATE_EXEC_ALGORITHM_ID = ExecAlgorithmId("PLAN_GATE")
ADMISSIBLE_ENTRY_ITEM_STATUSES = ("ARMED", "TRIGGERED", "PARTIALLY_FILLED")
EXIT_INTENT_TAGS = frozenset({"EXIT", "STOP_LOSS", "TAKE_PROFIT", "PROTECTIVE_OCO", "REDUCE_ONLY"})
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
    atr: Optional[Decimal] = None
    collar_market_order: bool = False
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
        return RustPlanGateOrderRequest(
            client_order_id=self.client_order_id,
            strategy_id=self.strategy_id,
            instrument_id=self.instrument_id,
            symbol=self.symbol,
            venue=self.venue,
            order_side=str(self.order_side),
            order_type=str(self.order_type),
            quantity=str(self.quantity),
            is_reduce_only=bool(self.is_reduce_only),
            evaluation_ts_ns=eval_ns,
            evaluation_ts_iso=eval_ts.isoformat(),
            limit_price=str(self.limit_price) if self.limit_price is not None else None,
            reference_price=str(self.reference_price) if self.reference_price is not None else None,
            atr=str(self.atr) if self.atr is not None else None,
            collar_market_order=bool(self.collar_market_order),
            is_exit_intent=bool(self.is_exit_intent),
            cache_open_entry_qty=str(self.cache_open_entry_qty),
            cache_filled_entry_qty=str(self.cache_filled_entry_qty),
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
    )


class PlanGateBackend(Protocol):
    """Protocol implemented by `PostgresPlanGateBackend` and `SnapshotPlanGateBackend`."""

    def evaluate_and_reserve(self, request: PlanGateOrderRequest) -> PlanGateDecision:
        """Evaluate pre-trade admission and atomically reserve capacity on approval."""
        ...

    def release_reservation(self, symbol: str, quantity: Decimal) -> None:
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
    engine = RustPlanGateEngine(entry_freeze_active, entry_freeze_reason)
    if plan_item is not None:
        adjusted_item = replace(plan_item, filled_qty=prior_reserved_or_filled_qty)
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
    ) -> None:
        self._rust_engine = RustPlanGateEngine(bool(entry_freeze), str(entry_freeze_reason))
        self._items_by_symbol: Dict[str, PlanItemContract] = {}
        self._alerts: List[Dict[str, Any]] = []
        for item in plan_items:
            self.upsert_plan_item(item)

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

    def upsert_plan_item(self, item: PlanItemContract) -> None:
        self._items_by_symbol[item.symbol] = item
        self._rust_engine.upsert_plan_item(_plan_item_to_rust_envelope(item), False)

    def get_plan_item(self, symbol: str) -> Optional[PlanItemContract]:
        return self._items_by_symbol.get(symbol)

    def reserved_qty(self, symbol: str) -> Decimal:
        return Decimal(self._rust_engine.reserved_qty(symbol))

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

    def release_reservation(self, symbol: str, quantity: Decimal) -> None:
        qty_dec = _to_decimal(quantity, "quantity")
        if qty_dec <= Decimal("0"):
            return
        self._rust_engine.release_reservation(symbol, str(qty_dec))
        self._rust_engine.drain_pending_mutations(64)


class _AsyncPlanGateDbWorker:
    """Dedicated background write-behind worker holding a persistent PostgreSQL connection."""

    def __init__(
        self,
        dsn: str,
        market: str,
        trading_date: Optional[date],
        connect_timeout_secs: int,
        rust_engine: RustPlanGateEngine,
    ) -> None:
        self._dsn = dsn
        self._market = market
        self._trading_date = trading_date
        self._connect_timeout_secs = connect_timeout_secs
        self._rust_engine = rust_engine
        self._queue: "queue.Queue[Optional[Dict[str, Any]]]" = queue.Queue()
        self._write_conn: Optional[Any] = None
        self._thread = threading.Thread(
            target=self._run_loop,
            name="nautilus-plangate-async-db-writer",
            daemon=True,
        )
        self._thread.start()

    def enqueue_mutations(self, raw_mutations: Sequence[Any]) -> None:
        for raw in raw_mutations:
            try:
                parsed = dict(raw) if isinstance(raw, Mapping) else json.loads(raw)
                self._queue.put_nowait(parsed)
            except Exception as exc:
                logger.error("Failed to enqueue PlanGate mutation: %s", exc)

    def flush(self, timeout_secs: float = 5.0) -> None:
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

    def _run_loop(self) -> None:
        try:
            while True:
                try:
                    item = self._queue.get(timeout=0.5)
                except queue.Empty:
                    self._close_write_conn()
                    continue
                if item is None:
                    self._queue.task_done()
                    break
                if item.get("kind") == "__FLUSH__":
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
                    conn = self._ensure_conn()
                    with conn:
                        with conn.cursor() as cur:
                            for mutation in batch:
                                self._apply_mutation(cur, mutation)
                except Exception as exc:
                    logger.error("PlanGate async write-behind failed (triggering ENTRY_FREEZE): %s", exc)
                    self._close_write_conn()
                    self._rust_engine.set_entry_freeze(True, f"ASYNC_DB_WRITER_FAILURE: {exc}")
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
                    %s, %s, 'ORDER_ADMITTED', %s, %s, %s::jsonb
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
                        %s, %s, 'ORDER_DENIED', %s, %s, %s::jsonb
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


class PostgresPlanGateBackend:
    """In-memory O(1) Rust `PlanGate` backend synchronized with PostgreSQL via `LISTEN/NOTIFY` and async write-behind persistence."""

    def __init__(
        self,
        dsn: Optional[str] = None,
        trading_date: Optional[date] = None,
        market: str = "US_EQUITY",
        connect_timeout_secs: int = 3,
        eager_flush: bool = True,
    ) -> None:
        self._dsn = resolve_database_dsn(dsn)
        self._trading_date = trading_date
        self._market = market
        self._connect_timeout_secs = max(1, int(connect_timeout_secs))
        self._eager_flush = bool(eager_flush)
        self._rust_engine = RustPlanGateEngine(False, "")
        self._listen_conn: Optional[Any] = None
        self._sync_lock = threading.Lock()
        self._initialize_sync_and_triggers()
        self._async_writer = _AsyncPlanGateDbWorker(
            dsn=self._dsn,
            market=self._market,
            trading_date=self._trading_date,
            connect_timeout_secs=self._connect_timeout_secs,
            rust_engine=self._rust_engine,
        )

    @property
    def dsn(self) -> str:
        return self._dsn

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
        """Atomically updates `ENTRY_FREEZE` in $O(1)$ Rust memory and persists to `system_flags`."""
        self._rust_engine.set_entry_freeze(bool(active), str(reason))
        self._async_writer.enqueue_mutations(
            [
                {
                    "kind": "SET_ENTRY_FREEZE",
                    "active": bool(active),
                    "reason": str(reason),
                    "updated_by": str(updated_by or "nautilus:plan_gate"),
                }
            ]
        )
        if self._eager_flush:
            self._async_writer.flush(timeout_secs=2.0)

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
            self.sync_from_db(preserve_higher_reservation=False)
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
                self.sync_from_db(preserve_higher_reservation=False)
        except Exception as exc:
            self._rust_engine.set_backend_unavailable(
                True, f"PostgreSQL notification poll failed ({exc})"
            )

    def sync_from_db(self, preserve_higher_reservation: bool = False) -> None:
        """Synchronizes `system_flags.ENTRY_FREEZE` and locked `trading_plan_items` into `RustPlanGateEngine`."""
        with self._sync_lock:
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
                    entry_freeze_active = bool(flag_row["active"]) if flag_row else False
                    entry_freeze_reason = str(flag_row["reason"]) if flag_row else ""
                    self._rust_engine.set_entry_freeze(entry_freeze_active, entry_freeze_reason)
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
                    self._rust_engine.clear_plan_items()
                    for row in rows:
                        try:
                            plan_item = PlanItemContract.from_db_row(row)
                            self._rust_engine.upsert_plan_item(
                                _plan_item_to_rust_envelope(plan_item),
                                preserve_higher_reservation,
                            )
                        except Exception as item_exc:
                            logger.warning("Skipping invalid PlanItemContract row: %s", item_exc)
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
        rust_decision = self._rust_engine.evaluate_and_reserve(request.to_rust_request())
        decision = PlanGateDecision.from_rust_decision(rust_decision)

        pending = self._rust_engine.drain_pending_mutations(64)
        if pending:
            self._async_writer.enqueue_mutations(pending)
            if self._eager_flush:
                self._async_writer.flush(timeout_secs=2.0)

        return decision

    def release_reservation(self, symbol: str, quantity: Decimal) -> None:
        qty_dec = _to_decimal(quantity, "quantity")
        if qty_dec <= Decimal("0"):
            return
        self._poll_notifications_nonblocking()
        self._rust_engine.release_reservation(symbol, str(qty_dec))
        pending = self._rust_engine.drain_pending_mutations(64)
        if pending:
            self._async_writer.enqueue_mutations(pending)
            if self._eager_flush:
                self._async_writer.flush(timeout_secs=2.0)


class PlanGateExecAlgorithmConfig(ExecutionAlgorithmConfig):
    """Configuration for `PlanGateExecAlgorithm`."""

    def __new__(
        cls,
        *,
        exec_algorithm_id: ExecAlgorithmId | str = PLAN_GATE_EXEC_ALGORITHM_ID,
        collar_market_orders: bool = False,
        log_events: bool = True,
        log_commands: bool = True,
        **kwargs: object,
    ) -> PlanGateExecAlgorithmConfig:
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
        collar_market_orders: bool = False,
        log_events: bool = True,
        log_commands: bool = True,
        **_kwargs: object,
    ) -> None:
        self.collar_market_orders = bool(collar_market_orders)


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
    """

    def __init__(
        self,
        backend: PlanGateBackend,
        config: Optional[PlanGateExecAlgorithmConfig] = None,
    ) -> None:
        resolved_cfg = config or PlanGateExecAlgorithmConfig(
            exec_algorithm_id=PLAN_GATE_EXEC_ALGORITHM_ID,
        )
        super().__init__(resolved_cfg)
        self._backend = backend
        self._collar_market_orders = bool(
            getattr(resolved_cfg, "collar_market_orders", False)
        )
        self._decisions: Dict[str, PlanGateDecision] = {}

    def __new__(
        cls,
        backend: Optional[PlanGateBackend] = None,
        config: Optional[PlanGateExecAlgorithmConfig] = None,
    ) -> PlanGateExecAlgorithm:
        resolved_cfg = config or PlanGateExecAlgorithmConfig(
            exec_algorithm_id=PLAN_GATE_EXEC_ALGORITHM_ID,
        )
        return super().__new__(cls, resolved_cfg)

    @property
    def backend(self) -> PlanGateBackend:
        return self._backend

    @property
    def decisions(self) -> Mapping[str, PlanGateDecision]:
        return dict(self._decisions)

    def close(self) -> None:
        close_fn = getattr(self._backend, "close", None)
        if callable(close_fn):
            close_fn()

    def on_order(self, order: Any) -> None:
        """Intercept a single routed order and either submit, spawn collared limit, or deny."""
        self._evaluate_and_route_single_order(order)

    def on_order_list(self, order_list: Any, orders: Sequence[Any]) -> None:
        """Intercept an `OrderList` (e.g., entry + protective OCO bracket legs)."""
        if not orders:
            return

        primary_order = orders[0]
        if len(orders) == 1:
            self._evaluate_and_route_single_order(primary_order)
            return

        request = self._build_order_request(primary_order)
        decision = self._backend.evaluate_and_reserve(request)
        self._decisions[str(primary_order.client_order_id)] = decision

        if not decision.allowed:
            reason = decision.reason or "PLAN_GATE_DENIED"
            for child_order in orders:
                self._decisions[str(child_order.client_order_id)] = decision
                self.deny_order(child_order, reason)
            return

        for idx, child_order in enumerate(orders):
            if idx == 0 and decision.derived_limit_price is not None and child_order.order_type == OrderType.MARKET:
                spawned = self.spawn_limit(
                    primary=child_order,
                    quantity=child_order.quantity,
                    price=Price.from_str(str(decision.derived_limit_price)),
                    time_in_force=child_order.time_in_force,
                    reduce_only=bool(getattr(child_order, "is_reduce_only", False)),
                    reduce_primary=True,
                )
                self.submit_order(spawned)
            else:
                self._decisions[str(child_order.client_order_id)] = decision
                self.submit_order(child_order)

    def _evaluate_and_route_single_order(self, order: Any) -> PlanGateDecision:
        request = self._build_order_request(order)
        decision = self._backend.evaluate_and_reserve(request)
        self._decisions[str(order.client_order_id)] = decision

        if not decision.allowed:
            self.deny_order(order, decision.reason or "PLAN_GATE_DENIED")
            return decision

        if (
            decision.derived_limit_price is not None
            and order.order_type == OrderType.MARKET
            and not decision.is_exit_bypass
        ):
            spawned = self.spawn_limit(
                primary=order,
                quantity=order.quantity,
                price=Price.from_str(str(decision.derived_limit_price)),
                time_in_force=order.time_in_force,
                reduce_only=bool(getattr(order, "is_reduce_only", False)),
                reduce_primary=True,
            )
            self.submit_order(spawned)
        else:
            self.submit_order(order)
        return decision

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
        atr_val = (
            _to_decimal(params["atr"], "atr")
            if params.get("atr") is not None
            else None
        )

        collar_market_param = str(params.get("collar_market_order") or "").strip().lower()
        collar_market_order = (
            self._collar_market_orders
            or collar_market_param in ("1", "true", "yes")
        )

        eval_ts = self._current_evaluation_ts(params)
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
            atr=atr_val,
            collar_market_order=collar_market_order,
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
        if not self.is_registered():
            return False
        try:
            positions = self.cache.positions_open(instrument_id=instrument_id)
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
        if self.is_registered():
            try:
                quote = self.cache.quote_tick(instrument_id)
                if quote is not None:
                    bid = Decimal(str(quote.bid_price.as_decimal()))
                    ask = Decimal(str(quote.ask_price.as_decimal()))
                    if bid > Decimal("0") and ask > Decimal("0"):
                        return ((bid + ask) / Decimal("2")).quantize(Decimal("0.0001"))
            except Exception:
                pass
            try:
                trade = self.cache.trade_tick(instrument_id)
                if trade is not None:
                    px = Decimal(str(trade.price.as_decimal()))
                    if px > Decimal("0"):
                        return px
            except Exception:
                pass
        return None

    def _current_evaluation_ts(self, params: Mapping[str, Any]) -> datetime:
        if params.get("evaluation_ts") is not None:
            raw_ts = str(params["evaluation_ts"])
            dt = datetime.fromisoformat(raw_ts)
            return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
        if self.is_registered():
            try:
                now_ns = int(self.clock.timestamp_ns())
                if now_ns > 100_000_000_000_000_000:
                    return datetime.fromtimestamp(now_ns / 1e9, tz=timezone.utc)
            except Exception:
                pass
        return datetime.now(timezone.utc)

    def _compute_cache_entry_quantities(
        self,
        instrument_id: Any,
        order_side: OrderSide,
        current_client_order_id: str,
    ) -> tuple[Decimal, Decimal]:
        if not self.is_registered():
            return Decimal("0"), Decimal("0")
        open_qty = Decimal("0")
        filled_qty = Decimal("0")
        try:
            for cached_order in self.cache.orders(instrument_id=instrument_id):
                cid = str(cached_order.client_order_id)
                if cid == current_client_order_id:
                    continue
                if getattr(cached_order, "is_reduce_only", False):
                    continue
                if cached_order.side != order_side:
                    continue
                if getattr(cached_order, "is_spawned", False):
                    continue
                f_qty = Decimal(str(cached_order.filled_qty.as_decimal()))
                filled_qty += f_qty
                if cached_order.is_open:
                    l_qty = Decimal(str(cached_order.leaves_qty.as_decimal()))
                    open_qty += l_qty
        except Exception:
            pass
        return open_qty, filled_qty
