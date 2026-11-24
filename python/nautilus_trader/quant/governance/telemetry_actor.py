"""Nautilus v2 PostgreSQL telemetry actor and protective OCO coverage monitor.

Subscribes to Nautilus order and position lifecycle events (`OrderSubmitted`,
`OrderAccepted`, `OrderDenied`, `OrderCanceled`, `OrderRejected`, `OrderFilled`,
`PositionOpened`, `PositionChanged`, `PositionClosed`), idempotently persists
execution telemetry into PostgreSQL (`orders`, `order_fills`, `trading_plan_events`,
`alert_outbox`, `trading_positions`), and enforces fail-closed protective OCO
coverage via `ProtectiveOcoMonitor`.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
import json
import logging
import queue
import threading
import time
import types
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple
from uuid import UUID
import weakref
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor

from nautilus_trader.common import DataActor, DataActorConfig
from nautilus_trader.model import (
    ActorId,
    ClientOrderId,
    OrderAccepted,
    OrderCanceled,
    OrderDenied,
    OrderExpired,
    OrderFilled,
    OrderInitialized,
    OrderRejected,
    OrderSubmitted,
    OrderUpdated,
    PositionChanged,
    PositionClosed,
    PositionOpened,
)
try:
    from nautilus_trader.model import AccountState
except ImportError:  # pragma: no cover
    AccountState = None  # type: ignore[assignment,misc]

from evolution_engine.db.dsn import resolve_database_dsn
from nautilus_trader.quant.contracts.models import PlanSide
from nautilus_trader.quant.contracts.repository import PlanContractRepository
from nautilus_trader.quant.governance.bounded_collections import (
    _BoundedFifoDict,
    _BoundedFifoSet,
)
from nautilus_trader.quant.governance.plan_gate_backend import EXIT_INTENT_TAGS

logger = logging.getLogger(__name__)


class _AsyncTelemetryDbWorker:
    """Dedicated background write-behind worker holding a persistent PostgreSQL connection."""

    def __init__(
        self,
        dsn: str,
        connect_timeout_secs: int = 3,
        on_db_error_callback: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        self._dsn = dsn
        self._connect_timeout_secs = max(1, int(connect_timeout_secs))
        self._on_db_error_callback: Optional[Callable[[str, str], None]] = None
        self._on_db_error_weak: Optional[weakref.WeakMethod[Any]] = None
        if on_db_error_callback is not None:
            if isinstance(on_db_error_callback, types.MethodType):
                self._on_db_error_weak = weakref.WeakMethod(on_db_error_callback)
            else:
                self._on_db_error_callback = on_db_error_callback
        self._queue: "queue.Queue[Optional[Tuple[Callable[[Any], None], Optional[threading.Event]]]]" = queue.Queue()
        self._state_lock = threading.Lock()
        self._retry_buffer: List[Callable[[Any], None]] = []
        self._consecutive_failures: int = 0
        self._next_retry_monotonic: float = 0.0
        self._conn: Optional[Any] = None
        self._thread = threading.Thread(
            target=self._run_loop,
            name="nautilus-telemetry-async-db-writer",
            daemon=True,
        )
        self._thread.start()

    def _resolve_db_error_callback(self) -> Optional[Callable[[str, str], None]]:
        if self._on_db_error_callback is not None:
            return self._on_db_error_callback
        if self._on_db_error_weak is not None:
            return self._on_db_error_weak()
        return None

    def submit(self, task_fn: Callable[[Any], None], eager_flush: bool = False) -> None:
        done = threading.Event() if eager_flush else None
        self._queue.put((task_fn, done))
        if done is not None:
            done.wait(timeout=2.0)

    def flush(self, timeout_secs: float = 5.0) -> None:
        if not self._thread.is_alive():
            return
        done = threading.Event()
        self._queue.put((lambda _cur: None, done))
        done.wait(timeout=max(0.05, float(timeout_secs)))

    def close(self) -> None:
        self._queue.put(None)
        if self._thread.is_alive() and threading.current_thread() is not self._thread:
            self._thread.join(timeout=2.0)
        self._close_conn()
        self._on_db_error_callback = None
        self._on_db_error_weak = None
        with self._state_lock:
            self._retry_buffer.clear()

    def _close_conn(self) -> None:
        conn = self._conn
        self._conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def _ensure_conn(self) -> Any:
        if self._conn is None or getattr(self._conn, "closed", 1) != 0:
            self._conn = psycopg2.connect(
                self._dsn,
                cursor_factory=RealDictCursor,
                connect_timeout=self._connect_timeout_secs,
            )
        return self._conn

    def _execute_tasks(self, tasks: List[Callable[[Any], None]]) -> None:
        with self._state_lock:
            combined = list(self._retry_buffer) + list(tasks)
            self._retry_buffer.clear()
        if not combined:
            return
        try:
            conn = self._ensure_conn()
            with conn:
                with conn.cursor() as cur:
                    for fn in combined:
                        fn(cur)
            with self._state_lock:
                self._consecutive_failures = 0
                self._next_retry_monotonic = 0.0
        except Exception as exc:
            logger.error("Telemetry async write-behind batch failed: %s", exc)
            with self._state_lock:
                merged = combined + list(self._retry_buffer)
                if len(merged) > 4096:
                    merged = merged[-4096:]
                self._retry_buffer = merged
                self._consecutive_failures += 1
                backoff = min(30.0, 0.5 * (2 ** min(self._consecutive_failures, 6)))
                self._next_retry_monotonic = time.monotonic() + backoff
            cb = self._resolve_db_error_callback()
            if cb is not None:
                try:
                    cb(
                        "POSTGRES_TELEMETRY",
                        f"TELEMETRY_DB_WRITE_FAILED: {exc}",
                    )
                except Exception as cb_exc:
                    logger.error("Telemetry DB error callback failed: %s", cb_exc)
            self._close_conn()

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
                        self._execute_tasks([])
                    elif not has_retries:
                        self._close_conn()
                    continue
                if item is None:
                    self._queue.task_done()
                    break
                batch = [item]
                should_exit = False
                while True:
                    try:
                        nxt = self._queue.get_nowait()
                        if nxt is None:
                            self._queue.task_done()
                            should_exit = True
                            break
                        batch.append(nxt)
                    except queue.Empty:
                        break

                try:
                    self._execute_tasks([fn for fn, _ in batch])
                finally:
                    for _, done_ev in batch:
                        if done_ev is not None:
                            done_ev.set()
                        self._queue.task_done()
                if should_exit:
                    break
        finally:
            self._close_conn()


# Canonical alias matching Nautilus Actor terminology
Actor = DataActor
ActorConfig = DataActorConfig

DEFAULT_TELEMETRY_ACTOR_ID = "POSTGRES_TELEMETRY"


def _to_decimal(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    if isinstance(value, Decimal):
        return value
    text = str(value).strip()
    if not text:
        return Decimal(default)
    token = text.split()[0]
    try:
        return Decimal(token)
    except Exception:
        return Decimal(default)


def _normalize_symbol(raw_symbol_or_instrument: Any) -> str:
    text = str(raw_symbol_or_instrument or "").strip().upper()
    if not text:
        return ""
    if "." in text:
        return text.split(".", 1)[0].strip()
    return text


def _normalize_side_str(raw_side: Any) -> str:
    text = str(raw_side or "").strip().upper()
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    if "BUY" in text or "LONG" in text:
        return "BUY"
    if "SELL" in text or "SHORT" in text:
        return "SELL"
    if "FLAT" in text:
        return "FLAT"
    return text or "BUY"


def _normalize_order_type_str(raw_type: Any) -> str:
    text = str(raw_type or "MARKET").strip().upper()
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    return text or "MARKET"


def _is_protective_stop_order_type(order_type_str: str) -> bool:
    normalized = order_type_str.upper()
    return any(
        token in normalized
        for token in ("STOP", "STOP_MARKET", "STOP_LIMIT", "TRAILING_STOP")
    )


def _is_protective_stop_meta(meta: Dict[str, Any]) -> bool:
    if not _is_protective_stop_order_type(str(meta.get("order_type") or "")):
        return False
    intent = str(meta.get("intent") or "").strip().upper()
    is_reduce_only = bool(meta.get("is_reduce_only", False))
    if intent == "ENTRY" and not is_reduce_only:
        return False
    return True


@dataclass(frozen=True)
class ProtectiveOrderCoverage:
    """Tracked protective stop-loss / OCO coverage for a symbol."""

    symbol: str
    broker: str = "alpaca"
    broker_order_id: str = ""
    covered_qty: Decimal = Decimal("0")
    stop_price: Decimal = Decimal("0")
    take_profit_price: Optional[Decimal] = None
    status: str = "ACTIVE"
    item_id: Optional[UUID] = None


@dataclass(frozen=True)
class OcoCheckResult:
    """Outcome of a `ProtectiveOcoMonitor` coverage verification."""

    symbol: str
    pos_qty: Decimal
    covered_qty: Decimal
    stop_price: Decimal
    status: str
    is_covered: bool
    triggered_entry_freeze: bool
    reason: Optional[str] = None


class ProtectiveOcoMonitor:
    """Monitors non-zero positions for protective stop-loss/OCO coverage.

    Invariants:
    - Every open position (`pos_qty > 0`) must have an active protective stop order
      with `covered_qty >= pos_qty` and `stop_price > 0`.
    - If `pos_qty > covered_qty` or protective stop coverage is lost after a cancel,
      reject, expiry, or partial replacement failure, `ProtectiveOcoMonitor`
      atomically:
        1. Updates `protective_orders.status = 'MISSING'`
        2. Sets `system_flags` (`flag = 'ENTRY_FREEZE', active = TRUE`)
        3. Appends to `system_flag_events`
        4. Inserts a `CRITICAL` `RECONCILIATION_MISMATCH` alert into `alert_outbox`
    """

    def __init__(
        self,
        dsn: Optional[str] = None,
        broker: str = "alpaca",
        market: str = "US_EQUITY",
        connect_timeout_secs: int = 3,
        on_entry_freeze_callback: Optional[Callable[[str, str], None]] = None,
        *,
        persist_to_db: bool = True,
    ) -> None:
        self._persist_to_db = bool(persist_to_db)
        self._dsn = (
            resolve_database_dsn(dsn)
            if self._persist_to_db
            else (str(dsn) if dsn is not None else "")
        )
        self._broker = str(broker or "alpaca").strip().lower()
        self._market = str(market or "US_EQUITY").strip().upper()
        self._connect_timeout_secs = max(1, int(connect_timeout_secs))
        self._on_entry_freeze_callback: Any = (
            self._wrap_callback(on_entry_freeze_callback)
            if on_entry_freeze_callback is not None
            else None
        )
        self._entry_freeze_callbacks: List[Any] = []
        self._positions_qty: Dict[str, Decimal] = {}
        self._unapplied_pos_reduction_by_symbol: Dict[str, Decimal] = {}
        self._coverage_by_symbol: Dict[str, ProtectiveOrderCoverage] = {}
        self._protective_orders_by_id: Dict[str, ProtectiveOrderCoverage] = {}
        self._safe_exit_symbols: Set[str] = set()
        self._async_writer: Optional[_AsyncTelemetryDbWorker] = (
            _AsyncTelemetryDbWorker(
                dsn=self._dsn,
                connect_timeout_secs=self._connect_timeout_secs,
                on_db_error_callback=self._notify_entry_freeze,
            )
            if self._persist_to_db
            else None
        )
        if self._persist_to_db and self._dsn:
            self._hydrate_coverage_from_db()

    def begin_safe_exit(self, symbol: str, reason: str = "TWO_PHASE_SAFE_EXIT") -> None:
        _ = reason
        norm = _normalize_symbol(symbol)
        if norm:
            self._safe_exit_symbols.add(norm)

    def end_safe_exit(self, symbol: str) -> None:
        norm = _normalize_symbol(symbol)
        if norm:
            self._safe_exit_symbols.discard(norm)

    def is_safe_exit_active(self, symbol: str) -> bool:
        norm = _normalize_symbol(symbol)
        return bool(norm and norm in self._safe_exit_symbols)

    def is_safe_exit_in_flight(self, symbol: str) -> bool:
        return self.is_safe_exit_active(symbol)

    @staticmethod
    def _wrap_callback(cb: Callable[[str, str], None]) -> Any:
        if isinstance(cb, types.MethodType):
            return weakref.WeakMethod(cb)
        return cb

    @staticmethod
    def _resolve_callback(entry: Any) -> Optional[Callable[[str, str], None]]:
        if isinstance(entry, weakref.WeakMethod):
            return entry()
        return entry

    def add_entry_freeze_callback(self, callback: Callable[[str, str], None]) -> None:
        if callback is None:
            return
        primary = self._resolve_callback(self._on_entry_freeze_callback)
        if callback is primary or callback == primary:
            return
        alive_entries: List[Any] = []
        already_present = False
        for entry in self._entry_freeze_callbacks:
            resolved = self._resolve_callback(entry)
            if resolved is None:
                continue
            alive_entries.append(entry)
            if resolved is callback or resolved == callback:
                already_present = True
        if not already_present:
            alive_entries.append(self._wrap_callback(callback))
        self._entry_freeze_callbacks = alive_entries

    def remove_entry_freeze_callback(
        self, callback: Optional[Callable[[str, str], None]]
    ) -> None:
        if callback is None:
            return
        primary = self._resolve_callback(self._on_entry_freeze_callback)
        if primary is callback or primary == callback:
            self._on_entry_freeze_callback = None
        kept: List[Any] = []
        for entry in self._entry_freeze_callbacks:
            resolved = self._resolve_callback(entry)
            if resolved is None or resolved is callback or resolved == callback:
                continue
            kept.append(entry)
        self._entry_freeze_callbacks = kept

    def _notify_entry_freeze(self, symbol: str, reason: str) -> None:
        primary = self._resolve_callback(self._on_entry_freeze_callback)
        if primary is not None:
            try:
                primary(symbol, reason)
            except Exception as exc:
                logger.error("Primary entry freeze callback failed: %s", exc)
        for entry in list(self._entry_freeze_callbacks):
            cb = self._resolve_callback(entry)
            if cb is None or cb is primary or cb == primary:
                continue
            try:
                cb(symbol, reason)
            except Exception as exc:
                logger.error("Secondary entry freeze callback failed: %s", exc)

    @property
    def dsn(self) -> str:
        return self._dsn

    @property
    def broker(self) -> str:
        return self._broker

    def flush(self, timeout_secs: float = 5.0) -> None:
        if self._async_writer is not None:
            self._async_writer.flush(timeout_secs=timeout_secs)

    def close(self) -> None:
        self._on_entry_freeze_callback = None
        self._entry_freeze_callbacks.clear()
        writer = self._async_writer
        self._async_writer = None
        try:
            if writer is not None:
                writer.close()
        except Exception:
            pass

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def get_position_qty(self, symbol: str) -> Decimal:
        norm = _normalize_symbol(symbol)
        return self._positions_qty.get(norm, Decimal("0"))

    @staticmethod
    def _table_exists(cur: Any, table_name: str) -> bool:
        cur.execute("SELECT to_regclass(%s) AS reg", (f"public.{table_name}",))
        row = cur.fetchone()
        if isinstance(row, Mapping):
            return row.get("reg") is not None
        if isinstance(row, (tuple, list)) and row:
            return row[0] is not None
        return False

    def _hydrate_coverage_from_db(self) -> None:
        if not self._persist_to_db or not self._dsn:
            return
        try:
            conn = psycopg2.connect(
                self._dsn,
                cursor_factory=RealDictCursor,
                connect_timeout=self._connect_timeout_secs,
            )
            try:
                with conn.cursor() as cur:
                    if not self._table_exists(cur, "protective_orders"):
                        return
                    cur.execute(
                        """
                        SELECT broker, symbol, item_id, broker_order_id,
                               qty, stop_price, take_profit_price, status
                        FROM protective_orders
                        WHERE LOWER(broker) = LOWER(%s)
                        """,
                        (self._broker,),
                    )
                    rows = cur.fetchall() or []
                    for row in rows:
                        norm_sym = _normalize_symbol(str(row.get("symbol") or ""))
                        if not norm_sym:
                            continue
                        raw_item_id = row.get("item_id")
                        parsed_item_id: Optional[UUID] = None
                        if raw_item_id:
                            try:
                                parsed_item_id = (
                                    raw_item_id
                                    if isinstance(raw_item_id, UUID)
                                    else UUID(str(raw_item_id))
                                )
                            except (TypeError, ValueError):
                                parsed_item_id = None
                        cov = ProtectiveOrderCoverage(
                            symbol=norm_sym,
                            broker=str(row.get("broker") or self._broker),
                            broker_order_id=str(row.get("broker_order_id") or ""),
                            covered_qty=_to_decimal(row.get("qty"), "0"),
                            stop_price=_to_decimal(row.get("stop_price"), "0"),
                            take_profit_price=(
                                _to_decimal(row.get("take_profit_price"), "0")
                                if row.get("take_profit_price") is not None
                                else None
                            ),
                            status=str(row.get("status") or "MISSING").strip().upper(),
                            item_id=parsed_item_id,
                        )
                        self._coverage_by_symbol[norm_sym] = cov
                        if cov.broker_order_id:
                            self._protective_orders_by_id[cov.broker_order_id] = cov
            finally:
                conn.close()
        except Exception as exc:
            logger.error(
                "ProtectiveOcoMonitor failed to hydrate protective_orders on startup: %s",
                exc,
                exc_info=True,
            )
            raise

    def get_coverage(self, symbol: str) -> Optional[ProtectiveOrderCoverage]:
        norm = _normalize_symbol(symbol)
        return self._coverage_by_symbol.get(norm)

    def refresh_coverage_from_db(self, symbol: Optional[str] = None) -> None:
        """Refreshes protective order coverage from PostgreSQL (`protective_orders`)."""
        if symbol:
            self.refresh_symbol_coverage_from_db(symbol)
        else:
            self._hydrate_coverage_from_db()

    sync_from_db = refresh_coverage_from_db

    def refresh_symbol_coverage_from_db(
        self, symbol: str
    ) -> Optional[ProtectiveOrderCoverage]:
        norm_sym = _normalize_symbol(symbol)
        if not norm_sym or not self._persist_to_db or not self._dsn:
            return None
        try:
            timeout_secs = max(1, min(int(self._connect_timeout_secs), 2))
            with contextlib.closing(
                psycopg2.connect(
                    self._dsn,
                    cursor_factory=RealDictCursor,
                    connect_timeout=timeout_secs,
                    options="-c statement_timeout=1500",
                )
            ) as conn:
                with conn.cursor() as cur:
                    if not self._table_exists(cur, "protective_orders"):
                        return None
                    cur.execute(
                        """
                        SELECT broker, symbol, item_id, broker_order_id,
                               qty, stop_price, take_profit_price, status
                        FROM protective_orders
                        WHERE LOWER(broker) = LOWER(%s) AND symbol = %s
                        """,
                        (self._broker, norm_sym),
                    )
                    row = cur.fetchone()
                    if not row:
                        return None
                    raw_item_id = row.get("item_id")
                    parsed_item_id: Optional[UUID] = None
                    if raw_item_id:
                        try:
                            parsed_item_id = (
                                raw_item_id
                                if isinstance(raw_item_id, UUID)
                                else UUID(str(raw_item_id))
                            )
                        except (TypeError, ValueError):
                            parsed_item_id = None
                    cov = ProtectiveOrderCoverage(
                        symbol=norm_sym,
                        broker=str(row.get("broker") or self._broker),
                        broker_order_id=str(row.get("broker_order_id") or "").strip(),
                        covered_qty=_to_decimal(row.get("qty"), "0"),
                        stop_price=_to_decimal(row.get("stop_price"), "0"),
                        take_profit_price=(
                            _to_decimal(row.get("take_profit_price"), "0")
                            if row.get("take_profit_price") is not None
                            else None
                        ),
                        status=str(row.get("status") or "MISSING").strip().upper(),
                        item_id=parsed_item_id,
                    )
                    self._coverage_by_symbol[norm_sym] = cov
                    if cov.broker_order_id:
                        self._protective_orders_by_id[cov.broker_order_id] = cov
                    return cov
        except Exception as exc:
            logger.error(
                "ProtectiveOcoMonitor failed to refresh protective_orders for %s: %s",
                norm_sym,
                exc,
                exc_info=True,
            )
            raise

    def register_protective_order(
        self,
        symbol: str,
        covered_qty: Any,
        stop_price: Any,
        take_profit_price: Optional[Any] = None,
        broker_order_id: str = "",
        item_id: Optional[UUID] = None,
        status: str = "ACTIVE",
        persist: bool = True,
    ) -> ProtectiveOrderCoverage:
        """Registers or updates a protective stop-loss / OCO order for `symbol`."""
        norm_sym = _normalize_symbol(symbol)
        if not norm_sym:
            raise ValueError("symbol must be non-empty")
        qty_dec = max(Decimal("0"), abs(_to_decimal(covered_qty, "0")))
        stop_dec = max(Decimal("0"), _to_decimal(stop_price, "0"))
        tp_dec = (
            _to_decimal(take_profit_price, "0")
            if take_profit_price is not None
            else None
        )
        norm_status = str(status or "ACTIVE").strip().upper()
        coverage = ProtectiveOrderCoverage(
            symbol=norm_sym,
            broker=self._broker,
            broker_order_id=str(broker_order_id or ""),
            covered_qty=qty_dec,
            stop_price=stop_dec,
            take_profit_price=tp_dec,
            status=norm_status,
            item_id=item_id,
        )
        prev_cov = self._coverage_by_symbol.get(norm_sym)
        if (
            prev_cov is not None
            and prev_cov.broker_order_id
            and prev_cov.broker_order_id != coverage.broker_order_id
        ):
            self._protective_orders_by_id.pop(prev_cov.broker_order_id, None)
        self._coverage_by_symbol[norm_sym] = coverage
        if coverage.broker_order_id:
            self._protective_orders_by_id[coverage.broker_order_id] = coverage

        if persist:
            self._persist_protective_order(coverage)

        pos_qty = self._positions_qty.get(norm_sym, Decimal("0"))
        if pos_qty > Decimal("0"):
            self.verify_position_coverage(
                symbol=norm_sym,
                pos_qty=pos_qty,
                item_id=item_id,
            )
        return coverage

    def invalidate_protective_order(
        self,
        symbol: Optional[str] = None,
        broker_order_id: Optional[str] = None,
        reason: str = "Protective OCO order canceled/rejected/expired",
    ) -> Optional[OcoCheckResult]:
        """Marks a protective order as lost (`covered_qty = 0`) and checks position exposure."""
        target_sym = _normalize_symbol(symbol) if symbol else ""
        existing: Optional[ProtectiveOrderCoverage] = None
        if broker_order_id and broker_order_id in self._protective_orders_by_id:
            existing = self._protective_orders_by_id.pop(broker_order_id)
            if not target_sym:
                target_sym = existing.symbol
        if not target_sym:
            return None

        current_sym_cov = self.get_coverage(target_sym)
        if existing is None:
            existing = current_sym_cov

        reason_upper = reason.upper()
        if (
            self.is_safe_exit_active(target_sym)
            and "CANCELED" in reason_upper
            and "REJECTED" not in reason_upper
            and "DENIED" not in reason_upper
            and "EXPIRED" not in reason_upper
        ):
            return None

        if (
            (existing is not None and existing.status == "REPLACING")
            or (current_sym_cov is not None and current_sym_cov.status == "REPLACING")
        ) and "CANCELED" in reason.upper():
            return None

        stale_ids = [
            oid
            for oid, cov in self._protective_orders_by_id.items()
            if cov.symbol == target_sym
        ]
        for oid in stale_ids:
            self._protective_orders_by_id.pop(oid, None)

        lost_coverage = ProtectiveOrderCoverage(
            symbol=target_sym,
            broker=self._broker,
            broker_order_id=str(
                broker_order_id
                or (existing.broker_order_id if existing is not None else "")
            ),
            covered_qty=Decimal("0"),
            stop_price=existing.stop_price if existing is not None else Decimal("0"),
            take_profit_price=(
                existing.take_profit_price if existing is not None else None
            ),
            status="MISSING",
            item_id=existing.item_id if existing is not None else None,
        )
        self._coverage_by_symbol[target_sym] = lost_coverage

        pos_qty = self._positions_qty.get(target_sym, Decimal("0"))
        if pos_qty > Decimal("0"):
            return self.verify_position_coverage(
                symbol=target_sym,
                pos_qty=pos_qty,
                item_id=lost_coverage.item_id,
                override_reason=reason,
            )
        self._persist_protective_order(lost_coverage)
        return None

    def record_position_qty_sync(self, symbol: str, new_qty: Decimal) -> None:
        norm_sym = _normalize_symbol(symbol)
        if not norm_sym:
            return
        prev_qty = self._positions_qty.get(norm_sym, Decimal("0"))
        if new_qty <= Decimal("0") or new_qty > prev_qty:
            self._unapplied_pos_reduction_by_symbol[norm_sym] = Decimal("0")
        elif prev_qty > new_qty:
            self._unapplied_pos_reduction_by_symbol[norm_sym] = (
                self._unapplied_pos_reduction_by_symbol.get(norm_sym, Decimal("0"))
                + (prev_qty - new_qty)
            )
        self._positions_qty[norm_sym] = new_qty

    def verify_position_coverage(
        self,
        symbol: str,
        pos_qty: Any,
        item_id: Optional[UUID] = None,
        override_reason: Optional[str] = None,
    ) -> OcoCheckResult:
        """Verifies protective stop coverage for `symbol` with open quantity `pos_qty`.

        If `pos_qty == 0`, closes the protective order record.
        If `pos_qty > covered_qty` or `stop_price <= 0` or `status != 'ACTIVE'`,
        atomically marks `protective_orders.status = 'MISSING'`, sets
        `system_flags.entry_freeze = True`, and writes a `CRITICAL`
        `RECONCILIATION_MISMATCH` alert into `alert_outbox`.
        """
        norm_sym = _normalize_symbol(symbol)
        abs_pos_qty = abs(_to_decimal(pos_qty, "0"))
        self.record_position_qty_sync(norm_sym, abs_pos_qty)
        self._positions_qty[norm_sym] = abs_pos_qty

        coverage = self.get_coverage(norm_sym)

        if abs_pos_qty == Decimal("0"):
            closed_cov = ProtectiveOrderCoverage(
                symbol=norm_sym,
                broker=self._broker,
                broker_order_id=coverage.broker_order_id if coverage else "",
                covered_qty=Decimal("0"),
                stop_price=coverage.stop_price if coverage else Decimal("0"),
                take_profit_price=coverage.take_profit_price if coverage else None,
                status="CLOSED",
                item_id=item_id or (coverage.item_id if coverage else None),
            )
            self._coverage_by_symbol[norm_sym] = closed_cov
            self._persist_protective_order(closed_cov)
            return OcoCheckResult(
                symbol=norm_sym,
                pos_qty=Decimal("0"),
                covered_qty=Decimal("0"),
                stop_price=closed_cov.stop_price,
                status="CLOSED",
                is_covered=True,
                triggered_entry_freeze=False,
            )

        covered_qty = (
            coverage.covered_qty
            if (
                coverage is not None
                and coverage.status in ("ACTIVE", "REPLACING")
                and bool(str(coverage.broker_order_id or "").strip())
            )
            else Decimal("0")
        )
        stop_price = coverage.stop_price if coverage is not None else Decimal("0")
        eff_item_id = item_id or (coverage.item_id if coverage is not None else None)

        if (
            coverage is not None
            and coverage.status in ("ACTIVE", "REPLACING")
            and bool(str(coverage.broker_order_id or "").strip())
            and covered_qty >= abs_pos_qty
            and stop_price > Decimal("0")
        ):
            self._persist_protective_order(coverage)
            return OcoCheckResult(
                symbol=norm_sym,
                pos_qty=abs_pos_qty,
                covered_qty=covered_qty,
                stop_price=stop_price,
                status=coverage.status,
                is_covered=True,
                triggered_entry_freeze=False,
            )

        uncovered_qty = max(Decimal("0"), abs_pos_qty - covered_qty)
        mismatch_reason = override_reason or (
            f"RECONCILIATION_MISMATCH: naked/under-protected position for {norm_sym} "
            f"(pos_qty={abs_pos_qty}, covered_qty={covered_qty}, "
            f"uncovered_qty={uncovered_qty}, stop_price={stop_price})"
        )
        missing_cov = ProtectiveOrderCoverage(
            symbol=norm_sym,
            broker=self._broker,
            broker_order_id=coverage.broker_order_id if coverage else "",
            covered_qty=covered_qty,
            stop_price=stop_price,
            take_profit_price=coverage.take_profit_price if coverage else None,
            status="MISSING",
            item_id=eff_item_id,
        )
        self._coverage_by_symbol[norm_sym] = missing_cov
        self._trigger_missing_oco_fail_closed(
            coverage=missing_cov,
            pos_qty=abs_pos_qty,
            uncovered_qty=uncovered_qty,
            reason=mismatch_reason,
        )
        self._notify_entry_freeze(norm_sym, mismatch_reason)

        return OcoCheckResult(
            symbol=norm_sym,
            pos_qty=abs_pos_qty,
            covered_qty=covered_qty,
            stop_price=stop_price,
            status="MISSING",
            is_covered=False,
            triggered_entry_freeze=True,
            reason=mismatch_reason,
        )

    def _persist_protective_order(self, coverage: ProtectiveOrderCoverage) -> None:
        if not self._persist_to_db or self._async_writer is None:
            return

        def _task(cur: Any) -> None:
            item_id_val = (
                str(coverage.item_id)
                if coverage.item_id is not None
                else self._resolve_active_item_id(cur, coverage.symbol)
            )
            cur.execute(
                """
                INSERT INTO protective_orders (
                    broker, symbol, item_id, broker_order_id,
                    qty, stop_price, take_profit_price, status, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (broker, symbol) DO UPDATE SET
                    item_id = COALESCE(EXCLUDED.item_id, protective_orders.item_id),
                    broker_order_id = CASE
                        WHEN EXCLUDED.broker_order_id <> '' THEN EXCLUDED.broker_order_id
                        ELSE protective_orders.broker_order_id
                    END,
                    qty = EXCLUDED.qty,
                    stop_price = EXCLUDED.stop_price,
                    take_profit_price = COALESCE(EXCLUDED.take_profit_price, protective_orders.take_profit_price),
                    status = EXCLUDED.status,
                    updated_at = NOW()
                """,
                (
                    coverage.broker,
                    coverage.symbol,
                    item_id_val,
                    coverage.broker_order_id,
                    coverage.covered_qty,
                    coverage.stop_price,
                    coverage.take_profit_price,
                    coverage.status,
                ),
            )

        self._async_writer.submit(_task)

    def _resolve_active_item_id(self, cur: Any, symbol: str) -> Optional[str]:
        cur.execute(
            """
            SELECT i.item_id
            FROM trading_plan_items i
            JOIN trading_plans p ON p.plan_id = i.plan_id
            WHERE i.symbol = %s
              AND p.status = 'LOCKED'
              AND UPPER(p.market) = UPPER(%s)
              AND LOWER(i.broker) = LOWER(%s)
            ORDER BY p.trading_date DESC, p.locked_at DESC NULLS LAST
            LIMIT 1
            """,
            (symbol, self._market, self._broker),
        )
        row = cur.fetchone()
        return str(row["item_id"]) if row and row.get("item_id") else None

    def _trigger_missing_oco_fail_closed(
        self,
        coverage: ProtectiveOrderCoverage,
        pos_qty: Decimal,
        uncovered_qty: Decimal,
        reason: str,
    ) -> None:
        if not self._persist_to_db or self._async_writer is None:
            return

        def _task(cur: Any) -> None:
            item_id_val = (
                str(coverage.item_id)
                if coverage.item_id is not None
                else self._resolve_active_item_id(cur, coverage.symbol)
            )
            cur.execute(
                """
                INSERT INTO protective_orders (
                    broker, symbol, item_id, broker_order_id,
                    qty, stop_price, take_profit_price, status, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, 'MISSING', NOW()
                )
                ON CONFLICT (broker, symbol) DO UPDATE SET
                    item_id = COALESCE(EXCLUDED.item_id, protective_orders.item_id),
                    broker_order_id = CASE
                        WHEN EXCLUDED.broker_order_id <> '' THEN EXCLUDED.broker_order_id
                        ELSE protective_orders.broker_order_id
                    END,
                    qty = EXCLUDED.qty,
                    stop_price = EXCLUDED.stop_price,
                    take_profit_price = COALESCE(EXCLUDED.take_profit_price, protective_orders.take_profit_price),
                    status = 'MISSING',
                    updated_at = NOW()
                """,
                (
                    coverage.broker,
                    coverage.symbol,
                    item_id_val,
                    coverage.broker_order_id,
                    coverage.covered_qty,
                    coverage.stop_price,
                    coverage.take_profit_price,
                ),
            )
            cur.execute(
                """
                INSERT INTO system_flags (
                    flag, active, reason, set_by, updated_at
                ) VALUES (
                    'ENTRY_FREEZE', TRUE, %s, 'nautilus:protective_oco_monitor', NOW()
                )
                ON CONFLICT (flag) DO UPDATE SET
                    active = TRUE,
                    reason = EXCLUDED.reason,
                    set_by = EXCLUDED.set_by,
                    updated_at = NOW()
                """,
                (reason,),
            )
            cur.execute(
                """
                INSERT INTO system_flag_events (
                    flag, active, reason, set_by, ts
                ) VALUES (
                    'ENTRY_FREEZE', TRUE, %s, 'nautilus:protective_oco_monitor', NOW()
                )
                """,
                (reason,),
            )
            payload = {
                "broker": coverage.broker,
                "symbol": coverage.symbol,
                "pos_qty": str(pos_qty),
                "covered_qty": str(coverage.covered_qty),
                "uncovered_qty": str(uncovered_qty),
                "stop_price": str(coverage.stop_price),
                "status": "MISSING",
                "entry_freeze": True,
                "reason": reason,
            }
            dedup_key = (
                f"oco_mismatch:{coverage.broker}:{coverage.symbol}:"
                f"{pos_qty}:{coverage.covered_qty}"
            )
            cur.execute(
                """
                INSERT INTO alert_outbox (
                    event_type, severity, source, title, body, payload, dedup_key, status
                ) VALUES (
                    'RECONCILIATION_MISMATCH',
                    'CRITICAL',
                    'nautilus:protective_oco_monitor',
                    %s,
                    %s,
                    %s::jsonb,
                    %s,
                    'PENDING'
                )
                """,
                (
                    f"Protective OCO coverage missing for {coverage.symbol}",
                    reason,
                    json.dumps(payload),
                    dedup_key,
                ),
            )

        self._async_writer.submit(_task)


class PostgresTelemetryActorConfig(DataActorConfig):
    """Configuration for `PostgresTelemetryActor`."""

    def __new__(
        cls,
        actor_id: Optional[str | ActorId] = None,
        dsn: Optional[str] = None,
        broker: str = "alpaca",
        environment: str = "PAPER",
        market: str = "US_EQUITY",
        trading_date: Optional[date] = None,
        connect_timeout_secs: int = 3,
        enforce_protective_oco_on_position_events: bool = True,
        persist_to_db: bool = True,
        log_events: bool = True,
        log_commands: bool = True,
    ):
        resolved_id = (
            actor_id
            if isinstance(actor_id, ActorId)
            else ActorId(str(actor_id or DEFAULT_TELEMETRY_ACTOR_ID))
        )
        return super().__new__(
            cls,
            actor_id=resolved_id,
            log_events=log_events,
            log_commands=log_commands,
        )

    def __init__(
        self,
        actor_id: Optional[str | ActorId] = None,
        dsn: Optional[str] = None,
        broker: str = "alpaca",
        environment: str = "PAPER",
        market: str = "US_EQUITY",
        trading_date: Optional[date] = None,
        connect_timeout_secs: int = 3,
        enforce_protective_oco_on_position_events: bool = True,
        persist_to_db: bool = True,
        log_events: bool = True,
        log_commands: bool = True,
    ) -> None:
        self.dsn = dsn
        self.broker = str(broker or "alpaca").strip().lower()
        self.environment = str(environment or "PAPER").strip().upper()
        self.market = str(market or "US_EQUITY").strip().upper()
        self.trading_date = trading_date
        self.connect_timeout_secs = max(1, int(connect_timeout_secs))
        self.enforce_protective_oco_on_position_events = bool(
            enforce_protective_oco_on_position_events
        )
        self.persist_to_db = bool(persist_to_db)


_ACTIVE_TELEMETRY_ACTORS: "weakref.WeakSet[PostgresTelemetryActor]" = weakref.WeakSet()


def find_active_telemetry_actor(
    cache: Optional[Any] = None,
    clock: Optional[Any] = None,
) -> Optional["PostgresTelemetryActor"]:
    """Finds a live, registered `PostgresTelemetryActor` sharing the exact kernel `cache` or `clock`."""
    if cache is None and clock is None:
        return None
    candidates = list(_ACTIVE_TELEMETRY_ACTORS)
    for actor in reversed(candidates):
        if not actor._safe_is_registered():
            continue
        actor._refresh_runtime_handles()
        actor_cache = actor._safe_cache()
        actor_clock = actor._safe_clock()
        if (cache is not None and actor_cache is cache) or (
            clock is not None and actor_clock is clock
        ):
            return actor
        if cache is not None and callable(getattr(cache, "get", None)):
            try:
                if cache.get(actor._cache_identity_key) == actor._cache_identity_bytes:
                    return actor
            except Exception:
                pass
    return None


class PostgresTelemetryActor(DataActor):
    """Nautilus v2 `DataActor` that synchronizes orders, fills, positions, plan events,
    and protective OCO status into PostgreSQL via `resolve_database_dsn()`.
    """

    def __new__(
        cls,
        config: Optional[PostgresTelemetryActorConfig] = None,
        oco_monitor: Optional[ProtectiveOcoMonitor] = None,
        plan_gate_backend: Optional[Any] = None,
        plan_repo: Optional[PlanContractRepository] = None,
        cancel_symbol_entries_callback: Optional[Callable[[str], Any]] = None,
    ) -> "PostgresTelemetryActor":
        _ = (oco_monitor, plan_gate_backend, plan_repo, cancel_symbol_entries_callback)
        effective_config = config or PostgresTelemetryActorConfig()
        return super().__new__(cls, effective_config)

    def __init__(
        self,
        config: Optional[PostgresTelemetryActorConfig] = None,
        oco_monitor: Optional[ProtectiveOcoMonitor] = None,
        plan_gate_backend: Optional[Any] = None,
        plan_repo: Optional[PlanContractRepository] = None,
        cancel_symbol_entries_callback: Optional[Callable[[str], Any]] = None,
    ) -> None:
        effective_config = config or PostgresTelemetryActorConfig()
        super().__init__(effective_config)
        self._telemetry_config = effective_config
        self._persist_to_db = bool(getattr(effective_config, "persist_to_db", True))
        self._dsn = (
            resolve_database_dsn(effective_config.dsn)
            if self._persist_to_db
            else (str(effective_config.dsn) if effective_config.dsn is not None else "")
        )
        self._broker = effective_config.broker
        self._environment = effective_config.environment
        self._market = effective_config.market
        self._trading_date = effective_config.trading_date
        self._connect_timeout_secs = effective_config.connect_timeout_secs
        self._enforce_oco_on_pos = (
            effective_config.enforce_protective_oco_on_position_events
        )
        self._owns_oco_monitor: bool = oco_monitor is None
        self._oco_monitor = oco_monitor or ProtectiveOcoMonitor(
            dsn=self._dsn if self._persist_to_db else None,
            broker=self._broker,
            market=self._market,
            connect_timeout_secs=self._connect_timeout_secs,
            persist_to_db=self._persist_to_db,
        )
        self._plan_gate_backend: Optional[Any] = plan_gate_backend
        self._plan_repo: Optional[PlanContractRepository] = plan_repo
        self._cancel_symbol_entries_callback: Optional[Callable[[str], Any]] = (
            cancel_symbol_entries_callback
        )
        self._processed_event_ids: _BoundedFifoSet = _BoundedFifoSet(16384)
        self._persisted_plan_event_ids: _BoundedFifoSet = _BoundedFifoSet(16384)
        self._released_entry_cids: _BoundedFifoSet = _BoundedFifoSet(16384)
        self._opened_position_ids: Set[str] = set()
        self._startup_entry_reservations: Dict[str, Tuple[str, Decimal]] = {}
        self._scheduled_expiry_item_ids: Set[str] = set()
        self._expiry_targets_by_timer: Dict[str, Tuple[str, UUID, int]] = {}
        self._pending_expiry_by_symbol: Dict[str, Tuple[UUID, int]] = {}
        self._plan_item_cache: Dict[Tuple[Optional[date], str, str], Tuple[Any, Any]] = {}
        self._order_meta: _BoundedFifoDict = _BoundedFifoDict(
            8192,
            can_evict=lambda _k, meta: not isinstance(meta, dict)
            or str(meta.get("status", "")).upper()
            in {"FILLED", "CANCELED", "REJECTED", "EXPIRED", "DENIED"},
        )
        self._fills_by_cid: _BoundedFifoDict = _BoundedFifoDict(8192)
        self._active_stop_legs_by_symbol: Dict[str, Dict[str, Decimal]] = {}
        self._active_stop_price_by_leg: Dict[str, Decimal] = {}
        self._terminated_stop_cids: _BoundedFifoSet = _BoundedFifoSet(4096)
        self._replaced_stop_cids: _BoundedFifoSet = _BoundedFifoSet(4096)
        self._safe_exit_symbols: Set[str] = set()
        self._msgbus_subscriptions: List[Tuple[str, Any]] = []
        self._msgbus_bindings: List[Tuple[Any, str, Any]] = []
        self._subscribed_msgbuses: Set[int] = set()
        self._is_started: bool = False
        self._owner_thread_id: int = threading.get_ident()
        self._cached_cache: Optional[Any] = None
        self._cached_clock: Optional[Any] = None
        self._cache_identity_key: str = f"__nautilus_telemetry_actor__{id(self)}"
        self._cache_identity_bytes: bytes = str(id(self)).encode("utf-8")
        self._async_writer = self._oco_monitor._async_writer
        self._oco_monitor.add_entry_freeze_callback(self._on_oco_entry_freeze)

    def _on_oco_entry_freeze(self, symbol: str, reason: str) -> None:
        backend = self._plan_gate_backend
        if backend is None:
            return
        set_freeze = getattr(backend, "set_entry_freeze", None)
        if callable(set_freeze):
            norm_sym = _normalize_symbol(symbol)
            updated_by = (
                f"nautilus:protective_oco_monitor:{norm_sym}"
                if norm_sym
                else "nautilus:protective_oco_monitor"
            )
            set_freeze(True, reason=reason, updated_by=updated_by)

    def attach_plan_gate_backend(self, backend: Optional[Any]) -> None:
        self._plan_gate_backend = backend
        if backend is not None and self._oco_monitor is not None:
            self._oco_monitor.add_entry_freeze_callback(self._on_oco_entry_freeze)

    def attach_plan_repo(self, repo: Optional[PlanContractRepository]) -> None:
        self._plan_repo = repo

    def set_cancel_symbol_entries_callback(
        self, callback: Optional[Callable[[str], Any]]
    ) -> None:
        self._cancel_symbol_entries_callback = callback

    def _is_owner_thread(self) -> bool:
        return threading.get_ident() == getattr(
            self, "_owner_thread_id", threading.get_ident()
        )

    @property
    def trading_date(self) -> Optional[date]:
        return self._trading_date

    def _clear_plan_item_cache_worker_only(self) -> None:
        if self._persist_to_db and self._async_writer is not None:
            self._async_writer.submit(lambda _cur: self._plan_item_cache.clear())
        else:
            self._plan_item_cache.clear()

    def set_trading_date(self, trading_date: Optional[date]) -> None:
        """Hot-reloads the active `trading_date` on `PostgresTelemetryActor` during daily rollover."""
        self._trading_date = trading_date
        self._telemetry_config.trading_date = trading_date
        self._clear_plan_item_cache_worker_only()
        self._startup_entry_reservations.clear()
        self._scheduled_expiry_item_ids.clear()
        self._expiry_targets_by_timer.clear()
        self._pending_expiry_by_symbol.clear()
        if self._is_started:
            self.sync_plan_expiry_timers()

    def begin_safe_exit(self, symbol: str, reason: str = "TWO_PHASE_SAFE_EXIT") -> None:
        """Marks `symbol` as entering an authorized Two-Phase Safe Exit window."""
        _ = reason
        norm_sym = _normalize_symbol(symbol)
        if not norm_sym:
            return
        self._safe_exit_symbols.add(norm_sym)
        self._oco_monitor.begin_safe_exit(norm_sym)
        self._refresh_runtime_handles()
        cache = self._safe_cache()
        if cache is not None and callable(getattr(cache, "add", None)):
            try:
                cache.add(f"__nautilus_safe_exit__{norm_sym}", b"1")
            except Exception:
                pass

    def end_safe_exit(self, symbol: str) -> None:
        """Clears the Two-Phase Safe Exit window for `symbol`."""
        norm_sym = _normalize_symbol(symbol)
        if not norm_sym:
            return
        self._safe_exit_symbols.discard(norm_sym)
        self._oco_monitor.end_safe_exit(norm_sym)
        self._refresh_runtime_handles()
        cache = self._safe_cache()
        if cache is not None and callable(getattr(cache, "add", None)):
            try:
                cache.add(f"__nautilus_safe_exit__{norm_sym}", b"0")
            except Exception:
                pass

    def is_safe_exit_active(self, symbol: str) -> bool:
        """Returns True if `symbol` is currently in an authorized Two-Phase Safe Exit window."""
        norm_sym = _normalize_symbol(symbol)
        if not norm_sym:
            return False
        if (
            norm_sym in self._safe_exit_symbols
            or self._oco_monitor.is_safe_exit_active(norm_sym)
        ):
            return True
        cache = self._safe_cache()
        if cache is not None and callable(getattr(cache, "get", None)):
            try:
                raw = cache.get(f"__nautilus_safe_exit__{norm_sym}")
                if raw == b"1":
                    return True
            except Exception:
                pass
        return False

    def is_safe_exit_in_flight(self, symbol: str) -> bool:
        """Alias for `is_safe_exit_active`."""
        return self.is_safe_exit_active(symbol)

    def mark_stop_leg_replaced(
        self,
        symbol_or_old_cid: str,
        old_or_new_cid: str = "",
        new_cid: Optional[str] = None,
        new_covered_qty: Any = None,
        stop_price: Any = None,
        status: Optional[str] = None,
        *,
        symbol: Optional[str] = None,
    ) -> None:
        """Records that `old_cid` is being replaced by a resized stop order so its cancellation does not freeze."""
        s1 = str(symbol_or_old_cid or "").strip()
        s2 = str(old_or_new_cid or "").strip()
        if s1.startswith("O-") or (not s2 and symbol):
            old_cid = s1
            norm_sym = _normalize_symbol(symbol or "")
            resolved_new_cid = str(new_cid or s2 or "").strip()
        else:
            norm_sym = _normalize_symbol(s1)
            old_cid = s2
            resolved_new_cid = str(new_cid or "").strip()

        prev_stop_px = self._active_stop_price_by_leg.get(str(old_cid), Decimal("0"))
        if not norm_sym and old_cid:
            for cand_sym, legs in self._active_stop_legs_by_symbol.items():
                if str(old_cid) in legs:
                    norm_sym = cand_sym
                    break

        if old_cid:
            self._replaced_stop_cids.add(str(old_cid))
            self._terminated_stop_cids.add(str(old_cid))
            deferred_key = f"deferred-replace-{old_cid}"
            if norm_sym and norm_sym in self._active_stop_legs_by_symbol:
                self._active_stop_legs_by_symbol[norm_sym].pop(str(old_cid), None)
                self._active_stop_legs_by_symbol[norm_sym].pop(deferred_key, None)
                self._active_stop_price_by_leg.pop(str(old_cid), None)
                self._active_stop_price_by_leg.pop(deferred_key, None)
            else:
                for legs in self._active_stop_legs_by_symbol.values():
                    legs.pop(str(old_cid), None)
                    legs.pop(deferred_key, None)
                self._active_stop_price_by_leg.pop(str(old_cid), None)
                self._active_stop_price_by_leg.pop(deferred_key, None)

        qty_dec = (
            _to_decimal(new_covered_qty, "0")
            if new_covered_qty is not None
            else Decimal("0")
        )
        if norm_sym and resolved_new_cid and qty_dec > Decimal("0"):
            self._terminated_stop_cids.discard(resolved_new_cid)
            self._replaced_stop_cids.discard(resolved_new_cid)
            legs = self._active_stop_legs_by_symbol.setdefault(norm_sym, {})
            is_deferred_placeholder = resolved_new_cid.startswith("deferred-replace-")
            if not is_deferred_placeholder:
                for k in [k for k in legs if str(k).startswith("deferred-replace-")]:
                    legs.pop(k, None)
                    self._active_stop_price_by_leg.pop(k, None)
                legs[resolved_new_cid] = qty_dec
            stop_px_dec = (
                _to_decimal(stop_price, "0")
                if stop_price is not None
                else Decimal("0")
            )
            if stop_px_dec <= Decimal("0"):
                existing_cov = self._oco_monitor.get_coverage(norm_sym)
                stop_px_dec = (
                    prev_stop_px
                    if prev_stop_px > Decimal("0")
                    else (
                        existing_cov.stop_price
                        if existing_cov is not None
                        else Decimal("0")
                    )
                )
            if stop_px_dec > Decimal("0"):
                if not is_deferred_placeholder:
                    self._active_stop_price_by_leg[resolved_new_cid] = stop_px_dec
                real_covered_qty = sum(
                    (
                        q
                        for leg_cid, q in legs.items()
                        if not str(leg_cid).startswith("deferred-replace-")
                    ),
                    Decimal("0"),
                )
                covered_for_monitor = (
                    real_covered_qty if real_covered_qty > Decimal("0") else qty_dec
                )
                target_status = (
                    str(status).strip().upper()
                    if status is not None and str(status).strip() != ""
                    else (
                        "REPLACING"
                        if is_deferred_placeholder
                        else "ACTIVE"
                    )
                )
                self._oco_monitor.register_protective_order(
                    symbol=norm_sym,
                    covered_qty=covered_for_monitor,
                    stop_price=stop_px_dec,
                    broker_order_id=resolved_new_cid,
                    status=target_status,
                )

    def _refresh_runtime_handles(self) -> None:
        if not self._is_owner_thread():
            return
        try:
            if self._safe_is_registered():
                try:
                    self._cached_cache = self.cache
                except Exception:
                    pass
                try:
                    self._cached_clock = self.clock
                except Exception:
                    pass
                if self._cached_cache is not None and callable(
                    getattr(self._cached_cache, "add", None)
                ):
                    try:
                        self._cached_cache.add(
                            self._cache_identity_key,
                            self._cache_identity_bytes,
                        )
                    except Exception:
                        pass
        except Exception:
            pass

    def _safe_is_registered(self) -> bool:
        if not self._is_owner_thread():
            return False
        try:
            is_reg_fn = getattr(self, "is_registered", None)
            if callable(is_reg_fn):
                return bool(is_reg_fn())
            return getattr(self, "trader_id", None) is not None
        except Exception:
            return self._is_started and (
                self._cached_cache is not None or self._cached_clock is not None
            )

    def _safe_cache(self) -> Optional[Any]:
        if not self._is_owner_thread():
            return None
        try:
            c = self.cache
            if c is not None:
                self._cached_cache = c
            return c
        except Exception:
            return self._cached_cache

    def _safe_clock(self) -> Optional[Any]:
        if not self._is_owner_thread():
            return None
        try:
            clk = self.clock
            if clk is not None:
                self._cached_clock = clk
            return clk
        except Exception:
            return self._cached_clock

    @property
    def dsn(self) -> str:
        return self._dsn

    @property
    def oco_monitor(self) -> ProtectiveOcoMonitor:
        return self._oco_monitor

    def ensure_schema(self) -> None:
        """Idempotently ensures `order_fills` and dependent telemetry tables exist via `db/migrations/`."""
        if not self._dsn:
            return
        from evolution_engine.db_migrate import apply_migrations

        apply_migrations(self._dsn)

    def subscribe_to_message_bus(self, msgbus: Any) -> None:
        """Subscribes to `events.order.*`, `events.position.*`, and `events.account.*` on a Nautilus `MessageBus`."""
        if msgbus is None:
            return
        bus_id = id(msgbus)
        if bus_id in self._subscribed_msgbuses:
            return
        try:
            bus_ref: Any = weakref.ref(msgbus)
        except TypeError:
            bus_ref = msgbus
        order_handler = self.on_order_event
        position_handler = self.on_position_event
        account_handler = self.on_account_state
        msgbus.subscribe("events.order.*", order_handler)
        msgbus.subscribe("events.position.*", position_handler)
        subscribed_account = False
        try:
            msgbus.subscribe("events.account.*", account_handler)
            subscribed_account = True
        except Exception:
            pass
        self._subscribed_msgbuses.add(bus_id)
        self._msgbus_subscriptions.append(("events.order.*", order_handler))
        self._msgbus_subscriptions.append(("events.position.*", position_handler))
        self._msgbus_bindings.append((bus_ref, "events.order.*", order_handler))
        self._msgbus_bindings.append((bus_ref, "events.position.*", position_handler))
        if subscribed_account:
            self._msgbus_subscriptions.append(("events.account.*", account_handler))
            self._msgbus_bindings.append((bus_ref, "events.account.*", account_handler))

    def on_start(self) -> None:
        self._is_started = True
        self._refresh_runtime_handles()
        subscribed = False
        if self._safe_is_registered():
            _ACTIVE_TELEMETRY_ACTORS.add(self)
            try:
                self.subscribe_topic("events.order.*", self.on_order_event)
                self.subscribe_topic("events.position.*", self.on_position_event)
                try:
                    self.subscribe_topic("events.account.*", self.on_account_state)
                except Exception:
                    pass
                subscribed = True
            except Exception:
                pass
        if not subscribed:
            try:
                msgbus = getattr(self, "msgbus", None)
                if msgbus is not None:
                    self.subscribe_to_message_bus(msgbus)
            except Exception:
                pass
        self.sync_from_cache()
        self.reconcile_startup_reservations()
        self.sync_plan_expiry_timers()
        self._flush_backend_mutations()

    def _flush_backend_mutations(self) -> int:
        if self._plan_gate_backend is None:
            return 0
        flush_fn = getattr(self._plan_gate_backend, "flush_rust_mutations", None)
        if callable(flush_fn):
            try:
                return int(flush_fn())
            except Exception as exc:
                logger.error("Failed to flush PlanGate Rust mutations: %s", exc, exc_info=True)
        return 0

    def _resolve_session_date(self) -> date:
        if self._trading_date is not None:
            return self._trading_date
        clk = self._safe_clock()
        if clk is not None:
            try:
                ts_ns = int(clk.timestamp_ns())
                if ts_ns > 0:
                    return (
                        datetime.fromtimestamp(ts_ns / 1_000_000_000.0, tz=timezone.utc)
                        .astimezone(ZoneInfo("America/New_York"))
                        .date()
                    )
            except Exception:
                pass
        return datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York")).date()

    @staticmethod
    def _is_entry_order_obj(order: Any) -> bool:
        if bool(getattr(order, "is_reduce_only", False)):
            return False
        otype = _normalize_order_type_str(getattr(order, "order_type", "MARKET"))
        if _is_protective_stop_order_type(otype):
            return False
        exec_params = getattr(order, "exec_algorithm_params", None) or {}
        if isinstance(exec_params, Mapping):
            intent = str(
                exec_params.get("intent") or exec_params.get("action") or ""
            ).strip().upper()
            if intent in EXIT_INTENT_TAGS:
                return False
        return True

    def _count_open_entry_leaves_in_cache(self, symbol: str) -> int:
        norm_sym = _normalize_symbol(symbol)
        if not norm_sym:
            return 0
        cache = self._safe_cache()
        if cache is None:
            return 0
        candidates: List[Any] = []
        seen_keys: Set[str] = set()
        for fn_name in ("orders_open", "orders_inflight", "orders"):
            fn = getattr(cache, fn_name, None)
            if not callable(fn):
                continue
            try:
                orders_seq = fn() or ()
            except Exception:
                continue
            for ord_obj in orders_seq:
                cid = str(getattr(ord_obj, "client_order_id", "") or "")
                key = cid if cid else f"id:{id(ord_obj)}"
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                candidates.append(ord_obj)

        count = 0
        for order in candidates:
            if _normalize_symbol(getattr(order, "instrument_id", "")) != norm_sym:
                continue
            if not self._is_entry_order_obj(order):
                continue
            if bool(getattr(order, "is_closed", False)):
                continue
            status_str = str(getattr(order, "status", "") or "").upper()
            if "." in status_str:
                status_str = status_str.rsplit(".", 1)[-1]
            if status_str in {
                "DENIED",
                "REJECTED",
                "CANCELED",
                "CANCELLED",
                "EXPIRED",
                "FILLED",
                "VOIDED",
            }:
                continue
            if status_str == "INITIALIZED":
                cid = str(getattr(order, "client_order_id", "") or "")
                ea_id = getattr(order, "exec_algorithm_id", None)
                spawn_id = getattr(order, "exec_spawn_id", None)
                is_primary_init = bool(getattr(order, "is_primary", False)) or (
                    ea_id is not None
                    and spawn_id is not None
                    and bool(cid)
                    and str(spawn_id) == cid
                )
                if is_primary_init:
                    continue
            leaves_qty = _to_decimal(getattr(order, "leaves_qty", None), "0")
            if leaves_qty > Decimal("0"):
                count += 1
        return count

    def reconcile_startup_reservations(self) -> None:
        """Reconciles per-symbol entry reservations on startup against cache, PG orders, and broker positions.

        Formula: `max(cache_fills_today, pg_orders_filled_today, max(0, |pos_broker| - |carry_over|)) + open_leaves`.
        Fails closed (`ENTRY_FREEZE`) on PostgreSQL query failure when `persist_to_db=True`.
        """
        if self._plan_gate_backend is None:
            return
        items_map = getattr(self._plan_gate_backend, "_items_by_symbol", None)
        if not items_map:
            return

        session_date = self._resolve_session_date()
        ny_midnight = datetime(
            session_date.year,
            session_date.month,
            session_date.day,
            0,
            0,
            0,
            tzinfo=ZoneInfo("America/New_York"),
        )
        session_start_ns = int(ny_midnight.astimezone(timezone.utc).timestamp() * 1_000_000_000)

        cache = self._safe_cache()
        try:
            all_orders = list(cache.orders() or ()) if cache is not None else []
        except Exception:
            all_orders = []
        try:
            open_orders = list(cache.orders_open() or ()) if cache is not None else []
        except Exception:
            open_orders = []
        try:
            open_positions = list(cache.positions_open() or ()) if cache is not None else []
        except Exception:
            open_positions = []

        pg_filled_by_key: Dict[Tuple[str, str], Decimal] = {}
        pg_pos_by_sym: Dict[str, Tuple[Decimal, Optional[date]]] = {}
        db_carry_over_by_sym: Dict[str, Decimal] = {}
        if self._persist_to_db and self._dsn:
            try:
                conn = psycopg2.connect(
                    self._dsn,
                    cursor_factory=RealDictCursor,
                    connect_timeout=self._connect_timeout_secs,
                )
                try:
                    with conn.cursor() as cur:
                        if ProtectiveOcoMonitor._table_exists(cur, "orders"):
                            cur.execute(
                                """
                                SELECT UPPER(symbol) AS sym,
                                       UPPER(action) AS act,
                                       COALESCE(SUM(COALESCE(filled_qty, 0)), 0) AS total_filled
                                FROM orders
                                WHERE LOWER(broker) = LOWER(%s)
                                  AND UPPER(environment) = UPPER(%s)
                                  AND UPPER(COALESCE(order_type, '')) NOT LIKE '%%STOP%%'
                                  AND COALESCE((created_at AT TIME ZONE 'America/New_York')::date, %s) = %s
                                GROUP BY UPPER(symbol), UPPER(action)
                                """,
                                (self._broker, self._environment, session_date, session_date),
                            )
                            for row in cur.fetchall() or ():
                                r_sym = str(row.get("sym") or "").strip().upper()
                                r_act = str(row.get("act") or "").strip().upper()
                                if r_sym and r_act:
                                    pg_filled_by_key[(r_sym, r_act)] = max(
                                        Decimal("0"),
                                        _to_decimal(row.get("total_filled"), "0"),
                                    )
                        if ProtectiveOcoMonitor._table_exists(cur, "trading_positions"):
                            cur.execute(
                                """
                                SELECT UPPER(symbol) AS sym,
                                       ABS(COALESCE(qty, 0)) AS abs_qty,
                                       (updated_at AT TIME ZONE 'America/New_York')::date AS pos_date
                                FROM trading_positions
                                WHERE LOWER(broker) = LOWER(%s)
                                  AND UPPER(environment) = UPPER(%s)
                                  AND COALESCE(qty, 0) <> 0
                                """,
                                (self._broker, self._environment),
                            )
                            for row in cur.fetchall() or ():
                                p_sym = str(row.get("sym") or "").strip().upper()
                                if p_sym:
                                    p_qty = max(
                                        Decimal("0"),
                                        _to_decimal(row.get("abs_qty"), "0"),
                                    )
                                    p_date = row.get("pos_date")
                                    if isinstance(p_date, datetime):
                                        p_date = p_date.date()
                                    if p_qty > Decimal("0"):
                                        pg_pos_by_sym[p_sym] = (p_qty, p_date)
                        if ProtectiveOcoMonitor._table_exists(cur, "trading_plan_items") and ProtectiveOcoMonitor._table_exists(cur, "trading_plans"):
                            cur.execute(
                                """
                                SELECT UPPER(i.symbol) AS sym,
                                       UPPER(COALESCE(i.kind, '')) AS kind,
                                       COALESCE(i.filled_qty, 0) AS filled_qty,
                                       i.sizing_trace
                                FROM trading_plan_items i
                                JOIN trading_plans p ON p.plan_id = i.plan_id
                                WHERE p.trading_date = %s
                                  AND p.market = %s
                                """,
                                (session_date, self._market),
                            )
                            for row in cur.fetchall() or ():
                                c_sym = str(row.get("sym") or "").strip().upper()
                                if not c_sym:
                                    continue
                                raw_trace = row.get("sizing_trace") or {}
                                if isinstance(raw_trace, str):
                                    try:
                                        raw_trace = json.loads(raw_trace)
                                    except (TypeError, ValueError):
                                        raw_trace = {}
                                trace = raw_trace if isinstance(raw_trace, Mapping) else {}
                                r_kind = str(row.get("kind") or trace.get("kind") or "").strip().upper()
                                r_co_status = str(trace.get("carry_over_status") or "").strip().upper()
                                r_co_qty = trace.get("carry_over_position_qty")
                                if r_co_qty is not None and str(r_co_qty).strip() != "":
                                    co_dec = abs(_to_decimal(r_co_qty, "0"))
                                    db_carry_over_by_sym[c_sym] = max(
                                        db_carry_over_by_sym.get(c_sym, Decimal("0")),
                                        co_dec,
                                    )
                                elif r_kind in ("CARRY_OVER", "POSITION_MGMT") or r_co_status in (
                                    "CARRY_OVER_EXIT_ONLY",
                                    "THESIS_INVALIDATED_EXIT",
                                    "RENEWED_ACTIVE",
                                ):
                                    co_dec = abs(_to_decimal(row.get("filled_qty"), "0"))
                                    if co_dec == Decimal("0") and c_sym in pg_pos_by_sym:
                                        co_dec = pg_pos_by_sym[c_sym][0]
                                    db_carry_over_by_sym[c_sym] = max(
                                        db_carry_over_by_sym.get(c_sym, Decimal("0")),
                                        co_dec,
                                    )
                finally:
                    conn.close()
            except Exception as exc:
                reason = f"STARTUP_RECONCILE_DB_ERROR: {exc}"
                logger.error("Startup reservation reconciliation DB error: %s", exc, exc_info=True)
                freeze_fn = getattr(self._plan_gate_backend, "set_entry_freeze", None)
                if callable(freeze_fn):
                    freeze_fn(
                        True,
                        reason=reason,
                        updated_by="nautilus:telemetry_actor:startup_reconcile",
                    )
                return

        rust_engine = getattr(self._plan_gate_backend, "rust_engine", None)
        reconcile_sym_fn = getattr(
            self._plan_gate_backend, "reconcile_symbol_reservation", None
        )
        if not callable(reconcile_sym_fn):
            return

        for raw_sym, item in list(items_map.items()):
            sym = _normalize_symbol(raw_sym)
            if not sym:
                continue
            expected_action = (
                "BUY" if item.risk_envelope.side == PlanSide.LONG else "SELL"
            )

            cache_fills_today = Decimal("0")
            for order in all_orders:
                if _normalize_symbol(getattr(order, "instrument_id", "")) != sym:
                    continue
                if not self._is_entry_order_obj(order):
                    continue
                if _normalize_side_str(getattr(order, "side", "")) != expected_action:
                    continue
                status_str = str(getattr(order, "status", "") or "").upper()
                if "." in status_str:
                    status_str = status_str.rsplit(".", 1)[-1]
                if status_str == "INITIALIZED":
                    continue
                ts_init = int(getattr(order, "ts_init", 0) or 0)
                if ts_init > 0 and ts_init < session_start_ns:
                    continue
                cache_fills_today += max(
                    Decimal("0"), _to_decimal(getattr(order, "filled_qty", None), "0")
                )

            pg_orders_filled_today = pg_filled_by_key.get(
                (sym, expected_action), Decimal("0")
            )

            abs_pos_broker = Decimal("0")
            for pos in open_positions:
                if bool(getattr(pos, "is_closed", False)):
                    continue
                p_sym = _normalize_symbol(
                    getattr(pos, "instrument_id", "") or getattr(pos, "symbol", "")
                )
                if p_sym == sym:
                    abs_pos_broker += abs(_to_decimal(getattr(pos, "quantity", None), "0"))

            co_status = str(
                getattr(item, "carry_over_status", None)
                or item.sizing_trace.get("carry_over_status")
                or ""
            ).strip().upper()
            item_kind = str(
                getattr(item, "kind", None)
                or item.sizing_trace.get("kind")
                or ""
            ).strip().upper()
            raw_co_qty = item.sizing_trace.get("carry_over_position_qty")
            pg_pos_entry = pg_pos_by_sym.get(sym)
            pg_pos_qty = pg_pos_entry[0] if pg_pos_entry is not None else Decimal("0")
            pg_pos_date = pg_pos_entry[1] if pg_pos_entry is not None else None
            db_co_qty = db_carry_over_by_sym.get(sym)
            if raw_co_qty is not None and str(raw_co_qty).strip() != "":
                abs_carry_over = abs(_to_decimal(raw_co_qty, "0"))
            elif item_kind in ("CARRY_OVER", "POSITION_MGMT") or co_status in (
                "CARRY_OVER_EXIT_ONLY",
                "THESIS_INVALIDATED_EXIT",
                "RENEWED_ACTIVE",
            ):
                abs_carry_over = max(
                    abs(_to_decimal(item.filled_qty, "0")),
                    pg_pos_qty,
                    db_co_qty or Decimal("0"),
                )
            elif db_co_qty is not None:
                abs_carry_over = max(db_co_qty, pg_pos_qty)
            elif pg_pos_qty > Decimal("0") and (
                (pg_pos_date is not None and pg_pos_date < session_date)
                or (
                    pg_orders_filled_today > Decimal("0")
                    and pg_pos_qty < abs_pos_broker
                    and (abs_pos_broker - pg_pos_qty) >= pg_orders_filled_today
                )
            ):
                abs_carry_over = pg_pos_qty
            else:
                abs_carry_over = Decimal("0")

            open_leaves = Decimal("0")
            for order in open_orders:
                if _normalize_symbol(getattr(order, "instrument_id", "")) != sym:
                    continue
                if not self._is_entry_order_obj(order):
                    continue
                if _normalize_side_str(getattr(order, "side", "")) != expected_action:
                    continue
                status_str = str(getattr(order, "status", "") or "").upper()
                if "." in status_str:
                    status_str = status_str.rsplit(".", 1)[-1]
                if status_str == "INITIALIZED":
                    continue
                ord_qty = max(
                    Decimal("0"), _to_decimal(getattr(order, "quantity", None), "0")
                )
                ord_filled = max(
                    Decimal("0"), _to_decimal(getattr(order, "filled_qty", None), "0")
                )
                leaves = max(Decimal("0"), ord_qty - ord_filled)
                if leaves > Decimal("0"):
                    open_leaves += leaves
                    cid = str(getattr(order, "client_order_id", "") or "")
                    if cid and (
                        rust_engine is None
                        or not rust_engine.has_admitted_reservation(cid)
                    ):
                        self._startup_entry_reservations[cid] = (sym, ord_qty)

            net_new_pos = max(Decimal("0"), abs_pos_broker - abs_carry_over)
            pyramid_enabled = bool(item.sizing_trace.get("pyramid_enabled", False))
            if co_status == "RENEWED_ACTIVE" and abs_carry_over > Decimal("0"):
                actual_filled_and_leaves = (
                    max(
                        abs_carry_over + max(cache_fills_today, pg_orders_filled_today),
                        abs_pos_broker,
                    )
                    + open_leaves
                )
                if not pyramid_enabled:
                    reconciled_reserved = max(
                        actual_filled_and_leaves,
                        item.risk_envelope.approved_qty_cap,
                    )
                else:
                    reconciled_reserved = actual_filled_and_leaves
            else:
                actual_filled_and_leaves = (
                    max(cache_fills_today, pg_orders_filled_today, net_new_pos)
                    + open_leaves
                )
                reconciled_reserved = actual_filled_and_leaves

            if reconciled_reserved != actual_filled_and_leaves:
                reconcile_sym_fn(
                    sym,
                    reconciled_reserved,
                    persist_db=self._persist_to_db,
                    persist_filled_qty=actual_filled_and_leaves,
                )
            else:
                reconcile_sym_fn(sym, reconciled_reserved, persist_db=self._persist_to_db)

    def sync_plan_expiry_timers(self) -> None:
        """Schedules `valid_until` expiry alerts for active plan items via `self.clock.set_time_alert_ns`."""
        if self._plan_gate_backend is None:
            return
        clk = self._safe_clock()
        if clk is None:
            return
        items_map = getattr(self._plan_gate_backend, "_items_by_symbol", None) or {}
        try:
            now_ns = int(clk.timestamp_ns())
        except Exception:
            now_ns = 0

        for raw_sym, item in list(items_map.items()):
            sym = _normalize_symbol(raw_sym)
            if not sym:
                continue
            status_str = str(getattr(item, "status", "") or "").strip().upper()
            if status_str in ("EXPIRED", "CANCELLED", "CLOSED", "FILLED"):
                continue
            co_status = str(
                getattr(item, "carry_over_status", None)
                or item.sizing_trace.get("carry_over_status")
                or ""
            ).strip().upper()
            if co_status in ("CARRY_OVER_EXIT_ONLY", "THESIS_INVALIDATED_EXIT"):
                continue
            item_id_str = str(item.item_id)
            if item_id_str in self._scheduled_expiry_item_ids:
                continue
            valid_until = item.risk_envelope.valid_until
            valid_until_ns = int(valid_until.timestamp() * 1_000_000_000)
            timer_name = f"plan_item_expired_{item_id_str}"
            self._scheduled_expiry_item_ids.add(item_id_str)
            self._expiry_targets_by_timer[timer_name] = (sym, item.item_id, valid_until_ns)
            if now_ns > 0 and now_ns >= valid_until_ns:
                self._expiry_targets_by_timer.pop(timer_name, None)
                self._handle_plan_item_expiry(sym, item.item_id, valid_until_ns)
            else:
                try:
                    if timer_name in set(clk.timer_names()):
                        clk.cancel_timer(timer_name)
                except Exception:
                    pass
                try:
                    clk.set_time_alert_ns(
                        name=timer_name,
                        alert_time_ns=valid_until_ns,
                        allow_past=True,
                    )
                except Exception as exc:
                    logger.warning(
                        "Failed to schedule expiry timer %s for %s: %s",
                        timer_name,
                        sym,
                        exc,
                    )

    def _persist_item_expired(self, item_id: UUID) -> None:
        if not self._persist_to_db or self._async_writer is None:
            return
        plan_repo = self._plan_repo

        def _expire_task(cur: Any) -> None:
            if plan_repo is not None:
                plan_repo.update_item_status_in_tx(
                    cur,
                    item_id,
                    "EXPIRED",
                    actor="system:telemetry_actor",
                    reason="VALID_UNTIL_EXPIRED",
                )
            else:
                PlanContractRepository.update_item_status_in_tx(
                    cur,
                    item_id,
                    "EXPIRED",
                    actor="system:telemetry_actor",
                    reason="VALID_UNTIL_EXPIRED",
                )

        self._async_writer.submit(_expire_task)

    def _escalate_expiry_cancel_failure(
        self,
        symbol: str,
        exc: Exception,
        *,
        context: str,
    ) -> None:
        norm_sym = _normalize_symbol(symbol)
        reason = f"EXPIRY_CANCEL_CALLBACK_FAILED:{norm_sym}:{context}:{exc}"
        if self._plan_gate_backend is not None:
            freeze_fn = getattr(self._plan_gate_backend, "set_entry_freeze", None)
            if callable(freeze_fn):
                try:
                    freeze_fn(
                        True,
                        reason=reason,
                        updated_by=f"nautilus:telemetry_actor:{context}",
                    )
                except Exception as freeze_exc:
                    logger.error(
                        "Failed setting ENTRY_FREEZE after expiry cancel failure for %s (%s): %s",
                        norm_sym,
                        context,
                        freeze_exc,
                        exc_info=True,
                    )
        if self._persist_to_db and self._async_writer is not None:
            session_date = self._resolve_session_date()
            dedup_key = f"expiry_cancel_failed:{norm_sym}:{context}:{session_date.isoformat()}"
            title = f"Expired entry cancellation failed for {norm_sym}"
            body = reason
            payload_json = json.dumps(
                {
                    "symbol": norm_sym,
                    "context": context,
                    "error": str(exc),
                    "entry_freeze": True,
                }
            )

            def _alert_task(cur: Any) -> None:
                if not ProtectiveOcoMonitor._table_exists(cur, "alert_outbox"):
                    return
                cur.execute(
                    "SELECT 1 FROM alert_outbox WHERE dedup_key = %s LIMIT 1",
                    (dedup_key,),
                )
                if cur.fetchone() is None:
                    cur.execute(
                        """
                        INSERT INTO alert_outbox (
                            event_type, severity, source, title, body, payload, dedup_key, status
                        ) VALUES (
                            'EXPIRY_CANCEL_FAILED', 'CRITICAL', 'nautilus:telemetry_actor', %s, %s, %s::jsonb, %s, 'PENDING'
                        )
                        """,
                        (title, body, payload_json, dedup_key),
                    )

            try:
                self._async_writer.submit(_alert_task)
            except Exception as writer_exc:
                logger.error(
                    "Failed enqueuing expiry cancel alert for %s (%s): %s",
                    norm_sym,
                    context,
                    writer_exc,
                    exc_info=True,
                )

    def _handle_plan_item_expiry(
        self,
        symbol: str,
        item_id: UUID,
        valid_until_ns: int,
    ) -> None:
        norm_sym = _normalize_symbol(symbol)
        if not norm_sym:
            return
        if self._plan_gate_backend is not None:
            mark_fn = getattr(self._plan_gate_backend, "mark_item_expired", None)
            if callable(mark_fn):
                mark_fn(norm_sym)

        if self._cancel_symbol_entries_callback is not None:
            try:
                self._cancel_symbol_entries_callback(norm_sym)
            except Exception as exc:
                logger.error(
                    "cancel_symbol_entries_callback failed for %s on expiry: %s",
                    norm_sym,
                    exc,
                    exc_info=True,
                )
                self._escalate_expiry_cancel_failure(
                    norm_sym,
                    exc,
                    context="plan_item_expiry",
                )

        remaining_open = self._count_open_entry_leaves_in_cache(norm_sym)
        if remaining_open > 0:
            self._pending_expiry_by_symbol[norm_sym] = (item_id, valid_until_ns)
            self._schedule_pending_expiry_reconcile(norm_sym)
        else:
            self._pending_expiry_by_symbol.pop(norm_sym, None)
            self._persist_item_expired(item_id)

    def _schedule_pending_expiry_reconcile(self, symbol: str) -> None:
        norm_sym = _normalize_symbol(symbol)
        if not norm_sym or not self._is_started or norm_sym not in self._pending_expiry_by_symbol:
            return
        clk = self._safe_clock()
        if clk is None:
            return
        try:
            now_ns = int(clk.timestamp_ns())
        except Exception:
            now_ns = 0
        if now_ns <= 0:
            return
        timer_name = f"plan_expiry_reconcile_{norm_sym}"
        try:
            if timer_name in set(clk.timer_names()):
                clk.cancel_timer(timer_name)
        except Exception:
            pass
        try:
            clk.set_time_alert_ns(
                name=timer_name,
                alert_time_ns=now_ns + 1_000_000_000,
                allow_past=True,
            )
        except Exception:
            pass

    def _trigger_pending_expiry_cancel_if_needed(self, meta: Dict[str, Any]) -> None:
        sym = _normalize_symbol(meta.get("symbol") or meta.get("instrument_id") or "")
        if not sym or sym not in self._pending_expiry_by_symbol:
            return
        if _is_protective_stop_meta(meta):
            return
        if bool(meta.get("is_reduce_only", False)) or str(
            meta.get("intent") or ""
        ).strip().upper() in EXIT_INTENT_TAGS:
            return
        if self._cancel_symbol_entries_callback is not None:
            try:
                self._cancel_symbol_entries_callback(sym)
            except Exception as exc:
                logger.error(
                    "cancel_symbol_entries_callback failed for %s during pending expiry: %s",
                    sym,
                    exc,
                    exc_info=True,
                )
                self._escalate_expiry_cancel_failure(
                    sym,
                    exc,
                    context="pending_expiry_event",
                )
        self._maybe_finalize_pending_expiry(sym)

    def _maybe_finalize_pending_expiry(self, symbol: str) -> None:
        norm_sym = _normalize_symbol(symbol)
        if not norm_sym or norm_sym not in self._pending_expiry_by_symbol:
            return
        if self._count_open_entry_leaves_in_cache(norm_sym) == 0:
            item_id, _ = self._pending_expiry_by_symbol.pop(norm_sym)
            clk = self._safe_clock()
            if clk is not None:
                rec_name = f"plan_expiry_reconcile_{norm_sym}"
                try:
                    if rec_name in set(clk.timer_names()):
                        clk.cancel_timer(rec_name)
                except Exception:
                    pass
            self._persist_item_expired(item_id)

    def flush(self, timeout_secs: float = 5.0) -> None:
        self.sync_from_cache()
        self._flush_backend_mutations()
        if self._plan_gate_backend is not None:
            backend_flush = getattr(self._plan_gate_backend, "flush", None)
            if callable(backend_flush):
                try:
                    backend_flush(timeout_secs=timeout_secs)
                except Exception:
                    pass
        if self._async_writer is not None:
            self._async_writer.flush(timeout_secs=timeout_secs)
        self._oco_monitor.flush(timeout_secs=timeout_secs)

    def _cancel_expiry_timers(self) -> None:
        clk = self._safe_clock()
        if clk is None:
            return
        try:
            active_timers = set(clk.timer_names())
        except Exception:
            active_timers = None
        timer_names = set(self._expiry_targets_by_timer.keys())
        for sym in list(self._pending_expiry_by_symbol.keys()):
            timer_names.add(f"plan_expiry_reconcile_{sym}")
        if active_timers is not None:
            for t_name in active_timers:
                if t_name.startswith("plan_expiry_") or t_name.startswith("plan_item_expired_"):
                    timer_names.add(t_name)
        for timer_name in timer_names:
            try:
                if active_timers is None or timer_name in active_timers:
                    clk.cancel_timer(timer_name)
            except Exception:
                pass

    def _unsubscribe_message_buses(self) -> None:
        if not self._is_owner_thread():
            return
        for bus_ref, topic, handler in list(self._msgbus_bindings):
            bus = bus_ref() if isinstance(bus_ref, weakref.ReferenceType) else bus_ref
            if bus is None:
                continue
            unsub = getattr(bus, "unsubscribe", None)
            if callable(unsub):
                try:
                    unsub(topic, handler)
                except Exception:
                    pass
        self._msgbus_bindings.clear()
        self._msgbus_subscriptions.clear()
        self._subscribed_msgbuses.clear()

    def _clear_runtime_collections(self) -> None:
        self._expiry_targets_by_timer.clear()
        self._pending_expiry_by_symbol.clear()
        self._scheduled_expiry_item_ids.clear()
        self._active_stop_legs_by_symbol.clear()
        self._active_stop_price_by_leg.clear()
        self._order_meta.clear()
        self._fills_by_cid.clear()
        self._clear_plan_item_cache_worker_only()
        self._startup_entry_reservations.clear()
        self._safe_exit_symbols.clear()
        self._opened_position_ids.clear()

    def on_stop(self) -> None:
        self._is_started = False
        _ACTIVE_TELEMETRY_ACTORS.discard(self)
        self._cancel_expiry_timers()
        if self._safe_is_registered():
            try:
                self.unsubscribe_topic("events.order.*", self.on_order_event)
            except Exception:
                pass
            try:
                self.unsubscribe_topic("events.position.*", self.on_position_event)
            except Exception:
                pass
            try:
                self.unsubscribe_topic("events.account.*", self.on_account_state)
            except Exception:
                pass
        self._unsubscribe_message_buses()
        self.flush(timeout_secs=2.0)

    def on_reset(self) -> None:
        self._is_started = False
        _ACTIVE_TELEMETRY_ACTORS.discard(self)
        self._cancel_expiry_timers()

    def on_dispose(self) -> None:
        self._is_started = False
        _ACTIVE_TELEMETRY_ACTORS.discard(self)
        if self._is_owner_thread():
            self.flush(timeout_secs=2.0)
            self._cancel_expiry_timers()
            self._unsubscribe_message_buses()
            self._cached_cache = None
            self._cached_clock = None
        if self._oco_monitor is not None:
            try:
                self._oco_monitor.remove_entry_freeze_callback(self._on_oco_entry_freeze)
            except Exception:
                pass
            if self._owns_oco_monitor:
                try:
                    self._oco_monitor.close()
                except Exception:
                    pass
                self._async_writer = None
        else:
            self._async_writer = None
        self._cancel_symbol_entries_callback = None
        self._plan_gate_backend = None
        self._plan_repo = None
        self._msgbus_subscriptions.clear()
        self._subscribed_msgbuses.clear()
        self._clear_runtime_collections()

    def close(self) -> None:
        self._is_started = False
        _ACTIVE_TELEMETRY_ACTORS.discard(self)
        if self._is_owner_thread():
            self.flush(timeout_secs=2.0)
            self._cancel_expiry_timers()
            self._unsubscribe_message_buses()
            self._cached_cache = None
            self._cached_clock = None
        else:
            if self._async_writer is not None:
                try:
                    self._async_writer.flush(timeout_secs=2.0)
                except Exception:
                    pass
        if self._oco_monitor is not None:
            try:
                self._oco_monitor.remove_entry_freeze_callback(self._on_oco_entry_freeze)
            except Exception:
                pass
            try:
                self._oco_monitor.close()
            except Exception:
                pass
        self._async_writer = None
        self._cancel_symbol_entries_callback = None
        self._plan_gate_backend = None
        self._plan_repo = None
        self._msgbus_subscriptions.clear()
        self._subscribed_msgbuses.clear()
        self._clear_runtime_collections()

    def __del__(self) -> None:
        if not self._is_owner_thread():
            return
        try:
            self.close()
        except Exception:
            pass

    def on_bar(self, bar: Any) -> None:
        _ = bar

    def on_quote(self, quote: Any) -> None:
        _ = quote

    def on_time_event(self, event: Any) -> None:
        timer_name = str(getattr(event, "name", "") or "")
        if timer_name in self._expiry_targets_by_timer:
            sym, item_id, vu_ns = self._expiry_targets_by_timer.pop(timer_name)
            self._handle_plan_item_expiry(sym, item_id, vu_ns)
        clk = self._safe_clock()
        now_ns = int(getattr(event, "ts_event", 0) or (clk.timestamp_ns() if clk else 0) or 0)
        if now_ns > 0 and self._expiry_targets_by_timer:
            for t_name, (sym, item_id, vu_ns) in list(self._expiry_targets_by_timer.items()):
                if now_ns >= vu_ns:
                    self._expiry_targets_by_timer.pop(t_name, None)
                    self._handle_plan_item_expiry(sym, item_id, vu_ns)
        self.sync_from_cache()
        if timer_name.startswith("plan_expiry_reconcile_") or self._pending_expiry_by_symbol:
            for pend_sym in list(self._pending_expiry_by_symbol.keys()):
                self._maybe_finalize_pending_expiry(pend_sym)
                if pend_sym in self._pending_expiry_by_symbol:
                    # Cancel any open (ACCEPTED) entry orders that may have transitioned without a bus push
                    cache = self._safe_cache()
                    has_open_non_pending = False
                    if cache is not None:
                        try:
                            for ord_obj in cache.orders_open() or ():
                                if (
                                    _normalize_symbol(getattr(ord_obj, "instrument_id", "")) == pend_sym
                                    and self._is_entry_order_obj(ord_obj)
                                    and not bool(getattr(ord_obj, "is_closed", False))
                                    and not bool(getattr(ord_obj, "is_pending_cancel", False))
                                ):
                                    has_open_non_pending = True
                                    break
                        except Exception as exc:
                            logger.warning(
                                "Failed inspecting open orders during pending expiry reconcile for %s: %s",
                                pend_sym,
                                exc,
                                exc_info=True,
                            )
                    if has_open_non_pending and self._cancel_symbol_entries_callback is not None:
                        try:
                            self._cancel_symbol_entries_callback(pend_sym)
                        except Exception as exc:
                            logger.error(
                                "cancel_symbol_entries_callback failed for %s during pending expiry reconcile: %s",
                                pend_sym,
                                exc,
                                exc_info=True,
                            )
                            self._escalate_expiry_cancel_failure(
                                pend_sym,
                                exc,
                                context="pending_expiry_reconcile",
                            )
                        self._maybe_finalize_pending_expiry(pend_sym)
                if pend_sym in self._pending_expiry_by_symbol:
                    self._schedule_pending_expiry_reconcile(pend_sym)
        self._flush_backend_mutations()

    def sync_from_cache(self) -> None:
        """Synchronizes any unprocessed position and order events from the Nautilus `Cache`."""
        self._refresh_runtime_handles()
        cache = self._safe_cache()
        if cache is None:
            return

        try:
            positions = cache.positions()
        except Exception:
            positions = []

        ordered_positions = sorted(
            positions or [],
            key=lambda p: (
                not bool(getattr(p, "is_closed", False)),
                int(getattr(p, "ts_last", 0) or 0),
            ),
        )
        for pos in ordered_positions:
            self._sync_position_snapshot(pos)

        try:
            orders = cache.orders()
        except Exception:
            orders = []

        for order in orders or []:
            self._sync_order_object(order)
            events_Attr = getattr(order, "events", None)
            events = events_Attr() if callable(events_Attr) else (events_Attr or [])
            for ev in events:
                self.on_order_event(ev)

        try:
            accounts_fn = getattr(cache, "accounts", None)
            accounts = accounts_fn() if callable(accounts_fn) else []
        except Exception:
            accounts = []

        for acct in accounts or []:
            try:
                self.on_account_state(acct)
            except Exception:
                pass

    def _has_open_protective_stop_for_symbol(self, symbol: str) -> bool:
        norm_sym = _normalize_symbol(symbol)
        if not norm_sym:
            return False
        cache = self._safe_cache()
        if cache is None:
            return False
        try:
            orders = cache.orders() or []
        except Exception:
            return False
        for order in orders:
            inst_sym = _normalize_symbol(getattr(order, "instrument_id", ""))
            if inst_sym != norm_sym:
                continue
            meta = self._cache_order_metadata(order)
            if not _is_protective_stop_meta(meta):
                continue
            cid = str(meta.get("client_order_id") or "")
            vid = str(meta.get("venue_order_id") or "")
            if (cid and cid in self._terminated_stop_cids) or (
                vid and vid in self._terminated_stop_cids
            ):
                continue
            is_closed = bool(getattr(order, "is_closed", False))
            raw_status = str(getattr(order, "status", "") or "").upper()
            if "." in raw_status:
                raw_status = raw_status.rsplit(".", 1)[-1]
            if not is_closed and raw_status not in {
                "CANCELED",
                "CANCELLED",
                "PENDING_CANCEL",
                "REJECTED",
                "EXPIRED",
                "DENIED",
                "FILLED",
                "CLOSED",
            }:
                return True
        return False

    def _register_active_stop_leg(
        self,
        meta: Dict[str, Any],
        leaves_qty: Decimal,
        stop_px: Any,
    ) -> None:
        sym = _normalize_symbol(meta.get("symbol") or meta.get("instrument_id") or "")
        if not sym or leaves_qty <= Decimal("0"):
            return
        stop_dec = _to_decimal(stop_px, "0") if stop_px is not None else Decimal("0")
        if stop_dec <= Decimal("0"):
            return
        cid = str(meta.get("client_order_id") or "")
        vid = str(meta.get("venue_order_id") or "")
        if (cid and cid in self._terminated_stop_cids) or (
            vid and vid in self._terminated_stop_cids
        ):
            return
        leg_id = cid or vid
        if not leg_id:
            return
        legs = self._active_stop_legs_by_symbol.setdefault(sym, {})
        if cid:
            legs.pop(cid, None)
        if vid:
            legs.pop(vid, None)
        legs[leg_id] = leaves_qty
        self._active_stop_price_by_leg[leg_id] = stop_dec
        if cid:
            self._active_stop_price_by_leg[cid] = stop_dec
        if vid:
            self._active_stop_price_by_leg[vid] = stop_dec
        total_covered_qty = sum(legs.values(), Decimal("0"))
        self._oco_monitor.register_protective_order(
            symbol=sym,
            covered_qty=total_covered_qty,
            stop_price=stop_dec,
            broker_order_id=vid or cid,
            status="ACTIVE",
        )

    def _invalidate_stop_leg(
        self,
        meta: Dict[str, Any],
        reason: str,
    ) -> None:
        sym = _normalize_symbol(meta.get("symbol") or meta.get("instrument_id") or "")
        cid = str(meta.get("client_order_id") or "")
        vid = str(meta.get("venue_order_id") or "")
        if cid:
            self._terminated_stop_cids.add(cid)
            self._active_stop_price_by_leg.pop(cid, None)
        if vid:
            self._terminated_stop_cids.add(vid)
            self._active_stop_price_by_leg.pop(vid, None)
        legs = self._active_stop_legs_by_symbol.get(sym)
        if legs is not None:
            if cid:
                legs.pop(cid, None)
            if vid:
                legs.pop(vid, None)
        if not sym:
            return

        self._sync_protective_orders_from_cache(sym)
        pos_qty = self._sync_open_position_qty_from_cache(sym)
        active_legs = self._active_stop_legs_by_symbol.get(sym, {})
        remaining_covered_qty = sum(active_legs.values(), Decimal("0"))

        if remaining_covered_qty > Decimal("0") and (
            pos_qty == Decimal("0") or remaining_covered_qty >= pos_qty
        ):
            active_leg_id = next(iter(active_legs.keys()), vid or cid)
            existing_cov = self._oco_monitor.get_coverage(sym)
            active_stop_px = self._active_stop_price_by_leg.get(
                active_leg_id,
                existing_cov.stop_price if existing_cov is not None else Decimal("0"),
            )
            if active_stop_px <= Decimal("0"):
                raw_px = (
                    meta.get("trigger_price")
                    or meta.get("limit_price")
                    or meta.get("stop_loss_price")
                )
                active_stop_px = _to_decimal(raw_px, "0") if raw_px is not None else Decimal("0")
            if active_stop_px > Decimal("0"):
                self._oco_monitor.register_protective_order(
                    symbol=sym,
                    covered_qty=remaining_covered_qty,
                    stop_price=active_stop_px,
                    broker_order_id=active_leg_id,
                    status="ACTIVE",
                )
                return

        if (
            cid and cid in self._replaced_stop_cids
        ) or (
            vid and vid in self._replaced_stop_cids
        ):
            if cid:
                self._replaced_stop_cids.discard(cid)
            if vid:
                self._replaced_stop_cids.discard(vid)
            if remaining_covered_qty >= pos_qty:
                return

        reason_upper = reason.upper()
        if (
            self.is_safe_exit_active(sym)
            and "CANCELED" in reason_upper
            and "REJECTED" not in reason_upper
            and "DENIED" not in reason_upper
            and "EXPIRED" not in reason_upper
        ):
            return

        if remaining_covered_qty < pos_qty and "CANCELED" in reason_upper:
            prev_cov = self._oco_monitor.get_coverage(sym)
            if prev_cov is not None and prev_cov.status == "REPLACING":
                return
            if prev_cov is None or prev_cov.broker_order_id in {cid, vid, ""}:
                try:
                    db_cov = self._oco_monitor.refresh_symbol_coverage_from_db(sym)
                except Exception as exc:
                    logger.warning(
                        "Failed refreshing DB protective_orders for %s on canceled stop leg: %s",
                        sym,
                        exc,
                    )
                    db_cov = None
                if db_cov is not None:
                    prev_cov = db_cov
            if (
                prev_cov is not None
                and prev_cov.broker_order_id not in {cid, vid, ""}
                and prev_cov.broker_order_id not in self._terminated_stop_cids
                and prev_cov.status == "ACTIVE"
                and prev_cov.covered_qty >= pos_qty
                and prev_cov.stop_price > Decimal("0")
            ):
                legs_map = self._active_stop_legs_by_symbol.setdefault(sym, {})
                legs_map[prev_cov.broker_order_id] = prev_cov.covered_qty
                self._active_stop_price_by_leg[prev_cov.broker_order_id] = (
                    prev_cov.stop_price
                )
                return

        if remaining_covered_qty > Decimal("0") and remaining_covered_qty < pos_qty:
            active_leg_id = next(iter(active_legs.keys()), vid or cid)
            existing_cov = self._oco_monitor.get_coverage(sym)
            active_stop_px = self._active_stop_price_by_leg.get(
                active_leg_id,
                existing_cov.stop_price if existing_cov is not None else Decimal("0"),
            )
            reduced_cov = ProtectiveOrderCoverage(
                symbol=sym,
                broker=self._broker,
                broker_order_id=active_leg_id,
                covered_qty=remaining_covered_qty,
                stop_price=active_stop_px,
                take_profit_price=(
                    existing_cov.take_profit_price if existing_cov is not None else None
                ),
                status="ACTIVE",
                item_id=existing_cov.item_id if existing_cov is not None else None,
            )
            self._oco_monitor._coverage_by_symbol[sym] = reduced_cov
            self._oco_monitor.verify_position_coverage(
                symbol=sym,
                pos_qty=pos_qty,
                item_id=reduced_cov.item_id,
                override_reason=reason,
            )
            return

        self._oco_monitor.invalidate_protective_order(
            symbol=sym,
            broker_order_id=vid or cid,
            reason=reason,
        )

    def _sync_order_object(self, order: Any) -> Dict[str, Any]:
        """Caches order metadata and registers active protective stop orders (`STOP_*`) if not closed."""
        meta = self._cache_order_metadata(order)
        if not meta:
            return meta
        if not _is_protective_stop_meta(meta):
            return meta
        cid = str(meta.get("client_order_id") or "")
        vid = str(meta.get("venue_order_id") or "")
        if (cid and cid in self._terminated_stop_cids) or (
            vid and vid in self._terminated_stop_cids
        ):
            return meta
        is_closed = bool(getattr(order, "is_closed", False))
        raw_status = str(getattr(order, "status", "") or "").upper()
        if "." in raw_status:
            raw_status = raw_status.rsplit(".", 1)[-1]
        if is_closed or raw_status in {
            "CANCELED",
            "CANCELLED",
            "PENDING_CANCEL",
            "REJECTED",
            "EXPIRED",
            "DENIED",
            "FILLED",
            "CLOSED",
        }:
            return meta
        cid = str(meta.get("client_order_id") or "")
        cum_filled = sum(
            (q for q, _ in self._fills_by_cid.get(cid, {}).values()),
            Decimal("0"),
        )
        order_filled = max(
            _to_decimal(meta.get("filled_qty"), "0"),
            cum_filled,
        )
        total_qty = _to_decimal(meta.get("quantity"), "0")
        leaves_attr = getattr(order, "leaves_qty", None)
        leaves_qty = (
            min(
                _to_decimal(leaves_attr, "0"),
                max(Decimal("0"), total_qty - order_filled),
            )
            if leaves_attr is not None
            else max(Decimal("0"), total_qty - order_filled)
        )
        if leaves_qty <= Decimal("0"):
            return meta
        stop_px = (
            meta.get("trigger_price")
            or meta.get("limit_price")
            or meta.get("stop_loss_price")
        )
        stop_dec = _to_decimal(stop_px, "0") if stop_px is not None else Decimal("0")
        if stop_dec > Decimal("0"):
            self._register_active_stop_leg(meta, leaves_qty, stop_dec)
        return meta

    def _sync_protective_orders_from_cache(
        self,
        symbol: Optional[str] = None,
    ) -> None:
        """Scans `self.cache.orders()` for active protective stop orders matching `symbol`."""
        cache = self._safe_cache()
        if cache is None:
            return
        try:
            orders = cache.orders()
        except Exception:
            return
        target_sym = _normalize_symbol(symbol) if symbol else ""
        for order in orders or []:
            if target_sym:
                inst_sym = _normalize_symbol(getattr(order, "instrument_id", ""))
                if inst_sym != target_sym:
                    continue
            self._sync_order_object(order)

    def _sync_open_position_qty_from_cache(self, symbol: str) -> Decimal:
        """Ensures `_oco_monitor._positions_qty[symbol]` reflects open positions in `self.cache`."""
        norm_sym = _normalize_symbol(symbol)
        if not norm_sym:
            return Decimal("0")
        existing_qty = self._oco_monitor.get_position_qty(norm_sym)
        cache = self._safe_cache()
        if cache is None:
            return existing_qty
        try:
            positions = cache.positions() or []
        except Exception:
            return existing_qty
        found_symbol = False
        total_open = Decimal("0")
        for pos in positions:
            pos_sym = _normalize_symbol(
                getattr(pos, "instrument_id", "") or getattr(pos, "symbol", "")
            )
            if pos_sym != norm_sym:
                continue
            found_symbol = True
            if bool(getattr(pos, "is_closed", False)):
                continue
            total_open += abs(_to_decimal(getattr(pos, "quantity", None), "0"))
        if found_symbol:
            self._oco_monitor.record_position_qty_sync(norm_sym, total_open)
            self._oco_monitor._positions_qty[norm_sym] = total_open
            return total_open
        return existing_qty

    @staticmethod
    def _positive_ns(value: Any) -> Optional[int]:
        if value is None or isinstance(value, bool):
            return None
        try:
            iv = int(value)
            return iv if iv > 0 else None
        except (TypeError, ValueError):
            return None

    def _cache_order_metadata(self, order: Any) -> Dict[str, Any]:
        cid = str(getattr(order, "client_order_id", "") or "")
        if not cid:
            return {}
        prev_meta = dict(self._order_meta.get(cid, {}))
        inst_id = str(getattr(order, "instrument_id", "") or "")
        symbol = _normalize_symbol(inst_id)
        side = _normalize_side_str(getattr(order, "side", "BUY"))
        order_type = _normalize_order_type_str(getattr(order, "order_type", "MARKET"))
        qty = _to_decimal(getattr(order, "quantity", None), "0")
        filled_qty = _to_decimal(getattr(order, "filled_qty", None), "0")
        price_attr = getattr(order, "price", None)
        trigger_attr = getattr(order, "trigger_price", None)
        limit_px = _to_decimal(price_attr, "0") if price_attr is not None else None
        trigger_px = (
            _to_decimal(trigger_attr, "0") if trigger_attr is not None else None
        )
        is_reduce_only = bool(getattr(order, "is_reduce_only", False))
        parent_order_id = str(getattr(order, "parent_order_id", "") or "")
        exec_params = getattr(order, "exec_algorithm_params", None) or {}
        stop_loss_raw = (
            exec_params.get("stop_loss_price")
            if isinstance(exec_params, dict)
            else None
        )
        stop_loss_px = (
            _to_decimal(stop_loss_raw, "0") if stop_loss_raw is not None else None
        )
        intent_val = ""
        if isinstance(exec_params, dict):
            intent_val = str(
                exec_params.get("intent") or exec_params.get("action") or ""
            ).strip().upper()
        if not intent_val:
            tags_raw = getattr(order, "tags", None) or []
            tags_upper = {str(t).strip().upper() for t in tags_raw}
            if (
                "PROTECTIVE_STOP" in tags_upper
                or "STOP_LOSS" in tags_upper
                or "EXIT" in tags_upper
            ):
                intent_val = "PROTECTIVE_STOP"
            elif "ENTRY" in tags_upper:
                intent_val = "ENTRY"
        signal_ts_ns = (
            self._positive_ns(prev_meta.get("signal_ts_ns"))
            or self._positive_ns(
                exec_params.get("signal_ts_ns")
                if isinstance(exec_params, Mapping)
                else None
            )
            or self._positive_ns(getattr(order, "ts_init", None))
        )
        order_vid = str(getattr(order, "venue_order_id", "") or "")
        prev_vid = str(prev_meta.get("venue_order_id") or "")
        is_stale_cache_snapshot = bool(prev_vid and not order_vid)
        eff_vid = order_vid or prev_vid
        eff_qty = (
            _to_decimal(prev_meta.get("quantity"), "0")
            if is_stale_cache_snapshot and _to_decimal(prev_meta.get("quantity"), "0") > Decimal("0")
            else qty
        )
        eff_filled_qty = max(filled_qty, _to_decimal(prev_meta.get("filled_qty"), "0"))
        eff_leaves_qty = max(Decimal("0"), _to_decimal(eff_qty, "0") - eff_filled_qty)
        eff_limit_px = (
            prev_meta.get("limit_price")
            if is_stale_cache_snapshot and prev_meta.get("limit_price") is not None
            else limit_px
        )
        eff_trigger_px = (
            prev_meta.get("trigger_price")
            if is_stale_cache_snapshot and prev_meta.get("trigger_price") is not None
            else trigger_px
        )
        raw_order_status = (
            order.status_string()
            if callable(getattr(order, "status_string", None))
            else getattr(order, "status", None)
        )
        order_status_str = (
            str(raw_order_status).rsplit(".", 1)[-1].strip().upper()
            if raw_order_status is not None and str(raw_order_status).strip()
            else ""
        )
        prev_status_str = str(prev_meta.get("status") or "").strip().upper()
        eff_status = (
            prev_status_str
            if is_stale_cache_snapshot and prev_status_str
            else (order_status_str or prev_status_str or "INITIALIZED")
        )
        meta = {
            "client_order_id": cid,
            "venue_order_id": eff_vid,
            "strategy_id": str(getattr(order, "strategy_id", "") or ""),
            "instrument_id": inst_id,
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "status": eff_status,
            "quantity": eff_qty,
            "filled_qty": eff_filled_qty,
            "leaves_qty": eff_leaves_qty,
            "limit_price": eff_limit_px,
            "trigger_price": eff_trigger_px,
            "stop_loss_price": stop_loss_px,
            "is_reduce_only": is_reduce_only,
            "parent_order_id": parent_order_id,
            "intent": intent_val,
            "signal_ts_ns": signal_ts_ns,
            "gate_admitted_ts_ns": self._positive_ns(prev_meta.get("gate_admitted_ts_ns")),
            "broker_ack_ts_ns": self._positive_ns(prev_meta.get("broker_ack_ts_ns")),
        }
        self._order_meta[cid] = meta
        return meta

    def _resolve_order_metadata(self, event: Any) -> Dict[str, Any]:
        cid = str(getattr(event, "client_order_id", "") or "")
        cached_meta = dict(self._order_meta.get(cid, {}))
        try:
            cache = self._safe_cache()
            if cache is not None and cid:
                order_obj = cache.order(ClientOrderId(cid))
                if order_obj is not None:
                    cached_meta.update(self._cache_order_metadata(order_obj))
        except Exception:
            pass

        inst_id = str(
            getattr(event, "instrument_id", None)
            or cached_meta.get("instrument_id")
            or ""
        )
        symbol = _normalize_symbol(inst_id or cached_meta.get("symbol") or "")
        side_raw = getattr(event, "order_side", None) or getattr(event, "side", None)
        side = (
            _normalize_side_str(side_raw)
            if side_raw is not None
            else str(cached_meta.get("side") or "BUY")
        )
        type_raw = getattr(event, "order_type", None) or getattr(event, "type", None)
        order_type = (
            _normalize_order_type_str(type_raw)
            if type_raw is not None
            else str(cached_meta.get("order_type") or "MARKET")
        )
        order_qty_raw = getattr(event, "quantity", None)
        cached_qty = _to_decimal(cached_meta.get("quantity"), "0")
        if order_qty_raw is not None:
            qty = _to_decimal(order_qty_raw, "0")
        elif cached_qty > Decimal("0"):
            qty = cached_qty
        else:
            qty = _to_decimal(getattr(event, "last_qty", None), "0")
        px_raw = getattr(event, "price", None)
        limit_px = (
            _to_decimal(px_raw, "0")
            if px_raw is not None
            else cached_meta.get("limit_price")
        )
        trig_raw = getattr(event, "trigger_price", None)
        trigger_px = (
            _to_decimal(trig_raw, "0")
            if trig_raw is not None
            else cached_meta.get("trigger_price")
        )
        vid_raw = getattr(event, "venue_order_id", None)
        vid = (
            str(vid_raw)
            if vid_raw is not None and str(vid_raw) != "None"
            else str(cached_meta.get("venue_order_id") or "")
        )
        strategy_id = str(
            getattr(event, "strategy_id", None) or cached_meta.get("strategy_id") or ""
        )
        is_reduce_only = bool(
            getattr(event, "reduce_only", None)
            if getattr(event, "reduce_only", None) is not None
            else cached_meta.get("is_reduce_only", False)
        )
        parent_order_id = str(
            getattr(event, "parent_order_id", None)
            or cached_meta.get("parent_order_id")
            or ""
        )
        ev_exec_params = getattr(event, "exec_algorithm_params", None) or {}
        ev_stop_raw = (
            ev_exec_params.get("stop_loss_price")
            if isinstance(ev_exec_params, dict)
            else None
        )
        stop_loss_px = (
            _to_decimal(ev_stop_raw, "0")
            if ev_stop_raw is not None
            else cached_meta.get("stop_loss_price")
        )
        intent_val = str(cached_meta.get("intent") or "").strip().upper()
        if isinstance(ev_exec_params, dict) and not intent_val:
            intent_val = str(
                ev_exec_params.get("intent") or ev_exec_params.get("action") or ""
            ).strip().upper()
        signal_ts_ns = (
            self._positive_ns(cached_meta.get("signal_ts_ns"))
            or self._positive_ns(
                ev_exec_params.get("signal_ts_ns")
                if isinstance(ev_exec_params, Mapping)
                else None
            )
            or (
                self._positive_ns(getattr(event, "ts_init", None))
                or self._positive_ns(getattr(event, "ts_event", None))
                if isinstance(event, OrderInitialized)
                else None
            )
        )
        gate_admitted_ts_ns = self._positive_ns(cached_meta.get("gate_admitted_ts_ns"))
        if gate_admitted_ts_ns is None and isinstance(event, OrderSubmitted):
            gate_admitted_ts_ns = self._positive_ns(
                getattr(event, "ts_init", None)
            ) or self._positive_ns(getattr(event, "ts_event", None))
        broker_ack_ts_ns = self._positive_ns(cached_meta.get("broker_ack_ts_ns"))
        if broker_ack_ts_ns is None and isinstance(event, OrderAccepted):
            broker_ack_ts_ns = self._positive_ns(
                getattr(event, "ts_init", None)
            ) or self._positive_ns(getattr(event, "ts_event", None))

        qty_dec = _to_decimal(qty, "0")
        filled_qty_dec = _to_decimal(cached_meta.get("filled_qty"), "0")
        merged_status = str(cached_meta.get("status") or "").strip().upper() or "INITIALIZED"
        merged = {
            "client_order_id": cid,
            "venue_order_id": vid,
            "strategy_id": strategy_id,
            "instrument_id": inst_id,
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "status": merged_status,
            "quantity": qty,
            "filled_qty": filled_qty_dec,
            "leaves_qty": max(Decimal("0"), qty_dec - filled_qty_dec),
            "limit_price": limit_px,
            "trigger_price": trigger_px,
            "stop_loss_price": stop_loss_px,
            "is_reduce_only": is_reduce_only,
            "parent_order_id": parent_order_id,
            "intent": intent_val,
            "signal_ts_ns": signal_ts_ns,
            "gate_admitted_ts_ns": gate_admitted_ts_ns,
            "broker_ack_ts_ns": broker_ack_ts_ns,
        }
        if cid:
            self._order_meta[cid] = merged
        return merged

    def on_event(self, event: Any) -> None:
        """Generic dispatcher for order, position, and account events."""
        if isinstance(
            event,
            (
                OrderInitialized,
                OrderSubmitted,
                OrderAccepted,
                OrderDenied,
                OrderCanceled,
                OrderRejected,
                OrderFilled,
                OrderUpdated,
                OrderExpired,
            ),
        ):
            self.on_order_event(event)
        elif isinstance(event, (PositionOpened, PositionChanged, PositionClosed)):
            self.on_position_event(event)
        elif (
            (AccountState is not None and isinstance(event, AccountState))
            or hasattr(event, "balances")
            or hasattr(event, "cash_balance")
            or (
                isinstance(event, Mapping)
                and (
                    "cash_balance" in event
                    or "equity" in event
                    or "buying_power" in event
                    or "balances" in event
                )
            )
        ):
            self.on_account_state(event)

    def on_account_state(self, event: Any) -> None:
        """Persists an `AccountState` or account balance snapshot into `trading_accounts`."""
        if event is None:
            return
        try:
            snap = self._extract_account_snapshot(event)
            event_id = str(snap.get("event_id") or "")
            if event_id and event_id in self._processed_event_ids:
                return

            def _task(cur: Any) -> None:
                self._upsert_trading_account_in_tx(cur, snap)

            if self._persist_to_db and self._async_writer is not None:
                self._async_writer.submit(_task)
            if event_id:
                self._processed_event_ids.add(event_id)
        except Exception as exc:
            logger.error("on_account_state failed: %s", exc, exc_info=True)
            raise

    on_account_event = on_account_state

    def _extract_account_snapshot(self, event: Any) -> Dict[str, Any]:
        if isinstance(event, Mapping):
            raw_map = dict(event)
        else:
            raw_map = {}

        def _get_val(*keys: str) -> Any:
            for k in keys:
                if k in raw_map and raw_map[k] is not None:
                    return raw_map[k]
                attr = getattr(event, k, None)
                if attr is not None:
                    return attr
            return None

        acct_id_raw = _get_val("account_id", "id")
        account_id = str(acct_id_raw).strip() if acct_id_raw is not None else ""
        event_id_raw = _get_val("event_id")
        event_id = str(event_id_raw).strip() if event_id_raw is not None else ""

        currency = str(_get_val("currency", "base_currency") or "USD").strip().upper()
        balances = _get_val("balances")
        if callable(balances):
            balances = balances()
        if isinstance(balances, Mapping):
            balances = list(balances.values())

        bal_total: Optional[Decimal] = None
        bal_free: Optional[Decimal] = None
        if isinstance(balances, (list, tuple)) and balances:
            b0 = balances[0]
            if isinstance(b0, Mapping):
                if b0.get("total") is not None:
                    bal_total = _to_decimal(b0.get("total"), "0")
                if b0.get("free") is not None:
                    bal_free = _to_decimal(b0.get("free"), "0")
                if b0.get("currency"):
                    currency = str(b0["currency"]).strip().upper()
            else:
                tot_attr = getattr(b0, "total", None)
                free_attr = getattr(b0, "free", None)
                cur_attr = getattr(b0, "currency", None)
                if callable(tot_attr):
                    tot_attr = tot_attr()
                if callable(free_attr):
                    free_attr = free_attr()
                if tot_attr is not None:
                    bal_total = _to_decimal(tot_attr, "0")
                if free_attr is not None:
                    bal_free = _to_decimal(free_attr, "0")
                if cur_attr is not None:
                    currency = str(cur_attr).strip().upper()

        cash_raw = _get_val("cash_balance", "cash", "free_balance", "balance")
        cash_dec: Optional[Decimal] = (
            _to_decimal(cash_raw, "0")
            if cash_raw is not None
            else (bal_total if bal_total is not None else bal_free)
        )
        equity_raw = _get_val("equity", "total_equity", "net_liquidation")
        equity_dec: Optional[Decimal] = (
            _to_decimal(equity_raw, "0") if equity_raw is not None else None
        )
        bp_raw = _get_val("buying_power")
        bp_dec: Optional[Decimal] = (
            _to_decimal(bp_raw, "0")
            if bp_raw is not None
            else (bal_free if bal_free is not None else None)
        )
        mv_raw = _get_val("total_market_value", "market_value")
        mv_dec: Optional[Decimal] = (
            _to_decimal(mv_raw, "0") if mv_raw is not None else None
        )
        cb_raw = _get_val("total_cost_basis", "cost_basis")
        cb_dec: Optional[Decimal] = (
            _to_decimal(cb_raw, "0") if cb_raw is not None else None
        )
        upnl_raw = _get_val("unrealized_pnl", "unrealized_pl")
        upnl_dec: Optional[Decimal] = (
            _to_decimal(upnl_raw, "0") if upnl_raw is not None else None
        )
        rpnl_raw = _get_val("realized_pnl", "realized_pl")
        rpnl_dec: Optional[Decimal] = (
            _to_decimal(rpnl_raw, "0") if rpnl_raw is not None else None
        )
        init_cap_raw = _get_val("initial_capital")
        init_cap_dec: Optional[Decimal] = (
            _to_decimal(init_cap_raw, "100000") if init_cap_raw is not None else None
        )

        return {
            "event_id": event_id,
            "account_id": account_id,
            "currency": currency or "USD",
            "initial_capital": init_cap_dec,
            "cash_balance": cash_dec,
            "equity": equity_dec,
            "buying_power": bp_dec,
            "total_market_value": mv_dec,
            "total_cost_basis": cb_dec,
            "unrealized_pnl": upnl_dec,
            "realized_pnl": rpnl_dec,
        }

    def _upsert_trading_account_in_tx(
        self,
        cur: Any,
        account_snapshot: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if not ProtectiveOcoMonitor._table_exists(cur, "trading_accounts"):
            return
        snap = dict(account_snapshot or {})
        cur.execute(
            """
            SELECT account_id, currency, initial_capital, cash_balance,
                   realized_pnl, unrealized_pnl, total_market_value,
                   total_cost_basis, equity, buying_power
            FROM trading_accounts
            WHERE broker = %s AND environment = %s
            LIMIT 1
            """,
            (self._broker, self._environment),
        )
        existing = cur.fetchone() or {}

        pos_mv = 0.0
        pos_cb = 0.0
        pos_upnl = 0.0
        pos_rpnl = 0.0
        if ProtectiveOcoMonitor._table_exists(cur, "trading_positions"):
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN ABS(qty) > 1e-9 THEN COALESCE(market_value, qty * COALESCE(current_price, avg_entry_price, 0.0)) ELSE 0.0 END), 0.0) AS pos_mv,
                    COALESCE(SUM(CASE WHEN ABS(qty) > 1e-9 THEN COALESCE(cost_basis, ABS(qty) * COALESCE(avg_entry_price, 0.0)) ELSE 0.0 END), 0.0) AS pos_cb,
                    COALESCE(SUM(CASE WHEN ABS(qty) > 1e-9 THEN COALESCE(unrealized_pl, 0.0) ELSE 0.0 END), 0.0) AS pos_upnl,
                    COALESCE(SUM(COALESCE(realized_pl, 0.0)), 0.0) AS pos_rpnl
                FROM trading_positions
                WHERE LOWER(broker) = LOWER(%s)
                  AND UPPER(environment) = UPPER(%s)
                """,
                (self._broker, self._environment),
            )
            pos_agg = cur.fetchone() or {}
            pos_mv = float(pos_agg.get("pos_mv") or 0.0)
            pos_cb = float(pos_agg.get("pos_cb") or 0.0)
            pos_upnl = float(pos_agg.get("pos_upnl") or 0.0)
            pos_rpnl = float(pos_agg.get("pos_rpnl") or 0.0)

        account_id = str(
            snap.get("account_id")
            or existing.get("account_id")
            or f"{self._broker.upper()}-{self._environment}"
        )
        currency = str(
            snap.get("currency") or existing.get("currency") or "USD"
        ).strip().upper()
        initial_capital = (
            float(snap["initial_capital"])
            if snap.get("initial_capital") is not None
            else float(existing.get("initial_capital") or 100000.0)
        )
        total_market_value = (
            float(snap["total_market_value"])
            if snap.get("total_market_value") is not None
            else pos_mv
        )
        total_cost_basis = (
            float(snap["total_cost_basis"])
            if snap.get("total_cost_basis") is not None
            else pos_cb
        )
        unrealized_pnl = (
            float(snap["unrealized_pnl"])
            if snap.get("unrealized_pnl") is not None
            else pos_upnl
        )
        realized_pnl = (
            float(snap["realized_pnl"])
            if snap.get("realized_pnl") is not None
            else (
                pos_rpnl
                if pos_rpnl != 0.0
                else float(existing.get("realized_pnl") or 0.0)
            )
        )
        if snap.get("cash_balance") is not None:
            cash_balance = float(snap["cash_balance"])
        elif pos_cb > 0.0 or pos_mv > 0.0 or pos_rpnl != 0.0:
            cash_balance = initial_capital - total_cost_basis + realized_pnl
        else:
            net_outflow = 0.0
            net_mv = 0.0
            if ProtectiveOcoMonitor._table_exists(cur, "order_fills"):
                cur.execute(
                    """
                    SELECT
                        COALESCE(SUM(CASE WHEN UPPER(side) = 'BUY' THEN last_qty * last_px + COALESCE(commission, 0.0)
                                          WHEN UPPER(side) = 'SELL' THEN -(last_qty * last_px - COALESCE(commission, 0.0))
                                          ELSE 0.0 END), 0.0) AS net_fill_outflow,
                        COALESCE(SUM(CASE WHEN UPPER(side) = 'BUY' THEN last_qty * last_px
                                          WHEN UPPER(side) = 'SELL' THEN -(last_qty * last_px)
                                          ELSE 0.0 END), 0.0) AS net_fill_mv
                    FROM order_fills
                    WHERE LOWER(broker) = LOWER(%s)
                      AND UPPER(environment) = UPPER(%s)
                    """,
                    (self._broker, self._environment),
                )
                fill_agg = cur.fetchone() or {}
                net_outflow = float(fill_agg.get("net_fill_outflow") or 0.0)
                net_mv = max(0.0, float(fill_agg.get("net_fill_mv") or 0.0))
            if net_outflow != 0.0:
                cash_balance = initial_capital - net_outflow
                if total_market_value == 0.0:
                    total_market_value = net_mv
                if total_cost_basis == 0.0:
                    total_cost_basis = net_mv
            elif existing.get("cash_balance") is not None:
                cash_balance = float(existing["cash_balance"])
            else:
                cash_balance = initial_capital - total_cost_basis + realized_pnl

        equity = (
            float(snap["equity"])
            if snap.get("equity") is not None
            else (cash_balance + total_market_value)
        )
        buying_power = (
            float(snap["buying_power"])
            if snap.get("buying_power") is not None
            else max(0.0, cash_balance)
        )

        cur.execute(
            """
            INSERT INTO trading_accounts (
                broker, environment, account_id, currency, initial_capital,
                cash_balance, realized_pnl, unrealized_pnl,
                total_market_value, total_cost_basis, equity, buying_power, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, NOW()
            )
            ON CONFLICT (broker, environment) DO UPDATE SET
                account_id = EXCLUDED.account_id,
                currency = EXCLUDED.currency,
                initial_capital = EXCLUDED.initial_capital,
                cash_balance = EXCLUDED.cash_balance,
                realized_pnl = EXCLUDED.realized_pnl,
                unrealized_pnl = EXCLUDED.unrealized_pnl,
                total_market_value = EXCLUDED.total_market_value,
                total_cost_basis = EXCLUDED.total_cost_basis,
                equity = EXCLUDED.equity,
                buying_power = EXCLUDED.buying_power,
                updated_at = NOW()
            """,
            (
                self._broker,
                self._environment,
                account_id,
                currency,
                initial_capital,
                cash_balance,
                realized_pnl,
                unrealized_pnl,
                total_market_value,
                total_cost_basis,
                equity,
                buying_power,
            ),
        )

    def on_order_event(self, event: Any) -> None:
        """Routes an order lifecycle event to its specific handler."""
        if isinstance(event, OrderInitialized):
            self.on_order_initialized(event)
        elif isinstance(event, OrderSubmitted):
            self.on_order_submitted(event)
        elif isinstance(event, OrderAccepted):
            self.on_order_accepted(event)
        elif isinstance(event, OrderDenied):
            self.on_order_denied(event)
        elif isinstance(event, OrderCanceled):
            self.on_order_canceled(event)
        elif isinstance(event, OrderRejected):
            self.on_order_rejected(event)
        elif isinstance(event, OrderExpired):
            self.on_order_expired(event)
        elif isinstance(event, OrderUpdated):
            self.on_order_updated(event)
        elif isinstance(event, OrderFilled):
            self.on_order_filled(event)

    def on_position_event(self, event: Any) -> None:
        """Routes a position lifecycle event to its specific handler."""
        if isinstance(event, PositionOpened):
            self.on_position_opened(event)
        elif isinstance(event, PositionChanged):
            self.on_position_changed(event)
        elif isinstance(event, PositionClosed):
            self.on_position_closed(event)

    def _release_entry_reservation_on_terminal(
        self,
        meta: Dict[str, Any],
        terminal_reason: str,
    ) -> None:
        if _is_protective_stop_meta(meta):
            return
        if bool(meta.get("is_reduce_only", False)) or str(
            meta.get("intent") or ""
        ).strip().upper() in EXIT_INTENT_TAGS:
            return
        cid = str(meta.get("client_order_id") or "").strip()
        sym = _normalize_symbol(meta.get("symbol") or meta.get("instrument_id") or "")
        if not cid or not sym:
            return
        if cid in self._released_entry_cids:
            return
        self._released_entry_cids.add(cid)

        fills_sum = sum(
            (q for q, _ in self._fills_by_cid.get(cid, {}).values()),
            Decimal("0"),
        )
        cached_order_filled = Decimal("0")
        cache = self._safe_cache()
        if cache is not None:
            try:
                ord_obj = cache.order(ClientOrderId(cid))
                if ord_obj is not None:
                    cached_order_filled = _to_decimal(
                        getattr(ord_obj, "filled_qty", None), "0"
                    )
            except Exception as exc:
                logger.warning(
                    "Failed reading cached order %s for reservation release (%s): %s",
                    cid,
                    sym,
                    exc,
                    exc_info=True,
                )
        cum_filled = max(
            _to_decimal(meta.get("filled_qty"), "0"),
            fills_sum,
            cached_order_filled,
        )

        if self._plan_gate_backend is not None:
            rust_engine = getattr(self._plan_gate_backend, "rust_engine", None)
            if rust_engine is not None and rust_engine.has_admitted_reservation(cid):
                self._plan_gate_backend.reconcile_terminal_order(
                    client_order_id=cid,
                    symbol=sym,
                    filled_qty=cum_filled,
                )
                self._startup_entry_reservations.pop(cid, None)
            elif cid in self._startup_entry_reservations:
                startup_sym, startup_qty = self._startup_entry_reservations.pop(cid)
                unfilled = max(Decimal("0"), startup_qty - cum_filled)
                if unfilled > Decimal("0"):
                    self._plan_gate_backend.release_reservation(
                        startup_sym,
                        unfilled,
                        reason=terminal_reason,
                    )

    def on_order_initialized(self, event: OrderInitialized) -> None:
        self._resolve_order_metadata(event)
        self._flush_backend_mutations()

    def _sync_protective_stop_leg_from_meta(
        self,
        meta: Dict[str, Any],
        *,
        zero_leaves_reason: str,
    ) -> None:
        if not _is_protective_stop_meta(meta):
            return
        stop_px = (
            meta.get("trigger_price")
            or meta.get("limit_price")
            or meta.get("stop_loss_price")
        )
        leaves_qty = max(
            Decimal("0"),
            _to_decimal(meta.get("leaves_qty"), "0")
            if meta.get("leaves_qty") is not None
            else _to_decimal(meta.get("quantity"), "0")
            - _to_decimal(meta.get("filled_qty"), "0"),
        )
        if (
            leaves_qty > Decimal("0")
            and stop_px is not None
            and _to_decimal(stop_px, "0") > Decimal("0")
        ):
            self._register_active_stop_leg(meta, leaves_qty, stop_px)
        elif leaves_qty <= Decimal("0"):
            self._invalidate_stop_leg(meta, reason=zero_leaves_reason)

    def on_order_submitted(self, event: OrderSubmitted) -> None:
        meta = self._resolve_order_metadata(event)
        event_id = str(getattr(event, "event_id", "") or f"sub:{meta['client_order_id']}")
        if event_id in self._processed_event_ids:
            return
        self._upsert_order_and_plan_event(
            meta=meta,
            status="SUBMITTED",
            plan_event_type="ORDER_SUBMITTED",
            event_id=event_id,
            reason="Order submitted to execution client",
        )
        if _is_protective_stop_meta(meta):
            self._sync_protective_stop_leg_from_meta(
                meta,
                zero_leaves_reason="ORDER_SUBMITTED_ZERO_LEAVES",
            )
        else:
            self._trigger_pending_expiry_cancel_if_needed(meta)
        self._flush_backend_mutations()

    def on_order_accepted(self, event: OrderAccepted) -> None:
        meta = self._resolve_order_metadata(event)
        event_id = str(getattr(event, "event_id", "") or f"acc:{meta['client_order_id']}")
        if event_id in self._processed_event_ids:
            return
        self._upsert_order_and_plan_event(
            meta=meta,
            status="ACCEPTED",
            plan_event_type="ORDER_ACCEPTED",
            event_id=event_id,
            reason="Order accepted by venue",
        )
        if _is_protective_stop_meta(meta):
            self._sync_protective_stop_leg_from_meta(
                meta,
                zero_leaves_reason="ORDER_ACCEPTED_ZERO_LEAVES",
            )
        else:
            self._trigger_pending_expiry_cancel_if_needed(meta)
        self._flush_backend_mutations()

    def _is_local_initialized_parent_update(self, event: Any, meta: Dict[str, Any]) -> bool:
        ev_vid = getattr(event, "venue_order_id", None)
        if ev_vid is not None and str(ev_vid) not in ("", "None"):
            return False
        cid = str(meta.get("client_order_id") or getattr(event, "client_order_id", "") or "")
        cached_status = ""
        cached_vid = ""
        cache = self._safe_cache()
        if cache is not None and cid:
            try:
                cached_order = cache.order(ClientOrderId(cid))
                if cached_order is not None:
                    raw_status = str(getattr(cached_order, "status", "") or "").upper()
                    cached_status = (
                        raw_status.rsplit(".", 1)[-1] if "." in raw_status else raw_status
                    )
                    raw_vid = getattr(cached_order, "venue_order_id", None)
                    cached_vid = (
                        str(raw_vid)
                        if raw_vid is not None and str(raw_vid) not in ("", "None")
                        else ""
                    )
            except Exception as exc:
                logger.warning(
                    "Failed reading cached order %s in _is_local_initialized_parent_update: %s",
                    cid,
                    exc,
                    exc_info=True,
                )
        if cached_status == "INITIALIZED":
            return True
        if not cached_vid and not meta.get("venue_order_id") and cached_status in ("", "INITIALIZED"):
            return True
        return False

    def on_order_updated(self, event: OrderUpdated) -> None:
        meta = self._resolve_order_metadata(event)
        if self._is_local_initialized_parent_update(event, meta):
            return
        event_id = str(getattr(event, "event_id", "") or f"upd:{meta['client_order_id']}")
        if event_id in self._processed_event_ids:
            return
        self._upsert_order_and_plan_event(
            meta=meta,
            status="ACCEPTED",
            plan_event_type="ORDER_UPDATED",
            event_id=event_id,
            reason="Order updated",
        )
        self._sync_protective_stop_leg_from_meta(
            meta,
            zero_leaves_reason="ORDER_UPDATED_ZERO_LEAVES",
        )
        self._flush_backend_mutations()

    def _handle_safe_exit_failure_if_applicable(
        self,
        meta: Dict[str, Any],
        failure_reason: str,
    ) -> None:
        sym = _normalize_symbol(meta.get("symbol") or meta.get("instrument_id") or "")
        if not sym or not self.is_safe_exit_active(sym):
            return
        intent_val = str(meta.get("intent") or "").strip().upper()
        is_reduce_only = bool(meta.get("is_reduce_only", False))
        if not (is_reduce_only or intent_val in {"EXIT", "FLATTEN", "CLOSE"}):
            return
        self.end_safe_exit(sym)
        pos_qty = self._sync_open_position_qty_from_cache(sym)
        if pos_qty > Decimal("0"):
            cid = str(meta.get("client_order_id") or "")
            vid = str(meta.get("venue_order_id") or "")
            self._oco_monitor.invalidate_protective_order(
                symbol=sym,
                broker_order_id=vid or cid,
                reason=failure_reason,
            )

    def on_order_denied(self, event: OrderDenied) -> None:
        meta = self._resolve_order_metadata(event)
        reason = str(getattr(event, "reason", "ORDER_DENIED") or "ORDER_DENIED")
        event_id = str(getattr(event, "event_id", "") or f"den:{meta['client_order_id']}")
        if event_id in self._processed_event_ids:
            return
        self._upsert_order_and_plan_event(
            meta=meta,
            status="DENIED",
            plan_event_type="ORDER_DENIED",
            event_id=event_id,
            reason=reason,
            alert_spec={
                "event_type": "ORDER_DENIED",
                "severity": "WARN",
                "title": f"Order denied for {meta['symbol']} ({meta['client_order_id']})",
                "body": reason,
                "dedup_key": f"telemetry_order_denied:{meta['client_order_id']}:{event_id}",
            },
        )
        if _is_protective_stop_meta(meta):
            self._invalidate_stop_leg(
                meta=meta,
                reason=f"Protective stop order {meta['client_order_id']} was DENIED: {reason}",
            )
        else:
            self._handle_safe_exit_failure_if_applicable(
                meta=meta,
                failure_reason=f"Safe exit order {meta['client_order_id']} was DENIED while position open: {reason}",
            )
            self._release_entry_reservation_on_terminal(meta, f"ORDER_DENIED: {reason}")
        self._maybe_finalize_pending_expiry(meta["symbol"])
        self._flush_backend_mutations()

    def on_order_canceled(self, event: OrderCanceled) -> None:
        meta = self._resolve_order_metadata(event)
        event_id = str(getattr(event, "event_id", "") or f"can:{meta['client_order_id']}")
        if event_id in self._processed_event_ids:
            return
        self._upsert_order_and_plan_event(
            meta=meta,
            status="CANCELED",
            plan_event_type="ORDER_CANCELED",
            event_id=event_id,
            reason="Order canceled",
        )
        if _is_protective_stop_meta(meta):
            self._invalidate_stop_leg(
                meta=meta,
                reason=f"Protective stop order {meta['client_order_id']} was CANCELED while position open",
            )
        else:
            self._handle_safe_exit_failure_if_applicable(
                meta=meta,
                failure_reason=f"Safe exit order {meta['client_order_id']} was CANCELED while position open",
            )
            self._release_entry_reservation_on_terminal(meta, "ORDER_CANCELED")
        self._maybe_finalize_pending_expiry(meta["symbol"])
        self._flush_backend_mutations()

    def on_order_expired(self, event: OrderExpired) -> None:
        meta = self._resolve_order_metadata(event)
        event_id = str(getattr(event, "event_id", "") or f"exp:{meta['client_order_id']}")
        if event_id in self._processed_event_ids:
            return
        self._upsert_order_and_plan_event(
            meta=meta,
            status="EXPIRED",
            plan_event_type="ORDER_EXPIRED",
            event_id=event_id,
            reason="Order expired",
        )
        if _is_protective_stop_meta(meta):
            self._invalidate_stop_leg(
                meta=meta,
                reason=f"Protective stop order {meta['client_order_id']} EXPIRED while position open",
            )
        else:
            self._handle_safe_exit_failure_if_applicable(
                meta=meta,
                failure_reason=f"Safe exit order {meta['client_order_id']} EXPIRED while position open",
            )
            self._release_entry_reservation_on_terminal(meta, "ORDER_EXPIRED")
        self._maybe_finalize_pending_expiry(meta["symbol"])
        self._flush_backend_mutations()

    def on_order_rejected(self, event: OrderRejected) -> None:
        meta = self._resolve_order_metadata(event)
        reason = str(getattr(event, "reason", "ORDER_REJECTED") or "ORDER_REJECTED")
        event_id = str(getattr(event, "event_id", "") or f"rej:{meta['client_order_id']}")
        if event_id in self._processed_event_ids:
            return
        self._upsert_order_and_plan_event(
            meta=meta,
            status="REJECTED",
            plan_event_type="ORDER_REJECTED",
            event_id=event_id,
            reason=reason,
            alert_spec={
                "event_type": "BROKER_ORDER_REJECTED",
                "severity": "WARN",
                "title": f"Venue rejected order for {meta['symbol']} ({meta['client_order_id']})",
                "body": reason,
                "dedup_key": f"telemetry_order_rejected:{meta['client_order_id']}:{event_id}",
            },
        )
        if _is_protective_stop_meta(meta):
            self._invalidate_stop_leg(
                meta=meta,
                reason=f"Protective stop order {meta['client_order_id']} was REJECTED: {reason}",
            )
        else:
            self._handle_safe_exit_failure_if_applicable(
                meta=meta,
                failure_reason=f"Safe exit order {meta['client_order_id']} was REJECTED while position open: {reason}",
            )
            self._release_entry_reservation_on_terminal(meta, f"ORDER_REJECTED: {reason}")
        self._maybe_finalize_pending_expiry(meta["symbol"])
        self._flush_backend_mutations()

    def on_order_filled(self, event: OrderFilled) -> None:
        meta = self._resolve_order_metadata(event)
        cid = meta["client_order_id"]
        vid = str(getattr(event, "venue_order_id", "") or meta["venue_order_id"] or "")
        order_id = cid or vid
        trade_id = str(getattr(event, "trade_id", "") or "")
        event_id = str(getattr(event, "event_id", "") or f"fill:{cid}:{trade_id}")
        if event_id in self._processed_event_ids:
            return
        last_qty = _to_decimal(getattr(event, "last_qty", None), "0")
        last_px = _to_decimal(getattr(event, "last_px", None), "0")
        commission = _to_decimal(getattr(event, "commission", None), "0")
        liquidity_side = str(getattr(event, "liquidity_side", "TAKER") or "TAKER")
        if "." in liquidity_side:
            liquidity_side = liquidity_side.rsplit(".", 1)[-1]
        ts_event_ns = int(getattr(event, "ts_event", 0) or 0)
        total_qty = max(meta["quantity"], last_qty)

        # O(1) in-memory cumulative fill & VWAP calculation
        cid_fills = self._fills_by_cid.setdefault(cid, {})
        cid_fills[trade_id or event_id] = (last_qty, last_px)
        cum_qty = sum((q for q, _ in cid_fills.values()), Decimal("0"))
        cum_notional = sum((q * p for q, p in cid_fills.values()), Decimal("0"))
        avg_px = (cum_notional / cum_qty) if cum_qty > Decimal("0") else last_px
        order_status = (
            "FILLED"
            if (total_qty > Decimal("0") and cum_qty >= total_qty)
            else "PARTIALLY_FILLED"
        )
        if cid in self._order_meta:
            self._order_meta[cid]["filled_qty"] = cum_qty
            self._order_meta[cid]["status"] = order_status
        if total_qty > Decimal("0") and cum_qty >= total_qty and cid:
            self._startup_entry_reservations.pop(cid, None)

        if _is_protective_stop_meta(meta):
            sym = meta["symbol"]
            cur_pos_qty = self._oco_monitor.get_position_qty(sym)
            unapplied_red = self._oco_monitor._unapplied_pos_reduction_by_symbol.get(
                sym, Decimal("0")
            )
            already_reduced = min(last_qty, unapplied_red)
            if already_reduced > Decimal("0"):
                self._oco_monitor._unapplied_pos_reduction_by_symbol[sym] = (
                    unapplied_red - already_reduced
                )
            deduct_qty = max(Decimal("0"), last_qty - already_reduced)
            new_pos_qty = max(Decimal("0"), cur_pos_qty - deduct_qty)
            self._oco_monitor._positions_qty[sym] = new_pos_qty
            leaves_qty = max(Decimal("0"), total_qty - cum_qty)
            legs = self._active_stop_legs_by_symbol.setdefault(sym, {})
            if cid:
                legs.pop(cid, None)
            if vid:
                legs.pop(vid, None)
            if leaves_qty <= Decimal("0"):
                if cid:
                    self._replaced_stop_cids.discard(cid)
                if vid:
                    self._replaced_stop_cids.discard(vid)
            else:
                legs[cid or vid] = leaves_qty
            total_remaining_cov = sum(legs.values(), Decimal("0"))
            stop_px = (
                meta.get("trigger_price")
                or meta.get("limit_price")
                or meta.get("stop_loss_price")
                or Decimal("0")
            )
            if new_pos_qty == Decimal("0"):
                legs.clear()
                closed_cov = ProtectiveOrderCoverage(
                    symbol=sym,
                    broker=self._broker,
                    broker_order_id=vid or cid,
                    covered_qty=Decimal("0"),
                    stop_price=_to_decimal(stop_px, "0"),
                    status="CLOSED",
                )
                self._oco_monitor._coverage_by_symbol[sym] = closed_cov
                if vid or cid:
                    self._oco_monitor._protective_orders_by_id.pop(vid or cid, None)
                self._oco_monitor._persist_protective_order(closed_cov)
            else:
                self._oco_monitor.register_protective_order(
                    symbol=sym,
                    covered_qty=total_remaining_cov,
                    stop_price=stop_px,
                    broker_order_id=vid or cid,
                    status="ACTIVE",
                )
        elif bool(meta.get("is_reduce_only", False)) or str(
            meta.get("intent") or ""
        ).strip().upper() in {"EXIT", "FLATTEN", "CLOSE"}:
            sym = meta["symbol"]
            unapplied_red = self._oco_monitor._unapplied_pos_reduction_by_symbol.get(
                sym, Decimal("0")
            )
            if unapplied_red > Decimal("0"):
                self._oco_monitor._unapplied_pos_reduction_by_symbol[sym] = max(
                    Decimal("0"), unapplied_red - last_qty
                )

        session_trading_date = self._trading_date
        meta_snapshot = dict(meta)

        def _task(cur: Any) -> None:
            cur.execute(
                """
                INSERT INTO order_fills (
                    trade_id, event_id, order_id, client_order_id, venue_order_id,
                    strategy_id, broker, environment, symbol, side,
                    last_qty, last_px, commission, liquidity_side, ts_event_ns, payload
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s::jsonb
                )
                ON CONFLICT (client_order_id, trade_id) DO UPDATE SET
                    venue_order_id = EXCLUDED.venue_order_id,
                    last_qty = EXCLUDED.last_qty,
                    last_px = EXCLUDED.last_px,
                    commission = EXCLUDED.commission,
                    ts_event_ns = EXCLUDED.ts_event_ns
                """,
                (
                    trade_id,
                    event_id,
                    order_id,
                    cid,
                    vid,
                    meta_snapshot["strategy_id"],
                    self._broker,
                    self._environment,
                    meta_snapshot["symbol"],
                    meta_snapshot["side"],
                    last_qty,
                    last_px,
                    commission,
                    liquidity_side,
                    ts_event_ns,
                    json.dumps(
                        {
                            "event_id": event_id,
                            "trade_id": trade_id,
                            "client_order_id": cid,
                            "venue_order_id": vid,
                            "symbol": meta_snapshot["symbol"],
                            "side": meta_snapshot["side"],
                            "last_qty": str(last_qty),
                            "last_px": str(last_px),
                            "commission": str(commission),
                        }
                    ),
                ),
            )
            self._upsert_order_row_in_tx(
                cur=cur,
                meta=meta_snapshot,
                status=order_status,
                filled_qty=cum_qty,
                filled_avg_price=avg_px,
                raw_response={
                    "last_event": "OrderFilled",
                    "event_id": event_id,
                    "trade_id": trade_id,
                    "venue_order_id": vid,
                },
                session_trading_date=session_trading_date,
            )
            self._insert_plan_event_in_tx(
                cur=cur,
                symbol=meta_snapshot["symbol"],
                event_type="ORDER_FILLED",
                actor=f"nautilus:telemetry:{meta_snapshot['strategy_id'] or 'actor'}",
                reason=f"Order fill {last_qty} @ {last_px} (cum={cum_qty})",
                event_id=event_id,
                payload={
                    "event_id": event_id,
                    "trade_id": trade_id,
                    "client_order_id": cid,
                    "venue_order_id": vid,
                    "symbol": meta_snapshot["symbol"],
                    "last_qty": str(last_qty),
                    "last_px": str(last_px),
                    "cum_qty": str(cum_qty),
                    "status": order_status,
                },
                session_trading_date=session_trading_date,
            )
            self._upsert_trading_account_in_tx(cur)

        if self._persist_to_db and self._async_writer is not None:
            self._async_writer.submit(_task)
        self._processed_event_ids.add(event_id)
        self._maybe_finalize_pending_expiry(meta["symbol"])
        self._flush_backend_mutations()

    def on_position_opened(self, event: PositionOpened) -> None:
        self._handle_position_lifecycle_event(event, "POSITION_OPENED", is_closed=False)

    def on_position_changed(self, event: PositionChanged) -> None:
        self._handle_position_lifecycle_event(
            event, "POSITION_CHANGED", is_closed=False
        )

    def on_position_closed(self, event: PositionClosed) -> None:
        self._handle_position_lifecycle_event(event, "POSITION_CLOSED", is_closed=True)

    def _submit_position_upsert_task(
        self,
        *,
        symbol: str,
        strategy_id: str,
        pos_id: str,
        side: str,
        qty: Decimal,
        avg_px_open: Decimal,
        realized_pnl: Decimal,
        ts_bar: int,
        plan_event_type: str,
        event_id: str,
    ) -> None:
        session_trading_date = self._trading_date

        def _task(cur: Any) -> None:
            cost_basis = float(qty * avg_px_open)
            cur.execute(
                """
                INSERT INTO trading_positions (
                    broker, environment, symbol, strategy_id, side,
                    qty, avg_entry_price, cost_basis, current_price,
                    market_value, realized_pl, last_bar_ts, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, NOW()
                )
                ON CONFLICT (broker, symbol) DO UPDATE SET
                    environment = EXCLUDED.environment,
                    strategy_id = EXCLUDED.strategy_id,
                    side = EXCLUDED.side,
                    qty = EXCLUDED.qty,
                    avg_entry_price = EXCLUDED.avg_entry_price,
                    cost_basis = EXCLUDED.cost_basis,
                    current_price = EXCLUDED.current_price,
                    market_value = EXCLUDED.market_value,
                    realized_pl = EXCLUDED.realized_pl,
                    last_bar_ts = EXCLUDED.last_bar_ts,
                    updated_at = NOW()
                """,
                (
                    self._broker,
                    self._environment,
                    symbol,
                    strategy_id,
                    side,
                    float(qty),
                    float(avg_px_open),
                    cost_basis,
                    float(avg_px_open),
                    cost_basis,
                    float(realized_pnl),
                    ts_bar,
                ),
            )
            self._insert_plan_event_in_tx(
                cur=cur,
                symbol=symbol,
                event_type=plan_event_type,
                actor=f"nautilus:telemetry:{strategy_id or 'actor'}",
                reason=f"{plan_event_type} for {symbol} qty={qty} avg_px={avg_px_open}",
                event_id=event_id,
                payload={
                    "event_id": event_id,
                    "position_id": pos_id,
                    "symbol": symbol,
                    "side": side,
                    "qty": str(qty),
                    "avg_px_open": str(avg_px_open),
                    "realized_pnl": str(realized_pnl),
                },
                session_trading_date=session_trading_date,
            )
            self._upsert_trading_account_in_tx(cur)

        if self._persist_to_db and self._async_writer is not None:
            self._async_writer.submit(_task)
        self._processed_event_ids.add(event_id)

    def _handle_position_lifecycle_event(
        self,
        event: Any,
        plan_event_type: str,
        is_closed: bool,
    ) -> None:
        inst_id = str(getattr(event, "instrument_id", "") or "")
        symbol = _normalize_symbol(inst_id or getattr(event, "symbol", ""))
        strategy_id = str(getattr(event, "strategy_id", "") or "")
        pos_id = str(getattr(event, "position_id", "") or "")
        event_id = str(
            getattr(event, "event_id", "") or f"{plan_event_type}:{pos_id}:{symbol}"
        )
        if event_id in self._processed_event_ids:
            return
        side_raw = str(getattr(event, "entry", None) or getattr(event, "side", "LONG"))
        side = "short" if ("SHORT" in side_raw.upper() or "SELL" in side_raw.upper()) else "long"
        qty = (
            Decimal("0")
            if is_closed
            else abs(_to_decimal(getattr(event, "quantity", None), "0"))
        )
        avg_px_open = _to_decimal(getattr(event, "avg_px_open", None), "0")
        realized_pnl = _to_decimal(getattr(event, "realized_pnl", None), "0")
        ts_event = int(getattr(event, "ts_event", 0) or 0)
        self._processed_event_ids.add(
            f"pos_snap:{pos_id}:{qty}:{is_closed}:{ts_event}"
        )
        if plan_event_type == "POSITION_OPENED" and pos_id:
            self._opened_position_ids.add(pos_id)

        self._submit_position_upsert_task(
            symbol=symbol,
            strategy_id=strategy_id,
            pos_id=pos_id,
            side=side,
            qty=qty,
            avg_px_open=avg_px_open,
            realized_pnl=realized_pnl,
            ts_bar=ts_event,
            plan_event_type=plan_event_type,
            event_id=event_id,
        )
        self._oco_monitor.record_position_qty_sync(symbol, qty)
        if is_closed:
            self.end_safe_exit(symbol)
            self._oco_monitor._positions_qty[symbol] = Decimal("0")
            if not self._has_open_protective_stop_for_symbol(symbol):
                self._active_stop_legs_by_symbol.pop(symbol, None)
                self._oco_monitor.verify_position_coverage(
                    symbol=symbol, pos_qty=Decimal("0")
                )
            if self._plan_gate_backend is not None and hasattr(
                self._plan_gate_backend, "release_closed_position_capacity"
            ):
                self._plan_gate_backend.release_closed_position_capacity(symbol)
        elif self.is_safe_exit_active(symbol):
            self._oco_monitor._positions_qty[symbol] = qty
        elif self._enforce_oco_on_pos or self._oco_monitor.get_coverage(symbol) is not None:
            self._sync_protective_orders_from_cache(symbol)
            self._oco_monitor.verify_position_coverage(symbol=symbol, pos_qty=qty)
        else:
            self._oco_monitor._positions_qty[symbol] = qty
        self._flush_backend_mutations()

    def _sync_position_snapshot(self, pos: Any) -> None:
        inst_id = str(getattr(pos, "instrument_id", "") or "")
        symbol = _normalize_symbol(inst_id or getattr(pos, "symbol", ""))
        if not symbol:
            return
        is_closed = bool(getattr(pos, "is_closed", False))
        qty = (
            Decimal("0")
            if is_closed
            else abs(_to_decimal(getattr(pos, "quantity", None), "0"))
        )
        pos_id = str(getattr(pos, "id", "") or "")
        ts_last = int(getattr(pos, "ts_last", 0) or 0)
        synth_event_id = f"pos_snap:{pos_id}:{qty}:{is_closed}:{ts_last}"
        if synth_event_id in self._processed_event_ids:
            if not is_closed:
                self._oco_monitor.record_position_qty_sync(symbol, qty)
                self._oco_monitor._positions_qty[symbol] = qty
            return
        self._oco_monitor.record_position_qty_sync(symbol, qty)
        if is_closed:
            self.end_safe_exit(symbol)
            self._oco_monitor._positions_qty[symbol] = Decimal("0")
            if not self._has_open_protective_stop_for_symbol(symbol):
                self._active_stop_legs_by_symbol.pop(symbol, None)
                self._oco_monitor.verify_position_coverage(
                    symbol=symbol, pos_qty=Decimal("0")
                )
        plan_event_type = (
            "POSITION_CLOSED"
            if is_closed
            else (
                "POSITION_OPENED"
                if int(getattr(pos, "event_count", 1) or 1) <= 1
                else "POSITION_CHANGED"
            )
        )
        avg_px_open = _to_decimal(getattr(pos, "avg_px_open", None), "0")
        realized_pnl = _to_decimal(getattr(pos, "realized_pnl", None), "0")
        strategy_id = str(getattr(pos, "strategy_id", "") or "")
        side_raw = str(getattr(pos, "side", "LONG"))
        side = "short" if "SHORT" in side_raw.upper() else "long"

        if is_closed and pos_id and pos_id not in self._opened_position_ids:
            self._opened_position_ids.add(pos_id)
            open_qty = abs(_to_decimal(getattr(pos, "peak_qty", None), "0"))
            ts_opened = int(getattr(pos, "ts_opened", 0) or ts_last or 0)
            self._submit_position_upsert_task(
                symbol=symbol,
                strategy_id=strategy_id,
                pos_id=pos_id,
                side=side,
                qty=open_qty,
                avg_px_open=avg_px_open,
                realized_pnl=Decimal("0"),
                ts_bar=ts_opened,
                plan_event_type="POSITION_OPENED",
                event_id=f"pos_open_snap:{pos_id}:{ts_opened}",
            )
        elif plan_event_type == "POSITION_OPENED" and pos_id:
            self._opened_position_ids.add(pos_id)

        self._submit_position_upsert_task(
            symbol=symbol,
            strategy_id=strategy_id,
            pos_id=pos_id,
            side=side,
            qty=qty,
            avg_px_open=avg_px_open,
            realized_pnl=realized_pnl,
            ts_bar=ts_last,
            plan_event_type=plan_event_type,
            event_id=synth_event_id,
        )
        if not is_closed:
            if self.is_safe_exit_active(symbol):
                self._oco_monitor._positions_qty[symbol] = qty
            elif self._enforce_oco_on_pos or self._oco_monitor.get_coverage(symbol) is not None:
                self._sync_protective_orders_from_cache(symbol)
                self._oco_monitor.verify_position_coverage(symbol=symbol, pos_qty=qty)
            else:
                self._oco_monitor._positions_qty[symbol] = qty

    def _upsert_order_and_plan_event(
        self,
        meta: Dict[str, Any],
        status: str,
        plan_event_type: str,
        event_id: str,
        reason: str,
        alert_spec: Optional[Dict[str, Any]] = None,
    ) -> None:
        norm_st = str(status or "").strip().upper()
        if isinstance(meta, dict) and norm_st:
            meta["status"] = norm_st
            meta_cid = str(meta.get("client_order_id") or "")
            if meta_cid and meta_cid in self._order_meta:
                self._order_meta[meta_cid]["status"] = norm_st

        session_trading_date = self._trading_date
        meta_snapshot = dict(meta)

        def _task(cur: Any) -> None:
            self._upsert_order_row_in_tx(
                cur=cur,
                meta=meta_snapshot,
                status=status,
                filled_qty=None,
                filled_avg_price=None,
                raw_response={
                    "last_event": plan_event_type,
                    "event_id": event_id,
                    "reason": reason,
                    "venue_order_id": meta_snapshot.get("venue_order_id", ""),
                },
                session_trading_date=session_trading_date,
            )
            self._insert_plan_event_in_tx(
                cur=cur,
                symbol=meta_snapshot["symbol"],
                event_type=plan_event_type,
                actor=f"nautilus:telemetry:{meta_snapshot.get('strategy_id') or 'actor'}",
                reason=reason,
                event_id=event_id,
                payload={
                    "event_id": event_id,
                    "client_order_id": meta_snapshot["client_order_id"],
                    "venue_order_id": meta_snapshot.get("venue_order_id", ""),
                    "symbol": meta_snapshot["symbol"],
                    "side": meta_snapshot["side"],
                    "quantity": str(meta_snapshot["quantity"]),
                    "status": status,
                    "reason": reason,
                },
                session_trading_date=session_trading_date,
            )
            if alert_spec is not None:
                dedup_key = alert_spec["dedup_key"]
                cur.execute(
                    "SELECT 1 FROM alert_outbox WHERE dedup_key = %s LIMIT 1",
                    (dedup_key,),
                )
                if cur.fetchone() is None:
                    cur.execute(
                        """
                        INSERT INTO alert_outbox (
                            event_type, severity, source, title, body, payload, dedup_key, status
                        ) VALUES (
                            %s, %s, 'nautilus:telemetry_actor', %s, %s, %s::jsonb, %s, 'PENDING'
                        )
                        """,
                        (
                            alert_spec["event_type"],
                            alert_spec["severity"],
                            alert_spec["title"],
                            alert_spec["body"],
                            json.dumps(
                                {
                                    "event_id": event_id,
                                    "client_order_id": meta_snapshot["client_order_id"],
                                    "symbol": meta_snapshot["symbol"],
                                    "status": status,
                                    "reason": reason,
                                }
                            ),
                            dedup_key,
                        ),
                    )

        if self._persist_to_db and self._async_writer is not None:
            self._async_writer.submit(_task)
        self._processed_event_ids.add(event_id)

    _UNSET_SESSION_DATE: Any = object()

    def _upsert_order_row_in_tx(
        self,
        cur: Any,
        meta: Dict[str, Any],
        status: str,
        filled_qty: Optional[Decimal],
        filled_avg_price: Optional[Decimal],
        raw_response: Dict[str, Any],
        session_trading_date: Any = _UNSET_SESSION_DATE,
    ) -> None:
        cid = str(meta.get("client_order_id") or "")
        vid = str(meta.get("venue_order_id") or "")
        order_id = cid or vid
        if not order_id:
            return
        limit_px = meta.get("limit_price")
        limit_px_float = float(limit_px) if limit_px is not None else None
        qty_float = float(_to_decimal(meta.get("quantity"), "0"))
        filled_qty_float = float(filled_qty) if filled_qty is not None else 0.0
        filled_avg_float = (
            float(filled_avg_price) if filled_avg_price is not None else None
        )
        signal_ts_ns = self._positive_ns(meta.get("signal_ts_ns"))
        gate_admitted_ts_ns = self._positive_ns(meta.get("gate_admitted_ts_ns"))
        broker_ack_ts_ns = self._positive_ns(meta.get("broker_ack_ts_ns"))
        resolved_td = (
            self._trading_date
            if session_trading_date is self._UNSET_SESSION_DATE
            else session_trading_date
        )
        if resolved_td is not None and "trading_date" not in raw_response:
            raw_response = {**raw_response, "trading_date": resolved_td.isoformat()}
        cur.execute(
            """
            INSERT INTO orders (
                order_id, client_order_id, strategy_id, broker, environment,
                symbol, action, qty, order_type, limit_price, status,
                filled_qty, filled_avg_price,
                signal_ts_ns, gate_admitted_ts_ns, broker_ack_ts_ns,
                raw_response, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s::jsonb, NOW()
            )
            ON CONFLICT (order_id) DO UPDATE SET
                client_order_id = CASE
                    WHEN EXCLUDED.client_order_id <> '' THEN EXCLUDED.client_order_id
                    ELSE orders.client_order_id
                END,
                strategy_id = CASE
                    WHEN EXCLUDED.strategy_id <> '' THEN EXCLUDED.strategy_id
                    ELSE orders.strategy_id
                END,
                qty = CASE
                    WHEN EXCLUDED.qty > 0 THEN EXCLUDED.qty
                    ELSE orders.qty
                END,
                order_type = CASE
                    WHEN EXCLUDED.order_type <> 'MARKET' THEN EXCLUDED.order_type
                    ELSE orders.order_type
                END,
                limit_price = COALESCE(EXCLUDED.limit_price, orders.limit_price),
                status = CASE
                    WHEN orders.status = 'FILLED' AND EXCLUDED.status IN ('SUBMITTED', 'ACCEPTED')
                    THEN orders.status
                    ELSE EXCLUDED.status
                END,
                filled_qty = GREATEST(orders.filled_qty, EXCLUDED.filled_qty),
                filled_avg_price = COALESCE(EXCLUDED.filled_avg_price, orders.filled_avg_price),
                signal_ts_ns = COALESCE(orders.signal_ts_ns, EXCLUDED.signal_ts_ns),
                gate_admitted_ts_ns = COALESCE(orders.gate_admitted_ts_ns, EXCLUDED.gate_admitted_ts_ns),
                broker_ack_ts_ns = COALESCE(orders.broker_ack_ts_ns, EXCLUDED.broker_ack_ts_ns),
                raw_response = EXCLUDED.raw_response,
                updated_at = NOW()
            """,
            (
                order_id,
                cid,
                str(meta.get("strategy_id") or ""),
                self._broker,
                self._environment,
                str(meta.get("symbol") or ""),
                str(meta.get("side") or "BUY"),
                qty_float,
                str(meta.get("order_type") or "MARKET"),
                limit_px_float,
                status,
                filled_qty_float,
                filled_avg_float,
                signal_ts_ns,
                gate_admitted_ts_ns,
                broker_ack_ts_ns,
                json.dumps(raw_response),
            ),
        )

    def _insert_plan_event_in_tx(
        self,
        cur: Any,
        symbol: str,
        event_type: str,
        actor: str,
        reason: str,
        event_id: str,
        payload: Dict[str, Any],
        session_trading_date: Any = _UNSET_SESSION_DATE,
    ) -> None:
        if not symbol:
            return
        resolved_td = (
            self._trading_date
            if session_trading_date is self._UNSET_SESSION_DATE
            else session_trading_date
        )
        cache_key = (resolved_td, self._market, symbol)
        cached_pair = self._plan_item_cache.get(cache_key)
        if cached_pair is not None:
            plan_id, item_id = cached_pair
        else:
            if resolved_td is not None:
                cur.execute(
                    """
                    SELECT p.plan_id, i.item_id
                    FROM trading_plans p
                    LEFT JOIN trading_plan_items i
                      ON i.plan_id = p.plan_id AND i.symbol = %s
                    WHERE p.trading_date = %s
                      AND p.market = %s
                    ORDER BY (p.status = 'LOCKED') DESC, p.locked_at DESC NULLS LAST, p.created_at DESC
                    LIMIT 1
                    """,
                    (symbol, resolved_td, self._market),
                )
            else:
                cur.execute(
                    """
                    SELECT p.plan_id, i.item_id
                    FROM trading_plans p
                    LEFT JOIN trading_plan_items i
                      ON i.plan_id = p.plan_id AND i.symbol = %s
                    WHERE p.market = %s
                    ORDER BY (i.item_id IS NOT NULL) DESC, (p.status = 'LOCKED') DESC,
                             p.trading_date DESC, p.locked_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (symbol, self._market),
                )
            plan_row = cur.fetchone()
            if not plan_row or not plan_row.get("plan_id"):
                return
            plan_id = plan_row["plan_id"]
            item_id = plan_row.get("item_id")
            self._plan_item_cache[cache_key] = (plan_id, item_id)

        dedup_key = f"{plan_id}:{event_type}:{event_id}" if event_id else ""
        if dedup_key and dedup_key in self._persisted_plan_event_ids:
            return

        cur.execute(
            """
            INSERT INTO trading_plan_events (
                plan_id, item_id, event_type, actor, reason, payload
            ) VALUES (
                %s, %s, %s, %s, %s, %s::jsonb
            )
            """,
            (
                plan_id,
                item_id,
                event_type,
                actor,
                reason,
                json.dumps(payload),
            ),
        )
        if dedup_key:
            self._persisted_plan_event_ids.add(dedup_key)
