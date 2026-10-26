"""Nautilus v2 PostgreSQL telemetry actor and protective OCO coverage monitor.

Subscribes to Nautilus order and position lifecycle events (`OrderSubmitted`,
`OrderAccepted`, `OrderDenied`, `OrderCanceled`, `OrderRejected`, `OrderFilled`,
`PositionOpened`, `PositionChanged`, `PositionClosed`), idempotently persists
execution telemetry into PostgreSQL (`orders`, `order_fills`, `trading_plan_events`,
`alert_outbox`, `trading_positions`), and enforces fail-closed protective OCO
coverage via `ProtectiveOcoMonitor`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
import json
import logging
import queue
import threading
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from uuid import UUID

import psycopg2
from psycopg2.extras import RealDictCursor

from evolution_engine.db.dsn import resolve_database_dsn

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
        self._on_db_error_callback = on_db_error_callback
        self._queue: "queue.Queue[Optional[Tuple[Callable[[Any], None], Optional[threading.Event]]]]" = queue.Queue()
        self._conn: Optional[Any] = None
        self._thread = threading.Thread(
            target=self._run_loop,
            name="nautilus-telemetry-async-db-writer",
            daemon=True,
        )
        self._thread.start()

    def submit(self, task_fn: Callable[[Any], None], eager_flush: bool = True) -> None:
        done = threading.Event() if eager_flush else None
        self._queue.put((task_fn, done))
        if done is not None:
            done.wait(timeout=2.0)

    def flush(self, timeout_secs: float = 5.0) -> None:
        done = threading.Event()
        self._queue.put((lambda _cur: None, done))
        done.wait(timeout=max(0.05, float(timeout_secs)))

    def _ensure_conn(self) -> Any:
        if self._conn is None or getattr(self._conn, "closed", 1) != 0:
            self._conn = psycopg2.connect(
                self._dsn,
                cursor_factory=RealDictCursor,
                connect_timeout=self._connect_timeout_secs,
            )
        return self._conn

    def _run_loop(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                break
            batch = [item]
            while True:
                try:
                    nxt = self._queue.get_nowait()
                    if nxt is None:
                        break
                    batch.append(nxt)
                except queue.Empty:
                    break

            try:
                conn = self._ensure_conn()
                with conn:
                    with conn.cursor() as cur:
                        for fn, _ in batch:
                            fn(cur)
            except Exception as exc:
                logger.error("Telemetry async write-behind batch failed: %s", exc)
                if self._on_db_error_callback is not None:
                    try:
                        self._on_db_error_callback(
                            "POSTGRES_TELEMETRY",
                            f"TELEMETRY_DB_WRITE_FAILED: {exc}",
                        )
                    except Exception:
                        pass
                try:
                    if self._conn is not None:
                        self._conn.close()
                except Exception:
                    pass
                self._conn = None
            finally:
                for _, done_ev in batch:
                    if done_ev is not None:
                        done_ev.set()
                    self._queue.task_done()
from nautilus_trader.common import DataActor, DataActorConfig
from nautilus_trader.model import (
    ActorId,
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
        connect_timeout_secs: int = 3,
        on_entry_freeze_callback: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        self._dsn = resolve_database_dsn(dsn)
        self._broker = str(broker or "alpaca").strip().lower()
        self._connect_timeout_secs = max(1, int(connect_timeout_secs))
        self._on_entry_freeze_callback = on_entry_freeze_callback
        self._positions_qty: Dict[str, Decimal] = {}
        self._coverage_by_symbol: Dict[str, ProtectiveOrderCoverage] = {}
        self._protective_orders_by_id: Dict[str, ProtectiveOrderCoverage] = {}
        self._async_writer = _AsyncTelemetryDbWorker(
            dsn=self._dsn,
            connect_timeout_secs=self._connect_timeout_secs,
            on_db_error_callback=self._on_entry_freeze_callback,
        )

    @property
    def dsn(self) -> str:
        return self._dsn

    @property
    def broker(self) -> str:
        return self._broker

    def get_position_qty(self, symbol: str) -> Decimal:
        norm = _normalize_symbol(symbol)
        return self._positions_qty.get(norm, Decimal("0"))

    def get_coverage(self, symbol: str) -> Optional[ProtectiveOrderCoverage]:
        norm = _normalize_symbol(symbol)
        return self._coverage_by_symbol.get(norm)

    def _connect(self):
        return psycopg2.connect(
            self._dsn,
            cursor_factory=RealDictCursor,
            connect_timeout=self._connect_timeout_secs,
        )

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

        if existing is None:
            existing = self._coverage_by_symbol.get(target_sym)

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
        return None

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
        self._positions_qty[norm_sym] = abs_pos_qty

        coverage = self._coverage_by_symbol.get(norm_sym)
        if coverage is None:
            coverage = self._load_coverage_from_db(norm_sym)
            if coverage is not None:
                self._coverage_by_symbol[norm_sym] = coverage

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
            if (coverage is not None and coverage.status == "ACTIVE")
            else Decimal("0")
        )
        stop_price = coverage.stop_price if coverage is not None else Decimal("0")
        eff_item_id = item_id or (coverage.item_id if coverage is not None else None)

        if (
            coverage is not None
            and coverage.status == "ACTIVE"
            and covered_qty >= abs_pos_qty
            and stop_price > Decimal("0")
        ):
            self._persist_protective_order(coverage)
            return OcoCheckResult(
                symbol=norm_sym,
                pos_qty=abs_pos_qty,
                covered_qty=covered_qty,
                stop_price=stop_price,
                status="ACTIVE",
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
        if self._on_entry_freeze_callback is not None:
            self._on_entry_freeze_callback(norm_sym, mismatch_reason)

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

    def _load_coverage_from_db(self, symbol: str) -> Optional[ProtectiveOrderCoverage]:
        try:
            conn = self._connect()
        except Exception:
            return None
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT broker, symbol, item_id, broker_order_id,
                               qty, stop_price, take_profit_price, status
                        FROM protective_orders
                        WHERE broker = %s AND symbol = %s
                        """,
                        (self._broker, symbol),
                    )
                    row = cur.fetchone()
                    if not row:
                        return None
                    raw_item_id = row.get("item_id")
                    parsed_item_id = (
                        UUID(str(raw_item_id)) if raw_item_id is not None else None
                    )
                    return ProtectiveOrderCoverage(
                        symbol=str(row["symbol"]).strip().upper(),
                        broker=str(row["broker"]).strip().lower(),
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
        except Exception:
            return None
        finally:
            conn.close()

    def _persist_protective_order(self, coverage: ProtectiveOrderCoverage) -> None:
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
            ORDER BY p.trading_date DESC, p.locked_at DESC NULLS LAST
            LIMIT 1
            """,
            (symbol,),
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
        enforce_protective_oco_on_position_events: bool = False,
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
        enforce_protective_oco_on_position_events: bool = False,
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


class PostgresTelemetryActor(DataActor):
    """Nautilus v2 `DataActor` that synchronizes orders, fills, positions, plan events,
    and protective OCO status into PostgreSQL via `resolve_database_dsn()`.
    """

    def __new__(
        cls,
        config: Optional[PostgresTelemetryActorConfig] = None,
        oco_monitor: Optional[ProtectiveOcoMonitor] = None,
    ) -> "PostgresTelemetryActor":
        effective_config = config or PostgresTelemetryActorConfig()
        return super().__new__(cls, effective_config)

    def __init__(
        self,
        config: Optional[PostgresTelemetryActorConfig] = None,
        oco_monitor: Optional[ProtectiveOcoMonitor] = None,
    ) -> None:
        effective_config = config or PostgresTelemetryActorConfig()
        super().__init__(effective_config)
        self._telemetry_config = effective_config
        self._dsn = resolve_database_dsn(effective_config.dsn)
        self._broker = effective_config.broker
        self._environment = effective_config.environment
        self._market = effective_config.market
        self._trading_date = effective_config.trading_date
        self._connect_timeout_secs = effective_config.connect_timeout_secs
        self._enforce_oco_on_pos = (
            effective_config.enforce_protective_oco_on_position_events
        )
        self._oco_monitor = oco_monitor or ProtectiveOcoMonitor(
            dsn=self._dsn,
            broker=self._broker,
            connect_timeout_secs=self._connect_timeout_secs,
        )
        self._processed_event_ids: Set[str] = set()
        self._order_meta: Dict[str, Dict[str, Any]] = {}
        self._fills_by_cid: Dict[str, Dict[str, Tuple[Decimal, Decimal]]] = {}
        self._msgbus_subscriptions: List[Tuple[str, Any]] = []
        self._async_writer = self._oco_monitor._async_writer
        self.ensure_schema()

    @property
    def dsn(self) -> str:
        return self._dsn

    @property
    def oco_monitor(self) -> ProtectiveOcoMonitor:
        return self._oco_monitor

    def _connect(self):
        return psycopg2.connect(
            self._dsn,
            cursor_factory=RealDictCursor,
            connect_timeout=self._connect_timeout_secs,
        )

    def ensure_schema(self) -> None:
        """Idempotently ensures `order_fills` exists in PostgreSQL."""
        conn = self._connect()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS order_fills (
                            fill_id           BIGSERIAL PRIMARY KEY,
                            trade_id          VARCHAR(128) NOT NULL,
                            event_id          VARCHAR(128) NOT NULL,
                            order_id          VARCHAR(128) NOT NULL,
                            client_order_id   VARCHAR(128) NOT NULL,
                            venue_order_id    VARCHAR(128) NOT NULL DEFAULT '',
                            strategy_id       VARCHAR(128) NOT NULL DEFAULT '',
                            broker            VARCHAR(32) NOT NULL DEFAULT 'alpaca',
                            environment       VARCHAR(32) NOT NULL DEFAULT 'PAPER',
                            symbol            VARCHAR(32) NOT NULL,
                            side              VARCHAR(16) NOT NULL,
                            last_qty          NUMERIC(20, 6) NOT NULL,
                            last_px           NUMERIC(20, 6) NOT NULL,
                            commission        NUMERIC(20, 6) NOT NULL DEFAULT 0,
                            liquidity_side    VARCHAR(32) NOT NULL DEFAULT 'TAKER',
                            ts_event_ns       BIGINT NOT NULL DEFAULT 0,
                            payload           JSONB NOT NULL DEFAULT '{}'::jsonb,
                            created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            UNIQUE (client_order_id, trade_id)
                        )
                        """
                    )
        finally:
            conn.close()

    def subscribe_to_message_bus(self, msgbus: Any) -> None:
        """Subscribes to `events.order.*` and `events.position.*` on a Nautilus `MessageBus`."""
        order_handler = self.on_order_event
        position_handler = self.on_position_event
        msgbus.subscribe("events.order.*", order_handler)
        msgbus.subscribe("events.position.*", position_handler)
        self._msgbus_subscriptions.append(("events.order.*", order_handler))
        self._msgbus_subscriptions.append(("events.position.*", position_handler))

    def on_start(self) -> None:
        self.sync_from_cache()

    def on_stop(self) -> None:
        self.sync_from_cache()

    def on_bar(self, bar: Any) -> None:
        self.sync_from_cache()

    def on_quote(self, quote: Any) -> None:
        self.sync_from_cache()

    def on_time_event(self, event: Any) -> None:
        self.sync_from_cache()

    def sync_from_cache(self) -> None:
        """Synchronizes any unprocessed order and position events from the Nautilus `Cache`."""
        try:
            cache = self.cache
        except Exception:
            return
        if cache is None:
            return

        try:
            orders = cache.orders()
        except Exception:
            orders = []

        for order in orders or []:
            self._cache_order_metadata(order)
            events_Attr = getattr(order, "events", None)
            events = events_Attr() if callable(events_Attr) else (events_Attr or [])
            for ev in events:
                self.on_order_event(ev)

        try:
            positions = cache.positions()
        except Exception:
            positions = []

        for pos in positions or []:
            self._sync_position_snapshot(pos)

    def _cache_order_metadata(self, order: Any) -> Dict[str, Any]:
        cid = str(getattr(order, "client_order_id", "") or "")
        if not cid:
            return {}
        inst_id = str(getattr(order, "instrument_id", "") or "")
        symbol = _normalize_symbol(inst_id)
        side = _normalize_side_str(getattr(order, "side", "BUY"))
        order_type = _normalize_order_type_str(getattr(order, "order_type", "MARKET"))
        qty = _to_decimal(getattr(order, "quantity", None), "0")
        price_attr = getattr(order, "price", None)
        trigger_attr = getattr(order, "trigger_price", None)
        limit_px = _to_decimal(price_attr, "0") if price_attr is not None else None
        trigger_px = (
            _to_decimal(trigger_attr, "0") if trigger_attr is not None else None
        )
        is_reduce_only = bool(getattr(order, "is_reduce_only", False))
        parent_order_id = str(getattr(order, "parent_order_id", "") or "")
        meta = {
            "client_order_id": cid,
            "venue_order_id": str(getattr(order, "venue_order_id", "") or ""),
            "strategy_id": str(getattr(order, "strategy_id", "") or ""),
            "instrument_id": inst_id,
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "quantity": qty,
            "limit_price": limit_px,
            "trigger_price": trigger_px,
            "is_reduce_only": is_reduce_only,
            "parent_order_id": parent_order_id,
        }
        self._order_meta[cid] = meta
        return meta

    def _resolve_order_metadata(self, event: Any) -> Dict[str, Any]:
        cid = str(getattr(event, "client_order_id", "") or "")
        cached_meta = dict(self._order_meta.get(cid, {}))
        try:
            cache = self.cache
            if cache is not None and cid:
                from nautilus_trader.model import ClientOrderId

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
        merged = {
            "client_order_id": cid,
            "venue_order_id": vid,
            "strategy_id": strategy_id,
            "instrument_id": inst_id,
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "quantity": qty,
            "limit_price": limit_px,
            "trigger_price": trigger_px,
            "is_reduce_only": is_reduce_only,
            "parent_order_id": parent_order_id,
        }
        if cid:
            self._order_meta[cid] = merged
        return merged

    def on_event(self, event: Any) -> None:
        """Generic dispatcher for order and position events."""
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

    def on_order_initialized(self, event: OrderInitialized) -> None:
        self._resolve_order_metadata(event)

    def on_order_submitted(self, event: OrderSubmitted) -> None:
        meta = self._resolve_order_metadata(event)
        event_id = str(getattr(event, "event_id", "") or f"sub:{meta['client_order_id']}")
        self._upsert_order_and_plan_event(
            meta=meta,
            status="SUBMITTED",
            plan_event_type="ORDER_SUBMITTED",
            event_id=event_id,
            reason="Order submitted to execution client",
        )

    def on_order_accepted(self, event: OrderAccepted) -> None:
        meta = self._resolve_order_metadata(event)
        event_id = str(getattr(event, "event_id", "") or f"acc:{meta['client_order_id']}")
        self._upsert_order_and_plan_event(
            meta=meta,
            status="ACCEPTED",
            plan_event_type="ORDER_ACCEPTED",
            event_id=event_id,
            reason="Order accepted by venue",
        )
        if _is_protective_stop_order_type(meta["order_type"]) and (
            meta.get("trigger_price") or meta.get("limit_price")
        ):
            stop_px = meta.get("trigger_price") or meta.get("limit_price")
            self._oco_monitor.register_protective_order(
                symbol=meta["symbol"],
                covered_qty=meta["quantity"],
                stop_price=stop_px,
                broker_order_id=meta["venue_order_id"] or meta["client_order_id"],
                status="ACTIVE",
            )

    def on_order_denied(self, event: OrderDenied) -> None:
        meta = self._resolve_order_metadata(event)
        reason = str(getattr(event, "reason", "ORDER_DENIED") or "ORDER_DENIED")
        event_id = str(getattr(event, "event_id", "") or f"den:{meta['client_order_id']}")
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

    def on_order_canceled(self, event: OrderCanceled) -> None:
        meta = self._resolve_order_metadata(event)
        event_id = str(getattr(event, "event_id", "") or f"can:{meta['client_order_id']}")
        self._upsert_order_and_plan_event(
            meta=meta,
            status="CANCELED",
            plan_event_type="ORDER_CANCELED",
            event_id=event_id,
            reason="Order canceled",
        )
        if _is_protective_stop_order_type(meta["order_type"]):
            self._oco_monitor.invalidate_protective_order(
                symbol=meta["symbol"],
                broker_order_id=meta["venue_order_id"] or meta["client_order_id"],
                reason=f"Protective stop order {meta['client_order_id']} was CANCELED while position open",
            )

    def on_order_rejected(self, event: OrderRejected) -> None:
        meta = self._resolve_order_metadata(event)
        reason = str(getattr(event, "reason", "ORDER_REJECTED") or "ORDER_REJECTED")
        event_id = str(getattr(event, "event_id", "") or f"rej:{meta['client_order_id']}")
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
        if _is_protective_stop_order_type(meta["order_type"]):
            self._oco_monitor.invalidate_protective_order(
                symbol=meta["symbol"],
                broker_order_id=meta["venue_order_id"] or meta["client_order_id"],
                reason=f"Protective stop order {meta['client_order_id']} was REJECTED: {reason}",
            )

    def on_order_filled(self, event: OrderFilled) -> None:
        meta = self._resolve_order_metadata(event)
        cid = meta["client_order_id"]
        vid = str(getattr(event, "venue_order_id", "") or meta["venue_order_id"] or "")
        order_id = cid or vid
        trade_id = str(getattr(event, "trade_id", "") or "")
        event_id = str(getattr(event, "event_id", "") or f"fill:{cid}:{trade_id}")
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
                    meta["strategy_id"],
                    self._broker,
                    self._environment,
                    meta["symbol"],
                    meta["side"],
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
                            "symbol": meta["symbol"],
                            "side": meta["side"],
                            "last_qty": str(last_qty),
                            "last_px": str(last_px),
                            "commission": str(commission),
                        }
                    ),
                ),
            )
            self._upsert_order_row_in_tx(
                cur=cur,
                meta=meta,
                status=order_status,
                filled_qty=cum_qty,
                filled_avg_price=avg_px,
                raw_response={
                    "last_event": "OrderFilled",
                    "event_id": event_id,
                    "trade_id": trade_id,
                    "venue_order_id": vid,
                },
            )
            self._insert_plan_event_in_tx(
                cur=cur,
                symbol=meta["symbol"],
                event_type="ORDER_FILLED",
                actor=f"nautilus:telemetry:{meta['strategy_id'] or 'actor'}",
                reason=f"Order fill {last_qty} @ {last_px} (cum={cum_qty})",
                event_id=event_id,
                payload={
                    "event_id": event_id,
                    "trade_id": trade_id,
                    "client_order_id": cid,
                    "venue_order_id": vid,
                    "symbol": meta["symbol"],
                    "last_qty": str(last_qty),
                    "last_px": str(last_px),
                    "cum_qty": str(cum_qty),
                    "status": order_status,
                },
            )

        self._async_writer.submit(_task)
        self._processed_event_ids.add(event_id)

    def on_position_opened(self, event: PositionOpened) -> None:
        self._handle_position_lifecycle_event(event, "POSITION_OPENED", is_closed=False)

    def on_position_changed(self, event: PositionChanged) -> None:
        self._handle_position_lifecycle_event(
            event, "POSITION_CHANGED", is_closed=False
        )

    def on_position_closed(self, event: PositionClosed) -> None:
        self._handle_position_lifecycle_event(event, "POSITION_CLOSED", is_closed=True)

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
                    ts_event,
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
            )

        self._async_writer.submit(_task)
        self._processed_event_ids.add(event_id)
        if is_closed:
            self._oco_monitor.verify_position_coverage(symbol=symbol, pos_qty=Decimal("0"))
        elif self._enforce_oco_on_pos or self._oco_monitor.get_coverage(symbol) is not None:
            self._oco_monitor.verify_position_coverage(symbol=symbol, pos_qty=qty)
        else:
            self._oco_monitor._positions_qty[symbol] = qty

    def _sync_position_snapshot(self, pos: Any) -> None:
        inst_id = str(getattr(pos, "instrument_id", "") or "")
        symbol = _normalize_symbol(inst_id)
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
            return
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
                    ts_last,
                ),
            )
            self._insert_plan_event_in_tx(
                cur=cur,
                symbol=symbol,
                event_type=plan_event_type,
                actor=f"nautilus:telemetry:{strategy_id or 'actor'}",
                reason=f"{plan_event_type} for {symbol} qty={qty} avg_px={avg_px_open}",
                event_id=synth_event_id,
                payload={
                    "event_id": synth_event_id,
                    "position_id": pos_id,
                    "symbol": symbol,
                    "side": side,
                    "qty": str(qty),
                    "avg_px_open": str(avg_px_open),
                    "realized_pnl": str(realized_pnl),
                },
            )

        self._async_writer.submit(_task)
        self._processed_event_ids.add(synth_event_id)

    def _upsert_order_and_plan_event(
        self,
        meta: Dict[str, Any],
        status: str,
        plan_event_type: str,
        event_id: str,
        reason: str,
        alert_spec: Optional[Dict[str, Any]] = None,
    ) -> None:
        def _task(cur: Any) -> None:
            self._upsert_order_row_in_tx(
                cur=cur,
                meta=meta,
                status=status,
                filled_qty=None,
                filled_avg_price=None,
                raw_response={
                    "last_event": plan_event_type,
                    "event_id": event_id,
                    "reason": reason,
                    "venue_order_id": meta.get("venue_order_id", ""),
                },
            )
            self._insert_plan_event_in_tx(
                cur=cur,
                symbol=meta["symbol"],
                event_type=plan_event_type,
                actor=f"nautilus:telemetry:{meta.get('strategy_id') or 'actor'}",
                reason=reason,
                event_id=event_id,
                payload={
                    "event_id": event_id,
                    "client_order_id": meta["client_order_id"],
                    "venue_order_id": meta.get("venue_order_id", ""),
                    "symbol": meta["symbol"],
                    "side": meta["side"],
                    "quantity": str(meta["quantity"]),
                    "status": status,
                    "reason": reason,
                },
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
                                    "client_order_id": meta["client_order_id"],
                                    "symbol": meta["symbol"],
                                    "status": status,
                                    "reason": reason,
                                }
                            ),
                            dedup_key,
                        ),
                    )

        self._async_writer.submit(_task)
        self._processed_event_ids.add(event_id)

    def _upsert_order_row_in_tx(
        self,
        cur: Any,
        meta: Dict[str, Any],
        status: str,
        filled_qty: Optional[Decimal],
        filled_avg_price: Optional[Decimal],
        raw_response: Dict[str, Any],
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
        cur.execute(
            """
            INSERT INTO orders (
                order_id, client_order_id, strategy_id, broker, environment,
                symbol, action, qty, order_type, limit_price, status,
                filled_qty, filled_avg_price, raw_response, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s::jsonb, NOW()
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
    ) -> None:
        if not symbol:
            return
        if self._trading_date is not None:
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
                (symbol, self._trading_date, self._market),
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
        cur.execute(
            """
            SELECT 1
            FROM trading_plan_events
            WHERE plan_id = %s
              AND event_type = %s
              AND payload->>'event_id' = %s
            LIMIT 1
            """,
            (plan_id, event_type, event_id),
        )
        if cur.fetchone() is not None:
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
