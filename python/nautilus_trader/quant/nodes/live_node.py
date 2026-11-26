"""Multi-venue `nautilus-live-node` assembler and PG `LISTEN/NOTIFY` plan loader.

Implements SPEC-0007 live node assembly and rollover:
- Loads `LOCKED` `PlanItemContract` items from PostgreSQL via `PlanContractRepository`
  (or awaits `LISTEN nautilus_plan_locked` via event-driven reactive wakeup, zero blind polling `D6.4`).
- Resolves live Alpaca equity metadata via `load_alpaca_equity_instruments` + PG `instrument_metadata_cache`
  before `run_async()` and off-thread prior to `call_soon_threadsafe` rollover.
- Assembles a production `LiveNode` with `DataInfraDataClientFactory`, `AlpacaExecutionClientFactory`,
  Rust `PreTradeRiskGate` (`PostgresPlanGateBackend.as_pre_trade_risk_gate()`),
  `CollaredTwapExecAlgorithm`, `PostgresTelemetryActor`, and `Strategy(StrategyConfig)` instances.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import math
import os
import select
import threading
import time
import urllib.request
import uuid
import weakref
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from evolution_engine.config_center.schema import SystemConfig
from evolution_engine.db.dsn import resolve_database_dsn
from nautilus_trader.quant.contracts.models import (
    MarketableLimitCollar,
    PlanItemContract,
    PlanSide,
    _to_decimal,
    rule_612_tick_size,
)
from nautilus_trader.quant.contracts.repository import PlanContractRepository
from nautilus_trader.quant.execution_algorithms.collared_twap import (
    COLLARED_TWAP_EXEC_ALGORITHM_ID,
    CollaredTwapExecAlgorithm,
    CollaredTwapExecAlgorithmConfig,
)
from nautilus_trader.quant.execution_algorithms.friction import (
    IbkrProFeeSchedule,
)
from nautilus_trader.quant.governance.plan_gate_backend import (
    PostgresPlanGateBackend,
)
from nautilus_trader.quant.governance.telemetry_actor import (
    PostgresTelemetryActor,
    PostgresTelemetryActorConfig,
    ProtectiveOcoMonitor,
)
from nautilus_trader.quant.nodes.backtest_runner import (
    _create_default_equity,
)
from nautilus_trader.quant.strategies.base import (
    BaseArchetypeStrategy,
    required_warmup_bars,
)
from nautilus_trader.quant.strategies.registry import (
    instantiate_strategy_from_contract,
)
from nautilus_trader.adapters.alpaca import (
    AlpacaExecClientConfig,
    AlpacaExecutionClientFactory,
    load_alpaca_equity_instruments,
)
from nautilus_trader.adapters.datainfra import (
    DataInfraCatalog,
    DataInfraConfig,
    DataInfraDataClientFactory,
)
from nautilus_trader.common import CacheConfig, Environment
from nautilus_trader.live import (
    LiveDataEngineConfig,
    LiveExecutionEngineConfig,
    LiveNode,
    LiveRiskEngineConfig,
)
from nautilus_trader.model import (
    Bar,
    Currency,
    Equity,
    InstrumentId,
    Price,
    Quantity,
    TraderId,
)

PLAN_LOCKED_NOTIFY_CHANNEL: str = "nautilus_plan_locked"
logger = logging.getLogger(__name__)


class EmptyPlanStartupError(RuntimeError, ValueError):
    """Raised when no LOCKED TradingPlan exists in PostgreSQL at startup."""


class PlanWaitTimeoutError(EmptyPlanStartupError, TimeoutError):
    """Raised when waiting on LISTEN nautilus_plan_locked times out without a LOCKED plan."""

TRADING_MODE_ENV_VARS: Tuple[str, ...] = (
    "TRADING_MODE",
    "TRADING_ENVIRONMENT",
    "ALPACA_ENVIRONMENT",
)


def _normalize_mode_token(raw_mode: str, source_name: str = "TRADING_MODE") -> str:
    norm = str(raw_mode or "").strip().upper()
    if norm in ("PAPER", "LIVE"):
        return norm
    raise ValueError(
        f"Invalid trading mode {raw_mode!r} in {source_name}: must be 'PAPER' or 'LIVE' (fail-closed)"
    )


def resolve_trading_mode(explicit_mode: Optional[str] = None) -> str:
    """Resolves the canonical trading mode (`"PAPER"` or `"LIVE"`) from `TRADING_MODE`
    (with legacy fallback to `TRADING_ENVIRONMENT` / `ALPACA_ENVIRONMENT`), failing closed
    with `ValueError` if any set mode environment variables conflict or contain an invalid token.
    """
    configured: Dict[str, str] = {}
    for env_key in TRADING_MODE_ENV_VARS:
        raw_val = os.environ.get(env_key, "").strip()
        if not raw_val:
            continue
        configured[env_key] = _normalize_mode_token(raw_val, source_name=env_key)

    distinct_modes = set(configured.values())
    if len(distinct_modes) > 1:
        conflict_desc = ", ".join(f"{k}={v}" for k, v in configured.items())
        raise ValueError(
            f"Conflicting trading mode environment variables detected ({conflict_desc}); "
            "unify on TRADING_MODE=PAPER|LIVE (fail-closed)"
        )

    if explicit_mode is not None and str(explicit_mode).strip() != "":
        norm_explicit = _normalize_mode_token(str(explicit_mode), source_name="explicit_mode")
        if configured and any(v != norm_explicit for v in configured.values()):
            conflict_desc = ", ".join(f"{k}={v}" for k, v in configured.items())
            raise ValueError(
                f"Explicit trading_mode={norm_explicit} conflicts with environment ({conflict_desc}) (fail-closed)"
            )
        return norm_explicit

    if configured:
        for env_key in TRADING_MODE_ENV_VARS:
            if env_key in configured:
                return configured[env_key]

    return "PAPER"


def validate_paper_live_isolation(
    trading_mode: str,
    api_key: str,
    api_secret: str,
    base_url: str = "",
) -> None:
    """Enforces strict fail-closed credential and endpoint isolation between PAPER and LIVE modes."""
    mode = _normalize_mode_token(trading_mode, source_name="trading_mode")
    trimmed_key = str(api_key or "").strip()
    upper_key = trimmed_key.upper()
    trimmed_secret = str(api_secret or "").strip()
    upper_secret = trimmed_secret.upper()
    clean_url = str(base_url or "").strip().rstrip("/").lower()
    if clean_url.endswith("/v2"):
        clean_url = clean_url[:-3]

    if mode == "LIVE":
        if not trimmed_key:
            raise ValueError("LIVE trading mode requires a non-empty API key (fail-closed)")
        if upper_key.startswith("PK") or "PAPER" in upper_key:
            raise ValueError(
                f"LIVE trading mode cannot use paper API key {trimmed_key!r} (fail-closed)"
            )
        if "PAPER" in upper_secret:
            raise ValueError("LIVE trading mode cannot use paper API secret (fail-closed)")
        if "paper" in clean_url:
            raise ValueError(
                f"LIVE trading mode cannot use paper base URL {base_url!r} (fail-closed)"
            )
    else:
        if upper_key.startswith("AK") or "LIVE" in upper_key:
            raise ValueError(
                f"PAPER trading mode cannot use live API key {trimmed_key!r} (fail-closed)"
            )
        if "LIVE" in upper_secret:
            raise ValueError("PAPER trading mode cannot use live API secret (fail-closed)")
        if "api.alpaca.markets" in clean_url and "paper" not in clean_url:
            raise ValueError(
                f"PAPER trading mode cannot use live Alpaca base URL {base_url!r} (fail-closed)"
            )


def build_alpaca_config_from_env(explicit_mode: Optional[str] = None) -> AlpacaExecClientConfig:
    trading_mode = resolve_trading_mode(explicit_mode)
    is_live = trading_mode == "LIVE"
    if is_live:
        api_key = (
            os.environ.get("APCA_LIVE_API_KEY_ID")
            or os.environ.get("APCA_API_KEY_ID")
            or os.environ.get("ALPACA_API_KEY")
            or ""
        ).strip()
        api_secret = (
            os.environ.get("APCA_LIVE_API_SECRET_KEY")
            or os.environ.get("APCA_API_SECRET_KEY")
            or os.environ.get("ALPACA_API_SECRET")
            or ""
        ).strip()
        base_url = (
            os.environ.get("APCA_LIVE_BASE_URL")
            or os.environ.get("APCA_API_BASE_URL")
            or os.environ.get("ALPACA_BASE_URL")
            or "https://api.alpaca.markets"
        ).strip()
    else:
        api_key = (
            os.environ.get("APCA_PAPER_API_KEY_ID")
            or os.environ.get("APCA_API_KEY_ID")
            or os.environ.get("ALPACA_API_KEY")
            or "paper-key"
        ).strip()
        api_secret = (
            os.environ.get("APCA_PAPER_API_SECRET_KEY")
            or os.environ.get("APCA_API_SECRET_KEY")
            or os.environ.get("ALPACA_API_SECRET")
            or "paper-secret"
        ).strip()
        base_url = (
            os.environ.get("APCA_PAPER_BASE_URL")
            or os.environ.get("APCA_API_BASE_URL")
            or os.environ.get("ALPACA_BASE_URL")
            or "https://paper-api.alpaca.markets"
        ).strip()

    validate_paper_live_isolation(
        trading_mode=trading_mode,
        api_key=api_key,
        api_secret=api_secret,
        base_url=base_url,
    )
    return AlpacaExecClientConfig(
        api_key=api_key,
        api_secret=api_secret,
        environment="live" if is_live else "paper",
        http_base_url=base_url,
    )


class RolloverLifecycleState(str, Enum):
    """Canonical 4-State Position & Plan Rollover Lifecycle Matrix (`P0-EXEC-01`)."""

    NEW_ACTIVE = "NEW_ACTIVE"
    RENEWED_ACTIVE = "RENEWED_ACTIVE"
    CARRY_OVER_EXIT_ONLY = "CARRY_OVER_EXIT_ONLY"
    THESIS_INVALIDATED_EXIT = "THESIS_INVALIDATED_EXIT"


@dataclass(frozen=True)
class SymbolRolloverDecision:
    """Per-symbol reconciliation decision emitted by `DailyPlanReconciler`."""

    symbol: str
    state: RolloverLifecycleState
    previous_position_qty: Decimal
    plan_item: Optional[PlanItemContract] = None
    effective_stop_price: Optional[Decimal] = None
    warmup_bars_fed: int = 0
    safe_exit_triggered: bool = False
    exit_order_id: Optional[str] = None
    rationale: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "state": self.state.value,
            "previous_position_qty": str(self.previous_position_qty),
            "item_id": str(self.plan_item.item_id) if self.plan_item is not None else None,
            "effective_stop_price": (
                str(self.effective_stop_price)
                if self.effective_stop_price is not None
                else None
            ),
            "warmup_bars_fed": self.warmup_bars_fed,
            "safe_exit_triggered": self.safe_exit_triggered,
            "exit_order_id": self.exit_order_id,
            "rationale": self.rationale,
        }


@dataclass(frozen=True)
class DailyPlanRolloverReport:
    """Summary report emitted when `DailyPlanReconciler` reconciles a new daily `TradingPlan`."""

    trading_date: date
    decisions: Tuple[SymbolRolloverDecision, ...]
    sip_feeder_subscriptions: Optional[Dict[str, Any]] = None

    @property
    def decisions_by_symbol(self) -> Dict[str, SymbolRolloverDecision]:
        return {d.symbol: d for d in self.decisions}

    @property
    def new_active_symbols(self) -> Tuple[str, ...]:
        return tuple(
            d.symbol
            for d in self.decisions
            if d.state == RolloverLifecycleState.NEW_ACTIVE
        )

    @property
    def renewed_active_symbols(self) -> Tuple[str, ...]:
        return tuple(
            d.symbol
            for d in self.decisions
            if d.state == RolloverLifecycleState.RENEWED_ACTIVE
        )

    @property
    def carry_over_exit_only_symbols(self) -> Tuple[str, ...]:
        return tuple(
            d.symbol
            for d in self.decisions
            if d.state == RolloverLifecycleState.CARRY_OVER_EXIT_ONLY
        )

    @property
    def thesis_invalidated_exit_symbols(self) -> Tuple[str, ...]:
        return tuple(
            d.symbol
            for d in self.decisions
            if d.state == RolloverLifecycleState.THESIS_INVALIDATED_EXIT
        )

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "trading_date": self.trading_date.isoformat(),
            "new_active_symbols": list(self.new_active_symbols),
            "renewed_active_symbols": list(self.renewed_active_symbols),
            "carry_over_exit_only_symbols": list(self.carry_over_exit_only_symbols),
            "thesis_invalidated_exit_symbols": list(self.thesis_invalidated_exit_symbols),
            "decisions": [d.to_dict() for d in self.decisions],
        }
        if self.sip_feeder_subscriptions is not None:
            payload["sip_feeder_subscriptions"] = dict(self.sip_feeder_subscriptions)
        return payload


class SipFeederSubscriptionClient:
    """Single-responsibility HTTP client synchronizing active `TradingPlan` and carry-over
    symbols with `sip-feeder` (`:8011/v1/subscriptions`).
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        *,
        timeout_secs: float = 5.0,
        max_retries: int = 2,
        require_configured: bool = False,
    ) -> None:
        resolved = (
            str(base_url).strip()
            if base_url is not None
            else os.environ.get("SIP_FEEDER_URL", "").strip()
        )
        self._base_url = resolved.rstrip("/")
        self._timeout_secs = max(0.5, float(timeout_secs))
        self._max_retries = max(0, int(max_retries))
        self._require_configured = bool(require_configured)

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def is_configured(self) -> bool:
        return bool(self._base_url)

    @staticmethod
    def normalize_symbols(symbols: Iterable[str]) -> List[str]:
        normalized: Set[str] = set()
        for raw in symbols:
            if not raw:
                continue
            token = str(raw).strip().upper().split(".", 1)[0].strip()
            if token:
                normalized.add(token)
        return sorted(normalized)

    def _request_subscriptions(
        self,
        method: str,
        symbols: Sequence[str],
    ) -> Dict[str, Any]:
        payload_bytes = json.dumps(
            {
                "bars": list(symbols),
                "quotes": list(symbols),
                "statuses": list(symbols),
            }
        ).encode("utf-8")
        url = f"{self._base_url}/v1/subscriptions"
        for attempt in range(self._max_retries + 1):
            req = urllib.request.Request(
                url,
                data=payload_bytes,
                headers={"Content-Type": "application/json"},
                method=method.upper(),
            )
            try:
                with urllib.request.urlopen(req, timeout=self._timeout_secs) as resp:
                    raw_body = resp.read().decode("utf-8")
                    return json.loads(raw_body) if raw_body.strip() else {}
            except Exception as exc:
                if attempt < self._max_retries:
                    continue
                raise RuntimeError(
                    f"sip-feeder {method.upper()} {url} failed for symbols={list(symbols)}: {exc}"
                ) from exc
        raise RuntimeError(
            f"sip-feeder {method.upper()} {url} failed for symbols={list(symbols)}"
        )

    def sync_plan_subscriptions(
        self,
        active_symbols: Iterable[str],
        remove_symbols: Iterable[str] = (),
    ) -> Dict[str, Any]:
        active_list = self.normalize_symbols(active_symbols)
        active_set = set(active_list)
        remove_list = [
            s for s in self.normalize_symbols(remove_symbols) if s not in active_set
        ]
        if not self.is_configured:
            if self._require_configured:
                raise RuntimeError(
                    "SIP_FEEDER_URL is required in live/paper mode (fail-closed)"
                )
            return {
                "status": "SKIPPED_UNCONFIGURED",
                "added": active_list,
                "removed": remove_list,
            }
        added_view: Optional[Dict[str, Any]] = None
        removed_view: Optional[Dict[str, Any]] = None
        if active_list:
            added_view = self._request_subscriptions("POST", active_list)
        if remove_list:
            removed_view = self._request_subscriptions("DELETE", remove_list)
        return {
            "status": "SYNCED",
            "added": active_list,
            "removed": remove_list,
            "added_view": added_view,
            "removed_view": removed_view,
        }


def _record_live_node_critical_alert(
    dsn: Optional[str],
    *,
    event_type: str,
    title: str,
    body: str,
    payload: Mapping[str, Any],
    dedup_key: str,
) -> None:
    dsn_str = str(dsn or "").strip()
    if not dsn_str.startswith(("postgresql://", "postgres://")):
        return
    try:
        import psycopg2

        with psycopg2.connect(dsn_str, connect_timeout=2) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alert_outbox (event_type, severity, source, title, body, payload, dedup_key)
                VALUES (%s, 'CRITICAL', 'nautilus:live_node', %s, %s, %s::jsonb, %s)
                """,
                (event_type, title, body, json.dumps(dict(payload)), dedup_key),
            )
    except Exception as exc:
        logger.error("Failed persisting %s alert into alert_outbox: %s", event_type, exc)


def verify_rust_live_node_contract(node: Any) -> Any:
    """Verify the target runtime node exposes a compiled cache supporting instrument registration.

    Returns `None` if `.cache` is absent or moved out by an active `LiveNode.run_async()` loop.
    """
    try:
        cache_obj = getattr(node, "cache", None)
    except RuntimeError:
        return None
    if cache_obj is None:
        return None
    if not callable(getattr(cache_obj, "add_instrument", None)) or not callable(
        getattr(cache_obj, "instrument", None)
    ):
        raise RuntimeError(
            f"Rust Cache contract violated on {type(cache_obj).__name__}: "
            "missing callable add_instrument() or instrument()"
        )
    return cache_obj


def verify_plan_instruments_ready(
    cache_obj: Any,
    items: Sequence[PlanItemContract],
    resolved_instruments: Mapping[str, "ResolvedInstrumentEntry"],
    *,
    pg_backend: Optional[Any] = None,
    dsn: Optional[str] = None,
    trading_date: Optional[date] = None,
    venue: str = "ALPACA",
) -> None:
    """Register all resolved plan instruments into Rust `PyCache` and read-back verify every item (fail-closed)."""
    if cache_obj is None:
        if items or any(getattr(entry, "instrument", None) is not None for entry in resolved_instruments.values()):
            msg = "LIVE_NODE_INSTRUMENT_CACHE_UNREADY: node.cache is None"
            if pg_backend is not None and callable(getattr(pg_backend, "set_entry_freeze", None)):
                pg_backend.set_entry_freeze(True, reason=msg, updated_by="nautilus:readiness_gate")
            _record_live_node_critical_alert(
                dsn,
                event_type="LIVE_NODE_INSTRUMENT_CACHE_UNREADY",
                title="LiveNode Startup Readiness Gate Failed (cache=None)",
                body=msg,
                payload={"trading_date": trading_date.isoformat() if trading_date else None},
                dedup_key=f"LIVE_NODE_INSTRUMENT_CACHE_UNREADY:{trading_date.isoformat() if trading_date else 'na'}",
            )
            raise RuntimeError(msg)
        return

    try:
        if not callable(getattr(cache_obj, "add_instrument", None)) or not callable(
            getattr(cache_obj, "instrument", None)
        ):
            raise RuntimeError(
                f"Rust PyCache contract missing add_instrument/instrument on {type(cache_obj).__name__}"
            )
        for entry in resolved_instruments.values():
            inst = getattr(entry, "instrument", None)
            if inst is not None:
                add_currency = getattr(cache_obj, "add_currency", None)
                quote_ccy = getattr(inst, "quote_currency", None)
                if callable(add_currency) and quote_ccy is not None:
                    cache_obj.add_currency(quote_ccy)
                cache_obj.add_instrument(inst)

        missing_ids: List[str] = []
        for item in items:
            sym = str(getattr(item, "symbol", "") or "").strip().upper()
            if not sym:
                continue
            entry = resolved_instruments.get(sym)
            inst_obj = getattr(entry, "instrument", None) if entry is not None else None
            inst_id = getattr(inst_obj, "id", None) if inst_obj is not None else InstrumentId.from_str(f"{sym}.{venue}")
            cached_inst = cache_obj.instrument(inst_id)
            if cached_inst is None:
                missing_ids.append(str(inst_id))
        if missing_ids:
            raise RuntimeError(
                f"Instrument read-back verification failed for Cache: missing {missing_ids}"
            )
    except Exception as exc:
        msg = f"LIVE_NODE_INSTRUMENT_CACHE_UNREADY: {exc}"
        logger.critical("%s", msg, exc_info=True)
        if pg_backend is not None and callable(getattr(pg_backend, "set_entry_freeze", None)):
            try:
                pg_backend.set_entry_freeze(
                    True,
                    reason=msg,
                    updated_by="nautilus:readiness_gate",
                )
            except Exception as freeze_exc:
                logger.error("Failed latching ENTRY_FREEZE after readiness gate failure: %s", freeze_exc)
        _record_live_node_critical_alert(
            dsn,
            event_type="LIVE_NODE_INSTRUMENT_CACHE_UNREADY",
            title="LiveNode Instrument Cache Readiness Gate Failed",
            body=msg,
            payload={
                "trading_date": trading_date.isoformat() if trading_date else None,
                "symbols": [str(i.symbol) for i in items],
                "error": str(exc),
            },
            dedup_key=f"LIVE_NODE_INSTRUMENT_CACHE_UNREADY:{trading_date.isoformat() if trading_date else 'na'}",
        )
        raise RuntimeError(msg) from exc


@dataclass(frozen=True)
class ExtraVenueClientSpec:
    """Optional secondary venue execution client specification (e.g., `IBKR`, `OKX`)."""

    name: Optional[str]
    venue: str
    factory: Any
    config: Any


@dataclass(frozen=True)
class ResolvedInstrumentEntry:
    """Result of resolving an Alpaca/cached `Equity` instrument for a plan item."""

    symbol: str
    instrument: Optional[Equity]
    status: str
    reason: str = ""


def _is_carry_over_item(item: PlanItemContract) -> bool:
    co_status = str(
        getattr(item, "carry_over_status", None)
        or item.sizing_trace.get("carry_over_status")
        or ""
    ).strip().upper()
    return (
        co_status in ("RENEWED_ACTIVE", "CARRY_OVER_EXIT_ONLY", "THESIS_INVALIDATED_EXIT")
        or item.sizing_trace.get("carry_over_position_qty") is not None
        or item.filled_qty > Decimal("0")
    )


def _metadata_record_from_equity(eq: Equity) -> Dict[str, Any]:
    return {
        "symbol": str(eq.id.symbol).split(".", 1)[0].strip().upper(),
        "asset_class": "us_equity",
        "status": "active",
        "tradable": True,
        "marginable": True,
        "shortable": True,
        "easy_to_borrow": False,
        "fractionable": False,
        "min_order_size": str(getattr(eq, "min_quantity", None) or getattr(eq, "lot_size", None) or "1"),
        "price_increment": str(eq.price_increment),
        "price_precision": int(eq.price_precision),
        "lot_size": str(getattr(eq, "lot_size", None) or "1"),
    }


def _equity_from_metadata_record(
    rec: Mapping[str, Any],
    venue_str: str = "ALPACA",
    ref_price: Optional[float] = None,
) -> Equity:
    sym = str(rec.get("symbol") or "").split(".", 1)[0].strip().upper()
    inst_id = InstrumentId.from_str(f"{sym}.{venue_str}")
    raw_tick = rec.get("price_increment")
    raw_prec = rec.get("price_precision")
    if raw_tick is not None and raw_prec is not None:
        tick_dec = _to_decimal(raw_tick, "price_increment")
        prec = max(1, min(9, int(raw_prec)))
        tick_str = f"{tick_dec:.{prec}f}"
    else:
        ref_dec = (
            Decimal(str(ref_price))
            if ref_price is not None and float(ref_price) > 0.0
            else Decimal("100")
        )
        tick = rule_612_tick_size(ref_dec)
        prec = 4 if tick < Decimal("0.01") else 2
        tick_str = "0.0001" if prec == 4 else "0.01"
    raw_lot = rec.get("lot_size") or rec.get("min_order_size") or 1
    lot_int = max(1, int(Decimal(str(raw_lot))))
    return Equity(
        instrument_id=inst_id,
        raw_symbol=inst_id.symbol,
        currency=Currency.from_str("USD"),
        price_precision=prec,
        price_increment=Price.from_str(tick_str),
        lot_size=Quantity.from_int(lot_int),
        ts_event=0,
        ts_init=0,
    )


def _emit_critical_instrument_alert(
    dsn: Optional[str],
    *,
    event_type: str,
    symbol: str,
    title: str,
    body: str,
    payload: Mapping[str, Any],
) -> None:
    if not dsn:
        return
    try:
        conn = psycopg2.connect(dsn)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT to_regclass('public.alert_outbox')")
                    row = cur.fetchone()
                    if not row or row[0] is None:
                        return
                    cur.execute(
                        """
                        INSERT INTO alert_outbox (
                            event_type, severity, source, title, body, payload, dedup_key, status
                        ) VALUES (
                            %s, 'CRITICAL', 'nautilus:live_node', %s, %s, %s::jsonb, %s, 'PENDING'
                        )
                        """,
                        (
                            event_type,
                            title,
                            body,
                            json.dumps(dict(payload)),
                            f"{event_type}:{symbol}:{uuid.uuid4().hex[:12]}",
                        ),
                    )
        finally:
            conn.close()
    except Exception as exc:
        logger.error("Failed to insert CRITICAL alert %s for %s: %s", event_type, symbol, exc)


def _build_live_risk_engine_config(
    items: Sequence[PlanItemContract],
    *,
    system_config: Optional[SystemConfig] = None,
) -> LiveRiskEngineConfig:
    sys_cfg = system_config or SystemConfig()
    max_slices = max(
        [int(getattr(it.execution_spec, "num_slices", 4) or 4) for it in items],
        default=4,
    )
    max_positions = max(
        1, int(getattr(sys_cfg.admission, "max_new_positions_per_day", 2) or 2)
    )
    submit_limit = max(30, len(items) * max_slices * max_positions * 2)
    modify_limit = max(submit_limit, len(items) * max_slices * max_positions * 4)
    return LiveRiskEngineConfig(
        max_notional_per_order=None,
        max_order_submit_rate=f"{submit_limit}/00:01:00",
        max_order_modify_rate=f"{modify_limit}/00:01:00",
    )


@dataclass(frozen=True)
class AssembledLiveNodeBundle:
    """Container holding the built `LiveNode` and its attached isomorphic components."""

    node: LiveNode
    trading_date: date
    plan_items: Tuple[PlanItemContract, ...]
    strategies: Tuple[BaseArchetypeStrategy, ...]
    plan_gate_backend: PostgresPlanGateBackend
    telemetry_actor: PostgresTelemetryActor
    routed_venues: Tuple[str, ...]
    exec_algorithm: Optional[CollaredTwapExecAlgorithm] = None
    reconciler: Optional["DailyPlanReconciler"] = None
    instrument_statuses: Mapping[str, str] = field(default_factory=dict)

    @property
    def instruments(self) -> Tuple[str, ...]:
        return tuple(item.strategy_config.instrument_id for item in self.plan_items)


def _compute_warmup_query_start_ns(
    bar_timeframe: str,
    req_bars: int,
    end_ts_ns: Optional[int] = None,
) -> int:
    tf = str(bar_timeframe or "15m").strip().lower()
    if tf == "1w":
        lookback_days = max(365, int(req_bars * 10))
    elif tf == "1d":
        lookback_days = max(60, int(req_bars * 2) + 30)
    else:
        lookback_days = max(30, int(math.ceil(req_bars / 6.0) * 3))
    anchor_ns = (
        int(end_ts_ns)
        if end_ts_ns is not None and end_ts_ns > 0
        else int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
    )
    delta_ns = int(timedelta(days=lookback_days).total_seconds() * 1_000_000_000)
    return max(0, anchor_ns - delta_ns)


def _load_warmup_bars_for_spec(
    instrument_id: str | InstrumentId,
    bar_timeframe: str,
    lookback_window: int,
    *,
    bars_override: Optional[Sequence[Bar]] = None,
    historical_bar_loader: Optional[Callable[[str, str, int], Sequence[Bar]]] = None,
    datainfra_config: Optional[DataInfraConfig] = None,
) -> List[Bar]:
    if bars_override is not None:
        return list(bars_override)

    req_bars = required_warmup_bars(lookback_window)
    prim_inst_str = str(instrument_id)
    prim_sym = prim_inst_str.split(".", 1)[0].strip().upper()
    venue = (
        prim_inst_str.split(".", 1)[1].strip().upper()
        if "." in prim_inst_str
        else "ALPACA"
    )

    collected: List[Bar] = []
    if historical_bar_loader is not None:
        loaded = list(historical_bar_loader(prim_sym, bar_timeframe, req_bars))
        loaded.sort(
            key=lambda b: int(
                getattr(b, "ts_init", 0) or getattr(b, "ts_event", 0) or 0
            )
        )
        if len(loaded) > req_bars:
            loaded = loaded[-req_bars:]
        collected.extend(loaded)
    elif datainfra_config is not None and getattr(
        datainfra_config, "clickhouse_url", None
    ):
        catalog = None
        try:
            catalog = DataInfraCatalog(datainfra_config)
            now_ns = int(time.time() * 1_000_000_000)
            start_ts_ns = _compute_warmup_query_start_ns(
                bar_timeframe,
                req_bars,
                end_ts_ns=now_ns,
            )
            if hasattr(catalog, "register_equity"):
                catalog.register_equity(prim_sym, venue=venue, lot_size=1)
            try:
                raw_queried = catalog.query_bars(
                    [prim_sym],
                    start_ts_ns=start_ts_ns,
                    end_ts_ns=now_ns,
                    timeframe=bar_timeframe,
                    adjustment="adjusted",
                    market_session="REG",
                    venue=venue,
                )
            except TypeError:
                raw_queried = catalog.query_bars(
                    [prim_sym],
                    start_ns=start_ts_ns,
                    end_ns=now_ns,
                    timeframe=bar_timeframe,
                )
            queried = sorted(
                raw_queried,
                key=lambda b: int(
                    getattr(b, "ts_init", 0) or getattr(b, "ts_event", 0) or 0
                ),
            )
            if len(queried) > req_bars:
                queried = queried[-req_bars:]
            collected.extend(queried)
        except Exception as exc:
            logger.error(
                "ClickHouse warmup query failed for %s: %s",
                prim_sym,
                exc,
            )
            raise
        finally:
            catalog = None

    collected.sort(
        key=lambda b: int(getattr(b, "ts_init", 0) or getattr(b, "ts_event", 0) or 0)
    )
    return collected


def _load_warmup_bars_for_strategy_with_sources(
    strategy: BaseArchetypeStrategy,
    *,
    bars_override: Optional[Sequence[Bar]] = None,
    historical_bar_loader: Optional[Callable[[str, str, int], Sequence[Bar]]] = None,
    datainfra_config: Optional[DataInfraConfig] = None,
) -> List[Bar]:
    cfg = strategy.archetype_config
    return _load_warmup_bars_for_spec(
        cfg.instrument_id,
        cfg.bar_timeframe,
        cfg.lookback_window,
        bars_override=bars_override,
        historical_bar_loader=historical_bar_loader,
        datainfra_config=datainfra_config,
    )


class NautilusLiveNodeAssembler:
    """Builds and wires a production `LiveNode` from PostgreSQL `LOCKED` `PlanItemContract` items."""

    def __init__(
        self,
        dsn: Optional[str] = None,
        datainfra_config: Optional[DataInfraConfig] = None,
        alpaca_config: Optional[AlpacaExecClientConfig] = None,
        extra_exec_clients: Optional[Sequence[ExtraVenueClientSpec]] = None,
        node_name: str = "nautilus-live-node",
        trader_id: str = "TRADER-LIVE-001",
        environment: Optional[Environment] = None,
        market: str = "US_EQUITY",
        reconciliation_enabled: bool = True,
        reconciliation_lookback_mins: int = 1440,
        open_check_interval_secs: Optional[float] = None,
        position_check_interval_secs: Optional[float] = None,
        cache_config: Optional[CacheConfig] = None,
        historical_bar_loader: Optional[Callable[[str, str, int], Sequence[Bar]]] = None,
        warmup_on_build: Optional[bool] = None,
        sip_feeder_url: Optional[str] = None,
        sip_feeder_client: Optional[SipFeederSubscriptionClient] = None,
        system_config: Optional[SystemConfig] = None,
        fee_schedule: Optional[IbkrProFeeSchedule] = None,
        instrument_loader: Optional[Callable[..., List[Equity]]] = None,
        resolve_instruments_via_alpaca: Optional[bool] = None,
        require_sip_feeder_configured: Optional[bool] = None,
    ) -> None:
        resolved_mode = resolve_trading_mode()
        self._dsn = resolve_database_dsn(dsn)
        self._explicit_datainfra_config = datainfra_config
        self._datainfra_config = datainfra_config or self._build_default_datainfra_config()
        self._alpaca_config = alpaca_config
        self._extra_exec_clients = tuple(extra_exec_clients or ())
        self._node_name = str(node_name or "nautilus-live-node").strip()
        self._trader_id = str(trader_id or "TRADER-LIVE-001").strip()
        self._environment = (
            environment
            if environment is not None
            else (
                Environment.LIVE
                if resolved_mode == "LIVE"
                else Environment.SANDBOX
            )
        )
        self._market = str(market or "US_EQUITY").strip().upper()
        self._reconciliation_enabled = bool(reconciliation_enabled)
        self._reconciliation_lookback_mins = max(60, int(reconciliation_lookback_mins))
        default_reconcile_secs = float(
            os.environ.get("NAUTILUS_RECONCILE_INTERVAL_SECS", "30.0") or 30.0
        )
        self._open_check_interval_secs = (
            float(open_check_interval_secs)
            if open_check_interval_secs is not None
            else default_reconcile_secs
        )
        self._position_check_interval_secs = (
            float(position_check_interval_secs)
            if position_check_interval_secs is not None
            else default_reconcile_secs
        )
        self._cache_config = cache_config
        self._historical_bar_loader = historical_bar_loader
        self._warmup_on_build = (
            bool(warmup_on_build)
            if warmup_on_build is not None
            else bool(historical_bar_loader is not None)
        )
        self._sip_feeder_client = sip_feeder_client or SipFeederSubscriptionClient(
            base_url=sip_feeder_url,
            require_configured=(
                bool(require_sip_feeder_configured)
                if require_sip_feeder_configured is not None
                else False
            ),
        )
        self._system_config = system_config
        self._fee_schedule = fee_schedule or IbkrProFeeSchedule()
        self._instrument_loader = instrument_loader
        self._resolve_instruments_via_alpaca = resolve_instruments_via_alpaca
        self._repo = PlanContractRepository(dsn=self._dsn)

    def resolve_instruments_for_items(
        self,
        items: Sequence[PlanItemContract],
        *,
        persist_to_db: bool = True,
        trading_date: Optional[date] = None,
    ) -> Dict[str, ResolvedInstrumentEntry]:
        """Resolves Alpaca `Equity` instruments (plus `instrument_metadata_cache` persistence/fallback).

        - `NEW_ENTRY` failure/untradable -> status=`INSTRUMENT_UNRESOLVED`, `CANCELLED` in PG + `CRITICAL` alert,
          isolated so valid symbols proceed.
        - `CARRY_OVER` transient API error -> restore from `instrument_metadata_cache`, status=`DEGRADED_CACHED_INSTRUMENT`.
        - `CARRY_OVER` confirmed venue halt/untradable (`ValueError`) -> status=`VENUE_HALTED_FROZEN` + `CRITICAL` alert,
          preserving reconciliation `Equity` in cache while freezing auto-modifies.
        """
        if not items:
            return {}
        items_by_sym: Dict[str, PlanItemContract] = {}
        for item in items:
            items_by_sym[item.symbol] = item

        if self._instrument_loader is not None:
            use_alpaca = True
        elif self._resolve_instruments_via_alpaca is not None:
            use_alpaca = bool(self._resolve_instruments_via_alpaca)
        else:
            api_key = str(getattr(self._alpaca_config, "api_key", "") or "")
            masked_key_id = str(getattr(self._alpaca_config, "masked_key_id", "") or "")
            use_alpaca = self._alpaca_config is not None and bool(api_key or masked_key_id)

        syms = sorted(items_by_sym.keys())
        cached_rows: Dict[str, Dict[str, Any]] = {}
        if persist_to_db and self._repo is not None:
            try:
                cached_rows = self._repo.load_instrument_metadata_cache(syms)
            except Exception as exc:
                logger.warning("Failed loading instrument_metadata_cache: %s", exc)

        resolved: Dict[str, ResolvedInstrumentEntry] = {}
        if not use_alpaca:
            for sym in syms:
                item = items_by_sym[sym]
                venue_str = (
                    item.strategy_config.instrument_id.split(".", 1)[1].strip().upper()
                    if "." in item.strategy_config.instrument_id
                    else (item.venue or "ALPACA")
                )
                ref_px = float(item.risk_envelope.reference_price or 0.0)
                if sym in cached_rows:
                    eq = _equity_from_metadata_record(
                        cached_rows[sym],
                        venue_str,
                        ref_price=ref_px if ref_px > 0.0 else None,
                    )
                else:
                    eq = _create_default_equity(
                        sym,
                        venue_str,
                        ref_price=ref_px if ref_px > 0.0 else None,
                    )
                resolved[sym] = ResolvedInstrumentEntry(
                    symbol=sym,
                    instrument=eq,
                    status="RESOLVED_OFFLINE",
                )
            return resolved

        loader = self._instrument_loader or load_alpaca_equity_instruments
        ref_prices = {
            sym: float(items_by_sym[sym].risk_envelope.reference_price)
            for sym in syms
            if items_by_sym[sym].risk_envelope.reference_price > Decimal("0")
        }
        venue_default = items_by_sym[syms[0]].venue or "ALPACA"
        live_map: Dict[str, Equity] = {}
        try:
            batch_eqs = loader(
                syms,
                config=self._alpaca_config,
                ref_prices=ref_prices,
                venue=venue_default,
            )
            for eq in batch_eqs or ():
                eq_sym = str(eq.id.symbol).split(".", 1)[0].strip().upper()
                if eq_sym:
                    live_map[eq_sym] = eq
        except Exception as batch_exc:
            logger.warning(
                "Batch Alpaca instrument resolution failed (%s); isolating per symbol",
                batch_exc,
            )

        upserts: List[Dict[str, Any]] = []
        for sym in syms:
            item = items_by_sym[sym]
            venue_str = (
                item.strategy_config.instrument_id.split(".", 1)[1].strip().upper()
                if "." in item.strategy_config.instrument_id
                else (item.venue or venue_default)
            )
            ref_px = float(item.risk_envelope.reference_price or 0.0)
            if sym in live_map:
                eq = live_map[sym]
                upserts.append(_metadata_record_from_equity(eq))
                resolved[sym] = ResolvedInstrumentEntry(
                    symbol=sym,
                    instrument=eq,
                    status="RESOLVED_LIVE",
                )
                continue

            try:
                single_eqs = loader(
                    [sym],
                    config=self._alpaca_config,
                    ref_prices=({sym: ref_px} if ref_px > 0.0 else {}),
                    venue=venue_str,
                )
                if not single_eqs:
                    raise ValueError(f"Alpaca returned empty instrument list for {sym}")
                eq = single_eqs[0]
                upserts.append(_metadata_record_from_equity(eq))
                resolved[sym] = ResolvedInstrumentEntry(
                    symbol=sym,
                    instrument=eq,
                    status="RESOLVED_LIVE",
                )
            except Exception as exc:
                is_confirmed_untradable = isinstance(exc, ValueError)
                if _is_carry_over_item(item):
                    if sym in cached_rows:
                        eq = _equity_from_metadata_record(
                            cached_rows[sym],
                            venue_str,
                            ref_price=ref_px if ref_px > 0.0 else None,
                        )
                    else:
                        eq = _create_default_equity(
                            sym,
                            venue_str,
                            ref_price=ref_px if ref_px > 0.0 else None,
                        )
                    if is_confirmed_untradable:
                        if persist_to_db:
                            _emit_critical_instrument_alert(
                                self._dsn,
                                event_type="VENUE_HALTED_FROZEN",
                                symbol=sym,
                                title=f"Carry-over instrument {sym} untradable/halted at venue",
                                body=str(exc),
                                payload={
                                    "symbol": sym,
                                    "item_id": str(item.item_id),
                                    "trading_date": str(trading_date or ""),
                                    "reason": str(exc),
                                },
                            )
                        resolved[sym] = ResolvedInstrumentEntry(
                            symbol=sym,
                            instrument=eq,
                            status="VENUE_HALTED_FROZEN",
                            reason=str(exc),
                        )
                    else:
                        logger.warning(
                            "Carry-over %s using DEGRADED_CACHED_INSTRUMENT after transient API error: %s",
                            sym,
                            exc,
                        )
                        resolved[sym] = ResolvedInstrumentEntry(
                            symbol=sym,
                            instrument=eq,
                            status="DEGRADED_CACHED_INSTRUMENT",
                            reason=str(exc),
                        )
                else:
                    if persist_to_db and self._repo is not None:
                        try:
                            self._repo.update_item_status(
                                item.item_id,
                                "CANCELLED",
                                actor="nautilus:live_node",
                                reason=f"INSTRUMENT_UNRESOLVED: {exc}",
                            )
                        except Exception as status_exc:
                            logger.warning(
                                "Failed updating %s status to CANCELLED: %s",
                                sym,
                                status_exc,
                            )
                    if persist_to_db:
                        _emit_critical_instrument_alert(
                            self._dsn,
                            event_type="INSTRUMENT_UNRESOLVED",
                            symbol=sym,
                            title=f"NEW_ENTRY instrument {sym} failed Alpaca resolution",
                            body=str(exc),
                            payload={
                                "symbol": sym,
                                "item_id": str(item.item_id),
                                "trading_date": str(trading_date or ""),
                                "reason": str(exc),
                            },
                        )
                    resolved[sym] = ResolvedInstrumentEntry(
                        symbol=sym,
                        instrument=None,
                        status="INSTRUMENT_UNRESOLVED",
                        reason=str(exc),
                    )

        if upserts and persist_to_db and self._repo is not None:
            try:
                self._repo.upsert_instrument_metadata_cache(upserts)
            except Exception as cache_exc:
                logger.warning("Failed upserting instrument_metadata_cache: %s", cache_exc)
        return resolved

    @staticmethod
    def _build_default_datainfra_config() -> DataInfraConfig:
        from urllib.parse import quote

        ch_url = (
            os.environ.get("DATAINFRA_CLICKHOUSE_URL")
            or os.environ.get("CLICKHOUSE_URL")
            or os.environ.get("CLICKHOUSE_HTTP_URL")
            or ""
        ).strip()
        ch_user = (
            os.environ.get("DATAINFRA_CLICKHOUSE_USER")
            or os.environ.get("CLICKHOUSE_USER")
            or os.environ.get("CH_USER")
            or ""
        ).strip()
        ch_pw = (
            os.environ.get("DATAINFRA_CLICKHOUSE_PASSWORD")
            or os.environ.get("CLICKHOUSE_PASSWORD")
            or os.environ.get("CH_PASSWORD")
            or ""
        ).strip()
        if not ch_url:
            ch_host = os.environ.get("CH_HOST", "127.0.0.1").strip()
            ch_port = (os.environ.get("CH_HTTP_PORT") or os.environ.get("CH_PORT") or "8123").strip()
            if ch_user:
                ch_url = f"http://{ch_host}:{ch_port}/?user={quote(ch_user)}&password={quote(ch_pw)}"
            else:
                ch_url = f"http://{ch_host}:{ch_port}"
        elif ch_user and "user=" not in ch_url and "@" not in ch_url:
            sep = "&" if "?" in ch_url else "?"
            ch_url = f"{ch_url}{sep}user={quote(ch_user)}&password={quote(ch_pw)}"
        ch_db = (
            os.environ.get("DATAINFRA_CLICKHOUSE_DATABASE")
            or os.environ.get("CLICKHOUSE_DATABASE")
            or os.environ.get("CH_DB")
            or "quant_market_data"
        ).strip()
        brokers_raw = (
            os.environ.get("DATAINFRA_KAFKA_BROKERS")
            or os.environ.get("KAFKA_BROKERS")
            or "127.0.0.1:9092"
        )
        brokers = [b.strip() for b in brokers_raw.split(",") if b.strip()]
        cal_url = (
            os.environ.get("DATAINFRA_CALENDAR_SERVICE_URL")
            or os.environ.get("CALENDAR_SERVICE_URL")
            or "http://127.0.0.1:8012"
        ).strip()
        venue = (
            os.environ.get("DATAINFRA_DEFAULT_VENUE")
            or os.environ.get("DEFAULT_VENUE")
            or "ALPACA"
        ).strip().upper()
        return DataInfraConfig(
            clickhouse_url=ch_url,
            clickhouse_database=ch_db,
            kafka_brokers=brokers,
            kafka_group_id=os.environ.get("KAFKA_GROUP_ID", "nautilus-datainfra-live"),
            calendar_service_url=cal_url,
            default_venue=venue,
        )

    @property
    def sip_feeder_client(self) -> SipFeederSubscriptionClient:
        return self._sip_feeder_client

    def _sync_sip_feeder_subscriptions(
        self,
        active_symbols: Iterable[str],
        *,
        remove_symbols: Iterable[str] = (),
        pg_backend: Optional[Any] = None,
    ) -> Dict[str, Any]:
        try:
            return self._sip_feeder_client.sync_plan_subscriptions(
                active_symbols=active_symbols,
                remove_symbols=remove_symbols,
            )
        except Exception as exc:
            logger.error(
                "Failed to synchronize sip-feeder subscriptions (active=%s, remove=%s): %s",
                list(active_symbols),
                list(remove_symbols),
                exc,
            )
            if pg_backend is not None and hasattr(pg_backend, "set_entry_freeze"):
                try:
                    pg_backend.set_entry_freeze(
                        True,
                        reason=f"SIP_FEEDER_SUBSCRIPTION_SYNC_FAILED: {exc}",
                        updated_by="nautilus:sip_feeder_sync",
                    )
                except Exception:
                    pass
            _record_live_node_critical_alert(
                self._dsn,
                event_type="SIP_FEEDER_SUBSCRIPTION_SYNC_FAILED",
                title="sip-feeder Subscription Synchronization Failed",
                body=f"SIP_FEEDER_SUBSCRIPTION_SYNC_FAILED: {exc}",
                payload={
                    "active_symbols": list(active_symbols),
                    "remove_symbols": list(remove_symbols),
                    "error": str(exc),
                },
                dedup_key="SIP_FEEDER_SUBSCRIPTION_SYNC_FAILED:startup",
            )
            return {
                "status": "ERROR",
                "error": str(exc),
                "added": SipFeederSubscriptionClient.normalize_symbols(active_symbols),
                "removed": SipFeederSubscriptionClient.normalize_symbols(remove_symbols),
            }

    @property
    def dsn(self) -> str:
        return self._dsn

    @property
    def repository(self) -> PlanContractRepository:
        return self._repo

    def _has_locked_plan(self, trading_date: date) -> bool:
        has_locked_fn = getattr(self._repo, "has_locked_plan", None)
        if not callable(has_locked_fn):
            return False
        try:
            return bool(has_locked_fn(trading_date=trading_date, market=self._market))
        except TypeError:
            return bool(has_locked_fn(trading_date, self._market))

    def _hydrate_carry_over_items(
        self,
        trading_date: date,
        locked_items: Sequence[PlanItemContract],
        *,
        allow_db_query: bool = True,
    ) -> List[PlanItemContract]:
        combined = list(locked_items)
        if not allow_db_query:
            return combined
        load_co_fn = getattr(
            self._repo, "load_carry_over_items_for_open_positions", None
        )
        if not callable(load_co_fn):
            return combined
        existing_idx_by_sym = {it.symbol: idx for idx, it in enumerate(combined)}
        existing_syms = set(existing_idx_by_sym.keys())
        resolved_env = "LIVE" if self._environment == Environment.LIVE else "PAPER"
        try:
            carry_over = load_co_fn(
                trading_date=trading_date,
                market=self._market,
                excluded_symbols=set(),
                broker="alpaca",
                environment=resolved_env,
            )
        except TypeError:
            try:
                carry_over = load_co_fn(
                    trading_date=trading_date,
                    market=self._market,
                    excluded_symbols=set(),
                )
            except TypeError:
                carry_over = load_co_fn(trading_date, self._market, set())
        for co_item in carry_over or ():
            if co_item.symbol not in existing_syms:
                combined.append(co_item)
                existing_syms.add(co_item.symbol)
            else:
                idx = existing_idx_by_sym[co_item.symbol]
                existing_it = combined[idx]
                co_st = str(
                    getattr(co_item, "carry_over_status", None)
                    or co_item.sizing_trace.get("carry_over_status")
                    or ""
                ).strip().upper()
                if (
                    co_item.risk_envelope.side != existing_it.risk_envelope.side
                    or co_st == "THESIS_INVALIDATED_EXIT"
                ):
                    inv_trace = dict(co_item.sizing_trace)
                    inv_trace["carry_over_status"] = "THESIS_INVALIDATED_EXIT"
                    combined[idx] = replace(
                        co_item,
                        risk_envelope=replace(
                            co_item.risk_envelope,
                            approved_qty_cap=Decimal("0"),
                        ),
                        status="EXPIRED",
                        carry_over_status="THESIS_INVALIDATED_EXIT",
                        sizing_trace=inv_trace,
                    )
                    continue
                co_stop = co_item.risk_envelope.initial_stop_price
                cur_stop = existing_it.risk_envelope.initial_stop_price
                if co_stop > Decimal("0"):
                    if existing_it.risk_envelope.side == PlanSide.LONG:
                        tight_stop = max(cur_stop, co_stop)
                    else:
                        tight_stop = min(cur_stop, co_stop) if cur_stop > Decimal("0") else co_stop
                else:
                    tight_stop = cur_stop
                merged_trace = dict(existing_it.sizing_trace)
                merged_trace.setdefault("carry_over_status", "RENEWED_ACTIVE")
                if "carry_over_position_qty" not in merged_trace:
                    raw_co_q = co_item.sizing_trace.get("carry_over_position_qty")
                    merged_trace["carry_over_position_qty"] = (
                        str(raw_co_q) if raw_co_q is not None else str(co_item.filled_qty)
                    )
                tightened_tp = BaseArchetypeStrategy._clear_crossed_take_profit(
                    existing_it.risk_envelope.side,
                    tight_stop,
                    existing_it.risk_envelope.take_profit_price,
                )
                combined[idx] = replace(
                    existing_it,
                    risk_envelope=replace(
                        existing_it.risk_envelope,
                        initial_stop_price=tight_stop,
                        take_profit_price=tightened_tp,
                    ),
                    sizing_trace=merged_trace,
                    filled_qty=max(existing_it.filled_qty, co_item.filled_qty),
                )
        return combined

    def load_warmup_bars_for_strategy(
        self,
        strategy: BaseArchetypeStrategy,
        *,
        bars_override: Optional[Sequence[Bar]] = None,
    ) -> List[Bar]:
        """Queries ClickHouse `DataInfraCatalog` (or `historical_bar_loader`) for the trailing
        `max(lookback_window * 3, 120)` bars required to warm up `strategy`.
        """
        return _load_warmup_bars_for_strategy_with_sources(
            strategy,
            bars_override=bars_override,
            historical_bar_loader=self._historical_bar_loader,
            datainfra_config=self._datainfra_config,
        )

    def warmup_strategy_from_catalog(
        self,
        strategy: BaseArchetypeStrategy,
        bars: Optional[Sequence[Bar]] = None,
    ) -> int:
        """Populates `strategy`'s rolling indicator windows using historical bars (`max(lookback_window * 3, 120)`)
        under `_in_warmup=True` without emitting any orders.
        """
        resolved_bars = self.load_warmup_bars_for_strategy(
            strategy,
            bars_override=bars,
        )
        return strategy.warmup_historical_bars(resolved_bars)

    def load_locked_plan_items_or_wait(
        self,
        trading_date: date | str,
        listen_timeout_secs: float = 0.0,
    ) -> List[PlanItemContract]:
        """Loads today's `LOCKED` `PlanItemContract` list (plus any open overnight carry-over positions)
        or waits via PG `LISTEN nautilus_plan_locked`. Returns immediately (including `[]`) when a
        `LOCKED` 0-item `LOCKED_NO_TRADE` plan is present in `trading_plans`.
        """
        td = (
            date.fromisoformat(str(trading_date)[:10])
            if isinstance(trading_date, str)
            else trading_date
        )
        existing = self._repo.load_locked_plan_items(
            trading_date=td,
            market=self._market,
        )
        if existing or self._has_locked_plan(td):
            return self._hydrate_carry_over_items(td, existing, allow_db_query=True)

        if listen_timeout_secs <= 0.0:
            raise EmptyPlanStartupError(
                f"No LOCKED TradingPlan items found in PostgreSQL for market={self._market} "
                f"trading_date={td.isoformat()} (fail-closed)"
            )

        conn = psycopg2.connect(self._dsn, connect_timeout=5)
        try:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                cur.execute(f"LISTEN {PLAN_LOCKED_NOTIFY_CHANNEL};")
                rechecked = self._repo.load_locked_plan_items(
                    trading_date=td,
                    market=self._market,
                )
                if rechecked or self._has_locked_plan(td):
                    return self._hydrate_carry_over_items(
                        td, rechecked, allow_db_query=True
                    )

                deadline = time.monotonic() + float(listen_timeout_secs)
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0.0:
                        break
                    ready, _, _ = select.select([conn], [], [], remaining)
                    if not ready:
                        break
                    conn.poll()
                    while conn.notifies:
                        _ = conn.notifies.pop(0)
                    notified_items = self._repo.load_locked_plan_items(
                        trading_date=td,
                        market=self._market,
                    )
                    if notified_items or self._has_locked_plan(td):
                        return self._hydrate_carry_over_items(
                            td, notified_items, allow_db_query=True
                        )
        finally:
            conn.close()

        raise PlanWaitTimeoutError(
            f"Timed out waiting ({listen_timeout_secs:.1f}s) on LISTEN {PLAN_LOCKED_NOTIFY_CHANNEL} "
            f"for LOCKED TradingPlan on {td.isoformat()} (fail-closed)"
        )

    def build_live_node(
        self,
        trading_date: date | str,
        *,
        plan_items: Optional[Sequence[PlanItemContract]] = None,
        plan_gate_backend: Optional[Any] = None,
        persist_telemetry_to_db: bool = True,
        listen_timeout_secs: float = 0.0,
        use_limit_orders: bool = True,
        submit_protective_stop: bool = True,
        enforce_protective_oco_on_position_events: bool = True,
        warmup_bars_by_symbol: Optional[Mapping[str, Sequence[Bar]]] = None,
        warmup_strategies: Optional[bool] = None,
        perform_warmup: Optional[bool] = None,
    ) -> AssembledLiveNodeBundle:
        """Constructs a `LiveNode` wired with `DataInfra`, `Alpaca`, Rust `PreTradeRiskGate`, `CollaredTwapExecAlgorithm`, `TelemetryActor`, and `Strategy`s."""
        td = (
            date.fromisoformat(str(trading_date)[:10])
            if isinstance(trading_date, str)
            else trading_date
        )
        if plan_items is not None:
            locked_items = list(plan_items)
            if not locked_items and not self._has_locked_plan(td):
                raise EmptyPlanStartupError(
                    "LiveNode cannot start with an empty PlanItemContract list"
                )
        else:
            locked_items = self.load_locked_plan_items_or_wait(
                trading_date=td,
                listen_timeout_secs=listen_timeout_secs,
            )

        resolved_instruments = self.resolve_instruments_for_items(
            locked_items,
            persist_to_db=bool(persist_telemetry_to_db),
            trading_date=td,
        )
        instrument_statuses = {
            sym: entry.status for sym, entry in resolved_instruments.items()
        }
        active_items = [
            item
            for item in locked_items
            if (
                resolved_instruments.get(item.symbol) is None
                or resolved_instruments[item.symbol].status != "INSTRUMENT_UNRESOLVED"
            )
        ]

        builder = LiveNode.builder(
            self._node_name,
            TraderId(self._trader_id),
            self._environment,
        )
        builder.with_data_engine_config(LiveDataEngineConfig())
        builder.with_exec_engine_config(
            LiveExecutionEngineConfig(
                open_check_interval_secs=self._open_check_interval_secs,
                position_check_interval_secs=self._position_check_interval_secs,
            )
        )
        builder.with_risk_engine_config(
            _build_live_risk_engine_config(
                active_items,
                system_config=self._system_config,
            )
        )
        builder.with_reconciliation(self._reconciliation_enabled)
        builder.with_reconciliation_lookback_mins(self._reconciliation_lookback_mins)
        if self._cache_config is not None:
            builder.with_cache_config(self._cache_config)

        builder.add_data_client(
            None,
            DataInfraDataClientFactory(),
            self._datainfra_config,
        )

        routed_venues: List[str] = []
        if self._alpaca_config is not None:
            builder.add_exec_client(
                None,
                AlpacaExecutionClientFactory(),
                self._alpaca_config,
            )
            routed_venues.append("ALPACA")

        for extra in self._extra_exec_clients:
            builder.add_exec_client(
                extra.name,
                extra.factory,
                extra.config,
            )
            routed_venues.append(extra.venue.strip().upper())

        node = builder.build()
        node_handle = (
            node.handle()
            if not hasattr(node, "trader")
            and hasattr(node, "handle")
            and callable(node.handle)
            else None
        )
        collared_twap: Optional[CollaredTwapExecAlgorithm] = None
        telemetry_actor: Optional[PostgresTelemetryActor] = None
        pg_backend: Optional[PostgresPlanGateBackend] = None
        try:
            env_label = "LIVE" if self._environment == Environment.LIVE else "PAPER"
            pg_backend = (
                plan_gate_backend
                if plan_gate_backend is not None
                else PostgresPlanGateBackend(
                    dsn=self._dsn,
                    trading_date=td,
                    market=self._market,
                )
            )
            cache_obj = verify_rust_live_node_contract(node)
            verify_plan_instruments_ready(
                cache_obj,
                active_items,
                resolved_instruments,
                pg_backend=pg_backend,
                dsn=self._dsn,
                trading_date=td,
                venue=str(getattr(self._datainfra_config, "default_venue", "ALPACA") or "ALPACA"),
            )
            node.set_pre_trade_risk_gate(pg_backend.as_pre_trade_risk_gate())

            oco_monitor = ProtectiveOcoMonitor(
                dsn=self._dsn if persist_telemetry_to_db else None,
                broker="alpaca",
                persist_to_db=bool(persist_telemetry_to_db),
                on_entry_freeze_callback=lambda sym, reason: pg_backend.set_entry_freeze(
                    True,
                    reason=reason,
                    updated_by=f"nautilus:protective_oco_monitor:{sym}",
                ),
            )
            if hasattr(pg_backend, "register_oco_monitor"):
                try:
                    pg_backend.register_oco_monitor(oco_monitor)
                except Exception:
                    pass
            first_spec = getattr(active_items[0], "execution_spec", None) if active_items else None
            exec_cfg = (
                self._system_config.execution
                if self._system_config is not None
                else SystemConfig().execution
            )
            first_collar = (
                active_items[0].risk_envelope.collar
                if active_items
                else MarketableLimitCollar(
                    limit_collar_pct=Decimal(str(exec_cfg.limit_collar_pct)),
                    limit_collar_atr=Decimal(str(exec_cfg.limit_collar_atr)),
                )
            )
            collared_twap = CollaredTwapExecAlgorithm(
                config=CollaredTwapExecAlgorithmConfig(
                    exec_algorithm_id=COLLARED_TWAP_EXEC_ALGORITHM_ID,
                    horizon_secs=float(getattr(first_spec, "twap_horizon_secs", 900) or 900.0),
                    interval_secs=float(getattr(first_spec, "twap_interval_secs", 60) or 60.0),
                    default_collar_pct=Decimal(str(first_collar.limit_collar_pct)),
                    default_collar_atr=Decimal(str(first_collar.limit_collar_atr)),
                    fee_schedule=self._fee_schedule,
                    log_events=True,
                    log_commands=True,
                )
            )
            telemetry_actor = PostgresTelemetryActor(
                PostgresTelemetryActorConfig(
                    dsn=self._dsn if persist_telemetry_to_db else None,
                    market=self._market,
                    trading_date=td,
                    environment=env_label,
                    persist_to_db=bool(persist_telemetry_to_db),
                    enforce_protective_oco_on_position_events=bool(
                        enforce_protective_oco_on_position_events
                    ),
                    kafka_brokers=os.environ.get("KAFKA_BROKERS", "").strip() or None,
                    kafka_execution_topic=os.environ.get(
                        "KAFKA_EXECUTION_TOPIC", "exec.nautilus.events"
                    ),
                ),
                oco_monitor=oco_monitor,
                plan_gate_backend=pg_backend,
                plan_repo=self._repo,
                cancel_symbol_entries_callback=collared_twap.cancel_symbol_entries,
            )
            node.add_exec_algorithm(collared_twap)
            node.add_actor(telemetry_actor)
            if hasattr(node, "trader") and getattr(node, "trader", None) is not None:
                if hasattr(node.trader, "add_exec_algorithm"):
                    try:
                        node.trader.add_exec_algorithm(collared_twap)
                    except Exception:
                        pass
                if hasattr(node.trader, "add_actor"):
                    try:
                        node.trader.add_actor(telemetry_actor)
                    except Exception:
                        pass

            should_warmup = (
                bool(warmup_strategies)
                if warmup_strategies is not None
                else (
                    bool(perform_warmup)
                    if perform_warmup is not None
                    else self._warmup_on_build
                )
            )
            strategies: List[BaseArchetypeStrategy] = []
            for item in active_items:
                co_status = str(
                    getattr(item, "carry_over_status", None)
                    or item.sizing_trace.get("carry_over_status")
                    or ""
                ).strip().upper()
                raw_co_qty = item.sizing_trace.get("carry_over_position_qty")
                raw_co_side = str(
                    item.sizing_trace.get("carry_over_position_side") or ""
                ).strip().upper()
                has_carry_over_pos = (
                    co_status in ("RENEWED_ACTIVE", "CARRY_OVER_EXIT_ONLY", "THESIS_INVALIDATED_EXIT")
                    or raw_co_qty is not None
                    or item.filled_qty > Decimal("0")
                )
                signed_co_qty = Decimal("0")
                if has_carry_over_pos:
                    if raw_co_qty is not None:
                        signed_co_qty = _to_decimal(
                            raw_co_qty, "carry_over_position_qty"
                        )
                        if raw_co_side == "SHORT":
                            signed_co_qty = -abs(signed_co_qty)
                        elif raw_co_side == "LONG":
                            signed_co_qty = abs(signed_co_qty)
                        elif signed_co_qty > Decimal("0") and item.risk_envelope.side == PlanSide.SHORT:
                            signed_co_qty = -abs(signed_co_qty)
                    elif co_status in ("CARRY_OVER_EXIT_ONLY", "THESIS_INVALIDATED_EXIT"):
                        abs_co_qty = max(
                            item.filled_qty, item.risk_envelope.target_qty
                        )
                        signed_co_qty = (
                            abs_co_qty
                            if item.risk_envelope.side == PlanSide.LONG
                            else -abs_co_qty
                        )
                    else:
                        signed_co_qty = (
                            item.filled_qty
                            if item.risk_envelope.side == PlanSide.LONG
                            else -item.filled_qty
                        )
                abs_pos_qty = abs(signed_co_qty)
                if hasattr(pg_backend, "upsert_runtime_plan_item"):
                    if (
                        co_status == "RENEWED_ACTIVE"
                        and abs_pos_qty > Decimal("0")
                        and not bool(item.sizing_trace.get("pyramid_enabled", False))
                    ):
                        init_reserved = max(
                            item.filled_qty,
                            abs_pos_qty,
                            item.risk_envelope.approved_qty_cap,
                        )
                    else:
                        init_reserved = max(item.filled_qty, abs_pos_qty)
                    pg_backend.upsert_runtime_plan_item(
                        item,
                        reserved_qty=init_reserved,
                    )
                strategy = instantiate_strategy_from_contract(
                    item,
                    use_limit_orders=use_limit_orders,
                    submit_protective_stop=submit_protective_stop,
                )
                if hasattr(strategy, "bind_telemetry_actor"):
                    strategy.bind_telemetry_actor(telemetry_actor)
                if hasattr(strategy, "bind_warmup_loader") and (
                    should_warmup
                    or self._historical_bar_loader is not None
                    or self._explicit_datainfra_config is not None
                    or bool(warmup_bars_by_symbol)
                ):
                    weak_strat = weakref.ref(strategy)
                    weak_self = weakref.ref(self)
                    strategy.bind_warmup_loader(
                        lambda ws=weak_strat, wa=weak_self: (
                            wa().load_warmup_bars_for_strategy(ws())
                            if wa() is not None and ws() is not None
                            else []
                        )
                    )
                if warmup_bars_by_symbol and item.symbol in warmup_bars_by_symbol:
                    self.warmup_strategy_from_catalog(
                        strategy,
                        bars=warmup_bars_by_symbol[item.symbol],
                    )
                elif should_warmup:
                    self.warmup_strategy_from_catalog(strategy)

                if has_carry_over_pos and signed_co_qty != Decimal("0"):
                    strategy.set_tracked_position_signed_qty(signed_co_qty)
                    if abs_pos_qty > Decimal("0"):
                        oco_monitor.record_position_qty_sync(item.symbol, abs_pos_qty)
                if co_status in ("CARRY_OVER_EXIT_ONLY", "THESIS_INVALIDATED_EXIT"):
                    if co_status == "THESIS_INVALIDATED_EXIT":
                        strategy.trigger_thesis_invalidated_exit(
                            execute_immediately=False,
                            reason_tag="THESIS_INVALIDATED_EXIT",
                        )
                    else:
                        strategy.enter_carry_over_exit_only_mode()

                inst_status = instrument_statuses.get(item.symbol, "")
                if inst_status == "DEGRADED_CACHED_INSTRUMENT":
                    if co_status != "THESIS_INVALIDATED_EXIT":
                        strategy.enter_carry_over_exit_only_mode()
                elif inst_status == "VENUE_HALTED_FROZEN":
                    strategy._instrument_halted = True
                    if co_status != "THESIS_INVALIDATED_EXIT":
                        strategy.enter_carry_over_exit_only_mode()

                node.add_strategy(strategy)
                if hasattr(node, "trader") and getattr(node, "trader", None) is not None and hasattr(node.trader, "add_strategy"):
                    try:
                        node.trader.add_strategy(strategy)
                    except Exception:
                        pass
                strategies.append(strategy)

            self._sync_sip_feeder_subscriptions(
                active_symbols=[item.symbol for item in active_items],
                pg_backend=pg_backend,
            )

            reconciler = DailyPlanReconciler(
                plan_gate_backend=pg_backend,
                telemetry_actor=telemetry_actor,
                strategies_by_symbol={
                    item.symbol: strat for item, strat in zip(active_items, strategies)
                },
                plan_items_by_symbol={item.symbol: item for item in active_items},
                node=node,
                node_handle=node_handle,
                assembler=self,
                repository=self._repo,
                warmup_bar_loader=self._historical_bar_loader,
                use_limit_orders=use_limit_orders,
                submit_protective_stop=submit_protective_stop,
                sip_feeder_client=self._sip_feeder_client,
            )
            weak_rec = weakref.ref(reconciler)

            def _clear_reconciler_refs() -> None:
                rec = weak_rec()
                if rec is not None:
                    rec.close()

            if hasattr(pg_backend, "_close_callbacks"):
                pg_backend._close_callbacks.append(_clear_reconciler_refs)

            return AssembledLiveNodeBundle(
                node=node,
                trading_date=td,
                plan_items=tuple(active_items),
                strategies=tuple(strategies),
                plan_gate_backend=pg_backend,
                telemetry_actor=telemetry_actor,
                routed_venues=tuple(routed_venues),
                exec_algorithm=collared_twap,
                reconciler=reconciler,
                instrument_statuses=instrument_statuses,
            )
        except Exception:
            if pg_backend is not None and plan_gate_backend is None:
                try:
                    pg_backend.close()
                except Exception:
                    pass
            if telemetry_actor is not None:
                try:
                    telemetry_actor._cancel_symbol_entries_callback = None
                    telemetry_actor._plan_gate_backend = None
                    telemetry_actor._plan_repo = None
                    telemetry_actor.close()
                except Exception:
                    pass
                telemetry_actor = None
            if collared_twap is not None:
                collared_twap._schedules_by_primary.clear()
                collared_twap._primary_by_child.clear()
                collared_twap._primary_by_timer.clear()
                collared_twap._primaries_by_symbol.clear()
                collared_twap = None
            try:
                node.dispose()
            except Exception:
                pass
            node = None  # type: ignore[assignment]
            raise


class DailyPlanReconciler:
    """Stateful daily `TradingPlan` rollover reconciler (`P0-EXEC-01`) implementing the
    4-State Position & Plan Lifecycle Matrix:
    - `NEW_ACTIVE`: Pos == 0, Symbol in Today's Plan -> instantiate/mount strategy, warm up
      historical bars (`max(lookback_window * 3, 120)`), and activate in `PostgresPlanGateBackend`.
    - `RENEWED_ACTIVE`: Pos > 0, Symbol in Today's Plan -> update strategy parameters in-place
      and enforce monotonic stop-loss tightening (`new_stop >= current_stop` for LONG,
      `new_stop <= current_stop` for SHORT).
    - `CARRY_OVER_EXIT_ONLY`: Pos > 0, Symbol absent from Today's Plan -> freeze new entries
      (`status="EXPIRED"`, `filled_qty = approved_qty_cap` in backend, `exit_only_mode=True`
      on strategy) while keeping protective stops (`GTC`, `reduce_only=True`) and exit signals active.
    - `THESIS_INVALIDATED_EXIT`: Pos > 0, Symbol VETOED or explicitly invalidated -> transition
      immediately to Two-Phase Safe Exit (`begin_safe_exit` -> cancel old stop leg -> submit
      `reduce_only=True` exit order).
    """

    def __init__(
        self,
        *,
        plan_gate_backend: Optional[Any] = None,
        plan_gate: Optional[Any] = None,
        telemetry_actor: Optional[PostgresTelemetryActor | Any] = None,
        strategies_by_symbol: Optional[
            Mapping[str, BaseArchetypeStrategy] | Sequence[BaseArchetypeStrategy]
        ] = None,
        plan_items_by_symbol: Optional[
            Mapping[str, PlanItemContract] | Sequence[PlanItemContract]
        ] = None,
        node: Optional[LiveNode | Any] = None,
        node_handle: Optional[Any] = None,
        assembler: Optional[NautilusLiveNodeAssembler] = None,
        repository: Optional[PlanContractRepository] = None,
        warmup_bar_loader: Optional[Callable[[str, str, int], Sequence[Bar]]] = None,
        use_limit_orders: bool = True,
        submit_protective_stop: bool = True,
        sip_feeder_url: Optional[str] = None,
        sip_feeder_client: Optional[SipFeederSubscriptionClient] = None,
    ) -> None:
        resolved_backend = plan_gate_backend
        if resolved_backend is None and plan_gate is not None:
            resolved_backend = getattr(plan_gate, "backend", None) or getattr(
                plan_gate, "_backend", None
            )
        self._backend = resolved_backend
        self._telemetry_actor = telemetry_actor
        self._node = node
        self._node_has_trader: bool = False
        self._node_handle: Optional[Any] = node_handle
        self._node_cache: Optional[Any] = None
        if self._node is not None:
            self._node_has_trader = hasattr(self._node, "trader")
            try:
                self._node_cache = getattr(self._node, "cache", None)
            except (AttributeError, RuntimeError):
                self._node_cache = None
            if (
                self._node_handle is None
                and not self._node_has_trader
                and hasattr(self._node, "handle")
                and callable(self._node.handle)
            ):
                self._node_handle = self._node.handle()
        self._assembler = assembler
        self._repository = repository or (
            assembler.repository if assembler is not None else None
        )
        self._warmup_bar_loader = warmup_bar_loader or (
            assembler._historical_bar_loader if assembler is not None else None
        )
        self._use_limit_orders = bool(use_limit_orders)
        self._submit_protective_stop = bool(submit_protective_stop)
        self._sip_feeder_client = (
            sip_feeder_client
            or (
                assembler._sip_feeder_client
                if assembler is not None and hasattr(assembler, "_sip_feeder_client")
                else None
            )
            or SipFeederSubscriptionClient(base_url=sip_feeder_url)
        )

        self._strategies_by_symbol: Dict[str, BaseArchetypeStrategy] = {}
        if isinstance(strategies_by_symbol, Mapping):
            for sym, strat in strategies_by_symbol.items():
                self._strategies_by_symbol[str(sym).strip().upper()] = strat
        elif strategies_by_symbol is not None:
            for strat in strategies_by_symbol:
                sym = str(strat.archetype_config.instrument_id).split(".", 1)[0].strip().upper()
                self._strategies_by_symbol[sym] = strat

        self._plan_items_by_symbol: Dict[str, PlanItemContract] = {}
        if isinstance(plan_items_by_symbol, Mapping):
            for sym, item in plan_items_by_symbol.items():
                self._plan_items_by_symbol[str(sym).strip().upper()] = item
        elif plan_items_by_symbol is not None:
            for item in plan_items_by_symbol:
                self._plan_items_by_symbol[item.symbol] = item

        self._subscribed_symbols: Set[str] = set(
            SipFeederSubscriptionClient.normalize_symbols(
                list(self._plan_items_by_symbol.keys())
                + list(self._strategies_by_symbol.keys())
            )
        )
        self._trading_date: Optional[date] = getattr(
            self._backend, "trading_date", None
        )
        self._last_report: Optional[DailyPlanRolloverReport] = None
        self._owner_thread_id: int = threading.get_ident()
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._pending_node_strategies: List[BaseArchetypeStrategy] = []
        self._pending_cache_instruments: List[
            Tuple[Sequence[PlanItemContract], Mapping[str, ResolvedInstrumentEntry], date]
        ] = []
        self._deferred_sip_sync_args: Optional[Tuple[List[str], List[str]]] = None
        self._deferred_reload_stop: bool = False
        self._defer_reload_stop_during_reconcile: bool = False
        self._node_reload_required: bool = False
        self._stopped_symbols: Set[str] = set()
        self._sync_owner_thread_state()

    @property
    def node_reload_required(self) -> bool:
        return self._node_reload_required

    def close(self) -> None:
        """Clears references to LiveNode, telemetry actor, strategies, and assembler."""
        self._node = None
        self._node_handle = None
        self._node_cache = None
        self._telemetry_actor = None
        self._strategies_by_symbol.clear()
        self._stopped_symbols.clear()
        self._assembler = None
        self._event_loop = None
        self._pending_node_strategies.clear()
        self._pending_cache_instruments.clear()
        self._deferred_sip_sync_args = None
        self._deferred_reload_stop = False
        self._defer_reload_stop_during_reconcile = False

    def bind_event_loop(self, loop: Optional[asyncio.AbstractEventLoop]) -> None:
        self._event_loop = loop
        if loop is not None:
            if self._is_owner_thread() and self._node is not None:
                if self._node_cache is None:
                    try:
                        self._node_cache = getattr(self._node, "cache", None)
                    except (AttributeError, RuntimeError):
                        pass
                if self._node_handle is None:
                    self._node_has_trader = hasattr(self._node, "trader")
                    if (
                        not self._node_has_trader
                        and hasattr(self._node, "handle")
                        and callable(self._node.handle)
                    ):
                        self._node_handle = self._node.handle()
            self._owner_thread_id = threading.get_ident()

    def _is_owner_thread(self) -> bool:
        return threading.get_ident() == getattr(
            self, "_owner_thread_id", threading.get_ident()
        )

    def _mount_or_queue_strategy(self, strat: BaseArchetypeStrategy) -> None:
        if self._node is None:
            return
        if self._is_owner_thread():
            self._mount_strategy_on_node(strat)
        else:
            self._pending_node_strategies.append(strat)

    def _mount_strategy_on_node(self, strat: BaseArchetypeStrategy) -> None:
        strat._owner_thread_id = threading.get_ident()
        if self._node is None:
            return
        if hasattr(self._node, "trader") and getattr(self._node, "trader", None) is not None:
            trader = self._node.trader
            if hasattr(trader, "add_strategy"):
                try:
                    trader.add_strategy(strat)
                except Exception:
                    pass
            is_trader_running = (
                bool(
                    trader.is_running()
                    if callable(getattr(trader, "is_running", None))
                    else getattr(trader, "is_running", False)
                )
                or bool(
                    self._node.is_running()
                    if callable(getattr(self._node, "is_running", None))
                    else getattr(self._node, "is_running", False)
                )
            )
            if is_trader_running and hasattr(strat, "start"):
                try:
                    strat_running = bool(
                        strat.is_running()
                        if callable(getattr(strat, "is_running", None))
                        else getattr(strat, "is_running", False)
                    )
                    if not strat_running:
                        strat.start()
                except Exception:
                    pass
        elif hasattr(self._node, "add_strategy"):
            try:
                self._node.add_strategy(strat)
            except Exception as exc:
                self._node_reload_required = True
                logger.warning(
                    "LiveNode cannot mount strategy %s dynamically while running (%s); "
                    "requesting clean node reload for trading_date=%s",
                    getattr(strat, "id", strat),
                    exc,
                    self._trading_date,
                )
                if getattr(self, "_defer_reload_stop_during_reconcile", False):
                    self._deferred_reload_stop = True
                    return
                handle = self._node_handle
                if handle is None and self._is_owner_thread() and hasattr(self._node, "handle") and callable(self._node.handle):
                    try:
                        handle = self._node.handle()
                        self._node_handle = handle
                    except Exception as stop_exc:
                        logger.debug(
                            "LiveNode handle() failed during reload request: %s",
                            stop_exc,
                        )
                        handle = None
                if handle is not None and hasattr(handle, "stop") and callable(handle.stop):
                    try:
                        handle.stop()
                    except Exception as stop_exc:
                        logger.debug(
                            "LiveNode handle.stop() failed during reload request: %s",
                            stop_exc,
                        )

    def _sync_owner_thread_state(self) -> None:
        for strat in list(self._strategies_by_symbol.values()):
            if self._is_owner_thread():
                strat._owner_thread_id = threading.get_ident()
            if hasattr(strat, "_is_owner_thread") and strat._is_owner_thread():
                try:
                    strat._flush_cross_thread_orders()
                    strat._open_position_signed_qty()
                    strat._get_open_protective_stop_orders()
                except Exception:
                    pass
        if self._is_owner_thread() and self._node is not None:
            while self._pending_cache_instruments:
                p_items, p_resolved, p_td = self._pending_cache_instruments.pop(0)
                cache_obj = self._node_cache or verify_rust_live_node_contract(self._node)
                if cache_obj is not None:
                    self._node_cache = cache_obj
                verify_plan_instruments_ready(
                    cache_obj,
                    p_items,
                    p_resolved,
                    pg_backend=self._backend,
                    dsn=getattr(self._assembler, "dsn", None),
                    trading_date=p_td,
                )
            while self._pending_node_strategies:
                pending_strat = self._pending_node_strategies.pop(0)
                self._mount_strategy_on_node(pending_strat)

    @classmethod
    def from_bundle(
        cls,
        bundle: AssembledLiveNodeBundle,
        *,
        assembler: Optional[NautilusLiveNodeAssembler] = None,
        warmup_bar_loader: Optional[Callable[[str, str, int], Sequence[Bar]]] = None,
    ) -> "DailyPlanReconciler":
        if bundle.reconciler is not None and assembler is None and warmup_bar_loader is None:
            return bundle.reconciler
        return cls(
            plan_gate_backend=bundle.plan_gate_backend,
            telemetry_actor=bundle.telemetry_actor,
            strategies_by_symbol=bundle.strategies,
            plan_items_by_symbol=bundle.plan_items,
            node=bundle.node,
            assembler=assembler,
            warmup_bar_loader=warmup_bar_loader,
        )

    @property
    def trading_date(self) -> Optional[date]:
        return self._trading_date

    @property
    def strategies_by_symbol(self) -> Dict[str, BaseArchetypeStrategy]:
        return dict(self._strategies_by_symbol)

    @property
    def plan_items_by_symbol(self) -> Dict[str, PlanItemContract]:
        return dict(self._plan_items_by_symbol)

    @property
    def last_report(self) -> Optional[DailyPlanRolloverReport]:
        return self._last_report

    def register_strategy(
        self,
        symbol: str,
        strategy: BaseArchetypeStrategy,
        plan_item: Optional[PlanItemContract] = None,
    ) -> None:
        norm_sym = str(symbol or "").strip().upper()
        self._strategies_by_symbol[norm_sym] = strategy
        if plan_item is not None:
            self._plan_items_by_symbol[norm_sym] = plan_item
        self._sync_owner_thread_state()

    def _build_fallback_carry_over_contract(
        self,
        symbol: str,
        strategy: Optional[BaseArchetypeStrategy],
        pos_qty: Decimal,
    ) -> PlanItemContract:
        if strategy is not None:
            cfg = strategy.archetype_config
            env = strategy.risk_envelope
            if env is None:
                raise ValueError(
                    f"Cannot build carry-over contract for {symbol}: mounted Strategy has no PlanRiskEnvelope"
                )
            venue = (
                str(cfg.instrument_id).split(".", 1)[1].strip().upper()
                if "." in str(cfg.instrument_id)
                else "ALPACA"
            )
            return PlanItemContract(
                item_id=uuid.uuid4(),
                plan_id=uuid.uuid4(),
                symbol=symbol,
                venue=venue,
                broker="alpaca",
                asset_class="US_EQUITY",
                strategy_id=f"{cfg.ARCHETYPE_ID.value}-{symbol[:8]}",
                strategy_config=cfg.archetype_contract,
                risk_envelope=env,
                sizing_trace={"source": "daily_plan_reconciler_carry_over"},
                status="EXPIRED",
                filled_qty=env.approved_qty_cap,
            )
        raise ValueError(
            f"Cannot build carry-over contract for {symbol} without prior PlanItemContract or mounted Strategy"
        )

    def _warmup_new_strategy(
        self,
        symbol: str,
        strategy: BaseArchetypeStrategy,
        warmup_bars_by_symbol: Optional[Mapping[str, Sequence[Bar]]] = None,
    ) -> int:
        bars_override = (
            warmup_bars_by_symbol[symbol]
            if (warmup_bars_by_symbol and symbol in warmup_bars_by_symbol)
            else None
        )
        bars_to_feed = _load_warmup_bars_for_strategy_with_sources(
            strategy,
            bars_override=bars_override,
            historical_bar_loader=self._warmup_bar_loader,
            datainfra_config=(
                self._assembler._datainfra_config
                if self._assembler is not None
                else None
            ),
        )
        return strategy.warmup_historical_bars(bars_to_feed)

    def reconcile(
        self,
        trading_date: date,
        new_plan_items: Optional[Sequence[PlanItemContract]] = None,
        *,
        vetoed_symbols: Optional[Iterable[str]] = None,
        open_positions_by_symbol: Optional[
            Mapping[str, Decimal | float | int | str]
        ] = None,
        warmup_bars_by_symbol: Optional[Mapping[str, Sequence[Bar]]] = None,
        execute_thesis_exits_immediately: bool = False,
        pre_resolved_instruments: Optional[Mapping[str, ResolvedInstrumentEntry]] = None,
        _defer_sip_sync: bool = False,
    ) -> DailyPlanRolloverReport:
        """Reconciles open positions from Day `T-1` against Day `T`'s locked `PlanItemContract`s."""
        if (
            not self._is_owner_thread()
            and self._event_loop is not None
            and self._event_loop.is_running()
        ):
            offthread_items = list(new_plan_items) if new_plan_items is not None else None
            if (
                offthread_items is None
                and self._repository is not None
            ):
                offthread_items = self._repository.load_locked_plan_items(
                    trading_date=trading_date,
                    market=(
                        self._assembler._market
                        if self._assembler is not None
                        else "US_EQUITY"
                    ),
                )
            offthread_resolved = pre_resolved_instruments
            if (
                offthread_resolved is None
                and self._assembler is not None
                and offthread_items
            ):
                offthread_resolved = self._assembler.resolve_instruments_for_items(
                    offthread_items,
                    persist_to_db=True,
                    trading_date=trading_date,
                )

            offthread_warmup: Optional[Dict[str, Sequence[Bar]]] = (
                dict(warmup_bars_by_symbol)
                if warmup_bars_by_symbol is not None
                else None
            )
            if offthread_items:
                vetoed_upper = {
                    str(s).strip().upper()
                    for s in (vetoed_symbols or ())
                    if str(s).strip()
                }
                def _needs_warmup_offthread(it: PlanItemContract) -> bool:
                    if it.symbol in self._stopped_symbols:
                        return True
                    strat_existing = self._strategies_by_symbol.get(it.symbol)
                    if strat_existing is None:
                        return True
                    if (
                        strat_existing.archetype_config.ARCHETYPE_ID
                        != it.strategy_config.archetype_id
                    ):
                        return True
                    if (
                        getattr(strat_existing.archetype_config, "bar_timeframe", None)
                        != it.strategy_config.bar_timeframe
                    ):
                        return True
                    if (
                        open_positions_by_symbol is not None
                        and it.symbol in open_positions_by_symbol
                    ):
                        return _to_decimal(open_positions_by_symbol.get(it.symbol), "0") == Decimal("0")
                    return strat_existing._open_position_signed_qty() == Decimal("0")

                need_warmup_items = [
                    it
                    for it in offthread_items
                    if it.symbol not in vetoed_upper
                    and (offthread_warmup is None or it.symbol not in offthread_warmup)
                    and (
                        not offthread_resolved
                        or offthread_resolved.get(it.symbol) is None
                        or offthread_resolved[it.symbol].status != "INSTRUMENT_UNRESOLVED"
                    )
                    and _needs_warmup_offthread(it)
                ]
                if need_warmup_items:
                    if offthread_warmup is None:
                        offthread_warmup = {}
                    datainfra_cfg = (
                        self._assembler._datainfra_config
                        if self._assembler is not None
                        else None
                    )
                    for it in need_warmup_items:
                        offthread_warmup[it.symbol] = _load_warmup_bars_for_spec(
                            it.strategy_config.instrument_id,
                            it.strategy_config.bar_timeframe,
                            it.strategy_config.lookback_window,
                            historical_bar_loader=self._warmup_bar_loader,
                            datainfra_config=datainfra_cfg,
                        )

            fut: concurrent.futures.Future[DailyPlanRolloverReport] = (
                concurrent.futures.Future()
            )

            def _run_on_owner_loop() -> None:
                try:
                    res = self.reconcile(
                        trading_date,
                        offthread_items,
                        vetoed_symbols=vetoed_symbols,
                        open_positions_by_symbol=open_positions_by_symbol,
                        warmup_bars_by_symbol=offthread_warmup,
                        execute_thesis_exits_immediately=execute_thesis_exits_immediately,
                        pre_resolved_instruments=offthread_resolved,
                        _defer_sip_sync=True,
                    )
                    fut.set_result(res)
                except Exception as exc:
                    fut.set_exception(exc)

            self._deferred_reload_stop = False
            self._event_loop.call_soon_threadsafe(_run_on_owner_loop)
            res = fut.result(timeout=30.0)
            pending_sip = self._deferred_sip_sync_args
            self._deferred_sip_sync_args = None
            if pending_sip is not None:
                active_syms, stale_flat_syms = pending_sip
                sip_sync_result = self._sync_sip_feeder_for_reconcile(
                    active_syms, stale_flat_syms
                )
                res = replace(res, sip_feeder_subscriptions=sip_sync_result)
                self._last_report = res
            if self._deferred_reload_stop:
                self._deferred_reload_stop = False
                self._request_node_reload_stop()
            return res

        prev_defer_stop = self._defer_reload_stop_during_reconcile
        self._defer_reload_stop_during_reconcile = True
        try:
            if self._is_owner_thread():
                self._sync_owner_thread_state()

            resolved_items: List[PlanItemContract]
            if new_plan_items is not None:
                resolved_items = list(new_plan_items)
            elif self._repository is not None:
                resolved_items = self._repository.load_locked_plan_items(
                    trading_date=trading_date,
                    market=(
                        self._assembler._market
                        if self._assembler is not None
                        else "US_EQUITY"
                    ),
                )
            else:
                resolved_items = []

            resolved_inst_map: Dict[str, ResolvedInstrumentEntry] = dict(
                pre_resolved_instruments or {}
            )
            if not resolved_inst_map and self._assembler is not None and resolved_items:
                resolved_inst_map = self._assembler.resolve_instruments_for_items(
                    resolved_items,
                    persist_to_db=True,
                    trading_date=trading_date,
                )

            verifiable_items = [
                it
                for it in resolved_items
                if resolved_inst_map.get(it.symbol) is None
                or resolved_inst_map[it.symbol].status != "INSTRUMENT_UNRESOLVED"
            ]
            needs_cache_verification = bool(resolved_inst_map) or (
                bool(verifiable_items) and self._assembler is not None
            )
            if self._is_owner_thread() and self._node is not None and needs_cache_verification:
                live_cache = self._node_cache or verify_rust_live_node_contract(self._node)
                if live_cache is not None:
                    self._node_cache = live_cache
                verify_plan_instruments_ready(
                    live_cache,
                    verifiable_items,
                    resolved_inst_map,
                    pg_backend=self._backend,
                    dsn=getattr(self._assembler, "dsn", None),
                    trading_date=trading_date,
                )
            elif not self._is_owner_thread() and self._node is not None and needs_cache_verification:
                self._pending_cache_instruments.append(
                    (list(verifiable_items), dict(resolved_inst_map), trading_date)
                )

            vetoed_set: Set[str] = {
                str(s).strip().upper() for s in (vetoed_symbols or ()) if str(s).strip()
            }
            new_items_by_symbol: Dict[str, PlanItemContract] = {}
            for item in resolved_items:
                sym = item.symbol
                status_upper = str(item.status or "").strip().upper()
                is_vetoed = (
                    sym in vetoed_set
                    or status_upper in ("VETOED", "INVALIDATED", "THESIS_INVALIDATED")
                    or bool(item.sizing_trace.get("vetoed", False))
                    or bool(item.sizing_trace.get("thesis_invalidated", False))
                )
                if is_vetoed:
                    vetoed_set.add(sym)
                else:
                    new_items_by_symbol[sym] = item

            # Snapshot prior items and prior trading_date before hot-reloading backend trading_date
            prior_trading_date = self._trading_date
            prior_items_by_symbol: Dict[str, PlanItemContract] = dict(
                self._plan_items_by_symbol
            )
            if self._backend is not None and hasattr(self._backend, "get_plan_item"):
                for sym in list(self._strategies_by_symbol.keys()) + list(vetoed_set):
                    if sym not in prior_items_by_symbol:
                        existing_item = self._backend.get_plan_item(sym)
                        if existing_item is not None:
                            prior_items_by_symbol[sym] = existing_item

            # Determine open positions from T-1
            pos_by_symbol: Dict[str, Decimal] = {}
            for sym, strat in self._strategies_by_symbol.items():
                signed_qty = strat._open_position_signed_qty()
                if signed_qty != Decimal("0"):
                    pos_by_symbol[sym] = signed_qty

            if open_positions_by_symbol is not None:
                for raw_sym, raw_qty in open_positions_by_symbol.items():
                    norm_sym = str(raw_sym).strip().upper()
                    qty_dec = _to_decimal(raw_qty, "0")
                    if qty_dec != Decimal("0"):
                        pos_by_symbol[norm_sym] = qty_dec
                        strat = self._strategies_by_symbol.get(norm_sym)
                        if strat is not None and strat._open_position_signed_qty() == Decimal("0"):
                            strat.set_tracked_position_signed_qty(qty_dec)
                    else:
                        pos_by_symbol.pop(norm_sym, None)
                        strat = self._strategies_by_symbol.get(norm_sym)
                        if strat is not None:
                            strat.set_tracked_position_signed_qty(Decimal("0"))

            self._trading_date = trading_date

            if self._backend is not None and hasattr(self._backend, "set_trading_date"):
                self._backend.set_trading_date(
                    trading_date,
                    clear_runtime_items=True,
                    sync=hasattr(self._backend, "sync_from_db") and new_plan_items is None,
                )

            all_symbols = sorted(
                set(self._strategies_by_symbol.keys())
                | set(pos_by_symbol.keys())
                | set(new_items_by_symbol.keys())
                | vetoed_set
            )
            decisions: List[SymbolRolloverDecision] = []
            newly_mounted_symbols: Set[str] = set()
            _defer_reload_only = (
                not self._is_owner_thread()
                and self._node is not None
                and not self._node_has_trader
            )

            for sym in all_symbols:
                pos_qty = pos_by_symbol.get(sym, Decimal("0"))
                has_open_pos = pos_qty != Decimal("0")
                strat = self._strategies_by_symbol.get(sym)
                prior_item = prior_items_by_symbol.get(sym)

                # State 4: THESIS_INVALIDATED_EXIT (Pos > 0 and Symbol VETOED/invalidated or side-flipped)
                planned_item_for_sym = new_items_by_symbol.get(sym)
                side_flipped_open = (
                    has_open_pos
                    and planned_item_for_sym is not None
                    and (
                        (
                            pos_qty > Decimal("0")
                            and planned_item_for_sym.risk_envelope.side == PlanSide.SHORT
                        )
                        or (
                            pos_qty < Decimal("0")
                            and planned_item_for_sym.risk_envelope.side == PlanSide.LONG
                        )
                    )
                )
                if (sym in vetoed_set or side_flipped_open) and has_open_pos:
                    if (strat is None or sym in self._stopped_symbols) and prior_item is not None:
                        newly_mounted_symbols.add(sym)
                        if not _defer_reload_only:
                            strat = instantiate_strategy_from_contract(
                                prior_item,
                                use_limit_orders=self._use_limit_orders,
                                submit_protective_stop=self._submit_protective_stop,
                            )
                            if self._telemetry_actor is not None:
                                strat.bind_telemetry_actor(self._telemetry_actor)
                            strat.set_tracked_position_signed_qty(pos_qty)
                            self._mount_or_queue_strategy(strat)
                            self._strategies_by_symbol[sym] = strat
                            self._stopped_symbols.discard(sym)

                    inst_entry = resolved_inst_map.get(sym)
                    if inst_entry is not None:
                        if inst_entry.status == "DEGRADED_CACHED_INSTRUMENT" and strat is not None:
                            strat.enter_carry_over_exit_only_mode()
                        elif inst_entry.status == "VENUE_HALTED_FROZEN" and strat is not None:
                            strat._instrument_halted = True
                            strat.enter_carry_over_exit_only_mode()

                    try:
                        base_contract = prior_item or self._build_fallback_carry_over_contract(
                            sym, strat, pos_qty
                        )
                    except ValueError as contract_exc:
                        logger.critical(
                            "Cannot isolate State-4 THESIS_INVALIDATED_EXIT carry-over for %s (missing contract and mounted strategy): %s",
                            sym,
                            contract_exc,
                        )
                        _emit_critical_instrument_alert(
                            getattr(self._backend, "dsn", None),
                            event_type="CARRY_OVER_CONTRACT_MISSING",
                            symbol=sym,
                            title=f"Missing State-4 carry-over contract for open position {sym}",
                            body=str(contract_exc),
                            payload={
                                "symbol": sym,
                                "pos_qty": str(pos_qty),
                                "trading_date": trading_date.isoformat(),
                                "state": "THESIS_INVALIDATED_EXIT",
                            },
                        )
                        if strat is not None:
                            strat.trigger_thesis_invalidated_exit(
                                execute_immediately=execute_thesis_exits_immediately,
                                reason_tag="THESIS_INVALIDATED_EXIT",
                            )
                        continue
                    invalidated_env = replace(
                        base_contract.risk_envelope,
                        approved_qty_cap=Decimal("0"),
                    )
                    invalidated_item = replace(
                        base_contract,
                        risk_envelope=invalidated_env,
                        status="EXPIRED",
                        filled_qty=abs(pos_qty),
                    )
                    if self._backend is not None and hasattr(
                        self._backend, "upsert_runtime_plan_item"
                    ):
                        self._backend.upsert_runtime_plan_item(
                            invalidated_item,
                            reserved_qty=abs(pos_qty),
                        )
                    self._plan_items_by_symbol[sym] = invalidated_item

                    exit_cid: Optional[str] = None
                    eff_stop: Optional[Decimal] = (
                        invalidated_item.risk_envelope.initial_stop_price
                    )
                    if strat is not None:
                        eff_stop = strat._current_tightest_stop_price() or eff_stop
                        exit_cid = strat.trigger_thesis_invalidated_exit(
                            execute_immediately=execute_thesis_exits_immediately,
                            reason_tag="THESIS_INVALIDATED_EXIT",
                        )

                    decisions.append(
                        SymbolRolloverDecision(
                            symbol=sym,
                            state=RolloverLifecycleState.THESIS_INVALIDATED_EXIT,
                            previous_position_qty=pos_qty,
                            plan_item=invalidated_item,
                            effective_stop_price=eff_stop,
                            warmup_bars_fed=0,
                            safe_exit_triggered=(strat is not None),
                            exit_order_id=exit_cid,
                            rationale=(
                                f"{sym} carries open position ({pos_qty}) and was VETOED/invalidated "
                                f"on {trading_date.isoformat()} -> THESIS_INVALIDATED_EXIT (Two-Phase Safe Exit)"
                            ),
                        )
                    )
                    continue

                # State 2: RENEWED_ACTIVE (Pos > 0 and Symbol in Today's Plan)
                if sym in new_items_by_symbol and has_open_pos:
                    new_item = new_items_by_symbol[sym]
                    can_reuse_strat = (
                        strat is not None
                        and sym not in self._stopped_symbols
                        and strat.archetype_config.ARCHETYPE_ID
                        == new_item.strategy_config.archetype_id
                        and getattr(strat.archetype_config, "bar_timeframe", None)
                        == new_item.strategy_config.bar_timeframe
                        and strat.archetype_config.plan_side
                        == new_item.risk_envelope.side
                    )
                    if can_reuse_strat:
                        if strat._open_position_signed_qty() == Decimal("0"):
                            strat.set_tracked_position_signed_qty(pos_qty)
                        renewed_env = strat.apply_renewed_plan_item(new_item)
                    else:
                        strat_tightest = (
                            strat._current_tightest_stop_price()
                            if strat is not None
                            else None
                        )
                        prior_stop = (
                            strat_tightest
                            if (strat_tightest is not None and strat_tightest > Decimal("0"))
                            else (
                                prior_item.risk_envelope.initial_stop_price
                                if prior_item is not None
                                else Decimal("0")
                            )
                        )
                        if prior_stop > Decimal("0"):
                            if new_item.risk_envelope.side == PlanSide.LONG:
                                tightened_stop = max(
                                    prior_stop, new_item.risk_envelope.initial_stop_price
                                )
                            else:
                                tightened_stop = min(
                                    prior_stop, new_item.risk_envelope.initial_stop_price
                                )
                            renewed_tp = BaseArchetypeStrategy._clear_crossed_take_profit(
                                new_item.risk_envelope.side,
                                tightened_stop,
                                new_item.risk_envelope.take_profit_price,
                            )
                            renewed_env = replace(
                                new_item.risk_envelope,
                                initial_stop_price=tightened_stop,
                                take_profit_price=renewed_tp,
                            )
                        else:
                            renewed_env = new_item.risk_envelope
                        newly_mounted_symbols.add(sym)
                        if not _defer_reload_only:
                            strat = instantiate_strategy_from_contract(
                                replace(new_item, risk_envelope=renewed_env),
                                use_limit_orders=self._use_limit_orders,
                                submit_protective_stop=self._submit_protective_stop,
                            )
                            if self._telemetry_actor is not None:
                                strat.bind_telemetry_actor(self._telemetry_actor)
                            strat.set_tracked_position_signed_qty(pos_qty)
                            self._mount_or_queue_strategy(strat)
                            self._strategies_by_symbol[sym] = strat
                            self._stopped_symbols.discard(sym)

                    inst_entry = resolved_inst_map.get(sym)
                    if inst_entry is not None:
                        if inst_entry.status == "DEGRADED_CACHED_INSTRUMENT" and strat is not None:
                            strat.enter_carry_over_exit_only_mode()
                        elif inst_entry.status == "VENUE_HALTED_FROZEN" and strat is not None:
                            strat._instrument_halted = True
                            strat.enter_carry_over_exit_only_mode()

                    carried_reserved = max(new_item.filled_qty, abs(pos_qty))
                    renewed_item = replace(
                        new_item,
                        risk_envelope=renewed_env,
                        filled_qty=carried_reserved,
                    )
                    pyramid_enabled = bool(
                        renewed_item.sizing_trace.get("pyramid_enabled", False)
                    )
                    effective_reserved_qty = (
                        carried_reserved
                        if pyramid_enabled
                        else max(carried_reserved, renewed_env.approved_qty_cap)
                    )
                    if self._backend is not None and hasattr(
                        self._backend, "upsert_runtime_plan_item"
                    ):
                        self._backend.upsert_runtime_plan_item(
                            renewed_item,
                            reserved_qty=effective_reserved_qty,
                        )
                    self._plan_items_by_symbol[sym] = renewed_item

                    decisions.append(
                        SymbolRolloverDecision(
                            symbol=sym,
                            state=RolloverLifecycleState.RENEWED_ACTIVE,
                            previous_position_qty=pos_qty,
                            plan_item=renewed_item,
                            effective_stop_price=renewed_env.initial_stop_price,
                            warmup_bars_fed=0,
                            safe_exit_triggered=False,
                            rationale=(
                                f"{sym} carries open position ({pos_qty}) and is renewed in "
                                f"{trading_date.isoformat()} plan -> RENEWED_ACTIVE "
                                f"(monotonic stop={renewed_env.initial_stop_price})"
                            ),
                        )
                    )
                    continue

                # State 3: CARRY_OVER_EXIT_ONLY (Pos > 0 and Symbol NOT in Today's Plan)
                if sym not in new_items_by_symbol and has_open_pos:
                    if (strat is None or sym in self._stopped_symbols) and prior_item is not None and not _defer_reload_only:
                        strat = instantiate_strategy_from_contract(
                            prior_item,
                            use_limit_orders=self._use_limit_orders,
                            submit_protective_stop=self._submit_protective_stop,
                        )
                        if self._telemetry_actor is not None:
                            strat.bind_telemetry_actor(self._telemetry_actor)
                        strat.set_tracked_position_signed_qty(pos_qty)
                        self._mount_or_queue_strategy(strat)
                        self._strategies_by_symbol[sym] = strat
                        self._stopped_symbols.discard(sym)

                    inst_entry = resolved_inst_map.get(sym)
                    if inst_entry is not None:
                        if inst_entry.status == "DEGRADED_CACHED_INSTRUMENT" and strat is not None:
                            strat.enter_carry_over_exit_only_mode()
                        elif inst_entry.status == "VENUE_HALTED_FROZEN" and strat is not None:
                            strat._instrument_halted = True
                            strat.enter_carry_over_exit_only_mode()

                    try:
                        base_contract = prior_item or self._build_fallback_carry_over_contract(
                            sym, strat, pos_qty
                        )
                    except ValueError as contract_exc:
                        logger.critical(
                            "Cannot build State-3 CARRY_OVER_EXIT_ONLY contract for %s (missing contract and mounted strategy): %s",
                            sym,
                            contract_exc,
                        )
                        _emit_critical_instrument_alert(
                            getattr(self._backend, "dsn", None),
                            event_type="CARRY_OVER_CONTRACT_MISSING",
                            symbol=sym,
                            title=f"Missing State-3 carry-over contract for open position {sym}",
                            body=str(contract_exc),
                            payload={
                                "symbol": sym,
                                "pos_qty": str(pos_qty),
                                "trading_date": trading_date.isoformat(),
                                "state": "CARRY_OVER_EXIT_ONLY",
                            },
                        )
                        if strat is not None:
                            strat.enter_carry_over_exit_only_mode()
                        continue
                    eff_stop = base_contract.risk_envelope.initial_stop_price
                    if strat is not None:
                        strat.enter_carry_over_exit_only_mode()
                        tightest = strat._current_tightest_stop_price()
                        if tightest is not None and tightest > Decimal("0"):
                            eff_stop = tightest

                    carry_tp = BaseArchetypeStrategy._clear_crossed_take_profit(
                        base_contract.risk_envelope.side,
                        eff_stop,
                        base_contract.risk_envelope.take_profit_price,
                    )
                    carry_env = replace(
                        base_contract.risk_envelope,
                        initial_stop_price=eff_stop,
                        take_profit_price=carry_tp,
                        approved_qty_cap=Decimal("0"),
                    )
                    carry_over_item = replace(
                        base_contract,
                        risk_envelope=carry_env,
                        status="EXPIRED",
                        filled_qty=abs(pos_qty),
                    )
                    if self._backend is not None and hasattr(
                        self._backend, "upsert_runtime_plan_item"
                    ):
                        self._backend.upsert_runtime_plan_item(
                            carry_over_item,
                            reserved_qty=abs(pos_qty),
                        )
                    self._plan_items_by_symbol[sym] = carry_over_item

                    decisions.append(
                        SymbolRolloverDecision(
                            symbol=sym,
                            state=RolloverLifecycleState.CARRY_OVER_EXIT_ONLY,
                            previous_position_qty=pos_qty,
                            plan_item=carry_over_item,
                            effective_stop_price=eff_stop,
                            warmup_bars_fed=0,
                            safe_exit_triggered=False,
                            rationale=(
                                f"{sym} carries open position ({pos_qty}) and is absent from "
                                f"{trading_date.isoformat()} plan -> CARRY_OVER_EXIT_ONLY "
                                f"(new entries blocked, protective stop={eff_stop} active)"
                            ),
                        )
                    )
                    continue

                # State 1: NEW_ACTIVE (Pos == 0 and Symbol in Today's Plan)
                if sym in new_items_by_symbol and not has_open_pos:
                    inst_entry = resolved_inst_map.get(sym)
                    if inst_entry is not None and inst_entry.status == "INSTRUMENT_UNRESOLVED":
                        logger.warning(
                            "Isolating NEW_ENTRY %s during rollover: INSTRUMENT_UNRESOLVED (%s)",
                            sym,
                            inst_entry.reason,
                        )
                        self._plan_items_by_symbol.pop(sym, None)
                        continue
                    new_item = new_items_by_symbol[sym]
                    warmup_fed = 0
                    can_reuse_flat_strat = (
                        strat is not None
                        and sym not in self._stopped_symbols
                        and strat.archetype_config.ARCHETYPE_ID
                        == new_item.strategy_config.archetype_id
                        and getattr(strat.archetype_config, "bar_timeframe", None)
                        == new_item.strategy_config.bar_timeframe
                    )
                    if not can_reuse_flat_strat:
                        newly_mounted_symbols.add(sym)
                    if not _defer_reload_only:
                        if not can_reuse_flat_strat:
                            strat = instantiate_strategy_from_contract(
                                new_item,
                                use_limit_orders=self._use_limit_orders,
                                submit_protective_stop=self._submit_protective_stop,
                            )
                            if self._telemetry_actor is not None and hasattr(
                                strat, "bind_telemetry_actor"
                            ):
                                strat.bind_telemetry_actor(self._telemetry_actor)
                            if self._assembler is not None and hasattr(
                                strat, "bind_warmup_loader"
                            ):
                                strat.bind_warmup_loader(
                                    lambda s=strat: self._assembler.load_warmup_bars_for_strategy(s)
                                )
                            self._mount_or_queue_strategy(strat)
                            self._strategies_by_symbol[sym] = strat
                            self._stopped_symbols.discard(sym)
                        else:
                            strat.apply_renewed_plan_item(
                                new_item,
                                enforce_monotonic_stop=False,
                            )
                            if hasattr(strat, "reset_rolling_windows"):
                                strat.reset_rolling_windows()

                        warmup_fed = self._warmup_new_strategy(
                            sym,
                            strat,
                            warmup_bars_by_symbol=warmup_bars_by_symbol,
                        )
                    if self._backend is not None and hasattr(
                        self._backend, "upsert_runtime_plan_item"
                    ):
                        self._backend.upsert_runtime_plan_item(
                            new_item,
                            reserved_qty=new_item.filled_qty,
                        )
                    self._plan_items_by_symbol[sym] = new_item

                    decisions.append(
                        SymbolRolloverDecision(
                            symbol=sym,
                            state=RolloverLifecycleState.NEW_ACTIVE,
                            previous_position_qty=Decimal("0"),
                            plan_item=new_item,
                            effective_stop_price=new_item.risk_envelope.initial_stop_price,
                            warmup_bars_fed=warmup_fed,
                            safe_exit_triggered=False,
                            rationale=(
                                f"{sym} has 0 position and is newly active in "
                                f"{trading_date.isoformat()} plan -> NEW_ACTIVE "
                                f"(warmup_bars_fed={warmup_fed})"
                            ),
                        )
                    )
                    continue

                # Flat position and absent/vetoed in Today's Plan: freeze any residual strategy entry
                if strat is not None:
                    strat.enter_carry_over_exit_only_mode()
                    if self._is_owner_thread() and hasattr(strat, "on_stop"):
                        try:
                            strat.on_stop()
                            self._stopped_symbols.add(sym)
                        except Exception:
                            pass
                if self._backend is not None and hasattr(
                    self._backend, "remove_runtime_plan_item"
                ):
                    self._backend.remove_runtime_plan_item(sym)
                self._plan_items_by_symbol.pop(sym, None)

            if self._telemetry_actor is not None and hasattr(
                self._telemetry_actor, "set_trading_date"
            ):
                self._telemetry_actor.set_trading_date(trading_date)

            if self._is_owner_thread():
                self._sync_owner_thread_state()

            active_syms = [d.symbol for d in decisions]
            prior_tracked = (
                set(prior_items_by_symbol.keys())
                | set(self._subscribed_symbols)
            )
            stale_flat_syms = sorted(
                prior_tracked
                - set(SipFeederSubscriptionClient.normalize_symbols(active_syms))
            )
            if _defer_sip_sync:
                self._deferred_sip_sync_args = (active_syms, stale_flat_syms)
                sip_sync_result = None
            else:
                self._deferred_sip_sync_args = None
                sip_sync_result = self._sync_sip_feeder_for_reconcile(
                    active_syms, stale_flat_syms
                )

            report = DailyPlanRolloverReport(
                trading_date=trading_date,
                decisions=tuple(decisions),
                sip_feeder_subscriptions=sip_sync_result,
            )
            self._last_report = report
            should_request_reload = self._deferred_reload_stop or (
                (prior_trading_date is not None and trading_date != prior_trading_date)
                or any(
                    d.state
                    in (
                        RolloverLifecycleState.NEW_ACTIVE,
                        RolloverLifecycleState.RENEWED_ACTIVE,
                        RolloverLifecycleState.THESIS_INVALIDATED_EXIT,
                    )
                    and d.symbol in newly_mounted_symbols
                    for d in decisions
                )
            )
            if (
                self._node is not None
                and not self._node_has_trader
                and (
                    self._node_handle is not None
                    or (
                        self._is_owner_thread()
                        and hasattr(self._node, "handle")
                        and callable(self._node.handle)
                    )
                    or (
                        self._event_loop is not None
                        and not self._event_loop.is_closed()
                        and self._event_loop.is_running()
                    )
                )
                and should_request_reload
            ):
                if _defer_sip_sync:
                    self._deferred_reload_stop = True
                else:
                    self._deferred_reload_stop = False
                    self._request_node_reload_stop()
            elif not _defer_sip_sync:
                self._deferred_reload_stop = False
            return report
        finally:
            self._defer_reload_stop_during_reconcile = prev_defer_stop

    def _sync_sip_feeder_for_reconcile(
        self,
        active_syms: List[str],
        stale_flat_syms: List[str],
    ) -> Dict[str, Any]:
        try:
            sip_sync_result = self._sip_feeder_client.sync_plan_subscriptions(
                active_symbols=active_syms,
                remove_symbols=stale_flat_syms,
            )
            self._subscribed_symbols = set(
                SipFeederSubscriptionClient.normalize_symbols(active_syms)
            )
            return sip_sync_result
        except Exception as exc:
            logger.error(
                "Failed to synchronize sip-feeder subscriptions during rollover (active=%s, remove=%s): %s",
                active_syms,
                stale_flat_syms,
                exc,
            )
            if self._backend is not None and hasattr(self._backend, "set_entry_freeze"):
                try:
                    self._backend.set_entry_freeze(
                        True,
                        reason=f"SIP_FEEDER_SUBSCRIPTION_SYNC_FAILED: {exc}",
                        updated_by="nautilus:sip_feeder_sync",
                    )
                except Exception:
                    pass
            _record_live_node_critical_alert(
                getattr(self._assembler, "dsn", None),
                event_type="SIP_FEEDER_SUBSCRIPTION_SYNC_FAILED",
                title="sip-feeder Rollover Subscription Synchronization Failed",
                body=f"SIP_FEEDER_SUBSCRIPTION_SYNC_FAILED: {exc}",
                payload={
                    "active_symbols": list(active_syms),
                    "remove_symbols": list(stale_flat_syms),
                    "error": str(exc),
                },
                dedup_key=f"SIP_FEEDER_SUBSCRIPTION_SYNC_FAILED:rollover:{self._trading_date.isoformat() if self._trading_date else 'na'}",
            )
            return {
                "status": "ERROR",
                "error": str(exc),
                "added": SipFeederSubscriptionClient.normalize_symbols(active_syms),
                "removed": stale_flat_syms,
            }

    def _request_node_reload_stop(self) -> None:
        self._node_reload_required = True
        handle = self._node_handle
        if handle is None and self._is_owner_thread() and self._node is not None:
            if hasattr(self._node, "handle") and callable(self._node.handle):
                try:
                    handle = self._node.handle()
                    self._node_handle = handle
                except Exception as stop_exc:
                    logger.debug(
                        "LiveNode handle() failed during rollover reload request: %s",
                        stop_exc,
                    )
                    return
        if handle is not None and hasattr(handle, "stop") and callable(handle.stop):
            if (
                not self._is_owner_thread()
                and self._event_loop is not None
                and not self._event_loop.is_closed()
                and self._event_loop.is_running()
            ):
                self._event_loop.call_soon_threadsafe(handle.stop)
            else:
                try:
                    handle.stop()
                except Exception as stop_exc:
                    logger.debug(
                        "LiveNode handle.stop() failed during rollover reload request: %s",
                        stop_exc,
                    )
        elif (
            not self._is_owner_thread()
            and self._node is not None
            and self._event_loop is not None
            and not self._event_loop.is_closed()
            and self._event_loop.is_running()
        ):

            def _stop_on_main_loop() -> None:
                try:
                    h = self._node_handle or (
                        self._node.handle()
                        if self._node is not None
                        and hasattr(self._node, "handle")
                        and callable(self._node.handle)
                        else None
                    )
                    if h is not None and hasattr(h, "stop") and callable(h.stop):
                        h.stop()
                except Exception as stop_exc:
                    logger.debug(
                        "LiveNode handle.stop() failed on main loop: %s",
                        stop_exc,
                    )

            self._event_loop.call_soon_threadsafe(_stop_on_main_loop)


class DailyPlanRolloverListener:
    """Event-driven PostgreSQL `LISTEN nautilus_plan_locked` listener (`D6.4` compliant,
    zero blind polling) that triggers `DailyPlanReconciler.reconcile(...)` when a new daily
    `TradingPlan` is locked in PostgreSQL.
    """

    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        reconciler: DailyPlanReconciler,
        repository: Optional[PlanContractRepository] = None,
        market: str = "US_EQUITY",
        channel: str = PLAN_LOCKED_NOTIFY_CHANNEL,
        connect_timeout_secs: int = 5,
        on_rollover_callback: Optional[Callable[[DailyPlanRolloverReport], None]] = None,
    ) -> None:
        self._dsn = resolve_database_dsn(dsn)
        self._reconciler = reconciler
        self._market = str(market or "US_EQUITY").strip().upper()
        self._repository = repository or PlanContractRepository(dsn=self._dsn)
        self._channel = str(channel or PLAN_LOCKED_NOTIFY_CHANNEL).strip()
        self._connect_timeout_secs = max(1, int(connect_timeout_secs))
        self._on_rollover_callback = on_rollover_callback
        self._conn: Optional[Any] = None
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._wake_r, self._wake_w = os.pipe()
        os.set_blocking(self._wake_r, False)
        os.set_blocking(self._wake_w, False)
        self._pipe_closed = False
        self._last_processed_plan_id: Optional[str] = self._infer_reconciler_plan_id()
        self._last_processed_locked_at: Optional[str] = self._infer_reconciler_locked_at()
        self._connected_once: bool = False
        self._pending_missed_plan_date: Optional[date] = None

    def _infer_reconciler_plan_id(self) -> Optional[str]:
        items_map = getattr(self._reconciler, "_plan_items_by_symbol", None)
        if isinstance(items_map, Mapping) and items_map:
            first_item = next(iter(items_map.values()), None)
            pid = getattr(first_item, "plan_id", None)
            if pid is not None:
                return str(pid)
        rec_td = getattr(self._reconciler, "trading_date", None)
        load_meta = getattr(self._repository, "load_locked_plan_metadata", None)
        if rec_td is not None and callable(load_meta):
            try:
                meta = load_meta(trading_date=rec_td, market=self._market)
                if meta is not None and meta[0]:
                    return str(meta[0])
            except Exception:
                pass
        return None

    def _infer_reconciler_locked_at(self) -> Optional[str]:
        rec_td = getattr(self._reconciler, "trading_date", None)
        load_meta = getattr(self._repository, "load_locked_plan_metadata", None)
        if rec_td is not None and callable(load_meta):
            try:
                meta = load_meta(trading_date=rec_td, market=self._market)
                if meta is not None and meta[1] is not None:
                    return str(meta[1])
            except Exception:
                pass
        return None

    @property
    def reconciler(self) -> DailyPlanReconciler:
        return self._reconciler

    def _ensure_listen_conn(self) -> Any:
        if self._conn is None or getattr(self._conn, "closed", 1) != 0:
            conn = psycopg2.connect(
                self._dsn,
                connect_timeout=self._connect_timeout_secs,
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                cur.execute(f"LISTEN {self._channel};")
                latest_row = None
                try:
                    cur.execute(
                        """
                        SELECT plan_id, trading_date, locked_at
                        FROM trading_plans
                        WHERE (
                            market = %s
                            OR (%s IN ('US', 'US_EQUITY') AND market IN ('US', 'US_EQUITY'))
                        )
                          AND status = 'LOCKED'
                        ORDER BY trading_date DESC, locked_at DESC NULLS LAST
                        LIMIT 1
                        """,
                        (self._market, self._market),
                    )
                    latest_row = cur.fetchone()
                except Exception as check_exc:
                    logger.debug(
                        "Rollover listener missed-plan check skipped: %s",
                        check_exc,
                    )
                if latest_row:
                    r_pid = str(
                        latest_row[0]
                        if isinstance(latest_row, tuple)
                        else latest_row.get("plan_id")
                        or ""
                    )
                    raw_dt = (
                        latest_row[1]
                        if isinstance(latest_row, tuple)
                        else latest_row.get("trading_date")
                    )
                    r_date = (
                        raw_dt
                        if isinstance(raw_dt, date)
                        else (
                            date.fromisoformat(str(raw_dt)[:10])
                            if raw_dt is not None
                            else None
                        )
                    )
                    raw_locked = (
                        latest_row[2]
                        if isinstance(latest_row, tuple)
                        else latest_row.get("locked_at")
                    )
                    r_locked_str = str(raw_locked) if raw_locked is not None else None
                    cur_rec_date = getattr(self._reconciler, "trading_date", None)
                    is_missed = False
                    if r_date is not None:
                        if cur_rec_date is None or r_date > cur_rec_date:
                            is_missed = True
                        elif r_date == cur_rec_date:
                            if (
                                self._last_processed_plan_id is not None
                                and r_pid
                                and r_pid != self._last_processed_plan_id
                            ):
                                is_missed = True
                            elif (
                                self._connected_once
                                and self._last_processed_locked_at is not None
                                and r_locked_str != self._last_processed_locked_at
                            ):
                                is_missed = True
                    if is_missed and r_date is not None:
                        self._pending_missed_plan_date = r_date
                    elif not self._connected_once and r_date is not None:
                        if r_pid:
                            self._last_processed_plan_id = r_pid
                        if r_locked_str is not None:
                            self._last_processed_locked_at = r_locked_str
            self._connected_once = True
            self._conn = conn
        return self._conn

    def _resolve_notified_trading_date(self, payload: str) -> Optional[date]:
        text = str(payload or "").strip()
        if text:
            if text.startswith("{"):
                try:
                    obj = json.loads(text)
                    raw_dt = obj.get("trading_date") or obj.get("plan_date")
                    if raw_dt:
                        return date.fromisoformat(str(raw_dt).strip()[:10])
                except Exception:
                    pass
            try:
                return date.fromisoformat(text[:10])
            except Exception:
                pass

        conn = self._ensure_listen_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trading_date
                FROM trading_plans
                WHERE market = %s AND status = 'LOCKED'
                ORDER BY trading_date DESC, locked_at DESC NULLS LAST
                LIMIT 1
                """,
                (self._market,),
            )
            row = cur.fetchone()
            if row:
                val = row[0] if isinstance(row, tuple) else row.get("trading_date")
                if isinstance(val, date):
                    return val
                if val is not None:
                    return date.fromisoformat(str(val)[:10])
        return self._reconciler.trading_date

    def _extract_vetoed_symbols_for_rollover(
        self,
        payload: str,
        target_date: date,
    ) -> List[str]:
        vetoed: List[str] = []

        def _collect_from_mapping(obj: Mapping[str, Any]) -> None:
            for raw_sym in obj.get("vetoed_symbols") or ():
                sym = str(raw_sym or "").strip().upper()
                if sym and sym not in vetoed:
                    vetoed.append(sym)
            for co in obj.get("carry_over_decisions") or ():
                if not isinstance(co, Mapping):
                    continue
                sym = str(co.get("symbol") or "").strip().upper()
                dec = str(
                    co.get("decision")
                    or co.get("lifecycle_state")
                    or co.get("action")
                    or ""
                ).strip().upper()
                if sym and dec == "THESIS_INVALIDATED_EXIT" and sym not in vetoed:
                    vetoed.append(sym)

        text = str(payload or "").strip()
        if text.startswith("{"):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, Mapping):
                    _collect_from_mapping(parsed)
            except Exception:
                pass

        if hasattr(self._repository, "load_latest_plan_lock_event_payload"):
            try:
                db_payload = self._repository.load_latest_plan_lock_event_payload(
                    trading_date=target_date,
                    market=self._market,
                )
                if isinstance(db_payload, Mapping):
                    _collect_from_mapping(db_payload)
            except Exception:
                pass
        return vetoed

    def _process_rollover_for_date(
        self,
        target_date: date,
        raw_payload: str = "",
    ) -> DailyPlanRolloverReport:
        items = self._repository.load_locked_plan_items(
            trading_date=target_date,
            market=self._market,
        )
        vetoed_symbols = self._extract_vetoed_symbols_for_rollover(
            payload=raw_payload,
            target_date=target_date,
        )
        pre_resolved: Optional[Dict[str, ResolvedInstrumentEntry]] = None
        if items and getattr(self._reconciler, "_assembler", None) is not None:
            pre_resolved = self._reconciler._assembler.resolve_instruments_for_items(
                items,
                persist_to_db=True,
                trading_date=target_date,
            )
        report = self._reconciler.reconcile(
            trading_date=target_date,
            new_plan_items=items,
            vetoed_symbols=vetoed_symbols if vetoed_symbols else None,
            pre_resolved_instruments=pre_resolved,
        )
        if items:
            self._last_processed_plan_id = str(items[0].plan_id)
        load_meta = getattr(self._repository, "load_locked_plan_metadata", None)
        if callable(load_meta):
            try:
                meta = load_meta(trading_date=target_date, market=self._market)
                if meta is not None:
                    if meta[0]:
                        self._last_processed_plan_id = str(meta[0])
                    if meta[1] is not None:
                        self._last_processed_locked_at = str(meta[1])
            except Exception:
                pass
        if self._on_rollover_callback is not None:
            self._on_rollover_callback(report)
        return report

    def poll_once(self, timeout_secs: float = 0.0) -> Optional[DailyPlanRolloverReport]:
        """Waits up to `timeout_secs` via `select.select` for a `NOTIFY nautilus_plan_locked`
        event and executes `reconciler.reconcile(...)` if notified.
        """
        if self._stop_event.is_set():
            return None
        conn = self._ensure_listen_conn()
        if self._pending_missed_plan_date is not None:
            missed_date = self._pending_missed_plan_date
            self._pending_missed_plan_date = None
            return self._process_rollover_for_date(missed_date, raw_payload="")
        watch_fds: List[Any] = [conn]
        if not self._pipe_closed and self._wake_r >= 0:
            watch_fds.append(self._wake_r)
        ready, _, _ = select.select(watch_fds, [], [], max(0.0, float(timeout_secs)))
        if not self._pipe_closed and self._wake_r in ready:
            try:
                os.read(self._wake_r, 4096)
            except (BlockingIOError, OSError):
                pass
            return None
        if conn not in ready:
            return None
        conn.poll()
        if not conn.notifies:
            return None
        last_notify = conn.notifies[-1]
        conn.notifies.clear()
        raw_payload = getattr(last_notify, "payload", "")
        target_date = self._resolve_notified_trading_date(raw_payload)
        if target_date is None:
            return None
        return self._process_rollover_for_date(target_date, raw_payload=raw_payload)

    def start_background_listener(
        self, select_timeout_secs: float = 5.0
    ) -> threading.Thread:
        """Starts a daemon thread blocked on `select.select` for `LISTEN nautilus_plan_locked`."""
        self._reconciler._sync_owner_thread_state()
        if self._thread is not None and self._thread.is_alive():
            return self._thread
        self._stop_event.clear()

        def _loop() -> None:
            while not self._stop_event.is_set():
                try:
                    self.poll_once(timeout_secs=select_timeout_secs)
                except Exception as exc:
                    if self._stop_event.is_set():
                        break
                    logger.warning(
                        "DailyPlanRolloverListener encountered error on %s: %s",
                        self._channel,
                        exc,
                    )
                    self._close_conn()
                    self._stop_event.wait(timeout=1.0)

        self._thread = threading.Thread(
            target=_loop,
            name="nautilus-daily-plan-rollover-listener",
            daemon=True,
        )
        self._thread.start()
        return self._thread

    def _close_conn(self) -> None:
        conn = self._conn
        self._conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def _close_wake_pipe(self) -> None:
        if self._pipe_closed:
            return
        self._pipe_closed = True
        for fd_attr in ("_wake_r", "_wake_w"):
            fd_val = getattr(self, fd_attr, -1)
            setattr(self, fd_attr, -1)
            if isinstance(fd_val, int) and fd_val >= 0:
                try:
                    os.close(fd_val)
                except OSError:
                    pass

    def stop(self) -> None:
        self._stop_event.set()
        if not self._pipe_closed and self._wake_w >= 0:
            try:
                os.write(self._wake_w, b"\x01")
            except (BlockingIOError, OSError):
                pass
        if self._thread is not None and self._thread.is_alive():
            if threading.current_thread() is not self._thread:
                self._thread.join(timeout=35.0)
        self._close_conn()
        self._close_wake_pipe()
        self._reconciler._sync_owner_thread_state()

    def __del__(self) -> None:
        try:
            self._close_wake_pipe()
        except Exception:
            pass
