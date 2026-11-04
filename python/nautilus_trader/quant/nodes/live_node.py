"""Multi-venue `nautilus-live-node` assembler, PG `LISTEN/NOTIFY` plan loader, and Backtest-to-Live 100% Parity Gate.

Implements Ticket #16 (`16-multi-venue-live-node-and-parity-gate-seam.md`):
- Loads `LOCKED` `PlanItemContract` items from PostgreSQL via `PlanContractRepository`
  (or awaits `LISTEN nautilus_plan_locked` via event-driven reactive wakeup, zero blind polling `D6.4`).
- Assembles a production `LiveNode` with `DataInfraDataClientFactory`, `AlpacaExecutionClientFactory`
  (plus optional `IBKR` / `OKX` execution clients and `RoutingConfig`), `PlanGateExecAlgorithm`,
  `PostgresTelemetryActor`, and the 1:1 isomorphic `Strategy(StrategyConfig)` instances.
- Provides `verify_backtest_to_live_parity` to prove 100% order-intent parity between the
  `IsomorphicBacktestRunner` pipeline and the `LiveNode` execution pipeline on identical bars.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import gc
import json
import logging
import math
import os
import select
import threading
import time
import uuid
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from evolution_engine.db.dsn import resolve_database_dsn
from nautilus_trader.quant.contracts.models import (
    MarketableLimitCollar,
    PlanItemContract,
    PlanRiskEnvelope,
    PlanSide,
    _to_decimal,
)
from nautilus_trader.quant.contracts.repository import PlanContractRepository
from nautilus_trader.quant.governance.plan_gate import (
    PLAN_GATE_EXEC_ALGORITHM_ID,
    PlanGateExecAlgorithm,
    PlanGateExecAlgorithmConfig,
    PostgresPlanGateBackend,
    SnapshotPlanGateBackend,
)
from nautilus_trader.quant.governance.telemetry_actor import (
    PostgresTelemetryActor,
    PostgresTelemetryActorConfig,
    ProtectiveOcoMonitor,
)
from nautilus_trader.quant.nodes.backtest_runner import (
    IsomorphicBacktestRunner,
    _build_parity_plan_items,
    _create_default_equity,
    _normalize_bars_for_parity,
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
)
from nautilus_trader.adapters.datainfra import (
    DataInfraCatalog,
    DataInfraConfig,
    DataInfraDataClientFactory,
)
from nautilus_trader.backtest import BacktestEngine, BacktestEngineConfig
from nautilus_trader.common import CacheConfig, Environment
from nautilus_trader.execution import (
    BestPriceFillModel,
    MakerTakerFeeModel,
    StaticLatencyModel,
)
from nautilus_trader.live import (
    LiveDataEngineConfig,
    LiveExecutionEngineConfig,
    LiveNode,
    LiveRiskEngineConfig,
)
from nautilus_trader.model import (
    AccountType,
    Bar,
    Currency,
    Equity,
    Money,
    OmsType,
    TraderId,
    Venue,
)

PLAN_LOCKED_NOTIFY_CHANNEL: str = "nautilus_plan_locked"
logger = logging.getLogger(__name__)

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
        return {
            "trading_date": self.trading_date.isoformat(),
            "new_active_symbols": list(self.new_active_symbols),
            "renewed_active_symbols": list(self.renewed_active_symbols),
            "carry_over_exit_only_symbols": list(self.carry_over_exit_only_symbols),
            "thesis_invalidated_exit_symbols": list(self.thesis_invalidated_exit_symbols),
            "decisions": [d.to_dict() for d in self.decisions],
        }


@dataclass(frozen=True)
class ExtraVenueClientSpec:
    """Optional secondary venue execution client specification (e.g., `IBKR`, `OKX`)."""

    name: Optional[str]
    venue: str
    factory: Any
    config: Any


@dataclass(frozen=True)
class AssembledLiveNodeBundle:
    """Container holding the built `LiveNode` and its attached isomorphic components."""

    node: LiveNode
    trading_date: date
    plan_items: Tuple[PlanItemContract, ...]
    strategies: Tuple[BaseArchetypeStrategy, ...]
    plan_gate: PlanGateExecAlgorithm
    telemetry_actor: PostgresTelemetryActor
    routed_venues: Tuple[str, ...]
    reconciler: Optional["DailyPlanReconciler"] = None


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


def _load_warmup_bars_for_strategy_with_sources(
    strategy: BaseArchetypeStrategy,
    *,
    bars_override: Optional[Sequence[Bar]] = None,
    historical_bar_loader: Optional[Callable[[str, str, int], Sequence[Bar]]] = None,
    datainfra_config: Optional[DataInfraConfig] = None,
) -> List[Bar]:
    if bars_override is not None:
        return list(bars_override)

    cfg = strategy.archetype_config
    req_bars = required_warmup_bars(cfg.lookback_window)
    prim_inst_str = str(cfg.instrument_id)
    prim_sym = prim_inst_str.split(".", 1)[0].strip().upper()
    venue = (
        prim_inst_str.split(".", 1)[1].strip().upper()
        if "." in prim_inst_str
        else "ALPACA"
    )
    symbols = [prim_sym]
    if cfg.secondary_instrument_id is not None:
        sec_sym = str(cfg.secondary_instrument_id).split(".", 1)[0].strip().upper()
        if sec_sym and sec_sym not in symbols:
            symbols.append(sec_sym)

    collected: List[Bar] = []
    if historical_bar_loader is not None:
        for sym in symbols:
            loaded = list(historical_bar_loader(sym, cfg.bar_timeframe, req_bars))
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
        try:
            catalog = DataInfraCatalog(datainfra_config)
            start_ts_ns = _compute_warmup_query_start_ns(cfg.bar_timeframe, req_bars)
            for sym in symbols:
                catalog.register_equity(sym, venue=venue, lot_size=1)
                queried = sorted(
                    catalog.query_bars(
                        [sym],
                        start_ts_ns=start_ts_ns,
                        timeframe=cfg.bar_timeframe,
                        adjustment="adjusted",
                        market_session="REG",
                        venue=venue,
                    ),
                    key=lambda b: int(
                        getattr(b, "ts_init", 0) or getattr(b, "ts_event", 0) or 0
                    ),
                )
                if len(queried) > req_bars:
                    queried = queried[-req_bars:]
                collected.extend(queried)
        except Exception as exc:
            logger.debug(
                "ClickHouse warmup query skipped/unavailable for %s: %s",
                prim_sym,
                exc,
            )

    collected.sort(
        key=lambda b: int(getattr(b, "ts_init", 0) or getattr(b, "ts_event", 0) or 0)
    )
    return collected


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
        cache_config: Optional[CacheConfig] = None,
        historical_bar_loader: Optional[Callable[[str, str, int], Sequence[Bar]]] = None,
        warmup_on_build: bool = True,
    ) -> None:
        resolved_mode = resolve_trading_mode()
        self._dsn = resolve_database_dsn(dsn)
        self._datainfra_config = datainfra_config or DataInfraConfig()
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
        self._cache_config = cache_config
        self._historical_bar_loader = historical_bar_loader
        self._warmup_on_build = bool(warmup_on_build)
        self._repo = PlanContractRepository(dsn=self._dsn)

    @property
    def dsn(self) -> str:
        return self._dsn

    @property
    def repository(self) -> PlanContractRepository:
        return self._repo

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
        trading_date: date,
        listen_timeout_secs: float = 0.0,
    ) -> List[PlanItemContract]:
        """Loads today's `LOCKED` `PlanItemContract` list or waits via PG `LISTEN nautilus_plan_locked`."""
        existing = self._repo.load_locked_plan_items(
            trading_date=trading_date,
            market=self._market,
        )
        if existing:
            return existing

        if listen_timeout_secs <= 0.0:
            raise RuntimeError(
                f"No LOCKED TradingPlan items found in PostgreSQL for market={self._market} "
                f"trading_date={trading_date.isoformat()} (fail-closed)"
            )

        conn = psycopg2.connect(self._dsn, connect_timeout=5)
        try:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                cur.execute(f"LISTEN {PLAN_LOCKED_NOTIFY_CHANNEL};")
                rechecked = self._repo.load_locked_plan_items(
                    trading_date=trading_date,
                    market=self._market,
                )
                if rechecked:
                    return rechecked

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
                        trading_date=trading_date,
                        market=self._market,
                    )
                    if notified_items:
                        return notified_items
        finally:
            conn.close()

        raise RuntimeError(
            f"Timed out waiting ({listen_timeout_secs:.1f}s) on LISTEN {PLAN_LOCKED_NOTIFY_CHANNEL} "
            f"for LOCKED TradingPlan on {trading_date.isoformat()} (fail-closed)"
        )

    def build_live_node(
        self,
        trading_date: date,
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
        """Constructs a `LiveNode` wired with `DataInfra`, `Alpaca`, `PlanGate`, `TelemetryActor`, and `Strategy`s."""
        locked_items = (
            list(plan_items)
            if plan_items is not None
            else self.load_locked_plan_items_or_wait(
                trading_date=trading_date,
                listen_timeout_secs=listen_timeout_secs,
            )
        )
        if not locked_items:
            raise ValueError("LiveNode cannot start with an empty PlanItemContract list")

        builder = LiveNode.builder(
            self._node_name,
            TraderId(self._trader_id),
            self._environment,
        )
        builder.with_data_engine_config(LiveDataEngineConfig())
        builder.with_exec_engine_config(LiveExecutionEngineConfig())
        builder.with_risk_engine_config(LiveRiskEngineConfig())
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

        env_label = "LIVE" if self._environment == Environment.LIVE else "PAPER"
        pg_backend = (
            plan_gate_backend
            if plan_gate_backend is not None
            else PostgresPlanGateBackend(
                dsn=self._dsn,
                trading_date=trading_date,
                market=self._market,
            )
        )
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
        telemetry_actor = PostgresTelemetryActor(
            PostgresTelemetryActorConfig(
                dsn=self._dsn if persist_telemetry_to_db else None,
                market=self._market,
                trading_date=trading_date,
                environment=env_label,
                persist_to_db=bool(persist_telemetry_to_db),
                enforce_protective_oco_on_position_events=bool(
                    enforce_protective_oco_on_position_events
                ),
            ),
            oco_monitor=oco_monitor,
        )
        plan_gate = PlanGateExecAlgorithm(
            backend=pg_backend,
            telemetry_actor=telemetry_actor,
            oco_monitor=oco_monitor,
            config=PlanGateExecAlgorithmConfig(
                exec_algorithm_id=PLAN_GATE_EXEC_ALGORITHM_ID,
                collar_market_orders=bool(use_limit_orders),
            ),
        )
        node.add_exec_algorithm(plan_gate)
        node.add_actor(telemetry_actor)
        if hasattr(node, "trader") and getattr(node, "trader", None) is not None:
            if hasattr(node.trader, "add_exec_algorithm"):
                try:
                    node.trader.add_exec_algorithm(plan_gate)
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
        primary_locked_items = [
            item
            for item in locked_items
            if not (
                str(item.sizing_trace.get("role", "")).strip() == "secondary_leg"
                or bool(item.sizing_trace.get("is_secondary_leg", False))
            )
        ] or list(locked_items)
        strategies: List[BaseArchetypeStrategy] = []
        for item in primary_locked_items:
            strategy = instantiate_strategy_from_contract(
                item,
                use_limit_orders=use_limit_orders,
                submit_protective_stop=submit_protective_stop,
            )
            if hasattr(strategy, "bind_telemetry_actor"):
                strategy.bind_telemetry_actor(telemetry_actor)
            if hasattr(strategy, "bind_plan_gate"):
                strategy.bind_plan_gate(plan_gate)
            if hasattr(strategy, "bind_warmup_loader"):
                strategy.bind_warmup_loader(
                    lambda s=strategy: self.load_warmup_bars_for_strategy(s)
                )
            if warmup_bars_by_symbol and item.symbol in warmup_bars_by_symbol:
                self.warmup_strategy_from_catalog(
                    strategy,
                    bars=warmup_bars_by_symbol[item.symbol],
                )
            elif should_warmup:
                self.warmup_strategy_from_catalog(strategy)
            node.add_strategy(strategy)
            if hasattr(node, "trader") and getattr(node, "trader", None) is not None and hasattr(node.trader, "add_strategy"):
                try:
                    node.trader.add_strategy(strategy)
                except Exception:
                    pass
            strategies.append(strategy)

        reconciler = DailyPlanReconciler(
            plan_gate=plan_gate,
            plan_gate_backend=pg_backend,
            telemetry_actor=telemetry_actor,
            strategies_by_symbol={
                item.symbol: strat for item, strat in zip(primary_locked_items, strategies)
            },
            plan_items_by_symbol={item.symbol: item for item in primary_locked_items},
            node=node,
            assembler=self,
            repository=self._repo,
            warmup_bar_loader=self._historical_bar_loader,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
        )

        return AssembledLiveNodeBundle(
            node=node,
            trading_date=trading_date,
            plan_items=tuple(locked_items),
            strategies=tuple(strategies),
            plan_gate=plan_gate,
            telemetry_actor=telemetry_actor,
            routed_venues=tuple(routed_venues),
            reconciler=reconciler,
        )


class DailyPlanReconciler:
    """Stateful daily `TradingPlan` rollover reconciler (`P0-EXEC-01`) implementing the
    4-State Position & Plan Lifecycle Matrix without restarting `LiveNode`:
    - `NEW_ACTIVE`: Pos == 0, Symbol in Today's Plan -> instantiate/mount strategy, warm up
      historical bars (`max(lookback_window * 3, 120)`), and activate in `PlanGate`.
    - `RENEWED_ACTIVE`: Pos > 0, Symbol in Today's Plan -> update strategy parameters in-place
      and enforce monotonic stop-loss tightening (`new_stop >= current_stop` for LONG,
      `new_stop <= current_stop` for SHORT).
    - `CARRY_OVER_EXIT_ONLY`: Pos > 0, Symbol absent from Today's Plan -> freeze new entries
      (`status="EXPIRED"`, `filled_qty = approved_qty_cap` in `PlanGate`, `exit_only_mode=True`
      on strategy) while keeping protective stops (`GTC`, `reduce_only=True`) and exit signals active.
    - `THESIS_INVALIDATED_EXIT`: Pos > 0, Symbol VETOED or explicitly invalidated -> transition
      immediately to Two-Phase Safe Exit (`begin_safe_exit` -> cancel old stop leg -> submit
      `reduce_only=True` exit order).
    """

    def __init__(
        self,
        *,
        plan_gate_backend: Optional[Any] = None,
        plan_gate: Optional[PlanGateExecAlgorithm | Any] = None,
        telemetry_actor: Optional[PostgresTelemetryActor | Any] = None,
        strategies_by_symbol: Optional[
            Mapping[str, BaseArchetypeStrategy] | Sequence[BaseArchetypeStrategy]
        ] = None,
        plan_items_by_symbol: Optional[
            Mapping[str, PlanItemContract] | Sequence[PlanItemContract]
        ] = None,
        node: Optional[LiveNode | Any] = None,
        assembler: Optional[NautilusLiveNodeAssembler] = None,
        repository: Optional[PlanContractRepository] = None,
        warmup_bar_loader: Optional[Callable[[str, str, int], Sequence[Bar]]] = None,
        use_limit_orders: bool = True,
        submit_protective_stop: bool = True,
    ) -> None:
        resolved_backend = plan_gate_backend
        if resolved_backend is None and plan_gate is not None:
            resolved_backend = getattr(plan_gate, "backend", None) or getattr(
                plan_gate, "_backend", None
            )
        self._backend = resolved_backend
        self._plan_gate = plan_gate
        self._telemetry_actor = telemetry_actor
        self._node = node
        self._assembler = assembler
        self._repository = repository or (
            assembler.repository if assembler is not None else None
        )
        self._warmup_bar_loader = warmup_bar_loader or (
            assembler._historical_bar_loader if assembler is not None else None
        )
        self._use_limit_orders = bool(use_limit_orders)
        self._submit_protective_stop = bool(submit_protective_stop)

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

        self._trading_date: Optional[date] = getattr(
            self._backend, "trading_date", None
        )
        self._last_report: Optional[DailyPlanRolloverReport] = None
        self._owner_thread_id: int = threading.get_ident()
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._pending_node_strategies: List[BaseArchetypeStrategy] = []
        self._sync_owner_thread_state()

    def bind_event_loop(self, loop: Optional[asyncio.AbstractEventLoop]) -> None:
        self._event_loop = loop
        if loop is not None:
            self._owner_thread_id = threading.get_ident()

    def _is_owner_thread(self) -> bool:
        return threading.get_ident() == getattr(
            self, "_owner_thread_id", threading.get_ident()
        )

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
                logger.debug("Skipping duplicate/offline node.add_strategy: %s", exc)

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
            plan_gate=bundle.plan_gate,
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
    ) -> DailyPlanRolloverReport:
        """Reconciles open positions from Day `T-1` against Day `T`'s locked `PlanItemContract`s."""
        if (
            not self._is_owner_thread()
            and self._event_loop is not None
            and self._event_loop.is_running()
        ):
            fut: concurrent.futures.Future[DailyPlanRolloverReport] = (
                concurrent.futures.Future()
            )

            def _run_on_owner_loop() -> None:
                try:
                    res = self.reconcile(
                        trading_date,
                        new_plan_items,
                        vetoed_symbols=vetoed_symbols,
                        open_positions_by_symbol=open_positions_by_symbol,
                        warmup_bars_by_symbol=warmup_bars_by_symbol,
                        execute_thesis_exits_immediately=execute_thesis_exits_immediately,
                    )
                    fut.set_result(res)
                except Exception as exc:
                    fut.set_exception(exc)

            self._event_loop.call_soon_threadsafe(_run_on_owner_loop)
            return fut.result(timeout=30.0)

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

        vetoed_set: Set[str] = {
            str(s).strip().upper() for s in (vetoed_symbols or ()) if str(s).strip()
        }
        new_items_by_symbol: Dict[str, PlanItemContract] = {}
        secondary_items_by_symbol: Dict[str, PlanItemContract] = {}
        for item in resolved_items:
            if (
                str(item.sizing_trace.get("role", "")).strip() == "secondary_leg"
                or bool(item.sizing_trace.get("is_secondary_leg", False))
            ):
                secondary_items_by_symbol[item.symbol] = item
                continue
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

        # Snapshot prior items before hot-reloading backend trading_date
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
        if self._telemetry_actor is not None and hasattr(
            self._telemetry_actor, "set_trading_date"
        ):
            self._telemetry_actor.set_trading_date(trading_date)

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

        for sym in all_symbols:
            pos_qty = pos_by_symbol.get(sym, Decimal("0"))
            has_open_pos = pos_qty != Decimal("0")
            strat = self._strategies_by_symbol.get(sym)
            prior_item = prior_items_by_symbol.get(sym)

            # State 4: THESIS_INVALIDATED_EXIT (Pos > 0 and Symbol VETOED / invalidated)
            if sym in vetoed_set and has_open_pos:
                if strat is None and prior_item is not None:
                    strat = instantiate_strategy_from_contract(
                        prior_item,
                        use_limit_orders=self._use_limit_orders,
                        submit_protective_stop=self._submit_protective_stop,
                    )
                    if self._telemetry_actor is not None:
                        strat.bind_telemetry_actor(self._telemetry_actor)
                    if self._plan_gate is not None and hasattr(strat, "bind_plan_gate"):
                        strat.bind_plan_gate(self._plan_gate)
                    strat.set_tracked_position_signed_qty(pos_qty)
                    self._strategies_by_symbol[sym] = strat

                base_contract = prior_item or self._build_fallback_carry_over_contract(
                    sym, strat, pos_qty
                )
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
                        safe_exit_triggered=True,
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
                if strat is not None:
                    renewed_env = strat.apply_renewed_plan_item(new_item)
                else:
                    prior_stop = (
                        prior_item.risk_envelope.initial_stop_price
                        if prior_item is not None
                        else Decimal("0")
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
                        renewed_env = replace(
                            new_item.risk_envelope,
                            initial_stop_price=tightened_stop,
                        )
                    else:
                        renewed_env = new_item.risk_envelope
                    strat = instantiate_strategy_from_contract(
                        replace(new_item, risk_envelope=renewed_env),
                        use_limit_orders=self._use_limit_orders,
                        submit_protective_stop=self._submit_protective_stop,
                    )
                    if self._telemetry_actor is not None:
                        strat.bind_telemetry_actor(self._telemetry_actor)
                    if self._plan_gate is not None and hasattr(strat, "bind_plan_gate"):
                        strat.bind_plan_gate(self._plan_gate)
                    strat.set_tracked_position_signed_qty(pos_qty)
                    self._strategies_by_symbol[sym] = strat

                carried_reserved = max(new_item.filled_qty, abs(pos_qty))
                renewed_item = replace(
                    new_item,
                    risk_envelope=renewed_env,
                    filled_qty=carried_reserved,
                )
                if self._backend is not None and hasattr(
                    self._backend, "upsert_runtime_plan_item"
                ):
                    self._backend.upsert_runtime_plan_item(
                        renewed_item,
                        reserved_qty=carried_reserved,
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
                base_contract = prior_item or self._build_fallback_carry_over_contract(
                    sym, strat, pos_qty
                )
                eff_stop = base_contract.risk_envelope.initial_stop_price
                if strat is not None:
                    strat.enter_carry_over_exit_only_mode()
                    tightest = strat._current_tightest_stop_price()
                    if tightest is not None and tightest > Decimal("0"):
                        eff_stop = tightest

                carry_env = replace(
                    base_contract.risk_envelope,
                    initial_stop_price=eff_stop,
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
                new_item = new_items_by_symbol[sym]
                if (
                    strat is None
                    or strat.archetype_config.ARCHETYPE_ID
                    != new_item.strategy_config.archetype_id
                ):
                    strat = instantiate_strategy_from_contract(
                        new_item,
                        use_limit_orders=self._use_limit_orders,
                        submit_protective_stop=self._submit_protective_stop,
                    )
                    if self._telemetry_actor is not None and hasattr(
                        strat, "bind_telemetry_actor"
                    ):
                        strat.bind_telemetry_actor(self._telemetry_actor)
                    if self._plan_gate is not None and hasattr(
                        strat, "bind_plan_gate"
                    ):
                        strat.bind_plan_gate(self._plan_gate)
                    if self._assembler is not None and hasattr(
                        strat, "bind_warmup_loader"
                    ):
                        strat.bind_warmup_loader(
                            lambda s=strat: self._assembler.load_warmup_bars_for_strategy(s)
                        )
                    if self._node is not None:
                        if self._is_owner_thread():
                            self._mount_strategy_on_node(strat)
                        else:
                            self._pending_node_strategies.append(strat)
                    self._strategies_by_symbol[sym] = strat
                else:
                    strat.apply_renewed_plan_item(new_item)
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
            if self._backend is not None and hasattr(
                self._backend, "remove_runtime_plan_item"
            ):
                self._backend.remove_runtime_plan_item(sym)
            self._plan_items_by_symbol.pop(sym, None)

        if self._is_owner_thread():
            self._sync_owner_thread_state()

        report = DailyPlanRolloverReport(
            trading_date=trading_date,
            decisions=tuple(decisions),
        )
        self._last_report = report
        return report


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

    def poll_once(self, timeout_secs: float = 0.0) -> Optional[DailyPlanRolloverReport]:
        """Waits up to `timeout_secs` via `select.select` for a `NOTIFY nautilus_plan_locked`
        event and executes `reconciler.reconcile(...)` if notified.
        """
        conn = self._ensure_listen_conn()
        ready, _, _ = select.select([conn], [], [], max(0.0, float(timeout_secs)))
        if not ready:
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
        items = self._repository.load_locked_plan_items(
            trading_date=target_date,
            market=self._market,
        )
        vetoed_symbols = self._extract_vetoed_symbols_for_rollover(
            payload=raw_payload,
            target_date=target_date,
        )
        report = self._reconciler.reconcile(
            trading_date=target_date,
            new_plan_items=items,
            vetoed_symbols=vetoed_symbols if vetoed_symbols else None,
        )
        if self._on_rollover_callback is not None:
            self._on_rollover_callback(report)
        return report

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

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None and self._thread.is_alive():
            if threading.current_thread() is not self._thread:
                self._thread.join(timeout=2.0)
        self._close_conn()
        self._reconciler._sync_owner_thread_state()


@dataclass(frozen=True)
class ParityOrderIntent:
    """Canonical order intent captured for Backtest-to-Live 1:1 Isomorphic Parity verification."""

    sequence_index: int
    symbol: str
    side: str
    order_type: str
    quantity: str
    limit_price: Optional[str]
    trigger_price: Optional[str]
    exec_algorithm_id: str
    ts_init_ns: int
    admitted_by_plan_gate: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sequence_index": self.sequence_index,
            "symbol": self.symbol,
            "side": self.side,
            "order_type": self.order_type,
            "quantity": self.quantity,
            "limit_price": self.limit_price,
            "trigger_price": self.trigger_price,
            "exec_algorithm_id": self.exec_algorithm_id,
            "ts_init_ns": self.ts_init_ns,
            "admitted_by_plan_gate": self.admitted_by_plan_gate,
        }


@dataclass(frozen=True)
class IsomorphicParityReport:
    """Report comparing order intents emitted by Backtest vs Live-wired pipelines on identical bars."""

    symbol: str
    archetype_id: str
    passed: bool
    parity_pct: float
    backtest_intents: Tuple[ParityOrderIntent, ...]
    live_intents: Tuple[ParityOrderIntent, ...]
    mismatches: Tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "archetype_id": self.archetype_id,
            "passed": self.passed,
            "parity_pct": round(self.parity_pct, 4),
            "backtest_intent_count": len(self.backtest_intents),
            "live_intent_count": len(self.live_intents),
            "backtest_intents": [i.to_dict() for i in self.backtest_intents],
            "live_intents": [i.to_dict() for i in self.live_intents],
            "mismatches": list(self.mismatches),
        }


def _has_existing_locked_plan_for_parity(
    dsn: Optional[str],
    trading_date: date,
    items_to_lock: Sequence[PlanItemContract],
) -> bool:
    if not dsn or not items_to_lock:
        return False
    try:
        resolved_dsn = resolve_database_dsn(dsn)
        conn = psycopg2.connect(resolved_dsn, connect_timeout=2)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT active FROM system_flags WHERE flag = 'ENTRY_FREEZE'"
                )
                row = cur.fetchone()
                if row is not None and bool(row[0]):
                    return False
                cur.execute(
                    """
                    SELECT 1
                    FROM trading_plan_items tpi
                    JOIN trading_plans tp ON tp.plan_id = tpi.plan_id
                    WHERE tp.trading_date = %s
                      AND tp.market = 'US_EQUITY'
                      AND tpi.item_id = %s
                    LIMIT 1
                    """,
                    (trading_date, str(items_to_lock[0].item_id)),
                )
                return cur.fetchone() is not None
        finally:
            conn.close()
    except Exception:
        return False


def _extract_parity_order_intents(
    orders: Sequence[Any],
    decisions: Mapping[str, Any],
) -> Tuple[ParityOrderIntent, ...]:
    sorted_orders = sorted(
        list(orders),
        key=lambda o: (
            int(getattr(o, "ts_init", 0)),
            str(getattr(o, "client_order_id", "")),
        ),
    )
    intents: List[ParityOrderIntent] = []
    for idx, order in enumerate(sorted_orders):
        cid = str(getattr(order, "client_order_id", ""))
        inst_str = str(getattr(order, "instrument_id", ""))
        sym = inst_str.split(".", 1)[0].strip().upper()
        side_raw = str(getattr(order, "side", "")).upper()
        side = (
            "BUY"
            if "BUY" in side_raw
            else ("SELL" if "SELL" in side_raw else side_raw)
        )
        type_raw = str(getattr(order, "order_type", "")).upper()
        if "." in type_raw:
            type_raw = type_raw.split(".")[-1]
        qty_str = str(getattr(order, "quantity", "0"))
        px_attr = getattr(order, "price", None)
        trig_attr = getattr(order, "trigger_price", None)
        exec_algo = str(getattr(order, "exec_algorithm_id", "") or "")
        dec = decisions.get(cid)
        admitted = bool(dec.allowed) if dec is not None else True
        intents.append(
            ParityOrderIntent(
                sequence_index=idx,
                symbol=sym,
                side=side,
                order_type=type_raw,
                quantity=qty_str,
                limit_price=(str(px_attr) if px_attr is not None else None),
                trigger_price=(str(trig_attr) if trig_attr is not None else None),
                exec_algorithm_id=exec_algo,
                ts_init_ns=int(getattr(order, "ts_init", 0)),
                admitted_by_plan_gate=admitted,
            )
        )
    return tuple(intents)


def _capture_pipeline_order_intents(
    *,
    plan_item: PlanItemContract,
    bars: Sequence[Bar],
    dsn: Optional[str] = None,
    trading_date: date,
    environment_label: str = "BACKTEST",
    use_limit_orders: bool = True,
    submit_protective_stop: bool = True,
    include_telemetry_actor: bool = True,
) -> Tuple[ParityOrderIntent, ...]:
    """Runs the exact `Strategy(StrategyConfig) + PlanGateExecAlgorithm(SnapshotPlanGateBackend)` pipeline
    in `BacktestEngine` and extracts the chronological sequence of `ParityOrderIntent`s.
    """
    normalized_bars = _normalize_bars_for_parity(bars)
    venue_str = plan_item.strategy_config.instrument_id.split(".", 1)[1].strip().upper()
    symbols = [plan_item.symbol]
    if plan_item.strategy_config.secondary_instrument_id:
        sec_sym = plan_item.strategy_config.secondary_instrument_id.split(".", 1)[0].strip().upper()
        if sec_sym and sec_sym not in symbols:
            symbols.append(sec_sym)

    equities: List[Equity] = [_create_default_equity(sym, venue_str) for sym in symbols]
    items_to_lock = _build_parity_plan_items(plan_item, venue_str)
    can_persist = bool(
        include_telemetry_actor
        and str(environment_label).upper() != "BACKTEST"
        and _has_existing_locked_plan_for_parity(dsn, trading_date, items_to_lock)
    )

    engine = BacktestEngine(
        BacktestEngineConfig(bypass_logging=True, run_analysis=False)
    )
    plan_gate: Optional[PlanGateExecAlgorithm] = None
    telemetry_actor: Optional[PostgresTelemetryActor] = None
    try:
        engine.add_venue(
            venue=Venue(venue_str),
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            starting_balances=[Money.from_str("100000.00 USD")],
            base_currency=Currency.from_str("USD"),
            fill_model=BestPriceFillModel(prob_fill_on_limit=1.0, prob_slippage=0.0),
            fee_model=MakerTakerFeeModel(
                maker_rate=Decimal("0.0002"),
                taker_rate=Decimal("0.0005"),
            ),
            latency_model=StaticLatencyModel(
                base_latency_nanos=0,
                insert_latency_nanos=1,
                update_latency_nanos=0,
                cancel_latency_nanos=0,
            ),
        )
        for eq in equities:
            engine.add_instrument(eq)

        snapshot_backend = SnapshotPlanGateBackend(
            plan_items=items_to_lock,
            entry_freeze=False,
            trading_date=trading_date,
            allow_multi_cycle_reentry=False,
        )
        oco_monitor = ProtectiveOcoMonitor(
            dsn=dsn if can_persist else None,
            persist_to_db=can_persist,
            on_entry_freeze_callback=lambda sym, reason: snapshot_backend.set_entry_freeze(
                True,
                reason=reason,
                updated_by=f"nautilus:protective_oco_monitor:{sym}",
            ),
        )
        telemetry_actor = PostgresTelemetryActor(
            PostgresTelemetryActorConfig(
                dsn=dsn if can_persist else None,
                market="US_EQUITY",
                trading_date=trading_date,
                environment=environment_label,
                persist_to_db=can_persist,
                enforce_protective_oco_on_position_events=bool(submit_protective_stop),
            ),
            oco_monitor=oco_monitor,
        )
        plan_gate = PlanGateExecAlgorithm(
            backend=snapshot_backend,
            telemetry_actor=telemetry_actor,
            oco_monitor=oco_monitor,
            config=PlanGateExecAlgorithmConfig(
                exec_algorithm_id=PLAN_GATE_EXEC_ALGORITHM_ID,
                collar_market_orders=bool(use_limit_orders),
            ),
        )
        engine.add_actor(telemetry_actor)
        engine.add_exec_algorithm(plan_gate)

        strategy = instantiate_strategy_from_contract(
            items_to_lock[0],
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
        )
        strategy.bind_telemetry_actor(telemetry_actor)
        if hasattr(strategy, "bind_plan_gate"):
            strategy.bind_plan_gate(plan_gate)
        engine.add_strategy(strategy)

        engine.add_data(normalized_bars)
        engine.run()

        if telemetry_actor is not None:
            telemetry_actor.sync_from_cache()
            telemetry_actor.flush()

        return _extract_parity_order_intents(
            orders=list(engine.cache.orders() or []),
            decisions=plan_gate.decisions,
        )
    finally:
        if plan_gate is not None:
            plan_gate.close()
        if telemetry_actor is not None:
            telemetry_actor.close()
        engine.dispose()


def _capture_live_node_order_intents(
    *,
    plan_item: PlanItemContract,
    bars: Sequence[Bar],
    bundle: Optional[AssembledLiveNodeBundle] = None,
    dsn: Optional[str] = None,
    trading_date: Optional[date] = None,
    use_limit_orders: bool = True,
    submit_protective_stop: bool = True,
) -> Tuple[ParityOrderIntent, ...]:
    """Drives the assembled `LiveNode` (`bundle`) over `bars` through the Nautilus kernel
    and extracts the chronological sequence of `ParityOrderIntent`s.
    """
    normalized_bars = _normalize_bars_for_parity(bars)
    owns_bundle = bundle is None
    if bundle is None:
        resolved_date = trading_date or (
            datetime.fromtimestamp(
                max(int(b.ts_init) for b in normalized_bars) / 1e9,
                tz=timezone.utc,
            ).date()
            if normalized_bars
            else datetime.now(tz=timezone.utc).date()
        )
        venue_str = plan_item.strategy_config.instrument_id.split(".", 1)[1].strip().upper()
        items_to_lock = _build_parity_plan_items(plan_item, venue_str)
        can_persist_live = _has_existing_locked_plan_for_parity(
            dsn, resolved_date, items_to_lock
        )
        snapshot_backend = SnapshotPlanGateBackend(
            plan_items=items_to_lock,
            entry_freeze=False,
            trading_date=resolved_date,
            allow_multi_cycle_reentry=False,
        )
        assembler = NautilusLiveNodeAssembler(
            dsn=dsn or "postgresql://localhost:5432/trading",
            alpaca_config=AlpacaExecClientConfig(
                api_key="parity-gate-key",
                api_secret="parity-gate-secret",
            ),
        )
        bundle = assembler.build_live_node(
            trading_date=resolved_date,
            plan_items=[items_to_lock[0]],
            plan_gate_backend=snapshot_backend,
            persist_telemetry_to_db=can_persist_live,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            warmup_strategies=False,
        )
    try:
        return _drive_live_node_bundle_order_intents(
            bundle=bundle,
            plan_item=plan_item,
            bars=normalized_bars,
        )
    finally:
        if owns_bundle and bundle is not None:
            try:
                if bundle.plan_gate.is_running():
                    bundle.plan_gate.stop()
            except Exception:
                pass
            try:
                if bundle.telemetry_actor.is_running():
                    bundle.telemetry_actor.stop()
            except Exception:
                pass
            bundle.plan_gate.close()
            bundle.telemetry_actor.close()
            bundle.node.dispose()
            bundle = None  # type: ignore[assignment]


def _match_next_bar_best_price_fill(
    ord_obj: Any,
    bar: Bar,
) -> Tuple[Optional[float], Any]:
    """Evaluates `BestPriceFillModel` bar-OHLC execution for an admitted `LiveNode` order
    during offline parity replay (`LiveNode` binds `AlpacaExecutionClient` in `Environment.LIVE`
    and its `PyDataActor` instances are permanently bound to `TRADER-LIVE-001` in Rust).
    """
    from nautilus_trader.model import LiquiditySide

    open_px = float(bar.open)
    high_px = float(bar.high)
    low_px = float(bar.low)
    otype = str(getattr(ord_obj, "order_type", "")).upper().split(".")[-1]
    oside_raw = str(getattr(ord_obj, "side", "")).upper()
    oside = "BUY" if "BUY" in oside_raw else ("SELL" if "SELL" in oside_raw else oside_raw)

    if otype == "MARKET":
        return open_px, LiquiditySide.TAKER
    if otype == "LIMIT":
        lim_px = float(ord_obj.price)
        if oside == "BUY" and low_px <= lim_px + 1e-9:
            return min(open_px, lim_px), LiquiditySide.MAKER
        if oside == "SELL" and high_px >= lim_px - 1e-9:
            return max(open_px, lim_px), LiquiditySide.MAKER
    elif otype == "STOP_MARKET":
        trig_px = float(ord_obj.trigger_price)
        if oside == "SELL" and low_px <= trig_px + 1e-9:
            return min(open_px, trig_px), LiquiditySide.TAKER
        if oside == "BUY" and high_px >= trig_px - 1e-9:
            return max(open_px, trig_px), LiquiditySide.TAKER
    return None, LiquiditySide.TAKER


def _drive_live_node_bundle_order_intents(
    *,
    bundle: AssembledLiveNodeBundle,
    plan_item: PlanItemContract,
    bars: Sequence[Bar],
) -> Tuple[ParityOrderIntent, ...]:
    from nautilus_trader.common import Clock, OrderFactory
    from nautilus_trader.core import UUID4
    from nautilus_trader.model import (
        AccountId,
        ClientOrderId,
        OrderAccepted,
        OrderCanceled,
        OrderFilled,
        OrderSubmitted,
        PositionId,
        Price,
        Quantity,
        StrategyId,
        TradeId,
        TraderId,
        VenueOrderId,
    )

    matching_strategies = [
        s
        for s in bundle.strategies
        if s.archetype_config.archetype_contract == plan_item.strategy_config
    ]
    if not matching_strategies:
        live_strat_cfg = (
            bundle.strategies[0].archetype_config.archetype_contract
            if bundle.strategies
            else None
        )
        raise AssertionError(
            f"LiveNode strategy config mismatch: {live_strat_cfg} != {plan_item.strategy_config}"
        )

    live_strat = matching_strategies[0]
    venue_str = plan_item.strategy_config.instrument_id.split(".", 1)[1].strip().upper()
    sorted_bars = _normalize_bars_for_parity(bars)
    items_to_lock = _build_parity_plan_items(plan_item, venue_str)
    if hasattr(bundle.plan_gate.backend, "upsert_plan_item"):
        for item in items_to_lock:
            bundle.plan_gate.backend.upsert_plan_item(item)
    elif hasattr(bundle.plan_gate.backend, "lock_plan"):
        bundle.plan_gate.backend.lock_plan(plan_item.trading_date, items_to_lock)
    if hasattr(bundle.plan_gate, "sync_from_db"):
        bundle.plan_gate.sync_from_db()

    bar_clock = Clock.new_test()
    prev_cached_of = live_strat._cached_order_factory
    live_strat._cached_order_factory = OrderFactory(
        TraderId(str(bundle.node.trader_id)),
        StrategyId(live_strat._resolve_strategy_id_str()),
        bar_clock,
    )

    bundle.telemetry_actor.start()
    bundle.plan_gate.start()

    accepted_cids: set[str] = set()
    filled_cids: set[str] = set()
    canceled_cids: set[str] = set()
    venue_id_by_cid: Dict[str, VenueOrderId] = {}
    net_pos_by_inst: Dict[str, float] = {}
    trade_seq = 0
    venue_seq = 0

    for b in sorted_bars:
        bar_ts = int(b.ts_init)
        bar_clock.set_time(bar_ts)
        inst_obj = b.bar_type.instrument_id
        inst_str = str(inst_obj)

        for ord_obj in list(live_strat.submitted_orders):
            cid = str(ord_obj.client_order_id)
            if (
                cid in filled_cids
                or cid in canceled_cids
                or cid in live_strat._canceled_order_ids_audit
                or str(ord_obj.instrument_id) != inst_str
                or int(getattr(ord_obj, "ts_init", bar_ts)) >= bar_ts
            ):
                continue
            decision = bundle.plan_gate.decisions.get(cid)
            if decision is not None and not decision.allowed:
                canceled_cids.add(cid)
                continue
            cached_ord = bundle.node.cache.order(ClientOrderId(cid))
            status_str = live_strat._normalized_order_status(cached_ord or ord_obj)
            if status_str in ("CANCELED", "DENIED", "REJECTED", "EXPIRED", "FILLED"):
                canceled_cids.add(cid)
                continue

            if cid not in accepted_cids:
                accepted_cids.add(cid)
                venue_seq += 1
                v_oid = VenueOrderId(f"{venue_str}-1-{venue_seq}")
                venue_id_by_cid[cid] = v_oid
                sub_ev = OrderSubmitted(
                    TraderId(str(bundle.node.trader_id)),
                    StrategyId(str(ord_obj.strategy_id)),
                    inst_obj,
                    ord_obj.client_order_id,
                    AccountId(f"{venue_str}-001"),
                    UUID4(),
                    bar_ts,
                    bar_ts,
                )
                acc_ev = OrderAccepted(
                    TraderId(str(bundle.node.trader_id)),
                    StrategyId(str(ord_obj.strategy_id)),
                    inst_obj,
                    ord_obj.client_order_id,
                    v_oid,
                    AccountId(f"{venue_str}-001"),
                    UUID4(),
                    bar_ts,
                    bar_ts,
                    False,
                )
                for target_ord in (cached_ord, ord_obj):
                    if target_ord is None:
                        continue
                    t_status = live_strat._normalized_order_status(target_ord)
                    try:
                        if t_status == "INITIALIZED":
                            target_ord.apply(sub_ev)
                            target_ord.apply(acc_ev)
                        elif t_status == "SUBMITTED":
                            target_ord.apply(acc_ev)
                    except Exception:
                        pass
                bundle.telemetry_actor.on_order_submitted(sub_ev)
                if cached_ord is not None:
                    try:
                        bundle.node.cache.update_order(cached_ord)
                    except Exception:
                        pass

            fill_px, liq_side = _match_next_bar_best_price_fill(ord_obj, b)
            if fill_px is None:
                continue

            filled_cids.add(cid)
            trade_seq += 1
            qty_f = float(ord_obj.quantity)
            v_oid = venue_id_by_cid.get(cid, VenueOrderId(f"V-{trade_seq}"))
            fill_ev = OrderFilled(
                TraderId(str(bundle.node.trader_id)),
                StrategyId(str(ord_obj.strategy_id)),
                inst_obj,
                ord_obj.client_order_id,
                v_oid,
                AccountId(f"{venue_str}-001"),
                TradeId(f"T-{trade_seq}"),
                ord_obj.side,
                ord_obj.order_type,
                Quantity.from_str(str(ord_obj.quantity)),
                Price.from_str(f"{fill_px:.2f}"),
                Currency.from_str("USD"),
                liq_side,
                UUID4(),
                bar_ts,
                bar_ts,
                False,
                PositionId(f"P-{inst_str}"),
                Money.from_str("0.00 USD"),
            )
            for target_ord in (cached_ord, ord_obj):
                if target_ord is None:
                    continue
                try:
                    target_ord.apply(fill_ev)
                except Exception:
                    pass
            if cached_ord is not None:
                try:
                    bundle.node.cache.update_order(cached_ord)
                except Exception:
                    pass
            bundle.plan_gate.on_order_filled(fill_ev)
            bundle.telemetry_actor.on_order_filled(fill_ev)
            live_strat.on_order_filled(fill_ev)

            oside_raw = str(getattr(ord_obj, "side", "")).upper()
            prev_pos = net_pos_by_inst.get(inst_str, 0.0)
            new_pos = prev_pos + (qty_f if "BUY" in oside_raw else -qty_f)
            net_pos_by_inst[inst_str] = new_pos
            if abs(prev_pos) > 1e-9 and abs(new_pos) <= 1e-9:
                pos_closed = type(" _LivePosClosed", (), {"instrument_id": inst_obj})()
                bundle.telemetry_actor.on_position_closed(pos_closed)
                live_strat.on_position_closed(pos_closed)

        live_strat.on_bar(b)

        for cid in list(live_strat._canceled_order_ids_audit):
            if cid in canceled_cids or cid in filled_cids or cid not in accepted_cids:
                continue
            canceled_cids.add(cid)
            ord_obj = live_strat._submitted_orders_by_cid.get(cid)
            cached_ord = bundle.node.cache.order(ClientOrderId(cid))
            target_ref = cached_ord or ord_obj
            if target_ref is None:
                continue
            if (
                cached_ord is not None
                and live_strat._normalized_order_status(cached_ord) != "CANCELED"
            ):
                try:
                    live_strat.cancel_order(cached_ord)
                except Exception:
                    pass
                cached_ord = bundle.node.cache.order(ClientOrderId(cid))
            v_oid = venue_id_by_cid.get(cid, VenueOrderId(f"{venue_str}-1-0"))
            can_ev = OrderCanceled(
                TraderId(str(bundle.node.trader_id)),
                StrategyId(str(target_ref.strategy_id)),
                target_ref.instrument_id,
                target_ref.client_order_id,
                UUID4(),
                bar_ts,
                bar_ts,
                False,
                v_oid,
                AccountId(f"{venue_str}-001"),
            )
            for target_ord in (cached_ord, ord_obj):
                if target_ord is None:
                    continue
                if live_strat._normalized_order_status(target_ord) != "CANCELED":
                    try:
                        target_ord.apply(can_ev)
                    except Exception:
                        pass
            if cached_ord is not None:
                try:
                    bundle.node.cache.update_order(cached_ord)
                except Exception:
                    pass
            bundle.plan_gate.on_order_canceled(can_ev)
            bundle.telemetry_actor.on_order_canceled(can_ev)
            live_strat.on_order_canceled(can_ev)

    bundle.telemetry_actor.sync_from_cache()
    bundle.telemetry_actor.flush()
    live_strat._cached_order_factory = prev_cached_of

    return _extract_parity_order_intents(
        orders=list(bundle.node.cache.orders() or []),
        decisions=bundle.plan_gate.decisions,
    )


def verify_backtest_to_live_parity(
    *,
    plan_item: PlanItemContract,
    bars: Sequence[Bar],
    trading_date: date,
    dsn: Optional[str] = None,
    use_limit_orders: bool = True,
    submit_protective_stop: bool = True,
) -> IsomorphicParityReport:
    """Executes the Backtest-to-Live 100% Parity Gate for `plan_item` over `bars`."""
    normalized_bars = _normalize_bars_for_parity(bars)
    venue_str = plan_item.strategy_config.instrument_id.split(".", 1)[1].strip().upper()
    items_to_lock = _build_parity_plan_items(plan_item, venue_str)

    # 1. Capture BacktestEngine order intents with SnapshotPlanGateBackend + ProtectiveOcoMonitor
    bt_intents = _capture_pipeline_order_intents(
        plan_item=plan_item,
        bars=normalized_bars,
        dsn=None,
        trading_date=trading_date,
        environment_label="BACKTEST",
        use_limit_orders=use_limit_orders,
        submit_protective_stop=submit_protective_stop,
        include_telemetry_actor=False,
    )

    # 2. Drive the assembled LiveNode with identical SnapshotPlanGateBackend + ProtectiveOcoMonitor over identical bars
    can_persist_live = _has_existing_locked_plan_for_parity(
        dsn, trading_date, items_to_lock
    )
    live_snapshot_backend = SnapshotPlanGateBackend(
        plan_items=items_to_lock,
        entry_freeze=False,
        trading_date=trading_date,
        allow_multi_cycle_reentry=False,
    )
    assembler = NautilusLiveNodeAssembler(
        dsn=dsn or "postgresql://localhost:5432/trading",
        alpaca_config=AlpacaExecClientConfig(
            api_key="parity-gate-key",
            api_secret="parity-gate-secret",
        ),
    )
    bundle = assembler.build_live_node(
        trading_date=trading_date,
        plan_items=[items_to_lock[0]],
        plan_gate_backend=live_snapshot_backend,
        persist_telemetry_to_db=can_persist_live,
        use_limit_orders=use_limit_orders,
        submit_protective_stop=submit_protective_stop,
        warmup_strategies=False,
    )
    try:
        live_intents = _capture_live_node_order_intents(
            bundle=bundle,
            plan_item=plan_item,
            bars=normalized_bars,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
        )
    finally:
        if bundle is not None:
            try:
                if bundle.plan_gate.is_running():
                    bundle.plan_gate.stop()
            except Exception:
                pass
            try:
                if bundle.telemetry_actor.is_running():
                    bundle.telemetry_actor.stop()
            except Exception:
                pass
            bundle.plan_gate.close()
            bundle.telemetry_actor.close()
            bundle.node.dispose()
            bundle = None  # type: ignore[assignment]

    mismatches: List[str] = []
    if len(bt_intents) == 0:
        mismatches.append("Zero order intents emitted during parity gate run")
    if len(bt_intents) != len(live_intents):
        mismatches.append(
            f"Order intent count mismatch: backtest={len(bt_intents)} vs live={len(live_intents)}"
        )

    matched = 0
    total = max(len(bt_intents), len(live_intents), 1)
    for b_int, l_int in zip(bt_intents, live_intents):
        if b_int == l_int:
            matched += 1
        else:
            mismatches.append(
                f"Intent #{b_int.sequence_index} mismatch: backtest={b_int.to_dict()} vs live={l_int.to_dict()}"
            )

    parity_pct = (100.0 * float(matched) / float(total)) if bt_intents else 0.0
    passed = (len(mismatches) == 0) and (len(bt_intents) > 0)

    return IsomorphicParityReport(
        symbol=plan_item.symbol,
        archetype_id=plan_item.strategy_config.archetype_id.value,
        passed=passed,
        parity_pct=parity_pct,
        backtest_intents=bt_intents,
        live_intents=live_intents,
        mismatches=tuple(mismatches),
    )
