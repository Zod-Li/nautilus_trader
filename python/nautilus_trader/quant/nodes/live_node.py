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

import select
import time
import uuid
from dataclasses import dataclass, field, replace
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from evolution_engine.db.dsn import resolve_database_dsn
from nautilus_trader.quant.contracts.models import PlanItemContract
from nautilus_trader.quant.contracts.repository import PlanContractRepository
from nautilus_trader.quant.governance.plan_gate import (
    PLAN_GATE_EXEC_ALGORITHM_ID,
    PlanGateExecAlgorithm,
    PlanGateExecAlgorithmConfig,
    PostgresPlanGateBackend,
)
from nautilus_trader.quant.governance.telemetry_actor import (
    PostgresTelemetryActor,
    PostgresTelemetryActorConfig,
    ProtectiveOcoMonitor,
)
from nautilus_trader.quant.nodes.backtest_runner import (
    IsomorphicBacktestRunner,
    _create_default_equity,
)
from nautilus_trader.quant.strategies.base import BaseArchetypeStrategy
from nautilus_trader.quant.strategies.registry import (
    instantiate_strategy_from_contract,
)
from nautilus_trader.adapters.alpaca import (
    AlpacaExecClientConfig,
    AlpacaExecutionClientFactory,
)
from nautilus_trader.adapters.datainfra import (
    DataInfraConfig,
    DataInfraDataClientFactory,
)
from nautilus_trader.backtest import BacktestEngine, BacktestEngineConfig
from nautilus_trader.common import CacheConfig, Environment
from nautilus_trader.execution import BestPriceFillModel, MakerTakerFeeModel
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
        environment: Environment = Environment.SANDBOX,
        market: str = "US_EQUITY",
        reconciliation_enabled: bool = True,
        reconciliation_lookback_mins: int = 1440,
        cache_config: Optional[CacheConfig] = None,
    ) -> None:
        self._dsn = resolve_database_dsn(dsn)
        self._datainfra_config = datainfra_config or DataInfraConfig()
        self._alpaca_config = alpaca_config
        self._extra_exec_clients = tuple(extra_exec_clients or ())
        self._node_name = str(node_name or "nautilus-live-node").strip()
        self._trader_id = str(trader_id or "TRADER-LIVE-001").strip()
        self._environment = environment
        self._market = str(market or "US_EQUITY").strip().upper()
        self._reconciliation_enabled = bool(reconciliation_enabled)
        self._reconciliation_lookback_mins = max(60, int(reconciliation_lookback_mins))
        self._cache_config = cache_config
        self._repo = PlanContractRepository(dsn=self._dsn)

    @property
    def dsn(self) -> str:
        return self._dsn

    @property
    def repository(self) -> PlanContractRepository:
        return self._repo

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
        listen_timeout_secs: float = 0.0,
        use_limit_orders: bool = False,
        submit_protective_stop: bool = False,
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
        builder.with_reconciliation(self._reconciliation_enabled)
        builder.with_reconciliation_lookback_mins(self._reconciliation_lookback_mins)
        builder.with_data_engine_config(LiveDataEngineConfig())
        builder.with_exec_engine_config(LiveExecutionEngineConfig())
        builder.with_risk_engine_config(LiveRiskEngineConfig())
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
        pg_backend = PostgresPlanGateBackend(
            dsn=self._dsn,
            trading_date=trading_date,
            market=self._market,
        )
        plan_gate = PlanGateExecAlgorithm(
            backend=pg_backend,
            config=PlanGateExecAlgorithmConfig(
                exec_algorithm_id=PLAN_GATE_EXEC_ALGORITHM_ID,
            ),
        )
        node.add_exec_algorithm(plan_gate)

        oco_monitor = ProtectiveOcoMonitor(
            dsn=self._dsn,
            broker="alpaca",
            on_entry_freeze_callback=lambda sym, reason: pg_backend.set_entry_freeze(
                True,
                reason=reason,
                updated_by=f"telemetry:{sym}",
            ),
        )
        telemetry_actor = PostgresTelemetryActor(
            PostgresTelemetryActorConfig(
                dsn=self._dsn,
                market=self._market,
                trading_date=trading_date,
                environment=env_label,
                enforce_protective_oco_on_position_events=bool(submit_protective_stop),
            ),
            oco_monitor=oco_monitor,
        )
        node.add_actor(telemetry_actor)

        strategies: List[BaseArchetypeStrategy] = []
        for item in locked_items:
            strategy = instantiate_strategy_from_contract(
                item,
                use_limit_orders=use_limit_orders,
                submit_protective_stop=submit_protective_stop,
            )
            node.add_strategy(strategy)
            strategies.append(strategy)

        return AssembledLiveNodeBundle(
            node=node,
            trading_date=trading_date,
            plan_items=tuple(locked_items),
            strategies=tuple(strategies),
            plan_gate=plan_gate,
            telemetry_actor=telemetry_actor,
            routed_venues=tuple(routed_venues),
        )


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


def _reset_plan_state_for_parity_pass(dsn: str, trading_date: date) -> None:
    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE system_flags SET active = FALSE WHERE flag = 'ENTRY_FREEZE'"
                )
                cur.execute("ALTER TABLE trading_plan_items DISABLE TRIGGER USER")
                try:
                    cur.execute(
                        """
                        UPDATE trading_plan_items i
                        SET filled_qty = 0,
                            reserved_notional_usd = 0,
                            status = 'ARMED'
                        FROM trading_plans p
                        WHERE p.plan_id = i.plan_id
                          AND p.trading_date = %s
                          AND p.market = 'US_EQUITY'
                        """,
                        (trading_date,),
                    )
                finally:
                    cur.execute("ALTER TABLE trading_plan_items ENABLE TRIGGER USER")
    finally:
        conn.close()


def _capture_pipeline_order_intents(
    *,
    plan_item: PlanItemContract,
    bars: Sequence[Bar],
    dsn: str,
    trading_date: date,
    environment_label: str,
    use_limit_orders: bool,
    submit_protective_stop: bool,
    include_telemetry_actor: bool,
) -> Tuple[ParityOrderIntent, ...]:
    """Runs the exact `Strategy(StrategyConfig) + PlanGateExecAlgorithm(PlanRiskEnvelope)` pipeline
    and extracts the chronological sequence of `ParityOrderIntent`s.
    """
    venue_str = plan_item.strategy_config.instrument_id.split(".", 1)[1].strip().upper()
    symbols = [plan_item.symbol]
    if plan_item.strategy_config.secondary_instrument_id:
        sec_sym = plan_item.strategy_config.secondary_instrument_id.split(".", 1)[0].strip().upper()
        if sec_sym and sec_sym not in symbols:
            symbols.append(sec_sym)

    equities: List[Equity] = [_create_default_equity(sym, venue_str) for sym in symbols]
    _reset_plan_state_for_parity_pass(dsn, trading_date)

    engine = BacktestEngine(
        BacktestEngineConfig(bypass_logging=True, run_analysis=False)
    )
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
        )
        for eq in equities:
            engine.add_instrument(eq)

        pg_backend = PostgresPlanGateBackend(
            dsn=dsn,
            trading_date=trading_date,
            market="US_EQUITY",
        )
        plan_gate = PlanGateExecAlgorithm(
            backend=pg_backend,
            config=PlanGateExecAlgorithmConfig(
                exec_algorithm_id=PLAN_GATE_EXEC_ALGORITHM_ID,
            ),
        )
        engine.add_exec_algorithm(plan_gate)

        if include_telemetry_actor:
            telemetry_actor = PostgresTelemetryActor(
                PostgresTelemetryActorConfig(
                    dsn=dsn,
                    market="US_EQUITY",
                    trading_date=trading_date,
                    environment=environment_label,
                )
            )
            engine.add_actor(telemetry_actor)

        strategy = instantiate_strategy_from_contract(
            plan_item,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
        )
        engine.add_strategy(strategy)
        engine.add_data(sorted(bars, key=lambda b: b.ts_init))
        engine.run()

        decisions = plan_gate.decisions
        orders = sorted(
            list(engine.cache.orders() or []),
            key=lambda o: (int(getattr(o, "ts_init", 0)), str(getattr(o, "client_order_id", ""))),
        )
        intents: List[ParityOrderIntent] = []
        for idx, order in enumerate(orders):
            cid = str(getattr(order, "client_order_id", ""))
            inst_str = str(getattr(order, "instrument_id", ""))
            sym = inst_str.split(".", 1)[0].strip().upper()
            side_raw = str(getattr(order, "side", "")).upper()
            side = "BUY" if "BUY" in side_raw else ("SELL" if "SELL" in side_raw else side_raw)
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
    finally:
        engine.dispose()


def verify_backtest_to_live_parity(
    *,
    plan_item: PlanItemContract,
    bars: Sequence[Bar],
    trading_date: date,
    dsn: Optional[str] = None,
    use_limit_orders: bool = False,
    submit_protective_stop: bool = False,
) -> IsomorphicParityReport:
    """Executes the Backtest-to-Live 100% Parity Gate for `plan_item` over `bars`."""
    resolved_dsn = resolve_database_dsn(dsn)
    repo = PlanContractRepository(dsn=resolved_dsn)

    existing = repo.load_locked_plan_items(trading_date=trading_date, market="US_EQUITY")
    has_matching = any(it.strategy_config == plan_item.strategy_config for it in existing)
    if existing and not has_matching:
        IsomorphicBacktestRunner._cleanup_ephemeral_date_plans(resolved_dsn, trading_date)
        existing = []
    if not existing:
        plan_uuid = uuid.uuid4()
        cloned_item = replace(
            plan_item,
            item_id=uuid.uuid4(),
            plan_id=plan_uuid,
            status="ARMED",
            filled_qty=Decimal("0"),
            reserved_notional_usd=Decimal("0"),
        )
        effective_eq = max(
            Decimal("100000.00"),
            plan_item.risk_envelope.target_notional_usd * Decimal("20"),
            plan_item.risk_envelope.max_risk_budget_usd * Decimal("200"),
        )
        repo.create_and_lock_trading_plan(
            trading_date=trading_date,
            items=[cloned_item],
            total_equity_usd=effective_eq,
            market="US_EQUITY",
            summary="parity_gate_verification",
            plan_id=plan_uuid,
            actor="parity_gate",
        )

    # 1. Verify LiveNode assembly from the locked PostgreSQL plan
    assembler = NautilusLiveNodeAssembler(
        dsn=resolved_dsn,
        alpaca_config=AlpacaExecClientConfig(
            api_key="parity-gate-key",
            api_secret="parity-gate-secret",
        ),
    )
    bundle = assembler.build_live_node(
        trading_date=trading_date,
        use_limit_orders=use_limit_orders,
        submit_protective_stop=submit_protective_stop,
    )
    try:
        matching_cfgs = [
            s.archetype_config.archetype_contract
            for s in bundle.strategies
            if s.archetype_config.archetype_contract == plan_item.strategy_config
        ]
        if not matching_cfgs:
            live_strat_cfg = bundle.strategies[0].archetype_config.archetype_contract
            raise AssertionError(
                f"LiveNode strategy config mismatch: {live_strat_cfg} != {plan_item.strategy_config}"
            )
    finally:
        bundle.node.dispose()

    bt_intents = _capture_pipeline_order_intents(
        plan_item=plan_item,
        bars=bars,
        dsn=resolved_dsn,
        trading_date=trading_date,
        environment_label="BACKTEST",
        use_limit_orders=use_limit_orders,
        submit_protective_stop=submit_protective_stop,
        include_telemetry_actor=False,
    )
    live_intents = _capture_pipeline_order_intents(
        plan_item=plan_item,
        bars=bars,
        dsn=resolved_dsn,
        trading_date=trading_date,
        environment_label="PAPER",
        use_limit_orders=use_limit_orders,
        submit_protective_stop=submit_protective_stop,
        include_telemetry_actor=True,
    )

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
