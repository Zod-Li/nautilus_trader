"""Isomorphic Backtest Runner, Walk-Forward Calibrator, and Counterfactual Stress Falsifier.

Implements Ticket #14 (`14-isomorphic-backtest-runner-and-stress-falsifier-seam.md`):
- Runs the exact 1:1 isomorphic combination `BacktestEngine` + `Strategy(StrategyConfig)` +
  `PlanGateExecAlgorithm(PlanRiskEnvelope)` over `DataInfraCatalog` historical bars/quotes.
- Computes non-annualized Per-Period Deflated Sharpe Ratio (DSR) per Bailey & Lopez de Prado,
  Walk-Forward In-Sample / Out-of-Sample statistics, and Pareto dominance frontier.
- Executes 3 counterfactual stress falsification experiments in `BacktestEngine`:
  1. `2x` Friction & Slippage Stress Test (`double_friction_stress`)
  2. `+1 Bar` Execution Signal Delay Stress Test (`plus_one_bar_delay_stress`)
  3. Empirical Tail Adverse Opening Gap Shock Test (`tail_gap_shock_stress`)
"""

from __future__ import annotations

import math
import statistics
import uuid
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

from nautilus_trader.quant.contracts.models import (
    PlanItemContract,
    PlanSide,
)
from nautilus_trader.quant.governance.plan_gate import (
    PLAN_GATE_EXEC_ALGORITHM_ID,
    PlanGateExecAlgorithm,
    PlanGateExecAlgorithmConfig,
    SnapshotPlanGateBackend,
    build_stat_arb_secondary_plan_item,
)
from nautilus_trader.quant.governance.telemetry_actor import (
    PostgresTelemetryActor,
    PostgresTelemetryActorConfig,
    ProtectiveOcoMonitor,
)
from nautilus_trader.quant.strategies.registry import (
    instantiate_strategy_from_contract,
)
from nautilus_trader.adapters.datainfra import DataInfraCatalog, DataInfraConfig
from nautilus_trader.backtest import BacktestEngine, BacktestEngineConfig
from nautilus_trader.execution import (
    BestPriceFillModel,
    FillModel,
    MakerTakerFeeModel,
    OneTickSlippageFillModel,
    StaticLatencyModel,
)
from nautilus_trader.model import (
    AccountType,
    Bar,
    Currency,
    Equity,
    InstrumentId,
    InstrumentStatus,
    Money,
    OmsType,
    OrderStatus,
    Price,
    Quantity,
    QuoteTick,
    Venue,
)

EULER_MASCHERONI: float = 0.5772156649015329

BARS_PER_TRADING_YEAR: Dict[str, float] = {
    "1m": 252.0 * 390.0,
    "5m": 252.0 * 78.0,
    "15m": 252.0 * 26.0,
    "1h": 252.0 * 6.5,
    "4h": 252.0 * 2.0,
    "1d": 252.0,
    "1w": 52.0,
}


def _normal_cdf(x: float) -> float:
    r"""Standard normal cumulative distribution function $\Phi(x)$."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _normal_ppf(p: float) -> float:
    r"""Acklam's rational approximation to the standard normal inverse CDF $\Phi^{-1}(p)$."""
    clamped = min(max(float(p), 1e-12), 1.0 - 1e-12)
    a = (
        -3.969683028665376e01,
        2.209460984245205e02,
        -2.759285104469687e02,
        1.383577518672690e02,
        -3.066479806614716e01,
        2.506628277459239e00,
    )
    b = (
        -5.4476098798222406e01,
        1.615858368580409e02,
        -1.556989798598866e02,
        6.680131188771972e01,
        -1.328068155288572e01,
    )
    c = (
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838e00,
        -2.549732539343734e00,
        4.374664141464968e00,
        2.938163982698783e00,
    )
    d = (
        7.784695709041462e-03,
        3.224671290700398e-01,
        2.445134137142996e00,
        3.754408661907416e00,
    )
    p_low = 0.02425
    p_high = 1.0 - p_low
    if clamped < p_low:
        q = math.sqrt(-2.0 * math.log(clamped))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            (((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0
        )
    if clamped <= p_high:
        q = clamped - 0.5
        r = q * q
        return (
            (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
        ) / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    q = math.sqrt(-2.0 * math.log(1.0 - clamped))
    return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
        (((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0
    )


def compute_per_period_deflated_sharpe_ratio(
    period_returns: Sequence[float],
    effective_trials: int = 1,
) -> float:
    r"""Computes the non-annualized Per-Period Deflated Sharpe Ratio (Bailey & Lopez de Prado).

    Both $\widehat{SR}$ and the expected maximum Sharpe benchmark $\widehat{SR}_0$
    are evaluated strictly on the non-annualized per-period scale so sample size
    $\sqrt{T - 1}$ is never distorted by annualization factors.
    """
    n = len(period_returns)
    if n < 3:
        return 0.0
    mean_ret = statistics.fmean(period_returns)
    std_ret = statistics.stdev(period_returns)
    if std_ret <= 1e-12:
        return 1.0 if mean_ret > 0.0 else 0.0

    sr_hat = mean_ret / std_ret
    n_trials = max(1, int(effective_trials))
    if n_trials <= 1:
        sr_zero = 0.0
    else:
        z1 = _normal_ppf(1.0 - 1.0 / float(n_trials))
        z2 = _normal_ppf(1.0 - 1.0 / (float(n_trials) * math.e))
        sr_zero = (1.0 - EULER_MASCHERONI) * z1 + EULER_MASCHERONI * z2
        sr_zero = sr_zero / math.sqrt(float(n - 1))

    centered = [(r - mean_ret) / std_ret for r in period_returns]
    skew = sum(z**3 for z in centered) / float(n)
    kurt = sum(z**4 for z in centered) / float(n)

    denom_sq = 1.0 - skew * sr_hat + ((kurt - 1.0) / 4.0) * (sr_hat**2)
    denom = math.sqrt(max(denom_sq, 1e-9))
    z_stat = ((sr_hat - sr_zero) * math.sqrt(float(n - 1))) / denom
    return float(_normal_cdf(z_stat))


def _parse_money_float(val: Any) -> float:
    if val is None:
        return 0.0
    if isinstance(val, (int, float, Decimal)):
        return float(val)
    text = str(val).strip()
    if not text:
        return 0.0
    token = text.split()[0].replace("_", "").replace(",", "")
    try:
        return float(token)
    except ValueError:
        return 0.0


def _create_default_equity(symbol: str, venue_str: str = "ALPACA") -> Equity:
    inst_id = InstrumentId.from_str(f"{symbol}.{venue_str}")
    return Equity(
        instrument_id=inst_id,
        raw_symbol=inst_id.symbol,
        currency=Currency.from_str("USD"),
        price_precision=2,
        price_increment=Price.from_str("0.01"),
        lot_size=Quantity.from_int(1),
        ts_event=0,
        ts_init=0,
    )


def _clone_bar_with_prices(
    bar: Bar,
    *,
    open_px: float,
    high_px: float,
    low_px: float,
    close_px: float,
    ts_event: Optional[int] = None,
    ts_init: Optional[int] = None,
) -> Bar:
    precision = bar.close.precision
    hi = max(open_px, high_px, low_px, close_px)
    lo = min(open_px, high_px, low_px, close_px)
    lo = max(0.01, lo)
    hi = max(lo, hi)
    op = min(max(open_px, lo), hi)
    cl = min(max(close_px, lo), hi)
    return Bar(
        bar_type=bar.bar_type,
        open=Price(op, precision),
        high=Price(hi, precision),
        low=Price(lo, precision),
        close=Price(cl, precision),
        volume=bar.volume,
        ts_event=int(bar.ts_event if ts_event is None else ts_event),
        ts_init=int(bar.ts_init if ts_init is None else ts_init),
    )


def _normalize_bars_for_parity(bars: Sequence[Bar]) -> List[Bar]:
    normalized: List[Bar] = []
    for b in bars:
        b_close_ns = max(
            int(getattr(b, "ts_init", 0) or 0),
            int(getattr(b, "ts_event", 0) or 0),
        )
        if int(b.ts_event) != b_close_ns or int(b.ts_init) != b_close_ns:
            normalized.append(
                _clone_bar_with_prices(
                    b,
                    open_px=float(b.open),
                    high_px=float(b.high),
                    low_px=float(b.low),
                    close_px=float(b.close),
                    ts_event=b_close_ns,
                    ts_init=b_close_ns,
                )
            )
        else:
            normalized.append(b)
    return sorted(normalized, key=lambda b: int(b.ts_init))


def _resolve_trading_date_from_bars(
    bars: Sequence[Bar],
    fallback: Optional[date] = None,
) -> date:
    if fallback is not None:
        return fallback
    if bars:
        max_bar_ts_ns = max(
            int(getattr(b, "ts_init", 0) or getattr(b, "ts_event", 0) or 0)
            for b in bars
        )
        if max_bar_ts_ns > 100_000_000_000_000_000:
            return datetime.fromtimestamp(max_bar_ts_ns / 1e9, tz=timezone.utc).date()
    return datetime.now(tz=timezone.utc).date()


def _build_parity_plan_items(
    plan_item: PlanItemContract,
    venue_str: str,
) -> List[PlanItemContract]:
    ephemeral_plan_id = plan_item.plan_id or uuid.uuid4()
    cloned_primary = replace(
        plan_item,
        item_id=plan_item.item_id or uuid.uuid4(),
        plan_id=ephemeral_plan_id,
        venue=plan_item.venue or venue_str,
        status="ARMED",
        filled_qty=Decimal("0"),
        reserved_notional_usd=Decimal("0"),
    )
    items_to_lock: List[PlanItemContract] = [cloned_primary]
    sec_item = build_stat_arb_secondary_plan_item(cloned_primary)
    if sec_item is not None:
        items_to_lock.append(sec_item)
    return items_to_lock


@dataclass(frozen=True)
class WalkForwardSummary:
    """In-Sample (IS) and Out-of-Sample (OOS) Walk-Forward evaluation summary."""

    in_sample_bars: int
    out_of_sample_bars: int
    in_sample_sharpe: float
    out_of_sample_sharpe: float
    out_of_sample_dsr: float
    in_sample_net_pnl_usd: float
    out_of_sample_net_pnl_usd: float
    out_of_sample_max_drawdown_pct: float
    retention_ratio: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "in_sample_bars": self.in_sample_bars,
            "out_of_sample_bars": self.out_of_sample_bars,
            "in_sample_sharpe": round(self.in_sample_sharpe, 6),
            "out_of_sample_sharpe": round(self.out_of_sample_sharpe, 6),
            "out_of_sample_dsr": round(self.out_of_sample_dsr, 6),
            "in_sample_net_pnl_usd": round(self.in_sample_net_pnl_usd, 4),
            "out_of_sample_net_pnl_usd": round(self.out_of_sample_net_pnl_usd, 4),
            "out_of_sample_max_drawdown_pct": round(self.out_of_sample_max_drawdown_pct, 6),
            "retention_ratio": round(self.retention_ratio, 6),
        }


@dataclass(frozen=True)
class IsomorphicBacktestMetrics:
    """Structured performance and execution audit metrics from `IsomorphicBacktestRunner`."""

    symbol: str
    archetype_id: str
    bar_timeframe: str
    starting_capital_usd: float
    ending_capital_usd: float
    net_pnl_usd: float
    gross_pnl_usd: float
    total_return_pct: float
    sharpe_ratio: float
    per_period_dsr: float
    max_drawdown_pct: float
    max_single_trade_loss_usd: float
    turnover_ratio: float
    total_commissions_usd: float
    estimated_slippage_cost_usd: float
    total_orders: int
    filled_orders: int
    denied_orders: int
    total_positions: int
    win_rate: float
    profit_factor: float
    plan_gate_admitted_orders: int
    plan_gate_denied_orders: int
    walk_forward: Optional[WalkForwardSummary] = None
    pareto_non_dominated: bool = True
    effective_trials: int = 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "archetype_id": self.archetype_id,
            "bar_timeframe": self.bar_timeframe,
            "starting_capital_usd": round(self.starting_capital_usd, 2),
            "ending_capital_usd": round(self.ending_capital_usd, 2),
            "net_pnl_usd": round(self.net_pnl_usd, 4),
            "gross_pnl_usd": round(self.gross_pnl_usd, 4),
            "total_return_pct": round(self.total_return_pct, 6),
            "sharpe_ratio": round(self.sharpe_ratio, 6),
            "per_period_dsr": round(self.per_period_dsr, 6),
            "effective_trials": int(self.effective_trials),
            "max_drawdown_pct": round(self.max_drawdown_pct, 6),
            "max_single_trade_loss_usd": round(self.max_single_trade_loss_usd, 4),
            "turnover_ratio": round(self.turnover_ratio, 6),
            "total_commissions_usd": round(self.total_commissions_usd, 4),
            "estimated_slippage_cost_usd": round(self.estimated_slippage_cost_usd, 4),
            "total_orders": self.total_orders,
            "filled_orders": self.filled_orders,
            "denied_orders": self.denied_orders,
            "total_positions": self.total_positions,
            "win_rate": round(self.win_rate, 6),
            "profit_factor": round(self.profit_factor, 6),
            "plan_gate_admitted_orders": self.plan_gate_admitted_orders,
            "plan_gate_denied_orders": self.plan_gate_denied_orders,
            "walk_forward": (
                self.walk_forward.to_dict() if self.walk_forward is not None else None
            ),
            "pareto_non_dominated": self.pareto_non_dominated,
        }


def select_pareto_frontier(
    candidates: Sequence[IsomorphicBacktestMetrics],
) -> List[IsomorphicBacktestMetrics]:
    """Identifies the Pareto-optimal non-dominated frontier across `(sharpe_ratio, -max_drawdown_pct, -turnover_ratio)`."""
    annotated: List[IsomorphicBacktestMetrics] = []
    for idx_i, cand_i in enumerate(candidates):
        dominated = False
        for idx_j, cand_j in enumerate(candidates):
            if idx_i == idx_j:
                continue
            ge_all = (
                cand_j.sharpe_ratio >= cand_i.sharpe_ratio
                and cand_j.max_drawdown_pct <= cand_i.max_drawdown_pct
                and cand_j.turnover_ratio <= cand_i.turnover_ratio
            )
            gt_one = (
                cand_j.sharpe_ratio > cand_i.sharpe_ratio
                or cand_j.max_drawdown_pct < cand_i.max_drawdown_pct
                or cand_j.turnover_ratio < cand_i.turnover_ratio
            )
            if ge_all and gt_one:
                dominated = True
                break
        annotated.append(replace(cand_i, pareto_non_dominated=not dominated))
    return [c for c in annotated if c.pareto_non_dominated]


class IsomorphicBacktestRunner:
    """Runs 1:1 isomorphic backtests in `Nautilus BacktestEngine` with `PlanGateExecAlgorithm`."""

    def __init__(
        self,
        dsn: Optional[str] = None,
        datainfra_config: Optional[DataInfraConfig] = None,
        starting_capital_usd: Decimal | float | str = Decimal("100000.00"),
        maker_fee_rate: Decimal | float | str = Decimal("0.0002"),
        taker_fee_rate: Decimal | float | str = Decimal("0.0005"),
        slippage_bps: Decimal | float | str = Decimal("2.5"),
        effective_trials: int = 1,
        venue_name: str = "ALPACA",
    ) -> None:
        self._dsn = dsn
        self._datainfra_config = datainfra_config
        self._starting_capital_usd = Decimal(str(starting_capital_usd))
        self._maker_fee_rate = Decimal(str(maker_fee_rate))
        self._taker_fee_rate = Decimal(str(taker_fee_rate))
        self._slippage_bps = Decimal(str(slippage_bps))
        self._effective_trials = max(1, int(effective_trials))
        self._venue_name = str(venue_name or "ALPACA").strip().upper()
        self._last_first_entry_fill: Optional[Tuple[int, float]] = None
        self._last_plan_gate: Optional[PlanGateExecAlgorithm] = None
        self._last_oco_monitor: Optional[ProtectiveOcoMonitor] = None
        self._last_telemetry_actor: Optional[PostgresTelemetryActor] = None
        self._last_orders: List[Any] = []
        self._last_period_returns: List[float] = []

    def load_historical_data(
        self,
        plan_item: PlanItemContract,
        start_ns: Optional[int] = None,
        end_ns: Optional[int] = None,
        include_quotes: bool = False,
        include_statuses: bool = False,
    ) -> Tuple[List[Equity], List[Bar], List[QuoteTick], List[InstrumentStatus]]:
        """Loads instruments, bars, quotes, and statuses from `DataInfraCatalog`."""
        if self._datainfra_config is None:
            raise ValueError(
                "DataInfraConfig is required to load historical data from DataInfraCatalog"
            )
        catalog = DataInfraCatalog(self._datainfra_config)
        symbols = [plan_item.symbol]
        sec_id = plan_item.strategy_config.secondary_instrument_id
        if sec_id:
            sec_sym = sec_id.split(".", 1)[0].strip().upper()
            if sec_sym and sec_sym not in symbols:
                symbols.append(sec_sym)

        venue = plan_item.venue or self._venue_name
        equities = [
            catalog.register_equity(sym, venue=venue, lot_size=1)
            for sym in symbols
        ]
        bars = catalog.query_bars(
            symbols,
            start_ts_ns=start_ns,
            end_ts_ns=end_ns,
            timeframe=plan_item.strategy_config.bar_timeframe,
            adjustment="adjusted",
            market_session="REG",
            venue=venue,
        )
        quotes = (
            catalog.query_quotes(
                symbols,
                start_ts_ns=start_ns,
                end_ts_ns=end_ns,
                venue=venue,
            )
            if include_quotes
            else []
        )
        statuses = (
            catalog.query_statuses(
                symbols,
                start_ts_ns=start_ns,
                end_ts_ns=end_ns,
                venue=venue,
            )
            if include_statuses
            else []
        )
        return equities, bars, quotes, statuses

    def run_backtest(
        self,
        plan_item: PlanItemContract,
        *,
        bars: Optional[Sequence[Bar]] = None,
        quotes: Optional[Sequence[QuoteTick]] = None,
        statuses: Optional[Sequence[InstrumentStatus]] = None,
        equities: Optional[Sequence[Equity]] = None,
        start_ns: Optional[int] = None,
        end_ns: Optional[int] = None,
        use_limit_orders: bool = True,
        submit_protective_stop: bool = True,
        execution_delay_bars: int = 0,
        fee_multiplier: float = 1.0,
        slippage_multiplier: float = 1.0,
        fill_model: Optional[FillModel] = None,
        trading_date: Optional[date] = None,
        walk_forward: Optional[WalkForwardSummary] = None,
        effective_trials: Optional[int] = None,
    ) -> IsomorphicBacktestMetrics:
        """Executes a 5-axis isomorphic backtest in `BacktestEngine` with `PlanGateExecAlgorithm(SnapshotPlanGateBackend)` and `ProtectiveOcoMonitor`."""
        venue_str = (
            plan_item.strategy_config.instrument_id.split(".", 1)[1].strip().upper()
            if "." in plan_item.strategy_config.instrument_id
            else self._venue_name
        )
        if bars is None:
            loaded_equities, loaded_bars, loaded_quotes, loaded_statuses = (
                self.load_historical_data(
                    plan_item,
                    start_ns=start_ns,
                    end_ns=end_ns,
                )
            )
            bar_list = list(loaded_bars)
            quote_list = list(loaded_quotes if quotes is None else quotes)
            status_list = list(loaded_statuses if statuses is None else statuses)
            equity_list = list(loaded_equities if equities is None else equities)
        else:
            bar_list = list(bars)
            quote_list = list(quotes or [])
            status_list = list(statuses or [])
            if equities is not None:
                equity_list = list(equities)
            else:
                symbols = [plan_item.symbol]
                sec_id = plan_item.strategy_config.secondary_instrument_id
                if sec_id:
                    sec_sym = sec_id.split(".", 1)[0].strip().upper()
                    if sec_sym and sec_sym not in symbols:
                        symbols.append(sec_sym)
                equity_list = [
                    _create_default_equity(sym, venue_str) for sym in symbols
                ]

        if not bar_list:
            raise ValueError(f"Cannot run backtest for {plan_item.symbol}: bar list is empty")

        bar_list = _normalize_bars_for_parity(bar_list)
        effective_date = _resolve_trading_date_from_bars(bar_list, fallback=trading_date)
        items_to_lock = _build_parity_plan_items(plan_item, venue_str)
        cloned_primary = items_to_lock[0]

        engine = BacktestEngine(
            BacktestEngineConfig(bypass_logging=True, run_analysis=False)
        )
        plan_gate: Optional[PlanGateExecAlgorithm] = None
        telemetry_actor: Optional[PostgresTelemetryActor] = None
        try:
            eff_maker = self._maker_fee_rate * Decimal(str(max(0.0, fee_multiplier)))
            eff_taker = self._taker_fee_rate * Decimal(str(max(0.0, fee_multiplier)))
            effective_fill_model = fill_model or BestPriceFillModel(
                prob_fill_on_limit=1.0,
                prob_slippage=0.0,
            )
            engine.add_venue(
                venue=Venue(venue_str),
                oms_type=OmsType.NETTING,
                account_type=AccountType.MARGIN,
                starting_balances=[Money.from_str(f"{self._starting_capital_usd:.2f} USD")],
                base_currency=Currency.from_str("USD"),
                fill_model=effective_fill_model,
                fee_model=MakerTakerFeeModel(
                    maker_rate=eff_maker,
                    taker_rate=eff_taker,
                ),
                latency_model=StaticLatencyModel(
                    base_latency_nanos=0,
                    insert_latency_nanos=1,
                    update_latency_nanos=0,
                    cancel_latency_nanos=0,
                ),
            )
            for eq in equity_list:
                engine.add_instrument(eq)

            snapshot_backend = SnapshotPlanGateBackend(
                plan_items=items_to_lock,
                entry_freeze=False,
                trading_date=effective_date,
            )
            oco_monitor = ProtectiveOcoMonitor(
                dsn=None,
                persist_to_db=False,
                on_entry_freeze_callback=lambda sym, reason: snapshot_backend.set_entry_freeze(
                    True,
                    reason=reason,
                    updated_by=f"nautilus:protective_oco_monitor:{sym}",
                ),
            )
            telemetry_actor = PostgresTelemetryActor(
                config=PostgresTelemetryActorConfig(
                    dsn=None,
                    persist_to_db=False,
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
                cloned_primary,
                use_limit_orders=use_limit_orders,
                submit_protective_stop=submit_protective_stop,
                execution_delay_bars=execution_delay_bars,
            )
            strategy.bind_telemetry_actor(telemetry_actor)
            if hasattr(strategy, "bind_plan_gate"):
                strategy.bind_plan_gate(plan_gate)
            engine.add_strategy(strategy)

            if status_list:
                engine.add_data(sorted(status_list, key=lambda s: s.ts_init))
            if quote_list:
                engine.add_data(sorted(quote_list, key=lambda q: q.ts_init))
            engine.add_data(bar_list)

            engine.run()

            self._last_plan_gate = plan_gate
            self._last_oco_monitor = oco_monitor
            self._last_telemetry_actor = telemetry_actor
            return self._extract_metrics(
                engine=engine,
                plan_item=cloned_primary,
                bars=bar_list,
                plan_gate=plan_gate,
                slippage_multiplier=slippage_multiplier,
                walk_forward=walk_forward,
                effective_trials=effective_trials,
            )
        finally:
            if telemetry_actor is not None:
                telemetry_actor.close()
            if plan_gate is not None:
                plan_gate.close()
            engine.dispose()

    def run_walk_forward(
        self,
        plan_item: PlanItemContract,
        *,
        bars: Optional[Sequence[Bar]] = None,
        equities: Optional[Sequence[Equity]] = None,
        in_sample_ratio: float = 0.65,
        use_limit_orders: bool = True,
        submit_protective_stop: bool = True,
        effective_trials: Optional[int] = None,
    ) -> IsomorphicBacktestMetrics:
        """Runs Walk-Forward In-Sample (IS) and Out-of-Sample (OOS) validation plus full-window backtest."""
        if not (0.20 <= in_sample_ratio <= 0.90):
            raise ValueError(f"in_sample_ratio must be in [0.20, 0.90], got {in_sample_ratio}")

        if bars is None:
            loaded_equities, loaded_bars, _, _ = self.load_historical_data(plan_item)
            bar_list = sorted(loaded_bars, key=lambda b: b.ts_init)
            equity_list = list(loaded_equities)
        else:
            bar_list = sorted(bars, key=lambda b: b.ts_init)
            equity_list = list(equities) if equities is not None else None

        primary_inst_str = plan_item.strategy_config.instrument_id
        primary_bars = [
            b for b in bar_list if str(b.bar_type.instrument_id) == primary_inst_str
        ]
        min_required = plan_item.strategy_config.lookback_window + 2
        if len(primary_bars) < min_required + 4:
            raise ValueError(
                f"Walk-forward requires at least {min_required + 4} primary bars, got {len(primary_bars)}"
            )

        split_idx = max(
            min_required,
            min(
                len(primary_bars) - 2,
                int(round(len(primary_bars) * in_sample_ratio)),
            ),
        )
        split_ts = primary_bars[split_idx].ts_init
        is_bars = [b for b in bar_list if b.ts_init < split_ts]
        warmup_bars = (
            int(plan_item.strategy_config.lookback_window)
            + int(plan_item.strategy_config.slow_window)
        )
        warmup_start_idx = max(0, split_idx - warmup_bars)
        oos_start_ts = primary_bars[warmup_start_idx].ts_init
        oos_bars = [b for b in bar_list if b.ts_init >= oos_start_ts]

        is_metrics = self.run_backtest(
            plan_item,
            bars=is_bars,
            equities=equity_list,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            trading_date=_resolve_trading_date_from_bars(is_bars),
            effective_trials=effective_trials,
        )
        oos_metrics = self.run_backtest(
            plan_item,
            bars=oos_bars,
            equities=equity_list,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            trading_date=_resolve_trading_date_from_bars(oos_bars),
            effective_trials=effective_trials,
        )
        retention = (
            oos_metrics.sharpe_ratio / is_metrics.sharpe_ratio
            if abs(is_metrics.sharpe_ratio) > 1e-6
            else (1.0 if oos_metrics.sharpe_ratio >= 0.0 else 0.0)
        )
        wf_summary = WalkForwardSummary(
            in_sample_bars=split_idx,
            out_of_sample_bars=len(primary_bars) - split_idx,
            in_sample_sharpe=is_metrics.sharpe_ratio,
            out_of_sample_sharpe=oos_metrics.sharpe_ratio,
            out_of_sample_dsr=oos_metrics.per_period_dsr,
            in_sample_net_pnl_usd=is_metrics.net_pnl_usd,
            out_of_sample_net_pnl_usd=oos_metrics.net_pnl_usd,
            out_of_sample_max_drawdown_pct=oos_metrics.max_drawdown_pct,
            retention_ratio=retention,
        )
        return self.run_backtest(
            plan_item,
            bars=bar_list,
            equities=equity_list,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            trading_date=_resolve_trading_date_from_bars(bar_list),
            walk_forward=wf_summary,
            effective_trials=effective_trials,
        )

    def _extract_metrics(
        self,
        *,
        engine: BacktestEngine,
        plan_item: PlanItemContract,
        bars: Sequence[Bar],
        plan_gate: Optional[PlanGateExecAlgorithm],
        slippage_multiplier: float,
        walk_forward: Optional[WalkForwardSummary],
        effective_trials: Optional[int] = None,
    ) -> IsomorphicBacktestMetrics:
        start_cap = float(self._starting_capital_usd)
        orders = list(engine.cache.orders() or [])
        raw_positions = list(engine.cache.position_snapshots() or []) + list(
            engine.cache.positions() or []
        )
        seen_pos_cycles: set[Tuple[str, int]] = set()
        positions: List[Any] = []
        for pos in raw_positions:
            cycle_key = (
                str(getattr(pos, "id", "")),
                int(getattr(pos, "ts_opened", 0) or 0),
            )
            if cycle_key not in seen_pos_cycles:
                seen_pos_cycles.add(cycle_key)
                positions.append(pos)

        total_orders = len(orders)
        filled_orders = sum(1 for o in orders if o.status == OrderStatus.FILLED)
        denied_orders = sum(
            1
            for o in orders
            if o.status in (OrderStatus.DENIED, OrderStatus.REJECTED)
        )

        total_commissions = 0.0
        traded_notional = 0.0
        trade_pnls: List[float] = []

        primary_inst = plan_item.strategy_config.instrument_id
        primary_bars = sorted(
            [b for b in bars if str(b.bar_type.instrument_id) == primary_inst],
            key=lambda b: b.ts_init,
        )
        last_close_by_inst: Dict[str, float] = {}
        for b in bars:
            last_close_by_inst[str(b.bar_type.instrument_id)] = float(b.close)

        first_entry_fill: Optional[Tuple[int, float]] = None
        for o in sorted(
            orders,
            key=lambda ord_obj: int(
                getattr(ord_obj, "ts_last", 0) or getattr(ord_obj, "ts_init", 0)
            ),
        ):
            if o.status == OrderStatus.FILLED:
                qty_f = _parse_money_float(
                    getattr(o, "filled_qty", None) or getattr(o, "quantity", 0)
                )
                px_f = _parse_money_float(
                    getattr(o, "avg_px", None) or getattr(o, "price", 0)
                )
                if px_f <= 0.0:
                    px_f = last_close_by_inst.get(str(o.instrument_id), 0.0)
                traded_notional += abs(qty_f * px_f)
                if (
                    first_entry_fill is None
                    and not bool(getattr(o, "is_reduce_only", False))
                    and str(getattr(o, "instrument_id", "")) == primary_inst
                ):
                    fill_ts = int(
                        getattr(o, "ts_last", 0) or getattr(o, "ts_init", 0)
                    )
                    first_entry_fill = (fill_ts, px_f)
        self._last_first_entry_fill = first_entry_fill

        net_pnl = 0.0
        for pos in positions:
            realized = _parse_money_float(getattr(pos, "realized_pnl", 0.0))
            inst_key = str(getattr(pos, "instrument_id", primary_inst))
            last_px = last_close_by_inst.get(inst_key, 0.0)
            unrealized = 0.0
            if getattr(pos, "is_open", False) and last_px > 0.0:
                try:
                    unrealized = _parse_money_float(
                        pos.unrealized_pnl(Price.from_str(f"{last_px:.2f}"))
                    )
                except Exception:
                    unrealized = 0.0
            pos_comms = sum(
                _parse_money_float(c)
                for c in (getattr(pos, "commissions", lambda: [])() or [])
            )
            total_commissions += abs(pos_comms)
            pos_total_pnl = realized + unrealized
            net_pnl += pos_total_pnl
            trade_pnls.append(pos_total_pnl)

        slippage_rate = (float(self._slippage_bps) / 10_000.0) * max(
            0.0, float(slippage_multiplier)
        )
        est_slippage_cost = traded_notional * slippage_rate
        if est_slippage_cost > 0.0:
            net_pnl -= est_slippage_cost

        gross_pnl = net_pnl + total_commissions + est_slippage_cost
        end_cap = start_cap + net_pnl
        total_return_pct = (net_pnl / start_cap) if start_cap > 0.0 else 0.0

        period_returns, max_dd_pct = self._build_equity_curve_returns(
            primary_bars=primary_bars,
            orders=orders,
            start_cap=start_cap,
            net_pnl_final=net_pnl,
            all_bars=bars,
            total_commissions=total_commissions,
            est_slippage_cost=est_slippage_cost,
        )
        self._last_orders = list(orders)
        self._last_period_returns = list(period_returns)

        ann_factor = math.sqrt(
            BARS_PER_TRADING_YEAR.get(plan_item.strategy_config.bar_timeframe, 252.0)
        )
        if len(period_returns) >= 2:
            mean_r = statistics.fmean(period_returns)
            std_r = statistics.stdev(period_returns)
            sharpe = (mean_r / std_r) * ann_factor if std_r > 1e-12 else 0.0
        else:
            sharpe = 0.0

        eff_trials = max(
            1,
            int(
                effective_trials
                if effective_trials is not None
                else self._effective_trials
            ),
        )
        dsr = compute_per_period_deflated_sharpe_ratio(
            period_returns,
            effective_trials=eff_trials,
        )

        wins = [p for p in trade_pnls if p > 0.0]
        losses = [p for p in trade_pnls if p < 0.0]
        win_rate = (len(wins) / len(trade_pnls)) if trade_pnls else 0.0
        gross_wins = sum(wins)
        gross_losses = abs(sum(losses))
        if gross_losses > 1e-9:
            profit_factor = gross_wins / gross_losses
        elif gross_wins > 0.0:
            profit_factor = 10.0
        else:
            profit_factor = 0.0

        max_single_loss = min(losses) if losses else min(0.0, net_pnl)
        turnover_ratio = (traded_notional / start_cap) if start_cap > 0.0 else 0.0

        if plan_gate is not None:
            admitted_cnt = sum(1 for d in plan_gate.decisions.values() if d.allowed)
            denied_cnt = sum(1 for d in plan_gate.decisions.values() if not d.allowed)
        else:
            admitted_cnt = filled_orders
            denied_cnt = denied_orders

        return IsomorphicBacktestMetrics(
            symbol=plan_item.symbol,
            archetype_id=plan_item.strategy_config.archetype_id.value,
            bar_timeframe=plan_item.strategy_config.bar_timeframe,
            starting_capital_usd=start_cap,
            ending_capital_usd=end_cap,
            net_pnl_usd=net_pnl,
            gross_pnl_usd=gross_pnl,
            total_return_pct=total_return_pct,
            sharpe_ratio=sharpe,
            per_period_dsr=dsr,
            max_drawdown_pct=max_dd_pct,
            max_single_trade_loss_usd=max_single_loss,
            turnover_ratio=turnover_ratio,
            total_commissions_usd=total_commissions,
            estimated_slippage_cost_usd=est_slippage_cost,
            total_orders=total_orders,
            filled_orders=filled_orders,
            denied_orders=denied_orders,
            total_positions=len(positions),
            win_rate=win_rate,
            profit_factor=profit_factor,
            plan_gate_admitted_orders=admitted_cnt,
            plan_gate_denied_orders=denied_cnt,
            walk_forward=walk_forward,
            effective_trials=eff_trials,
        )

    @staticmethod
    def _build_equity_curve_returns(
        *,
        primary_bars: Sequence[Bar],
        orders: Sequence[Any],
        start_cap: float,
        net_pnl_final: float = 0.0,
        all_bars: Optional[Sequence[Bar]] = None,
        total_commissions: float = 0.0,
        est_slippage_cost: float = 0.0,
    ) -> Tuple[List[float], float]:
        _ = net_pnl_final
        if len(primary_bars) < 2:
            return [], 0.0

        bar_universe = sorted(
            list(all_bars if all_bars is not None else primary_bars),
            key=lambda b: int(b.ts_init),
        )
        total_notional = 0.0
        raw_fills: List[Tuple[int, str, float, float, float]] = []
        for o in orders:
            if o.status != OrderStatus.FILLED:
                continue
            ts = int(getattr(o, "ts_last", 0) or getattr(o, "ts_init", 0))
            inst_key = str(getattr(o, "instrument_id", ""))
            qty = _parse_money_float(
                getattr(o, "filled_qty", None) or getattr(o, "quantity", 0)
            )
            px = _parse_money_float(
                getattr(o, "avg_px", None) or getattr(o, "price", 0)
            )
            side_str = str(getattr(o, "side", "")).upper()
            signed_qty = qty if "BUY" in side_str else -qty
            notional = abs(qty * px)
            total_notional += notional
            raw_fills.append((ts, inst_key, signed_qty, px, notional))
        raw_fills.sort(key=lambda item: item[0])

        total_friction = max(0.0, float(total_commissions)) + max(0.0, float(est_slippage_cost))

        equity_series: List[float] = [start_cap]
        cash = start_cap
        pos_qty_by_inst: Dict[str, float] = {}
        last_close_by_inst: Dict[str, float] = {}
        fill_idx = 0
        bar_idx = 0

        for p_bar in primary_bars:
            bar_ts = int(p_bar.ts_init)
            while bar_idx < len(bar_universe) and int(bar_universe[bar_idx].ts_init) <= bar_ts:
                b_obj = bar_universe[bar_idx]
                last_close_by_inst[str(b_obj.bar_type.instrument_id)] = float(b_obj.close)
                bar_idx += 1
            while fill_idx < len(raw_fills) and raw_fills[fill_idx][0] <= bar_ts:
                _, inst_key, s_qty, fill_px, notional = raw_fills[fill_idx]
                eff_px = (
                    fill_px
                    if fill_px > 0.0
                    else last_close_by_inst.get(inst_key, float(p_bar.close))
                )
                fill_friction = (
                    total_friction * (notional / total_notional)
                    if total_notional > 0.0
                    else 0.0
                )
                cash -= (s_qty * eff_px) + fill_friction
                pos_qty_by_inst[inst_key] = pos_qty_by_inst.get(inst_key, 0.0) + s_qty
                if abs(pos_qty_by_inst[inst_key]) <= 1e-9:
                    pos_qty_by_inst.pop(inst_key, None)
                fill_idx += 1

            mtm_value = 0.0
            for inst_key, held_qty in pos_qty_by_inst.items():
                mark_px = last_close_by_inst.get(inst_key, float(p_bar.close))
                mtm_value += held_qty * mark_px
            eq = cash + mtm_value
            equity_series.append(max(1.0, eq))

        returns: List[float] = []
        peak = equity_series[0]
        max_dd = 0.0
        for i in range(1, len(equity_series)):
            prev_eq = equity_series[i - 1]
            curr_eq = equity_series[i]
            ret = (curr_eq - prev_eq) / prev_eq if prev_eq > 0.0 else 0.0
            returns.append(ret)
            if curr_eq > peak:
                peak = curr_eq
            dd = (peak - curr_eq) / peak if peak > 0.0 else 0.0
            if dd > max_dd:
                max_dd = dd

        return returns, max_dd


@dataclass(frozen=True)
class StressExperimentResult:
    """Result of a single counterfactual stress falsification experiment."""

    experiment_name: str
    passed: bool
    metrics: IsomorphicBacktestMetrics
    threshold_description: str
    observed_value: float
    reason: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "experiment_name": self.experiment_name,
            "passed": self.passed,
            "threshold_description": self.threshold_description,
            "observed_value": round(self.observed_value, 6),
            "reason": self.reason,
            "metrics": self.metrics.to_dict(),
        }


@dataclass(frozen=True)
class StressFalsificationReport:
    """Complete counterfactual falsification report across all 3 mandatory stress tests."""

    symbol: str
    archetype_id: str
    passed: bool
    baseline_metrics: IsomorphicBacktestMetrics
    double_friction_result: StressExperimentResult
    signal_delay_result: StressExperimentResult
    tail_gap_shock_result: StressExperimentResult
    failure_reasons: Tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "archetype_id": self.archetype_id,
            "passed": self.passed,
            "baseline_metrics": self.baseline_metrics.to_dict(),
            "double_friction_result": self.double_friction_result.to_dict(),
            "signal_delay_result": self.signal_delay_result.to_dict(),
            "tail_gap_shock_result": self.tail_gap_shock_result.to_dict(),
            "failure_reasons": list(self.failure_reasons),
        }


class StressFalsifier:
    """Counterfactual stress falsifier executing 3 mandatory experiments in `BacktestEngine`."""

    def __init__(
        self,
        runner: IsomorphicBacktestRunner,
        friction_multiplier: float = 2.0,
        tail_quantile: Decimal | float | str = Decimal("0.95"),
        tail_shock_multiplier: float = 1.5,
    ) -> None:
        self._runner = runner
        self._friction_multiplier = max(1.5, float(friction_multiplier))
        self._tail_quantile = float(tail_quantile)
        self._tail_shock_multiplier = max(1.0, float(tail_shock_multiplier))

    def run_falsification(
        self,
        plan_item: PlanItemContract,
        *,
        bars: Optional[Sequence[Bar]] = None,
        equities: Optional[Sequence[Equity]] = None,
        use_limit_orders: bool = True,
        submit_protective_stop: bool = True,
        effective_trials: Optional[int] = None,
    ) -> StressFalsificationReport:
        """Runs baseline + 3 counterfactual stress experiments in `BacktestEngine`."""
        if bars is None:
            loaded_equities, loaded_bars, _, _ = self._runner.load_historical_data(plan_item)
            bar_list = sorted(loaded_bars, key=lambda b: b.ts_init)
            equity_list = list(loaded_equities)
        else:
            bar_list = sorted(bars, key=lambda b: b.ts_init)
            equity_list = list(equities) if equities is not None else None

        resolved_date = _resolve_trading_date_from_bars(bar_list)
        baseline = self._runner.run_backtest(
            plan_item,
            bars=bar_list,
            equities=equity_list,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            trading_date=resolved_date,
            effective_trials=effective_trials,
        )
        baseline_first_entry = getattr(self._runner, "_last_first_entry_fill", None)

        # 1. Experiment 1: 2x Friction & Slippage Stress Test across entire horizon
        friction_fill_model: FillModel = OneTickSlippageFillModel(
            prob_fill_on_limit=1.0,
            prob_slippage=1.0,
            random_seed=42,
        )
        friction_metrics = self._runner.run_backtest(
            plan_item,
            bars=bar_list,
            equities=equity_list,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            fee_multiplier=self._friction_multiplier,
            slippage_multiplier=self._friction_multiplier,
            fill_model=friction_fill_model,
            trading_date=resolved_date,
            effective_trials=effective_trials,
        )
        friction_passed = (
            friction_metrics.filled_orders > 0 and friction_metrics.net_pnl_usd > 0.0
        )
        friction_reason = (
            f"2x friction net_pnl_usd={friction_metrics.net_pnl_usd:.2f} > 0.00"
            if friction_passed
            else (
                f"FAILED 2x friction stress: net_pnl_usd={friction_metrics.net_pnl_usd:.2f} "
                f"(filled_orders={friction_metrics.filled_orders}) <= 0.00"
            )
        )
        double_friction_result = StressExperimentResult(
            experiment_name="double_friction_stress",
            passed=friction_passed,
            metrics=friction_metrics,
            threshold_description="net_pnl_usd > 0.0 under 2x fees + 2x slippage + OneTickSlippageFillModel",
            observed_value=friction_metrics.net_pnl_usd,
            reason=friction_reason,
        )

        # 2. Experiment 2: +1 Bar Signal Execution Latency Stress Test (signal on bar_t -> order submitted on bar_{t+1} -> filled on bar_{t+2})
        delay_metrics = self._runner.run_backtest(
            plan_item,
            bars=bar_list,
            equities=equity_list,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            execution_delay_bars=1,
            trading_date=resolved_date,
            effective_trials=effective_trials,
        )
        max_budget = float(plan_item.risk_envelope.max_risk_budget_usd)
        delay_passed = (
            delay_metrics.filled_orders > 0
            and delay_metrics.net_pnl_usd >= -max_budget
            and abs(min(0.0, delay_metrics.max_single_trade_loss_usd)) <= max_budget
        )
        delay_reason = (
            f"+1 bar delay net_pnl_usd={delay_metrics.net_pnl_usd:.2f} within budget"
            if delay_passed
            else (
                f"FAILED +1 bar signal delay stress: net_pnl_usd={delay_metrics.net_pnl_usd:.2f}, "
                f"max_single_trade_loss_usd={delay_metrics.max_single_trade_loss_usd:.2f}"
            )
        )
        signal_delay_result = StressExperimentResult(
            experiment_name="plus_one_bar_delay_stress",
            passed=delay_passed,
            metrics=delay_metrics,
            threshold_description=(
                f"filled_orders > 0 and net_pnl_usd >= {-max_budget:.2f} and "
                f"|max_single_trade_loss_usd| <= {max_budget:.2f}"
            ),
            observed_value=delay_metrics.net_pnl_usd,
            reason=delay_reason,
        )

        # 3. Experiment 3: Empirical Tail Adverse Opening Gap Shock Test
        shocked_bars = self._inject_tail_adverse_gap_shock(
            bar_list,
            plan_item=plan_item,
            baseline_first_entry=baseline_first_entry,
        )
        gap_metrics = self._runner.run_backtest(
            plan_item,
            bars=shocked_bars,
            equities=equity_list,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            trading_date=resolved_date,
            effective_trials=effective_trials,
        )
        observed_worst_loss = max(
            abs(min(0.0, gap_metrics.max_single_trade_loss_usd)),
            abs(min(0.0, gap_metrics.net_pnl_usd)),
        )
        gap_passed = (
            gap_metrics.filled_orders > 0
            and observed_worst_loss <= max_budget
        )
        gap_reason = (
            f"Tail gap shock worst loss ${observed_worst_loss:.2f} <= max_risk_budget_usd ${max_budget:.2f}"
            if gap_passed
            else (
                f"FAILED tail gap shock stress: worst loss ${observed_worst_loss:.2f} "
                f"exceeds max_risk_budget_usd ${max_budget:.2f}"
            )
        )
        tail_gap_shock_result = StressExperimentResult(
            experiment_name="tail_gap_shock_stress",
            passed=gap_passed,
            metrics=gap_metrics,
            threshold_description=f"max_loss_usd <= max_risk_budget_usd ({max_budget:.2f} USD)",
            observed_value=observed_worst_loss,
            reason=gap_reason,
        )

        failures: List[str] = []
        for res in (double_friction_result, signal_delay_result, tail_gap_shock_result):
            if not res.passed:
                failures.append(res.reason)

        return StressFalsificationReport(
            symbol=plan_item.symbol,
            archetype_id=plan_item.strategy_config.archetype_id.value,
            passed=(len(failures) == 0),
            baseline_metrics=baseline,
            double_friction_result=double_friction_result,
            signal_delay_result=signal_delay_result,
            tail_gap_shock_result=tail_gap_shock_result,
            failure_reasons=tuple(failures),
        )

    def _inject_tail_adverse_gap_shock(
        self,
        bars: Sequence[Bar],
        *,
        plan_item: PlanItemContract,
        baseline_first_entry: Optional[Tuple[int, float]] = None,
    ) -> List[Bar]:
        primary_inst = plan_item.strategy_config.instrument_id
        primary_bars = [
            b for b in bars if str(b.bar_type.instrument_id) == primary_inst
        ]
        other_bars = [
            b for b in bars if str(b.bar_type.instrument_id) != primary_inst
        ]
        if len(primary_bars) < 5:
            return list(bars)

        abs_ranges: List[float] = []
        for idx in range(1, len(primary_bars)):
            prev_c = float(primary_bars[idx - 1].close)
            curr_h = float(primary_bars[idx].high)
            curr_l = float(primary_bars[idx].low)
            tr = max(curr_h - curr_l, abs(curr_h - prev_c), abs(curr_l - prev_c))
            abs_ranges.append(tr)

        sorted_ranges = sorted(abs_ranges)
        q_idx = min(
            len(sorted_ranges) - 1,
            max(0, int(round(self._tail_quantile * float(len(sorted_ranges) - 1)))),
        )
        empirical_tail_range = sorted_ranges[q_idx] if sorted_ranges else 0.0
        shock_distance = empirical_tail_range * self._tail_shock_multiplier

        baseline_entry_px = 0.0
        shock_idx: Optional[int] = None
        if baseline_first_entry is not None:
            entry_fill_ts_ns, baseline_entry_px = baseline_first_entry
            for idx, b in enumerate(primary_bars):
                if int(b.ts_init) > int(entry_fill_ts_ns):
                    shock_idx = min(len(primary_bars) - 1, idx + 1)
                    break
            if shock_idx is None:
                shock_idx = len(primary_bars) - 1

        if shock_idx is None:
            shock_idx = min(
                len(primary_bars) - 1,
                plan_item.strategy_config.lookback_window + 2,
            )

        ref_bar = primary_bars[shock_idx]
        prev_bar_close = (
            float(primary_bars[shock_idx - 1].close)
            if shock_idx > 0
            else float(ref_bar.close)
        )
        anchor_px = (
            baseline_entry_px
            if (baseline_first_entry is not None and baseline_entry_px > 0.0)
            else prev_bar_close
        )

        if plan_item.risk_envelope.side == PlanSide.LONG:
            shocked_close = max(0.01, anchor_px - shock_distance)
        else:
            shocked_close = anchor_px + shock_distance

        shocked_low = shocked_close
        shocked_high = shocked_close

        shocked_primary = list(primary_bars)
        shocked_primary[shock_idx] = _clone_bar_with_prices(
            ref_bar,
            open_px=shocked_close,
            high_px=shocked_high,
            low_px=shocked_low,
            close_px=shocked_close,
        )

        combined = shocked_primary + other_bars
        combined.sort(key=lambda b: b.ts_init)
        return combined


StressFalsificationSuite = StressFalsifier

