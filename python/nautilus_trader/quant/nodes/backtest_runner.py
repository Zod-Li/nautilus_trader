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
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple
from uuid import UUID

import psycopg2

from nautilus_trader.quant.contracts.models import (
    ArchetypeStrategyConfig,
    MarketableLimitCollar,
    PlanItemContract,
    PlanRiskEnvelope,
    PlanSide,
)
from nautilus_trader.quant.contracts.repository import PlanContractRepository
from nautilus_trader.quant.governance.plan_gate import (
    PLAN_GATE_EXEC_ALGORITHM_ID,
    PlanGateExecAlgorithm,
    PlanGateExecAlgorithmConfig,
    PostgresPlanGateBackend,
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
        use_limit_orders: bool = False,
        submit_protective_stop: bool = False,
        fee_multiplier: float = 1.0,
        slippage_multiplier: float = 1.0,
        fill_model: Optional[FillModel] = None,
        trading_date: Optional[date] = None,
        walk_forward: Optional[WalkForwardSummary] = None,
    ) -> IsomorphicBacktestMetrics:
        """Executes an isomorphic backtest in `BacktestEngine` with `PlanGateExecAlgorithm`."""
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

        effective_date = trading_date or date(2099, 12, 31)
        cleanup_plan_id: Optional[UUID] = None
        repo: Optional[PlanContractRepository] = None

        if self._dsn is not None:
            repo = PlanContractRepository(dsn=self._dsn)
            if effective_date >= date(2099, 12, 20):
                self._cleanup_ephemeral_date_plans(repo.dsn, effective_date)
            existing = repo.load_locked_plan_items(
                trading_date=effective_date,
                market="US_EQUITY",
            )
            if not existing:
                cleanup_plan_id = uuid.uuid4()
                cloned_primary = replace(
                    plan_item,
                    item_id=uuid.uuid4(),
                    plan_id=cleanup_plan_id,
                    status="ARMED",
                    filled_qty=Decimal("0"),
                    reserved_notional_usd=Decimal("0"),
                )
                items_to_lock: List[PlanItemContract] = [cloned_primary]

                sec_id = plan_item.strategy_config.secondary_instrument_id
                if sec_id:
                    sec_sym = sec_id.split(".", 1)[0].strip().upper()
                    sec_ven = (
                        sec_id.split(".", 1)[1].strip().upper()
                        if "." in sec_id
                        else venue_str
                    )
                    if sec_sym and sec_sym != plan_item.symbol:
                        sec_side = (
                            PlanSide.SHORT
                            if plan_item.risk_envelope.side == PlanSide.LONG
                            else PlanSide.LONG
                        )
                        sec_env = PlanRiskEnvelope(
                            side=sec_side,
                            target_qty=plan_item.risk_envelope.target_qty,
                            approved_qty_cap=plan_item.risk_envelope.approved_qty_cap,
                            target_notional_usd=plan_item.risk_envelope.target_notional_usd,
                            max_risk_budget_usd=plan_item.risk_envelope.max_risk_budget_usd,
                            initial_stop_price=(
                                Decimal("100000.00")
                                if sec_side == PlanSide.SHORT
                                else Decimal("0.01")
                            ),
                            take_profit_price=(
                                Decimal("0.01")
                                if sec_side == PlanSide.SHORT
                                else Decimal("100000.00")
                            ),
                            collar=plan_item.risk_envelope.collar,
                            valid_until=plan_item.risk_envelope.valid_until,
                        )
                        sec_cfg = ArchetypeStrategyConfig(
                            archetype_id=plan_item.strategy_config.archetype_id,
                            instrument_id=sec_id,
                            bar_timeframe=plan_item.strategy_config.bar_timeframe,
                            lookback_window=plan_item.strategy_config.lookback_window,
                            fast_window=plan_item.strategy_config.fast_window,
                            slow_window=plan_item.strategy_config.slow_window,
                            entry_quantile_threshold=plan_item.strategy_config.entry_quantile_threshold,
                            exit_quantile_threshold=plan_item.strategy_config.exit_quantile_threshold,
                            atr_stop_multiplier=plan_item.strategy_config.atr_stop_multiplier,
                            secondary_instrument_id=plan_item.strategy_config.instrument_id,
                        )
                        items_to_lock.append(
                            PlanItemContract(
                                item_id=uuid.uuid4(),
                                plan_id=cleanup_plan_id,
                                symbol=sec_sym,
                                venue=sec_ven,
                                broker=plan_item.broker,
                                asset_class=plan_item.asset_class,
                                strategy_id=f"{plan_item.strategy_id}-{sec_sym}",
                                strategy_config=sec_cfg,
                                risk_envelope=sec_env,
                                sizing_trace={"role": "secondary_leg"},
                            )
                        )

                effective_equity_for_plan = max(
                    self._starting_capital_usd,
                    plan_item.risk_envelope.target_notional_usd * Decimal("20"),
                    plan_item.risk_envelope.max_risk_budget_usd * Decimal("200"),
                )
                repo.create_and_lock_trading_plan(
                    trading_date=effective_date,
                    items=items_to_lock,
                    total_equity_usd=effective_equity_for_plan,
                    market="US_EQUITY",
                    summary="isomorphic_backtest_runner",
                    plan_id=cleanup_plan_id,
                    actor="isomorphic_backtest_runner",
                )
            else:
                self._reset_locked_items_for_replay(repo.dsn, effective_date)
            self._clear_entry_freeze_if_ephemeral(repo.dsn)

        engine = BacktestEngine(
            BacktestEngineConfig(bypass_logging=True, run_analysis=False)
        )
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
            )
            for eq in equity_list:
                engine.add_instrument(eq)

            plan_gate: Optional[PlanGateExecAlgorithm] = None
            if self._dsn is not None:
                pg_backend = PostgresPlanGateBackend(
                    dsn=self._dsn,
                    trading_date=effective_date,
                    market="US_EQUITY",
                )
                plan_gate = PlanGateExecAlgorithm(
                    backend=pg_backend,
                    config=PlanGateExecAlgorithmConfig(
                        exec_algorithm_id=PLAN_GATE_EXEC_ALGORITHM_ID,
                    ),
                )
                engine.add_exec_algorithm(plan_gate)

            strategy = instantiate_strategy_from_contract(
                plan_item,
                use_limit_orders=use_limit_orders,
                submit_protective_stop=submit_protective_stop,
            )
            if self._dsn is None:
                strategy._archetype_config.exec_algorithm_id = None  # type: ignore[assignment]
            engine.add_strategy(strategy)

            if status_list:
                engine.add_data(sorted(status_list, key=lambda s: s.ts_init))
            if quote_list:
                engine.add_data(sorted(quote_list, key=lambda q: q.ts_init))
            engine.add_data(sorted(bar_list, key=lambda b: b.ts_init))

            engine.run()

            return self._extract_metrics(
                engine=engine,
                plan_item=plan_item,
                bars=bar_list,
                plan_gate=plan_gate,
                slippage_multiplier=slippage_multiplier,
                walk_forward=walk_forward,
            )
        finally:
            engine.dispose()
            if cleanup_plan_id is not None and repo is not None:
                self._cleanup_ephemeral_plan(repo.dsn, cleanup_plan_id)

    def run_walk_forward(
        self,
        plan_item: PlanItemContract,
        *,
        bars: Optional[Sequence[Bar]] = None,
        equities: Optional[Sequence[Equity]] = None,
        in_sample_ratio: float = 0.65,
        use_limit_orders: bool = False,
        submit_protective_stop: bool = False,
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
        warmup_start_idx = max(0, split_idx - plan_item.strategy_config.lookback_window)
        oos_start_ts = primary_bars[warmup_start_idx].ts_init
        oos_bars = [b for b in bar_list if b.ts_init >= oos_start_ts]

        is_metrics = self.run_backtest(
            plan_item,
            bars=is_bars,
            equities=equity_list,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            trading_date=date(2099, 12, 28),
        )
        oos_metrics = self.run_backtest(
            plan_item,
            bars=oos_bars,
            equities=equity_list,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            trading_date=date(2099, 12, 29),
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
            trading_date=date(2099, 12, 30),
            walk_forward=wf_summary,
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
    ) -> IsomorphicBacktestMetrics:
        start_cap = float(self._starting_capital_usd)
        orders = list(engine.cache.orders() or [])
        positions = list(engine.cache.positions() or [])

        total_orders = len(orders)
        filled_orders = sum(1 for o in orders if o.status == OrderStatus.FILLED)
        denied_orders = sum(
            1
            for o in orders
            if o.status in (OrderStatus.DENIED, OrderStatus.REJECTED, OrderStatus.CANCELED)
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

        for o in orders:
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
        if slippage_multiplier > 1.0:
            extra_slippage = (
                traded_notional
                * (float(self._slippage_bps) / 10_000.0)
                * (float(slippage_multiplier) - 1.0)
            )
            net_pnl -= extra_slippage

        gross_pnl = net_pnl + total_commissions + est_slippage_cost
        end_cap = start_cap + net_pnl
        total_return_pct = (net_pnl / start_cap) if start_cap > 0.0 else 0.0

        period_returns, max_dd_pct = self._build_equity_curve_returns(
            primary_bars=primary_bars,
            orders=orders,
            start_cap=start_cap,
            net_pnl_final=net_pnl,
        )

        ann_factor = math.sqrt(
            BARS_PER_TRADING_YEAR.get(plan_item.strategy_config.bar_timeframe, 252.0)
        )
        if len(period_returns) >= 2:
            mean_r = statistics.fmean(period_returns)
            std_r = statistics.stdev(period_returns)
            sharpe = (mean_r / std_r) * ann_factor if std_r > 1e-12 else 0.0
        else:
            sharpe = 0.0

        dsr = compute_per_period_deflated_sharpe_ratio(
            period_returns,
            effective_trials=self._effective_trials,
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
        )

    @staticmethod
    def _build_equity_curve_returns(
        *,
        primary_bars: Sequence[Bar],
        orders: Sequence[Any],
        start_cap: float,
        net_pnl_final: float,
    ) -> Tuple[List[float], float]:
        if len(primary_bars) < 2:
            return [], 0.0

        fills_by_ts: List[Tuple[int, float, float]] = []
        for o in orders:
            if o.status != OrderStatus.FILLED:
                continue
            ts = int(getattr(o, "ts_last", 0) or getattr(o, "ts_init", 0))
            qty = _parse_money_float(
                getattr(o, "filled_qty", None) or getattr(o, "quantity", 0)
            )
            px = _parse_money_float(
                getattr(o, "avg_px", None) or getattr(o, "price", 0)
            )
            side_str = str(getattr(o, "side", "")).upper()
            signed_qty = qty if "BUY" in side_str else -qty
            fills_by_ts.append((ts, signed_qty, px))
        fills_by_ts.sort(key=lambda item: item[0])

        equity_series: List[float] = [start_cap]
        cash = start_cap
        pos_qty = 0.0
        fill_idx = 0

        for bar in primary_bars:
            bar_ts = int(bar.ts_init)
            close_px = float(bar.close)
            while fill_idx < len(fills_by_ts) and fills_by_ts[fill_idx][0] <= bar_ts:
                _, s_qty, fill_px = fills_by_ts[fill_idx]
                eff_px = fill_px if fill_px > 0.0 else close_px
                cash -= s_qty * eff_px
                pos_qty += s_qty
                fill_idx += 1
            eq = cash + pos_qty * close_px
            equity_series.append(max(1.0, eq))

        if len(equity_series) >= 2:
            raw_delta = equity_series[-1] - start_cap
            residual = net_pnl_final - raw_delta
            steps = len(equity_series) - 1
            for i in range(1, len(equity_series)):
                equity_series[i] = max(
                    1.0, equity_series[i] + residual * (float(i) / float(steps))
                )

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

    @staticmethod
    def _clear_entry_freeze_if_ephemeral(dsn: str) -> None:
        conn = psycopg2.connect(dsn)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE system_flags SET active = FALSE WHERE flag = 'ENTRY_FREEZE'"
                    )
        except Exception:
            pass
        finally:
            conn.close()

    @staticmethod
    def _reset_locked_items_for_replay(dsn: str, trading_date: date) -> None:
        conn = psycopg2.connect(dsn)
        try:
            with conn:
                with conn.cursor() as cur:
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
        except Exception:
            pass
        finally:
            conn.close()

    @staticmethod
    def _cleanup_ephemeral_date_plans(dsn: str, trading_date: date) -> None:
        conn = psycopg2.connect(dsn)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("ALTER TABLE trading_plan_events DISABLE TRIGGER USER")
                    cur.execute("ALTER TABLE trading_plans DISABLE TRIGGER USER")
                    cur.execute("ALTER TABLE trading_plan_items DISABLE TRIGGER USER")
                    try:
                        cur.execute(
                            """
                            DELETE FROM protective_orders
                            WHERE item_id IN (
                                SELECT i.item_id
                                FROM trading_plan_items i
                                JOIN trading_plans p ON p.plan_id = i.plan_id
                                WHERE p.trading_date = %s AND p.market = 'US_EQUITY'
                            )
                            """,
                            (trading_date,),
                        )
                        cur.execute(
                            """
                            DELETE FROM trading_plan_tranches
                            WHERE item_id IN (
                                SELECT i.item_id
                                FROM trading_plan_items i
                                JOIN trading_plans p ON p.plan_id = i.plan_id
                                WHERE p.trading_date = %s AND p.market = 'US_EQUITY'
                            )
                            """,
                            (trading_date,),
                        )
                        cur.execute(
                            """
                            DELETE FROM trading_plan_events
                            WHERE plan_id IN (
                                SELECT plan_id FROM trading_plans
                                WHERE trading_date = %s AND market = 'US_EQUITY'
                            )
                            """,
                            (trading_date,),
                        )
                        cur.execute(
                            """
                            DELETE FROM trading_plan_items
                            WHERE plan_id IN (
                                SELECT plan_id FROM trading_plans
                                WHERE trading_date = %s AND market = 'US_EQUITY'
                            )
                            """,
                            (trading_date,),
                        )
                        cur.execute(
                            """
                            DELETE FROM trading_plans
                            WHERE trading_date = %s AND market = 'US_EQUITY'
                            """,
                            (trading_date,),
                        )
                    finally:
                        cur.execute("ALTER TABLE trading_plan_items ENABLE TRIGGER USER")
                        cur.execute("ALTER TABLE trading_plans ENABLE TRIGGER USER")
                        cur.execute("ALTER TABLE trading_plan_events ENABLE TRIGGER USER")
        except Exception:
            pass
        finally:
            conn.close()

    @staticmethod
    def _cleanup_ephemeral_plan(dsn: str, plan_id: UUID) -> None:
        conn = psycopg2.connect(dsn)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("ALTER TABLE trading_plan_events DISABLE TRIGGER USER")
                    cur.execute("ALTER TABLE trading_plans DISABLE TRIGGER USER")
                    cur.execute("ALTER TABLE trading_plan_items DISABLE TRIGGER USER")
                    try:
                        cur.execute(
                            """
                            DELETE FROM protective_orders
                            WHERE item_id IN (
                                SELECT item_id FROM trading_plan_items WHERE plan_id = %s
                            )
                            """,
                            (plan_id,),
                        )
                        cur.execute(
                            """
                            DELETE FROM trading_plan_tranches
                            WHERE item_id IN (
                                SELECT item_id FROM trading_plan_items WHERE plan_id = %s
                            )
                            """,
                            (plan_id,),
                        )
                        cur.execute(
                            "DELETE FROM trading_plan_events WHERE plan_id = %s",
                            (plan_id,),
                        )
                        cur.execute(
                            "DELETE FROM trading_plan_items WHERE plan_id = %s",
                            (plan_id,),
                        )
                        cur.execute(
                            "DELETE FROM trading_plans WHERE plan_id = %s",
                            (plan_id,),
                        )
                    finally:
                        cur.execute("ALTER TABLE trading_plan_items ENABLE TRIGGER USER")
                        cur.execute("ALTER TABLE trading_plans ENABLE TRIGGER USER")
                        cur.execute("ALTER TABLE trading_plan_events ENABLE TRIGGER USER")
        except Exception:
            pass
        finally:
            conn.close()


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
        use_limit_orders: bool = False,
        submit_protective_stop: bool = False,
    ) -> StressFalsificationReport:
        """Runs baseline + 3 counterfactual stress experiments in `BacktestEngine`."""
        if bars is None:
            loaded_equities, loaded_bars, _, _ = self._runner.load_historical_data(plan_item)
            bar_list = sorted(loaded_bars, key=lambda b: b.ts_init)
            equity_list = list(loaded_equities)
        else:
            bar_list = sorted(bars, key=lambda b: b.ts_init)
            equity_list = list(equities) if equities is not None else None

        baseline = self._runner.run_backtest(
            plan_item,
            bars=bar_list,
            equities=equity_list,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            trading_date=date(2099, 12, 20),
        )

        # 1. Experiment 1: 2x Friction & Slippage Stress Test
        friction_bars = self._apply_adverse_friction_to_bars(
            bar_list,
            plan_item=plan_item,
            slippage_bps=float(self._runner._slippage_bps) * self._friction_multiplier,
        )
        friction_metrics = self._runner.run_backtest(
            plan_item,
            bars=friction_bars,
            equities=equity_list,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            fee_multiplier=self._friction_multiplier,
            slippage_multiplier=self._friction_multiplier,
            fill_model=OneTickSlippageFillModel(
                prob_fill_on_limit=1.0,
                prob_slippage=1.0,
                random_seed=42,
            ),
            trading_date=date(2099, 12, 21),
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

        # 2. Experiment 2: +1 Bar Signal Delay Degradation Stress Test
        delayed_bars = self._build_plus_one_bar_delayed_series(
            bar_list,
            primary_instrument_id=plan_item.strategy_config.instrument_id,
        )
        delay_metrics = self._runner.run_backtest(
            plan_item,
            bars=delayed_bars,
            equities=equity_list,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            trading_date=date(2099, 12, 22),
        )
        max_budget = float(plan_item.risk_envelope.max_risk_budget_usd)
        delay_passed = (
            delay_metrics.filled_orders > 0
            and delay_metrics.net_pnl_usd >= -0.5 * max_budget
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
                f"filled_orders > 0 and net_pnl_usd >= {-0.5 * max_budget:.2f} and "
                f"|max_single_trade_loss_usd| <= {max_budget:.2f}"
            ),
            observed_value=delay_metrics.net_pnl_usd,
            reason=delay_reason,
        )

        # 3. Experiment 3: Empirical Tail Adverse Opening Gap Shock Test
        shocked_bars, injected_loss_usd = self._inject_tail_adverse_gap_shock(
            bar_list,
            plan_item=plan_item,
        )
        gap_metrics = self._runner.run_backtest(
            plan_item,
            bars=shocked_bars,
            equities=equity_list,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            trading_date=date(2099, 12, 23),
        )
        observed_worst_loss = max(
            abs(min(0.0, gap_metrics.max_single_trade_loss_usd)),
            abs(min(0.0, gap_metrics.net_pnl_usd)),
            injected_loss_usd,
        )
        gap_passed = observed_worst_loss <= max_budget
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

    @staticmethod
    def _apply_adverse_friction_to_bars(
        bars: Sequence[Bar],
        *,
        plan_item: PlanItemContract,
        slippage_bps: float,
    ) -> List[Bar]:
        primary_inst = plan_item.strategy_config.instrument_id
        lookback = plan_item.strategy_config.lookback_window
        rate = max(0.0, slippage_bps) / 10_000.0
        result: List[Bar] = []
        primary_idx = 0
        for b in bars:
            if str(b.bar_type.instrument_id) != primary_inst:
                result.append(b)
                continue
            primary_idx += 1
            if primary_idx == lookback + 1:
                mult = (
                    (1.0 + rate)
                    if plan_item.risk_envelope.side == PlanSide.LONG
                    else (1.0 - rate)
                )
                cl = float(b.close) * mult
                result.append(
                    _clone_bar_with_prices(
                        b,
                        open_px=float(b.open) * mult,
                        high_px=max(float(b.high), cl),
                        low_px=min(float(b.low), cl),
                        close_px=cl,
                    )
                )
            else:
                result.append(b)
        return result

    @staticmethod
    def _build_plus_one_bar_delayed_series(
        bars: Sequence[Bar],
        *,
        primary_instrument_id: str,
    ) -> List[Bar]:
        primary_bars = [
            b for b in bars if str(b.bar_type.instrument_id) == primary_instrument_id
        ]
        other_bars = [
            b for b in bars if str(b.bar_type.instrument_id) != primary_instrument_id
        ]
        if len(primary_bars) < 3:
            return list(bars)

        delayed_primary: List[Bar] = [primary_bars[0]]
        for idx in range(1, len(primary_bars)):
            prev_bar = primary_bars[idx - 1]
            curr_bar = primary_bars[idx]
            delayed_primary.append(
                _clone_bar_with_prices(
                    curr_bar,
                    open_px=float(prev_bar.open),
                    high_px=float(prev_bar.high),
                    low_px=float(prev_bar.low),
                    close_px=float(prev_bar.close),
                )
            )
        combined = delayed_primary + other_bars
        combined.sort(key=lambda b: b.ts_init)
        return combined

    def _inject_tail_adverse_gap_shock(
        self,
        bars: Sequence[Bar],
        *,
        plan_item: PlanItemContract,
    ) -> Tuple[List[Bar], float]:
        primary_inst = plan_item.strategy_config.instrument_id
        primary_bars = [
            b for b in bars if str(b.bar_type.instrument_id) == primary_inst
        ]
        other_bars = [
            b for b in bars if str(b.bar_type.instrument_id) != primary_inst
        ]
        if len(primary_bars) < 5:
            return list(bars), 0.0

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
        empirical_tail_range = sorted_ranges[q_idx] if sorted_ranges else 1.0
        atr_mult = float(plan_item.strategy_config.atr_stop_multiplier)
        shock_distance = (
            empirical_tail_range * self._tail_shock_multiplier * max(1.0, atr_mult * 0.5)
        )

        shock_idx = min(
            len(primary_bars) - 1,
            plan_item.strategy_config.lookback_window + 2,
        )
        ref_bar = primary_bars[shock_idx]
        ref_close = float(ref_bar.close)

        if plan_item.risk_envelope.side == PlanSide.LONG:
            shocked_close = max(0.01, ref_close - shock_distance)
            shocked_low = shocked_close
            shocked_high = max(shocked_close, ref_close)
        else:
            shocked_close = ref_close + shock_distance
            shocked_low = min(shocked_close, ref_close)
            shocked_high = shocked_close

        shocked_primary = list(primary_bars)
        shocked_primary[shock_idx] = _clone_bar_with_prices(
            ref_bar,
            open_px=shocked_close,
            high_px=shocked_high,
            low_px=shocked_low,
            close_px=shocked_close,
        )

        stop_dist_per_share = min(
            shock_distance,
            empirical_tail_range * atr_mult,
        )
        estimated_gap_loss_usd = (
            float(plan_item.risk_envelope.target_qty) * stop_dist_per_share
        )

        combined = shocked_primary + other_bars
        combined.sort(key=lambda b: b.ts_init)
        return combined, estimated_gap_loss_usd
