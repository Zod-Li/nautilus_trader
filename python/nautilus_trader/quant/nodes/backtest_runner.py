"""Isomorphic Backtest Runner, Walk-Forward Calibrator, and Counterfactual Stress Falsifier.

Implements SPEC-0007 isomorphic backtest verification:
- Runs the 1:1 isomorphic combination `BacktestEngine` + `Strategy(StrategyConfig)` +
  Rust `PreTradeRiskGate` (`PostgresPlanGateBackend.as_pre_trade_risk_gate()`) +
  `CollaredTwapExecAlgorithm` + `IbkrProFixedUsEquityFeeModel` over dual-stream
  (`15m` + `1m`) `DataInfraCatalog` historical bars/quotes.
- Computes non-annualized Per-Period Deflated Sharpe Ratio (DSR) per Bailey & Lopez de Prado,
  Walk-Forward In-Sample / Out-of-Sample statistics, and Pareto dominance frontier.
- Executes 3 counterfactual stress falsification experiments in `BacktestEngine`:
  1. `2x` Friction & Slippage Stress Test (`double_friction_stress`)
  2. `+1 Bar` Execution Signal Delay Stress Test (`plus_one_bar_delay_stress`)
  3. Empirical Tail Adverse Opening Gap Shock Test (`tail_gap_shock_stress`)
"""

from __future__ import annotations

import math
import os
import statistics
import uuid
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote

from evolution_engine.config_center.envelope import load_envelope
from evolution_engine.config_center.schema import SystemConfig
from nautilus_trader.quant.contracts.models import (
    ArchetypeStrategyConfig,
    ExecutionSpec,
    PlanItemContract,
    PlanRiskEnvelope,
    PlanSide,
    rule_612_tick_size,
)
from nautilus_trader.quant.strategies.empirical import rolling_atr
from nautilus_trader.quant.execution_algorithms.collared_twap import (
    COLLARED_TWAP_EXEC_ALGORITHM_ID,
    CollaredTwapExecAlgorithm,
    CollaredTwapExecAlgorithmConfig,
)
from nautilus_trader.quant.execution_algorithms.friction import (
    IbkrProFeeSchedule,
    IbkrProFixedUsEquityFeeModel,
    MarketImpactSpec,
    compute_half_spread_and_sqrt_impact,
)
from nautilus_trader.quant.governance.plan_gate_backend import (
    PostgresPlanGateBackend,
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
from nautilus_trader.common import LoggerConfig, LogLevel
from nautilus_trader.execution import (
    BestPriceFillModel,
    FillModel,
    OneTickSlippageFillModel,
    StaticLatencyModel,
)
from nautilus_trader.risk import RiskEngineConfig
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


def _bar_step_ns(bar: Bar) -> int:
    """Returns bar duration in nanoseconds so `1m` bars sort ahead of `15m` bars at identical `ts_init`."""
    bar_type = getattr(bar, "bar_type", None)
    spec = getattr(bar_type, "spec", None)
    step = int(getattr(spec, "step", 1) or 1)
    agg = str(getattr(spec, "aggregation", "") or "").upper()
    if "SECOND" in agg:
        return step * 1_000_000_000
    if "MINUTE" in agg:
        return step * 60_000_000_000
    if "HOUR" in agg:
        return step * 3_600_000_000_000
    if "DAY" in agg:
        return step * 86_400_000_000_000
    raw = str(bar_type or "").upper()
    if "1-MINUTE" in raw:
        return 60_000_000_000
    if "5-MINUTE" in raw:
        return 300_000_000_000
    if "15-MINUTE" in raw:
        return 900_000_000_000
    if "1-HOUR" in raw:
        return 3_600_000_000_000
    if "1-DAY" in raw:
        return 86_400_000_000_000
    return 900_000_000_000


def _timeframe_step_ns(bar_timeframe: str) -> int:
    tf = (bar_timeframe or "").strip().lower()
    if tf.endswith("s") and tf[:-1].isdigit():
        return int(tf[:-1]) * 1_000_000_000
    if tf.endswith("m") and tf[:-1].isdigit():
        return int(tf[:-1]) * 60_000_000_000
    if tf.endswith("h") and tf[:-1].isdigit():
        return int(tf[:-1]) * 3_600_000_000_000
    if tf.endswith("d") and tf[:-1].isdigit():
        return int(tf[:-1]) * 86_400_000_000_000
    return 900_000_000_000


def _filter_primary_bars(
    bars: Sequence[Bar],
    plan_item: PlanItemContract,
) -> List[Bar]:
    primary_inst = plan_item.strategy_config.instrument_id
    inst_bars = [b for b in bars if str(b.bar_type.instrument_id) == primary_inst]
    target_step = _timeframe_step_ns(plan_item.strategy_config.bar_timeframe)
    matched = [b for b in inst_bars if _bar_step_ns(b) == target_step]
    return matched if matched else inst_bars


def _sort_bars_deterministic(bars: Sequence[Bar]) -> List[Bar]:
    return sorted(
        list(bars),
        key=lambda b: (int(b.ts_init), _bar_step_ns(b), str(b.bar_type)),
    )


def _create_default_equity(
    symbol: str,
    venue_str: str = "ALPACA",
    ref_price: Optional[float] = None,
) -> Equity:
    inst_id = InstrumentId.from_str(f"{symbol}.{venue_str}")
    ref_dec = (
        Decimal(str(ref_price))
        if ref_price is not None and float(ref_price) > 0.0
        else Decimal("100")
    )
    tick = rule_612_tick_size(ref_dec)
    tick_str = f"{tick:.4f}".rstrip("0").rstrip(".") if tick < Decimal("0.01") else "0.01"
    if tick_str == "0.0001":
        prec = 4
    else:
        tick_str = "0.01"
        prec = 2
    return Equity(
        instrument_id=inst_id,
        raw_symbol=inst_id.symbol,
        currency=Currency.from_str("USD"),
        price_precision=prec,
        price_increment=Price.from_str(tick_str),
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


def _extract_dual_track_extremes(
    bars: Sequence[Bar],
    *,
    raw_bars: Optional[Sequence[Bar]] = None,
    raw_low_by_ts_ns: Optional[Mapping[int, float]] = None,
    raw_high_by_ts_ns: Optional[Mapping[int, float]] = None,
) -> Tuple[Dict[int, Tuple[float, float]], Dict[int, Tuple[float, float]], bool]:
    """Extracts `(cleaned_by_ts_ns, raw_by_ts_ns, has_divergence)` before bar cloning."""
    cleaned_by_ts_ns: Dict[int, Tuple[float, float]] = {}
    raw_by_ts_ns: Dict[int, Tuple[float, float]] = {}
    raw_bar_map: Dict[int, Tuple[float, float]] = {}
    if raw_bars:
        for rb in raw_bars:
            rb_ts = max(
                int(getattr(rb, "ts_init", 0) or 0),
                int(getattr(rb, "ts_event", 0) or 0),
            )
            raw_bar_map[rb_ts] = (float(rb.high), float(rb.low))

    has_divergence = False
    for b in bars:
        b_ts = max(
            int(getattr(b, "ts_init", 0) or 0),
            int(getattr(b, "ts_event", 0) or 0),
        )
        attr_ch = getattr(b, "cleaned_high", None)
        attr_cl = getattr(b, "cleaned_low", None)
        c_high = float(attr_ch) if attr_ch is not None else float(b.high)
        c_low = float(attr_cl) if attr_cl is not None else float(b.low)
        cleaned_by_ts_ns[b_ts] = (c_high, c_low)

        r_high = c_high
        r_low = c_low
        if b_ts in raw_bar_map:
            r_high = max(r_high, raw_bar_map[b_ts][0])
            r_low = min(r_low, raw_bar_map[b_ts][1])
        attr_rh = getattr(b, "raw_high", None)
        attr_rl = getattr(b, "raw_low", None)
        if attr_rh is not None:
            r_high = max(r_high, float(attr_rh))
        if attr_rl is not None:
            r_low = min(r_low, float(attr_rl))
        if raw_high_by_ts_ns and b_ts in raw_high_by_ts_ns:
            r_high = max(r_high, float(raw_high_by_ts_ns[b_ts]))
        if raw_low_by_ts_ns and b_ts in raw_low_by_ts_ns:
            r_low = min(r_low, float(raw_low_by_ts_ns[b_ts]))

        raw_by_ts_ns[b_ts] = (r_high, r_low)
        if abs(r_high - c_high) > 1e-9 or abs(r_low - c_low) > 1e-9:
            has_divergence = True

    return cleaned_by_ts_ns, raw_by_ts_ns, has_divergence


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
    return _sort_bars_deterministic(normalized)


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
    return [cloned_primary]


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
    """Runs 1:1 isomorphic backtests in `Nautilus BacktestEngine` with Rust `PreTradeRiskGate` and `CollaredTwapExecAlgorithm`."""

    def __init__(
        self,
        dsn: Optional[str] = None,
        datainfra_config: Optional[DataInfraConfig] = None,
        starting_capital_usd: Decimal | float | str = Decimal("100000.00"),
        maker_fee_rate: Optional[Decimal | float | str] = None,
        taker_fee_rate: Optional[Decimal | float | str] = None,
        slippage_bps: Optional[Decimal | float | str] = None,
        effective_trials: int = 1,
        venue_name: str = "ALPACA",
        fee_schedule: Optional[IbkrProFeeSchedule] = None,
        market_impact_spec: Optional[MarketImpactSpec] = None,
        half_spread_bps: Optional[float] = None,
        impact_coefficient_y: Optional[float] = None,
        system_config: Optional[SystemConfig] = None,
    ) -> None:
        self._dsn = dsn
        self._datainfra_config = datainfra_config
        self._starting_capital_usd = Decimal(str(starting_capital_usd))
        _ = (maker_fee_rate, taker_fee_rate)
        self._fee_schedule = fee_schedule or IbkrProFeeSchedule()
        eff_half_bps = (
            float(half_spread_bps)
            if half_spread_bps is not None
            else (float(slippage_bps) if slippage_bps is not None else None)
        )
        self._explicit_half_spread_bps: Optional[float] = eff_half_bps
        self._explicit_impact_y: Optional[float] = (
            float(impact_coefficient_y) if impact_coefficient_y is not None else None
        )
        self._market_impact_spec = market_impact_spec or MarketImpactSpec(
            half_spread_bps=float(eff_half_bps if eff_half_bps is not None else 0.0),
            impact_coefficient_y=float(
                impact_coefficient_y if impact_coefficient_y is not None else 0.0
            ),
        )
        self._system_config = system_config
        self._effective_trials = max(1, int(effective_trials))
        self._venue_name = str(venue_name or "ALPACA").strip().upper()
        self._last_first_entry_fill: Optional[Tuple[int, float]] = None
        self._last_plan_gate_backend: Optional[PostgresPlanGateBackend] = None
        self._last_oco_monitor: Optional[ProtectiveOcoMonitor] = None
        self._last_telemetry_actor: Optional[PostgresTelemetryActor] = None
        self._last_strategy: Optional[Any] = None
        self._last_orders: List[Any] = []
        self._last_period_returns: List[float] = []
        self._last_trade_cycles: List[Dict[str, Any]] = []

    def load_historical_data(
        self,
        plan_item: PlanItemContract,
        start_ns: Optional[int] = None,
        end_ns: Optional[int] = None,
        include_quotes: bool = False,
        include_statuses: bool = False,
    ) -> Tuple[List[Equity], List[Bar], List[QuoteTick], List[InstrumentStatus]]:
        """Loads instruments, dual-stream bars (`15m` + `1m`), quotes, and statuses from `DataInfraCatalog`."""
        if self._datainfra_config is None:
            raise ValueError(
                "DataInfraConfig is required to load historical data from DataInfraCatalog"
            )
        catalog: Optional[DataInfraCatalog] = DataInfraCatalog(self._datainfra_config)
        try:
            symbols = [plan_item.symbol]
            venue = plan_item.venue or self._venue_name
            equities = [
                catalog.register_equity(sym, venue=venue, lot_size=1)
                for sym in symbols
            ]
            primary_bars = catalog.query_bars(
                symbols,
                start_ts_ns=start_ns,
                end_ts_ns=end_ns,
                timeframe=plan_item.strategy_config.bar_timeframe,
                adjustment="adjusted",
                market_session="REG",
                venue=venue,
            )
            dedup_bars: Dict[Tuple[str, int], Bar] = {}
            for b in primary_bars or ():
                dedup_bars[(str(b.bar_type), int(b.ts_init))] = b
            tf_norm = (plan_item.strategy_config.bar_timeframe or "15m").strip().lower()
            if tf_norm != "1m":
                exec_bars = catalog.query_bars(
                    symbols,
                    start_ts_ns=start_ns,
                    end_ts_ns=end_ns,
                    timeframe="1m",
                    adjustment="adjusted",
                    market_session="REG",
                    venue=venue,
                )
                for b in exec_bars or ():
                    if _bar_step_ns(b) == 60_000_000_000:
                        dedup_bars[(str(b.bar_type), int(b.ts_init))] = b
            bars = _sort_bars_deterministic(list(dedup_bars.values()))
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
        finally:
            catalog = None

    def run_backtest(
        self,
        plan_item: PlanItemContract,
        *,
        bars: Optional[Sequence[Bar]] = None,
        warmup_bars: Optional[Sequence[Bar]] = None,
        raw_bars: Optional[Sequence[Bar]] = None,
        raw_low_by_ts_ns: Optional[Mapping[int, float]] = None,
        raw_high_by_ts_ns: Optional[Mapping[int, float]] = None,
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
        """Executes a 5-axis isomorphic backtest in `BacktestEngine` with Rust `PreTradeRiskGate`, `CollaredTwapExecAlgorithm`, and `ProtectiveOcoMonitor`."""
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
                ref_px = float(plan_item.risk_envelope.reference_price or 0.0)
                if ref_px <= 0.0 and bar_list:
                    ref_px = float(bar_list[0].close)
                equity_list = [
                    _create_default_equity(
                        plan_item.symbol,
                        venue_str,
                        ref_price=ref_px if ref_px > 0.0 else None,
                    )
                ]

        if not bar_list:
            raise ValueError(f"Cannot run backtest for {plan_item.symbol}: bar list is empty")

        cleaned_by_ts_ns, raw_by_ts_ns, has_raw_divergence = _extract_dual_track_extremes(
            bar_list,
            raw_bars=raw_bars,
            raw_low_by_ts_ns=raw_low_by_ts_ns,
            raw_high_by_ts_ns=raw_high_by_ts_ns,
        )
        bar_list = _normalize_bars_for_parity(bar_list)
        if has_raw_divergence:
            engine_bar_list: List[Bar] = []
            for b in bar_list:
                b_ts = int(b.ts_init)
                r_pair = raw_by_ts_ns.get(b_ts)
                if r_pair is not None:
                    engine_bar_list.append(
                        _clone_bar_with_prices(
                            b,
                            open_px=float(b.open),
                            high_px=max(float(b.high), r_pair[0]),
                            low_px=min(float(b.low), r_pair[1]),
                            close_px=float(b.close),
                            ts_event=b_ts,
                            ts_init=b_ts,
                        )
                    )
                else:
                    engine_bar_list.append(b)
            engine_bar_list = _sort_bars_deterministic(engine_bar_list)
        else:
            engine_bar_list = bar_list
        effective_date = _resolve_trading_date_from_bars(bar_list, fallback=trading_date)
        items_to_lock = _build_parity_plan_items(plan_item, venue_str)
        cloned_primary = items_to_lock[0]

        sys_cfg = self._system_config or SystemConfig()
        env_cfg = load_envelope()
        env_notional = env_cfg.numeric.get("risk.max_order_notional_usd")
        if self._system_config is not None:
            order_notional_cap = float(sys_cfg.risk.max_order_notional_usd)
        elif isinstance(env_notional, Mapping) and env_notional.get("max") is not None:
            order_notional_cap = float(env_notional["max"])
        else:
            order_notional_cap = float(sys_cfg.risk.max_order_notional_usd)
        max_slices = max(
            1,
            max(
                int(getattr(it.execution_spec, "num_slices", 4) or 4)
                for it in items_to_lock
            ),
        )
        max_positions = max(
            1, int(getattr(sys_cfg.admission, "max_new_positions_per_day", 2) or 2)
        )
        submit_limit = max(30, len(items_to_lock) * max_slices * max_positions * 2)
        modify_limit = max(submit_limit, len(items_to_lock) * max_slices * max_positions * 4)
        risk_engine_cfg = RiskEngineConfig(
            max_notional_per_order={
                str(eq.id): f"{order_notional_cap:.2f}" for eq in equity_list
            },
            max_order_submit_rate=f"{submit_limit}/00:01:00",
            max_order_modify_rate=f"{modify_limit}/00:01:00",
        )
        engine = BacktestEngine(
            BacktestEngineConfig(
                bypass_logging=True,
                run_analysis=False,
                logging=LoggerConfig(
                    stdout_level=LogLevel.ERROR,
                    bypass_logging=True,
                    print_config=False,
                ),
                risk_engine=risk_engine_cfg,
            )
        )
        snapshot_backend: Optional[PostgresPlanGateBackend] = None
        collared_twap: Optional[CollaredTwapExecAlgorithm] = None
        telemetry_actor: Optional[PostgresTelemetryActor] = None
        strategy: Optional[Any] = None
        effective_fill_model: Optional[FillModel] = None
        effective_fee_model: Optional[IbkrProFixedUsEquityFeeModel] = None
        effective_latency_model: Optional[StaticLatencyModel] = None
        self._last_orders.clear()
        try:
            effective_fill_model = fill_model or BestPriceFillModel(
                prob_fill_on_limit=1.0,
                prob_slippage=0.0,
            )
            effective_fee_model = IbkrProFixedUsEquityFeeModel(
                schedule=self._fee_schedule,
                fee_multiplier=max(0.0, float(fee_multiplier)),
            )
            effective_latency_model = StaticLatencyModel(
                base_latency_nanos=0,
                insert_latency_nanos=1,
                update_latency_nanos=0,
                cancel_latency_nanos=0,
            )
            engine_slippage_active = isinstance(
                effective_fill_model, OneTickSlippageFillModel
            ) or (
                float(getattr(effective_fill_model, "prob_slippage", 0.0) or 0.0) > 0.0
            )
            engine.add_venue(
                venue=Venue(venue_str),
                oms_type=OmsType.NETTING,
                account_type=AccountType.MARGIN,
                starting_balances=[Money.from_str(f"{self._starting_capital_usd:.2f} USD")],
                base_currency=Currency.from_str("USD"),
                fill_model=effective_fill_model,
                fee_model=effective_fee_model,
                latency_model=effective_latency_model,
                reject_stop_orders=True,
            )
            for eq in equity_list:
                engine.add_instrument(eq)

            snapshot_backend = PostgresPlanGateBackend(
                plan_items=items_to_lock,
                entry_freeze=False,
                trading_date=effective_date,
                persist_to_db=False,
                allow_multi_cycle_reentry=True,
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
            primary_exec_spec = getattr(cloned_primary, "execution_spec", None)
            collared_twap = CollaredTwapExecAlgorithm(
                config=CollaredTwapExecAlgorithmConfig(
                    exec_algorithm_id=COLLARED_TWAP_EXEC_ALGORITHM_ID,
                    horizon_secs=float(
                        getattr(primary_exec_spec, "twap_horizon_secs", 900) or 900.0
                    ),
                    interval_secs=float(
                        getattr(primary_exec_spec, "twap_interval_secs", 60) or 60.0
                    ),
                    default_collar_pct=Decimal(
                        str(cloned_primary.risk_envelope.collar.limit_collar_pct)
                    ),
                    default_collar_atr=Decimal(
                        str(cloned_primary.risk_envelope.collar.limit_collar_atr)
                    ),
                    fee_schedule=self._fee_schedule,
                    log_events=False,
                    log_commands=False,
                ),
            )
            telemetry_actor = PostgresTelemetryActor(
                config=PostgresTelemetryActorConfig(
                    dsn=None,
                    persist_to_db=False,
                    enforce_protective_oco_on_position_events=bool(submit_protective_stop),
                ),
                oco_monitor=oco_monitor,
                plan_gate_backend=snapshot_backend,
                cancel_symbol_entries_callback=collared_twap.cancel_symbol_entries,
            )
            engine.set_pre_trade_risk_gate(snapshot_backend.as_pre_trade_risk_gate())
            engine.add_actor(telemetry_actor)
            engine.add_exec_algorithm(collared_twap)

            strategy = instantiate_strategy_from_contract(
                cloned_primary,
                use_limit_orders=use_limit_orders,
                submit_protective_stop=submit_protective_stop,
                execution_delay_bars=execution_delay_bars,
            )
            strategy.bind_telemetry_actor(telemetry_actor)
            if has_raw_divergence and hasattr(strategy, "bind_dual_track_bar_extremes"):
                strategy.bind_dual_track_bar_extremes(
                    cleaned_by_ts_ns=cleaned_by_ts_ns,
                    raw_by_ts_ns=raw_by_ts_ns,
                )
            if warmup_bars:
                normalized_warmup = _normalize_bars_for_parity(list(warmup_bars))
                strategy.warmup_historical_bars(normalized_warmup)
            engine.add_strategy(strategy)

            if status_list:
                engine.add_data(sorted(status_list, key=lambda s: s.ts_init))
            if quote_list:
                engine.add_data(sorted(quote_list, key=lambda q: q.ts_init))
            engine.add_data(engine_bar_list)

            engine.run()
            snapshot_backend.flush_rust_mutations(1024)

            self._last_plan_gate_backend = snapshot_backend
            self._last_oco_monitor = oco_monitor
            self._last_telemetry_actor = telemetry_actor
            self._last_strategy = strategy
            return self._extract_metrics(
                engine=engine,
                plan_item=cloned_primary,
                bars=bar_list,
                quotes=quote_list,
                backend=snapshot_backend,
                slippage_multiplier=slippage_multiplier,
                walk_forward=walk_forward,
                effective_trials=effective_trials,
                engine_slippage_active=engine_slippage_active,
            )
        finally:
            if strategy is not None:
                if hasattr(strategy, "_telemetry_actor_ref"):
                    strategy._telemetry_actor_ref = None
                if hasattr(strategy, "_cached_order_factory"):
                    strategy._cached_order_factory = None
                if hasattr(strategy, "_cached_clock"):
                    strategy._cached_clock = None
                if hasattr(strategy, "_warmup_loader"):
                    strategy._warmup_loader = None
            if collared_twap is not None:
                collared_twap._schedules_by_primary.clear()
                collared_twap._primary_by_child.clear()
                collared_twap._primary_by_timer.clear()
                collared_twap._primaries_by_symbol.clear()
            if telemetry_actor is not None:
                telemetry_actor._cached_cache = None
                telemetry_actor._cached_clock = None
                telemetry_actor._cancel_symbol_entries_callback = None
                telemetry_actor._plan_gate_backend = None
                telemetry_actor._plan_repo = None
                if (
                    hasattr(telemetry_actor, "_oco_monitor")
                    and telemetry_actor._oco_monitor is not None
                ):
                    telemetry_actor._oco_monitor._on_entry_freeze_callback = None
                    telemetry_actor._oco_monitor._entry_freeze_callbacks.clear()
                telemetry_actor.close()
            if snapshot_backend is not None:
                snapshot_backend.close()
            engine.dispose()
            del (
                effective_fill_model,
                effective_fee_model,
                effective_latency_model,
                strategy,
                collared_twap,
                telemetry_actor,
                engine,
            )

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
            bar_list = _sort_bars_deterministic(loaded_bars)
            equity_list = list(loaded_equities)
        else:
            bar_list = _sort_bars_deterministic(bars)
            equity_list = list(equities) if equities is not None else None

        primary_bars = _filter_primary_bars(bar_list, plan_item)
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
        warmup_count = (
            int(plan_item.strategy_config.lookback_window)
            + int(plan_item.strategy_config.slow_window)
        )
        warmup_start_idx = max(0, split_idx - warmup_count)
        warmup_start_ts = primary_bars[warmup_start_idx].ts_init
        oos_warmup_bars = [
            b for b in bar_list if warmup_start_ts <= b.ts_init < split_ts
        ]
        oos_bars = [b for b in bar_list if b.ts_init >= split_ts]

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
            warmup_bars=oos_warmup_bars,
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
        quotes: Sequence[QuoteTick] = (),
        backend: Optional[PostgresPlanGateBackend],
        slippage_multiplier: float,
        walk_forward: Optional[WalkForwardSummary],
        effective_trials: Optional[int] = None,
        engine_slippage_active: bool = False,
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
        primary_bars = _sort_bars_deterministic(_filter_primary_bars(bars, plan_item))
        last_close_by_inst: Dict[str, float] = {}
        for b in bars:
            last_close_by_inst[str(b.bar_type.instrument_id)] = float(b.close)

        filled_fills: List[Tuple[float, float]] = []
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
                if qty_f > 0.0 and px_f > 0.0:
                    filled_fills.append((px_f, qty_f))
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

        order_init_ts_by_id: Dict[str, int] = {
            str(getattr(o, "client_order_id", "")): int(getattr(o, "ts_init", 0) or 0)
            for o in orders
            if str(getattr(o, "client_order_id", ""))
        }
        order_spawn_id_by_id: Dict[str, str] = {
            str(getattr(o, "client_order_id", "")): str(
                getattr(o, "exec_spawn_id", "") or ""
            )
            for o in orders
            if str(getattr(o, "client_order_id", ""))
        }
        net_pnl = 0.0
        trade_cycles: List[Dict[str, Any]] = []
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
            ts_opened = int(getattr(pos, "ts_opened", 0) or 0)
            ts_closed = int(getattr(pos, "ts_closed", 0) or 0)
            opening_ord_id = str(getattr(pos, "opening_order_id", "") or "")
            spawn_ord_id = order_spawn_id_by_id.get(opening_ord_id, "")
            signal_ts_ns = order_init_ts_by_id.get(
                spawn_ord_id or opening_ord_id,
                order_init_ts_by_id.get(opening_ord_id, 0),
            )
            entry_px = _parse_money_float(getattr(pos, "avg_px_open", 0.0))
            trade_cycles.append(
                {
                    "instrument_id": inst_key,
                    "opening_order_id": opening_ord_id,
                    "signal_ts_ns": signal_ts_ns,
                    "ts_opened": ts_opened,
                    "ts_closed": ts_closed,
                    "entry_px": entry_px,
                    "net_pnl_usd": pos_total_pnl,
                }
            )
        self._last_trade_cycles = trade_cycles

        sizing_trace = dict(getattr(plan_item, "sizing_trace", {}) or {})
        ref_px = float(plan_item.risk_envelope.reference_price or 0.0)
        if ref_px <= 0.0 and primary_bars:
            ref_px = float(primary_bars[-1].close)
        daily_atr_val = float(getattr(plan_item.risk_envelope, "daily_atr_14", 0.0) or 0.0)
        if daily_atr_val > 0.0 and ref_px > 0.0:
            daily_sigma = daily_atr_val / ref_px
        elif len(primary_bars) >= 3:
            bar_rets = [
                (float(primary_bars[i].close) / float(primary_bars[i - 1].close)) - 1.0
                for i in range(1, len(primary_bars))
                if float(primary_bars[i - 1].close) > 0.0
            ]
            if len(bar_rets) >= 2:
                bars_per_day = max(
                    1.0,
                    BARS_PER_TRADING_YEAR.get(plan_item.strategy_config.bar_timeframe, 252.0)
                    / 252.0,
                )
                daily_sigma = statistics.stdev(bar_rets) * math.sqrt(bars_per_day)
            else:
                daily_sigma = 0.0
        else:
            daily_sigma = 0.0

        adv_raw = sizing_trace.get("adv_20_shares")
        if adv_raw is not None and float(adv_raw) > 0.0:
            adv_20_shares = float(adv_raw)
        elif primary_bars:
            vols_by_day: Dict[date, float] = {}
            for pb in primary_bars:
                pb_dt = datetime.fromtimestamp(
                    int(pb.ts_init) / 1_000_000_000.0, tz=timezone.utc
                ).date()
                vols_by_day[pb_dt] = vols_by_day.get(pb_dt, 0.0) + float(pb.volume)
            day_vols = [v for v in vols_by_day.values() if v > 0.0]
            adv_20_shares = (
                statistics.fmean(day_vols[-20:])
                if day_vols
                else max(1.0, sum(q for _, q in filled_fills))
            )
        else:
            adv_20_shares = max(1.0, sum(q for _, q in filled_fills))

        eff_half_bps = self._explicit_half_spread_bps
        if eff_half_bps is None and quotes:
            quote_spreads_bps: List[float] = []
            for qt in quotes:
                bid_f = float(qt.bid_price)
                ask_f = float(qt.ask_price)
                mid_f = 0.5 * (bid_f + ask_f)
                if mid_f > 0.0 and ask_f >= bid_f:
                    quote_spreads_bps.append(((ask_f - bid_f) / (2.0 * mid_f)) * 10_000.0)
            if quote_spreads_bps:
                eff_half_bps = statistics.median(quote_spreads_bps)
        if eff_half_bps is None and sizing_trace.get("half_spread_bps") is not None:
            eff_half_bps = max(0.0, float(sizing_trace["half_spread_bps"]))
        if eff_half_bps is None:
            if ref_px > 0.0:
                eff_half_bps = (
                    float(rule_612_tick_size(Decimal(str(ref_px)))) / (2.0 * ref_px)
                ) * 10_000.0
            else:
                eff_half_bps = 0.0

        eff_impact_y = self._explicit_impact_y
        if eff_impact_y is None and sizing_trace.get("impact_coefficient_y") is not None:
            eff_impact_y = max(0.0, float(sizing_trace["impact_coefficient_y"]))
        if eff_impact_y is None:
            eff_impact_y = float(self._market_impact_spec.impact_coefficient_y)

        impact_spec = MarketImpactSpec(
            half_spread_bps=max(0.0, float(eff_half_bps)),
            impact_coefficient_y=max(0.0, float(eff_impact_y)),
            min_adv_shares=self._market_impact_spec.min_adv_shares,
        )
        slip_scale = max(0.0, float(slippage_multiplier))
        est_slippage_cost = (
            sum(
                compute_half_spread_and_sqrt_impact(
                    price=px_f,
                    quantity=qty_f,
                    daily_sigma=daily_sigma,
                    adv_20_shares=adv_20_shares,
                    spec=impact_spec,
                ).total_slippage_usd
                for px_f, qty_f in filled_fills
            )
            * slip_scale
        )
        deducted_slippage_cost = (
            0.0 if engine_slippage_active else est_slippage_cost
        )
        if deducted_slippage_cost > 0.0:
            net_pnl -= deducted_slippage_cost

        gross_pnl = net_pnl + total_commissions + deducted_slippage_cost
        end_cap = start_cap + net_pnl
        total_return_pct = (net_pnl / start_cap) if start_cap > 0.0 else 0.0

        period_returns, max_dd_pct = self._build_equity_curve_returns(
            primary_bars=primary_bars,
            orders=orders,
            start_cap=start_cap,
            net_pnl_final=net_pnl,
            all_bars=bars,
            total_commissions=total_commissions,
            est_slippage_cost=deducted_slippage_cost,
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

        if backend is not None:
            gate_denied = sum(
                1 for a in backend.alerts if str(a.get("kind") or "") == "ORDER_DENIED"
            )
            denied_cnt = max(
                gate_denied,
                sum(1 for o in orders if o.status == OrderStatus.DENIED),
            )
            admitted_cnt = sum(
                1
                for o in orders
                if o.status not in (OrderStatus.DENIED, OrderStatus.INITIALIZED)
            )
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

        bar_universe = _sort_bars_deterministic(
            list(all_bars if all_bars is not None else primary_bars)
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


MAX_LIMIT_COLLAR_MISSED_FILL_RATE: float = 0.35


@dataclass(frozen=True)
class StressFalsificationReport:
    """Complete counterfactual falsification report across all 4 mandatory stress tests."""

    symbol: str
    archetype_id: str
    passed: bool
    baseline_metrics: IsomorphicBacktestMetrics
    double_friction_result: StressExperimentResult
    signal_delay_result: StressExperimentResult
    tail_gap_shock_result: StressExperimentResult
    limit_collar_missed_fill_result: Optional[StressExperimentResult] = None
    failure_reasons: Tuple[str, ...] = field(default_factory=tuple)
    conditional_event_r_multiples: Tuple[float, ...] = field(default_factory=tuple)
    conditional_event_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "symbol": self.symbol,
            "archetype_id": self.archetype_id,
            "passed": self.passed,
            "baseline_metrics": self.baseline_metrics.to_dict(),
            "double_friction_result": self.double_friction_result.to_dict(),
            "signal_delay_result": self.signal_delay_result.to_dict(),
            "tail_gap_shock_result": self.tail_gap_shock_result.to_dict(),
            "failure_reasons": list(self.failure_reasons),
        }
        if self.limit_collar_missed_fill_result is not None:
            payload["limit_collar_missed_fill_result"] = (
                self.limit_collar_missed_fill_result.to_dict()
            )
        if self.conditional_event_count > 0:
            payload["conditional_event_count"] = int(self.conditional_event_count)
            payload["conditional_event_r_multiples"] = [
                round(float(r), 6) for r in self.conditional_event_r_multiples
            ]
        return payload


def _normalize_event_ts_ns(raw_ts: Any) -> int:
    if isinstance(raw_ts, datetime):
        dt = raw_ts if raw_ts.tzinfo is not None else raw_ts.replace(tzinfo=timezone.utc)
        return int(round(dt.astimezone(timezone.utc).timestamp() * 1_000_000_000))
    val = int(raw_ts)
    if val <= 0:
        return val
    if val < 100_000_000_000:
        return val * 1_000_000_000
    if val < 100_000_000_000_000:
        return val * 1_000_000
    if val < 100_000_000_000_000_000:
        return val * 1_000
    return val


class StressFalsifier:
    """Counterfactual stress falsifier executing 4 mandatory experiments in `BacktestEngine`."""

    def __init__(
        self,
        runner: IsomorphicBacktestRunner,
        friction_multiplier: float = 2.0,
        tail_quantile: Decimal | float | str = Decimal("0.95"),
        tail_shock_multiplier: float = 1.5,
        max_missed_fill_rate: float = MAX_LIMIT_COLLAR_MISSED_FILL_RATE,
    ) -> None:
        self._runner = runner
        self._friction_multiplier = max(1.5, float(friction_multiplier))
        self._tail_quantile = float(tail_quantile)
        self._tail_shock_multiplier = max(1.0, float(tail_shock_multiplier))
        self._max_missed_fill_rate = min(1.0, max(0.0, float(max_missed_fill_rate)))

    def run_falsification(
        self,
        plan_item: PlanItemContract,
        *,
        bars: Optional[Sequence[Bar]] = None,
        raw_bars: Optional[Sequence[Bar]] = None,
        raw_low_by_ts_ns: Optional[Mapping[int, float]] = None,
        raw_high_by_ts_ns: Optional[Mapping[int, float]] = None,
        equities: Optional[Sequence[Equity]] = None,
        use_limit_orders: bool = True,
        submit_protective_stop: bool = True,
        effective_trials: Optional[int] = None,
        conditional_event_windows: Optional[Sequence[Tuple[Any, ...]]] = None,
        execution_spec: Optional[ExecutionSpec | Mapping[str, Any]] = None,
        daily_atr_14: Optional[Decimal | float | str] = None,
        impact_spec: Optional[MarketImpactSpec] = None,
    ) -> StressFalsificationReport:
        """Runs baseline + 4 counterfactual stress experiments in `BacktestEngine`."""
        if conditional_event_windows is not None:
            return self.run_conditional_swing_falsification(
                plan_item,
                bars=bars,
                conditional_event_windows=conditional_event_windows,
                execution_spec=execution_spec,
                daily_atr_14=daily_atr_14,
                impact_spec=impact_spec,
                raw_bars=raw_bars,
                raw_low_by_ts_ns=raw_low_by_ts_ns,
                raw_high_by_ts_ns=raw_high_by_ts_ns,
                equities=equities,
                use_limit_orders=use_limit_orders,
                submit_protective_stop=submit_protective_stop,
                effective_trials=effective_trials,
            )

        if bars is None:
            loaded_equities, loaded_bars, _, _ = self._runner.load_historical_data(plan_item)
            bar_list = _sort_bars_deterministic(loaded_bars)
            equity_list = list(loaded_equities)
        else:
            bar_list = _sort_bars_deterministic(bars)
            equity_list = list(equities) if equities is not None else None

        resolved_date = _resolve_trading_date_from_bars(bar_list)
        baseline = self._runner.run_backtest(
            plan_item,
            bars=bar_list,
            raw_bars=raw_bars,
            raw_low_by_ts_ns=raw_low_by_ts_ns,
            raw_high_by_ts_ns=raw_high_by_ts_ns,
            equities=equity_list,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            trading_date=resolved_date,
            effective_trials=effective_trials,
        )
        baseline_first_entry = getattr(self._runner, "_last_first_entry_fill", None)
        baseline_trade_cycles = list(getattr(self._runner, "_last_trade_cycles", []) or [])
        baseline_orders = list(getattr(self._runner, "_last_orders", []) or [])

        # 1. Experiment 1: 2x Friction & Slippage Stress Test across entire horizon
        friction_metrics = self._runner.run_backtest(
            plan_item,
            bars=bar_list,
            raw_bars=raw_bars,
            raw_low_by_ts_ns=raw_low_by_ts_ns,
            raw_high_by_ts_ns=raw_high_by_ts_ns,
            equities=equity_list,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            fee_multiplier=self._friction_multiplier,
            slippage_multiplier=self._friction_multiplier,
            fill_model=None,
            trading_date=resolved_date,
            effective_trials=effective_trials,
        )
        max_budget = float(plan_item.risk_envelope.max_risk_budget_usd)
        if baseline.net_pnl_usd > 0.0:
            friction_passed = (
                friction_metrics.filled_orders > 0 and friction_metrics.net_pnl_usd > 0.0
            )
        else:
            incremental_friction_loss = max(
                0.0, float(baseline.net_pnl_usd) - float(friction_metrics.net_pnl_usd)
            )
            friction_passed = (
                friction_metrics.filled_orders > 0
                and friction_metrics.net_pnl_usd >= -max_budget
                and abs(min(0.0, friction_metrics.max_single_trade_loss_usd)) <= max_budget
                and incremental_friction_loss <= 0.50 * max_budget
            )
        friction_reason = (
            f"2x friction net_pnl_usd={friction_metrics.net_pnl_usd:.2f} within risk envelope (max_budget={max_budget:.2f})"
            if friction_passed
            else (
                f"FAILED 2x friction stress: net_pnl_usd={friction_metrics.net_pnl_usd:.2f} "
                f"(filled_orders={friction_metrics.filled_orders}, max_budget={max_budget:.2f})"
            )
        )
        double_friction_result = StressExperimentResult(
            experiment_name="double_friction_stress",
            passed=friction_passed,
            metrics=friction_metrics,
            threshold_description="net_pnl_usd > 0.0 under 2x fees + 2x slippage",
            observed_value=friction_metrics.net_pnl_usd,
            reason=friction_reason,
        )

        # 2. Experiment 2: +1 Bar Signal Execution Latency Stress Test (signal on bar_t -> order submitted on bar_{t+1} -> filled on bar_{t+2})
        delay_metrics = self._runner.run_backtest(
            plan_item,
            bars=bar_list,
            raw_bars=raw_bars,
            raw_low_by_ts_ns=raw_low_by_ts_ns,
            raw_high_by_ts_ns=raw_high_by_ts_ns,
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
            raw_bars=raw_bars,
            raw_low_by_ts_ns=raw_low_by_ts_ns,
            raw_high_by_ts_ns=raw_high_by_ts_ns,
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

        # 4. Experiment 4: Marketable Limit Collar Missed-Fill Stress Test (missed_fill_rate <= 0.35)
        try:
            limit_collar_missed_fill_result = self._evaluate_limit_collar_missed_fill_stress(
                plan_item=plan_item,
                bars=bar_list,
                baseline_metrics=baseline,
                trade_cycles=baseline_trade_cycles,
                orders=baseline_orders,
            )
        finally:
            baseline_orders.clear()

        failures: List[str] = []
        for res in (
            double_friction_result,
            signal_delay_result,
            tail_gap_shock_result,
            limit_collar_missed_fill_result,
        ):
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
            limit_collar_missed_fill_result=limit_collar_missed_fill_result,
            failure_reasons=tuple(failures),
        )

    def _evaluate_limit_collar_missed_fill_stress(
        self,
        *,
        plan_item: PlanItemContract,
        bars: Sequence[Bar],
        baseline_metrics: IsomorphicBacktestMetrics,
        trade_cycles: Sequence[Dict[str, Any]],
        orders: Sequence[Any],
    ) -> StressExperimentResult:
        """Evaluates whether `plan_item.risk_envelope.collar` misses <= 35% of winning signals."""
        primary_inst = plan_item.strategy_config.instrument_id
        inst_bars = _sort_bars_deterministic(
            [b for b in bars if str(b.bar_type.instrument_id) == primary_inst]
        )
        if not inst_bars:
            inst_bars = _sort_bars_deterministic(bars)
        primary_bars = _sort_bars_deterministic(_filter_primary_bars(bars, plan_item))
        if not primary_bars:
            primary_bars = inst_bars

        highs = [float(b.high) for b in primary_bars]
        lows = [float(b.low) for b in primary_bars]
        closes = [float(b.close) for b in primary_bars]
        opens = [float(b.open) for b in primary_bars]
        inst_bar_by_ts: Dict[int, Bar] = {
            int(b.ts_init): b for b in inst_bars
        }

        collar = plan_item.risk_envelope.collar
        side = plan_item.risk_envelope.side
        fast_win = max(2, int(plan_item.strategy_config.fast_window))
        slow_win = max(fast_win + 1, int(plan_item.strategy_config.slow_window))
        init_stop = float(getattr(plan_item.risk_envelope, "initial_stop_price", 0.0) or 0.0)
        atr_mult = max(1.0, float(plan_item.strategy_config.atr_stop_multiplier))
        plan_daily_atr = getattr(plan_item.risk_envelope, "daily_atr_14", None)

        orders_by_cid: Dict[str, Any] = {
            str(getattr(o, "client_order_id", "") or ""): o
            for o in orders
            if str(getattr(o, "client_order_id", "") or "")
        }
        filled_cids = {
            cid
            for cid, o in orders_by_cid.items()
            if getattr(o, "status", None) == OrderStatus.FILLED
        }
        filled_spawn_ids: set[str] = set()
        for o in orders:
            if bool(getattr(o, "is_reduce_only", False)):
                continue
            ord_status = getattr(o, "status", None)
            filled_qty_val = _parse_money_float(getattr(o, "filled_qty", 0))
            if ord_status == OrderStatus.FILLED or filled_qty_val > 0.0:
                spawn_id = str(getattr(o, "exec_spawn_id", "") or "")
                cid_str = str(getattr(o, "client_order_id", "") or "")
                if spawn_id:
                    filled_spawn_ids.add(spawn_id)
                if cid_str:
                    filled_spawn_ids.add(cid_str)

        winning_cycles = [
            c for c in trade_cycles if float(c.get("net_pnl_usd", 0.0)) > 0.0
        ]
        missed_winners = 0
        total_winners = len(winning_cycles)

        for cyc in winning_cycles:
            entry_px = float(cyc.get("entry_px", 0.0) or 0.0)
            opening_ord_id = str(cyc.get("opening_order_id") or "")
            opening_ord = orders_by_cid.get(opening_ord_id) if opening_ord_id else None
            if opening_ord is not None:
                ord_type_str = str(getattr(opening_ord, "order_type", "") or "").upper()
                ord_status = getattr(opening_ord, "status", None)
                ord_filled_qty = _parse_money_float(getattr(opening_ord, "filled_qty", 0))
                ord_limit_px = _parse_money_float(getattr(opening_ord, "price", None))
                is_filled_limit = (
                    "LIMIT" in ord_type_str
                    and (ord_status == OrderStatus.FILLED or ord_filled_qty > 0.0)
                    and ord_limit_px > 0.0
                )
                if is_filled_limit and entry_px > 0.0:
                    within_limit = (
                        entry_px <= ord_limit_px + 1e-9
                        if side == PlanSide.LONG
                        else entry_px >= ord_limit_px - 1e-9
                    )
                    if within_limit:
                        continue

            signal_ts = int(cyc.get("signal_ts_ns", 0) or 0)
            ts_opened = int(cyc.get("ts_opened", 0) or cyc.get("entry_ts_ns", 0) or 0)
            anchor_ts = signal_ts if signal_ts > 0 else ts_opened
            bar_idx = 0
            for idx, b in enumerate(primary_bars):
                if int(b.ts_init) <= anchor_ts:
                    bar_idx = idx
                else:
                    break
            next_idx = min(len(primary_bars) - 1, bar_idx + 1)
            if ts_opened > anchor_ts:
                for idx in range(bar_idx + 1, len(primary_bars)):
                    if int(primary_bars[idx].ts_init) >= ts_opened:
                        next_idx = idx
                        break

            exact_sig_bar = inst_bar_by_ts.get(anchor_ts)
            ref_px = float(exact_sig_bar.close) if exact_sig_bar is not None else closes[bar_idx]

            if plan_daily_atr is not None and float(plan_daily_atr) > 0.0:
                atr_val = float(plan_daily_atr)
            elif init_stop > 0.0 and ref_px > 0.0 and abs(ref_px - init_stop) > 0.0:
                atr_val = abs(ref_px - init_stop) / atr_mult
            else:
                atr_window = min(slow_win, bar_idx + 1)
                tick_floor = float(rule_612_tick_size(Decimal(f"{ref_px:.4f}")))
                atr_val = (
                    rolling_atr(
                        highs[: bar_idx + 1],
                        lows[: bar_idx + 1],
                        closes[: bar_idx + 1],
                        atr_window,
                    )
                    if bar_idx >= 1
                    else max(highs[bar_idx] - lows[bar_idx], tick_floor)
                )
            atr_val = max(atr_val, 1e-6)
            worst_limit_px = float(
                collar.compute_worst_limit_price(
                    side,
                    Decimal(f"{ref_px:.4f}"),
                    Decimal(f"{atr_val:.4f}"),
                    daily_atr_14=(
                        Decimal(f"{float(plan_daily_atr):.4f}")
                        if plan_daily_atr is not None and float(plan_daily_atr) > 0.0
                        else None
                    ),
                )
            )

            exec_bar = inst_bar_by_ts.get(ts_opened) if ts_opened > 0 else None
            has_next_bar = exec_bar is not None or next_idx != bar_idx
            exec_open = float(exec_bar.open) if exec_bar is not None else opens[next_idx]
            exec_low = float(exec_bar.low) if exec_bar is not None else lows[next_idx]
            exec_high = float(exec_bar.high) if exec_bar is not None else highs[next_idx]

            if side == PlanSide.LONG:
                price_breached = (
                    entry_px > worst_limit_px + 1e-9
                    if entry_px > 0.0
                    else (has_next_bar and exec_open > worst_limit_px + 1e-9)
                )
                bar_unfillable = has_next_bar and (exec_low > worst_limit_px + 1e-9)
            else:
                price_breached = (
                    entry_px < worst_limit_px - 1e-9
                    if entry_px > 0.0
                    else (has_next_bar and exec_open < worst_limit_px - 1e-9)
                )
                bar_unfillable = has_next_bar and (exec_high < worst_limit_px - 1e-9)

            if bar_unfillable or price_breached:
                missed_winners += 1

        # Also inspect any unfilled entry limit orders in BacktestEngine that would have profited
        if total_winners == 0:
            seen_unfilled_spawns: set[str] = set()
            for ord_obj in orders:
                ord_cid = str(getattr(ord_obj, "client_order_id", "") or "")
                if ord_cid in filled_cids:
                    continue
                if ord_obj.status in (
                    OrderStatus.FILLED,
                    OrderStatus.DENIED,
                    OrderStatus.REJECTED,
                ):
                    continue
                if bool(getattr(ord_obj, "is_reduce_only", False)):
                    continue
                spawn_key = str(getattr(ord_obj, "exec_spawn_id", "") or ord_cid)
                if spawn_key and (
                    spawn_key in filled_spawn_ids or spawn_key in seen_unfilled_spawns
                ):
                    continue
                if spawn_key:
                    seen_unfilled_spawns.add(spawn_key)

                ord_ts = int(getattr(ord_obj, "ts_init", 0) or 0)
                ord_idx = 0
                for idx, b in enumerate(primary_bars):
                    if int(b.ts_init) <= ord_ts:
                        ord_idx = idx
                    else:
                        break
                horizon_idx = min(len(primary_bars) - 1, ord_idx + fast_win)
                exact_ord_bar = inst_bar_by_ts.get(ord_ts)
                entry_ref_px = (
                    float(exact_ord_bar.close) if exact_ord_bar is not None else closes[ord_idx]
                )
                fwd_move = (
                    closes[horizon_idx] - entry_ref_px
                    if side == PlanSide.LONG
                    else entry_ref_px - closes[horizon_idx]
                )
                if fwd_move > 0.0:
                    total_winners += 1
                    missed_winners += 1

        missed_fill_rate = (
            float(missed_winners) / float(total_winners)
            if total_winners > 0
            else 1.0
        )
        passed = (
            total_winners > 0
            and missed_fill_rate <= self._max_missed_fill_rate + 1e-9
        )
        reason = (
            f"Limit collar missed_fill_rate={missed_fill_rate:.4f} <= {self._max_missed_fill_rate:.2f} "
            f"(missed={missed_winners}/{total_winners}, collar_atr={collar.limit_collar_atr})"
            if passed
            else (
                f"FAILED limit collar missed-fill stress: missed_fill_rate={missed_fill_rate:.4f} > "
                f"{self._max_missed_fill_rate:.2f} (missed={missed_winners}/{total_winners}, "
                f"collar_atr={collar.limit_collar_atr})"
            )
        )
        return StressExperimentResult(
            experiment_name="limit_collar_missed_fill_stress",
            passed=passed,
            metrics=baseline_metrics,
            threshold_description=(
                f"winning_signals > 0 and missed_fill_rate <= {self._max_missed_fill_rate:.2f}"
            ),
            observed_value=missed_fill_rate,
            reason=reason,
        )

    def run_conditional_swing_falsification(
        self,
        symbol: str | PlanItemContract = "",
        *,
        plan_item: Optional[PlanItemContract] = None,
        strategy_config: Optional[ArchetypeStrategyConfig] = None,
        risk_envelope: Optional[PlanRiskEnvelope] = None,
        bars_by_symbol: Optional[Mapping[str, Sequence[Bar]]] = None,
        bars: Optional[Sequence[Bar]] = None,
        conditional_event_windows: Sequence[Tuple[Any, ...]] = (),
        execution_spec: Optional[ExecutionSpec | Mapping[str, Any]] = None,
        daily_atr_14: Optional[Decimal | float | str] = None,
        impact_spec: Optional[MarketImpactSpec] = None,
        plan_items_by_symbol: Optional[Mapping[str, PlanItemContract]] = None,
        plan_items_by_window: Optional[Sequence[PlanItemContract]] = None,
        raw_bars: Optional[Sequence[Bar]] = None,
        raw_low_by_ts_ns: Optional[Mapping[int, float]] = None,
        raw_high_by_ts_ns: Optional[Mapping[int, float]] = None,
        raw_low_by_symbol_ts_ns: Optional[Mapping[str, Mapping[int, float]]] = None,
        equities: Optional[Sequence[Equity]] = None,
        use_limit_orders: bool = True,
        submit_protective_stop: bool = True,
        effective_trials: Optional[int] = None,
    ) -> StressFalsificationReport:
        """Evaluates 4 mandatory stress experiments strictly across conditional Top-K swing event windows `[tau, tau + h]` with `0R` missed-trade accounting."""
        if isinstance(symbol, PlanItemContract):
            base_plan_item = symbol
            target_sym = base_plan_item.symbol.strip().upper().split(".", 1)[0]
        else:
            base_plan_item = plan_item
            target_sym = str(symbol or "").strip().upper().split(".", 1)[0]

        if not target_sym and base_plan_item is not None:
            target_sym = base_plan_item.symbol.strip().upper().split(".", 1)[0]
        if not target_sym and plan_items_by_symbol:
            target_sym = next(iter(plan_items_by_symbol.keys())).strip().upper().split(".", 1)[0]
        if not target_sym and bars_by_symbol:
            target_sym = next(iter(bars_by_symbol.keys())).strip().upper().split(".", 1)[0]

        def _resolve_item_for_sym(sym_key: str) -> PlanItemContract:
            if plan_items_by_symbol and sym_key in plan_items_by_symbol:
                src_item = plan_items_by_symbol[sym_key]
            elif plan_items_by_window and len(plan_items_by_window) > 0 and base_plan_item is None:
                src_item = plan_items_by_window[0]
            elif base_plan_item is not None:
                src_item = base_plan_item
            else:
                if strategy_config is None or risk_envelope is None:
                    raise ValueError(
                        "run_conditional_swing_falsification requires either plan_item or (strategy_config, risk_envelope)"
                    )
                venue_part = (
                    strategy_config.instrument_id.split(".", 1)[1].strip().upper()
                    if "." in strategy_config.instrument_id
                    else self._runner._venue_name
                )
                eff_cfg = (
                    strategy_config
                    if strategy_config.instrument_id == f"{sym_key}.{venue_part}"
                    else replace(strategy_config, instrument_id=f"{sym_key}.{venue_part}")
                )
                src_item = PlanItemContract(
                    item_id=uuid.uuid4(),
                    plan_id=uuid.uuid4(),
                    symbol=sym_key,
                    venue=venue_part,
                    broker="alpaca",
                    asset_class="US_EQUITY",
                    strategy_id=f"{eff_cfg.archetype_id.value}-COND-{sym_key}",
                    strategy_config=eff_cfg,
                    risk_envelope=risk_envelope,
                    sizing_trace={"calibration": "conditional_swing_falsification"},
                )

            venue_str = src_item.venue or self._runner._venue_name
            eff_strat_cfg = src_item.strategy_config
            if eff_strat_cfg.instrument_id != f"{sym_key}.{venue_str}":
                eff_strat_cfg = replace(eff_strat_cfg, instrument_id=f"{sym_key}.{venue_str}")

            eff_env = src_item.risk_envelope
            if execution_spec is not None:
                norm_exec = (
                    execution_spec
                    if isinstance(execution_spec, ExecutionSpec)
                    else ExecutionSpec.from_dict(
                        execution_spec,
                        default_bar_timeframe=eff_strat_cfg.bar_timeframe,
                    )
                )
            else:
                norm_exec = src_item.execution_spec or eff_env.execution_spec

            if daily_atr_14 is not None and str(daily_atr_14).strip() != "":
                norm_atr: Optional[Decimal] = Decimal(str(daily_atr_14))
            else:
                norm_atr = src_item.daily_atr_14 or eff_env.daily_atr_14

            eff_env = replace(
                eff_env,
                execution_spec=norm_exec,
                daily_atr_14=norm_atr,
            )
            eff_trace = dict(src_item.sizing_trace or {})
            if impact_spec is not None:
                eff_trace["half_spread_bps"] = float(impact_spec.half_spread_bps)
                eff_trace["impact_coefficient_y"] = float(impact_spec.impact_coefficient_y)
            return replace(
                src_item,
                symbol=sym_key,
                strategy_config=eff_strat_cfg,
                risk_envelope=eff_env,
                execution_spec=norm_exec,
                daily_atr_14=norm_atr,
                sizing_trace=eff_trace,
            )

        seed_item = _resolve_item_for_sym(target_sym or "SPY")
        bars_map: Dict[str, List[Bar]] = {}
        equities_map: Dict[str, List[Equity]] = {}
        if bars_by_symbol is not None:
            for k, seq in bars_by_symbol.items():
                if seq:
                    norm_k = str(k).strip().upper().split(".", 1)[0]
                    bars_map[norm_k] = _sort_bars_deterministic(seq)
        elif bars is not None:
            sym_groups: Dict[str, List[Bar]] = {}
            for b in bars:
                b_sym = str(b.bar_type.instrument_id).split(".", 1)[0].strip().upper()
                sym_groups.setdefault(b_sym or seed_item.symbol, []).append(b)
            for k, seq in sym_groups.items():
                bars_map[k] = _sort_bars_deterministic(seq)
        else:
            loaded_eq, loaded_bars, _, _ = self._runner.load_historical_data(seed_item)
            bars_map[seed_item.symbol] = _sort_bars_deterministic(loaded_bars)
            equities_map[seed_item.symbol] = list(loaded_eq)

        if equities is not None:
            for eq in equities:
                eq_sym = str(eq.id).split(".", 1)[0].strip().upper()
                equities_map.setdefault(eq_sym, []).append(eq)

        normalized_windows: List[Tuple[str, int, int]] = []
        for win in conditional_event_windows or ():
            if len(win) == 3:
                w_sym_raw, w_s, w_e = win
                w_sym = str(w_sym_raw).strip().upper().split(".", 1)[0]
                s_ns = _normalize_event_ts_ns(w_s)
                e_ns = _normalize_event_ts_ns(w_e)
                if w_sym in bars_map and e_ns >= s_ns:
                    normalized_windows.append((w_sym, s_ns, e_ns))
            elif len(win) == 2:
                w_s, w_e = win
                s_ns = _normalize_event_ts_ns(w_s)
                e_ns = _normalize_event_ts_ns(w_e)
                if e_ns < s_ns:
                    continue
                if target_sym and target_sym in bars_map:
                    normalized_windows.append((target_sym, s_ns, e_ns))
                else:
                    for sym_k in bars_map:
                        normalized_windows.append((sym_k, s_ns, e_ns))

        start_cap = float(self._runner._starting_capital_usd)
        eff_trials = max(
            1,
            int(
                effective_trials
                if effective_trials is not None
                else self._runner._effective_trials
            ),
        )

        def _build_agg_metrics(
            *,
            sym_label: str,
            event_pnls: Sequence[float],
            event_gross_pnls: Sequence[float],
            event_commissions: Sequence[float],
            event_slippages: Sequence[float],
            event_orders: int,
            event_fills: int,
            event_denied: int,
            event_positions: int,
            event_wins: int,
            event_losses: int,
            event_worst_loss: float,
            event_traded_notional: float,
            event_admitted: int,
            event_gate_denied: int,
            event_gross_win_sum: float,
            event_gross_loss_sum: float,
        ) -> IsomorphicBacktestMetrics:
            net_pnl = float(sum(event_pnls))
            gross_pnl = float(sum(event_gross_pnls))
            comms = float(sum(event_commissions))
            slips = float(sum(event_slippages))
            rets = [(p / start_cap) if start_cap > 0.0 else 0.0 for p in event_pnls]
            if len(rets) >= 2:
                m_r = statistics.fmean(rets)
                s_r = statistics.stdev(rets)
                ann = math.sqrt(252.0 / max(1.0, 5.0))
                sh = (m_r / s_r) * ann if s_r > 1e-12 else 0.0
            else:
                sh = 0.0
            dsr_val = compute_per_period_deflated_sharpe_ratio(
                rets,
                effective_trials=eff_trials,
            )
            eq_curve = start_cap
            peak_eq = start_cap
            max_dd = 0.0
            for p in event_pnls:
                eq_curve = max(1.0, eq_curve + p)
                if eq_curve > peak_eq:
                    peak_eq = eq_curve
                dd = (peak_eq - eq_curve) / peak_eq if peak_eq > 0.0 else 0.0
                if dd > max_dd:
                    max_dd = dd
            total_closed = event_wins + event_losses
            wr = (float(event_wins) / float(total_closed)) if total_closed > 0 else 0.0
            if event_gross_loss_sum > 1e-9:
                pf = event_gross_win_sum / event_gross_loss_sum
            elif event_gross_win_sum > 0.0:
                pf = 10.0
            else:
                pf = 0.0
            return IsomorphicBacktestMetrics(
                symbol=sym_label,
                archetype_id=seed_item.strategy_config.archetype_id.value,
                bar_timeframe=seed_item.strategy_config.bar_timeframe,
                starting_capital_usd=start_cap,
                ending_capital_usd=start_cap + net_pnl,
                net_pnl_usd=net_pnl,
                gross_pnl_usd=gross_pnl,
                total_return_pct=(net_pnl / start_cap) if start_cap > 0.0 else 0.0,
                sharpe_ratio=sh,
                per_period_dsr=dsr_val,
                max_drawdown_pct=max_dd,
                max_single_trade_loss_usd=min(0.0, float(event_worst_loss)),
                turnover_ratio=(event_traded_notional / start_cap) if start_cap > 0.0 else 0.0,
                total_commissions_usd=comms,
                estimated_slippage_cost_usd=slips,
                total_orders=event_orders,
                filled_orders=event_fills,
                denied_orders=event_denied,
                total_positions=event_positions,
                win_rate=wr,
                profit_factor=pf,
                plan_gate_admitted_orders=event_admitted,
                plan_gate_denied_orders=event_gate_denied,
                effective_trials=eff_trials,
            )

        if not normalized_windows:
            empty_m = _build_agg_metrics(
                sym_label=seed_item.symbol,
                event_pnls=[],
                event_gross_pnls=[],
                event_commissions=[],
                event_slippages=[],
                event_orders=0,
                event_fills=0,
                event_denied=0,
                event_positions=0,
                event_wins=0,
                event_losses=0,
                event_worst_loss=0.0,
                event_traded_notional=0.0,
                event_admitted=0,
                event_gate_denied=0,
                event_gross_win_sum=0.0,
                event_gross_loss_sum=0.0,
            )
            fail_reason = "NO_CONDITIONAL_EVENT_WINDOWS"
            return StressFalsificationReport(
                symbol=seed_item.symbol,
                archetype_id=seed_item.strategy_config.archetype_id.value,
                passed=False,
                baseline_metrics=empty_m,
                double_friction_result=StressExperimentResult(
                    experiment_name="double_friction_stress",
                    passed=False,
                    metrics=empty_m,
                    threshold_description="conditional_event_windows > 0",
                    observed_value=0.0,
                    reason=fail_reason,
                ),
                signal_delay_result=StressExperimentResult(
                    experiment_name="plus_one_bar_delay_stress",
                    passed=False,
                    metrics=empty_m,
                    threshold_description="conditional_event_windows > 0",
                    observed_value=0.0,
                    reason=fail_reason,
                ),
                tail_gap_shock_result=StressExperimentResult(
                    experiment_name="tail_gap_shock_stress",
                    passed=False,
                    metrics=empty_m,
                    threshold_description="conditional_event_windows > 0",
                    observed_value=0.0,
                    reason=fail_reason,
                ),
                limit_collar_missed_fill_result=StressExperimentResult(
                    experiment_name="limit_collar_missed_fill_stress",
                    passed=False,
                    metrics=empty_m,
                    threshold_description=f"winning_events > 0 and missed_fill_rate <= {self._max_missed_fill_rate:.2f}",
                    observed_value=1.0,
                    reason=fail_reason,
                ),
                failure_reasons=(fail_reason,),
            )

        prev_explicit_half = self._runner._explicit_half_spread_bps
        prev_explicit_y = self._runner._explicit_impact_y
        prev_impact_spec = self._runner._market_impact_spec
        if impact_spec is not None:
            self._runner._explicit_half_spread_bps = float(impact_spec.half_spread_bps)
            self._runner._explicit_impact_y = float(impact_spec.impact_coefficient_y)
            self._runner._market_impact_spec = impact_spec

        try:
            items_cache: Dict[str, PlanItemContract] = {}
            base_pnls: List[float] = []
            base_gross_pnls: List[float] = []
            base_comms: List[float] = []
            base_slips: List[float] = []
            base_orders = base_fills = base_denied = base_pos = 0
            base_wins = base_losses = base_admitted = base_gate_denied = 0
            base_worst_loss = 0.0
            base_notional = base_gwin = base_gloss = 0.0
            event_r_multiples: List[float] = []
            filled_event_count = 0
            missed_event_count = 0

            fric_pnls: List[float] = []
            fric_gross_pnls: List[float] = []
            fric_comms: List[float] = []
            fric_slips: List[float] = []
            fric_orders = fric_fills = fric_denied = fric_pos = 0
            fric_wins = fric_losses = fric_admitted = fric_gate_denied = 0
            fric_worst_loss = 0.0
            fric_notional = fric_gwin = fric_gloss = 0.0
            fric_filled_events = 0

            delay_pnls: List[float] = []
            delay_gross_pnls: List[float] = []
            delay_comms: List[float] = []
            delay_slips: List[float] = []
            delay_orders = delay_fills = delay_denied = delay_pos = 0
            delay_wins = delay_losses = delay_admitted = delay_gate_denied = 0
            delay_worst_loss = 0.0
            delay_notional = delay_gwin = delay_gloss = 0.0
            delay_filled_events = 0

            gap_pnls: List[float] = []
            gap_gross_pnls: List[float] = []
            gap_comms: List[float] = []
            gap_slips: List[float] = []
            gap_orders = gap_fills = gap_denied = gap_pos = 0
            gap_wins = gap_losses = gap_admitted = gap_gate_denied = 0
            gap_worst_loss = 0.0
            gap_notional = gap_gwin = gap_gloss = 0.0
            gap_filled_events = 0

            total_budget = 0.0
            max_single_budget = 0.0

            for win_idx, (win_sym, win_start_ns, win_end_ns) in enumerate(normalized_windows):
                if plan_items_by_window is not None and win_idx < len(plan_items_by_window):
                    base_item = plan_items_by_window[win_idx]
                    items_cache[win_sym] = base_item
                else:
                    if win_sym not in items_cache:
                        items_cache[win_sym] = _resolve_item_for_sym(win_sym)
                    base_item = items_cache[win_sym]
                sym_bars = bars_map[win_sym]
                sym_equities = equities_map.get(win_sym)
                sym_raw_low = (
                    raw_low_by_symbol_ts_ns.get(win_sym, raw_low_by_ts_ns)
                    if raw_low_by_symbol_ts_ns is not None
                    else raw_low_by_ts_ns
                )

                primary_step_ns = _timeframe_step_ns(base_item.strategy_config.bar_timeframe)
                max_warmup_primary = max(int(base_item.strategy_config.lookback_window) * 3, 120)
                pre_bars = [b for b in sym_bars if int(b.ts_init) < win_start_ns]
                pre_primary = [b for b in pre_bars if _bar_step_ns(b) == primary_step_ns]
                if len(pre_primary) > max_warmup_primary:
                    cutoff_ns = int(pre_primary[-max_warmup_primary].ts_init)
                    warmup_slice = [b for b in pre_bars if int(b.ts_init) >= cutoff_ns]
                else:
                    warmup_slice = pre_bars
                window_slice = [
                    b for b in sym_bars if win_start_ns <= int(b.ts_init) <= win_end_ns
                ]

                env_valid_ns = int(
                    round(base_item.risk_envelope.valid_until.timestamp() * 1_000_000_000)
                )
                if env_valid_ns < win_start_ns:
                    win_valid_dt = datetime.fromtimestamp(
                        win_end_ns / 1_000_000_000.0, tz=timezone.utc
                    ) + timedelta(days=1)
                    win_item = replace(
                        base_item,
                        risk_envelope=replace(
                            base_item.risk_envelope,
                            valid_until=win_valid_dt,
                        ),
                    )
                else:
                    win_item = base_item

                win_budget = max(1.0, float(win_item.risk_envelope.max_risk_budget_usd))
                total_budget += win_budget
                max_single_budget = max(max_single_budget, win_budget)

                window_primary = _filter_primary_bars(window_slice, win_item)
                if not window_primary:
                    # 0R missed-trade accounting
                    base_pnls.append(0.0)
                    base_gross_pnls.append(0.0)
                    base_comms.append(0.0)
                    base_slips.append(0.0)
                    event_r_multiples.append(0.0)
                    missed_event_count += 1
                    for lst in (
                        fric_pnls,
                        fric_gross_pnls,
                        fric_comms,
                        fric_slips,
                        delay_pnls,
                        delay_gross_pnls,
                        delay_comms,
                        delay_slips,
                        gap_pnls,
                        gap_gross_pnls,
                        gap_comms,
                        gap_slips,
                    ):
                        lst.append(0.0)
                    continue

                win_date = _resolve_trading_date_from_bars(window_slice)
                b_m = self._runner.run_backtest(
                    win_item,
                    bars=window_slice,
                    warmup_bars=warmup_slice,
                    raw_bars=raw_bars,
                    raw_low_by_ts_ns=sym_raw_low,
                    raw_high_by_ts_ns=raw_high_by_ts_ns,
                    equities=sym_equities,
                    use_limit_orders=use_limit_orders,
                    submit_protective_stop=submit_protective_stop,
                    trading_date=win_date,
                    effective_trials=eff_trials,
                )
                b_first_entry = getattr(self._runner, "_last_first_entry_fill", None)
                b_cycles = list(getattr(self._runner, "_last_trade_cycles", []) or [])
                b_orders = list(getattr(self._runner, "_last_orders", []) or [])

                if b_m.filled_orders > 0:
                    filled_event_count += 1
                    base_pnls.append(float(b_m.net_pnl_usd))
                    base_gross_pnls.append(float(b_m.gross_pnl_usd))
                    base_comms.append(float(b_m.total_commissions_usd))
                    base_slips.append(float(b_m.estimated_slippage_cost_usd))
                    event_r_multiples.append(float(b_m.net_pnl_usd) / win_budget)
                    base_orders += int(b_m.total_orders)
                    base_fills += int(b_m.filled_orders)
                    base_denied += int(b_m.denied_orders)
                    base_pos += int(b_m.total_positions)
                    base_admitted += int(b_m.plan_gate_admitted_orders)
                    base_gate_denied += int(b_m.plan_gate_denied_orders)
                    base_worst_loss = min(base_worst_loss, float(b_m.max_single_trade_loss_usd))
                    base_notional += float(b_m.turnover_ratio) * start_cap
                    for cyc in b_cycles:
                        c_pnl = float(cyc.get("net_pnl_usd", 0.0))
                        if c_pnl > 0.0:
                            base_wins += 1
                            base_gwin += c_pnl
                        elif c_pnl < 0.0:
                            base_losses += 1
                            base_gloss += abs(c_pnl)
                    if not use_limit_orders:
                        collar_chk = self._evaluate_limit_collar_missed_fill_stress(
                            plan_item=win_item,
                            bars=window_slice,
                            baseline_metrics=b_m,
                            trade_cycles=b_cycles,
                            orders=b_orders,
                        )
                        if any(float(c.get("net_pnl_usd", 0.0)) > 0.0 for c in b_cycles) and collar_chk.observed_value > 0.0:
                            missed_event_count += 1
                else:
                    # Strictly 0R opportunity cost for missed entry
                    base_pnls.append(0.0)
                    base_gross_pnls.append(0.0)
                    base_comms.append(0.0)
                    base_slips.append(0.0)
                    event_r_multiples.append(0.0)
                    base_orders += int(b_m.total_orders)
                    base_denied += int(b_m.denied_orders)
                    base_admitted += int(b_m.plan_gate_admitted_orders)
                    base_gate_denied += int(b_m.plan_gate_denied_orders)
                    missed_event_count += 1
                b_orders.clear()

                # 1. 2x Friction & Slippage
                f_m = self._runner.run_backtest(
                    win_item,
                    bars=window_slice,
                    warmup_bars=warmup_slice,
                    raw_bars=raw_bars,
                    raw_low_by_ts_ns=sym_raw_low,
                    raw_high_by_ts_ns=raw_high_by_ts_ns,
                    equities=sym_equities,
                    use_limit_orders=use_limit_orders,
                    submit_protective_stop=submit_protective_stop,
                    fee_multiplier=self._friction_multiplier,
                    slippage_multiplier=self._friction_multiplier,
                    trading_date=win_date,
                    effective_trials=eff_trials,
                )
                f_cycles = list(getattr(self._runner, "_last_trade_cycles", []) or [])
                if f_m.filled_orders > 0:
                    fric_filled_events += 1
                    fric_pnls.append(float(f_m.net_pnl_usd))
                    fric_gross_pnls.append(float(f_m.gross_pnl_usd))
                    fric_comms.append(float(f_m.total_commissions_usd))
                    fric_slips.append(float(f_m.estimated_slippage_cost_usd))
                    fric_worst_loss = min(fric_worst_loss, float(f_m.max_single_trade_loss_usd))
                    fric_notional += float(f_m.turnover_ratio) * start_cap
                    for cyc in f_cycles:
                        c_pnl = float(cyc.get("net_pnl_usd", 0.0))
                        if c_pnl > 0.0:
                            fric_wins += 1
                            fric_gwin += c_pnl
                        elif c_pnl < 0.0:
                            fric_losses += 1
                            fric_gloss += abs(c_pnl)
                else:
                    fric_pnls.append(0.0)
                    fric_gross_pnls.append(0.0)
                    fric_comms.append(0.0)
                    fric_slips.append(0.0)
                fric_orders += int(f_m.total_orders)
                fric_fills += int(f_m.filled_orders)
                fric_denied += int(f_m.denied_orders)
                fric_pos += int(f_m.total_positions)
                fric_admitted += int(f_m.plan_gate_admitted_orders)
                fric_gate_denied += int(f_m.plan_gate_denied_orders)

                # 2. +1 Bar Delay
                d_m = self._runner.run_backtest(
                    win_item,
                    bars=window_slice,
                    warmup_bars=warmup_slice,
                    raw_bars=raw_bars,
                    raw_low_by_ts_ns=sym_raw_low,
                    raw_high_by_ts_ns=raw_high_by_ts_ns,
                    equities=sym_equities,
                    use_limit_orders=use_limit_orders,
                    submit_protective_stop=submit_protective_stop,
                    execution_delay_bars=1,
                    trading_date=win_date,
                    effective_trials=eff_trials,
                )
                d_cycles = list(getattr(self._runner, "_last_trade_cycles", []) or [])
                if d_m.filled_orders > 0:
                    delay_filled_events += 1
                    delay_pnls.append(float(d_m.net_pnl_usd))
                    delay_gross_pnls.append(float(d_m.gross_pnl_usd))
                    delay_comms.append(float(d_m.total_commissions_usd))
                    delay_slips.append(float(d_m.estimated_slippage_cost_usd))
                    delay_worst_loss = min(delay_worst_loss, float(d_m.max_single_trade_loss_usd))
                    delay_notional += float(d_m.turnover_ratio) * start_cap
                    for cyc in d_cycles:
                        c_pnl = float(cyc.get("net_pnl_usd", 0.0))
                        if c_pnl > 0.0:
                            delay_wins += 1
                            delay_gwin += c_pnl
                        elif c_pnl < 0.0:
                            delay_losses += 1
                            delay_gloss += abs(c_pnl)
                else:
                    delay_pnls.append(0.0)
                    delay_gross_pnls.append(0.0)
                    delay_comms.append(0.0)
                    delay_slips.append(0.0)
                delay_orders += int(d_m.total_orders)
                delay_fills += int(d_m.filled_orders)
                delay_denied += int(d_m.denied_orders)
                delay_pos += int(d_m.total_positions)
                delay_admitted += int(d_m.plan_gate_admitted_orders)
                delay_gate_denied += int(d_m.plan_gate_denied_orders)

                # 3. Empirical Tail Adverse Opening Gap Shock
                shocked_win = self._inject_tail_adverse_gap_shock(
                    window_slice,
                    plan_item=win_item,
                    baseline_first_entry=b_first_entry,
                    empirical_range_bars=warmup_slice + window_slice,
                )
                g_m = self._runner.run_backtest(
                    win_item,
                    bars=shocked_win,
                    warmup_bars=warmup_slice,
                    raw_bars=raw_bars,
                    raw_low_by_ts_ns=sym_raw_low,
                    raw_high_by_ts_ns=raw_high_by_ts_ns,
                    equities=sym_equities,
                    use_limit_orders=use_limit_orders,
                    submit_protective_stop=submit_protective_stop,
                    trading_date=win_date,
                    effective_trials=eff_trials,
                )
                g_cycles = list(getattr(self._runner, "_last_trade_cycles", []) or [])
                if g_m.filled_orders > 0:
                    gap_filled_events += 1
                    gap_pnls.append(float(g_m.net_pnl_usd))
                    gap_gross_pnls.append(float(g_m.gross_pnl_usd))
                    gap_comms.append(float(g_m.total_commissions_usd))
                    gap_slips.append(float(g_m.estimated_slippage_cost_usd))
                    gap_worst_loss = min(
                        gap_worst_loss,
                        float(g_m.max_single_trade_loss_usd),
                        float(g_m.net_pnl_usd),
                    )
                    gap_notional += float(g_m.turnover_ratio) * start_cap
                    for cyc in g_cycles:
                        c_pnl = float(cyc.get("net_pnl_usd", 0.0))
                        if c_pnl > 0.0:
                            gap_wins += 1
                            gap_gwin += c_pnl
                        elif c_pnl < 0.0:
                            gap_losses += 1
                            gap_gloss += abs(c_pnl)
                else:
                    gap_pnls.append(0.0)
                    gap_gross_pnls.append(0.0)
                    gap_comms.append(0.0)
                    gap_slips.append(0.0)
                gap_orders += int(g_m.total_orders)
                gap_fills += int(g_m.filled_orders)
                gap_denied += int(g_m.denied_orders)
                gap_pos += int(g_m.total_positions)
                gap_admitted += int(g_m.plan_gate_admitted_orders)
                gap_gate_denied += int(g_m.plan_gate_denied_orders)

            sym_label = (
                seed_item.symbol
                if len(items_cache) <= 1
                else f"{seed_item.symbol}+POOLED({len(items_cache)})"
            )
            baseline_agg = _build_agg_metrics(
                sym_label=sym_label,
                event_pnls=base_pnls,
                event_gross_pnls=base_gross_pnls,
                event_commissions=base_comms,
                event_slippages=base_slips,
                event_orders=base_orders,
                event_fills=base_fills,
                event_denied=base_denied,
                event_positions=base_pos,
                event_wins=base_wins,
                event_losses=base_losses,
                event_worst_loss=base_worst_loss,
                event_traded_notional=base_notional,
                event_admitted=base_admitted,
                event_gate_denied=base_gate_denied,
                event_gross_win_sum=base_gwin,
                event_gross_loss_sum=base_gloss,
            )
            fric_agg = _build_agg_metrics(
                sym_label=sym_label,
                event_pnls=fric_pnls,
                event_gross_pnls=fric_gross_pnls,
                event_commissions=fric_comms,
                event_slippages=fric_slips,
                event_orders=fric_orders,
                event_fills=fric_fills,
                event_denied=fric_denied,
                event_positions=fric_pos,
                event_wins=fric_wins,
                event_losses=fric_losses,
                event_worst_loss=fric_worst_loss,
                event_traded_notional=fric_notional,
                event_admitted=fric_admitted,
                event_gate_denied=fric_gate_denied,
                event_gross_win_sum=fric_gwin,
                event_gross_loss_sum=fric_gloss,
            )
            delay_agg = _build_agg_metrics(
                sym_label=sym_label,
                event_pnls=delay_pnls,
                event_gross_pnls=delay_gross_pnls,
                event_commissions=delay_comms,
                event_slippages=delay_slips,
                event_orders=delay_orders,
                event_fills=delay_fills,
                event_denied=delay_denied,
                event_positions=delay_pos,
                event_wins=delay_wins,
                event_losses=delay_losses,
                event_worst_loss=delay_worst_loss,
                event_traded_notional=delay_notional,
                event_admitted=delay_admitted,
                event_gate_denied=delay_gate_denied,
                event_gross_win_sum=delay_gwin,
                event_gross_loss_sum=delay_gloss,
            )
            gap_agg = _build_agg_metrics(
                sym_label=sym_label,
                event_pnls=gap_pnls,
                event_gross_pnls=gap_gross_pnls,
                event_commissions=gap_comms,
                event_slippages=gap_slips,
                event_orders=gap_orders,
                event_fills=gap_fills,
                event_denied=gap_denied,
                event_positions=gap_pos,
                event_wins=gap_wins,
                event_losses=gap_losses,
                event_worst_loss=gap_worst_loss,
                event_traded_notional=gap_notional,
                event_admitted=gap_admitted,
                event_gate_denied=gap_gate_denied,
                event_gross_win_sum=gap_gwin,
                event_gross_loss_sum=gap_gloss,
            )

            eff_total_budget = max(1.0, total_budget)
            eff_single_budget = max(1.0, max_single_budget)
            if baseline_agg.net_pnl_usd > 0.0:
                friction_passed = (
                    fric_filled_events > 0 and fric_agg.net_pnl_usd > 0.0
                )
            else:
                inc_fric_loss = max(
                    0.0, float(baseline_agg.net_pnl_usd) - float(fric_agg.net_pnl_usd)
                )
                friction_passed = (
                    fric_filled_events > 0
                    and fric_agg.net_pnl_usd >= -eff_total_budget
                    and abs(min(0.0, fric_agg.max_single_trade_loss_usd)) <= eff_single_budget
                    and inc_fric_loss <= 0.50 * eff_total_budget
                )
            double_friction_result = StressExperimentResult(
                experiment_name="double_friction_stress",
                passed=friction_passed,
                metrics=fric_agg,
                threshold_description="conditional pooled net_pnl_usd > 0.0 under 2x fees + 2x slippage",
                observed_value=fric_agg.net_pnl_usd,
                reason=(
                    f"Conditional 2x friction net_pnl_usd={fric_agg.net_pnl_usd:.2f} passed across {len(normalized_windows)} events"
                    if friction_passed
                    else f"FAILED conditional 2x friction stress: net_pnl_usd={fric_agg.net_pnl_usd:.2f} (filled_events={fric_filled_events})"
                ),
            )

            delay_passed = (
                delay_filled_events > 0
                and delay_agg.net_pnl_usd >= -eff_total_budget
                and abs(min(0.0, delay_agg.max_single_trade_loss_usd)) <= eff_single_budget
            )
            signal_delay_result = StressExperimentResult(
                experiment_name="plus_one_bar_delay_stress",
                passed=delay_passed,
                metrics=delay_agg,
                threshold_description=f"filled_events > 0 and net_pnl_usd >= {-eff_total_budget:.2f}",
                observed_value=delay_agg.net_pnl_usd,
                reason=(
                    f"Conditional +1 bar delay net_pnl_usd={delay_agg.net_pnl_usd:.2f} within budget"
                    if delay_passed
                    else f"FAILED conditional +1 bar delay stress: net_pnl_usd={delay_agg.net_pnl_usd:.2f} (filled_events={delay_filled_events})"
                ),
            )

            observed_gap_loss = abs(min(0.0, gap_worst_loss))
            gap_passed = (
                gap_filled_events > 0
                and observed_gap_loss <= eff_single_budget
            )
            tail_gap_shock_result = StressExperimentResult(
                experiment_name="tail_gap_shock_stress",
                passed=gap_passed,
                metrics=gap_agg,
                threshold_description=f"worst_event_loss_usd <= max_risk_budget_usd ({eff_single_budget:.2f} USD)",
                observed_value=observed_gap_loss,
                reason=(
                    f"Conditional tail gap shock worst loss ${observed_gap_loss:.2f} <= ${eff_single_budget:.2f}"
                    if gap_passed
                    else f"FAILED conditional tail gap shock stress: worst loss ${observed_gap_loss:.2f} > ${eff_single_budget:.2f} (filled_events={gap_filled_events})"
                ),
            )

            total_ev = len(normalized_windows)
            missed_rate = (
                float(missed_event_count) / float(total_ev)
                if total_ev > 0 and filled_event_count > 0
                else 1.0
            )
            missed_passed = (
                filled_event_count > 0
                and missed_rate <= self._max_missed_fill_rate + 1e-9
            )
            limit_collar_missed_fill_result = StressExperimentResult(
                experiment_name="limit_collar_missed_fill_stress",
                passed=missed_passed,
                metrics=baseline_agg,
                threshold_description=(
                    f"filled_events > 0 and conditional missed_fill_rate <= {self._max_missed_fill_rate:.2f}"
                ),
                observed_value=missed_rate,
                reason=(
                    f"Conditional missed_fill_rate={missed_rate:.4f} <= {self._max_missed_fill_rate:.2f} "
                    f"(missed={missed_event_count}/{total_ev})"
                    if missed_passed
                    else f"FAILED conditional missed-fill stress: missed_fill_rate={missed_rate:.4f} > {self._max_missed_fill_rate:.2f} "
                    f"(missed={missed_event_count}/{total_ev})"
                ),
            )

            failures = [
                res.reason
                for res in (
                    double_friction_result,
                    signal_delay_result,
                    tail_gap_shock_result,
                    limit_collar_missed_fill_result,
                )
                if not res.passed
            ]
            return StressFalsificationReport(
                symbol=sym_label,
                archetype_id=seed_item.strategy_config.archetype_id.value,
                passed=(len(failures) == 0),
                baseline_metrics=baseline_agg,
                double_friction_result=double_friction_result,
                signal_delay_result=signal_delay_result,
                tail_gap_shock_result=tail_gap_shock_result,
                limit_collar_missed_fill_result=limit_collar_missed_fill_result,
                failure_reasons=tuple(failures),
                conditional_event_r_multiples=tuple(event_r_multiples),
                conditional_event_count=total_ev,
            )
        finally:
            self._runner._explicit_half_spread_bps = prev_explicit_half
            self._runner._explicit_impact_y = prev_explicit_y
            self._runner._market_impact_spec = prev_impact_spec

    def _inject_tail_adverse_gap_shock(
        self,
        bars: Sequence[Bar],
        *,
        plan_item: PlanItemContract,
        baseline_first_entry: Optional[Tuple[int, float]] = None,
        empirical_range_bars: Optional[Sequence[Bar]] = None,
    ) -> List[Bar]:
        primary_bars = _sort_bars_deterministic(_filter_primary_bars(bars, plan_item))
        primary_ids = {id(b) for b in primary_bars}
        other_bars = [b for b in bars if id(b) not in primary_ids]
        range_source = (
            _sort_bars_deterministic(_filter_primary_bars(empirical_range_bars, plan_item))
            if empirical_range_bars is not None
            else primary_bars
        )
        if not primary_bars or (empirical_range_bars is None and len(primary_bars) < 5):
            return list(bars)
        if len(range_source) < 2:
            return list(bars)

        abs_ranges: List[float] = []
        for idx in range(1, len(range_source)):
            prev_c = float(range_source[idx - 1].close)
            curr_h = float(range_source[idx].high)
            curr_l = float(range_source[idx].low)
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
                    shock_idx = idx
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

        tick_floor = float(rule_612_tick_size(Decimal(f"{max(anchor_px, 0.0):.6f}")))
        if plan_item.risk_envelope.side == PlanSide.LONG:
            shocked_close = max(tick_floor, anchor_px - shock_distance)
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
        if shock_idx + 1 < len(shocked_primary):
            nxt = shocked_primary[shock_idx + 1]
            shocked_primary[shock_idx + 1] = _clone_bar_with_prices(
                nxt,
                open_px=shocked_close,
                high_px=max(float(nxt.high), shocked_close),
                low_px=min(float(nxt.low), shocked_close),
                close_px=float(nxt.close),
            )

        return _sort_bars_deterministic(shocked_primary + other_bars)


StressFalsificationSuite = StressFalsifier


def build_datainfra_config_from_env() -> DataInfraConfig:
    """Constructs `DataInfraConfig` from container environment variables."""
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
        kafka_group_id=os.environ.get("KAFKA_GROUP_ID", "nautilus-mcp-service"),
        calendar_service_url=cal_url,
        default_venue=venue,
    )

