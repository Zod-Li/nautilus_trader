"""Offline 3x2 Pooled Regime Panel Calibrator & FrozenRegimeParamTable Repository (SPEC-0006 §2.2).

Replaces pre-market single-stock parameter grid optimization with offline pooled
regime calibration across 3x2 volatility-liquidity buckets:
- Volatility Buckets (`VolBucket`):
  `LOW_VOL` ([0, 1/3)), `MID_VOL` ([1/3, 2/3)), `HIGH_VOL` ([2/3, 1.0])
  computed from 0.5 * Q_hist_252d(ATR_14 / Close) + 0.5 * Q_cross_sec(ATR_14 / Close).
- Liquidity Buckets (`LiqBucket`):
  `HIGH_LIQ_TIGHT_SPREAD` (ADV_60d >= median and spread_bps <= median),
  `LOW_LIQ_WIDE_SPREAD` (otherwise).
- Dominant Style (`dominant_style`):
  Mapped to the 3 long-only swing archetypes (`TREND_MOMENTUM`, `VOLATILITY_BREAKOUT`,
  `MEAN_REVERSION`). `STAT_ARB_PAIRS` is physically abolished.
"""

from __future__ import annotations

import concurrent.futures
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
import json
import math
import multiprocessing
import os
import statistics
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple
import uuid
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import Json

from evolution_engine.alpha.stats_gates import (
    MAX_BY_FDR_Q,
    apply_benjamini_yekutieli_fdr_gate,
    compute_newey_west_t_stat,
)
from evolution_engine.config_center.envelope import load_envelope
from evolution_engine.config_center.schema import SystemConfig
from evolution_engine.db.dsn import resolve_database_dsn
from market_data_service.pipeline.us_equity_universe import get_gics_sector_map
from market_data_service.store.clickhouse_store import _MIN_TRADABLE_RAW_PRICE
from nautilus_trader.model import Bar, BarType, Price, Quantity
from nautilus_trader.quant.contracts.models import (
    PENNY_TICK_RULE_612,
    ArchetypeStrategyConfig,
    ExecAlgorithm,
    ExecutionSpec,
    MarketableLimitCollar,
    PlanItemContract,
    PlanRiskEnvelope,
    PlanSide,
    StrategyArchetypeId,
    TriggerPolicy,
    default_limit_collar_atr_for_archetype,
    rule_612_tick_size,
)
from nautilus_trader.quant.nodes.backtest_runner import (
    MAX_LIMIT_COLLAR_MISSED_FILL_RATE,
    IsomorphicBacktestRunner,
    StressFalsificationReport,
    StressFalsifier,
    _bar_step_ns,
    _normal_cdf,
    _normal_ppf,
    _sort_bars_deterministic,
    compute_per_period_deflated_sharpe_ratio,
)
from nautilus_trader.quant.strategies.empirical import (
    empirical_rank_quantile,
    rolling_atr,
    rolling_linear_slope,
    rolling_mad_zscore,
    rolling_mean,
    rolling_std,
    rolling_vwap,
)

ET_TZ = ZoneInfo("America/New_York")
MIN_POOLED_REGIME_SAMPLE_TRADES: int = 100
VOL_BUCKET_LOWER_TERTILE: float = 1.0 / 3.0
VOL_BUCKET_UPPER_TERTILE: float = 2.0 / 3.0
LIQ_BUCKET_MEDIAN_QUANTILE: float = 0.50
MIN_EQUITY_TICK_USD: float = float(PENNY_TICK_RULE_612)
MIN_TRADABLE_STOCK_PRICE_USD: float = float(_MIN_TRADABLE_RAW_PRICE)
DEFAULT_SWING_HORIZON_SESSIONS: int = 5
ONE_SIDED_ALPHA_005_CRITICAL_T: float = _normal_ppf(0.95)
TOTAL_3X2_REGIME_CELLS: int = 3 * 2 * len(StrategyArchetypeId)


def _default_limit_collar_pct() -> Decimal:
    return Decimal(str(SystemConfig().execution.limit_collar_pct)).quantize(Decimal("0.0001"))


H0_EXECUTION_SPEC: ExecutionSpec = ExecutionSpec(
    trigger_policy=TriggerPolicy.FIRST_WINDOW_IMMEDIATE,
    exec_algorithm=ExecAlgorithm.COLLARED_TWAP,
    window_quantile=None,
    bar_timeframe="15m",
)

DEFAULT_EXECUTION_SPEC_GRID: Tuple[ExecutionSpec, ...] = (
    H0_EXECUTION_SPEC,
    ExecutionSpec(
        trigger_policy=TriggerPolicy.HYBRID_15M_1M_BREAKOUT,
        exec_algorithm=ExecAlgorithm.DIRECT_COLLARED_LIMIT,
        window_quantile=Decimal("0.50"),
        bar_timeframe="15m",
    ),
    ExecutionSpec(
        trigger_policy=TriggerPolicy.HYBRID_15M_1M_BREAKOUT,
        exec_algorithm=ExecAlgorithm.DIRECT_COLLARED_LIMIT,
        window_quantile=Decimal("0.75"),
        bar_timeframe="15m",
    ),
    ExecutionSpec(
        trigger_policy=TriggerPolicy.HYBRID_15M_1M_BREAKOUT,
        exec_algorithm=ExecAlgorithm.COLLARED_TWAP,
        window_quantile=Decimal("0.50"),
        bar_timeframe="15m",
    ),
    ExecutionSpec(
        trigger_policy=TriggerPolicy.HYBRID_15M_1M_BREAKOUT,
        exec_algorithm=ExecAlgorithm.COLLARED_TWAP,
        window_quantile=Decimal("0.75"),
        bar_timeframe="15m",
    ),
    ExecutionSpec(
        trigger_policy=TriggerPolicy.INTRADAY_AVWAP_PULLBACK,
        exec_algorithm=ExecAlgorithm.DIRECT_COLLARED_LIMIT,
        window_quantile=None,
        bar_timeframe="15m",
    ),
)


class VolBucket(str, Enum):
    """3-tier composite volatility regime bucket (`[0, 33%)`, `[33%, 67%)`, `[67%, 100%]`)."""

    LOW_VOL = "LOW_VOL"
    MID_VOL = "MID_VOL"
    HIGH_VOL = "HIGH_VOL"


class LiqBucket(str, Enum):
    """2-tier liquidity & microstructure spread regime bucket."""

    HIGH_LIQ_TIGHT_SPREAD = "HIGH_LIQ_TIGHT_SPREAD"
    LOW_LIQ_WIDE_SPREAD = "LOW_LIQ_WIDE_SPREAD"


_STYLE_TO_ARCHETYPE: Dict[str, StrategyArchetypeId] = {
    "TREND_MOMENTUM": StrategyArchetypeId.TREND_MOMENTUM,
    "MOMENTUM": StrategyArchetypeId.TREND_MOMENTUM,
    "TREND": StrategyArchetypeId.TREND_MOMENTUM,
    "VOLATILITY_BREAKOUT": StrategyArchetypeId.VOLATILITY_BREAKOUT,
    "VOL_BREAKOUT": StrategyArchetypeId.VOLATILITY_BREAKOUT,
    "BREAKOUT": StrategyArchetypeId.VOLATILITY_BREAKOUT,
    "VOLATILITY": StrategyArchetypeId.VOLATILITY_BREAKOUT,
    "MEAN_REVERSION": StrategyArchetypeId.MEAN_REVERSION,
    "REVERSAL": StrategyArchetypeId.MEAN_REVERSION,
    "MEAN_REV": StrategyArchetypeId.MEAN_REVERSION,
}


def compute_composite_vol_quantile(
    hist_252d_vol_quantile: float | Decimal,
    cross_sec_vol_quantile: float | Decimal,
) -> float:
    r"""Computes $Q_{\text{vol}, i, t} = 0.5 \cdot Q_{\text{hist\_252d}}(\text{ATR}_{14}/C) + 0.5 \cdot Q_{\text{cross\_sec}}(\text{ATR}_{14}/C)$."""
    q_hist = min(1.0, max(0.0, float(hist_252d_vol_quantile)))
    q_cs = min(1.0, max(0.0, float(cross_sec_vol_quantile)))
    return 0.5 * q_hist + 0.5 * q_cs


def classify_vol_bucket(composite_vol_quantile: float | Decimal) -> VolBucket:
    """Maps a composite volatility quantile in `[0.0, 1.0]` to its tertile `VolBucket`."""
    q = min(1.0, max(0.0, float(composite_vol_quantile)))
    if q < VOL_BUCKET_LOWER_TERTILE:
        return VolBucket.LOW_VOL
    if q < VOL_BUCKET_UPPER_TERTILE:
        return VolBucket.MID_VOL
    return VolBucket.HIGH_VOL


def classify_liq_bucket(
    *,
    adv_60d_quantile: float | Decimal = 0.50,
    spread_bps_quantile: float | Decimal = 0.50,
) -> LiqBucket:
    """Maps 60d ADV quantile and spread_bps quantile to a 2-tier `LiqBucket`."""
    adv_q = min(1.0, max(0.0, float(adv_60d_quantile)))
    spread_q = min(1.0, max(0.0, float(spread_bps_quantile)))
    if adv_q >= LIQ_BUCKET_MEDIAN_QUANTILE and spread_q <= LIQ_BUCKET_MEDIAN_QUANTILE:
        return LiqBucket.HIGH_LIQ_TIGHT_SPREAD
    return LiqBucket.LOW_LIQ_WIDE_SPREAD


def resolve_archetype_for_dominant_style(
    dominant_style: str | StrategyArchetypeId,
) -> StrategyArchetypeId:
    """Resolves a dominant factor style label or archetype name to `StrategyArchetypeId`."""
    if isinstance(dominant_style, StrategyArchetypeId):
        return dominant_style
    norm = str(dominant_style or "").strip().upper()
    if norm in _STYLE_TO_ARCHETYPE:
        return _STYLE_TO_ARCHETYPE[norm]
    raise ValueError(
        f"Unsupported dominant_style {dominant_style!r}; expected one of "
        f"{sorted(_STYLE_TO_ARCHETYPE.keys())}"
    )


def classify_3x2_regime_bucket(
    *,
    hist_252d_vol_quantile: float | Decimal,
    cross_sec_vol_quantile: float | Decimal,
    adv_60d_quantile: float | Decimal,
    spread_bps_quantile: float | Decimal,
    dominant_style: str | StrategyArchetypeId,
) -> Tuple[VolBucket, LiqBucket, str, StrategyArchetypeId]:
    """Deterministically classifies a stock/day into `(VolBucket, LiqBucket, dominant_style, StrategyArchetypeId)`."""
    comp_vol_q = compute_composite_vol_quantile(
        hist_252d_vol_quantile,
        cross_sec_vol_quantile,
    )
    vol_b = classify_vol_bucket(comp_vol_q)
    liq_b = classify_liq_bucket(
        adv_60d_quantile=adv_60d_quantile,
        spread_bps_quantile=spread_bps_quantile,
    )
    arch_id = resolve_archetype_for_dominant_style(dominant_style)
    style_key = (
        dominant_style.value
        if isinstance(dominant_style, StrategyArchetypeId)
        else str(dominant_style).strip().upper()
    )
    return vol_b, liq_b, style_key, arch_id


_TF_BAR_SPECS: Dict[str, Tuple[str, int, int]] = {
    "1m": ("1-MINUTE-LAST-EXTERNAL", 390, 60_000_000_000),
    "5m": ("5-MINUTE-LAST-EXTERNAL", 78, 300_000_000_000),
    "15m": ("15-MINUTE-LAST-EXTERNAL", 26, 900_000_000_000),
    "1h": ("1-HOUR-LAST-EXTERNAL", 7, 3_600_000_000_000),
    "4h": ("4-HOUR-LAST-EXTERNAL", 2, 14_400_000_000_000),
    "1d": ("1-DAY-LAST-EXTERNAL", 1, 86_400_000_000_000),
}


def _scale_epoch_int(raw_num: int, fallback_ns: int) -> int:
    """Normalize positive epoch integer from s/ms/us/ns to UTC nanoseconds."""
    if raw_num <= 0:
        return fallback_ns
    if raw_num < 100_000_000_000:
        return raw_num * 1_000_000_000
    if raw_num < 100_000_000_000_000:
        return raw_num * 1_000_000
    if raw_num < 100_000_000_000_000_000:
        return raw_num * 1_000
    return raw_num


def _to_epoch_ns(raw_ts: Any, fallback_ns: int) -> int:
    """Convert numeric, datetime, or ISO timestamp to UTC epoch nanoseconds."""
    if raw_ts is None or isinstance(raw_ts, bool):
        return fallback_ns
    if isinstance(raw_ts, datetime):
        dt = raw_ts if raw_ts.tzinfo is not None else raw_ts.replace(tzinfo=timezone.utc)
        return int(round(dt.astimezone(timezone.utc).timestamp() * 1_000_000_000))
    if hasattr(raw_ts, "__index__"):
        return _scale_epoch_int(int(raw_ts), fallback_ns)
    if isinstance(raw_ts, str):
        txt = raw_ts.strip()
        if not txt:
            return fallback_ns
        if txt.isdigit():
            return _scale_epoch_int(int(txt), fallback_ns)
        try:
            iso_txt = (txt[:-1] + "+00:00") if txt.endswith("Z") else txt
            dt = datetime.fromisoformat(iso_txt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(round(dt.astimezone(timezone.utc).timestamp() * 1_000_000_000))
        except ValueError:
            try:
                raw_num = float(txt)
            except ValueError:
                return fallback_ns
    elif isinstance(raw_ts, float) or hasattr(raw_ts, "__float__"):
        try:
            raw_num = float(raw_ts)
        except (TypeError, ValueError):
            return fallback_ns
    else:
        return fallback_ns

    if not math.isfinite(raw_num) or raw_num <= 0.0:
        return fallback_ns
    if raw_num < 1e11:
        return int(round(raw_num * 1_000_000_000))
    if raw_num < 1e14:
        return int(round(raw_num * 1_000_000))
    if raw_num < 1e17:
        return int(round(raw_num * 1_000))
    return int(round(raw_num))


def _build_monotonic_ts_ns(raw_timestamps: Sequence[Any], step_ns: int) -> List[int]:
    """Convert a slice of timestamps into strictly increasing UTC epoch nanoseconds."""
    base_ns = 1_700_000_000_000_000_000
    eff_step = max(1, int(step_ns))
    out: List[int] = []
    prev_ns = 0
    for idx, raw_ts in enumerate(raw_timestamps):
        fallback_ns = (prev_ns + eff_step) if prev_ns > 0 else (base_ns + idx * eff_step)
        ns = _to_epoch_ns(raw_ts, fallback_ns)
        if prev_ns > 0 and ns <= prev_ns:
            ns = prev_ns + eff_step
        out.append(ns)
        prev_ns = ns
    return out


def _compute_vectorized_rolling_atr(rel_tr_hist: Any, window: int = 14) -> Any:
    """Compute rolling mean of relative true range `(T, N)` via vectorized `np.cumsum`."""
    import numpy as np

    w = max(1, int(window))
    finite_tr = np.isfinite(rel_tr_hist)
    tr_zero = np.where(finite_tr, rel_tr_hist, 0.0)
    csum_tr = np.cumsum(tr_zero, axis=0)
    csum_cnt = np.cumsum(finite_tr.astype(np.int32), axis=0)
    win_sum = np.empty_like(tr_zero, dtype=np.float64)
    win_cnt = np.empty_like(csum_cnt, dtype=np.int32)
    if tr_zero.shape[0] <= w:
        win_sum[:] = csum_tr
        win_cnt[:] = csum_cnt
    else:
        win_sum[:w] = csum_tr[:w]
        win_sum[w:] = csum_tr[w:] - csum_tr[:-w]
        win_cnt[:w] = csum_cnt[:w]
        win_cnt[w:] = csum_cnt[w:] - csum_cnt[:-w]
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(win_cnt > 0, win_sum / np.maximum(win_cnt, 1), np.nan)


def _select_sector_stratified_candidates(
    candidates: Sequence[Tuple[int, str, float, float, str]],
    max_count: int,
) -> List[Tuple[int, str]]:
    """Select up to `max_count` symbols via GICS-sector round-robin ordered by centroid proximity."""
    if not candidates or max_count <= 0:
        return []
    med_vol = statistics.median(c[2] for c in candidates)
    med_adv = statistics.median(c[3] for c in candidates)
    by_sector: Dict[str, List[Tuple[float, str, int]]] = {}
    for col_idx, sym, comp_vol_q, adv_q, sector in candidates:
        dist = abs(comp_vol_q - med_vol) + abs(adv_q - med_adv)
        by_sector.setdefault(sector, []).append((dist, sym, col_idx))

    for sec_entries in by_sector.values():
        sec_entries.sort(key=lambda item: (item[0], item[1]))

    sector_order = sorted(by_sector.keys(), key=lambda s: (s == "UNCLASSIFIED", s))
    cursors: Dict[str, int] = {s: 0 for s in sector_order}
    selected: List[Tuple[int, str]] = []
    limit = min(len(candidates), max_count)

    while len(selected) < limit:
        progressed = False
        for sec in sector_order:
            pos = cursors[sec]
            sec_list = by_sector[sec]
            if pos >= len(sec_list):
                continue
            _, sym, col_idx = sec_list[pos]
            cursors[sec] = pos + 1
            selected.append((col_idx, sym))
            progressed = True
            if len(selected) >= limit:
                break
        if not progressed:
            break
    return selected


DEFAULT_REGIME_CANDIDATE_GRIDS: Dict[StrategyArchetypeId, Tuple[Dict[str, Any], ...]] = {
    StrategyArchetypeId.TREND_MOMENTUM: (
        {
            "bar_timeframe": "15m",
            "lookback_window": 20,
            "fast_window": 5,
            "slow_window": 15,
            "entry_quantile_threshold": Decimal("0.80"),
            "exit_quantile_threshold": Decimal("0.35"),
            "atr_stop_multiplier": Decimal("2.00"),
            "limit_collar_atr_mult": Decimal("0.25"),
        },
        {
            "bar_timeframe": "15m",
            "lookback_window": 8,
            "fast_window": 3,
            "slow_window": 6,
            "entry_quantile_threshold": Decimal("0.70"),
            "exit_quantile_threshold": Decimal("0.25"),
            "atr_stop_multiplier": Decimal("1.80"),
            "limit_collar_atr_mult": Decimal("0.25"),
        },
        {
            "bar_timeframe": "15m",
            "lookback_window": 52,
            "fast_window": 8,
            "slow_window": 26,
            "entry_quantile_threshold": Decimal("0.80"),
            "exit_quantile_threshold": Decimal("0.20"),
            "atr_stop_multiplier": Decimal("2.80"),
            "limit_collar_atr_mult": Decimal("0.25"),
        },
        {
            "bar_timeframe": "15m",
            "lookback_window": 78,
            "fast_window": 13,
            "slow_window": 52,
            "entry_quantile_threshold": Decimal("0.85"),
            "exit_quantile_threshold": Decimal("0.15"),
            "atr_stop_multiplier": Decimal("3.20"),
            "limit_collar_atr_mult": Decimal("0.25"),
        },
    ),
    StrategyArchetypeId.VOLATILITY_BREAKOUT: (
        {
            "bar_timeframe": "15m",
            "lookback_window": 20,
            "fast_window": 5,
            "slow_window": 15,
            "entry_quantile_threshold": Decimal("0.82"),
            "exit_quantile_threshold": Decimal("0.35"),
            "atr_stop_multiplier": Decimal("2.00"),
            "limit_collar_atr_mult": Decimal("0.25"),
        },
        {
            "bar_timeframe": "15m",
            "lookback_window": 5,
            "fast_window": 3,
            "slow_window": 4,
            "entry_quantile_threshold": Decimal("0.70"),
            "exit_quantile_threshold": Decimal("0.25"),
            "atr_stop_multiplier": Decimal("1.80"),
            "limit_collar_atr_mult": Decimal("0.25"),
        },
        {
            "bar_timeframe": "15m",
            "lookback_window": 52,
            "fast_window": 8,
            "slow_window": 26,
            "entry_quantile_threshold": Decimal("0.80"),
            "exit_quantile_threshold": Decimal("0.20"),
            "atr_stop_multiplier": Decimal("2.80"),
            "limit_collar_atr_mult": Decimal("0.25"),
        },
        {
            "bar_timeframe": "15m",
            "lookback_window": 78,
            "fast_window": 13,
            "slow_window": 52,
            "entry_quantile_threshold": Decimal("0.85"),
            "exit_quantile_threshold": Decimal("0.15"),
            "atr_stop_multiplier": Decimal("3.20"),
            "limit_collar_atr_mult": Decimal("0.25"),
        },
    ),
    StrategyArchetypeId.MEAN_REVERSION: (
        {
            "bar_timeframe": "15m",
            "lookback_window": 20,
            "fast_window": 5,
            "slow_window": 15,
            "entry_quantile_threshold": Decimal("0.80"),
            "exit_quantile_threshold": Decimal("0.40"),
            "atr_stop_multiplier": Decimal("1.80"),
            "limit_collar_atr_mult": Decimal("0.10"),
        },
        {
            "bar_timeframe": "15m",
            "lookback_window": 12,
            "fast_window": 3,
            "slow_window": 9,
            "entry_quantile_threshold": Decimal("0.76"),
            "exit_quantile_threshold": Decimal("0.40"),
            "atr_stop_multiplier": Decimal("1.80"),
            "limit_collar_atr_mult": Decimal("0.06"),
        },
        {
            "bar_timeframe": "15m",
            "lookback_window": 52,
            "fast_window": 8,
            "slow_window": 26,
            "entry_quantile_threshold": Decimal("0.82"),
            "exit_quantile_threshold": Decimal("0.20"),
            "atr_stop_multiplier": Decimal("2.80"),
            "limit_collar_atr_mult": Decimal("0.10"),
        },
        {
            "bar_timeframe": "15m",
            "lookback_window": 78,
            "fast_window": 13,
            "slow_window": 52,
            "entry_quantile_threshold": Decimal("0.85"),
            "exit_quantile_threshold": Decimal("0.15"),
            "atr_stop_multiplier": Decimal("3.20"),
            "limit_collar_atr_mult": Decimal("0.10"),
        },
    ),
}


@dataclass(frozen=True)
class FrozenRegimeParamRecord:
    """Frozen regime-calibrated parameter tuple persisted in `frozen_regime_param_table`."""

    vol_bucket: VolBucket
    liq_bucket: LiqBucket
    dominant_style: str
    archetype_id: StrategyArchetypeId
    bar_timeframe: str
    lookback_window: int
    fast_window: int
    slow_window: int
    entry_quantile_threshold: Decimal
    exit_quantile_threshold: Decimal
    atr_stop_multiplier: Decimal
    limit_collar_atr_mult: Decimal
    pooled_sample_trades: int
    pooled_sharpe: Decimal
    pooled_dsr: Decimal
    stress_passed: bool
    stress_missed_fill_rate: Decimal
    limit_collar_pct: Decimal = field(default_factory=_default_limit_collar_pct)
    pooled_expectancy_r: Decimal = Decimal("0.0000")
    is_active: bool = True
    stress_report_json: Dict[str, Any] = field(default_factory=dict)
    calibrated_at: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    execution_spec: ExecutionSpec = field(default_factory=ExecutionSpec)
    h0_paired_delta_r: float = 0.0
    h0_paired_nw_t_stat: float = 0.0
    by_fdr_q_value: float = 1.0
    oos_event_count: int = 0

    def __post_init__(self) -> None:
        raw_spec = self.execution_spec
        if raw_spec is None:
            norm_spec = ExecutionSpec(bar_timeframe=str(self.bar_timeframe or "15m"))
        elif isinstance(raw_spec, Mapping):
            norm_spec = ExecutionSpec.from_dict(
                raw_spec,
                default_bar_timeframe=str(self.bar_timeframe or "15m"),
            )
        elif isinstance(raw_spec, ExecutionSpec):
            norm_spec = raw_spec
        else:
            raise ValueError(
                f"execution_spec must be ExecutionSpec or mapping, got {type(raw_spec).__name__}"
            )
        object.__setattr__(self, "execution_spec", norm_spec)

    def validate(self) -> None:
        if not isinstance(self.vol_bucket, VolBucket):
            raise ValueError(f"Invalid vol_bucket: {self.vol_bucket!r}")
        if not isinstance(self.liq_bucket, LiqBucket):
            raise ValueError(f"Invalid liq_bucket: {self.liq_bucket!r}")
        if not str(self.dominant_style or "").strip():
            raise ValueError("dominant_style must be non-empty")
        if not isinstance(self.archetype_id, StrategyArchetypeId):
            raise ValueError(f"Invalid archetype_id: {self.archetype_id!r}")
        if self.pooled_sample_trades < MIN_POOLED_REGIME_SAMPLE_TRADES:
            raise ValueError(
                f"pooled_sample_trades must be >= {MIN_POOLED_REGIME_SAMPLE_TRADES}, "
                f"got {self.pooled_sample_trades}"
            )
        if not self.stress_passed:
            raise ValueError("Cannot freeze regime parameters with stress_passed=False")
        if self.stress_missed_fill_rate < Decimal("0") or self.stress_missed_fill_rate > Decimal(
            str(MAX_LIMIT_COLLAR_MISSED_FILL_RATE)
        ):
            raise ValueError(
                f"stress_missed_fill_rate {self.stress_missed_fill_rate} exceeds "
                f"maximum {MAX_LIMIT_COLLAR_MISSED_FILL_RATE}"
            )
        if self.limit_collar_atr_mult <= Decimal("0") or self.limit_collar_atr_mult > Decimal("0.50"):
            raise ValueError(
                f"limit_collar_atr_mult must be in (0, 0.50], got {self.limit_collar_atr_mult}"
            )
        if not isinstance(self.execution_spec, ExecutionSpec):
            raise ValueError(f"Invalid execution_spec: {self.execution_spec!r}")
        for fname, fval in (
            ("h0_paired_delta_r", self.h0_paired_delta_r),
            ("h0_paired_nw_t_stat", self.h0_paired_nw_t_stat),
            ("by_fdr_q_value", self.by_fdr_q_value),
        ):
            if not math.isfinite(float(fval)):
                raise ValueError(f"{fname} must be finite, got {fval!r}")
        # Validate underlying strategy parameters via ArchetypeStrategyConfig
        self.to_strategy_config("SPY.ALPACA").validate()

    def to_strategy_config(
        self,
        instrument_id: str,
        *,
        bar_timeframe: Optional[str] = None,
    ) -> ArchetypeStrategyConfig:
        return ArchetypeStrategyConfig(
            archetype_id=self.archetype_id,
            instrument_id=str(instrument_id).strip(),
            bar_timeframe=str(bar_timeframe or self.bar_timeframe),
            lookback_window=int(self.lookback_window),
            fast_window=int(self.fast_window),
            slow_window=int(self.slow_window),
            entry_quantile_threshold=Decimal(str(self.entry_quantile_threshold)),
            exit_quantile_threshold=Decimal(str(self.exit_quantile_threshold)),
            atr_stop_multiplier=Decimal(str(self.atr_stop_multiplier)),
        )

    def to_limit_collar(
        self,
        limit_collar_pct: Optional[Decimal] = None,
    ) -> MarketableLimitCollar:
        eff_pct = (
            Decimal(str(limit_collar_pct))
            if limit_collar_pct is not None
            else Decimal(str(self.limit_collar_pct))
        )
        return MarketableLimitCollar(
            limit_collar_pct=eff_pct,
            limit_collar_atr=Decimal(str(self.limit_collar_atr_mult)),
        )

    def to_params_dict(self) -> Dict[str, Any]:
        return {
            "bar_timeframe": self.bar_timeframe,
            "lookback_window": int(self.lookback_window),
            "fast_window": int(self.fast_window),
            "slow_window": int(self.slow_window),
            "entry_quantile_threshold": str(self.entry_quantile_threshold),
            "exit_quantile_threshold": str(self.exit_quantile_threshold),
            "atr_stop_multiplier": str(self.atr_stop_multiplier),
            "limit_collar_atr_mult": str(self.limit_collar_atr_mult),
            "limit_collar_pct": str(self.limit_collar_pct),
            "execution_spec": self.execution_spec.to_dict(),
        }

    @property
    def strategy_archetype(self) -> str:
        return (
            self.archetype_id.value
            if isinstance(self.archetype_id, StrategyArchetypeId)
            else str(self.archetype_id)
        )

    @property
    def strategy_config_json(self) -> Dict[str, Any]:
        return self.to_params_dict()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "vol_bucket": self.vol_bucket.value,
            "liq_bucket": self.liq_bucket.value,
            "dominant_style": self.dominant_style,
            "archetype_id": self.archetype_id.value,
            "strategy_archetype": self.archetype_id.value,
            "bar_timeframe": self.bar_timeframe,
            "lookback_window": int(self.lookback_window),
            "fast_window": int(self.fast_window),
            "slow_window": int(self.slow_window),
            "entry_quantile_threshold": str(self.entry_quantile_threshold),
            "exit_quantile_threshold": str(self.exit_quantile_threshold),
            "atr_stop_multiplier": str(self.atr_stop_multiplier),
            "limit_collar_atr_mult": str(self.limit_collar_atr_mult),
            "limit_collar_pct": str(self.limit_collar_pct),
            "execution_spec": self.execution_spec.to_dict(),
            "h0_paired_delta_r": float(self.h0_paired_delta_r),
            "h0_paired_nw_t_stat": float(self.h0_paired_nw_t_stat),
            "by_fdr_q_value": float(self.by_fdr_q_value),
            "oos_event_count": int(self.oos_event_count),
            "pooled_sample_trades": int(self.pooled_sample_trades),
            "pooled_expectancy_r": str(self.pooled_expectancy_r),
            "pooled_sharpe": str(self.pooled_sharpe),
            "pooled_dsr": str(self.pooled_dsr),
            "stress_passed": bool(self.stress_passed),
            "stress_missed_fill_rate": str(self.stress_missed_fill_rate),
            "is_active": bool(self.is_active),
            "stress_report_json": dict(self.stress_report_json),
            "calibrated_at": self.calibrated_at.isoformat(),
        }


class FrozenRegimeParamTable:
    """PostgreSQL-backed repository for `frozen_regime_param_table` supporting O(1) lookup."""

    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn = resolve_database_dsn(dsn)

    @property
    def dsn(self) -> str:
        return self._dsn

    def _connect(self):
        return psycopg2.connect(self._dsn)

    def upsert_record(self, record: FrozenRegimeParamRecord) -> None:
        """Validates and atomically upserts a `FrozenRegimeParamRecord` into `frozen_regime_param_table`."""
        record.validate()
        eff_stress_json = dict(record.stress_report_json or {})
        eff_stress_json.setdefault("execution_spec", record.execution_spec.to_dict())
        eff_stress_json.setdefault("h0_paired_delta_r", float(record.h0_paired_delta_r))
        eff_stress_json.setdefault("h0_paired_nw_t_stat", float(record.h0_paired_nw_t_stat))
        eff_stress_json.setdefault("by_fdr_q_value", float(record.by_fdr_q_value))
        eff_stress_json.setdefault("oos_event_count", int(record.oos_event_count))

        conn = self._connect()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO frozen_regime_param_table (
                            vol_bucket,
                            liq_bucket,
                            dominant_style,
                            archetype_id,
                            strategy_archetype,
                            bar_timeframe,
                            strategy_config_json,
                            lookback_window,
                            fast_window,
                            slow_window,
                            entry_quantile_threshold,
                            exit_quantile_threshold,
                            atr_stop_multiplier,
                            limit_collar_atr_mult,
                            limit_collar_pct,
                            pooled_sample_trades,
                            pooled_expectancy_r,
                            pooled_sharpe,
                            pooled_dsr,
                            stress_passed,
                            stress_missed_fill_rate,
                            stress_report_json,
                            calibrated_at,
                            is_active
                        ) VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s
                        )
                        ON CONFLICT (vol_bucket, liq_bucket, dominant_style) DO UPDATE SET
                            archetype_id = EXCLUDED.archetype_id,
                            strategy_archetype = EXCLUDED.strategy_archetype,
                            bar_timeframe = EXCLUDED.bar_timeframe,
                            strategy_config_json = EXCLUDED.strategy_config_json,
                            lookback_window = EXCLUDED.lookback_window,
                            fast_window = EXCLUDED.fast_window,
                            slow_window = EXCLUDED.slow_window,
                            entry_quantile_threshold = EXCLUDED.entry_quantile_threshold,
                            exit_quantile_threshold = EXCLUDED.exit_quantile_threshold,
                            atr_stop_multiplier = EXCLUDED.atr_stop_multiplier,
                            limit_collar_atr_mult = EXCLUDED.limit_collar_atr_mult,
                            limit_collar_pct = EXCLUDED.limit_collar_pct,
                            pooled_sample_trades = EXCLUDED.pooled_sample_trades,
                            pooled_expectancy_r = EXCLUDED.pooled_expectancy_r,
                            pooled_sharpe = EXCLUDED.pooled_sharpe,
                            pooled_dsr = EXCLUDED.pooled_dsr,
                            stress_passed = EXCLUDED.stress_passed,
                            stress_missed_fill_rate = EXCLUDED.stress_missed_fill_rate,
                            stress_report_json = EXCLUDED.stress_report_json,
                            calibrated_at = EXCLUDED.calibrated_at,
                            is_active = EXCLUDED.is_active
                        """,
                        (
                            record.vol_bucket.value,
                            record.liq_bucket.value,
                            str(record.dominant_style).strip().upper(),
                            record.archetype_id.value,
                            record.archetype_id.value,
                            record.bar_timeframe,
                            Json(record.to_params_dict()),
                            int(record.lookback_window),
                            int(record.fast_window),
                            int(record.slow_window),
                            Decimal(str(record.entry_quantile_threshold)),
                            Decimal(str(record.exit_quantile_threshold)),
                            Decimal(str(record.atr_stop_multiplier)),
                            Decimal(str(record.limit_collar_atr_mult)),
                            Decimal(str(record.limit_collar_pct)),
                            int(record.pooled_sample_trades),
                            float(record.pooled_expectancy_r),
                            float(record.pooled_sharpe),
                            float(record.pooled_dsr),
                            bool(record.stress_passed),
                            float(record.stress_missed_fill_rate),
                            Json(eff_stress_json),
                            record.calibrated_at,
                            bool(record.is_active),
                        ),
                    )
                    cur.execute(
                        """
                        UPDATE frozen_regime_param_table
                        SET is_active = FALSE
                        WHERE vol_bucket = %s
                          AND liq_bucket = %s
                          AND archetype_id = %s
                          AND dominant_style <> %s
                          AND is_active = TRUE
                        """,
                        (
                            record.vol_bucket.value,
                            record.liq_bucket.value,
                            record.archetype_id.value,
                            str(record.dominant_style).strip().upper(),
                        ),
                    )
        finally:
            conn.close()

    def upsert_many(self, records: Sequence[FrozenRegimeParamRecord]) -> int:
        count = 0
        for rec in records:
            self.upsert_record(rec)
            count += 1
        return count

    @staticmethod
    def _normalize_cell_lookup_keys(
        vol_bucket: VolBucket | str,
        liq_bucket: LiqBucket | str,
        dominant_style: str | StrategyArchetypeId,
    ) -> Tuple[str, str, str, StrategyArchetypeId]:
        vb = vol_bucket.value if isinstance(vol_bucket, VolBucket) else VolBucket(str(vol_bucket).strip().upper()).value
        lb = liq_bucket.value if isinstance(liq_bucket, LiqBucket) else LiqBucket(str(liq_bucket).strip().upper()).value
        style_str = (
            dominant_style.value
            if isinstance(dominant_style, StrategyArchetypeId)
            else str(dominant_style).strip().upper()
        )
        arch_id = resolve_archetype_for_dominant_style(dominant_style)
        return vb, lb, style_str, arch_id

    def deactivate_cell(
        self,
        vol_bucket: VolBucket | str,
        liq_bucket: LiqBucket | str,
        dominant_style: str | StrategyArchetypeId,
    ) -> int:
        """Mark records (canonical or alias style) for `(vol_bucket, liq_bucket, archetype)` inactive and stamp `calibrated_at = NOW()`."""
        vb, lb, style_str, arch_id = self._normalize_cell_lookup_keys(
            vol_bucket,
            liq_bucket,
            dominant_style,
        )
        conn = self._connect()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE frozen_regime_param_table
                        SET is_active = FALSE,
                            calibrated_at = NOW()
                        WHERE vol_bucket = %s
                          AND liq_bucket = %s
                          AND (dominant_style = %s OR dominant_style = %s OR archetype_id = %s)
                        """,
                        (vb, lb, style_str, arch_id.value, arch_id.value),
                    )
                    return int(cur.rowcount or 0)
        finally:
            conn.close()

    @staticmethod
    def _row_to_record(row: Tuple[Any, ...]) -> FrozenRegimeParamRecord:
        raw_json = row[17]
        if isinstance(raw_json, str):
            report_dict = json.loads(raw_json) if raw_json.strip() else {}
        elif isinstance(raw_json, dict):
            report_dict = dict(raw_json)
        else:
            report_dict = {}
        cal_at = row[18]
        if not isinstance(cal_at, datetime):
            cal_at = datetime.now(tz=timezone.utc)
        elif cal_at.tzinfo is None:
            cal_at = cal_at.replace(tzinfo=timezone.utc)
        bar_tf = str(row[4])
        collar_pct = (
            Decimal(str(row[19]))
            if len(row) > 19 and row[19] is not None
            else _default_limit_collar_pct()
        )
        exp_r = Decimal(f"{float(row[20]):.4f}") if len(row) > 20 and row[20] is not None else Decimal("0.0000")
        is_act = bool(row[21]) if len(row) > 21 and row[21] is not None else True

        cfg_json: Dict[str, Any] = {}
        if len(row) > 22 and row[22] is not None:
            raw_cfg = row[22]
            if isinstance(raw_cfg, str):
                cfg_json = json.loads(raw_cfg) if raw_cfg.strip() else {}
            elif isinstance(raw_cfg, Mapping):
                cfg_json = dict(raw_cfg)

        raw_exec = cfg_json.get("execution_spec") or report_dict.get("execution_spec")
        exec_spec = ExecutionSpec.from_dict(
            raw_exec if isinstance(raw_exec, Mapping) else None,
            default_bar_timeframe=bar_tf,
        )
        h0_delta_r = float(report_dict.get("h0_paired_delta_r", 0.0) or 0.0)
        h0_nw_t = float(report_dict.get("h0_paired_nw_t_stat", 0.0) or 0.0)
        by_q = float(report_dict.get("by_fdr_q_value", 1.0) if report_dict.get("by_fdr_q_value") is not None else 1.0)
        oos_cnt = int(report_dict.get("oos_event_count", 0) or 0)

        return FrozenRegimeParamRecord(
            vol_bucket=VolBucket(str(row[0])),
            liq_bucket=LiqBucket(str(row[1])),
            dominant_style=str(row[2]),
            archetype_id=StrategyArchetypeId(str(row[3])),
            bar_timeframe=bar_tf,
            lookback_window=int(row[5]),
            fast_window=int(row[6]),
            slow_window=int(row[7]),
            entry_quantile_threshold=Decimal(str(row[8])),
            exit_quantile_threshold=Decimal(str(row[9])),
            atr_stop_multiplier=Decimal(str(row[10])),
            limit_collar_atr_mult=Decimal(str(row[11])),
            pooled_sample_trades=int(row[12]),
            pooled_sharpe=Decimal(f"{float(row[13]):.4f}"),
            pooled_dsr=Decimal(f"{float(row[14]):.6f}"),
            stress_passed=bool(row[15]),
            stress_missed_fill_rate=Decimal(f"{float(row[16]):.4f}"),
            stress_report_json=report_dict,
            calibrated_at=cal_at,
            limit_collar_pct=collar_pct,
            pooled_expectancy_r=exp_r,
            is_active=is_act,
            execution_spec=exec_spec,
            h0_paired_delta_r=h0_delta_r,
            h0_paired_nw_t_stat=h0_nw_t,
            by_fdr_q_value=by_q,
            oos_event_count=oos_cnt,
        )

    def get_frozen_params(
        self,
        vol_bucket: VolBucket | str,
        liq_bucket: LiqBucket | str,
        dominant_style: str | StrategyArchetypeId,
    ) -> Optional[FrozenRegimeParamRecord]:
        """O(1) composite-PK lookup for `(vol_bucket, liq_bucket, dominant_style)`."""
        vb, lb, style_str, arch_id = self._normalize_cell_lookup_keys(
            vol_bucket,
            liq_bucket,
            dominant_style,
        )

        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        vol_bucket,
                        liq_bucket,
                        dominant_style,
                        archetype_id,
                        bar_timeframe,
                        lookback_window,
                        fast_window,
                        slow_window,
                        entry_quantile_threshold,
                        exit_quantile_threshold,
                        atr_stop_multiplier,
                        limit_collar_atr_mult,
                        pooled_sample_trades,
                        pooled_sharpe,
                        pooled_dsr,
                        stress_passed,
                        stress_missed_fill_rate,
                        stress_report_json,
                        calibrated_at,
                        limit_collar_pct,
                        pooled_expectancy_r,
                        is_active,
                        strategy_config_json
                    FROM frozen_regime_param_table
                    WHERE vol_bucket = %s
                      AND liq_bucket = %s
                      AND is_active = TRUE
                      AND (dominant_style = %s OR dominant_style = %s OR archetype_id = %s)
                    ORDER BY CASE WHEN dominant_style = %s THEN 0 ELSE 1 END, calibrated_at DESC
                    LIMIT 1
                    """,
                    (vb, lb, style_str, arch_id.value, arch_id.value, style_str),
                )
                row = cur.fetchone()
                return self._row_to_record(row) if row is not None else None
        finally:
            conn.close()

    def list_all(self, *, active_only: bool = False) -> List[FrozenRegimeParamRecord]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                where_clause = "WHERE is_active = TRUE" if active_only else ""
                cur.execute(
                    f"""
                    SELECT
                        vol_bucket,
                        liq_bucket,
                        dominant_style,
                        archetype_id,
                        bar_timeframe,
                        lookback_window,
                        fast_window,
                        slow_window,
                        entry_quantile_threshold,
                        exit_quantile_threshold,
                        atr_stop_multiplier,
                        limit_collar_atr_mult,
                        pooled_sample_trades,
                        pooled_sharpe,
                        pooled_dsr,
                        stress_passed,
                        stress_missed_fill_rate,
                        stress_report_json,
                        calibrated_at,
                        limit_collar_pct,
                        pooled_expectancy_r,
                        is_active,
                        strategy_config_json
                    FROM frozen_regime_param_table
                    {where_clause}
                    ORDER BY vol_bucket, liq_bucket, dominant_style
                    """
                )
                return [self._row_to_record(r) for r in cur.fetchall()]
        finally:
            conn.close()


@dataclass(frozen=True)
class RegimeCandidateEvaluation:
    """Evaluation outcome for a single candidate parameter tuple on a 3x2 regime bucket."""

    vol_bucket: VolBucket
    liq_bucket: LiqBucket
    dominant_style: str
    archetype_id: StrategyArchetypeId
    params: Dict[str, Any]
    passed: bool
    pooled_sample_trades: int
    pooled_sharpe: float
    pooled_dsr: float
    stress_missed_fill_rate: float
    failure_reasons: Tuple[str, ...]
    stress_reports: Tuple[StressFalsificationReport, ...]
    frozen_record: Optional[FrozenRegimeParamRecord] = None
    execution_spec: ExecutionSpec = field(default_factory=ExecutionSpec)
    h0_paired_delta_r: float = 0.0
    h0_paired_nw_t_stat: float = 0.0
    by_fdr_q_value: float = 1.0
    oos_event_count: int = 0


@dataclass(frozen=True)
class RegimeCellCalibrationResult:
    """Calibration outcome for one `(vol_bucket, liq_bucket, dominant_style)` cell."""

    vol_bucket: VolBucket
    liq_bucket: LiqBucket
    dominant_style: str
    archetype_id: StrategyArchetypeId
    selected_record: Optional[FrozenRegimeParamRecord]
    evaluated_candidates: Tuple[RegimeCandidateEvaluation, ...]
    persisted: bool


@dataclass(frozen=True)
class ConditionalSwingEvent:
    """A single conditional Top-K swing event window `[tau, tau + h]`."""

    symbol: str
    start_ns: int
    end_ns: int
    horizon_sessions: int = DEFAULT_SWING_HORIZON_SESSIONS
    score: float = 0.0


@dataclass(frozen=True)
class _PanelCalibrationContext:
    """Internal context forwarded from `calibrate_from_panel_matrices` without altering `calibrate_regime_cell` call signature."""

    execution_spec_grid: Tuple[ExecutionSpec, ...]
    bars_1m_by_symbol: Optional[Mapping[str, Sequence[Bar]]] = None
    bars_1d_by_symbol: Optional[Mapping[str, Sequence[Bar]]] = None
    alpha_scores_by_symbol: Optional[Mapping[str, Sequence[Tuple[int, float]]]] = None
    total_family_trials: Optional[int] = None
    holding_horizon_sessions: int = DEFAULT_SWING_HORIZON_SESSIONS


class CausalMarketMicrostructureEstimator:
    """Computes strictly causal `daily_atr_14` (from `1d` bars) and empirical `MarketImpactSpec`."""

    @staticmethod
    def _ny_date_from_ns(ts_ns: int) -> date:
        if ts_ns > 100_000_000_000_000_000:
            return datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc).astimezone(ET_TZ).date()
        return datetime(1970, 1, 1, tzinfo=timezone.utc).date()

    @classmethod
    def _aggregate_daily_ohlcv(
        cls,
        bars: Sequence[Bar],
        cutoff_ts_ns: Optional[int] = None,
    ) -> List[Tuple[date, float, float, float, float, float]]:
        sorted_b = sorted(
            [
                b
                for b in bars
                if cutoff_ts_ns is None or int(b.ts_init) <= int(cutoff_ts_ns)
            ],
            key=lambda b: int(b.ts_init),
        )
        if not sorted_b:
            return []
        by_day: Dict[date, List[float]] = {}
        order_days: List[date] = []
        for b in sorted_b:
            d_key = cls._ny_date_from_ns(int(b.ts_init))
            o_px = float(b.open)
            h_px = float(b.high)
            l_px = float(b.low)
            c_px = float(b.close)
            v_qty = max(0.0, float(b.volume))
            if d_key not in by_day:
                order_days.append(d_key)
                by_day[d_key] = [o_px, h_px, l_px, c_px, v_qty]
            else:
                agg = by_day[d_key]
                agg[1] = max(agg[1], h_px)
                agg[2] = min(agg[2], l_px)
                agg[3] = c_px
                agg[4] += v_qty
        return [(d, *by_day[d]) for d in order_days]

    @classmethod
    def compute_daily_atr_14(
        cls,
        primary_bars: Sequence[Bar],
        *,
        bars_1d: Optional[Sequence[Bar]] = None,
        cutoff_ts_ns: Optional[int] = None,
    ) -> Optional[Decimal]:
        """Computes `ATR_14` strictly from `1d` bars (or NY-session `1d` aggregates — NEVER `15m` bar ATR)."""
        if bars_1d:
            d_bars = sorted(
                [
                    b
                    for b in bars_1d
                    if cutoff_ts_ns is None or int(b.ts_init) <= int(cutoff_ts_ns)
                ],
                key=lambda b: int(b.ts_init),
            )
            if len(d_bars) >= 2:
                atr_val = rolling_atr(
                    [float(b.high) for b in d_bars],
                    [float(b.low) for b in d_bars],
                    [float(b.close) for b in d_bars],
                    min(14, len(d_bars)),
                )
                if math.isfinite(atr_val) and atr_val > 0.0:
                    return Decimal(f"{atr_val:.6f}")

        daily_ohlcv = cls._aggregate_daily_ohlcv(primary_bars, cutoff_ts_ns=cutoff_ts_ns)
        if len(daily_ohlcv) >= 2:
            atr_val = rolling_atr(
                [row[2] for row in daily_ohlcv],
                [row[3] for row in daily_ohlcv],
                [row[4] for row in daily_ohlcv],
                min(14, len(daily_ohlcv)),
            )
            if math.isfinite(atr_val) and atr_val > 0.0:
                return Decimal(f"{atr_val:.6f}")
        return None

    @classmethod
    def estimate_market_impact(
        cls,
        primary_bars: Sequence[Bar],
        *,
        bars_1d: Optional[Sequence[Bar]] = None,
        cutoff_ts_ns: Optional[int] = None,
        ref_px: float = 100.0,
        daily_atr_14: Optional[Decimal] = None,
    ) -> Dict[str, float]:
        """Estimates empirical `(half_spread_bps, impact_coefficient_y, adv_20_shares, daily_sigma)` strictly causally."""
        causal_bars = sorted(
            [
                b
                for b in primary_bars
                if cutoff_ts_ns is None or int(b.ts_init) <= int(cutoff_ts_ns)
            ],
            key=lambda b: int(b.ts_init),
        )
        eff_ref = ref_px if (math.isfinite(ref_px) and ref_px > 0.0) else 100.0
        if causal_bars and eff_ref <= 0.0:
            eff_ref = float(causal_bars[-1].close)

        if bars_1d:
            d_bars = sorted(
                [
                    b
                    for b in bars_1d
                    if cutoff_ts_ns is None or int(b.ts_init) <= int(cutoff_ts_ns)
                ],
                key=lambda b: int(b.ts_init),
            )
            day_vols = [max(0.0, float(b.volume)) for b in d_bars if float(b.volume) > 0.0]
        else:
            daily_ohlcv = cls._aggregate_daily_ohlcv(causal_bars, cutoff_ts_ns=cutoff_ts_ns)
            day_vols = [row[5] for row in daily_ohlcv if row[5] > 0.0]

        if day_vols:
            adv_20_shares = max(1.0, float(statistics.fmean(day_vols[-20:])))
        elif causal_bars:
            bar_vols = [max(0.0, float(b.volume)) for b in causal_bars if float(b.volume) > 0.0]
            bars_per_day = max(
                1.0,
                86_400_000_000_000.0 / float(max(1, _bar_step_ns(causal_bars[0]))) / 2.5,
            )
            adv_20_shares = max(1.0, float(statistics.fmean(bar_vols)) * bars_per_day) if bar_vols else 100_000.0
        else:
            adv_20_shares = 100_000.0

        tick_sz = float(rule_612_tick_size(Decimal(f"{max(eff_ref, 0.0):.6f}")))
        tick_half_bps = (tick_sz / (2.0 * max(eff_ref, tick_sz))) * 10_000.0

        if daily_atr_14 is not None and float(daily_atr_14) > 0.0 and eff_ref > 0.0:
            daily_sigma = float(daily_atr_14) / eff_ref
        elif len(causal_bars) >= 3:
            rets = [
                (float(causal_bars[i].close) / float(causal_bars[i - 1].close)) - 1.0
                for i in range(1, len(causal_bars))
                if float(causal_bars[i - 1].close) > 0.0
            ]
            step_ns = _bar_step_ns(causal_bars[0])
            bars_per_day = 1.0 if step_ns >= 86_400_000_000_000 else max(1.0, (6.5 * 3600.0 * 1e9) / float(step_ns))
            range_sigma = (
                max(
                    tick_sz / max(eff_ref, tick_sz),
                    (float(causal_bars[-1].high) - float(causal_bars[-1].low)) / max(eff_ref, tick_sz),
                )
                if causal_bars
                else tick_sz / max(eff_ref, tick_sz)
            )
            daily_sigma = (
                max(tick_sz / max(eff_ref, tick_sz), statistics.stdev(rets) * math.sqrt(bars_per_day))
                if len(rets) >= 2
                else range_sigma
            )
        else:
            daily_sigma = (
                max(
                    tick_sz / max(eff_ref, tick_sz),
                    (float(causal_bars[-1].high) - float(causal_bars[-1].low)) / max(eff_ref, tick_sz),
                )
                if causal_bars
                else tick_sz / max(eff_ref, tick_sz)
            )

        if causal_bars:
            rel_ranges = [
                ((float(b.high) - float(b.low)) / float(b.close)) * 10_000.0
                for b in causal_bars[-min(len(causal_bars), 260) :]
                if float(b.close) > 0.0
            ]
            step_ns = _bar_step_ns(causal_bars[0])
            bars_per_day = 1.0 if step_ns >= 86_400_000_000_000 else max(1.0, (6.5 * 3600.0 * 1e9) / float(step_ns))
            emp_half_bps = (
                0.5 * float(statistics.median(rel_ranges)) / math.sqrt(bars_per_day)
                if rel_ranges
                else tick_half_bps
            )
            half_spread_bps = max(tick_half_bps, emp_half_bps)
        else:
            half_spread_bps = tick_half_bps

        kyle_ratios: List[float] = []
        if len(causal_bars) >= 3 and daily_sigma > 0.0 and adv_20_shares > 0.0:
            for idx in range(1, len(causal_bars)):
                prev_c = float(causal_bars[idx - 1].close)
                curr_c = float(causal_bars[idx].close)
                curr_v = max(1.0, float(causal_bars[idx].volume))
                if prev_c <= 0.0:
                    continue
                abs_ret = abs(curr_c / prev_c - 1.0)
                part_sqrt = math.sqrt(curr_v / adv_20_shares)
                denom = daily_sigma * part_sqrt
                if abs_ret > 0.0 and denom > 1e-12:
                    kyle_ratios.append(abs_ret / denom)
        if kyle_ratios:
            impact_y = max(tick_half_bps * 1e-4, float(statistics.median(kyle_ratios)))
        else:
            impact_y = max(tick_half_bps * 1e-4, 1.0)

        return {
            "half_spread_bps": round(float(half_spread_bps), 4),
            "impact_coefficient_y": round(float(impact_y), 4),
            "adv_20_shares": round(float(adv_20_shares), 2),
            "daily_sigma": round(float(daily_sigma), 6),
        }


class ConditionalSwingEventExtractor:
    """Extracts chronological conditional Top-K swing event windows `[tau, tau + h]` strictly causally."""

    @staticmethod
    def _causal_archetype_score(
        bars_before: Sequence[Bar],
        archetype_id: StrategyArchetypeId,
    ) -> float:
        if len(bars_before) < 6:
            return 0.0
        closes = [float(b.close) for b in bars_before[-60:]]
        highs = [float(b.high) for b in bars_before[-60:]]
        lows = [float(b.low) for b in bars_before[-60:]]
        vols = [float(b.volume) for b in bars_before[-60:]]
        if archetype_id == StrategyArchetypeId.TREND_MOMENTUM:
            fast_n = min(5, len(closes))
            slow_n = min(15, len(closes))
            scale = max(rolling_std(closes[-slow_n:]), 1e-6)
            slope_diff = rolling_linear_slope(closes[-fast_n:]) - rolling_linear_slope(closes[-slow_n:])
            return ((rolling_mean(closes[-fast_n:]) - rolling_mean(closes[-slow_n:])) + slope_diff * fast_n) / scale
        if archetype_id == StrategyArchetypeId.VOLATILITY_BREAKOUT:
            win_n = min(len(closes), len(vols), 20)
            q_px = empirical_rank_quantile(closes[-win_n:], closes[-1])
            q_vol = empirical_rank_quantile(vols[-win_n:], vols[-1])
            return q_px + q_vol
        # MEAN_REVERSION: higher score for deeper oversold residual
        slow_n = min(15, len(closes))
        typ = [(h + lo + c) / 3.0 for h, lo, c in zip(highs[-slow_n:], lows[-slow_n:], closes[-slow_n:])]
        anchor = rolling_vwap(typ, vols[-slow_n:], slow_n)
        res = [c - anchor for c in closes[-min(len(closes), 20) :]]
        mad_z = rolling_mad_zscore(res, closes[-1] - anchor)
        return -mad_z

    @classmethod
    def extract_events(
        cls,
        *,
        primary_bars_by_symbol: Mapping[str, Sequence[Bar]],
        archetype_id: StrategyArchetypeId,
        warmup_bars_min: int,
        horizon_sessions: int = DEFAULT_SWING_HORIZON_SESSIONS,
        alpha_scores_by_symbol: Optional[Mapping[str, Sequence[Tuple[int, float]]]] = None,
        min_required_events: int = MIN_POOLED_REGIME_SAMPLE_TRADES,
    ) -> List[ConditionalSwingEvent]:
        if not primary_bars_by_symbol:
            return []

        # Partition each symbol's primary bars into chronological session buckets
        session_bars_by_sym: Dict[str, List[Tuple[int, int, List[Bar]]]] = {}
        all_session_keys: set[date] = set()
        for sym, seq in primary_bars_by_symbol.items():
            sorted_seq = sorted(seq, key=lambda b: int(b.ts_init))
            if not sorted_seq:
                continue
            by_date: Dict[date, List[Bar]] = {}
            for b in sorted_seq:
                d_key = CausalMarketMicrostructureEstimator._ny_date_from_ns(int(b.ts_init))
                by_date.setdefault(d_key, []).append(b)
                all_session_keys.add(d_key)
            if len(by_date) >= 4:
                sess_list = [
                    (int(by_date[d][0].ts_init), int(by_date[d][-1].ts_init), by_date[d])
                    for d in sorted(by_date.keys())
                ]
            else:
                step_b = max(4, min(26, max(4, len(sorted_seq) // 12)))
                sess_list = [
                    (int(sorted_seq[i].ts_init), int(sorted_seq[min(len(sorted_seq) - 1, i + step_b - 1)].ts_init), sorted_seq[i : i + step_b])
                    for i in range(0, len(sorted_seq), step_b)
                ]
            session_bars_by_sym[sym] = sess_list

        if not session_bars_by_sym:
            return []

        alpha_lookup: Dict[str, List[Tuple[int, float]]] = {}
        if alpha_scores_by_symbol:
            for sym, seq in alpha_scores_by_symbol.items():
                norm_s = str(sym).strip().upper().split(".", 1)[0]
                alpha_lookup[norm_s] = sorted(
                    [(int(ts), float(sc)) for ts, sc in seq if math.isfinite(float(sc))],
                    key=lambda item: item[0],
                )

        # Align decision sessions on a common ordered timeline
        all_sess_starts: List[int] = sorted(
            {
                sess[0]
                for sess_list in session_bars_by_sym.values()
                for sess in sess_list
            }
        )
        if len(all_sess_starts) < 3:
            return []

        max_sess_len = max(len(v) for v in session_bars_by_sym.values())
        eff_h = min(max(1, int(horizon_sessions)), max(1, max_sess_len // 4))

        # Estimate warmup sessions needed before first event
        sample_sym_sess = next(iter(session_bars_by_sym.values()))
        avg_bars_per_sess = max(1, int(round(sum(len(s[2]) for s in sample_sym_sess) / max(1, len(sample_sym_sess)))))
        warmup_sess = min(
            max(1, len(all_sess_starts) // 4),
            max(1, math.ceil(max(2, int(warmup_bars_min)) / float(avg_bars_per_sess))),
        )

        # Index symbol bars up to each timestamp for causal scoring
        sym_all_bars: Dict[str, List[Bar]] = {
            sym: [b for _, _, bl in sess_list for b in bl]
            for sym, sess_list in session_bars_by_sym.items()
        }
        sym_ts_index: Dict[str, List[int]] = {
            sym: [int(b.ts_init) for b in bl]
            for sym, bl in sym_all_bars.items()
        }

        import bisect

        session_candidates: List[List[ConditionalSwingEvent]] = []
        for s_idx in range(warmup_sess, max(warmup_sess + 1, len(all_sess_starts) - eff_h)):
            tau_ns = all_sess_starts[s_idx]
            day_scored: List[Tuple[float, str, int, int]] = []
            for sym, sess_list in session_bars_by_sym.items():
                # Find session starting at or right around tau_ns and ending eff_h sessions later
                sym_starts = [s[0] for s in sess_list]
                pos = bisect.bisect_left(sym_starts, tau_ns)
                if pos >= len(sess_list) or abs(sess_list[pos][0] - tau_ns) > 86_400_000_000_000:
                    continue
                end_pos = min(len(sess_list) - 1, pos + eff_h - 1)
                if end_pos < pos:
                    continue
                win_start = sess_list[pos][0]
                win_end = sess_list[end_pos][1]

                bar_cut = bisect.bisect_left(sym_ts_index[sym], win_start)
                if bar_cut < 6:
                    continue
                if sym in alpha_lookup and alpha_lookup[sym]:
                    a_seq = alpha_lookup[sym]
                    a_pos = bisect.bisect_left([x[0] for x in a_seq], win_start) - 1
                    score = a_seq[a_pos][1] if a_pos >= 0 else 0.0
                else:
                    score = cls._causal_archetype_score(sym_all_bars[sym][:bar_cut], archetype_id)
                day_scored.append((score, sym, win_start, win_end))

            if not day_scored:
                continue
            day_scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
            session_candidates.append(
                [
                    ConditionalSwingEvent(
                        symbol=sym,
                        start_ns=ws,
                        end_ns=we,
                        horizon_sessions=eff_h,
                        score=sc,
                    )
                    for sc, sym, ws, we in day_scored
                ]
            )

        if not session_candidates:
            return []

        n_sessions = len(session_candidates)
        max_avail = max(len(day_list) for day_list in session_candidates)
        # Default Top-K = top quartile capped at 4 (matching L1 HARD_MAX_CANDIDATES)
        k_top = min(max_avail, max(1, min(4, math.ceil(max_avail * 0.25))))
        target_total_with_is = math.ceil(float(min_required_events) / 0.70)
        if k_top * n_sessions < target_total_with_is and max_avail > k_top:
            # Expand strictly within the top half of the cross-section if needed to meet OOS depth
            k_cap = max(k_top, max_avail // 2)
            k_top = min(k_cap, max(k_top, math.ceil(target_total_with_is / max(1, n_sessions))))

        extracted: List[ConditionalSwingEvent] = []
        for day_list in session_candidates:
            extracted.extend(day_list[:k_top])
        extracted.sort(key=lambda ev: (ev.start_ns, ev.symbol))
        return extracted


def _run_panel_cell_calibration_task(
    task: Mapping[str, Any],
) -> Tuple[VolBucket, LiqBucket, str, StrategyArchetypeId, Optional[FrozenRegimeParamRecord]]:
    """Spawned worker entrypoint for parallel 3x2 `(vol_bucket, liq_bucket, archetype_id)` cell calibration."""
    runner_cfg = task["runner_config"]
    worker_runner = IsomorphicBacktestRunner(
        dsn=None,
        starting_capital_usd=runner_cfg["starting_capital_usd"],
        effective_trials=runner_cfg["effective_trials"],
        venue_name=task["venue_name"],
        fee_schedule=runner_cfg["fee_schedule"],
        market_impact_spec=runner_cfg["market_impact_spec"],
        half_spread_bps=runner_cfg["explicit_half_spread_bps"],
        impact_coefficient_y=runner_cfg["explicit_impact_y"],
        system_config=runner_cfg["system_config"],
    )
    worker_falsifier = StressFalsifier(
        worker_runner,
        max_missed_fill_rate=task["max_missed_fill_rate"],
    )
    worker_calibrator = OfflineRegimePanelCalibrator(
        dsn=None,
        runner=worker_runner,
        falsifier=worker_falsifier,
        param_table=None,
        min_pooled_sample_trades=task["min_pooled_sample_trades"],
        max_missed_fill_rate=task["max_missed_fill_rate"],
        venue_name=task["venue_name"],
    )
    res = worker_calibrator.calibrate_regime_cell(
        vol_bucket=task["vol_bucket"],
        liq_bucket=task["liq_bucket"],
        dominant_style=task["arch_id"],
        symbol_bars=task["symbol_bars"],
        candidate_param_grid=task["candidate_param_grid"],
        raw_low_by_symbol_ts_ns=task["raw_low_by_symbol_ts_ns"],
        persist=False,
        execution_spec_grid=task["execution_spec_grid"],
        bars_1m_by_symbol=task["bars_1m_by_symbol"],
        bars_1d_by_symbol=task["bars_1d_by_symbol"],
        alpha_scores_by_symbol=task["alpha_scores_by_symbol"],
        total_family_trials=task["total_family_trials"],
        holding_horizon_sessions=task["holding_horizon_sessions"],
    )
    return (
        res.vol_bucket,
        res.liq_bucket,
        res.dominant_style,
        res.archetype_id,
        res.selected_record,
    )


class OfflineRegimePanelCalibrator:
    """Stage 2.4 Offline 3x2 Pooled Regime Panel Calibrator.

    Evaluates archetype parameter candidates across pooled Top-5% swing windows
    within each `(VolBucket, LiqBucket, dominant_style)` cell using
    `IsomorphicBacktestRunner` and `StressFalsifier` (4 counterfactual stress gates
    including `missed_fill_rate <= 0.35` and `raw_low` stop triggers), persisting
    surviving frozen parameter sets to PostgreSQL `frozen_regime_param_table`.
    """

    def __init__(
        self,
        dsn: Optional[str] = None,
        *,
        runner: Optional[IsomorphicBacktestRunner] = None,
        falsifier: Optional[StressFalsifier] = None,
        param_table: Optional[FrozenRegimeParamTable] = None,
        min_pooled_sample_trades: int = MIN_POOLED_REGIME_SAMPLE_TRADES,
        max_missed_fill_rate: float = MAX_LIMIT_COLLAR_MISSED_FILL_RATE,
        venue_name: str = "ALPACA",
    ) -> None:
        self._dsn = resolve_database_dsn(dsn) if dsn is not None else None
        self._runner = runner or IsomorphicBacktestRunner(dsn=self._dsn, venue_name=venue_name)
        self._falsifier = falsifier or StressFalsifier(
            self._runner,
            max_missed_fill_rate=max_missed_fill_rate,
        )
        self._param_table = param_table or (
            FrozenRegimeParamTable(dsn=self._dsn) if self._dsn is not None else None
        )
        self._min_pooled_sample_trades = max(1, int(min_pooled_sample_trades))
        self._max_missed_fill_rate = min(1.0, max(0.0, float(max_missed_fill_rate)))
        self._venue_name = str(venue_name or "ALPACA").strip().upper()
        self._pending_panel_context: Optional[_PanelCalibrationContext] = None

    @property
    def param_table(self) -> Optional[FrozenRegimeParamTable]:
        return self._param_table

    def _clear_runner_last_refs(self) -> None:
        """Clears retained disposed PyO3/actor references on `_runner` after calibration."""
        seen: set[int] = set()
        for runner_obj in (self._runner, getattr(self._falsifier, "_runner", None)):
            if runner_obj is None or id(runner_obj) in seen:
                continue
            seen.add(id(runner_obj))
            for attr in (
                "_last_plan_gate_backend",
                "_last_oco_monitor",
                "_last_telemetry_actor",
                "_last_strategy",
            ):
                if hasattr(runner_obj, attr):
                    setattr(runner_obj, attr, None)

    def _build_calibration_plan_item(
        self,
        *,
        symbol: str,
        archetype_id: StrategyArchetypeId,
        params: Mapping[str, Any],
        bars: Sequence[Bar],
        bars_1d: Optional[Sequence[Bar]] = None,
        execution_spec: Optional[ExecutionSpec | Mapping[str, Any]] = None,
        cutoff_ts_ns: Optional[int] = None,
    ) -> Tuple[PlanItemContract, Decimal]:
        sym = str(symbol).strip().upper().split(".", 1)[0]
        inst_id = str(params.get("instrument_id") or f"{sym}.{self._venue_name}").strip()
        bar_tf = str(params.get("bar_timeframe", "15m"))
        lookback_window = int(params.get("lookback_window", 20))
        fast_window = int(params.get("fast_window", 5))
        slow_window = int(params.get("slow_window", 15))
        entry_q = Decimal(str(params.get("entry_quantile_threshold", "0.80")))
        exit_q = Decimal(str(params.get("exit_quantile_threshold", "0.35")))
        atr_mult = Decimal(str(params.get("atr_stop_multiplier", "2.00")))
        collar_atr = Decimal(
            str(
                params.get(
                    "limit_collar_atr_mult",
                    params.get(
                        "limit_collar_atr",
                        default_limit_collar_atr_for_archetype(archetype_id),
                    ),
                )
            )
        )
        collar_pct = Decimal(str(params.get("limit_collar_pct", _default_limit_collar_pct())))

        if execution_spec is not None:
            resolved_exec_spec = (
                execution_spec
                if isinstance(execution_spec, ExecutionSpec)
                else ExecutionSpec.from_dict(execution_spec, default_bar_timeframe=bar_tf)
            )
        elif params.get("execution_spec") is not None:
            raw_es = params["execution_spec"]
            resolved_exec_spec = (
                raw_es
                if isinstance(raw_es, ExecutionSpec)
                else ExecutionSpec.from_dict(raw_es, default_bar_timeframe=bar_tf)
            )
        else:
            resolved_exec_spec = ExecutionSpec(bar_timeframe=bar_tf)

        strat_cfg = ArchetypeStrategyConfig(
            archetype_id=archetype_id,
            instrument_id=inst_id,
            bar_timeframe=bar_tf,
            lookback_window=lookback_window,
            fast_window=fast_window,
            slow_window=slow_window,
            entry_quantile_threshold=entry_q,
            exit_quantile_threshold=exit_q,
            atr_stop_multiplier=atr_mult,
        )

        primary_step_ns = _TF_BAR_SPECS.get(bar_tf.strip().lower(), _TF_BAR_SPECS["15m"])[2]
        sorted_all = sorted(bars, key=lambda b: int(b.ts_init))
        primary_all = [b for b in sorted_all if _bar_step_ns(b) == primary_step_ns] or sorted_all
        if cutoff_ts_ns is not None:
            causal_primary = [b for b in primary_all if int(b.ts_init) <= int(cutoff_ts_ns)]
            if not causal_primary:
                causal_primary = primary_all[:1]
            # Anchor on trailing causal slice right before event cutoff
            init_n = min(max(lookback_window, 14), len(causal_primary))
            anchor_slice = causal_primary[-init_n:]
            eff_cutoff_ts_ns: Optional[int] = (
                int(cutoff_ts_ns)
                if any(int(b.ts_init) <= int(cutoff_ts_ns) for b in primary_all)
                else (int(anchor_slice[-1].ts_init) if anchor_slice else int(cutoff_ts_ns))
            )
            causal_est_bars = causal_primary
        else:
            causal_primary = primary_all
            # Anchor sizing and stop loss strictly on the initial causal warmup slice so later
            # prices never leak into the plan envelope and `initial_stop_price` cannot sit
            # artificially below the full-window low.
            init_n = min(max(lookback_window, 14), len(causal_primary))
            anchor_slice = causal_primary[:init_n]
            eff_cutoff_ts_ns = int(anchor_slice[-1].ts_init) if anchor_slice else None
            causal_est_bars = anchor_slice

        init_closes = [float(b.close) for b in anchor_slice]
        init_highs = [float(b.high) for b in anchor_slice]
        init_lows = [float(b.low) for b in anchor_slice]
        ref_px = init_closes[-1] if init_closes else 100.0
        tick_floor = float(rule_612_tick_size(Decimal(f"{max(ref_px, 0.0):.6f}")))
        px_prec = 4 if tick_floor < float(PENNY_TICK_RULE_612) else 2

        daily_atr_14 = CausalMarketMicrostructureEstimator.compute_daily_atr_14(
            causal_est_bars,
            bars_1d=bars_1d,
            cutoff_ts_ns=eff_cutoff_ts_ns,
        )
        if len(init_closes) >= 2:
            emp_atr = rolling_atr(init_highs, init_lows, init_closes, min(14, len(init_closes)))
        elif init_highs and init_lows:
            emp_atr = max(init_highs[0] - init_lows[0], tick_floor)
        else:
            emp_atr = tick_floor

        impact_meta = CausalMarketMicrostructureEstimator.estimate_market_impact(
            causal_est_bars,
            bars_1d=bars_1d,
            cutoff_ts_ns=eff_cutoff_ts_ns,
            ref_px=ref_px,
            daily_atr_14=daily_atr_14,
        )

        sys_cfg = SystemConfig()
        env_num = load_envelope().numeric
        notional_bounds = env_num["risk.max_order_notional_usd"]
        min_notional_usd = float(notional_bounds["min"])
        max_notional_usd = float(notional_bounds["max"])
        min_stop_frac = float(env_num["risk.risk_per_trade_pct"]["min"]) / 100.0
        risk_pct_cap = float(env_num["risk.risk_per_trade_pct"]["max"]) / 100.0
        single_name_pct_cap = float(env_num["risk.max_single_name_pct"]["max"]) / 100.0
        starting_equity = float(getattr(self._runner, "_starting_capital_usd", 100_000.0))

        use_daily_atr_for_sizing = (
            daily_atr_14 is not None
            and (
                bars_1d is not None
                or cutoff_ts_ns is not None
                or primary_step_ns >= 86_400_000_000_000
            )
        )
        eff_atr = max(
            float(daily_atr_14) if use_daily_atr_for_sizing else emp_atr,
            tick_floor,
        )
        stop_dist = max(float(atr_mult) * eff_atr, ref_px * min_stop_frac, tick_floor)
        stop_px = (
            max(tick_floor, min(ref_px - tick_floor, ref_px - stop_dist))
            if ref_px > 2.0 * tick_floor
            else tick_floor
        )

        max_pos_usd = min(max_notional_usd, starting_equity * single_name_pct_cap)
        target_pos_usd = min(
            max_pos_usd,
            float(sys_cfg.risk.max_order_notional_usd),
            starting_equity * (float(sys_cfg.risk.max_single_name_pct) / 100.0),
        )
        unit_risk_usd = min(
            target_pos_usd,
            starting_equity * (float(sys_cfg.risk.risk_per_trade_pct) / 100.0),
        )
        if params.get("max_risk_budget_usd") is not None:
            unit_risk_usd = min(unit_risk_usd, float(params["max_risk_budget_usd"]))

        if params.get("target_qty") is not None:
            target_qty = Decimal(str(params["target_qty"]))
        else:
            min_qty_for_notional = max(
                1,
                math.ceil(min_notional_usd / max(ref_px, tick_floor)),
            )
            max_qty_for_pos = max(
                1,
                int(max_pos_usd // max(ref_px, tick_floor)),
            )
            risk_sized_qty = int(
                min(
                    unit_risk_usd / max(stop_dist, tick_floor),
                    target_pos_usd / max(ref_px, tick_floor),
                )
            )
            target_qty = Decimal(
                str(max(1, min(max_qty_for_pos, max(min_qty_for_notional, risk_sized_qty))))
            )

        raw_notional = float(target_qty) * max(ref_px, tick_floor)
        bounded_notional = min(
            max_pos_usd,
            max(min_notional_usd, raw_notional),
        )
        target_notional = Decimal(f"{bounded_notional:.2f}")
        default_risk_cap = min(bounded_notional, starting_equity * risk_pct_cap)
        max_risk_budget = (
            Decimal(str(params["max_risk_budget_usd"]))
            if params.get("max_risk_budget_usd") is not None
            else min(target_notional, Decimal(f"{default_risk_cap:.2f}"))
        )
        take_profit_dec = (
            Decimal(str(params["take_profit_price"]))
            if params.get("take_profit_price") is not None
            else None
        )
        last_ts_ns = int(sorted_all[-1].ts_init) if sorted_all else 0
        valid_until = (
            datetime.fromtimestamp(last_ts_ns / 1e9, tz=timezone.utc) + timedelta(days=5)
            if last_ts_ns > 100_000_000_000_000_000
            else datetime.now(tz=timezone.utc) + timedelta(days=365)
        )
        env = PlanRiskEnvelope(
            side=PlanSide.LONG,
            target_qty=target_qty,
            approved_qty_cap=target_qty,
            target_notional_usd=target_notional,
            max_risk_budget_usd=max_risk_budget,
            initial_stop_price=Decimal(f"{stop_px:.{px_prec}f}"),
            take_profit_price=take_profit_dec,
            collar=MarketableLimitCollar(
                limit_collar_pct=collar_pct,
                limit_collar_atr=collar_atr,
            ),
            valid_until=valid_until,
            daily_atr_14=daily_atr_14,
            execution_spec=resolved_exec_spec,
        )
        item = PlanItemContract(
            item_id=uuid.uuid4(),
            plan_id=uuid.uuid4(),
            symbol=sym,
            venue=self._venue_name,
            broker="alpaca",
            asset_class="US_EQUITY",
            strategy_id=f"{archetype_id.value}-CAL-{sym}",
            strategy_config=strat_cfg,
            risk_envelope=env,
            sizing_trace={
                "calibration": "offline_3x2_regime_panel",
                **impact_meta,
            },
            daily_atr_14=daily_atr_14,
            execution_spec=resolved_exec_spec,
        )
        return item, collar_atr

    def calibrate_regime_cell(
        self,
        *,
        vol_bucket: VolBucket | str,
        liq_bucket: LiqBucket | str,
        dominant_style: str | StrategyArchetypeId,
        symbol_bars: Mapping[str, Sequence[Bar]] | Sequence[Bar],
        candidate_param_grid: Optional[Sequence[Mapping[str, Any]]] = None,
        raw_low_by_symbol_ts_ns: Optional[Mapping[str, Mapping[int, float]]] = None,
        persist: bool = True,
        execution_spec_grid: Optional[Sequence[ExecutionSpec | Mapping[str, Any]]] = None,
        bars_1m_by_symbol: Optional[Mapping[str, Sequence[Bar]]] = None,
        bars_1d_by_symbol: Optional[Mapping[str, Sequence[Bar]]] = None,
        alpha_scores_by_symbol: Optional[Mapping[str, Sequence[Tuple[int, float]]]] = None,
        conditional_events: Optional[Sequence[ConditionalSwingEvent | Tuple[Any, ...]]] = None,
        total_family_trials: Optional[int] = None,
        holding_horizon_sessions: int = DEFAULT_SWING_HORIZON_SESSIONS,
    ) -> RegimeCellCalibrationResult:
        """Calibrates a single `(vol_bucket, liq_bucket, dominant_style)` cell and persists the winner."""
        vb = (
            vol_bucket
            if isinstance(vol_bucket, VolBucket)
            else VolBucket(str(vol_bucket).strip().upper())
        )
        lb = (
            liq_bucket
            if isinstance(liq_bucket, LiqBucket)
            else LiqBucket(str(liq_bucket).strip().upper())
        )
        arch_id = resolve_archetype_for_dominant_style(dominant_style)
        style_key = (
            dominant_style.value
            if isinstance(dominant_style, StrategyArchetypeId)
            else str(dominant_style).strip().upper()
        )

        if isinstance(symbol_bars, Mapping):
            bars_by_symbol: Dict[str, List[Bar]] = {
                str(sym).strip().upper().split(".", 1)[0]: list(b_seq)
                for sym, b_seq in symbol_bars.items()
                if b_seq
            }
        else:
            bar_list = list(symbol_bars)
            inferred_sym = (
                str(bar_list[0].bar_type.instrument_id).split(".", 1)[0].strip().upper()
                if bar_list
                else "SPY"
            )
            bars_by_symbol = {inferred_sym: bar_list} if bar_list else {}

        if not bars_by_symbol:
            raise ValueError(
                f"Cannot calibrate regime cell ({vb.value}, {lb.value}, {style_key}): empty symbol_bars"
            )

        grid = list(
            candidate_param_grid
            if candidate_param_grid is not None
            else DEFAULT_REGIME_CANDIDATE_GRIDS[arch_id]
        )
        if not grid:
            raise ValueError(f"Empty candidate_param_grid for {arch_id.value}")

        pending_ctx = self._pending_panel_context
        eff_exec_grid = execution_spec_grid or (
            pending_ctx.execution_spec_grid if pending_ctx is not None else None
        )
        eff_bars_1m = bars_1m_by_symbol or (
            pending_ctx.bars_1m_by_symbol if pending_ctx is not None else None
        )
        eff_bars_1d = bars_1d_by_symbol or (
            pending_ctx.bars_1d_by_symbol if pending_ctx is not None else None
        )
        eff_alpha = alpha_scores_by_symbol or (
            pending_ctx.alpha_scores_by_symbol if pending_ctx is not None else None
        )
        eff_family_trials = total_family_trials if total_family_trials is not None else (
            pending_ctx.total_family_trials if pending_ctx is not None else None
        )
        eff_horizon_sess = (
            pending_ctx.holding_horizon_sessions
            if pending_ctx is not None and holding_horizon_sessions == DEFAULT_SWING_HORIZON_SESSIONS
            else holding_horizon_sessions
        )

        has_custom_exec_in_grid = any("execution_spec" in cand for cand in grid)
        use_legacy_direct_grid = (
            not hasattr(self._falsifier, "run_conditional_swing_falsification")
            or (
                candidate_param_grid is not None
                and eff_exec_grid is None
                and conditional_events is None
                and eff_bars_1m is None
                and eff_bars_1d is None
                and eff_alpha is None
                and pending_ctx is None
                and not has_custom_exec_in_grid
            )
        )
        if not use_legacy_direct_grid:
            return self._calibrate_cell_walk_forward_counterfactual(
                vb=vb,
                lb=lb,
                style_key=style_key,
                arch_id=arch_id,
                bars_by_symbol=bars_by_symbol,
                grid=grid,
                raw_low_by_symbol_ts_ns=raw_low_by_symbol_ts_ns,
                persist=persist,
                execution_spec_grid=eff_exec_grid,
                bars_1m_by_symbol=eff_bars_1m,
                bars_1d_by_symbol=eff_bars_1d,
                alpha_scores_by_symbol=eff_alpha,
                conditional_events=conditional_events,
                total_family_trials=eff_family_trials,
                holding_horizon_sessions=eff_horizon_sess,
            )

        evaluations: List[RegimeCandidateEvaluation] = []
        effective_trials = max(1, len(grid))

        try:
            for raw_params in grid:
                params = dict(raw_params)
                reports: List[StressFalsificationReport] = []
                plan_items_by_sym: Dict[str, PlanItemContract] = {}
                raw_filled_trades = 0
                resolved_collar_atr = default_limit_collar_atr_for_archetype(arch_id)

                for sym, sym_bars in bars_by_symbol.items():
                    sym_1d = eff_bars_1d.get(sym) if eff_bars_1d is not None else None
                    plan_item, resolved_collar_atr = self._build_calibration_plan_item(
                        symbol=sym,
                        archetype_id=arch_id,
                        params=params,
                        bars=sym_bars,
                        bars_1d=sym_1d,
                    )
                    plan_items_by_sym[sym] = plan_item
                    sym_raw_lows = (
                        raw_low_by_symbol_ts_ns.get(sym)
                        if raw_low_by_symbol_ts_ns is not None
                        else None
                    )
                    report = self._falsifier.run_falsification(
                        plan_item,
                        bars=sym_bars,
                        raw_low_by_ts_ns=sym_raw_lows,
                        effective_trials=effective_trials,
                    )
                    reports.append(report)
                    raw_filled_trades += max(
                        int(report.baseline_metrics.total_positions),
                        int(report.baseline_metrics.filled_orders) // 2,
                        1 if report.baseline_metrics.filled_orders > 0 else 0,
                    )

                eff_pooled_trades = int(raw_filled_trades)
                failures, sharpes, dsrs, missed_rates, expectancies_r = self._evaluate_cell_reports(
                    sym_count=len(bars_by_symbol),
                    reports=reports,
                    plan_items_by_sym=plan_items_by_sym,
                )

                if eff_pooled_trades < self._min_pooled_sample_trades:
                    failures.append(
                        f"Insufficient pooled_sample_trades={eff_pooled_trades} < {self._min_pooled_sample_trades}"
                    )

                avg_missed_rate = (
                    statistics.fmean(missed_rates) if missed_rates else 1.0
                )
                if avg_missed_rate > self._max_missed_fill_rate + 1e-9:
                    failures.append(
                        f"Pooled missed_fill_rate={avg_missed_rate:.4f} > {self._max_missed_fill_rate:.2f}"
                    )

                avg_sharpe = statistics.fmean(sharpes) if sharpes else 0.0
                avg_dsr = statistics.fmean(dsrs) if dsrs else 0.0
                avg_expectancy_r = (
                    statistics.fmean(expectancies_r) if expectancies_r else 0.0
                )
                passed = len(failures) == 0

                frozen_rec: Optional[FrozenRegimeParamRecord] = None
                if passed:
                    bar_tf = str(params.get("bar_timeframe", "15m"))
                    rec_exec_spec = ExecutionSpec(bar_timeframe=bar_tf)
                    frozen_rec = FrozenRegimeParamRecord(
                        vol_bucket=vb,
                        liq_bucket=lb,
                        dominant_style=style_key,
                        archetype_id=arch_id,
                        bar_timeframe=bar_tf,
                        lookback_window=int(params.get("lookback_window", 20)),
                        fast_window=int(params.get("fast_window", 5)),
                        slow_window=int(params.get("slow_window", 15)),
                        entry_quantile_threshold=Decimal(
                            str(params.get("entry_quantile_threshold", "0.80"))
                        ).quantize(Decimal("0.0001")),
                        exit_quantile_threshold=Decimal(
                            str(params.get("exit_quantile_threshold", "0.35"))
                        ).quantize(Decimal("0.0001")),
                        atr_stop_multiplier=Decimal(
                            str(params.get("atr_stop_multiplier", "2.00"))
                        ).quantize(Decimal("0.0001")),
                        limit_collar_atr_mult=Decimal(str(resolved_collar_atr)).quantize(
                            Decimal("0.0001")
                        ),
                        limit_collar_pct=Decimal(
                            str(params.get("limit_collar_pct", _default_limit_collar_pct()))
                        ).quantize(Decimal("0.0001")),
                        pooled_sample_trades=int(eff_pooled_trades),
                        pooled_expectancy_r=Decimal(f"{avg_expectancy_r:.4f}"),
                        pooled_sharpe=Decimal(f"{avg_sharpe:.4f}"),
                        pooled_dsr=Decimal(f"{avg_dsr:.6f}"),
                        stress_passed=True,
                        stress_missed_fill_rate=Decimal(f"{avg_missed_rate:.4f}"),
                        stress_report_json={
                            "symbol_count": len(bars_by_symbol),
                            "active_symbol_count": sum(
                                1 for r in reports if r.baseline_metrics.filled_orders > 0
                            ),
                            "bar_timeframe": bar_tf,
                            "execution_spec": rec_exec_spec.to_dict(),
                            "reports": [r.to_dict() for r in reports],
                        },
                        calibrated_at=datetime.now(tz=timezone.utc),
                        execution_spec=rec_exec_spec,
                    )

                evaluations.append(
                    RegimeCandidateEvaluation(
                        vol_bucket=vb,
                        liq_bucket=lb,
                        dominant_style=style_key,
                        archetype_id=arch_id,
                        params=params,
                        passed=passed,
                        pooled_sample_trades=eff_pooled_trades,
                        pooled_sharpe=avg_sharpe,
                        pooled_dsr=avg_dsr,
                        stress_missed_fill_rate=avg_missed_rate,
                        failure_reasons=tuple(failures),
                        stress_reports=tuple(reports),
                        frozen_record=frozen_rec,
                    )
                )
        finally:
            self._clear_runner_last_refs()

        survivors = [ev for ev in evaluations if ev.passed and ev.frozen_record is not None]
        survivors.sort(
            key=lambda ev: (
                ev.pooled_dsr,
                ev.pooled_sharpe,
                -ev.stress_missed_fill_rate,
            ),
            reverse=True,
        )
        selected = survivors[0].frozen_record if survivors else None
        persisted = False
        if persist and self._param_table is not None:
            if selected is not None:
                self._param_table.upsert_record(selected)
                persisted = True
            elif hasattr(self._param_table, "deactivate_cell"):
                self._param_table.deactivate_cell(vb, lb, style_key)

        return RegimeCellCalibrationResult(
            vol_bucket=vb,
            liq_bucket=lb,
            dominant_style=style_key,
            archetype_id=arch_id,
            selected_record=selected,
            evaluated_candidates=tuple(evaluations),
            persisted=persisted,
        )

    def _calibrate_cell_walk_forward_counterfactual(
        self,
        *,
        vb: VolBucket,
        lb: LiqBucket,
        style_key: str,
        arch_id: StrategyArchetypeId,
        bars_by_symbol: Mapping[str, Sequence[Bar]],
        grid: Sequence[Mapping[str, Any]],
        raw_low_by_symbol_ts_ns: Optional[Mapping[str, Mapping[int, float]]],
        persist: bool,
        execution_spec_grid: Optional[Sequence[ExecutionSpec | Mapping[str, Any]]],
        bars_1m_by_symbol: Optional[Mapping[str, Sequence[Bar]]],
        bars_1d_by_symbol: Optional[Mapping[str, Sequence[Bar]]],
        alpha_scores_by_symbol: Optional[Mapping[str, Sequence[Tuple[int, float]]]],
        conditional_events: Optional[Sequence[ConditionalSwingEvent | Tuple[Any, ...]]],
        total_family_trials: Optional[int],
        holding_horizon_sessions: int,
    ) -> RegimeCellCalibrationResult:
        """Executes 3x2 Walk-Forward counterfactual calibration on conditional Top-K swing events vs `H_0`."""
        bar_tf = str(grid[0].get("bar_timeframe", "15m")).strip() or "15m"
        primary_step_ns = _TF_BAR_SPECS.get(bar_tf.lower(), _TF_BAR_SPECS["15m"])[2]

        # Build separated primary (15m), dual-stream (15m+1m), and daily (1d) bars per symbol
        primary_bars_map: Dict[str, List[Bar]] = {}
        dual_bars_map: Dict[str, List[Bar]] = {}
        daily_bars_map: Dict[str, List[Bar]] = {}

        for sym, seq in bars_by_symbol.items():
            norm_sym = str(sym).strip().upper().split(".", 1)[0]
            all_b: List[Bar] = list(seq)
            if bars_1m_by_symbol and norm_sym in bars_1m_by_symbol:
                all_b.extend(bars_1m_by_symbol[norm_sym])
            prim = [b for b in all_b if _bar_step_ns(b) == primary_step_ns]
            if not prim:
                prim = [b for b in seq if _bar_step_ns(b) != 60_000_000_000] or list(seq)
            d_bars = [b for b in all_b if _bar_step_ns(b) == 86_400_000_000_000 and primary_step_ns != 86_400_000_000_000]
            if bars_1d_by_symbol and norm_sym in bars_1d_by_symbol:
                d_bars = list(bars_1d_by_symbol[norm_sym])
            if d_bars:
                daily_bars_map[norm_sym] = sorted(d_bars, key=lambda b: int(b.ts_init))
            primary_bars_map[norm_sym] = sorted(prim, key=lambda b: int(b.ts_init))
            dual_bars_map[norm_sym] = _sort_bars_deterministic(
                [b for b in all_b if _bar_step_ns(b) in (primary_step_ns, 60_000_000_000)] or prim
            )

        # Normalize ExecutionSpec grid, always anchoring H0 = FIRST_WINDOW_IMMEDIATE x COLLARED_TWAP
        h0_spec = ExecutionSpec(
            trigger_policy=TriggerPolicy.FIRST_WINDOW_IMMEDIATE,
            exec_algorithm=ExecAlgorithm.COLLARED_TWAP,
            window_quantile=None,
            bar_timeframe=bar_tf,
        )
        raw_exec_list: List[ExecutionSpec] = [h0_spec]
        for es in (execution_spec_grid or DEFAULT_EXECUTION_SPEC_GRID):
            norm_es = (
                replace(es, bar_timeframe=bar_tf)
                if isinstance(es, ExecutionSpec)
                else ExecutionSpec.from_dict(es, default_bar_timeframe=bar_tf)
            )
            if norm_es not in raw_exec_list:
                raw_exec_list.append(norm_es)

        # Extract or normalize conditional Top-K swing events [tau, tau + h]
        events: List[ConditionalSwingEvent] = []
        if conditional_events is not None:
            for item_ev in conditional_events:
                if isinstance(item_ev, ConditionalSwingEvent):
                    ev_sym = item_ev.symbol.strip().upper().split(".", 1)[0]
                    if ev_sym in dual_bars_map and item_ev.end_ns >= item_ev.start_ns:
                        events.append(replace(item_ev, symbol=ev_sym))
                elif isinstance(item_ev, (tuple, list)):
                    if len(item_ev) == 3:
                        ev_sym = str(item_ev[0]).strip().upper().split(".", 1)[0]
                        s_ns = _to_epoch_ns(item_ev[1], 0)
                        e_ns = _to_epoch_ns(item_ev[2], 0)
                        if ev_sym in dual_bars_map and e_ns >= s_ns > 0:
                            events.append(
                                ConditionalSwingEvent(
                                    symbol=ev_sym,
                                    start_ns=s_ns,
                                    end_ns=e_ns,
                                    horizon_sessions=max(1, int(holding_horizon_sessions)),
                                )
                            )
                    elif len(item_ev) == 2:
                        s_ns = _to_epoch_ns(item_ev[0], 0)
                        e_ns = _to_epoch_ns(item_ev[1], 0)
                        if e_ns >= s_ns > 0:
                            for ev_sym in dual_bars_map:
                                events.append(
                                    ConditionalSwingEvent(
                                        symbol=ev_sym,
                                        start_ns=s_ns,
                                        end_ns=e_ns,
                                        horizon_sessions=max(1, int(holding_horizon_sessions)),
                                    )
                                )
            events.sort(key=lambda e: (e.start_ns, e.symbol))
        else:
            max_lookback = max(int(cand.get("lookback_window", 20)) for cand in grid)
            events = ConditionalSwingEventExtractor.extract_events(
                primary_bars_by_symbol=primary_bars_map,
                archetype_id=arch_id,
                warmup_bars_min=max_lookback,
                horizon_sessions=max(1, int(holding_horizon_sessions)),
                alpha_scores_by_symbol=alpha_scores_by_symbol,
                min_required_events=self._min_pooled_sample_trades,
            )

        if len(events) < self._min_pooled_sample_trades:
            fail_reason = (
                f"Insufficient conditional Top-K OOS events: {len(events)} < {self._min_pooled_sample_trades}"
            )
            ev_fail = RegimeCandidateEvaluation(
                vol_bucket=vb,
                liq_bucket=lb,
                dominant_style=style_key,
                archetype_id=arch_id,
                params={**dict(grid[0]), "execution_spec": h0_spec.to_dict()},
                passed=False,
                pooled_sample_trades=len(events),
                pooled_sharpe=0.0,
                pooled_dsr=0.0,
                stress_missed_fill_rate=1.0,
                failure_reasons=(fail_reason,),
                stress_reports=(),
                frozen_record=None,
                execution_spec=h0_spec,
                oos_event_count=len(events),
            )
            if persist and self._param_table is not None and hasattr(self._param_table, "deactivate_cell"):
                self._param_table.deactivate_cell(vb, lb, style_key)
            return RegimeCellCalibrationResult(
                vol_bucket=vb,
                liq_bucket=lb,
                dominant_style=style_key,
                archetype_id=arch_id,
                selected_record=None,
                evaluated_candidates=(ev_fail,),
                persisted=False,
            )

        # Anchored chronological Walk-Forward IS/OOS splits
        n_total = len(events)
        min_req = self._min_pooled_sample_trades
        if n_total == min_req:
            is_init = 0
        else:
            is_init = min(n_total - min_req, max(4, int(round(n_total * 0.30))))
        oos_span = n_total - is_init
        n_folds = min(4, max(2 if is_init == 0 else 1, oos_span // max(5, min_req // 4)))
        fold_step = max(1, oos_span // n_folds)
        fold_ranges: List[Tuple[int, int]] = []
        for f_idx in range(n_folds):
            f_start = is_init + f_idx * fold_step
            f_end = n_total if f_idx == n_folds - 1 else min(n_total, is_init + (f_idx + 1) * fold_step)
            if f_start < f_end:
                fold_ranges.append((f_start, f_end))

        eff_family_trials = max(
            int(total_family_trials or 0),
            TOTAL_3X2_REGIME_CELLS * len(grid) * len(raw_exec_list),
            len(grid) * len(raw_exec_list),
        )

        def _spec_key(es: ExecutionSpec) -> Tuple[str, str, Optional[str], str]:
            return (
                es.trigger_policy.value,
                es.exec_algorithm.value,
                str(es.window_quantile) if es.window_quantile is not None else None,
                es.bar_timeframe,
            )

        def _run_single_event(
            ev: ConditionalSwingEvent,
            params_dict: Mapping[str, Any],
            es: ExecutionSpec,
        ) -> Tuple[PlanItemContract, float, bool]:
            sym_dual = dual_bars_map[ev.symbol]
            sym_prim = primary_bars_map[ev.symbol]
            sym_1d = daily_bars_map.get(ev.symbol)
            sym_raw_low = (
                raw_low_by_symbol_ts_ns.get(ev.symbol)
                if raw_low_by_symbol_ts_ns is not None
                else None
            )
            base_item, _ = self._build_calibration_plan_item(
                symbol=ev.symbol,
                archetype_id=arch_id,
                params=params_dict,
                bars=sym_prim,
                bars_1d=sym_1d,
                execution_spec=es,
                cutoff_ts_ns=max(0, int(ev.start_ns) - 1),
            )
            win_valid_dt = datetime.fromtimestamp(
                ev.end_ns / 1_000_000_000.0, tz=timezone.utc
            ) + timedelta(days=1)
            win_item = replace(
                base_item,
                risk_envelope=replace(base_item.risk_envelope, valid_until=win_valid_dt),
            )
            max_warmup_prim = max(int(win_item.strategy_config.lookback_window) * 3, 120)
            pre_prim = [b for b in sym_prim if int(b.ts_init) < ev.start_ns]
            if len(pre_prim) > max_warmup_prim:
                cutoff_warm_ns = int(pre_prim[-max_warmup_prim].ts_init)
                warmup_prim = [b for b in pre_prim if int(b.ts_init) >= cutoff_warm_ns]
            else:
                warmup_prim = pre_prim
            ev_start_date = CausalMarketMicrostructureEstimator._ny_date_from_ns(ev.start_ns)
            same_day_pre_1m = [
                b
                for b in sym_dual
                if _bar_step_ns(b) == 60_000_000_000
                and int(b.ts_init) < ev.start_ns
                and CausalMarketMicrostructureEstimator._ny_date_from_ns(int(b.ts_init)) == ev_start_date
            ]
            warmup_slice = _sort_bars_deterministic(warmup_prim + same_day_pre_1m)
            window_slice = [
                b for b in sym_dual if ev.start_ns <= int(b.ts_init) <= ev.end_ns
            ]
            if not any(_bar_step_ns(b) == primary_step_ns for b in window_slice):
                return win_item, 0.0, False
            m = self._runner.run_backtest(
                win_item,
                bars=window_slice,
                warmup_bars=warmup_slice,
                raw_low_by_ts_ns=sym_raw_low,
                effective_trials=eff_family_trials,
            )
            filled = m.filled_orders > 0
            budget = max(1.0, float(win_item.risk_envelope.max_risk_budget_usd))
            r_val = (float(m.net_pnl_usd) / budget) if filled else 0.0
            return win_item, r_val, filled

        oos_events_all: List[ConditionalSwingEvent] = []
        oos_r_by_spec: Dict[Tuple[str, str, Optional[str], str], List[float]] = {
            _spec_key(es): [] for es in raw_exec_list
        }
        oos_filled_by_spec: Dict[Tuple[str, str, Optional[str], str], List[bool]] = {
            _spec_key(es): [] for es in raw_exec_list
        }
        oos_items_by_spec: Dict[Tuple[str, str, Optional[str], str], List[PlanItemContract]] = {
            _spec_key(es): [] for es in raw_exec_list
        }
        fold_selected_params: List[Dict[str, Any]] = []

        try:
            for f_start, f_end in fold_ranges:
                is_events = events[:f_start]
                oos_events = events[f_start:f_end]

                # Screen indicator parameter grid strictly on In-Sample events
                best_params = dict(grid[0])
                if is_events and len(grid) > 1:
                    is_sample = is_events[-min(len(is_events), 12) :]
                    best_is_score: Optional[Tuple[float, float]] = None
                    for cand_p in grid:
                        cand_dict = dict(cand_p)
                        is_rs = [_run_single_event(ev, cand_dict, h0_spec)[1] for ev in is_sample]
                        m_is = statistics.fmean(is_rs) if is_rs else 0.0
                        s_is = statistics.stdev(is_rs) if len(is_rs) >= 2 else 0.0
                        sh_is = (m_is / s_is) if s_is > 1e-12 else 0.0
                        sc_tuple = (m_is, sh_is)
                        if best_is_score is None or sc_tuple > best_is_score:
                            best_is_score = sc_tuple
                            best_params = cand_dict
                fold_selected_params.append(best_params)

                # Evaluate H0 and all candidate ExecutionSpecs strictly Out-Of-Sample
                for ev in oos_events:
                    oos_events_all.append(ev)
                    for es in raw_exec_list:
                        s_key = _spec_key(es)
                        w_item, r_val, filled = _run_single_event(ev, best_params, es)
                        oos_r_by_spec[s_key].append(r_val)
                        oos_filled_by_spec[s_key].append(filled)
                        oos_items_by_spec[s_key].append(w_item)

            n_oos = len(oos_events_all)
            h_lags = max(
                1,
                int(round(statistics.fmean([e.horizon_sessions for e in oos_events_all])))
                if oos_events_all
                else 1,
            )
            h0_key = _spec_key(h0_spec)
            h0_rs = oos_r_by_spec[h0_key]

            p_values_by_spec_name: Dict[str, float] = {}
            delta_series_by_spec_name: Dict[str, List[float]] = {}
            stats_by_spec_key: Dict[Tuple[str, str, Optional[str], str], Dict[str, Any]] = {}

            for es in raw_exec_list:
                s_key = _spec_key(es)
                s_name = (
                    f"{es.trigger_policy.value}:{es.exec_algorithm.value}"
                    f":{es.window_quantile if es.window_quantile is not None else 'NONE'}"
                )
                pi_rs = oos_r_by_spec[s_key]
                pi_fills = oos_filled_by_spec[s_key]
                deltas = [pi_rs[i] - h0_rs[i] for i in range(n_oos)]
                mean_delta = statistics.fmean(deltas) if deltas else 0.0
                if s_key != h0_key:
                    t_nw = compute_newey_west_t_stat(deltas, horizon_bars=h_lags)
                    p_val = max(0.0, min(1.0, 1.0 - _normal_cdf(t_nw)))
                    p_values_by_spec_name[s_name] = p_val
                    delta_series_by_spec_name[s_name] = deltas
                else:
                    t_nw = 0.0
                    p_val = 1.0
                mean_r = statistics.fmean(pi_rs) if pi_rs else 0.0
                std_r = statistics.stdev(pi_rs) if len(pi_rs) >= 2 else 0.0
                sharpe_r = (
                    (mean_r / std_r) * math.sqrt(252.0 / float(h_lags))
                    if std_r > 1e-12
                    else 0.0
                )
                dsr_r = compute_per_period_deflated_sharpe_ratio(
                    pi_rs,
                    effective_trials=eff_family_trials,
                )
                missed_rate = (
                    1.0 - (sum(1 for fl in pi_fills if fl) / float(n_oos))
                    if n_oos > 0
                    else 1.0
                )
                stats_by_spec_key[s_key] = {
                    "spec": es,
                    "spec_name": s_name,
                    "mean_r": mean_r,
                    "sharpe_r": sharpe_r,
                    "dsr_r": dsr_r,
                    "mean_delta_r": mean_delta,
                    "t_nw": t_nw,
                    "p_val": p_val,
                    "missed_rate": missed_rate,
                }

            by_fdr_results = (
                apply_benjamini_yekutieli_fdr_gate(
                    p_values_by_spec_name,
                    q=MAX_BY_FDR_Q,
                    total_family_trials=eff_family_trials,
                    candidate_series_map=delta_series_by_spec_name,
                )
                if p_values_by_spec_name
                else {}
            )

            rep_params = fold_selected_params[-1] if fold_selected_params else dict(grid[0])
            collar_atr_val = Decimal(
                str(
                    rep_params.get(
                        "limit_collar_atr_mult",
                        default_limit_collar_atr_for_archetype(arch_id),
                    )
                )
            ).quantize(Decimal("0.0001"))
            collar_pct_val = Decimal(
                str(rep_params.get("limit_collar_pct", _default_limit_collar_pct()))
            ).quantize(Decimal("0.0001"))

            def _run_conditional_stress_for_spec(es: ExecutionSpec) -> StressFalsificationReport:
                s_key = _spec_key(es)
                return self._falsifier.run_conditional_swing_falsification(
                    symbol=oos_events_all[0].symbol,
                    bars_by_symbol=dual_bars_map,
                    conditional_event_windows=[
                        (e.symbol, e.start_ns, e.end_ns) for e in oos_events_all
                    ],
                    plan_items_by_window=oos_items_by_spec[s_key],
                    execution_spec=es,
                    raw_low_by_symbol_ts_ns=raw_low_by_symbol_ts_ns,
                    effective_trials=eff_family_trials,
                )

            # Identify statistically significant non-H0 timing candidates
            qualifying_non_h0: List[Dict[str, Any]] = []
            for es in raw_exec_list:
                s_key = _spec_key(es)
                st = stats_by_spec_key[s_key]
                by_passed, by_q = by_fdr_results.get(st["spec_name"], (False, 1.0))
                st["by_passed"] = bool(by_passed)
                st["by_q"] = float(by_q)
                if s_key == h0_key:
                    st["by_passed"] = True
                    st["by_q"] = 1.0
                    continue
                if (
                    n_oos >= self._min_pooled_sample_trades
                    and st["mean_r"] > 0.0
                    and st["mean_delta_r"] > 0.0
                    and st["t_nw"] >= ONE_SIDED_ALPHA_005_CRITICAL_T
                    and st["by_passed"]
                    and st["missed_rate"] <= self._max_missed_fill_rate + 1e-9
                ):
                    qualifying_non_h0.append(st)

            qualifying_non_h0.sort(
                key=lambda s: (s["dsr_r"], s["mean_delta_r"], s["t_nw"]),
                reverse=True,
            )

            chosen_stat: Optional[Dict[str, Any]] = None
            chosen_stress_report: Optional[StressFalsificationReport] = None
            stress_reports_by_key: Dict[Tuple[str, str, Optional[str], str], StressFalsificationReport] = {}

            for cand_st in qualifying_non_h0:
                es = cand_st["spec"]
                s_key = _spec_key(es)
                rep = _run_conditional_stress_for_spec(es)
                stress_reports_by_key[s_key] = rep
                rep_missed = (
                    float(rep.limit_collar_missed_fill_result.observed_value)
                    if rep.limit_collar_missed_fill_result is not None
                    else cand_st["missed_rate"]
                )
                if rep.passed and rep_missed <= self._max_missed_fill_rate + 1e-9:
                    chosen_stat = cand_st
                    chosen_stress_report = rep
                    break

            # Fallback to H0 if no intraday timing policy beat H0 significantly and passed stress
            if chosen_stat is None:
                h0_st = stats_by_spec_key[h0_key]
                h0_rep = _run_conditional_stress_for_spec(h0_spec)
                stress_reports_by_key[h0_key] = h0_rep
                h0_rep_missed = (
                    float(h0_rep.limit_collar_missed_fill_result.observed_value)
                    if h0_rep.limit_collar_missed_fill_result is not None
                    else h0_st["missed_rate"]
                )
                if (
                    h0_rep.passed
                    and n_oos >= self._min_pooled_sample_trades
                    and h0_st["mean_r"] > 0.0
                    and h0_rep_missed <= self._max_missed_fill_rate + 1e-9
                ):
                    chosen_stat = h0_st
                    chosen_stress_report = h0_rep

            evaluations: List[RegimeCandidateEvaluation] = []
            selected_record: Optional[FrozenRegimeParamRecord] = None
            for es in raw_exec_list:
                s_key = _spec_key(es)
                st = stats_by_spec_key[s_key]
                rep = stress_reports_by_key.get(s_key)
                is_chosen = chosen_stat is not None and _spec_key(chosen_stat["spec"]) == s_key
                eff_missed = (
                    float(rep.limit_collar_missed_fill_result.observed_value)
                    if rep is not None and rep.limit_collar_missed_fill_result is not None
                    else float(st["missed_rate"])
                )
                fail_list: List[str] = []
                if n_oos < self._min_pooled_sample_trades:
                    fail_list.append(
                        f"Insufficient OOS events={n_oos} < {self._min_pooled_sample_trades}"
                    )
                if st["mean_r"] <= 0.0:
                    fail_list.append(f"Non-positive OOS mean_r={st['mean_r']:.4f}")
                if s_key != h0_key:
                    if st["mean_delta_r"] <= 0.0:
                        fail_list.append(f"delta_vs_H0={st['mean_delta_r']:.4f} <= 0.0")
                    if st["t_nw"] < ONE_SIDED_ALPHA_005_CRITICAL_T:
                        fail_list.append(
                            f"t_NW={st['t_nw']:.3f} < {ONE_SIDED_ALPHA_005_CRITICAL_T:.3f}"
                        )
                    if not st["by_passed"]:
                        fail_list.append(f"BY-FDR q={st['by_q']:.4f} > {MAX_BY_FDR_Q:.2f}")
                if eff_missed > self._max_missed_fill_rate + 1e-9:
                    fail_list.append(
                        f"missed_fill_rate={eff_missed:.4f} > {self._max_missed_fill_rate:.2f}"
                    )
                if rep is not None and not rep.passed:
                    fail_list.extend(rep.failure_reasons)

                frozen_rec: Optional[FrozenRegimeParamRecord] = None
                if is_chosen and chosen_stress_report is not None:
                    frozen_rec = FrozenRegimeParamRecord(
                        vol_bucket=vb,
                        liq_bucket=lb,
                        dominant_style=style_key,
                        archetype_id=arch_id,
                        bar_timeframe=bar_tf,
                        lookback_window=int(rep_params.get("lookback_window", 20)),
                        fast_window=int(rep_params.get("fast_window", 5)),
                        slow_window=int(rep_params.get("slow_window", 15)),
                        entry_quantile_threshold=Decimal(
                            str(rep_params.get("entry_quantile_threshold", "0.80"))
                        ).quantize(Decimal("0.0001")),
                        exit_quantile_threshold=Decimal(
                            str(rep_params.get("exit_quantile_threshold", "0.35"))
                        ).quantize(Decimal("0.0001")),
                        atr_stop_multiplier=Decimal(
                            str(rep_params.get("atr_stop_multiplier", "2.00"))
                        ).quantize(Decimal("0.0001")),
                        limit_collar_atr_mult=collar_atr_val,
                        limit_collar_pct=collar_pct_val,
                        pooled_sample_trades=int(n_oos),
                        pooled_expectancy_r=Decimal(f"{st['mean_r']:.4f}"),
                        pooled_sharpe=Decimal(f"{st['sharpe_r']:.4f}"),
                        pooled_dsr=Decimal(f"{st['dsr_r']:.6f}"),
                        stress_passed=True,
                        stress_missed_fill_rate=Decimal(f"{eff_missed:.4f}"),
                        stress_report_json={
                            "symbol_count": len(bars_by_symbol),
                            "active_symbol_count": sum(
                                1 for fl in oos_filled_by_spec[s_key] if fl
                            ),
                            "bar_timeframe": bar_tf,
                            "execution_spec": es.to_dict(),
                            "h0_paired_delta_r": round(float(st["mean_delta_r"]), 6),
                            "h0_paired_nw_t_stat": round(float(st["t_nw"]), 6),
                            "by_fdr_q_value": round(float(st["by_q"]), 6),
                            "oos_event_count": int(n_oos),
                            "effective_trials": int(eff_family_trials),
                            "reports": [chosen_stress_report.to_dict()],
                        },
                        calibrated_at=datetime.now(tz=timezone.utc),
                        execution_spec=es,
                        h0_paired_delta_r=float(st["mean_delta_r"]),
                        h0_paired_nw_t_stat=float(st["t_nw"]),
                        by_fdr_q_value=float(st["by_q"]),
                        oos_event_count=int(n_oos),
                    )
                    selected_record = frozen_rec

                evaluations.append(
                    RegimeCandidateEvaluation(
                        vol_bucket=vb,
                        liq_bucket=lb,
                        dominant_style=style_key,
                        archetype_id=arch_id,
                        params={**dict(rep_params), "execution_spec": es.to_dict()},
                        passed=is_chosen,
                        pooled_sample_trades=int(n_oos),
                        pooled_sharpe=float(st["sharpe_r"]),
                        pooled_dsr=float(st["dsr_r"]),
                        stress_missed_fill_rate=eff_missed,
                        failure_reasons=tuple(fail_list) if not is_chosen else (),
                        stress_reports=(rep,) if rep is not None else (),
                        frozen_record=frozen_rec,
                        execution_spec=es,
                        h0_paired_delta_r=float(st["mean_delta_r"]),
                        h0_paired_nw_t_stat=float(st["t_nw"]),
                        by_fdr_q_value=float(st["by_q"]),
                        oos_event_count=int(n_oos),
                    )
                )
        finally:
            self._clear_runner_last_refs()

        persisted = False
        if persist and self._param_table is not None:
            if selected_record is not None:
                self._param_table.upsert_record(selected_record)
                persisted = True
            elif hasattr(self._param_table, "deactivate_cell"):
                self._param_table.deactivate_cell(vb, lb, style_key)

        return RegimeCellCalibrationResult(
            vol_bucket=vb,
            liq_bucket=lb,
            dominant_style=style_key,
            archetype_id=arch_id,
            selected_record=selected_record,
            evaluated_candidates=tuple(evaluations),
            persisted=persisted,
        )

    def calibrate_3x2_panel(
        self,
        cell_symbol_bars: Mapping[
            Tuple[VolBucket | str, LiqBucket | str, str | StrategyArchetypeId],
            Mapping[str, Sequence[Bar]] | Sequence[Bar],
        ],
        *,
        candidate_grids_by_style: Optional[
            Mapping[str | StrategyArchetypeId, Sequence[Mapping[str, Any]]]
        ] = None,
        raw_low_by_symbol_ts_ns: Optional[Mapping[str, Mapping[int, float]]] = None,
        persist: bool = True,
    ) -> Dict[Tuple[str, str, str], RegimeCellCalibrationResult]:
        """Calibrates all provided `(vol_bucket, liq_bucket, dominant_style)` cells across the 3x2 panel."""
        results: Dict[Tuple[str, str, str], RegimeCellCalibrationResult] = {}
        for (vb_raw, lb_raw, style_raw), sym_bars in cell_symbol_bars.items():
            arch_id = resolve_archetype_for_dominant_style(style_raw)
            style_key = (
                style_raw.value
                if isinstance(style_raw, StrategyArchetypeId)
                else str(style_raw).strip().upper()
            )
            grid: Optional[Sequence[Mapping[str, Any]]] = None
            if candidate_grids_by_style is not None:
                grid = (
                    candidate_grids_by_style.get(style_raw)
                    or candidate_grids_by_style.get(style_key)
                    or candidate_grids_by_style.get(arch_id)
                    or candidate_grids_by_style.get(arch_id.value)
                )
            res = self.calibrate_regime_cell(
                vol_bucket=vb_raw,
                liq_bucket=lb_raw,
                dominant_style=style_key,
                symbol_bars=sym_bars,
                candidate_param_grid=grid,
                raw_low_by_symbol_ts_ns=raw_low_by_symbol_ts_ns,
                persist=persist,
            )
            results[(res.vol_bucket.value, res.liq_bucket.value, res.dominant_style)] = res
        return results

    @staticmethod
    def _collect_finite_cell_metrics(
        reports: Sequence[StressFalsificationReport],
        failures: List[str],
    ) -> Tuple[List[float], List[float], List[float], List[float]]:
        """Extract finite metrics across `reports` and record any non-finite observations."""
        sharpes: List[float] = []
        dsrs: List[float] = []
        expectancies_r: List[float] = []
        missed_rates: List[float] = []
        for report in reports:
            bm = report.baseline_metrics
            sh = float(bm.sharpe_ratio)
            dsr = float(bm.per_period_dsr)
            ret = float(bm.total_return_pct)
            if not (math.isfinite(sh) and math.isfinite(dsr) and math.isfinite(ret)):
                failures.append(f"[{report.symbol}] Non-finite baseline backtest metrics")
            else:
                sharpes.append(sh)
                dsrs.append(dsr)
                expectancies_r.append(ret)
            if report.limit_collar_missed_fill_result is not None:
                mr = float(report.limit_collar_missed_fill_result.observed_value)
                if math.isfinite(mr):
                    missed_rates.append(mr)
                else:
                    failures.append(f"[{report.symbol}] Non-finite missed_fill_rate")
        return sharpes, dsrs, missed_rates, expectancies_r

    @staticmethod
    def _metrics_breach_budget(metrics: Any, max_risk_budget: float) -> bool:
        net_val = float(metrics.net_pnl_usd)
        worst_val = float(metrics.max_single_trade_loss_usd)
        cum_cap = max_risk_budget * float(max(1, int(metrics.total_positions)))
        return (
            not (math.isfinite(net_val) and math.isfinite(worst_val))
            or net_val < -cum_cap - 1e-6
            or abs(min(0.0, worst_val)) > max_risk_budget + 1e-6
        )

    @staticmethod
    def _evaluate_cell_reports(
        *,
        sym_count: int,
        reports: Sequence[StressFalsificationReport],
        plan_items_by_sym: Mapping[str, PlanItemContract],
    ) -> Tuple[List[str], List[float], List[float], List[float], List[float]]:
        """Evaluate per-symbol safety invariants and pooled bucket edge across reports."""
        failures: List[str] = []
        if sym_count <= 1:
            sharpes, dsrs, missed_rates, expectancies_r = OfflineRegimePanelCalibrator._collect_finite_cell_metrics(
                reports, failures
            )
            for report in reports:
                if not report.passed:
                    failures.extend(report.failure_reasons)
            return failures, sharpes, dsrs, missed_rates, expectancies_r

        # Hard per-symbol risk-envelope safety gate across every symbol in the bucket.
        for report in reports:
            sym_key = str(report.symbol).strip().upper()
            plan_item = plan_items_by_sym.get(report.symbol) or plan_items_by_sym.get(sym_key)
            if plan_item is None:
                failures.append(f"[{sym_key}] Missing plan_item contract during pooled evaluation")
                continue
            max_b = float(plan_item.risk_envelope.max_risk_budget_usd)

            bm = report.baseline_metrics
            if bm.filled_orders > 0 and OfflineRegimePanelCalibrator._metrics_breach_budget(bm, max_b):
                failures.append(
                    f"[{sym_key}] Baseline loss exceeds max_risk_budget_usd={max_b:.2f}"
                )

            for res_attr in ("signal_delay_result", "tail_gap_shock_result", "double_friction_result"):
                stress_res = getattr(report, res_attr, None)
                if (
                    stress_res is not None
                    and stress_res.metrics.filled_orders > 0
                    and OfflineRegimePanelCalibrator._metrics_breach_budget(stress_res.metrics, max_b)
                ):
                    failures.append(f"[{sym_key}] {stress_res.reason}")

        active_reports = [r for r in reports if r.baseline_metrics.filled_orders > 0]
        sharpes, dsrs, missed_rates, expectancies_r = OfflineRegimePanelCalibrator._collect_finite_cell_metrics(
            active_reports, failures
        )
        if not active_reports:
            failures.append("Pooled baseline backtests produced 0 filled orders")
            return failures, sharpes, dsrs, missed_rates, expectancies_r

        pooled_delay_fills = sum(
            int(r.signal_delay_result.metrics.filled_orders)
            for r in active_reports
            if r.signal_delay_result is not None
        )
        if pooled_delay_fills == 0:
            failures.append("Pooled +1 bar delay stress produced 0 filled orders")

        pooled_gap_fills = sum(
            int(r.tail_gap_shock_result.metrics.filled_orders)
            for r in active_reports
            if r.tail_gap_shock_result is not None
        )
        if pooled_gap_fills == 0:
            failures.append("Pooled tail gap shock stress produced 0 filled orders")

        pooled_base_pnl = sum(float(r.baseline_metrics.net_pnl_usd) for r in active_reports)
        pooled_fric_metrics = [
            r.double_friction_result.metrics
            for r in active_reports
            if (
                r.double_friction_result is not None
                and r.double_friction_result.metrics.filled_orders > 0
            )
        ]
        pooled_fric_pnl = sum(float(m.net_pnl_usd) for m in pooled_fric_metrics)
        pooled_fric_fills = sum(int(m.filled_orders) for m in pooled_fric_metrics)
        if pooled_fric_fills == 0:
            failures.append("Pooled 2x friction stress produced 0 filled orders")
        elif not math.isfinite(pooled_base_pnl) or pooled_base_pnl <= 0.0:
            failures.append(
                f"FAILED pooled baseline expectancy: baseline_net_pnl={pooled_base_pnl:.2f} <= 0.0"
            )
        elif not math.isfinite(pooled_fric_pnl) or pooled_fric_pnl <= 0.0:
            failures.append(
                f"FAILED pooled 2x friction stress: baseline_net_pnl={pooled_base_pnl:.2f}, "
                f"friction_net_pnl={pooled_fric_pnl:.2f} <= 0.0"
            )

        return failures, sharpes, dsrs, missed_rates, expectancies_r

    def _deactivate_all_cells(
        self,
        all_vol_buckets: Sequence[VolBucket],
        all_liq_buckets: Sequence[LiqBucket],
    ) -> None:
        if self._param_table is None or not hasattr(self._param_table, "deactivate_cell"):
            return
        for vb in all_vol_buckets:
            for lb in all_liq_buckets:
                for arch_id in (
                    StrategyArchetypeId.TREND_MOMENTUM,
                    StrategyArchetypeId.VOLATILITY_BREAKOUT,
                    StrategyArchetypeId.MEAN_REVERSION,
                ):
                    self._param_table.deactivate_cell(vb, lb, arch_id)

    def _build_symbol_bars_from_matrices(
        self,
        *,
        panel_matrices: Mapping[str, Any],
        symbols: Sequence[str],
        timestamps: Sequence[Any],
        target_symbols: Sequence[str],
        bar_timeframe: str,
        max_bars: Optional[int] = None,
    ) -> Tuple[Dict[str, List[Bar]], Dict[str, Dict[int, float]]]:
        import numpy as np

        tf_norm = str(bar_timeframe or "15m").strip().lower()
        if tf_norm not in _TF_BAR_SPECS or "close" not in panel_matrices:
            return {}, {}
        bar_spec_str, _, step_ns = _TF_BAR_SPECS[tf_norm]
        closes_full = np.asarray(panel_matrices["close"], dtype=np.float64)
        if closes_full.ndim != 2 or closes_full.shape[0] == 0 or closes_full.shape[1] == 0:
            return {}, {}
        t_len, n_sym = closes_full.shape
        bar_slice = min(t_len, len(timestamps))
        if max_bars is not None and int(max_bars) > 0:
            bar_slice = min(bar_slice, int(max_bars))
        if bar_slice <= 0:
            return {}, {}

        closes = closes_full[-bar_slice:]
        opens = np.asarray(panel_matrices.get("open", closes_full), dtype=np.float64)[-bar_slice:]
        highs = np.asarray(panel_matrices.get("high", closes_full), dtype=np.float64)[-bar_slice:]
        lows = np.asarray(panel_matrices.get("low", closes_full), dtype=np.float64)[-bar_slice:]
        volumes = np.asarray(panel_matrices.get("volume", np.ones_like(closes_full)), dtype=np.float64)[-bar_slice:]
        raw_lows_mat = panel_matrices.get("raw_low")
        raw_lows = (
            np.asarray(raw_lows_mat, dtype=np.float64)[-bar_slice:]
            if raw_lows_mat is not None
            else lows
        )

        sym_to_col: Dict[str, int] = {}
        for idx, s in enumerate(symbols[:n_sym]):
            norm_s = str(s).strip().upper().split(".", 1)[0]
            if norm_s and norm_s not in sym_to_col:
                sym_to_col[norm_s] = idx

        ts_ns_arr = _build_monotonic_ts_ns(list(timestamps)[-bar_slice:], step_ns=step_ns)
        bars_out: Dict[str, List[Bar]] = {}
        raw_low_out: Dict[str, Dict[int, float]] = {}

        for sym in target_symbols:
            norm_s = str(sym).strip().upper().split(".", 1)[0]
            j = sym_to_col.get(norm_s)
            if j is None:
                continue
            bt = BarType.from_str(f"{norm_s}.{self._venue_name}-{bar_spec_str}")
            sym_bar_list: List[Bar] = []
            sym_raw_low_map: Dict[int, float] = {}
            for t_idx in range(bar_slice):
                c_px = float(closes[t_idx, j])
                if not np.isfinite(c_px) or c_px <= 0.0:
                    continue
                px_tick = float(rule_612_tick_size(Decimal(f"{c_px:.6f}")))
                px_prec = 4 if px_tick < float(PENNY_TICK_RULE_612) else 2
                o_px = float(opens[t_idx, j]) if np.isfinite(opens[t_idx, j]) else c_px
                h_px = max(o_px, c_px, float(highs[t_idx, j]) if np.isfinite(highs[t_idx, j]) else c_px)
                l_px = max(px_tick, min(o_px, c_px, float(lows[t_idx, j]) if np.isfinite(lows[t_idx, j]) else c_px))
                v_qty = max(1, int(round(float(volumes[t_idx, j])))) if np.isfinite(volumes[t_idx, j]) else 100
                ts_ns = ts_ns_arr[t_idx]
                sym_bar_list.append(
                    Bar(
                        bar_type=bt,
                        open=Price(o_px, px_prec),
                        high=Price(h_px, px_prec),
                        low=Price(l_px, px_prec),
                        close=Price(c_px, px_prec),
                        volume=Quantity.from_int(v_qty),
                        ts_event=ts_ns,
                        ts_init=ts_ns,
                    )
                )
                r_lo = float(raw_lows[t_idx, j]) if np.isfinite(raw_lows[t_idx, j]) else l_px
                sym_raw_low_map[ts_ns] = min(l_px, max(px_tick, r_lo))
            if sym_bar_list:
                bars_out[norm_s] = sym_bar_list
                raw_low_out[norm_s] = sym_raw_low_map
        return bars_out, raw_low_out

    def _partition_panel_bucket_candidates(
        self,
        panel_matrices: Mapping[str, Any],
        symbols: Sequence[str],
        timestamps: Sequence[Any],
        *,
        bar_timeframe: str = "15m",
        sector_map: Optional[Mapping[str, str]] = None,
        max_bars: int = 520,
    ) -> Optional[
        Tuple[
            Dict[Tuple[VolBucket, LiqBucket], List[Tuple[int, str, float, float, str]]],
            int,
            str,
            int,
            Any,
            Any,
            Any,
            Any,
            Any,
            Any,
        ]
    ]:
        """Classify eligible panel columns into `3x2` `(VolBucket, LiqBucket)` candidate lists."""
        if not symbols or not timestamps or "close" not in panel_matrices:
            return None
        import warnings
        import numpy as np

        all_vol_buckets = (VolBucket.LOW_VOL, VolBucket.MID_VOL, VolBucket.HIGH_VOL)
        all_liq_buckets = (LiqBucket.HIGH_LIQ_TIGHT_SPREAD, LiqBucket.LOW_LIQ_WIDE_SPREAD)

        closes_full = np.asarray(panel_matrices["close"], dtype=np.float64)
        if closes_full.ndim != 2 or closes_full.shape[0] < 30 or closes_full.shape[1] == 0:
            return None
        tf_norm = str(bar_timeframe or "15m").strip().lower()
        if tf_norm not in _TF_BAR_SPECS:
            raise ValueError(f"Unsupported bar_timeframe {bar_timeframe!r}; expected one of {sorted(_TF_BAR_SPECS)}")
        bar_spec_str, bars_per_day, step_ns = _TF_BAR_SPECS[tf_norm]

        t_len, n_sym = closes_full.shape
        bar_slice = min(t_len, len(timestamps), max(30, int(max_bars)))
        if bar_slice < 30:
            return None
        hist_slice = min(t_len, 252 * bars_per_day)

        raw_c_hist = closes_full[-hist_slice:]
        closes_hist = np.where(raw_c_hist > 0.0, raw_c_hist, np.nan)
        raw_h_hist = np.asarray(panel_matrices.get("high", closes_full), dtype=np.float64)[-hist_slice:]
        highs_hist = np.where(raw_h_hist > 0.0, raw_h_hist, np.nan)
        raw_l_hist = np.asarray(panel_matrices.get("low", closes_full), dtype=np.float64)[-hist_slice:]
        lows_hist = np.where(raw_l_hist > 0.0, raw_l_hist, np.nan)
        prev_closes_hist = np.vstack([closes_hist[:1], closes_hist[:-1]])
        tr_hist = np.maximum(
            highs_hist - lows_hist,
            np.maximum(np.abs(highs_hist - prev_closes_hist), np.abs(lows_hist - prev_closes_hist)),
        )
        rel_tr_hist = tr_hist / closes_hist
        atr_14_series = _compute_vectorized_rolling_atr(rel_tr_hist, window=14)
        rel_vol = atr_14_series[-1]
        finite_mask = np.isfinite(atr_14_series) & np.isfinite(rel_vol[None, :])
        valid_counts = np.sum(finite_mask, axis=0)
        less_or_eq_counts = np.sum(
            finite_mask & (atr_14_series <= rel_vol[None, :]),
            axis=0,
        )
        hist_vol_q = np.clip(
            np.where(
                valid_counts > 1,
                (less_or_eq_counts - 1.0) / np.maximum(valid_counts - 1.0, 1.0),
                LIQ_BUCKET_MEDIAN_QUANTILE,
            ),
            0.0,
            1.0,
        )

        closes = closes_full[-bar_slice:]
        opens = np.asarray(panel_matrices.get("open", closes_full), dtype=np.float64)[-bar_slice:]
        highs = np.asarray(panel_matrices.get("high", closes_full), dtype=np.float64)[-bar_slice:]
        lows = np.asarray(panel_matrices.get("low", closes_full), dtype=np.float64)[-bar_slice:]
        volumes = np.asarray(panel_matrices.get("volume", np.ones_like(closes_full)), dtype=np.float64)[-bar_slice:]
        raw_lows_mat = panel_matrices.get("raw_low")
        raw_lows = (
            np.asarray(raw_lows_mat, dtype=np.float64)[-bar_slice:]
            if raw_lows_mat is not None
            else lows
        )

        norm_syms = [str(s).strip().upper() for s in symbols[:n_sym]]
        valid_bar_counts = np.sum(np.isfinite(closes) & (closes > 0.0), axis=0)
        eligible_indices = np.array(
            [j for j, sym in enumerate(norm_syms) if sym and int(valid_bar_counts[j]) >= 30],
            dtype=np.int64,
        )
        if eligible_indices.size == 0:
            return ({}, bar_slice, bar_spec_str, step_ns, closes, opens, highs, lows, volumes, raw_lows)

        adv_win = min(bar_slice, 60 * bars_per_day)
        spread_win = min(bar_slice, 14 * bars_per_day)
        closes_pos = np.where(closes > 0.0, closes, np.nan)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            median_closes = np.nanmedian(closes_pos, axis=0)
            adv_60 = np.nanmean(closes_pos[-adv_win:] * np.maximum(volumes[-adv_win:], 0.0), axis=0)
            rel_spread = np.nanmean(
                (highs[-spread_win:] - lows[-spread_win:]) / closes_pos[-spread_win:],
                axis=0,
            )
            elig_adv = adv_60[eligible_indices]
            finite_adv = elig_adv[np.isfinite(elig_adv) & (elig_adv > 0.0)]
            if finite_adv.size > 0:
                med_adv = max(1.0, float(np.median(finite_adv)))
                liq_scale = np.sqrt(np.maximum(adv_60, 1.0) / med_adv)
                rel_spread = rel_spread / np.where(liq_scale > 0.0, liq_scale, 1.0)

        def _rank_eligible(values: Any) -> Any:
            out = np.zeros(n_sym, dtype=np.float64)
            sub = np.asarray(values[eligible_indices], dtype=np.float64)
            finite_sub = sub[np.isfinite(sub)]
            fill_val = float(np.median(finite_sub)) if finite_sub.size > 0 else 0.0
            clean_sub = np.where(np.isfinite(sub), sub, fill_val)
            m_elig = int(eligible_indices.size)
            denom = float(max(1, m_elig - 1))
            elig_ranks = np.zeros(m_elig, dtype=np.float64)
            elig_ranks[np.argsort(clean_sub)] = np.arange(m_elig, dtype=np.float64) / denom
            out[eligible_indices] = elig_ranks
            return out

        vol_ranks = _rank_eligible(rel_vol)
        adv_ranks = _rank_eligible(adv_60)
        spread_ranks = _rank_eligible(rel_spread)

        min_universe_for_micro = TOTAL_3X2_REGIME_CELLS * 2
        apply_micro_floor = int(eligible_indices.size) >= min_universe_for_micro
        liq_floor_quantile = 1.0 / float(len(all_vol_buckets) + len(all_liq_buckets))
        sys_collar_bps_cap = float(SystemConfig().execution.limit_collar_pct) * 100.0
        elig_tick_bps: List[float] = []
        for j_np in eligible_indices:
            med_px = float(median_closes[int(j_np)])
            if math.isfinite(med_px) and med_px >= MIN_TRADABLE_STOCK_PRICE_USD:
                tick_sz = float(rule_612_tick_size(Decimal(f"{med_px:.6f}")))
                elig_tick_bps.append((tick_sz / med_px) * 10_000.0)
        emp_tick_p95 = (
            float(np.quantile(np.asarray(elig_tick_bps, dtype=np.float64), 0.95))
            if elig_tick_bps
            else sys_collar_bps_cap
        )
        max_tick_spread_bps = min(emp_tick_p95, sys_collar_bps_cap)

        eligible_syms = [norm_syms[int(j)] for j in eligible_indices]
        if sector_map is not None:
            eff_sector_map = {
                str(k).strip().upper(): (str(v).strip().upper() or "UNCLASSIFIED")
                for k, v in sector_map.items()
            }
        else:
            eff_sector_map = get_gics_sector_map(symbols=eligible_syms, dsn=self._dsn)

        candidates_by_cell: Dict[Tuple[VolBucket, LiqBucket], List[Tuple[int, str, float, float, str]]] = {}
        for j_np in eligible_indices:
            j = int(j_np)
            sym = norm_syms[j]
            med_px = float(median_closes[j])
            if not math.isfinite(med_px) or med_px < MIN_TRADABLE_STOCK_PRICE_USD:
                continue
            if apply_micro_floor:
                tick_sz = float(rule_612_tick_size(Decimal(f"{med_px:.6f}")))
                tick_bps = (tick_sz / med_px) * 10_000.0
                if tick_bps > max_tick_spread_bps or float(adv_ranks[j]) < liq_floor_quantile:
                    continue
            comp_vol_q = compute_composite_vol_quantile(float(hist_vol_q[j]), float(vol_ranks[j]))
            vb = classify_vol_bucket(comp_vol_q)
            lb = classify_liq_bucket(
                adv_60d_quantile=adv_ranks[j],
                spread_bps_quantile=spread_ranks[j],
            )
            sector = str(eff_sector_map.get(sym, "UNCLASSIFIED") or "UNCLASSIFIED").strip().upper()
            candidates_by_cell.setdefault((vb, lb), []).append(
                (j, sym, comp_vol_q, float(adv_ranks[j]), sector)
            )
        return (
            candidates_by_cell,
            bar_slice,
            bar_spec_str,
            step_ns,
            closes,
            opens,
            highs,
            lows,
            volumes,
            raw_lows,
        )

    def select_stratified_bucket_symbols(
        self,
        panel_matrices: Mapping[str, Any],
        symbols: Sequence[str],
        timestamps: Sequence[Any],
        *,
        bar_timeframe: str = "15m",
        sector_map: Optional[Mapping[str, str]] = None,
        max_symbols_per_bucket: int = 16,
        max_bars: int = 520,
    ) -> List[str]:
        """Return the exact sorted list of sector-stratified symbols selected across all 3x2 regime buckets."""
        partitioned = self._partition_panel_bucket_candidates(
            panel_matrices,
            symbols,
            timestamps,
            bar_timeframe=bar_timeframe,
            sector_map=sector_map,
            max_bars=max_bars,
        )
        if partitioned is None:
            return []
        candidates_by_cell = partitioned[0]
        selected: set[str] = set()
        cap = max(1, int(max_symbols_per_bucket))
        for cell_cands in candidates_by_cell.values():
            for _j, sym in _select_sector_stratified_candidates(cell_cands, cap):
                selected.add(sym)
        return sorted(selected)

    def calibrate_from_panel_matrices(
        self,
        panel_matrices: Mapping[str, Any],
        symbols: Sequence[str],
        timestamps: Sequence[Any],
        *,
        bar_timeframe: str = "15m",
        sector_map: Optional[Mapping[str, str]] = None,
        max_symbols_per_bucket: int = 16,
        max_bars: int = 520,
        persist: bool = True,
        panel_matrices_1m: Optional[Mapping[str, Any]] = None,
        timestamps_1m: Optional[Sequence[Any]] = None,
        symbols_1m: Optional[Sequence[str]] = None,
        panel_matrices_1d: Optional[Mapping[str, Any]] = None,
        timestamps_1d: Optional[Sequence[Any]] = None,
        symbols_1d: Optional[Sequence[str]] = None,
        alpha_scores: Optional[Any] = None,
        holding_horizon_sessions: int = DEFAULT_SWING_HORIZON_SESSIONS,
        only_cells: Optional[Sequence[Tuple[Any, Any, Any]]] = None,
        on_cell_complete: Optional[Callable[[Any], None]] = None,
    ) -> int:
        """Partitions a 2D panel `(T, N)` into 3x2 `(VolBucket, LiqBucket)` buckets and calibrates frozen parameters."""
        import numpy as np

        all_vol_buckets = (VolBucket.LOW_VOL, VolBucket.MID_VOL, VolBucket.HIGH_VOL)
        all_liq_buckets = (LiqBucket.HIGH_LIQ_TIGHT_SPREAD, LiqBucket.LOW_LIQ_WIDE_SPREAD)
        tf_norm = str(bar_timeframe or "15m").strip().lower()
        partitioned = self._partition_panel_bucket_candidates(
            panel_matrices,
            symbols,
            timestamps,
            bar_timeframe=bar_timeframe,
            sector_map=sector_map,
            max_bars=max_bars,
        )
        if partitioned is None:
            return 0
        (
            candidates_by_cell,
            bar_slice,
            bar_spec_str,
            step_ns,
            closes,
            opens,
            highs,
            lows,
            volumes,
            raw_lows,
        ) = partitioned
        if not candidates_by_cell:
            if persist:
                self._deactivate_all_cells(all_vol_buckets, all_liq_buckets)
            return 0

        allowed_cells: Optional[set[Tuple[VolBucket, LiqBucket, StrategyArchetypeId]]] = None
        allowed_buckets: Optional[set[Tuple[VolBucket, LiqBucket]]] = None
        if only_cells is not None:
            allowed_cells = set()
            allowed_buckets = set()
            for raw_vb, raw_lb, raw_arch in only_cells:
                c_vb = raw_vb if isinstance(raw_vb, VolBucket) else VolBucket(str(raw_vb).strip().upper())
                c_lb = raw_lb if isinstance(raw_lb, LiqBucket) else LiqBucket(str(raw_lb).strip().upper())
                c_arch = resolve_archetype_for_dominant_style(raw_arch)
                allowed_cells.add((c_vb, c_lb, c_arch))
                allowed_buckets.add((c_vb, c_lb))
            if not allowed_cells:
                return 0

        ts_ns_arr = _build_monotonic_ts_ns(list(timestamps)[-bar_slice:], step_ns=step_ns)
        bucket_bars: Dict[Tuple[VolBucket, LiqBucket], Dict[str, List[Bar]]] = {}
        raw_low_by_sym: Dict[str, Dict[int, float]] = {}
        selected_syms_set: set[str] = set()

        for (vb, lb), cell_cands in candidates_by_cell.items():
            if allowed_buckets is not None and (vb, lb) not in allowed_buckets:
                continue
            chosen = _select_sector_stratified_candidates(cell_cands, max(1, int(max_symbols_per_bucket)))
            cell_map = bucket_bars.setdefault((vb, lb), {})
            for j, sym in chosen:
                bt = BarType.from_str(f"{sym}.{self._venue_name}-{bar_spec_str}")
                sym_bar_list: List[Bar] = []
                sym_raw_low_map: Dict[int, float] = {}
                for t_idx in range(bar_slice):
                    c_px = float(closes[t_idx, j])
                    if not np.isfinite(c_px) or c_px <= 0.0:
                        continue
                    px_tick = float(rule_612_tick_size(Decimal(f"{c_px:.6f}")))
                    px_prec = 4 if px_tick < float(PENNY_TICK_RULE_612) else 2
                    o_px = float(opens[t_idx, j]) if np.isfinite(opens[t_idx, j]) else c_px
                    h_px = max(o_px, c_px, float(highs[t_idx, j]) if np.isfinite(highs[t_idx, j]) else c_px)
                    l_px = max(px_tick, min(o_px, c_px, float(lows[t_idx, j]) if np.isfinite(lows[t_idx, j]) else c_px))
                    v_qty = max(1, int(round(float(volumes[t_idx, j])))) if np.isfinite(volumes[t_idx, j]) else 100
                    ts_ns = ts_ns_arr[t_idx]
                    sym_bar_list.append(
                        Bar(
                            bar_type=bt,
                            open=Price(o_px, px_prec),
                            high=Price(h_px, px_prec),
                            low=Price(l_px, px_prec),
                            close=Price(c_px, px_prec),
                            volume=Quantity.from_int(v_qty),
                            ts_event=ts_ns,
                            ts_init=ts_ns,
                        )
                    )
                    r_lo = float(raw_lows[t_idx, j]) if np.isfinite(raw_lows[t_idx, j]) else l_px
                    sym_raw_low_map[ts_ns] = min(l_px, max(px_tick, r_lo))
                if len(sym_bar_list) >= 30:
                    cell_map[sym] = sym_bar_list
                    raw_low_by_sym[sym] = sym_raw_low_map
                    selected_syms_set.add(sym)

        selected_syms_list = sorted(selected_syms_set)
        bars_1m_by_sym: Optional[Dict[str, List[Bar]]] = None
        if panel_matrices_1m is not None and timestamps_1m is not None and selected_syms_list:
            b1m, _ = self._build_symbol_bars_from_matrices(
                panel_matrices=panel_matrices_1m,
                symbols=symbols_1m or symbols,
                timestamps=timestamps_1m,
                target_symbols=selected_syms_list,
                bar_timeframe="1m",
            )
            if b1m:
                bars_1m_by_sym = b1m

        bars_1d_by_sym: Optional[Dict[str, List[Bar]]] = None
        if panel_matrices_1d is not None and timestamps_1d is not None and selected_syms_list:
            b1d, _ = self._build_symbol_bars_from_matrices(
                panel_matrices=panel_matrices_1d,
                symbols=symbols_1d or symbols,
                timestamps=timestamps_1d,
                target_symbols=selected_syms_list,
                bar_timeframe="1d",
            )
            if b1d:
                bars_1d_by_sym = b1d

        alpha_scores_by_sym: Optional[Dict[str, List[Tuple[int, float]]]] = None
        raw_alpha = alpha_scores if alpha_scores is not None else panel_matrices.get("alpha_score")
        if raw_alpha is not None and selected_syms_list:
            if isinstance(raw_alpha, Mapping):
                alpha_scores_by_sym = {}
                for sym, seq in raw_alpha.items():
                    norm_s = str(sym).strip().upper().split(".", 1)[0]
                    if norm_s in selected_syms_set and seq:
                        alpha_scores_by_sym[norm_s] = [
                            (_to_epoch_ns(ts_v, ts_ns_arr[0]), float(sc_v))
                            for ts_v, sc_v in seq
                            if math.isfinite(float(sc_v))
                        ]
            else:
                alpha_arr = np.asarray(raw_alpha, dtype=np.float64)
                if alpha_arr.ndim == 2:
                    a_syms = list(symbols_1d or symbols)
                    a_ts_raw = list(timestamps_1d or timestamps)
                    a_t = min(alpha_arr.shape[0], len(a_ts_raw))
                    a_step = _TF_BAR_SPECS["1d"][2] if timestamps_1d is not None else step_ns
                    a_ts_ns = _build_monotonic_ts_ns(a_ts_raw[-a_t:], step_ns=a_step)
                    alpha_mat = alpha_arr[-a_t:]
                    sym_col_a = {
                        str(s).strip().upper().split(".", 1)[0]: idx
                        for idx, s in enumerate(a_syms[: alpha_mat.shape[1]])
                    }
                    alpha_scores_by_sym = {}
                    for norm_s in selected_syms_list:
                        col_i = sym_col_a.get(norm_s)
                        if col_i is None:
                            continue
                        pts = [
                            (a_ts_ns[ti], float(alpha_mat[ti, col_i]))
                            for ti in range(a_t)
                            if math.isfinite(float(alpha_mat[ti, col_i]))
                        ]
                        if pts:
                            alpha_scores_by_sym[norm_s] = pts

        persisted_count = 0
        eff_horizon_sessions = max(1, int(holding_horizon_sessions))
        runner_config = {
            "starting_capital_usd": getattr(self._runner, "_starting_capital_usd", Decimal("100000.00")),
            "effective_trials": getattr(self._runner, "_effective_trials", 1),
            "fee_schedule": getattr(self._runner, "_fee_schedule", None),
            "market_impact_spec": getattr(self._runner, "_market_impact_spec", None),
            "explicit_half_spread_bps": getattr(self._runner, "_explicit_half_spread_bps", None),
            "explicit_impact_y": getattr(self._runner, "_explicit_impact_y", None),
            "system_config": getattr(self._runner, "_system_config", None),
        }
        cell_tasks: List[Dict[str, Any]] = []

        for vb in all_vol_buckets:
            for lb in all_liq_buckets:
                if allowed_buckets is not None and (vb, lb) not in allowed_buckets:
                    continue
                sym_map = bucket_bars.get((vb, lb), {})
                if not sym_map:
                    if (
                        persist
                        and self._param_table is not None
                        and hasattr(self._param_table, "deactivate_cell")
                    ):
                        for arch_id in (
                            StrategyArchetypeId.TREND_MOMENTUM,
                            StrategyArchetypeId.VOLATILITY_BREAKOUT,
                            StrategyArchetypeId.MEAN_REVERSION,
                        ):
                            if allowed_cells is None or (vb, lb, arch_id) in allowed_cells:
                                self._param_table.deactivate_cell(vb, lb, arch_id)
                    continue

                bucket_syms = set(sym_map.keys())
                bucket_raw_low = {
                    s: raw_low_by_sym[s]
                    for s in bucket_syms
                    if s in raw_low_by_sym
                }
                bucket_bars_1m = (
                    ({s: bars_1m_by_sym[s] for s in bucket_syms if s in bars_1m_by_sym} or None)
                    if bars_1m_by_sym is not None
                    else None
                )
                bucket_bars_1d = (
                    ({s: bars_1d_by_sym[s] for s in bucket_syms if s in bars_1d_by_sym} or None)
                    if bars_1d_by_sym is not None
                    else None
                )
                bucket_alpha_scores = (
                    ({s: alpha_scores_by_sym[s] for s in bucket_syms if s in alpha_scores_by_sym} or None)
                    if alpha_scores_by_sym is not None
                    else None
                )

                for arch_id in (
                    StrategyArchetypeId.TREND_MOMENTUM,
                    StrategyArchetypeId.VOLATILITY_BREAKOUT,
                    StrategyArchetypeId.MEAN_REVERSION,
                ):
                    if allowed_cells is not None and (vb, lb, arch_id) not in allowed_cells:
                        continue
                    tf_grid = tuple(
                        {**cand, "bar_timeframe": tf_norm}
                        for cand in DEFAULT_REGIME_CANDIDATE_GRIDS[arch_id]
                    )
                    total_trials_est = TOTAL_3X2_REGIME_CELLS * len(tf_grid) * len(DEFAULT_EXECUTION_SPEC_GRID)
                    cell_tasks.append(
                        {
                            "vol_bucket": vb,
                            "liq_bucket": lb,
                            "arch_id": arch_id,
                            "symbol_bars": sym_map,
                            "candidate_param_grid": tf_grid,
                            "raw_low_by_symbol_ts_ns": bucket_raw_low,
                            "execution_spec_grid": DEFAULT_EXECUTION_SPEC_GRID,
                            "bars_1m_by_symbol": bucket_bars_1m,
                            "bars_1d_by_symbol": bucket_bars_1d,
                            "alpha_scores_by_symbol": bucket_alpha_scores,
                            "total_family_trials": total_trials_est,
                            "holding_horizon_sessions": eff_horizon_sessions,
                            "min_pooled_sample_trades": self._min_pooled_sample_trades,
                            "max_missed_fill_rate": self._max_missed_fill_rate,
                            "venue_name": self._venue_name,
                            "runner_config": runner_config,
                        }
                    )

        if not cell_tasks:
            return 0

        max_workers = min(3, os.cpu_count() or 1)
        can_parallelize_in_workers = (
            type(self._runner) is IsomorphicBacktestRunner
            and type(self._falsifier) is StressFalsifier
            and getattr(self.calibrate_regime_cell, "__func__", None)
            is OfflineRegimePanelCalibrator.calibrate_regime_cell
            and getattr(self._runner.run_backtest, "__func__", None)
            is IsomorphicBacktestRunner.run_backtest
            and getattr(self._falsifier.run_falsification, "__func__", None)
            is StressFalsifier.run_falsification
            and getattr(self._falsifier.run_conditional_swing_falsification, "__func__", None)
            is StressFalsifier.run_conditional_swing_falsification
        )

        if max_workers > 1 and len(cell_tasks) > 1 and can_parallelize_in_workers:
            mp_ctx = multiprocessing.get_context("spawn")
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=max_workers,
                mp_context=mp_ctx,
            ) as executor:
                futures = [
                    executor.submit(_run_panel_cell_calibration_task, task)
                    for task in cell_tasks
                ]
                try:
                    for fut in concurrent.futures.as_completed(futures):
                        vb, lb, style_key, arch_id, selected_record = fut.result()
                        persisted = False
                        if persist and self._param_table is not None:
                            if selected_record is not None:
                                self._param_table.upsert_record(selected_record)
                                persisted = True
                                persisted_count += 1
                            elif hasattr(self._param_table, "deactivate_cell"):
                                self._param_table.deactivate_cell(vb, lb, style_key)
                        if on_cell_complete is not None:
                            on_cell_complete(
                                RegimeCellCalibrationResult(
                                    vol_bucket=vb,
                                    liq_bucket=lb,
                                    dominant_style=style_key,
                                    archetype_id=arch_id,
                                    selected_record=selected_record,
                                    evaluated_candidates=(),
                                    persisted=persisted,
                                )
                            )
                except Exception:
                    for pending_fut in futures:
                        pending_fut.cancel()
                    raise
        else:
            for task in cell_tasks:
                self._pending_panel_context = _PanelCalibrationContext(
                    execution_spec_grid=task["execution_spec_grid"],
                    bars_1m_by_symbol=task["bars_1m_by_symbol"],
                    bars_1d_by_symbol=task["bars_1d_by_symbol"],
                    alpha_scores_by_symbol=task["alpha_scores_by_symbol"],
                    total_family_trials=task["total_family_trials"],
                    holding_horizon_sessions=task["holding_horizon_sessions"],
                )
                try:
                    res = self.calibrate_regime_cell(
                        vol_bucket=task["vol_bucket"],
                        liq_bucket=task["liq_bucket"],
                        dominant_style=task["arch_id"],
                        symbol_bars=task["symbol_bars"],
                        candidate_param_grid=task["candidate_param_grid"],
                        raw_low_by_symbol_ts_ns=task["raw_low_by_symbol_ts_ns"],
                        persist=persist,
                    )
                finally:
                    self._pending_panel_context = None
                if res.persisted:
                    persisted_count += 1
                if on_cell_complete is not None:
                    on_cell_complete(res)

        return persisted_count
