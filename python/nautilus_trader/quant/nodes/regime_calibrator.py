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

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
import json
import math
import statistics
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
import uuid

import psycopg2
from psycopg2.extras import Json

from evolution_engine.config_center.envelope import load_envelope
from evolution_engine.config_center.schema import SystemConfig
from evolution_engine.db.dsn import resolve_database_dsn
from market_data_service.pipeline.us_equity_universe import get_gics_sector_map
from nautilus_trader.model import Bar, BarType, Price, Quantity
from nautilus_trader.quant.contracts.models import (
    ArchetypeStrategyConfig,
    BREAKOUT_LIMIT_COLLAR_ATR,
    MarketableLimitCollar,
    PlanItemContract,
    PlanRiskEnvelope,
    PlanSide,
    StrategyArchetypeId,
    default_limit_collar_atr_for_archetype,
)
from nautilus_trader.quant.nodes.backtest_runner import (
    MAX_LIMIT_COLLAR_MISSED_FILL_RATE,
    IsomorphicBacktestRunner,
    StressFalsificationReport,
    StressFalsifier,
)
from nautilus_trader.quant.strategies.empirical import rolling_atr

MIN_POOLED_REGIME_SAMPLE_TRADES: int = 100
VOL_BUCKET_LOWER_TERTILE: float = 1.0 / 3.0
VOL_BUCKET_UPPER_TERTILE: float = 2.0 / 3.0
LIQ_BUCKET_MEDIAN_QUANTILE: float = 0.50
MIN_EQUITY_TICK_USD: float = 0.01


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
    limit_collar_pct: Decimal = Decimal("0.3500")
    pooled_expectancy_r: Decimal = Decimal("0.0000")
    is_active: bool = True
    stress_report_json: Dict[str, Any] = field(default_factory=dict)
    calibrated_at: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

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
                            Json(record.stress_report_json),
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
        """Mark any active records (canonical or alias style) for `(vol_bucket, liq_bucket, archetype)` inactive."""
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
                        SET is_active = FALSE
                        WHERE vol_bucket = %s
                          AND liq_bucket = %s
                          AND is_active = TRUE
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
        collar_pct = Decimal(str(row[19])) if len(row) > 19 and row[19] is not None else Decimal("0.3500")
        exp_r = Decimal(f"{float(row[20]):.4f}") if len(row) > 20 and row[20] is not None else Decimal("0.0000")
        is_act = bool(row[21]) if len(row) > 21 and row[21] is not None else True
        return FrozenRegimeParamRecord(
            vol_bucket=VolBucket(str(row[0])),
            liq_bucket=LiqBucket(str(row[1])),
            dominant_style=str(row[2]),
            archetype_id=StrategyArchetypeId(str(row[3])),
            bar_timeframe=str(row[4]),
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
                        is_active
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
                        is_active
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

    @property
    def param_table(self) -> Optional[FrozenRegimeParamTable]:
        return self._param_table

    def _build_calibration_plan_item(
        self,
        *,
        symbol: str,
        archetype_id: StrategyArchetypeId,
        params: Mapping[str, Any],
        bars: Sequence[Bar],
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
        collar_pct = Decimal(str(params.get("limit_collar_pct", "0.35")))

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

        sorted_bars = sorted(bars, key=lambda b: int(b.ts_init))
        closes = [float(b.close) for b in sorted_bars]
        highs = [float(b.high) for b in sorted_bars]
        lows = [float(b.low) for b in sorted_bars]

        # Anchor sizing and stop loss strictly on the initial causal warmup slice so later
        # prices never leak into the plan envelope and `initial_stop_price` cannot sit
        # artificially below the full-window low.
        init_n = min(max(lookback_window, 14), len(closes))
        init_closes = closes[:init_n]
        init_highs = highs[:init_n]
        init_lows = lows[:init_n]
        ref_px = init_closes[-1] if init_closes else 100.0
        emp_atr = (
            rolling_atr(init_highs, init_lows, init_closes, min(14, len(init_closes)))
            if len(init_closes) >= 2
            else max(0.5, ref_px * 0.01)
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

        eff_atr = max(emp_atr, MIN_EQUITY_TICK_USD)
        stop_dist = max(float(atr_mult) * eff_atr, ref_px * min_stop_frac, MIN_EQUITY_TICK_USD)
        stop_px = (
            max(MIN_EQUITY_TICK_USD, min(ref_px - MIN_EQUITY_TICK_USD, ref_px - stop_dist))
            if ref_px > 2.0 * MIN_EQUITY_TICK_USD
            else MIN_EQUITY_TICK_USD
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
                math.ceil(min_notional_usd / max(ref_px, MIN_EQUITY_TICK_USD)),
            )
            max_qty_for_pos = max(
                1,
                int(max_pos_usd // max(ref_px, MIN_EQUITY_TICK_USD)),
            )
            risk_sized_qty = int(
                min(
                    unit_risk_usd / max(stop_dist, MIN_EQUITY_TICK_USD),
                    target_pos_usd / max(ref_px, MIN_EQUITY_TICK_USD),
                )
            )
            target_qty = Decimal(
                str(max(1, min(max_qty_for_pos, max(min_qty_for_notional, risk_sized_qty))))
            )

        raw_notional = float(target_qty) * max(ref_px, MIN_EQUITY_TICK_USD)
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
        last_ts_ns = int(sorted_bars[-1].ts_init) if sorted_bars else 0
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
            initial_stop_price=Decimal(f"{stop_px:.2f}"),
            take_profit_price=take_profit_dec,
            collar=MarketableLimitCollar(
                limit_collar_pct=collar_pct,
                limit_collar_atr=collar_atr,
            ),
            valid_until=valid_until,
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
            sizing_trace={"calibration": "offline_3x2_regime_panel"},
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
                str(sym).strip().upper(): list(b_seq)
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

        evaluations: List[RegimeCandidateEvaluation] = []
        effective_trials = max(1, len(grid))

        for raw_params in grid:
            params = dict(raw_params)
            reports: List[StressFalsificationReport] = []
            plan_items_by_sym: Dict[str, PlanItemContract] = {}
            raw_filled_trades = 0
            resolved_collar_atr = default_limit_collar_atr_for_archetype(arch_id)

            for sym, sym_bars in bars_by_symbol.items():
                plan_item, resolved_collar_atr = self._build_calibration_plan_item(
                    symbol=sym,
                    archetype_id=arch_id,
                    params=params,
                    bars=sym_bars,
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
                frozen_rec = FrozenRegimeParamRecord(
                    vol_bucket=vb,
                    liq_bucket=lb,
                    dominant_style=style_key,
                    archetype_id=arch_id,
                    bar_timeframe=str(params.get("bar_timeframe", "15m")),
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
                        str(params.get("limit_collar_pct", "0.35"))
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
                        "bar_timeframe": str(params.get("bar_timeframe", "15m")),
                        "reports": [r.to_dict() for r in reports],
                    },
                    calibrated_at=datetime.now(tz=timezone.utc),
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
    ) -> int:
        """Partitions a 2D panel `(T, N)` into 3x2 `(VolBucket, LiqBucket)` buckets and calibrates frozen parameters."""
        if not symbols or not timestamps or "close" not in panel_matrices:
            return 0
        import warnings
        import numpy as np

        all_vol_buckets = (VolBucket.LOW_VOL, VolBucket.MID_VOL, VolBucket.HIGH_VOL)
        all_liq_buckets = (LiqBucket.HIGH_LIQ_TIGHT_SPREAD, LiqBucket.LOW_LIQ_WIDE_SPREAD)

        closes_full = np.asarray(panel_matrices["close"], dtype=np.float64)
        if closes_full.ndim != 2 or closes_full.shape[0] < 30 or closes_full.shape[1] == 0:
            return 0
        tf_norm = str(bar_timeframe or "15m").strip().lower()
        if tf_norm not in _TF_BAR_SPECS:
            raise ValueError(f"Unsupported bar_timeframe {bar_timeframe!r}; expected one of {sorted(_TF_BAR_SPECS)}")
        bar_spec_str, bars_per_day, step_ns = _TF_BAR_SPECS[tf_norm]

        t_len, n_sym = closes_full.shape
        bar_slice = min(t_len, len(timestamps), max(30, int(max_bars)))
        if bar_slice < 30:
            return 0
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
                0.5,
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
            if persist:
                self._deactivate_all_cells(all_vol_buckets, all_liq_buckets)
            return 0

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

        apply_micro_floor = int(eligible_indices.size) >= 40
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
            if apply_micro_floor:
                med_px = float(median_closes[j])
                tick_bps = (MIN_EQUITY_TICK_USD / med_px * 10_000.0) if (math.isfinite(med_px) and med_px > 0.0) else float("inf")
                if tick_bps > 20.0 or float(adv_ranks[j]) < 0.20:
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

        ts_ns_arr = _build_monotonic_ts_ns(list(timestamps)[-bar_slice:], step_ns=step_ns)
        bucket_bars: Dict[Tuple[VolBucket, LiqBucket], Dict[str, List[Bar]]] = {}
        raw_low_by_sym: Dict[str, Dict[int, float]] = {}

        for (vb, lb), cell_cands in candidates_by_cell.items():
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
                    o_px = float(opens[t_idx, j]) if np.isfinite(opens[t_idx, j]) else c_px
                    h_px = max(o_px, c_px, float(highs[t_idx, j]) if np.isfinite(highs[t_idx, j]) else c_px)
                    l_px = max(0.01, min(o_px, c_px, float(lows[t_idx, j]) if np.isfinite(lows[t_idx, j]) else c_px))
                    v_qty = max(1, int(round(float(volumes[t_idx, j])))) if np.isfinite(volumes[t_idx, j]) else 100
                    ts_ns = ts_ns_arr[t_idx]
                    sym_bar_list.append(
                        Bar(
                            bar_type=bt,
                            open=Price(o_px, 2),
                            high=Price(h_px, 2),
                            low=Price(l_px, 2),
                            close=Price(c_px, 2),
                            volume=Quantity.from_int(v_qty),
                            ts_event=ts_ns,
                            ts_init=ts_ns,
                        )
                    )
                    r_lo = float(raw_lows[t_idx, j]) if np.isfinite(raw_lows[t_idx, j]) else l_px
                    sym_raw_low_map[ts_ns] = min(l_px, max(0.01, r_lo))
                if len(sym_bar_list) >= 30:
                    cell_map[sym] = sym_bar_list
                    raw_low_by_sym[sym] = sym_raw_low_map

        persisted_count = 0
        for vb in all_vol_buckets:
            for lb in all_liq_buckets:
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
                            self._param_table.deactivate_cell(vb, lb, arch_id)
                    continue
                for arch_id in (
                    StrategyArchetypeId.TREND_MOMENTUM,
                    StrategyArchetypeId.VOLATILITY_BREAKOUT,
                    StrategyArchetypeId.MEAN_REVERSION,
                ):
                    tf_grid = tuple(
                        {**cand, "bar_timeframe": tf_norm}
                        for cand in DEFAULT_REGIME_CANDIDATE_GRIDS[arch_id]
                    )
                    res = self.calibrate_regime_cell(
                        vol_bucket=vb,
                        liq_bucket=lb,
                        dominant_style=arch_id,
                        symbol_bars=sym_map,
                        candidate_param_grid=tf_grid,
                        raw_low_by_symbol_ts_ns=raw_low_by_sym,
                        persist=persist,
                    )
                    if res.persisted:
                        persisted_count += 1
        return persisted_count
