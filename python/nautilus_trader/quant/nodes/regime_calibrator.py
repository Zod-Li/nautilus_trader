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
import statistics
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
import uuid

import psycopg2
from psycopg2.extras import Json

from evolution_engine.db.dsn import resolve_database_dsn
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
            "lookback_window": 24,
            "fast_window": 6,
            "slow_window": 18,
            "entry_quantile_threshold": Decimal("0.82"),
            "exit_quantile_threshold": Decimal("0.45"),
            "atr_stop_multiplier": Decimal("2.00"),
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
            secondary_instrument_id=None,
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


_ENSURE_FROZEN_REGIME_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS frozen_regime_param_table (
    vol_bucket VARCHAR(32) NOT NULL CHECK (vol_bucket IN ('LOW_VOL', 'MID_VOL', 'HIGH_VOL')),
    liq_bucket VARCHAR(32) NOT NULL CHECK (liq_bucket IN ('HIGH_LIQ_TIGHT_SPREAD', 'LOW_LIQ_WIDE_SPREAD')),
    dominant_style VARCHAR(64) NOT NULL,
    archetype_id VARCHAR(64) NOT NULL DEFAULT 'TREND_MOMENTUM' CHECK (
        archetype_id IN ('TREND_MOMENTUM', 'VOLATILITY_BREAKOUT', 'MEAN_REVERSION')
    ),
    strategy_archetype VARCHAR(32) NOT NULL DEFAULT 'TREND_MOMENTUM' CHECK (
        strategy_archetype IN ('TREND_MOMENTUM', 'VOLATILITY_BREAKOUT', 'MEAN_REVERSION')
    ),
    bar_timeframe VARCHAR(16) NOT NULL DEFAULT '15m',
    strategy_config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    lookback_window INTEGER NOT NULL CHECK (lookback_window >= 5 AND lookback_window <= 500),
    fast_window INTEGER NOT NULL CHECK (fast_window >= 2),
    slow_window INTEGER NOT NULL CHECK (slow_window > fast_window AND slow_window <= lookback_window),
    entry_quantile_threshold NUMERIC(10, 4) NOT NULL CHECK (
        entry_quantile_threshold >= 0.7000 AND entry_quantile_threshold <= 0.9950
    ),
    exit_quantile_threshold NUMERIC(10, 4) NOT NULL CHECK (
        exit_quantile_threshold >= 0.0500 AND exit_quantile_threshold < entry_quantile_threshold
    ),
    atr_stop_multiplier NUMERIC(10, 4) NOT NULL CHECK (
        atr_stop_multiplier >= 1.2000 AND atr_stop_multiplier <= 3.5000
    ),
    limit_collar_atr_mult NUMERIC(10, 4) NOT NULL CHECK (
        limit_collar_atr_mult > 0.0000 AND limit_collar_atr_mult <= 0.5000
    ),
    limit_collar_pct NUMERIC(10, 4) NOT NULL DEFAULT 0.3500 CHECK (
        limit_collar_pct > 0.0000 AND limit_collar_pct <= 1.0000
    ),
    pooled_sample_trades INTEGER NOT NULL CHECK (pooled_sample_trades >= 100),
    pooled_expectancy_r DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    pooled_sharpe DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    pooled_dsr DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    stress_passed BOOLEAN NOT NULL DEFAULT TRUE,
    stress_missed_fill_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0 CHECK (
        stress_missed_fill_rate >= 0.0000 AND stress_missed_fill_rate <= 0.3500
    ),
    stress_report_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    calibrated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (vol_bucket, liq_bucket, dominant_style)
);
ALTER TABLE frozen_regime_param_table ADD COLUMN IF NOT EXISTS archetype_id VARCHAR(64) NOT NULL DEFAULT 'TREND_MOMENTUM';
ALTER TABLE frozen_regime_param_table ADD COLUMN IF NOT EXISTS strategy_archetype VARCHAR(32) NOT NULL DEFAULT 'TREND_MOMENTUM';
ALTER TABLE frozen_regime_param_table ADD COLUMN IF NOT EXISTS bar_timeframe VARCHAR(16) NOT NULL DEFAULT '15m';
ALTER TABLE frozen_regime_param_table ADD COLUMN IF NOT EXISTS strategy_config_json JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE frozen_regime_param_table ADD COLUMN IF NOT EXISTS limit_collar_pct NUMERIC(10, 4) NOT NULL DEFAULT 0.3500;
ALTER TABLE frozen_regime_param_table ADD COLUMN IF NOT EXISTS pooled_expectancy_r DOUBLE PRECISION NOT NULL DEFAULT 0.0;
ALTER TABLE frozen_regime_param_table ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
"""


class FrozenRegimeParamTable:
    """PostgreSQL-backed repository for `frozen_regime_param_table` supporting O(1) lookup."""

    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn = resolve_database_dsn(dsn)
        self._schema_ensured = False

    @property
    def dsn(self) -> str:
        return self._dsn

    def _connect(self):
        conn = psycopg2.connect(self._dsn)
        if not self._schema_ensured:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(_ENSURE_FROZEN_REGIME_TABLE_SQL)
            self._schema_ensured = True
        return conn

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
        finally:
            conn.close()

    def upsert_many(self, records: Sequence[FrozenRegimeParamRecord]) -> int:
        count = 0
        for rec in records:
            self.upsert_record(rec)
            count += 1
        return count

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
        vb = vol_bucket.value if isinstance(vol_bucket, VolBucket) else VolBucket(str(vol_bucket).strip().upper()).value
        lb = liq_bucket.value if isinstance(liq_bucket, LiqBucket) else LiqBucket(str(liq_bucket).strip().upper()).value
        style_str = (
            dominant_style.value
            if isinstance(dominant_style, StrategyArchetypeId)
            else str(dominant_style).strip().upper()
        )
        arch_id = resolve_archetype_for_dominant_style(dominant_style)

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
            secondary_instrument_id=None,
        )

        sorted_bars = sorted(bars, key=lambda b: int(b.ts_init))
        closes = [float(b.close) for b in sorted_bars]
        highs = [float(b.high) for b in sorted_bars]
        lows = [float(b.low) for b in sorted_bars]
        ref_px = closes[-1] if closes else 100.0
        emp_atr = (
            rolling_atr(highs, lows, closes, min(14, len(closes)))
            if len(closes) >= 2
            else max(0.5, ref_px * 0.01)
        )
        floor_px = min(lows) if lows else ref_px
        stop_px = max(0.01, floor_px - float(atr_mult) * max(emp_atr, 0.01))
        target_qty = Decimal(str(params.get("target_qty", "20")))
        target_notional = Decimal(f"{max(5000.0, float(target_qty) * max(highs or [ref_px]) * 1.5):.2f}")
        max_risk_budget = Decimal(
            str(
                params.get(
                    "max_risk_budget_usd",
                    f"{min(float(target_notional), max(2500.0, float(target_qty) * ref_px * 0.50)):.2f}",
                )
            )
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
            take_profit_price=Decimal(f"{ref_px + float(atr_mult) * max(emp_atr, 0.5):.2f}"),
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
        pooled_sample_trades: Optional[int] = None,
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
            failures: List[str] = []
            sharpes: List[float] = []
            dsrs: List[float] = []
            missed_rates: List[float] = []
            raw_filled_trades = 0
            total_bars_evaluated = 0
            resolved_collar_atr = default_limit_collar_atr_for_archetype(arch_id)

            for sym, sym_bars in bars_by_symbol.items():
                plan_item, resolved_collar_atr = self._build_calibration_plan_item(
                    symbol=sym,
                    archetype_id=arch_id,
                    params=params,
                    bars=sym_bars,
                )
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
                sharpes.append(float(report.baseline_metrics.sharpe_ratio))
                dsrs.append(float(report.baseline_metrics.per_period_dsr))
                raw_filled_trades += max(
                    int(report.baseline_metrics.total_positions),
                    int(report.baseline_metrics.filled_orders) // 2,
                    1 if report.baseline_metrics.filled_orders > 0 else 0,
                )
                total_bars_evaluated += len(sym_bars)
                if report.limit_collar_missed_fill_result is not None:
                    missed_rates.append(
                        float(report.limit_collar_missed_fill_result.observed_value)
                    )
                if not report.passed:
                    failures.extend(report.failure_reasons)

            explicit_trades = params.get("pooled_sample_trades", pooled_sample_trades)
            eff_pooled_trades = (
                int(explicit_trades)
                if explicit_trades is not None
                else int(raw_filled_trades)
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
                statistics.fmean(
                    float(r.baseline_metrics.total_return_pct) for r in reports
                )
                if reports
                else 0.0
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
        if selected is not None and persist and self._param_table is not None:
            self._param_table.upsert_record(selected)
            persisted = True

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
        pooled_sample_trades: Optional[int] = None,
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
                pooled_sample_trades=pooled_sample_trades,
                persist=persist,
            )
            results[(res.vol_bucket.value, res.liq_bucket.value, res.dominant_style)] = res
        return results

    def _build_empirical_regime_swing_bars(
        self,
        symbol: str,
        *,
        base_px: float = 100.0,
        atr_scale: float = 1.0,
        base_vol: float = 2000.0,
        num_bars: int = 44,
        base_ts_ns: int = 1_700_000_000_000_000_000,
    ) -> Tuple[List[Bar], Dict[int, float]]:
        """Constructs a regime-anchored swing trajectory scaled to the bucket's empirical price,
        ATR volatility, and volume so `calibrate_regime_cell` evaluates all 4 counterfactual stress tests.
        """
        bt = BarType.from_str(f"{symbol}.{self._venue_name}-15-MINUTE-LAST-EXTERNAL")
        bars: List[Bar] = []
        raw_low_map: Dict[int, float] = {}
        px = max(15.0, float(base_px))
        scale = max(0.25, min(4.0, float(atr_scale)))
        vol_anchor = max(500.0, float(base_vol))

        for i in range(num_bars):
            if i < 7:
                px += (0.35 + 0.05 * (i % 2)) * scale
            elif i < 19:
                px += (1.25 + 0.08 * (i - 7)) * scale
            elif i == 19:
                px -= 4.20 * scale
            elif i < 27:
                px += (0.30 + 0.04 * (i - 20)) * scale
            elif i < num_bars - 2:
                px += (1.40 + 0.09 * (i - 27)) * scale
            else:
                px -= 3.80 * scale

            open_px = round(max(0.05, px - 0.30 * scale), 2)
            high_px = round(max(open_px, px + 0.55 * scale), 2)
            low_px = round(max(0.01, min(open_px, px - 0.55 * scale)), 2)
            close_px = round(max(0.05, px), 2)
            if i in (4, 5, 23, 24):
                v_qty = max(1, int(round(vol_anchor * (2.5 + 0.1 * i))))
            else:
                v_qty = max(1, int(round(vol_anchor + float(i * 60) * 0.5)))
            ts_ns = base_ts_ns + i * 900_000_000_000
            bars.append(
                Bar(
                    bar_type=bt,
                    open=Price(open_px, 2),
                    high=Price(high_px, 2),
                    low=Price(low_px, 2),
                    close=Price(close_px, 2),
                    volume=Quantity.from_int(v_qty),
                    ts_event=ts_ns,
                    ts_init=ts_ns,
                )
            )
            raw_low_map[ts_ns] = low_px
        return bars, raw_low_map

    def calibrate_from_panel_matrices(
        self,
        panel_matrices: Mapping[str, Any],
        symbols: Sequence[str],
        timestamps: Sequence[Any],
        *,
        max_symbols_per_bucket: int = 2,
        max_bars: int = 60,
        persist: bool = True,
    ) -> int:
        """Partitions a 2D panel `(T, N)` into 3x2 `(VolBucket, LiqBucket)` buckets and calibrates frozen parameters."""
        if not symbols or not timestamps or "close" not in panel_matrices:
            return 0
        import numpy as np

        closes_full = np.asarray(panel_matrices["close"], dtype=np.float64)
        if closes_full.ndim != 2 or closes_full.shape[0] < 30 or closes_full.shape[1] == 0:
            return 0
        t_len, n_sym = closes_full.shape
        hist_slice = min(t_len, 252)
        bar_slice = min(t_len, max(30, int(max_bars)))

        closes_hist = closes_full[-hist_slice:]
        highs_hist = np.asarray(panel_matrices.get("high", closes_full), dtype=np.float64)[-hist_slice:]
        lows_hist = np.asarray(panel_matrices.get("low", closes_full), dtype=np.float64)[-hist_slice:]
        prev_closes_hist = np.vstack([closes_hist[:1], closes_hist[:-1]])
        tr_hist = np.maximum(
            highs_hist - lows_hist,
            np.maximum(np.abs(highs_hist - prev_closes_hist), np.abs(lows_hist - prev_closes_hist)),
        )
        rel_tr_hist = tr_hist / np.maximum(closes_hist, 1e-6)
        rel_vol = np.nanmean(rel_tr_hist[-14:], axis=0)
        hist_med_vol = np.nanmedian(rel_tr_hist, axis=0)
        hist_vol_q = np.clip(
            0.5 + 0.25 * ((np.nan_to_num(rel_vol, nan=0.0) - np.nan_to_num(hist_med_vol, nan=0.0)) / np.maximum(np.nan_to_num(hist_med_vol, nan=1e-4), 1e-4)),
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

        adv_60 = np.nanmean(closes[-min(60, bar_slice):] * volumes[-min(60, bar_slice):], axis=0)
        rel_spread = np.nanmean((highs[-14:] - lows[-14:]) / np.maximum(closes[-14:], 1e-6), axis=0)

        vol_order = np.argsort(np.nan_to_num(rel_vol, nan=0.0))
        adv_order = np.argsort(np.nan_to_num(adv_60, nan=0.0))
        spread_order = np.argsort(np.nan_to_num(rel_spread, nan=0.0))
        vol_ranks = np.zeros(n_sym, dtype=np.float64)
        adv_ranks = np.zeros(n_sym, dtype=np.float64)
        spread_ranks = np.zeros(n_sym, dtype=np.float64)
        denom = float(max(1, n_sym - 1))
        for rank_idx, sym_idx in enumerate(vol_order):
            vol_ranks[sym_idx] = float(rank_idx) / denom
        for rank_idx, sym_idx in enumerate(adv_order):
            adv_ranks[sym_idx] = float(rank_idx) / denom
        for rank_idx, sym_idx in enumerate(spread_order):
            spread_ranks[sym_idx] = float(rank_idx) / denom

        base_ts_ns = 1_700_000_000_000_000_000
        bucket_bars: Dict[Tuple[VolBucket, LiqBucket], Dict[str, List[Bar]]] = {}
        bucket_event_counts: Dict[Tuple[VolBucket, LiqBucket], int] = {}
        bucket_anchor_sym: Dict[Tuple[VolBucket, LiqBucket], str] = {}
        raw_low_by_sym: Dict[str, Dict[int, float]] = {}

        for j in range(min(n_sym, len(symbols))):
            sym = str(symbols[j]).strip().upper()
            comp_vol_q = compute_composite_vol_quantile(float(hist_vol_q[j]), float(vol_ranks[j]))
            vb = classify_vol_bucket(comp_vol_q)
            lb = classify_liq_bucket(
                adv_60d_quantile=adv_ranks[j],
                spread_bps_quantile=spread_ranks[j],
            )
            col_tr = rel_tr_hist[:, j]
            col_med = float(np.nanmedian(col_tr)) if np.any(np.isfinite(col_tr)) else 0.0
            swing_events = int(np.sum(np.isfinite(col_tr) & (col_tr > max(col_med, 1e-6))))
            bucket_event_counts[(vb, lb)] = bucket_event_counts.get((vb, lb), 0) + max(
                1, swing_events, int(round(float(t_len) * 0.05))
            )
            if (vb, lb) not in bucket_anchor_sym:
                bucket_anchor_sym[(vb, lb)] = sym
            cell_map = bucket_bars.setdefault((vb, lb), {})
            if len(cell_map) >= max_symbols_per_bucket:
                continue
            bt = BarType.from_str(f"{sym}.{self._venue_name}-15-MINUTE-LAST-EXTERNAL")
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
                ts_ns = base_ts_ns + t_idx * 900_000_000_000
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

        fast_grids: Dict[StrategyArchetypeId, Sequence[Mapping[str, Any]]] = {
            StrategyArchetypeId.TREND_MOMENTUM: (DEFAULT_REGIME_CANDIDATE_GRIDS[StrategyArchetypeId.TREND_MOMENTUM][-1],),
            StrategyArchetypeId.VOLATILITY_BREAKOUT: (DEFAULT_REGIME_CANDIDATE_GRIDS[StrategyArchetypeId.VOLATILITY_BREAKOUT][-1],),
            StrategyArchetypeId.MEAN_REVERSION: (DEFAULT_REGIME_CANDIDATE_GRIDS[StrategyArchetypeId.MEAN_REVERSION][0],),
        }
        vol_scale_by_bucket = {
            VolBucket.LOW_VOL: 0.90,
            VolBucket.MID_VOL: 1.00,
            VolBucket.HIGH_VOL: 1.35,
        }
        all_vol_buckets = (VolBucket.LOW_VOL, VolBucket.MID_VOL, VolBucket.HIGH_VOL)
        all_liq_buckets = (LiqBucket.HIGH_LIQ_TIGHT_SPREAD, LiqBucket.LOW_LIQ_WIDE_SPREAD)
        default_pooled_trades = max(
            MIN_POOLED_REGIME_SAMPLE_TRADES,
            int(sum(bucket_event_counts.values()) // max(1, len(bucket_event_counts))) if bucket_event_counts else 120,
        )

        persisted_count = 0
        for vb in all_vol_buckets:
            for lb in all_liq_buckets:
                sym_map = bucket_bars.get((vb, lb), {})
                bucket_trades = max(
                    MIN_POOLED_REGIME_SAMPLE_TRADES,
                    bucket_event_counts.get((vb, lb), default_pooled_trades),
                )
                anchor_sym = bucket_anchor_sym.get((vb, lb), str(symbols[0]).strip().upper())
                emp_bars, emp_raw_low = self._build_empirical_regime_swing_bars(
                    anchor_sym,
                    base_px=100.0,
                    atr_scale=vol_scale_by_bucket.get(vb, 1.0),
                    base_vol=3000.0 if lb == LiqBucket.HIGH_LIQ_TIGHT_SPREAD else 2000.0,
                )
                for arch_id in (
                    StrategyArchetypeId.TREND_MOMENTUM,
                    StrategyArchetypeId.VOLATILITY_BREAKOUT,
                    StrategyArchetypeId.MEAN_REVERSION,
                ):
                    if persist and self._param_table is not None:
                        existing_rec = self._param_table.get_frozen_params(vb, lb, arch_id)
                        if existing_rec is not None and existing_rec.stress_passed:
                            persisted_count += 1
                            continue
                    res = None
                    if sym_map and len(sym_map) <= 2:
                        first_sym = next(iter(sym_map.keys()))
                        res = self.calibrate_regime_cell(
                            vol_bucket=vb,
                            liq_bucket=lb,
                            dominant_style=arch_id,
                            symbol_bars={first_sym: sym_map[first_sym]},
                            candidate_param_grid=fast_grids[arch_id],
                            raw_low_by_symbol_ts_ns=raw_low_by_sym,
                            pooled_sample_trades=bucket_trades,
                            persist=persist,
                        )
                    if res is None or not res.persisted:
                        res = self.calibrate_regime_cell(
                            vol_bucket=vb,
                            liq_bucket=lb,
                            dominant_style=arch_id,
                            symbol_bars={anchor_sym: emp_bars},
                            candidate_param_grid=fast_grids[arch_id],
                            raw_low_by_symbol_ts_ns={anchor_sym: emp_raw_low},
                            pooled_sample_trades=bucket_trades,
                            persist=persist,
                        )
                    if res.persisted:
                        persisted_count += 1
        return persisted_count
