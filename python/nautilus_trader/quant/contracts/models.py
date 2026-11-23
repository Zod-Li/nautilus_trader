"""Immutable domain models for the 1:1 Isomorphic Trading Plan Contract (`PlanItem = StrategyConfig + PlanRiskEnvelope`)."""

from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN, Decimal
from enum import Enum
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional
from uuid import UUID
from zoneinfo import ZoneInfo

from evolution_engine.config_center.envelope import Envelope, load_envelope
from evolution_engine.config_center.schema import SystemConfig

NY_SESSION_TZ = ZoneInfo("America/New_York")

SUPPORTED_TIMEFRAMES = ("1m", "15m", "1h", "4h", "1d", "1w")
BAR_TIMEFRAME_DELTAS: Dict[str, timedelta] = {
    "1m": timedelta(minutes=1),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "1d": timedelta(days=1),
    "1w": timedelta(days=7),
}
VALID_ITEM_STATUSES = (
    "ARMED",
    "TRIGGERED",
    "PARTIALLY_FILLED",
    "FILLED",
    "EXPIRED",
    "CANCELLED",
    "CLOSED",
)


def compute_intraday_entry_valid_until(
    trading_date: date,
    bar_timeframe: str = "15m",
    entry_ttl_bars: int = 2,
    *,
    rth_open_utc: Optional[datetime] = None,
    rth_close_utc: Optional[datetime] = None,
    anchor_ts_utc: Optional[datetime] = None,
) -> datetime:
    """Computes the intraday entry `valid_until` UTC timestamp for a daily `TradingPlan` item.

    Converts `entry_ttl_bars` into `entry_ttl_bars * bar_timeframe` duration anchored at
    regular trading hours open (`09:30:00 America/New_York`, or `anchor_ts_utc` when within RTH)
    and strictly clamps the result to `rth_close_utc` (`16:00:00 America/New_York`) on `trading_date`.
    """
    if rth_open_utc is None:
        rth_open_et = datetime(
            trading_date.year,
            trading_date.month,
            trading_date.day,
            9,
            30,
            0,
            tzinfo=NY_SESSION_TZ,
        )
        open_utc = rth_open_et.astimezone(timezone.utc)
    else:
        open_utc = (
            rth_open_utc.astimezone(timezone.utc)
            if rth_open_utc.tzinfo is not None
            else rth_open_utc.replace(tzinfo=timezone.utc)
        )

    if rth_close_utc is None:
        rth_close_et = datetime(
            trading_date.year,
            trading_date.month,
            trading_date.day,
            16,
            0,
            0,
            tzinfo=NY_SESSION_TZ,
        )
        close_utc = rth_close_et.astimezone(timezone.utc)
    else:
        close_utc = (
            rth_close_utc.astimezone(timezone.utc)
            if rth_close_utc.tzinfo is not None
            else rth_close_utc.replace(tzinfo=timezone.utc)
        )

    if close_utc < open_utc:
        close_utc = open_utc

    tf_key = str(bar_timeframe or "15m").strip()
    bar_delta = BAR_TIMEFRAME_DELTAS.get(tf_key, timedelta(minutes=15))
    ttl_bars = max(1, int(entry_ttl_bars))

    if bar_delta >= timedelta(days=1):
        return close_utc

    if anchor_ts_utc is not None:
        norm_anchor = (
            anchor_ts_utc.astimezone(timezone.utc)
            if anchor_ts_utc.tzinfo is not None
            else anchor_ts_utc.replace(tzinfo=timezone.utc)
        )
        start_anchor = (
            open_utc if norm_anchor < open_utc else min(norm_anchor, close_utc)
        )
    else:
        start_anchor = open_utc

    raw_expiry = start_anchor + (bar_delta * ttl_bars)
    return min(raw_expiry, close_utc)


class PlanContractValidationError(ValueError):
    """Raised when a PlanItemContract, StrategyConfig, or PlanRiskEnvelope violates a safety invariant."""


class StrategyArchetypeId(str, Enum):
    """Pre-audited pure Long-Only Nautilus Strategy archetypes eligible for offline regime calibration and live execution."""

    TREND_MOMENTUM = "TREND_MOMENTUM"
    VOLATILITY_BREAKOUT = "VOLATILITY_BREAKOUT"
    MEAN_REVERSION = "MEAN_REVERSION"


BREAKOUT_ARCHETYPE_IDS = frozenset(
    {
        StrategyArchetypeId.TREND_MOMENTUM,
        StrategyArchetypeId.VOLATILITY_BREAKOUT,
    }
)
BREAKOUT_LIMIT_COLLAR_ATR = Decimal("0.25")
MEAN_REVERSION_LIMIT_COLLAR_ATR = Decimal("0.10")


def default_limit_collar_atr_for_archetype(
    archetype_id: StrategyArchetypeId | str,
) -> Decimal:
    """Returns the canonical limit collar ATR multiplier for `archetype_id`:
    - `0.25` for breakout/momentum archetypes (`TREND_MOMENTUM`, `VOLATILITY_BREAKOUT`)
    - `0.10` for mean reversion (`MEAN_REVERSION`)
    """
    resolved = (
        archetype_id
        if isinstance(archetype_id, StrategyArchetypeId)
        else StrategyArchetypeId(str(archetype_id).strip().upper())
    )
    if resolved in BREAKOUT_ARCHETYPE_IDS:
        return BREAKOUT_LIMIT_COLLAR_ATR
    return MEAN_REVERSION_LIMIT_COLLAR_ATR


class PlanSide(str, Enum):
    """Directional side approved by the TradingPlan."""

    LONG = "long"
    SHORT = "short"


class TriggerPolicy(str, Enum):
    """Intraday entry trigger policy persisted in `trading_plan_items.execution_spec`."""

    FIRST_WINDOW_IMMEDIATE = "FIRST_WINDOW_IMMEDIATE"
    HYBRID_15M_1M_BREAKOUT = "HYBRID_15M_1M_BREAKOUT"
    INTRADAY_AVWAP_PULLBACK = "INTRADAY_AVWAP_PULLBACK"


class ExecAlgorithm(str, Enum):
    """Execution algorithm mode persisted in `trading_plan_items.execution_spec`."""

    COLLARED_TWAP = "COLLARED_TWAP"
    DIRECT_COLLARED_LIMIT = "DIRECT_COLLARED_LIMIT"


SUB_PENNY_TICK_RULE_612 = Decimal("0.0001")
PENNY_TICK_RULE_612 = Decimal("0.01")
RULE_612_THRESHOLD_USD = Decimal("1.00")


def rule_612_tick_size(price: Decimal) -> Decimal:
    """Returns Reg NMS Rule 612 minimum pricing increment (`$0.0001` below `$1.00`, `$0.01` >= `$1.00`)."""
    return SUB_PENNY_TICK_RULE_612 if price < RULE_612_THRESHOLD_USD else PENNY_TICK_RULE_612


def quantize_price_rule_612(
    price: Decimal,
    *,
    side: Optional[PlanSide | str] = None,
    tick_size: Optional[Decimal] = None,
) -> Decimal:
    """Quantizes `price` per Reg NMS Rule 612 (or explicit `tick_size`).

    When `side` is supplied, floors BUY (`LONG`) prices and ceils SELL (`SHORT`) prices
    so directional collars never become more aggressive than the unrounded limit.
    When `side` is omitted, uses `ROUND_HALF_EVEN` matching Rust `RustPlanItemEnvelope`.
    """
    px = _to_decimal(price, "price")
    if tick_size is not None:
        tick = _to_decimal(tick_size, "tick_size")
        if tick <= Decimal("0"):
            raise PlanContractValidationError(f"tick_size must be > 0, got {tick}")
    else:
        tick = rule_612_tick_size(px)

    if side is None:
        if tick_size is None:
            if px < RULE_612_THRESHOLD_USD:
                rounded = max(
                    SUB_PENNY_TICK_RULE_612,
                    px.quantize(SUB_PENNY_TICK_RULE_612, rounding=ROUND_HALF_EVEN),
                )
                if rounded >= RULE_612_THRESHOLD_USD:
                    return max(
                        PENNY_TICK_RULE_612,
                        px.quantize(PENNY_TICK_RULE_612, rounding=ROUND_HALF_EVEN),
                    )
                return rounded
            return max(
                PENNY_TICK_RULE_612,
                px.quantize(PENNY_TICK_RULE_612, rounding=ROUND_HALF_EVEN),
            )
        steps = (px / tick).to_integral_value(rounding=ROUND_HALF_EVEN)
        return max(tick, steps * tick)

    is_long = (
        side == PlanSide.LONG
        or str(getattr(side, "value", side)).strip().upper() in ("LONG", "BUY")
    )
    rounding_mode = ROUND_FLOOR if is_long else ROUND_CEILING
    if tick_size is None and px < RULE_612_THRESHOLD_USD:
        steps = (px / SUB_PENNY_TICK_RULE_612).to_integral_value(rounding=rounding_mode)
        rounded = max(SUB_PENNY_TICK_RULE_612, steps * SUB_PENNY_TICK_RULE_612)
        if rounded >= RULE_612_THRESHOLD_USD:
            penny_steps = (px / PENNY_TICK_RULE_612).to_integral_value(rounding=rounding_mode)
            return max(PENNY_TICK_RULE_612, penny_steps * PENNY_TICK_RULE_612)
        return rounded

    steps = (px / tick).to_integral_value(rounding=rounding_mode)
    return max(tick, steps * tick)


@dataclass(frozen=True)
class ExecutionSpec:
    """Isomorphic intraday execution specification (`TriggerPolicy x ExecAlgorithm`) persisted in PG `execution_spec`."""

    trigger_policy: TriggerPolicy = TriggerPolicy.FIRST_WINDOW_IMMEDIATE
    exec_algorithm: ExecAlgorithm = ExecAlgorithm.COLLARED_TWAP
    window_quantile: Optional[Decimal] = None
    bar_timeframe: str = "15m"

    def __post_init__(self) -> None:
        raw_tp = (
            self.trigger_policy.value
            if isinstance(self.trigger_policy, TriggerPolicy)
            else str(self.trigger_policy or TriggerPolicy.FIRST_WINDOW_IMMEDIATE.value)
        )
        try:
            norm_tp = TriggerPolicy(raw_tp.strip().upper())
        except ValueError as exc:
            raise PlanContractValidationError(
                f"Invalid trigger_policy {self.trigger_policy!r}"
            ) from exc
        object.__setattr__(self, "trigger_policy", norm_tp)

        raw_ea = (
            self.exec_algorithm.value
            if isinstance(self.exec_algorithm, ExecAlgorithm)
            else str(self.exec_algorithm or ExecAlgorithm.COLLARED_TWAP.value)
        )
        try:
            norm_ea = ExecAlgorithm(raw_ea.strip().upper())
        except ValueError as exc:
            raise PlanContractValidationError(
                f"Invalid exec_algorithm {self.exec_algorithm!r}"
            ) from exc
        object.__setattr__(self, "exec_algorithm", norm_ea)

        if self.window_quantile is not None and str(self.window_quantile).strip() != "":
            wq = _to_decimal(self.window_quantile, "window_quantile")
            if not (Decimal("0") < wq < Decimal("1")):
                raise PlanContractValidationError(
                    f"window_quantile ({wq}) must be in (0, 1)"
                )
            object.__setattr__(self, "window_quantile", wq)
        else:
            object.__setattr__(self, "window_quantile", None)

        tf = str(self.bar_timeframe or "15m").strip()
        if tf not in SUPPORTED_TIMEFRAMES:
            raise PlanContractValidationError(
                f"ExecutionSpec.bar_timeframe '{tf}' not in {SUPPORTED_TIMEFRAMES}"
            )
        object.__setattr__(self, "bar_timeframe", tf)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trigger_policy": self.trigger_policy.value,
            "exec_algorithm": self.exec_algorithm.value,
            "window_quantile": (
                float(self.window_quantile) if self.window_quantile is not None else None
            ),
            "bar_timeframe": self.bar_timeframe,
        }

    @classmethod
    def from_dict(
        cls,
        data: Optional[Mapping[str, Any]],
        *,
        default_bar_timeframe: str = "15m",
    ) -> ExecutionSpec:
        if data is None:
            return cls(bar_timeframe=default_bar_timeframe)
        if not isinstance(data, Mapping):
            raise PlanContractValidationError("ExecutionSpec data must be a mapping")
        if not data:
            return cls(bar_timeframe=default_bar_timeframe)
        raw_tp = data.get("trigger_policy") or TriggerPolicy.FIRST_WINDOW_IMMEDIATE.value
        raw_ea = data.get("exec_algorithm") or ExecAlgorithm.COLLARED_TWAP.value
        raw_wq = data.get("window_quantile")
        raw_tf = data.get("bar_timeframe") or default_bar_timeframe
        return cls(
            trigger_policy=TriggerPolicy(str(raw_tp).strip().upper()),
            exec_algorithm=ExecAlgorithm(str(raw_ea).strip().upper()),
            window_quantile=(
                _to_decimal(raw_wq, "window_quantile")
                if raw_wq is not None and str(raw_wq).strip() != ""
                else None
            ),
            bar_timeframe=str(raw_tf).strip(),
        )


def _to_decimal(val: Any, field_name: str) -> Decimal:
    if isinstance(val, bool) or val is None:
        raise PlanContractValidationError(f"{field_name} must be a valid decimal, got {val!r}")
    try:
        dec = Decimal(str(val))
    except Exception as exc:
        raise PlanContractValidationError(f"{field_name} invalid decimal {val!r}: {exc}") from exc
    if not dec.is_finite():
        raise PlanContractValidationError(f"{field_name} must be finite, got {val!r}")
    return dec


@dataclass(frozen=True)
class ArchetypeStrategyConfig:
    """Regime-calibrated, empirical-distribution-driven parameters for a pre-audited Strategy archetype.

    All thresholds are expressed as rolling empirical quantiles or volatility-scaled Z-scores/ATR multipliers
    (strictly adhering to Red Line R9: zero static retail magic numbers).
    """

    archetype_id: StrategyArchetypeId
    instrument_id: str
    bar_timeframe: str
    lookback_window: int
    fast_window: int
    slow_window: int
    entry_quantile_threshold: Decimal
    exit_quantile_threshold: Decimal
    atr_stop_multiplier: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "archetype_id", StrategyArchetypeId(self.archetype_id))
        object.__setattr__(
            self,
            "entry_quantile_threshold",
            _to_decimal(self.entry_quantile_threshold, "entry_quantile_threshold"),
        )
        object.__setattr__(
            self,
            "exit_quantile_threshold",
            _to_decimal(self.exit_quantile_threshold, "exit_quantile_threshold"),
        )
        object.__setattr__(
            self,
            "atr_stop_multiplier",
            _to_decimal(self.atr_stop_multiplier, "atr_stop_multiplier"),
        )
        self.validate()

    def validate(self) -> None:
        inst = self.instrument_id.strip()
        if "." not in inst or any( ch.isspace() for ch in inst ):
            raise PlanContractValidationError(
                f"instrument_id '{self.instrument_id}' must be in 'SYMBOL.VENUE' format"
            )
        sym_part, venue_part = inst.split(".", 1)
        if not sym_part or not venue_part:
            raise PlanContractValidationError(
                f"instrument_id '{self.instrument_id}' must have non-empty symbol and venue"
            )

        if self.bar_timeframe not in SUPPORTED_TIMEFRAMES:
            raise PlanContractValidationError(
                f"bar_timeframe '{self.bar_timeframe}' not in {SUPPORTED_TIMEFRAMES}"
            )

        if isinstance(self.lookback_window, bool) or self.lookback_window < 5:
            raise PlanContractValidationError(
                f"lookback_window must be >= 5, got {self.lookback_window}"
            )
        if isinstance(self.fast_window, bool) or self.fast_window < 2:
            raise PlanContractValidationError(
                f"fast_window must be >= 2, got {self.fast_window}"
            )
        if isinstance(self.slow_window, bool) or self.slow_window <= self.fast_window:
            raise PlanContractValidationError(
                f"slow_window ({self.slow_window}) must be > fast_window ({self.fast_window})"
            )
        if self.lookback_window < self.slow_window:
            raise PlanContractValidationError(
                f"lookback_window ({self.lookback_window}) must be >= slow_window ({self.slow_window})"
            )

        if not (Decimal("0.50") <= self.entry_quantile_threshold <= Decimal("0.999")):
            raise PlanContractValidationError(
                f"entry_quantile_threshold ({self.entry_quantile_threshold}) must be in [0.50, 0.999]"
            )
        if not (Decimal("0.001") <= self.exit_quantile_threshold < self.entry_quantile_threshold):
            raise PlanContractValidationError(
                f"exit_quantile_threshold ({self.exit_quantile_threshold}) must be in [0.001, entry_quantile_threshold)"
            )
        if self.atr_stop_multiplier <= Decimal("0"):
            raise PlanContractValidationError(
                f"atr_stop_multiplier ({self.atr_stop_multiplier}) must be > 0"
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "archetype_id": self.archetype_id.value,
            "instrument_id": self.instrument_id,
            "bar_timeframe": self.bar_timeframe,
            "lookback_window": self.lookback_window,
            "fast_window": self.fast_window,
            "slow_window": self.slow_window,
            "entry_quantile_threshold": str(self.entry_quantile_threshold),
            "exit_quantile_threshold": str(self.exit_quantile_threshold),
            "atr_stop_multiplier": str(self.atr_stop_multiplier),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ArchetypeStrategyConfig:
        if not isinstance(data, Mapping):
            raise PlanContractValidationError("ArchetypeStrategyConfig data must be a mapping")
        try:
            return cls(
                archetype_id=StrategyArchetypeId(data["archetype_id"]),
                instrument_id=str(data["instrument_id"]),
                bar_timeframe=str(data["bar_timeframe"]),
                lookback_window=int(data["lookback_window"]),
                fast_window=int(data["fast_window"]),
                slow_window=int(data["slow_window"]),
                entry_quantile_threshold=_to_decimal(
                    data["entry_quantile_threshold"], "entry_quantile_threshold"
                ),
                exit_quantile_threshold=_to_decimal(
                    data["exit_quantile_threshold"], "exit_quantile_threshold"
                ),
                atr_stop_multiplier=_to_decimal(
                    data["atr_stop_multiplier"], "atr_stop_multiplier"
                ),
            )
        except KeyError as exc:
            raise PlanContractValidationError(
                f"Missing required field in ArchetypeStrategyConfig: {exc}"
            ) from exc


@dataclass(frozen=True)
class MarketableLimitCollar:
    """Slippage guard specifying maximum allowable adverse price offset on entry orders."""

    limit_collar_pct: Decimal
    limit_collar_atr: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "limit_collar_pct", _to_decimal(self.limit_collar_pct, "limit_collar_pct")
        )
        object.__setattr__(
            self, "limit_collar_atr", _to_decimal(self.limit_collar_atr, "limit_collar_atr")
        )
        if self.limit_collar_pct < Decimal("0"):
            raise PlanContractValidationError("limit_collar_pct must be >= 0")
        if self.limit_collar_atr < Decimal("0"):
            raise PlanContractValidationError("limit_collar_atr must be >= 0")

    def compute_worst_limit_price(
        self,
        side: PlanSide,
        reference_price: Decimal,
        atr: Optional[Decimal] = None,
        *,
        daily_atr_14: Optional[Decimal] = None,
        tick_size: Optional[Decimal] = None,
    ) -> Decimal:
        """Compute the worst permissible limit price per Reg NMS Rule 612 (`$0.0001` < `$1.00`, `$0.01` >= `$1.00`)."""
        ref_px = _to_decimal(reference_price, "reference_price")
        if ref_px <= Decimal("0"):
            raise PlanContractValidationError(f"reference_price must be > 0, got {ref_px}")

        pct_offset = ref_px * (self.limit_collar_pct / Decimal("100"))
        eff_atr = atr
        if eff_atr is None and daily_atr_14 is not None:
            cand_daily = _to_decimal(daily_atr_14, "daily_atr_14")
            if cand_daily > Decimal("0"):
                eff_atr = cand_daily
        if eff_atr is not None:
            atr_dec = _to_decimal(eff_atr, "atr")
            if atr_dec < Decimal("0"):
                raise PlanContractValidationError(f"atr must be >= 0, got {atr_dec}")
            atr_offset = atr_dec * self.limit_collar_atr
            allowed_offset = (
                min(pct_offset, atr_offset)
                if self.limit_collar_atr > Decimal("0")
                else pct_offset
            )
        else:
            allowed_offset = pct_offset

        raw = (ref_px + allowed_offset) if side == PlanSide.LONG else (ref_px - allowed_offset)
        if tick_size is not None:
            return quantize_price_rule_612(raw, side=side, tick_size=tick_size)
        return quantize_price_rule_612(raw, side=None, tick_size=None)

    def to_dict(self) -> Dict[str, str]:
        return {
            "limit_collar_pct": str(self.limit_collar_pct),
            "limit_collar_atr": str(self.limit_collar_atr),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MarketableLimitCollar:
        pct_raw = data.get("limit_collar_pct", data.get("max_slippage_bps", "0"))
        atr_raw = data.get("limit_collar_atr", data.get("max_atr_fraction", "0"))
        return cls(
            limit_collar_pct=_to_decimal(pct_raw, "limit_collar_pct"),
            limit_collar_atr=_to_decimal(atr_raw, "limit_collar_atr"),
        )


def default_limit_collar_for_archetype(
    archetype_id: StrategyArchetypeId | str,
    limit_collar_pct: Optional[Decimal | float | str] = None,
) -> MarketableLimitCollar:
    """Constructs the canonical `MarketableLimitCollar` for `archetype_id`."""
    eff_pct = (
        limit_collar_pct
        if limit_collar_pct is not None
        else str(SystemConfig().execution.limit_collar_pct)
    )
    return MarketableLimitCollar(
        limit_collar_pct=_to_decimal(eff_pct, "limit_collar_pct"),
        limit_collar_atr=default_limit_collar_atr_for_archetype(archetype_id),
    )


@dataclass(frozen=True)
class PlanRiskEnvelope:
    """Authoritative pre-trade risk envelope enforced by `PreTradeRiskGate` (`RustPlanGateEngine`)."""

    side: PlanSide
    target_qty: Decimal
    approved_qty_cap: Decimal
    target_notional_usd: Decimal
    max_risk_budget_usd: Decimal
    initial_stop_price: Decimal
    take_profit_price: Optional[Decimal]
    collar: MarketableLimitCollar
    valid_until: datetime
    daily_atr_14: Optional[Decimal] = None
    execution_spec: ExecutionSpec = field(default_factory=ExecutionSpec)

    def __post_init__(self) -> None:
        object.__setattr__(self, "side", PlanSide(self.side))
        object.__setattr__(self, "target_qty", _to_decimal(self.target_qty, "target_qty"))
        object.__setattr__(
            self, "approved_qty_cap", _to_decimal(self.approved_qty_cap, "approved_qty_cap")
        )
        object.__setattr__(
            self,
            "target_notional_usd",
            _to_decimal(self.target_notional_usd, "target_notional_usd"),
        )
        object.__setattr__(
            self,
            "max_risk_budget_usd",
            _to_decimal(self.max_risk_budget_usd, "max_risk_budget_usd"),
        )
        object.__setattr__(
            self,
            "initial_stop_price",
            _to_decimal(self.initial_stop_price, "initial_stop_price"),
        )
        if self.take_profit_price is not None:
            object.__setattr__(
                self,
                "take_profit_price",
                _to_decimal(self.take_profit_price, "take_profit_price"),
            )
        if self.daily_atr_14 is not None and str(self.daily_atr_14).strip() != "":
            atr_dec = _to_decimal(self.daily_atr_14, "daily_atr_14")
            if atr_dec <= Decimal("0"):
                raise PlanContractValidationError(
                    f"daily_atr_14 must be > 0 when provided, got {atr_dec}"
                )
            object.__setattr__(self, "daily_atr_14", atr_dec)
        else:
            object.__setattr__(self, "daily_atr_14", None)

        if self.execution_spec is None:
            object.__setattr__(self, "execution_spec", ExecutionSpec())
        elif isinstance(self.execution_spec, Mapping):
            object.__setattr__(self, "execution_spec", ExecutionSpec.from_dict(self.execution_spec))
        elif not isinstance(self.execution_spec, ExecutionSpec):
            raise PlanContractValidationError(
                f"execution_spec must be ExecutionSpec or mapping, got {type(self.execution_spec).__name__}"
            )
        if not isinstance(self.valid_until, datetime) or self.valid_until.tzinfo is None:
            raise PlanContractValidationError("valid_until must be a timezone-aware datetime")

        self.validate_intrinsic()

    @property
    def reference_price(self) -> Decimal:
        """Implied plan reference price (`target_notional_usd / target_qty`)."""
        if self.target_qty > Decimal("0"):
            return self.target_notional_usd / self.target_qty
        return Decimal("0")

    @property
    def limit_collar_pct(self) -> Decimal:
        if "limit_collar_pct" in self.__dict__:
            return _to_decimal(self.__dict__["limit_collar_pct"], "limit_collar_pct")
        return self.collar.limit_collar_pct if self.collar is not None else Decimal("0")

    @property
    def limit_collar_atr(self) -> Decimal:
        if "limit_collar_atr" in self.__dict__:
            return _to_decimal(self.__dict__["limit_collar_atr"], "limit_collar_atr")
        return self.collar.limit_collar_atr if self.collar is not None else Decimal("0")

    def validate_intrinsic(self) -> None:
        if self.target_qty <= Decimal("0"):
            raise PlanContractValidationError(f"target_qty must be > 0, got {self.target_qty}")
        if self.approved_qty_cap < Decimal("0") or self.approved_qty_cap > self.target_qty:
            raise PlanContractValidationError(
                f"approved_qty_cap ({self.approved_qty_cap}) must be in [0, target_qty ({self.target_qty})]"
            )
        if self.target_notional_usd <= Decimal("0"):
            raise PlanContractValidationError(
                f"target_notional_usd must be > 0, got {self.target_notional_usd}"
            )
        if (
            self.max_risk_budget_usd <= Decimal("0")
            or self.max_risk_budget_usd > self.target_notional_usd
        ):
            raise PlanContractValidationError(
                f"max_risk_budget_usd ({self.max_risk_budget_usd}) must be in (0, target_notional_usd ({self.target_notional_usd})]"
            )
        if self.initial_stop_price <= Decimal("0"):
            raise PlanContractValidationError(
                f"initial_stop_price must be > 0, got {self.initial_stop_price}"
            )
        if self.take_profit_price is not None:
            if self.take_profit_price <= Decimal("0"):
                raise PlanContractValidationError("take_profit_price must be > 0")
            if self.side == PlanSide.LONG and self.take_profit_price <= self.initial_stop_price:
                raise PlanContractValidationError(
                    f"LONG plan requires take_profit_price ({self.take_profit_price}) > initial_stop_price ({self.initial_stop_price})"
                )
            if self.side == PlanSide.SHORT and self.take_profit_price >= self.initial_stop_price:
                raise PlanContractValidationError(
                    f"SHORT plan requires take_profit_price ({self.take_profit_price}) < initial_stop_price ({self.initial_stop_price})"
                )

    def validate_against_safety_envelope(
        self,
        safety_envelope: Optional[Envelope] = None,
        total_equity_usd: Optional[Decimal] = None,
    ) -> None:
        """Fail-closed validation against `config/risk_envelope.json` hard bounds."""
        env = safety_envelope or load_envelope()
        num = env.numeric

        notional_bounds = num.get("risk.max_order_notional_usd", {"min": 100, "max": 50000})
        max_notional_cap = Decimal(str(notional_bounds["max"]))
        min_notional_floor = Decimal(str(notional_bounds["min"]))
        if self.target_notional_usd > max_notional_cap or self.target_notional_usd < min_notional_floor:
            raise PlanContractValidationError(
                f"target_notional_usd ({self.target_notional_usd}) outside risk.max_order_notional_usd "
                f"[{min_notional_floor}, {max_notional_cap}]"
            )

        collar_pct_bounds = num.get("execution.limit_collar_pct", {"min": 0.0, "max": 1.0})
        if self.collar.limit_collar_pct > Decimal(str(collar_pct_bounds["max"])) or (
            self.collar.limit_collar_pct < Decimal(str(collar_pct_bounds["min"]))
        ):
            raise PlanContractValidationError(
                f"limit_collar_pct ({self.collar.limit_collar_pct}) outside execution.limit_collar_pct "
                f"[{collar_pct_bounds['min']}, {collar_pct_bounds['max']}]"
            )

        collar_atr_bounds = num.get("execution.limit_collar_atr", {"min": 0.0, "max": 0.5})
        if self.collar.limit_collar_atr > Decimal(str(collar_atr_bounds["max"])) or (
            self.collar.limit_collar_atr < Decimal(str(collar_atr_bounds["min"]))
        ):
            raise PlanContractValidationError(
                f"limit_collar_atr ({self.collar.limit_collar_atr}) outside execution.limit_collar_atr "
                f"[{collar_atr_bounds['min']}, {collar_atr_bounds['max']}]"
            )

        if total_equity_usd is not None:
            eq = _to_decimal(total_equity_usd, "total_equity_usd")
            if eq <= Decimal("0"):
                raise PlanContractValidationError(f"total_equity_usd must be > 0, got {eq}")

            single_name_bounds = num.get("risk.max_single_name_pct", {"min": 0.5, "max": 20.0})
            max_single_name_usd = eq * (Decimal(str(single_name_bounds["max"])) / Decimal("100"))
            if self.target_notional_usd > max_single_name_usd:
                raise PlanContractValidationError(
                    f"target_notional_usd ({self.target_notional_usd}) exceeds max_single_name_pct cap ({max_single_name_usd})"
                )

            risk_per_trade_bounds = num.get("risk.risk_per_trade_pct", {"min": 0.05, "max": 2.0})
            max_risk_per_trade_usd = eq * (Decimal(str(risk_per_trade_bounds["max"])) / Decimal("100"))
            if self.max_risk_budget_usd > max_risk_per_trade_usd:
                raise PlanContractValidationError(
                    f"max_risk_budget_usd ({self.max_risk_budget_usd}) exceeds risk_per_trade_pct cap ({max_risk_per_trade_usd})"
                )

    def with_reduced_qty_cap(self, new_cap: Decimal) -> PlanRiskEnvelope:
        """Return a new PlanRiskEnvelope with a monotonically reduced `approved_qty_cap`."""
        cap_dec = _to_decimal(new_cap, "new_cap")
        if cap_dec < Decimal("0"):
            raise PlanContractValidationError(f"approved_qty_cap cannot be negative: {cap_dec}")
        if cap_dec > self.approved_qty_cap:
            raise PlanContractValidationError(
                f"approved_qty_cap can only be reduced: requested {cap_dec} > current {self.approved_qty_cap}"
            )
        return replace(self, approved_qty_cap=cap_dec)

    def to_dict(self) -> Dict[str, Any]:
        collar_dict = (
            {
                "limit_collar_pct": str(self.limit_collar_pct),
                "limit_collar_atr": str(self.limit_collar_atr),
            }
            if self.collar is not None
            or "limit_collar_pct" in self.__dict__
            or "limit_collar_atr" in self.__dict__
            else None
        )
        return {
            "side": self.side.value if isinstance(self.side, PlanSide) else str(self.side),
            "target_qty": str(self.target_qty),
            "approved_qty_cap": str(self.approved_qty_cap),
            "target_notional_usd": str(self.target_notional_usd),
            "max_risk_budget_usd": str(self.max_risk_budget_usd),
            "initial_stop_price": str(self.initial_stop_price),
            "take_profit_price": (
                str(self.take_profit_price) if self.take_profit_price is not None else None
            ),
            "limit_collar_pct": str(self.limit_collar_pct),
            "limit_collar_atr": str(self.limit_collar_atr),
            "collar": collar_dict,
            "valid_until": self.valid_until.isoformat(),
            "daily_atr_14": (
                str(self.daily_atr_14) if self.daily_atr_14 is not None else None
            ),
            "execution_spec": self.execution_spec.to_dict(),
        }


@dataclass(frozen=True)
class PlanItemContract:
    """The 1:1 Isomorphic Trading Plan Item Contract (`StrategyConfig + PlanRiskEnvelope`).

    Used identically by `IsomorphicBacktestRunner` (pre-market `nautilus-mcp` validation)
    and `LiveNode` (`PreTradeRiskGate` + `CollaredTwapExecAlgorithm` + `Strategy` live execution).
    """

    item_id: UUID
    plan_id: UUID
    symbol: str
    venue: str
    broker: str
    asset_class: str
    strategy_id: str
    strategy_config: ArchetypeStrategyConfig
    risk_envelope: PlanRiskEnvelope
    sizing_trace: Mapping[str, Any]
    status: str = "ARMED"
    filled_qty: Decimal = Decimal("0")
    reserved_notional_usd: Decimal = Decimal("0")
    carry_over_status: Optional[str] = None
    daily_atr_14: Optional[Decimal] = None
    execution_spec: Optional[ExecutionSpec] = None

    def __post_init__(self) -> None:
        if not isinstance(self.item_id, UUID):
            object.__setattr__(self, "item_id", UUID(str(self.item_id)))
        if not isinstance(self.plan_id, UUID):
            object.__setattr__(self, "plan_id", UUID(str(self.plan_id)))
        object.__setattr__(self, "filled_qty", _to_decimal(self.filled_qty, "filled_qty"))
        object.__setattr__(
            self,
            "reserved_notional_usd",
            _to_decimal(self.reserved_notional_usd, "reserved_notional_usd"),
        )
        raw_trace = dict(self.sizing_trace or {})
        co_status = self.carry_over_status
        if co_status is None and raw_trace.get("carry_over_status"):
            co_status = str(raw_trace["carry_over_status"]).strip().upper()
            object.__setattr__(self, "carry_over_status", co_status)
        elif co_status is not None:
            co_status = str(co_status).strip().upper()
            object.__setattr__(self, "carry_over_status", co_status)
            raw_trace.setdefault("carry_over_status", co_status)
        object.__setattr__(self, "sizing_trace", MappingProxyType(raw_trace))

        # Synchronize daily_atr_14 between PlanItemContract and PlanRiskEnvelope
        resolved_atr: Optional[Decimal] = self.risk_envelope.daily_atr_14
        if self.daily_atr_14 is not None and str(self.daily_atr_14).strip() != "":
            item_atr = _to_decimal(self.daily_atr_14, "daily_atr_14")
            if item_atr <= Decimal("0"):
                raise PlanContractValidationError(
                    f"daily_atr_14 must be > 0 when provided, got {item_atr}"
                )
            if resolved_atr is not None and resolved_atr != item_atr:
                raise PlanContractValidationError(
                    f"PlanItemContract.daily_atr_14 ({item_atr}) != risk_envelope.daily_atr_14 ({resolved_atr})"
                )
            resolved_atr = item_atr
        object.__setattr__(self, "daily_atr_14", resolved_atr)
        updated_env = self.risk_envelope
        if updated_env.daily_atr_14 != resolved_atr:
            updated_env = replace(updated_env, daily_atr_14=resolved_atr)

        # Synchronize execution_spec between PlanItemContract and PlanRiskEnvelope
        if self.execution_spec is None:
            resolved_spec = updated_env.execution_spec
        elif isinstance(self.execution_spec, Mapping):
            resolved_spec = ExecutionSpec.from_dict(
                self.execution_spec,
                default_bar_timeframe=self.strategy_config.bar_timeframe,
            )
        elif isinstance(self.execution_spec, ExecutionSpec):
            resolved_spec = self.execution_spec
        else:
            raise PlanContractValidationError(
                f"execution_spec must be ExecutionSpec or mapping, got {type(self.execution_spec).__name__}"
            )
        if updated_env.execution_spec != resolved_spec:
            updated_env = replace(updated_env, execution_spec=resolved_spec)
        object.__setattr__(self, "execution_spec", resolved_spec)
        object.__setattr__(self, "risk_envelope", updated_env)

        sym = self.symbol.strip()
        ven = self.venue.strip()
        if not sym or not ven:
            raise PlanContractValidationError("symbol and venue must be non-empty")
        expected_inst_id = f"{sym}.{ven}"
        if self.strategy_config.instrument_id != expected_inst_id:
            raise PlanContractValidationError(
                f"strategy_config.instrument_id ('{self.strategy_config.instrument_id}') "
                f"must match '{expected_inst_id}'"
            )
        if self.status not in VALID_ITEM_STATUSES:
            raise PlanContractValidationError(
                f"status '{self.status}' not in {VALID_ITEM_STATUSES}"
            )

    @property
    def max_new_orders(self) -> int:
        """Maximum number of new entry orders permitted for this item (`0` when exit-only/carry-over)."""
        if (
            self.risk_envelope.approved_qty_cap <= Decimal("0")
            or str(self.carry_over_status or "").strip().upper()
            in ("CARRY_OVER_EXIT_ONLY", "THESIS_INVALIDATED_EXIT")
        ):
            return 0
        raw_max = self.sizing_trace.get("max_new_orders")
        if raw_max is not None:
            try:
                return max(0, int(raw_max))
            except (TypeError, ValueError):
                pass
        return 1

    def validate(
        self,
        safety_envelope: Optional[Envelope] = None,
        total_equity_usd: Optional[Decimal] = None,
    ) -> None:
        self.strategy_config.validate()
        self.risk_envelope.validate_against_safety_envelope(
            safety_envelope=safety_envelope,
            total_equity_usd=total_equity_usd,
        )

    def with_reduced_qty_cap(self, new_cap: Decimal) -> PlanItemContract:
        updated_env = self.risk_envelope.with_reduced_qty_cap(new_cap)
        return replace(self, risk_envelope=updated_env)

    def to_trigger_spec_dict(self) -> Dict[str, Any]:
        """Serialize the isomorphic `StrategyConfig`, `MarketableLimitCollar`, and `ExecutionSpec` into `trigger_spec`."""
        ref_px = (
            float(self.risk_envelope.target_notional_usd / self.risk_envelope.target_qty)
            if self.risk_envelope.target_qty > Decimal("0")
            else 0.0
        )
        daily_atr_val = float(self.daily_atr_14) if self.daily_atr_14 is not None else None
        atr_val = daily_atr_val
        if atr_val is None:
            raw_trace_atr = self.sizing_trace.get("atr")
            if raw_trace_atr is None:
                raw_trace_atr = self.sizing_trace.get("stop_atr")
            if raw_trace_atr is not None:
                try:
                    cand_trace_atr = float(raw_trace_atr)
                    if cand_trace_atr > 0.0 and math.isfinite(cand_trace_atr):
                        atr_val = cand_trace_atr
                except (TypeError, ValueError):
                    pass
        if atr_val is None and ref_px > 0.0:
            stop_px = float(self.risk_envelope.initial_stop_price)
            atr_mult = max(1.0, float(self.strategy_config.atr_stop_multiplier))
            if stop_px > 0.0 and abs(ref_px - stop_px) > 0.0:
                atr_val = abs(ref_px - stop_px) / atr_mult
        spec_dict = (
            self.execution_spec.to_dict()
            if self.execution_spec is not None
            else ExecutionSpec(bar_timeframe=self.strategy_config.bar_timeframe).to_dict()
        )
        return {
            "schema_version": 2,
            "venue": self.venue,
            "strategy_config": self.strategy_config.to_dict(),
            "collar": self.collar_dict(),
            "reference_price": round(ref_px, 4) if ref_px > 0.0 else None,
            "daily_atr_14": round(daily_atr_val, 6) if daily_atr_val is not None else None,
            "atr": round(atr_val, 6) if atr_val is not None else None,
            "execution_spec": spec_dict,
            "limit_collar_atr": float(self.risk_envelope.collar.limit_collar_atr),
            "limit_collar_pct": float(self.risk_envelope.collar.limit_collar_pct),
        }

    def collar_dict(self) -> Dict[str, str]:
        return self.risk_envelope.collar.to_dict()

    @classmethod
    def from_db_row(cls, row: Mapping[str, Any]) -> PlanItemContract:
        """Reconstruct a validated `PlanItemContract` from a `trading_plan_items` PostgreSQL row."""
        import json

        trigger_spec = row["trigger_spec"]
        if isinstance(trigger_spec, str):
            trigger_spec = json.loads(trigger_spec)

        if not isinstance(trigger_spec, Mapping) or "strategy_config" not in trigger_spec:
            raise PlanContractValidationError(
                "trading_plan_items.trigger_spec missing 'strategy_config' isomorphic payload"
            )

        strat_cfg = ArchetypeStrategyConfig.from_dict(trigger_spec["strategy_config"])
        collar_data = trigger_spec.get("collar")
        if not isinstance(collar_data, Mapping):
            raise PlanContractValidationError(
                "trading_plan_items.trigger_spec missing 'collar' isomorphic payload"
            )
        collar = MarketableLimitCollar.from_dict(collar_data)

        raw_daily_atr = row.get("daily_atr_14")
        if raw_daily_atr is None or str(raw_daily_atr).strip() == "":
            raw_daily_atr = trigger_spec.get("daily_atr_14")
        daily_atr_14 = (
            _to_decimal(raw_daily_atr, "daily_atr_14")
            if raw_daily_atr is not None and str(raw_daily_atr).strip() != ""
            else None
        )
        if daily_atr_14 is not None and daily_atr_14 <= Decimal("0"):
            raise PlanContractValidationError(
                f"daily_atr_14 must be > 0 when present, got {daily_atr_14}"
            )

        raw_exec_spec = row.get("execution_spec")
        if isinstance(raw_exec_spec, str):
            raw_exec_spec = json.loads(raw_exec_spec) if raw_exec_spec.strip() else {}
        if not isinstance(raw_exec_spec, Mapping) or not raw_exec_spec:
            fallback_exec = trigger_spec.get("execution_spec")
            if isinstance(fallback_exec, str):
                fallback_exec = json.loads(fallback_exec) if fallback_exec.strip() else {}
            raw_exec_spec = fallback_exec if isinstance(fallback_exec, Mapping) else {}
        execution_spec = ExecutionSpec.from_dict(
            raw_exec_spec,
            default_bar_timeframe=strat_cfg.bar_timeframe,
        )

        symbol = str(row["symbol"]).strip()
        venue = str(
            trigger_spec.get("venue")
            or strat_cfg.instrument_id.split(".", 1)[1]
        ).strip()

        target_qty = _to_decimal(row["target_qty"], "target_qty")
        approved_cap = (
            _to_decimal(row["approved_qty_cap"], "approved_qty_cap")
            if row.get("approved_qty_cap") is not None
            else target_qty
        )

        valid_until = row["valid_until"]
        if isinstance(valid_until, str):
            valid_until = datetime.fromisoformat(valid_until)
        if valid_until.tzinfo is None:
            valid_until = valid_until.replace(tzinfo=timezone.utc)

        side = PlanSide(str(row["side"]).lower())
        init_stop_raw = row.get("initial_stop_price")
        curr_stop_raw = row.get("current_stop_price")
        init_stop = (
            _to_decimal(init_stop_raw, "initial_stop_price")
            if init_stop_raw is not None and str(init_stop_raw).strip() != ""
            else Decimal("0")
        )
        curr_stop = (
            _to_decimal(curr_stop_raw, "current_stop_price")
            if curr_stop_raw is not None and str(curr_stop_raw).strip() != ""
            else Decimal("0")
        )
        if init_stop > Decimal("0") and curr_stop > Decimal("0"):
            effective_stop = (
                max(init_stop, curr_stop)
                if side == PlanSide.LONG
                else min(init_stop, curr_stop)
            )
        elif curr_stop > Decimal("0"):
            effective_stop = curr_stop
        else:
            effective_stop = _to_decimal(row["initial_stop_price"], "initial_stop_price")

        tp_price = (
            _to_decimal(row["take_profit_price"], "take_profit_price")
            if row.get("take_profit_price") is not None
            else None
        )
        if tp_price is not None:
            if side == PlanSide.LONG and tp_price <= effective_stop:
                tp_price = None
            elif side == PlanSide.SHORT and tp_price >= effective_stop:
                tp_price = None

        risk_env = PlanRiskEnvelope(
            side=side,
            target_qty=target_qty,
            approved_qty_cap=approved_cap,
            target_notional_usd=_to_decimal(
                row["target_notional_usd"], "target_notional_usd"
            ),
            max_risk_budget_usd=_to_decimal(row["risk_budget_usd"], "risk_budget_usd"),
            initial_stop_price=effective_stop,
            take_profit_price=tp_price,
            collar=collar,
            valid_until=valid_until,
            daily_atr_14=daily_atr_14,
            execution_spec=execution_spec,
        )

        sizing_trace = row.get("sizing_trace") or {}
        if isinstance(sizing_trace, str):
            sizing_trace = json.loads(sizing_trace)

        return cls(
            item_id=UUID(str(row["item_id"])),
            plan_id=UUID(str(row["plan_id"])),
            symbol=symbol,
            venue=venue,
            broker=str(row["broker"]),
            asset_class=str(row["asset_class"]),
            strategy_id=str(row["strategy_id"]),
            strategy_config=strat_cfg,
            risk_envelope=risk_env,
            sizing_trace=sizing_trace,
            status=str(row["status"]),
            filled_qty=_to_decimal(row.get("filled_qty", "0"), "filled_qty"),
            reserved_notional_usd=_to_decimal(
                row.get("reserved_notional_usd", "0"), "reserved_notional_usd"
            ),
            carry_over_status=(
                str(row["carry_over_status"])
                if row.get("carry_over_status") is not None
                else None
            ),
            daily_atr_14=daily_atr_14,
            execution_spec=execution_spec,
        )
