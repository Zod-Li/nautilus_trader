"""Immutable domain models for the 1:1 Isomorphic Trading Plan Contract (`PlanItem = StrategyConfig + PlanRiskEnvelope`)."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional
from uuid import UUID

from evolution_engine.config_center.envelope import Envelope, load_envelope

SUPPORTED_TIMEFRAMES = ("1m", "15m", "1h", "4h", "1d", "1w")
VALID_ITEM_STATUSES = (
    "ARMED",
    "TRIGGERED",
    "PARTIALLY_FILLED",
    "FILLED",
    "EXPIRED",
    "CANCELLED",
    "CLOSED",
)


class PlanContractValidationError(ValueError):
    """Raised when a PlanItemContract, StrategyConfig, or PlanRiskEnvelope violates a safety invariant."""


class StrategyArchetypeId(str, Enum):
    """Pre-audited Nautilus Strategy archetypes eligible for pre-market calibration and live execution."""

    TREND_MOMENTUM = "TREND_MOMENTUM"
    VOLATILITY_BREAKOUT = "VOLATILITY_BREAKOUT"
    MEAN_REVERSION = "MEAN_REVERSION"
    STAT_ARB_PAIRS = "STAT_ARB_PAIRS"


class PlanSide(str, Enum):
    """Directional side approved by the TradingPlan."""

    LONG = "long"
    SHORT = "short"


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
    secondary_instrument_id: Optional[str] = None

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

        if self.archetype_id == StrategyArchetypeId.STAT_ARB_PAIRS:
            sec = (self.secondary_instrument_id or "").strip()
            if "." not in sec or sec == inst:
                raise PlanContractValidationError(
                    "STAT_ARB_PAIRS requires a distinct secondary_instrument_id in 'SYMBOL.VENUE' format"
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
            "secondary_instrument_id": self.secondary_instrument_id,
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
                secondary_instrument_id=(
                    str(data["secondary_instrument_id"])
                    if data.get("secondary_instrument_id") is not None
                    else None
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
    ) -> Decimal:
        """Compute the worst permissible limit price for an entry order given reference price and optional ATR."""
        ref_px = _to_decimal(reference_price, "reference_price")
        if ref_px <= Decimal("0"):
            raise PlanContractValidationError(f"reference_price must be > 0, got {ref_px}")

        pct_offset = ref_px * (self.limit_collar_pct / Decimal("100"))
        if atr is not None:
            atr_dec = _to_decimal(atr, "atr")
            if atr_dec < Decimal("0"):
                raise PlanContractValidationError(f"atr must be >= 0, got {atr_dec}")
            atr_offset = atr_dec * self.limit_collar_atr
            allowed_offset = min(pct_offset, atr_offset) if self.limit_collar_atr > Decimal("0") else pct_offset
        else:
            allowed_offset = pct_offset

        if side == PlanSide.LONG:
            return (ref_px + allowed_offset).quantize(Decimal("0.01"))
        return max(Decimal("0.01"), (ref_px - allowed_offset).quantize(Decimal("0.01")))

    def to_dict(self) -> Dict[str, str]:
        return {
            "limit_collar_pct": str(self.limit_collar_pct),
            "limit_collar_atr": str(self.limit_collar_atr),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MarketableLimitCollar:
        return cls(
            limit_collar_pct=_to_decimal(data["limit_collar_pct"], "limit_collar_pct"),
            limit_collar_atr=_to_decimal(data["limit_collar_atr"], "limit_collar_atr"),
        )


@dataclass(frozen=True)
class PlanRiskEnvelope:
    """Authoritative pre-trade risk envelope enforced by `PlanGateExecAlgorithm`."""

    side: PlanSide
    target_qty: Decimal
    approved_qty_cap: Decimal
    target_notional_usd: Decimal
    max_risk_budget_usd: Decimal
    initial_stop_price: Decimal
    take_profit_price: Optional[Decimal]
    collar: MarketableLimitCollar
    valid_until: datetime

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
        if not isinstance(self.valid_until, datetime) or self.valid_until.tzinfo is None:
            raise PlanContractValidationError("valid_until must be a timezone-aware datetime")

        self.validate_intrinsic()

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


@dataclass(frozen=True)
class PlanItemContract:
    """The 1:1 Isomorphic Trading Plan Item Contract (`StrategyConfig + PlanRiskEnvelope`).

    Used identically by `IsomorphicBacktestRunner` (pre-market `nautilus-mcp` validation)
    and `LiveNode` (`PlanGateExecAlgorithm` + `Strategy` live execution).
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
        if not isinstance(self.sizing_trace, MappingProxyType):
            object.__setattr__(
                self, "sizing_trace", MappingProxyType(dict(self.sizing_trace or {}))
            )

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
        """Serialize the isomorphic `StrategyConfig` and `MarketableLimitCollar` into `trading_plan_items.trigger_spec`."""
        return {
            "schema_version": 2,
            "venue": self.venue,
            "strategy_config": self.strategy_config.to_dict(),
            "collar": self.collar_dict(),
        }

    def collar_dict(self) -> Dict[str, str]:
        return self.risk_envelope.collar.to_dict()

    @classmethod
    def from_db_row(cls, row: Mapping[str, Any]) -> PlanItemContract:
        """Reconstruct a validated `PlanItemContract` from a `trading_plan_items` PostgreSQL row."""
        trigger_spec = row["trigger_spec"]
        if isinstance(trigger_spec, str):
            import json

            trigger_spec = json.loads(trigger_spec)

        if not isinstance(trigger_spec, Mapping) or "strategy_config" not in trigger_spec:
            raise PlanContractValidationError(
                "trading_plan_items.trigger_spec missing 'strategy_config' isomorphic payload"
            )

        strat_cfg = ArchetypeStrategyConfig.from_dict(trigger_spec["strategy_config"])
        collar_data = trigger_spec.get(
            "collar", {"limit_collar_pct": "0.5", "limit_collar_atr": "0.25"}
        )
        collar = MarketableLimitCollar.from_dict(collar_data)

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

        risk_env = PlanRiskEnvelope(
            side=PlanSide(str(row["side"]).lower()),
            target_qty=target_qty,
            approved_qty_cap=approved_cap,
            target_notional_usd=_to_decimal(
                row["target_notional_usd"], "target_notional_usd"
            ),
            max_risk_budget_usd=_to_decimal(row["risk_budget_usd"], "risk_budget_usd"),
            initial_stop_price=_to_decimal(
                row.get("current_stop_price") or row["initial_stop_price"],
                "initial_stop_price",
            ),
            take_profit_price=(
                _to_decimal(row["take_profit_price"], "take_profit_price")
                if row.get("take_profit_price") is not None
                else None
            ),
            collar=collar,
            valid_until=valid_until,
        )

        sizing_trace = row.get("sizing_trace") or {}
        if isinstance(sizing_trace, str):
            import json

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
        )
