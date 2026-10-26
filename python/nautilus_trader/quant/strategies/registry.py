"""Registry and reflective schema catalog for pre-audited Nautilus v2 strategy archetypes.

Provides `STRATEGY_ARCHETYPE_REGISTRY`, parameter schema reflection, empirical
calibration bounds, and 1:1 isomorphic instantiation from `PlanItemContract`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Type

from nautilus_trader.quant.contracts.models import (
    SUPPORTED_TIMEFRAMES,
    ArchetypeStrategyConfig,
    PlanItemContract,
    StrategyArchetypeId,
)
from nautilus_trader.quant.strategies.base import (
    BaseArchetypeStrategy,
    BaseArchetypeStrategyConfig,
)
from nautilus_trader.quant.strategies.mean_reversion import (
    MeanReversionConfig,
    MeanReversionStrategy,
)
from nautilus_trader.quant.strategies.stat_arb_pairs import (
    StatArbPairsConfig,
    StatArbPairsStrategy,
)
from nautilus_trader.quant.strategies.trend_momentum import (
    TrendMomentumConfig,
    TrendMomentumStrategy,
)
from nautilus_trader.quant.strategies.volatility_breakout import (
    VolatilityBreakoutConfig,
    VolatilityBreakoutStrategy,
)

ALLOWED_BAR_TIMEFRAMES = SUPPORTED_TIMEFRAMES
MIN_LOOKBACK_WINDOW = 5
MAX_LOOKBACK_WINDOW = 500
MIN_FAST_WINDOW = 2
MIN_SLOW_WINDOW = 3
MIN_ATR_STOP_MULTIPLIER = "0.5"
MAX_ATR_STOP_MULTIPLIER = "10.0"


def _default_parameter_bounds(requires_secondary: bool = False) -> Dict[str, Dict[str, Any]]:
    bounds: Dict[str, Dict[str, Any]] = {
        "bar_timeframe": {
            "type": "enum",
            "allowed": list(ALLOWED_BAR_TIMEFRAMES),
            "default": "15m",
            "description": "Canonical bar aggregation timeframe from DataInfra.",
        },
        "lookback_window": {
            "type": "integer",
            "min": MIN_LOOKBACK_WINDOW,
            "max": MAX_LOOKBACK_WINDOW,
            "default": 30,
            "description": "Rolling sample window for empirical rank quantiles and MAD Z-scores.",
        },
        "fast_window": {
            "type": "integer",
            "min": MIN_FAST_WINDOW,
            "max": MAX_LOOKBACK_WINDOW - 1,
            "default": 5,
            "description": "Fast rolling window for slope/channel/residual estimation (must be < slow_window).",
        },
        "slow_window": {
            "type": "integer",
            "min": MIN_SLOW_WINDOW,
            "max": MAX_LOOKBACK_WINDOW,
            "default": 20,
            "description": "Slow rolling window for baseline trend/volatility estimation (must be <= lookback_window).",
        },
        "entry_quantile_threshold": {
            "type": "decimal_quantile",
            "min_exclusive": "0.50",
            "max_exclusive": "1.00",
            "default": "0.80",
            "description": "Empirical CDF rank quantile required to trigger an entry signal (R9 zero magic numbers).",
        },
        "exit_quantile_threshold": {
            "type": "decimal_quantile",
            "min_exclusive": "0.00",
            "max_exclusive": "entry_quantile_threshold",
            "default": "0.35",
            "description": "Empirical CDF rank quantile below which open positions exit.",
        },
        "atr_stop_multiplier": {
            "type": "decimal",
            "min": str(MIN_ATR_STOP_MULTIPLIER),
            "max": str(MAX_ATR_STOP_MULTIPLIER),
            "default": "2.0",
            "description": "Rolling ATR multiplier for adaptive volatility-scaled stop placement.",
        },
    }
    if requires_secondary:
        bounds["secondary_instrument_id"] = {
            "type": "instrument_id",
            "required": True,
            "description": "Secondary hedge leg instrument identifier (e.g. 'MSFT.ALPACA').",
        }
    return bounds


@dataclass(frozen=True)
class ArchetypeDescriptor:
    """Metadata and constructors for a pre-audited Nautilus `Strategy` archetype."""

    archetype_id: StrategyArchetypeId
    name: str
    description: str
    config_cls: Type[BaseArchetypeStrategyConfig]
    strategy_cls: Type[BaseArchetypeStrategy]
    requires_secondary_instrument: bool
    parameter_bounds: Dict[str, Dict[str, Any]]

    def to_schema_dict(self) -> Dict[str, Any]:
        """Returns a JSON-serializable reflection schema for MCP discovery and validation."""
        return {
            "archetype_id": self.archetype_id.value,
            "name": self.name,
            "description": self.description,
            "config_class": f"{self.config_cls.__module__}.{self.config_cls.__qualname__}",
            "strategy_class": f"{self.strategy_cls.__module__}.{self.strategy_cls.__qualname__}",
            "requires_secondary_instrument": self.requires_secondary_instrument,
            "exec_algorithm_id": "PLAN_GATE",
            "parameter_bounds": dict(self.parameter_bounds),
        }


STRATEGY_ARCHETYPE_REGISTRY: Dict[StrategyArchetypeId, ArchetypeDescriptor] = {
    StrategyArchetypeId.TREND_MOMENTUM: ArchetypeDescriptor(
        archetype_id=StrategyArchetypeId.TREND_MOMENTUM,
        name="Trend Momentum Strategy",
        description=(
            "Cross-window EMA slope quantile and volatility-standardized momentum "
            "MAD Z-score strategy with adaptive ATR trailing stop."
        ),
        config_cls=TrendMomentumConfig,
        strategy_cls=TrendMomentumStrategy,
        requires_secondary_instrument=False,
        parameter_bounds=_default_parameter_bounds(requires_secondary=False),
    ),
    StrategyArchetypeId.VOLATILITY_BREAKOUT: ArchetypeDescriptor(
        archetype_id=StrategyArchetypeId.VOLATILITY_BREAKOUT,
        name="Volatility Breakout Strategy",
        description=(
            "Rolling range-expansion channel breakout combined with volume expansion "
            "empirical rank quantiles and volatility-scaled adaptive stops."
        ),
        config_cls=VolatilityBreakoutConfig,
        strategy_cls=VolatilityBreakoutStrategy,
        requires_secondary_instrument=False,
        parameter_bounds=_default_parameter_bounds(requires_secondary=False),
    ),
    StrategyArchetypeId.MEAN_REVERSION: ArchetypeDescriptor(
        archetype_id=StrategyArchetypeId.MEAN_REVERSION,
        name="Mean Reversion Strategy",
        description=(
            "Rolling residual non-parametric MAD Z-score and volume-weighted price "
            "deviation quantile strategy reverting toward rolling equilibrium."
        ),
        config_cls=MeanReversionConfig,
        strategy_cls=MeanReversionStrategy,
        requires_secondary_instrument=False,
        parameter_bounds=_default_parameter_bounds(requires_secondary=False),
    ),
    StrategyArchetypeId.STAT_ARB_PAIRS: ArchetypeDescriptor(
        archetype_id=StrategyArchetypeId.STAT_ARB_PAIRS,
        name="Statistical Arbitrage Pairs Strategy",
        description=(
            "Dual-leg cointegration spread residual MAD Z-score strategy with dynamic "
            "Rolling OLS / Kalman-EWMA hedge ratio beta."
        ),
        config_cls=StatArbPairsConfig,
        strategy_cls=StatArbPairsStrategy,
        requires_secondary_instrument=True,
        parameter_bounds=_default_parameter_bounds(requires_secondary=True),
    ),
}


def get_archetype_descriptor(
    archetype_id: str | StrategyArchetypeId,
) -> ArchetypeDescriptor:
    """Looks up an `ArchetypeDescriptor` by `StrategyArchetypeId` or string name."""
    resolved = (
        archetype_id
        if isinstance(archetype_id, StrategyArchetypeId)
        else StrategyArchetypeId(str(archetype_id).strip())
    )
    descriptor = STRATEGY_ARCHETYPE_REGISTRY.get(resolved)
    if descriptor is None:
        raise KeyError(f"Unknown strategy archetype_id: {archetype_id!r}")
    return descriptor


def list_strategy_archetype_schemas() -> List[Dict[str, Any]]:
    """Returns reflection schemas for all registered pre-audited strategy archetypes."""
    return [
        descriptor.to_schema_dict()
        for descriptor in STRATEGY_ARCHETYPE_REGISTRY.values()
    ]


def create_strategy_config_from_contract(
    plan_item: PlanItemContract,
    use_limit_orders: bool = True,
    submit_protective_stop: bool = False,
) -> BaseArchetypeStrategyConfig:
    """Creates a concrete `StrategyConfig` instance from a locked `PlanItemContract`."""
    descriptor = get_archetype_descriptor(plan_item.strategy_config.archetype_id)
    return descriptor.config_cls.from_plan_item(
        plan_item=plan_item,
        use_limit_orders=use_limit_orders,
        submit_protective_stop=submit_protective_stop,
    )


def instantiate_strategy_from_contract(
    plan_item: PlanItemContract,
    use_limit_orders: bool = True,
    submit_protective_stop: bool = False,
) -> BaseArchetypeStrategy:
    """Instantiates the exact pre-audited `Strategy` bound to `plan_item` (1:1 isomorphic)."""
    descriptor = get_archetype_descriptor(plan_item.strategy_config.archetype_id)
    config = descriptor.config_cls.from_plan_item(
        plan_item=plan_item,
        use_limit_orders=use_limit_orders,
        submit_protective_stop=submit_protective_stop,
    )
    return descriptor.strategy_cls(config)


def instantiate_strategy_by_archetype(
    archetype_id: str | StrategyArchetypeId,
    **kwargs: Any,
) -> BaseArchetypeStrategy:
    """Validates parameters against `ArchetypeStrategyConfig` and instantiates the `Strategy`."""
    descriptor = get_archetype_descriptor(archetype_id)
    # Validate core empirical parameters first via ArchetypeStrategyConfig
    _ = ArchetypeStrategyConfig(
        archetype_id=descriptor.archetype_id,
        instrument_id=str(kwargs["instrument_id"]),
        bar_timeframe=str(kwargs.get("bar_timeframe", "15m")),
        lookback_window=int(kwargs.get("lookback_window", 30)),
        fast_window=int(kwargs.get("fast_window", 5)),
        slow_window=int(kwargs.get("slow_window", 20)),
        entry_quantile_threshold=kwargs.get("entry_quantile_threshold", "0.80"),
        exit_quantile_threshold=kwargs.get("exit_quantile_threshold", "0.35"),
        atr_stop_multiplier=kwargs.get("atr_stop_multiplier", "2.0"),
        secondary_instrument_id=(
            str(kwargs["secondary_instrument_id"])
            if kwargs.get("secondary_instrument_id") is not None
            else None
        ),
    )
    config = descriptor.config_cls(**kwargs)
    return descriptor.strategy_cls(config)
