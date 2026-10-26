"""Pre-audited Nautilus v2 strategy archetypes and registry (`nautilus_trader.quant.strategies`)."""

from nautilus_trader.quant.strategies.base import (
    TIMEFRAME_TO_BAR_SPEC,
    BaseArchetypeStrategy,
    BaseArchetypeStrategyConfig,
    format_datainfra_bar_type,
)
from nautilus_trader.quant.strategies.empirical import (
    compute_true_range,
    empirical_quantile_value,
    empirical_rank_quantile,
    ewma_mean,
    ewma_slope,
    quantile_to_float,
    rolling_atr,
    rolling_linear_slope,
    rolling_mad_zscore,
    rolling_mean,
    rolling_ols_hedge_params,
    rolling_std,
    rolling_vwap,
    volatility_standardized_momentum,
)
from nautilus_trader.quant.strategies.mean_reversion import (
    MeanReversionConfig,
    MeanReversionStrategy,
)
from nautilus_trader.quant.strategies.registry import (
    STRATEGY_ARCHETYPE_REGISTRY,
    ArchetypeDescriptor,
    create_strategy_config_from_contract,
    get_archetype_descriptor,
    instantiate_strategy_by_archetype,
    instantiate_strategy_from_contract,
    list_strategy_archetype_schemas,
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

__all__ = [
    "ArchetypeDescriptor",
    "BaseArchetypeStrategy",
    "BaseArchetypeStrategyConfig",
    "MeanReversionConfig",
    "MeanReversionStrategy",
    "STRATEGY_ARCHETYPE_REGISTRY",
    "StatArbPairsConfig",
    "StatArbPairsStrategy",
    "TIMEFRAME_TO_BAR_SPEC",
    "TrendMomentumConfig",
    "TrendMomentumStrategy",
    "VolatilityBreakoutConfig",
    "VolatilityBreakoutStrategy",
    "compute_true_range",
    "create_strategy_config_from_contract",
    "empirical_quantile_value",
    "empirical_rank_quantile",
    "ewma_mean",
    "ewma_slope",
    "format_datainfra_bar_type",
    "get_archetype_descriptor",
    "instantiate_strategy_by_archetype",
    "instantiate_strategy_from_contract",
    "list_strategy_archetype_schemas",
    "quantile_to_float",
    "rolling_atr",
    "rolling_linear_slope",
    "rolling_mad_zscore",
    "rolling_mean",
    "rolling_ols_hedge_params",
    "rolling_std",
    "rolling_vwap",
    "volatility_standardized_momentum",
]
