"""Pre-audited VolatilityBreakoutStrategy and VolatilityBreakoutConfig for Nautilus v2.

Computes rolling price channel quantiles, rolling volume expansion empirical quantiles,
and volatility-adaptive ATR stops, routing all entry/exit orders through `PLAN_GATE`.
"""

from __future__ import annotations

from decimal import Decimal
from typing import List, Optional

from nautilus_trader.quant.contracts.models import PlanSide, StrategyArchetypeId
from nautilus_trader.quant.strategies.base import (
    BaseArchetypeStrategy,
    BaseArchetypeStrategyConfig,
)
from nautilus_trader.quant.strategies.empirical import (
    empirical_quantile_value,
    empirical_rank_quantile,
    quantile_to_float,
    rolling_atr,
    rolling_mean,
)
from nautilus_trader.model import Bar, OrderSide


class VolatilityBreakoutConfig(BaseArchetypeStrategyConfig):
    """Configuration for `VolatilityBreakoutStrategy`."""

    ARCHETYPE_ID = StrategyArchetypeId.VOLATILITY_BREAKOUT


class VolatilityBreakoutStrategy(BaseArchetypeStrategy):
    """Empirical-distribution-driven Volatility Breakout strategy (`VOLATILITY_BREAKOUT`).

    Signal construction (Zero Magic Numbers / Red Line R9 compliant):
    1. Rolling channel breakout quantile `q_price` of current close against trailing
       `lookback_window` closes (`empirical_rank_quantile` & `empirical_quantile_value`).
    2. Rolling volume expansion ratio `vol_ratio = volume[-1] / rolling_mean(volume[-slow_window:])`,
       ranked as an empirical quantile `q_vol` over the trailing `lookback_window` volume ratios.
    3. Adaptive volatility stop-loss (`atr_stop_multiplier * rolling_atr(fast_window)`).
    """

    def __init__(self, config: VolatilityBreakoutConfig) -> None:
        super().__init__(config)
        self._vol_ratios: List[float] = []
        self._entry_reference_price: Optional[float] = None

    @property
    def volume_expansion_ratios(self) -> List[float]:
        return list(self._vol_ratios)

    def on_bar(self, bar: Bar) -> None:
        if bar.bar_type != self._archetype_config.bar_type:
            return

        self._record_primary_bar(bar)
        n = len(self._closes)
        cfg = self._archetype_config
        if n < cfg.slow_window:
            return

        vol_slice = self._volumes[-cfg.slow_window :]
        mean_vol = rolling_mean(vol_slice)
        curr_vol = self._volumes[-1]
        vol_ratio = curr_vol / mean_vol if mean_vol > 0.0 else 1.0
        self._vol_ratios.append(vol_ratio)
        max_keep = max(cfg.lookback_window * 3, 120)
        if len(self._vol_ratios) > max_keep:
            del self._vol_ratios[:-max_keep]

        atr = rolling_atr(self._highs, self._lows, self._closes, cfg.fast_window)
        if atr <= 0.0:
            return

        lookback_len = min(n, cfg.lookback_window)
        close_window = self._closes[-lookback_len:]
        vol_ratio_window = self._vol_ratios[-min(len(self._vol_ratios), cfg.lookback_window) :]

        close_px = self._closes[-1]
        high_px = self._highs[-1]
        low_px = self._lows[-1]

        price_quantile = empirical_rank_quantile(close_window, close_px)
        vol_quantile = empirical_rank_quantile(vol_ratio_window, vol_ratio)

        entry_q = quantile_to_float(cfg.entry_quantile_threshold)
        exit_q = quantile_to_float(cfg.exit_quantile_threshold)
        atr_mult = quantile_to_float(cfg.atr_stop_multiplier)

        upper_channel = empirical_quantile_value(close_window[:-1] or close_window, entry_q)
        lower_channel = empirical_quantile_value(
            close_window[:-1] or close_window, 1.0 - entry_q
        )

        signed_pos = self._open_position_signed_qty()
        if signed_pos > Decimal("0"):
            candidate_stop = high_px - (atr_mult * atr)
            if (
                self._trailing_stop_price is None
                or candidate_stop > self._trailing_stop_price
            ):
                self._trailing_stop_price = candidate_stop

            stop_hit = (
                self._trailing_stop_price is not None
                and low_px <= self._trailing_stop_price
            )
            channel_failure = price_quantile <= exit_q
            if stop_hit or channel_failure:
                reason = "VOL_ADAPTIVE_STOP" if stop_hit else "CHANNEL_BREAKDOWN_EXIT"
                self._submit_plan_gated_exit(
                    exit_side=OrderSide.SELL,
                    quantity=signed_pos,
                    reference_price=close_px,
                    reason_tag=reason,
                )
                self._entry_reference_price = None
            return

        if signed_pos < Decimal("0"):
            candidate_stop = low_px + (atr_mult * atr)
            if (
                self._trailing_stop_price is None
                or candidate_stop < self._trailing_stop_price
            ):
                self._trailing_stop_price = candidate_stop

            stop_hit = (
                self._trailing_stop_price is not None
                and high_px >= self._trailing_stop_price
            )
            channel_failure = (1.0 - price_quantile) <= exit_q
            if stop_hit or channel_failure:
                reason = "VOL_ADAPTIVE_STOP" if stop_hit else "CHANNEL_BREAKOUT_REVERSAL_EXIT"
                self._submit_plan_gated_exit(
                    exit_side=OrderSide.BUY,
                    quantity=abs(signed_pos),
                    reference_price=close_px,
                    reason_tag=reason,
                )
                self._entry_reference_price = None
            return

        if n < cfg.lookback_window or self._has_inflight_or_open_entry_order():
            return

        if cfg.plan_side == PlanSide.LONG:
            if (
                (price_quantile >= entry_q or close_px >= upper_channel)
                and vol_quantile >= exit_q
            ):
                stop_px = close_px - (atr_mult * atr)
                self._trailing_stop_price = stop_px
                self._entry_reference_price = close_px
                self._submit_plan_gated_entry(
                    order_side=OrderSide.BUY,
                    reference_price=close_px,
                    atr=atr,
                    limit_price=close_px,
                    stop_loss_price=stop_px,
                    tags=["VOLATILITY_BREAKOUT_LONG_ENTRY"],
                )
        elif cfg.plan_side == PlanSide.SHORT:
            if (
                ((1.0 - price_quantile) >= entry_q or close_px <= lower_channel)
                and vol_quantile >= exit_q
            ):
                stop_px = close_px + (atr_mult * atr)
                self._trailing_stop_price = stop_px
                self._entry_reference_price = close_px
                self._submit_plan_gated_entry(
                    order_side=OrderSide.SELL,
                    reference_price=close_px,
                    atr=atr,
                    limit_price=close_px,
                    stop_loss_price=stop_px,
                    tags=["VOLATILITY_BREAKOUT_SHORT_ENTRY"],
                )
