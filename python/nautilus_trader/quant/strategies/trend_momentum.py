"""Pre-audited TrendMomentumStrategy and TrendMomentumConfig for Nautilus v2.

Computes fast/slow slope differentials and volatility-standardized momentum scores,
evaluates their rolling empirical rank quantiles over `lookback_window`, and routes
all entry/exit orders with ATR trailing stops through `PlanGateExecAlgorithm` (`PLAN_GATE`).
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
    empirical_rank_quantile,
    quantile_to_float,
    rolling_atr,
    rolling_linear_slope,
    rolling_mean,
    rolling_std,
)
from nautilus_trader.model import Bar, OrderSide


class TrendMomentumConfig(BaseArchetypeStrategyConfig):
    """Configuration for `TrendMomentumStrategy`."""

    ARCHETYPE_ID = StrategyArchetypeId.TREND_MOMENTUM


class TrendMomentumStrategy(BaseArchetypeStrategy):
    """Empirical-distribution-driven Trend Momentum strategy (`TREND_MOMENTUM`).

    Signal construction (Zero Magic Numbers / Red Line R9 compliant):
    1. Rolling fast slope (`fast_window`) vs rolling slow slope (`slow_window`),
       standardized by rolling bar return volatility (`fast_window`).
    2. Volatility-standardized momentum score:
       `mom_z = ((fast_mean - slow_mean) + (fast_slope - slow_slope)) / price_vol`.
    3. Rolling empirical quantile rank `q_mom = empirical_rank_quantile(mom_history[-lookback_window:], mom_z)`.
    4. ATR trailing stop (`atr_stop_multiplier * rolling_atr`) ratcheted in the trade direction.
    """

    def __init__(self, config: TrendMomentumConfig) -> None:
        super().__init__(config)
        self._momentum_scores: List[float] = []
        self._slope_diffs: List[float] = []
        self._peak_favorable_price: Optional[float] = None

    @property
    def momentum_scores(self) -> List[float]:
        return list(self._momentum_scores)

    def on_bar(self, bar: Bar) -> None:
        if bar.bar_type != self._archetype_config.bar_type:
            return

        self._record_primary_bar(bar)
        n = len(self._closes)
        cfg = self._archetype_config
        if n < cfg.slow_window:
            return

        fast_slice = self._closes[-cfg.fast_window :]
        slow_slice = self._closes[-cfg.slow_window :]
        fast_mean = rolling_mean(fast_slice)
        slow_mean = rolling_mean(slow_slice)
        fast_slope = rolling_linear_slope(fast_slice)
        slow_slope = rolling_linear_slope(slow_slice)
        slope_diff = fast_slope - slow_slope

        price_std = rolling_std(slow_slice)
        atr = rolling_atr(self._highs, self._lows, self._closes, cfg.fast_window)
        scale = max(price_std, atr)
        if scale <= 0.0:
            return

        mom_score = ((fast_mean - slow_mean) + slope_diff * float(cfg.fast_window)) / scale
        self._momentum_scores.append(mom_score)
        self._slope_diffs.append(slope_diff)
        max_keep = max(cfg.lookback_window * 3, 120)
        if len(self._momentum_scores) > max_keep:
            del self._momentum_scores[:-max_keep]
            del self._slope_diffs[:-max_keep]

        history_len = min(len(self._momentum_scores), cfg.lookback_window)
        if history_len < cfg.slow_window:
            return

        mom_window = self._momentum_scores[-history_len:]
        slope_window = self._slope_diffs[-history_len:]
        mom_quantile = empirical_rank_quantile(mom_window, mom_score)
        slope_quantile = empirical_rank_quantile(slope_window, slope_diff)

        entry_q = quantile_to_float(cfg.entry_quantile_threshold)
        exit_q = quantile_to_float(cfg.exit_quantile_threshold)
        atr_mult = quantile_to_float(cfg.atr_stop_multiplier)
        close_px = self._closes[-1]
        high_px = self._highs[-1]
        low_px = self._lows[-1]

        signed_pos = self._open_position_signed_qty()
        if signed_pos > Decimal("0"):
            if self._peak_favorable_price is None or high_px > self._peak_favorable_price:
                self._peak_favorable_price = high_px
            candidate_stop = self._peak_favorable_price - (atr_mult * atr)
            if (
                self._trailing_stop_price is None
                or candidate_stop > self._trailing_stop_price
            ):
                self._trailing_stop_price = candidate_stop

            stop_hit = (
                self._trailing_stop_price is not None
                and low_px <= self._trailing_stop_price
            )
            momentum_exhausted = mom_quantile <= exit_q
            if stop_hit or momentum_exhausted:
                reason = "ATR_TRAILING_STOP" if stop_hit else "MOMENTUM_DECAY_EXIT"
                self._submit_plan_gated_exit(
                    exit_side=OrderSide.SELL,
                    quantity=signed_pos,
                    reference_price=close_px,
                    reason_tag=reason,
                )
                self._peak_favorable_price = None
            return

        if signed_pos < Decimal("0"):
            if self._peak_favorable_price is None or low_px < self._peak_favorable_price:
                self._peak_favorable_price = low_px
            candidate_stop = self._peak_favorable_price + (atr_mult * atr)
            if (
                self._trailing_stop_price is None
                or candidate_stop < self._trailing_stop_price
            ):
                self._trailing_stop_price = candidate_stop

            stop_hit = (
                self._trailing_stop_price is not None
                and high_px >= self._trailing_stop_price
            )
            momentum_exhausted = (1.0 - mom_quantile) <= exit_q
            if stop_hit or momentum_exhausted:
                reason = "ATR_TRAILING_STOP" if stop_hit else "MOMENTUM_DECAY_EXIT"
                self._submit_plan_gated_exit(
                    exit_side=OrderSide.BUY,
                    quantity=abs(signed_pos),
                    reference_price=close_px,
                    reason_tag=reason,
                )
                self._peak_favorable_price = None
            return

        if n < cfg.lookback_window or self._has_inflight_or_open_entry_order():
            return

        if cfg.plan_side == PlanSide.LONG:
            if mom_quantile >= entry_q and slope_quantile >= exit_q and slope_diff > 0.0:
                stop_px = close_px - (atr_mult * atr)
                self._trailing_stop_price = stop_px
                self._peak_favorable_price = high_px
                self._submit_plan_gated_entry(
                    order_side=OrderSide.BUY,
                    reference_price=close_px,
                    atr=atr,
                    limit_price=close_px,
                    stop_loss_price=stop_px,
                    tags=["TREND_MOMENTUM_LONG_ENTRY"],
                )
        elif cfg.plan_side == PlanSide.SHORT:
            short_mom_q = 1.0 - mom_quantile
            short_slope_q = 1.0 - slope_quantile
            if short_mom_q >= entry_q and short_slope_q >= exit_q and slope_diff < 0.0:
                stop_px = close_px + (atr_mult * atr)
                self._trailing_stop_price = stop_px
                self._peak_favorable_price = low_px
                self._submit_plan_gated_entry(
                    order_side=OrderSide.SELL,
                    reference_price=close_px,
                    atr=atr,
                    limit_price=close_px,
                    stop_loss_price=stop_px,
                    tags=["TREND_MOMENTUM_SHORT_ENTRY"],
                )
