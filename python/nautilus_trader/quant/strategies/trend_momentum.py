"""Pre-audited TrendMomentumStrategy and TrendMomentumConfig for Nautilus v2.

Computes fast/slow slope differentials and volatility-standardized momentum scores,
evaluates their rolling empirical rank quantiles over `lookback_window`, and routes
entry signals through `BaseArchetypeStrategy` (`ExecutionSpec` + `PreTradeRiskGate` +
`COLLARED_TWAP` / `DIRECT_COLLARED_LIMIT` + `daily_atr_14` Chandelier ratchet).
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
       standardized by rolling bar return volatility (`slow_window`) or `daily_atr_14`.
    2. Volatility-standardized momentum score:
       `mom_z = ((fast_mean - slow_mean) + (fast_slope - slow_slope)) / scale`.
    3. Rolling empirical quantile rank `q_mom = empirical_rank_quantile(mom_history[-lookback_window:], mom_z)`.
    4. Initial stop and `1m` Chandelier ratchet anchored strictly to `daily_atr_14`.
    """

    def __init__(self, config: TrendMomentumConfig) -> None:
        super().__init__(config)
        self._momentum_scores: List[float] = []
        self._slope_diffs: List[float] = []
        self._peak_favorable_price: Optional[float] = None

    @property
    def momentum_scores(self) -> List[float]:
        return list(self._momentum_scores)

    def reset_rolling_windows(self) -> None:
        super().reset_rolling_windows()
        self._momentum_scores.clear()
        self._slope_diffs.clear()

    def evaluate_primary_bar(self, bar: Bar) -> None:
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
        daily_atr = self._resolve_daily_atr_14(self._closes[-1])
        scale = max(price_std, daily_atr)
        if scale <= 0.0:
            return

        mom_score = ((fast_mean - slow_mean) + slope_diff * float(cfg.fast_window)) / scale
        self._momentum_scores.append(mom_score)
        self._slope_diffs.append(slope_diff)
        max_keep = max(cfg.lookback_window * 3, 120)
        if len(self._momentum_scores) > max_keep:
            del self._momentum_scores[:-max_keep]
            del self._slope_diffs[:-max_keep]

        if self._in_warmup:
            return

        bar_ts = int(getattr(bar, "ts_event", 0) or getattr(bar, "ts_init", 0) or 0)
        tod_vol_q = self._update_rth_tod_volume(bar_ts, self._volumes[-1])

        history_len = min(len(self._momentum_scores), cfg.lookback_window)
        if history_len < cfg.slow_window:
            return

        if self._open_position_signed_qty() != Decimal("0"):
            return

        if (
            self._exit_only_mode
            or n < cfg.lookback_window
            or self._has_inflight_or_open_entry_order()
            or daily_atr <= 0.0
        ):
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

        if cfg.plan_side == PlanSide.LONG:
            if mom_quantile >= entry_q and slope_quantile >= exit_q and slope_diff > 0.0:
                stop_px = self._resolve_entry_stop_price(
                    close_px, close_px - (atr_mult * daily_atr)
                )
                prev_stop = self._trailing_stop_price
                prev_peak = self._peak_favorable_price
                self._trailing_stop_price = stop_px
                self._peak_favorable_price = high_px
                cid = self._dispatch_entry_signal(
                    order_side=OrderSide.BUY,
                    bar=bar,
                    reference_price=close_px,
                    stop_loss_price=stop_px,
                    tags=["TREND_MOMENTUM_LONG_ENTRY"],
                    tod_volume_quantile=tod_vol_q,
                )
                if not cid or self._is_order_denied_or_rejected(cid):
                    self._trailing_stop_price = prev_stop
                    self._peak_favorable_price = prev_peak
        elif cfg.plan_side == PlanSide.SHORT:
            short_mom_q = 1.0 - mom_quantile
            short_slope_q = 1.0 - slope_quantile
            if short_mom_q >= entry_q and short_slope_q >= exit_q and slope_diff < 0.0:
                stop_px = self._resolve_entry_stop_price(
                    close_px, close_px + (atr_mult * daily_atr)
                )
                prev_stop = self._trailing_stop_price
                prev_peak = self._peak_favorable_price
                self._trailing_stop_price = stop_px
                self._peak_favorable_price = low_px
                cid = self._dispatch_entry_signal(
                    order_side=OrderSide.SELL,
                    bar=bar,
                    reference_price=close_px,
                    stop_loss_price=stop_px,
                    tags=["TREND_MOMENTUM_SHORT_ENTRY"],
                    tod_volume_quantile=tod_vol_q,
                )
                if not cid or self._is_order_denied_or_rejected(cid):
                    self._trailing_stop_price = prev_stop
                    self._peak_favorable_price = prev_peak
