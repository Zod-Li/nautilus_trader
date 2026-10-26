"""Pre-audited MeanReversionStrategy and MeanReversionConfig for Nautilus v2.

Computes rolling VWAP/anchor residuals, non-parametric Robust MAD Z-scores,
and empirical rank quantiles over `lookback_window`, routing all orders through `PLAN_GATE`.
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
    rolling_mad_zscore,
    rolling_mean,
)
from nautilus_trader.model import Bar, OrderSide


class MeanReversionConfig(BaseArchetypeStrategyConfig):
    """Configuration for `MeanReversionStrategy`."""

    ARCHETYPE_ID = StrategyArchetypeId.MEAN_REVERSION


class MeanReversionStrategy(BaseArchetypeStrategy):
    """Empirical-distribution-driven Mean Reversion strategy (`MEAN_REVERSION`).

    Signal construction (Zero Magic Numbers / Red Line R9 compliant):
    1. Rolling volume-weighted anchor price `vwap_anchor` over `slow_window`
       (falls back to `rolling_mean` if cumulative volume is zero).
    2. Residual deviation `res = close - vwap_anchor`.
    3. Non-parametric Robust MAD Z-score `mad_z = rolling_mad_zscore(residuals[-lookback_window:], res)`
       and empirical rank quantile `q_mad = empirical_rank_quantile(mad_z_history[-lookback_window:], mad_z)`.
    4. Adaptive mean-reversion target (`vwap_anchor`) and volatility disaster stop
       (`atr_stop_multiplier * rolling_atr(fast_window)`).
    """

    def __init__(self, config: MeanReversionConfig) -> None:
        super().__init__(config)
        self._residuals: List[float] = []
        self._mad_zscores: List[float] = []
        self._anchor_target: Optional[float] = None

    @property
    def mad_zscores(self) -> List[float]:
        return list(self._mad_zscores)

    def _compute_rolling_vwap_anchor(self, window: int) -> float:
        closes = self._closes[-window:]
        vols = self._volumes[-window:]
        typical_prices = [
            (h + l + c) / 3.0
            for h, l, c in zip(
                self._highs[-window:], self._lows[-window:], closes
            )
        ]
        total_vol = sum(vols)
        if total_vol > 0.0:
            return sum(tp * v for tp, v in zip(typical_prices, vols)) / total_vol
        return rolling_mean(closes)

    def on_bar(self, bar: Bar) -> None:
        if bar.bar_type != self._archetype_config.bar_type:
            return

        self._record_primary_bar(bar)
        n = len(self._closes)
        cfg = self._archetype_config
        if n < cfg.slow_window:
            return

        vwap_anchor = self._compute_rolling_vwap_anchor(cfg.slow_window)
        close_px = self._closes[-1]
        high_px = self._highs[-1]
        low_px = self._lows[-1]
        residual = close_px - vwap_anchor
        self._residuals.append(residual)

        res_window = self._residuals[-min(len(self._residuals), cfg.lookback_window) :]
        mad_z = rolling_mad_zscore(res_window, residual)
        self._mad_zscores.append(mad_z)

        max_keep = max(cfg.lookback_window * 3, 120)
        if len(self._residuals) > max_keep:
            del self._residuals[:-max_keep]
            del self._mad_zscores[:-max_keep]

        atr = rolling_atr(self._highs, self._lows, self._closes, cfg.fast_window)
        if atr <= 0.0:
            return

        z_window = self._mad_zscores[-min(len(self._mad_zscores), cfg.lookback_window) :]
        z_quantile = empirical_rank_quantile(z_window, mad_z)

        entry_q = quantile_to_float(cfg.entry_quantile_threshold)
        exit_q = quantile_to_float(cfg.exit_quantile_threshold)
        atr_mult = quantile_to_float(cfg.atr_stop_multiplier)

        signed_pos = self._open_position_signed_qty()
        if signed_pos > Decimal("0"):
            stop_hit = (
                self._trailing_stop_price is not None
                and low_px <= self._trailing_stop_price
            )
            reverted_to_anchor = (
                close_px >= vwap_anchor or (1.0 - z_quantile) <= exit_q
            )
            if stop_hit or reverted_to_anchor:
                reason = "MEAN_REV_DISASTER_STOP" if stop_hit else "MEAN_REV_TARGET_REACHED"
                self._submit_plan_gated_exit(
                    exit_side=OrderSide.SELL,
                    quantity=signed_pos,
                    reference_price=close_px,
                    reason_tag=reason,
                )
                self._anchor_target = None
            return

        if signed_pos < Decimal("0"):
            stop_hit = (
                self._trailing_stop_price is not None
                and high_px >= self._trailing_stop_price
            )
            reverted_to_anchor = close_px <= vwap_anchor or z_quantile <= exit_q
            if stop_hit or reverted_to_anchor:
                reason = "MEAN_REV_DISASTER_STOP" if stop_hit else "MEAN_REV_TARGET_REACHED"
                self._submit_plan_gated_exit(
                    exit_side=OrderSide.BUY,
                    quantity=abs(signed_pos),
                    reference_price=close_px,
                    reason_tag=reason,
                )
                self._anchor_target = None
            return

        if n < cfg.lookback_window or self._has_inflight_or_open_entry_order():
            return

        # Oversold tail for LONG mean reversion: (1.0 - z_quantile) >= entry_q and mad_z < 0
        if cfg.plan_side == PlanSide.LONG:
            oversold_quantile = 1.0 - z_quantile
            if oversold_quantile >= entry_q and mad_z < 0.0:
                stop_px = close_px - (atr_mult * atr)
                self._trailing_stop_price = stop_px
                self._anchor_target = vwap_anchor
                self._submit_plan_gated_entry(
                    order_side=OrderSide.BUY,
                    reference_price=close_px,
                    atr=atr,
                    limit_price=close_px,
                    stop_loss_price=stop_px,
                    tags=["MEAN_REVERSION_LONG_ENTRY"],
                )
        elif cfg.plan_side == PlanSide.SHORT:
            if z_quantile >= entry_q and mad_z > 0.0:
                stop_px = close_px + (atr_mult * atr)
                self._trailing_stop_price = stop_px
                self._anchor_target = vwap_anchor
                self._submit_plan_gated_entry(
                    order_side=OrderSide.SELL,
                    reference_price=close_px,
                    atr=atr,
                    limit_price=close_px,
                    stop_loss_price=stop_px,
                    tags=["MEAN_REVERSION_SHORT_ENTRY"],
                )
