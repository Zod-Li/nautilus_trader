"""Pre-audited MeanReversionStrategy and MeanReversionConfig for Nautilus v2.

Computes rolling VWAP/anchor residuals, non-parametric Robust MAD Z-scores,
and empirical rank quantiles over `lookback_window`, routing entry signals through
`BaseArchetypeStrategy` (`ExecutionSpec` + `PreTradeRiskGate` + `COLLARED_TWAP` / `DIRECT_COLLARED_LIMIT`).
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
    rolling_mad_zscore,
    rolling_vwap,
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
    4. Initial stop and `1m` Chandelier ratchet anchored strictly to `daily_atr_14`
       (per Spec 0007 M3, 15m oscillator target exits are abolished; `exit_quantile_threshold`
       is retained only as a shared `ArchetypeStrategyConfig` field).
    """

    def __init__(self, config: MeanReversionConfig) -> None:
        super().__init__(config)
        self._residuals: List[float] = []
        self._mad_zscores: List[float] = []
        self._anchor_target: Optional[float] = None

    @property
    def mad_zscores(self) -> List[float]:
        return list(self._mad_zscores)

    def reset_rolling_windows(self) -> None:
        super().reset_rolling_windows()
        self._residuals.clear()
        self._mad_zscores.clear()
        self._anchor_target = None

    def _compute_rolling_vwap_anchor(self, window: int) -> float:
        typical_prices = [
            (h + lo + c) / 3.0
            for h, lo, c in zip(
                self._highs[-window:], self._lows[-window:], self._closes[-window:]
            )
        ]
        return rolling_vwap(typical_prices, self._volumes[-window:], window)

    def evaluate_primary_bar(self, bar: Bar) -> None:
        n = len(self._closes)
        cfg = self._archetype_config
        if n < cfg.slow_window:
            return

        vwap_anchor = self._compute_rolling_vwap_anchor(cfg.slow_window)
        close_px = self._closes[-1]
        residual = close_px - vwap_anchor
        self._residuals.append(residual)

        res_window = self._residuals[-min(len(self._residuals), cfg.lookback_window) :]
        causal_res_window = res_window[:-1] if len(res_window) >= 2 else res_window
        mad_z = rolling_mad_zscore(causal_res_window, residual)
        self._mad_zscores.append(mad_z)

        max_keep = max(cfg.lookback_window * 3, 120)
        if len(self._residuals) > max_keep:
            del self._residuals[:-max_keep]
            del self._mad_zscores[:-max_keep]

        if self._in_warmup:
            return

        tod_vol_q = self._last_tod_volume_quantile
        z_window = self._mad_zscores[-min(len(self._mad_zscores), cfg.lookback_window) :]
        causal_z_window = z_window[:-1] if len(z_window) >= 2 else z_window
        z_quantile = empirical_rank_quantile(causal_z_window, mad_z)

        if self._open_position_signed_qty() != Decimal("0"):
            return

        daily_atr = self._resolve_daily_atr_14(close_px)
        if daily_atr <= 0.0:
            return

        if (
            self._exit_only_mode
            or n < cfg.lookback_window
            or self._has_inflight_or_open_entry_order()
        ):
            return

        entry_q = quantile_to_float(cfg.entry_quantile_threshold)
        atr_mult = quantile_to_float(cfg.atr_stop_multiplier)

        # Oversold tail for LONG mean reversion: (1.0 - z_quantile) >= entry_q and mad_z < 0
        if cfg.plan_side == PlanSide.LONG:
            oversold_quantile = 1.0 - z_quantile
            if oversold_quantile >= entry_q and mad_z < 0.0:
                stop_px = self._resolve_entry_stop_price(
                    close_px, close_px - (atr_mult * daily_atr)
                )
                prev_stop = self._trailing_stop_price
                prev_anchor = self._anchor_target
                self._trailing_stop_price = stop_px
                self._anchor_target = vwap_anchor
                cid = self._dispatch_entry_signal(
                    order_side=OrderSide.BUY,
                    bar=bar,
                    reference_price=close_px,
                    stop_loss_price=stop_px,
                    tags=["MEAN_REVERSION_LONG_ENTRY"],
                    tod_volume_quantile=tod_vol_q,
                )
                if not cid or self._is_order_denied_or_rejected(cid):
                    self._trailing_stop_price = prev_stop
                    self._anchor_target = prev_anchor
        elif cfg.plan_side == PlanSide.SHORT:
            if z_quantile >= entry_q and mad_z > 0.0:
                stop_px = self._resolve_entry_stop_price(
                    close_px, close_px + (atr_mult * daily_atr)
                )
                prev_stop = self._trailing_stop_price
                prev_anchor = self._anchor_target
                self._trailing_stop_price = stop_px
                self._anchor_target = vwap_anchor
                cid = self._dispatch_entry_signal(
                    order_side=OrderSide.SELL,
                    bar=bar,
                    reference_price=close_px,
                    stop_loss_price=stop_px,
                    tags=["MEAN_REVERSION_SHORT_ENTRY"],
                    tod_volume_quantile=tod_vol_q,
                )
                if not cid or self._is_order_denied_or_rejected(cid):
                    self._trailing_stop_price = prev_stop
                    self._anchor_target = prev_anchor
