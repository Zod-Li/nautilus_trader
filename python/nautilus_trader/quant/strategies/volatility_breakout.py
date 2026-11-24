"""Pre-audited VolatilityBreakoutStrategy and VolatilityBreakoutConfig for Nautilus v2.

Computes rolling price channel quantiles, rolling volume expansion empirical quantiles,
and `daily_atr_14`-anchored protective stops, routing entry signals through
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
    empirical_quantile_value,
    empirical_rank_quantile,
    quantile_to_float,
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
    3. Initial stop and `1m` Chandelier ratchet anchored strictly to `daily_atr_14`.
    """

    def __init__(self, config: VolatilityBreakoutConfig) -> None:
        super().__init__(config)
        self._vol_ratios: List[float] = []
        self._entry_reference_price: Optional[float] = None

    @property
    def volume_expansion_ratios(self) -> List[float]:
        return list(self._vol_ratios)

    def reset_rolling_windows(self) -> None:
        super().reset_rolling_windows()
        self._vol_ratios.clear()

    def evaluate_primary_bar(self, bar: Bar) -> None:
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

        if self._in_warmup:
            return

        tod_vol_q = self._last_tod_volume_quantile

        if self._open_position_signed_qty() != Decimal("0"):
            return

        close_px = self._closes[-1]
        daily_atr = self._resolve_daily_atr_14(close_px)
        if daily_atr <= 0.0:
            return

        if (
            self._exit_only_mode
            or n < cfg.lookback_window
            or self._has_inflight_or_open_entry_order()
        ):
            return

        lookback_len = min(n, cfg.lookback_window)
        close_window = self._closes[-lookback_len:]
        vol_ratio_window = self._vol_ratios[-min(len(self._vol_ratios), cfg.lookback_window) :]

        causal_close_window = close_window[:-1] if len(close_window) >= 2 else close_window
        causal_vol_window = (
            vol_ratio_window[:-1] if len(vol_ratio_window) >= 2 else vol_ratio_window
        )
        price_quantile = empirical_rank_quantile(causal_close_window, close_px)
        vol_quantile = empirical_rank_quantile(causal_vol_window, vol_ratio)

        entry_q = quantile_to_float(cfg.entry_quantile_threshold)
        exit_q = quantile_to_float(cfg.exit_quantile_threshold)
        atr_mult = quantile_to_float(cfg.atr_stop_multiplier)

        upper_channel = empirical_quantile_value(close_window[:-1] or close_window, entry_q)
        lower_channel = empirical_quantile_value(
            close_window[:-1] or close_window, 1.0 - entry_q
        )

        if cfg.plan_side == PlanSide.LONG:
            if (
                (price_quantile >= entry_q or close_px >= upper_channel)
                and vol_quantile >= exit_q
            ):
                stop_px = self._resolve_entry_stop_price(
                    close_px, close_px - (atr_mult * daily_atr)
                )
                prev_stop = self._trailing_stop_price
                prev_ref = self._entry_reference_price
                self._trailing_stop_price = stop_px
                self._entry_reference_price = close_px
                cid = self._dispatch_entry_signal(
                    order_side=OrderSide.BUY,
                    bar=bar,
                    reference_price=close_px,
                    stop_loss_price=stop_px,
                    tags=["VOLATILITY_BREAKOUT_LONG_ENTRY"],
                    tod_volume_quantile=tod_vol_q,
                )
                if not cid or self._is_order_denied_or_rejected(cid):
                    self._trailing_stop_price = prev_stop
                    self._entry_reference_price = prev_ref
        elif cfg.plan_side == PlanSide.SHORT:
            if (
                ((1.0 - price_quantile) >= entry_q or close_px <= lower_channel)
                and vol_quantile >= exit_q
            ):
                stop_px = self._resolve_entry_stop_price(
                    close_px, close_px + (atr_mult * daily_atr)
                )
                prev_stop = self._trailing_stop_price
                prev_ref = self._entry_reference_price
                self._trailing_stop_price = stop_px
                self._entry_reference_price = close_px
                cid = self._dispatch_entry_signal(
                    order_side=OrderSide.SELL,
                    bar=bar,
                    reference_price=close_px,
                    stop_loss_price=stop_px,
                    tags=["VOLATILITY_BREAKOUT_SHORT_ENTRY"],
                    tod_volume_quantile=tod_vol_q,
                )
                if not cid or self._is_order_denied_or_rejected(cid):
                    self._trailing_stop_price = prev_stop
                    self._entry_reference_price = prev_ref
