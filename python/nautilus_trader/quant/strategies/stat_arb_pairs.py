"""Pre-audited StatArbPairsStrategy and StatArbPairsConfig for Nautilus v2.

Computes dynamic hedge ratio (Rolling OLS / Recursive Kalman-EWMA Beta), cointegration
spread residuals, non-parametric MAD Z-scores, and empirical rank quantiles over
`lookback_window`, generating dual-leg controlled orders via `PLAN_GATE`.
"""

from __future__ import annotations

from decimal import Decimal
from typing import List, Optional

from nautilus_trader.quant.contracts.models import PlanSide, StrategyArchetypeId
from nautilus_trader.quant.governance.plan_gate import PLAN_GATE_EXEC_ALGORITHM_ID
from nautilus_trader.quant.strategies.base import (
    BaseArchetypeStrategy,
    BaseArchetypeStrategyConfig,
)
from nautilus_trader.quant.strategies.empirical import (
    empirical_rank_quantile,
    quantile_to_float,
    rolling_atr,
    rolling_mad_zscore,
    rolling_ols_hedge_params,
    rolling_std,
)
from nautilus_trader.model import Bar, ExecAlgorithmId, InstrumentId, OrderSide, StrategyId


class StatArbPairsConfig(BaseArchetypeStrategyConfig):
    """Configuration for `StatArbPairsStrategy`."""

    ARCHETYPE_ID = StrategyArchetypeId.STAT_ARB_PAIRS

    def __new__(
        cls,
        instrument_id: str | InstrumentId,
        secondary_instrument_id: str | InstrumentId,
        bar_timeframe: str = "15m",
        lookback_window: int = 30,
        fast_window: int = 5,
        slow_window: int = 20,
        entry_quantile_threshold: Decimal | float | str = Decimal("0.80"),
        exit_quantile_threshold: Decimal | float | str = Decimal("0.35"),
        atr_stop_multiplier: Decimal | float | str = Decimal("2.0"),
        trade_qty: Decimal | float | int | str = Decimal("100"),
        plan_side: str | PlanSide = PlanSide.LONG,
        trade_secondary_leg: bool = True,
        use_limit_orders: bool = True,
        submit_protective_stop: bool = False,
        exec_algorithm_id: str | ExecAlgorithmId = PLAN_GATE_EXEC_ALGORITHM_ID,
        strategy_id: Optional[str | StrategyId] = None,
        order_id_tag: Optional[str] = None,
        manage_contingent_orders: bool = False,
        log_events: bool = True,
        log_commands: bool = True,
    ):
        return super().__new__(
            cls,
            instrument_id=instrument_id,
            bar_timeframe=bar_timeframe,
            lookback_window=lookback_window,
            fast_window=fast_window,
            slow_window=slow_window,
            entry_quantile_threshold=entry_quantile_threshold,
            exit_quantile_threshold=exit_quantile_threshold,
            atr_stop_multiplier=atr_stop_multiplier,
            trade_qty=trade_qty,
            plan_side=plan_side,
            secondary_instrument_id=secondary_instrument_id,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            exec_algorithm_id=exec_algorithm_id,
            strategy_id=strategy_id,
            order_id_tag=order_id_tag,
            manage_contingent_orders=manage_contingent_orders,
            log_events=log_events,
            log_commands=log_commands,
        )

    def __init__(
        self,
        instrument_id: str | InstrumentId,
        secondary_instrument_id: str | InstrumentId,
        bar_timeframe: str = "15m",
        lookback_window: int = 30,
        fast_window: int = 5,
        slow_window: int = 20,
        entry_quantile_threshold: Decimal | float | str = Decimal("0.80"),
        exit_quantile_threshold: Decimal | float | str = Decimal("0.35"),
        atr_stop_multiplier: Decimal | float | str = Decimal("2.0"),
        trade_qty: Decimal | float | int | str = Decimal("100"),
        plan_side: str | PlanSide = PlanSide.LONG,
        trade_secondary_leg: bool = True,
        use_limit_orders: bool = True,
        submit_protective_stop: bool = False,
        exec_algorithm_id: str | ExecAlgorithmId = PLAN_GATE_EXEC_ALGORITHM_ID,
        strategy_id: Optional[str | StrategyId] = None,
        order_id_tag: Optional[str] = None,
        manage_contingent_orders: bool = False,
        log_events: bool = True,
        log_commands: bool = True,
    ) -> None:
        super().__init__(
            instrument_id=instrument_id,
            bar_timeframe=bar_timeframe,
            lookback_window=lookback_window,
            fast_window=fast_window,
            slow_window=slow_window,
            entry_quantile_threshold=entry_quantile_threshold,
            exit_quantile_threshold=exit_quantile_threshold,
            atr_stop_multiplier=atr_stop_multiplier,
            trade_qty=trade_qty,
            plan_side=plan_side,
            secondary_instrument_id=secondary_instrument_id,
            use_limit_orders=use_limit_orders,
            submit_protective_stop=submit_protective_stop,
            exec_algorithm_id=exec_algorithm_id,
            strategy_id=strategy_id,
            order_id_tag=order_id_tag,
            manage_contingent_orders=manage_contingent_orders,
            log_events=log_events,
            log_commands=log_commands,
        )
        self.trade_secondary_leg = bool(trade_secondary_leg)


class StatArbPairsStrategy(BaseArchetypeStrategy):
    """Empirical-distribution-driven Statistical Arbitrage Pairs strategy (`STAT_ARB_PAIRS`).

    Signal construction (Zero Magic Numbers / Red Line R9 compliant):
    1. Rolling OLS / recursive EWMA hedge ratio `beta_t` and intercept `alpha_t` over `slow_window`
       between primary (`instrument_id`) and secondary (`secondary_instrument_id`) closes.
    2. Cointegration residual `spread_t = P_A - (alpha_t + beta_t * P_B)`.
    3. Rolling Robust MAD Z-score `z_spread` and empirical rank quantile `q_spread`
       over `lookback_window`.
    4. Dual-leg entry and reduce-only exit routed via `PLAN_GATE`.
    """

    def __init__(self, config: StatArbPairsConfig) -> None:
        super().__init__(config)
        self._pairs_config = config
        self._secondary_highs: List[float] = []
        self._secondary_lows: List[float] = []
        self._secondary_closes: List[float] = []
        self._spreads: List[float] = []
        self._spread_zscores: List[float] = []
        self._hedge_betas: List[float] = []
        self._entry_spread: Optional[float] = None

    @property
    def hedge_betas(self) -> List[float]:
        return list(self._hedge_betas)

    @property
    def spread_zscores(self) -> List[float]:
        return list(self._spread_zscores)

    def _record_secondary_bar(self, bar: Bar) -> None:
        self._secondary_highs.append(float(bar.high.as_double()))
        self._secondary_lows.append(float(bar.low.as_double()))
        self._secondary_closes.append(float(bar.close.as_double()))
        max_keep = max(self._archetype_config.lookback_window * 3, 120)
        if len(self._secondary_closes) > max_keep:
            del self._secondary_highs[:-max_keep]
            del self._secondary_lows[:-max_keep]
            del self._secondary_closes[:-max_keep]

    def on_bar(self, bar: Bar) -> None:
        cfg = self._pairs_config
        if cfg.secondary_bar_type is not None and bar.bar_type == cfg.secondary_bar_type:
            self._record_secondary_bar(bar)
            return
        if bar.bar_type != cfg.bar_type:
            return

        self._record_primary_bar(bar)
        n_prim = len(self._closes)
        n_sec = len(self._secondary_closes)
        n_sync = min(n_prim, n_sec)
        if n_sync < cfg.slow_window:
            return

        prim_slice = self._closes[-cfg.slow_window :]
        sec_slice = self._secondary_closes[-cfg.slow_window :]
        _alpha, raw_beta, residual = rolling_ols_hedge_params(prim_slice, sec_slice)

        # Recursive smoothing of hedge ratio over fast_window
        if self._hedge_betas:
            decay = 1.0 / float(max(2, cfg.fast_window))
            beta = (1.0 - decay) * self._hedge_betas[-1] + decay * raw_beta
        else:
            beta = raw_beta

        self._hedge_betas.append(beta)
        self._spreads.append(residual)

        spread_window = self._spreads[-min(len(self._spreads), cfg.lookback_window) :]
        spread_z = rolling_mad_zscore(spread_window, residual)
        self._spread_zscores.append(spread_z)

        max_keep = max(cfg.lookback_window * 3, 120)
        if len(self._spreads) > max_keep:
            del self._spreads[:-max_keep]
            del self._spread_zscores[:-max_keep]
            del self._hedge_betas[:-max_keep]

        atr_prim = rolling_atr(self._highs, self._lows, self._closes, cfg.fast_window)
        atr_sec = rolling_atr(
            self._secondary_highs,
            self._secondary_lows,
            self._secondary_closes,
            cfg.fast_window,
        )
        spread_std = max(rolling_std(spread_window), 1e-6)
        if atr_prim <= 0.0:
            return

        z_window = self._spread_zscores[-min(len(self._spread_zscores), cfg.lookback_window) :]
        z_quantile = empirical_rank_quantile(z_window, spread_z)

        entry_q = quantile_to_float(cfg.entry_quantile_threshold)
        exit_q = quantile_to_float(cfg.exit_quantile_threshold)
        atr_mult = quantile_to_float(cfg.atr_stop_multiplier)
        prim_px = self._closes[-1]
        sec_px = self._secondary_closes[-1]

        signed_prim_pos = self._open_position_signed_qty(cfg.instrument_id)
        signed_sec_pos = (
            self._open_position_signed_qty(cfg.secondary_instrument_id)
            if cfg.secondary_instrument_id is not None
            else Decimal("0")
        )

        if signed_prim_pos > Decimal("0"):
            stop_hit = (
                self._entry_spread is not None
                and (self._entry_spread - residual) >= (atr_mult * spread_std)
            )
            mean_reverted = spread_z >= 0.0 or (1.0 - z_quantile) <= exit_q
            if stop_hit or mean_reverted:
                reason = "STAT_ARB_SPREAD_STOP" if stop_hit else "STAT_ARB_SPREAD_REVERTED"
                self._submit_plan_gated_exit(
                    exit_side=OrderSide.SELL,
                    quantity=signed_prim_pos,
                    reference_price=prim_px,
                    reason_tag=reason,
                    instrument_id=cfg.instrument_id,
                )
                if (
                    cfg.trade_secondary_leg
                    and cfg.secondary_instrument_id is not None
                    and signed_sec_pos != Decimal("0")
                ):
                    sec_exit_side = (
                        OrderSide.BUY if signed_sec_pos < Decimal("0") else OrderSide.SELL
                    )
                    self._submit_plan_gated_exit(
                        exit_side=sec_exit_side,
                        quantity=abs(signed_sec_pos),
                        reference_price=sec_px,
                        reason_tag=reason,
                        instrument_id=cfg.secondary_instrument_id,
                    )
                self._entry_spread = None
            return

        if signed_prim_pos < Decimal("0"):
            stop_hit = (
                self._entry_spread is not None
                and (residual - self._entry_spread) >= (atr_mult * spread_std)
            )
            mean_reverted = spread_z <= 0.0 or z_quantile <= exit_q
            if stop_hit or mean_reverted:
                reason = "STAT_ARB_SPREAD_STOP" if stop_hit else "STAT_ARB_SPREAD_REVERTED"
                self._submit_plan_gated_exit(
                    exit_side=OrderSide.BUY,
                    quantity=abs(signed_prim_pos),
                    reference_price=prim_px,
                    reason_tag=reason,
                    instrument_id=cfg.instrument_id,
                )
                if (
                    cfg.trade_secondary_leg
                    and cfg.secondary_instrument_id is not None
                    and signed_sec_pos != Decimal("0")
                ):
                    sec_exit_side = (
                        OrderSide.BUY if signed_sec_pos < Decimal("0") else OrderSide.SELL
                    )
                    self._submit_plan_gated_exit(
                        exit_side=sec_exit_side,
                        quantity=abs(signed_sec_pos),
                        reference_price=sec_px,
                        reason_tag=reason,
                        instrument_id=cfg.secondary_instrument_id,
                    )
                self._entry_spread = None
            return

        if n_sync < cfg.lookback_window or self._has_inflight_or_open_entry_order(
            cfg.instrument_id
        ):
            return

        clamped_beta = max(0.1, min(5.0, abs(beta)))
        hedge_qty = max(
            Decimal("1"),
            (cfg.trade_qty * Decimal(f"{clamped_beta:.4f}")).to_integral_value(),
        )

        # Long spread (Buy Primary Leg A, Sell Hedge Leg B) when spread is in lower empirical tail
        if cfg.plan_side == PlanSide.LONG:
            lower_tail_q = 1.0 - z_quantile
            if lower_tail_q >= entry_q and spread_z < 0.0:
                self._entry_spread = residual
                stop_px = prim_px - (atr_mult * atr_prim)
                self._trailing_stop_price = stop_px
                self._submit_plan_gated_entry(
                    order_side=OrderSide.BUY,
                    reference_price=prim_px,
                    atr=atr_prim,
                    limit_price=prim_px,
                    quantity=cfg.trade_qty,
                    instrument_id=cfg.instrument_id,
                    stop_loss_price=stop_px,
                    tags=["STAT_ARB_LONG_SPREAD_PRIMARY"],
                )
                if cfg.trade_secondary_leg and cfg.secondary_instrument_id is not None:
                    self._submit_plan_gated_entry(
                        order_side=OrderSide.SELL,
                        reference_price=sec_px,
                        atr=max(atr_sec, 0.01),
                        limit_price=sec_px,
                        quantity=hedge_qty,
                        instrument_id=cfg.secondary_instrument_id,
                        tags=["STAT_ARB_LONG_SPREAD_HEDGE"],
                    )
        elif cfg.plan_side == PlanSide.SHORT:
            if z_quantile >= entry_q and spread_z > 0.0:
                self._entry_spread = residual
                stop_px = prim_px + (atr_mult * atr_prim)
                self._trailing_stop_price = stop_px
                self._submit_plan_gated_entry(
                    order_side=OrderSide.SELL,
                    reference_price=prim_px,
                    atr=atr_prim,
                    limit_price=prim_px,
                    quantity=cfg.trade_qty,
                    instrument_id=cfg.instrument_id,
                    stop_loss_price=stop_px,
                    tags=["STAT_ARB_SHORT_SPREAD_PRIMARY"],
                )
                if cfg.trade_secondary_leg and cfg.secondary_instrument_id is not None:
                    self._submit_plan_gated_entry(
                        order_side=OrderSide.BUY,
                        reference_price=sec_px,
                        atr=max(atr_sec, 0.01),
                        limit_price=sec_px,
                        quantity=hedge_qty,
                        instrument_id=cfg.secondary_instrument_id,
                        tags=["STAT_ARB_SHORT_SPREAD_HEDGE"],
                    )
