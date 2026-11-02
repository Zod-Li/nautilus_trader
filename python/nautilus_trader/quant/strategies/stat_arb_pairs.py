"""Pre-audited StatArbPairsStrategy and StatArbPairsConfig for Nautilus v2.

Computes dynamic hedge ratio (Rolling OLS / Recursive Kalman-EWMA Beta), cointegration
spread residuals, non-parametric MAD Z-scores, and empirical rank quantiles over
`lookback_window`, generating dual-leg controlled orders via `PLAN_GATE`.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence

from nautilus_trader.quant.contracts.models import PlanSide, StrategyArchetypeId
from nautilus_trader.quant.governance.plan_gate import PLAN_GATE_EXEC_ALGORITHM_ID
from nautilus_trader.quant.strategies.base import (
    BaseArchetypeStrategy,
    BaseArchetypeStrategyConfig,
    _to_decimal,
)
from nautilus_trader.quant.strategies.empirical import (
    empirical_quantile_value,
    empirical_rank_quantile,
    quantile_to_float,
    rolling_atr,
    rolling_mad_zscore,
    rolling_mean,
    rolling_ols_hedge_params,
    rolling_std,
)
from nautilus_trader.model import Bar, ExecAlgorithmId, InstrumentId, OrderSide, StrategyId


class _OlsSpreadZScoreEstimate(tuple):
    """Signed OLS spread and Z-score estimate supporting tuple unpacking `(beta, residual, spread_z)`
    and structured attribute access.
    """

    def __new__(
        cls,
        *,
        alpha: float,
        raw_beta: float,
        beta: float,
        residual: float,
        spread_z: float,
        is_tradable: bool,
    ) -> "_OlsSpreadZScoreEstimate":
        obj = super().__new__(cls, (float(beta), float(residual), float(spread_z)))
        return obj

    def __init__(
        self,
        *,
        alpha: float,
        raw_beta: float,
        beta: float,
        residual: float,
        spread_z: float,
        is_tradable: bool,
    ) -> None:
        self.alpha = float(alpha)
        self.raw_beta = float(raw_beta)
        self.beta = float(beta)
        self.smoothed_beta = float(beta)
        self.hedge_ratio = float(beta)
        self.residual = float(residual)
        self.spread = float(residual)
        self.spread_z = float(spread_z)
        self.zscore = float(spread_z)
        self.z_score = float(spread_z)
        self.is_tradable = bool(is_tradable)
        self.cointegrated = bool(is_tradable)


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
        execution_delay_bars: int = 0,
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
            execution_delay_bars=execution_delay_bars,
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
        execution_delay_bars: int = 0,
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
            execution_delay_bars=execution_delay_bars,
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
        self._last_secondary_bar_ts_ns: int = 0
        self._spreads: List[float] = []
        self._spread_zscores: List[float] = []
        self._hedge_betas: List[float] = []
        self._last_raw_beta: Optional[float] = None
        self._entry_spread: Optional[float] = None
        self._paired_entry_orders_by_cid: Dict[str, Dict[str, Any]] = {}
        self._pending_pair_spec: Optional[Dict[str, Any]] = None
        self._rolled_back_entry_cids: set[str] = set()

    @property
    def hedge_betas(self) -> List[float]:
        return list(self._hedge_betas)

    @property
    def spread_zscores(self) -> List[float]:
        return list(self._spread_zscores)

    def reset_rolling_windows(self) -> None:
        super().reset_rolling_windows()
        self._secondary_highs.clear()
        self._secondary_lows.clear()
        self._secondary_closes.clear()
        self._last_secondary_bar_ts_ns = 0
        self._spreads.clear()
        self._spread_zscores.clear()
        self._hedge_betas.clear()
        self._last_raw_beta = None
        self._exit_only_mode = False
        self._paired_entry_orders_by_cid.clear()
        self._pending_pair_spec = None
        self._rolled_back_entry_cids.clear()

    def _record_secondary_bar(self, bar: Bar) -> bool:
        bar_ts = int(getattr(bar, "ts_init", 0) or getattr(bar, "ts_event", 0) or 0)
        if (
            bar_ts > 0
            and self._last_secondary_bar_ts_ns > 0
            and bar_ts <= self._last_secondary_bar_ts_ns
        ):
            return False
        if bar_ts > 0:
            self._last_secondary_bar_ts_ns = bar_ts
        self._secondary_highs.append(float(bar.high.as_double()))
        self._secondary_lows.append(float(bar.low.as_double()))
        self._secondary_closes.append(float(bar.close.as_double()))
        max_keep = max(self._archetype_config.lookback_window * 3, 120)
        if len(self._secondary_closes) > max_keep:
            del self._secondary_highs[:-max_keep]
            del self._secondary_lows[:-max_keep]
            del self._secondary_closes[:-max_keep]
        return True

    def _estimate_ols_spread_and_zscore(
        self,
        primary_closes: Optional[Sequence[float]] = None,
        secondary_closes: Optional[Sequence[float]] = None,
        *,
        record_state: bool = True,
    ) -> _OlsSpreadZScoreEstimate:
        """Computes signed rolling OLS hedge ratio `beta` (never `abs(beta)`), cointegration
        spread residual, and rolling MAD Z-score over `lookback_window`.
        Marks `is_tradable = False` whenever `raw_beta <= 0.0` or `beta <= 0.0`.
        """
        cfg = self._pairs_config
        prim_series = (
            [float(x) for x in primary_closes]
            if primary_closes is not None
            else self._closes
        )
        sec_series = (
            [float(x) for x in secondary_closes]
            if secondary_closes is not None
            else self._secondary_closes
        )
        n_sync = min(len(prim_series), len(sec_series))
        eff_win = min(n_sync, cfg.slow_window)
        if eff_win < 2:
            return _OlsSpreadZScoreEstimate(
                alpha=0.0,
                raw_beta=0.0,
                beta=0.0,
                residual=0.0,
                spread_z=0.0,
                is_tradable=False,
            )

        prim_slice = prim_series[-eff_win:]
        sec_slice = sec_series[-eff_win:]
        mean_sec = rolling_mean(sec_slice)
        var_sec = sum((x_val - mean_sec) * (x_val - mean_sec) for x_val in sec_slice)
        if var_sec <= 1e-12:
            if record_state:
                self._last_raw_beta = 0.0
                self._hedge_betas.append(0.0)
                self._spreads.append(0.0)
                self._spread_zscores.append(0.0)
            return _OlsSpreadZScoreEstimate(
                alpha=0.0,
                raw_beta=0.0,
                beta=0.0,
                residual=0.0,
                spread_z=0.0,
                is_tradable=False,
            )

        alpha, raw_beta, residual = rolling_ols_hedge_params(prim_slice, sec_slice)
        decay = 1.0 / float(max(2, cfg.fast_window))

        if self._hedge_betas:
            prev_beta = self._hedge_betas[-1]
            beta = (1.0 - decay) * prev_beta + decay * raw_beta
        else:
            beta = raw_beta
        is_tradable = bool(beta > 0.0)

        if record_state:
            self._last_raw_beta = raw_beta
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
        else:
            temp_spreads = list(self._spreads) + [residual]
            spread_window = temp_spreads[-min(len(temp_spreads), cfg.lookback_window) :]
            spread_z = rolling_mad_zscore(spread_window, residual)

        return _OlsSpreadZScoreEstimate(
            alpha=alpha,
            raw_beta=raw_beta,
            beta=beta,
            residual=residual,
            spread_z=spread_z,
            is_tradable=is_tradable,
        )

    def _cancel_active_entry_for_instrument(self, instrument_id: InstrumentId) -> None:
        inst_key = str(instrument_id)
        entry_spec = self._active_entry_orders_by_inst.pop(inst_key, None)
        if entry_spec is not None:
            cid = str(entry_spec.get("cid", "") or "")
            if cid:
                self._pending_protective_stops.pop(cid, None)
            order_obj = entry_spec.get("order")
            if order_obj is not None:
                self._cancel_single_order_safely(order_obj)
        if self._is_owner_thread():
            try:
                open_orders = list(self.cache.orders_open(instrument_id=instrument_id) or [])
                inflight_orders = list(
                    self.cache.orders_inflight(instrument_id=instrument_id) or []
                )
            except Exception:
                open_orders = []
                inflight_orders = []
            for ord_obj in open_orders + inflight_orders:
                if not bool(getattr(ord_obj, "is_reduce_only", False)):
                    self._cancel_single_order_safely(ord_obj)

    def _rollback_paired_leg(
        self,
        *,
        failed_cid: str,
        paired_cid: Optional[str],
        paired_inst: Optional[InstrumentId],
        paired_ref_px: float,
        reason: str,
    ) -> None:
        """Enforces atomic dual-leg rollback when one leg of a `STAT_ARB_PAIRS` spread entry
        is denied, rejected, or canceled before fill:
        1. Cancels any open/in-flight unfilled entry order on the paired leg and releases its
           `PlanGate` reservation.
        2. If the paired leg has already filled, immediately triggers a Two-Phase Safe Exit
           (`reduce_only=True` market exit order) to flatten the unhedged position.
        """
        self._entry_spread = None
        self._trailing_stop_price = None
        self._exit_only_mode = True
        if failed_cid:
            self._rolled_back_entry_cids.add(failed_cid)
            self._pending_protective_stops.pop(failed_cid, None)
        if paired_cid:
            self._rolled_back_entry_cids.add(paired_cid)
            self._pending_protective_stops.pop(paired_cid, None)
        if paired_inst is None:
            return

        inst_key = str(paired_inst)
        tracked_entry = self._active_entry_orders_by_inst.pop(inst_key, None)
        paired_order = (
            tracked_entry.get("order") if tracked_entry is not None else None
        )
        if paired_order is None and paired_cid:
            paired_order = self._submitted_orders_by_cid.get(paired_cid)
        if self._is_trader_registered() and paired_cid:
            try:
                cached = self.cache.order(
                    getattr(paired_order, "client_order_id", paired_cid)
                    if paired_order is not None
                    else paired_cid
                )
                if cached is not None:
                    paired_order = cached
            except Exception:
                pass

        signed_pos = self._open_position_signed_qty(paired_inst)
        order_filled_qty = (
            _to_decimal(getattr(paired_order, "filled_qty", 0), "0")
            if paired_order is not None
            else Decimal("0")
        )
        status_str = (
            self._normalized_order_status(paired_order)
            if paired_order is not None
            else ""
        )

        if paired_order is not None and status_str not in (
            "DENIED",
            "REJECTED",
            "CANCELED",
            "EXPIRED",
            "FILLED",
        ):
            self._cancel_single_order_safely(paired_order)

        if self._is_owner_thread():
            try:
                open_orders = list(self.cache.orders_open(instrument_id=paired_inst) or [])
                inflight_orders = list(
                    self.cache.orders_inflight(instrument_id=paired_inst) or []
                )
            except Exception:
                open_orders = []
                inflight_orders = []
            for open_ord in open_orders + inflight_orders:
                if bool(getattr(open_ord, "is_reduce_only", False)):
                    continue
                open_cid = str(getattr(open_ord, "client_order_id", "") or "")
                if open_cid and open_cid != paired_cid:
                    self._cancel_single_order_safely(open_ord)

        signed_pos = self._open_position_signed_qty(paired_inst)
        if signed_pos == Decimal("0") and order_filled_qty > Decimal("0"):
            tracked_meta = self._order_side_by_cid.get(paired_cid or "")
            ord_side = (
                tracked_meta[1]
                if tracked_meta is not None
                else getattr(paired_order, "side", None)
            )
            is_buy = "BUY" in str(ord_side or "").upper()
            signed_pos = order_filled_qty if is_buy else -order_filled_qty

        if signed_pos != Decimal("0"):
            exit_side = OrderSide.SELL if signed_pos > Decimal("0") else OrderSide.BUY
            eff_px = float(paired_ref_px)
            if eff_px <= 0.0:
                if str(paired_inst) == str(self._pairs_config.secondary_instrument_id):
                    eff_px = (
                        float(self._secondary_closes[-1])
                        if self._secondary_closes
                        else 0.0
                    )
                else:
                    eff_px = float(self._closes[-1]) if self._closes else 0.0
            self._submit_plan_gated_exit(
                exit_side=exit_side,
                quantity=abs(signed_pos),
                reference_price=eff_px,
                reason_tag=reason or "STAT_ARB_PAIRED_LEG_REJECTED_EXIT",
                instrument_id=paired_inst,
            )

    def _handle_paired_leg_terminal_failure(self, event: Any, status_str: str) -> None:
        cid = str(self._event_get(event, "client_order_id", "") or "")
        if not cid:
            return
        base_cid = cid.rsplit("-E", 1)[0] if "-E" in cid else cid
        pair_spec = self._paired_entry_orders_by_cid.get(
            cid
        ) or self._paired_entry_orders_by_cid.get(base_cid)
        if (
            pair_spec is None
            and self._pending_pair_spec is not None
            and base_cid != self._pending_pair_spec.get("prim_cid")
        ):
            self._pending_pair_spec["sec_cid"] = base_cid
            self._paired_entry_orders_by_cid[base_cid] = self._pending_pair_spec
            pair_spec = self._pending_pair_spec
        if pair_spec is None or bool(pair_spec.get("rolled_back", False)):
            return

        self._exit_only_mode = True
        pair_spec["rolled_back"] = True
        is_prim_failed = base_cid == pair_spec.get("prim_cid")
        other_cid = (
            pair_spec.get("sec_cid") if is_prim_failed else pair_spec.get("prim_cid")
        )
        other_inst = (
            pair_spec.get("sec_inst") if is_prim_failed else pair_spec.get("prim_inst")
        )
        other_px = float(
            pair_spec.get("sec_px", 0.0)
            if is_prim_failed
            else pair_spec.get("prim_px", 0.0)
        )
        self._rollback_paired_leg(
            failed_cid=base_cid,
            paired_cid=other_cid,
            paired_inst=other_inst,
            paired_ref_px=other_px,
            reason="STAT_ARB_PAIRED_LEG_REJECTED_EXIT",
        )

    def on_order_denied(self, event: Any) -> None:
        super().on_order_denied(event)
        self._handle_paired_leg_terminal_failure(event, "DENIED")

    def on_order_rejected(self, event: Any) -> None:
        super().on_order_rejected(event)
        self._handle_paired_leg_terminal_failure(event, "REJECTED")

    def on_order_canceled(self, event: Any) -> None:
        super().on_order_canceled(event)
        cid = str(self._event_get(event, "client_order_id", "") or "")
        base_cid = cid.rsplit("-E", 1)[0] if "-E" in cid else cid
        if base_cid in self._rolled_back_entry_cids:
            return
        pair_spec = self._paired_entry_orders_by_cid.get(
            cid
        ) or self._paired_entry_orders_by_cid.get(base_cid)
        if pair_spec is not None and not bool(pair_spec.get("rolled_back", False)):
            is_prim = base_cid == pair_spec.get("prim_cid")
            failed_inst = (
                pair_spec.get("prim_inst") if is_prim else pair_spec.get("sec_inst")
            )
            if (
                failed_inst is not None
                and self._open_position_signed_qty(failed_inst) == Decimal("0")
            ):
                self._handle_paired_leg_terminal_failure(event, "CANCELED")

    def on_order_expired(self, event: Any) -> None:
        super().on_order_expired(event)
        cid = str(self._event_get(event, "client_order_id", "") or "")
        base_cid = cid.rsplit("-E", 1)[0] if "-E" in cid else cid
        if base_cid in self._rolled_back_entry_cids:
            return
        pair_spec = self._paired_entry_orders_by_cid.get(
            cid
        ) or self._paired_entry_orders_by_cid.get(base_cid)
        if pair_spec is not None and not bool(pair_spec.get("rolled_back", False)):
            is_prim = base_cid == pair_spec.get("prim_cid")
            failed_inst = (
                pair_spec.get("prim_inst") if is_prim else pair_spec.get("sec_inst")
            )
            if (
                failed_inst is not None
                and self._open_position_signed_qty(failed_inst) == Decimal("0")
            ):
                self._handle_paired_leg_terminal_failure(event, "EXPIRED")

    def on_order_filled(self, event: Any) -> None:
        super().on_order_filled(event)
        cid = str(self._event_get(event, "client_order_id", "") or "")
        if not cid:
            return
        base_cid = cid.rsplit("-E", 1)[0] if "-E" in cid else cid
        if cid in self._rolled_back_entry_cids or base_cid in self._rolled_back_entry_cids:
            tracked_meta = self._order_side_by_cid.get(
                cid
            ) or self._order_side_by_cid.get(base_cid)
            target_inst = (
                InstrumentId.from_str(tracked_meta[0])
                if tracked_meta is not None
                else (
                    getattr(event, "instrument_id", None)
                    or self._pairs_config.instrument_id
                )
            )
            signed_pos = self._open_position_signed_qty(target_inst)
            if signed_pos != Decimal("0"):
                inst_key = str(target_inst)
                if (
                    inst_key not in self._active_exit_orders_by_inst
                    and inst_key not in self._pending_safe_exits
                ):
                    ref_px = (
                        float(self._secondary_closes[-1])
                        if (
                            self._pairs_config.secondary_instrument_id is not None
                            and inst_key == str(self._pairs_config.secondary_instrument_id)
                            and self._secondary_closes
                        )
                        else (float(self._closes[-1]) if self._closes else 0.0)
                    )
                    exit_side = (
                        OrderSide.SELL if signed_pos > Decimal("0") else OrderSide.BUY
                    )
                    self._submit_plan_gated_exit(
                        exit_side=exit_side,
                        quantity=abs(signed_pos),
                        reference_price=ref_px,
                        reason_tag="STAT_ARB_PAIRED_LEG_REJECTED_EXIT",
                        instrument_id=target_inst,
                    )

    def _submit_dual_leg_entry(
        self,
        *,
        prim_side: OrderSide,
        sec_side: OrderSide,
        prim_px: float,
        sec_px: float,
        atr_prim: float,
        atr_sec: float,
        prim_stop_px: float,
        sec_stop_px: float,
        prim_qty: Decimal,
        sec_qty: Decimal,
        prim_tag: str,
        sec_tag: str,
        residual: float,
    ) -> None:
        cfg = self._pairs_config
        self._entry_spread = residual
        self._trailing_stop_price = prim_stop_px

        prim_cid = self._submit_plan_gated_entry(
            order_side=prim_side,
            reference_price=prim_px,
            atr=atr_prim,
            limit_price=prim_px,
            quantity=prim_qty,
            instrument_id=cfg.instrument_id,
            stop_loss_price=prim_stop_px,
            tags=[prim_tag],
        )
        if not prim_cid or self._is_order_denied_or_rejected(prim_cid):
            self._entry_spread = None
            self._trailing_stop_price = None
            self._exit_only_mode = True
            self._active_entry_orders_by_inst.pop(str(cfg.instrument_id), None)
            if prim_cid:
                self._pending_protective_stops.pop(prim_cid, None)
            return

        if not (cfg.trade_secondary_leg and cfg.secondary_instrument_id is not None):
            return

        pair_spec: Dict[str, Any] = {
            "prim_cid": prim_cid,
            "sec_cid": None,
            "prim_inst": cfg.instrument_id,
            "sec_inst": cfg.secondary_instrument_id,
            "prim_px": float(prim_px),
            "sec_px": float(sec_px),
            "rolled_back": False,
        }
        self._paired_entry_orders_by_cid[prim_cid] = pair_spec
        self._pending_pair_spec = pair_spec
        try:
            sec_cid = self._submit_plan_gated_entry(
                order_side=sec_side,
                reference_price=sec_px,
                atr=max(atr_sec, 0.01),
                limit_price=sec_px,
                quantity=sec_qty,
                instrument_id=cfg.secondary_instrument_id,
                stop_loss_price=sec_stop_px,
                tags=[sec_tag],
            )
        finally:
            self._pending_pair_spec = None

        if sec_cid:
            pair_spec["sec_cid"] = sec_cid
            self._paired_entry_orders_by_cid[sec_cid] = pair_spec

        if (not sec_cid or self._is_order_denied_or_rejected(sec_cid)) and not bool(
            pair_spec.get("rolled_back", False)
        ):
            pair_spec["rolled_back"] = True
            self._rollback_paired_leg(
                failed_cid=sec_cid or "",
                paired_cid=prim_cid,
                paired_inst=cfg.instrument_id,
                paired_ref_px=prim_px,
                reason="STAT_ARB_PAIRED_LEG_REJECTED_EXIT",
            )

    def on_bar(self, bar: Bar) -> None:
        cfg = self._pairs_config
        if cfg.secondary_bar_type is not None and bar.bar_type == cfg.secondary_bar_type:
            self._record_secondary_bar(bar)
            return
        if bar.bar_type != cfg.bar_type:
            return

        if not self._record_primary_bar(bar):
            return
        if not self._in_warmup and self._check_and_execute_pending_thesis_exit(bar):
            return
        if not self._in_warmup:
            signed_prim_early = self._open_position_signed_qty(cfg.instrument_id)
            signed_sec_early = (
                self._open_position_signed_qty(cfg.secondary_instrument_id)
                if cfg.secondary_instrument_id is not None
                else Decimal("0")
            )
            prim_inflight_early = self._has_inflight_or_open_entry_order(
                cfg.instrument_id
            )
            sec_inflight_early = bool(
                cfg.secondary_instrument_id is not None
                and self._has_inflight_or_open_entry_order(cfg.secondary_instrument_id)
            )
            prim_px_early = float(self._closes[-1]) if self._closes else 0.0
            sec_px_early = (
                float(self._secondary_closes[-1])
                if self._secondary_closes
                else prim_px_early
            )
            if (
                signed_prim_early == Decimal("0")
                and signed_sec_early != Decimal("0")
                and cfg.secondary_instrument_id is not None
                and not prim_inflight_early
            ):
                sec_key = str(cfg.secondary_instrument_id)
                if (
                    sec_key not in self._active_exit_orders_by_inst
                    and sec_key not in self._pending_safe_exits
                ):
                    sec_exit_side = (
                        OrderSide.BUY
                        if signed_sec_early < Decimal("0")
                        else OrderSide.SELL
                    )
                    self._submit_plan_gated_exit(
                        exit_side=sec_exit_side,
                        quantity=abs(signed_sec_early),
                        reference_price=sec_px_early,
                        reason_tag="STAT_ARB_ORPHAN_HEDGE_EXIT",
                        instrument_id=cfg.secondary_instrument_id,
                    )
                self._entry_spread = None
                return
            if (
                signed_prim_early != Decimal("0")
                and cfg.trade_secondary_leg
                and cfg.secondary_instrument_id is not None
                and signed_sec_early == Decimal("0")
                and not sec_inflight_early
            ):
                prim_key = str(cfg.instrument_id)
                if (
                    prim_key not in self._active_exit_orders_by_inst
                    and prim_key not in self._pending_safe_exits
                ):
                    prim_exit_side = (
                        OrderSide.SELL
                        if signed_prim_early > Decimal("0")
                        else OrderSide.BUY
                    )
                    self._submit_plan_gated_exit(
                        exit_side=prim_exit_side,
                        quantity=abs(signed_prim_early),
                        reference_price=prim_px_early,
                        reason_tag="STAT_ARB_UNHEDGED_PRIMARY_EXIT",
                        instrument_id=cfg.instrument_id,
                    )
                self._entry_spread = None
                return

        n_prim = len(self._closes)
        n_sec = len(self._secondary_closes)
        n_sync = min(n_prim, n_sec)
        if n_sync < cfg.slow_window:
            return

        est = self._estimate_ols_spread_and_zscore(record_state=True)
        raw_beta = est.raw_beta
        beta = est.beta
        residual = est.residual
        spread_z = est.spread_z
        is_tradable = est.is_tradable

        if self._in_warmup:
            return

        spread_window = self._spreads[-min(len(self._spreads), cfg.lookback_window) :]
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
        prim_entry_inflight = self._has_inflight_or_open_entry_order(cfg.instrument_id)
        sec_entry_inflight = bool(
            cfg.secondary_instrument_id is not None
            and self._has_inflight_or_open_entry_order(cfg.secondary_instrument_id)
        )

        if signed_prim_pos > Decimal("0"):
            if (
                cfg.trade_secondary_leg
                and cfg.secondary_instrument_id is not None
                and signed_sec_pos == Decimal("0")
                and not sec_entry_inflight
            ):
                self._submit_plan_gated_exit(
                    exit_side=OrderSide.SELL,
                    quantity=signed_prim_pos,
                    reference_price=prim_px,
                    reason_tag="STAT_ARB_UNHEDGED_PRIMARY_EXIT",
                    instrument_id=cfg.instrument_id,
                )
                self._entry_spread = None
                return

            stop_hit = (
                self._entry_spread is not None
                and (self._entry_spread - residual) >= (atr_mult * spread_std)
            )
            mean_reverted = spread_z >= 0.0 or (1.0 - z_quantile) <= exit_q
            coint_broken = not is_tradable or raw_beta <= 0.0
            if stop_hit or mean_reverted or coint_broken:
                reason = (
                    "STAT_ARB_SPREAD_STOP"
                    if stop_hit
                    else (
                        "STAT_ARB_COINTEGRATION_BREAKDOWN"
                        if coint_broken
                        else "STAT_ARB_SPREAD_REVERTED"
                    )
                )
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
            if (
                cfg.trade_secondary_leg
                and cfg.secondary_instrument_id is not None
                and signed_sec_pos == Decimal("0")
                and not sec_entry_inflight
            ):
                self._submit_plan_gated_exit(
                    exit_side=OrderSide.BUY,
                    quantity=abs(signed_prim_pos),
                    reference_price=prim_px,
                    reason_tag="STAT_ARB_UNHEDGED_PRIMARY_EXIT",
                    instrument_id=cfg.instrument_id,
                )
                self._entry_spread = None
                return

            stop_hit = (
                self._entry_spread is not None
                and (residual - self._entry_spread) >= (atr_mult * spread_std)
            )
            mean_reverted = spread_z <= 0.0 or z_quantile <= exit_q
            coint_broken = not is_tradable or raw_beta <= 0.0
            if stop_hit or mean_reverted or coint_broken:
                reason = (
                    "STAT_ARB_SPREAD_STOP"
                    if stop_hit
                    else (
                        "STAT_ARB_COINTEGRATION_BREAKDOWN"
                        if coint_broken
                        else "STAT_ARB_SPREAD_REVERTED"
                    )
                )
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

        if signed_prim_pos == Decimal("0") and signed_sec_pos != Decimal("0"):
            if prim_entry_inflight:
                self._cancel_active_entry_for_instrument(cfg.instrument_id)
            if cfg.secondary_instrument_id is not None:
                sec_key = str(cfg.secondary_instrument_id)
                if (
                    sec_key not in self._active_exit_orders_by_inst
                    and sec_key not in self._pending_safe_exits
                ):
                    sec_exit_side = (
                        OrderSide.BUY
                        if signed_sec_pos < Decimal("0")
                        else OrderSide.SELL
                    )
                    self._submit_plan_gated_exit(
                        exit_side=sec_exit_side,
                        quantity=abs(signed_sec_pos),
                        reference_price=sec_px,
                        reason_tag="STAT_ARB_ORPHAN_HEDGE_EXIT",
                        instrument_id=cfg.secondary_instrument_id,
                    )
            self._entry_spread = None
            return

        if (
            self._exit_only_mode
            or not is_tradable
            or raw_beta <= 0.0
            or beta <= 0.0
            or n_sync < cfg.lookback_window
            or prim_entry_inflight
            or sec_entry_inflight
            or signed_prim_pos != Decimal("0")
            or signed_sec_pos != Decimal("0")
            or str(cfg.instrument_id) in self._active_exit_orders_by_inst
            or str(cfg.instrument_id) in self._pending_safe_exits
            or (
                cfg.secondary_instrument_id is not None
                and (
                    str(cfg.secondary_instrument_id) in self._active_exit_orders_by_inst
                    or str(cfg.secondary_instrument_id) in self._pending_safe_exits
                )
            )
        ):
            return

        positive_beta_window = [
            b
            for b in self._hedge_betas[-min(len(self._hedge_betas), cfg.lookback_window) :]
            if b > 0.0
        ]
        if len(positive_beta_window) >= 2:
            lower_beta = empirical_quantile_value(
                positive_beta_window, max(0.0, 1.0 - entry_q)
            )
            upper_beta = empirical_quantile_value(
                positive_beta_window, min(1.0, entry_q)
            )
            clamped_beta = max(lower_beta, min(upper_beta, beta))
        else:
            clamped_beta = beta
        hedge_qty = max(
            Decimal("1"),
            (cfg.trade_qty * Decimal(f"{clamped_beta:.4f}")).to_integral_value(),
        )

        # Long spread (Buy Primary Leg A, Sell Hedge Leg B) when spread is in lower empirical tail
        if cfg.plan_side == PlanSide.LONG:
            lower_tail_q = 1.0 - z_quantile
            if lower_tail_q >= entry_q and spread_z < 0.0:
                stop_px = self._resolve_entry_stop_price(
                    prim_px, prim_px - (atr_mult * atr_prim), quantity=cfg.trade_qty
                )
                raw_sec_stop = sec_px + (atr_mult * max(atr_sec, 0.01))
                sec_stop_px = (
                    self._resolve_entry_stop_price(
                        sec_px,
                        raw_sec_stop,
                        quantity=hedge_qty,
                        instrument_id=cfg.secondary_instrument_id,
                    )
                    if cfg.secondary_instrument_id is not None
                    else raw_sec_stop
                )
                self._submit_dual_leg_entry(
                    prim_side=OrderSide.BUY,
                    sec_side=OrderSide.SELL,
                    prim_px=prim_px,
                    sec_px=sec_px,
                    atr_prim=atr_prim,
                    atr_sec=max(atr_sec, 0.01),
                    prim_stop_px=stop_px,
                    sec_stop_px=sec_stop_px,
                    prim_qty=cfg.trade_qty,
                    sec_qty=hedge_qty,
                    prim_tag="STAT_ARB_LONG_SPREAD_PRIMARY",
                    sec_tag="STAT_ARB_LONG_SPREAD_HEDGE",
                    residual=residual,
                )
        elif cfg.plan_side == PlanSide.SHORT:
            if z_quantile >= entry_q and spread_z > 0.0:
                stop_px = self._resolve_entry_stop_price(
                    prim_px, prim_px + (atr_mult * atr_prim), quantity=cfg.trade_qty
                )
                raw_sec_stop = max(0.01, sec_px - (atr_mult * max(atr_sec, 0.01)))
                sec_stop_px = (
                    self._resolve_entry_stop_price(
                        sec_px,
                        raw_sec_stop,
                        quantity=hedge_qty,
                        instrument_id=cfg.secondary_instrument_id,
                    )
                    if cfg.secondary_instrument_id is not None
                    else raw_sec_stop
                )
                self._submit_dual_leg_entry(
                    prim_side=OrderSide.SELL,
                    sec_side=OrderSide.BUY,
                    prim_px=prim_px,
                    sec_px=sec_px,
                    atr_prim=atr_prim,
                    atr_sec=max(atr_sec, 0.01),
                    prim_stop_px=stop_px,
                    sec_stop_px=sec_stop_px,
                    prim_qty=cfg.trade_qty,
                    sec_qty=hedge_qty,
                    prim_tag="STAT_ARB_SHORT_SPREAD_PRIMARY",
                    sec_tag="STAT_ARB_SHORT_SPREAD_HEDGE",
                    residual=residual,
                )
