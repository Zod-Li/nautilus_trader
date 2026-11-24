"""Institutional IBKR Pro Fixed US Equity fee model and microstructure market-impact core.

Pure, `BacktestEngine`-independent domain models and vectorized calculators shared across:
- M3 `IbkrProFixedUsEquityFeeModel(FeeModel)`, `CollaredTwapExecAlgorithm`, and `IsomorphicBacktestRunner`
- M4 `RegimeCalibrator`, `shadow_ledger.py`, and Triple-Barrier path R-multiple labeling.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_HALF_EVEN, Decimal
import math
from typing import Any, Optional

import numpy as np

from nautilus_trader.execution import FeeModel
from nautilus_trader.model import Currency, Money, OrderSide, Price, Quantity

CENT_QUANT = Decimal("0.01")


def _to_dec(val: Any, name: str) -> Decimal:
    if isinstance(val, bool) or val is None:
        raise ValueError(f"{name} must be a valid decimal, got {val!r}")
    dec = val if isinstance(val, Decimal) else Decimal(str(val))
    if not dec.is_finite():
        raise ValueError(f"{name} must be finite, got {val!r}")
    return dec


def _ceil_cent(val: Decimal) -> Decimal:
    if val <= Decimal("0"):
        return Decimal("0.00")
    return val.quantize(CENT_QUANT, rounding=ROUND_CEILING)


def _is_sell_side(side: Any) -> bool:
    if side == OrderSide.SELL:
        return True
    text = str(getattr(side, "value", side) or "").strip().upper()
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    return text in ("SELL", "SHORT")


@dataclass(frozen=True)
class IbkrProFeeSchedule:
    """Standard IBKR Pro Fixed US Equities schedule + statutory sell-side pass-through rates."""

    per_share_usd: Decimal = Decimal("0.005")
    min_per_order_usd: Decimal = Decimal("1.00")
    max_notional_pct: Decimal = Decimal("0.01")
    sec_section_31_rate: Decimal = Decimal("0.0000278")
    finra_taf_per_share_usd: Decimal = Decimal("0.000166")
    finra_taf_max_per_order_usd: Decimal = Decimal("8.30")

    def __post_init__(self) -> None:
        object.__setattr__(self, "per_share_usd", _to_dec(self.per_share_usd, "per_share_usd"))
        object.__setattr__(
            self, "min_per_order_usd", _to_dec(self.min_per_order_usd, "min_per_order_usd")
        )
        object.__setattr__(
            self, "max_notional_pct", _to_dec(self.max_notional_pct, "max_notional_pct")
        )
        object.__setattr__(
            self, "sec_section_31_rate", _to_dec(self.sec_section_31_rate, "sec_section_31_rate")
        )
        object.__setattr__(
            self,
            "finra_taf_per_share_usd",
            _to_dec(self.finra_taf_per_share_usd, "finra_taf_per_share_usd"),
        )
        object.__setattr__(
            self,
            "finra_taf_max_per_order_usd",
            _to_dec(self.finra_taf_max_per_order_usd, "finra_taf_max_per_order_usd"),
        )
        if self.per_share_usd <= Decimal("0"):
            raise ValueError(f"per_share_usd must be > 0, got {self.per_share_usd}")
        if self.min_per_order_usd < Decimal("0"):
            raise ValueError(f"min_per_order_usd must be >= 0, got {self.min_per_order_usd}")
        if self.max_notional_pct <= Decimal("0"):
            raise ValueError(f"max_notional_pct must be > 0, got {self.max_notional_pct}")
        if self.sec_section_31_rate < Decimal("0"):
            raise ValueError(
                f"sec_section_31_rate must be >= 0, got {self.sec_section_31_rate}"
            )
        if self.finra_taf_per_share_usd < Decimal("0"):
            raise ValueError(
                f"finra_taf_per_share_usd must be >= 0, got {self.finra_taf_per_share_usd}"
            )
        if self.finra_taf_max_per_order_usd < Decimal("0"):
            raise ValueError(
                f"finra_taf_max_per_order_usd must be >= 0, got {self.finra_taf_max_per_order_usd}"
            )

    @property
    def breakeven_slice_shares(self) -> int:
        """Minimum slice size where per-share commission equals the per-order minimum (`ceil(1.00 / 0.005) == 200`)."""
        return max(
            1,
            int((self.min_per_order_usd / self.per_share_usd).to_integral_value(rounding=ROUND_CEILING)),
        )

    @property
    def min_multi_slice_shares(self) -> int:
        """Minimum parent order size eligible for multi-slice TWAP (`2 * breakeven_slice_shares == 400`)."""
        return 2 * self.breakeven_slice_shares


def compute_ibkr_pro_fixed_commission(
    side: OrderSide | str,
    quantity: Decimal | int | float | str,
    fill_px: Decimal | int | float | str,
    schedule: Optional[IbkrProFeeSchedule] = None,
    *,
    prior_filled_qty: Decimal | int | float | str = Decimal("0"),
    prior_avg_px: Optional[Decimal | int | float | str] = None,
) -> Decimal:
    """Computes incremental IBKR Pro Fixed US Equity commission + sell-side regulatory fees.

    SPEC-0007 §2.1:
      Broker Commission(Q, P) = min(0.01 * Q * P, max(1.00, 0.005 * Q))
      Sell-Side SEC Section 31 = ceil_0.01(Q * P * sec_section_31_rate)
      Sell-Side FINRA TAF      = min(8.30, ceil_0.01(0.000166 * Q))

    When `prior_filled_qty > 0` (partial fills on the same order), computes the incremental
    delta `(cum_after - cum_before)` so partial fills never double-charge the `$1.00` order
    minimum or exceed the `$8.30` FINRA TAF per-order cap.
    """
    sched = schedule or IbkrProFeeSchedule()
    qty = _to_dec(quantity, "quantity")
    px = _to_dec(fill_px, "fill_px")
    prior_q = max(Decimal("0"), _to_dec(prior_filled_qty, "prior_filled_qty"))

    if qty <= Decimal("0") or px <= Decimal("0"):
        return Decimal("0.00")

    prior_px = (
        _to_dec(prior_avg_px, "prior_avg_px")
        if prior_avg_px is not None and _to_dec(prior_avg_px, "prior_avg_px") > Decimal("0")
        else px
    )

    def _cumulative_broker_comm(cum_q: Decimal, cum_notional: Decimal) -> Decimal:
        if cum_q <= Decimal("0") or cum_notional <= Decimal("0"):
            return Decimal("0.00")
        raw_comm = max(sched.min_per_order_usd, sched.per_share_usd * cum_q)
        cap_comm = sched.max_notional_pct * cum_notional
        return min(cap_comm, raw_comm)

    fill_notional = qty * px
    prior_notional = prior_q * prior_px
    cum_q_after = prior_q + qty
    cum_notional_after = prior_notional + fill_notional

    broker_before = _cumulative_broker_comm(prior_q, prior_notional)
    broker_after = _cumulative_broker_comm(cum_q_after, cum_notional_after)
    broker_delta = max(Decimal("0"), broker_after - broker_before)

    reg_delta = Decimal("0.00")
    if _is_sell_side(side):
        sec_before = (
            _ceil_cent(prior_notional * sched.sec_section_31_rate)
            if prior_q > Decimal("0")
            else Decimal("0.00")
        )
        sec_after = _ceil_cent(cum_notional_after * sched.sec_section_31_rate)
        sec_delta = max(Decimal("0.00"), sec_after - sec_before)
        taf_before = (
            min(sched.finra_taf_max_per_order_usd, _ceil_cent(sched.finra_taf_per_share_usd * prior_q))
            if prior_q > Decimal("0")
            else Decimal("0.00")
        )
        taf_after = min(
            sched.finra_taf_max_per_order_usd,
            _ceil_cent(sched.finra_taf_per_share_usd * cum_q_after),
        )
        taf_delta = max(Decimal("0.00"), taf_after - taf_before)
        reg_delta = sec_delta + taf_delta

    return (broker_delta + reg_delta).quantize(CENT_QUANT, rounding=ROUND_HALF_EVEN)


@dataclass(frozen=True)
class MarketImpactSpec:
    """Calibrated half-spread and square-root market-impact parameters (zero R9 magic defaults)."""

    half_spread_bps: float = 0.0
    impact_coefficient_y: float = 0.0
    min_adv_shares: float = 1.0

    def __post_init__(self) -> None:
        if not math.isfinite(float(self.half_spread_bps)) or float(self.half_spread_bps) < 0.0:
            raise ValueError(f"half_spread_bps must be finite >= 0, got {self.half_spread_bps}")
        if (
            not math.isfinite(float(self.impact_coefficient_y))
            or float(self.impact_coefficient_y) < 0.0
        ):
            raise ValueError(
                f"impact_coefficient_y must be finite >= 0, got {self.impact_coefficient_y}"
            )
        if not math.isfinite(float(self.min_adv_shares)) or float(self.min_adv_shares) <= 0.0:
            raise ValueError(f"min_adv_shares must be finite > 0, got {self.min_adv_shares}")


@dataclass(frozen=True)
class MarketImpactBreakdown:
    """Decomposed half-spread + square-root market impact slippage for a trade/slice."""

    half_spread_per_share: float
    impact_per_share: float
    total_slippage_per_share: float
    total_slippage_bps: float
    total_slippage_usd: float


def compute_half_spread_and_sqrt_impact(
    price: float,
    quantity: float,
    daily_sigma: float,
    adv_20_shares: float,
    spec: Optional[MarketImpactSpec] = None,
    *,
    half_spread_bps: Optional[float] = None,
    impact_coefficient_y: Optional[float] = None,
) -> MarketImpactBreakdown:
    """Computes `half_spread * P + Y * sigma_1d * P * sqrt(Q / ADV_20)`."""
    eff_spec = spec or MarketImpactSpec()
    eff_half_bps = float(
        half_spread_bps if half_spread_bps is not None else eff_spec.half_spread_bps
    )
    eff_y = float(
        impact_coefficient_y
        if impact_coefficient_y is not None
        else eff_spec.impact_coefficient_y
    )
    if not math.isfinite(eff_half_bps) or eff_half_bps < 0.0:
        raise ValueError(f"half_spread_bps must be finite >= 0, got {eff_half_bps}")
    if not math.isfinite(eff_y) or eff_y < 0.0:
        raise ValueError(f"impact_coefficient_y must be finite >= 0, got {eff_y}")
    raw_px = float(price)
    raw_qty = float(quantity)
    raw_sigma = float(daily_sigma)
    raw_adv = float(adv_20_shares)
    px = max(0.0, raw_px) if math.isfinite(raw_px) else 0.0
    qty = max(0.0, raw_qty) if math.isfinite(raw_qty) else 0.0
    sigma = max(0.0, raw_sigma) if math.isfinite(raw_sigma) else 0.0
    adv = max(float(eff_spec.min_adv_shares), raw_adv) if math.isfinite(raw_adv) else float(eff_spec.min_adv_shares)

    if px <= 0.0 or qty <= 0.0:
        return MarketImpactBreakdown(
            half_spread_per_share=0.0,
            impact_per_share=0.0,
            total_slippage_per_share=0.0,
            total_slippage_bps=0.0,
            total_slippage_usd=0.0,
        )

    half_spread_frac = max(0.0, eff_half_bps) * 1e-4
    impact_frac = max(0.0, eff_y) * sigma * math.sqrt(qty / adv)
    half_per_sh = half_spread_frac * px
    imp_per_sh = impact_frac * px
    total_per_sh = half_per_sh + imp_per_sh
    total_bps = (half_spread_frac + impact_frac) * 10_000.0
    return MarketImpactBreakdown(
        half_spread_per_share=half_per_sh,
        impact_per_share=imp_per_sh,
        total_slippage_per_share=total_per_sh,
        total_slippage_bps=total_bps,
        total_slippage_usd=total_per_sh * qty,
    )


def compute_vectorized_friction_per_share(
    prices: Any,
    quantities: Any,
    *,
    sides: Optional[Any] = None,
    daily_sigmas: Any = 0.0,
    adv_20_shares: Any = 1_000_000.0,
    half_spread_bps: float = 0.0,
    impact_coefficient_y: float = 0.0,
    fee_schedule: Optional[IbkrProFeeSchedule] = None,
    impact_spec: Optional[MarketImpactSpec] = None,
    round_trip: bool = False,
    fee_multiplier: float = 1.0,
    slippage_multiplier: float = 1.0,
) -> np.ndarray:
    """Vectorized per-share dollar friction (IBKR Pro commission + sell-side regulatory + spread/impact)."""
    sched = fee_schedule or IbkrProFeeSchedule()
    eff_spec = impact_spec or MarketImpactSpec(
        half_spread_bps=half_spread_bps,
        impact_coefficient_y=impact_coefficient_y,
    )
    eff_half_bps = (
        float(half_spread_bps) if impact_spec is None else float(eff_spec.half_spread_bps)
    )
    eff_y = (
        float(impact_coefficient_y)
        if impact_spec is None
        else float(eff_spec.impact_coefficient_y)
    )
    eff_fee_mult = float(fee_multiplier)
    eff_slip_mult = float(slippage_multiplier)
    if not math.isfinite(eff_half_bps) or eff_half_bps < 0.0:
        raise ValueError(f"half_spread_bps must be finite >= 0, got {eff_half_bps}")
    if not math.isfinite(eff_y) or eff_y < 0.0:
        raise ValueError(f"impact_coefficient_y must be finite >= 0, got {eff_y}")
    if not math.isfinite(eff_fee_mult) or eff_fee_mult < 0.0:
        raise ValueError(f"fee_multiplier must be finite >= 0, got {eff_fee_mult}")
    if not math.isfinite(eff_slip_mult) or eff_slip_mult < 0.0:
        raise ValueError(f"slippage_multiplier must be finite >= 0, got {eff_slip_mult}")

    raw_px = np.asarray(prices, dtype=np.float64)
    raw_qty = np.asarray(quantities, dtype=np.float64)
    raw_sig = np.asarray(daily_sigmas, dtype=np.float64)
    raw_adv = np.asarray(adv_20_shares, dtype=np.float64)

    raw_px, raw_qty, raw_sig, raw_adv = np.broadcast_arrays(raw_px, raw_qty, raw_sig, raw_adv)
    active = np.isfinite(raw_px) & np.isfinite(raw_qty) & (raw_px > 0.0) & (raw_qty > 0.0)
    px_arr = np.where(active, raw_px, 0.0)
    qty_arr = np.where(active, raw_qty, 0.0)
    sig_arr = np.where(np.isfinite(raw_sig), np.maximum(raw_sig, 0.0), 0.0)
    adv_arr = np.where(
        np.isfinite(raw_adv),
        np.maximum(raw_adv, float(eff_spec.min_adv_shares)),
        float(eff_spec.min_adv_shares),
    )
    safe_qty = np.where(active, qty_arr, 1.0)
    notional = np.where(active, px_arr * qty_arr, 0.0)

    per_sh_cents = float(sched.per_share_usd * Decimal("100"))
    min_ord_cents = float(sched.min_per_order_usd * Decimal("100"))
    max_pct_cents = float(sched.max_notional_pct * Decimal("100"))
    sec_rate = float(sched.sec_section_31_rate)
    taf_per_sh = float(sched.finra_taf_per_share_usd)
    taf_max_cents = float(sched.finra_taf_max_per_order_usd * Decimal("100"))

    def _round_half_even_cents(cents_arr: np.ndarray) -> np.ndarray:
        twice = cents_arr * 2.0
        nearest_half = np.rint(twice)
        snapped = np.where(
            np.abs(twice - nearest_half) < 1e-10,
            nearest_half * 0.5,
            cents_arr,
        )
        return np.rint(snapped) / 100.0

    broker_cents = np.where(
        active,
        np.minimum(max_pct_cents * notional, np.maximum(min_ord_cents, per_sh_cents * qty_arr)),
        0.0,
    )

    raw_sec_cents = notional * sec_rate * 100.0
    sec_cents = np.where(raw_sec_cents > 0.0, np.ceil(raw_sec_cents - 1e-12), 0.0)

    raw_taf_cents = np.where(active, qty_arr * taf_per_sh * 100.0, 0.0)
    taf_cents = np.where(
        raw_taf_cents > 0.0,
        np.minimum(taf_max_cents, np.ceil(raw_taf_cents - 1e-12)),
        0.0,
    )

    if round_trip:
        comm_total = _round_half_even_cents(broker_cents) + _round_half_even_cents(
            broker_cents + sec_cents + taf_cents
        )
    else:
        if sides is None:
            sell_mask = np.zeros(px_arr.shape, dtype=bool)
        else:
            side_arr = np.asarray(sides)
            if np.issubdtype(side_arr.dtype, np.number):
                sell_mask = np.broadcast_to(side_arr < 0, px_arr.shape)
            else:
                flat_sell = np.array([_is_sell_side(s) for s in side_arr.flat], dtype=bool)
                sell_mask = np.broadcast_to(flat_sell.reshape(side_arr.shape), px_arr.shape)
        comm_total = _round_half_even_cents(
            broker_cents + np.where(sell_mask, sec_cents + taf_cents, 0.0)
        )

    comm_per_share = np.where(
        active,
        (comm_total * eff_fee_mult) / safe_qty,
        0.0,
    )

    half_spread_frac = eff_half_bps * 1e-4
    impact_frac = eff_y * sig_arr * np.sqrt(np.where(active, qty_arr / adv_arr, 0.0))
    one_way_slip_per_sh = (half_spread_frac + impact_frac) * px_arr * eff_slip_mult
    slip_per_share = np.where(active, one_way_slip_per_sh * (2.0 if round_trip else 1.0), 0.0)

    return comm_per_share + slip_per_share


class IbkrProFixedUsEquityFeeModel(FeeModel):
    """Nautilus `FeeModel` implementing IBKR Pro Fixed US Equities + statutory sell-side fees."""

    def __init__(
        self,
        schedule: Optional[IbkrProFeeSchedule] = None,
        *,
        sec_section_31_rate: Optional[Decimal | float | str] = None,
        fee_multiplier: float = 1.0,
    ) -> None:
        base = schedule or IbkrProFeeSchedule()
        if sec_section_31_rate is not None:
            base = IbkrProFeeSchedule(
                per_share_usd=base.per_share_usd,
                min_per_order_usd=base.min_per_order_usd,
                max_notional_pct=base.max_notional_pct,
                sec_section_31_rate=_to_dec(sec_section_31_rate, "sec_section_31_rate"),
                finra_taf_per_share_usd=base.finra_taf_per_share_usd,
                finra_taf_max_per_order_usd=base.finra_taf_max_per_order_usd,
            )
        self._schedule = base
        self._fee_multiplier = max(0.0, float(fee_multiplier))

    @property
    def schedule(self) -> IbkrProFeeSchedule:
        return self._schedule

    @property
    def fee_multiplier(self) -> float:
        return self._fee_multiplier

    def get_commission(
        self,
        order: Any,
        fill_quantity: Quantity,
        fill_px: Price,
        instrument: Any,
    ) -> Money:
        qty_dec = (
            fill_quantity.as_decimal()
            if hasattr(fill_quantity, "as_decimal")
            else Decimal(str(fill_quantity))
        )
        px_dec = (
            fill_px.as_decimal()
            if hasattr(fill_px, "as_decimal")
            else Decimal(str(fill_px))
        )
        prior_filled_attr = getattr(order, "filled_qty", None)
        prior_filled = (
            prior_filled_attr.as_decimal()
            if prior_filled_attr is not None and hasattr(prior_filled_attr, "as_decimal")
            else (Decimal(str(prior_filled_attr)) if prior_filled_attr is not None else Decimal("0"))
        )
        avg_px_attr = getattr(order, "avg_px", None)
        prior_avg_px = Decimal(str(avg_px_attr)) if avg_px_attr is not None else None

        commission_dec = compute_ibkr_pro_fixed_commission(
            side=getattr(order, "side", OrderSide.BUY),
            quantity=qty_dec,
            fill_px=px_dec,
            schedule=self._schedule,
            prior_filled_qty=prior_filled,
            prior_avg_px=prior_avg_px,
        )
        if self._fee_multiplier != 1.0:
            commission_dec = (
                commission_dec * Decimal(str(self._fee_multiplier))
            ).quantize(CENT_QUANT, rounding=ROUND_HALF_EVEN)

        quote_ccy = getattr(instrument, "quote_currency", None) or Currency.from_str("USD")
        return Money.from_decimal(commission_dec, quote_ccy)
