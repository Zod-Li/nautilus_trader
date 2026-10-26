"""Pure rolling empirical distribution and non-parametric statistical estimators for Nautilus strategies.

All signal thresholds in pre-audited strategies derive from rolling empirical quantiles,
non-parametric Median Absolute Deviation (MAD), volatility-normalized slopes, and rolling OLS
cointegration residuals (strictly enforcing Red Line R9: zero static retail magic numbers).
"""

from __future__ import annotations

from decimal import Decimal
import math
from typing import Sequence, Tuple


def empirical_rank_quantile(window: Sequence[float], value: float) -> float:
    """Computes the empirical CDF rank quantile of `value` within `window` in [0.0, 1.0]."""
    n = len(window)
    if n == 0:
        return 0.5
    less_count = 0
    equal_count = 0
    for item in window:
        if item < value:
            less_count += 1
        elif item == value:
            equal_count += 1
    # Mid-rank empirical CDF estimator: (less + 0.5 * equal) / n
    return (less_count + (equal_count / 2.0)) / float(n)


def empirical_quantile_value(window: Sequence[float], quantile: float) -> float:
    """Computes the value at empirical `quantile` in [0.0, 1.0] from `window`."""
    if not window:
        return 0.0
    sorted_vals = sorted(window)
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    clamped_q = min(1.0, max(0.0, float(quantile)))
    pos = clamped_q * (n - 1)
    lower_idx = int(math.floor(pos))
    upper_idx = int(math.ceil(pos))
    if lower_idx == upper_idx:
        return sorted_vals[lower_idx]
    weight = pos - lower_idx
    return sorted_vals[lower_idx] * (1.0 - weight) + sorted_vals[upper_idx] * weight


def rolling_mean(window: Sequence[float]) -> float:
    """Arithmetic mean of `window`."""
    if not window:
        return 0.0
    return sum(window) / float(len(window))


def rolling_std(window: Sequence[float]) -> float:
    """Sample standard deviation of `window`."""
    n = len(window)
    if n < 2:
        return 0.0
    mean_val = rolling_mean(window)
    variance = sum((x - mean_val) ** 2 for x in window) / float(n - 1)
    return math.sqrt(max(0.0, variance))


def rolling_mad_zscore(window: Sequence[float], value: float) -> float:
    """Non-parametric robust Z-score using Median and Median Absolute Deviation (MAD)."""
    if len(window) < 2:
        return 0.0
    median_val = empirical_quantile_value(window, 0.5)
    abs_devs = [abs(x - median_val) for x in window]
    mad = empirical_quantile_value(abs_devs, 0.5)
    if mad <= 0.0:
        std_fallback = rolling_std(window)
        if std_fallback <= 0.0:
            return 0.0
        return (value - median_val) / std_fallback
    return (value - median_val) / mad


def compute_true_range(high: float, low: float, prev_close: float) -> float:
    """Computes Wilder True Range for a bar given `prev_close`."""
    return max(
        high - low,
        abs(high - prev_close),
        abs(low - prev_close),
    )


def rolling_atr(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    window: int,
) -> float:
    """Computes rolling Average True Range over the trailing `window` bars."""
    n = min(len(highs), len(lows), len(closes))
    if n == 0:
        return 0.0
    if n == 1:
        return max(0.0, highs[-1] - lows[-1])
    eff_window = max(1, min(int(window), n - 1))
    trs = []
    start_idx = n - eff_window
    for idx in range(start_idx, n):
        trs.append(compute_true_range(highs[idx], lows[idx], closes[idx - 1]))
    return rolling_mean(trs)


def rolling_linear_slope(window: Sequence[float]) -> float:
    """Ordinary least squares slope of `window` wrt time index `0..len(window)-1`."""
    n = len(window)
    if n < 2:
        return 0.0
    mean_x = (n - 1) / 2.0
    mean_y = rolling_mean(window)
    num = 0.0
    den = 0.0
    for idx, y_val in enumerate(window):
        dx = idx - mean_x
        num += dx * (y_val - mean_y)
        den += dx * dx
    if den <= 0.0:
        return 0.0
    return num / den


def rolling_ols_hedge_params(
    primary_series: Sequence[float],
    secondary_series: Sequence[float],
) -> Tuple[float, float, float]:
    """Computes rolling OLS intercept `alpha`, hedge ratio `beta`, and current residual
    `spread = primary[-1] - (alpha + beta * secondary[-1])` for two synchronized price series.
    """
    n = min(len(primary_series), len(secondary_series))
    if n < 2:
        return 0.0, 1.0, 0.0
    xs = secondary_series[-n:]
    ys = primary_series[-n:]
    mean_x = rolling_mean(xs)
    mean_y = rolling_mean(ys)
    cov_xy = 0.0
    var_x = 0.0
    for x_val, y_val in zip(xs, ys):
        dx = x_val - mean_x
        dy = y_val - mean_y
        cov_xy += dx * dy
        var_x += dx * dx
    beta = cov_xy / var_x if var_x > 0.0 else 1.0
    alpha = mean_y - beta * mean_x
    residual = ys[-1] - (alpha + beta * xs[-1])
    return alpha, beta, residual


def quantile_to_float(val: Decimal | float) -> float:
    return float(val)


def ewma_mean(series: Sequence[float], span: int) -> float:
    """Computes Exponentially Weighted Moving Average over `series` with decay `span`."""
    if not series:
        return 0.0
    eff_span = max(1, int(span))
    alpha = 2.0 / (float(eff_span) + 1.0)
    ema = float(series[0])
    for val in series[1:]:
        ema = alpha * float(val) + (1.0 - alpha) * ema
    return ema


def ewma_slope(series: Sequence[float], fast_span: int, slow_span: int) -> float:
    """Computes volatility-normalized EWMA differential `(ewma_fast - ewma_slow) / std`."""
    if len(series) < 2:
        return 0.0
    fast_val = ewma_mean(series, fast_span)
    slow_val = ewma_mean(series, slow_span)
    std_val = rolling_std(series[-max(2, int(slow_span)) :])
    if std_val <= 0.0:
        return fast_val - slow_val
    return (fast_val - slow_val) / std_val


def volatility_standardized_momentum(series: Sequence[float], window: int) -> float:
    """Computes return over `window` standardized by rolling return volatility."""
    n = len(series)
    eff_win = max(1, min(int(window), n - 1))
    if n < 2 or eff_win < 1:
        return 0.0
    start_px = float(series[-eff_win - 1])
    end_px = float(series[-1])
    if start_px <= 0.0:
        return 0.0
    cum_ret = (end_px - start_px) / start_px
    sub = series[-(eff_win + 1) :]
    rets = [
        (float(sub[i]) - float(sub[i - 1])) / float(sub[i - 1])
        for i in range(1, len(sub))
        if float(sub[i - 1]) > 0.0
    ]
    vol = rolling_std(rets)
    if vol <= 0.0:
        return cum_ret
    return cum_ret / (vol * math.sqrt(float(len(rets))))


def rolling_vwap(
    prices: Sequence[float],
    volumes: Sequence[float],
    window: int,
) -> float:
    """Computes rolling Volume-Weighted Average Price over trailing `window` bars."""
    n = min(len(prices), len(volumes))
    if n == 0:
        return 0.0
    eff_win = max(1, min(int(window), n))
    px_slice = prices[-eff_win:]
    vol_slice = volumes[-eff_win:]
    tot_vol = sum(max(0.0, float(v)) for v in vol_slice)
    if tot_vol <= 0.0:
        return rolling_mean(px_slice)
    notional = sum(float(p) * max(0.0, float(v)) for p, v in zip(px_slice, vol_slice))
    return notional / tot_vol

