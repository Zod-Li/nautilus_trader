"""Controlled `NautilusMcpServer` implementation exposing the 5 isomorphic MCP tools.

Implements Ticket #15 (`15-nautilus-mcp-server-and-tool-schema-seam.md`):
- Exposes strictly the 5 pre-audited tools (`list_strategy_archetypes`,
  `query_datainfra_bars_summary`, `run_archetype_backtest`, `run_stress_falsification`,
  `compile_and_lock_trading_plan`).
- Physically blocks dynamic code execution (`eval`/`exec`) and unauthorized tool calls.
- Enforces the Fail-Closed Stress Falsification Gate: `compile_and_lock_trading_plan`
  rejects any candidate that has not passed `run_stress_falsification` (`passed=True`).
- Calls the deterministic capital sizing engine (`evolution_engine.capital.sizing.size_position`)
  to bound `target_qty` / `approved_qty_cap` and `max_risk_budget_usd` before atomically locking
  `PlanItemContract` items in PostgreSQL `trading_plans` via `PlanContractRepository`.
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import hmac
import inspect
import json
import logging
import math
import os
import queue
import statistics
import threading
import uuid
from dataclasses import replace
from datetime import date, datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple
from uuid import UUID
from zoneinfo import ZoneInfo

from evolution_engine.capital.sizing import (
    PortfolioState,
    SizingDecision,
    SizingRequest,
    size_position,
)
from evolution_engine.config_center.envelope import load_envelope
from evolution_engine.config_center.schema import SystemConfig
from evolution_engine.db.dsn import resolve_database_dsn
from market_data_service.pipeline.us_equity_universe import get_gics_sector_map
from nautilus_trader.quant.contracts.models import (
    ArchetypeStrategyConfig,
    ExecutionSpec,
    MarketableLimitCollar,
    PlanItemContract,
    PlanRiskEnvelope,
    PlanSide,
    StrategyArchetypeId,
    TriggerPolicy,
    compute_intraday_entry_valid_until,
    default_limit_collar_atr_for_archetype,
    quantize_price_rule_612,
    rule_612_tick_size,
)
from nautilus_trader.quant.contracts.repository import PlanContractRepository
from nautilus_trader.quant.mcp.schemas import (
    ALLOWED_MCP_TOOL_NAMES,
    McpPlanLockRejectedError,
    McpSecurityViolationError,
    assert_no_code_injection,
    get_mcp_tool_input_schemas,
)
from nautilus_trader.quant.nodes.backtest_runner import (
    BARS_PER_TRADING_YEAR,
    IsomorphicBacktestMetrics,
    IsomorphicBacktestRunner,
    StressFalsificationReport,
    StressFalsifier,
    _bar_step_ns,
    _sort_bars_deterministic,
    build_datainfra_config_from_env,
)
from nautilus_trader.quant.nodes.live_node import SipFeederSubscriptionClient
from nautilus_trader.quant.strategies.empirical import rolling_atr
from nautilus_trader.quant.strategies.registry import (
    get_archetype_descriptor,
    list_strategy_archetype_schemas,
)
from nautilus_trader.adapters.datainfra import DataInfraCatalog, DataInfraConfig
from nautilus_trader.model import Bar, Equity

logger = logging.getLogger(__name__)

REQUIRED_ATTESTATION_KEYS: Tuple[str, ...] = (
    "macro_catalyst_summary",
    "quant_research_summary",
    "adversarial_risk_summary",
    "portfolio_chair_decision",
)


def _canonicalize_risk_envelope(
    risk_envelope: PlanRiskEnvelope | Mapping[str, Any],
) -> Dict[str, Any]:
    if isinstance(risk_envelope, PlanRiskEnvelope):
        raw = risk_envelope.to_dict()
        env_pct = getattr(risk_envelope, "limit_collar_pct", None)
        env_atr = getattr(risk_envelope, "limit_collar_atr", None)
        if env_pct is not None:
            raw["limit_collar_pct"] = env_pct
        if env_atr is not None:
            raw["limit_collar_atr"] = env_atr
    elif isinstance(risk_envelope, Mapping):
        raw = dict(risk_envelope)
    elif hasattr(risk_envelope, "to_dict") and callable(risk_envelope.to_dict):
        raw = dict(risk_envelope.to_dict())
    else:
        raw = dict(risk_envelope)
    collar = raw.get("collar")
    collar_map: Mapping[str, Any] = (
        collar.to_dict()
        if isinstance(collar, MarketableLimitCollar)
        else (collar if isinstance(collar, Mapping) else {})
    )
    pct_val = raw.get(
        "limit_collar_pct",
        collar_map.get(
            "limit_collar_pct",
            raw.get("max_slippage_bps", collar_map.get("max_slippage_bps")),
        ),
    )
    atr_val = raw.get(
        "limit_collar_atr",
        collar_map.get(
            "limit_collar_atr",
            raw.get("max_atr_fraction", collar_map.get("max_atr_fraction")),
        ),
    )
    if pct_val is not None or atr_val is not None or collar is not None:
        collar_dict: Optional[Dict[str, str]] = {
            "limit_collar_pct": str(
                Decimal(str(pct_val if pct_val is not None else "0"))
            ),
            "limit_collar_atr": str(
                Decimal(str(atr_val if atr_val is not None else "0"))
            ),
        }
    else:
        collar_dict = None
    side_val = raw.get("side", "LONG")
    side_str = side_val.value if isinstance(side_val, PlanSide) else str(side_val).strip().upper()
    raw_exec_spec = raw.get("execution_spec")
    if isinstance(raw_exec_spec, ExecutionSpec):
        exec_spec_canon: Optional[Dict[str, Any]] = raw_exec_spec.to_dict()
    elif isinstance(raw_exec_spec, Mapping) and raw_exec_spec:
        exec_spec_canon = ExecutionSpec.from_dict(raw_exec_spec).to_dict()
    else:
        exec_spec_canon = None
    raw_daily_atr = raw.get("daily_atr_14")
    daily_atr_str = (
        str(Decimal(str(raw_daily_atr)))
        if raw_daily_atr is not None and str(raw_daily_atr).strip() != ""
        else None
    )
    return {
        "side": side_str,
        "target_qty": str(Decimal(str(raw.get("target_qty", "0")))),
        "approved_qty_cap": str(Decimal(str(raw.get("approved_qty_cap", "0")))),
        "target_notional_usd": str(Decimal(str(raw.get("target_notional_usd", "0")))),
        "max_risk_budget_usd": str(Decimal(str(raw.get("max_risk_budget_usd", "0")))),
        "initial_stop_price": str(Decimal(str(raw.get("initial_stop_price", "0")))),
        "take_profit_price": (
            str(Decimal(str(raw["take_profit_price"])))
            if raw.get("take_profit_price") is not None
            else None
        ),
        "collar": collar_dict,
        "daily_atr_14": daily_atr_str,
        "execution_spec": exec_spec_canon,
    }


def compute_strategy_fingerprint(
    symbol: str,
    strategy_config: ArchetypeStrategyConfig,
    risk_envelope: Optional[PlanRiskEnvelope | Mapping[str, Any]] = None,
) -> str:
    """Computes a canonical SHA-256 fingerprint for `(symbol, strategy_config[, risk_envelope])`."""
    canonical: Dict[str, Any] = {
        "symbol": symbol.strip().upper(),
        "strategy_config": strategy_config.to_dict(),
    }
    if risk_envelope is not None:
        canonical["risk_envelope"] = _canonicalize_risk_envelope(risk_envelope)
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _is_daily_bar(bar: Bar) -> bool:
    if _bar_step_ns(bar) == 86_400_000_000_000:
        return True
    bt_str = str(getattr(bar, "bar_type", "")).upper()
    return (
        "1-DAY" in bt_str
        or "-DAY-" in bt_str
        or bt_str == "1D"
        or bt_str.endswith("-1D")
        or "-1D-" in bt_str
    )


def _is_minute_bar(bar: Bar) -> bool:
    if _bar_step_ns(bar) == 60_000_000_000:
        return True
    bt_str = str(getattr(bar, "bar_type", "")).upper()
    return "1-MINUTE" in bt_str or "-1M-" in bt_str or bt_str.endswith("-1M")


def _parse_date_to_ns(date_str: Optional[str], end_of_day: bool = False) -> Optional[int]:
    if not date_str:
        return None
    text = str(date_str).strip()
    if not text:
        return None
    parsed = date.fromisoformat(text[:10])
    dt = (
        datetime(parsed.year, parsed.month, parsed.day, 23, 59, 59, tzinfo=timezone.utc)
        if end_of_day
        else datetime(parsed.year, parsed.month, parsed.day, 0, 0, 0, tzinfo=timezone.utc)
    )
    return int(dt.timestamp() * 1_000_000_000)


class NautilusMcpServer:
    """Session-attested MCP Server exposing strictly 5 controlled Nautilus tools."""

    def __init__(
        self,
        dsn: Optional[str] = None,
        datainfra_config: Optional[DataInfraConfig] = None,
        backtest_runner: Optional[IsomorphicBacktestRunner] = None,
        stress_falsifier: Optional[StressFalsifier] = None,
        system_config: Optional[SystemConfig] = None,
        capital_basis_usd: Decimal | float | str = Decimal("100000.00"),
        venue_name: str = "ALPACA",
        bar_provider: Optional[Callable[[str, str, int], Sequence[Bar]]] = None,
        equity_provider: Optional[Callable[[str], Equity]] = None,
        auth_token: Optional[str] = None,
        portfolio_state_provider: Optional[Callable[..., Any]] = None,
        sip_feeder_url: Optional[str] = None,
        sip_feeder_client: Optional[SipFeederSubscriptionClient] = None,
    ) -> None:
        self._dsn = resolve_database_dsn(dsn) if dsn is not None else None
        self._datainfra_config = datainfra_config
        self._venue_name = str(venue_name or "ALPACA").strip().upper()
        self._capital_basis_usd = Decimal(str(capital_basis_usd))
        self._system_config = system_config or SystemConfig()
        self._custom_runner_injected = backtest_runner is not None
        self._custom_falsifier_injected = stress_falsifier is not None
        self._runner = backtest_runner or self._create_isolated_runner()
        self._falsifier = stress_falsifier or StressFalsifier(runner=self._runner)
        self._bar_provider = bar_provider
        self._equity_provider = equity_provider
        self._auth_token = str(auth_token).strip() if auth_token is not None else None
        self._portfolio_state_provider = portfolio_state_provider
        self._sip_feeder_client = sip_feeder_client or SipFeederSubscriptionClient(
            base_url=sip_feeder_url
        )

        self._lock = threading.Lock()
        self._runner_lock = threading.RLock()
        self._max_verified_cache_entries: int = 1024
        self._cumulative_trials: int = 0
        self._verified_backtests: Dict[str, IsomorphicBacktestMetrics] = {}
        self._verified_falsifications: Dict[str, Tuple[str, StressFalsificationReport]] = {}
        self._verified_falsification_envelopes: Dict[str, PlanRiskEnvelope] = {}

    @staticmethod
    def _clear_runner_last_refs(runner: Optional[Any]) -> None:
        if runner is None:
            return
        for attr in (
            "_last_plan_gate_backend",
            "_last_strategy",
            "_last_telemetry_actor",
            "_last_oco_monitor",
        ):
            if hasattr(runner, attr):
                setattr(runner, attr, None)

    def _create_isolated_runner(self) -> IsomorphicBacktestRunner:
        return IsomorphicBacktestRunner(
            dsn=None,
            datainfra_config=self._datainfra_config,
            starting_capital_usd=self._capital_basis_usd,
            effective_trials=2,
            venue_name=self._venue_name,
        )

    def _prune_verified_caches_locked(self) -> None:
        max_entries = max(1, int(self._max_verified_cache_entries))
        while len(self._verified_backtests) > max_entries:
            oldest_key = next(iter(self._verified_backtests))
            self._verified_backtests.pop(oldest_key, None)
        while len(self._verified_falsifications) > max_entries:
            oldest_key = next(iter(self._verified_falsifications))
            self._verified_falsifications.pop(oldest_key, None)
            self._verified_falsification_envelopes.pop(oldest_key, None)
        while len(self._verified_falsification_envelopes) > max_entries:
            oldest_key = next(iter(self._verified_falsification_envelopes))
            self._verified_falsification_envelopes.pop(oldest_key, None)

    @property
    def allowed_tools(self) -> List[str]:
        return sorted(ALLOWED_MCP_TOOL_NAMES)

    def list_tools(self) -> List[Dict[str, Any]]:
        """Returns the 5 MCP tool schemas."""
        return get_mcp_tool_input_schemas()

    def call_tool(
        self,
        tool_name: str,
        arguments: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Dispatches a controlled MCP tool call after security validation."""
        name = str(tool_name or "").strip()
        if name not in ALLOWED_MCP_TOOL_NAMES:
            raise McpSecurityViolationError(
                f"Tool {name!r} is not permitted on NautilusMcpServer. "
                f"Allowed tools: {sorted(ALLOWED_MCP_TOOL_NAMES)}"
            )
        args = dict(arguments or {})
        assert_no_code_injection(args, path=name)

        if name == "list_strategy_archetypes":
            return self.list_strategy_archetypes()
        if name == "query_datainfra_bars_summary":
            return self.query_datainfra_bars_summary(**args)
        if name == "run_archetype_backtest":
            return self.run_archetype_backtest(**args)
        if name == "run_stress_falsification":
            return self.run_stress_falsification(**args)
        if name == "compile_and_lock_trading_plan":
            return self.compile_and_lock_trading_plan(**args)
        raise McpSecurityViolationError(f"Unhandled tool: {name!r}")

    # =========================================================================
    # Tool 1: list_strategy_archetypes
    # =========================================================================

    def list_strategy_archetypes(self) -> Dict[str, Any]:
        """Returns all registered pre-audited strategy archetypes and parameter bounds."""
        schemas = list_strategy_archetype_schemas()
        return {
            "count": len(schemas),
            "archetypes": schemas,
        }

    # =========================================================================
    # Tool 2: query_datainfra_bars_summary
    # =========================================================================

    def query_datainfra_bars_summary(
        self,
        symbols: Sequence[str],
        lookback_days: int = 60,
        bar_timeframe: str = "15m",
    ) -> Dict[str, Any]:
        """Reads historical bars for `symbols` and computes volatility, ATR quantiles, and liquidity summaries."""
        if not symbols:
            raise ValueError("symbols must be a non-empty list")
        if int(lookback_days) <= 0:
            raise ValueError(f"lookback_days must be > 0, got {lookback_days}")

        summaries: Dict[str, Dict[str, Any]] = {}
        for raw_sym in symbols:
            sym = str(raw_sym).strip().upper()
            if not sym:
                continue
            bars = self._load_bars_for_symbol(
                symbol=sym,
                bar_timeframe=bar_timeframe,
                lookback_days=int(lookback_days),
            )
            if not bars:
                summaries[sym] = {
                    "symbol": sym,
                    "bar_count": 0,
                    "status": "NO_DATA",
                }
                continue

            sorted_bars = sorted(bars, key=lambda b: b.ts_init)
            closes = [float(b.close) for b in sorted_bars]
            highs = [float(b.high) for b in sorted_bars]
            lows = [float(b.low) for b in sorted_bars]
            volumes = [float(b.volume) for b in sorted_bars]

            returns: List[float] = []
            true_ranges: List[float] = []
            rel_ranges_bps: List[float] = []
            for idx in range(len(sorted_bars)):
                h = highs[idx]
                lo = lows[idx]
                c = closes[idx]
                if c > 0.0:
                    rel_ranges_bps.append(((h - lo) / c) * 10_000.0)
                if idx > 0:
                    prev_c = closes[idx - 1]
                    if prev_c > 0.0:
                        returns.append((c - prev_c) / prev_c)
                    tr = max(h - lo, abs(h - prev_c), abs(lo - prev_c))
                    true_ranges.append(tr)
                else:
                    true_ranges.append(max(0.0, h - lo))

            ret_vol = statistics.stdev(returns) if len(returns) >= 2 else 0.0
            atr_window = min(14, len(sorted_bars))
            current_atr = rolling_atr(highs, lows, closes, atr_window)

            sorted_tr = sorted(true_ranges)
            n_tr = len(sorted_tr)
            atr_p50 = sorted_tr[int(round(0.50 * (n_tr - 1)))] if n_tr else 0.0
            atr_p80 = sorted_tr[int(round(0.80 * (n_tr - 1)))] if n_tr else 0.0
            atr_p95 = sorted_tr[int(round(0.95 * (n_tr - 1)))] if n_tr else 0.0

            def _sample_quantile(sorted_vals: Sequence[float], q: float) -> float:
                if not sorted_vals:
                    return 0.0
                idx_q = int(round(max(0.0, min(1.0, q)) * (len(sorted_vals) - 1)))
                return float(sorted_vals[idx_q])

            sorted_returns = sorted(returns)
            sorted_abs_returns = sorted(abs(r) for r in returns)
            return_quantiles = {
                "p10": round(_sample_quantile(sorted_returns, 0.10), 6),
                "p25": round(_sample_quantile(sorted_returns, 0.25), 6),
                "p50": round(_sample_quantile(sorted_returns, 0.50), 6),
                "p75": round(_sample_quantile(sorted_returns, 0.75), 6),
                "p90": round(_sample_quantile(sorted_returns, 0.90), 6),
            }
            abs_return_quantiles = {
                "p50": round(_sample_quantile(sorted_abs_returns, 0.50), 6),
                "p75": round(_sample_quantile(sorted_abs_returns, 0.75), 6),
                "p90": round(_sample_quantile(sorted_abs_returns, 0.90), 6),
                "p95": round(_sample_quantile(sorted_abs_returns, 0.95), 6),
            }

            if len(returns) >= 3:
                mean_ret = statistics.mean(returns)
                cov_lag1 = sum(
                    (returns[i] - mean_ret) * (returns[i - 1] - mean_ret)
                    for i in range(1, len(returns))
                )
                var_ret = sum((r - mean_ret) ** 2 for r in returns)
                autocorr_lag1 = (cov_lag1 / var_ret) if var_ret > 1e-12 else 0.0
            else:
                autocorr_lag1 = 0.0

            run_lengths: List[float] = []
            cur_run = 0
            prev_sign = 0
            for r_val in returns:
                sign = 1 if r_val >= 0.0 else -1
                if sign == prev_sign or prev_sign == 0:
                    cur_run += 1
                else:
                    run_lengths.append(float(cur_run))
                    cur_run = 1
                prev_sign = sign
            if cur_run > 0:
                run_lengths.append(float(cur_run))
            sorted_runs = sorted(run_lengths) if run_lengths else [5.0]
            q75_run_bars = int(round(_sample_quantile(sorted_runs, 0.75)))
            max_half_life = max(5, len(sorted_bars) // 5)
            if 0.0 < abs(autocorr_lag1) < 1.0:
                ar_hl_bars = -math.log(2.0) / math.log(abs(autocorr_lag1))
            else:
                ar_hl_bars = float(q75_run_bars)
            raw_hl_candidate = int(round(max(float(q75_run_bars), ar_hl_bars)))
            empirical_half_life_bars = max(5, min(max_half_life, raw_hl_candidate))

            atr_denom = max(current_atr, atr_p50, 1e-6)
            mfe_atrs: List[float] = []
            mae_atrs: List[float] = []
            for idx in range(len(sorted_bars) - 1):
                end_idx = min(len(sorted_bars), idx + 1 + empirical_half_life_bars)
                entry_c = closes[idx]
                max_h = max(highs[idx + 1 : end_idx])
                min_l = min(lows[idx + 1 : end_idx])
                mfe_atrs.append(max(0.0, (max_h - entry_c) / atr_denom))
                mae_atrs.append(max(0.0, (entry_c - min_l) / atr_denom))
            sorted_mfe = sorted(mfe_atrs)
            sorted_mae = sorted(mae_atrs)
            mfe_q50 = round(_sample_quantile(sorted_mfe, 0.50), 4)
            mfe_q75 = round(_sample_quantile(sorted_mfe, 0.75), 4)
            mfe_q90 = round(_sample_quantile(sorted_mfe, 0.90), 4)
            mae_q50 = round(_sample_quantile(sorted_mae, 0.50), 4)
            mae_q75 = round(_sample_quantile(sorted_mae, 0.75), 4)
            mae_q80 = round(_sample_quantile(sorted_mae, 0.80), 4)
            mae_q90 = round(_sample_quantile(sorted_mae, 0.90), 4)
            mae_q95 = round(_sample_quantile(sorted_mae, 0.95), 4)
            swing_mfe_atr_quantiles = {
                "p50": mfe_q50,
                "p75": mfe_q75,
                "p90": mfe_q90,
            }
            swing_mae_atr_quantiles = {
                "p50": mae_q50,
                "p75": mae_q75,
                "p80": mae_q80,
                "p90": mae_q90,
                "p95": mae_q95,
            }

            tf_norm = str(bar_timeframe or "15m").strip().lower()
            bars_per_day_map = {
                "1m": 390.0,
                "5m": 78.0,
                "15m": 26.0,
                "30m": 13.0,
                "1h": 6.5,
                "1d": 1.0,
            }
            bars_per_day = bars_per_day_map.get(tf_norm, 26.0)
            if 0.0 < abs(autocorr_lag1) < 1.0:
                ar_hl_bars = -math.log(2.0) / math.log(abs(autocorr_lag1))
            else:
                ar_hl_bars = float(empirical_half_life_bars)
            ou_half_life_days = round(max(0.05, ar_hl_bars / bars_per_day), 4)

            ret_5bar = [
                (closes[i] - closes[i - 5]) / closes[i - 5]
                for i in range(5, len(closes))
                if closes[i - 5] > 0.0
            ]
            if len(ret_5bar) >= 2 and len(returns) >= 2:
                var_1 = statistics.variance(returns)
                var_5 = statistics.variance(ret_5bar)
                variance_ratio_5d = (
                    round(var_5 / (5.0 * var_1), 6) if var_1 > 1e-12 else 1.0
                )
            else:
                variance_ratio_5d = 1.0

            median_vol = statistics.median(volumes) if volumes else 0.0
            last_close = closes[-1]
            dollar_vol_median = median_vol * last_close
            sorted_rel_ranges = sorted(rel_ranges_bps)
            est_spread_bps = (
                _sample_quantile(sorted_rel_ranges, 0.05) if sorted_rel_ranges else 0.0
            )

            daily_atr_14_val: Optional[float] = None
            daily_atr_quantiles: Optional[Dict[str, float]] = None
            if tf_norm == "1d":
                if current_atr > 0.0 and math.isfinite(current_atr):
                    daily_atr_14_val = round(current_atr, 6)
                daily_atr_quantiles = {
                    "p50": round(atr_p50, 4),
                    "p80": round(atr_p80, 4),
                    "p95": round(atr_p95, 4),
                }
            else:
                raw_1d_bars: Sequence[Bar] = ()
                try:
                    raw_1d_bars = self._load_bars_for_symbol(
                        symbol=sym,
                        bar_timeframe="1d",
                        lookback_days=max(30, int(lookback_days)),
                    )
                except KeyError:
                    if self._bar_provider is None:
                        raise
                    raw_1d_bars = ()
                filtered_1d = [b for b in (raw_1d_bars or ()) if _is_daily_bar(b)]
                if len(filtered_1d) >= 2:
                    filtered_1d.sort(key=lambda b: int(b.ts_init))
                    d_highs = [float(b.high) for b in filtered_1d]
                    d_lows = [float(b.low) for b in filtered_1d]
                    d_closes = [float(b.close) for b in filtered_1d]
                    d_atr = rolling_atr(d_highs, d_lows, d_closes, min(14, len(d_closes)))
                    if d_atr > 0.0 and math.isfinite(d_atr):
                        daily_atr_14_val = round(d_atr, 6)
                    d_trs: List[float] = []
                    for d_idx in range(len(filtered_1d)):
                        dh = d_highs[d_idx]
                        dl = d_lows[d_idx]
                        if d_idx > 0:
                            dprev = d_closes[d_idx - 1]
                            d_trs.append(max(dh - dl, abs(dh - dprev), abs(dl - dprev)))
                        else:
                            d_trs.append(max(0.0, dh - dl))
                    sorted_d_tr = sorted(d_trs)
                    daily_atr_quantiles = {
                        "p50": round(_sample_quantile(sorted_d_tr, 0.50), 4),
                        "p80": round(_sample_quantile(sorted_d_tr, 0.80), 4),
                        "p95": round(_sample_quantile(sorted_d_tr, 0.95), 4),
                    }

            summaries[sym] = {
                "symbol": sym,
                "bar_timeframe": bar_timeframe,
                "atr_timeframe": bar_timeframe,
                "bar_count": len(sorted_bars),
                "status": "OK",
                "last_close": round(last_close, 4),
                "window_min_low": round(min(lows), 4) if lows else round(last_close, 4),
                "window_max_high": round(max(highs), 4) if highs else round(last_close, 4),
                "return_volatility": round(ret_vol, 6),
                "current_atr": round(current_atr, 4),
                "daily_atr_14": daily_atr_14_val,
                "daily_atr_quantiles": daily_atr_quantiles,
                "atr_quantiles": {
                    "p50": round(atr_p50, 4),
                    "p80": round(atr_p80, 4),
                    "p95": round(atr_p95, 4),
                },
                "return_quantiles": return_quantiles,
                "abs_return_quantiles": abs_return_quantiles,
                "return_autocorr_lag1": round(autocorr_lag1, 6),
                "empirical_autocorr_lag1": round(autocorr_lag1, 6),
                "ou_half_life_days": ou_half_life_days,
                "variance_ratio_5d": variance_ratio_5d,
                "empirical_half_life_bars": int(empirical_half_life_bars),
                "empirical_mae_q75_atr_mult": mae_q75,
                "empirical_mae_q90_atr_mult": mae_q90,
                "empirical_mfe_q75_atr_mult": mfe_q75,
                "empirical_mfe_q90_atr_mult": mfe_q90,
                "swing_mfe_atr_quantiles": swing_mfe_atr_quantiles,
                "swing_mae_atr_quantiles": swing_mae_atr_quantiles,
                "estimated_spread_bps": round(est_spread_bps, 4),
                "median_bar_volume": round(median_vol, 2),
                "median_dollar_volume_usd": round(dollar_vol_median, 2),
                "first_ts_init_ns": int(sorted_bars[0].ts_init),
                "last_ts_init_ns": int(sorted_bars[-1].ts_init),
            }

        return {
            "lookback_days": int(lookback_days),
            "bar_timeframe": bar_timeframe,
            "summaries": summaries,
        }

    # =========================================================================
    # Tool 3: run_archetype_backtest
    # =========================================================================

    def run_archetype_backtest(
        self,
        symbol: str,
        archetype_id: str,
        params: Dict[str, Any],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        risk_envelope: Optional[Dict[str, Any]] = None,
        execution_spec: Optional[Dict[str, Any]] = None,
        walk_forward: bool = False,
        lookback_days: int = 60,
        effective_trials: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Runs `IsomorphicBacktestRunner` for `(symbol, archetype_id, params)` and returns metrics."""
        sym = str(symbol).strip().upper()
        strat_cfg = self._build_strategy_config(sym, archetype_id, params)
        resolved_exec_spec = self._resolve_execution_spec(
            execution_spec=execution_spec,
            params=params,
            raw_envelope=risk_envelope,
            bar_timeframe=strat_cfg.bar_timeframe,
        )
        bars, equities = self._resolve_bars_and_equities_for_contract(
            strat_cfg,
            start_date=start_date,
            end_date=end_date,
            lookback_days=int(lookback_days),
            execution_spec=resolved_exec_spec,
        )
        resolved_daily_atr = self._resolve_daily_atr_14(
            sym,
            raw_envelope=risk_envelope,
            lookback_days=max(60, int(lookback_days)),
        )
        env = self._build_risk_envelope(
            sym,
            risk_envelope,
            bars=bars,
            bar_timeframe=strat_cfg.bar_timeframe,
            archetype_id=strat_cfg.archetype_id,
            atr_stop_multiplier=float(strat_cfg.atr_stop_multiplier),
            daily_atr_14=resolved_daily_atr,
            execution_spec=resolved_exec_spec,
        )
        plan_item = self._make_plan_item_contract(
            symbol=sym,
            strategy_config=strat_cfg,
            risk_envelope=env,
        )
        eff_trials = self._resolve_effective_trials(
            increment=(2 if walk_forward else 1),
            explicit_trials=effective_trials,
        )

        if self._custom_runner_injected:
            with self._runner_lock:
                try:
                    if walk_forward:
                        metrics = self._runner.run_walk_forward(
                            plan_item,
                            bars=bars,
                            equities=equities,
                            effective_trials=eff_trials,
                        )
                    else:
                        metrics = self._runner.run_backtest(
                            plan_item,
                            bars=bars,
                            equities=equities,
                            effective_trials=eff_trials,
                        )
                finally:
                    self._clear_runner_last_refs(self._runner)
        else:
            request_runner = self._create_isolated_runner()
            try:
                if walk_forward:
                    metrics = request_runner.run_walk_forward(
                        plan_item,
                        bars=bars,
                        equities=equities,
                        effective_trials=eff_trials,
                    )
                else:
                    metrics = request_runner.run_backtest(
                        plan_item,
                        bars=bars,
                        equities=equities,
                        effective_trials=eff_trials,
                    )
            finally:
                self._clear_runner_last_refs(request_runner)
                del request_runner

        fp = compute_strategy_fingerprint(sym, strat_cfg)
        backtest_id = f"bt-{fp[:16]}"
        with self._lock:
            self._verified_backtests[fp] = metrics
            self._prune_verified_caches_locked()

        return {
            "backtest_attestation_id": backtest_id,
            "strategy_fingerprint": fp,
            "effective_trials": int(eff_trials),
            "metrics": metrics.to_dict(),
        }

    # =========================================================================
    # Tool 4: run_stress_falsification
    # =========================================================================

    def run_stress_falsification(
        self,
        symbol: str,
        archetype_id: str,
        params: Dict[str, Any],
        risk_envelope: Dict[str, Any],
        execution_spec: Optional[Dict[str, Any]] = None,
        conditional_event_windows: Optional[Sequence[Any]] = None,
        lookback_days: int = 60,
        effective_trials: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Executes `StressFalsifier` (`2x` friction, `+1 bar` delay, tail gap shock) on candidate."""
        sym = str(symbol).strip().upper()
        strat_cfg = self._build_strategy_config(sym, archetype_id, params)
        resolved_exec_spec = self._resolve_execution_spec(
            execution_spec=execution_spec,
            params=params,
            raw_envelope=risk_envelope,
            bar_timeframe=strat_cfg.bar_timeframe,
        )
        bars, equities = self._resolve_bars_and_equities_for_contract(
            strat_cfg,
            lookback_days=int(lookback_days),
            execution_spec=resolved_exec_spec,
        )
        resolved_daily_atr = self._resolve_daily_atr_14(
            sym,
            raw_envelope=risk_envelope,
            lookback_days=max(60, int(lookback_days)),
        )
        env = self._build_risk_envelope(
            sym,
            risk_envelope,
            bars=bars,
            bar_timeframe=strat_cfg.bar_timeframe,
            archetype_id=strat_cfg.archetype_id,
            atr_stop_multiplier=float(strat_cfg.atr_stop_multiplier),
            daily_atr_14=resolved_daily_atr,
            execution_spec=resolved_exec_spec,
        )
        plan_item = self._make_plan_item_contract(
            symbol=sym,
            strategy_config=strat_cfg,
            risk_envelope=env,
        )
        eff_trials = self._resolve_effective_trials(
            increment=1,
            explicit_trials=effective_trials,
        )

        if self._custom_falsifier_injected or self._custom_runner_injected:
            with self._runner_lock:
                try:
                    report = self._invoke_stress_falsifier(
                        self._falsifier,
                        symbol=sym,
                        strategy_config=strat_cfg,
                        risk_envelope=env,
                        plan_item=plan_item,
                        bars=bars,
                        equities=equities,
                        effective_trials=eff_trials,
                        conditional_event_windows=conditional_event_windows,
                        execution_spec=resolved_exec_spec,
                        daily_atr_14=env.daily_atr_14,
                    )
                finally:
                    self._clear_runner_last_refs(self._runner)
                    self._clear_runner_last_refs(getattr(self._falsifier, "_runner", None))
        else:
            request_runner = self._create_isolated_runner()
            request_falsifier = StressFalsifier(runner=request_runner)
            try:
                report = self._invoke_stress_falsifier(
                    request_falsifier,
                    symbol=sym,
                    strategy_config=strat_cfg,
                    risk_envelope=env,
                    plan_item=plan_item,
                    bars=bars,
                    equities=equities,
                    effective_trials=eff_trials,
                    conditional_event_windows=conditional_event_windows,
                    execution_spec=resolved_exec_spec,
                    daily_atr_14=env.daily_atr_14,
                )
            finally:
                self._clear_runner_last_refs(request_runner)
                del request_falsifier, request_runner
        fp = compute_strategy_fingerprint(sym, strat_cfg, risk_envelope=env)
        base_fp = compute_strategy_fingerprint(sym, strat_cfg)
        with self._lock:
            prev_entry = self._verified_falsifications.get(fp)
            if report.passed:
                prev_tok = (
                    prev_entry[0]
                    if (prev_entry is not None and prev_entry[1].passed and prev_entry[0])
                    else ""
                )
                token = prev_tok or f"falsified-{fp[:16]}-{uuid.uuid4().hex[:8]}"
            else:
                token = ""
            self._verified_backtests[fp] = report.baseline_metrics
            self._verified_backtests[base_fp] = report.baseline_metrics
            self._verified_falsifications[fp] = (token, report)
            self._verified_falsifications[base_fp] = (token, report)
            self._verified_falsification_envelopes[fp] = env
            self._verified_falsification_envelopes[base_fp] = env
            self._prune_verified_caches_locked()

        return {
            "symbol": sym,
            "archetype_id": strat_cfg.archetype_id.value,
            "strategy_fingerprint": fp,
            "backtest_attestation_id": f"bt-{fp[:16]}",
            "passed": report.passed,
            "falsification_token": token,
            "daily_atr_14": (
                str(env.daily_atr_14) if env.daily_atr_14 is not None else None
            ),
            "execution_spec": env.execution_spec.to_dict(),
            "report": report.to_dict(),
        }

    # =========================================================================
    # Tool 5: compile_and_lock_trading_plan
    # =========================================================================

    def compile_and_lock_trading_plan(
        self,
        plan_date: str | date,
        validated_items: Sequence[Dict[str, Any]],
        committee_attestation: Dict[str, Any],
        market: str = "US_EQUITY",
        plan_id: Optional[UUID | str] = None,
    ) -> Dict[str, Any]:
        """Validates backtest & falsification gate, applies deterministic capital sizing, and locks plan in PG."""
        if not isinstance(committee_attestation, dict):
            raise McpPlanLockRejectedError("committee_attestation must be a dictionary")
        all_candidates_vetoed = bool(committee_attestation.get("all_candidates_vetoed", False))
        att_k_max = committee_attestation.get("k_max")
        att_m_regime = committee_attestation.get("m_regime")
        att_macro_regime = str(committee_attestation.get("macro_regime") or "").strip().upper()
        if att_k_max is not None and (int(att_k_max) < 0 or int(att_k_max) > 2):
            raise McpPlanLockRejectedError(f"k_max must be in [0, 2], got {att_k_max}")
        if att_m_regime is not None and (float(att_m_regime) < 0.0 or float(att_m_regime) > 1.0):
            raise McpPlanLockRejectedError(f"m_regime must be in [0.0, 1.0], got {att_m_regime}")
        l0_zero_quota = (
            (att_k_max is not None and int(att_k_max) == 0)
            or (att_m_regime is not None and float(att_m_regime) == 0.0)
            or att_macro_regime == "RISK_OFF_DEFENSIVE"
        )
        if not validated_items and not (all_candidates_vetoed or l0_zero_quota):
            raise McpPlanLockRejectedError("validated_items cannot be empty")
        if l0_zero_quota:
            validated_items = []
            all_candidates_vetoed = True
        elif att_k_max is not None and int(att_k_max) > 0:
            validated_items = list(validated_items)[: int(att_k_max)]

        for req_key in REQUIRED_ATTESTATION_KEYS:
            val = str(committee_attestation.get(req_key, "") or "").strip()
            if not val:
                raise McpPlanLockRejectedError(
                    f"committee_attestation missing required deliberation field: {req_key!r}"
                )

        resolved_date = (
            plan_date
            if isinstance(plan_date, date)
            else date.fromisoformat(str(plan_date).strip()[:10])
        )
        resolved_plan_uuid = (
            plan_id
            if isinstance(plan_id, UUID)
            else (UUID(str(plan_id)) if plan_id is not None else uuid.uuid4())
        )
        cap_float = float(self._capital_basis_usd)
        existing_exposure = self._load_current_portfolio_exposure(
            trading_date=resolved_date,
            market=market,
        )
        raw_eq_val = existing_exposure.get("equity_usd")
        equity_usd = (
            float(raw_eq_val)
            if (raw_eq_val is not None and float(raw_eq_val) > 0.0)
            else cap_float
        )
        open_buy_usd = float(existing_exposure.get("open_buy_usd") or 0.0)
        gross_exposure_usd = float(existing_exposure.get("gross_exposure_usd") or 0.0)
        raw_cash_val = existing_exposure.get("cash_usd")
        cash_usd = (
            max(0.0, float(raw_cash_val))
            if raw_cash_val is not None
            else max(0.0, equity_usd - gross_exposure_usd)
        )
        symbol_exposure_usd: Dict[str, float] = dict(
            existing_exposure.get("symbol_exposure_usd") or {}
        )
        sector_exposure_usd: Dict[str, float] = dict(
            existing_exposure.get("sector_exposure_usd") or {}
        )
        committed_by_symbol: Dict[str, float] = {}
        committed_by_sector: Dict[str, float] = {}
        committed_by_venue: Dict[str, float] = {"alpaca": 0.0}
        new_risk_used_today = 0.0

        locked_items: List[PlanItemContract] = []
        sizing_decisions: Dict[str, Dict[str, Any]] = {}
        scaled_to_zero_items: int = 0

        for raw_item in validated_items:
            sym = str(raw_item["symbol"]).strip().upper()
            arch_id = str(raw_item["archetype_id"]).strip()
            params = dict(raw_item["params"])
            raw_env = dict(raw_item["risk_envelope"])
            raw_sector = str(raw_item.get("sector") or "").strip().upper()
            if not raw_sector:
                raw_sector = get_gics_sector_map([sym], dsn=self._dsn).get(sym, "UNCLASSIFIED")
            if not raw_sector or raw_sector in ("UNCLASSIFIED", "UNKNOWN"):
                raise McpPlanLockRejectedError(
                    f"Cannot lock plan for {sym} with unverified or missing GICS sector: {raw_sector!r}"
                )
            sector = raw_sector

            strat_cfg = self._build_strategy_config(sym, arch_id, params)
            raw_conviction = raw_item.get("conviction")
            conviction = float(
                raw_conviction
                if raw_conviction is not None
                else strat_cfg.entry_quantile_threshold
            )
            resolved_exec_spec = self._resolve_execution_spec(
                execution_spec=raw_item.get("execution_spec"),
                params=params,
                raw_envelope=raw_env,
                bar_timeframe=strat_cfg.bar_timeframe,
            )
            resolved_daily_atr = self._resolve_daily_atr_14(
                sym,
                raw_envelope=raw_env,
                raw_item=raw_item,
            )
            req_env = self._build_risk_envelope(
                sym,
                raw_env,
                trading_date=resolved_date,
                bar_timeframe=strat_cfg.bar_timeframe,
                archetype_id=strat_cfg.archetype_id,
                atr_stop_multiplier=float(strat_cfg.atr_stop_multiplier),
                daily_atr_14=resolved_daily_atr,
                execution_spec=resolved_exec_spec,
            )
            fp = compute_strategy_fingerprint(sym, strat_cfg, risk_envelope=req_env)
            base_fp = compute_strategy_fingerprint(sym, strat_cfg)

            with self._lock:
                falsification_entry = (
                    self._verified_falsifications.get(fp)
                    or self._verified_falsifications.get(base_fp)
                )
                falsified_env = (
                    self._verified_falsification_envelopes.get(fp)
                    or self._verified_falsification_envelopes.get(base_fp)
                )

            if falsification_entry is None:
                raise McpPlanLockRejectedError(
                    f"Candidate {sym} ({arch_id}) has not been evaluated by "
                    f"run_stress_falsification (fingerprint={fp[:12]})"
                )
            expected_token, falsification_report = falsification_entry
            if not falsification_report.passed:
                raise McpPlanLockRejectedError(
                    f"Candidate {sym} ({arch_id}) FAILED run_stress_falsification: "
                    f"{list(falsification_report.failure_reasons)}"
                )
            raw_m_regime = (
                raw_item.get("m_regime")
                if raw_item.get("m_regime") is not None
                else committee_attestation.get("m_regime", 1.0)
            )
            eff_m_regime = float(raw_m_regime if raw_m_regime is not None else 1.0)
            if eff_m_regime < 0.0 or eff_m_regime > 1.0:
                raise McpPlanLockRejectedError(f"m_regime must be in [0.0, 1.0], got {raw_m_regime}")
            raw_circuit_mult = raw_item.get("circuit_multiplier")
            circuit_mult = float(raw_circuit_mult if raw_circuit_mult is not None else 1.0)
            if circuit_mult < 0.0 or circuit_mult > 1.0:
                raise McpPlanLockRejectedError(
                    f"circuit_multiplier must be in [0.0, 1.0], got {raw_circuit_mult}"
                )
            raw_alpha_div = raw_item.get("alpha_diversification_scale")
            alpha_div_scale = float(raw_alpha_div if raw_alpha_div is not None else 1.0)
            if not math.isfinite(alpha_div_scale) or alpha_div_scale < 0.0 or alpha_div_scale > 1.0:
                raise McpPlanLockRejectedError(
                    f"alpha_diversification_scale must be in [0.0, 1.0], got {raw_alpha_div}"
                )
            eff_scale = eff_m_regime * circuit_mult * alpha_div_scale

            if falsified_env is not None:
                falsified_env_fp = compute_strategy_fingerprint(
                    sym, strat_cfg, risk_envelope=falsified_env
                )
                if fp != falsified_env_fp:
                    is_valid_regime_downscale = (
                        bool(raw_item.get("_env_already_scaled_by_m_regime", False))
                        and 0.0 < eff_scale < 1.0
                        and req_env.side == falsified_env.side
                        and req_env.initial_stop_price == falsified_env.initial_stop_price
                        and req_env.take_profit_price == falsified_env.take_profit_price
                        and req_env.collar == falsified_env.collar
                        and req_env.execution_spec == falsified_env.execution_spec
                        and req_env.daily_atr_14 == falsified_env.daily_atr_14
                        and req_env.target_qty <= falsified_env.target_qty
                        and req_env.approved_qty_cap <= falsified_env.approved_qty_cap
                        and req_env.target_notional_usd <= falsified_env.target_notional_usd
                        and req_env.max_risk_budget_usd <= falsified_env.max_risk_budget_usd
                    )
                    if not is_valid_regime_downscale:
                        raise McpPlanLockRejectedError(
                            f"Candidate {sym} ({arch_id}) risk_envelope does not match "
                            f"falsified risk_envelope (fingerprint={fp[:12]} vs {falsified_env_fp[:12]})"
                        )
            provided_token = str(raw_item.get("falsification_token", "") or "").strip()
            if provided_token and provided_token != expected_token:
                raise McpPlanLockRejectedError(
                    f"Invalid falsification_token for {sym}: {provided_token!r}"
                )

            # Determine entry reference price for deterministic sizing
            entry_ref = self._resolve_entry_reference_price(sym, req_env, raw_env)
            stop_px_for_sizing = self._normalize_stop_for_sizing(
                entry_price=entry_ref,
                stop_loss_price=float(req_env.initial_stop_price),
                side=req_env.side,
            )
            portfolio_state = PortfolioState(
                complete=True,
                equity_usd=equity_usd,
                venue_equity_usd={"alpaca": equity_usd},
                venue_cash_usd={"alpaca": cash_usd},
                venue_open_buy_usd={"alpaca": open_buy_usd},
                gross_exposure_usd=gross_exposure_usd,
                symbol_exposure_usd=dict(symbol_exposure_usd),
                sector_exposure_usd=dict(sector_exposure_usd),
                committed_usd_by_symbol=dict(committed_by_symbol),
                committed_usd_by_sector=dict(committed_by_sector),
                committed_usd_by_venue=dict(committed_by_venue),
                new_risk_used_today_usd=new_risk_used_today,
            )
            sizing_req = SizingRequest(
                symbol=sym,
                broker="alpaca",
                sector=sector,
                archetype=strat_cfg.archetype_id.value,
                entry_price=entry_ref,
                stop_price=stop_px_for_sizing,
                conviction=conviction,
                fractionable=False,
            )
            decision: SizingDecision = size_position(
                sizing_req,
                portfolio_state,
                self._system_config,
            )
            if not decision.approved or decision.qty <= 0:
                raise McpPlanLockRejectedError(
                    f"Deterministic capital sizing rejected {sym}: "
                    f"{decision.reject_reason or 'ZERO_QTY'}"
                )

            if eff_scale == 0.0:
                scaled_to_zero_items += 1
                continue
            should_scale_env = eff_scale < 1.0 and not bool(
                raw_item.get("_env_already_scaled_by_m_regime", False)
            )
            raw_dec_qty = int(math.floor(float(decision.qty) * eff_scale + 1e-9))
            scaled_dec_notional = float(decision.notional_usd) * eff_scale
            raw_env_target_qty = (
                int(math.floor(float(req_env.target_qty) * eff_scale + 1e-9))
                if should_scale_env
                else int(req_env.target_qty)
            )
            raw_env_cap_qty = (
                int(math.floor(float(req_env.approved_qty_cap) * eff_scale + 1e-9))
                if should_scale_env
                else int(req_env.approved_qty_cap)
            )
            scaled_env_notional = float(req_env.target_notional_usd) * (
                eff_scale if should_scale_env else 1.0
            )
            eff_raw_qty = min(raw_dec_qty, raw_env_target_qty, raw_env_cap_qty)
            eff_budget_notional = min(scaled_dec_notional, scaled_env_notional)
            if eff_scale < 1.0 and eff_raw_qty < 1 and eff_budget_notional < entry_ref - 1e-6:
                sizing_decisions[sym] = {
                    "target_qty": "0",
                    "approved_qty_cap": "0",
                    "target_notional_usd": f"{eff_budget_notional:.2f}",
                    "max_risk_budget_usd": "0.00",
                    "binding_constraint": "REJECTED_WHOLE_SHARE_BUDGET",
                    "sizing_trace": dict(decision.trace or {}),
                }
                scaled_to_zero_items += 1
                continue
            scaled_decision_qty = max(1, raw_dec_qty)
            scaled_env_target_qty = (
                Decimal(str(max(1, raw_env_target_qty)))
                if should_scale_env
                else req_env.target_qty
            )
            scaled_env_cap_qty = (
                Decimal(str(max(1, raw_env_cap_qty)))
                if should_scale_env
                else req_env.approved_qty_cap
            )
            approved_qty_cap = min(
                scaled_env_cap_qty,
                scaled_env_target_qty,
                Decimal(str(scaled_decision_qty)),
            )
            if approved_qty_cap <= Decimal("0"):
                raise McpPlanLockRejectedError(
                    f"Approved quantity cap for {sym} is non-positive: {approved_qty_cap}"
                )
            target_qty = min(scaled_env_target_qty, approved_qty_cap)
            raw_sized_notional = min(
                scaled_env_notional,
                scaled_dec_notional,
                float(target_qty) * entry_ref,
            )
            approved_notional = Decimal(f"{raw_sized_notional:.2f}")
            min_order_notional = Decimal(
                str(load_envelope().numeric["risk.max_order_notional_usd"]["min"])
            )
            if approved_notional < min_order_notional:
                sizing_decisions[sym] = {
                    "target_qty": "0",
                    "approved_qty_cap": "0",
                    "target_notional_usd": str(approved_notional),
                    "max_risk_budget_usd": "0.00",
                    "binding_constraint": "REJECTED_DUST_NOTIONAL",
                    "sizing_trace": dict(decision.trace or {}),
                }
                continue
            approved_risk_budget = min(
                req_env.max_risk_budget_usd,
                approved_notional,
                Decimal(
                    f"{max(decision.risk_budget_usd * eff_scale, decision.risk_usd * eff_scale, 1.0):.2f}"
                ),
            )

            co_dec_map = {
                str(d.get("symbol", "")).strip().upper(): d
                for d in (committee_attestation.get("carry_over_decisions") or [])
                if isinstance(d, Mapping) and str(d.get("symbol", "")).strip()
            }
            matched_co = co_dec_map.get(sym)
            effective_stop_px = req_env.initial_stop_price
            co_filled_qty = Decimal("0")
            if matched_co is not None:
                raw_co_stop = matched_co.get("current_stop_price")
                if raw_co_stop is not None and str(raw_co_stop).strip() != "":
                    try:
                        co_stop_dec = Decimal(str(raw_co_stop))
                        if co_stop_dec > Decimal("0"):
                            if req_env.side == PlanSide.LONG:
                                effective_stop_px = max(effective_stop_px, co_stop_dec)
                            else:
                                effective_stop_px = min(effective_stop_px, co_stop_dec)
                    except Exception:
                        pass
                raw_co_qty = matched_co.get("qty")
                if raw_co_qty is not None and str(raw_co_qty).strip() != "":
                    try:
                        co_filled_qty = abs(Decimal(str(raw_co_qty)))
                    except Exception:
                        co_filled_qty = Decimal("0")

            sized_envelope = PlanRiskEnvelope(
                side=req_env.side,
                target_qty=target_qty,
                approved_qty_cap=approved_qty_cap,
                target_notional_usd=approved_notional,
                max_risk_budget_usd=approved_risk_budget,
                initial_stop_price=effective_stop_px,
                take_profit_price=req_env.take_profit_price,
                collar=req_env.collar,
                valid_until=req_env.valid_until,
                daily_atr_14=req_env.daily_atr_14,
                execution_spec=req_env.execution_spec,
            )
            trace_atr: Optional[float] = (
                float(req_env.daily_atr_14)
                if req_env.daily_atr_14 is not None
                else (
                    abs(entry_ref - float(effective_stop_px))
                    / max(1.0, float(strat_cfg.atr_stop_multiplier))
                    if entry_ref > 0.0
                    and float(effective_stop_px) > 0.0
                    and abs(entry_ref - float(effective_stop_px)) > 0.0
                    else None
                )
            )
            enriched_trace: Dict[str, Any] = dict(decision.trace or {})
            enriched_trace.update(
                {
                    "sector": sector,
                    "conviction": round(conviction, 4),
                    "m_regime": round(eff_m_regime, 4),
                    "macro_regime": str(
                        raw_item.get("macro_regime")
                        or committee_attestation.get("macro_regime")
                        or "RISK_ON_EXPANSION"
                    ),
                    "entry_reference_price": round(entry_ref, 4),
                    "falsification_token": expected_token,
                    "falsification_report": falsification_report.to_dict(),
                    "backtest_metrics": (
                        dict(raw_item["backtest_metrics"])
                        if isinstance(raw_item.get("backtest_metrics"), dict)
                        else falsification_report.baseline_metrics.to_dict()
                    ),
                    "synthesis_rationale": str(raw_item.get("synthesis_rationale", "") or ""),
                    "calibration_rationale": str(raw_item.get("calibration_rationale", "") or ""),
                    "red_team_rationale": str(raw_item.get("red_team_rationale", "") or ""),
                    "committee_attestation": dict(committee_attestation),
                }
            )
            if trace_atr is not None and trace_atr > 0.0 and math.isfinite(trace_atr):
                enriched_trace["atr"] = round(trace_atr, 6)
            if matched_co is not None:
                enriched_trace["carry_over_status"] = str(
                    matched_co.get("lifecycle_state") or "RENEWED_ACTIVE"
                )
                if matched_co.get("qty") is not None:
                    enriched_trace["carry_over_position_qty"] = str(matched_co.get("qty"))
                if matched_co.get("avg_entry_price") is not None:
                    enriched_trace["carry_over_avg_entry_price"] = str(
                        matched_co.get("avg_entry_price")
                    )
            item_contract = self._make_plan_item_contract(
                symbol=sym,
                strategy_config=strat_cfg,
                risk_envelope=sized_envelope,
                plan_id=resolved_plan_uuid,
                sizing_trace=enriched_trace,
            )
            if co_filled_qty > Decimal("0"):
                item_contract = replace(item_contract, filled_qty=co_filled_qty)
            locked_items.append(item_contract)
            sizing_decisions[sym] = {
                "target_qty": str(target_qty),
                "approved_qty_cap": str(approved_qty_cap),
                "target_notional_usd": str(approved_notional),
                "max_risk_budget_usd": str(approved_risk_budget),
                "binding_constraint": decision.binding_constraint,
                "sizing_trace": enriched_trace,
            }

            committed_by_symbol[sym] = committed_by_symbol.get(sym, 0.0) + float(approved_notional)
            committed_by_sector[sector] = committed_by_sector.get(sector, 0.0) + float(approved_notional)
            committed_by_venue["alpaca"] = committed_by_venue.get("alpaca", 0.0) + float(approved_notional)
            new_risk_used_today += float(approved_risk_budget)

        all_scaled_to_zero = bool(validated_items) and scaled_to_zero_items == len(validated_items)
        if not locked_items and not (all_candidates_vetoed or l0_zero_quota or all_scaled_to_zero):
            min_order_notional = Decimal(
                str(load_envelope().numeric["risk.max_order_notional_usd"]["min"])
            )
            raise McpPlanLockRejectedError(
                f"All validated_items were rejected due to sub-${min_order_notional:.2f} dust notional"
            )

        resolved_equity_dec = Decimal(f"{equity_usd:.2f}")
        carry_over_decisions = list(committee_attestation.get("carry_over_decisions") or [])
        zero_item_reason_code = (
            str(
                committee_attestation.get("zero_item_reason_code")
                or (
                    "ALL_CANDIDATES_VETOED"
                    if (all_candidates_vetoed or all_scaled_to_zero)
                    else ""
                )
            ).strip()
            or None
        )

        if self._dsn is not None:
            repo = PlanContractRepository(dsn=self._dsn)
            resolved_plan_uuid = repo.create_and_lock_trading_plan(
                trading_date=resolved_date,
                items=list(locked_items),
                total_equity_usd=resolved_equity_dec,
                market=str(market or "US_EQUITY").strip().upper(),
                summary=str(committee_attestation.get("portfolio_chair_decision", "LOCKED")),
                plan_id=resolved_plan_uuid,
                actor="nautilus_mcp:epistemic_committee",
                carry_over_decisions=carry_over_decisions,
                zero_item_reason_code=zero_item_reason_code,
            )

        plan_symbols = [item.symbol for item in locked_items]
        carry_over_symbols = [
            str(d.get("symbol", "")).strip().upper()
            for d in carry_over_decisions
            if isinstance(d, dict) and str(d.get("symbol", "")).strip()
        ]
        open_pos_symbols = [
            sym
            for sym, mv in symbol_exposure_usd.items()
            if float(mv or 0.0) > 0.0
        ]
        active_sub_symbols = [
            *plan_symbols,
            *carry_over_symbols,
            *open_pos_symbols,
        ]
        try:
            sip_feeder_subscriptions = self._sip_feeder_client.sync_plan_subscriptions(
                active_symbols=active_sub_symbols,
            )
        except Exception as exc:
            logger.error(
                "Failed to synchronize pre-market sip-feeder subscriptions for plan %s: %s",
                resolved_plan_uuid,
                exc,
            )
            sip_feeder_subscriptions = {
                "status": "ERROR",
                "error": str(exc),
                "added": SipFeederSubscriptionClient.normalize_symbols(active_sub_symbols),
                "removed": [],
            }

        return {
            "status": "LOCKED",
            "plan_status": "LOCKED_NO_TRADE" if not locked_items else "LOCKED",
            "all_candidates_vetoed": all_candidates_vetoed,
            "zero_item_reason_code": zero_item_reason_code,
            "carry_over_decisions": carry_over_decisions,
            "sip_feeder_subscriptions": sip_feeder_subscriptions,
            "plan_id": str(resolved_plan_uuid),
            "market": str(market or "US_EQUITY").strip().upper(),
            "trading_date": resolved_date.isoformat(),
            "capital_basis_usd": str(resolved_equity_dec),
            "locked_item_count": len(locked_items),
            "items": [
                {
                    "item_id": str(item.item_id),
                    "plan_id": str(resolved_plan_uuid),
                    "symbol": item.symbol,
                    "venue": item.venue,
                    "strategy_id": item.strategy_id,
                    "strategy_config": item.strategy_config.to_dict(),
                    "target_qty": str(item.risk_envelope.target_qty),
                    "approved_qty_cap": str(item.risk_envelope.approved_qty_cap),
                    "target_notional_usd": str(item.risk_envelope.target_notional_usd),
                    "max_risk_budget_usd": str(item.risk_envelope.max_risk_budget_usd),
                    "initial_stop_price": str(item.risk_envelope.initial_stop_price),
                    "take_profit_price": (
                        str(item.risk_envelope.take_profit_price)
                        if item.risk_envelope.take_profit_price is not None
                        else None
                    ),
                    "daily_atr_14": (
                        str(item.daily_atr_14)
                        if item.daily_atr_14 is not None
                        else None
                    ),
                    "execution_spec": (
                        item.execution_spec.to_dict()
                        if item.execution_spec is not None
                        else None
                    ),
                }
                for item in locked_items
            ],
            "sizing_decisions": sizing_decisions,
            "committee_attestation": dict(committee_attestation),
        }

    # =========================================================================
    # JSON-RPC 2.0 MCP Protocol Handler & HTTP Server
    # =========================================================================

    def handle_jsonrpc_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handles a JSON-RPC 2.0 MCP request (`initialize`, `tools/list`, `tools/call`)."""
        req_id = request.get("id")
        method = str(request.get("method", "")).strip()
        params = dict(request.get("params") or {})

        try:
            if method == "initialize":
                result = {
                    "protocolVersion": "2024-11-05",
                    "serverInfo": {
                        "name": "nautilus-mcp",
                        "version": "2.0.0",
                    },
                    "capabilities": {"tools": {"listChanged": False}},
                }
            elif method == "tools/list":
                result = {"tools": self.list_tools()}
            elif method == "tools/call":
                tool_name = str(params.get("name", ""))
                tool_args = dict(params.get("arguments") or {})
                payload = self.call_tool(tool_name, tool_args)
                result = {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(payload, sort_keys=True),
                        }
                    ],
                    "structuredContent": payload,
                    "isError": False,
                }
            else:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {
                        "code": -32601,
                        "message": f"Method not found: {method!r}",
                    },
                }
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": result,
            }
        except (
            McpSecurityViolationError,
            McpPlanLockRejectedError,
            ValueError,
            KeyError,
            RuntimeError,
        ) as exc:
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32602,
                    "message": str(exc),
                    "data": {"exception_type": type(exc).__name__},
                },
            }
        except Exception as exc:
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32603,
                    "message": f"{type(exc).__name__}: {exc}",
                    "data": {"exception_type": type(exc).__name__},
                },
            }

    def _resolve_auth_token(self, explicit_token: Optional[str] = None) -> str:
        if explicit_token is not None:
            return str(explicit_token).strip()
        if self._auth_token is not None:
            return self._auth_token
        return str(os.environ.get("NAUTILUS_MCP_AUTH_TOKEN", "") or "").strip()

    def create_http_server(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        auth_token: Optional[str] = None,
    ) -> ThreadingHTTPServer:
        """Creates a `ThreadingHTTPServer` serving JSON-RPC MCP over HTTP POST `/mcp` and GET `/healthz`."""
        mcp_server = self
        explicit_auth_token = auth_token

        class _AffinityWorker:
            """Single persistent daemon worker thread keeping PyO3 backtest objects thread-affine."""

            def __init__(self) -> None:
                self._queue: (
                    "queue.Queue[Optional[Tuple[Callable[[], Any], concurrent.futures.Future[Any]]]]"
                ) = queue.Queue()
                self._closed = False
                self._lock = threading.Lock()
                self._thread = threading.Thread(
                    target=self._run,
                    name="nautilus-mcp-affinity-worker",
                    daemon=True,
                )
                self._thread.start()

            def _run(self) -> None:
                while True:
                    item = self._queue.get()
                    if item is None:
                        self._queue.task_done()
                        break
                    fn, fut = item
                    try:
                        res = fn()
                    except BaseException as exc:
                        fut.set_exception(exc)
                    else:
                        fut.set_result(res)
                    finally:
                        self._queue.task_done()

            def submit(self, fn: Callable[[], Any]) -> Any:
                with self._lock:
                    if self._closed:
                        return fn()
                    fut: concurrent.futures.Future[Any] = concurrent.futures.Future()
                    self._queue.put((fn, fut))
                return fut.result()

            def close(self) -> None:
                with self._lock:
                    if self._closed:
                        return
                    self._closed = True
                    self._queue.put(None)
                if self._thread.is_alive() and threading.current_thread() is not self._thread:
                    self._thread.join(timeout=2.0)

        affinity_worker = _AffinityWorker()

        class _McpHttpHandler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                return

            def do_GET(self) -> None:
                if self.path in ("/healthz", "/health"):
                    body = json.dumps(
                        {
                            "status": "ok",
                            "service": "nautilus-mcp",
                            "tools": mcp_server.allowed_tools,
                        }
                    ).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                self.send_response(404)
                self.end_headers()

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(length) if length > 0 else b"{}"
                expected_token = mcp_server._resolve_auth_token(explicit_auth_token)
                if expected_token:
                    auth_hdr = str(self.headers.get("Authorization", "") or "").strip()
                    alt_hdr = str(self.headers.get("X-MCP-Auth-Token", "") or "").strip()
                    if not auth_hdr and not alt_hdr:
                        err_payload = json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": None,
                                "error": {
                                    "code": -32001,
                                    "message": "Unauthorized: missing MCP authentication token",
                                    "data": {"exception_type": "McpSecurityViolationError"},
                                },
                            }
                        ).encode("utf-8")
                        self.send_response(401)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("WWW-Authenticate", "Bearer")
                        self.send_header("Content-Length", str(len(err_payload)))
                        self.end_headers()
                        self.wfile.write(err_payload)
                        return

                    if auth_hdr.lower().startswith("bearer "):
                        candidate_token = auth_hdr[7:].strip()
                    elif alt_hdr:
                        candidate_token = alt_hdr
                    else:
                        candidate_token = ""

                    if not candidate_token or not hmac.compare_digest(
                        candidate_token.encode("utf-8"),
                        expected_token.encode("utf-8"),
                    ):
                        err_payload = json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": None,
                                "error": {
                                    "code": -32001,
                                    "message": "Forbidden: invalid MCP authentication token",
                                    "data": {"exception_type": "McpSecurityViolationError"},
                                },
                            }
                        ).encode("utf-8")
                        self.send_response(403)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(err_payload)))
                        self.end_headers()
                        self.wfile.write(err_payload)
                        return

                try:
                    req_obj = json.loads(raw_body.decode("utf-8"))
                    resp_obj = affinity_worker.submit(
                        lambda: mcp_server.handle_jsonrpc_request(req_obj)
                    )
                    status_code = 200
                except Exception as exc:
                    resp_obj = {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {"code": -32700, "message": str(exc)},
                    }
                    status_code = 400
                finally:
                    # Do not invoke gc.collect() across HTTP worker threads when PyO3 unsendable
                    # objects may exist on other threads; per-request BacktestEngine instances
                    # already dispose and collect synchronously on the affinity worker thread.
                    pass
                payload = json.dumps(resp_obj).encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        class _AffinityMcpHttpServer(ThreadingHTTPServer):
            def server_close(self) -> None:
                try:
                    affinity_worker.close()
                finally:
                    super().server_close()

        return _AffinityMcpHttpServer((host, port), _McpHttpHandler)

    # =========================================================================
    # Internal Helpers
    # =========================================================================

    def _build_strategy_config(
        self,
        symbol: str,
        archetype_id: str,
        params: Dict[str, Any],
    ) -> ArchetypeStrategyConfig:
        arch_enum = StrategyArchetypeId(str(archetype_id).strip())
        bounds = get_archetype_descriptor(arch_enum).parameter_bounds
        inst_id = str(params.get("instrument_id") or f"{symbol}.{self._venue_name}").strip()
        return ArchetypeStrategyConfig(
            archetype_id=arch_enum,
            instrument_id=inst_id,
            bar_timeframe=str(params.get("bar_timeframe", bounds["bar_timeframe"]["default"])),
            lookback_window=int(params.get("lookback_window", bounds["lookback_window"]["default"])),
            fast_window=int(params.get("fast_window", bounds["fast_window"]["default"])),
            slow_window=int(params.get("slow_window", bounds["slow_window"]["default"])),
            entry_quantile_threshold=Decimal(
                str(params.get("entry_quantile_threshold", bounds["entry_quantile_threshold"]["default"]))
            ),
            exit_quantile_threshold=Decimal(
                str(params.get("exit_quantile_threshold", bounds["exit_quantile_threshold"]["default"]))
            ),
            atr_stop_multiplier=Decimal(
                str(params.get("atr_stop_multiplier", bounds["atr_stop_multiplier"]["default"]))
            ),
        )

    @staticmethod
    def _invoke_stress_falsifier(
        falsifier: Any,
        *,
        symbol: str,
        strategy_config: ArchetypeStrategyConfig,
        risk_envelope: PlanRiskEnvelope,
        plan_item: PlanItemContract,
        bars: Sequence[Bar],
        equities: Optional[Sequence[Equity]],
        effective_trials: int,
        conditional_event_windows: Optional[Sequence[Any]] = None,
        execution_spec: Optional[ExecutionSpec] = None,
        daily_atr_14: Optional[Decimal] = None,
    ) -> StressFalsificationReport:
        if conditional_event_windows:
            norm_windows: List[Any] = []
            for w in conditional_event_windows:
                if isinstance(w, Mapping):
                    w_start = w.get("start_ts_ns") or w.get("start_ts") or w.get("start") or w.get("tau")
                    w_end = w.get("end_ts_ns") or w.get("end_ts") or w.get("end") or w.get("tau_end")
                    w_sym = w.get("symbol")
                    if w_sym is not None:
                        norm_windows.append((w_sym, w_start, w_end))
                    else:
                        norm_windows.append((w_start, w_end))
                elif isinstance(w, list):
                    norm_windows.append(tuple(w))
                else:
                    norm_windows.append(w)

            cond_fn = getattr(falsifier, "run_conditional_swing_falsification", None)
            if callable(cond_fn):
                sig_params = inspect.signature(cond_fn).parameters
                accepts_kwargs = any(
                    p.kind == inspect.Parameter.VAR_KEYWORD for p in sig_params.values()
                )
                candidate_kwargs: Dict[str, Any] = {
                    "symbol": symbol,
                    "strategy_config": strategy_config,
                    "risk_envelope": risk_envelope,
                    "plan_item": plan_item,
                    "bars_by_symbol": {symbol: list(bars)},
                    "bars": list(bars),
                    "equities": list(equities) if equities is not None else None,
                    "conditional_event_windows": norm_windows,
                    "execution_spec": execution_spec,
                    "daily_atr_14": daily_atr_14,
                    "effective_trials": int(effective_trials),
                }
                call_kwargs = (
                    candidate_kwargs
                    if accepts_kwargs
                    else {k: v for k, v in candidate_kwargs.items() if k in sig_params}
                )
                return cond_fn(**call_kwargs)

            run_fn = falsifier.run_falsification
            run_params = inspect.signature(run_fn).parameters
            if "conditional_event_windows" in run_params or any(
                p.kind == inspect.Parameter.VAR_KEYWORD for p in run_params.values()
            ):
                return run_fn(
                    plan_item,
                    bars=bars,
                    equities=equities,
                    effective_trials=effective_trials,
                    conditional_event_windows=norm_windows,
                )

        return falsifier.run_falsification(
            plan_item,
            bars=bars,
            equities=equities,
            effective_trials=effective_trials,
        )

    @staticmethod
    def _resolve_execution_spec(
        *,
        execution_spec: Optional[Mapping[str, Any]] = None,
        params: Optional[Mapping[str, Any]] = None,
        raw_envelope: Optional[Mapping[str, Any]] = None,
        bar_timeframe: str = "15m",
    ) -> ExecutionSpec:
        raw: Optional[Mapping[str, Any]] = None
        for candidate in (
            execution_spec,
            raw_envelope.get("execution_spec") if isinstance(raw_envelope, Mapping) else None,
            params.get("execution_spec") if isinstance(params, Mapping) else None,
        ):
            if isinstance(candidate, ExecutionSpec):
                return candidate
            if isinstance(candidate, Mapping) and candidate:
                raw = candidate
                break
        return ExecutionSpec.from_dict(raw, default_bar_timeframe=bar_timeframe)

    def _resolve_daily_atr_14(
        self,
        symbol: str,
        *,
        raw_envelope: Optional[Mapping[str, Any]] = None,
        raw_item: Optional[Mapping[str, Any]] = None,
        lookback_days: int = 60,
    ) -> Optional[Decimal]:
        for src in (raw_item, raw_envelope):
            if isinstance(src, Mapping) and src.get("daily_atr_14") is not None:
                raw_val = src["daily_atr_14"]
                if str(raw_val).strip() != "":
                    atr_dec = Decimal(str(raw_val))
                    if atr_dec > Decimal("0"):
                        return atr_dec

        if self._bar_provider is None and self._datainfra_config is None:
            return None

        try:
            daily_bars = self._load_bars_for_symbol(
                symbol=symbol,
                bar_timeframe="1d",
                lookback_days=max(30, int(lookback_days)),
            )
        except Exception as exc:
            logger.debug("Unable to load 1d bars for daily_atr_14 (%s): %s", symbol, exc)
            return None

        filtered_1d: List[Bar] = [b for b in (daily_bars or ()) if _is_daily_bar(b)]
        if len(filtered_1d) < 2:
            return None
        filtered_1d.sort(key=lambda b: int(b.ts_init))
        d_highs = [float(b.high) for b in filtered_1d]
        d_lows = [float(b.low) for b in filtered_1d]
        d_closes = [float(b.close) for b in filtered_1d]
        atr_val = rolling_atr(d_highs, d_lows, d_closes, min(14, len(d_closes)))
        if atr_val > 0.0 and math.isfinite(atr_val):
            return Decimal(f"{atr_val:.6f}")
        return None

    def _build_risk_envelope(
        self,
        symbol: str,
        raw_envelope: Optional[Dict[str, Any]],
        bars: Optional[Sequence[Bar]] = None,
        *,
        trading_date: Optional[date] = None,
        bar_timeframe: str = "15m",
        archetype_id: Optional[StrategyArchetypeId | str] = None,
        atr_stop_multiplier: Optional[float] = None,
        daily_atr_14: Optional[Decimal] = None,
        execution_spec: Optional[ExecutionSpec] = None,
    ) -> PlanRiskEnvelope:
        risk_cfg = self._system_config.risk
        exec_cfg = self._system_config.execution
        default_collar_pct = Decimal(str(exec_cfg.limit_collar_pct))
        default_collar_atr = (
            default_limit_collar_atr_for_archetype(archetype_id)
            if archetype_id is not None
            else Decimal(str(exec_cfg.limit_collar_atr))
        )
        risk_per_trade_frac = Decimal(str(risk_cfg.risk_per_trade_pct)) / Decimal("100")
        max_single_name_frac = Decimal(str(risk_cfg.max_single_name_pct)) / Decimal("100")
        ttl_bars = max(1, int(exec_cfg.entry_ttl_bars))
        ny_tz = ZoneInfo("America/New_York")

        if trading_date is not None:
            default_valid_until = compute_intraday_entry_valid_until(
                trading_date=trading_date,
                bar_timeframe=bar_timeframe,
                entry_ttl_bars=ttl_bars,
            )
        elif bars:
            max_ts_ns = max(int(b.ts_init) for b in bars)
            last_bar_dt = datetime.fromtimestamp(max_ts_ns / 1_000_000_000.0, tz=timezone.utc)
            inferred_date = last_bar_dt.astimezone(ny_tz).date()
            default_valid_until = max(
                last_bar_dt,
                compute_intraday_entry_valid_until(
                    trading_date=inferred_date,
                    bar_timeframe=bar_timeframe,
                    entry_ttl_bars=ttl_bars,
                    anchor_ts_utc=last_bar_dt,
                ),
            )
        else:
            now_utc = datetime.now(timezone.utc)
            inferred_date = now_utc.astimezone(ny_tz).date()
            default_valid_until = compute_intraday_entry_valid_until(
                trading_date=inferred_date,
                bar_timeframe=bar_timeframe,
                entry_ttl_bars=ttl_bars,
            )

        # Compute empirical price & ATR distribution from bars when available
        bar_closes: List[float] = []
        bar_highs: List[float] = []
        bar_lows: List[float] = []
        if bars:
            for b in bars:
                if str(b.bar_type.instrument_id).startswith(symbol):
                    bar_closes.append(float(b.close))
                    bar_highs.append(float(b.high))
                    bar_lows.append(float(b.low))

        emp_last_px = bar_closes[-1] if bar_closes else 0.0
        emp_atr = (
            rolling_atr(bar_highs, bar_lows, bar_closes, min(14, len(bar_closes)))
            if len(bar_closes) >= 2
            else 0.0
        )
        eff_daily_atr_14 = daily_atr_14
        if eff_daily_atr_14 is None and isinstance(raw_envelope, Mapping) and raw_envelope.get("daily_atr_14") is not None:
            raw_d = str(raw_envelope["daily_atr_14"]).strip()
            if raw_d:
                cand_d = Decimal(raw_d)
                if cand_d > Decimal("0"):
                    eff_daily_atr_14 = cand_d
        eff_exec_spec = execution_spec or self._resolve_execution_spec(
            raw_envelope=raw_envelope,
            bar_timeframe=bar_timeframe,
        )
        stop_atr = float(eff_daily_atr_14) if eff_daily_atr_14 is not None else emp_atr
        arch_desc = get_archetype_descriptor(archetype_id or StrategyArchetypeId.TREND_MOMENTUM)
        atr_bounds = dict(arch_desc.parameter_bounds.get("atr_stop_multiplier") or {})
        min_atr_mult = float(atr_bounds.get("min_inclusive", atr_bounds.get("min", "0.5")))
        default_atr_mult = float(atr_bounds.get("default", min_atr_mult))
        eff_atr_mult = max(
            float(atr_stop_multiplier if atr_stop_multiplier is not None else default_atr_mult),
            min_atr_mult,
        )

        if raw_envelope is not None:
            side = PlanSide(str(raw_envelope.get("side", "long")).strip().lower())
            raw_target_qty = raw_envelope.get("target_qty")
            if raw_target_qty is None:
                raise ValueError(f"risk_envelope for {symbol} missing required 'target_qty'")
            target_qty = Decimal(str(raw_target_qty))
            approved_qty_cap = Decimal(
                str(raw_envelope.get("approved_qty_cap", target_qty))
            )
            raw_notional = raw_envelope.get(
                "target_notional_usd",
                raw_envelope.get("max_notional_usd"),
            )
            if raw_notional is None:
                if emp_last_px <= 0.0:
                    raise ValueError(
                        f"risk_envelope for {symbol} missing 'target_notional_usd' and no bars available"
                    )
                target_notional = Decimal(f"{float(target_qty) * emp_last_px:.2f}")
            else:
                target_notional = Decimal(str(raw_notional))

            raw_risk_budget = raw_envelope.get("max_risk_budget_usd")
            if raw_risk_budget is None:
                max_risk_budget = (
                    self._capital_basis_usd * risk_per_trade_frac
                ).quantize(Decimal("0.01"))
            else:
                max_risk_budget = Decimal(str(raw_risk_budget))

            raw_stop = raw_envelope.get(
                "initial_stop_price",
                raw_envelope.get("stop_loss_price"),
            )
            if raw_stop is None:
                if emp_last_px <= 0.0 or stop_atr <= 0.0:
                    raise ValueError(
                        f"risk_envelope for {symbol} missing 'initial_stop_price' and no empirical ATR available"
                    )
                emp_stop_dist = max(
                    stop_atr * eff_atr_mult,
                    emp_last_px * float(risk_per_trade_frac),
                )
                tick_floor = float(rule_612_tick_size(Decimal(f"{emp_last_px:.4f}")))
                raw_calc_stop = max(
                    tick_floor,
                    emp_last_px - emp_stop_dist if side == PlanSide.LONG else emp_last_px + emp_stop_dist,
                )
                initial_stop = quantize_price_rule_612(Decimal(f"{raw_calc_stop:.6f}"), side=side)
            else:
                initial_stop = Decimal(str(raw_stop))
                if eff_daily_atr_14 is not None and raw_envelope.get("daily_atr_14") is None:
                    ref_px_val = 0.0
                    for ref_k in ("reference_price", "entry_price"):
                        if raw_envelope.get(ref_k) is not None:
                            ref_px_val = float(raw_envelope[ref_k])
                            break
                    if ref_px_val <= 0.0 and target_qty > Decimal("0") and target_notional > Decimal("0"):
                        ref_px_val = float(target_notional / target_qty)
                    if ref_px_val > 0.0 and (
                        str(raw_envelope.get("atr_timeframe") or "").strip().lower() == "15m"
                        or abs(ref_px_val - float(initial_stop)) < float(eff_daily_atr_14) * min_atr_mult
                    ):
                        widened_dist = max(
                            float(eff_daily_atr_14) * eff_atr_mult,
                            ref_px_val * float(risk_per_trade_frac),
                        )
                        tick_floor = float(rule_612_tick_size(Decimal(f"{ref_px_val:.4f}")))
                        widened_stop = max(
                            tick_floor,
                            ref_px_val - widened_dist if side == PlanSide.LONG else ref_px_val + widened_dist,
                        )
                        initial_stop = quantize_price_rule_612(
                            Decimal(f"{widened_stop:.6f}"),
                            side=side,
                        )

            tp_raw = raw_envelope.get("take_profit_price")
            take_profit = Decimal(str(tp_raw)) if tp_raw is not None else None
            collar_raw = raw_envelope.get("collar")
            if collar_raw is None:
                raw_collar_pct = raw_envelope.get(
                    "limit_collar_pct",
                    raw_envelope.get("max_slippage_bps", default_collar_pct),
                )
                raw_collar_atr = raw_envelope.get(
                    "limit_collar_atr",
                    raw_envelope.get("max_atr_fraction", default_collar_atr),
                )
                collar = MarketableLimitCollar(
                    limit_collar_pct=Decimal(str(raw_collar_pct)),
                    limit_collar_atr=Decimal(str(raw_collar_atr)),
                )
            elif isinstance(collar_raw, MarketableLimitCollar):
                collar = collar_raw
            else:
                collar = MarketableLimitCollar.from_dict(collar_raw)

            vu_raw = raw_envelope.get("valid_until")
            if isinstance(vu_raw, datetime):
                valid_until = vu_raw if vu_raw.tzinfo else vu_raw.replace(tzinfo=timezone.utc)
            elif isinstance(vu_raw, str) and vu_raw.strip():
                parsed_vu = datetime.fromisoformat(vu_raw.strip())
                valid_until = (
                    parsed_vu if parsed_vu.tzinfo else parsed_vu.replace(tzinfo=timezone.utc)
                )
            else:
                valid_until = default_valid_until

            return PlanRiskEnvelope(
                side=side,
                target_qty=target_qty,
                approved_qty_cap=approved_qty_cap,
                target_notional_usd=target_notional,
                max_risk_budget_usd=max_risk_budget,
                initial_stop_price=initial_stop,
                take_profit_price=take_profit,
                collar=collar,
                valid_until=valid_until,
                daily_atr_14=eff_daily_atr_14,
                execution_spec=eff_exec_spec,
            )

        if emp_last_px <= 0.0:
            raise ValueError(
                f"Cannot derive empirical PlanRiskEnvelope for {symbol} without raw_envelope or historical bars"
            )

        stop_dist = max(stop_atr * eff_atr_mult, emp_last_px * float(risk_per_trade_frac))
        tick_floor = float(rule_612_tick_size(Decimal(f"{emp_last_px:.4f}")))
        stop_px_dec = quantize_price_rule_612(
            Decimal(f"{max(tick_floor, emp_last_px - stop_dist):.6f}"),
            side=PlanSide.LONG,
        )
        tp_px = emp_last_px + stop_dist
        max_risk_usd = float(self._capital_basis_usd * risk_per_trade_frac)
        max_pos_usd = float(self._capital_basis_usd * max_single_name_frac)
        derived_qty = max(
            1,
            int(min(max_risk_usd / max(stop_dist, 1e-6), max_pos_usd / emp_last_px)),
        )
        notional = float(derived_qty) * emp_last_px
        return PlanRiskEnvelope(
            side=PlanSide.LONG,
            target_qty=Decimal(str(derived_qty)),
            approved_qty_cap=Decimal(str(derived_qty)),
            target_notional_usd=Decimal(f"{notional:.2f}"),
            max_risk_budget_usd=Decimal(f"{max_risk_usd:.2f}"),
            initial_stop_price=stop_px_dec,
            take_profit_price=Decimal(f"{tp_px:.2f}"),
            collar=MarketableLimitCollar(
                limit_collar_pct=default_collar_pct,
                limit_collar_atr=default_collar_atr,
            ),
            valid_until=default_valid_until,
            daily_atr_14=eff_daily_atr_14,
            execution_spec=eff_exec_spec,
        )

    def _make_plan_item_contract(
        self,
        *,
        symbol: str,
        strategy_config: ArchetypeStrategyConfig,
        risk_envelope: PlanRiskEnvelope,
        plan_id: Optional[UUID] = None,
        sizing_trace: Optional[Dict[str, Any]] = None,
    ) -> PlanItemContract:
        venue = (
            strategy_config.instrument_id.split(".", 1)[1].strip().upper()
            if "." in strategy_config.instrument_id
            else self._venue_name
        )
        return PlanItemContract(
            item_id=uuid.uuid4(),
            plan_id=plan_id or uuid.uuid4(),
            symbol=symbol,
            venue=venue,
            broker="alpaca",
            asset_class="US_EQUITY",
            strategy_id=f"{strategy_config.archetype_id.value}-{symbol[:8]}",
            strategy_config=strategy_config,
            risk_envelope=risk_envelope,
            sizing_trace=sizing_trace or {"source": "nautilus_mcp"},
            daily_atr_14=risk_envelope.daily_atr_14,
            execution_spec=risk_envelope.execution_spec,
        )

    @staticmethod
    def _resolve_entry_reference_price(
        symbol: str,
        req_env: PlanRiskEnvelope,
        raw_env: Dict[str, Any],
    ) -> float:
        for key in ("entry_price", "reference_price"):
            if key in raw_env and raw_env[key] is not None:
                return float(raw_env[key])
        if (
            "entry_price_band_low" in raw_env
            and "entry_price_band_high" in raw_env
            and raw_env["entry_price_band_low"] is not None
            and raw_env["entry_price_band_high"] is not None
        ):
            return 0.5 * (
                float(raw_env["entry_price_band_low"])
                + float(raw_env["entry_price_band_high"])
            )
        if req_env.target_qty > Decimal("0"):
            implied = float(req_env.target_notional_usd / req_env.target_qty)
            if implied > 0.0:
                return implied
        if req_env.take_profit_price is not None and req_env.take_profit_price > Decimal("0"):
            return 0.5 * (
                float(req_env.initial_stop_price) + float(req_env.take_profit_price)
            )
        raise McpPlanLockRejectedError(
            f"Cannot resolve entry reference price for {symbol}: missing reference_price and target_notional_usd"
        )

    def _resolve_effective_trials(
        self,
        *,
        increment: int = 1,
        explicit_trials: Optional[int] = None,
    ) -> int:
        with self._lock:
            self._cumulative_trials += max(1, int(increment))
            session_trials = self._cumulative_trials

        if explicit_trials is not None:
            resolved = max(2, int(explicit_trials))
            self._runner._effective_trials = resolved
            return resolved

        db_trials = self._query_db_trial_count()
        runner_base = int(getattr(self._runner, "_effective_trials", 2) or 2)
        resolved = max(2, session_trials + db_trials, runner_base)
        self._runner._effective_trials = resolved
        return resolved

    def _query_db_trial_count(self) -> int:
        if not self._dsn:
            return 0
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor

            with psycopg2.connect(self._dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT
                            to_regclass('public.factor_zoo_registry') AS zoo_reg,
                            to_regclass('public.alpha_factors') AS af_reg,
                            to_regclass('public.alpha_mining_runs') AS amr_reg
                        """
                    )
                    reg_row = cur.fetchone() or {}
                    zoo_trials = 0
                    af_trials = 0
                    mining_trials = 0

                    if reg_row.get("zoo_reg") is not None:
                        cur.execute("SELECT COUNT(*) AS total FROM factor_zoo_registry")
                        zoo_trials = int((cur.fetchone() or {}).get("total") or 0)

                    if reg_row.get("af_reg") is not None:
                        cur.execute(
                            """
                            SELECT COALESCE(SUM(GREATEST(COALESCE(eval_count, 1), 1)), 0) AS total
                            FROM alpha_factors
                            """
                        )
                        af_trials = int((cur.fetchone() or {}).get("total") or 0)

                    if reg_row.get("amr_reg") is not None:
                        cur.execute(
                            """
                            SELECT COALESCE(SUM(GREATEST(COALESCE(candidates_generated, 1), 1)), 0) AS total
                            FROM alpha_mining_runs
                            """
                        )
                        mining_trials = int((cur.fetchone() or {}).get("total") or 0)

                    return max(zoo_trials, af_trials, mining_trials, zoo_trials + af_trials)
        except Exception as exc:
            logger.error(
                "Failed to query cumulative trial count from PostgreSQL for DSR deflation: %s",
                exc,
                exc_info=True,
            )
            raise

    def _load_current_portfolio_exposure(
        self,
        *,
        trading_date: Optional[date] = None,
        market: str = "US_EQUITY",
    ) -> Dict[str, Any]:
        cap_float = float(self._capital_basis_usd)
        exposure: Dict[str, Any] = {
            "equity_usd": cap_float,
            "cash_usd": cap_float,
            "open_buy_usd": 0.0,
            "gross_exposure_usd": 0.0,
            "symbol_exposure_usd": {},
            "sector_exposure_usd": {},
        }

        if self._portfolio_state_provider is not None:
            try:
                provided = self._portfolio_state_provider(
                    trading_date=trading_date,
                    market=market,
                )
            except TypeError:
                provided = self._portfolio_state_provider()

            if isinstance(provided, PortfolioState):
                raw_eq = (
                    float(provided.equity_usd)
                    if (provided.equity_usd is not None and float(provided.equity_usd) > 0.0)
                    else cap_float
                )
                gross_exp = float(provided.gross_exposure_usd)
                raw_venue_cash = provided.venue_cash_usd.get("alpaca")
                return {
                    "equity_usd": raw_eq,
                    "cash_usd": (
                        max(0.0, float(raw_venue_cash))
                        if raw_venue_cash is not None
                        else max(0.0, raw_eq - gross_exp)
                    ),
                    "open_buy_usd": float(provided.venue_open_buy_usd.get("alpaca", 0.0)),
                    "gross_exposure_usd": gross_exp,
                    "symbol_exposure_usd": dict(provided.symbol_exposure_usd),
                    "sector_exposure_usd": dict(provided.sector_exposure_usd),
                }
            if isinstance(provided, dict):
                exposure.update(provided)
                if provided.get("cash_usd") is not None:
                    exposure["cash_usd"] = max(0.0, float(provided["cash_usd"]))
                else:
                    eff_eq = float(exposure.get("equity_usd") or cap_float)
                    eff_gross = float(exposure.get("gross_exposure_usd") or 0.0)
                    exposure["cash_usd"] = max(0.0, eff_eq - eff_gross)
                return exposure

        if not self._dsn:
            return exposure

        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor

            symbol_exposure: Dict[str, float] = {}
            sector_exposure: Dict[str, float] = {}
            unresolved_symbols: List[str] = []
            explicit_sector_by_symbol: Dict[str, str] = {}
            open_buy_usd = 0.0
            equity_usd = cap_float
            cash_usd = cap_float
            has_explicit_cash = False

            with psycopg2.connect(self._dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT
                            to_regclass('public.trading_positions') AS tp_reg,
                            to_regclass('public.positions') AS pos_reg,
                            to_regclass('public.orders') AS ord_reg,
                            to_regclass('public.trading_accounts') AS acct_reg,
                            to_regclass('public.trading_plan_items') AS tpi_reg,
                            to_regclass('public.trading_plans') AS tpl_reg
                        """
                    )
                    reg = cur.fetchone() or {}

                    if reg.get("acct_reg") is not None:
                        cur.execute("SELECT * FROM trading_accounts ORDER BY updated_at DESC NULLS LAST LIMIT 1")
                        acct_row = cur.fetchone()
                        if acct_row:
                            eq_val = next(
                                (
                                    acct_row.get(k)
                                    for k in ("equity", "equity_usd")
                                    if acct_row.get(k) is not None
                                ),
                                None,
                            )
                            cash_val = next(
                                (
                                    acct_row.get(k)
                                    for k in ("cash_balance", "cash", "cash_usd")
                                    if acct_row.get(k) is not None
                                ),
                                None,
                            )
                            if eq_val is not None and float(eq_val) > 0.0:
                                equity_usd = float(eq_val)
                            if cash_val is not None:
                                cash_usd = max(0.0, float(cash_val))
                                has_explicit_cash = True
                            else:
                                cash_usd = equity_usd

                    position_rows: List[Dict[str, Any]] = []
                    if reg.get("tp_reg") is not None:
                        cur.execute("SELECT * FROM trading_positions")
                        position_rows.extend(cur.fetchall() or [])
                    if reg.get("pos_reg") is not None:
                        cur.execute("SELECT * FROM positions")
                        position_rows.extend(cur.fetchall() or [])

                    seen_pos_symbols: set[str] = set()
                    for row in position_rows:
                        status_str = str(row.get("status") or "OPEN").strip().upper()
                        if status_str in ("CLOSED", "FLAT", "CANCELED", "CANCELLED", "EXPIRED"):
                            continue
                        sym = str(row.get("symbol") or "").strip().upper()
                        if not sym or sym in seen_pos_symbols:
                            continue
                        qty_raw = (
                            row.get("qty")
                            if row.get("qty") is not None
                            else row.get("quantity")
                        )
                        mv_raw = (
                            row.get("market_value_usd")
                            if row.get("market_value_usd") is not None
                            else (
                                row.get("market_value")
                                if row.get("market_value") is not None
                                else row.get("notional_usd")
                            )
                        )
                        px_raw = (
                            row.get("current_price")
                            if row.get("current_price") is not None
                            else (
                                row.get("avg_entry_price")
                                if row.get("avg_entry_price") is not None
                                else (
                                    row.get("entry_price")
                                    if row.get("entry_price") is not None
                                    else row.get("cost_basis")
                                )
                            )
                        )
                        qty_f = float(qty_raw or 0.0)
                        if mv_raw is not None and abs(float(mv_raw)) > 0.0:
                            if qty_raw is not None and abs(qty_f) <= 1e-9:
                                continue
                            mv_f = abs(float(mv_raw))
                        else:
                            if abs(qty_f) <= 1e-9:
                                continue
                            px_f = max(0.0, float(px_raw or 0.0))
                            mv_f = abs(qty_f) * px_f if px_f > 0.0 else abs(qty_f)

                        if mv_f <= 0.0:
                            continue
                        seen_pos_symbols.add(sym)
                        symbol_exposure[sym] = symbol_exposure.get(sym, 0.0) + mv_f
                        row_sec = str(
                            row.get("sector") or row.get("gics_sector") or ""
                        ).strip().upper()
                        if row_sec and row_sec not in ("UNCLASSIFIED", "UNKNOWN"):
                            explicit_sector_by_symbol[sym] = row_sec
                        else:
                            unresolved_symbols.append(sym)

                    if reg.get("tpi_reg") is not None and reg.get("tpl_reg") is not None:
                        cur.execute(
                            """
                            SELECT i.*
                            FROM trading_plan_items i
                            JOIN trading_plans p ON p.plan_id = i.plan_id
                            WHERE COALESCE(i.filled_qty, 0) > 0
                              AND UPPER(COALESCE(i.status, '')) NOT IN ('CLOSED', 'EXPIRED', 'CANCELED', 'CANCELLED', 'SUPERSEDED')
                            """
                        )
                        for item_row in cur.fetchall() or []:
                            sym = str(item_row.get("symbol") or "").strip().upper()
                            if not sym or sym in seen_pos_symbols:
                                continue
                            f_qty = abs(float(item_row.get("filled_qty") or 0.0))
                            if f_qty <= 1e-9:
                                continue
                            t_qty = float(item_row.get("target_qty") or 0.0)
                            t_notional = float(item_row.get("target_notional_usd") or 0.0)
                            ref_px = (t_notional / t_qty) if t_qty > 0.0 else 0.0
                            mv_f = f_qty * ref_px if ref_px > 0.0 else float(item_row.get("reserved_notional_usd") or 0.0)
                            if mv_f <= 0.0:
                                continue
                            seen_pos_symbols.add(sym)
                            symbol_exposure[sym] = symbol_exposure.get(sym, 0.0) + mv_f
                            trace_obj = item_row.get("sizing_trace")
                            if isinstance(trace_obj, str):
                                try:
                                    trace_obj = json.loads(trace_obj)
                                except Exception:
                                    trace_obj = {}
                            trace_sec = (
                                str((trace_obj or {}).get("sector") or "").strip().upper()
                                if isinstance(trace_obj, dict)
                                else ""
                            )
                            if trace_sec and trace_sec not in ("UNCLASSIFIED", "UNKNOWN"):
                                explicit_sector_by_symbol[sym] = trace_sec
                            else:
                                unresolved_symbols.append(sym)

                    if reg.get("ord_reg") is not None:
                        cur.execute("SELECT * FROM orders")
                        for ord_row in cur.fetchall() or []:
                            ord_status = str(ord_row.get("status") or "").strip().upper()
                            if ord_status not in (
                                "SUBMITTED",
                                "ACCEPTED",
                                "PARTIALLY_FILLED",
                                "NEW",
                                "OPEN",
                                "PENDING",
                                "WORKING",
                            ):
                                continue
                            if bool(ord_row.get("is_reduce_only") or ord_row.get("reduce_only")):
                                continue
                            side_str = str(
                                ord_row.get("side") or ord_row.get("action") or ""
                            ).strip().upper()
                            if side_str not in ("BUY", "LONG"):
                                continue
                            o_qty = float(
                                ord_row.get("qty")
                                if ord_row.get("qty") is not None
                                else (ord_row.get("quantity") or 0.0)
                            )
                            f_qty = float(ord_row.get("filled_qty") or 0.0)
                            rem_qty = max(0.0, o_qty - f_qty)
                            o_px = float(
                                ord_row.get("limit_price")
                                or ord_row.get("price")
                                or ord_row.get("avg_fill_price")
                                or 0.0
                            )
                            ord_notional = (
                                rem_qty * o_px
                                if (rem_qty > 0.0 and o_px > 0.0)
                                else float(ord_row.get("notional_usd") or 0.0)
                            )
                            if ord_notional > 0.0:
                                open_buy_usd += ord_notional

            resolved_sector_map = (
                get_gics_sector_map(unresolved_symbols, dsn=self._dsn)
                if unresolved_symbols
                else {}
            )
            for sym, sym_mv in symbol_exposure.items():
                sec = explicit_sector_by_symbol.get(sym) or resolved_sector_map.get(
                    sym, "UNCLASSIFIED"
                )
                sector_exposure[sec] = sector_exposure.get(sec, 0.0) + sym_mv

            gross_exposure_usd = sum(symbol_exposure.values())
            if not has_explicit_cash:
                cash_usd = max(0.0, equity_usd - gross_exposure_usd)
            elif gross_exposure_usd > 0.0 and (cash_usd + gross_exposure_usd) > (equity_usd + 1.0):
                cash_usd = max(0.0, min(cash_usd, equity_usd - gross_exposure_usd))
            exposure.update(
                {
                    "equity_usd": equity_usd,
                    "cash_usd": cash_usd,
                    "open_buy_usd": open_buy_usd,
                    "gross_exposure_usd": gross_exposure_usd,
                    "symbol_exposure_usd": symbol_exposure,
                    "sector_exposure_usd": sector_exposure,
                }
            )
        except Exception as exc:
            logger.error(
                "Failed to load current portfolio exposure from PostgreSQL (trading_date=%s, market=%s): %s",
                trading_date,
                market,
                exc,
                exc_info=True,
            )
            raise

        return exposure

    def _load_bars_for_symbol(
        self,
        symbol: str,
        bar_timeframe: str,
        lookback_days: int = 60,
        start_ns: Optional[int] = None,
        end_ns: Optional[int] = None,
    ) -> List[Bar]:
        if self._bar_provider is not None:
            return list(self._bar_provider(symbol, bar_timeframe, lookback_days))
        if self._datainfra_config is None:
            raise ValueError(
                "Either DataInfraConfig or bar_provider must be configured on NautilusMcpServer"
            )
        catalog = DataInfraCatalog(self._datainfra_config)
        try:
            catalog.register_equity(symbol, venue=self._venue_name, lot_size=1)
            bars = sorted(
                catalog.query_bars(
                    [symbol],
                    start_ts_ns=start_ns,
                    end_ts_ns=end_ns,
                    timeframe=bar_timeframe,
                    adjustment="adjusted",
                    market_session="REG",
                    venue=self._venue_name,
                ),
                key=lambda b: b.ts_init,
            )
        finally:
            del catalog
        if start_ns is None and end_ns is None and lookback_days > 0:
            tf_norm = str(bar_timeframe or "15m").strip().lower()
            bars_per_day = int(
                math.ceil(BARS_PER_TRADING_YEAR.get(tf_norm, 252.0 * 26.0) / 252.0)
            )
            max_bars = max(120, int(lookback_days) * max(1, bars_per_day))
            if len(bars) > max_bars:
                bars = bars[-max_bars:]
        return bars

    def _resolve_bars_and_equities_for_contract(
        self,
        strategy_config: ArchetypeStrategyConfig,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        lookback_days: int = 60,
        execution_spec: Optional[ExecutionSpec] = None,
    ) -> Tuple[List[Bar], Optional[List[Equity]]]:
        start_ns = _parse_date_to_ns(start_date, end_of_day=False)
        end_ns = _parse_date_to_ns(end_date, end_of_day=True)

        primary_sym = strategy_config.instrument_id.split(".", 1)[0].strip().upper()
        primary_bars = list(
            self._load_bars_for_symbol(
                symbol=primary_sym,
                bar_timeframe=strategy_config.bar_timeframe,
                lookback_days=int(lookback_days),
                start_ns=start_ns,
                end_ns=end_ns,
            )
        )
        dedup_bars: Dict[Tuple[str, int], Bar] = {}
        for b in primary_bars:
            dedup_bars[(str(b.bar_type), int(b.ts_init))] = b

        tf_norm = str(strategy_config.bar_timeframe or "15m").strip().lower()
        needs_1m_policy = execution_spec is not None and execution_spec.trigger_policy in (
            TriggerPolicy.HYBRID_15M_1M_BREAKOUT,
            TriggerPolicy.INTRADAY_AVWAP_PULLBACK,
        )
        if tf_norm != "1m" and needs_1m_policy:
            if primary_bars:
                step_primary_ns = max(
                    (_bar_step_ns(b) for b in primary_bars),
                    default=900_000_000_000,
                )
                if step_primary_ns <= 0:
                    step_primary_ns = 900_000_000_000
                min_ts_primary = min(int(b.ts_init) for b in primary_bars)
                max_ts_primary = max(int(b.ts_init) for b in primary_bars)
                exec_start_ns = (
                    start_ns
                    if start_ns is not None
                    else max(0, min_ts_primary - step_primary_ns)
                )
                exec_end_ns = (
                    end_ns
                    if end_ns is not None
                    else (max_ts_primary + step_primary_ns)
                )
            else:
                step_primary_ns = 900_000_000_000
                min_ts_primary = 0
                max_ts_primary = 0
                exec_start_ns = start_ns
                exec_end_ns = end_ns
            try:
                exec_bars = self._load_bars_for_symbol(
                    symbol=primary_sym,
                    bar_timeframe="1m",
                    lookback_days=int(lookback_days),
                    start_ns=exec_start_ns,
                    end_ns=exec_end_ns,
                )
            except KeyError:
                if self._bar_provider is None:
                    raise
                exec_bars = []
            minute_bars = [b for b in (exec_bars or ()) if _is_minute_bar(b)]
            if primary_bars and minute_bars:
                min_ts_1m = min(int(b.ts_init) for b in minute_bars)
                max_ts_1m = max(int(b.ts_init) for b in minute_bars)
                if (
                    min_ts_1m <= min_ts_primary + step_primary_ns
                    and max_ts_1m >= max_ts_primary - step_primary_ns
                ):
                    for b in minute_bars:
                        dedup_bars[(str(b.bar_type), int(b.ts_init))] = b
                else:
                    logger.warning(
                        "Skipping partial 1m execution bars for %s: 1m range [%s, %s] "
                        "does not cover %s range [%s, %s] (step_ns=%s)",
                        primary_sym,
                        min_ts_1m,
                        max_ts_1m,
                        strategy_config.bar_timeframe,
                        min_ts_primary,
                        max_ts_primary,
                        step_primary_ns,
                    )
            elif minute_bars:
                for b in minute_bars:
                    dedup_bars[(str(b.bar_type), int(b.ts_init))] = b

        all_bars = _sort_bars_deterministic(list(dedup_bars.values()))
        equities: List[Equity] = []
        if self._equity_provider is not None:
            equities.append(self._equity_provider(primary_sym))
        elif self._datainfra_config is not None:
            catalog = DataInfraCatalog(self._datainfra_config)
            try:
                equities.append(
                    catalog.register_equity(primary_sym, venue=self._venue_name, lot_size=1)
                )
            finally:
                del catalog

        return all_bars, (equities if equities else None)

    @staticmethod
    def _normalize_stop_for_sizing(
        *,
        entry_price: float,
        stop_loss_price: float,
        side: PlanSide,
    ) -> float:
        if side == PlanSide.LONG:
            return stop_loss_price
        if stop_loss_price <= entry_price:
            return entry_price
        dist = stop_loss_price - entry_price
        return entry_price - dist





def create_production_mcp_server(dsn: Optional[str] = None) -> NautilusMcpServer:
    """Instantiates `NautilusMcpServer` backed by PostgreSQL and `DataInfraConfig`."""
    resolved_dsn = resolve_database_dsn(dsn)
    datainfra_cfg = build_datainfra_config_from_env()
    capital_basis = Decimal(os.environ.get("CAPITAL_BASIS_USD", "100000.00"))
    return NautilusMcpServer(
        dsn=resolved_dsn,
        datainfra_config=datainfra_cfg,
        capital_basis_usd=capital_basis,
        venue_name=os.environ.get("DEFAULT_VENUE", "ALPACA"),
    )
