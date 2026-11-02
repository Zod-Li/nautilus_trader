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

import hashlib
import hmac
import json
import logging
import math
import os
import statistics
import threading
import uuid
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from uuid import UUID
from zoneinfo import ZoneInfo

from evolution_engine.capital.sizing import (
    PortfolioState,
    SizingDecision,
    SizingRequest,
    size_position,
)
from evolution_engine.config_center.schema import SystemConfig
from evolution_engine.db.dsn import resolve_database_dsn
from market_data_service.pipeline.us_equity_universe import get_gics_sector_map
from nautilus_trader.quant.contracts.models import (
    ArchetypeStrategyConfig,
    MarketableLimitCollar,
    PlanItemContract,
    PlanRiskEnvelope,
    PlanSide,
    StrategyArchetypeId,
    compute_intraday_entry_valid_until,
)
from nautilus_trader.quant.contracts.repository import PlanContractRepository
from nautilus_trader.quant.governance.plan_gate import build_stat_arb_secondary_plan_item
from nautilus_trader.quant.mcp.schemas import (
    ALLOWED_MCP_TOOL_NAMES,
    McpPlanLockRejectedError,
    McpSecurityViolationError,
    assert_no_code_injection,
    get_mcp_tool_input_schemas,
)
from nautilus_trader.quant.nodes.backtest_runner import (
    IsomorphicBacktestMetrics,
    IsomorphicBacktestRunner,
    StressFalsificationReport,
    StressFalsifier,
)
from nautilus_trader.quant.strategies.empirical import rolling_atr
from nautilus_trader.quant.strategies.registry import list_strategy_archetype_schemas
from nautilus_trader.adapters.datainfra import DataInfraCatalog, DataInfraConfig
from nautilus_trader.model import Bar, Equity

logger = logging.getLogger(__name__)

REQUIRED_ATTESTATION_KEYS: Tuple[str, ...] = (
    "macro_catalyst_summary",
    "quant_research_summary",
    "adversarial_risk_summary",
    "portfolio_chair_decision",
)


def compute_strategy_fingerprint(
    symbol: str,
    strategy_config: ArchetypeStrategyConfig,
) -> str:
    """Computes a canonical SHA-256 fingerprint for `(symbol, strategy_config)`."""
    canonical = {
        "symbol": symbol.strip().upper(),
        "strategy_config": strategy_config.to_dict(),
    }
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


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
    ) -> None:
        self._dsn = resolve_database_dsn(dsn) if dsn is not None else None
        self._datainfra_config = datainfra_config
        self._venue_name = str(venue_name or "ALPACA").strip().upper()
        self._capital_basis_usd = Decimal(str(capital_basis_usd))
        self._system_config = system_config or SystemConfig()
        self._runner = backtest_runner or IsomorphicBacktestRunner(
            dsn=None,
            datainfra_config=self._datainfra_config,
            starting_capital_usd=self._capital_basis_usd,
            effective_trials=2,
            venue_name=self._venue_name,
        )
        self._falsifier = stress_falsifier or StressFalsifier(runner=self._runner)
        self._bar_provider = bar_provider
        self._equity_provider = equity_provider
        self._auth_token = str(auth_token).strip() if auth_token is not None else None
        self._portfolio_state_provider = portfolio_state_provider

        self._lock = threading.Lock()
        self._cumulative_trials: int = 0
        self._verified_backtests: Dict[str, IsomorphicBacktestMetrics] = {}
        self._verified_falsifications: Dict[str, Tuple[str, StressFalsificationReport]] = {}

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

            summaries[sym] = {
                "symbol": sym,
                "bar_timeframe": bar_timeframe,
                "bar_count": len(sorted_bars),
                "status": "OK",
                "last_close": round(last_close, 4),
                "return_volatility": round(ret_vol, 6),
                "current_atr": round(current_atr, 4),
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
        walk_forward: bool = False,
        lookback_days: int = 60,
        effective_trials: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Runs `IsomorphicBacktestRunner` for `(symbol, archetype_id, params)` and returns metrics."""
        sym = str(symbol).strip().upper()
        strat_cfg = self._build_strategy_config(sym, archetype_id, params)
        bars, equities = self._resolve_bars_and_equities_for_contract(
            strat_cfg,
            start_date=start_date,
            end_date=end_date,
            lookback_days=int(lookback_days),
        )
        env = self._build_risk_envelope(
            sym,
            risk_envelope,
            bars=bars,
            bar_timeframe=strat_cfg.bar_timeframe,
        )
        bt_env = self._align_backtest_envelope_horizon(env, bars)
        plan_item = self._make_plan_item_contract(
            symbol=sym,
            strategy_config=strat_cfg,
            risk_envelope=bt_env,
        )
        eff_trials = self._resolve_effective_trials(
            increment=(2 if walk_forward else 1),
            explicit_trials=effective_trials,
        )

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

        fp = compute_strategy_fingerprint(sym, strat_cfg)
        backtest_id = f"bt-{fp[:16]}"
        with self._lock:
            self._verified_backtests[fp] = metrics

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
        lookback_days: int = 60,
        effective_trials: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Executes `StressFalsifier` (`2x` friction, `+1 bar` delay, tail gap shock) on candidate."""
        sym = str(symbol).strip().upper()
        strat_cfg = self._build_strategy_config(sym, archetype_id, params)
        bars, equities = self._resolve_bars_and_equities_for_contract(
            strat_cfg,
            lookback_days=int(lookback_days),
        )
        env = self._build_risk_envelope(
            sym,
            risk_envelope,
            bars=bars,
            bar_timeframe=strat_cfg.bar_timeframe,
        )
        bt_env = self._align_backtest_envelope_horizon(env, bars)
        plan_item = self._make_plan_item_contract(
            symbol=sym,
            strategy_config=strat_cfg,
            risk_envelope=bt_env,
        )
        eff_trials = self._resolve_effective_trials(
            increment=1,
            explicit_trials=effective_trials,
        )

        report = self._falsifier.run_falsification(
            plan_item,
            bars=bars,
            equities=equities,
            effective_trials=eff_trials,
        )
        fp = compute_strategy_fingerprint(sym, strat_cfg)
        token = f"falsified-{fp[:16]}-{uuid.uuid4().hex[:8]}" if report.passed else ""

        with self._lock:
            self._verified_backtests[fp] = report.baseline_metrics
            self._verified_falsifications[fp] = (token, report)

        return {
            "symbol": sym,
            "archetype_id": strat_cfg.archetype_id.value,
            "strategy_fingerprint": fp,
            "passed": report.passed,
            "falsification_token": token,
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
        if not validated_items and not all_candidates_vetoed:
            raise McpPlanLockRejectedError("validated_items cannot be empty")

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
        equity_usd = float(existing_exposure.get("equity_usd") or cap_float)
        cash_usd = float(existing_exposure.get("cash_usd") or equity_usd)
        open_buy_usd = float(existing_exposure.get("open_buy_usd") or 0.0)
        gross_exposure_usd = float(existing_exposure.get("gross_exposure_usd") or 0.0)
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

        for raw_item in validated_items:
            sym = str(raw_item["symbol"]).strip().upper()
            arch_id = str(raw_item["archetype_id"]).strip()
            params = dict(raw_item["params"])
            raw_env = dict(raw_item["risk_envelope"])
            conviction = float(raw_item.get("conviction", 0.85))
            raw_sector = str(raw_item.get("sector") or "").strip().upper()
            if not raw_sector:
                raw_sector = get_gics_sector_map([sym], dsn=self._dsn).get(sym, "UNCLASSIFIED")
            if not raw_sector or raw_sector in ("UNCLASSIFIED", "UNKNOWN"):
                raise McpPlanLockRejectedError(
                    f"Cannot lock plan for {sym} with unverified or missing GICS sector: {raw_sector!r}"
                )
            sector = raw_sector

            strat_cfg = self._build_strategy_config(sym, arch_id, params)
            req_env = self._build_risk_envelope(
                sym,
                raw_env,
                trading_date=resolved_date,
                bar_timeframe=strat_cfg.bar_timeframe,
            )
            fp = compute_strategy_fingerprint(sym, strat_cfg)

            with self._lock:
                falsification_entry = self._verified_falsifications.get(fp)

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

            approved_qty_cap = min(
                req_env.approved_qty_cap,
                req_env.target_qty,
                Decimal(str(int(decision.qty))),
            )
            if approved_qty_cap <= Decimal("0"):
                raise McpPlanLockRejectedError(
                    f"Approved quantity cap for {sym} is non-positive: {approved_qty_cap}"
                )
            target_qty = min(req_env.target_qty, approved_qty_cap)
            raw_sized_notional = min(
                float(req_env.target_notional_usd),
                float(decision.notional_usd),
                float(target_qty) * entry_ref,
            )
            approved_notional = max(
                Decimal("100.00"),
                Decimal(f"{raw_sized_notional:.2f}"),
            )
            approved_risk_budget = min(
                req_env.max_risk_budget_usd,
                approved_notional,
                Decimal(f"{max(decision.risk_budget_usd, decision.risk_usd, 1.0):.2f}"),
            )

            sized_envelope = PlanRiskEnvelope(
                side=req_env.side,
                target_qty=target_qty,
                approved_qty_cap=approved_qty_cap,
                target_notional_usd=approved_notional,
                max_risk_budget_usd=approved_risk_budget,
                initial_stop_price=req_env.initial_stop_price,
                take_profit_price=req_env.take_profit_price,
                collar=req_env.collar,
                valid_until=req_env.valid_until,
            )
            enriched_trace: Dict[str, Any] = dict(decision.trace or {})
            enriched_trace.update(
                {
                    "sector": sector,
                    "conviction": round(conviction, 4),
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
            if (
                strat_cfg.archetype_id == StrategyArchetypeId.STAT_ARB_PAIRS
                and strat_cfg.secondary_instrument_id
            ):
                sec_inst = str(strat_cfg.secondary_instrument_id).strip()
                sec_sym = sec_inst.split(".", 1)[0].strip().upper()
                sec_side = (
                    PlanSide.SHORT if sized_envelope.side == PlanSide.LONG else PlanSide.LONG
                )
                enriched_trace.setdefault("secondary_instrument_id", sec_inst)
                enriched_trace.setdefault("secondary_symbol", sec_sym)
                enriched_trace.setdefault("secondary_side", sec_side.value)
                enriched_trace.setdefault("register_secondary_hedge_envelope", True)
            item_contract = self._make_plan_item_contract(
                symbol=sym,
                strategy_config=strat_cfg,
                risk_envelope=sized_envelope,
                plan_id=resolved_plan_uuid,
                sizing_trace=enriched_trace,
            )
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

        resolved_equity_dec = Decimal(f"{equity_usd:.2f}")
        carry_over_decisions = list(committee_attestation.get("carry_over_decisions") or [])
        zero_item_reason_code = (
            str(
                committee_attestation.get("zero_item_reason_code")
                or ("ALL_CANDIDATES_VETOED" if all_candidates_vetoed else "")
            ).strip()
            or None
        )

        if self._dsn is not None:
            db_items = list(locked_items)
            existing_db_syms = {it.symbol for it in db_items}
            for item_contract in locked_items:
                sec_item = build_stat_arb_secondary_plan_item(item_contract)
                if sec_item is not None and sec_item.symbol not in existing_db_syms:
                    db_items.append(sec_item)
                    existing_db_syms.add(sec_item.symbol)
            repo = PlanContractRepository(dsn=self._dsn)
            resolved_plan_uuid = repo.create_and_lock_trading_plan(
                trading_date=resolved_date,
                items=db_items,
                total_equity_usd=resolved_equity_dec,
                market=str(market or "US_EQUITY").strip().upper(),
                summary=str(committee_attestation.get("portfolio_chair_decision", "LOCKED")),
                plan_id=resolved_plan_uuid,
                actor="nautilus_mcp:epistemic_committee",
                carry_over_decisions=carry_over_decisions,
                zero_item_reason_code=zero_item_reason_code,
            )

        return {
            "status": "LOCKED",
            "plan_status": "LOCKED_NO_TRADE" if not locked_items else "LOCKED",
            "all_candidates_vetoed": all_candidates_vetoed,
            "zero_item_reason_code": zero_item_reason_code,
            "carry_over_decisions": carry_over_decisions,
            "plan_id": str(resolved_plan_uuid),
            "market": str(market or "US_EQUITY").strip().upper(),
            "trading_date": resolved_date.isoformat(),
            "capital_basis_usd": str(resolved_equity_dec),
            "locked_item_count": len(locked_items),
            "items": [
                {
                    "item_id": str(item.item_id),
                    "plan_id": str(item.plan_id),
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
                    resp_obj = mcp_server.handle_jsonrpc_request(req_obj)
                    status_code = 200
                except Exception as exc:
                    resp_obj = {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {"code": -32700, "message": str(exc)},
                    }
                    status_code = 400
                payload = json.dumps(resp_obj).encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        return ThreadingHTTPServer((host, port), _McpHttpHandler)

    # =========================================================================
    # Internal Helpers
    # =========================================================================

    def _build_strategy_config(
        self,
        symbol: str,
        archetype_id: str,
        params: Dict[str, Any],
    ) -> ArchetypeStrategyConfig:
        inst_id = str(params.get("instrument_id") or f"{symbol}.{self._venue_name}").strip()
        sec_id = params.get("secondary_instrument_id")
        return ArchetypeStrategyConfig(
            archetype_id=StrategyArchetypeId(str(archetype_id).strip()),
            instrument_id=inst_id,
            bar_timeframe=str(params.get("bar_timeframe", "15m")),
            lookback_window=int(params.get("lookback_window", 20)),
            fast_window=int(params.get("fast_window", 5)),
            slow_window=int(params.get("slow_window", 15)),
            entry_quantile_threshold=Decimal(str(params.get("entry_quantile_threshold", "0.80"))),
            exit_quantile_threshold=Decimal(str(params.get("exit_quantile_threshold", "0.35"))),
            atr_stop_multiplier=Decimal(str(params.get("atr_stop_multiplier", "2.0"))),
            secondary_instrument_id=(str(sec_id).strip() if sec_id is not None else None),
        )

    @staticmethod
    def _align_backtest_envelope_horizon(
        envelope: PlanRiskEnvelope,
        bars: Optional[Sequence[Bar]],
        *,
        primary_symbol: Optional[str] = None,
        atr_stop_multiplier: float = 2.0,
    ) -> PlanRiskEnvelope:
        """Aligns `valid_until` and historical stop floor across calibration/falsification bar windows
        when a candidate's intraday entry window (`entry_ttl_bars * bar_timeframe`) or end-of-window
        stop floor differs from the historical bar series used by `IsomorphicBacktestRunner`.
        """
        if not bars:
            return envelope
        max_ts_ns = max(int(b.ts_init) for b in bars)
        last_bar_dt = datetime.fromtimestamp(max_ts_ns / 1_000_000_000.0, tz=timezone.utc)
        new_valid_until = (
            last_bar_dt + timedelta(minutes=1)
            if last_bar_dt > envelope.valid_until
            else envelope.valid_until
        )
        sym_bars = [
            b
            for b in bars
            if primary_symbol is None
            or str(b.bar_type.instrument_id).upper().startswith(primary_symbol.upper())
        ] or list(bars)
        lows = [float(b.low) for b in sym_bars]
        highs = [float(b.high) for b in sym_bars]
        ranges = [max(0.01, h - lo) for h, lo in zip(highs, lows)]
        max_range = max(ranges) if ranges else 1.0
        stop_buf = max(max_range * (float(atr_stop_multiplier) + 1.5), min(lows) * 0.05)
        new_stop = envelope.initial_stop_price
        if envelope.side == PlanSide.LONG:
            hist_floor = Decimal(f"{max(0.01, min(lows) - stop_buf):.2f}")
            if hist_floor < new_stop:
                new_stop = hist_floor
        else:
            hist_ceil = Decimal(f"{max(highs) + stop_buf:.2f}")
            if hist_ceil > new_stop:
                new_stop = hist_ceil
        return replace(envelope, valid_until=new_valid_until, initial_stop_price=new_stop)

    def _build_risk_envelope(
        self,
        symbol: str,
        raw_envelope: Optional[Dict[str, Any]],
        bars: Optional[Sequence[Bar]] = None,
        *,
        trading_date: Optional[date] = None,
        bar_timeframe: str = "15m",
    ) -> PlanRiskEnvelope:
        risk_cfg = self._system_config.risk
        exec_cfg = self._system_config.execution
        default_collar_pct = Decimal(str(exec_cfg.limit_collar_pct))
        default_collar_atr = Decimal(str(exec_cfg.limit_collar_atr))
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
            default_valid_until = compute_intraday_entry_valid_until(
                trading_date=inferred_date,
                bar_timeframe=bar_timeframe,
                entry_ttl_bars=ttl_bars,
                anchor_ts_utc=last_bar_dt,
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
                if emp_last_px <= 0.0 or emp_atr <= 0.0:
                    raise ValueError(
                        f"risk_envelope for {symbol} missing 'initial_stop_price' and no empirical ATR available"
                    )
                initial_stop = Decimal(
                    f"{max(0.01, emp_last_px - emp_atr if side == PlanSide.LONG else emp_last_px + emp_atr):.2f}"
                )
            else:
                initial_stop = Decimal(str(raw_stop))

            tp_raw = raw_envelope.get("take_profit_price")
            take_profit = Decimal(str(tp_raw)) if tp_raw is not None else None
            collar_raw = raw_envelope.get("collar")
            if collar_raw is None:
                collar = MarketableLimitCollar(
                    limit_collar_pct=default_collar_pct,
                    limit_collar_atr=default_collar_atr,
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
            )

        if emp_last_px <= 0.0:
            raise ValueError(
                f"Cannot derive empirical PlanRiskEnvelope for {symbol} without raw_envelope or historical bars"
            )

        stop_dist = max(emp_atr, emp_last_px * float(risk_per_trade_frac))
        stop_px = max(0.01, emp_last_px - stop_dist)
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
            initial_stop_price=Decimal(f"{stop_px:.2f}"),
            take_profit_price=Decimal(f"{tp_px:.2f}"),
            collar=MarketableLimitCollar(
                limit_collar_pct=default_collar_pct,
                limit_collar_atr=default_collar_atr,
            ),
            valid_until=default_valid_until,
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
                        cur.execute("SELECT * FROM factor_zoo_registry LIMIT 500")
                        zoo_rows = cur.fetchall() or []
                        for r in zoo_rows:
                            for col in (
                                "effective_trials",
                                "total_trials",
                                "trial_count",
                                "eval_count",
                                "total_experiments",
                            ):
                                if r.get(col) is not None:
                                    zoo_trials += max(0, int(r[col]))
                                    break
                            else:
                                zoo_trials += 1

                    if reg_row.get("af_reg") is not None:
                        cur.execute("SELECT * FROM alpha_factors LIMIT 1000")
                        af_rows = cur.fetchall() or []
                        for r in af_rows:
                            for col in ("eval_count", "trial_count", "effective_trials"):
                                if r.get(col) is not None and int(r[col]) > 0:
                                    af_trials += int(r[col])
                                    break
                            else:
                                af_trials += 1

                    if reg_row.get("amr_reg") is not None:
                        cur.execute("SELECT * FROM alpha_mining_runs LIMIT 500")
                        amr_rows = cur.fetchall() or []
                        for r in amr_rows:
                            cand_gen = r.get("candidates_generated") or r.get("total_trials")
                            if cand_gen is not None and int(cand_gen) > 0:
                                mining_trials += int(cand_gen)
                            else:
                                mining_trials += 1

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
                return {
                    "equity_usd": float(provided.equity_usd or cap_float),
                    "cash_usd": float(
                        provided.venue_cash_usd.get("alpaca", provided.equity_usd or cap_float)
                    ),
                    "open_buy_usd": float(provided.venue_open_buy_usd.get("alpaca", 0.0)),
                    "gross_exposure_usd": float(provided.gross_exposure_usd),
                    "symbol_exposure_usd": dict(provided.symbol_exposure_usd),
                    "sector_exposure_usd": dict(provided.sector_exposure_usd),
                }
            if isinstance(provided, dict):
                exposure.update(provided)
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
                            eq_val = acct_row.get("equity") or acct_row.get("equity_usd")
                            cash_val = (
                                acct_row.get("cash_balance")
                                or acct_row.get("cash")
                                or acct_row.get("cash_usd")
                            )
                            if eq_val is not None and float(eq_val) > 0.0:
                                equity_usd = float(eq_val)
                                cash_usd = float(cash_val) if (cash_val is not None and float(cash_val) > 0.0) else equity_usd

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
        if start_ns is None and end_ns is None and lookback_days > 0:
            max_bars = max(120, int(lookback_days) * 26)
            if len(bars) > max_bars:
                bars = bars[-max_bars:]
        return bars

    def _resolve_bars_and_equities_for_contract(
        self,
        strategy_config: ArchetypeStrategyConfig,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        lookback_days: int = 60,
    ) -> Tuple[List[Bar], Optional[List[Equity]]]:
        start_ns = _parse_date_to_ns(start_date, end_of_day=False)
        end_ns = _parse_date_to_ns(end_date, end_of_day=True)

        primary_sym = strategy_config.instrument_id.split(".", 1)[0].strip().upper()
        symbols = [primary_sym]
        if strategy_config.secondary_instrument_id:
            sec_sym = strategy_config.secondary_instrument_id.split(".", 1)[0].strip().upper()
            if sec_sym and sec_sym not in symbols:
                symbols.append(sec_sym)

        all_bars: List[Bar] = []
        equities: List[Equity] = []
        for sym in symbols:
            all_bars.extend(
                self._load_bars_for_symbol(
                    symbol=sym,
                    bar_timeframe=strategy_config.bar_timeframe,
                    lookback_days=int(lookback_days),
                    start_ns=start_ns,
                    end_ns=end_ns,
                )
            )
            if self._equity_provider is not None:
                equities.append(self._equity_provider(sym))
            elif self._datainfra_config is not None:
                catalog = DataInfraCatalog(self._datainfra_config)
                equities.append(
                    catalog.register_equity(sym, venue=self._venue_name, lot_size=1)
                )

        all_bars.sort(key=lambda b: b.ts_init)
        return all_bars, (equities if equities else None)

    @staticmethod
    def _normalize_stop_for_sizing(
        *,
        entry_price: float,
        stop_loss_price: float,
        side: PlanSide,
    ) -> float:
        if side == PlanSide.LONG:
            return min(entry_price * 0.99, max(0.01, stop_loss_price))
        dist = max(0.01 * entry_price, stop_loss_price - entry_price)
        return max(0.01, entry_price - dist)
