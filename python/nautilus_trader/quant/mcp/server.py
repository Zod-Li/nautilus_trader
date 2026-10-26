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
import json
import statistics
import threading
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from uuid import UUID

from evolution_engine.capital.sizing import (
    PortfolioState,
    SizingDecision,
    SizingRequest,
    size_position,
)
from evolution_engine.config_center.schema import SystemConfig
from evolution_engine.db.dsn import resolve_database_dsn
from nautilus_trader.quant.contracts.models import (
    ArchetypeStrategyConfig,
    MarketableLimitCollar,
    PlanItemContract,
    PlanRiskEnvelope,
    PlanSide,
    StrategyArchetypeId,
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
    IsomorphicBacktestMetrics,
    IsomorphicBacktestRunner,
    StressFalsificationReport,
    StressFalsifier,
)
from nautilus_trader.quant.strategies.empirical import rolling_atr
from nautilus_trader.quant.strategies.registry import list_strategy_archetype_schemas
from nautilus_trader.adapters.datainfra import DataInfraCatalog, DataInfraConfig
from nautilus_trader.model import Bar, Equity

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
    ) -> None:
        self._dsn = resolve_database_dsn(dsn) if dsn is not None else None
        self._datainfra_config = datainfra_config
        self._venue_name = str(venue_name or "ALPACA").strip().upper()
        self._capital_basis_usd = Decimal(str(capital_basis_usd))
        self._system_config = system_config or SystemConfig()
        self._runner = backtest_runner or IsomorphicBacktestRunner(
            dsn=self._dsn,
            datainfra_config=self._datainfra_config,
            starting_capital_usd=self._capital_basis_usd,
            venue_name=self._venue_name,
        )
        self._falsifier = stress_falsifier or StressFalsifier(runner=self._runner)
        self._bar_provider = bar_provider
        self._equity_provider = equity_provider

        self._lock = threading.Lock()
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
        lookback_days: int = 20,
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
                l = lows[idx]
                c = closes[idx]
                if c > 0.0:
                    rel_ranges_bps.append(((h - l) / c) * 10_000.0)
                if idx > 0:
                    prev_c = closes[idx - 1]
                    if prev_c > 0.0:
                        returns.append((c - prev_c) / prev_c)
                    tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
                    true_ranges.append(tr)
                else:
                    true_ranges.append(max(0.0, h - l))

            ret_vol = statistics.stdev(returns) if len(returns) >= 2 else 0.0
            atr_window = min(14, len(sorted_bars))
            current_atr = rolling_atr(highs, lows, closes, atr_window)

            sorted_tr = sorted(true_ranges)
            n_tr = len(sorted_tr)
            atr_p50 = sorted_tr[int(round(0.50 * (n_tr - 1)))] if n_tr else 0.0
            atr_p80 = sorted_tr[int(round(0.80 * (n_tr - 1)))] if n_tr else 0.0
            atr_p95 = sorted_tr[int(round(0.95 * (n_tr - 1)))] if n_tr else 0.0

            median_vol = statistics.median(volumes) if volumes else 0.0
            last_close = closes[-1]
            dollar_vol_median = median_vol * last_close
            est_spread_bps = (
                statistics.median(rel_ranges_bps) * 0.10 if rel_ranges_bps else 0.0
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
    ) -> Dict[str, Any]:
        """Runs `IsomorphicBacktestRunner` for `(symbol, archetype_id, params)` and returns metrics."""
        sym = str(symbol).strip().upper()
        strat_cfg = self._build_strategy_config(sym, archetype_id, params)
        bars, equities = self._resolve_bars_and_equities_for_contract(
            strat_cfg,
            start_date=start_date,
            end_date=end_date,
        )
        env = self._build_risk_envelope(sym, risk_envelope, bars=bars)
        plan_item = self._make_plan_item_contract(
            symbol=sym,
            strategy_config=strat_cfg,
            risk_envelope=env,
        )

        if walk_forward:
            metrics = self._runner.run_walk_forward(
                plan_item,
                bars=bars,
                equities=equities,
            )
        else:
            metrics = self._runner.run_backtest(
                plan_item,
                bars=bars,
                equities=equities,
            )

        fp = compute_strategy_fingerprint(sym, strat_cfg)
        backtest_id = f"bt-{fp[:16]}"
        with self._lock:
            self._verified_backtests[fp] = metrics

        return {
            "backtest_attestation_id": backtest_id,
            "strategy_fingerprint": fp,
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
    ) -> Dict[str, Any]:
        """Executes `StressFalsifier` (`2x` friction, `+1 bar` delay, tail gap shock) on candidate."""
        sym = str(symbol).strip().upper()
        strat_cfg = self._build_strategy_config(sym, archetype_id, params)
        bars, equities = self._resolve_bars_and_equities_for_contract(strat_cfg)
        env = self._build_risk_envelope(sym, risk_envelope, bars=bars)
        plan_item = self._make_plan_item_contract(
            symbol=sym,
            strategy_config=strat_cfg,
            risk_envelope=env,
        )

        report = self._falsifier.run_falsification(
            plan_item,
            bars=bars,
            equities=equities,
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
        if not validated_items:
            raise McpPlanLockRejectedError("validated_items cannot be empty")
        if not isinstance(committee_attestation, dict):
            raise McpPlanLockRejectedError("committee_attestation must be a dictionary")

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
            sector = str(raw_item.get("sector", "TECHNOLOGY")).strip().upper()

            strat_cfg = self._build_strategy_config(sym, arch_id, params)
            req_env = self._build_risk_envelope(sym, raw_env)
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
                equity_usd=cap_float,
                venue_equity_usd={"alpaca": cap_float},
                venue_cash_usd={"alpaca": cap_float},
                venue_open_buy_usd={"alpaca": 0.0},
                gross_exposure_usd=0.0,
                symbol_exposure_usd={},
                sector_exposure_usd={},
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
            approved_notional = max(
                Decimal("100.00"),
                min(
                    req_env.target_notional_usd,
                    Decimal(f"{max(decision.notional_usd, float(target_qty) * entry_ref):.2f}"),
                ),
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
            item_contract = self._make_plan_item_contract(
                symbol=sym,
                strategy_config=strat_cfg,
                risk_envelope=sized_envelope,
                plan_id=resolved_plan_uuid,
                sizing_trace=decision.trace,
            )
            locked_items.append(item_contract)
            sizing_decisions[sym] = {
                "target_qty": str(target_qty),
                "approved_qty_cap": str(approved_qty_cap),
                "target_notional_usd": str(approved_notional),
                "max_risk_budget_usd": str(approved_risk_budget),
                "binding_constraint": decision.binding_constraint,
                "sizing_trace": decision.trace,
            }

            committed_by_symbol[sym] = committed_by_symbol.get(sym, 0.0) + float(approved_notional)
            committed_by_sector[sector] = committed_by_sector.get(sector, 0.0) + float(approved_notional)
            committed_by_venue["alpaca"] = committed_by_venue.get("alpaca", 0.0) + float(approved_notional)
            new_risk_used_today += float(approved_risk_budget)

        if self._dsn is not None:
            repo = PlanContractRepository(dsn=self._dsn)
            repo.create_and_lock_trading_plan(
                trading_date=resolved_date,
                items=locked_items,
                total_equity_usd=self._capital_basis_usd,
                market=str(market or "US_EQUITY").strip().upper(),
                summary=str(committee_attestation.get("portfolio_chair_decision", "LOCKED")),
                plan_id=resolved_plan_uuid,
                actor="nautilus_mcp:epistemic_committee",
            )

        return {
            "status": "LOCKED",
            "plan_id": str(resolved_plan_uuid),
            "market": str(market or "US_EQUITY").strip().upper(),
            "trading_date": resolved_date.isoformat(),
            "capital_basis_usd": str(self._capital_basis_usd),
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

    def create_http_server(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
    ) -> ThreadingHTTPServer:
        """Creates a `ThreadingHTTPServer` serving JSON-RPC MCP over HTTP POST `/mcp` and GET `/healthz`."""
        mcp_server = self

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
    def _build_risk_envelope(
        symbol: str,
        raw_envelope: Optional[Dict[str, Any]],
        bars: Optional[Sequence[Bar]] = None,
    ) -> PlanRiskEnvelope:
        default_valid_until = datetime.now(timezone.utc) + timedelta(days=30)
        if raw_envelope is not None:
            side = PlanSide(str(raw_envelope.get("side", "long")).strip().lower())
            target_qty = Decimal(str(raw_envelope.get("target_qty", "50")))
            approved_qty_cap = Decimal(
                str(raw_envelope.get("approved_qty_cap", target_qty))
            )
            target_notional = Decimal(
                str(
                    raw_envelope.get(
                        "target_notional_usd",
                        raw_envelope.get("max_notional_usd", "10000.00"),
                    )
                )
            )
            max_risk_budget = Decimal(
                str(raw_envelope.get("max_risk_budget_usd", "1000.00"))
            )
            initial_stop = Decimal(
                str(
                    raw_envelope.get(
                        "initial_stop_price",
                        raw_envelope.get("stop_loss_price", "90.00"),
                    )
                )
            )
            tp_raw = raw_envelope.get("take_profit_price")
            take_profit = Decimal(str(tp_raw)) if tp_raw is not None else None
            collar_raw = raw_envelope.get("collar") or {
                "limit_collar_pct": "0.50",
                "limit_collar_atr": "0.25",
            }
            collar = (
                collar_raw
                if isinstance(collar_raw, MarketableLimitCollar)
                else MarketableLimitCollar.from_dict(collar_raw)
            )
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

        last_px = 100.0
        if bars:
            closes = [
                float(b.close)
                for b in bars
                if str(b.bar_type.instrument_id).startswith(symbol)
            ]
            if closes:
                last_px = closes[-1]

        stop_px = max(0.01, last_px * 0.90)
        tp_px = last_px * 1.15
        notional = max(1000.0, min(15000.0, 50.0 * last_px))
        return PlanRiskEnvelope(
            side=PlanSide.LONG,
            target_qty=Decimal("50"),
            approved_qty_cap=Decimal("50"),
            target_notional_usd=Decimal(f"{notional:.2f}"),
            max_risk_budget_usd=Decimal("1000.00"),
            initial_stop_price=Decimal(f"{stop_px:.2f}"),
            take_profit_price=Decimal(f"{tp_px:.2f}"),
            collar=MarketableLimitCollar(
                limit_collar_pct=Decimal("0.50"),
                limit_collar_atr=Decimal("0.25"),
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
        if "entry_price" in raw_env and raw_env["entry_price"] is not None:
            return float(raw_env["entry_price"])
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
        return float(req_env.initial_stop_price) * (
            1.05 if req_env.side == PlanSide.LONG else 0.95
        )

    def _load_bars_for_symbol(
        self,
        symbol: str,
        bar_timeframe: str,
        lookback_days: int = 20,
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
