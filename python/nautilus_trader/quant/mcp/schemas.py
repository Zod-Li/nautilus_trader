"""JSON Schema definitions and security validators for the 5 controlled `nautilus-mcp` tools."""

from __future__ import annotations

from typing import Any, Dict, FrozenSet, List, Tuple

from nautilus_trader.quant.contracts.models import (
    SUPPORTED_TIMEFRAMES,
    StrategyArchetypeId,
)

ALLOWED_BAR_TIMEFRAMES = SUPPORTED_TIMEFRAMES

ALLOWED_MCP_TOOL_NAMES: FrozenSet[str] = frozenset(
    {
        "list_strategy_archetypes",
        "query_datainfra_bars_summary",
        "run_archetype_backtest",
        "run_stress_falsification",
        "compile_and_lock_trading_plan",
    }
)

FORBIDDEN_CODE_TOKENS: Tuple[str, ...] = (
    "__import__",
    "eval(",
    "exec(",
    "compile(",
    "subprocess",
    "os.system",
    "import os",
    "import sys",
    "class ",
    "def on_bar",
)


class McpSecurityViolationError(PermissionError):
    """Raised when an caller attempts to invoke an unauthorized tool or inject code."""


class McpPlanLockRejectedError(ValueError):
    """Raised when `compile_and_lock_trading_plan` rejects an unverified or failed candidate."""


def assert_no_code_injection(payload: Any, path: str = "root") -> None:
    """Recursively scans tool arguments to block dynamic code strings."""
    if isinstance(payload, str):
        lowered = payload.lower()
        for token in FORBIDDEN_CODE_TOKENS:
            if token in lowered:
                raise McpSecurityViolationError(
                    f"Forbidden code token {token!r} detected in MCP argument at {path}"
                )
    elif isinstance(payload, dict):
        for k, v in payload.items():
            assert_no_code_injection(k, f"{path}.{k}")
            assert_no_code_injection(v, f"{path}.{k}")
    elif isinstance(payload, (list, tuple)):
        for idx, item in enumerate(payload):
            assert_no_code_injection(item, f"{path}[{idx}]")


def get_mcp_tool_input_schemas() -> List[Dict[str, Any]]:
    """Returns MCP-compliant tool descriptors for the 5 controlled tools."""
    archetype_enum = [a.value for a in StrategyArchetypeId]
    timeframe_enum = list(ALLOWED_BAR_TIMEFRAMES)

    strategy_params_schema: Dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "bar_timeframe",
            "lookback_window",
            "fast_window",
            "slow_window",
            "entry_quantile_threshold",
            "exit_quantile_threshold",
            "atr_stop_multiplier",
        ],
        "properties": {
            "instrument_id": {"type": "string"},
            "bar_timeframe": {"type": "string", "enum": timeframe_enum},
            "lookback_window": {"type": "integer", "minimum": 5, "maximum": 500},
            "fast_window": {"type": "integer", "minimum": 2, "maximum": 499},
            "slow_window": {"type": "integer", "minimum": 5, "maximum": 500},
            "entry_quantile_threshold": {"type": ["number", "string"]},
            "exit_quantile_threshold": {"type": ["number", "string"]},
            "atr_stop_multiplier": {"type": ["number", "string"]},
            "secondary_instrument_id": {"type": ["string", "null"]},
        },
    }

    risk_envelope_schema: Dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "side",
            "target_qty",
            "max_risk_budget_usd",
        ],
        "properties": {
            "symbol": {"type": "string"},
            "side": {"type": "string", "enum": ["long", "short"]},
            "target_qty": {"type": ["number", "string"]},
            "approved_qty_cap": {"type": ["number", "string"]},
            "target_notional_usd": {"type": ["number", "string"]},
            "max_notional_usd": {"type": ["number", "string"]},
            "entry_price": {"type": ["number", "string"]},
            "entry_price_band_low": {"type": ["number", "string"]},
            "entry_price_band_high": {"type": ["number", "string"]},
            "initial_stop_price": {"type": ["number", "string"]},
            "stop_loss_price": {"type": ["number", "string"]},
            "take_profit_price": {"type": ["number", "string", "null"]},
            "max_risk_budget_usd": {"type": ["number", "string"]},
            "collar": {"type": "object"},
            "valid_until": {"type": "string"},
            "replace_allowed": {"type": "boolean"},
        },
    }

    return [
        {
            "name": "list_strategy_archetypes",
            "description": (
                "List all pre-audited Nautilus v2 Strategy archetypes in "
                "STRATEGY_ARCHETYPE_REGISTRY and their empirical parameter bounds."
            ),
            "inputSchema": {
                "type": "object",
                "additionalProperties": False,
                "properties": {},
            },
        },
        {
            "name": "query_datainfra_bars_summary",
            "description": (
                "Query DataInfraCatalog for recent volatility, ATR quantiles, "
                "spread estimates, and liquidity summaries across candidate symbols."
            ),
            "inputSchema": {
                "type": "object",
                "additionalProperties": False,
                "required": ["symbols"],
                "properties": {
                    "symbols": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                    },
                    "lookback_days": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 365,
                        "default": 20,
                    },
                    "bar_timeframe": {
                        "type": "string",
                        "enum": timeframe_enum,
                        "default": "15m",
                    },
                },
            },
        },
        {
            "name": "run_archetype_backtest",
            "description": (
                "Run a 1:1 isomorphic backtest in Nautilus BacktestEngine with "
                "Strategy(StrategyConfig) + PlanGateExecAlgorithm and return structured metrics."
            ),
            "inputSchema": {
                "type": "object",
                "additionalProperties": False,
                "required": ["symbol", "archetype_id", "params"],
                "properties": {
                    "symbol": {"type": "string"},
                    "archetype_id": {"type": "string", "enum": archetype_enum},
                    "params": strategy_params_schema,
                    "start_date": {"type": ["string", "null"]},
                    "end_date": {"type": ["string", "null"]},
                    "risk_envelope": risk_envelope_schema,
                    "walk_forward": {"type": "boolean", "default": False},
                },
            },
        },
        {
            "name": "run_stress_falsification",
            "description": (
                "Run the 3 mandatory counterfactual stress falsification experiments "
                "(2x friction, +1 bar delay, tail gap shock) on a candidate PlanItem."
            ),
            "inputSchema": {
                "type": "object",
                "additionalProperties": False,
                "required": ["symbol", "archetype_id", "params", "risk_envelope"],
                "properties": {
                    "symbol": {"type": "string"},
                    "archetype_id": {"type": "string", "enum": archetype_enum},
                    "params": strategy_params_schema,
                    "risk_envelope": risk_envelope_schema,
                },
            },
        },
        {
            "name": "compile_and_lock_trading_plan",
            "description": (
                "Verify all candidate items passed backtest and stress falsification, "
                "apply deterministic capital sizing, and atomically lock the TradingPlan "
                "in PostgreSQL trading_plans (status=LOCKED)."
            ),
            "inputSchema": {
                "type": "object",
                "additionalProperties": False,
                "required": ["plan_date", "validated_items", "committee_attestation"],
                "properties": {
                    "plan_date": {"type": "string"},
                    "validated_items": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "required": [
                                "symbol",
                                "archetype_id",
                                "params",
                                "risk_envelope",
                            ],
                            "properties": {
                                "symbol": {"type": "string"},
                                "archetype_id": {"type": "string", "enum": archetype_enum},
                                "params": strategy_params_schema,
                                "risk_envelope": risk_envelope_schema,
                                "conviction": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                                "sector": {"type": "string"},
                                "falsification_token": {"type": "string"},
                            },
                        },
                    },
                    "committee_attestation": {
                        "type": "object",
                        "required": [
                            "macro_catalyst_summary",
                            "quant_research_summary",
                            "adversarial_risk_summary",
                            "portfolio_chair_decision",
                        ],
                    },
                },
            },
        },
    ]
