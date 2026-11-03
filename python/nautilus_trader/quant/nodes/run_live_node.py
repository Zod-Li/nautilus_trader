"""Production daemon entrypoint for the unified `nautilus-trader` engine container.

Starts the `:8015` Agent-facing MCP proxy (`NautilusMcpServer` HTTP JSON-RPC 2.0) in a
background thread while the main thread loads or awaits today's `LOCKED` `TradingPlanContract`
from PostgreSQL via `LISTEN nautilus_plan_locked`, assembles `LiveNode` (`TRADER-LIVE-001`)
with `DataInfra`, `Alpaca`, `PlanGateExecAlgorithm`, `PostgresTelemetryActor`, and pre-audited
`Strategy`s, and runs the live execution event loop.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import threading
from datetime import date, datetime
from http.server import ThreadingHTTPServer
from typing import Optional, Sequence
from zoneinfo import ZoneInfo

from evolution_engine.db.dsn import resolve_database_dsn
from evolution_engine.db_migrate import apply_migrations
from nautilus_trader.quant.mcp.server import (
    build_datainfra_config_from_env,
    create_production_mcp_server,
)
from nautilus_trader.quant.nodes.live_node import (
    TRADING_MODE_ENV_VARS,
    DailyPlanReconciler,
    DailyPlanRolloverListener,
    NautilusLiveNodeAssembler,
    build_alpaca_config_from_env,
    resolve_trading_mode,
    validate_paper_live_isolation,
)
from nautilus_trader.adapters.alpaca import AlpacaExecClientConfig
from nautilus_trader.common import Environment

logger = logging.getLogger("nautilus.live_node")
NY_TZ = ZoneInfo("America/New_York")


def resolve_trading_date(explicit_date: Optional[str] = None) -> date:
    if explicit_date:
        return date.fromisoformat(explicit_date.strip()[:10])
    env_date = os.environ.get("TRADING_DATE", "").strip()
    if env_date:
        return date.fromisoformat(env_date[:10])
    return datetime.now(NY_TZ).date()


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Nautilus Trader Unified Engine & MCP Daemon")
    parser.add_argument(
        "--trading-date",
        default=None,
        help="Trading date in YYYY-MM-DD (defaults to America/New_York current date)",
    )
    parser.add_argument(
        "--listen-timeout-secs",
        type=float,
        default=float(os.environ.get("PLAN_LISTEN_TIMEOUT_SECS", "86400")),
        help="Seconds to wait on LISTEN nautilus_plan_locked if plan is not yet locked",
    )
    parser.add_argument(
        "--mcp-host",
        default=os.environ.get("NAUTILUS_MCP_HOST", "127.0.0.1"),
        help="Bind host for embedded Agent-facing MCP HTTP proxy",
    )
    parser.add_argument(
        "--mcp-port",
        type=int,
        default=int(os.environ.get("NAUTILUS_MCP_PORT", "8015")),
        help="Bind port for embedded Agent-facing MCP HTTP proxy",
    )
    parser.add_argument(
        "--no-mcp",
        action="store_true",
        help="Disable starting the embedded MCP HTTP proxy",
    )
    parser.add_argument(
        "--dry-run-build",
        action="store_true",
        help="Build and verify LiveNode wiring from LOCKED plan, then dispose and exit 0",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    trading_mode = resolve_trading_mode()
    alpaca_config = build_alpaca_config_from_env(trading_mode)
    env_mode = Environment.LIVE if trading_mode == "LIVE" else Environment.SANDBOX

    dsn = resolve_database_dsn()
    apply_migrations(dsn)
    trading_date = resolve_trading_date(args.trading_date)

    mcp_http_server: Optional[ThreadingHTTPServer] = None
    if not args.no_mcp and not args.dry_run_build:
        mcp_server = create_production_mcp_server(dsn=dsn)
        mcp_http_server = mcp_server.create_http_server(
            host=args.mcp_host,
            port=args.mcp_port,
        )
        mcp_thread = threading.Thread(
            target=mcp_http_server.serve_forever,
            name="nautilus-mcp-http",
            daemon=True,
        )
        mcp_thread.start()
        logger.info(
            "nautilus-trader MCP proxy listening on http://%s:%d (tools=%s)",
            args.mcp_host,
            args.mcp_port,
            mcp_server.allowed_tools,
        )

    assembler = NautilusLiveNodeAssembler(
        dsn=dsn,
        datainfra_config=build_datainfra_config_from_env(),
        alpaca_config=alpaca_config,
        environment=env_mode,
        market=os.environ.get("TRADING_MARKET", "US_EQUITY"),
    )
    try:
        bundle = assembler.build_live_node(
            trading_date=trading_date,
            listen_timeout_secs=args.listen_timeout_secs,
            submit_protective_stop=True,
        )
        logger.info(
            "Built LiveNode for trading_date=%s items=%d strategies=%d venues=%s",
            bundle.trading_date.isoformat(),
            len(bundle.plan_items),
            len(bundle.strategies),
            bundle.routed_venues,
        )
        market_name = os.environ.get("TRADING_MARKET", "US_EQUITY")
        reconciler = bundle.reconciler or DailyPlanReconciler.from_bundle(
            bundle,
            assembler=assembler,
        )
        rollover_listener = DailyPlanRolloverListener(
            dsn=dsn,
            reconciler=reconciler,
            repository=assembler.repository,
            market=market_name,
            on_rollover_callback=lambda report: logger.info(
                "Completed daily TradingPlan rollover for %s: new=%s renewed=%s carry_over=%s invalidated=%s",
                report.trading_date.isoformat(),
                report.new_active_symbols,
                report.renewed_active_symbols,
                report.carry_over_exit_only_symbols,
                report.thesis_invalidated_exit_symbols,
            ),
        )
        try:
            if args.dry_run_build:
                return 0
            rollover_listener.start_background_listener()
            if hasattr(bundle.node, "run_async") and callable(bundle.node.run_async):
                async def _drive_live_node() -> None:
                    reconciler.bind_event_loop(asyncio.get_running_loop())
                    await bundle.node.run_async()

                asyncio.run(_drive_live_node())
            else:
                bundle.node.run()
        finally:
            rollover_listener.stop()
            bundle.plan_gate.close()
            bundle.telemetry_actor.close()
            bundle.node.dispose()
    finally:
        if mcp_http_server is not None:
            mcp_http_server.shutdown()
            mcp_http_server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
