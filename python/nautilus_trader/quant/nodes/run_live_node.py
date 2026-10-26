"""Production daemon entrypoint for `nautilus-live-node`.

Waits for or loads today's `LOCKED` `TradingPlanContract` from PostgreSQL via
`LISTEN nautilus_plan_locked`, assembles `LiveNode` with `DataInfra`, `Alpaca`,
`PlanGateExecAlgorithm`, `PostgresTelemetryActor`, and pre-audited `Strategy`s,
and runs the live event loop.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime
from typing import Optional, Sequence
from zoneinfo import ZoneInfo

from evolution_engine.db.dsn import resolve_database_dsn
from nautilus_trader.quant.mcp.run_mcp_service import build_datainfra_config_from_env
from nautilus_trader.quant.nodes.live_node import NautilusLiveNodeAssembler
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


def build_alpaca_config_from_env() -> AlpacaExecClientConfig:
    is_live = os.environ.get("ALPACA_ENVIRONMENT", "PAPER").strip().upper() == "LIVE"
    return AlpacaExecClientConfig(
        api_key=os.environ.get("APCA_API_KEY_ID") or os.environ.get("ALPACA_API_KEY") or "paper-key",
        api_secret=os.environ.get("APCA_API_SECRET_KEY") or os.environ.get("ALPACA_API_SECRET") or "paper-secret",
        environment="live" if is_live else "paper",
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Nautilus v2 LiveNode Daemon")
    parser.add_argument(
        "--trading-date",
        default=None,
        help="Trading date in YYYY-MM-DD (defaults to America/New_York current date)",
    )
    parser.add_argument(
        "--listen-timeout-secs",
        type=float,
        default=float(os.environ.get("PLAN_LISTEN_TIMEOUT_SECS", "3600")),
        help="Seconds to wait on LISTEN nautilus_plan_locked if plan is not yet locked",
    )
    parser.add_argument(
        "--dry-run-build",
        action="store_true",
        help="Build and verify LiveNode wiring from LOCKED plan, then dispose and exit 0",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    dsn = resolve_database_dsn()
    trading_date = resolve_trading_date(args.trading_date)
    env_mode = (
        Environment.LIVE
        if os.environ.get("TRADING_ENVIRONMENT", "PAPER").strip().upper() == "LIVE"
        else Environment.SANDBOX
    )

    assembler = NautilusLiveNodeAssembler(
        dsn=dsn,
        datainfra_config=build_datainfra_config_from_env(),
        alpaca_config=build_alpaca_config_from_env(),
        environment=env_mode,
        market=os.environ.get("TRADING_MARKET", "US_EQUITY"),
    )
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
    try:
        if args.dry_run_build:
            return 0
        bundle.node.run()
    finally:
        bundle.node.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(main())
