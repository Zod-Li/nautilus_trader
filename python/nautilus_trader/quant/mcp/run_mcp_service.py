"""Production daemon entrypoint for `nautilus-mcp-service` (`:8020`).

Starts `NautilusMcpServer` wired to PostgreSQL (`resolve_database_dsn`) and
`DataInfraConfig` (`ClickHouse` + `Kafka` + `calendar-service`).
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
from decimal import Decimal
from typing import Optional, Sequence

from evolution_engine.db.dsn import resolve_database_dsn
from nautilus_trader.quant.mcp.server import NautilusMcpServer
from nautilus_trader.adapters.datainfra import DataInfraConfig

logger = logging.getLogger("nautilus.mcp_service")


def build_datainfra_config_from_env() -> DataInfraConfig:
    """Constructs `DataInfraConfig` from container environment variables."""
    from urllib.parse import quote

    ch_url = os.environ.get("CLICKHOUSE_URL", "").strip()
    if not ch_url:
        ch_host = os.environ.get("CH_HOST", "127.0.0.1").strip()
        ch_port = (os.environ.get("CH_HTTP_PORT") or os.environ.get("CH_PORT") or "8123").strip()
        ch_user = os.environ.get("CH_USER", "").strip()
        ch_pw = os.environ.get("CH_PASSWORD", "").strip()
        if ch_user:
            ch_url = f"http://{ch_host}:{ch_port}/?user={quote(ch_user)}&password={quote(ch_pw)}"
        else:
            ch_url = f"http://{ch_host}:{ch_port}"
    ch_db = (
        os.environ.get("CLICKHOUSE_DATABASE")
        or os.environ.get("CH_DB")
        or "market_data"
    ).strip()
    brokers_raw = os.environ.get("KAFKA_BROKERS", "127.0.0.1:9092")
    brokers = [b.strip() for b in brokers_raw.split(",") if b.strip()]
    cal_url = os.environ.get("CALENDAR_SERVICE_URL", "http://127.0.0.1:8012").strip()
    venue = os.environ.get("DEFAULT_VENUE", "ALPACA").strip().upper()
    return DataInfraConfig(
        clickhouse_url=ch_url,
        clickhouse_database=ch_db,
        kafka_brokers=brokers,
        kafka_group_id=os.environ.get("KAFKA_GROUP_ID", "nautilus-mcp-service"),
        calendar_service_url=cal_url,
        default_venue=venue,
    )


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


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Nautilus MCP Server Daemon")
    parser.add_argument(
        "--host",
        default=os.environ.get("NAUTILUS_MCP_HOST", "0.0.0.0"),
        help="Bind host for HTTP JSON-RPC MCP server",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("NAUTILUS_MCP_PORT", "8020")),
        help="Bind port for HTTP JSON-RPC MCP server",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    mcp_server = create_production_mcp_server()
    http_server = mcp_server.create_http_server(host=args.host, port=args.port)

    def _shutdown_handler(signum: int, _frame: object) -> None:
        logger.info("Received signal %s, shutting down nautilus-mcp-service...", signum)
        http_server.shutdown()

    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)

    logger.info(
        "nautilus-mcp-service listening on http://%s:%d (tools=%s)",
        args.host,
        args.port,
        mcp_server.allowed_tools,
    )
    try:
        http_server.serve_forever()
    finally:
        http_server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
