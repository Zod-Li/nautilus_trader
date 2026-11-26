"""PostgreSQL repository for atomic locking and loading of Isomorphic `PlanItemContract` items."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal
import json
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from uuid import UUID, uuid4

import psycopg2
from psycopg2.extras import RealDictCursor, register_uuid

from evolution_engine.config_center.envelope import Envelope, load_envelope
from evolution_engine.db.dsn import resolve_database_dsn
from nautilus_trader.quant.contracts.models import (
    VALID_ITEM_STATUSES,
    ExecutionSpec,
    PlanContractValidationError,
    PlanItemContract,
    PlanSide,
    _to_decimal,
)

register_uuid()


class PlanContractRepository:
    """Authoritative PostgreSQL persistence adapter for `PlanItemContract` (`trading_plans` & `trading_plan_items`)."""

    def __init__(
        self,
        dsn: Optional[str] = None,
        safety_envelope: Optional[Envelope] = None,
    ) -> None:
        self._dsn = resolve_database_dsn(dsn)
        self._safety_envelope = safety_envelope or load_envelope()

    @property
    def dsn(self) -> str:
        return self._dsn

    def create_and_lock_trading_plan(
        self,
        trading_date: date,
        items: Sequence[PlanItemContract],
        total_equity_usd: Decimal,
        market: str = "US_EQUITY",
        config_version_id: int = 1,
        capital_snapshot_id: Optional[UUID] = None,
        summary: str = "",
        plan_id: Optional[UUID] = None,
        actor: str = "system:epistemic-committee",
        carry_over_decisions: Optional[Sequence[Mapping[str, Any]]] = None,
        zero_item_reason_code: Optional[str] = None,
    ) -> UUID:
        """Validate all candidate `PlanItemContract` items against `config/risk_envelope.json` and
        atomically persist + lock the daily `TradingPlan` in a single PostgreSQL transaction.
        """
        eq_dec = _to_decimal(total_equity_usd, "total_equity_usd")
        if eq_dec <= Decimal("0"):
            raise PlanContractValidationError(f"total_equity_usd must be > 0, got {eq_dec}")

        max_new_pos_bounds = self._safety_envelope.numeric.get(
            "admission.max_new_positions_per_day", {"min": 0, "max": 5}
        )
        max_new_positions = int(max_new_pos_bounds["max"])
        if len(items) > max_new_positions:
            raise PlanContractValidationError(
                f"Candidate count ({len(items)}) exceeds admission.max_new_positions_per_day ({max_new_positions})"
            )

        total_new_risk_usd = Decimal("0")
        for item in items:
            item.validate(
                safety_envelope=self._safety_envelope,
                total_equity_usd=eq_dec,
            )
            total_new_risk_usd += item.risk_envelope.max_risk_budget_usd

        max_daily_risk_bounds = self._safety_envelope.numeric.get(
            "risk.max_daily_new_risk_pct", {"min": 0.0, "max": 4.0}
        )
        max_daily_risk_usd = eq_dec * (
            Decimal(str(max_daily_risk_bounds["max"])) / Decimal("100")
        )
        if total_new_risk_usd > max_daily_risk_usd:
            raise PlanContractValidationError(
                f"Aggregate daily risk budget ({total_new_risk_usd}) exceeds risk.max_daily_new_risk_pct cap ({max_daily_risk_usd})"
            )

        resolved_plan_id = plan_id or (items[0].plan_id if items else uuid4())
        rebound_items: List[PlanItemContract] = list(items)
        serialized_carry_over = [dict(c) for c in (carry_over_decisions or ())]
        vetoed_carry_over_symbols: List[str] = []
        for entry in serialized_carry_over:
            sym = str(entry.get("symbol") or "").strip().upper()
            dec_state = str(
                entry.get("decision")
                or entry.get("lifecycle_state")
                or entry.get("action")
                or ""
            ).strip().upper()
            if sym and dec_state == "THESIS_INVALIDATED_EXIT" and sym not in vetoed_carry_over_symbols:
                vetoed_carry_over_symbols.append(sym)

        conn = psycopg2.connect(self._dsn, cursor_factory=RealDictCursor)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT plan_id, status
                        FROM trading_plans
                        WHERE trading_date = %s AND market = %s
                        FOR UPDATE
                        """,
                        (trading_date, market),
                    )
                    existing_plan = cur.fetchone()
                    if existing_plan is not None:
                        existing_id = UUID(str(existing_plan["plan_id"]))
                        existing_status = str(existing_plan["status"] or "").strip().upper()
                        if existing_status in ("LOCKED", "CLOSED"):
                            if not items:
                                return existing_id
                            raise PlanContractValidationError(
                                f"Trading plan {existing_id} for {trading_date.isoformat()} ({market}) "
                                f"is already {existing_status} and immutable"
                            )
                        if existing_status == "DRAFT":
                            if plan_id is None or (items and items[0].plan_id == resolved_plan_id):
                                resolved_plan_id = existing_id
                                rebound_items = [
                                    replace(it, plan_id=resolved_plan_id)
                                    if it.plan_id != resolved_plan_id
                                    else it
                                    for it in items
                                ]
                            cur.execute(
                                "DELETE FROM trading_plan_items WHERE plan_id = %s",
                                (existing_id,),
                            )
                            cur.execute(
                                """
                                UPDATE trading_plans
                                SET plan_id = %s,
                                    config_version_id = %s,
                                    capital_snapshot_id = %s,
                                    candidate_pool = %s::jsonb,
                                    rejections = %s::jsonb,
                                    summary = %s
                                WHERE plan_id = %s
                                """,
                                (
                                    resolved_plan_id,
                                    config_version_id,
                                    capital_snapshot_id,
                                    json.dumps([item.symbol for item in rebound_items]),
                                    json.dumps(vetoed_carry_over_symbols),
                                    summary,
                                    existing_id,
                                ),
                            )
                        else:
                            raise PlanContractValidationError(
                                f"Unsupported existing trading plan status {existing_status!r} for {trading_date.isoformat()} ({market})"
                            )
                    else:
                        cur.execute(
                            """
                            INSERT INTO trading_plans (
                                plan_id, trading_date, market, status, config_version_id,
                                capital_snapshot_id, candidate_pool, rejections, summary
                            ) VALUES (
                                %s, %s, %s, 'DRAFT', %s, %s, %s::jsonb, %s::jsonb, %s
                            )
                            """,
                            (
                                resolved_plan_id,
                                trading_date,
                                market,
                                config_version_id,
                                capital_snapshot_id,
                                json.dumps([item.symbol for item in rebound_items]),
                                json.dumps(vetoed_carry_over_symbols),
                                summary,
                            ),
                        )

                    for item in rebound_items:
                        if item.plan_id != resolved_plan_id:
                            raise PlanContractValidationError(
                                f"Item {item.item_id} plan_id ({item.plan_id}) != resolved_plan_id ({resolved_plan_id})"
                            )
                        exec_spec_obj = (
                            item.execution_spec
                            if item.execution_spec is not None
                            else ExecutionSpec(bar_timeframe=item.strategy_config.bar_timeframe)
                        )
                        cur.execute(
                            """
                            INSERT INTO trading_plan_items (
                                item_id, plan_id, kind, symbol, broker, asset_class,
                                strategy_id, archetype, side, trigger_spec,
                                execution_spec, daily_atr_14,
                                target_qty, target_notional_usd, risk_budget_usd,
                                initial_stop_price, take_profit_price, valid_until,
                                sizing_trace, status, reserved_notional_usd, filled_qty,
                                current_stop_price, approved_qty_cap
                            ) VALUES (
                                %s, %s, 'NEW_ENTRY', %s, %s, %s,
                                %s, %s, %s, %s::jsonb,
                                %s::jsonb, %s,
                                %s, %s, %s,
                                %s, %s, %s,
                                %s::jsonb, %s, %s, %s,
                                %s, %s
                            )
                            """,
                            (
                                item.item_id,
                                resolved_plan_id,
                                item.symbol,
                                item.broker,
                                item.asset_class,
                                item.strategy_id,
                                item.strategy_config.archetype_id.value,
                                item.risk_envelope.side.value,
                                json.dumps(item.to_trigger_spec_dict()),
                                json.dumps(exec_spec_obj.to_dict()),
                                item.daily_atr_14,
                                item.risk_envelope.target_qty,
                                item.risk_envelope.target_notional_usd,
                                item.risk_envelope.max_risk_budget_usd,
                                item.risk_envelope.initial_stop_price,
                                item.risk_envelope.take_profit_price,
                                item.risk_envelope.valid_until,
                                json.dumps(dict(item.sizing_trace)),
                                item.status,
                                item.reserved_notional_usd,
                                item.filled_qty,
                                item.risk_envelope.initial_stop_price,
                                item.risk_envelope.approved_qty_cap,
                            ),
                        )
                        cur.execute(
                            """
                            INSERT INTO trading_plan_tranches (
                                item_id, tranche_index, weight, qty, trigger_atr, status, filled_qty
                            ) VALUES (
                                %s, 0, 1.0, %s, NULL, 'PENDING', 0
                            )
                            """,
                            (
                                item.item_id,
                                item.risk_envelope.target_qty,
                            ),
                        )

                    cur.execute(
                        """
                        UPDATE trading_plans
                        SET status = 'LOCKED', locked_at = NOW()
                        WHERE plan_id = %s
                        """,
                        (resolved_plan_id,),
                    )

                    event_payload: Dict[str, Any] = {
                        "trading_date": trading_date.isoformat(),
                        "market": market,
                        "item_count": len(rebound_items),
                        "carry_over_decisions": serialized_carry_over,
                        "vetoed_symbols": vetoed_carry_over_symbols,
                    }
                    if not rebound_items:
                        event_payload["zero_item_reason_code"] = (
                            zero_item_reason_code or "ALL_CANDIDATES_VETOED"
                        )

                    cur.execute(
                        """
                        INSERT INTO trading_plan_events (
                            plan_id, item_id, event_type, actor, reason, payload
                        ) VALUES (
                            %s, NULL, 'PLAN_LOCKED', %s, %s, %s::jsonb
                        )
                        """,
                        (
                            resolved_plan_id,
                            actor,
                            summary or "Isomorphic trading plan locked",
                            json.dumps(event_payload),
                        ),
                    )

                    notify_payload: Dict[str, Any] = {
                        "plan_id": str(resolved_plan_id),
                        "trading_date": trading_date.isoformat(),
                        "market": market,
                        "vetoed_symbols": vetoed_carry_over_symbols,
                        "carry_over_decisions": [
                            {
                                "symbol": str(c.get("symbol") or ""),
                                "decision": str(
                                    c.get("decision")
                                    or c.get("lifecycle_state")
                                    or c.get("action")
                                    or ""
                                ),
                            }
                            for c in serialized_carry_over
                        ],
                    }
                    if not rebound_items:
                        notify_payload["zero_item_reason_code"] = (
                            zero_item_reason_code or "ALL_CANDIDATES_VETOED"
                        )

                    cur.execute(
                        "SELECT pg_notify('nautilus_plan_locked', %s)",
                        (json.dumps(notify_payload),),
                    )
        finally:
            conn.close()

        return resolved_plan_id

    @staticmethod
    def _table_exists(cur: Any, table_name: str) -> bool:
        cur.execute("SELECT to_regclass(%s) AS reg", (f"public.{table_name}",))
        row = cur.fetchone()
        if isinstance(row, Mapping):
            return row.get("reg") is not None
        if isinstance(row, (tuple, list)) and row:
            return row[0] is not None
        return False

    def has_locked_plan(
        self,
        trading_date: date | str,
        market: str = "US_EQUITY",
    ) -> bool:
        """Return True if a `LOCKED` trading plan exists in `trading_plans` for `(trading_date, market)`
        (including 0-item `LOCKED_NO_TRADE` plans).
        """
        td = (
            date.fromisoformat(str(trading_date)[:10])
            if isinstance(trading_date, str)
            else trading_date
        )
        mkt = str(market or "US_EQUITY").strip()
        conn = psycopg2.connect(self._dsn, cursor_factory=RealDictCursor)
        try:
            with conn.cursor() as cur:
                if not self._table_exists(cur, "trading_plans"):
                    return False
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM trading_plans
                        WHERE trading_date = %s::date
                          AND (market = %s OR (%s IN ('US', 'US_EQUITY') AND market IN ('US', 'US_EQUITY')))
                          AND status = 'LOCKED'
                    ) AS has_locked
                    """,
                    (td, mkt, mkt),
                )
                row = cur.fetchone()
                if isinstance(row, Mapping):
                    return bool(row.get("has_locked", row.get("exists", False)))
                if isinstance(row, (tuple, list)) and row:
                    return bool(row[0])
                return False
        finally:
            conn.close()

    def load_locked_plan_metadata(
        self,
        trading_date: date | str,
        market: str = "US_EQUITY",
    ) -> Optional[Tuple[str, Optional[str]]]:
        """Return `(plan_id, locked_at_str)` for the active `LOCKED` plan on `(trading_date, market)`."""
        td = (
            date.fromisoformat(str(trading_date)[:10])
            if isinstance(trading_date, str)
            else trading_date
        )
        mkt = str(market or "US_EQUITY").strip()
        conn = psycopg2.connect(self._dsn, cursor_factory=RealDictCursor)
        try:
            with conn.cursor() as cur:
                if not self._table_exists(cur, "trading_plans"):
                    return None
                cur.execute(
                    """
                    SELECT plan_id, locked_at
                    FROM trading_plans
                    WHERE trading_date = %s::date
                      AND (market = %s OR (%s IN ('US', 'US_EQUITY') AND market IN ('US', 'US_EQUITY')))
                      AND status = 'LOCKED'
                    ORDER BY locked_at DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (td, mkt, mkt),
                )
                row = cur.fetchone()
                if not row:
                    return None
                if isinstance(row, Mapping):
                    pid = row.get("plan_id")
                    locked_at = row.get("locked_at")
                else:
                    pid = row[0]
                    locked_at = row[1] if len(row) > 1 else None
                if pid is None:
                    return None
                return (str(pid), str(locked_at) if locked_at is not None else None)
        finally:
            conn.close()

    def load_latest_plan_lock_event_payload(
        self,
        trading_date: date | str,
        market: str = "US_EQUITY",
    ) -> Dict[str, Any]:
        """Loads the latest `PLAN_LOCKED` event payload for `(trading_date, market)`."""
        td = (
            date.fromisoformat(str(trading_date)[:10])
            if isinstance(trading_date, str)
            else trading_date
        )
        mkt = str(market or "US_EQUITY").strip()
        conn = psycopg2.connect(self._dsn, cursor_factory=RealDictCursor)
        try:
            with conn.cursor() as cur:
                if not self._table_exists(cur, "trading_plans"):
                    return {}
                cur.execute(
                    """
                    SELECT e.payload, p.rejections
                    FROM trading_plans p
                    LEFT JOIN trading_plan_events e
                      ON e.plan_id = p.plan_id AND e.event_type = 'PLAN_LOCKED'
                    WHERE p.trading_date = %s
                      AND (p.market = %s OR (%s IN ('US', 'US_EQUITY') AND p.market IN ('US', 'US_EQUITY')))
                    ORDER BY e.ts DESC NULLS LAST, p.locked_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (td, mkt, mkt),
                )
                row = cur.fetchone()
                if row is None:
                    return {}
                raw_payload = row.get("payload")
                if isinstance(raw_payload, str):
                    try:
                        parsed = json.loads(raw_payload)
                    except (TypeError, ValueError):
                        parsed = {}
                elif isinstance(raw_payload, Mapping):
                    parsed = dict(raw_payload)
                else:
                    parsed = {}
                raw_rej = row.get("rejections")
                if isinstance(raw_rej, str):
                    try:
                        rej_list = json.loads(raw_rej)
                    except (TypeError, ValueError):
                        rej_list = []
                elif isinstance(raw_rej, list):
                    rej_list = list(raw_rej)
                else:
                    rej_list = []
                if rej_list and not parsed.get("vetoed_symbols"):
                    parsed["vetoed_symbols"] = [
                        str(s).strip().upper() for s in rej_list if str(s).strip()
                    ]
                return parsed
        finally:
            conn.close()

    def load_locked_plan_items(
        self,
        trading_date: date | str,
        market: str = "US_EQUITY",
    ) -> List[PlanItemContract]:
        """Load all `PlanItemContract` records belonging to the `LOCKED` plan for `(trading_date, market)`."""
        td = (
            date.fromisoformat(str(trading_date)[:10])
            if isinstance(trading_date, str)
            else trading_date
        )
        mkt = str(market or "US_EQUITY").strip()
        conn = psycopg2.connect(self._dsn, cursor_factory=RealDictCursor)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT i.*
                    FROM trading_plan_items i
                    JOIN trading_plans p ON p.plan_id = i.plan_id
                    WHERE p.trading_date = %s
                      AND (p.market = %s OR (%s IN ('US', 'US_EQUITY') AND p.market IN ('US', 'US_EQUITY')))
                      AND p.status = 'LOCKED'
                    ORDER BY i.symbol ASC
                    """,
                    (td, mkt, mkt),
                )
                rows = cur.fetchall()
        finally:
            conn.close()

        contracts: List[PlanItemContract] = []
        for row in rows:
            contract = PlanItemContract.from_db_row(row)
            contract.validate(safety_envelope=self._safety_envelope)
            contracts.append(contract)
        return contracts

    def load_carry_over_items_for_open_positions(
        self,
        trading_date: date | str,
        market: str = "US_EQUITY",
        excluded_symbols: Optional[Iterable[str]] = None,
        broker: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> List[PlanItemContract]:
        """Hydrate exit-only `PlanItemContract` instances for open overnight positions in `trading_positions`
        whose symbols are not already present in today's locked `trading_plan_items`.
        """
        td = (
            date.fromisoformat(str(trading_date)[:10])
            if isinstance(trading_date, str)
            else trading_date
        )
        mkt = str(market or "US_EQUITY").strip()
        excluded = {
            str(s).strip().upper()
            for s in (excluded_symbols or ())
            if str(s).strip()
        }

        conn = psycopg2.connect(self._dsn, cursor_factory=RealDictCursor)
        try:
            with conn.cursor() as cur:
                if not self._table_exists(cur, "trading_positions"):
                    return []
                pos_filters = ["ABS(qty) > 1e-9"]
                pos_params: List[Any] = []
                if broker is not None and str(broker).strip():
                    pos_filters.append("LOWER(broker) = LOWER(%s)")
                    pos_params.append(str(broker).strip())
                if environment is not None and str(environment).strip():
                    pos_filters.append("UPPER(environment) = UPPER(%s)")
                    pos_params.append(str(environment).strip())
                cur.execute(
                    f"""
                    SELECT *
                    FROM trading_positions
                    WHERE {' AND '.join(pos_filters)}
                    ORDER BY symbol ASC
                    """,
                    tuple(pos_params),
                )
                pos_rows = cur.fetchall() or []
                open_positions = [
                    r
                    for r in pos_rows
                    if str(r.get("symbol") or "").strip().upper()
                    and str(r.get("symbol") or "").strip().upper() not in excluded
                ]
                if not open_positions:
                    return []

                vetoed_symbols: set[str] = set()
                decisions_by_symbol: Dict[str, str] = {}
                today_plan_id: Optional[UUID] = None

                def _ingest_decisions(raw_decisions: Any) -> None:
                    if isinstance(raw_decisions, str):
                        try:
                            raw_decisions = json.loads(raw_decisions)
                        except (TypeError, ValueError):
                            return
                    if isinstance(raw_decisions, Mapping):
                        if "carry_over_decisions" in raw_decisions or "vetoed_symbols" in raw_decisions:
                            for vs in raw_decisions.get("vetoed_symbols") or ():
                                vs_norm = str(vs).strip().upper()
                                if vs_norm:
                                    vetoed_symbols.add(vs_norm)
                            _ingest_decisions(raw_decisions.get("carry_over_decisions"))
                            return
                        for k, v in raw_decisions.items():
                            sym_k = str(k).strip().upper()
                            if not sym_k:
                                continue
                            if isinstance(v, Mapping):
                                dec_v = str(
                                    v.get("decision")
                                    or v.get("lifecycle_state")
                                    or v.get("action")
                                    or v.get("status")
                                    or ""
                                ).strip().upper()
                            else:
                                dec_v = str(v).strip().upper()
                            if dec_v:
                                decisions_by_symbol[sym_k] = dec_v
                    elif isinstance(raw_decisions, list):
                        for entry in raw_decisions:
                            if isinstance(entry, Mapping):
                                sym_e = str(entry.get("symbol") or "").strip().upper()
                                dec_e = str(
                                    entry.get("decision")
                                    or entry.get("lifecycle_state")
                                    or entry.get("action")
                                    or entry.get("status")
                                    or ""
                                ).strip().upper()
                                if sym_e and dec_e:
                                    decisions_by_symbol[sym_e] = dec_e

                if self._table_exists(cur, "trading_plans"):
                    cur.execute(
                        """
                        SELECT *
                        FROM trading_plans
                        WHERE trading_date = %s::date
                          AND (market = %s OR (%s IN ('US', 'US_EQUITY') AND market IN ('US', 'US_EQUITY')))
                        ORDER BY locked_at DESC NULLS LAST, created_at DESC NULLS LAST
                        LIMIT 1
                        """,
                        (td, mkt, mkt),
                    )
                    plan_row = cur.fetchone()
                    if plan_row is not None:
                        if plan_row.get("plan_id") is not None:
                            today_plan_id = UUID(str(plan_row["plan_id"]))
                        raw_rej = plan_row.get("rejections")
                        if isinstance(raw_rej, str):
                            try:
                                raw_rej = json.loads(raw_rej)
                            except (TypeError, ValueError):
                                raw_rej = []
                        if isinstance(raw_rej, list):
                            for item in raw_rej:
                                if isinstance(item, str) and item.strip():
                                    vetoed_symbols.add(item.strip().upper())
                                elif isinstance(item, Mapping):
                                    _ingest_decisions([item])
                        for col_name in ("regime_consensus_json", "carry_over_decisions", "review"):
                            if col_name in plan_row and plan_row.get(col_name) is not None:
                                _ingest_decisions(plan_row.get(col_name))

                        if today_plan_id is not None and self._table_exists(cur, "trading_plan_events"):
                            cur.execute(
                                """
                                SELECT payload
                                FROM trading_plan_events
                                WHERE plan_id = %s AND event_type = 'PLAN_LOCKED'
                                ORDER BY ts DESC NULLS LAST
                                LIMIT 1
                                """,
                                (today_plan_id,),
                            )
                            ev_row = cur.fetchone()
                            if ev_row is not None and ev_row.get("payload") is not None:
                                _ingest_decisions(ev_row.get("payload"))

                if self._table_exists(cur, "committee_deliberation_runs"):
                    cur.execute(
                        """
                        SELECT carry_over_decisions, vetoed_symbols
                        FROM committee_deliberation_runs
                        WHERE trading_date = %s::date
                        ORDER BY COALESCE(completed_at, started_at) DESC NULLS LAST
                        LIMIT 1
                        """,
                        (td,),
                    )
                    delib_row = cur.fetchone()
                    if delib_row is not None:
                        _ingest_decisions(delib_row.get("carry_over_decisions"))
                        raw_vs = delib_row.get("vetoed_symbols")
                        if isinstance(raw_vs, str):
                            try:
                                raw_vs = json.loads(raw_vs)
                            except (TypeError, ValueError):
                                raw_vs = []
                        if isinstance(raw_vs, list):
                            for vs in raw_vs:
                                if str(vs).strip():
                                    vetoed_symbols.add(str(vs).strip().upper())

                has_plan_items_table = self._table_exists(cur, "trading_plan_items")
                has_protective_orders_table = self._table_exists(cur, "protective_orders")

                carry_over_contracts: List[PlanItemContract] = []
                for pos_row in open_positions:
                    sym = str(pos_row["symbol"]).strip().upper()
                    raw_pos_qty = _to_decimal(pos_row["qty"], "qty")
                    abs_qty = abs(raw_pos_qty)
                    raw_pos_side = str(pos_row.get("side") or "").strip().lower()
                    if raw_pos_side in ("short", "sell"):
                        pos_side = PlanSide.SHORT
                        pos_qty = -abs_qty
                    elif raw_pos_side in ("long", "buy"):
                        pos_side = PlanSide.LONG
                        pos_qty = abs_qty
                    else:
                        pos_side = (
                            PlanSide.LONG if raw_pos_qty > Decimal("0") else PlanSide.SHORT
                        )
                        pos_qty = raw_pos_qty

                    sym_dec = decisions_by_symbol.get(sym, "")
                    if sym in vetoed_symbols or sym_dec in (
                        "THESIS_INVALIDATED_EXIT",
                        "EXIT_AT_OPEN",
                        "VETO",
                        "VETOED",
                        "INVALIDATED",
                        "FORCE_EXIT",
                        "IMMEDIATE_EXIT",
                    ):
                        carry_over_status = "THESIS_INVALIDATED_EXIT"
                    else:
                        carry_over_status = "CARRY_OVER_EXIT_ONLY"

                    hist_row = None
                    if has_plan_items_table:
                        cur.execute(
                            """
                            SELECT i.*
                            FROM trading_plan_items i
                            JOIN trading_plans p ON p.plan_id = i.plan_id
                            WHERE p.trading_date < %s::date
                              AND (p.market = %s OR (%s IN ('US', 'US_EQUITY') AND p.market IN ('US', 'US_EQUITY')))
                              AND p.status IN ('LOCKED', 'CLOSED')
                              AND i.symbol = %s
                            ORDER BY p.trading_date DESC, i.updated_at DESC NULLS LAST
                            LIMIT 1
                            """,
                            (td, mkt, mkt, sym),
                        )
                        hist_row = cur.fetchone()
                        if hist_row is None and pos_row.get("plan_item_id") is not None:
                            cur.execute(
                                "SELECT * FROM trading_plan_items WHERE item_id = %s LIMIT 1",
                                (pos_row["plan_item_id"],),
                            )
                            hist_row = cur.fetchone()

                    if hist_row is None:
                        raise RuntimeError(
                            f"Open carry-over position for {sym} has no historical LOCKED "
                            "trading_plan_items contract in PostgreSQL (fail-closed)"
                        )

                    prot_row = None
                    if has_protective_orders_table:
                        prot_broker = str(broker or pos_row.get("broker") or "").strip()
                        if prot_broker:
                            cur.execute(
                                """
                                SELECT stop_price, status
                                FROM protective_orders
                                WHERE symbol = %s
                                  AND LOWER(broker) = LOWER(%s)
                                  AND status IN ('ACTIVE', 'WORKING', 'PENDING_NEW', 'REPLACING')
                                ORDER BY updated_at DESC NULLS LAST
                                LIMIT 1
                                """,
                                (sym, prot_broker),
                            )
                        else:
                            cur.execute(
                                """
                                SELECT stop_price, status
                                FROM protective_orders
                                WHERE symbol = %s
                                  AND status IN ('ACTIVE', 'WORKING', 'PENDING_NEW', 'REPLACING')
                                ORDER BY updated_at DESC NULLS LAST
                                LIMIT 1
                                """,
                                (sym,),
                            )
                        prot_row = cur.fetchone()

                    stop_candidates: List[Decimal] = []
                    for raw_stop in (
                        pos_row.get("stop_loss_price"),
                        prot_row.get("stop_price") if prot_row is not None else None,
                        hist_row.get("current_stop_price"),
                        hist_row.get("initial_stop_price"),
                    ):
                        if raw_stop is not None and str(raw_stop).strip() != "":
                            dec_s = _to_decimal(raw_stop, "stop_price")
                            if dec_s > Decimal("0"):
                                stop_candidates.append(dec_s)

                    row_dict = dict(hist_row)
                    hist_plan_id_str = str(row_dict.get("plan_id") or "")
                    if today_plan_id is not None:
                        row_dict["plan_id"] = today_plan_id
                    row_side = PlanSide(str(row_dict.get("side") or pos_side.value).lower())
                    if stop_candidates:
                        row_dict["current_stop_price"] = (
                            max(stop_candidates)
                            if row_side == PlanSide.LONG
                            else min(stop_candidates)
                        )
                    row_dict["approved_qty_cap"] = Decimal("0")
                    row_dict["status"] = "EXPIRED"
                    row_dict["filled_qty"] = abs_qty
                    row_dict["carry_over_status"] = carry_over_status
                    raw_trace = row_dict.get("sizing_trace") or {}
                    if isinstance(raw_trace, str):
                        try:
                            raw_trace = json.loads(raw_trace)
                        except (TypeError, ValueError):
                            raw_trace = {}
                    trace_dict = dict(raw_trace) if isinstance(raw_trace, Mapping) else {}
                    trace_dict["carry_over_status"] = carry_over_status
                    trace_dict["carry_over_position_qty"] = str(pos_qty)
                    trace_dict["carry_over_position_side"] = pos_side.value
                    trace_dict["carry_over_from_plan_id"] = hist_plan_id_str
                    trace_dict["max_new_orders"] = 0
                    row_dict["sizing_trace"] = trace_dict
                    contract = PlanItemContract.from_db_row(row_dict)
                    carry_over_contracts.append(contract)

                return carry_over_contracts
        finally:
            conn.close()

    def get_plan_item(self, item_id: UUID) -> PlanItemContract:
        """Fetch a single `PlanItemContract` by `item_id`."""
        conn = psycopg2.connect(self._dsn, cursor_factory=RealDictCursor)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM trading_plan_items WHERE item_id = %s",
                    (item_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise PlanContractValidationError(f"PlanItem {item_id} not found")
                return PlanItemContract.from_db_row(row)
        finally:
            conn.close()

    def reduce_approved_qty_cap(
        self,
        item_id: UUID,
        new_cap: Decimal,
        actor: str = "operator:127.0.0.1",
        reason: str = "Operator risk reduction",
    ) -> PlanItemContract:
        """Monotonically reduce `approved_qty_cap` on an existing locked plan item."""
        cap_dec = _to_decimal(new_cap, "new_cap")
        if cap_dec < Decimal("0"):
            raise PlanContractValidationError(f"new_cap cannot be negative: {cap_dec}")

        conn = psycopg2.connect(self._dsn, cursor_factory=RealDictCursor)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT * FROM trading_plan_items WHERE item_id = %s FOR UPDATE",
                        (item_id,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise PlanContractValidationError(f"PlanItem {item_id} not found")

                    current_contract = PlanItemContract.from_db_row(row)
                    updated_contract = current_contract.with_reduced_qty_cap(cap_dec)

                    cur.execute(
                        """
                        UPDATE trading_plan_items
                        SET approved_qty_cap = %s,
                            updated_at = NOW()
                        WHERE item_id = %s
                        """,
                        (updated_contract.risk_envelope.approved_qty_cap, item_id),
                    )

                    cur.execute(
                        """
                        INSERT INTO trading_plan_events (
                            plan_id, item_id, event_type, actor, reason, payload
                        ) VALUES (
                            %s, %s, 'QTY_CAP_REDUCED', %s, %s, %s::jsonb
                        )
                        """,
                        (
                            updated_contract.plan_id,
                            item_id,
                            actor,
                            reason,
                            json.dumps(
                                {
                                    "old_cap": str(current_contract.risk_envelope.approved_qty_cap),
                                    "new_cap": str(updated_contract.risk_envelope.approved_qty_cap),
                                }
                            ),
                        ),
                    )
                    return updated_contract
        finally:
            conn.close()

    @staticmethod
    def update_item_status_in_tx(
        cur: Any,
        item_id: UUID | str,
        new_status: str,
        *,
        actor: str = "system:telemetry_actor",
        reason: str = "",
        extra_payload: Optional[Mapping[str, Any]] = None,
    ) -> Optional[PlanItemContract]:
        """Atomically transition a `trading_plan_items.status` inside an active transaction.

        Idempotently preserves terminal states (`EXPIRED`, `CANCELLED`, `CLOSED`), records a
        `trading_plan_events` audit entry, and notifies `nautilus_plan_gate_sync`.
        """
        norm_status = str(new_status or "").strip().upper()
        if norm_status not in VALID_ITEM_STATUSES:
            raise PlanContractValidationError(
                f"Invalid plan item status {new_status!r}; expected one of {VALID_ITEM_STATUSES}"
            )
        item_uuid = item_id if isinstance(item_id, UUID) else UUID(str(item_id))

        cur.execute(
            "SELECT * FROM trading_plan_items WHERE item_id = %s FOR UPDATE",
            (item_uuid,),
        )
        row = cur.fetchone()
        if row is None:
            return None

        current_contract = PlanItemContract.from_db_row(row)
        cur_status = str(current_contract.status or "").strip().upper()
        if cur_status in ("EXPIRED", "CANCELLED", "CLOSED") or (
            cur_status == "FILLED" and norm_status == "EXPIRED"
        ):
            return current_contract
        if cur_status == norm_status:
            return current_contract

        cur.execute(
            """
            UPDATE trading_plan_items
            SET status = %s,
                updated_at = NOW()
            WHERE item_id = %s
            """,
            (norm_status, item_uuid),
        )
        cur.execute(
            """
            INSERT INTO trading_plan_events (
                plan_id, item_id, event_type, actor, reason, payload
            ) VALUES (
                %s, %s, %s, %s, %s, %s::jsonb
            )
            """,
            (
                current_contract.plan_id,
                item_uuid,
                f"ITEM_{norm_status}",
                actor,
                reason or f"Plan item status transitioned {cur_status} -> {norm_status}",
                json.dumps(
                    {
                        "symbol": current_contract.symbol,
                        "old_status": cur_status,
                        "new_status": norm_status,
                        **(dict(extra_payload) if extra_payload else {}),
                    }
                ),
            ),
        )
        cur.execute("SELECT pg_notify('nautilus_plan_gate_sync', 'trading_plan_items')")
        return replace(current_contract, status=norm_status)

    def update_item_status(
        self,
        item_id: UUID | str,
        new_status: str,
        *,
        actor: str = "system:telemetry_actor",
        reason: str = "",
    ) -> Optional[PlanItemContract]:
        """Atomically update `trading_plan_items.status` with row-level lock and `pg_notify`."""
        conn = psycopg2.connect(self._dsn, cursor_factory=RealDictCursor)
        try:
            with conn:
                with conn.cursor() as cur:
                    return self.update_item_status_in_tx(
                        cur,
                        item_id,
                        new_status,
                        actor=actor,
                        reason=reason,
                    )
        finally:
            conn.close()

    def upsert_instrument_metadata_cache(
        self,
        records: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    ) -> int:
        """Upsert authoritative Alpaca `/v2/assets/{symbol}` metadata into `instrument_metadata_cache`."""
        items: List[Mapping[str, Any]] = (
            [records] if isinstance(records, Mapping) else list(records)
        )
        if not items:
            return 0

        conn = psycopg2.connect(self._dsn, cursor_factory=RealDictCursor)
        upserted = 0
        try:
            with conn:
                with conn.cursor() as cur:
                    if not self._table_exists(cur, "instrument_metadata_cache"):
                        return 0
                    for rec in items:
                        sym = str(rec.get("symbol") or "").split(".", 1)[0].strip().upper()
                        if not sym:
                            raise PlanContractValidationError(
                                "instrument_metadata_cache record missing symbol"
                            )
                        price_increment = _to_decimal(
                            rec.get("price_increment", "0.01"), "price_increment"
                        )
                        if price_increment <= Decimal("0"):
                            raise PlanContractValidationError(
                                f"price_increment must be > 0 for {sym}, got {price_increment}"
                            )
                        price_precision = int(rec.get("price_precision", 2))
                        if not (1 <= price_precision <= 9):
                            raise PlanContractValidationError(
                                f"price_precision must be in [1, 9] for {sym}, got {price_precision}"
                            )
                        min_order_size = _to_decimal(
                            rec.get("min_order_size", "1"), "min_order_size"
                        )
                        if min_order_size <= Decimal("0"):
                            raise PlanContractValidationError(
                                f"min_order_size must be > 0 for {sym}, got {min_order_size}"
                            )
                        lot_size = _to_decimal(rec.get("lot_size", "1"), "lot_size")
                        if lot_size <= Decimal("0"):
                            raise PlanContractValidationError(
                                f"lot_size must be > 0 for {sym}, got {lot_size}"
                            )
                        raw_payload = rec.get("raw_payload")
                        if raw_payload is None:
                            raw_payload = dict(rec)
                        cur.execute(
                            """
                            INSERT INTO instrument_metadata_cache (
                                symbol, asset_id, exchange, asset_class, name,
                                status, tradable, marginable, shortable,
                                easy_to_borrow, fractionable, min_order_size,
                                price_increment, price_precision, lot_size,
                                raw_payload, fetched_at, updated_at
                            ) VALUES (
                                %s, %s, %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s,
                                %s, %s, %s,
                                %s::jsonb, NOW(), NOW()
                            )
                            ON CONFLICT (symbol) DO UPDATE SET
                                asset_id = EXCLUDED.asset_id,
                                exchange = EXCLUDED.exchange,
                                asset_class = EXCLUDED.asset_class,
                                name = EXCLUDED.name,
                                status = EXCLUDED.status,
                                tradable = EXCLUDED.tradable,
                                marginable = EXCLUDED.marginable,
                                shortable = EXCLUDED.shortable,
                                easy_to_borrow = EXCLUDED.easy_to_borrow,
                                fractionable = EXCLUDED.fractionable,
                                min_order_size = EXCLUDED.min_order_size,
                                price_increment = EXCLUDED.price_increment,
                                price_precision = EXCLUDED.price_precision,
                                lot_size = EXCLUDED.lot_size,
                                raw_payload = EXCLUDED.raw_payload,
                                fetched_at = NOW(),
                                updated_at = NOW()
                            """,
                            (
                                sym,
                                str(rec.get("asset_id") or ""),
                                str(rec.get("exchange") or ""),
                                str(rec.get("asset_class") or "us_equity"),
                                str(rec.get("name") or ""),
                                str(rec.get("status") or "active"),
                                bool(rec.get("tradable", True)),
                                bool(rec.get("marginable", False)),
                                bool(rec.get("shortable", False)),
                                bool(rec.get("easy_to_borrow", False)),
                                bool(rec.get("fractionable", False)),
                                min_order_size,
                                price_increment,
                                price_precision,
                                lot_size,
                                json.dumps(raw_payload, default=str),
                            ),
                        )
                        upserted += 1
        finally:
            conn.close()
        return upserted

    def load_instrument_metadata_cache(
        self,
        symbols: Optional[Sequence[str]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Load cached Alpaca instrument metadata from `instrument_metadata_cache` keyed by uppercase symbol."""
        norm_symbols: Optional[List[str]] = None
        if symbols is not None:
            norm_symbols = [
                str(s).split(".", 1)[0].strip().upper()
                for s in symbols
                if str(s).strip()
            ]
            if not norm_symbols:
                return {}

        conn = psycopg2.connect(self._dsn, cursor_factory=RealDictCursor)
        try:
            with conn.cursor() as cur:
                if not self._table_exists(cur, "instrument_metadata_cache"):
                    return {}
                if norm_symbols is not None:
                    cur.execute(
                        """
                        SELECT *
                        FROM instrument_metadata_cache
                        WHERE symbol = ANY(%s)
                        ORDER BY symbol ASC
                        """,
                        (norm_symbols,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT *
                        FROM instrument_metadata_cache
                        ORDER BY symbol ASC
                        """
                    )
                rows = cur.fetchall() or []
                out: Dict[str, Dict[str, Any]] = {}
                for row in rows:
                    sym = str(row.get("symbol") or "").strip().upper()
                    if sym:
                        out[sym] = dict(row)
                return out
        finally:
            conn.close()
