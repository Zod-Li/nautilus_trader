"""PostgreSQL repository for atomic locking and loading of Isomorphic `PlanItemContract` items."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal
import json
from typing import Any, Dict, List, Mapping, Optional, Sequence
from uuid import UUID, uuid4

import psycopg2
from psycopg2.extras import RealDictCursor, register_uuid

from evolution_engine.config_center.envelope import Envelope, load_envelope
from evolution_engine.db.dsn import resolve_database_dsn
from nautilus_trader.quant.contracts.models import (
    PlanContractValidationError,
    PlanItemContract,
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
                        if existing_status in ("LOCKED", "CLOSED") and not items:
                            return existing_id
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
                        cur.execute(
                            """
                            INSERT INTO trading_plan_items (
                                item_id, plan_id, kind, symbol, broker, asset_class,
                                strategy_id, archetype, side, trigger_spec,
                                target_qty, target_notional_usd, risk_budget_usd,
                                initial_stop_price, take_profit_price, valid_until,
                                sizing_trace, status, reserved_notional_usd, filled_qty,
                                current_stop_price, approved_qty_cap
                            ) VALUES (
                                %s, %s, 'NEW_ENTRY', %s, %s, %s,
                                %s, %s, %s, %s::jsonb,
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

    def load_latest_plan_lock_event_payload(
        self,
        trading_date: date,
        market: str = "US_EQUITY",
    ) -> Dict[str, Any]:
        """Loads the latest `PLAN_LOCKED` event payload for `(trading_date, market)`."""
        conn = psycopg2.connect(self._dsn, cursor_factory=RealDictCursor)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT e.payload, p.rejections
                    FROM trading_plans p
                    LEFT JOIN trading_plan_events e
                      ON e.plan_id = p.plan_id AND e.event_type = 'PLAN_LOCKED'
                    WHERE p.trading_date = %s AND p.market = %s
                    ORDER BY e.ts DESC NULLS LAST, p.locked_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (trading_date, market),
                )
                row = cur.fetchone()
                if row is None:
                    return {}
                raw_payload = row.get("payload")
                if isinstance(raw_payload, str):
                    try:
                        parsed = json.loads(raw_payload)
                    except Exception:
                        parsed = {}
                elif isinstance(raw_payload, Mapping):
                    parsed = dict(raw_payload)
                else:
                    parsed = {}
                raw_rej = row.get("rejections")
                if isinstance(raw_rej, str):
                    try:
                        rej_list = json.loads(raw_rej)
                    except Exception:
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
        trading_date: date,
        market: str = "US_EQUITY",
    ) -> List[PlanItemContract]:
        """Load all `PlanItemContract` records belonging to the `LOCKED` plan for `(trading_date, market)`."""
        conn = psycopg2.connect(self._dsn, cursor_factory=RealDictCursor)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT i.*
                    FROM trading_plan_items i
                    JOIN trading_plans p ON p.plan_id = i.plan_id
                    WHERE p.trading_date = %s
                      AND p.market = %s
                      AND p.status = 'LOCKED'
                    ORDER BY i.symbol ASC
                    """,
                    (trading_date, market),
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
