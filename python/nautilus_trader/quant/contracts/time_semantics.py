"""Canonical time semantics and dual-clock expiry constants (Zero Magic Numbers R9).

Strictly separates:
- Event-Time admission (`signal_ts_ns <= valid_until_ns`) evaluated identically by strategies
  and Rust `PlanGate` in both `IsomorphicBacktestRunner` and `LiveNode`.
- Wall-Clock expiry finalization (`valid_until_ns + BAR_FINALIZATION_GRACE_NS`), explicitly derived
  from `sip-feeder` synthetic bar completion watermark (`+3s`) plus Kafka/`nautilus-datainfra`
  delivery budget (`<= 2s`), giving a strict `+5s` finalization bound.
"""

from __future__ import annotations

# Authoritative upstream source: `sip-feeder/crates/sip-feeder/src/pipeline/coordinator.rs`
# 1-second RTH `+3s` watermark evaluation loop for bar completion.
SIP_FEEDER_WATERMARK_DELAY_SECS: int = 3

# Authoritative transport & rollup budget: Kafka partition fetch + DataInfra decode + 15m bucket rollup.
KAFKA_DATAINFRA_DELIVERY_BUDGET_SECS: int = 2

# Strictly derived wall-clock bar-finalization grace window (`+5s`, zero magic numbers R9).
BAR_FINALIZATION_GRACE_SECS: int = (
    SIP_FEEDER_WATERMARK_DELAY_SECS + KAFKA_DATAINFRA_DELIVERY_BUDGET_SECS
)
BAR_FINALIZATION_GRACE_NS: int = BAR_FINALIZATION_GRACE_SECS * 1_000_000_000


def compute_wall_clock_expiry_finalize_ns(valid_until_ns: int) -> int:
    """Return the authoritative wall-clock finalization timestamp (`valid_until_ns + 5s`)."""
    return int(valid_until_ns) + BAR_FINALIZATION_GRACE_NS
