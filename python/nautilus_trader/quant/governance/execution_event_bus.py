"""Non-blocking Kafka execution event publisher for Nautilus real-time UI streaming.

Publishes normalized execution lifecycle events (`order_submitted`, `order_accepted`,
`order_updated`, `order_canceled`, `order_rejected`, `order_denied`, `order_expired`,
`order_filled`, `position_opened`, `position_changed`, `position_closed`,
`account_updated`, `protective_order_updated`, `protective_order_missing`) to the
`exec.nautilus.events` Kafka topic from a dedicated daemon worker thread so Nautilus's
critical execution loop never blocks or raises when Kafka is unavailable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
import os
import queue
import threading
import time
from typing import Any, Callable, Dict, Optional, Protocol

logger = logging.getLogger(__name__)

DEFAULT_EXECUTION_EVENT_TOPIC = "exec.nautilus.events"
DEFAULT_EXECUTION_EVENT_SCHEMA_VERSION = 1


def normalize_execution_event_type(raw: str) -> str:
    """Normalizes an uppercase/mixed plan event type to lowercase wire event name."""
    cleaned = str(raw or "").strip().lower()
    if cleaned in {"order_cancelled", "canceled", "cancelled"}:
        return "order_canceled"
    return cleaned or "order_update"


@dataclass(frozen=True)
class ExecutionEvent:
    """Immutable Kafka execution event payload shared with `control-plane`."""

    event_type: str
    symbol: str = ""
    broker: str = "alpaca"
    environment: str = "PAPER"
    market: str = "US_EQUITY"
    payload: Dict[str, Any] = field(default_factory=dict)
    event_id: str = ""
    ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    schema_version: int = DEFAULT_EXECUTION_EVENT_SCHEMA_VERSION

    def partition_key(self) -> bytes:
        sym = str(self.symbol or "").strip().upper()
        return sym.encode("utf-8") if sym else b"__ACCOUNT__"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": int(self.schema_version),
            "event_id": str(self.event_id or ""),
            "event_type": normalize_execution_event_type(self.event_type),
            "ts_ms": int(self.ts_ms),
            "broker": str(self.broker or "alpaca").strip().lower(),
            "environment": str(self.environment or "PAPER").strip().upper(),
            "market": str(self.market or "US_EQUITY").strip().upper(),
            "symbol": str(self.symbol or "").strip().upper(),
            "payload": dict(self.payload) if isinstance(self.payload, dict) else {},
        }

    def to_json_bytes(self) -> bytes:
        return json.dumps(
            self.to_dict(),
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")


class ExecutionEventPublisher(Protocol):
    """Protocol for non-blocking execution event publishers."""

    def publish(self, event: ExecutionEvent) -> bool:
        ...

    def close(self, timeout_secs: float = 2.0) -> None:
        ...


class KafkaExecutionEventPublisher:
    """Async daemon-worker Kafka producer for `exec.nautilus.events`.

    - `publish()` is O(1) non-blocking via bounded `queue.Queue.put_nowait`.
    - When queue is full or Kafka is temporarily unreachable, errors are rate-logged
      and never propagated back to `PostgresTelemetryActor` or trading strategies.
    """

    def __init__(
        self,
        brokers: str,
        topic: str = DEFAULT_EXECUTION_EVENT_TOPIC,
        max_queue_size: int = 4096,
        producer_factory: Optional[Callable[[list[str]], Any]] = None,
    ) -> None:
        raw_brokers = str(brokers or "").strip()
        self._brokers = [b.strip() for b in raw_brokers.split(",") if b.strip()]
        self._topic = str(topic or DEFAULT_EXECUTION_EVENT_TOPIC).strip() or DEFAULT_EXECUTION_EVENT_TOPIC
        self._max_queue_size = max(16, int(max_queue_size))
        self._producer_factory = producer_factory
        self._queue: "queue.Queue[Optional[ExecutionEvent]]" = queue.Queue(
            maxsize=self._max_queue_size
        )
        self._stop_event = threading.Event()
        self._closed = False
        self._lock = threading.Lock()
        self._dropped_count = 0
        self._sent_count = 0
        self._error_count = 0
        self._last_warn_ts = 0.0
        self._producer: Any = None
        self._worker = threading.Thread(
            target=self._run_loop,
            name=f"nautilus-exec-kafka-{self._topic}",
            daemon=True,
        )
        self._worker.start()

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def dropped_count(self) -> int:
        return self._dropped_count

    @property
    def sent_count(self) -> int:
        return self._sent_count

    def _warn_rate_limited(self, msg: str, *args: Any) -> None:
        now = time.monotonic()
        if now - self._last_warn_ts >= 10.0:
            self._last_warn_ts = now
            logger.warning(msg, *args)

    def publish(self, event: ExecutionEvent) -> bool:
        if self._closed or self._stop_event.is_set() or event is None:
            return False
        try:
            self._queue.put_nowait(event)
            return True
        except queue.Full:
            try:
                _ = self._queue.get_nowait()
                self._dropped_count += 1
            except queue.Empty:
                pass
            try:
                self._queue.put_nowait(event)
                self._warn_rate_limited(
                    "KafkaExecutionEventPublisher queue full (max=%d, dropped=%d); dropped oldest event",
                    self._max_queue_size,
                    self._dropped_count,
                )
                return True
            except queue.Full:
                self._dropped_count += 1
                return False
        except Exception as exc:
            self._warn_rate_limited("KafkaExecutionEventPublisher publish failed: %s", exc)
            return False

    def _ensure_producer(self) -> Any:
        if self._producer is not None:
            return self._producer
        if not self._brokers:
            return None
        if self._producer_factory is not None:
            self._producer = self._producer_factory(self._brokers)
            return self._producer
        from kafka import KafkaProducer  # type: ignore[import-untyped]

        self._producer = KafkaProducer(
            bootstrap_servers=self._brokers,
            acks=1,
            linger_ms=5,
            request_timeout_ms=5000,
            max_block_ms=3000,
            retries=1,
        )
        return self._producer

    def _reset_producer(self) -> None:
        prod = self._producer
        self._producer = None
        if prod is not None:
            close_fn = getattr(prod, "close", None)
            if callable(close_fn):
                try:
                    close_fn(timeout=1.0)
                except TypeError:
                    try:
                        close_fn()
                    except Exception:
                        pass
                except Exception:
                    pass

    def _send_one(self, event: ExecutionEvent) -> None:
        backoff_secs = 0.5
        while not self._stop_event.is_set():
            try:
                prod = self._ensure_producer()
                if prod is None:
                    return
                key = event.partition_key()
                value = event.to_json_bytes()
                fut = prod.send(self._topic, value=value, key=key)
                if fut is not None and hasattr(fut, "get"):
                    fut.get(timeout=5.0)
                self._sent_count += 1
                return
            except Exception as exc:
                self._error_count += 1
                self._warn_rate_limited(
                    "KafkaExecutionEventPublisher send error (topic=%s errors=%d): %s",
                    self._topic,
                    self._error_count,
                    exc,
                )
                self._reset_producer()
                if self._stop_event.wait(timeout=backoff_secs):
                    return
                backoff_secs = min(10.0, backoff_secs * 2.0)

    def _run_loop(self) -> None:
        try:
            while not self._stop_event.is_set() or not self._queue.empty():
                try:
                    item = self._queue.get(timeout=0.25)
                except queue.Empty:
                    continue
                if item is None:
                    break
                self._send_one(item)
        finally:
            self._reset_producer()

    def flush(self, timeout_secs: float = 2.0) -> bool:
        deadline = time.monotonic() + max(0.1, float(timeout_secs))
        while not self._queue.empty() and time.monotonic() < deadline:
            time.sleep(0.01)
        prod = self._producer
        if prod is not None and callable(getattr(prod, "flush", None)):
            try:
                prod.flush(timeout=max(0.1, deadline - time.monotonic()))
            except TypeError:
                try:
                    prod.flush()
                except Exception:
                    pass
            except Exception:
                pass
        return self._queue.empty()

    def close(self, timeout_secs: float = 2.0) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self.flush(timeout_secs=min(1.0, max(0.1, float(timeout_secs) * 0.5)))
        self._stop_event.set()
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass
        self._worker.join(timeout=max(0.2, float(timeout_secs)))
        self._reset_producer()


def build_execution_event_publisher(
    brokers: Optional[str] = None,
    topic: Optional[str] = None,
) -> Optional[KafkaExecutionEventPublisher]:
    """Builds a `KafkaExecutionEventPublisher` when `brokers` is configured, else `None`."""
    resolved_brokers = str(
        brokers if brokers is not None else os.environ.get("KAFKA_BROKERS", "")
    ).strip()
    if not resolved_brokers:
        return None
    resolved_topic = str(
        topic
        if topic is not None
        else os.environ.get("KAFKA_EXECUTION_TOPIC", DEFAULT_EXECUTION_EVENT_TOPIC)
    ).strip() or DEFAULT_EXECUTION_EVENT_TOPIC
    return KafkaExecutionEventPublisher(brokers=resolved_brokers, topic=resolved_topic)
