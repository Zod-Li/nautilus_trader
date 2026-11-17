"""Shared bounded FIFO collection primitives with optional active-item eviction pinning."""

from __future__ import annotations

from collections import OrderedDict
from typing import Any, Callable, Optional

MAX_TRACKED_ORDER_HISTORY: int = 4096


class _BoundedFifoList(list):
    """FIFO-bounded list that evicts oldest entries when capacity exceeds `max_size`."""

    def __init__(self, max_size: int = MAX_TRACKED_ORDER_HISTORY) -> None:
        super().__init__()
        self._max_size = max(1, int(max_size))

    def append(self, item: Any) -> None:
        super().append(item)
        overflow = len(self) - self._max_size
        if overflow > 0:
            del self[:overflow]

    def extend(self, items: Any) -> None:
        super().extend(items)
        overflow = len(self) - self._max_size
        if overflow > 0:
            del self[:overflow]


class _BoundedFifoSet(set):
    """FIFO-bounded set that evicts oldest entries when capacity exceeds `max_size`."""

    def __init__(
        self,
        max_size: int = MAX_TRACKED_ORDER_HISTORY,
        can_evict: Optional[Callable[[Any], bool]] = None,
    ) -> None:
        super().__init__()
        self._max_size = max(1, int(max_size))
        self._can_evict = can_evict
        self._order: OrderedDict[Any, None] = OrderedDict()

    def add(self, item: Any) -> None:
        if item in self._order:
            self._order.move_to_end(item)
            super().add(item)
            return
        attempts = len(self._order)
        while len(self._order) >= self._max_size and attempts > 0:
            attempts -= 1
            oldest = next(iter(self._order))
            if self._can_evict is not None and not self._can_evict(oldest):
                self._order.move_to_end(oldest)
                continue
            self._order.popitem(last=False)
            super().discard(oldest)
        self._order[item] = None
        super().add(item)

    def discard(self, item: Any) -> None:
        self._order.pop(item, None)
        super().discard(item)

    def remove(self, item: Any) -> None:
        self._order.pop(item, None)
        super().remove(item)

    def clear(self) -> None:
        self._order.clear()
        super().clear()


class _BoundedFifoDict(OrderedDict):
    """FIFO-bounded dictionary that evicts oldest entries when capacity exceeds `max_size`."""

    def __init__(
        self,
        max_size: int = MAX_TRACKED_ORDER_HISTORY,
        can_evict: Optional[Callable[[Any, Any], bool]] = None,
    ) -> None:
        super().__init__()
        self._max_size = max(1, int(max_size))
        self._can_evict = can_evict

    def __setitem__(self, key: Any, value: Any) -> None:
        if key in self:
            super().__setitem__(key, value)
            self.move_to_end(key)
            return
        attempts = len(self)
        while len(self) >= self._max_size and attempts > 0:
            attempts -= 1
            oldest_key, oldest_val = next(iter(self.items()))
            if self._can_evict is not None and not self._can_evict(oldest_key, oldest_val):
                self.move_to_end(oldest_key)
                continue
            self.popitem(last=False)
        super().__setitem__(key, value)

    def setdefault(self, key: Any, default: Any = None) -> Any:
        if key in self:
            self.move_to_end(key)
            return self[key]
        self[key] = default
        return default
