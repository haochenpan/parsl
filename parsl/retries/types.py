from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Union


@dataclass(frozen=True)
class RetryPatch:
    """A runtime-only callable patch to apply on the next retry attempt."""

    func: Callable
    patch_id: str
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RetryDirective:
    """Retry decision that can optionally carry a replacement callable."""

    cost: float
    patch: RetryPatch | None = None
    reason: str | None = None


RetryDecision = Union[float, RetryDirective]
