"""Tiny in-process metrics (Prometheus text format) so no extra dependency is needed.

Counters and histograms are keyed by their label values. One process = one registry; `GET /metrics` renders it.
"""

import threading
from bisect import bisect_left
from typing import Dict, Iterable, Optional, Tuple

_lock = threading.Lock()


class Counter:
    def __init__(self, name: str, doc: str, labels: Iterable[str] = ()):
        self.name, self.doc, self.labels = name, doc, tuple(labels)
        self.values: Dict[Tuple[str, ...], float] = {}
        REGISTRY.append(self)

    def inc(self, amount: float = 1, **labels: str) -> None:
        key = tuple(str(labels.get(l, "")) for l in self.labels)
        with _lock:
            self.values[key] = self.values.get(key, 0) + amount

    def get(self, **labels: str) -> float:
        return self.values.get(tuple(str(labels.get(l, "")) for l in self.labels), 0)

    def render(self) -> list[str]:
        out = [f"# HELP {self.name} {self.doc}", f"# TYPE {self.name} counter"]
        for key, v in sorted(self.values.items()):
            out.append(f"{self.name}{_fmt(self.labels, key)} {v}")
        return out


class Gauge(Counter):
    def set(self, value: float, **labels: str) -> None:
        key = tuple(str(labels.get(l, "")) for l in self.labels)
        with _lock:
            self.values[key] = value

    def render(self) -> list[str]:
        out = super().render()
        out[1] = f"# TYPE {self.name} gauge"
        return out


class Histogram:
    BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.25, 0.5, 1.0, 2.5)

    def __init__(self, name: str, doc: str):
        self.name, self.doc = name, doc
        self.counts = [0] * (len(self.BUCKETS) + 1)
        self.total = 0.0
        self.n = 0
        REGISTRY.append(self)

    def observe(self, seconds: float) -> None:
        with _lock:
            self.counts[bisect_left(self.BUCKETS, seconds)] += 1
            self.total += seconds
            self.n += 1

    def quantile(self, q: float) -> Optional[float]:
        """Upper bound of the bucket containing quantile q (None when empty)."""
        if not self.n:
            return None
        target, seen = q * self.n, 0
        for i, c in enumerate(self.counts):
            seen += c
            if seen >= target:
                return self.BUCKETS[i] if i < len(self.BUCKETS) else float("inf")
        return float("inf")

    def render(self) -> list[str]:
        out = [f"# HELP {self.name} {self.doc}", f"# TYPE {self.name} histogram"]
        cum = 0
        for i, b in enumerate(self.BUCKETS):
            cum += self.counts[i]
            out.append(f'{self.name}_bucket{{le="{b}"}} {cum}')
        out.append(f'{self.name}_bucket{{le="+Inf"}} {self.n}')
        out.append(f"{self.name}_sum {self.total}")
        out.append(f"{self.name}_count {self.n}")
        return out


def _fmt(labels: tuple, key: tuple) -> str:
    if not labels:
        return ""
    return "{" + ",".join(f'{l}="{v}"' for l, v in zip(labels, key)) + "}"


REGISTRY: list = []


def render_all() -> str:
    lines: list[str] = []
    for m in REGISTRY:
        lines.extend(m.render())
    return "\n".join(lines) + "\n"


# --- billing integration (plan P6, task 9) ---
billing_reserve_total = Counter("billing_reserve_total", "CheckAndReserve outcomes", ("mode", "decision", "reason"))
billing_reserve_seconds = Histogram("billing_reserve_seconds", "CheckAndReserve latency (seconds)")
billing_breaker_open = Gauge("billing_breaker_open", "1 while the billing circuit breaker is open")
billing_degraded_messages_total = Counter("billing_degraded_messages_total", "Messages allowed in degraded mode")
billing_release_total = Counter("billing_release_total", "Release calls", ("reason",))
plan_limit_denied_total = Counter("plan_limit_denied_total", "Service-owned plan limit refusals", ("limit",))
