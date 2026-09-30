"""Anomaly detection over per-window service metrics, scored against the injected faults.

The detector is deliberately simple: a robust z-score (median and MAD) per window on the
number of successful operations and on their mean latency. Because the simulator knows which
faults it injected, every anomaly can be attributed to the faults active around it, and the
detector can be checked on fault-free runs, where it should stay quiet.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass

from .sim import RunResult, TimelineEvent

WINDOW_MS = 500
THRESHOLD = 3.5  # modified z-score above which a window is flagged


@dataclass(frozen=True)
class Window:
    start: int
    ok_count: int
    mean_latency: float


@dataclass(frozen=True)
class Anomaly:
    start: int
    end: int
    reason: str
    score: float


@dataclass(frozen=True)
class FaultInterval:
    kind: str
    start: int
    end: int


@dataclass(frozen=True)
class Detection:
    anomalies: list[Anomaly]
    causes: list[list[FaultInterval]]  # faults active near each anomaly, same order
    faults: list[FaultInterval]
    windows: int

    @property
    def flag_rate(self) -> float:
        return len(self.anomalies) / self.windows if self.windows else 0.0


def build_windows(result: RunResult, window_ms: int = WINDOW_MS) -> list[Window]:
    duration = result.config.duration_ms
    count = max(1, duration // window_ms)
    buckets: list[list[int]] = [[] for _ in range(count)]
    for op in result.history:
        if op.response is not None and op.response < duration:
            buckets[min(count - 1, op.response // window_ms)].append(op.response - op.invoke)
    return [
        Window(i * window_ms, len(b), statistics.fmean(b) if b else 0.0)
        for i, b in enumerate(buckets)
    ]


def _robust_z(values: list[float]) -> list[float]:
    median = statistics.median(values)
    mad = statistics.median(abs(v - median) for v in values)
    if mad == 0:
        return [0.0 for _ in values]
    return [0.6745 * (v - median) / mad for v in values]


def detect(result: RunResult, window_ms: int = WINDOW_MS) -> list[Anomaly]:
    windows = build_windows(result, window_ms)
    if len(windows) < 4:
        return []
    ok_z = _robust_z([float(w.ok_count) for w in windows])
    latency_z = _robust_z([w.mean_latency for w in windows])
    found = []
    for w, zc, zl in zip(windows, ok_z, latency_z, strict=True):
        if zc <= -THRESHOLD or (w.ok_count == 0 and zc < 0):
            found.append(Anomaly(w.start, w.start + window_ms, "throughput drop", zc))
        elif zl >= THRESHOLD:
            found.append(Anomaly(w.start, w.start + window_ms, "latency spike", zl))
    return found


def fault_intervals(timeline: list[TimelineEvent], end: int) -> list[FaultInterval]:
    """Turn injection events into intervals during which the fault was active."""
    intervals: list[FaultInterval] = []
    open_network: tuple[str, int] | None = None
    open_crash: dict[int, int] = {}
    flaky_since: int | None = None

    def close_network(at: int) -> None:
        nonlocal open_network
        if open_network is not None:
            intervals.append(FaultInterval(open_network[0], open_network[1], at))
            open_network = None

    for e in timeline:
        if e.time > end:
            break
        if e.kind in ("partition", "isolate"):
            close_network(e.time)
            open_network = (e.kind, e.time)
        elif e.kind == "heal":
            close_network(e.time)
            if flaky_since is not None:
                intervals.append(FaultInterval("flaky", flaky_since, e.time))
                flaky_since = None
        elif e.kind == "flaky":
            drop = float(e.detail.rsplit(" ", 1)[-1])
            if flaky_since is not None:
                intervals.append(FaultInterval("flaky", flaky_since, e.time))
                flaky_since = None
            if drop >= 0.1:
                flaky_since = e.time
        elif e.kind in ("crash", "torn", "vote-crash") and e.node is not None:
            open_crash[e.node] = e.time
        elif e.kind == "restart" and e.node is not None and e.node in open_crash:
            intervals.append(FaultInterval("crash", open_crash.pop(e.node), e.time))
    close_network(end)
    if flaky_since is not None:
        intervals.append(FaultInterval("flaky", flaky_since, end))
    for since in open_crash.values():
        intervals.append(FaultInterval("crash", since, end))
    return sorted(intervals, key=lambda f: f.start)


def analyze(result: RunResult, window_ms: int = WINDOW_MS, grace_ms: int = 500) -> Detection:
    """Flag anomalous windows and name the injected faults that were active around each.

    `grace_ms` covers the fact that a fault's effect outlasts it (elections, retries).
    Under chaos most windows are near some fault, so attribution says little on its own;
    the meaningful check is the flag rate with and without faults (see the README).
    """
    anomalies = detect(result, window_ms)
    faults = fault_intervals(result.timeline, result.config.duration_ms)
    causes = [
        [f for f in faults if f.start < a.end and a.start < f.end + grace_ms] for a in anomalies
    ]
    windows = max(1, result.config.duration_ms // window_ms)
    return Detection(anomalies, causes, faults, windows)
