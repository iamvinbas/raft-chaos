"""Service-level metrics for a run, measured the way a client would see them.

Time is the simulator's virtual clock, so the numbers are exactly reproducible from a seed.
Availability is probed in fixed windows: a window is "up" if at least one client operation
succeeded in it. That is a black-box signal, the same one an external prober would give.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from .sim import RunResult

WINDOW_MS = 250
LATENCY_BUCKETS_MS = (5, 10, 25, 50, 100, 250, 500, 1000)


@dataclass
class Metrics:
    duration_ms: int
    window_ms: int
    ops_ok: int
    ops_unknown: int  # timed out: outcome unknown
    ops_rejected: int  # refused by a non-leader
    latencies_ms: list[int] = field(default_factory=list)  # successful operations, sorted
    up_windows: list[bool] = field(default_factory=list)
    max_outage_ms: int = 0
    leader_changes: int = 0
    crashes: int = 0
    partitions: int = 0

    @property
    def availability(self) -> float:
        return sum(self.up_windows) / len(self.up_windows) if self.up_windows else 1.0

    def percentile(self, q: float) -> float:
        """Nearest-rank percentile of successful-operation latency in ms (0 if no data)."""
        if not self.latencies_ms:
            return 0.0
        rank = max(1, math.ceil(q / 100 * len(self.latencies_ms)))
        return float(self.latencies_ms[rank - 1])


def compute_metrics(result: RunResult, window_ms: int = WINDOW_MS) -> Metrics:
    duration = result.config.duration_ms
    ok = sorted(
        (op.response, op.response - op.invoke)
        for op in result.history
        if op.response is not None and op.response <= duration
    )
    windows = max(1, duration // window_ms)
    up = [False] * windows
    for done, _ in ok:
        up[min(windows - 1, done // window_ms)] = True

    # Longest stretch with no successful operation, including the start and the end.
    marks = [0, *[done for done, _ in ok], duration]
    outage = max(b - a for a, b in zip(marks, marks[1:], strict=False))

    stats = result.stats
    leaders = [e for e in result.timeline if e.kind == "state" and e.detail == "leader"]
    return Metrics(
        duration_ms=duration,
        window_ms=window_ms,
        ops_ok=stats.get("ops_ok", 0),
        ops_unknown=stats.get("ops_info", 0),
        ops_rejected=stats.get("ops_fail", 0),
        latencies_ms=sorted(latency for _, latency in ok),
        up_windows=up,
        max_outage_ms=outage,
        leader_changes=len(leaders),
        crashes=stats.get("crashes", 0),
        partitions=stats.get("partitions", 0),
    )


def to_prometheus(metrics: Metrics, labels: dict[str, str] | None = None) -> str:
    """Render metrics in the Prometheus text exposition format."""

    def fmt(extra: dict[str, str] | None = None) -> str:
        merged = {**(labels or {}), **(extra or {})}
        if not merged:
            return ""
        return "{" + ",".join(f'{k}="{v}"' for k, v in sorted(merged.items())) + "}"

    lines: list[str] = []

    def metric(name: str, kind: str, help_text: str) -> None:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {kind}")

    metric("raftchaos_ops_total", "counter", "Client operations by outcome.")
    for status, value in (
        ("ok", metrics.ops_ok),
        ("unknown", metrics.ops_unknown),
        ("rejected", metrics.ops_rejected),
    ):
        lines.append(f"raftchaos_ops_total{fmt({'status': status})} {value}")

    metric("raftchaos_op_latency_ms", "histogram", "Latency of successful operations.")
    for bound in LATENCY_BUCKETS_MS:
        count = sum(1 for x in metrics.latencies_ms if x <= bound)
        lines.append(f"raftchaos_op_latency_ms_bucket{fmt({'le': str(bound)})} {count}")
    lines.append(f"raftchaos_op_latency_ms_bucket{fmt({'le': '+Inf'})} {len(metrics.latencies_ms)}")
    lines.append(f"raftchaos_op_latency_ms_sum{fmt()} {sum(metrics.latencies_ms)}")
    lines.append(f"raftchaos_op_latency_ms_count{fmt()} {len(metrics.latencies_ms)}")

    metric("raftchaos_availability_ratio", "gauge", "Share of probe windows with a success.")
    lines.append(f"raftchaos_availability_ratio{fmt()} {metrics.availability:.4f}")
    metric("raftchaos_max_outage_ms", "gauge", "Longest period without a successful operation.")
    lines.append(f"raftchaos_max_outage_ms{fmt()} {metrics.max_outage_ms}")
    metric("raftchaos_leader_changes_total", "counter", "Times a node became leader.")
    lines.append(f"raftchaos_leader_changes_total{fmt()} {metrics.leader_changes}")
    metric("raftchaos_node_crashes_total", "counter", "Injected node crashes.")
    lines.append(f"raftchaos_node_crashes_total{fmt()} {metrics.crashes}")
    metric("raftchaos_partitions_total", "counter", "Injected network partitions.")
    lines.append(f"raftchaos_partitions_total{fmt()} {metrics.partitions}")
    return "\n".join(lines) + "\n"
