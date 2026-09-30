"""Controlled experiments: one precise fault, many seeds, with and without a protocol change.

The PreVote experiment isolates one follower, heals the network, and measures what the
rejoining node does to a cluster that was healthy the whole time.
"""

from __future__ import annotations

from dataclasses import dataclass
from statistics import fmean

from .node import RaftConfig
from .sim import RunResult, SimConfig, run_simulation

ISOLATE_AT = 1500
HEAL_AT = 4500
DURATION = 7000


def isolation_config(pre_vote: bool, nodes: int = 3) -> SimConfig:
    return SimConfig(
        n_nodes=nodes,
        nemesis=False,
        duration_ms=DURATION,
        script=((ISOLATE_AT, "isolate_follower"), (HEAL_AT, "heal")),
        raft=RaftConfig(pre_vote=pre_vote),
    )


@dataclass(frozen=True)
class IsolationRun:
    ok: bool
    isolated: int
    term_before: int  # the cluster's term when the follower was cut off
    isolated_term_at_heal: int
    leader_changes_after_heal: int
    outage_after_heal_ms: int  # longest gap without a successful client operation


def measure(result: RunResult) -> IsolationRun:
    states = [e for e in result.timeline if e.kind == "state"]
    cut = next(e for e in result.timeline if e.kind == "partition")
    isolated = int(cut.detail.split("[", 1)[1].split("]", 1)[0])

    def term_of(node: int, at: int) -> int:
        seen = [e.term for e in states if e.node == node and e.time <= at and e.detail != "down"]
        return seen[-1] if seen else 0

    term_before = max(term_of(n, ISOLATE_AT) for n in range(result.config.n_nodes))
    changes = sum(1 for e in states if e.detail == "leader" and HEAL_AT <= e.time <= DURATION)
    done = sorted(
        op.response
        for op in result.history
        if op.response is not None and HEAL_AT <= op.response <= DURATION
    )
    marks = [HEAL_AT, *done, DURATION]
    outage = max(b - a for a, b in zip(marks, marks[1:], strict=False))
    return IsolationRun(
        result.ok, isolated, term_before, term_of(isolated, HEAL_AT), changes, outage
    )


@dataclass(frozen=True)
class Summary:
    pre_vote: bool
    runs: int
    safe: int
    disrupted: int  # runs where the rejoining node forced at least one election
    mean_term_inflation: float  # isolated node's term at heal minus the term before
    mean_outage_ms: float
    max_outage_ms: int


def run_isolation(seeds: range, pre_vote: bool, nodes: int = 3) -> Summary:
    runs = [measure(run_simulation(s, isolation_config(pre_vote, nodes))) for s in seeds]
    return Summary(
        pre_vote,
        len(runs),
        sum(r.ok for r in runs),
        sum(r.leader_changes_after_heal > 0 for r in runs),
        fmean(r.isolated_term_at_heal - r.term_before for r in runs),
        fmean(r.outage_after_heal_ms for r in runs),
        max(r.outage_after_heal_ms for r in runs),
    )


def render(summaries: list[Summary]) -> str:
    rows = [
        f"{'':12} {'safe':>7} {'disrupted':>10} {'term +':>7} {'outage avg':>11} {'outage max':>11}"
    ]
    for s in summaries:
        name = "PreVote" if s.pre_vote else "no PreVote"
        rows.append(
            f"{name:12} {s.safe:>3}/{s.runs:<3} {s.disrupted:>6}/{s.runs:<3} "
            f"{s.mean_term_inflation:>7.1f} {s.mean_outage_ms:>9.0f}ms {s.max_outage_ms:>9}ms"
        )
    return "\n".join(rows)
