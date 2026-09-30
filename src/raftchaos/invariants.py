"""Raft safety invariants, checked after every simulated event.

These are the properties from Figure 3 of the Raft paper. Violating any of them is a bug in
the protocol implementation, never an acceptable outcome of network faults.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .messages import LogEntry
from .node import RaftNode, Role

if TYPE_CHECKING:
    from .sim import Simulator


@dataclass(frozen=True)
class Violation:
    kind: str
    message: str
    time: int

    def __str__(self) -> str:
        return f"[{self.time}ms] {self.kind}: {self.message}"


def describe_entry(entry: LogEntry) -> str:
    """A short, readable form of a log entry, such as `put x=45 (term 18)`."""
    command = entry.command
    if command[0] == "put":
        what = f"put {command[1]}={command[2]}"
    elif command[0] == "get":
        what = f"get {command[1]}"
    else:
        what = str(command[0])
    return f"{what} (term {entry.term})"


class InvariantViolation(Exception):
    def __init__(self, violation: Violation) -> None:
        super().__init__(str(violation))
        self.violation = violation


class InvariantChecker:
    def __init__(self) -> None:
        self.leader_of_term: dict[int, int] = {}
        # log index -> (entry, highest term seen anywhere when it was first applied)
        self.committed: dict[int, tuple[LogEntry, int]] = {}

    def _fail(self, sim: Simulator, kind: str, message: str) -> None:
        raise InvariantViolation(Violation(kind, message, sim.now))

    def on_apply(self, sim: Simulator, node_id: int, index: int, entry: LogEntry) -> None:
        known = self.committed.get(index)
        if known is None:
            top = max(n.current_term for n in sim.live_nodes())
            self.committed[index] = (entry, top)
        elif known[0] != entry:
            self._fail(
                sim,
                "state-machine-safety",
                f"node {node_id} applied {describe_entry(entry)} at index {index}, "
                f"but {describe_entry(known[0])} was already applied there",
            )

    def check(self, sim: Simulator) -> None:
        nodes = sim.live_nodes()
        for node in nodes:
            if node.role is Role.LEADER:
                self._check_election_safety(sim, node)
                self._check_leader_completeness(sim, node)
        for i, a in enumerate(nodes):
            for b in nodes[i + 1 :]:
                self._check_log_matching(sim, a, b)

    def _check_election_safety(self, sim: Simulator, node: RaftNode) -> None:
        other = self.leader_of_term.setdefault(node.current_term, node.id)
        if other != node.id:
            self._fail(
                sim,
                "election-safety",
                f"nodes {other} and {node.id} both lead term {node.current_term}",
            )

    def _check_leader_completeness(self, sim: Simulator, leader: RaftNode) -> None:
        for index, (entry, seen_term) in self.committed.items():
            if leader.current_term <= seen_term:
                continue
            if index > leader.last_index or leader.log[index - 1] != entry:
                self._fail(
                    sim,
                    "leader-completeness",
                    f"leader {leader.id} of term {leader.current_term} is missing "
                    f"committed entry {describe_entry(entry)} at index {index}",
                )

    def _check_log_matching(self, sim: Simulator, a: RaftNode, b: RaftNode) -> None:
        for i in range(min(a.last_index, b.last_index), 0, -1):
            if a.log[i - 1].term == b.log[i - 1].term:
                if a.log[:i] != b.log[:i]:
                    self._fail(
                        sim,
                        "log-matching",
                        f"nodes {a.id} and {b.id} share term at index {i} "
                        f"but their logs differ before it",
                    )
                return
