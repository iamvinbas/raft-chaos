"""Raft safety invariants, checked after every simulated event.

These are the properties from Figure 3 of the Raft paper. Violating any of them is a bug in
the protocol implementation, never an acceptable outcome of network faults.

State Machine Safety is checked on the data as well as on the log: the checker replays the
committed history into a reference store, and every node's state after each apply, and every
snapshot a node takes or installs, must equal the reference at that index.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .messages import LogEntry, Snapshot
from .node import RaftNode, Role
from .statemachine import Frozen, KvStore, freeze_snapshot

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
        # The committed history replayed in order: the state every node must have at each index.
        self._reference = KvStore()
        self.reference_states: dict[int, Frozen] = {}

    def _fail(self, sim: Simulator, kind: str, message: str) -> None:
        raise InvariantViolation(Violation(kind, message, sim.now))

    def on_apply(self, sim: Simulator, node_id: int, index: int, entry: LogEntry) -> None:
        known = self.committed.get(index)
        if known is None:
            # A node applies in order, so every earlier index is already recorded.
            top = max(n.current_term for n in sim.live_nodes())
            self.committed[index] = (entry, top)
            self._reference.apply(entry)
            self.reference_states[index] = self._reference.freeze()
        elif known[0] != entry:
            self._fail(
                sim,
                "state-machine-safety",
                f"node {node_id} applied {describe_entry(entry)} at index {index}, "
                f"but {describe_entry(known[0])} was already applied there",
            )
        node = sim.nodes.get(node_id)
        if node is not None and node.kv.freeze() != self.reference_states[index]:
            self._fail(
                sim,
                "state-machine-safety",
                f"node {node_id}'s data after index {index} differs from the committed history",
            )

    def on_snapshot(self, sim: Simulator, node_id: int, snap: Snapshot, how: str) -> None:
        verb = "took" if how == "take" else "installed"
        expected = self.reference_states.get(snap.last_index)
        known = self.committed.get(snap.last_index)
        if expected is None or known is None:
            self._fail(
                sim,
                "state-machine-safety",
                f"node {node_id} {verb} a snapshot up to index {snap.last_index}, "
                "which no node has applied",
            )
            return
        if known[0].term != snap.last_term or freeze_snapshot(snap) != expected:
            self._fail(
                sim,
                "state-machine-safety",
                f"node {node_id} {verb} a snapshot up to index {snap.last_index} whose state "
                "differs from the committed history",
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
            if leader.current_term <= seen_term or index <= leader.snap_index:
                continue  # entries inside a snapshot are checked when it is taken or installed
            if index > leader.last_index or leader.entry_at(index) != entry:
                self._fail(
                    sim,
                    "leader-completeness",
                    f"leader {leader.id} of term {leader.current_term} is missing "
                    f"committed entry {describe_entry(entry)} at index {index}",
                )

    def _check_log_matching(self, sim: Simulator, a: RaftNode, b: RaftNode) -> None:
        # Only the entries both nodes still hold can be compared; anything below a snapshot is
        # committed and covered by the snapshot checks.
        low = max(a.snap_index, b.snap_index)
        for i in range(min(a.last_index, b.last_index), low, -1):
            if a.term_at(i) == b.term_at(i):
                ours = a.log[low - a.snap_index : i - a.snap_index]
                theirs = b.log[low - b.snap_index : i - b.snap_index]
                if ours != theirs:
                    self._fail(
                        sim,
                        "log-matching",
                        f"nodes {a.id} and {b.id} share term at index {i} "
                        f"but their logs differ before it",
                    )
                return
