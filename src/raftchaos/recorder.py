"""Record a simulated run in enough detail to replay it visually.

Recording is opt-in (`Simulator(record=True)`) because it costs memory; hunting across
thousands of seeds never pays for it. The output is plain JSON-ready data, consumed by the
web visualiser in `raftchaos.viz`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .messages import (
    Addr,
    AppendEntries,
    AppendEntriesReply,
    ClientRequest,
    ClientResponse,
    Message,
    RequestVote,
    RequestVoteReply,
)

if TYPE_CHECKING:
    from .sim import RunResult, Simulator

LOG_TAIL = 12  # log entries per node kept in each snapshot, enough to see divergence

# What happened to a message.
DELIVERED, LOST, CUT, CUT_IN_FLIGHT, RECEIVER_DOWN, PENDING = range(6)
NOMINAL_LATENCY = 8  # ms, used to animate messages that never arrive


def describe(msg: Message) -> tuple[str, object]:
    """A short type code and one detail worth showing for each message."""
    if isinstance(msg, RequestVote):
        return "RV", msg.term
    if isinstance(msg, RequestVoteReply):
        return "RVR", int(msg.vote_granted)
    if isinstance(msg, AppendEntries):
        return "AE", len(msg.entries)
    if isinstance(msg, AppendEntriesReply):
        return "AER", int(msg.success)
    if isinstance(msg, ClientRequest):
        return "REQ", " ".join(str(part) for part in msg.command[:2])
    if isinstance(msg, ClientResponse):
        return "RES", int(msg.ok)
    return "?", None


class Recorder:
    def __init__(self, sim: Simulator) -> None:
        self.sim = sim
        self.messages: list[list[Any]] = []
        self.nodes: dict[int, list[list[Any]]] = {i: [] for i in sim.ids}
        self.network: list[list[Any]] = []
        self._last_node: dict[int, tuple[Any, ...]] = {}
        self._last_net: tuple[Any, ...] | None = None

    # ---- hooks called by the simulator ---------------------------------------------------

    def on_send(self, src: Addr, dst: Addr, msg: Message, fate: int) -> int:
        kind, info = describe(msg)
        mid = len(self.messages)
        arrive = self.sim.now + NOMINAL_LATENCY if fate != PENDING else None
        self.messages.append([self.sim.now, src, dst, kind, info, arrive, fate])
        return mid

    def on_deliver(self, mid: int, fate: int) -> None:
        if 0 <= mid < len(self.messages):
            record = self.messages[mid]
            record[5] = self.sim.now
            record[6] = fate

    def on_step(self) -> None:
        now = self.sim.now
        for i in self.sim.ids:
            node = self.sim.nodes[i]
            if node is None:
                state: tuple[Any, ...] = ("down",)
                row: list[Any] = [now, "down"]
            else:
                tail = [e.term for e in node.log[-LOG_TAIL:]]
                state = (
                    node.role.value,
                    node.current_term,
                    node.commit_index,
                    node.last_index,
                    node.storage.voted_for,
                    tuple(tail),
                )
                row = [
                    now,
                    node.role.value,
                    node.current_term,
                    node.commit_index,
                    node.last_index,
                    node.storage.voted_for,
                    tail,
                ]
            if self._last_node.get(i) != state:
                self._last_node[i] = state
                self.nodes[i].append(row)

        groups = None if self.sim.groups is None else [self.sim.groups[i] for i in self.sim.ids]
        net = (tuple(groups) if groups else None, self.sim.drop_prob)
        if net != self._last_net:
            self._last_net = net
            self.network.append([now, groups, self.sim.drop_prob])

    # ---- export ----------------------------------------------------------------------------

    def export(
        self, result: RunResult, title: str, bug: str | None, profile: str
    ) -> dict[str, Any]:
        config = result.config
        for record in self.messages:
            if record[6] == PENDING:  # still in flight when the run stopped
                record[5] = record[0] + NOMINAL_LATENCY
        faults = [[e.time, e.kind, e.node, e.detail] for e in result.timeline if e.kind != "state"]
        ops = [
            [r.id, r.client, r.kind, r.key, r.value, r.result, r.invoke, r.response, r.status]
            for r in self.sim.workload.records
        ]
        violation = None
        if result.violation is not None:
            v = result.violation
            violation = {"kind": v.kind, "message": v.message, "time": v.time}
        command = f"raftchaos run --seed {result.seed} --nodes {config.n_nodes}"
        if bug:
            command += f" --bug {bug}"
        if profile != "default":
            command += f" --profile {profile}"
        return {
            "title": title,
            "seed": result.seed,
            "bug": bug,
            "profile": profile,
            "nodes": config.n_nodes,
            "clients": config.n_clients,
            "duration": config.duration_ms,
            "client_timeout": config.client_timeout,
            "end": result.end_time,
            "reproduce": command + " --trace",
            "violation": violation,
            "stats": result.stats,
            "states": [self.nodes[i] for i in self.sim.ids],
            "network": self.network,
            "messages": self.messages,
            "faults": faults,
            "ops": ops,
        }
