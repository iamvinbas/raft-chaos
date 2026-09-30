"""Deterministic discrete-event simulator for a Raft cluster under faults.

Everything is driven by one seed: network latency, drops, duplication, partitions, crashes
and client behaviour. Same seed, same config, same code gives the exact same run, so any
failure is reproducible with `raftchaos run --seed N`.
"""

from __future__ import annotations

import heapq
import random
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from .bugs import Bugs
from .invariants import InvariantChecker, InvariantViolation, Violation
from .linearizability import Op, find_violation
from .messages import Addr, AppendEntries, ClientResponse, Message, RequestVote
from .node import RaftConfig, RaftNode, Role, Storage
from .recorder import CUT, CUT_IN_FLIGHT, DELIVERED, LOST, PENDING, RECEIVER_DOWN, Recorder
from .workload import Workload


@dataclass(frozen=True)
class SimConfig:
    n_nodes: int = 3
    duration_ms: int = 8000  # faults and load run this long
    settle_ms: int = 4000  # then the network heals and the cluster must converge
    tick_ms: int = 10
    latency_min: int = 1
    latency_max: int = 15
    drop_prob: float = 0.02
    dup_prob: float = 0.01
    n_clients: int = 3
    client_timeout: int = 400
    keys: tuple[str, ...] = ("x", "y")
    nemesis: bool = True
    # Targeted faults, off by default. Each fires on an event instead of on a timer.
    crash_on_vote_prob: float = 0.0  # crash a node right after it grants a vote
    torn_broadcast_prob: float = 0.0  # leader crashes after its append reaches only one peer
    raft: RaftConfig = field(default_factory=RaftConfig)
    bugs: Bugs = field(default_factory=Bugs)

    @classmethod
    def adversarial(cls, **overrides: Any) -> SimConfig:
        """Targeted faults, tight election timeouts and one-entry appends.

        Finds the bugs that random faults rarely hit: a node that forgets its vote after a
        crash, and the Raft paper's Figure 8 scenario.
        """
        params: dict[str, Any] = {
            "crash_on_vote_prob": 0.5,
            "torn_broadcast_prob": 0.15,
            "raft": RaftConfig(election_timeout_max=180, max_batch=1),
        }
        params.update(overrides)
        return cls(**params)


@dataclass(frozen=True)
class TimelineEvent:
    """Something that happened in a run: a node changing role, or a fault being injected."""

    time: int
    kind: str  # "state" for a role change, otherwise the fault: partition, crash, heal, ...
    node: int | None
    detail: str  # for "state": follower | candidate | leader | down
    term: int = 0


@dataclass
class RunResult:
    seed: int
    violation: Violation | None
    history: list[Op]
    stats: dict[str, int]
    end_time: int
    trace: list[str]
    timeline: list[TimelineEvent]
    config: SimConfig
    recording: Recorder | None = None

    @property
    def ok(self) -> bool:
        return self.violation is None


class Simulator:
    def __init__(
        self,
        seed: int,
        config: SimConfig | None = None,
        trace: bool = False,
        record: bool = False,
    ) -> None:
        self.seed = seed
        self.config = config or SimConfig()
        self.now = 0
        self.ids = list(range(self.config.n_nodes))
        self.rng = random.Random(f"{seed}/net")
        self.nemesis_rng = random.Random(f"{seed}/nemesis")
        self.storages = {i: Storage() for i in self.ids}
        self.nodes: dict[int, RaftNode | None] = {}
        self.boots: Counter[int] = Counter()
        self.groups: dict[int, int] | None = None  # network partition: node -> side
        self.drop_prob = self.config.drop_prob
        self.checker = InvariantChecker()
        self.stats: Counter[str] = Counter()
        self.trace_log: list[str] | None = [] if trace else None
        self.timeline: list[TimelineEvent] = []
        self._last_state: dict[int, tuple[str, int]] = {}
        self._queue: list[tuple[int, int, str, tuple[Any, ...]]] = []
        self._seq = 0
        self.recorder = Recorder(self) if record else None
        self.workload = Workload(
            self,
            self.config.n_clients,
            self.config.keys,
            self.config.client_timeout,
            random.Random(f"{seed}/workload"),
        )

        for i in self.ids:
            self._boot(i)
            self.schedule(self.rng.randint(0, self.config.tick_ms), "tick", i)
        self.workload.start()
        if self.config.nemesis:
            self.schedule(self.nemesis_rng.randint(200, 600), "nemesis")

    # ---- plumbing ----------------------------------------------------------------------

    def schedule(self, delay: int, kind: str, *args: Any) -> None:
        self._seq += 1
        heapq.heappush(self._queue, (self.now + delay, self._seq, kind, args))

    def log(self, text: str) -> None:
        if self.trace_log is not None:
            self.trace_log.append(f"[{self.now:>6}ms] {text}")

    def _mark(self, kind: str, text: str, node: int | None = None) -> None:
        """Record a fault in the timeline (and in the trace, when tracing)."""
        self.timeline.append(TimelineEvent(self.now, kind, node, text))
        self.log(text)

    def _observe(self) -> None:
        """Record every role change so a run can be drawn or measured afterwards."""
        for i in self.ids:
            node = self.nodes[i]
            state = ("down", 0) if node is None else (node.role.value, node.current_term)
            if self._last_state.get(i) != state:
                self._last_state[i] = state
                self.timeline.append(TimelineEvent(self.now, "state", i, state[0], state[1]))

    def live_nodes(self) -> list[RaftNode]:
        return [n for i in self.ids if (n := self.nodes.get(i)) is not None]

    def leader(self) -> RaftNode | None:
        leaders = [n for n in self.live_nodes() if n.role is Role.LEADER]
        return max(leaders, key=lambda n: n.current_term) if leaders else None

    def can_talk(self, a: int, b: int) -> bool:
        return self.groups is None or self.groups[a] == self.groups[b]

    def _boot(self, node_id: int) -> None:
        self.boots[node_id] += 1
        self.nodes[node_id] = RaftNode(
            node_id,
            [p for p in self.ids if p != node_id],
            random.Random(f"{self.seed}/node{node_id}/{self.boots[node_id]}"),
            now=self.now,
            config=self.config.raft,
            bugs=self.config.bugs,
            storage=self.storages[node_id],
            apply_hook=lambda nid, idx, entry: self.checker.on_apply(self, nid, idx, entry),
        )

    # ---- network -----------------------------------------------------------------------

    def send(self, src: Addr, dst: Addr, msg: Message) -> None:
        self.stats["msgs_sent"] += 1
        rec = self.recorder
        if isinstance(src, int) and isinstance(dst, int) and not self.can_talk(src, dst):
            self.stats["msgs_dropped"] += 1
            if rec is not None:
                rec.on_send(src, dst, msg, CUT)
            return
        if self.rng.random() < self.drop_prob:
            self.stats["msgs_dropped"] += 1
            if rec is not None:
                rec.on_send(src, dst, msg, LOST)
            return
        copies = 2 if self.rng.random() < self.config.dup_prob else 1
        for _ in range(copies):
            delay = self.rng.randint(self.config.latency_min, self.config.latency_max)
            mid = rec.on_send(src, dst, msg, PENDING) if rec is not None else -1
            self.schedule(delay, "deliver", src, dst, msg, mid)

    def _dispatch(self, src: int, outbox: list[tuple[Addr, Message]]) -> None:
        outbox = self._tear_broadcast(src, outbox)
        for dst, msg in outbox:
            self.send(src, dst, msg)

    # ---- faults ------------------------------------------------------------------------

    def _can_crash(self) -> bool:
        down = self.config.n_nodes - len(self.live_nodes())
        return self.workload.active and down < self.config.n_nodes // 2

    def _crash(self, node_id: int, quick: bool = False) -> None:
        self.nodes[node_id] = None
        self.stats["crashes"] += 1
        self._mark("crash", f"crash node {node_id}", node_id)
        low, high = (10, 80) if quick else (50, 1200)
        self.schedule(self.nemesis_rng.randint(low, high), "restart", node_id)

    def _tear_broadcast(
        self, src: int, outbox: list[tuple[Addr, Message]]
    ) -> list[tuple[Addr, Message]]:
        """Leader dies mid-broadcast: only one follower ever sees the new entries."""
        prob = self.config.torn_broadcast_prob
        node = self.nodes[src]
        if prob <= 0 or node is None or node.role is not Role.LEADER:
            return outbox
        appends = [(d, m) for d, m in outbox if isinstance(m, AppendEntries) and m.entries]
        rng = self.nemesis_rng
        if not appends or rng.random() >= prob or not self._can_crash():
            return outbox
        self.stats["torn_broadcasts"] += 1
        self._mark("torn", f"leader {src} crashes mid-broadcast", src)
        self.schedule(0, "crash_now", src)
        return [rng.choice(appends)]

    def _maybe_crash_voter(self, node_id: int, msg: RequestVote) -> None:
        prob = self.config.crash_on_vote_prob
        node = self.nodes[node_id]
        if prob <= 0 or node is None or node.storage.voted_for != msg.candidate_id:
            return
        if msg.term == node.current_term and self.nemesis_rng.random() < prob:
            if self._can_crash():
                self._mark(
                    "vote-crash",
                    f"node {node_id} crashes right after voting for {msg.candidate_id}",
                    node_id,
                )
                self._crash(node_id, quick=True)

    def _nemesis(self) -> None:
        rng = self.nemesis_rng
        action = rng.choices(
            ["partition", "isolate_leader", "heal", "crash", "crash_leader", "flaky"],
            weights=[3, 3, 3, 2, 2, 1],
        )[0]
        alive = [n.id for n in self.live_nodes()]
        max_crashed = self.config.n_nodes // 2
        can_crash = self.config.n_nodes - len(alive) < max_crashed
        leader = self.leader()
        if action == "partition":
            order = self.ids[:]
            rng.shuffle(order)
            cut = rng.randint(1, self.config.n_nodes - 1)
            self.groups = {n: (1 if k < cut else 0) for k, n in enumerate(order)}
            self.stats["partitions"] += 1
            self._mark("partition", f"partition {sorted(order[:cut])} | {sorted(order[cut:])}")
        elif action == "isolate_leader" and leader is not None:
            self.groups = {n: (1 if n == leader.id else 0) for n in self.ids}
            self.stats["partitions"] += 1
            self._mark("isolate", f"isolate leader {leader.id}", leader.id)
        elif action == "heal":
            self.groups = None
            self.drop_prob = self.config.drop_prob
            self._mark("heal", "heal network")
        elif action == "crash" and can_crash and alive:
            self._crash(rng.choice(alive))
        elif action == "crash_leader" and can_crash and leader is not None:
            self._crash(leader.id)
        elif action == "flaky":
            self.drop_prob = rng.choice([0.0, 0.1, 0.3])
            self._mark("flaky", f"drop probability {self.drop_prob}")
        self.schedule(rng.randint(100, 800), "nemesis")

    # ---- event loop --------------------------------------------------------------------

    def _handle(self, kind: str, args: tuple[Any, ...]) -> None:
        if kind == "tick":
            (node_id,) = args
            node = self.nodes[node_id]
            if node is not None:
                self._dispatch(node_id, node.tick(self.now))
            self.schedule(self.config.tick_ms, "tick", node_id)
        elif kind == "deliver":
            src, dst, msg, mid = args
            rec = self.recorder
            if isinstance(dst, str):
                if rec is not None:
                    rec.on_deliver(mid, DELIVERED)
                if isinstance(msg, ClientResponse):
                    self.workload.on_response(int(dst[1:]), msg)
                return
            if isinstance(src, int) and not self.can_talk(src, dst):
                self.stats["msgs_dropped"] += 1
                if rec is not None:
                    rec.on_deliver(mid, CUT_IN_FLIGHT)
                return
            node = self.nodes[dst]
            if rec is not None:
                rec.on_deliver(mid, DELIVERED if node is not None else RECEIVER_DOWN)
            if node is not None:
                self._dispatch(dst, node.receive(src, msg, self.now))
                if isinstance(msg, RequestVote):
                    self._maybe_crash_voter(dst, msg)
        elif kind == "crash_now":
            (node_id,) = args
            if self.nodes[node_id] is not None:
                self._crash(node_id, quick=True)
        elif kind == "client_wake":
            self.workload.on_wake(*args)
        elif kind == "client_timeout":
            self.workload.on_timeout(*args)
        elif kind == "restart":
            (node_id,) = args
            if self.nodes[node_id] is None:
                self._mark("restart", f"restart node {node_id}", node_id)
                self._boot(node_id)
        elif kind == "nemesis":
            if self.workload.active:
                self._nemesis()

    def _run_until(self, end: int) -> None:
        while self._queue and self._queue[0][0] <= end:
            time, _, kind, args = heapq.heappop(self._queue)
            self.now = time
            self._handle(kind, args)
            self._observe()
            if self.recorder is not None:
                self.recorder.on_step()
            self.checker.check(self)
        self.now = end

    def _quiesce(self) -> None:
        """Stop the load and the nemesis, heal everything, bring every node back."""
        self.workload.stop()
        self.groups = None
        self.drop_prob = 0.0
        for i in self.ids:
            if self.nodes[i] is None:
                self._boot(i)
        self._mark("heal", "quiesce: network healed, all nodes up")

    def _check_liveness(self) -> None:
        leader = self.leader()
        if leader is None:
            raise InvariantViolation(
                Violation("liveness", "no leader after the network healed", self.now)
            )
        behind = [n.id for n in self.live_nodes() if n.commit_index != leader.last_index]
        if behind:
            raise InvariantViolation(
                Violation(
                    "liveness",
                    f"nodes {behind} did not catch up to leader {leader.id} after healing",
                    self.now,
                )
            )

    def _check_linearizability(self) -> None:
        key = find_violation(self.workload.history())
        if key is not None:
            message = f"history of key {key!r} is not linearizable"
            raise InvariantViolation(Violation("linearizability", message, self.now))

    def run(self) -> RunResult:
        violation: Violation | None = None
        try:
            self._run_until(self.config.duration_ms)
            self._quiesce()
            self._run_until(self.config.duration_ms + self.config.settle_ms)
            self._check_liveness()
            self._check_linearizability()
        except InvariantViolation as exc:
            violation = exc.violation

        stats = dict(self.stats)
        stats["elections"] = len(self.checker.leader_of_term)
        stats["committed"] = len(self.checker.committed)
        for status, count in self.workload.counts().items():
            stats[f"ops_{status}"] = count
        return RunResult(
            self.seed,
            violation,
            self.workload.history(),
            stats,
            self.now,
            self.trace_log or [],
            self.timeline,
            self.config,
            self.recorder,
        )


def run_simulation(
    seed: int, config: SimConfig | None = None, trace: bool = False, record: bool = False
) -> RunResult:
    return Simulator(seed, config, trace, record).run()
