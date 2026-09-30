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
from .messages import Addr, ClientResponse, Message
from .node import RaftConfig, RaftNode, Role, Storage
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
    raft: RaftConfig = field(default_factory=RaftConfig)
    bugs: Bugs = field(default_factory=Bugs)


@dataclass
class RunResult:
    seed: int
    violation: Violation | None
    history: list[Op]
    stats: dict[str, int]
    end_time: int
    trace: list[str]

    @property
    def ok(self) -> bool:
        return self.violation is None


class Simulator:
    def __init__(self, seed: int, config: SimConfig | None = None, trace: bool = False) -> None:
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
        self._queue: list[tuple[int, int, str, tuple[Any, ...]]] = []
        self._seq = 0
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
        if isinstance(src, int) and isinstance(dst, int) and not self.can_talk(src, dst):
            self.stats["msgs_dropped"] += 1
            return
        if self.rng.random() < self.drop_prob:
            self.stats["msgs_dropped"] += 1
            return
        copies = 2 if self.rng.random() < self.config.dup_prob else 1
        for _ in range(copies):
            delay = self.rng.randint(self.config.latency_min, self.config.latency_max)
            self.schedule(delay, "deliver", src, dst, msg)

    def _dispatch(self, src: int, outbox: list[tuple[Addr, Message]]) -> None:
        for dst, msg in outbox:
            self.send(src, dst, msg)

    # ---- faults ------------------------------------------------------------------------

    def _crash(self, node_id: int) -> None:
        self.nodes[node_id] = None
        self.stats["crashes"] += 1
        self.log(f"crash node {node_id}")
        self.schedule(self.nemesis_rng.randint(50, 1200), "restart", node_id)

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
            self.log(f"partition {sorted(order[:cut])} | {sorted(order[cut:])}")
        elif action == "isolate_leader" and leader is not None:
            self.groups = {n: (1 if n == leader.id else 0) for n in self.ids}
            self.stats["partitions"] += 1
            self.log(f"isolate leader {leader.id}")
        elif action == "heal":
            self.groups = None
            self.drop_prob = self.config.drop_prob
            self.log("heal network")
        elif action == "crash" and can_crash and alive:
            self._crash(rng.choice(alive))
        elif action == "crash_leader" and can_crash and leader is not None:
            self._crash(leader.id)
        elif action == "flaky":
            self.drop_prob = rng.choice([0.0, 0.1, 0.3])
            self.log(f"drop probability {self.drop_prob}")
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
            src, dst, msg = args
            if isinstance(dst, str):
                if isinstance(msg, ClientResponse):
                    self.workload.on_response(int(dst[1:]), msg)
                return
            if isinstance(src, int) and not self.can_talk(src, dst):
                self.stats["msgs_dropped"] += 1
                return
            node = self.nodes[dst]
            if node is not None:
                self._dispatch(dst, node.receive(src, msg, self.now))
        elif kind == "client_wake":
            self.workload.on_wake(*args)
        elif kind == "client_timeout":
            self.workload.on_timeout(*args)
        elif kind == "restart":
            (node_id,) = args
            if self.nodes[node_id] is None:
                self.log(f"restart node {node_id}")
                self._boot(node_id)
        elif kind == "nemesis":
            if self.workload.active:
                self._nemesis()

    def _run_until(self, end: int) -> None:
        while self._queue and self._queue[0][0] <= end:
            time, _, kind, args = heapq.heappop(self._queue)
            self.now = time
            self._handle(kind, args)
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
        self.log("quiesce: network healed, all nodes up")

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
        )


def run_simulation(seed: int, config: SimConfig | None = None, trace: bool = False) -> RunResult:
    return Simulator(seed, config, trace).run()
