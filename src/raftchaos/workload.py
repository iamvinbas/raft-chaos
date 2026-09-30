"""Simulated clients that issue puts and gets and record the history for the checker."""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .linearizability import Op
from .messages import ClientRequest, ClientResponse

if TYPE_CHECKING:
    from .sim import Simulator


@dataclass
class OpRecord:
    id: int
    client: int
    kind: str
    key: str
    value: object
    invoke: int
    status: str = "pending"  # pending | ok | info | fail
    response: int | None = None
    result: object = None


@dataclass
class _Client:
    target: int
    current: OpRecord | None = None


class Workload:
    def __init__(
        self,
        sim: Simulator,
        n_clients: int,
        keys: tuple[str, ...],
        timeout: int,
        rng: random.Random,
    ) -> None:
        self.sim = sim
        self.keys = keys
        self.timeout = timeout
        self.rng = rng
        self.records: list[OpRecord] = []
        self.clients = [_Client(rng.choice(sim.ids)) for _ in range(n_clients)]
        self.active = True
        self._next_value = 1

    def start(self) -> None:
        for cid in range(len(self.clients)):
            self.sim.schedule(self.rng.randint(10, 100), "client_wake", cid)

    def stop(self) -> None:
        self.active = False

    def on_wake(self, cid: int) -> None:
        client = self.clients[cid]
        if not self.active or client.current is not None:
            return
        key = self.rng.choice(self.keys)
        if self.rng.random() < 0.6:
            value: object = self._next_value
            self._next_value += 1
            command: tuple[object, ...] = ("put", key, value)
            kind = "put"
        else:
            value, command, kind = None, ("get", key), "get"
        op = OpRecord(len(self.records), cid, kind, key, value, self.sim.now)
        self.records.append(op)
        client.current = op
        self.sim.send(f"c{cid}", client.target, ClientRequest(op.id, command))
        self.sim.schedule(self.timeout, "client_timeout", cid, op.id)

    def on_response(self, cid: int, msg: ClientResponse) -> None:
        client = self.clients[cid]
        op = client.current
        if op is None or op.id != msg.req_id:
            return  # a late answer to an operation the client already gave up on
        if msg.ok:
            op.status, op.response, op.result = "ok", self.sim.now, msg.result
            client.target = msg.leader_hint if msg.leader_hint is not None else client.target
        else:
            # Rejected by a non-leader. A duplicated copy of the request may still have been
            # appended by a leader that then stepped down, so a rejected put stays "maybe".
            op.status = "fail"
            hint = msg.leader_hint
            client.target = hint if hint is not None else self.rng.choice(self.sim.ids)
        self._finish(cid)

    def on_timeout(self, cid: int, op_id: int) -> None:
        client = self.clients[cid]
        op = client.current
        if op is None or op.id != op_id:
            return
        op.status = "info"  # outcome unknown: it may still take effect later
        client.target = self.rng.choice(self.sim.ids)
        self._finish(cid)

    def _finish(self, cid: int) -> None:
        self.clients[cid].current = None
        self.sim.schedule(self.rng.randint(5, 60), "client_wake", cid)

    def history(self) -> list[Op]:
        ops = []
        for r in self.records:
            if r.kind == "get" and r.status != "ok":
                continue
            response = r.response if r.status == "ok" else None
            ops.append(Op(r.id, r.kind, r.key, r.value, r.result, r.invoke, response))
        return ops

    def counts(self) -> dict[str, int]:
        counts = {"ok": 0, "info": 0, "fail": 0, "pending": 0}
        for r in self.records:
            counts[r.status] += 1
        return counts
