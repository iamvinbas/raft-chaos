"""Check a real cluster with the same linearizability checker the simulator uses.

Concurrent clients issue puts and gets over TCP while you (or `scripts/chaos-demo.sh`) break
the cluster. Timeouts become operations with unknown outcome, exactly as in the simulator.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass

from ..linearizability import Op, find_violation
from .client import Address, KvClient, Unavailable


@dataclass
class VerifyReport:
    ops_ok: int
    ops_unknown: int
    violation_key: str | None
    seconds: float

    @property
    def ok(self) -> bool:
        return self.violation_key is None


async def _worker(
    wid: int,
    nodes: list[Address],
    keys: tuple[str, ...],
    stop_at: float,
    t0: float,
    history: list[Op],
    counter: list[int],
    unknown: list[int],
    rng: random.Random,
    op_deadline_s: float,
) -> None:
    client = KvClient(nodes, name=f"verify-{wid}-{rng.getrandbits(24):x}")
    while time.monotonic() < stop_at:
        key = rng.choice(keys)
        counter[0] += 1
        op_id = counter[0]
        is_put = rng.random() < 0.6
        invoke = int((time.monotonic() - t0) * 1000)
        try:
            if is_put:
                await client.put(key, op_id, op_deadline_s)  # the value is unique per operation
                result = None
            else:
                result = await client.get(key, op_deadline_s)
        except Unavailable:
            # Outcome unknown. A put may still have been applied; a get tells us nothing.
            if is_put:
                history.append(Op(op_id, "put", key, op_id, None, invoke, None))
            unknown[0] += 1
        else:
            done = int((time.monotonic() - t0) * 1000)
            kind = "put" if is_put else "get"
            history.append(Op(op_id, kind, key, op_id if is_put else None, result, invoke, done))
        await asyncio.sleep(rng.uniform(0.0, 0.05))


async def run_verify(
    nodes: list[Address],
    seconds: float,
    clients: int = 3,
    keys: tuple[str, ...] = ("x", "y"),
    seed: int | None = None,
    op_deadline_s: float = 3.0,
) -> VerifyReport:
    rng = random.Random(seed)
    history: list[Op] = []
    counter = [0]
    unknown = [0]
    t0 = time.monotonic()
    stop_at = t0 + seconds
    await asyncio.gather(
        *(
            _worker(
                w,
                nodes,
                keys,
                stop_at,
                t0,
                history,
                counter,
                unknown,
                random.Random(rng.random()),
                op_deadline_s,
            )
            for w in range(clients)
        )
    )
    ok = sum(1 for op in history if op.response is not None)
    return VerifyReport(ok, unknown[0], find_violation(history), time.monotonic() - t0)
