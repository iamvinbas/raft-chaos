"""Linearizability checker for a key-value register history (Wing & Gong style search).

Keys are independent registers, so each key is checked on its own. An operation that never
got a response (a client timeout) may or may not have taken effect: a pending put can be
placed anywhere after its invocation, or dropped. A pending get carries no information and
is ignored.
"""

from __future__ import annotations

from dataclasses import dataclass

INF = float("inf")


@dataclass(frozen=True)
class Op:
    id: int
    kind: str  # "put" or "get"
    key: str
    value: object  # value written by a put
    result: object  # value returned by a get
    invoke: int
    response: int | None  # None: no response was observed


def check_key(ops: list[Op], max_states: int = 500_000) -> bool | None:
    """True if linearizable, False if not, None if the search budget ran out."""
    ops = [op for op in ops if not (op.response is None and op.kind == "get")]
    # A pending put nobody read from can always be left out of the linearization.
    read = {op.result for op in ops if op.kind == "get"}
    ops = [op for op in ops if not (op.response is None and op.value not in read)]
    ops.sort(key=lambda op: (op.invoke, op.id))
    required = 0
    for i, op in enumerate(ops):
        if op.response is not None:
            required |= 1 << i

    seen: set[tuple[int, object]] = set()
    stack: list[tuple[int, object]] = [(0, None)]
    while stack:
        mask, state = stack.pop()
        if (mask, state) in seen:
            continue
        if len(seen) >= max_states:
            return None
        seen.add((mask, state))
        if mask & required == required:
            return True

        # An operation can be linearized next only if no unlinearized operation had
        # already finished before it started.
        horizon = INF
        for i, op in enumerate(ops):
            if not (mask >> i) & 1 and op.response is not None:
                horizon = min(horizon, op.response)
        for i, op in enumerate(ops):
            if op.invoke > horizon:
                break
            if (mask >> i) & 1:
                continue
            if op.kind == "put":
                stack.append((mask | (1 << i), op.value))
            elif op.result == state:
                stack.append((mask | (1 << i), state))
    return False


def find_violation(history: list[Op]) -> str | None:
    """Return a key whose history is not linearizable, or None if all keys pass."""
    by_key: dict[str, list[Op]] = {}
    for op in history:
        by_key.setdefault(op.key, []).append(op)
    for key in sorted(by_key):
        if check_key(by_key[key]) is False:
            return key
    return None
