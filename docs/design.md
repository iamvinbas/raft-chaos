# Design notes

## Invariants (checked after every event)

| Invariant | Statement | Where |
| --- | --- | --- |
| Election Safety | At most one leader per term | `invariants.py` |
| Leader Completeness | A leader of a later term holds every entry committed earlier | `invariants.py` |
| Log Matching | If two logs share an index and term, they are identical up to it | `invariants.py` |
| State Machine Safety | No two nodes apply different entries at the same index | `invariants.py` |

After the network heals, a **liveness** check requires a leader and converged commit indexes.
Client histories are checked for **linearizability** per key.

## Decisions

- **Pure node.** `RaftNode` consumes events and returns outgoing messages; no clock, I/O or
  global randomness. This is what makes runs replayable from a seed.
- **One seeded RNG** drives latency, drops, duplication and nemesis choices.
- **Client sessions.** Requests carry `(client, req_id)` and are deduplicated in the state
  machine, because the network may duplicate them.
- **Indeterminate outcomes.** Timeouts and non-leader rejections are recorded as "may or may
  not have executed"; the checker allows either.
- **Bugs as flags.** Planted bugs validate that the simulator can actually detect failures.
