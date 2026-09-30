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
- **One seed** derives every random generator: latency, drops, duplication, nemesis choices
  and client behaviour.
- **Client sessions.** Requests carry `(client, req_id)` and are deduplicated in the state
  machine, because the network may duplicate them.
- **Indeterminate outcomes.** Timeouts and non-leader rejections are recorded as "may or may
  not have executed"; the checker allows either.
- **Bugs as flags.** Planted bugs validate that the simulator can actually detect failures.
- **Write-ahead log.** The real runtime appends log entries to a JSONL file and keeps term and
  vote in a separate small file, fsynced before any reply. Rewriting the whole log on each change,
  the first design, made catch-up O(n^2).

## PreVote

Off in the simulator by default so every published seed still replays, on by default in the real
runtime. On an election timeout a node sends `PreVote(term + 1)` instead of raising its term. A
receiver grants it only if the candidate's log is up to date and it has not heard from a leader
within the minimum election timeout; granting changes neither its term nor its vote. Only a
majority of grants starts a real election. A node that cannot reach a majority therefore keeps its
term, and a rejoining node cannot depose a leader the others still hear from.

