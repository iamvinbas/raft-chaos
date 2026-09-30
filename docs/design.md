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

## Snapshots

A node compacts its log every `snapshot_every` applied entries: it saves the applied state (the
key-value data and the client sessions used for deduplication) with the index and term of the last
entry it covers, then deletes that prefix of the log. Only applied, hence committed, entries go
into a snapshot. Off in the simulator by default, every 1000 entries in the real runtime.

- **InstallSnapshot.** When a follower needs an entry the leader has already compacted, the leader
  sends its snapshot instead. The follower ignores a snapshot it has already moved past; if its
  log agrees with the snapshot's last entry it keeps the entries after it; otherwise it discards
  its log and restores the state from the snapshot (Raft paper, Figure 13).
- **The leader waits for followers that keep up.** It does not compact entries that a follower
  within two snapshots of it has not acknowledged yet; otherwise every compaction would push a
  whole snapshot onto the slowest healthy follower. A follower further behind needs a snapshot
  anyway and does not hold compaction back.
- **Checked against the history.** The invariant checker replays the committed entries into a
  reference store. Every snapshot taken or installed, and every node's data after each apply,
  must equal that reference at the same index.
- **Durable in the right order.** The runtime writes the snapshot atomically first and only then
  rewrites the log without the covered prefix, also atomically. Each log line carries its absolute
  index, so a log left over from a crash between the two steps is aligned with the snapshot on
  load.
- **Message storms are reported.** A correct run never has more than a few dozen events pending.
  More than 10,000 means the nodes generate traffic without bound, which the simulator reports as
  a liveness violation instead of simulating forever. The planted `install_snapshot_discards_log`
  bug can cause exactly that: a follower wipes its log on a late snapshot copy, the leader sends
  another snapshot, and the loop feeds itself.

