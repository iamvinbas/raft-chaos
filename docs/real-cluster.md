# Running a real cluster

The simulator tests the protocol (see the [README](../README.md)). The same `RaftNode` also runs as a real service over TCP:
newline-delimited JSON frames, the term, vote and log fsynced to disk before any reply is sent,
and a `/metrics` (Prometheus) and `/status` HTTP endpoint per node.

```bash
rm -rf /tmp/raft   # a previous run's term and log would otherwise be reloaded
PEERS=127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102
for i in 0 1 2; do
  raftchaos node --id $i --peers $PEERS --listen 127.0.0.1:$((7100+i)) \
      --data-dir /tmp/raft --metrics-port $((9100+i)) &
done

raftchaos kv --nodes $PEERS put greeting ciao
raftchaos kv --nodes $PEERS get greeting          # ciao
raftchaos status --metrics 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102
```

`raftchaos verify` closes the loop: it drives concurrent clients against the real cluster and
checks the recorded history with the same linearizability checker the simulator uses. Here it ran
while the leader (node 0) was killed with `kill -9` and restarted, then node 1 was killed and
restarted, on three local processes:

```console
$ raftchaos verify --nodes $PEERS --seconds 14 --clients 3
1285 operations acknowledged, 0 with unknown outcome, 14.1s
OK: the history is linearizable

$ raftchaos status --metrics 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102
{"id": 0, "role": "follower", "term": 2, "leader": 2, "commit_index": 1289, ...}
{"id": 1, "role": "follower", "term": 2, "leader": 2, "commit_index": 1289, ...}
{"id": 2, "role": "leader", "term": 2, "leader": 2, "commit_index": 1289, ...}
```

Both restarted nodes recovered their term and log from disk and caught up to the leader.

**Docker.** `docker-compose.yml` runs the three nodes, and `scripts/chaos-demo.sh` breaks them
while `verify` runs: `kill -9` on the leader, latency and packet loss with `tc netem`, and a
partition with `iptables`. Add `--profile monitoring` for a Prometheus instance.

```bash
docker compose up -d --build
./scripts/chaos-demo.sh          # exit code 0 means the history was linearizable
```

Result of one run on Docker Desktop (macOS, three containers, about 46 s of load), with the
current code:

```console
$ ./scripts/chaos-demo.sh
== cluster is up, leader is node0
== fault 1: kill -9 the leader (node0)
== restarting node0 (it recovers term and log from its volume)
== fault 2: 150ms delay, 15% packet loss on node1
== fault 3: partition node2 from the others
== healing the partition
4240 operations acknowledged, 0 with unknown outcome, 46.1s
OK: the history is linearizable
{"id": 0, "role": "leader", "term": 2, "leader": 0, "commit_index": 4242, ...}
{"id": 1, "role": "follower", "term": 2, "leader": 0, "commit_index": 4242, ...}
{"id": 2, "role": "follower", "term": 2, "leader": 0, "commit_index": 4242, ...}
```

The first run of this script also passed the safety check, but it ended at term 69, with 3167
operations and a follower still catching up. Three problems were behind that; none broke safety,
all three cost availability.

## Findings from the real cluster

| # | Symptom | Root cause | Fix | Before | After |
| --- | --- | --- | --- | --- | --- |
| 1 | A node isolated for 8 s came back and forced two leader changes on a healthy cluster | Without PreVote, a node that cannot win keeps raising its term; its high term deposes the leader when it rejoins | PreVote (Raft thesis 9.6), on by default in `raftchaos node` | term 69 to ~107, 2 needless elections | term unchanged, 0 elections |
| 2 | A rejoining follower caught up at about 40 entries per second | Storage rewrote and fsynced the whole log on every change: O(log size) per message, blocking the event loop | Append-only write-ahead log plus a small metadata file; a torn last line is dropped on load | ~14 s to catch up | under 5 s |
| 3 | After healing, a deposed leader kept believing it led for 2-3 s | Frames queued during the partition sat in TCP retransmission backoff | `TCP_USER_TIMEOUT` of 1 s on peer sockets, so a dead connection is replaced at once | 2-3 s | converged 0.6 s (leader cut off) and 1.0 s (follower cut off) after healing |

Each "before / after" is a single run of the same `iptables` isolation on the Docker cluster, so
the numbers show the order of magnitude, not a benchmark.

Finding 1 is also reproduced in the simulator, where it can be measured over many seeds:

```console
$ raftchaos experiment prevote --seeds 50
one follower isolated at 1500 ms, network healed at 4500 ms, 3 nodes, seeds 0-49
                safe  disrupted  term +  outage avg  outage max
no PreVote    50/50      50/50     12.6       155ms       519ms
PreVote       50/50       0/50      0.0       107ms       427ms
```

The same holds with 5 nodes (50/50 disrupted without PreVote, 0/50 with it). The visualiser
has both runs side by side: scenes "Isolated node, no PreVote" and "Same fault, with PreVote".

> The node ports have no authentication or TLS, so this is a demo, not a deployment.
