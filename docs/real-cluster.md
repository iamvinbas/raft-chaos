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

| 4 | The live view's history check reported one non-linearizable history on key `x` | A node replies on the client's latest connection, and `KvClient` took the first reply without checking its request id: a late reply to a put the client had given up on could answer its next get | The client ignores replies whose request id is not the one it is waiting for | a `get` of a never-written key returned `'late'` | fixed |

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

### About finding 4

The live check flagged one violation after a node was killed and another isolated, so the leader
lost its majority and then regained it. The mechanism above is reproduced deterministically on a
real TCP cluster by `test_late_commit_of_an_abandoned_put_does_not_answer_a_later_get`: it fails
3 out of 3 times with the old client and passes with the fix. The original live run itself could
not be reproduced on demand (5 attempts with the old client came back clean), so this bug is the
likely cause, not a proven one. After the fix, the same fault sequences ran clean 3 out of 3 times.

The simulator did not catch it because its workload matches replies by request id already: the bug
lived in the real client only, which is why checking the real system matters.

## Snapshots on the real cluster

`raftchaos node` compacts its log every 1000 applied entries (`--snapshot-every`, 0 disables it).
To measure it, one follower of three local processes was killed with `kill -9`, three clients
wrote for 30 or 90 seconds, then the follower was restarted and timed until its commit index
matched the leader's. Every load round was checked and found linearizable.

| Entries written | Snapshots | Follower caught up after restart | Files per node |
| --- | --- | --- | --- |
| about 3,100 | off | 0.58 s | 264 KB |
| about 3,100 | every 1000 | 0.36 s, snapshot up to 3002 | 16 KB |
| about 9,600 | off | 0.93 s | 840 KB |
| about 9,600 | every 1000 | 0.35 s, snapshot up to 9002 | 56 KB |

Without snapshots both columns grow with the log; with them they stay flat. These are single runs
and the times include about 0.3 s of Python start-up, so read them as a trend, not a benchmark.

The Docker chaos demo passes with snapshots on as well: 3,981 operations, linearizable, all three
nodes at commit 3,986 with snapshots at 3,000 to 3,089. Node 0's volume held a 184-byte snapshot
and a log of the 986 entries after it, instead of all 3,986.

> The node ports have no authentication or TLS, so this is a demo, not a deployment.
