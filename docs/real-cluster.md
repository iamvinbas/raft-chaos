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

> The Docker setup is written and the compose file validates, but the demo has not been run
> end to end yet. The localhost run above is the tested path. The node ports have no
> authentication or TLS, so this is a demo, not a deployment.
