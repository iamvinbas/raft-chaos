#!/usr/bin/env bash
# Break a real 3-node Raft cluster while a client checks the history for linearizability.
#
#   docker compose up -d --build && ./scripts/chaos-demo.sh
#
# Faults: kill -9 the leader, add latency and packet loss with tc netem, partition a node
# with iptables. The exit code is that of `raftchaos verify`: 0 means the history was linearizable.
set -euo pipefail

NODES="node0:7000,node1:7000,node2:7000"
METRICS="node0:9100,node1:9100,node2:9100"
SECONDS_TOTAL="${SECONDS_TOTAL:-45}"
dc() { docker compose "$@"; }

leader() {
  # Prints the service name of the current leader, e.g. node1.
  dc exec -T node0 raftchaos status --metrics "$METRICS" 2>/dev/null \
    | python3 -c 'import sys, json
for line in sys.stdin:
    d = json.loads(line)
    if d.get("role") == "leader":
        print("node%d" % d["id"])
        break'
}

wait_for_leader() {
  for _ in $(seq 1 60); do
    if [ -n "$(leader || true)" ]; then return 0; fi
    sleep 0.5
  done
  echo "no leader elected" >&2
  return 1
}

say() { printf '\n== %s\n' "$*"; }

wait_for_leader
say "cluster is up, leader is $(leader)"

say "starting the linearizability checker for ${SECONDS_TOTAL}s"
dc run --rm -T client verify --nodes "$NODES" --seconds "$SECONDS_TOTAL" --clients 3 &
VERIFY_PID=$!
sleep 5

say "fault 1: kill -9 the leader ($(leader))"
victim="$(leader)"
dc kill -s KILL "$victim"
sleep 6
say "restarting $victim (it recovers term and log from its volume)"
dc start "$victim"
sleep 6

say "fault 2: 150ms delay, 15% packet loss on node1"
dc exec -T node1 tc qdisc add dev eth0 root netem delay 150ms 50ms loss 15%
sleep 8
dc exec -T node1 tc qdisc del dev eth0 root
sleep 3

say "fault 3: partition node2 from the others"
dc exec -T node2 sh -c '
  for h in node0 node1; do
    ip=$(getent hosts "$h" | cut -d" " -f1)
    iptables -A INPUT -s "$ip" -j DROP
    iptables -A OUTPUT -d "$ip" -j DROP
  done'
sleep 8
say "healing the partition"
dc exec -T node2 iptables -F
sleep 3

set +e
wait "$VERIFY_PID"
status=$?
set -e
say "cluster state after the faults"
dc exec -T node0 raftchaos status --metrics "$METRICS" || true
exit "$status"
