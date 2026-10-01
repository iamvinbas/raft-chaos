# Hands-on guide

A step-by-step tour of raft-chaos, from installing it to breaking a real cluster and putting it
back together. Every part says what to type, what you should see, and what it means. Parts 1 to 4
need only Python; parts 5 to 7 run real servers; part 6 needs Docker.

- [0. What you need](#0-what-you-need)
- [1. Install](#1-install)
- [2. A simulated run](#2-a-simulated-run)
- [3. A planted bug, caught](#3-a-planted-bug-caught)
- [4. The visualiser](#4-the-visualiser)
- [5. A real cluster on your machine](#5-a-real-cluster-on-your-machine)
- [6. Live mode: break the Docker cluster from the browser](#6-live-mode-break-the-docker-cluster-from-the-browser)
- [7. Backup and restore](#7-backup-and-restore)
- [8. Stopping and cleaning up](#8-stopping-and-cleaning-up)
- [9. When something goes wrong](#9-when-something-goes-wrong)

## 0. What you need

- **Python 3.10 or newer** and **git**. Check with `python3 --version` and `git --version`.
- **Docker Desktop**, only for part 6. It must be running (its whale icon in the menu bar) before
  you use any `docker compose` command.
- macOS or Linux. The commands below are for a bash or zsh terminal.

## 1. Install

```bash
git clone https://github.com/iamvinbas/raft-chaos.git
cd raft-chaos
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
raftchaos bugs
```

`python3 -m venv .venv` creates a private Python environment inside the folder, and `source
.venv/bin/activate` switches the terminal to it: the prompt now starts with `(.venv)`. The last
command should list eight bug names, which proves the install worked.

> **Every new terminal window starts outside the environment.** Go back to the `raft-chaos`
> folder and run `source .venv/bin/activate` again, or `raftchaos` and `pytest` will be
> "command not found".

## 2. A simulated run

```bash
raftchaos run --seed 3
```

The simulator builds three Raft servers in memory and attacks them for eight seconds with network
partitions, crashes, lost and duplicated messages, while three clients keep writing and reading.
Then the network heals and the cluster must converge. Nothing is real: there are no sockets and
no clock, which is what makes it fast and exactly repeatable.

You should see, on the last line:

```
OK: all invariants held and the history is linearizable
```

The `stats` line above it counts what happened: `crashes` and `partitions` injected, `elections`
held, `committed` log entries, and the client operations that succeeded (`ops_ok`), timed out with
an unknown outcome (`ops_info`) or were refused by a server that was not the leader (`ops_fail`).

**The seed is the whole experiment.** Run the same command again: every number is identical. A
different seed gives a different storm of faults.

## 3. A planted bug, caught

```bash
raftchaos run --seed 1 --bug double_vote --trace
```

`--bug double_vote` removes one safety check: a server may vote twice in the same term. `--trace`
prints every fault as it is injected. The run stops at the first broken rule:

```
VIOLATION [7226ms] election-safety: nodes 0 and 2 both lead term 11
reproduce: raftchaos run --seed 1 --nodes 3 --bug double_vote --trace
```

Two leaders in one term is exactly what Raft must never allow. The `reproduce:` line replays the
same failure on any machine.

To search many seeds automatically:

```bash
raftchaos hunt --all --seeds 200 --jobs 2
```

It tries each planted bug (and a correct server, the `none` row, which must never fail) until a
seed breaks it. `--jobs 2` uses two processor cores; more is faster but keeps the machine busy.
Some bugs need rare timing; `--profile adversarial` adds targeted faults that find all eight.

## 4. The visualiser

```bash
raftchaos viz --out viz.html --open
```

This writes one HTML file and opens it in the browser. The same page is online:
<https://iamvinbas.github.io/raft-chaos/viz/>.

- **Scenes**, at the top: a correct cluster under chaos, two planted bugs, the same fault with and
  without PreVote, and a node brought back by a snapshot.
- **Servers**: each box shows the role (leader, follower, candidate, crashed), the term, the commit
  index, the vote, and the last log entries as small squares coloured by term (filled means
  committed).
- **Dots** are messages in flight; a lost message stops half way and bursts red. Red dashed lines
  are partitioned links.
- **Timeline**, at the bottom: click or drag to jump to any moment. **Events**, on the right: click
  one to jump to it.
- When a rule breaks, playback stops and a card explains what went wrong and how to reproduce it.
  The Link button copies an address that opens this exact moment.

## 5. A real cluster on your machine

The same server code also runs for real, over TCP, saving to disk. Start three servers in the
background (one terminal is enough):

```bash
rm -rf /tmp/raft-demo
PEERS=127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102
for i in 0 1 2; do
  raftchaos node --id $i --peers $PEERS --listen 127.0.0.1:$((7100+i)) \
    --data-dir /tmp/raft-demo --metrics-port $((9100+i)) > /tmp/raft-demo-$i.log 2>&1 &
done
sleep 2
raftchaos status --metrics 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102
```

`--peers` lists every server of the cluster, `--data-dir` is where each one keeps its log, and
`--metrics-port` serves `/status` and Prometheus `/metrics`. `status` prints one line per server:
one is `leader`, two are `follower`.

Write and read:

```bash
raftchaos kv --nodes $PEERS put greeting hello
raftchaos kv --nodes $PEERS get greeting
```

Now kill the leader. Read its id in the `status` output; here it is server 0:

```bash
pkill -9 -f "raftchaos node --id 0"
sleep 2
raftchaos status --metrics 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102
raftchaos kv --nodes $PEERS get greeting
```

Server 0 is `unreachable`, another one became leader with a higher term, and the read still
answers `hello`: nothing was lost. Bring server 0 back; it reloads its log from disk:

```bash
raftchaos node --id 0 --peers $PEERS --listen 127.0.0.1:7100 \
  --data-dir /tmp/raft-demo --metrics-port 9100 >> /tmp/raft-demo-0.log 2>&1 &
```

To check correctness while you break things, run this in a second terminal (activate the
environment and set `PEERS` there too) and kill and restart servers during the 20 seconds:

```bash
raftchaos verify --nodes $PEERS --seconds 20
```

It ends with `OK: the history is linearizable` when every read was consistent with the writes.
Stop the servers with `pkill -f "raftchaos node"` before part 6, which uses the same ports.

## 6. Live mode: break the Docker cluster from the browser

Start Docker Desktop, then:

```bash
docker compose up -d --build
raftchaos live --open
```

The first command builds the image and starts three servers in containers (the first time takes a
minute or two). The second opens <http://127.0.0.1:8080>: the visualiser connected to the real
cluster, refreshed every 100 ms. Leave that terminal open; the page needs it.

The **Break it** panel on the right:

| Button | What it does | Enabled when |
| --- | --- | --- |
| **Kill** | `kill -9` that server's container: it stops at once, mid-work | the server is up |
| **Revive** | starts that server again; it reloads term, log and snapshot from its volume | the server is down |
| **Isolate** | `iptables` drops all its Raft traffic: it is cut off from the others | the server is up |
| **Slow** | `tc netem` adds 150 ms of delay and 15% packet loss to it | the server is up |
| **Kill the leader** | kills whichever server leads right now | there is a leader |
| **Revive all** | starts every server that is down | at least one is down |
| **Heal network** | removes every `iptables` and `tc` rule | there is a network fault |
| **Start load / Stop load** | three clients write and read continuously | always |
| **Check history** | checks everything the clients saw since the load started | always |

Things to try, with the load running:

1. **Kill the leader.** Within a few hundred milliseconds another server wins an election (watch
   the term go up) and the clients carry on. Then **Revive** it: it rejoins as a follower.
2. **Kill two servers.** One out of three is not a majority: writes stop, the rate on the
   "Clients" line drops to 0/s and operations with an unknown outcome appear. **Revive all**, and
   the cluster recovers on its own.
3. **Isolate a follower** for ten seconds, then **Heal network**. Thanks to PreVote its term does not
   climb while it is cut off, and it rejoins without disturbing the leader.
4. **Slow a server**, and watch its dots crawl and some burst red.
5. **Stop load** and **Check history**: the verdict should be "Linearizable". Faults cost
   availability, never correctness.

There is also a scripted version that runs the faults for you and checks the result:
`./scripts/chaos-demo.sh` (exit code 0 means the history was linearizable).

## 7. Backup and restore

A server survives crashes on its own: everything is on disk before it replies, and committed data
also lives on a majority. What a backup protects against is losing **every** server at once, for
example `docker compose down -v`, which deletes all their files.

This part uses the local servers of part 5. If you went on to part 6, first stop the Docker cluster
(`docker compose down`, since it uses the same ports) and start the three servers again with the
loop of part 5, without the `rm -rf` so they keep their data. Then:

```bash
raftchaos backup save --metrics 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 --out backup.json
raftchaos backup show backup.json
```

`save` asks every server, picks the most up-to-date one, and writes its committed data to the
file. To rebuild a lost cluster, start **every** server from the same file, in an empty data
directory:

```bash
pkill -9 -f "raftchaos node"; rm -rf /tmp/raft-demo      # the disaster
for i in 0 1 2; do
  raftchaos node --id $i --peers $PEERS --listen 127.0.0.1:$((7100+i)) \
    --data-dir /tmp/raft-restored --metrics-port $((9100+i)) --restore backup.json \
    > /tmp/raft-demo-$i.log 2>&1 &
done
sleep 2
raftchaos kv --nodes $PEERS get greeting
```

The data is back. Restoring over a server that already has data is refused on purpose.

## 8. Stopping and cleaning up

| To stop | Do |
| --- | --- |
| live mode (`raftchaos live`) | `Ctrl+C` in its terminal |
| the Docker cluster, keeping its data | `docker compose stop` (resume with `docker compose start`) |
| the Docker cluster, removing the containers but keeping the data | `docker compose down` |
| the Docker cluster and **all its data** | `docker compose down -v` |
| the local servers of parts 5 and 7 | `pkill -f "raftchaos node"`, then `rm -rf /tmp/raft-demo /tmp/raft-restored` |
| the Python environment | `deactivate` |
| Docker itself | quit Docker Desktop from the menu bar, to free memory |

Stop live mode before the Docker cluster, or the page keeps polling servers that are gone.

## 9. When something goes wrong

| You see | Why, and what to do |
| --- | --- |
| `command not found: raftchaos` | the environment is not active: `cd` into the folder, `source .venv/bin/activate` |
| `address already in use` | servers from an earlier step are still running: `pkill -f "raftchaos node"`, or `docker compose down` |
| strange results after mixing parts 5 and 6 | both use ports 7100-7102 and 9100-9102; run one cluster at a time |
| `Cannot connect to the Docker daemon` | Docker Desktop is not running yet: start it and wait for it |
| the live page says "bridge offline" | the `raftchaos live` terminal was closed: run it again and reload the page |
| `pytest` reports many collection errors | it was started from a parent folder: run it from `raft-chaos` |
| a `get` hangs for a few seconds | an election is in progress or no majority is up: wait, or Revive a server |

To run the test suite: `pytest -q` from the `raft-chaos` folder (about a minute).
