# Metrics, SLOs and anomalies

The simulator also measures the service the way a client sees it, on the same virtual clock,
so the numbers reproduce from a seed.

**Prometheus metrics.** `raftchaos metrics --seed 7` prints the text exposition format
(operation outcomes, a latency histogram, availability, longest outage, leader changes).
Availability is probed in 250 ms windows: a window is up if at least one operation succeeded.

**SLO report.** `raftchaos slo --seed 7` runs the same seed twice, once without faults and once
under chaos, and reports how much of each error budget was used:

```console
$ raftchaos slo --seed 7
seed 7, 3 nodes, steady state (no faults)
SLO                target     actual  budget used  status
availability        0.900      1.000          0%  MET
latency-p99         250ms       45ms         18%  MET
max-outage         2000ms      217ms         11%  MET

seed 7, 3 nodes, under chaos
SLO                target     actual  budget used  status
availability        0.900      0.812        188%  MISSED
latency-p99         250ms       77ms         31%  MET
max-outage         2000ms     1051ms         53%  MET
```

The targets are examples, not claims about Raft: the point is that the budget is measured, so
a change that makes the cluster slower to recover shows up as a number instead of a hunch.

**Anomaly detection.** A robust z-score (median and MAD) over per-window throughput and latency
flags unusual windows, and each flag is attributed to the injected faults active around it:

```console
$ raftchaos anomalies --seed 7
seed 7: 3 of 16 windows flagged
   2000-2500 ms  throughput drop  score -0.9  faults: crash, partition
   3000-3500 ms  latency spike    score +4.5  faults: crash, partition
   5500-6000 ms  latency spike    score +4.7  faults: isolate
same seed without faults: 0 of 16 windows flagged
```

Under chaos nearly every window is close to some fault, so "did it flag a faulty window"
would be a meaningless score. The check that means something is the flag rate with and without
faults. Over seeds 0-29 the detector flags 7 of 480 windows (1.5%) on fault-free runs and 119
of 480 (25%) under chaos. It is a baseline detector, not a production one. A window with no
successful operation at all is flagged even when its score is below the 3.5 threshold, which is
why the first line above shows -0.9.

**Timelines.** `raftchaos timeline --seed 1 --bug double_vote --out run.svg` draws a run: role of
every node over time, network faults, successful client operations, and a red line where an
invariant broke.

The timeline of a run with a planted bug is shown in the [README](../README.md).
