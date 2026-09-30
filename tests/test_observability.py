import xml.etree.ElementTree as ET

from raftchaos import Bugs, SimConfig, run_simulation
from raftchaos.anomaly import analyze, fault_intervals
from raftchaos.metrics import Metrics, compute_metrics, to_prometheus
from raftchaos.sim import TimelineEvent
from raftchaos.slo import Slo, default_slos, evaluate, render_report
from raftchaos.timeline_svg import render_timeline

QUIET = SimConfig(nemesis=False)


def test_percentile_uses_nearest_rank():
    m = Metrics(1000, 250, 4, 0, 0, latencies_ms=[10, 20, 30, 40])
    assert m.percentile(50) == 20
    assert m.percentile(99) == 40
    assert Metrics(1000, 250, 0, 0, 0).percentile(99) == 0


def test_steady_state_is_available_after_the_first_election():
    for seed in range(5):
        metrics = compute_metrics(run_simulation(seed, QUIET))
        assert metrics.availability >= 0.95  # only the initial election is unavailable
        assert metrics.max_outage_ms < 1000


def test_chaos_costs_availability():
    quiet = [compute_metrics(run_simulation(s, QUIET)).availability for s in range(8)]
    chaos = [compute_metrics(run_simulation(s)).availability for s in range(8)]
    assert sum(chaos) < sum(quiet)


def test_prometheus_output_is_well_formed():
    text = to_prometheus(compute_metrics(run_simulation(3)), {"seed": "3"})
    samples = [line for line in text.splitlines() if line and not line.startswith("#")]
    assert samples
    for line in samples:
        name_and_labels, value = line.rsplit(" ", 1)
        float(value)
        assert name_and_labels.startswith("raftchaos_")
    buckets = [int(line.rsplit(" ", 1)[1]) for line in samples if "op_latency_ms_bucket" in line]
    assert buckets == sorted(buckets)  # cumulative histogram never decreases
    assert 'seed="3"' in text


def test_budget_accounting():
    availability = Slo("a", "", 0.9, "ratio", lambda m: m.availability, higher_is_better=True)
    assert availability.budget_used(1.0) == 0
    assert abs(availability.budget_used(0.95) - 0.5) < 1e-9
    assert availability.budget_used(0.8) > 1
    latency = Slo("l", "", 200.0, "ms", lambda m: m.percentile(99))
    assert latency.budget_used(100) == 0.5


def test_steady_state_meets_default_slos_and_report_renders():
    results = evaluate(compute_metrics(run_simulation(7, QUIET)), default_slos())
    assert all(r.met for r in results)
    report = render_report("title", results)
    assert "MET" in report and "MISSED" not in report


def test_fault_intervals_from_timeline():
    timeline = [
        TimelineEvent(100, "partition", None, "partition [0] | [1, 2]"),
        TimelineEvent(400, "heal", None, "heal network"),
        TimelineEvent(500, "crash", 1, "crash node 1"),
        TimelineEvent(900, "restart", 1, "restart node 1"),
        TimelineEvent(1000, "flaky", None, "drop probability 0.3"),
    ]
    got = [(f.kind, f.start, f.end) for f in fault_intervals(timeline, 2000)]
    assert got == [
        ("partition", 100, 400),
        ("crash", 500, 900),
        ("flaky", 1000, 2000),
    ]


def test_detector_is_quiet_without_faults_and_louder_with_them():
    quiet = sum(len(analyze(run_simulation(s, QUIET)).anomalies) for s in range(10))
    noisy = sum(len(analyze(run_simulation(s)).anomalies) for s in range(10))
    windows = 10 * analyze(run_simulation(0, QUIET)).windows
    assert quiet / windows < 0.05
    assert noisy > 3 * max(quiet, 1)


def test_timeline_svg_is_valid_xml_and_marks_violations():
    healthy = render_timeline(run_simulation(2))
    ET.fromstring(healthy)
    assert "violated" not in healthy
    buggy = render_timeline(run_simulation(1, SimConfig(bugs=Bugs.only("double_vote"))))
    root = ET.fromstring(buggy)
    assert "election-safety violated" in buggy
    assert root.tag.endswith("svg")
