import json
import re
from pathlib import Path

from raftchaos import Bugs, SimConfig, run_simulation
from raftchaos.recorder import DELIVERED
from raftchaos.viz import DEMO_SCENARIOS, build_html, demo_html, record

ROOT = Path(__file__).resolve().parent.parent


def embedded_data(html):
    match = re.search(r'<script id="data" type="application/json">(.*?)</script>', html, re.S)
    assert match, "data block missing"
    return json.loads(match.group(1))


def test_recording_does_not_change_the_run():
    for seed in (3, 7):
        plain, recorded = run_simulation(seed), run_simulation(seed, record=True)
        assert plain.stats == recorded.stats
        assert plain.violation == recorded.violation
        assert plain.recording is None and recorded.recording is not None


def test_recording_is_consistent():
    result = run_simulation(1, SimConfig(bugs=Bugs.only("double_vote")), record=True)
    data = result.recording.export(result, "t", "double_vote", "default")
    assert len(data["states"]) == 3
    for rows in data["states"]:
        times = [r[0] for r in rows]
        assert times == sorted(times)
    sends = [m[0] for m in data["messages"]]
    assert sends == sorted(sends)
    for m in data["messages"]:
        assert m[5] >= m[0]  # nothing arrives before it was sent
    delivered = sum(1 for m in data["messages"] if m[6] == DELIVERED)
    assert delivered > 0
    assert data["violation"]["kind"] == "election-safety"
    assert "--bug double_vote" in data["reproduce"]


def test_demo_scenarios_show_what_they_claim():
    for scenario in DEMO_SCENARIOS:
        data = record(scenario)
        if scenario.bug is None:
            assert data["violation"] is None, scenario.key
        else:
            assert data["violation"] is not None, scenario.key


def test_html_embeds_data_safely():
    scenario = DEMO_SCENARIOS[0]
    data = record(scenario)
    data["description"] = "tricky </script><script>alert(1)</script>"
    html = build_html([data])
    assert html.count("</script>") == 2  # the data block and the app script, nothing injected
    assert embedded_data(html)["scenarios"][0]["description"] == data["description"]


def test_published_demo_page_is_up_to_date():
    page = ROOT / "docs" / "viz" / "index.html"
    # Compare to a bool first: a failing == on two large strings makes pytest compute a diff
    # of hundreds of kilobytes, which takes minutes.
    up_to_date = page.read_text(encoding="utf-8") == demo_html()
    assert up_to_date, "docs/viz/index.html is stale: run `raftchaos viz --out docs/viz/index.html`"
