"""Build the web visualiser: one self-contained HTML file with recorded runs embedded.

The page needs no server and no build step. It works from disk, from GitHub Pages, or as a
CI artifact, and every scene in it is a deterministic replay of `raftchaos run`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from importlib import resources
from typing import Any

from ..bugs import Bugs
from ..sim import SimConfig, run_simulation


@dataclass(frozen=True)
class Scenario:
    key: str
    short: str
    title: str
    description: str
    seed: int
    bug: str | None = None
    profile: str = "default"
    nodes: int = 3


DEMO_SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        "healthy",
        "Correct Raft under chaos",
        "A correct cluster under chaos",
        "For eight seconds the network partitions, the leader is cut off and servers crash. "
        "Leaders come and go, but every safety invariant holds and no acknowledged write is lost.",
        7,
    ),
    Scenario(
        "double-vote",
        "Bug: double vote",
        "Planted bug: a server votes twice in one term",
        "One protocol check is removed: a server may vote for a second candidate in a term it "
        "already voted in. Watch the elections: sooner or later two servers win the same term.",
        1,
        "double_vote",
    ),
    Scenario(
        "forget-vote",
        "Bug: forgotten vote",
        "Planted bug: a restarted server forgets its vote",
        "The vote is not saved to disk. The adversarial profile crashes a server right after it "
        "votes; when it comes back it votes again, for someone else, in the same term.",
        4,
        "forget_vote_on_restart",
        "adversarial",
    ),
    Scenario(
        "figure-8",
        "Bug: Figure 8",
        "Planted bug: committing an old term's entry (Raft paper, Figure 8)",
        "The leader counts replicas of an entry from an earlier term and commits it. A leader "
        "that crashes mid-broadcast sets up the trap; a later leader then lacks a committed entry.",
        61,
        "commit_old_term",
        "adversarial",
    ),
    Scenario(
        "stale-log",
        "Bug: stale log vote",
        "Planted bug: voting for a candidate with a stale log",
        "Servers stop checking that a candidate's log is up to date before voting, so a server "
        "that missed committed entries can become leader.",
        0,
        "stale_log_vote",
    ),
)


def make_config(
    nodes: int, bug: str | None, profile: str, duration: int | None = None
) -> SimConfig:
    config = (
        SimConfig.adversarial(n_nodes=nodes)
        if profile == "adversarial"
        else SimConfig(n_nodes=nodes)
    )
    if duration:
        config = replace(config, duration_ms=duration)
    if bug:
        config = replace(config, bugs=Bugs.only(bug))
    return config


def record(scenario: Scenario, duration: int | None = None) -> dict[str, Any]:
    config = make_config(scenario.nodes, scenario.bug, scenario.profile, duration)
    result = run_simulation(scenario.seed, config, record=True)
    assert result.recording is not None
    data = result.recording.export(result, scenario.title, scenario.bug, scenario.profile)
    data.update(key=scenario.key, short=scenario.short, description=scenario.description)
    return data


def build_html(scenarios: list[dict[str, Any]]) -> str:
    template = resources.files(__package__).joinpath("template.html").read_text(encoding="utf-8")
    payload = json.dumps({"scenarios": scenarios}, separators=(",", ":"))
    payload = payload.replace("</", "<\\/")  # never close the script tag from inside the data
    return template.replace("__DATA__", payload)


def demo_html() -> str:
    return build_html([record(s) for s in DEMO_SCENARIOS])
