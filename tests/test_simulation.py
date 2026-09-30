import pytest

from raftchaos import BUG_NAMES, Bugs, SimConfig, run_simulation


def fingerprint(result):
    history = [(o.id, o.invoke, o.response, o.result) for o in result.history]
    return (result.violation, result.stats, history)


def test_same_seed_gives_identical_run():
    assert fingerprint(run_simulation(11)) == fingerprint(run_simulation(11))


def test_different_seeds_diverge():
    assert fingerprint(run_simulation(1)) != fingerprint(run_simulation(2))


@pytest.mark.parametrize("nodes", [3, 5])
def test_correct_raft_survives_faults(nodes):
    config = SimConfig(n_nodes=nodes)
    for seed in range(25):
        result = run_simulation(seed, config)
        assert result.ok, f"seed {seed}: {result.violation}"
        assert result.stats["committed"] > 0


def test_single_node_cluster_makes_progress():
    result = run_simulation(3, SimConfig(n_nodes=1, nemesis=False))
    assert result.ok
    assert result.stats["ops_ok"] > 0


def test_cluster_elects_exactly_one_leader_without_faults():
    result = run_simulation(5, SimConfig(nemesis=False, drop_prob=0.0, dup_prob=0.0))
    assert result.ok
    assert result.stats["elections"] == 1


@pytest.mark.parametrize(
    "bug",
    ["double_vote", "stale_log_vote", "commit_without_majority", "no_truncate_on_conflict"],
)
def test_simulator_finds_injected_bug(bug):
    config = SimConfig(bugs=Bugs.only(bug))
    assert any(not run_simulation(seed, config).ok for seed in range(20)), bug


def test_bug_registry_is_consistent():
    for name in BUG_NAMES:
        assert getattr(Bugs.only(name), name) is True
    with pytest.raises(ValueError):
        Bugs.only("no_such_bug")


def test_duplicated_client_requests_do_not_break_linearizability():
    # Seed 913 used to fail: a duplicated put was applied twice and rolled a key back.
    assert run_simulation(913, SimConfig()).ok
    assert run_simulation(913, SimConfig(dup_prob=0.3)).ok


@pytest.mark.parametrize("nodes", [3, 5])
def test_correct_raft_survives_adversarial_profile(nodes):
    config = SimConfig.adversarial(n_nodes=nodes)
    for seed in range(25):
        result = run_simulation(seed, config)
        assert result.ok, f"seed {seed}: {result.violation}"


@pytest.mark.parametrize(
    ("bug", "seeds"),
    [("forget_vote_on_restart", range(30)), ("commit_old_term", range(100))],
)
def test_adversarial_profile_finds_the_hard_bugs(bug, seeds):
    config = SimConfig.adversarial(bugs=Bugs.only(bug))
    assert any(not run_simulation(seed, config).ok for seed in seeds), bug
