import random
from dataclasses import replace

import pytest

from raftchaos import Bugs, SimConfig, run_simulation
from raftchaos.experiments import isolation_config, measure, run_isolation
from raftchaos.messages import AppendEntries, LogEntry, PreVote, PreVoteReply, RequestVote
from raftchaos.node import RaftConfig, RaftNode, Role

PV = RaftConfig(pre_vote=True)


def node(node_id=0, peers=(1, 2), config=PV):
    return RaftNode(node_id, list(peers), random.Random(0), now=0, config=config)


def test_timeout_asks_for_pre_votes_without_raising_the_term():
    n = node()
    out = n.tick(10_000)
    assert n.current_term == 0 and n.role is Role.FOLLOWER
    assert {dst for dst, _ in out} == {1, 2}
    assert all(isinstance(m, PreVote) and m.term == 1 for _, m in out)


def test_majority_of_pre_votes_starts_a_real_election():
    n = node()
    n.tick(10_000)
    out = n.receive(1, PreVoteReply(1, True, 1), 10_001)
    assert n.role is Role.CANDIDATE and n.current_term == 1
    assert any(isinstance(m, RequestVote) for _, m in out)


def test_pre_vote_is_refused_while_the_leader_is_alive():
    n = node(1, (0, 2))
    n.receive(0, AppendEntries(1, 0, 0, 0, (), 0), 100)
    ((dst, reply),) = n.receive(2, PreVote(2, 2, 0, 0), 120)
    assert dst == 2 and not reply.granted
    assert n.current_term == 1  # a pre-vote never moves the receiver's term
    ((_, later),) = n.receive(2, PreVote(2, 2, 0, 0), 100 + PV.election_timeout_min + 1)
    assert later.granted


def test_pre_vote_is_refused_to_a_stale_log():
    n = node(1, (0, 2))
    n.storage.log.append(LogEntry(1, ("noop",)))
    n.storage.current_term = 1
    ((_, reply),) = n.receive(2, PreVote(2, 2, 0, 0), 5_000)
    assert not reply.granted


def test_stale_pre_vote_rounds_are_ignored():
    n = node()
    n.tick(10_000)
    n.tick(20_000)  # a second round replaces the first
    n.receive(1, PreVoteReply(1, True, 999), 20_001)
    assert n.role is Role.FOLLOWER


@pytest.mark.parametrize("profile", ["default", "adversarial"])
def test_correct_raft_with_pre_vote_survives_faults(profile):
    base = SimConfig.adversarial() if profile == "adversarial" else SimConfig()
    config = replace(base, raft=replace(base.raft, pre_vote=True))
    for seed in range(20):
        result = run_simulation(seed, config)
        assert result.ok, f"seed {seed}: {result.violation}"


def test_planted_bugs_are_still_found_with_pre_vote():
    for bug, seeds in (("double_vote", range(10)), ("forget_vote_on_restart", range(30))):
        base = SimConfig.adversarial(bugs=Bugs.only(bug))
        config = replace(base, raft=replace(base.raft, pre_vote=True))
        assert any(not run_simulation(s, config).ok for s in seeds), bug


def test_isolated_follower_disrupts_the_leader_only_without_pre_vote():
    without = run_isolation(range(10), pre_vote=False)
    with_pv = run_isolation(range(10), pre_vote=True)
    assert without.safe == with_pv.safe == 10
    assert without.disrupted == 10 and without.mean_term_inflation > 3
    assert with_pv.disrupted == 0 and with_pv.mean_term_inflation == 0


def test_measure_identifies_the_isolated_node():
    run = measure(run_simulation(0, isolation_config(True)))
    assert run.ok and run.isolated in (0, 1, 2)
    assert run.isolated_term_at_heal == run.term_before
