import random
from dataclasses import replace

import pytest

from raftchaos import Bugs, SimConfig, run_simulation
from raftchaos.messages import (
    AppendEntries,
    AppendEntriesReply,
    ClientRequest,
    InstallSnapshot,
    InstallSnapshotReply,
    LogEntry,
    Snapshot,
)
from raftchaos.node import RaftConfig, RaftNode, Role, Storage
from raftchaos.statemachine import KvStore


def put(term, value, req_id=0, client=None):
    return LogEntry(term, ("put", "k", value), client, req_id)


def single_node(every=5, storage=None):
    config = RaftConfig(snapshot_every=every)
    return RaftNode(0, [], random.Random(0), now=0, config=config, storage=storage)


def test_log_is_compacted_every_n_applied_entries():
    n = single_node(every=5)
    n.tick(10_000)  # elects itself and commits a no-op at index 1
    for i in range(11):
        n.receive("c0", ClientRequest(i + 1, ("put", "k", i)), 10_001)
    assert n.last_index == 12
    assert n.snap_index == 10  # snapshots at 5 and 10
    assert len(n.log) == 2  # only entries 11 and 12 remain
    assert n.kv.data == {"k": 10}
    assert n.term_at(10) == n.storage.snapshot.last_term
    with pytest.raises(IndexError):
        n.term_at(9)


def test_restart_restores_the_snapshot():
    n = single_node(every=5)
    n.tick(10_000)
    for i in range(11):
        n.receive("c0", ClientRequest(i + 1, ("put", "k", i)), 10_001)
    reborn = single_node(every=5, storage=n.storage)
    assert reborn.commit_index == reborn.last_applied == 10
    assert reborn.kv.data == {"k": 8}  # the state at index 10: no-op + puts of 0..8
    assert reborn.kv.sessions["c0"][0] == 9


def leader_with_snapshot():
    snap = KvStore().to_snapshot(10, 1)
    storage = Storage(current_term=2, log=[put(1, 11), put(2, 12)], snapshot=snap)
    n = RaftNode(0, [1, 2], random.Random(0), config=RaftConfig(snapshot_every=5), storage=storage)
    n.role = Role.LEADER
    n.next_index = {1: 3, 2: 13}
    n.match_index = {1: 0, 2: 12}
    return n


def test_leader_sends_its_snapshot_to_a_follower_behind_it():
    n = leader_with_snapshot()
    out = dict(n.tick(10_000))
    assert isinstance(out[1], InstallSnapshot) and out[1].snapshot.last_index == 10
    assert isinstance(out[2], AppendEntries) and out[2].prev_log_index == 12


def test_install_reply_moves_the_follower_forward():
    n = leader_with_snapshot()
    out = n.receive(1, InstallSnapshotReply(2, 10), 10_000)
    assert n.match_index[1] == 10 and n.next_index[1] == 11
    assert any(isinstance(m, AppendEntries) and m.prev_log_index == 10 for _, m in out)


def follower(log, commit=0):
    n = RaftNode(1, [0, 2], random.Random(0), config=RaftConfig(snapshot_every=5))
    n.storage.current_term = 1
    n.storage.log.extend(log)
    n.commit_index = commit
    return n


def snapshot_at(index, term, value):
    store = KvStore()
    store.data = {"k": value}
    return store.to_snapshot(index, term)


def test_follower_keeps_the_entries_after_a_matching_snapshot():
    n = follower([put(1, v) for v in range(1, 16)])
    ((_, reply),) = n.receive(0, InstallSnapshot(1, 0, snapshot_at(10, 1, 10)), 100)
    assert reply == InstallSnapshotReply(1, 10)
    assert n.snap_index == 10 and n.last_index == 15
    assert [e.command[2] for e in n.log] == [11, 12, 13, 14, 15]
    assert n.commit_index == n.last_applied == 10 and n.kv.data == {"k": 10}


def test_follower_drops_a_conflicting_log_for_the_snapshot():
    n = follower([put(1, v) for v in range(1, 9)] + [put(2, 9), put(2, 10)])
    n.receive(0, InstallSnapshot(3, 0, snapshot_at(10, 3, 99)), 100)
    assert n.snap_index == 10 and n.log == [] and n.kv.data == {"k": 99}


def test_snapshot_older_than_what_the_follower_has_is_ignored():
    n = follower([put(1, v) for v in range(1, 16)], commit=12)
    ((_, reply),) = n.receive(0, InstallSnapshot(1, 0, snapshot_at(10, 1, 10)), 100)
    assert reply.match_index == 10
    assert n.snap_index == 0 and n.last_index == 15  # nothing was thrown away


def test_append_entries_below_the_snapshot_are_skipped():
    snap = snapshot_at(10, 1, 10)
    n = RaftNode(1, [0, 2], random.Random(0), storage=Storage(1, None, [], snap))
    entries = tuple(put(1, v) for v in range(9, 13))  # indexes 9..12
    ((_, reply),) = n.receive(0, AppendEntries(1, 0, 8, 1, entries, 10), 100)
    assert reply == AppendEntriesReply(1, True, 12, 0)
    assert n.last_index == 12 and [e.command[2] for e in n.log] == [11, 12]


def with_snapshots(config, every=10):
    return replace(config, raft=replace(config.raft, snapshot_every=every))


@pytest.mark.parametrize("nodes", [3, 5])
@pytest.mark.parametrize("profile", ["default", "adversarial"])
def test_correct_raft_with_snapshots_survives_faults(profile, nodes):
    base = (
        SimConfig.adversarial(n_nodes=nodes)
        if profile == "adversarial"
        else SimConfig(n_nodes=nodes)
    )
    config = with_snapshots(base)
    installed = 0
    for seed in range(12):
        result = run_simulation(seed, config)
        assert result.ok, f"seed {seed}: {result.violation}"
        installed += result.stats.get("snapshots_installed", 0)
    assert installed > 0  # the InstallSnapshot path was really exercised


def test_planted_snapshot_bugs_are_found():
    sessions = with_snapshots(SimConfig(bugs=Bugs.only("snapshot_without_sessions")), 20)
    assert not run_simulation(0, sessions).ok
    wipe = Bugs.only("install_snapshot_discards_log")
    # Random faults find it late, as a lost committed entry...
    lost = run_simulation(1121, with_snapshots(SimConfig(bugs=wipe), 20)).violation
    assert lost is not None and lost.kind == "leader-completeness"
    # ...late snapshot copies find it early, as a feedback loop of snapshots and wiped logs.
    storm = run_simulation(52, with_snapshots(SimConfig.adversarial(bugs=wipe), 20)).violation
    assert storm is not None and storm.kind == "liveness" and "storm" in storm.message


def test_snapshots_off_by_default_keep_published_runs_exact():
    # The README shows this run; turning snapshots into an option must not change it.
    stats = run_simulation(3).stats
    assert (stats["committed"], stats["msgs_sent"], stats["crashes"], stats["elections"]) == (
        147,
        2046,
        4,
        7,
    )
    assert "snapshots_taken" not in stats


def test_checker_rejects_a_snapshot_that_does_not_match_history():
    # A snapshot with the right index but made-up data must be reported, not trusted.
    result = run_simulation(
        0, with_snapshots(SimConfig(bugs=Bugs.only("snapshot_without_sessions")), 20)
    )
    assert result.violation.kind == "state-machine-safety"
    assert "snapshot" in result.violation.message


def test_snapshot_round_trips_through_the_store():
    store = KvStore()
    store.apply(put(1, 5, req_id=3, client="c1"))
    snap = store.to_snapshot(7, 2)
    assert isinstance(snap, Snapshot) and snap.last_index == 7
    assert KvStore.from_snapshot(snap).freeze() == store.freeze()
    assert KvStore.from_snapshot(store.to_snapshot(7, 2, sessions=False)).sessions == {}
