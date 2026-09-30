import asyncio
import json
import socket

import pytest
from hypothesis import given
from hypothesis import strategies as st

from raftchaos.messages import (
    AppendEntries,
    AppendEntriesReply,
    ClientRequest,
    ClientResponse,
    InstallSnapshot,
    InstallSnapshotReply,
    LogEntry,
    PreVote,
    PreVoteReply,
    RequestVote,
    RequestVoteReply,
    Snapshot,
)
from raftchaos.node import RaftConfig, Role
from raftchaos.runtime.client import KvClient, Unavailable, fetch_status
from raftchaos.runtime.codec import decode, encode, entry_to_json, snapshot_to_json
from raftchaos.runtime.server import NodeServer
from raftchaos.runtime.storage import FileStorage
from raftchaos.runtime.verify import run_verify

# ---- codec ---------------------------------------------------------------------------------

scalars = st.one_of(st.none(), st.integers(-(10**6), 10**6), st.text(max_size=12), st.booleans())
commands = st.one_of(
    st.tuples(st.just("put"), st.text(max_size=6), scalars),
    st.tuples(st.just("get"), st.text(max_size=6)),
    st.just(("noop",)),
)
entries = st.builds(
    LogEntry,
    st.integers(0, 50),
    commands,
    st.one_of(st.none(), st.text(min_size=1, max_size=6)),
    st.integers(0, 10**6),
)
small = st.integers(0, 10**6)
snapshots = st.builds(
    Snapshot,
    small,
    small,
    st.lists(st.tuples(st.text(max_size=4), scalars), max_size=4).map(tuple),
    st.lists(st.tuples(st.text(min_size=1, max_size=4), small, scalars), max_size=3).map(tuple),
)
messages = st.one_of(
    st.builds(RequestVote, small, small, small, small),
    st.builds(RequestVoteReply, small, st.booleans()),
    st.builds(PreVote, small, small, small, small),
    st.builds(PreVoteReply, small, st.booleans(), small),
    st.builds(
        AppendEntries, small, small, small, small, st.lists(entries, max_size=5).map(tuple), small
    ),
    st.builds(AppendEntriesReply, small, st.booleans(), small, small),
    st.builds(InstallSnapshot, small, small, snapshots),
    st.builds(InstallSnapshotReply, small, small),
    st.builds(ClientRequest, small, commands),
    st.builds(ClientResponse, small, st.booleans(), scalars, st.one_of(st.none(), small)),
)


@given(src=st.one_of(small, st.text(min_size=1, max_size=8)), msg=messages)
def test_codec_roundtrip(src, msg):
    assert decode(encode(src, msg)) == (src, msg)


@pytest.mark.parametrize(
    "line",
    [
        b"",
        b"not json",
        b"{}",
        b'{"src":1,"type":"Nope","body":{}}',
        b'{"src":1,"type":"RequestVote","body":{"term":1}}',
        b'{"src":true,"type":"RequestVote","body":{}}',
    ],
)
def test_decode_rejects_malformed_frames(line):
    with pytest.raises(ValueError):
        decode(line)


# ---- storage -------------------------------------------------------------------------------


def entry(term, value):
    return LogEntry(term, ("put", "k", value), "c1", value)


def test_storage_persists_changes_and_reloads(tmp_path):
    base = tmp_path / "n"
    store = FileStorage(base)
    assert store.sync() is False  # nothing changed yet
    store.state.current_term = 3
    store.state.voted_for = 1
    store.state.log.extend([entry(3, 1), entry(3, 2)])
    assert store.sync() is True
    assert store.sync() is False  # unchanged state is not rewritten
    reloaded = FileStorage(base)
    assert (reloaded.state.current_term, reloaded.state.voted_for) == (3, 1)
    assert reloaded.state.log == store.state.log
    assert not reloaded.torn_tail


def test_appends_only_write_new_entries(tmp_path):
    store = FileStorage(tmp_path / "n")
    store.state.log.extend(entry(1, i) for i in range(50))
    store.sync()
    size = store.log_path.stat().st_size
    store.state.log.append(entry(1, 50))
    store.sync()
    grown = store.log_path.stat().st_size - size
    assert 0 < grown < 100  # one line, not a rewrite of 51 entries


def test_conflicting_suffix_is_truncated(tmp_path):
    base = tmp_path / "n"
    store = FileStorage(base)
    store.state.log.extend([entry(1, 1), entry(1, 2), entry(1, 3)])
    store.sync()
    del store.state.log[1:]  # what a follower does on a conflict
    store.state.log.append(entry(2, 9))
    store.sync()
    assert FileStorage(base).state.log == [entry(1, 1), entry(2, 9)]


def test_torn_last_line_is_dropped_on_load(tmp_path):
    base = tmp_path / "n"
    store = FileStorage(base)
    store.state.log.extend([entry(1, 1), entry(1, 2)])
    store.sync()
    with open(store.log_path, "ab") as handle:
        handle.write(b'{"term":1,"command":["put","k"')  # crash in the middle of a write
    reloaded = FileStorage(base)
    assert reloaded.torn_tail
    assert reloaded.state.log == [entry(1, 1), entry(1, 2)]
    reloaded.state.log.append(entry(1, 3))
    reloaded.sync()
    assert FileStorage(base).state.log == [entry(1, 1), entry(1, 2), entry(1, 3)]


def test_snapshot_compacts_the_files_and_reloads(tmp_path):
    base = tmp_path / "n"
    store = FileStorage(base)
    store.state.log.extend(entry(1, i) for i in range(1, 11))
    store.sync()
    # what a node does when it compacts entries 1..6
    store.state.snapshot = Snapshot(6, 1, (("k", 6),), (("c1", 6, 6),))
    del store.state.log[:6]
    assert store.sync() is True
    assert store.log_path.read_text().count("\n") == 4  # only entries 7..10 are left
    reloaded = FileStorage(base)
    assert reloaded.state.snapshot == store.state.snapshot
    assert reloaded.state.log == [entry(1, i) for i in range(7, 11)]
    reloaded.state.log.append(entry(1, 11))
    reloaded.sync()
    assert FileStorage(base).state.log[-1] == entry(1, 11)


def test_crash_between_snapshot_and_log_rewrite_is_harmless(tmp_path):
    base = tmp_path / "n"
    store = FileStorage(base)
    store.state.log.extend(entry(1, i) for i in range(1, 11))
    store.sync()
    # The snapshot reached the disk, the log rewrite did not: the old full log is still there.
    snap = Snapshot(6, 1, (("k", 6),), ())
    store.snapshot_path.write_text(json.dumps(snapshot_to_json(snap)))
    reloaded = FileStorage(base)
    assert reloaded.state.snapshot == snap
    assert reloaded.state.log == [entry(1, i) for i in range(7, 11)]


def test_log_files_from_before_indexes_still_load(tmp_path):
    base = tmp_path / "n"
    FileStorage(base)  # creates the files
    lines = [json.dumps(entry_to_json(entry(1, i))) for i in range(1, 4)]
    (tmp_path / "n.log.jsonl").write_text("\n".join(lines) + "\n")
    assert FileStorage(base).state.log == [entry(1, i) for i in range(1, 4)]


# ---- a real cluster over localhost TCP -----------------------------------------------------


def free_ports(n):
    socks = [socket.socket() for _ in range(n)]
    for s in socks:
        s.bind(("127.0.0.1", 0))
    ports = [s.getsockname()[1] for s in socks]
    for s in socks:
        s.close()
    return ports


async def wait_for(condition, timeout=8.0):
    end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < end:
        if condition():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition not reached in time")


def leader_of(servers):
    leaders = [s for s in servers if s.node and s.node.role is Role.LEADER]
    return leaders[0] if len(leaders) == 1 else None


async def start_cluster(tmp_path, n=3, metrics=False):
    ports = free_ports(n + (n if metrics else 0))
    addresses = {i: ("127.0.0.1", ports[i]) for i in range(n)}
    servers = [
        NodeServer(i, addresses, tmp_path, metrics_port=ports[n + i] if metrics else None)
        for i in range(n)
    ]
    for s in servers:
        await s.start()
    return addresses, servers, ports


def test_cluster_elects_leader_replicates_and_survives_leader_loss(tmp_path):
    async def scenario():
        addresses, servers, _ = await start_cluster(tmp_path)
        try:
            await wait_for(lambda: leader_of(servers) is not None)
            client = KvClient([addresses[i] for i in range(3)])
            await client.put("colour", "blue")
            assert await client.get("colour") == "blue"

            old = leader_of(servers)
            old_term = old.node.current_term
            await old.stop()
            rest = [s for s in servers if s is not old]
            await wait_for(lambda: leader_of(rest) is not None)
            assert leader_of(rest).node.current_term > old_term
            assert await client.get("colour") == "blue"  # committed data survived
            await client.put("colour", "green")

            # Restart the old leader from its data directory: term and log come back from disk.
            reborn = NodeServer(old.id, addresses, tmp_path)
            assert reborn.storage.state.current_term >= old_term
            assert len(reborn.storage.state.log) > 0
            await reborn.start()
            servers[servers.index(old)] = reborn
            leader = leader_of([s for s in servers if s is not reborn]) or leader_of(servers)
            await wait_for(lambda: reborn.node.commit_index >= leader.node.commit_index - 1)
            assert await client.get("colour") == "green"
        finally:
            await asyncio.gather(*(s.stop() for s in servers), return_exceptions=True)

    asyncio.run(scenario())


def test_garbage_frames_are_counted_and_do_not_break_the_node(tmp_path):
    async def scenario():
        addresses, servers, _ = await start_cluster(tmp_path)
        try:
            await wait_for(lambda: leader_of(servers) is not None)
            host, port = addresses[0]
            _, writer = await asyncio.open_connection(host, port)
            writer.write(b'garbage\n{"src":1}\n\x00\xff\n')
            await writer.drain()
            writer.close()
            await wait_for(lambda: servers[0].counters.bad_frames >= 3)
            client = KvClient([addresses[i] for i in range(3)])
            await client.put("still", "works")
            assert await client.get("still") == "works"
        finally:
            await asyncio.gather(*(s.stop() for s in servers), return_exceptions=True)

    asyncio.run(scenario())


def test_metrics_and_status_endpoints(tmp_path):
    async def scenario():
        addresses, servers, ports = await start_cluster(tmp_path, metrics=True)
        try:
            await wait_for(lambda: leader_of(servers) is not None)
            leader = leader_of(servers)
            status = json.loads(await fetch_status(("127.0.0.1", ports[3 + leader.id])))
            assert status["role"] == "leader" and status["id"] == leader.id
            assert status["pre_vote"] is True  # on by default in the real service
            reader, writer = await asyncio.open_connection("127.0.0.1", ports[3])
            writer.write(b"GET /metrics HTTP/1.1\r\n\r\n")
            await writer.drain()
            body = (await reader.read()).decode()
            writer.close()
            assert "raftchaos_node_term" in body and "raftchaos_node_is_leader" in body
        finally:
            await asyncio.gather(*(s.stop() for s in servers), return_exceptions=True)

    asyncio.run(scenario())


def test_real_cluster_history_is_linearizable(tmp_path):
    async def scenario():
        addresses, servers, _ = await start_cluster(tmp_path)
        try:
            await wait_for(lambda: leader_of(servers) is not None)
            nodes = [addresses[i] for i in range(3)]
            report = await run_verify(nodes, seconds=2.0, clients=3, seed=1)
            assert report.ok, report
            assert report.ops_ok > 10
        finally:
            await asyncio.gather(*(s.stop() for s in servers), return_exceptions=True)

    asyncio.run(scenario())


def test_client_ignores_a_late_reply_to_an_older_request():
    # A node may answer a request the client already gave up on, on the connection the client
    # opened for its next request. That stale reply must not be taken as the new answer.
    async def scenario():
        async def server(reader, writer):
            line = await reader.readline()
            _, request = decode(line)
            stale = ClientResponse(request.req_id - 1, True, "stale value", 0)
            writer.write(encode(0, stale))
            writer.write(encode(0, ClientResponse(request.req_id, True, "fresh value", 0)))
            await writer.drain()
            writer.close()

        srv = await asyncio.start_server(server, "127.0.0.1", 0)
        port = srv.sockets[0].getsockname()[1]
        client = KvClient([("127.0.0.1", port)])
        client._req_id = 7  # as if earlier requests existed
        try:
            assert await client.get("k", 2.0) == "fresh value"
        finally:
            srv.close()
            await srv.wait_closed()

    asyncio.run(scenario())


def test_late_commit_of_an_abandoned_put_does_not_answer_a_later_get(tmp_path):
    # The leader loses its followers, a put times out on the client, the client moves on to a
    # get of another key. When the followers return, the leader commits the old put and replies
    # to it on the connection the client opened for the get. The get must still read its key.
    async def scenario():
        addresses, servers, _ = await start_cluster(tmp_path)
        try:
            await wait_for(lambda: leader_of(servers) is not None)
            leader = leader_of(servers)
            followers = [s for s in servers if s is not leader]
            for f in followers:
                await f.stop()
            client = KvClient([addresses[leader.id]])
            try:
                await client.put("k", "late", 0.4)
            except Unavailable:
                pass
            else:
                raise AssertionError("put should not commit without a majority")
            pending_get = asyncio.create_task(client.get("other", 5.0))
            await asyncio.sleep(0.1)
            reborn = [NodeServer(f.id, addresses, tmp_path) for f in followers]
            for r in reborn:
                await r.start()
            servers[:] = [leader, *reborn]
            assert await pending_get is None  # "other" was never written
        finally:
            await asyncio.gather(*(s.stop() for s in servers), return_exceptions=True)

    asyncio.run(scenario())


def test_node_back_after_a_long_outage_catches_up_with_a_snapshot(tmp_path):
    async def scenario():
        config = RaftConfig(pre_vote=True, snapshot_every=5)
        ports = free_ports(3)
        addresses = {i: ("127.0.0.1", ports[i]) for i in range(3)}
        servers = [NodeServer(i, addresses, tmp_path, config=config) for i in range(3)]
        for s in servers:
            await s.start()
        try:
            await wait_for(lambda: leader_of(servers) is not None)
            leader = leader_of(servers)
            away = next(s for s in servers if s is not leader)
            await away.stop()
            client = KvClient([addresses[leader.id]])
            for i in range(30):
                await client.put("k", i)
            assert leader.node.snap_index >= 25  # the leader compacted past the absent node

            back = NodeServer(away.id, addresses, tmp_path, config=config)
            await back.start()
            servers[servers.index(away)] = back
            await wait_for(lambda: back.node.commit_index >= leader.node.commit_index)
            assert back.node.snap_index > 0  # it was brought up to date by a snapshot
            assert back.node.kv.data == leader.node.kv.data == {"k": 29}
            assert back.storage.snapshot_path.exists()
        finally:
            await asyncio.gather(*(s.stop() for s in servers), return_exceptions=True)

    asyncio.run(scenario())
