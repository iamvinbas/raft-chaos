import asyncio
import json
import subprocess
import sys

import pytest

from raftchaos.messages import Snapshot
from raftchaos.node import Role
from raftchaos.runtime.backup import backup_to_json, describe, fetch_backup, load_backup
from raftchaos.runtime.client import KvClient
from raftchaos.runtime.server import NodeServer
from raftchaos.runtime.storage import FileStorage
from test_runtime import entry, free_ports, wait_for


@pytest.mark.parametrize(
    "module",
    [
        "raftchaos.runtime.server",
        "raftchaos.runtime.backup",
        "raftchaos.runtime.client",
        "raftchaos.live.bridge",
        "raftchaos.cli",
    ],
)
def test_module_imports_on_its_own(module):
    # The test session imports modules in an order that can hide an import cycle.
    subprocess.run([sys.executable, "-c", f"import {module}"], check=True)


def test_seed_starts_an_empty_node_from_a_backup(tmp_path):
    snap = Snapshot(40, 3, (("k", 1),), (("c1", 7, 1),))
    store = FileStorage(tmp_path / "n")
    assert store.is_empty()
    store.seed(snap)
    reloaded = FileStorage(tmp_path / "n")
    assert reloaded.state.snapshot == snap
    assert reloaded.state.current_term == 3 and reloaded.state.log == []


def test_seed_refuses_to_overwrite_data(tmp_path):
    store = FileStorage(tmp_path / "n")
    store.state.log.append(entry(1, 1))
    store.sync()
    with pytest.raises(ValueError, match="existing data"):
        FileStorage(tmp_path / "n").seed(Snapshot(5, 1, (), ()))


def test_backup_file_format_is_checked(tmp_path):
    good = tmp_path / "good.json"
    good.write_text(json.dumps(backup_to_json(Snapshot(9, 2, (("x", 1),), ()), 0)))
    snap, raw = load_backup(good)
    assert snap.last_index == 9 and "1 keys" in describe(raw)
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"format": "something else"}))
    with pytest.raises(ValueError):
        load_backup(bad)


def test_cluster_lost_entirely_is_rebuilt_from_a_backup(tmp_path):
    async def cluster(data_dir, restore=None):
        ports = free_ports(6)
        addresses = {i: ("127.0.0.1", ports[i]) for i in range(3)}
        servers = [NodeServer(i, addresses, data_dir, metrics_port=ports[3 + i]) for i in range(3)]
        if restore is not None:
            for s in servers:
                s.storage.seed(restore)
        for s in servers:
            await s.start()
        await wait_for(lambda: any(s.node.role is Role.LEADER for s in servers))
        metrics = [("127.0.0.1", ports[3 + i]) for i in range(3)]
        return servers, [addresses[i] for i in range(3)], metrics

    async def scenario():
        servers, nodes, metrics = await cluster(tmp_path / "old")
        try:
            client = KvClient(nodes)
            for i in range(20):
                await client.put(f"key{i}", i)
            raw = await fetch_backup(metrics)
        finally:
            await asyncio.gather(*(s.stop() for s in servers), return_exceptions=True)

        # Every node's files are gone, as after `docker compose down -v`.
        path = tmp_path / "backup.json"
        path.write_text(json.dumps(raw))
        snapshot, _ = load_backup(path)
        assert dict(snapshot.data) == {f"key{i}": i for i in range(20)}

        servers, nodes, _ = await cluster(tmp_path / "new", restore=snapshot)
        try:
            client = KvClient(nodes)
            assert await client.get("key7") == 7
            await client.put("after", "restore")
            assert await client.get("after") == "restore"
            leader = next(s for s in servers if s.node.role is Role.LEADER)
            assert leader.node.current_term > snapshot.last_term  # a new election, past the backup
        finally:
            await asyncio.gather(*(s.stop() for s in servers), return_exceptions=True)

    asyncio.run(scenario())
