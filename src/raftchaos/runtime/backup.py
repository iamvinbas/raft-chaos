"""Backups of a running cluster: save the committed state to a file, start a new cluster from it.

A node's durability already covers crashes: everything is fsynced before any reply, and every
committed entry also lives on a majority. A backup covers what neither can: losing the whole
cluster, for example `docker compose down -v`, which deletes every node's files.

A backup is the applied state of the most up-to-date reachable node, in the same form as a Raft
snapshot (data, client sessions, index and term). Restoring starts every node of a new cluster
from that snapshot, so they agree on the history up to it from the first moment.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

from ..messages import Snapshot
from .codec import snapshot_from_json, snapshot_to_json

FORMAT = "raftchaos-backup/1"
Address = tuple[str, int]  # host, port (kept local: the server imports this module)


def backup_to_json(snapshot: Snapshot, node_id: int) -> dict[str, Any]:
    return {
        "format": FORMAT,
        "created": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source_node": node_id,
        "snapshot": snapshot_to_json(snapshot),
    }


def load_backup(path: Path) -> tuple[Snapshot, dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or raw.get("format") != FORMAT:
        raise ValueError(f"{path} is not a {FORMAT} file")
    return snapshot_from_json(raw["snapshot"]), raw


async def _get(address: Address, path: str, timeout: float = 2.0) -> dict[str, Any]:
    reader, writer = await asyncio.wait_for(asyncio.open_connection(*address), timeout=timeout)
    try:
        writer.write(f"GET {path} HTTP/1.1\r\nHost: node\r\n\r\n".encode())
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(), timeout=timeout)
        head, _, body = raw.partition(b"\r\n\r\n")
        if b" 200 " not in head.split(b"\r\n", 1)[0]:
            raise ValueError(f"{path} on {address[0]}:{address[1]} answered {head[:40]!r}")
        result = json.loads(body)
        if not isinstance(result, dict):
            raise ValueError("unexpected answer")
        return result
    finally:
        writer.close()


async def fetch_backup(metrics: list[Address]) -> dict[str, Any]:
    """Take the backup from the reachable node that has applied the most entries."""
    best: tuple[int, Address] | None = None
    for address in metrics:
        try:
            status = await _get(address, "/status")
        except (OSError, asyncio.TimeoutError, ValueError):
            continue
        applied = int(status.get("last_applied", 0))
        if best is None or applied > best[0]:
            best = (applied, address)
    if best is None:
        raise ConnectionError("no node answered on its metrics port")
    return await _get(best[1], "/backup")


def describe(raw: dict[str, Any]) -> str:
    snap = raw["snapshot"]
    return (
        f"backup of node {raw['source_node']} taken {raw['created']}: "
        f"{len(snap['data'])} keys, {len(snap['sessions'])} client sessions, "
        f"history up to index {snap['last_index']} (term {snap['last_term']})"
    )
