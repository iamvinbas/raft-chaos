"""Newline-delimited JSON framing for Raft messages.

One frame is `{"src": <node id or client id>, "type": <message class>, "body": {...}}`.
Frames from the network are untrusted: `decode` raises ValueError on anything malformed.
"""

from __future__ import annotations

import dataclasses
import json
from typing import Any

from ..messages import (
    Addr,
    AppendEntries,
    AppendEntriesReply,
    ClientRequest,
    ClientResponse,
    InstallSnapshot,
    InstallSnapshotReply,
    LogEntry,
    Message,
    PreVote,
    PreVoteReply,
    RequestVote,
    RequestVoteReply,
    Snapshot,
)

_TYPES: dict[str, type[Any]] = {
    cls.__name__: cls
    for cls in (
        RequestVote,
        RequestVoteReply,
        PreVote,
        PreVoteReply,
        AppendEntries,
        AppendEntriesReply,
        InstallSnapshot,
        InstallSnapshotReply,
        ClientRequest,
        ClientResponse,
    )
}


def entry_to_json(entry: LogEntry) -> dict[str, Any]:
    return {
        "term": entry.term,
        "command": list(entry.command),
        "client": entry.client,
        "req_id": entry.req_id,
    }


def entry_from_json(raw: dict[str, Any]) -> LogEntry:
    return LogEntry(int(raw["term"]), tuple(raw["command"]), raw.get("client"), int(raw["req_id"]))


def snapshot_to_json(snap: Snapshot) -> dict[str, Any]:
    return {
        "last_index": snap.last_index,
        "last_term": snap.last_term,
        "data": [list(pair) for pair in snap.data],
        "sessions": [list(session) for session in snap.sessions],
    }


def snapshot_from_json(raw: dict[str, Any]) -> Snapshot:
    return Snapshot(
        int(raw["last_index"]),
        int(raw["last_term"]),
        tuple((k, v) for k, v in raw["data"]),
        tuple((str(c), int(rid), res) for c, rid, res in raw["sessions"]),
    )


def encode(src: Addr, msg: Message) -> bytes:
    body = dataclasses.asdict(msg)
    if isinstance(msg, AppendEntries):
        body["entries"] = [entry_to_json(e) for e in msg.entries]
    if isinstance(msg, ClientRequest):
        body["command"] = list(msg.command)
    if isinstance(msg, InstallSnapshot):
        body["snapshot"] = snapshot_to_json(msg.snapshot)
    frame = {"src": src, "type": type(msg).__name__, "body": body}
    return json.dumps(frame, separators=(",", ":")).encode() + b"\n"


def decode(line: bytes) -> tuple[Addr, Message]:
    try:
        frame = json.loads(line)
        src = frame["src"]
        cls = _TYPES[frame["type"]]
        body = dict(frame["body"])
        if not isinstance(src, (int, str)) or isinstance(src, bool):
            raise ValueError("bad source")
        if cls is AppendEntries:
            body["entries"] = tuple(entry_from_json(e) for e in body["entries"])
        elif cls is ClientRequest:
            body["command"] = tuple(body["command"])
        elif cls is InstallSnapshot:
            body["snapshot"] = snapshot_from_json(body["snapshot"])
        return src, cls(**body)
    except (KeyError, TypeError, AttributeError, json.JSONDecodeError) as exc:
        raise ValueError(f"malformed frame: {exc}") from exc
