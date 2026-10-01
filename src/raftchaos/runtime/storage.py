"""Durable Raft state: a write-ahead log, a snapshot, and a small metadata file.

- `<base>.log.jsonl`: one log entry per line with its absolute index, append-only. Appending
  costs only the new entries. A conflicting suffix (rare) is cut with `truncate` at a
  remembered byte offset.
- `<base>.snapshot.json`: the state up to some index, which replaces that prefix of the log.
- `<base>.meta.json`: current term and vote, rewritten atomically only when they change.

Everything is fsynced before `sync()` returns, and the server sends no reply before that.
A crash in the middle of an append leaves a torn last line; loading drops it, which is safe
because an entry is acknowledged only after its fsync completed.

A new snapshot is written first, atomically, and only then is the log rewritten without the
prefix it covers (also atomically). A crash in between leaves an old, longer log next to the new
snapshot; loading skips the entries the snapshot already covers, using their indexes.

The first version rewrote the whole log on every change. That is O(log size) per message, and
on a real cluster it slowed a rejoining follower's catch-up to about 40 entries per second.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from ..messages import LogEntry, Snapshot
from ..node import Storage
from .codec import entry_from_json, entry_to_json, snapshot_from_json, snapshot_to_json


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_atomically(path: Path, data: bytes) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def _line(index: int, entry: LogEntry) -> bytes:
    return json.dumps({"i": index, **entry_to_json(entry)}, separators=(",", ":")).encode() + b"\n"


class FileStorage:
    """Wraps the in-memory `Storage` a `RaftNode` mutates, and persists changes on demand."""

    def __init__(self, base: Path) -> None:
        self.meta_path = base.with_name(base.name + ".meta.json")
        self.log_path = base.with_name(base.name + ".log.jsonl")
        self.snapshot_path = base.with_name(base.name + ".snapshot.json")
        self.state = Storage()
        self.torn_tail = False  # set when loading found a partly written last entry
        base.parent.mkdir(parents=True, exist_ok=True)

        if self.meta_path.exists():
            meta = json.loads(self.meta_path.read_text())
            self.state.current_term = int(meta["current_term"])
            self.state.voted_for = meta["voted_for"]
        self._meta = self._meta_json()

        if self.snapshot_path.exists():
            self.state.snapshot = snapshot_from_json(json.loads(self.snapshot_path.read_text()))
        self._snapshot: Snapshot | None = self.state.snapshot  # the one on disk

        # _persisted[k] is the entry object on disk right after the snapshot, plus k;
        # _offsets[k] is where its line starts in the log file.
        self._persisted: list[LogEntry] = []
        self._offsets: list[int] = []
        if self.log_path.exists():
            self._load_log()
        else:
            self.log_path.touch()
            _fsync_dir(self.log_path.parent)
        self.state.log = list(self._persisted)

    def is_empty(self) -> bool:
        """True for a node that has never stored anything."""
        state = self.state
        return state.snapshot is None and not state.log and state.current_term == 0

    def seed(self, snapshot: Snapshot) -> None:
        """Start an empty node from a backup: the snapshot becomes its whole history."""
        if not self.is_empty():
            raise ValueError("refusing to restore over existing data")
        self.state.snapshot = snapshot
        self.state.current_term = snapshot.last_term
        self.sync()

    def _base(self) -> int:
        return self.state.snapshot.last_index if self.state.snapshot else 0

    def _meta_json(self) -> str:
        return json.dumps(
            {"current_term": self.state.current_term, "voted_for": self.state.voted_for}
        )

    def _load_log(self) -> None:
        base = self._base()
        position = 0
        good = 0  # bytes of the file that hold complete, usable entries
        sequential = 0  # index of entries written before lines carried their own index
        with open(self.log_path, "rb") as handle:
            for line in handle:
                if not line.endswith(b"\n"):
                    self.torn_tail = True
                    break
                try:
                    raw = json.loads(line)
                    entry = entry_from_json(raw)
                except (ValueError, KeyError, TypeError):
                    self.torn_tail = True
                    break
                sequential += 1
                index = int(raw.get("i", sequential))
                if index <= base:
                    position += len(line)  # already inside the snapshot
                    good = position
                    continue
                if index != base + len(self._persisted) + 1:
                    self.torn_tail = True  # a gap: nothing after it can be trusted
                    break
                self._offsets.append(position)
                self._persisted.append(entry)
                position += len(line)
                good = position
        if self.torn_tail:
            os.truncate(self.log_path, good)

    def sync(self) -> bool:
        """Persist whatever changed. Returns True if anything was written."""
        if self.state.snapshot is not self._snapshot:
            self._install_snapshot()
            wrote = True
        else:
            wrote = self._sync_log()
        meta = self._meta_json()
        if meta != self._meta:
            _write_atomically(self.meta_path, meta.encode())
            self._meta = meta
            wrote = True
        return wrote

    def _install_snapshot(self) -> None:
        snapshot = self.state.snapshot
        assert snapshot is not None
        _write_atomically(self.snapshot_path, json.dumps(snapshot_to_json(snapshot)).encode())
        lines = [_line(snapshot.last_index + k + 1, e) for k, e in enumerate(self.state.log)]
        _write_atomically(self.log_path, b"".join(lines))
        self._snapshot = snapshot
        self._persisted = list(self.state.log)
        self._offsets = []
        position = 0
        for line in lines:
            self._offsets.append(position)
            position += len(line)

    def _sync_log(self) -> bool:
        log, disk = self.state.log, self._persisted
        # Entries are immutable and a conflict replaces them with new objects, so an unchanged
        # prefix is detected by identity without comparing the whole log.
        keep = min(len(disk), len(log))
        while keep and log[keep - 1] is not disk[keep - 1]:
            keep -= 1
        if keep == len(disk) == len(log):
            return False
        base = self._base()
        with open(self.log_path, "r+b") as handle:
            if keep < len(disk):
                handle.truncate(self._offsets[keep])
                del disk[keep:], self._offsets[keep:]
            handle.seek(0, os.SEEK_END)
            position = handle.tell()
            for k in range(keep, len(log)):
                line = _line(base + k + 1, log[k])
                handle.write(line)
                self._offsets.append(position)
                disk.append(log[k])
                position += len(line)
            handle.flush()
            os.fsync(handle.fileno())
        return True
