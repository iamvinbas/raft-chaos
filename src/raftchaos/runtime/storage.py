"""Durable Raft state: a write-ahead log plus a small metadata file.

- `<base>.log.jsonl`: one log entry per line, append-only. Appending costs only the new
  entries. A conflicting suffix (rare) is cut with `truncate` at a remembered byte offset.
- `<base>.meta.json`: current term and vote, rewritten atomically only when they change.

Everything is fsynced before `sync()` returns, and the server sends no reply before that.
A crash in the middle of an append leaves a torn last line; loading drops it, which is safe
because an entry is acknowledged only after its fsync completed.

The first version rewrote the whole log on every change. That is O(log size) per message, and
on a real cluster it slowed a rejoining follower's catch-up to about 40 entries per second.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from ..messages import LogEntry
from ..node import Storage
from .codec import entry_from_json, entry_to_json


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class FileStorage:
    """Wraps the in-memory `Storage` a `RaftNode` mutates, and persists changes on demand."""

    def __init__(self, base: Path) -> None:
        self.meta_path = base.with_name(base.name + ".meta.json")
        self.log_path = base.with_name(base.name + ".log.jsonl")
        self.state = Storage()
        self.torn_tail = False  # set when loading found a partly written last entry
        base.parent.mkdir(parents=True, exist_ok=True)

        if self.meta_path.exists():
            meta = json.loads(self.meta_path.read_text())
            self.state.current_term = int(meta["current_term"])
            self.state.voted_for = meta["voted_for"]
        self._meta = self._meta_json()

        # _persisted[i] is the entry object on disk at index i+1, _offsets[i] where it starts.
        self._persisted: list[LogEntry] = []
        self._offsets: list[int] = []
        if self.log_path.exists():
            self._load_log()
        else:
            self.log_path.touch()
            _fsync_dir(self.log_path.parent)
        self.state.log = list(self._persisted)

    def _meta_json(self) -> str:
        return json.dumps(
            {"current_term": self.state.current_term, "voted_for": self.state.voted_for}
        )

    def _load_log(self) -> None:
        good = 0
        with open(self.log_path, "rb") as handle:
            for line in handle:
                if not line.endswith(b"\n"):
                    self.torn_tail = True
                    break
                try:
                    entry = entry_from_json(json.loads(line))
                except (ValueError, KeyError, TypeError):
                    self.torn_tail = True
                    break
                self._offsets.append(good)
                self._persisted.append(entry)
                good += len(line)
        if self.torn_tail:
            os.truncate(self.log_path, good)

    def sync(self) -> bool:
        """Persist whatever changed. Returns True if anything was written."""
        wrote = self._sync_log()
        meta = self._meta_json()
        if meta != self._meta:
            tmp = self.meta_path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as handle:
                handle.write(meta)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self.meta_path)
            _fsync_dir(self.meta_path.parent)
            self._meta = meta
            wrote = True
        return wrote

    def _sync_log(self) -> bool:
        log, disk = self.state.log, self._persisted
        # Entries are immutable and a conflict replaces them with new objects, so an unchanged
        # prefix is detected by identity without comparing the whole log.
        keep = min(len(disk), len(log))
        while keep and log[keep - 1] is not disk[keep - 1]:
            keep -= 1
        if keep == len(disk) == len(log):
            return False
        with open(self.log_path, "r+b") as handle:
            if keep < len(disk):
                handle.truncate(self._offsets[keep])
                del disk[keep:], self._offsets[keep:]
            handle.seek(0, os.SEEK_END)
            position = handle.tell()
            for entry in log[keep:]:
                line = json.dumps(entry_to_json(entry), separators=(",", ":")).encode() + b"\n"
                handle.write(line)
                self._offsets.append(position)
                disk.append(entry)
                position += len(line)
            handle.flush()
            os.fsync(handle.fileno())
        return True
