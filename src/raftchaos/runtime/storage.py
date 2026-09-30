"""Durable Raft state: term, vote and log are fsynced before any reply leaves the node."""

from __future__ import annotations

import json
import os
from pathlib import Path

from ..node import Storage
from .codec import entry_from_json, entry_to_json


class FileStorage:
    """Wraps the in-memory `Storage` a `RaftNode` mutates, and persists it on demand."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.state = Storage()
        self._last = ""
        if path.exists():
            raw = json.loads(path.read_text())
            self.state.current_term = int(raw["current_term"])
            self.state.voted_for = raw["voted_for"]
            self.state.log = [entry_from_json(e) for e in raw["log"]]
            self._last = self._dump()

    def _dump(self) -> str:
        return json.dumps(
            {
                "current_term": self.state.current_term,
                "voted_for": self.state.voted_for,
                "log": [entry_to_json(e) for e in self.state.log],
            },
            separators=(",", ":"),
        )

    def sync(self) -> bool:
        """Write the state if it changed. Returns True if a write happened."""
        current = self._dump()
        if current == self._last:
            return False
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as handle:
            handle.write(current)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self.path)
        directory = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        self._last = current
        return True
