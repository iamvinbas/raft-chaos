"""The replicated key-value store that Raft's log drives.

Kept apart from the node so that the invariant checker can replay the committed history into a
reference copy, and so that snapshots can capture and restore the whole state in one place.
"""

from __future__ import annotations

from .messages import Command, LogEntry, Snapshot

# The comparable form of a store's state: sorted data and sorted sessions.
Frozen = tuple[tuple[tuple[str, object], ...], tuple[tuple[str, int, object], ...]]


class KvStore:
    def __init__(self) -> None:
        self.data: dict[object, object] = {}
        # client -> (last request id applied, its result). The network may duplicate a request,
        # so the same client operation can be committed twice; it must run only once.
        self.sessions: dict[str, tuple[int, object]] = {}

    def apply(self, entry: LogEntry) -> object:
        # Each client numbers its requests in increasing order: applying one that is not newer
        # than the last would let an old write overwrite a newer one.
        if entry.client is not None:
            last_id, last_result = self.sessions.get(entry.client, (-1, None))
            if entry.req_id <= last_id:
                return last_result if entry.req_id == last_id else None
        result = self._execute(entry.command)
        if entry.client is not None:
            self.sessions[entry.client] = (entry.req_id, result)
        return result

    def _execute(self, command: Command) -> object:
        kind = command[0]
        if kind == "put":
            self.data[command[1]] = command[2]
            return command[2]
        if kind == "get":
            return self.data.get(command[1])
        return None

    def freeze(self) -> Frozen:
        data = tuple(sorted(((str(k), v) for k, v in self.data.items()), key=lambda kv: kv[0]))
        sessions = tuple(sorted((c, rid, res) for c, (rid, res) in self.sessions.items()))
        return data, sessions

    def to_snapshot(self, last_index: int, last_term: int, sessions: bool = True) -> Snapshot:
        data, frozen_sessions = self.freeze()
        return Snapshot(last_index, last_term, data, frozen_sessions if sessions else ())

    @classmethod
    def from_snapshot(cls, snapshot: Snapshot) -> KvStore:
        store = cls()
        store.data = dict(snapshot.data)
        store.sessions = {client: (rid, res) for client, rid, res in snapshot.sessions}
        return store


def freeze_snapshot(snapshot: Snapshot) -> Frozen:
    return KvStore.from_snapshot(snapshot).freeze()
