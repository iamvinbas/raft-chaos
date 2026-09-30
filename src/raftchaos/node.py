"""A Raft node as a pure, deterministic state machine.

The node performs no I/O and reads no clock. Callers feed it time (`tick`) and messages
(`receive`) and get back the messages to send. That is what lets the simulator replay any
run exactly from a seed.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum

from .bugs import Bugs
from .messages import (
    Addr,
    AppendEntries,
    AppendEntriesReply,
    ClientRequest,
    ClientResponse,
    Command,
    LogEntry,
    Message,
    Outbox,
    PreVote,
    PreVoteReply,
    RequestVote,
    RequestVoteReply,
)


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass(frozen=True)
class RaftConfig:
    election_timeout_min: int = 150  # ms
    election_timeout_max: int = 300
    heartbeat_interval: int = 50
    max_batch: int = 16  # entries per AppendEntries
    # Ask for pre-votes before starting an election, so a node that cannot win (for example
    # one cut off by a partition) never raises its term and never disrupts a healthy leader.
    pre_vote: bool = False


@dataclass
class Storage:
    """State that survives a crash (the equivalent of an fsynced file)."""

    current_term: int = 0
    voted_for: int | None = None
    log: list[LogEntry] = field(default_factory=list)


ApplyHook = Callable[[int, int, LogEntry], None]


class RaftNode:
    def __init__(
        self,
        node_id: int,
        peers: list[int],
        rng: random.Random,
        now: int = 0,
        config: RaftConfig | None = None,
        bugs: Bugs | None = None,
        storage: Storage | None = None,
        apply_hook: ApplyHook | None = None,
    ) -> None:
        self.id = node_id
        self.peers = peers
        self.rng = rng
        self.config = config or RaftConfig()
        self.bugs = bugs or Bugs()
        self.storage = storage if storage is not None else Storage()
        self.apply_hook = apply_hook

        if self.bugs.forget_vote_on_restart:
            self.storage.voted_for = None

        # Volatile state: lost on crash.
        self.role = Role.FOLLOWER
        self.leader_id: int | None = None
        self.commit_index = 0
        self.last_applied = 0
        self.state_machine: dict[object, object] = {}
        self.sessions: dict[str, tuple[int, object]] = {}  # client -> (last req_id, its result)
        self.votes: set[int] = set()
        self.prevotes: set[int] = set()
        self.prevote_term: int | None = None  # the term of the pre-vote round in progress
        self.last_leader_contact = -(10**9)
        self.next_index: dict[int, int] = {}
        self.match_index: dict[int, int] = {}
        self.pending: dict[int, tuple[Addr, int, int]] = {}  # log index -> (client, req_id, term)
        self.next_heartbeat = 0
        self.election_deadline = 0
        self._reset_election_timer(now)

    # ---- convenience views -------------------------------------------------------------

    @property
    def current_term(self) -> int:
        return self.storage.current_term

    @property
    def log(self) -> list[LogEntry]:
        return self.storage.log

    @property
    def last_index(self) -> int:
        return len(self.storage.log)

    def term_at(self, index: int) -> int:
        return self.storage.log[index - 1].term if index > 0 else 0

    @property
    def majority(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    # ---- driving the node --------------------------------------------------------------

    def tick(self, now: int) -> Outbox:
        out: Outbox = []
        if self.role is Role.LEADER:
            if now >= self.next_heartbeat:
                self._broadcast_append(out)
                self.next_heartbeat = now + self.config.heartbeat_interval
        elif now >= self.election_deadline:
            if self.config.pre_vote:
                self._start_pre_vote(now, out)
            else:
                self._start_election(now, out)
        return out

    def receive(self, src: Addr, msg: Message, now: int) -> Outbox:
        out: Outbox = []
        if isinstance(msg, ClientRequest):
            self._on_client_request(src, msg, now, out)
            return out
        if isinstance(msg, ClientResponse):
            return out  # nodes never consume client responses

        assert isinstance(src, int)
        if isinstance(msg, PreVote):
            # A pre-vote carries a term the candidate has not started: it must not move ours.
            self._on_pre_vote(src, msg, now, out)
            return out
        granted_pre_vote = isinstance(msg, PreVoteReply) and msg.granted
        if msg.term > self.storage.current_term and not granted_pre_vote:
            self._step_down(msg.term)
        if isinstance(msg, PreVoteReply):
            self._on_pre_vote_reply(src, msg, now, out)
        elif isinstance(msg, RequestVote):
            self._on_request_vote(src, msg, now, out)
        elif isinstance(msg, RequestVoteReply):
            self._on_vote_reply(src, msg, now, out)
        elif isinstance(msg, AppendEntries):
            self._on_append_entries(src, msg, now, out)
        elif isinstance(msg, AppendEntriesReply):
            self._on_append_reply(src, msg, out)
        return out

    # ---- term / role transitions -------------------------------------------------------

    def _reset_election_timer(self, now: int) -> None:
        cfg = self.config
        self.election_deadline = now + self.rng.randint(
            cfg.election_timeout_min, cfg.election_timeout_max
        )

    def _step_down(self, term: int) -> None:
        self.storage.current_term = term
        self.storage.voted_for = None
        self.role = Role.FOLLOWER
        self.leader_id = None
        self.votes = set()
        self.prevote_term = None
        self.pending.clear()

    def _start_pre_vote(self, now: int, out: Outbox) -> None:
        self.prevote_term = self.current_term + 1
        self.prevotes = {self.id}
        self._reset_election_timer(now)
        if len(self.prevotes) >= self.majority:
            self._start_election(now, out)
            return
        req = PreVote(self.prevote_term, self.id, self.last_index, self.term_at(self.last_index))
        for peer in self.peers:
            out.append((peer, req))

    def _start_election(self, now: int, out: Outbox) -> None:
        self.prevote_term = None
        self.storage.current_term += 1
        self.storage.voted_for = self.id
        self.role = Role.CANDIDATE
        self.leader_id = None
        self.votes = {self.id}
        self._reset_election_timer(now)
        if len(self.votes) >= self.majority:
            self._become_leader(now, out)
            return
        req = RequestVote(
            self.current_term, self.id, self.last_index, self.term_at(self.last_index)
        )
        for peer in self.peers:
            out.append((peer, req))

    def _become_leader(self, now: int, out: Outbox) -> None:
        self.role = Role.LEADER
        self.leader_id = self.id
        self.next_index = {p: self.last_index + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        # A no-op in the new term lets the leader commit entries from earlier terms safely.
        self.log.append(LogEntry(self.current_term, ("noop",)))
        self._broadcast_append(out)
        self.next_heartbeat = now + self.config.heartbeat_interval
        self._advance_commit()  # a single-node cluster commits immediately
        self._apply_committed(out)

    # ---- elections ---------------------------------------------------------------------

    def _log_ok(self, last_log_term: int, last_log_index: int) -> bool:
        """Is a candidate's log at least as up to date as ours?"""
        mine = (self.term_at(self.last_index), self.last_index)
        return (last_log_term, last_log_index) >= mine or self.bugs.stale_log_vote

    def _on_pre_vote(self, src: int, msg: PreVote, now: int, out: Outbox) -> None:
        # Refuse while we have a live leader: that is what keeps a rejoining node from
        # forcing an election on a cluster that is working fine.
        leader_alive = self.role is Role.LEADER or (
            self.leader_id is not None
            and now - self.last_leader_contact < self.config.election_timeout_min
        )
        grant = (
            msg.term >= self.current_term
            and not leader_alive
            and self._log_ok(msg.last_log_term, msg.last_log_index)
        )
        term = msg.term if grant else self.current_term
        out.append((src, PreVoteReply(term, grant, msg.term)))

    def _on_pre_vote_reply(self, src: int, msg: PreVoteReply, now: int, out: Outbox) -> None:
        if self.role is Role.LEADER or msg.for_term != self.prevote_term or not msg.granted:
            return
        self.prevotes.add(src)
        if len(self.prevotes) >= self.majority:
            self._start_election(now, out)

    def _on_request_vote(self, src: int, msg: RequestVote, now: int, out: Outbox) -> None:
        grant = False
        if msg.term >= self.current_term:
            up_to_date = self._log_ok(msg.last_log_term, msg.last_log_index)
            free_vote = self.storage.voted_for in (None, msg.candidate_id)
            free_vote = free_vote or self.bugs.double_vote
            if free_vote and up_to_date:
                grant = True
                self.storage.voted_for = msg.candidate_id
                self._reset_election_timer(now)
        out.append((src, RequestVoteReply(self.current_term, grant)))

    def _on_vote_reply(self, src: int, msg: RequestVoteReply, now: int, out: Outbox) -> None:
        if self.role is not Role.CANDIDATE or msg.term != self.current_term:
            return
        if msg.vote_granted:
            self.votes.add(src)
            if len(self.votes) >= self.majority:
                self._become_leader(now, out)

    # ---- replication: follower side ----------------------------------------------------

    def _on_append_entries(self, src: int, msg: AppendEntries, now: int, out: Outbox) -> None:
        if msg.term < self.current_term:
            out.append((src, AppendEntriesReply(self.current_term, False, 0, 0)))
            return

        self.role = Role.FOLLOWER
        self.leader_id = msg.leader_id
        self.last_leader_contact = now
        self.prevote_term = None
        self._reset_election_timer(now)

        prev = msg.prev_log_index
        if prev > self.last_index:
            reply = AppendEntriesReply(self.current_term, False, 0, self.last_index + 1)
            out.append((src, reply))
            return
        if prev > 0 and self.term_at(prev) != msg.prev_log_term:
            out.append((src, AppendEntriesReply(self.current_term, False, 0, prev)))
            return

        for offset, entry in enumerate(msg.entries):
            index = prev + 1 + offset
            if index <= self.last_index:
                if self.term_at(index) == entry.term:
                    continue
                if self.bugs.no_truncate_on_conflict:
                    continue
                del self.log[index - 1 :]
            self.log.append(entry)

        last_new = prev + len(msg.entries)
        if msg.leader_commit > self.commit_index:
            self.commit_index = min(msg.leader_commit, last_new)
            self._apply_committed(out)
        out.append((src, AppendEntriesReply(self.current_term, True, last_new, 0)))

    # ---- replication: leader side ------------------------------------------------------

    def _broadcast_append(self, out: Outbox) -> None:
        for peer in self.peers:
            self._send_append(peer, out)

    def _send_append(self, peer: int, out: Outbox) -> None:
        nxt = self.next_index[peer]
        prev = nxt - 1
        entries = tuple(self.log[nxt - 1 : nxt - 1 + self.config.max_batch])
        out.append(
            (
                peer,
                AppendEntries(
                    self.current_term,
                    self.id,
                    prev,
                    self.term_at(prev),
                    entries,
                    self.commit_index,
                ),
            )
        )

    def _on_append_reply(self, src: int, msg: AppendEntriesReply, out: Outbox) -> None:
        if self.role is not Role.LEADER or msg.term != self.current_term:
            return
        if msg.success:
            self.match_index[src] = max(self.match_index[src], msg.match_index)
            self.next_index[src] = self.match_index[src] + 1
            self._advance_commit()
            self._apply_committed(out)
            if self.next_index[src] <= self.last_index:
                self._send_append(src, out)
        else:
            self.next_index[src] = max(1, min(self.next_index[src] - 1, msg.conflict_index))
            self._send_append(src, out)

    def _advance_commit(self) -> None:
        needed = self.majority
        if self.bugs.commit_without_majority:
            needed = max(1, needed - 1)
        for index in range(self.last_index, self.commit_index, -1):
            replicas = 1 + sum(1 for p in self.peers if self.match_index[p] >= index)
            if replicas < needed:
                continue
            if self.term_at(index) == self.current_term or self.bugs.commit_old_term:
                self.commit_index = index
                break

    # ---- clients and state machine -----------------------------------------------------

    def _on_client_request(self, src: Addr, msg: ClientRequest, now: int, out: Outbox) -> None:
        if self.role is not Role.LEADER:
            out.append((src, ClientResponse(msg.req_id, False, None, self.leader_id)))
            return
        client = src if isinstance(src, str) else None
        self.log.append(LogEntry(self.current_term, msg.command, client, msg.req_id))
        self.pending[self.last_index] = (src, msg.req_id, self.current_term)
        self._broadcast_append(out)
        self._advance_commit()
        self._apply_committed(out)

    def _apply_committed(self, out: Outbox) -> None:
        # Apply whatever became committed, either locally (leader) or via leader_commit.
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            result = self._apply(entry)
            if self.apply_hook is not None:
                self.apply_hook(self.id, self.last_applied, entry)
            waiting = self.pending.pop(self.last_applied, None)
            if waiting is not None and self.role is Role.LEADER:
                client, req_id, term = waiting
                if term == entry.term:
                    out.append((client, ClientResponse(req_id, True, result, self.id)))

    def _apply(self, entry: LogEntry) -> object:
        # The network may duplicate a request, so the same client operation can be committed
        # twice. Each client numbers its requests in increasing order: applying one that is not
        # newer than the last would let an old write overwrite a newer one.
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
            self.state_machine[command[1]] = command[2]
            return command[2]
        if kind == "get":
            return self.state_machine.get(command[1])
        return None
