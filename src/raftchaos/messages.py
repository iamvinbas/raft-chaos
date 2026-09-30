"""Wire messages and log entries. All immutable so the simulator can duplicate them freely."""

from __future__ import annotations

from dataclasses import dataclass

# A command is a tuple: ("put", key, value), ("get", key) or ("noop",).
Command = tuple[object, ...]

# Addresses: nodes are ints, clients are strings such as "c0".
Addr = int | str


@dataclass(frozen=True)
class LogEntry:
    term: int
    command: Command
    client: str | None = None  # who asked; with req_id it lets the state machine deduplicate
    req_id: int = 0


@dataclass(frozen=True)
class RequestVote:
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class RequestVoteReply:
    term: int
    vote_granted: bool


@dataclass(frozen=True)
class PreVote:
    """Would you vote for me in `term`? Asking changes nobody's term or vote (Raft thesis 9.6)."""

    term: int  # the term the candidate would start: its current term + 1
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class PreVoteReply:
    term: int  # the proposed term when granted, otherwise the replier's current term
    granted: bool
    for_term: int  # which pre-vote round this answers


@dataclass(frozen=True)
class AppendEntries:
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: tuple[LogEntry, ...]
    leader_commit: int


@dataclass(frozen=True)
class AppendEntriesReply:
    term: int
    success: bool
    match_index: int  # valid when success
    conflict_index: int  # hint for the leader when not success


@dataclass(frozen=True)
class ClientRequest:
    req_id: int
    command: Command


@dataclass(frozen=True)
class ClientResponse:
    req_id: int
    ok: bool  # False means "definitely not executed" (e.g. not the leader)
    result: object
    leader_hint: int | None


Message = (
    RequestVote
    | RequestVoteReply
    | PreVote
    | PreVoteReply
    | AppendEntries
    | AppendEntriesReply
    | ClientRequest
    | ClientResponse
)

Outbox = list[tuple[Addr, Message]]
