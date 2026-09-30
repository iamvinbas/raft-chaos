"""Deliberate protocol bugs used to prove the simulator can find real failures.

Each flag re-introduces a classic Raft implementation mistake. The default is a correct node.
"""

from __future__ import annotations

from dataclasses import dataclass, fields


@dataclass(frozen=True)
class Bugs:
    # Grant a vote even if already voted for someone else in this term.
    double_vote: bool = False
    # Skip the "candidate log is at least as up to date" check when voting.
    stale_log_vote: bool = False
    # Leader commits with one fewer replica than a majority.
    commit_without_majority: bool = False
    # Leader commits entries from earlier terms by counting replicas (Raft paper, Figure 8).
    commit_old_term: bool = False
    # Follower keeps conflicting entries instead of truncating its log.
    no_truncate_on_conflict: bool = False
    # Node forgets its vote after a restart (voted_for is not persisted).
    forget_vote_on_restart: bool = False
    # Snapshots leave out the client sessions, so deduplication is lost after a restore.
    snapshot_without_sessions: bool = False
    # A follower that receives a snapshot throws away its whole log and state, even when its log
    # already goes past the snapshot (Raft paper, Figure 13, step 6 skipped).
    install_snapshot_discards_log: bool = False

    @classmethod
    def only(cls, name: str) -> Bugs:
        if name not in BUG_NAMES:
            raise ValueError(f"unknown bug {name!r}; choose from {', '.join(BUG_NAMES)}")
        return cls(**{name: True})


BUG_NAMES: tuple[str, ...] = tuple(f.name for f in fields(Bugs))

# Bugs that live in snapshot code: they can only show up when snapshots are turned on.
SNAPSHOT_BUGS: frozenset[str] = frozenset(
    {"snapshot_without_sessions", "install_snapshot_discards_log"}
)
# How often those bug hunts take a snapshot when the caller did not choose.
DEFAULT_SNAPSHOT_EVERY = 20
