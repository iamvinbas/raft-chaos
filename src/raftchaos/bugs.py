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

    @classmethod
    def only(cls, name: str) -> Bugs:
        if name not in BUG_NAMES:
            raise ValueError(f"unknown bug {name!r}; choose from {', '.join(BUG_NAMES)}")
        return cls(**{name: True})


BUG_NAMES: tuple[str, ...] = tuple(f.name for f in fields(Bugs))
