"""Validated retail task graphs and per-node execution state."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from uuid import uuid4

from core.retail_subtasks import Task, _ordered_tasks


class Status(str, Enum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BLOCKED = "blocked"
    WAITING_USER = "waiting_user"
    SKIPPED = "skipped"


_TRANSITIONS = {
    Status.PENDING: {Status.READY, Status.BLOCKED, Status.SKIPPED},
    Status.READY: {Status.RUNNING, Status.BLOCKED},
    Status.RUNNING: {Status.SUCCEEDED, Status.FAILED, Status.BLOCKED, Status.WAITING_USER},
    Status.BLOCKED: {Status.PENDING, Status.SKIPPED},
    Status.WAITING_USER: {Status.READY, Status.BLOCKED},
}


@dataclass(frozen=True)
class Plan:
    id: str
    client: str
    conversation: str
    nodes: tuple[Task, ...]
    version: int = 1

    @classmethod
    def build(cls, rows, message, client, conversation):
        return cls(uuid4().hex, client, conversation, tuple(_ordered_tasks(rows, message, "dag")))


@dataclass
class Run:
    plan_id: str
    task_id: str
    status: Status = Status.PENDING
    role: str = ""
    turn: int = -1
    attempt: int = 0
    reason: str = ""
    facts: dict = field(default_factory=dict)
    refs: tuple[str, ...] = ()

    def move(self, status, *, reason="", refs=(), facts=None):
        if status not in _TRANSITIONS.get(self.status, ()):
            raise ValueError(f"invalid task transition: {self.status} -> {status}")
        if status == Status.SUCCEEDED and not refs:
            raise ValueError("missing_evidence")
        self.status, self.reason = status, reason
        self.refs, self.facts = tuple(refs), dict(facts or {})

    def claim(self, role, turn):
        if self.turn >= turn:
            raise ValueError("already_attempted")
        self.move(Status.RUNNING)
        self.turn, self.role = turn, role
        self.attempt += 1
