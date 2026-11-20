"""Validated retail task graphs and per-node execution state."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import operator
import re
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
    Status.READY: {Status.BLOCKED},
    Status.RUNNING: {Status.SUCCEEDED, Status.FAILED, Status.BLOCKED, Status.WAITING_USER},
    Status.BLOCKED: {Status.PENDING, Status.SKIPPED},
    Status.WAITING_USER: {Status.READY, Status.BLOCKED},
}


# Only server-defined fields may cross a dependency edge.
OUTPUTS = {
    "order_lookup": {"order_id", "status", "purchased_item_count", "item_ids"},
    "billing_lookup": {"order_id", "status", "purchased_item_count", "payment_type"},
    "product_info": {"variants", "ventilation", "size", "color"},
    "return_policy": {"policy"}, "general_policy": {"policy"},
    "return_change": {"executed"}, "order_change": {"executed"},
}
_COMPARISONS = {"至少": operator.ge, "超过": operator.gt, "少于": operator.lt,
                "至多": operator.le, "等于": operator.eq, "at least": operator.ge,
                "more than": operator.gt, "fewer than": operator.lt, "at most": operator.le,
                "exactly": operator.eq, "is": operator.eq}
_COUNT = re.compile(r"(?:订单(?:中)?(?P<zh>至少|至多|超过|少于|等于)(?:有)?\s*(?P<n>[0-9]{1,6})\s*件商品"
                    r"|(?:the )?order (?:has|contains) (?P<en>at least|more than|fewer than|at most|exactly) "
                    r"(?P<m>[0-9]{1,6}) (?:items?|products?))", re.I)
_VENTILATION = re.compile(r"(?:通风等级(?:为|是)|(?:the )?ventilation(?: level)? is )"
                          r"(low|medium|high|低|中|高)", re.I)


@dataclass(frozen=True)
class Condition:
    source: str
    field: str
    comparison: str
    value: int | str
    otherwise: bool

    def evaluate(self, upstream: Run) -> bool | None:
        if upstream.task_id != self.source or upstream.status != Status.SUCCEEDED or not upstream.refs:
            return None
        actual = upstream.facts.get(self.field)
        if type(actual) is not type(self.value):
            return None
        if self.field == "ventilation" and actual not in {"low", "medium", "high"}:
            return None
        return _COMPARISONS[self.comparison](actual, self.value) != self.otherwise


@dataclass(frozen=True)
class Node(Task):
    output: str
    target: str
    run_if: Condition | None


def _condition(raw, task, message, sources):
    if not isinstance(raw, dict) or set(raw) != {"source", "quote"}:
        raise ValueError("invalid_condition")
    source, quote = raw["source"], raw["quote"]
    if source not in task.depends_on or not isinstance(quote, str) or not 0 < len(quote) <= 512:
        raise ValueError("invalid_condition_source")
    # Require a full user-authored conditional sentence. The model cannot pick an
    # operator, constant, or branch polarity by merely citing part of a sentence.
    sentences = [part.strip() for part in re.split(r"[。！？.!?\n]+", message)]
    if sentences.count(quote.rstrip("。！？.!?")) != 1:
        raise ValueError("unverified_condition_quote")
    match = re.fullmatch(r"(?:如果|if )(.+?)[，,]\s*(?:就|then )?(.+?)"
                         r"(?:[，,;；]\s*(?:否则|otherwise |else )(.+))?", quote.rstrip("。！？.!?"), re.I)
    if not match:
        raise ValueError("unsupported_condition")
    predicate, yes, no = match.groups()
    if yes == no or task.quote not in (yes, no):
        raise ValueError("unverified_condition_branch")
    count = _COUNT.fullmatch(predicate)
    vent = _VENTILATION.fullmatch(predicate)
    if count:
        field = "purchased_item_count"
        comparison = (count["zh"] or count["en"]).lower()
        value = int(count["n"] or count["m"])
    elif vent:
        field, comparison = "ventilation", "is"
        value = {"低": "low", "中": "medium", "高": "high"}.get(vent[1], vent[1].lower())
    else:
        raise ValueError("unsupported_condition")
    if sources[source].output != field:
        raise ValueError("unavailable_condition_field")
    return Condition(source, field, comparison, value, task.quote == no)


@dataclass(frozen=True)
class Plan:
    id: str
    client: str
    conversation: str
    nodes: tuple[Node, ...]
    version: int = 1

    @classmethod
    def build(cls, rows, message, client, conversation):
        ordered = _ordered_tasks(rows, message, "dag")
        by_id = {row["id"]: row for row in rows}
        nodes = {}
        for task in ordered:
            row = by_id[task.id]
            if "condition" in row:
                raise ValueError("use_run_if")
            output, target = row.get("output", ""), row.get("target", "")
            if not isinstance(output, str) or (output and output not in OUTPUTS[task.kind]):
                raise ValueError("invalid_output_field")
            if (not isinstance(target, str) or len(target) > 128 or
                    (target and not re.search(r"(?<![a-zA-Z0-9_-])" + re.escape(target)
                                              + r"(?![a-zA-Z0-9_-])", task.quote))):
                raise ValueError("unverified_target")
            condition = _condition(row["run_if"], task, message, nodes) if "run_if" in row else None
            nodes[task.id] = Node(task.id, task.kind, task.quote, task.depends_on, output, target, condition)
        for task in nodes.values():
            if task.run_if:
                clause = by_id[task.id]["run_if"]["quote"]
                if any(other.id != task.run_if.source and other.quote in clause
                       and other.run_if is None for other in nodes.values()):
                    raise ValueError("missing_branch_condition")
        return cls(uuid4().hex, client, conversation, tuple(nodes.values()))


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
        if status == Status.RUNNING:
            raise ValueError("claim the node before executing it")
        if status not in _TRANSITIONS.get(self.status, ()):
            raise ValueError(f"invalid task transition: {self.status} -> {status}")
        if status == Status.SUCCEEDED and not refs:
            raise ValueError("missing_evidence")
        self.status, self.reason = status, reason
        self.refs, self.facts = tuple(refs), dict(facts or {})

    def claim(self, role, turn):
        if self.status != Status.READY:
            raise ValueError("task_not_ready")
        if self.turn >= turn:
            raise ValueError("already_attempted")
        self.status, self.reason = Status.RUNNING, ""
        self.turn, self.role = turn, role
        self.attempt += 1
