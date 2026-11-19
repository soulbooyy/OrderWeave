"""Turn-local role planning. Business progress and permissions belong to RetailStore."""
from __future__ import annotations

import re
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter


MODES = ("keyword", "subintent", "dag")
OWNERS = {
    "product_info": "technical", "general_policy": "general", "order_lookup": "general",
    "order_change": "general", "return_policy": "billing", "billing_lookup": "billing",
    "return_change": "billing",
}
WRITES = {"order_change", "return_change"}
PRIVATE = WRITES | {"order_lookup", "billing_lookup"}


def extraction_instruction(mode: str) -> str:
    if mode == "keyword":
        return ""
    edges = (' Include "depends_on" (array of node ids, [] when independent). Add an edge only if the '
             'user requires that order or the result is needed. Mention order alone is not a dependency.'
             if mode == "dag" else "")
    return ('\nIn the same JSON object also include "subtasks", in mention order. Each has "id" (t1, t2, ...), '
            '"kind", and "quote" (a verbatim span of the current user message). Split only active requests. '
            'Exclude negations, abandoned actions, hypothetical future changes, and background. '
            'Return [] for unsupported, unclear, or purely social requests. Use order_lookup for personal order '
            'or delivery facts, billing_lookup for personal payment/refund facts, product_info for public catalog '
            'specs, general_policy for general policy, return_policy for return terms or options, return_change '
            'for a requested return/exchange and order_change for the other supported retail changes.' + edges)


@dataclass(frozen=True)
class Task:
    id: str
    kind: str
    quote: str
    depends_on: tuple[str, ...]


@dataclass
class Dispatch:
    # Ordered mapping: read-only helpers first, primary last.
    roles: dict[str, list[Task]]
    deferred: list[Task]

    def prerequisite_roles(self, role: str) -> list[str]:
        ids = {d for task in self.roles.get(role, []) for d in task.depends_on}
        return [r for r, tasks in self.roles.items() if r != role and any(t.id in ids for t in tasks)]


def _ordered_tasks(raw: object, message: str, mode: str) -> list[Task]:
    if not isinstance(raw, list) or not 1 <= len(raw) <= 12:
        raise ValueError("missing_or_oversize_subtasks")
    tasks = {}
    for row in raw:
        if not isinstance(row, dict):
            raise ValueError("invalid_subtask")
        key, kind, quote = (row.get(field) for field in ("id", "kind", "quote"))
        if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,31}", key) or key in tasks:
            raise ValueError("invalid_subtask_id")
        if not isinstance(kind, str) or kind not in OWNERS:
            raise ValueError("unknown_subtask_kind")
        if not isinstance(quote, str) or not quote.strip() or len(quote) > 512 or quote not in message:
            raise ValueError("unverified_subtask_quote")
        deps = row.get("depends_on") if mode == "dag" else []
        if not isinstance(deps, list) or len(deps) > 12 or not all(isinstance(d, str) for d in deps):
            raise ValueError("invalid_dependencies")
        tasks[key] = Task(key, kind, quote, tuple(dict.fromkeys(deps)))
    graph = {t.id: t.depends_on for t in tasks.values()}
    if any(d not in tasks for deps in graph.values() for d in deps):
        raise ValueError("unknown_dependency")
    try:
        return [tasks[k] for k in TopologicalSorter(graph).static_order()]
    except CycleError as exc:
        raise ValueError("cyclic_dependencies") from exc


def plan_dispatch(raw: object, message: str, mode: str) -> Dispatch:
    tasks = _ordered_tasks(raw, message, mode)
    # No successor may assume that displaying a write for confirmation completed it.
    blocked = set()
    active, deferred = [], []
    for task in tasks:
        if blocked.intersection(task.depends_on):
            deferred.append(task)
            blocked.add(task.id)
        else:
            active.append(task)
        if task.kind in WRITES:
            blocked.add(task.id)

    private = [t for t in active if t.kind in PRIVATE]
    # Both business roles can bind identity and act. Coalesce private work under a single owner.
    primary = None
    if private:
        business_roles = {OWNERS[t.kind] for t in active}
        primary = "billing" if "billing" in business_roles else "general"
    # An order/billing primary already has the order and policy context. Reserve a helper
    # for a separate public catalog question, not for another step within its own scope.
    owners = {t.id: primary if primary and t.kind != "product_info" else OWNERS[t.kind] for t in active}
    preferred = primary or owners[active[0].id]
    remaining = list(dict.fromkeys(owners.values()))
    roles = {}
    done = set()
    while remaining:
        ready = [r for r in remaining if all(owners[d] == r or d in done
                 for t in active if owners[t.id] == r for d in t.depends_on)]
        if not ready:
            # Interleaved roles (A -> B -> A) cannot run twice in the current executor.
            # The topologically first outstanding role can do its ready prefix this turn.
            role = next(owners[t.id] for t in active if t.id not in done)
            roles[role] = []
            for task in active:
                if owners[task.id] == role and set(task.depends_on) <= done:
                    roles[role].append(task)
                    done.add(task.id)
            break
        role = next((r for r in ready if r != preferred), ready[0])
        roles[role] = [t for t in active if owners[t.id] == role]
        done.update(t.id for t in roles[role])
        remaining.remove(role)
        if role == primary:
            break  # This role needs primary-only permissions, so it must run last.
    deferred.extend(t for t in active if t.id not in done)
    return Dispatch(roles, deferred)
