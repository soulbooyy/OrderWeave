"""Retail DAG acceptance tests. No real models or network."""
from dataclasses import FrozenInstanceError

import pytest

from core.retail_dag import Plan, Run, Status


def node(key, kind="product_info", *parents, **fields):
    return dict(id=key, kind=kind, depends_on=list(parents), quote=kind, **fields)


def build(rows, message=None):
    return Plan.build(rows, message or " ".join(r["quote"] for r in rows), "client", "conv")


def test_plan_scope_and_repeated_role_runs_are_independent():
    plan = build([node("a"), node("b", "product_info", "a")])
    assert plan.version == 1 and (plan.client, plan.conversation) == ("client", "conv")
    with pytest.raises(FrozenInstanceError):
        plan.nodes = ()
    runs = {task.id: Run(plan.id, task.id) for task in plan.nodes}
    a = runs["a"]
    with pytest.raises(ValueError):
        a.move(Status.SUCCEEDED)
    a.move(Status.READY)
    a.claim("technical", turn=2)
    a.move(Status.WAITING_USER, reason="choose_variant")
    a.move(Status.READY)
    with pytest.raises(ValueError):
        a.claim("technical", turn=2)
    a.claim("technical", turn=3)
    a.move(Status.FAILED, reason="missing_evidence")
    assert runs["b"].status == Status.PENDING and a.attempt == 2
    with pytest.raises(ValueError):
        a.move(Status.READY)


def test_graph_size_and_validation():
    assert len(build([node(f"n{i}") for i in range(12)]).nodes) == 12
    for rows in ([node(f"n{i}") for i in range(13)], [node("a"), node("a")],
                 [node("a", "product_info", "a")], [node("a", "product_info", "b")],
                 [node("a", "product_info", "b"), node("b", "product_info", "a")]):
        with pytest.raises(ValueError):
            build(rows)
    with pytest.raises(ValueError, match="quote"):
        build([node("a")], "different")
