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


COUNT_QUERY = "查询订单 #W100 的件数。如果订单至少有 3 件商品，就查鼠标规格，否则查头盔规格。"
CLAUSE = COUNT_QUERY.split("。")[1]


def conditional_rows():
    return [
        dict(node("n", "order_lookup"), quote="查询订单 #W100 的件数", output="purchased_item_count", target="#W100"),
        dict(node("yes", "product_info", "n"), quote="查鼠标规格", run_if={"source": "n", "quote": CLAUSE}),
        dict(node("no", "product_info", "n"), quote="查头盔规格", run_if={"source": "n", "quote": CLAUSE}),
    ]


@pytest.mark.parametrize("count,expected", [(2, [False, True]), (3, [True, False]), (None, [None, None]),
                                            (True, [None, None]), ("3", [None, None])])
def test_conditions_derive_operator_threshold_and_branch_from_user(count, expected):
    plan = build(conditional_rows(), COUNT_QUERY)
    source = Run(plan.id, "n", status=Status.SUCCEEDED, facts={"purchased_item_count": count}, refs=("order-tool",))
    assert [t.run_if.evaluate(source) for t in plan.nodes[1:]] == expected
    source.status = Status.FAILED
    assert all(t.run_if.evaluate(source) is None for t in plan.nodes[1:])


@pytest.mark.parametrize("change", [
    {"run_if": "__import__('os')"}, {"run_if": {"source": "bad", "quote": CLAUSE}},
    {"run_if": {"source": "n", "quote": CLAUSE, "threshold": 2}},
    {"run_if": {"source": "n", "quote": CLAUSE.replace("3", "2")}}, {"quote": "鼠标"},
    {"depends_on": []}, {"output": "order.address"}, {"target": "made-up"},
])
def test_invalid_contract_rejected_before_dispatch(change):
    rows = conditional_rows()
    rows[1].update(change)
    with pytest.raises(ValueError):
        build(rows, COUNT_QUERY)


def trace(name, args, **result):
    return {"tool_name": name, "tool_use_id": "tool-1", "input": args, "success": True,
            "result_success": True, "result": {"success": True, **result}}


def test_evidence_reads_require_correct_target_field_binding_and_actual_call():
    from core.retail_dag_evidence import verify
    task = build(conditional_rows(), COUNT_QUERY).nodes[0]
    t = trace("get_order_details", {"order_id": "#W100"}, order={"order_id": "#W100", "user_id": "u", "items": [{}, {}]})
    assert verify(task, [t], bound_user="u").facts == {"purchased_item_count": 2}
    for bad in ([], [dict(t, blocked_by_guard=True)], [dict(t, skipped=True)], [dict(t, result_success=False)],
                [dict(t, input={"order_id": "#W101"})]):
        assert verify(task, bad, bound_user="u") is None
    assert verify(task, [t], bound_user="other") is None
    spec = build([dict(node("t"), quote="ventilation of item 111222", target="111222", output="ventilation")]).nodes[0]
    for item, expected in [({"item_id": "111222", "options": {"ventilation": "high"}}, "high"),
                           ({"item_id": "111222", "options": {"size": "L"}}, None),
                           ({"item_id": "000000", "options": {"ventilation": "low"}}, None)]:
        found = verify(spec, [trace("get_item_details", {"item_id": item["item_id"]}, item=item)])
        assert (found.facts["ventilation"] if found else None) == expected


def test_write_requires_matching_business_event_and_canonical_arguments():
    from core.retail_dag_evidence import verify
    task = build([dict(node("w", "return_change"), quote="return from #W100", target="#W100", output="executed")]).nodes[0]
    args = {"order_id": "#W100", "item_ids": ["1"], "payment_method_id": "card"}
    call = trace("return_delivered_order_items", args, op_id="op1", kind="return", action_status="executed")
    event = dict(event_id=7, op_id="op1", event_type="return_requested", order_id="#W100", payload={"kind": "return", "args": args})
    assert verify(task, [call], events=[event], bound_user="u").refs == ("event:7",)
    for bad in ([], [dict(event, op_id="new")], [dict(event, event_type="return_prepared")],
                [dict(event, payload={"kind": "return", "args": {**args, "item_ids": ["2"]}})]):
        assert verify(task, [call], events=bad, bound_user="u") is None
    pending = trace("return_delivered_order_items", args, op_id="op1", kind="return", action_status="pending_confirmation")
    assert verify(task, [pending], events=[event], bound_user="u") is None


def test_real_entry_point_never_discards_explicit_condition():
    import asyncio
    from agents.agent_orchestrator import Request
    from tests.test_retail_dispatch import router

    for guard in ({"source": "n", "quote": CLAUSE}, {"source": "n", "expr": "n.count > 2"}):
        rows = conditional_rows()
        rows[1]["run_if"] = guard
        app = router("dag", rows)
        result = asyncio.run(app.run(Request(COUNT_QUERY, "client", "conv")))
        assert not result.success and not result.tool_traces
        assert result.routing_reason.startswith("dag_condition:")
        assert all(agent._client.calls == 0 for pool in app._pool.values() for agent in pool)


def test_ventilation_condition_cannot_activate_on_unverified_or_unknown_enum():
    message = "查通风等级。如果通风等级为低，就退货，否则查条款。"
    clause = message.split("。")[1]
    rows = [dict(node("spec"), quote="查通风等级", output="ventilation"),
            dict(node("w", "return_change", "spec"), quote="退货", run_if=dict(source="spec", quote=clause))]
    plan = build(rows, message)
    condition = plan.nodes[-1].run_if
    src = Run(plan.id, "spec", status=Status.SUCCEEDED, facts={"ventilation": "low"})
    assert condition.evaluate(src) is None
    src.refs = ("tool:1",)
    assert condition.evaluate(src) is True
    src.facts["ventilation"] = "unknown"
    assert condition.evaluate(src) is None


def test_target_id_must_be_complete_and_partial_branch_cannot_escape_guard():
    rows = conditional_rows()
    rows[0]["target"] = "#W10"
    with pytest.raises(ValueError, match="target"):
        build(rows, COUNT_QUERY)
    rows = conditional_rows()
    del rows[2]["run_if"]
    with pytest.raises(ValueError, match="condition"):
        build(rows, COUNT_QUERY)


def test_checked_state_changes_require_scoped_business_records(tmp_path):
    import json
    from core.retail_dag_evidence import verify
    from evaluation.harness.config import SEED_PATH
    from mcp_tools.retail_store import RetailStore

    if not SEED_PATH.exists():
        pytest.skip("requires the public retail seed")
    order = next(o for o in json.loads(SEED_PATH.read_text())["orders"].values() if o["status"] == "delivered")
    store = RetailStore(tmp_path / "evidence.db", SEED_PATH, language="en")
    user = json.loads(SEED_PATH.read_text())["users"][order["user_id"]]
    store.begin_turn("client", "conv")
    store.find_user_by_email("client", "conv", user["email"])
    args = dict(order_id=order["order_id"], item_ids=[order["items"][0]["item_id"]],
                payment_method_id=order["payment_history"][0]["payment_method_id"])
    task = build([dict(node("w", "return_change"), quote=f"return from {order['order_id']}",
                       target=order["order_id"], output="executed")]).nodes[0]
    tool = "return_delivered_order_items"
    prepared = store.write_action("client", "conv", tool, args, "")
    assert prepared["action_status"] == "pending_confirmation"
    assert verify(task, [trace(tool, args, **prepared)], events=store.list_events("client", "conv"),
                  bound_user=user["user_id"]) is None
    assert store.mark_pending_displayed("client", "conv", prepared["op_id"], 1, False)
    store.begin_turn("client", "conv")
    receipt = store.write_action("client", "conv", tool, args, "Yes, please proceed.")
    t = trace(tool, args, **receipt)
    evidence = verify(task, [t], events=store.list_events("client", "conv"), bound_user=user["user_id"])
    assert evidence.facts["op_id"] == prepared["op_id"]
    assert verify(task, [t], events=store.list_events("client", "other"), bound_user=user["user_id"]) is None
