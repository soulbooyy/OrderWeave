import asyncio
import json
from dataclasses import replace

import pytest

from agents.agent_orchestrator import AgentOrchestrator, AgentType, Request
from core.intent_recognizer import IntentCategory, IntentRecognizer, build_intent_prompt
from core.retail_subtasks import plan_dispatch
from evaluation.harness.config import PROFILES
from evaluation.harness.routing_ablation import ReplayClient, ROLES


def node(key, kind, *parents):
    return {"id": key, "kind": kind, "quote": kind, "depends_on": list(parents)}


def plan(rows, mode="dag"):
    return plan_dispatch(rows, " ".join(r["quote"] for r in rows), mode)


def router(mode, rows):
    client = ReplayClient(json.dumps({"intent": "query", "confidence": 1, "subtasks": rows}))
    return AgentOrchestrator(model="offline", client=ReplayClient("OK"), routing_mode=mode,
                             retail_mode=True, retail_role_classes=ROLES,
                             intent_recognizer=IntentRecognizer(model="offline", client=client))


@pytest.mark.parametrize("raw,reason", [
    (None, "missing_or_oversize"), ([node("t", "product_info")] * 13, "missing_or_oversize"),
    ([node("a", "product_info"), node("a", "order_change")], "invalid_subtask_id"),
    ([node("a", "unsupported")], "unknown_subtask_kind"),
    ([node("a", "product_info", "unknown")], "unknown_dependency"),
    ([node("a", "product_info", "b"), node("b", "return_policy", "a")], "cyclic_dependencies"),
    ([{**node("a", "product_info"), "depends_on": "abc"}], "invalid_dependencies"),
    ([{**node("a", "product_info"), "quote": "invented"}], "unverified_subtask_quote"),
])
def test_bad_graph_falls_back_to_original_route(raw, reason):
    req = Request("product_info return_policy order_change unsupported", "u", "c", intent=IntentCategory.QUERY)
    old = router("keyword", raw)._route_decision(req)
    actual = router("dag", raw)._route_decision(req, raw)
    assert actual.dispatch is None and reason in actual.reason
    assert actual.agent_types == old.agent_types


def test_dag_validates_and_orders_the_tasks_within_a_role():
    rows = [node("second", "return_policy", "first"), node("first", "return_policy")]
    assert [t.id for t in plan(rows).roles["billing"]] == ["first", "second"]
    plain = plan(rows, "subintent")
    assert [t.id for t in plain.roles["billing"]] == ["second", "first"]
    assert all(not t.depends_on for t in plain.roles["billing"])


def test_write_barrier_and_interleaved_roles_have_an_executable_prefix():
    rows = [node("a", "order_change"), node("b", "return_change", "a"), node("c", "product_info", "b")]
    dispatch = plan(rows)
    assert list(dispatch.roles) == ["general"] and [t.id for t in dispatch.deferred] == ["b", "c"]
    rows = [node("a", "product_info"), node("b", "return_policy", "a"), node("c", "product_info", "b")]
    dispatch = plan(rows)
    assert [t.id for t in dispatch.roles["technical"]] == ["a"]
    assert [t.id for t in dispatch.deferred] == ["b", "c"]


def test_personal_data_and_writes_stay_with_one_primary():
    rows = [node("a", "order_lookup"), node("b", "billing_lookup"), node("c", "return_change")]
    assert list(plan(rows).roles) == ["billing"]
    rows = [node("a", "order_lookup"), node("b", "return_policy", "a")]
    assert list(plan(rows).roles) == ["billing"]
    rows = [node("a", "order_lookup"), node("b", "product_info", "a")]
    dispatch = plan(rows)
    assert list(dispatch.roles) == ["general"] and [t.id for t in dispatch.deferred] == ["b"]


def test_roles_receive_verified_dependencies_and_wait_for_failed_predecessor():
    rows = [node("spec", "product_info"), node("terms", "return_policy", "spec")]
    app = router("dag", rows)
    req = Request("product_info return_policy", "u", "c", turn=1)

    async def run():
        decision = await app.prepare_route(req)
        assert decision.primary_agent is AgentType.BILLING
        assert decision.supporting_agents == [AgentType.TECHNICAL]
        primary = app._best_agent(AgentType.BILLING)
        for outcome in ({}, {"technical": {"success": False}}):
            req.dispatch_outcomes = outcome
            with pytest.raises(RuntimeError, match="technical -> billing"):
                await app.execute_role(req, AgentType.BILLING)
            assert primary._client.calls == 0
        await app.execute_role(req, AgentType.TECHNICAL)
        packet = json.loads(primary._build_role_packet(req))["assignment"]
        assert [t["id"] for t in packet["tasks"]] == ["terms"]
        assert packet["prerequisite_results"][0]["role"] == "technical"
        assert json.loads(primary._transient_blocks(req)[0]["text"])["assignment"] == packet
        await app.execute_role(req, AgentType.BILLING)
        assert app._intent_recognizer.client.calls == 1

    asyncio.run(run())


def test_special_routes_and_unavailable_role():
    rows = [node("spec", "product_info")]
    app = router("dag", rows)
    pending = Request("Yes.", "u", "pending", retail_pending=[{"confirmation_approved": True}])
    app.language = "en"
    decision = asyncio.run(app.prepare_route(pending))
    assert decision.primary_agent is AgentType.BILLING and decision.dispatch is None
    assert app._intent_recognizer.client.calls == 0
    human = Request("human", "u", "c", intent=IntentCategory.HUMAN_HANDOFF)
    assert app._route_decision(human, rows).primary_agent is AgentType.ESCALATION
    app._pool.pop(AgentType.TECHNICAL)
    ordinary = Request("product_info", "u", "c", intent=IntentCategory.QUERY)
    decision = app._route_decision(ordinary, rows)
    assert decision.dispatch is None and "planned_role_unavailable" in decision.reason


def test_modes_are_isolated_in_prompts_cache_and_harness():
    app = router("keyword", [])
    recognizer = app._intent_recognizer
    keys = []
    for mode in ("keyword", "subintent", "dag"):
        prompt = build_intent_prompt("查订单", retail_mode=True, routing_mode=mode)
        assert ('"subtasks"' in prompt) == (mode != "keyword")
        assert ('"depends_on"' in prompt) == (mode == "dag")
        recognizer.routing_mode = mode
        keys.append(recognizer._cache_key("查订单"))
    assert len(set(keys)) == 3
    for mode in ("subintent", "dag"):
        candidate = PROFILES[f"plan-multi-{mode}"]
        assert replace(candidate, name="plan-multi", routing_mode="keyword") == PROFILES["plan-multi"]
    with pytest.raises(ValueError, match="RETAIL_ROUTING_MODE"):
        router("invalid", [])


def test_general_retry_of_a_helper_preserves_assignment_and_read_only_permissions(monkeypatch):
    from agents.agent_orchestrator import AgentResponse

    app = router("dag", [node("a", "product_info"), node("b", "order_change", "a")])
    req = Request("product_info order_change", "u", "c", turn=1)
    general = app._best_agent(AgentType.GENERAL)
    seen = []

    async def handle(request):
        seen.append((general._is_primary(request), general._assignment(request)["tasks"][0]["id"]))
        return AgentResponse(AgentType.GENERAL, "checked", True)

    monkeypatch.setattr(general, "handle", handle)

    async def run():
        decision = await app.prepare_route(req)
        assert decision.primary_agent is AgentType.GENERAL
        await app.execute_role(req, AgentType.GENERAL, assigned_role=AgentType.TECHNICAL)
        assert req.dispatch_outcomes["technical"]["role"] == "general"
        await app.execute_role(req, AgentType.GENERAL)
        assert req.dispatch_role is None

    asyncio.run(run())
    assert seen == [(False, "a"), (True, "b")]
