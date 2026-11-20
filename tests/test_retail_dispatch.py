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


def legacy_assignment_router(rows):
    """Exercise the retained role-assignment/permission path with already extracted v1 tasks."""
    from types import SimpleNamespace
    from core.intent_recognizer import UrgencyLevel
    app = router("dag", rows)
    async def recognized(*args, **kwargs):
        app._intent_recognizer.client.calls += 1
        return SimpleNamespace(intent=IntentCategory.QUERY, intent_group="query", urgency=UrgencyLevel.LOW,
                               confidence=1, entities={}, subtasks=rows)
    app._intent_recognizer.recognize = recognized
    return app


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
    app = legacy_assignment_router(rows)
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


def test_general_retry_preserves_assignment_and_cannot_borrow_write_permissions(monkeypatch):
    from agents.agent_orchestrator import AgentResponse

    app = legacy_assignment_router([node("a", "product_info"), node("b", "order_change", "a")])
    req = Request("product_info order_change", "u", "c", turn=1)
    general = app._best_agent(AgentType.GENERAL)
    seen = []
    app.set_shared_tools({"cancel_pending_order": object()})

    async def handle(request):
        seen.append(("cancel_pending_order" in general._tools_for_request(request),
                     general._assignment(request)["tasks"][0]["id"]))
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


def test_deferred_lookup_resumes_only_its_verified_order_and_catalog_request():
    from core.retail_subtasks import next_catalog_steps

    previous = [dict(node("read", "billing_lookup"), quote="receipt #W1234"),
                dict(node("spec", "product_info", "read"), quote="variant 111222")]
    graph = plan(previous)
    trace = {"agent_type": "billing", "tool_name": "get_order_details", "input": {"order_id": "#W1234"},
             "result_success": True, "result": {"success": True, "order": {"order_id": "#W1234"}}}
    saved = next_catalog_steps(graph, [trace])
    assert saved and not next_catalog_steps(graph, [dict(trace, blocked_by_guard=True)])
    current = [dict(node("read", "billing_lookup"), quote="receipt"),
               dict(node("spec", "product_info", "read"), quote="check again")]
    msg = "receipt: check again for variant 111222"
    assert list(plan_dispatch(current, msg, "dag", saved).roles) == ["technical", "billing"]
    for other in ["receipt: check again for 333444", msg + " on #W4321"]:
        assert not plan_dispatch(current, other, "dag", saved).resumed
    assert not plan_dispatch([current[0], dict(current[1], continues=None)], msg, "dag", saved).resumed
    assert plan_dispatch([current[0], dict(current[1], continues="spec")],
                         "receipt: check again", "dag", saved).resumed
    assert not plan_dispatch(current, msg, "subintent", saved).resumed
    app = router("dag", current)
    app._best_agent(AgentType.TECHNICAL).retail_allowed_tools = ()
    req = Request(msg, "u", "c", intent=IntentCategory.QUERY, catalog_steps=saved)
    decision = app._route_decision(req, current)
    assert list(decision.dispatch.roles) == ["billing"]
    assert not decision.dispatch.resumed


def test_return_lookup_basis_uses_verified_variant_and_matching_pending_card():
    from agents.retail_response_guard import constrain_response, return_lookup_basis

    msg = "If variant 111222 is below $20.00, return item 333444 on #W1234, otherwise item 555666."
    trace = {"tool_name": "get_item_details", "input": {"item_id": "111222"}, "result_success": True,
             "result": {"success": True, "item": {"item_id": "111222", "price": 19.95}}}
    basis = return_lookup_basis(msg, [trace])
    assert basis and not return_lookup_basis(msg, [dict(trace, result_success=False)])
    pending = {"kind": "return", "args": {"item_ids": ["333444"]}, "order_id": "#W1234",
               "op_id": "op1", "prepared_turn": 3, "status": "prepared", "display_text": "Pending card"}
    def guard(card):
        return constrain_response("Refund arrived.", traces=[trace], events=[], pending_before=[],
                                  pending_after=[card], turn=3, bound_user_id="u", language="en",
                                  return_basis=basis).text
    reply = guard(pending)
    assert "$19.95, below your $20.00" in reply and reply.index("333444") < reply.index("Pending card")
    assert reply.count("Pending card") == 1 and "Refund arrived" not in reply
    assert "$19.95" not in guard(dict(pending, order_id="#W9999"))


@pytest.mark.parametrize("setting,expected", [(None, "dag"), ("", "dag"), ("keyword", "keyword"),
                                              ("subintent", "subintent"), ("dag", "dag")])
def test_application_routing_defaults_and_overrides(monkeypatch, tmp_path, setting, expected):
    from unittest.mock import AsyncMock
    from agents import retail_runtime
    from api import web_console

    for key, value in {"ANTHROPIC_API_KEY": "offline", "LLM_MODEL": "offline",
                       "RETAIL_INPUT_CNY_PER_MTOK": "1", "RETAIL_OUTPUT_CNY_PER_MTOK": "1"}.items():
        monkeypatch.setenv(key, value)
    if setting is None:
        monkeypatch.delenv("RETAIL_ROUTING_MODE", raising=False)
    else:
        monkeypatch.setenv("RETAIL_ROUTING_MODE", setting)
    assert retail_runtime.RetailRuntimeConfig.from_env().routing_mode == expected

    web_configs = []
    def build(config):
        web_configs.append(config)
        return AsyncMock()
    monkeypatch.setattr(retail_runtime, "build_retail_runtime", build)
    monkeypatch.setattr(web_console, "asset_paths", lambda: (tmp_path/"seed", tmp_path/"policy"))
    asyncio.run(web_console.build_session_runtime(tmp_path, "multi", "en"))
    assert web_configs[0].routing_mode == expected


def test_standalone_retail_router_defaults_to_dag():
    app = AgentOrchestrator(model="offline", client=ReplayClient("OK"), retail_mode=True,
                            retail_role_classes=ROLES)
    assert app.routing_mode == app._intent_recognizer.routing_mode == "dag"
    assert '"depends_on"' in build_intent_prompt("查订单", retail_mode=True)
    assert IntentRecognizer(model="offline", client=ReplayClient("OK"), retail_mode=True).routing_mode == "dag"
