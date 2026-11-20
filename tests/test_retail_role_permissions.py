from dataclasses import replace
from types import SimpleNamespace

import pytest

from agents.agent_orchestrator import AgentType, Request
from agents.retail_tools import TASK_TOOLS
from tests.test_retail_dispatch import router

IDENTITY = {"find_user_id_by_email", "find_user_id_by_name_zip"}
ORDERS = {"get_user_details", "get_order_details"}
POLICY = {"search_knowledge_base", "load_skill"}
EXPECTED = {
    "general": IDENTITY | ORDERS | POLICY | {"cancel_pending_order", "modify_pending_order_items",
               "modify_pending_order_address", "modify_pending_order_payment", "modify_user_address"},
    "billing": IDENTITY | ORDERS | POLICY | {"calculate", "return_delivered_order_items",
                                             "exchange_delivered_order_items"},
    "technical": {"get_item_details", "get_product_details", "list_all_product_types",
                  "search_knowledge_base", "calculate"},
    "escalation": {"transfer_to_human_agents"},
}


@pytest.mark.parametrize("role", EXPECTED)
def test_role_and_task_intersection_at_exposure_and_dispatch(role):
    from agents.retail_hooks import TaskPermissionHook

    app = router("dag", [])
    registered = set.union(*EXPECTED.values()) | {"model_invented_tool"}
    app.set_shared_tools(dict.fromkeys(registered, object()))
    agent = app._best_agent(AgentType(role))
    req = Request("check", "u", "c", retail_primary="another-role")
    assert set(agent.get_tools()) == set(agent._tools_for_request(req)) == EXPECTED[role]
    for kind, limit in list(TASK_TOOLS.items()) + [("model_invented_kind", set())]:
        # Even a broader request-local limit and a spurious list in the assignment cannot grant a tool.
        scoped = replace(req, dag_assignment={"kind": kind, "target": "#W7", "tools": list(registered)},
                         dag_tool_limit=frozenset(registered))
        allowed = EXPECTED[role] & limit
        assert set(agent._tools_for_request(scoped)) == allowed
        ctx = SimpleNamespace(req=scoped, agent=agent)
        for name in registered - allowed:
            assert TaskPermissionHook().pre_tool(ctx, name, {})


def test_reused_billing_agent_loses_write_scope_on_next_node():
    from agents.retail_hooks import TaskPermissionHook

    app = router("dag", [])
    app.set_shared_tools(dict.fromkeys(EXPECTED["billing"], object()))
    agent = app._best_agent(AgentType.BILLING)
    tool = "return_delivered_order_items"
    req = Request("return", "u", "c")
    for kind, allowed in [("return_change", True), ("billing_lookup", False), ("return_policy", False)]:
        node = replace(req, dag_assignment={"kind": kind, "target": "#W7"})
        ctx = SimpleNamespace(req=node, agent=agent)
        assert (tool in agent._tools_for_request(node)) is allowed
        assert (TaskPermissionHook().pre_tool(ctx, tool, {"order_id": "#W7"}) is None) is allowed
        assert TaskPermissionHook().pre_tool(ctx, tool, {"order_id": "#W8"})


@pytest.mark.parametrize("mode", ["keyword", "subintent", "dag"])
@pytest.mark.parametrize("kind,owner", [("modify_address", "general"), ("cancel", "general"),
                                        ("return", "billing"), ("exchange", "billing")])
def test_pending_confirmation_routes_to_role_with_write_permission(mode, kind, owner):
    import asyncio

    req = Request("Yes, please proceed.", "u", "c",
                  retail_pending=[{"kind": kind, "confirmation_approved": True}])
    app = router(mode, [])
    app.language = "en"
    assert asyncio.run(app.prepare_route(req)).primary_agent.value == owner


def test_real_mcp_return_confirms_once_after_restarting_service(tmp_path):
    import asyncio
    import json
    from pathlib import Path

    from agents.agent_orchestrator import AgentOrchestrator
    from agents.retail_agent import RetailService
    from agents.retail_multi_agent import RetailMultiAgent
    from agents.retail_tools import retail_tools
    from evaluation.harness.config import SEED_PATH
    from evaluation.harness.dag_script import Script, role_classes
    from mcp_tools.retail_client import RetailMCPClient
    from mcp_tools.retail_store import RetailStore

    if not SEED_PATH.exists():
        pytest.skip("requires public retail seed")
    seed = json.loads(SEED_PATH.read_text())
    order = next(o for o in seed["orders"].values() if o["status"] == "delivered")
    store = RetailStore(tmp_path/"store.sqlite", SEED_PATH, language="en")
    uid = order["user_id"]
    assert store.find_user_by_email("u", "c", seed["users"][uid]["email"])["success"]

    async def turn(message):
        model = Script("dag")
        app = AgentOrchestrator(model="offline", client=model, retail_role_classes=role_classes(),
                                retail_mode=True, language="en")
        service = RetailService(store, RetailMultiAgent(app, language="en"), workflow_mode="langgraph", language="en")
        mcp = RetailMCPClient(db_path=tmp_path/"store.sqlite", seed_path=SEED_PATH, language="en",
                              expected_specs=retail_tools(store), project_root=Path(__file__).resolve().parents[1],
                              stderr_log_path=tmp_path/"mcp.log")
        try:
            await mcp.start()
            app.set_shared_tools(mcp.tools())
            result = await service.handle_turn(message, "u", "c")
            assert mcp.call_count == 1
            return result
        finally:
            await service.workflow.close()
            await mcp.close()

    first = asyncio.run(turn(f"Return item {order['items'][0]['item_id']} from order {order['order_id']} "
                             f"to {order['payment_history'][0]['payment_method_id']}."))
    op, = store.active_pending("u", "c")
    assert op["status"] == "displayed" and op["display_text"] in first.response
    assert not any(e["event_type"] == "return_requested" for e in store.list_events("u", "c"))
    second = asyncio.run(turn("Yes, please proceed."))
    assert second.success and not store.active_pending("u", "c")
    assert [e["op_id"] for e in store.list_events("u", "c") if e["event_type"] == "return_requested"] == [op["op_id"]]


def test_legacy_dispatch_keeps_changes_with_capable_roles():
    import asyncio
    from core.intent_recognizer import IntentCategory
    from tests.test_retail_dispatch import node, plan

    tasks = [node("cancel", "order_change"), node("read", "billing_lookup"), node("refund", "return_change")]
    dispatch = plan(tasks, mode="subintent")
    assert {r: [t.id for t in ts] for r, ts in dispatch.roles.items()} == {
        "general": ["cancel"], "billing": ["read", "refund"],
    }
    req = Request("Cancel my order", "u", "c", turn=3, intent=IntentCategory.REFUND)
    req.retail_plan = [{"turn": 2, "kind": "return"}, {"turn": 3, "kind": "cancel"}]
    assert asyncio.run(router("keyword", []).prepare_route(req)).primary_agent == AgentType.GENERAL


def test_open_request_reminder_and_handoff_stay_with_escalation():
    import asyncio
    from agents.tools import make_tool
    from core.intent_recognizer import IntentCategory

    called = []
    def record(req, args):
        called.append(args)
        return {"success": True}
    app = router("keyword", [])
    app.set_shared_tools({"transfer_to_human_agents": make_tool("transfer_to_human_agents", "handoff",
                         {"summary": {"type": "string"}, "reason": {"type": "string"}}, record)})
    req = Request("human agent", "u", "c", intent=IntentCategory.HUMAN_HANDOFF)
    req.retail_plan = [{"id": "goal", "kind": "cancel", "status": "open"}]
    decision = asyncio.run(app.prepare_route(req))
    assert decision.primary_agent == AgentType.ESCALATION
    actor = app._best_agent(decision.primary_agent)
    assert asyncio.run(actor.handle(req)).success and not called and req.plan_handoff_nudged == ["goal"]
    req.retail_plan[0]["nudged"] = True
    assert asyncio.run(actor.handle(req)).escalate and len(called) == 1


def test_empty_dag_cannot_grant_a_fresh_write_via_ordinary_routing():
    import asyncio
    from agents.retail_hooks import TaskPermissionHook

    app = router("dag", [])
    app.set_shared_tools(dict.fromkeys(EXPECTED["general"], object()))
    actor = app._best_agent(AgentType.GENERAL)
    req = Request("hello", "u", "c")
    asyncio.run(app.prepare_route(req, schedule=True))
    assert req.dag_assignment is None
    tool = "cancel_pending_order"
    assert tool not in actor._tools_for_request(req)
    args = {"order_id": "#W7", "reason": "ordered by mistake"}
    req = Request("Yes, please proceed.", "u", "c", retail_pending=[
        {"kind": "cancel", "status": "displayed", "args": args, "confirmation_approved": True},
    ])
    asyncio.run(app.prepare_route(req, schedule=True))
    assert tool in actor._tools_for_request(req)
    ctx = SimpleNamespace(req=req, agent=actor)
    assert TaskPermissionHook().pre_tool(ctx, tool, args) is None
    assert TaskPermissionHook().pre_tool(ctx, tool, {**args, "order_id": "#W8"})
