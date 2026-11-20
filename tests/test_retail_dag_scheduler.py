import pytest
from core.retail_dag import Status
from tests.test_retail_dag import build, node, conditional_rows, COUNT_QUERY


def test_ready_loop_repeated_role_fan_in_and_independent_failure():
    from core.retail_scheduler import Execution
    plan = build([node("a"), node("b", "billing_lookup", "a"), node("c", "product_info", "b"),
                  node("d"), node("join", "product_info", "c", "d")])
    state = Execution.start(plan)
    visits = []
    while task := state.next_node(turn=1):
        run = state.runs[task.id]
        run.claim("billing" if task.kind == "billing_lookup" else "technical", 1)
        visits.append(task.id)
        if task.id == "d":
            run.move(Status.FAILED, reason="bad_read")
        else:
            run.move(Status.SUCCEEDED, refs=("tool:" + task.id,))
    assert visits == ["a", "d", "b", "c"]
    assert state.runs["join"].status == Status.BLOCKED
    assert state.next_node(1) is None


@pytest.mark.parametrize("n,chosen", [(2, "no"), (3, "yes")])
def test_conditional_join_requires_success_of_selected_branch(n, chosen):
    from core.retail_scheduler import Execution
    rows = conditional_rows() + [dict(node("j", "product_info", "yes", "no"), quote="汇总")]
    state = Execution.start(build(rows, COUNT_QUERY + "汇总"))
    assert state.next_node(1).id == "n"
    first = state.runs["n"]
    first.claim("billing", 1)
    first.move(Status.SUCCEEDED, refs=("tool:read",), facts={"purchased_item_count": n})
    assert state.next_node(1).id == chosen
    state.runs[chosen].claim("technical", 1)
    state.runs[chosen].move(Status.SUCCEEDED, refs=("tool:spec",))
    assert state.next_node(1).id == "j"


@pytest.mark.parametrize("workflow", ["langgraph", "legacy"])
def test_repeated_specialist_through_service(workflow, tmp_path, monkeypatch):
    import asyncio
    import json
    from agents.agent_orchestrator import AgentResponse, AgentType
    from agents.retail_agent import RetailService
    from agents.retail_multi_agent import RetailMultiAgent
    from agents.retail_tools import retail_tools
    from evaluation.harness.config import SEED_PATH
    from mcp_tools.retail_store import RetailStore
    from tests.test_retail_dispatch import router
    from tests.test_retail_dag import trace

    rows = [dict(node("v"), quote="variants 4768869376", target="4768869376", output="variants"),
            dict(node("o", "billing_lookup", "v"), quote="read #W6564160", target="#W6564160", output="item_ids"),
            dict(node("s", "product_info", "v", "o"), quote="which color?", output="color")]
    store = RetailStore(tmp_path / "retail.sqlite", SEED_PATH)
    seed = json.loads(SEED_PATH.read_text())
    uid = seed["orders"]["#W6564160"]["user_id"]
    store.find_user_by_email("u","c",seed["users"][uid]["email"])
    app = router("dag", rows)
    tools = retail_tools(store)
    app.set_shared_tools(tools)
    seen = []
    async def execute(req):
        a = req.dag_assignment
        seen.append((a["id"], req.dispatch_role, a["target"]))
        assert "address" not in json.dumps(a) and "payment_history" not in json.dumps(a)
        tool, arg = {"v": ("get_product_details","product_id"), "o": ("get_order_details","order_id"),
                     "s": ("get_item_details","item_id")}[a["id"]]
        args = {arg:a["target"]}
        t = trace(tool,args,**tools[tool].handler(req,args))
        return AgentResponse(AgentType(req.dispatch_role), "verified", True, tool_traces=[t])
    for role in ("technical","billing"):
        monkeypatch.setattr(app._best_agent(AgentType(role)), "handle", execute)
    service = RetailService(store, RetailMultiAgent(app), workflow_mode=workflow)
    async def run():
        try:
            return await service.handle_turn(" ".join(r["quote"] for r in rows),"u","c")
        finally:
            if service.workflow:
                await service.workflow.close()
    result = asyncio.run(run())
    assert seen == [("v","technical","4768869376"),("o","billing","#W6564160"),("s","technical","2652637226")]
    assert result.success


def test_durable_claim_compare_and_swap(tmp_path):
    from core.retail_scheduler import Execution
    from evaluation.harness.config import SEED_PATH
    from mcp_tools.retail_store import RetailStore, RetailStoreError
    store = RetailStore(tmp_path / "state.sqlite", SEED_PATH)
    state = Execution.start(build([node("x")]), "u")
    store.save_dag(state)
    a,b = [store.open_dags("client", "conv")[0] for _ in range(2)]
    for copy in (a,b):
        task = copy.next_node(1)
        copy.runs[task.id].claim("technical",1)
    store.save_dag(a)
    with pytest.raises(RetailStoreError, match="concurrent"):
        store.save_dag(b)
    assert store.open_dags("client","other") == []


@pytest.mark.parametrize("workflow", ["legacy","langgraph"])
def test_confirm_after_restarting_service(tmp_path,monkeypatch,workflow):
    import asyncio,json
    from agents.agent_orchestrator import AgentType,AgentResponse
    from agents.retail_agent import RetailService
    from agents.retail_multi_agent import RetailMultiAgent
    from agents.retail_tools import retail_tools
    from evaluation.harness.config import SEED_PATH
    from mcp_tools.retail_store import RetailStore
    from tests.test_retail_dispatch import router
    from tests.test_retail_dag import trace
    store = RetailStore(tmp_path / "confirm.sqlite",SEED_PATH,language="en")
    seed = json.loads(SEED_PATH.read_text())
    order = next(o for o in seed["orders"].values() if o["status"] == "delivered")
    oid = order["order_id"]
    uid = order["user_id"]
    store.find_user_by_email("u","c",seed["users"][uid]["email"])
    args = {"order_id":oid,"item_ids":[order["items"][0]["item_id"]],
            "payment_method_id":order["payment_history"][0]["payment_method_id"]}
    rows = [dict(node("ret","return_change"),quote="return from "+oid,target=oid,output="executed"),
            dict(node("read","order_lookup","ret"),quote="then check status of "+oid,target=oid,output="status")]
    tools = retail_tools(store)
    calls = []
    async def turn(message):
        app = router("dag",rows)
        app.language = "en"
        app.set_shared_tools(tools)
        async def execute(req):
            a = req.dag_assignment
            calls.append((req.turn,a["id"]))
            tool,params = ("return_delivered_order_items",args) if a["id"]=="ret" else ("get_order_details",{"order_id":oid})
            result = tools[tool].handler(req,params)
            return AgentResponse(AgentType(req.dispatch_role), result.get("display_text","done"), True,
                                 tool_traces=[trace(tool,params,**result)])
        for role in ("billing","general"):
            monkeypatch.setattr(app._best_agent(AgentType(role)),"handle",execute)
        service = RetailService(store,RetailMultiAgent(app),workflow_mode=workflow,language="en")
        try:
            return await service.handle_turn(message,"u","c")
        finally:
            if service.workflow:
                await service.workflow.close()
    first = asyncio.run(turn(" ".join(r["quote"] for r in rows)))
    op, = store.active_pending("u","c")
    assert op["status"]=="displayed" and op["display_text"] in first.response and not first.success
    second = asyncio.run(turn("Yes, please proceed."))
    assert calls == [(1,"ret"),(2,"ret"),(2,"read")]
    assert second.success and first.semantic_routing["dag"]["plan_id"] == second.semantic_routing["dag"]["plan_id"]
    assert sum(e["event_type"]=="return_requested" for e in store.list_events("u","c"))==1


@pytest.mark.parametrize("workflow", ["legacy","langgraph"])
@pytest.mark.parametrize("failed", [False, True])
def test_original_two_item_order_with_real_tool_loop(tmp_path,workflow,failed,monkeypatch):
    import asyncio,json
    from types import SimpleNamespace
    from agents.agent_orchestrator import AgentType
    from agents.retail_agent import RetailService
    from agents.retail_multi_agent import RetailMultiAgent
    from agents.retail_tools import retail_tools
    from core.retail_subtasks import OWNERS
    from evaluation.harness.config import SEED_PATH
    from mcp_tools.retail_store import RetailStore
    from tests.test_retail_dispatch import router
    clause = ("if there are at least three purchased items, find the sensor type of Gaming Mouse variant "
              "7420906769 (product 5713490933); otherwise find the ventilation of Cycling Helmet variant "
              "1665571435 (product 7765186836)")
    read = "Please look up order #W6564160 and count the purchased items recorded on that order."
    msg = ("Thanks. " + read + " Then apply this rule exactly: " + clause +
           ". Use the order record to choose which single variant to check. Please tell me the order finding "
           "and the applicable specification. This is read-only; no changes.")
    yes,no = clause.split(", ",1)[1].split("; otherwise ")
    rows = [dict(node("order","order_lookup"),quote=read,target="#W6564160",output="purchased_item_count")]
    for key,quote,target,field in [("mouse",yes,"7420906769","sensor_type"),("helmet",no,"1665571435","ventilation")]:
        rows.append(dict(node(key,"product_info","order"),quote=quote,target=target,output=field,
                         run_if={"source":"order","quote":clause}))
    app = router("dag",rows)
    monkeypatch.setitem(OWNERS,"order_lookup","billing")
    db = RetailStore(tmp_path / "db.sqlite",SEED_PATH)
    seed = json.loads(SEED_PATH.read_text())
    uid = seed["orders"]["#W6564160"]["user_id"]
    db.find_user_by_email("u","c",seed["users"][uid]["email"])
    db.begin_turn("u","c"); db.begin_turn("u","c")
    app.set_shared_tools(retail_tools(db))
    seen = []
    async def script(**kwargs):
        if kwargs["messages"][-1]["role"]=="user" and kwargs["messages"][-1]["content"][0].get("type")=="tool_result":
            block = SimpleNamespace(type="text",text="checked")
        else:
            assignment = json.loads(kwargs["messages"][0]["content"][0]["text"])["assignment"]
            role = "billing" if assignment["id"]=="order" else "technical"
            if role=="technical":
                assert assignment["inputs"] == {"order":{"purchased_item_count":2}}
                assert "payment_history" not in json.dumps(kwargs["messages"])
            seen.append((role,assignment["target"]))
            if failed and role=="technical":
                block = SimpleNamespace(type="text",text="unverified")
            else:
                name,arg = ("get_order_details","order_id") if role=="billing" else ("get_item_details","item_id")
                block = SimpleNamespace(type="tool_use",id="call"+role,name=name,input={arg:assignment["target"]})
        return SimpleNamespace(content=[block],stop_reason="tool_use" if block.type=="tool_use" else "end_turn",
                               usage=SimpleNamespace(input_tokens=0,output_tokens=0))
    monkeypatch.setattr(app._best_agent(AgentType.BILLING)._client.messages,"create",script)
    service = RetailService(db,RetailMultiAgent(app),workflow_mode=workflow)
    async def run():
        try:
            return await service.handle_turn(msg,"u","c")
        finally:
            if service.workflow:
                await service.workflow.close()
    result = asyncio.run(run())
    assert seen == [("billing","#W6564160"),("technical","1665571435")]
    assert result.turn == 3 and result.success is (not failed)
    assert [r["status"] for r in result.semantic_routing["dag"]["tasks"]] == [
        "succeeded","skipped","failed" if failed else "succeeded"]
    assert ("ventilation: high" in result.response) is (not failed)
