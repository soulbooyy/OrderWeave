import pytest
from evaluation.harness.config import SEED_PATH
from core.retail_dag import Status
from tests.test_retail_dag import build, node, conditional_rows, COUNT_QUERY


requires_seed = pytest.mark.skipif(not SEED_PATH.is_file(), reason="retail seed unavailable")


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
@pytest.mark.parametrize("verified", [True, False])
def test_conditional_join_requires_success_of_selected_branch(n, chosen, verified):
    from core.retail_scheduler import Execution
    rows = conditional_rows() + [dict(node("j", "product_info", "yes", "no"), quote="汇总")]
    state = Execution.start(build(rows, COUNT_QUERY + "汇总"))
    assert state.next_node(1).id == "n"
    first = state.runs["n"]
    first.claim("billing", 1)
    first.move(Status.SUCCEEDED, refs=("tool:read",), facts={"purchased_item_count": n})
    assert state.next_node(1).id == chosen
    state.runs[chosen].claim("technical", 1)
    if verified:
        state.runs[chosen].move(Status.SUCCEEDED, refs=("tool:spec",))
        assert state.next_node(1).id == "j"
    else:
        state.runs[chosen].move(Status.FAILED, reason="missing_evidence")
        assert state.next_node(1) is None and state.runs["j"].status == Status.BLOCKED


@pytest.mark.parametrize("workflow", ["langgraph", "legacy"])
@requires_seed
def test_repeated_specialist_through_service(workflow, tmp_path, monkeypatch):
    import asyncio
    import json
    from agents.agent_orchestrator import AgentResponse, AgentType
    from agents.retail_agent import RetailService
    from agents.retail_multi_agent import RetailMultiAgent
    from agents.retail_tools import retail_tools
    from mcp_tools.retail_store import RetailStore
    from tests.test_retail_dispatch import router
    from tests.test_retail_dag import trace

    rows = [dict(node("v"), quote="variants 4768869376", target="4768869376", output="variants"),
            dict(node("o", "billing_lookup", "v"), quote="read #W6564160", target="#W6564160", output="item_ids"),
            dict(node("s", "product_info", "v", "o"), quote="which color?", target="", output="color")]
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
    assert result.orchestration_mode == "dag" and result.turn_status == "completed"
    assert [row["id"] for row in result.task_runs] == ["v", "o", "s"]
    public = service.get_tool_trace(result.request_id)
    assert public["task_runs"] == result.task_runs and "payment_history" not in json.dumps(public)
    from api import main
    from types import SimpleNamespace
    async def observed(*args):
        return result
    monkeypatch.setattr(main, "_retail_runtime", SimpleNamespace(service=SimpleNamespace(handle_turn=observed)))
    payload = asyncio.run(main.chat(main.ChatRequest(message="example", conv_id="c")))
    assert not payload.primary_agent and payload.task_runs == result.task_runs and payload.success


@requires_seed
def test_durable_claim_compare_and_swap(tmp_path):
    from core.retail_scheduler import Execution
    from mcp_tools.retail_store import RetailStore, RetailStoreError
    store = RetailStore(tmp_path / "state.sqlite", SEED_PATH)
    state = Execution.start(build([node("x")]), "u")
    store.save_dag(state)
    a,b = [store.open_dags("client", "conv")[0] for _ in range(2)]
    for copy in (a,b):
        task = copy.next_node(1)
        copy.runs[task.id].claim("technical",1)
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    gate = Barrier(2)

    def claim(copy):
        gate.wait(timeout=5)
        try:
            store.save_dag(copy)
        except RetailStoreError as exc:
            assert "concurrent" in str(exc)
            return False
        return True

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(claim, (a, b))) == [False, True]
    assert store.open_dags("client","other") == []


@pytest.mark.parametrize("workflow", ["legacy","langgraph"])
@pytest.mark.parametrize("reply", ["Yes, please proceed.", "No", "human agent", "change the refund destination", "Use order #W9999999",
                                   "What color is variant 1665571435?", "rollback_keyword", "publish_basis", "publish_after_commit", "expired", "binding_mismatch", "undisplayed", "crash_after_commit"])
@requires_seed
def test_confirm_after_restarting_service(tmp_path,monkeypatch,workflow,reply):
    import asyncio,json
    from agents.agent_orchestrator import AgentType,AgentResponse
    from agents.retail_agent import RetailService
    from agents.retail_multi_agent import RetailMultiAgent
    from agents.retail_tools import retail_tools
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
    if reply == "publish_after_commit":
        rows[1]["output"] = "purchased_item_count"
    if reply == "publish_basis":
        rows[0]["depends_on"] = ["count"]
        rows.insert(0, dict(node("count", "order_lookup"), quote="count items in "+oid, target=oid,
                            output="purchased_item_count"))
    tools = retail_tools(store)
    calls = []
    async def turn(message):
        side = [dict(node("spec"),quote=message,target="1665571435",output="color")]
        mode = "keyword" if reply == "rollback_keyword" and message.startswith("Yes") else "dag"
        app = router(mode, side if message.startswith("What color") else rows)
        app.language = "en"
        app.set_shared_tools(tools)
        async def execute(req):
            a = req.dag_assignment
            calls.append((req.turn,a["id"]))
            tool,params = ("return_delivered_order_items",args) if a["id"]=="ret" else ("get_order_details",{"order_id":oid})
            if a["id"] == "spec":
                tool,params = "get_item_details",{"item_id":a["target"]}
            result = tools[tool].handler(req,params)
            return AgentResponse(AgentType(req.dispatch_role), result.get("display_text","done"), True,
                                 tool_traces=[trace(tool,params,**result)])
        for role in ("billing","general","technical"):
            monkeypatch.setattr(app._best_agent(AgentType(role)),"handle",execute)
        service = RetailService(store,RetailMultiAgent(app),workflow_mode=workflow,language="en")
        try:
            return await service.handle_turn(message,"u","c")
        finally:
            if service.workflow:
                await service.workflow.close()
    if reply == "undisplayed":
        with monkeypatch.context() as patch:
            from core.dag_tool_log import JournalConflict
            patch.setattr(store, "_mark_displayed", lambda *args: False)
            with pytest.raises(JournalConflict, match="confirmation card"):
                asyncio.run(turn(" ".join(r["quote"] for r in rows)))
        assert store.active_pending("u", "c")[0]["status"] == "prepared"
        asyncio.run(turn("Yes, please proceed."))
        assert not store.active_pending("u", "c") and not store.open_dags("u", "c")
        assert not any(e["event_type"] == "return_requested" for e in store.list_events("u", "c"))
        return

    first = asyncio.run(turn(" ".join(r["quote"] for r in rows)))
    op, = store.active_pending("u","c")
    assert op["status"]=="displayed" and op["display_text"] in first.response and not first.success
    if reply == "publish_basis":
        count = f'{len(order["items"])} items'
        assert count in first.response and first.response.index(count) < first.response.index(op["display_text"])
        assert first.response.count(op["display_text"]) == 1
        return
    assert [r["status"] for r in first.semantic_routing["dag"]["tasks"]] == ["waiting_user", "blocked"]
    assert first.turn_status == "waiting_user" and not first.success
    if reply == "binding_mismatch":
        from agents.agent_orchestrator import Request
        from agents.retail_dag_runner import DagRunner

        changed = Request("Yes, please proceed.", "u", "c", retail_store=store, turn=2,
                          retail_user_id="different-user", retail_pending=[op])
        assert DagRunner.resume(router("dag", []), changed) is None
        assert not store.open_dags("u", "c") and not store.active_pending("u", "c")
        assert not any(e["event_type"] == "return_requested" for e in store.list_events("u", "c"))
        return
    if reply == "crash_after_commit":
        class SimulatedCrash(BaseException):
            pass

        from agents.retail_dag_runner import DagRunner
        save = DagRunner.save
        def crash_before_checkpoint(driver):
            if driver.state.runs["ret"].status == Status.SUCCEEDED:
                raise SimulatedCrash()
            return save(driver)
        with monkeypatch.context() as patch:
            patch.setattr(DagRunner, "save", crash_before_checkpoint)
            with pytest.raises(SimulatedCrash):
                asyncio.run(turn("Yes, please proceed."))
        assert store.open_dags("u","c")[0].runs["ret"].status == Status.RUNNING
        recovered = asyncio.run(turn("continue"))
        if workflow == "langgraph" and recovered.workflow.get("recovered"):
            assert recovered.response_guard.get("version")
        if not recovered.success:
            recovered = asyncio.run(turn("continue"))
        assert recovered.success and [key for _,key in calls] == ["ret","ret","read"]
        assert sum(e["event_type"]=="return_requested" for e in store.list_events("u","c")) == 1
        return
    if reply == "expired":
        store.cancel_pending("u","c","expired")
        asyncio.run(turn("Yes, please proceed."))
        assert not store.active_pending("u","c")
        assert not any(e["event_type"]=="return_requested" for e in store.list_events("u","c"))
        return
    if reply == "rollback_keyword":
        recovered = asyncio.run(turn("Yes, please proceed."))
        assert recovered.success and recovered.orchestration_mode == "dag"
        assert recovered.semantic_routing["dag"]["plan_id"] == first.semantic_routing["dag"]["plan_id"]
        assert sum(e["event_type"] == "return_requested" for e in store.list_events("u", "c")) == 1
        return
    if reply == "publish_after_commit":
        answer = asyncio.run(turn("Yes, please proceed."))
        assert answer.success and f'{len(order["items"])} items' in answer.response
        return
    second = asyncio.run(turn(reply))
    if reply.startswith("What color"):
        assert second.success and calls == [(1,"ret"),(2,"spec")]
        second = asyncio.run(turn("Yes, please proceed."))
        assert calls[-2:] == [(3,"ret"),(3,"read")]
    elif reply != "Yes, please proceed.":
        assert not store.active_pending("u","c") and not store.open_dags("u","c")
        assert not any(e["event_type"]=="return_requested" for e in store.list_events("u","c"))
        if reply == "human agent":
            assert second.escalated
        return
    else:
        assert calls == [(1,"ret"),(2,"ret"),(2,"read")]
    asyncio.run(turn("Yes, please proceed."))
    assert second.success and first.semantic_routing["dag"]["plan_id"] == second.semantic_routing["dag"]["plan_id"]
    assert sum(e["event_type"]=="return_requested" for e in store.list_events("u","c"))==1


@pytest.mark.parametrize("workflow", ["legacy","langgraph"])
@pytest.mark.parametrize("failed", [False, True])
@pytest.mark.parametrize("count", [2,3])
@requires_seed
def test_order_count_branches_with_real_tool_loop(tmp_path,workflow,failed,monkeypatch,count):
    import asyncio,json
    from types import SimpleNamespace
    from agents.agent_orchestrator import AgentType
    from agents.retail_agent import RetailService
    from agents.retail_multi_agent import RetailMultiAgent
    from agents.retail_tools import retail_tools
    from core.retail_subtasks import OWNERS
    from mcp_tools.retail_store import RetailStore
    from tests.test_retail_dispatch import router
    clause = ("if there are at least three purchased items, find the sensor type of Gaming Mouse variant "
              "7420906769 (product 5713490933); otherwise find the ventilation of Cycling Helmet variant "
              "1665571435 (product 7765186836)")
    seed = json.loads(SEED_PATH.read_text())
    oid = "#W6564160" if count == 2 else next(k for k,o in seed["orders"].items() if len(o["items"]) == count)
    read = f"Please look up order {oid} and count the purchased items recorded on that order."
    msg = ("Thanks. " + read + " Then apply this rule exactly: " + clause +
           ". Use the order record to choose which single variant to check. Please tell me the order finding "
           "and the applicable specification. This is read-only; no changes.")
    yes,no = clause.split(", ",1)[1].split("; otherwise ")
    rows = [dict(node("order","order_lookup"),quote=read,target=oid,output="purchased_item_count")]
    for key,quote,target,field in [("mouse",yes,"7420906769","sensor_type"),("helmet",no,"1665571435","ventilation")]:
        rows.append(dict(node(key,"product_info","order"),quote=quote,target=target,output=field,
                         run_if={"source":"order","branch":"then" if quote == yes else "else"}))
    app = router("dag",rows)
    monkeypatch.setitem(OWNERS,"order_lookup","billing")
    db = RetailStore(tmp_path / "db.sqlite",SEED_PATH)
    uid = seed["orders"][oid]["user_id"]
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
                assert assignment["inputs"] == {"order":{"purchased_item_count":count}}
                assert assignment["input_refs"]["order"] == ["tool:callbilling"]
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
    chosen_id = "1665571435" if count == 2 else "7420906769"
    assert seen == [("billing",oid),("technical",chosen_id)]
    assert result.turn == 3 and result.success is (not failed)
    expected = "failed" if failed else "succeeded"
    branches = ["skipped",expected] if count == 2 else [expected,"skipped"]
    assert [r["status"] for r in result.semantic_routing["dag"]["tasks"]] == ["succeeded",*branches]
    assert f"{count} 件" in result.response
    field = "通风等级" if count == 2 else "传感器类型"
    assert field in result.response and chosen_id in result.response
    assert ("尚未核实" in result.response) is failed
    assert (("high" if count == 2 else "laser") in result.response) is (not failed)


def test_unknown_or_omitted_condition_never_reaches_an_agent():
    import asyncio
    from agents.agent_orchestrator import Request
    from tests.test_retail_dispatch import router
    msg = "如果订单至少有 3 件商品，就查鼠标规格，否则查头盔规格。"
    for rows in ([], [dict(node("t"), quote="查头盔规格", output="color")]):
        result = asyncio.run(router("dag", rows).run(Request(msg,"u","c")))
        assert result.routing_reason.startswith("dag_contract:") and not result.success
        assert not result.role_traces


@requires_seed
def test_ambiguous_targets_can_only_resume_from_a_verified_choice(tmp_path):
    from types import SimpleNamespace
    from agents.agent_orchestrator import Request
    from agents.retail_dag_runner import DagRunner
    from core.retail_scheduler import Execution
    from mcp_tools.retail_store import RetailStore
    store = RetailStore(tmp_path/"db.sqlite",SEED_PATH)
    state = Execution.start(build([dict(node("read","order_lookup"),output="item_ids"),
                                   dict(node("spec","product_info","read"),output="color")]))
    r = state.runs["read"]
    r.move(Status.READY); r.claim("billing",1)
    r.move(Status.SUCCEEDED,facts={"item_ids":["111222","333444"]},refs=("tool:1",))
    req = Request("read", "client","conv",turn=1,retail_store=store)
    app = SimpleNamespace(language="en")
    driver = DagRunner(app,req,state)
    assert driver.next_role() is None
    req.turn = 2
    for msg in ("some other question","999999"):
        req.message = msg
        assert DagRunner.resume(app,req) is None
    req.message = "333444"
    resumed = DagRunner.resume(app,req)
    assert resumed.state.target(state.plan.nodes[1]) == ("333444",[])
    assert resumed.state.runs["read"].attempt == 1


@requires_seed
def test_recovery_uses_op_event_without_replaying_write(tmp_path):
    import json
    from types import SimpleNamespace
    from agents.agent_orchestrator import Request
    from agents.retail_dag_runner import DagRunner
    from core.retail_scheduler import Execution
    from mcp_tools.retail_store import RetailStore
    store = RetailStore(tmp_path/"recovery.sqlite", SEED_PATH, language="en")
    seed = json.loads(SEED_PATH.read_text())
    order = next(o for o in seed["orders"].values() if o["status"]=="delivered")
    uid,oid = order["user_id"],order["order_id"]
    store.begin_turn("client","conv")
    store.find_user_by_email("client","conv",seed["users"][uid]["email"])
    args = dict(order_id=oid,item_ids=[order["items"][0]["item_id"]],
                payment_method_id=order["payment_history"][0]["payment_method_id"])
    tool = "return_delivered_order_items"
    prepared = store.write_action("client","conv",tool,args,"")
    store.mark_pending_displayed("client","conv",prepared["op_id"],1,False)
    rejected = store.write_action("client", "conv", tool, args, "yes", expected_op_id=prepared["op_id"])
    assert not rejected["success"] and [p["op_id"] for p in store.active_pending("client", "conv")] == [prepared["op_id"]]
    store.begin_turn("client","conv")
    state = Execution.start(build([dict(node("ret","return_change"),quote=oid,target=oid,output="executed")]),uid)
    state.operation = dict(kind="return",args=args,op_id=prepared["op_id"])
    state.next_node(2)
    state.runs["ret"].claim("billing",2)
    store.save_dag(state)
    req = Request("continue","client","conv",retail_store=store,retail_user_id=uid,turn=3)
    app = SimpleNamespace(language="en")
    interrupted = DagRunner.resume(app,req)
    assert interrupted.state.stop == "unknown_result" and interrupted.next_role() is None
    assert store.write_action("client","conv",tool,args,"Yes, please proceed.")["action_status"] == "executed"
    recovered = DagRunner.resume(app,req)
    assert recovered.state.complete and recovered.next_role() is None
    assert len([e for e in store.list_events("client","conv") if e["event_type"]=="return_requested"]) == 1


@requires_seed
def test_stale_claim_cannot_prepare_business_operation(tmp_path):
    import json
    from core.retail_scheduler import Execution
    from mcp_tools.retail_store import RetailStore
    store = RetailStore(tmp_path/"race.sqlite",SEED_PATH)
    seed = json.loads(SEED_PATH.read_text())
    order = next(o for o in seed["orders"].values() if o["status"]=="delivered")
    oid,uid = order["order_id"],order["user_id"]
    turn = store.begin_turn("client","conv")
    store.find_user_by_email("client","conv",seed["users"][uid]["email"])
    state = Execution.start(build([dict(node("w","return_change"),quote=oid,target=oid,output="executed")]),uid)
    state.next_node(turn)
    state.runs["w"].claim("billing",turn)
    store.save_dag(state)
    ticket = (state.plan.id,"w",state.revision,turn)
    store.begin_turn("client","conv")
    args = dict(order_id=oid,item_ids=[order["items"][0]["item_id"]],
                payment_method_id=order["payment_history"][0]["payment_method_id"])
    answer = store.write_action("client","conv","return_delivered_order_items",args,"",
                                allow_execute=False,dag_ticket=ticket)
    assert not answer["success"] and not store.active_pending("client","conv")


@pytest.mark.parametrize("workflow", ["legacy","langgraph"])
@requires_seed
def test_longest_plan_and_unavailable_role(workflow,tmp_path,monkeypatch):
    import asyncio
    from agents.agent_orchestrator import AgentType, AgentResponse
    from agents.retail_agent import RetailService
    from agents.retail_multi_agent import RetailMultiAgent
    from agents.retail_tools import retail_tools
    from mcp_tools.retail_store import RetailStore
    from tests.test_retail_dispatch import router
    from tests.test_retail_dag import trace
    rows = [dict(node(f"n{i}","product_info",*([f"n{i-1}"] if i else [])),
                 quote="1665571435 color",target="1665571435",output="color") for i in range(12)]
    store = RetailStore(tmp_path/"db.sqlite",SEED_PATH)
    app = router("dag",rows)
    tools = retail_tools(store)
    app.set_shared_tools(tools)
    calls = []
    async def handle(req):
        calls.append(req.dag_assignment["id"])
        args = {"item_id":"1665571435"}
        return AgentResponse(AgentType.TECHNICAL,"checked",True,tool_traces=[
            trace("get_item_details",args,**tools["get_item_details"].handler(req,args))])
    monkeypatch.setattr(app._best_agent(AgentType.TECHNICAL),"handle",handle)
    async def run():
        service = RetailService(store,RetailMultiAgent(app),workflow_mode=workflow)
        try:
            completed = await service.handle_turn(rows[0]["quote"],"u","a")
            app._pool.pop(AgentType.TECHNICAL)
            blocked = await service.handle_turn(rows[0]["quote"],"u","b")
            return completed,blocked
        finally:
            if service.workflow:
                await service.workflow.close()
    completed,blocked = asyncio.run(run())
    assert completed.success and calls == [r["id"] for r in rows]
    assert not blocked.success and all(r["status"]=="blocked" for r in blocked.semantic_routing["dag"]["tasks"])


def test_dispatch_cap_and_budget_stop_are_bounded():
    from core.retail_scheduler import Execution
    from agents.retail_dag_runner import DagRunner
    from agents.agent_orchestrator import Request,AgentType
    from evaluation.retail_budget import BudgetExceeded
    from tests.test_retail_dispatch import router
    import asyncio
    state = Execution.start(build([dict(node("a"),quote="111222 color",target="111222",output="color"),node("b")]))
    first = state.next_node(0,limit=1)
    state.runs[first.id].claim("technical",0)
    state.runs[first.id].move(Status.FAILED,reason="test")
    assert state.next_node(0,limit=1) is None and state.stop=="dispatch_limit"
    fresh = Execution.start(state.plan)
    app = router("dag",[])
    app.set_shared_tools({"get_item_details": object()})
    async def stopped(req):
        raise BudgetExceeded("offline")
    app._best_agent(AgentType.TECHNICAL).handle = stopped
    runner = DagRunner(app,Request("read","u","c"),fresh)
    assert runner.next_role()=="technical"
    with pytest.raises(BudgetExceeded):
        asyncio.run(runner.execute())
    assert runner.next_role() is None and fresh.stop=="budget_stop"


def test_task_guard_rejects_wrong_branch_target_even_when_role_has_tool():
    from agents.agent_orchestrator import Request,AgentType
    from agents.retail_hooks import TaskPermissionHook
    from types import SimpleNamespace
    req = Request("check helmet","u","c",dag_tool_limit=frozenset({"get_item_details"}),
                  dag_assignment={"kind":"product_info","target":"1665571435","output":"ventilation"})
    ctx = SimpleNamespace(req=req,agent=SimpleNamespace(language="en",_tools_for_request=lambda req: req.dag_tool_limit))
    guard = TaskPermissionHook()
    assert guard.pre_tool(ctx,"get_item_details",{"item_id":"7420906769"})
    assert guard.pre_tool(ctx,"get_item_details",{"item_id":"1665571435"}) is None


def test_no_overlap_cannot_be_replaced_with_unrelated_user_chosen_item():
    from core.retail_scheduler import Execution
    from agents.retail_dag_runner import DagRunner
    from agents.agent_orchestrator import Request
    state = Execution.start(build([
        dict(node("v"),output="variants"),dict(node("o","order_lookup"),output="item_ids"),
        dict(node("s","product_info","v","o"),output="color")]))
    for key,facts in [("v",{"variants":["111222"]}),("o",{"item_ids":["333444"]})]:
        task = state.next_node(1)
        assert task.id == key
        state.runs[key].claim("technical",1)
        state.runs[key].move(Status.SUCCEEDED,facts=facts,refs=("tool:1",))
    runner = DagRunner(None,Request("color","u","c",turn=1),state)
    assert runner.next_role() is None
    assert state.runs["s"].status == Status.BLOCKED and state.runs["s"].reason == "no_matching_target"


def test_unrepresented_else_action_is_rejected():
    from core.retail_dag import Plan
    rows = conditional_rows()
    with pytest.raises(ValueError, match="branch"):
        Plan.for_execution(rows[:2], COUNT_QUERY, "u","c")


def test_specialist_with_no_capability_is_blocked_before_claim():
    from agents.agent_orchestrator import Request
    from agents.retail_dag_runner import DagRunner
    from core.retail_scheduler import Execution
    from tests.test_retail_dispatch import router

    app = router("dag", [])
    app.set_shared_tools({"calculate": object()})
    row = dict(node("spec"), quote="111222 color", target="111222", output="color")
    state = Execution.start(build([row]))
    driver = DagRunner(app, Request("111222 color", "u", "c"), state)
    assert driver.next_role() is None
    assert (state.runs["spec"].reason, state.runs["spec"].attempt) == ("capability_unavailable", 0)


def test_composer_orders_verified_basis_before_unmodified_card():
    from agents.retail_dag_composer import compose_tasks
    from core.retail_scheduler import Execution

    rows = conditional_rows()
    rows[1].update(kind="return_change", output="executed")
    state = Execution.start(build(rows, COUNT_QUERY))
    count = state.runs["n"]
    count.status, count.refs, count.facts = Status.SUCCEEDED, ("tool:count",), {"purchased_item_count": 3}
    state.runs["yes"].status = Status.WAITING_USER
    state.runs["no"].status = Status.SKIPPED
    card = "确认详情\n请核对全部内容。"
    text = compose_tasks(state, "zh", card)
    assert text.index("3 件") < text.index("根据已核实") < text.index(card)
    assert text.endswith(card) and text.count(card) == 1 and "已提交" not in text
