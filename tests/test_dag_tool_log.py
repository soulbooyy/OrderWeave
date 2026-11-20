"""Durability and protocol tests through the DAG tool and SQLite boundaries."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
from threading import Barrier

import pytest

from agents.agent_orchestrator import AgentOrchestrator, Request
from agents.retail_agent import RetailService
from agents.retail_multi_agent import RetailMultiAgent
from agents.retail_tools import retail_tools
from core.context_engine import ContextEngine
from core.retail_dag import Plan
from core.retail_scheduler import Execution
from evaluation.harness.config import SEED_PATH
from evaluation.harness.routing_ablation import ROLES
from mcp_tools.retail_store import RetailStore
from tests.test_dag_context_baseline import MESSAGE, PLAN, assert_checkpoint_scope
from tests.test_dag_task_context import Model

KIND = "dag_exchange_v1"
pytestmark = pytest.mark.skipif(not SEED_PATH.is_file(), reason="requires frozen public retail seed")


@pytest.fixture
def db(tmp_path):
    store = RetailStore(tmp_path / "retail.sqlite", SEED_PATH, language="en")
    seed = json.loads(SEED_PATH.read_bytes())
    owner = seed["orders"]["#W6564160"]["user_id"]
    store.find_user_by_email("u", "c", seed["users"][owner]["email"])
    return store


def run_service(store, workflow, mode, nodes=PLAN, message=MESSAGE, model=None, engine=None):
    model = model or Model(nodes)
    router = AgentOrchestrator(model="offline", client=model, routing_mode="dag",
                               retail_mode=True, retail_role_classes=ROLES, language="en")
    router.set_shared_tools(retail_tools(store, language="en"))
    service = RetailService(store, RetailMultiAgent(router, language="en"), workflow_mode=workflow,
                            context_engine=engine or ContextEngine(store), conversation_context_enabled=True,
                            dag_log_mode=mode, language="en")

    async def chat():
        try:
            result = await service.handle_turn(message, "u", "c")
            if service.workflow and result.success and not result.workflow.get("recovered"):
                graph = service.workflow
                states = [s async for s in graph._graph.aget_state_history(graph.config("u", "c"))]
                if nodes == PLAN:
                    assert_checkpoint_scope(states, result.semantic_routing["dag"]["plan_id"])
            return result
        finally:
            if service.workflow:
                await service.workflow.close()

    return asyncio.run(chat()), model


@pytest.mark.parametrize("mode", ["m", "j"])
@pytest.mark.parametrize("workflow", ["legacy", "langgraph"])
def test_tools_attributed_without_contaminating_role_history(db, workflow, mode):
    result, model = run_service(db, workflow, mode)
    assert result.success
    rows = db.conversation_log("u", "c")
    assert [r["kind"] for r in rows] == ["user", KIND, KIND, KIND, "assistant"]
    batches = [row["content"][0] for row in rows[1:-1]]
    assert [b["task_id"] for b in batches] == ["T1", "T2", "T3"]
    for b, trace in zip(batches, result.tool_traces, strict=True):
        assert b["plan_id"] == result.semantic_routing["dag"]["plan_id"] and b["attempt"] == 1 and b["batch_no"] == 1
        call, = b["calls"]
        assert call["tool_use"]["id"] == call["result"]["tool_use_id"] == trace["tool_use_id"]
        assert json.loads(call["result"]["content"]) == trace["result"]
    assert len(model.requests[-2]["messages"]) == 1
    assert "payment_history" not in json.dumps(model.requests[-2]["messages"])
    assert rows[-1]["content"][0]["text"] == result.response


def test_old_close_preserves_incremental_batch(db):
    # Simulate a persisted batch left by a previous process, then roll back the feature flag.
    with db._tx() as con:
        con.execute("INSERT INTO conversation_log VALUES (?,?,?,?,?,?,?,?,?)",
                    ("u", "c", 1, 1, "assistant", KIND, "general", '[{"version":1}]', "old"))
    engine = ContextEngine(db)
    rows = engine.log_rows(engine.user_blocks(1, {}, "request"), [], "Published")
    db.save_turn_messages("u", "c", 1, rows)
    assert any(row["kind"] == KIND for row in db.conversation_log("u", "c"))


class PowerLoss(BaseException):
    """Bypass ordinary exception handlers, like losing the worker between two commits."""


def reopen(store):
    return RetailStore(store.db_path, SEED_PATH, language="en")


def batches(store, turn):
    return [r["content"][0] for r in store.conversation_log("u", "c") if r["kind"] == KIND and r["turn"] == turn]


def crash_on(store, monkeypatch, boundary, *, after):
    record = store.record_tool_step
    def step(client, conv, batch, index, action, result=None):
        if action == boundary and not after:
            raise PowerLoss()
        record(client, conv, batch, index, action, result)
        if action == boundary and after:
            raise PowerLoss()
    monkeypatch.setattr(store, "record_tool_step", step)


READ = [dict(id="read", kind="product_info", target="2652637226", output="color",
             quote="Check color of variant 2652637226", depends_on=[])]
SWAP = [dict(id="swap", kind="return_change", target="#W6564160", output="executed", depends_on=[],
             quote="Exchange item 2652637226 on #W6564160 to 3254583681 using paypal_2233507")]


@pytest.mark.parametrize("workflow", ["langgraph", "legacy"])
@pytest.mark.parametrize("boundary,after,success,count", [("started", True, False, 0), ("returned", False, False, 1),
                                                        ("returned", True, True, 1)])
def test_recovery_does_not_replay_read(db, monkeypatch, workflow, boundary, after, success, count):
    invoked = []
    original = db.get_item_details
    def read(*args):
        invoked.append(args)
        return original(*args)
    monkeypatch.setattr(db, "get_item_details", read)
    crash_on(db, monkeypatch, boundary, after=after)
    with pytest.raises(PowerLoss):
        run_service(db, workflow, "j", READ, READ[0]["quote"])
    assert len(invoked) == count
    call, = batches(db, 1)[0]["calls"]
    assert call["receipt"] == ("original" if success else "raw_result_missing")
    assert ("result" in call) == success
    assert all(r["kind"] == KIND for r in db.conversation_log("u", "c"))
    restored = reopen(db)
    recovered, model = run_service(restored, workflow, "off", READ, "continue")
    assert recovered.success == success and not model.requests and not model.plans
    assert recovered.response_guard["version"]
    assert batches(restored, 1)[0]["calls"] == [call]
    if success:
        assert recovered.semantic_routing["dag"]["tasks"][0]["refs"] == ["tool:call-read"]


@pytest.mark.parametrize("workflow", ["langgraph", "legacy"])
@pytest.mark.parametrize("boundary", ["before_handler", "after_commit", "after_receipt"])
def test_write_crash_resolves_only_from_matching_event(db, monkeypatch, workflow, boundary):
    prepared, _ = run_service(db, workflow, "j", SWAP, SWAP[0]["quote"])
    pending, = db.active_pending("u", "c")
    assert pending["status"] == "displayed" and pending["display_text"] in prepared.response
    with monkeypatch.context() as patch:
        if boundary == "before_handler":
            crash_on(db, patch, "started", after=True)
        else:
            crash_on(db, patch, "returned", after=boundary == "after_receipt")
        with pytest.raises(PowerLoss):
            run_service(db, workflow, "j", SWAP, "Yes, I confirm.")
    restored = reopen(db)
    recovered, model = run_service(restored, workflow, "off", SWAP, "continue")
    events = [e for e in restored.list_events("u", "c") if e["event_type"] == restored.executed_event("exchange")]
    assert len(events) == int(boundary != "before_handler") and not model.plans and not model.requests
    call, = batches(restored, 2)[0]["calls"]
    assert ("result" in call) == (boundary == "after_receipt")
    if boundary == "before_handler":
        assert recovered.semantic_routing["dag"]["stop"] == "unknown_result"
        assert call["outcome"] == "unknown_result"
    else:
        ref = f"event:{events[0]['event_id']}"
        assert recovered.success and recovered.semantic_routing["dag"]["tasks"][0]["refs"] == [ref]
        assert events[0]["op_id"] == pending["op_id"]
        if boundary == "after_commit":
            assert call["event_ref"] == ref and call["receipt"] == "raw_result_missing"


@pytest.mark.parametrize("change", ["op_id", "args", "binding", "order_owner"])
def test_mismatched_write_evidence_stays_unknown(db, monkeypatch, change):
    run_service(db, "legacy", "j", SWAP, SWAP[0]["quote"])
    crash_on(db, monkeypatch, "returned", after=False)
    with pytest.raises(PowerLoss):
        run_service(db, "legacy", "j", SWAP, "Yes, I confirm.")
    state, = db.open_dags("u", "c")
    op = deepcopy(state.operation)
    if change == "op_id":
        op["op_id"] += "_different"
    elif change == "args":
        op["args"]["new_item_ids"] = []
    else:
        with db._tx() as con:
            other = con.execute("SELECT user_id FROM users WHERE user_id<>? LIMIT 1", (state.bound_user,)).fetchone()[0]
            if change == "binding":
                con.execute("UPDATE sessions SET retail_user_id=? WHERE client_user_id='u' AND conv_id='c'", (other,))
            else:
                con.execute("UPDATE orders SET user_id=? WHERE order_id='#W6564160'", (other,))
    run = state.runs["swap"]
    assert db.reconcile_tool_write("u", "c", state.plan.id, run, "#W6564160", op, batches(db, 2)) is None
    call, = batches(db, 2)[0]["calls"]
    assert call["outcome"] == "unknown_result" and "result" not in call and "event_ref" not in call


def claimed_batch(db, blocks):
    from core.dag_tool_log import ToolBatch
    turn = db.begin_turn("u", "c")
    node = READ[0]
    state = Execution.start(Plan.for_execution(READ, node["quote"], "u", "c"), db.bound_user_id("u", "c"))
    task = state.next_node(turn)
    state.runs[task.id].claim("technical", turn)
    db.save_dag(state)
    req = Request(node["quote"], "u", "c", retail_store=db, retail_user_id=state.bound_user, turn=turn,
                  dag_log_mode="j", dag_assignment=node, dag_attempt=1, dag_ticket=(state.plan.id, task.id, state.revision, turn))
    return ToolBatch(req, "technical", 1, blocks)


def use(ident):
    return dict(type="tool_use", id=ident, name="get_item_details", input={"item_id":"2652637226"})


def receipt(ident, **value):
    return dict(type="tool_result", tool_use_id=ident, content=json.dumps(value))


def test_mixed_batch_and_scoped_projection(db):
    from core.dag_tool_log import JournalConflict, ToolBatch
    batch = claimed_batch(db, [use("first"), {"type":"text", "text":"DRAFT_MARKER"}, use("second")])
    with pytest.raises(JournalConflict):
        ToolBatch(batch.req, "technical", 2, [use("duplicate"), use("duplicate")])
    req = batch.req
    def view(role="technical", tools=("get_item_details",)):
        return db.task_tool_messages("u","c",req.dag_ticket,role,tools)
    batch.step(0,"started")
    batch.step(0,"returned",receipt("first", success=True, marker="ORIGINAL"))
    batch.step(1,"started")
    assert view() == []
    failed = receipt("second", success=False, error="real business refusal")
    with pytest.raises(JournalConflict):
        batch.step(1,"returned",receipt("not-second", success=True))
    batch.step(1,"returned",failed)
    batch.step(1,"returned",failed)
    with pytest.raises(JournalConflict):
        batch.step(1,"returned",receipt("second", success=True))
    with pytest.raises(JournalConflict):
        batch.step(0,"started")
    assert len(view()) == 2 and view("billing") == [] and view(tools=()) == []
    visible = view()
    visible[1]["content"].clear()
    assert [b["tool_use_id"] for b in view()[1]["content"]] == ["first","second"]
    saved, = batches(db,1)
    assert saved["calls"][1]["outcome"] == "business_failure"
    assert "DRAFT_MARKER" not in json.dumps(saved)
    assert "ORIGINAL" not in json.dumps(ContextEngine(db).history("u","c",["get_item_details"]))
    assert "ORIGINAL" not in ContextEngine._transcript_text(db.conversation_log("u","c"), "", 0)


def test_delayed_original_only_updates_its_old_slot(db):
    batch = claimed_batch(db,[use("old"),use("blocked")])
    batch.step(0,"started")
    batch.step(1,"blocked")
    db.begin_turn("u","c")
    batch.step(0,"returned",receipt("old",success=True))
    old, = batches(db,1)
    assert [c["outcome"] for c in old["calls"]] == ["success","not_executed"]
    assert "result" not in old["calls"][1] and not batches(db,2)
    state, = db.open_dags("u","c")
    assert state.runs["read"].status.value == "running"


@pytest.mark.parametrize("conflicting",[False,True])
def test_two_connections_first_close_and_receipt_are_immutable(db, conflicting):
    from core.dag_tool_log import JournalConflict
    batch = claimed_batch(db,[use("id")])
    batch.step(0,"started")
    second = reopen(db)

    def concurrent(fn):
        barrier = Barrier(2)
        def enter(store, i):
            barrier.wait(timeout=5)
            try:
                fn(store, i)
            except JournalConflict:
                return False
            return True
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(enter, store, i) for i, store in enumerate((db, second))]
            return [f.result(timeout=10) for f in futures]

    def save_receipt(store, i):
        store.record_tool_step("u","c",batch.batch,0,"returned",receipt("id",success=True, value=i if conflicting else 0))
    def close(store, i):
        text = f"Guarded {i if conflicting else 0}"
        engine = ContextEngine(store)
        rows = engine.log_rows(engine.user_blocks(1, {}, "Request"), [], text)
        store.finish_dialogue("u","c",1,"Request",text,rows=rows)
    assert sum(concurrent(save_receipt)) == 2-int(conflicting)
    before = batches(db,1)
    assert sum(concurrent(close)) == 2-int(conflicting)
    rows = db.conversation_log("u","c")
    assert [r["kind"] for r in rows] == ["user",KIND,"assistant"] and batches(db,1) == before
    winner = rows[-1]["content"][0]["text"]
    assert db.last_visible_turn("u","c")["assistant"] == winner
    db.save_visible_turn("u","c",1,"Request",winner)
    with pytest.raises(JournalConflict):
        db.save_visible_turn("u","c",1,"Request","changed")
    assert db.conversation_log("u","c") == rows and db.last_visible_turn("u","c")["assistant"] == winner


@pytest.mark.parametrize("workflow", ["legacy", "langgraph"])
def test_terminal_before_publish_preserves_journal_and_never_replays(db, monkeypatch, workflow):
    def crash(*args, **kwargs):
        raise PowerLoss()
    monkeypatch.setattr(db, "finish_dialogue", crash)
    with pytest.raises(PowerLoss):
        run_service(db, workflow, "j", READ, READ[0]["quote"])
    prior = db.conversation_log("u","c")
    assert len(prior) == 1 and not db.open_dags("u","c")
    restored = reopen(db)
    recovered, model = run_service(restored, workflow, "off", READ, "continue")
    assert recovered.success and not model.plans and not model.requests and recovered.response_guard["version"]
    assert [r for r in restored.conversation_log("u","c") if r["turn"] == 1] == prior


def test_two_writers_cannot_dispatch_same_call(db):
    from core.dag_tool_log import JournalConflict
    batch = claimed_batch(db, [use("once")])
    barrier = Barrier(2)
    def start(store):
        barrier.wait(timeout=5)
        try:
            store.record_tool_step("u","c",batch.batch,0,"started")
        except JournalConflict:
            return False
        return True
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(start, store) for store in (db, reopen(db))]
        assert sum(f.result(timeout=10) for f in futures) == 1
    wrong = deepcopy(batch.batch)
    wrong["calls"][0]["tool_use"]["input"] = {"item_id":"changed"}
    with pytest.raises(JournalConflict):
        db.record_tool_step("u","c",wrong,0,"returned",receipt("once", success=True))
    assert batches(db,1)[0]["calls"][0]["outcome"] == "unknown_result"


def test_legacy_and_journal_skill_gate_requires_matched_real_receipt(db):
    from core.dag_tool_log import loaded_skills
    engine = ContextEngine(db)
    tool = {"type":"tool_use","name":"load_skill","input":{"name":"exchange"},"id":"s"}
    result = receipt("s", success=True, skill="exchange", playbook="PRIVATE_POLICY_MARKER")
    exchange = dict(agent="billing", assistant=[tool], results=[result])
    db.save_turn_messages("u","c",0,engine.log_rows(engine.user_blocks(0,{}, "x"),[exchange],"done"))
    legacy = db.conversation_log("u","c")
    assert loaded_skills(legacy, None) == ["exchange"]
    for mutate in ("id", "tool", "input", "unclosed"):
        altered = deepcopy(legacy)
        if mutate == "id":
            altered[2]["content"][0]["tool_use_id"] = "wrong"
        elif mutate == "tool":
            altered[1]["content"][0]["name"] = "other"
        elif mutate == "input":
            altered[1]["content"][0]["input"]["name"] = "not_exchange"
        else:
            altered.pop()
        assert loaded_skills(altered, None) == []
    batch = claimed_batch(db,[tool])
    batch.step(0,"started")
    batch.step(0,"returned",result)
    bound = db.bound_user_id("u","c")
    def journal():
        return [r for r in db.conversation_log("u","c") if r["turn"] == 1]
    assert loaded_skills(journal(),bound) == []
    db.save_turn_messages("u","c",1,engine.log_rows(engine.user_blocks(1,{"address":"PRIVATE_ADDRESS"}, "private"), [],"FINAL_PRIVATE"))
    assert loaded_skills(journal(),bound) == ["exchange"] and loaded_skills(journal(),"elsewhere") == []
    db.save_context_state("u","c",{"summary":{"text":"PRIVATE_OLD_SUMMARY","upto_turn":1}})
    assert "PRIVATE" not in json.dumps(engine.history("u","c",[]))
    before = batches(db,1)
    asyncio.run(engine.prepare("u","c",2))
    assert batches(db,1) == before


@pytest.mark.parametrize("mode,lost", [("m",True), ("j",False)])
def test_cut_after_tool_before_second_model_call(db,mode,lost):
    class Interrupted(Model):
        async def create(self, **kwargs):
            content = kwargs["messages"][-1]["content"]
            if isinstance(content,list) and content[-1]["type"] == "tool_result":
                content[-1]["content"] = "MODEL_ONLY"
                raise PowerLoss()
            return await super().create(**kwargs)
    with pytest.raises(PowerLoss):
        run_service(db,"legacy",mode,READ,READ[0]["quote"],model=Interrupted(READ))
    saved = batches(db,1)
    assert len(saved) == int(not lost) and "MODEL_ONLY" not in json.dumps(saved)
    if saved:
        result = saved[0]["calls"][0]["result"]
        assert json.loads(result["content"])["item"]["item_id"] == "2652637226"
    assert all(row["kind"] == KIND for row in db.conversation_log("u","c"))


def test_journal_failure_prevents_handler_dispatch(db, monkeypatch):
    import sqlite3
    from core.dag_tool_log import JournalConflict
    def refuse(*args, **kwargs):
        raise sqlite3.OperationalError("injected writer failure")
    def never(*args):
        pytest.fail("an unregistered tool reached the handler")
    monkeypatch.setattr(db, "record_tool_step", refuse)
    monkeypatch.setattr(db, "get_item_details", never)
    with pytest.raises(JournalConflict):
        run_service(db,"legacy","j",READ,READ[0]["quote"])
    assert not batches(db,1)
    state, = db.open_dags("u","c")
    assert state.runs["read"].status.value == "running"


def test_journal_does_not_require_identity_for_public_catalog(tmp_path):
    store = RetailStore(tmp_path / "anon.sqlite", SEED_PATH, language="en")
    result, _ = run_service(store, "legacy", "j", READ, READ[0]["quote"])
    assert result.success and batches(store,1)[0]["bound"] is None


def test_same_key_different_originals_on_m_close_is_a_conflict(db):
    from core.dag_tool_log import JournalConflict
    run_service(db,"legacy","m",READ,READ[0]["quote"])
    rows = db.conversation_log("u","c")
    db.save_turn_messages("u","c",1,rows)
    before = db.conversation_log("u","c")
    rows[1]["content"][0]["calls"][0]["result"]["content"] = "changed"
    with pytest.raises(JournalConflict):
        db.save_turn_messages("u","c",1,rows)
    assert db.conversation_log("u","c") == before


@pytest.mark.parametrize("workflow", ["legacy", "langgraph"])
def test_four_visible_turns_and_shared_summary_survive_journal_compaction(db, workflow):
    for n in range(1,6):
        node = dict(READ[0], quote=f"Request {n}: color of 2652637226")
        assert run_service(db, workflow, "j", [node], node["quote"])[0].success
    old = [r for r in db.conversation_log("u","c") if r["kind"] == KIND]
    sources = []
    def summarize(text, cutoff):
        assert cutoff == 1
        sources.append(text)
        return "Asked about the variant."
    engine = ContextEngine(db,summarizer=summarize)
    engine.budget_tokens = 1
    result, model = run_service(db,workflow,"j",READ,READ[0]["quote"],engine=engine)
    assert result.success and len(sources) == 1 and "Request 1" in sources[0]
    assert all(s not in sources[0] for s in ["tool_result", "tool_use", "raw_result", "Request 2"])
    planner = json.loads(model.planner_requests[0]["messages"][0]["content"])
    agent = json.loads(model.requests[0]["messages"][0]["content"][0]["text"])
    assert agent["history_summary"] == planner["history_summary"] == "Asked about the variant."
    assert [t["turn"] for t in agent["recent_conversation"]] == [2,3,4,5]
    assert agent["recent_conversation"] == planner["recent_conversation"]
    assert [r for r in db.conversation_log("u","c") if r["kind"] == KIND and r["turn"] <= 5] == old


def test_mismatched_guarded_reply_is_rejected_atomically(db):
    from core.dag_tool_log import JournalConflict
    batch = claimed_batch(db,[use("id")])
    batch.step(0,"started")
    engine = ContextEngine(db)
    rows = engine.log_rows(engine.user_blocks(1, {}, "user"), [], "draft")
    with pytest.raises(JournalConflict):
        db.finish_dialogue("u","c",1,"user","Guarded",rows=rows)
    assert not db.last_visible_turn("u","c") and len(db.conversation_log("u","c")) == 1


def test_process_loss_between_two_real_tool_calls(db, monkeypatch):
    from evaluation.harness.dag_script import response
    class TwoCalls(Model):
        async def create(self, **kwargs):
            if "tools" not in kwargs:
                return await super().create(**kwargs)
            reply = response(use("first"))
            reply.content.append(use("second"))
            return reply
    executed = []
    read = db.get_item_details
    def counted(*args):
        executed.append(args)
        return read(*args)
    monkeypatch.setattr(db,"get_item_details",counted)
    record = db.record_tool_step
    def cut(client, conv, batch, index, action, result=None):
        record(client, conv, batch, index, action, result)
        if index == 1 and action == "started":
            raise PowerLoss()
    monkeypatch.setattr(db, "record_tool_step", cut)
    with pytest.raises(PowerLoss):
        run_service(db,"legacy","j",READ,READ[0]["quote"],model=TwoCalls(READ))
    assert len(executed) == 1
    first, second = batches(db,1)[0]["calls"]
    assert first["result"]["tool_use_id"] == "first"
    assert second["receipt"] == "raw_result_missing" and "result" not in second


@pytest.mark.parametrize("failure", ["denied","returned","transport"])
def test_journal_distinguishes_rejection_failure_and_missing_receipt(db, monkeypatch, failure):
    from evaluation.harness.dag_script import response
    class OneCall(Model):
        async def create(self, **kwargs):
            if "tools" not in kwargs or kwargs["messages"][-1]["content"][-1]["type"] == "tool_result":
                return await super().create(**kwargs)
            block = use("call")
            if failure == "denied":
                block["name"] = "exchange_delivered_order_items"
            return response(block)
    def handler(*args):
        if failure == "denied":
            pytest.fail("denied tool was executed")
        if failure == "transport":
            raise OSError("injected loss")
        return {"success":False, "error":"source refused read"}
    monkeypatch.setattr(db,"get_item_details",handler)
    run_service(db,"legacy","j",READ,READ[0]["quote"],model=OneCall(READ))
    call, = batches(db,1)[0]["calls"]
    assert call["outcome"] == {"returned":"business_failure", "transport":"unknown_result","denied":"not_executed"}[failure]
    assert ("result" in call) == (failure == "returned")
