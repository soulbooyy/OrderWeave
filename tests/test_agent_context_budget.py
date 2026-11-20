"""Request-window tests use an offline transport with a known wire-token contract."""
import asyncio
from copy import deepcopy
import json
from hashlib import sha256
from types import SimpleNamespace

import pytest

from core.agent_context_budget import ContextLimit, ModelProfile, ModelRequestBudgetGuard
from evaluation.retail_budget import request_input_upper_tokens
from evaluation.retail_ledger import BudgetedClient
from tests.test_dag_tool_log import READ, SWAP, batches, db as phase2_store, run_service
from tests.test_dag_task_context import Model, remember
from evaluation.harness.config import POLICY_PATH, SEED_PATH
from evaluation.harness.dag_script import response


@pytest.fixture
def db(request):
    if not SEED_PATH.is_file():
        pytest.skip("requires frozen public retail seed")
    return request.getfixturevalue("phase2_store")


def request():
    return dict(model="offline-v1", system="中文", max_tokens=16,
                messages=[dict(role="user", content="你好")],
                tools=[dict(name="lookup", description="查找", input_schema={"type": "object"})])


class Ledger:
    def __init__(self):
        self.calls = []

    def consume_turn_request(self, stage):
        self.calls.append(("quota", stage))

    def reserve(self, stage, kwargs):
        self.calls.append(("reserve", stage))

    def settle_success(self, *args):
        return 0


class Spy:
    def __init__(self):
        self.requests = []
        self.messages = self

    async def create(self, **kwargs):
        self.requests.append(deepcopy(kwargs))
        return SimpleNamespace(content=[], usage=SimpleNamespace(input_tokens=20, output_tokens=1,
                               cache_read_input_tokens=3, cache_creation_input_tokens=2))


def client(mode, window, *, thinking=None, count=request_input_upper_tokens, inner=None):
    events = []
    profile = ModelProfile("offline-v1", window, 7, "offline-wire", count)
    guard = ModelRequestBudgetGuard(mode, {profile.model: profile}, report=events.append)
    inner = inner or Spy()
    ledger = Ledger()
    wrapper = BudgetedClient(inner, ledger, "agent", True, thinking, guard)
    return wrapper, inner, ledger, events


def test_observe_records_final_request_without_changing_it():
    req = request()
    original = deepcopy(req)
    wrapper, spy, ledger, events = client("observe", 100, thinking={"type": "disabled"})
    asyncio.run(wrapper.for_stage("plan").messages.create(**req))
    sent, = spy.requests
    assert sent == {**original, "extra_body": {"thinking": {"type": "disabled"}}}
    assert req == original and ledger.calls == [("quota", "plan"), ("reserve", "plan")]
    decision, usage = events
    assert decision["stage"] == "plan" and decision["reason"] == "window_exceeded"
    assert decision["input"] == request_input_upper_tokens(sent) and decision["action"] == "would_stop"
    assert usage["actual_input"] == 25 and usage["difference"] == decision["input"] - 25
    assert "你好" not in json.dumps(events, ensure_ascii=False)


def test_off_skips_counter():
    def forbidden(_):
        pytest.fail("off called the counter")
    wrapper, spy, _, events = client("off", 100, count=forbidden)
    asyncio.run(wrapper.messages.create(**request()))
    assert spy.requests == [request()] and events == []


def test_local_observe_proposes_trim_without_mutating_nested_blocks():
    from agents.agent_loop import LoopContext
    from agents.retail_hooks import AgentContextBudgetHook
    wrapper, _, _, events = client("observe", 1400)
    ctx = LoopContext(req=SimpleNamespace(dag_assignment={"kind": "product_info", "target": "42"},
                          conversation_context=object(), retail_pending=[]),
                      agent=SimpleNamespace(_client=wrapper), state={})
    req = request()
    req["messages"][0]["content"] = [
        dict(type="text", text=json.dumps({"recent_conversation": ["old" * 500]})),
        dict(type="text", text="current assignment with predecessors")]
    original = deepcopy(req)
    AgentContextBudgetHook().pre_model(ctx, req)
    assert req == original and events[-1]["action"] == "would_trim"
    assert events[-1]["before"] > events[-1]["input"]


@pytest.mark.parametrize("over", [0, 1])
def test_exact_complete_window_and_one_over(over):
    req = request()
    thinking = {"type": "enabled", "budget_tokens": 8}
    actual = {**req, "extra_body": {"thinking": thinking}}
    wrapper, spy, ledger, events = client("enforce", request_input_upper_tokens(actual) + 16 + 7 - over,
                                          thinking=thinking)
    if over:
        with pytest.raises(ContextLimit, match="window_exceeded"):
            asyncio.run(wrapper.messages.create(**req))
        assert not spy.requests and not ledger.calls
    else:
        asyncio.run(wrapper.messages.create(**req))
        assert spy.requests == [actual] and events[0]["output"] == 16


@pytest.mark.parametrize("field", ["system", "tools"])
def test_large_fixed_overhead_is_never_cut(field):
    req = request()
    wrapper, spy, ledger, _ = client("enforce", request_input_upper_tokens(req) + 23)
    if field == "system":
        req["system"] += "!"
    else:
        req["tools"][0]["input_schema"]["description"] = "!"
    with pytest.raises(ContextLimit):
        asyncio.run(wrapper.messages.create(**req))
    assert not ledger.calls and not spy.requests


@pytest.mark.parametrize("alias", ["deepseek-flash", "unknown"])
def test_unverified_model_fails_closed(alias):
    guard = ModelRequestBudgetGuard.for_endpoint("enforce", "https://api.deepseek.com/anthropic")
    with pytest.raises(ContextLimit, match="unknown_model_window|unverified_counter"):
        guard.final("intent", {**request(), "model": alias})


@pytest.mark.parametrize("identifiers", [("a",), ("a","a"), ("b","a"), ("a","c")])
def test_incomplete_or_mismatched_parallel_round_is_rejected(identifiers):
    req = request()
    req["messages"] += [
        dict(role="assistant", content=[dict(type="tool_use", id=k, name="lookup", input={}) for k in ("a","b")]),
        dict(role="user", content=[dict(type="tool_result", tool_use_id=k, content="ok") for k in identifiers])]
    wrapper, spy, _, _ = client("enforce", 10000)
    with pytest.raises(ContextLimit, match="invalid_tool_protocol"):
        asyncio.run(wrapper.messages.create(**req))
    assert not spy.requests


def retail_client(inner, mode, window=40000, count=request_input_upper_tokens):
    profile = ModelProfile("offline", window, 128, "fixture-v1", count)
    events = []
    guard = ModelRequestBudgetGuard(mode, {"offline": profile}, events.append)
    return BudgetedClient(inner, Ledger(), "roles", context_guard=guard), events


class FourCalls(Model):
    async def create(self, **kwargs):
        if "tools" not in kwargs:
            return await super().create(**kwargs)
        self.requests.append(deepcopy(kwargs))
        index = len(self.requests)
        if index == 4:
            return response(dict(type="text", text="Checked."))
        def call(ident):
            return dict(type="tool_use", id=ident, name="get_item_details", input={"item_id": READ[0]["target"]})
        result = response(call(f"step-{index}"))
        if index == 2:
            result.content.append(SimpleNamespace(**call("parallel")))
        return result


@pytest.mark.parametrize("workflow", ["legacy", "langgraph"])
@pytest.mark.parametrize("mode", ["observe", "enforce"])
def test_four_calls_real_journal_and_scoped_prefix(db, monkeypatch, workflow, mode):
    role = "technical"
    owner = db.bound_user_id("u", "c")
    scope = [role, "product_info", READ[0]["target"]]
    remember(db, owner, "old", audience=scope)
    remember(db, owner, "earlier", audience=scope)
    summary = {"upto_turn": 1, "text": "persistent anchor", "scope": {
        "format": "visible-v1", "bound": owner, "since": 0, "audience": scope}}
    db.save_context_state("u", "c", {"summary": deepcopy(summary)})
    original_get = db.get_item_details
    originals = []
    def lookup(*args):
        result = original_get(*args)
        result["extra_diagnostics"] = "large" * (15000 if len(originals) == 1 else 1)
        originals.append(deepcopy(result))
        return result
    monkeypatch.setattr(db, "get_item_details", lookup)
    inner = FourCalls(READ)
    wrapper, events = retail_client(inner, mode)
    result, _ = run_service(db, workflow, "j", READ, READ[0]["quote"], model=wrapper)
    assert result.success and result.response_guard["version"]
    assert len(inner.requests) == 4
    first, second, third, fourth = [r["messages"] for r in inner.requests]
    assert second[:len(first)] == first
    assert fourth[:-2] == third
    assert [b["id"] for b in fourth[-2]["content"]] == ["step-3"]
    assert [b["tool_use_id"] for b in fourth[-1]["content"]] == ["step-3"]
    assert [b["tool_use_id"] for b in third[-1]["content"]] == ["step-2", "parallel"]
    assert len(originals) == 4
    stored = [call["result"]["content"] for b in batches(db, 3) for call in b["calls"]]
    assert [sha256(s.encode()).digest() for s in stored] == [
        sha256(json.dumps(value, ensure_ascii=False).encode()).digest() for value in originals]
    if workflow == "langgraph":
        from agents.retail_workflow import RetailWorkflow
        from tests.test_dag_context_baseline import assert_checkpoint_scope
        async def check_saved_states():
            graph = RetailWorkflow(SimpleNamespace(store=db))
            try:
                await graph.start()
                states = [s async for s in graph._graph.aget_state_history(graph.config("u", "c"))]
                assert_checkpoint_scope(states, result.semantic_routing["dag"]["plan_id"])
                for state in states:
                    saved = json.dumps(state.values)
                    assert all(label not in saved for label in ("visible_prefix", "raw_cursor", "large" * 20))
            finally:
                await graph.close()
        asyncio.run(check_saved_states())
    assert [t["result"] for t in result.tool_traces] == originals
    assert db.context_state("u", "c") == {"summary": summary}
    large = originals[1]["extra_diagnostics"]
    assert (large in json.dumps(third)) == (mode == "observe")
    assert (large in json.dumps(fourth)) == (mode == "observe")
    assert third[0]["content"][-1] == first[0]["content"][-1]
    checks = [e for e in events if e.get("layer") == "role"]
    assert len(checks) == 4 and checks[0]["actions"] == checks[1]["actions"] == []
    assert checks[2]["actions"][0] == "omit_summary" and "project_read_result" in checks[2]["actions"]
    assert checks[2]["action"] == ("would_trim" if mode == "observe" else "trim")
    if mode == "enforce":
        assert checks[3]["actions"] == []
        assert all(e["input"] + e["output"] + e["margin"] <= e["window"] for e in events if e.get("layer") == "final")


@pytest.mark.parametrize("workflow", ["legacy", "langgraph"])
@pytest.mark.parametrize("boundary", ["first", "prepared", "committed", "unknown", "lost_after_commit"])
def test_context_stop_at_write_boundaries(db, monkeypatch, workflow, boundary):
    from agents.retail_hooks import ConfirmationStopHook
    if boundary in {"committed", "unknown", "lost_after_commit"}:
        run_service(db, workflow, "j", SWAP, SWAP[0]["quote"])
    if boundary == "prepared":
        # Force a further model call after prepare, normally short-circuited by this hook.
        monkeypatch.setattr(ConfirmationStopHook, "stop_response", lambda *_: None)
    submitted = []
    original = db.write_action
    def write(*args, **kwargs):
        submitted.append(kwargs.get("expected_op_id"))
        if boundary == "unknown":
            raise OSError("connection lost")
        value = original(*args, **kwargs)
        if boundary == "lost_after_commit":
            raise OSError("response lost")
        return value
    monkeypatch.setattr(db, "write_action", write)
    def count(req):
        if "tools" in req and (boundary == "first" or len(req["messages"]) > 1):
            return 100000
        return request_input_upper_tokens(req)
    model = Model(SWAP)
    wrapper, _ = retail_client(model, "enforce", count=count)
    message = SWAP[0]["quote"] if boundary in {"prepared", "first"} else "Yes, I confirm."
    result, _ = run_service(db, workflow, "j", SWAP, message, model=wrapper)
    assert result.response_guard["version"]
    assert result.semantic_routing["context_limit"]["reason"] == "window_exceeded"
    dag = result.semantic_routing["dag"]
    assert len(submitted) == len(model.requests) == int(boundary != "first")
    executed = [e for e in db.list_events("u","c") if e["event_type"] == db.executed_event("exchange")]
    if boundary == "prepared":
        pending, = db.active_pending("u", "c")
        assert pending["status"] == "displayed" and pending["display_text"] in result.response
        assert dag["stop"] == "confirmation" and not executed
    elif boundary in {"committed", "lost_after_commit"}:
        assert len(executed) == 1 and dag["tasks"][0]["refs"] == [f"event:{executed[0]['event_id']}"]
    elif boundary == "unknown":
        assert dag["stop"] == "unknown_result" and not executed
        again, next_model = run_service(db, workflow, "j", SWAP, "continue")
        assert again.semantic_routing["dag"]["stop"] == "unknown_result" and not next_model.requests
        assert len(submitted) == 1
    else:
        assert dag["stop"] == "context_limit" and dag["tasks"][0]["reason"] == "context_limit"
        assert not batches(db, 1) and not executed


@pytest.mark.parametrize("stage", ["intent", "correction", "plan", "confirm", "extract", "compact",
                                   "memory", "rewrite", "rerank", "composer"])
def test_real_stage_entry_cannot_dispatch_over_window(stage):
    from agents.agent_orchestrator import AgentResponse, AgentType, Request, ResponseComposer
    from agents.retail_confirmation import classify_confirmations
    from agents.retail_plan import extract as plan
    from core.context_engine import make_llm_summarizer
    from core.intent_recognizer import IntentRecognizer
    from mcp_tools.tool_manager import MCPToolManager
    from memory.conversation_memory import MemoryManager
    from memory.task_facts import extract

    model = "offline-v1"
    name = {"correction": "intent", "rewrite": "retrieval", "rerank": "retrieval"}.get(stage, stage)
    def count(req):
        return 1 if stage == "correction" and len(req["messages"]) == 1 else 50000
    wrapper, spy, ledger, events = client("enforce", 20000, count=count)
    bound = wrapper.for_stage(name)
    if stage in {"intent", "correction"}:
        recognizer = object.__new__(IntentRecognizer)
        recognizer.client, recognizer.model = bound, model
        recognizer.retail_mode, recognizer.routing_mode = True, "dag"
        call = recognizer._llm_recognize("check my order", None)
    elif stage == "confirm":
        call = classify_confirmations(bound, model, "Yes", "Displayed", [{"op_id": "op-current"}])
    elif stage == "plan":
        call = plan(bound, model, {}, "Previous", "Current")
    elif stage == "extract":
        call = extract(bound, model, [], "Previous", "Current")
    elif stage == "compact":
        call = make_llm_summarizer(bound, model)("transcript", 3)
    elif stage == "memory":
        manager = object.__new__(MemoryManager)
        manager._model, manager._client = model, bound
        call = manager._merge_summary("old", "new")
    elif stage in {"rewrite", "rerank"}:
        manager = object.__new__(MCPToolManager)
        manager._model, manager._client, manager._reranker = model, bound, None
        call = (manager.rewrite_query("query") if stage == "rewrite" else
                manager._rerank("query", [{"content": "a"}, {"content": "b"}], top_k=1))
    else:
        agent = ResponseComposer(bound, model, retail_mode=True)
        responses = [AgentResponse(role, "found", True) for role in (AgentType.GENERAL, AgentType.TECHNICAL)]
        call = agent.compose_result(Request("message", "u", "c"), responses)
    with pytest.raises(ContextLimit) as stopped:
        asyncio.run(call)
    assert stopped.value.decision["stage"] == name
    assert len(spy.requests) == int(stage == "correction")
    assert len(ledger.calls) == len(spy.requests) * 2
    assert events[-1]["layer"] == "final"


@pytest.mark.parametrize("which", ["quota", "fee"])
def test_other_stops_preserve_their_type(which):
    from evaluation.retail_budget import BudgetStopExceeded, TurnRequestLimitExceeded
    wrapper, spy, ledger, events = client("enforce", 10000)
    kind = TurnRequestLimitExceeded if which == "quota" else BudgetStopExceeded
    def refuse(*args):
        raise kind()
    if which == "quota":
        ledger.consume_turn_request = refuse
    else:
        ledger.reserve = refuse
    with pytest.raises(kind):
        asyncio.run(wrapper.messages.create(**request()))
    assert not spy.requests and events[-1]["action"] == "allow"


@pytest.mark.skipif(not (SEED_PATH.is_file() and POLICY_PATH.is_file()), reason="requires frozen public retail assets")
def test_runtime_guard_reaches_real_transport_constructor(tmp_path, monkeypatch):
    from dataclasses import replace
    from agents import retail_runtime as runtime
    for key, value in {"ANTHROPIC_API_KEY":"offline", "LLM_MODEL":"offline-v1", "RETAIL_INPUT_CNY_PER_MTOK":"1",
                       "RETAIL_OUTPUT_CNY_PER_MTOK":"1", "RETAIL_MODEL_CONTEXT_MODE":"observe"}.items():
        monkeypatch.setenv(key, value)
    config = replace(runtime.RetailRuntimeConfig.from_env(), seed_path=SEED_PATH, policy_path=POLICY_PATH,
                     support_enabled=False, enable_knowledge=False,
                     enable_memory=False, ledger_path=tmp_path / "ledger.sqlite")
    guard = ModelRequestBudgetGuard("enforce")
    class ReachedTransport(Exception):
        pass
    def build(**kwargs):
        assert kwargs["context_guard"] is guard
        raise ReachedTransport
    monkeypatch.setattr(runtime, "build_budgeted_client", build)
    assert config.model_context_mode == "observe"
    with pytest.raises(ReachedTransport):
        runtime.build_retail_runtime(config, context_guard=guard)


@pytest.mark.parametrize("can_resolve", [False, True])
def test_old_reference_requires_matching_live_claim_and_keeps_current_result(can_resolve):
    from agents.agent_loop import LoopContext
    from agents.retail_hooks import AgentContextBudgetHook
    # Two reads return the same long, required policy text. The later hit must stay visible.
    assignment = dict(id="policy", kind="general_policy", output="policy", quote="policy?", target="p1")
    text = "Policy terms " * 300
    def pair(ident):
        return [dict(role="assistant", content=[dict(type="tool_use", id=ident, name="search_knowledge_base",
                    input={"query":"policy?"})]), dict(role="user", content=[dict(type="tool_result", tool_use_id=ident,
                    content=json.dumps({"success":True, "results":[{"evidence_id":"p1", "content":text}],
                                        "extra":"x" * 3000}))])]
    exchanges = pair("older") + pair("current")
    ticket = ("plan-id", "policy", 1, 3)
    def resolve(user, conv, claim, role, tools):
        assert (user, conv, claim, role, tools) == ("u", "c", ticket, "general", {"search_knowledge_base"})
        return deepcopy(exchanges) if can_resolve else []
    req = SimpleNamespace(dag_assignment=assignment, dag_tool_limit={"search_knowledge_base"}, retail_user_id=None,
                          dag_log_mode="j", retail_pending=[], user_id="u", conv_id="c", dag_ticket=ticket,
                          retail_store=SimpleNamespace(task_tool_messages=resolve))
    payload = request()
    payload["tools"][0]["name"] = "search_knowledge_base"
    payload["messages"] += deepcopy(exchanges)
    original = deepcopy(payload)
    wrapper, _, _, events = client("enforce", 6500)
    ctx = LoopContext(req, SimpleNamespace(_client=wrapper, agent_type=SimpleNamespace(value="general")), {})
    if not can_resolve:
        with pytest.raises(ContextLimit):
            AgentContextBudgetHook().pre_model(ctx, payload)
        assert payload == original
    else:
        AgentContextBudgetHook().pre_model(ctx, payload)
        older = json.loads(payload["messages"][2]["content"][0]["content"])
        current = json.loads(payload["messages"][4]["content"][0]["content"])
        assert older["ref"] == "tool:older" and current["facts"]["policy"] == text
        assert events[-1]["actions"] == ["project_read_result", "project_read_result", "reference_old_read"]
        assert exchanges == original["messages"][1:]


def test_unprojectable_current_read_stops_without_fabricating_a_receipt(db, monkeypatch):
    def unfamiliar(*args):
        return {"success":True, "warning":"x" * 100000}
    monkeypatch.setattr(db, "get_item_details", unfamiliar)
    inner = Model(READ)
    wrapper, _ = retail_client(inner, "enforce")
    result, _ = run_service(db, "legacy", "j", READ, READ[0]["quote"], model=wrapper)
    assert not result.success and len(inner.requests) == 1
    assert result.semantic_routing["dag"]["stop"] == "context_limit"
    saved, = batches(db, 1)
    assert json.loads(saved["calls"][0]["result"]["content"]) == unfamiliar()
    assert not result.semantic_routing["dag"]["tasks"][0]["refs"]


def test_disproved_profile_stops_subsequent_stages():
    wrapper, spy, _, events = client("enforce", 2000, count=lambda _: 10)
    asyncio.run(wrapper.messages.create(**request()))
    assert events[-1]["difference"] == -15 and events[-1]["invalidated"]
    with pytest.raises(ContextLimit, match="unverified_counter"):
        asyncio.run(wrapper.for_stage("confirm").messages.create(**request()))
    assert len(spy.requests) == 1


@pytest.mark.parametrize("previous", ["finished", "stopped"])
def test_new_attempt_on_same_client_uses_its_own_full_context(db, monkeypatch, previous):
    inner = Model(READ)
    wrapper, _ = retail_client(inner, "enforce")
    original = db.get_item_details
    def lookup(*args):
        value = original(*args)
        if previous == "stopped":
            value.pop("item", None)
        return {**value, "large":"x" * 100000}
    with monkeypatch.context() as patch:
        patch.setattr(db, "get_item_details", lookup)
        first, _ = run_service(db, "legacy", "j", READ, READ[0]["quote"], model=wrapper)
        assert first.success == (previous == "finished")
    first_count = len(inner.requests)
    message = "Please check color of variant 2652637226 again"
    inner.nodes = [{**READ[0], "quote":message, "id":"new"}]
    second, _ = run_service(db, "legacy", "j", inner.nodes, message, model=wrapper)
    assert second.success
    opening = inner.requests[first_count]["messages"]
    assert len(opening) == 1 and json.loads(opening[0]["content"][-1]["text"])["assignment"]["id"] == "new"
    assert "call-read" not in json.dumps(opening)


def test_full_initial_request_within_limit_is_identical_in_all_modes():
    from agents.agent_loop import LoopContext
    from agents.retail_hooks import AgentContextBudgetHook
    for mode in ("off", "observe", "enforce"):
        wrapper, spy, _, _ = client(mode, 10000)
        req = request()
        original = deepcopy(req)
        ctx = LoopContext(SimpleNamespace(dag_assignment=None), SimpleNamespace(_client=wrapper), {})
        AgentContextBudgetHook().pre_model(ctx, req)
        asyncio.run(wrapper.messages.create(**req))
        assert spy.requests == [original]


def test_local_projection_cannot_recalibrate_global_history(db):
    from core.context_engine import ContextEngine
    state = {"chars_per_token":2.5, "summary":{"text":"persisted", "upto_turn":2}}
    db.save_context_state("u", "c", state)
    ContextEngine(db).calibrate("u", "c", [dict(kind="model", local_projection=True,
                               request_chars=9999, usage={"input":100})])
    assert db.context_state("u", "c") == state
