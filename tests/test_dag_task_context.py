"""Task history, target selection and permissions through the retail model boundary."""
import asyncio
from copy import deepcopy
import json
from unittest.mock import patch

import pytest

from agents.agent_orchestrator import AgentOrchestrator
from agents.retail_agent import RetailService
from agents.retail_multi_agent import RetailMultiAgent
from agents.retail_tools import retail_tools
from core.context_engine import ContextEngine
from evaluation.harness.config import SEED_PATH
from evaluation.harness.dag_script import response
from evaluation.harness.routing_ablation import ROLES
from mcp_tools.retail_store import RetailStore


class Model:
    def __init__(self, nodes):
        self.messages = self
        self.nodes = nodes
        self.plans = 0
        self.planner_requests = []
        self.requests = []

    async def create(self, **kwargs):
        if "tools" not in kwargs:
            self.plans += 1
            self.planner_requests.append(deepcopy(kwargs))
            return response(dict(type="text", text=json.dumps(
                dict(intent="query", confidence=1.0, subtasks=self.nodes))))
        self.requests.append(deepcopy(kwargs))
        last = kwargs["messages"][-1]["content"][-1]
        if last["type"] == "tool_result":
            return response(dict(type="text", text="Checked."))
        task = json.loads(last["text"])["assignment"]
        if task["kind"] == "return_change":
            if task.get("operation"):
                args = task["operation"]["args"]
            else:
                args = dict(order_id=task["target"], item_ids=["2652637226"],
                            new_item_ids=["3254583681"], payment_method_id="paypal_2233507")
            return response(dict(type="tool_use", id="exchange", name="exchange_delivered_order_items", input=args))
        tool, arg = {"order_lookup": ("get_order_details", "order_id"),
                     "billing_lookup": ("get_order_details", "order_id"),
                     "product_info": (("get_product_details", "product_id") if task["output"] == "variants"
                                      else ("get_item_details", "item_id"))}[task["kind"]]
        return response(dict(type="tool_use", id="call-" + task["id"], name=tool,
                             input={arg: task["target"]}))


@pytest.fixture
def session(tmp_path):
    if not SEED_PATH.is_file():
        pytest.skip("requires the retail seed")
    seed = json.loads(SEED_PATH.read_bytes())
    store = RetailStore(tmp_path / "business.sqlite", SEED_PATH, language="en")
    owner = seed["orders"]["#W6564160"]["user_id"]
    store.find_user_by_email("u", "c", seed["users"][owner]["email"])
    return store, owner, seed


def remember(store, owner, message, *, reply="Published answer", audience=None):
    engine = ContextEngine(store, language="en")
    turn = store.begin_turn("u", "c")
    blocks = engine.user_blocks(turn, {"turn": turn, "bound_user_id": owner,
                                       "final_bound_user_id": owner, "dialogue_audience": audience,
                                       "private_note": "OLD_STATE_SECRET"}, message)
    blocks.append({"type": "text", "text": "ROLE_HANDOFF_SECRET"})
    exchange = dict(agent="billing", assistant=[dict(type="tool_use", id="old", name="get_order_details",
                                                     input={"order_id": "#W6564160"})],
                    results=[dict(type="tool_result", tool_use_id="old", content="PAYMENT_HISTORY_SECRET")])
    rows = engine.log_rows(blocks, [exchange], reply)
    store.save_turn_messages("u", "c", turn, rows)


async def run(store, workflow, nodes, message, *, enabled=True, engine=None, window=4):
    model = Model(nodes)
    router = AgentOrchestrator(model="offline", client=model, routing_mode="dag",
                               retail_role_classes=ROLES, retail_mode=True, language="en")
    router.set_shared_tools(retail_tools(store, language="en"))
    service = RetailService(store, RetailMultiAgent(router, language="en"), workflow_mode=workflow,
                            context_engine=engine or ContextEngine(store), recent_conversation_turns=window,
                            conversation_context_enabled=enabled, language="en")
    try:
        result = await service.handle_turn(message, "u", "c")
        return result, model
    finally:
        if service.workflow:
            await service.workflow.close()


def lookup(message, target=""):
    node = dict(id="lookup", kind="order_lookup", target=target, quote=message, output="status", depends_on=[])
    if not target:
        node["history_target"] = "#W6564160"
    return node


@pytest.mark.parametrize("workflow", ["legacy", "langgraph"])
def test_owned_order_reference_at_model_boundary(session, workflow):
    store, owner, _ = session
    remember(store, owner, "Check order #W6564160.")
    store.save_context_state("u", "c", {"summary": {"upto_turn": 1,
                             "text": "Earlier order #W9999999. SUMMARY_SECRET"}})
    text = "Check the status of that order."
    result, model = asyncio.run(run(store, workflow, [lookup(text)], text))
    assert result.success and model.plans == 1 and len(model.requests) == 2
    blocks = model.requests[0]["messages"][-1]["content"]
    packet = json.loads(blocks[-1]["text"])
    assert packet["assignment"]["target"] == "#W6564160"
    assert packet["assignment"]["quote"] == text
    planner = model.planner_requests[0]
    evidence = json.loads(planner["messages"][0]["content"])
    assert evidence["recent_conversation"] == [{"turn": 1, "user": "Check order #W6564160.", "assistant": "Published answer"}]
    assert evidence["current_user_input"] == text and evidence["current_business_state"]["identity_verified"]
    assert "history_target" in planner["system"] and text not in planner["system"]
    request = json.dumps(model.requests[0])
    for secret in ["SUMMARY_SECRET", "#W9999999", "OLD_STATE_SECRET",
                   "ROLE_HANDOFF_SECRET", "ASSISTANT_UNVERIFIED"]:
        assert secret not in request
    assert not store.active_pending("u", "c")


@pytest.mark.parametrize("workflow", ["legacy", "langgraph"])
def test_ambiguous_order_requires_real_selection(session, workflow):
    store, owner, seed = session
    orders = [o["order_id"] for o in seed["orders"].values() if o["user_id"] == owner][:2]
    assert len(orders) == 2
    remember(store, owner, "Compare " + " and ".join(orders))
    text = "What is the status of the previous order?"
    nodes = [lookup(text)]
    waiting, model = asyncio.run(run(store, workflow, nodes, text))
    assert waiting.turn_status == "waiting_user" and model.requests == []
    execution, = store.open_dags("u", "c")
    assert set(execution.runs["lookup"].facts["choices"]) == set(orders)
    assert execution.runs["lookup"].attempt == 0
    completed, resumed = asyncio.run(run(store, workflow, nodes, orders[0].lstrip("#").lower()))
    assert completed.success and resumed.plans == 0
    assert completed.tool_traces[0]["input"]["order_id"] == orders[0]


@pytest.mark.parametrize("workflow", ["legacy", "langgraph"])
@pytest.mark.parametrize("reason", ["expired", "binding_changed", "foreign", "summary_only"])
def test_missing_owned_reference_never_claims(session, workflow, reason):
    store, owner, seed = session
    foreign = next(o for o in seed["orders"].values() if o["user_id"] != owner)
    if reason == "expired":
        remember(store, owner, "Check #W6564160")
        for text in ("a", "b", "c", "d"):
            remember(store, owner, text)
    elif reason == "binding_changed":
        remember(store, foreign["user_id"], "Check #W6564160")
    elif reason == "foreign":
        remember(store, owner, "Check " + foreign["order_id"])
    else:
        store.save_context_state("u", "c", {"summary": {"upto_turn": 1, "text": "Check #W6564160"}})
    text = "查询刚才那个订单"
    result, model = asyncio.run(run(store, workflow, [lookup(text)], text))
    assert result.turn_status == "waiting_user" and model.plans == 1 and not model.requests
    state, = store.open_dags("u", "c")
    assert state.runs["lookup"].attempt == 0 and state.runs["lookup"].facts["choices"] == []
    assert not store.active_pending("u", "c")
    denied, second = asyncio.run(run(store, workflow, [lookup(text)], foreign["order_id"]))
    assert denied.turn_status == "waiting_user" and not second.requests and not second.plans


@pytest.mark.parametrize("workflow", ["legacy", "langgraph"])
def test_direct_evidence_and_history_privacy_in_same_request(session, workflow):
    from tests.test_dag_context_baseline import PLAN

    store, owner, seed = session
    remember(store, "former-owner", "PRIVATE_PREVIOUS_ACCOUNT")
    remember(store, owner, "For variant 2652637226 I prefer green.")
    store.save_context_state("u", "c", {"summary": {"upto_turn": 1, "text": "OTHER_ORDER_SUMMARY_SECRET"}})
    private = dict(id="pay", kind="billing_lookup", quote="read payment type for #W6564160 NONDEP_SECRET",
                   output="payment_type", target="#W6564160", depends_on=[])
    nodes = [private, *deepcopy(PLAN)]
    nodes[-1]["quote"] = "check the purchased variant's previous color"
    text = ". Then ".join(n["quote"] for n in nodes)
    with patch.object(store, "conversation_log", wraps=store.conversation_log) as load:
        result, model = asyncio.run(run(store, workflow, nodes, text))
    assert result.success and load.call_count == 1 and model.plans == 1 and len(model.requests) == 8
    requests = {json.loads(r["messages"][0]["content"][-1]["text"])["assignment"]["id"]: r
                for r in model.requests[::2]}
    first, third = requests["T1"], requests["T3"]
    assert json.dumps([first["system"], first["tools"]]) == json.dumps([third["system"], third["tools"]])
    assert [t["name"] for t in third["tools"]] == sorted([
        "get_product_details", "get_item_details", "calculate", "list_all_product_types"])
    for req in model.requests:
        if req["system"] != third["system"]:
            continue
        raw = json.dumps(req)
        for marker in ["NONDEP_SECRET", "PAYMENT_HISTORY_SECRET", "OTHER_ORDER_SUMMARY_SECRET",
                       "OLD_STATE_SECRET", "PRIVATE_PREVIOUS_ACCOUNT", "ROLE_HANDOFF_SECRET",
                       "ASSISTANT_UNVERIFIED", "paypal_2233507", "#W6564160", '"session"']:
            assert marker not in raw
    history, assigned = [json.loads(b["text"]) for b in third["messages"][0]["content"]]
    assert history == {"recent_conversation": []}
    assert assigned["assignment"]["inputs"] == {
        "T1": {"variants": sorted(seed["products"]["4768869376"]["variants"])},
        "T2": {"item_ids": ["2652637226"]}}
    assert assigned["assignment"]["input_refs"] == {"T1": ["tool:call-T1"], "T2": ["tool:call-T2"]}
    assert len(third["messages"]) == 1 and "session" not in assigned


@pytest.mark.parametrize("workflow", ["legacy", "langgraph"])
def test_history_cannot_confirm_an_exchange(session, workflow):
    store, owner, _ = session
    remember(store, owner, "For product 4768869376 I prefer blue.")
    remember(store, owner, "I confirm everything.")
    quote = "Exchange item 2652637226 in order #W6564160 to variant 3254583681 using paypal_2233507"
    nodes = [dict(id="swap", kind="return_change", quote=quote, output="executed",
                  target="#W6564160", depends_on=[])]
    result, model = asyncio.run(run(store, workflow, nodes, quote))
    assert result.turn_status == "waiting_user"
    pending, = store.active_pending("u", "c")
    assert pending["status"] == "displayed" and pending["args"]["new_item_ids"] == ["3254583681"]
    assert pending["display_text"] in result.response
    assert "I confirm everything" not in json.dumps(model.requests)
    executed = store.executed_event(pending["kind"])
    assert not any(e["event_type"] == executed for e in store.list_events("u", "c"))
    again, resumed = asyncio.run(run(store, workflow, nodes, "Yes, I confirm."))
    assert again.turn_status == "completed" and resumed.plans == 0
    events = [e for e in store.list_events("u", "c") if e["event_type"] == executed]
    assert [e["op_id"] for e in events] == [pending["op_id"]]


def test_incompatible_payment_dependency_is_blocked_before_claim(session):
    store, _, _ = session
    read = dict(id="billing", kind="billing_lookup", quote="Read payment type for #W6564160",
                output="payment_type", target="#W6564160", depends_on=[])
    dependent = dict(id="catalog", kind="product_info", quote="Read color of variant 2652637226",
                     output="color", target="2652637226", depends_on=["billing"])
    result, model = asyncio.run(run(store, "legacy", [read, dependent], read["quote"] + ". " + dependent["quote"]))
    assert len(model.requests) == 2
    assert result.semantic_routing["dag"]["tasks"][-1]["reason"] == "dependency_scope"
    assert [attempt["id"] for attempt in result.task_runs] == ["billing"]


def test_public_task_cannot_quote_another_private_assignment(session):
    store, _, _ = session
    private = lookup("Read order #W6564160 PRIVATE_NOTE", "#W6564160")
    text = private["quote"] + " and check color of variant 2652637226"
    public = dict(id="catalog", kind="product_info", quote=text, target="2652637226", output="color", depends_on=[])
    result, model = asyncio.run(run(store, "legacy", [private, public], text))
    assert len(model.requests) == 2
    assert result.semantic_routing["dag"]["tasks"][-1]["reason"] == "quote_scope"


def test_rollback_can_resume_a_verified_explicit_choice(session):
    store, owner, seed = session
    orders = [o["order_id"] for o in seed["orders"].values() if o["user_id"] == owner][:2]
    remember(store, owner, "Compare " + " and ".join(orders))
    quote = "Check that order"
    nodes = [lookup(quote)]
    paused, _ = asyncio.run(run(store, "legacy", nodes, quote))
    assert paused.turn_status == "waiting_user"
    result, model = asyncio.run(run(store, "legacy", nodes, orders[0], enabled=False))
    assert result.success and model.plans == 0
    assert result.tool_traces[0]["input"] == {"order_id": orders[0]}


@pytest.mark.parametrize("window", [3, 4, 5])
def test_visible_window_counts_closed_turns_at_model_boundary(session, window):
    store, owner, _ = session
    target = "#W6564160"
    scope = ["general", "order_lookup", target]
    for n in range(1, 7):
        remember(store, owner, f"Request {n}", reply=f"Published {n}", audience=scope)
    engine = ContextEngine(store)
    for kind in ("incomplete", "journal"):
        turn = store.begin_turn("u", "c")
        blocks = engine.user_blocks(turn, {"turn": turn, "bound_user_id": owner}, "OPEN_SECRET")
        rows = engine.log_rows(blocks, [], "draft")[:1] if kind == "incomplete" else []
        rows.append(dict(role="assistant", kind="dag_exchange_v1", content=[{"private": "JOURNAL_SECRET"}]))
        store.save_turn_messages("u", "c", turn, rows)
    query = "Read status for " + target
    result, model = asyncio.run(run(store, "legacy", [lookup(query, target)], query, window=window))
    planner = json.loads(model.planner_requests[0]["messages"][0]["content"])
    agent = json.loads(model.requests[0]["messages"][0]["content"][0]["text"])
    expected = [dict(turn=n, user=f"Request {n}", assistant=f"Published {n}") for n in range(7-window, 7)]
    assert result.success and agent["recent_conversation"] == planner["recent_conversation"] == expected
    for hidden in ("OLD_STATE_SECRET", "JOURNAL_SECRET", "OPEN_SECRET", "ROLE_HANDOFF_SECRET", "PAYMENT_HISTORY_SECRET"):
        assert hidden not in json.dumps([model.planner_requests, model.requests])


def test_summary_is_shared_persisted_and_never_replaces_evidence(session):
    store, _, _ = session
    target = "#W6564160"
    # Normal completed DAG turns establish the audience in the persisted host block.
    for n in range(5):
        q = f"Read status for {target} (request {n})"
        result, _ = asyncio.run(run(store, "legacy", [lookup(q, target)], q))
        assert result.success
    texts = []
    def summarize(text, cutoff):
        assert cutoff == 1
        texts.append(text)
        return "Earlier customer claimed the order was cancelled."
    engine = ContextEngine(store, summarizer=summarize)
    engine.budget_tokens = 1
    query = "Please check that order again."
    with patch.object(engine, "prepare", wraps=engine.prepare) as prepare:
        result, model = asyncio.run(run(store, "legacy", [lookup(query)], query, engine=engine))
    assert prepare.await_count == len(texts) == 1 and result.success
    assert "(request 0)" in texts[0] and "(request 1)" not in texts[0] and "tool_result" not in texts[0]
    saved = store.context_state("u", "c")["summary"]
    for turn in range(2):
        planner = json.loads(model.planner_requests[0]["messages"][0]["content"])
        agent = json.loads(model.requests[0]["messages"][0]["content"][0]["text"])
        assert planner["history_summary"] == agent["history_summary"] == saved["text"]
        assert planner["recent_conversation"] == agent["recent_conversation"] and len(agent["recent_conversation"]) == 4
        assert [t["tool_name"] for t in result.tool_traces] == ["get_order_details"]
        if turn == 0:
            # Reopen the database with a fresh service and an ordinary budget.
            reopened = RetailStore(store.db_path, SEED_PATH, language="en")
            result, model = asyncio.run(run(reopened, "legacy", [lookup(query)], query,
                                            engine=ContextEngine(reopened, summarizer=summarize)))
    assert len(texts) == 1


def test_opaque_and_other_role_data_are_not_shared_with_technical(session):
    store, owner, _ = session
    remember(store, owner, "PRIVATE_ADDRESS", reply="PRIVATE_PAYMENT", audience=["billing", "billing_lookup", "#W6564160"])
    store.save_context_state("u", "c", {"summary": {"upto_turn": 1, "text": "OLDER_PRIVATE_PAYMENT",
                              "scope": dict(format="visible-v1", bound=owner, since=0,
                                            audience=["billing", "billing_lookup", "#W6564160"])}})
    query = "Check color of variant 2652637226"
    node = dict(id="catalog", kind="product_info", quote=query, output="color", target="2652637226", depends_on=[])
    result, model = asyncio.run(run(store, "legacy", [node], query))
    assert result.success
    for marker in ("PRIVATE_ADDRESS", "PRIVATE_PAYMENT", "OLDER_PRIVATE_PAYMENT"):
        assert marker in json.dumps(model.planner_requests)
        assert marker not in json.dumps(model.requests)


@pytest.mark.parametrize("bad", ["foreign", "invented", "empty"])
def test_bad_proposal_cannot_be_substituted_with_unique_owned_candidate(session, bad):
    store, owner, seed = session
    remember(store, owner, "Order #W6564160")
    text = "麻烦再看一下"
    node = lookup(text)
    foreign = next(o["order_id"] for o in seed["orders"].values() if o["user_id"] != owner)
    node["history_target"] = {"foreign": foreign, "invented": "#W99999999", "empty": ""}[bad]
    result, model = asyncio.run(run(store, "legacy", [node], text))
    assert result.turn_status == "waiting_user" and not model.requests and not result.tool_traces
    assert foreign not in result.response and "#W99999999" not in result.response


@pytest.mark.parametrize("error", ["fake_quote", "fake_literal", "literal_conflict", "extra_args", "flag_off"])
def test_history_extension_preserves_strict_contract(session, error):
    store, owner, _ = session
    remember(store, owner, "Check order #W6564160.")
    text = "查刚才那笔订单"
    node = lookup(text)
    if error == "fake_quote":
        node["quote"] += " #W6564160"
    elif error == "fake_literal":
        node["target"] = "#W6564160"
    elif error == "literal_conflict":
        text = node["quote"] = "查 #W6564160"
    elif error == "extra_args":
        node["args"] = {"order_id": "#W6564160"}
    result, model = asyncio.run(run(store, "legacy", [node], text, enabled=error != "flag_off"))
    assert model.plans == 2 and not model.requests and not result.tool_traces and not store.open_dags("u", "c")


def test_latest_policy_question_reaches_planner_after_two_corrections(session):
    store, owner, _ = session
    remember(store, owner, "我想把订单 #W6564160 的耳机换成黑色。", reply="想换成黑色，对吗？")
    remember(store, owner, "算了，还是原来的颜色吧。", reply="好的，保留原颜色。")
    query = "先看看换货手续，不要直接申请。"
    policy = dict(id="terms", kind="return_policy", depends_on=[], target="", output="policy", quote=query)
    result, model = asyncio.run(run(store, "legacy", [policy], query))
    prompt = json.loads(model.planner_requests[0]["messages"][0]["content"])
    assert prompt["current_user_input"] == query
    assert [t["assistant"] for t in prompt["recent_conversation"]] == ["想换成黑色，对吗？", "好的，保留原颜色。"]
    assert [t["user"] for t in prompt["recent_conversation"]][-1] == "算了，还是原来的颜色吧。"
    state, = store.open_dags("u", "c")
    assert [node.kind for node in state.plan.nodes] == ["return_policy"]
    assert model.plans == 1 and not result.tool_traces and not store.active_pending("u", "c")


def test_unbound_summary_cannot_authorize_private_lookup(session):
    store, owner, _ = session
    store = RetailStore(store.db_path.parent / "anonymous.sqlite", SEED_PATH, language="en")
    remember(store, None, "I am " + owner)
    store.save_context_state("u", "c", {"summary": {"upto_turn": 1, "text": "Identity verified by chat",
                              "scope": dict(format="visible-v1", bound=None, since=0, audience=None)}})
    query = "Read order #W6564160"
    result, model = asyncio.run(run(store, "legacy", [lookup(query, "#W6564160")], query))
    assert result.semantic_routing["dag"]["stop"] == "identity_required" and not model.requests
    assert not store.active_pending("u", "c")


def test_foreign_literal_cannot_be_claimed(session):
    store, owner, seed = session
    foreign = next(o["order_id"] for o in seed["orders"].values() if o["user_id"] != owner)
    text = "Read order " + foreign
    result, model = asyncio.run(run(store, "legacy", [lookup(text, foreign)], text))
    assert not model.requests and not result.task_runs
    assert result.semantic_routing["dag"]["tasks"][0]["reason"] == "unverified_target"


def test_malformed_legacy_and_changed_binding_in_an_unclosed_turn(session):
    store, owner, _ = session
    remember(store, owner, "OLD_PRIVATE_MARKER")
    engine = ContextEngine(store)
    for blocks in ([{"type": "text", "text": "Legacy message"}], None):
        turn = store.begin_turn("u", "c")
        if blocks is None:
            blocks = engine.user_blocks(turn, {"turn": turn, "bound_user_id": "former"}, "PRIVATE_INPUT")
        store.save_turn_messages("u", "c", turn, engine.log_rows(blocks, [], "Unpublished")[:1])
    remember(store, owner, "Available context")
    query = "Check #W6564160"
    result, model = asyncio.run(run(store, "legacy", [lookup(query, "#W6564160")], query))
    history = json.loads(model.planner_requests[0]["messages"][0]["content"])["recent_conversation"]
    assert result.success and [t["user"] for t in history] == ["Available context"]


def test_expired_write_selection_is_retired_before_confirm(session):
    store, owner, _ = session
    remember(store, owner, "Check #W6564160")
    quote = "Exchange item 2652637226 on that order for variant 3254583681 using paypal_2233507"
    node = dict(id="swap", kind="return_change", target="", history_target="#W6564160", quote=quote,
                output="executed", depends_on=[])
    prepared, _ = asyncio.run(run(store, "legacy", [node], quote))
    assert prepared.turn_status == "waiting_user" and len(store.active_pending("u", "c")) == 1
    for _ in range(4):
        remember(store, owner, "Hello")
    result, model = asyncio.run(run(store, "legacy", [node], "Yes, I confirm."))
    assert result.semantic_routing["dag"]["stop"] == "choose_target" and not model.requests
    assert not store.active_pending("u", "c")
    assert not any(e["event_type"] == store.executed_event("exchange") for e in store.list_events("u", "c"))


def test_runtime_window_configuration(monkeypatch, session):
    from agents.retail_runtime import RetailRuntimeConfig

    monkeypatch.setenv("ANTHROPIC_API_KEY", "offline-test-placeholder")
    monkeypatch.setenv("LLM_MODEL", "offline")
    monkeypatch.setenv("RETAIL_INPUT_CNY_PER_MTOK", "1")
    monkeypatch.setenv("RETAIL_OUTPUT_CNY_PER_MTOK", "1")
    monkeypatch.delenv("RETAIL_RECENT_CONVERSATION_TURNS", raising=False)
    assert RetailRuntimeConfig.from_env().recent_conversation_turns == 4
    monkeypatch.setenv("RETAIL_RECENT_CONVERSATION_TURNS", "5")
    monkeypatch.setenv("RETAIL_CONVERSATION_CONTEXT_ENABLED", "true")
    config = RetailRuntimeConfig.from_env()
    assert config.recent_conversation_turns == 5 and config.conversation_context_enabled
    with pytest.raises(ValueError, match="3, 4 or 5"):
        asyncio.run(run(session[0], "legacy", [], "Hello", window=6))
