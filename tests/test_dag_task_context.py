"""Task history, target selection and permissions through the retail model boundary."""
import asyncio
from copy import deepcopy
import json
import re
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
        self.requests = []

    async def create(self, **kwargs):
        if "tools" not in kwargs:
            self.plans += 1
            return response(dict(type="text", text=json.dumps(
                dict(intent="query", confidence=1.0, subtasks=self.nodes))))
        self.requests.append(deepcopy(kwargs))
        last = kwargs["messages"][-1]["content"][-1]
        if last["type"] == "tool_result":
            return response(dict(type="text", text="Checked."))
        blocks = kwargs["messages"][-1]["content"]
        task = json.loads(last["text"])["assignment"]
        if task["kind"] == "return_change":
            if task.get("operation"):
                args = task["operation"]["args"]
            else:
                pref = next(h for h in json.loads(blocks[0]["text"])["history"]
                            if h["source"] == "user_preference")
                args = dict(order_id=task["target"], item_ids=[re.search(r"item (\d+)", task["quote"])[1]],
                            new_item_ids=[pref["matching_variant"]],
                            payment_method_id=re.search(r"paypal_\d+", task["quote"])[0])
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


def remember(store, owner, message):
    engine = ContextEngine(store, language="en")
    turn = store.begin_turn("u", "c")
    blocks = engine.user_blocks(turn, {"turn": turn, "bound_user_id": owner,
                                       "private_note": "OLD_STATE_SECRET"}, message)
    blocks.append({"type": "text", "text": "ROLE_HANDOFF_SECRET"})
    exchange = dict(agent="billing", assistant=[dict(type="tool_use", id="old", name="get_order_details",
                                                     input={"order_id": "#W6564160"})],
                    results=[dict(type="tool_result", tool_use_id="old", content="PAYMENT_HISTORY_SECRET")])
    rows = engine.log_rows(blocks, [exchange], "ASSISTANT_UNVERIFIED")
    store.save_turn_messages("u", "c", turn, rows)


async def run(store, workflow, nodes, message, *, enabled=True):
    model = Model(nodes)
    router = AgentOrchestrator(model="offline", client=model, routing_mode="dag",
                               retail_role_classes=ROLES, retail_mode=True, language="en")
    router.set_shared_tools(retail_tools(store, language="en"))
    service = RetailService(store, RetailMultiAgent(router, language="en"), workflow_mode=workflow,
                            context_engine=ContextEngine(store), task_context_enabled=enabled, language="en")
    try:
        result = await service.handle_turn(message, "u", "c")
        return result, model
    finally:
        if service.workflow:
            await service.workflow.close()


def lookup(message, target=""):
    return dict(id="lookup", kind="order_lookup", target=target, quote=message,
                output="status", depends_on=[])


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
    assert "#W6564160" in blocks[0]["text"] and "turn" in blocks[0]["text"]
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
        for text in ("Thank you", "Hello"):
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
    assert any(h.get("color") == "green" and h["turn"] == 2 for h in history["history"])
    assert assigned["assignment"]["inputs"] == {
        "T1": {"variants": sorted(seed["products"]["4768869376"]["variants"])},
        "T2": {"item_ids": ["2652637226"]}}
    assert assigned["assignment"]["input_refs"] == {"T1": ["tool:call-T1"], "T2": ["tool:call-T2"]}
    assert len(third["messages"]) == 1 and "session" not in assigned


@pytest.mark.parametrize("workflow", ["legacy", "langgraph"])
def test_color_preference_is_an_option_only(session, workflow):
    store, owner, _ = session
    remember(store, owner, "For product 4768869376 I prefer blue.")
    remember(store, owner, "I confirm everything.")
    quote = "Exchange item 2652637226 in order #W6564160 to the previous color using paypal_2233507"
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


@pytest.mark.parametrize("workflow", ["legacy", "langgraph"])
def test_new_literal_and_color_override_prior_preference(session, workflow):
    store, owner, _ = session
    remember(store, owner, "For variant 2652637226 I prefer green.")
    quote = "Check blue instead of the previous color for variant 3254583681"
    nodes = [dict(id="new", kind="product_info", quote=quote, output="color",
                  target="3254583681", depends_on=[])]
    result, model = asyncio.run(run(store, workflow, nodes, quote))
    assert result.success and len(model.requests[0]["messages"][0]["content"]) == 1
    assert result.tool_traces[0]["input"] == {"item_id": "3254583681"}
    assert "2652637226" not in json.dumps(model.requests)


@pytest.mark.parametrize("enabled,text,target", [(False, "Check that order", ""),
                         (True, "Check my status", ""), (True, "Check that order", "#W6564160")])
def test_planner_rejects_unsupported_empty_or_invented_literal(session, enabled, text, target):
    store, owner, _ = session
    remember(store, owner, "Check #W6564160.")
    result, model = asyncio.run(run(store, "legacy", [lookup(text, target)], text, enabled=enabled))
    assert not model.requests and not result.tool_traces
    assert not store.open_dags("u", "c")


@pytest.mark.parametrize("workflow", ["legacy", "langgraph"])
def test_color_reference_with_implicit_order_and_expired_write(session, workflow):
    store, owner, _ = session
    remember(store, owner, "For product 4768869376 I prefer blue.")
    remember(store, owner, "Check order #W6564160")
    quote = "Exchange item 2652637226 to the previous color using paypal_2233507"
    task = dict(id="swap", kind="return_change", quote=quote, target="", output="executed", depends_on=[])
    result, model = asyncio.run(run(store, workflow, [task], quote))
    assert result.turn_status == "waiting_user" and model.plans == 1
    state, = store.open_dags("u", "c")
    assert state.selected == {"swap": "#W6564160"} and state.plan.nodes[0].target == ""
    assert len(store.active_pending("u", "c")) == 1
    remember(store, owner, "Hello again")
    later, model = asyncio.run(run(store, workflow, [task], "Yes, I confirm."))
    assert later.turn_status == "waiting_user" and not model.requests and not model.plans
    assert not store.active_pending("u", "c")
    assert not any(e["event_type"] == store.executed_event("exchange") for e in store.list_events("u", "c"))


@pytest.mark.parametrize("workflow", ["legacy", "langgraph"])
def test_unattributed_preference_requires_fresh_change_details(session, workflow):
    store, owner, _ = session
    remember(store, owner, "For product 4768869376 I prefer blue.")
    remember(store, owner, "For product 4768869376 I no longer want blue.")
    quote = "Exchange item 2652637226 in #W6564160 to the previous color using paypal_2233507"
    task = dict(id="swap", kind="return_change", quote=quote, target="#W6564160", output="executed", depends_on=[])
    result, model = asyncio.run(run(store, workflow, [task], quote))
    assert result.turn_status == "waiting_user" and not model.requests and not store.active_pending("u", "c")
    assert result.semantic_routing["dag"]["stop"] == "specify_change"


@pytest.mark.parametrize("summary,allowed", [
    ("For variant 2652637226 the user prefers green.", True),
    ("For variant 2652637226 the user prefers blue. Another bag is green.", False),
])
def test_old_preference_requires_matching_summary_clause_and_user_source(session, summary, allowed):
    store, owner, _ = session
    remember(store, owner, "For variant 2652637226 I prefer green.")
    for text in ("Hello", "Thank you"):
        remember(store, owner, text)
    store.save_context_state("u", "c", {"summary": {"upto_turn": 1, "text": summary}})
    quote = "Check the previous color of variant 2652637226"
    task = dict(id="spec", kind="product_info", quote=quote, target="2652637226", output="color", depends_on=[])
    result, model = asyncio.run(run(store, "legacy", [task], quote))
    blocks = model.requests[0]["messages"][0]["content"]
    assert result.success and ("user_preference" in json.dumps(blocks)) == allowed
    assert summary not in json.dumps(blocks)


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
