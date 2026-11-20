"""Characterize the compiled-context DAG before introducing per-task history or logging."""
import asyncio
import json
import sqlite3
from contextlib import closing
from copy import deepcopy

import pytest

from agents.agent_orchestrator import AgentOrchestrator
from agents.retail_agent import RetailService
from agents.retail_multi_agent import RetailMultiAgent
from agents.retail_tools import retail_tools
from core.context_engine import ContextEngine
from core.retail_scheduler import Execution
from evaluation.harness.config import SEED_PATH
from evaluation.harness.dag_cases import SEED_SHA256
from evaluation.harness.dag_script import response
from evaluation.harness.routing_ablation import ROLES
from mcp_tools.retail_store import RetailStore


pytestmark = pytest.mark.skipif(not SEED_PATH.is_file(), reason="requires frozen public retail seed")

PLAN = [
    dict(id="T1", kind="product_info", quote="List variants of product 4768869376",
         depends_on=[], output="variants", target="4768869376"),
    dict(id="T2", kind="billing_lookup", quote="read items in order #W6564160",
         depends_on=["T1"], output="item_ids", target="#W6564160"),
    dict(id="T3", kind="product_info", quote="check the purchased variant's color",
         depends_on=["T1", "T2"], output="color", target=""),
]
MESSAGE = ". Then ".join(node["quote"] for node in PLAN) + "."
HISTORY_MARKER = "Previously I asked about a red bag."
READS = {
    "T1": ("get_product_details", "product_id"),
    "T2": ("get_order_details", "order_id"),
    "T3": ("get_item_details", "item_id"),
}


class RecordingModel:
    """Consume only actual provider kwargs. Observe the persisted claim independently."""

    def __init__(self, store):
        self.store = store
        self.messages = self
        self.requests = []
        self.claims = []

    async def create(self, **request):
        self.requests.append(deepcopy(request))
        if "tools" not in request:
            return response(dict(type="text", text=json.dumps(
                dict(intent="query", confidence=1.0, subtasks=PLAN))))
        messages = request["messages"]
        if messages[-1]["content"][0]["type"] == "tool_result":
            return response(dict(type="text", text="Checked."))
        assignment = json.loads(messages[0]["content"][0]["text"])["assignment"]
        execution, = self.store.open_dags("u", "c")
        run = execution.runs[assignment["id"]]
        self.claims.append((run.task_id, run.role, run.status.value, run.attempt, execution.revision))
        tool, key = READS[assignment["id"]]
        return response(dict(type="tool_use", id="call-" + assignment["id"],
                             name=tool, input={key: assignment["target"]}))


def assert_checkpoint_scope(snapshots, plan_id):
    assert snapshots[0].values["plan_id"] == plan_id
    assert snapshots[0].values["phase"] == "complete"
    assert any(s.values.get("phase") == "dag_executed" for s in snapshots)
    # Inspect intermediate states too: a later node could clear an oversized field.
    for snapshot in snapshots:
        state = json.dumps(snapshot.values)
        for key in ("runs", "nodes", "tool_result", "tool_use", "tool_traces",
                    "variants", "payment_history", "input_schema", "messages"):
            assert f'"{key}"' not in state


@pytest.mark.parametrize("workflow", ["langgraph", "legacy"])
def test_compiled_dag_at_model_boundary(tmp_path, workflow):
    store = RetailStore(tmp_path / "business.sqlite", SEED_PATH, SEED_SHA256, language="en")
    seed = json.loads(SEED_PATH.read_bytes())
    order = seed["orders"]["#W6564160"]
    email = seed["users"][order["user_id"]]["email"]
    assert store.find_user_by_email("u", "c", email)["success"]
    engine = ContextEngine(store, language="en")
    prior = store.begin_turn("u", "c")
    store.save_turn_messages("u", "c", prior, engine.log_rows(
        engine.user_blocks(prior, {}, HISTORY_MARKER), [], "Understood."))

    model = RecordingModel(store)
    router = AgentOrchestrator(model="offline", client=model, routing_mode="dag",
                               retail_mode=True, retail_role_classes=ROLES, language="en")
    router.set_shared_tools(retail_tools(store, language="en"))
    service = RetailService(store, RetailMultiAgent(router, language="en"),
                            workflow_mode=workflow, context_engine=engine, language="en")

    async def run_turn():
        graph = service.workflow
        try:
            result = await service.handle_turn(MESSAGE, "u", "c")
            if graph is not None:
                snapshots = [s async for s in graph._graph.aget_state_history(graph.config("u", "c"))]
                assert_checkpoint_scope(snapshots, result.semantic_routing["dag"]["plan_id"])
            return result
        finally:
            if graph is not None:
                await graph.close()

    result = asyncio.run(run_turn())
    assert result.success and result.turn_status == "completed"
    assert len(model.requests) == 7  # One planner call, two calls per role invocation.
    planner, *role_requests = model.requests
    assert MESSAGE in planner["messages"][0]["content"] and "tools" not in planner

    assert [(task, role, status, attempt) for task, role, status, attempt, _ in model.claims] == [
        ("T1", "technical", "running", 1), ("T2", "billing", "running", 1),
        ("T3", "technical", "running", 1)]
    revisions = [claim[-1] for claim in model.claims]
    assert 0 < revisions[0] < revisions[1] < revisions[2]
    with closing(sqlite3.connect(store.db_path)) as conn:
        revision, active, payload = conn.execute(
            "SELECT revision, active, payload FROM dag_executions WHERE plan_id=?",
            (result.semantic_routing["dag"]["plan_id"],)).fetchone()
    execution = Execution.load(json.loads(payload))
    assert revision == execution.revision and revision > revisions[-1] and not active
    assert execution.complete and execution.runs["T3"].facts == {"color": "green"}

    catalog = {"get_product_details", "get_item_details", "list_all_product_types", "calculate"}
    billing = {"get_order_details", "get_user_details", "calculate"}
    variants = sorted(seed["products"]["4768869376"]["variants"])
    parents = {"T1": {"variants": variants}, "T2": {"item_ids": ["2652637226"]}}
    expected_scopes = [
        (catalog, "4768869376"), (billing, "#W6564160"), (catalog, "2652637226"),
    ]
    for node, (allowed, target), first, after_tool in zip(
        PLAN, expected_scopes, role_requests[::2], role_requests[1::2], strict=True
    ):
        assignment = json.loads(first["messages"][0]["content"][0]["text"])["assignment"]
        assert assignment["id"] == node["id"] and assignment["target"] == target
        assert assignment["inputs"] == {key: parents[key] for key in node["depends_on"]}
        assert assignment["input_refs"] == {key: ["tool:call-" + key] for key in node["depends_on"]}
        assert {spec["name"] for spec in first["tools"]} == allowed
        assert first["system"] == after_tool["system"] and first["tools"] == after_tool["tools"]
        call, reply = after_tool["messages"][-2:]
        assert call["content"][0]["id"] == reply["content"][0]["tool_use_id"] == "call-" + node["id"]
        assert json.loads(reply["content"][0]["content"])["success"]
        assert execution.runs[node["id"]].refs == ("tool:call-" + node["id"],)
        assert call["content"][0]["name"] == READS[node["id"]][0]

    # Current gaps: phase 1 changes the scoped view, phase 2 changes these log kinds.
    assert all(HISTORY_MARKER not in json.dumps(r["messages"]) for r in role_requests)
    history = store.conversation_log("u", "c")
    assert HISTORY_MARKER in json.dumps(history[0]["content"])
    assert [r["kind"] for r in history if r["turn"] == result.turn] == ["user", "assistant"]
    assert history[-1]["content"][0]["text"] == result.response


def test_summary_anchor_survives_store_reopen(tmp_path):
    path = tmp_path / "history.sqlite"
    store = RetailStore(path, SEED_PATH, SEED_SHA256, language="en")
    calls = []

    def summarize(text, cutoff):
        calls.append(cutoff)
        return "The customer discussed a red bag."

    def make_engine(database):
        return ContextEngine(database, language="en", budget_tokens=2000,
                             keep_recent_turns=1, summarizer=summarize)

    engine = make_engine(store)
    for message in ["earlier discussion " * 600, "Which colors are available?"]:
        turn = store.begin_turn("u", "c")
        store.save_turn_messages("u", "c", turn, engine.log_rows(
            engine.user_blocks(turn, {}, message), [], "Understood."))
    original_log = store.conversation_log("u", "c")
    turn = store.begin_turn("u", "c")
    asyncio.run(engine.prepare("u", "c", turn))
    state = store.context_state("u", "c")
    assert state["summary"]["upto_turn"] == state["elided_upto_turn"] == 1
    expected = engine.history("u", "c", ())

    reopened = RetailStore(path, SEED_PATH, SEED_SHA256, language="en")
    resumed = make_engine(reopened)
    asyncio.run(resumed.prepare("u", "c", turn))
    assert calls == [1]
    assert reopened.context_state("u", "c") == state
    assert resumed.history("u", "c", ()) == expected
    assert reopened.conversation_log("u", "c") == original_log
