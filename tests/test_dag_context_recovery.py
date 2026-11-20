"""Reopen the service at publication boundaries, on both retail backends."""
from copy import deepcopy

import pytest

from agents.retail_agent import RetailService
from evaluation.harness.config import SEED_PATH
from tests.test_dag_tool_log import READ, SWAP, PowerLoss, batches, db, reopen, run_service

pytestmark = pytest.mark.skipif(not SEED_PATH.is_file(), reason="requires frozen public retail seed")


@pytest.mark.parametrize("workflow", ["legacy", "langgraph"])
@pytest.mark.parametrize("mode", ["off", "m", "j"])
@pytest.mark.parametrize("published", [False, True])
def test_restart_closes_original_turn_once(db, monkeypatch, workflow, mode, published):
    original = db.finish_dialogue
    message = READ[0]["quote"]
    def die(*args, **kwargs):
        if published:
            original(*args, **kwargs)
        raise PowerLoss()
    with monkeypatch.context() as patch:
        patch.setattr(db, "finish_dialogue", die)
        with pytest.raises(PowerLoss):
            run_service(db, workflow, mode, READ, message)
    before = deepcopy(batches(db, 1))
    visible = db.last_visible_turn("u", "c")
    restored = reopen(db)
    async def forbidden(*args, **kwargs):
        pytest.fail("recovery ran normal turn preparation or finalization")
    with monkeypatch.context() as patch:
        patch.setattr(RetailService, "_prepare_turn", forbidden)
        patch.setattr(RetailService, "_finish_turn", forbidden)
        result, spy = run_service(restored, workflow, "off", READ, "continue")
    assert result.turn == restored.current_turn("u","c") == 1
    assert result.success and result.turn_status == "completed" and result.response_guard["version"]
    assert not spy.requests and not spy.plans
    assert batches(restored, 1) == before
    rows = restored.conversation_log("u", "c")
    assert [r["content"][-1]["text"] for r in rows if r["kind"] in {"user","assistant"}] == [message, result.response]
    if published:
        assert visible["assistant"] == result.response
    assert result.task_runs[0]["refs"] == ["tool:call-read"]
    later, _ = run_service(restored, workflow, "j", READ, message)
    assert later.turn == 2


@pytest.mark.parametrize("workflow", ["legacy","langgraph"])
def test_card_and_publication_share_a_commit(db, monkeypatch, workflow):
    def die(*args, **kwargs):
        raise PowerLoss()
    with monkeypatch.context() as patch:
        patch.setattr(db, "finish_dialogue", die)
        with pytest.raises(PowerLoss):
            run_service(db, workflow, "j", SWAP, SWAP[0]["quote"])
    pending, = db.active_pending("u","c")
    assert pending["status"] == "prepared"
    restored = reopen(db)
    result, spy = run_service(restored, workflow, "j", SWAP, "Yes, I confirm.")
    assert result.turn == 1 and not spy.requests and not spy.plans
    assert not restored.active_pending("u","c")
    assert not any(e["event_type"].endswith(("_executed","_displayed")) for e in restored.list_events("u","c"))
    assert pending["display_text"] not in result.response


@pytest.mark.parametrize("workflow", ["legacy","langgraph"])
@pytest.mark.parametrize("limit", ["fee","quota"])
def test_hard_stop_exposes_guarded_partial_and_resumes_only_successors(db, workflow, limit):
    from evaluation.retail_budget import BudgetStopExceeded, TurnRequestLimitExceeded
    from tests.test_dag_context_baseline import MESSAGE, PLAN
    from tests.test_dag_task_context import Model
    kind = BudgetStopExceeded if limit == "fee" else TurnRequestLimitExceeded
    class StopAfterFirstRead(Model):
        async def create(self, **kwargs):
            if self.requests:
                raise kind("offline limit")
            return await super().create(**kwargs)
    with pytest.raises(kind) as stopped:
        run_service(db, workflow, "j", PLAN, MESSAGE, model=StopAfterFirstRead(PLAN))
    partial = stopped.value.turn_result
    assert not partial.success and partial.response_guard["version"]
    reason = "budget_stop" if limit == "fee" else "request_limit"
    assert partial.semantic_routing["hard_stop"] == reason and reason in partial.response
    assert partial.task_runs[0]["refs"] == ["tool:call-T1"]
    restored = reopen(db)
    replay, spy = run_service(restored, workflow, "j", PLAN, "continue")
    assert replay.response == partial.response and replay.turn == 1
    assert not spy.requests and not spy.plans
    later, spy = run_service(restored, workflow, "j", PLAN, "continue")
    assert later.success and later.turn == 2 and not spy.plans
    assert [t["task_id"] for t in later.tool_traces] == ["T2", "T3"]


@pytest.mark.parametrize("workflow", ["legacy","langgraph"])
def test_confirmed_commit_before_fee_stop_is_never_replayed(db, workflow):
    from evaluation.retail_budget import BudgetStopExceeded
    from tests.test_dag_task_context import Model
    prepared, _ = run_service(db, workflow, "j", SWAP, SWAP[0]["quote"])
    pending, = db.active_pending("u","c")
    class StopAfterCommit(Model):
        async def create(self, **kwargs):
            if self.requests:
                raise BudgetStopExceeded("offline")
            return await super().create(**kwargs)
    with pytest.raises(BudgetStopExceeded) as stopped:
        run_service(db, workflow, "j", SWAP, "Yes, I confirm.", model=StopAfterCommit(SWAP))
    saved = stopped.value.turn_result
    events = [e for e in db.list_events("u","c") if e["event_type"] == db.executed_event("exchange")]
    assert len(events) == 1 and events[0]["op_id"] == pending["op_id"]
    assert saved.task_runs[0]["refs"] == [f"event:{events[0]['event_id']}"]
    assert not saved.success and saved.turn_status == "unresolved"
    assert pending["op_id"] in saved.response and "budget_stop" in saved.response
    previous = db.list_events("u","c")
    restored = reopen(db)
    replay, spy = run_service(restored, workflow, "j", SWAP, "continue")
    assert replay.response == saved.response and not spy.requests and not spy.plans
    assert restored.list_events("u","c") == previous


def recovery_service(store):
    from types import SimpleNamespace
    from agents.agent_orchestrator import AgentOrchestrator
    from tests.test_dag_task_context import Model
    from evaluation.harness.routing_ablation import ROLES
    router = AgentOrchestrator(model="offline", client=Model(READ), retail_mode=True, retail_role_classes=ROLES,
                               routing_mode="dag", language="en")
    return RetailService(store, SimpleNamespace(orchestrator=router), language="en")


@pytest.mark.parametrize("wrong", ["plan","revision","turn","binding","scope"])
def test_stale_or_foreign_checkpoint_cannot_republish(db, wrong):
    result, _ = run_service(db, "legacy", "j", READ, READ[0]["quote"])
    dag = result.semantic_routing["dag"]
    checkpoint = dict(turn=1, plan_id=dag["plan_id"], dag_revision=dag["revision"])
    user = "u"
    if wrong == "scope":
        user = "elsewhere"
    elif wrong == "binding":
        with db._tx() as con:
            con.execute("UPDATE sessions SET retail_user_id=NULL WHERE client_user_id='u' AND conv_id='c'")
    elif wrong == "plan":
        checkpoint["plan_id"] = "another-plan"
    elif wrong == "revision":
        checkpoint["dag_revision"] += 1
    else:
        checkpoint["turn"] += 1
    before = db.conversation_log("u", "c")
    replay = recovery_service(db).recover_dag(user, "c", checkpoint=checkpoint)
    assert replay is None or (not replay.success and result.response not in replay.response and not replay.task_runs)
    assert db.conversation_log("u","c") == before


@pytest.mark.parametrize("workflow", ["legacy","langgraph"])
@pytest.mark.parametrize("missing", ["assistant", "user"])
def test_recover_published_original_can_repair_missing_row_without_republishing(db, monkeypatch, workflow, missing):
    original = db.finish_dialogue
    def die(*args, **kwargs):
        original(*args, **kwargs)
        raise PowerLoss()
    monkeypatch.setattr(db, "finish_dialogue", die)
    with pytest.raises(PowerLoss):
        run_service(db, workflow, "j", READ, READ[0]["quote"])
    before = db.conversation_log("u","c")
    with db._tx() as con:
        con.execute("DELETE FROM conversation_log WHERE kind=? AND turn=1", (missing,))
    restored = reopen(db)
    replay, spy = run_service(restored, workflow, "j", READ, "continue")
    after = restored.conversation_log("u","c")
    def public_and_raw(rows):
        return [(r["seq"], r["kind"], r["content"][-1] if r["kind"] == "user" else r["content"]) for r in rows]
    assert public_and_raw(after) == public_and_raw(before)
    assert replay.response == before[-1]["content"][0]["text"] and not spy.requests and not spy.plans


@pytest.mark.parametrize("first,second", [("legacy","langgraph"), ("langgraph","legacy")])
def test_switch_backends_with_inactive_unpublished_plan(db, monkeypatch, first, second):
    def die(*args, **kwargs):
        raise PowerLoss()
    monkeypatch.setattr(db, "finish_dialogue", die)
    with pytest.raises(PowerLoss):
        run_service(db, first, "off", READ, READ[0]["quote"])
    restored = reopen(db)
    replay, spy = run_service(restored, second, "off", READ, "continue")
    assert replay.turn == 1 and replay.success and not spy.requests and not spy.plans


def test_old_checkpoint_without_plan_needs_unique_matching_original_turn(db):
    import json
    result, _ = run_service(db, "legacy", "j", READ, READ[0]["quote"])
    service = recovery_service(db)
    old = {"turn":1}
    assert service.recover_dag("u","c",checkpoint=old).response == result.response
    with db._tx() as con:
        row = con.execute("SELECT * FROM dag_executions").fetchone()
        payload = json.loads(row["payload"])
        payload["plan"]["id"] = "ambiguous"
        con.execute("INSERT INTO dag_executions VALUES (?,?,?,?,?,?)",
                    ("ambiguous","u","c",row["revision"],0,json.dumps(payload)))
    denied = service.recover_dag("u","c",checkpoint=old)
    assert not denied.success and not denied.task_runs
    assert db.last_visible_turn("u","c")["assistant"] == result.response


def test_source_turn_must_still_be_in_visible_window_before_confirming_selection(db):
    from tests.test_dag_task_context import remember
    owner = db.bound_user_id("u","c")
    order = "#W6564160"
    remember(db, owner, "Check " + order)
    quote = "Exchange item 2652637226 on that order for variant 3254583681 using paypal_2233507"
    node = {**SWAP[0], "target":"", "history_target":order, "quote":quote}
    prepared, _ = run_service(db, "legacy", "j", [node], quote)
    assert prepared.turn_status == "waiting_user"
    # A fresh, unrelated mention of the same order must not renew an expired choice.
    for _ in range(4):
        remember(db, owner, "A separate request about " + order)
    result, spy = run_service(db, "legacy", "j", [node], "Yes, I confirm.")
    assert result.turn_status == "waiting_user" and result.semantic_routing["dag"]["stop"] == "choose_target"
    assert not spy.requests and not db.active_pending("u","c")
    assert not any(e["event_type"] == db.executed_event("exchange") for e in db.list_events("u","c"))


@pytest.mark.parametrize("workflow", ["legacy","langgraph"])
def test_budget_after_confirmation_before_first_tool_keeps_original_pending(db, workflow):
    from evaluation.retail_budget import TurnRequestLimitExceeded
    from tests.test_dag_task_context import Model
    run_service(db, workflow, "j", SWAP, SWAP[0]["quote"])
    pending, = db.active_pending("u","c")
    class Refuse(Model):
        async def create(self, **kwargs):
            raise TurnRequestLimitExceeded("offline")
    with pytest.raises(TurnRequestLimitExceeded) as stopped:
        run_service(db, workflow, "j", SWAP, "Yes, I confirm.", model=Refuse(SWAP))
    partial = stopped.value.turn_result
    assert partial.turn_status == "waiting_user" and "request_limit" in partial.response
    assert partial.response.count(pending["display_text"]) == 1
    assert len([e for e in db.list_events("u","c") if e["event_type"].endswith("_displayed")]) == 1
    assert not batches(db, 2) and not any(e["event_type"].endswith("_executed") for e in db.list_events("u","c"))
    replay, spy = run_service(reopen(db), workflow, "j", SWAP, "continue")
    assert replay.response == partial.response and not spy.requests and not spy.plans

@pytest.mark.parametrize("workflow", ["legacy", "langgraph"])
def test_recovery_uses_original_history_and_summary_boundary(db, monkeypatch, workflow):
    import json
    from core.context_engine import ContextEngine
    from tests.test_dag_task_context import Model
    for _ in range(5):
        run_service(db, workflow, "j", READ, READ[0]["quote"])
    calls = []
    def summarize(text, cutoff):
        calls.append(cutoff)
        return "Previous verified read requests"
    engine = ContextEngine(db, summarizer=summarize)
    engine.budget_tokens = 1
    model = Model(READ)
    def die(*args, **kwargs):
        raise PowerLoss()
    with monkeypatch.context() as patch:
        patch.setattr(db, "finish_dialogue", die)
        with pytest.raises(PowerLoss):
            run_service(db, workflow, "j", READ, READ[0]["quote"], model=model, engine=engine)
    planner = json.loads(model.planner_requests[0]["messages"][0]["content"])
    role = json.loads(model.requests[0]["messages"][0]["content"][0]["text"])
    assert planner["history_summary"] == role["history_summary"] == "Previous verified read requests"
    assert planner["recent_conversation"] == role["recent_conversation"]
    assert [r["turn"] for r in role["recent_conversation"]] == [2, 3, 4, 5]
    state, = db.recovery_dags("u", "c", 6)
    assert state.source["summary_turn"] == 1
    assert state.source["history_seq"] == max(r["seq"] for r in db.conversation_log("u", "c") if r["turn"] < 6)
    saved = db.context_state("u", "c")
    replay, spy = run_service(db, workflow, "j", READ, "continue", engine=engine)
    assert calls == [1] and not spy.plans and not spy.requests and db.context_state("u", "c") == saved
    _, later = run_service(db, workflow, "j", READ, READ[0]["quote"])
    visible = json.loads(later.planner_requests[0]["messages"][0]["content"])["recent_conversation"]
    assert visible[-1] == {"turn": 6, "user": READ[0]["quote"], "assistant": replay.response}


def test_pre_upgrade_checkpoint_rejects_inconsistent_published_text(db):
    import json
    result, _ = run_service(db, "legacy", "j", READ, READ[0]["quote"])
    with db._tx() as con:
        row = con.execute("SELECT payload FROM dag_executions").fetchone()
        payload = json.loads(row[0])
        payload.pop("source", None)
        con.execute("UPDATE dag_executions SET payload=?", (json.dumps(payload),))
        pairs = json.loads(con.execute("SELECT payload_json FROM retail_dialogue").fetchone()[0])
        pairs[0].pop("dag_receipt")
        con.execute("UPDATE retail_dialogue SET payload_json=?", (json.dumps(pairs),))
    service = recovery_service(db)
    replay = service.recover_dag("u", "c", checkpoint={"turn": 1})
    assert replay.response == result.response
    with db._tx() as con:
        pairs[0]["assistant"] = "conflicting visible reply"
        con.execute("UPDATE retail_dialogue SET payload_json=?", (json.dumps(pairs),))
    refused = service.recover_dag("u", "c", checkpoint={"turn": 1})
    assert not refused.success and refused.routing_reason == "unverified_recovery"


def test_chat_returns_guarded_partial_on_global_stop(db, monkeypatch):
    import asyncio
    from types import SimpleNamespace
    from api import main
    from evaluation.retail_budget import BudgetStopExceeded
    from tests.test_dag_context_baseline import MESSAGE, PLAN
    from tests.test_dag_task_context import Model

    class Limited(Model):
        async def create(self, **kwargs):
            if self.requests:
                raise BudgetStopExceeded("offline")
            return await super().create(**kwargs)

    with pytest.raises(BudgetStopExceeded) as stopped:
        run_service(db, "legacy", "j", PLAN, MESSAGE, model=Limited(PLAN))
    async def handle_turn(*args):
        raise stopped.value
    service = SimpleNamespace(handle_turn=handle_turn, store=db)
    monkeypatch.setattr(main, "_retail_runtime", SimpleNamespace(service=service))
    reply = asyncio.run(main.chat(main.ChatRequest(message=MESSAGE, user_id="u", conv_id="c")))
    assert reply.success is False and reply.turn_status == "unresolved"
    assert reply.task_runs == stopped.value.turn_result.task_runs
    assert reply.response == db.last_visible_turn("u", "c")["assistant"]
    assert db.dag_publication("u", "c", 1)["dag_receipt"]["finished"]
