"""Offline acceptance for the frozen prospective holdout. No model calls."""
import pytest


def test_frozen_gold_is_executable_and_grounded_in_seed():
    import hashlib
    import json
    from evaluation.harness.config import SEED_PATH
    from evaluation.harness.dag_cases import SEED_SHA256
    from evaluation.harness.deepseek_holdout import load_gold, load_inputs
    from core.retail_dag import Plan, Status
    from core.retail_scheduler import Execution
    from mcp_tools.retail_domain import apply_write

    if not SEED_PATH.is_file():
        pytest.skip("retail seed unavailable")
    raw = SEED_PATH.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == SEED_SHA256
    seed = json.loads(raw)
    cases = load_gold()
    assert len({c.key for c in cases}) == len(cases) == 6
    assert all(set(x) == {"key", "turns"} for x in load_inputs())
    for case in cases:
        order = seed["orders"][case.order]
        assert order["status"] == case.initial_status
        state = Execution.start(Plan.for_execution([n.planned() for n in case.nodes], case.message, "offline", "gold"))
        nodes = {n.id:n for n in case.nodes}
        while task := state.next_node(1):
            gold = nodes[task.id]
            assert gold.applies
            run = state.runs[task.id]
            run.claim(gold.role, 1)
            target, choices = state.target(task)
            assert target == gold.target and not choices
            if gold.field == "variants":
                fact = sorted(seed["products"][target]["variants"])
            elif gold.kind == "product_info":
                product = next(p for p in seed["products"].values() if target in p["variants"])
                fact = product["variants"][target]["options"][gold.field]
            elif gold.field == "item_ids":
                fact = [x["item_id"] for x in order["items"]]
                for inputs in state.inputs(task).values():
                    if "variants" in inputs:
                        fact = [v for v in fact if v in inputs["variants"]]
            elif gold.field == "executed":
                item, method = case.write_args
                updated, _, _ = apply_write("return", dict(order_id=case.order, item_ids=[item], payment_method_id=method),
                                            order, seed["users"][order["user_id"]], seed["products"])
                assert updated["return_items"] == [item] and updated["status"] == case.expected_status
                fact = True
            else:
                fact = len(order["items"]) if gold.field == "purchased_item_count" else order[gold.field]
            assert fact == gold.value
            run.move(Status.SUCCEEDED, facts={gold.field:fact}, refs=("offline-seed",))
        assert state.complete
        assert all((state.runs[n.id].status == Status.SUCCEEDED) == n.applies for n in case.nodes)


def _reviewed_trace(case):
    calls = [dict(id=f"local_{i}", role=n.role, tool=n.tool, target=n.target, facts={n.field:n.value},
                  turn=1, phase="done") for i, n in enumerate(case.nodes) if n.applies]
    cards, events = [], []
    replies = ["Reviewed answer"]
    if case.write_args:
        write = calls.pop()
        calls += [{**write, "phase":"prepared", "facts":{}, "op_id":"receipt"},
                  {**write, "turn":2, "op_id":"receipt"}]
        cards = [dict(op_id="receipt", text="Frozen card", displayed=True, turn=1)]
        replies = ["The rating is low, so I prepared the return. Frozen card", "Return submitted."]
        events = [dict(turn=2, order=case.order, item=case.write_args[0], payment=case.write_args[1], op_id="receipt")]
    return dict(calls=calls, cards=cards, events=events, replies=replies, user_turns=case.turns, cutoff=False,
                final_order_status=case.expected_status, pending_at_end=0, unexpected_mutations=0,
                final_return_items=list(case.write_args[:1]), final_return_payment=case.write_args[1] if case.write_args else None)


def test_grade_new_holdout_requires_blind_review_exact_receipt_and_business_end_state():
    from evaluation.harness.deepseek_holdout import grade_session, load_gold

    review = dict(requested_facts=True, selection_explained=True, no_contradiction=True,
                  basis_before_card=True, submission_acknowledged=True)
    for case in load_gold():
        row = _reviewed_trace(case)
        assert grade_session(case, row, review)["success"]
        assert not grade_session(case, row)["success"]
        assert not grade_session(case, {**row, "pending_at_end":1}, review)["success"]
        if case.write_args:
            assert not grade_session(case, row, {**review,"basis_before_card":False})["success"]
            assert not grade_session(case, {**row, "cards":[{**row["cards"][0],"op_id":"other"}]}, review)["success"]
            assert not grade_session(case, {**row, "user_turns":(case.message, "stop")}, review)["success"]
            assert not grade_session(case, {**row, "events":row["events"] * 2}, review)["success"]
        else:
            assert not grade_session(case, {**row, "cards":[dict(turn=1)]}, review)["success"]


def test_only_the_documented_redundant_handoff_is_allowed():
    from evaluation.harness.dag_alignment import grade_plan
    from evaluation.harness.deepseek_holdout import load_gold

    chain, join = load_gold()[2:4]
    rows = [n.planned() for n in reversed(chain.nodes)]
    rows[0]["depends_on"].append("catalog")
    assert grade_plan(chain, rows)["exact"]
    rows = [n.planned() for n in reversed(join.nodes)]
    rows[1]["depends_on"].append("catalog")
    assert not grade_plan(join, rows)["exact"]


def test_manifest_budget_and_frozen_denominators():
    from decimal import Decimal
    import json
    from evaluation.harness.deepseek_holdout import ROOT, load_gold, verify_freeze

    manifest = verify_freeze()
    budget = json.loads((ROOT / "budget.json").read_text())
    assert Decimal(budget["previous_cumulative_cap"]) + Decimal(budget["authorized_increase"]) == Decimal("10")
    assert Decimal(budget["settled_at_freeze"]) + Decimal(budget["unspent_at_freeze"]) == Decimal(budget["cumulative_cap"])
    assert Decimal(budget["step_1_spend"]) == 0
    cases = load_gold()
    nodes = [n for c in cases for n in c.nodes]
    assert manifest["denominators"] == dict(cases=len(cases), turns=sum(len(c.turns) for c in cases),
            planned_nodes=len(nodes), planned_edges=sum(len(n.after) for n in nodes),
            conditioned_nodes=sum(bool(n.condition) for n in nodes), applicable_nodes=sum(n.applies for n in nodes),
            applicable_edges=sum(len(n.after) for n in nodes if n.applies),
            mandatory_waits=sum(bool(c.write_args) for c in cases))
