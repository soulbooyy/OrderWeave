import pytest
from dataclasses import replace

from evaluation.harness.dag_cases import CASES
from evaluation.harness.dag_scoring import grade


def attempt(node, *, turn=1):
    return dict(id=node.id, role=node.role, tool=node.tool, target=node.target,
                facts={node.field: node.value}, turn=turn, phase="done")


def score(case, calls, **overrides):
    return grade(case, {"calls": calls, "replies": ["2 items, size: XXL"], "events": [],
                        "waiting": [], "cutoff": False, **overrides})


@pytest.mark.parametrize("error,order,success", [
    ("none", 1, 1), ("reverse", 0, 0), ("no_proof", 0, 0),
    ("omitted", 0, 0), ("wrong_variant", 0, 0), ("extra_branch", 1, 0),
    ("cutoff", 1, 0), ("false_answer", 1, 0)])
def test_branch_scoring(error, order, success):
    case = CASES[0]
    a, unused, b = case.nodes
    calls = [attempt(a), attempt(b)]
    if error == "reverse":
        calls.reverse()
    elif error == "no_proof":
        calls[0]["facts"] = {}
    elif error == "omitted":
        calls.pop()
    elif error == "wrong_variant":
        calls[1]["target"] = unused.target
    elif error == "extra_branch":
        calls.append(attempt(unused))
    result = score(case, calls, cutoff=error == "cutoff", replies=["done" if error == "false_answer" else "2 items, size: XXL"])
    assert result["edges_ok"] == order and result["success"] == success


def test_repeated_legacy_roles_are_matched_by_target_and_ambiguity_is_reported():
    case = CASES[2]
    calls = [{**attempt(n), "id": ""} for n in case.nodes]
    assert score(case, calls)["assigned"] == 3
    ambiguous = replace(case, nodes=(case.nodes[0], replace(case.nodes[0], id="other")))
    assert score(ambiguous, calls[:1])["ambiguous"] == 1
    assert score(case, [*calls, calls[0]])["duplicate"] == 1


def test_correct_wait_requires_display_then_later_exactly_one_business_event():
    case = CASES[-1]
    a, b = case.nodes
    calls = [attempt(a), {**attempt(b), "phase": "prepared", "facts": {}}, attempt(b, turn=2)]
    event = {"turn": 2, "order": case.order, "item": case.write_args[0], "payment": case.write_args[1]}
    args = dict(waiting=[1], replies=["2 items. Please confirm.", "Return submitted."], events=[event])
    assert score(case, calls, **args)["success"]
    for key, value in [("events", []), ("events", [event, event]), ("waiting", []),
                       ("events", [{**event, "turn": 1}]), ("events", [{**event, "item": "other"}])]:
        assert not score(case, calls, **{**args, key: value})["success"]


def test_offline_runner_records_real_user_reply_and_tool_evidence(tmp_path):
    import asyncio
    import json
    from unittest.mock import patch
    from core.retail_subtasks import OWNERS
    from evaluation.harness.config import SEED_PATH
    from evaluation.harness.dag_ablation import run_case
    from evaluation.harness.dag_cases import Case, Gold

    if not SEED_PATH.is_file():
        pytest.skip("retail seed unavailable")
    message = "Check the color of variant 5253880258"
    case = Case("smoke", message, "#W9549057", (
        Gold("t1", "product_info", message, "5253880258", "color", "black"),), (r"color.{0,12}black",))
    with patch.dict(OWNERS, {"order_lookup": "billing"}):
        row = asyncio.run(run_case(case, "dag", 1, tmp_path, SEED_PATH, json.loads(SEED_PATH.read_text())))
    assert row["grade"]["success"] and row["requests"] == {"classifier": 1, "role": 2}
    assert row["calls"][0]["id"] == "t1" and row["latency_ms"][0] > 0
    assert row["plan_grade"]["exact"] and row["plan"] == [case.nodes[0].planned()]


def renamed_plan(case):
    names = {n.id: f"local_{i}" for i, n in enumerate(reversed(case.nodes))}
    rows = [n.planned() for n in reversed(case.nodes)]
    for row in rows:
        row["id"] = names[row["id"]]
        row["depends_on"] = [names[p] for p in row["depends_on"]]
        if "run_if" in row:
            row["run_if"]["source"] = names[row["run_if"]["source"]]
    return rows, names


def test_local_labels_reordered_and_swapped_cannot_change_scores():
    from evaluation.harness.dag_alignment import grade_plan

    case = CASES[0]
    rows, names = renamed_plan(case)
    calls = [{**attempt(n), "id": names[n.id]} for n in case.nodes if n.applies]
    assert score(case, calls)["success"] and score(case, calls, plan=rows)["success"]
    metrics = grade_plan(case, rows)
    assert metrics["exact"] and metrics["edges_correct"] == 2 and metrics["conditions_correct"] == 2
    assert metrics["nodes_correct"] == metrics["nodes_expected"] == 3
    calls[-1]["target"] = case.nodes[1].target
    assert not score(case, calls, plan=rows)["success"]


def test_plan_scores_edges_even_when_scheduling_would_happen_in_the_right_order():
    from evaluation.harness.dag_alignment import grade_plan

    case = CASES[3]
    rows, names = renamed_plan(case)
    rows[0]["depends_on"].pop()
    rows[1]["depends_on"] = [names[case.nodes[0].id]]
    result = grade_plan(case, rows)
    assert result["contract_ok"] and not result["exact"]
    assert result["nodes_correct"] == 3
    assert (result["edges_expected"], result["edges_predicted"], result["edges_correct"]) == (2, 2, 1)


def test_branch_source_unsupported_quote_duplicates_and_subintent():
    from evaluation.harness.dag_alignment import grade_plan

    case = CASES[0]
    rows, _ = renamed_plan(case)
    flat = [{k:r[k] for k in ("id", "kind", "quote")} for r in rows]
    flat_result = grade_plan(case, flat, mode="subintent")
    assert flat_result["exact"] and flat_result["edges_expected"] is None
    rows[0]["run_if"]["quote"] += " invented"
    result = grade_plan(case, rows)
    assert not result["exact"] and not result["contract_ok"] and result["conditions_correct"] < 2
    duplicate = grade_plan(case, [*rows, rows[0]])
    assert not duplicate["exact"] and not duplicate["contract_ok"]


def test_ambiguous_twins_use_only_anchored_dependencies_not_model_labels():
    from evaluation.harness.dag_alignment import grade_plan

    base = CASES[2]
    a, b, _ = base.nodes
    twin = replace(a, id="again", after=(b.id,))
    case = replace(base, nodes=(a,b,twin))
    rows, _ = renamed_plan(case)
    assert grade_plan(case, rows)["exact"]
    rows[0]["depends_on"] = []
    rows[1]["depends_on"] = []
    assert grade_plan(case, rows)["ambiguous"] >= 2
    calls = [attempt(a)]
    assert score(case, calls)["ambiguous"] == 1


def test_conflicting_task_semantics_are_not_rescued_by_target_or_equal_id():
    from evaluation.harness.dag_alignment import grade_plan

    case = CASES[0]
    rows = [n.planned() for n in case.nodes]
    rows[-1]["output"] = "color"
    result = grade_plan(case, rows)
    assert not result["exact"] and result["nodes_correct"] == 2
    calls = [attempt(n) for n in case.nodes if n.applies]
    assert not score(case, calls, plan=rows)["success"]


def test_offline_observation_preserves_typed_evidence_for_new_targets():
    from evaluation.harness.dag_ablation import observation

    def traced(tool, args, **result):
        return {"tool_use_id": "x", "tool_name": tool, "input": args, "success": True,
                "result_success": True, "agent_type": "technical", "result": {"success": True, **result}}

    product = traced("get_product_details", {"product_id": "p"}, product={"product_id":"p", "variants":{"b":{}, "a":{}}})
    assert sorted(observation(product, 1, "u")["facts"]["variants"]) == ["a", "b"]
    order = traced("get_order_details", {"order_id":"o"}, order={"user_id":"u","order_id":"o",
                    "status":"delivered","items":[{"item_id":"1"},{"item_id":"1"}]})
    assert observation(order, 1, "u")["facts"]["item_ids"] == ["1","1"]
    assert not observation(order, 1, "other")["facts"]
    assert not observation({**order, "not_executed": True}, 1, "u")["facts"]
    item = traced("get_item_details", {"item_id":"i"}, item={"item_id":"i","options":{"ventilation":"low"}})
    assert observation(item, 1, "u")["facts"] == {"ventilation":"low"}


def test_plan_allows_sentence_terminator_on_verbatim_guard_and_rejects_bare_flat_quote():
    from evaluation.harness.dag_alignment import grade_plan
    case = CASES[0]
    rows, _ = renamed_plan(case)
    for row in rows:
        if row.get("run_if"):
            row["run_if"]["quote"] += "."
    assert grade_plan(case, rows)["exact"]
    flat = [{k: r[k] for k in ("id","kind","quote")} for r in rows]
    flat[-1]["quote"] = "the"
    assert not grade_plan(case, flat, mode="subintent")["exact"]


def test_malformed_tool_success_has_no_proof_and_numeric_one_cannot_prove_a_write():
    from evaluation.harness.dag_ablation import observation
    from evaluation.harness.dag_scoring import _verified_value
    for name in ("get_order_details", "get_item_details", "get_product_details"):
        trace = dict(input={"order_id":"o"}, tool_name=name, result={"success":True},
                     result_success=True, tool_use_id="ref", agent_type="billing")
        assert observation(trace, 1, "owner")["facts"] == {}
    assert not _verified_value("executed", 1, True)
    assert not _verified_value("item_ids", ["1"], ["1","1"])


def test_swapped_outputs_on_same_variant_cannot_be_hidden_by_local_ids():
    from evaluation.harness.dag_alignment import grade_plan

    a = CASES[2].nodes[0]
    b = replace(a, id="size", field="size", quote=a.quote.replace("color", "size"))
    case = replace(CASES[2], nodes=(a,b), message=f"{a.quote}. {b.quote}.")
    rows, _ = renamed_plan(case)
    rows[0]["output"], rows[1]["output"] = rows[1]["output"], rows[0]["output"]
    assert not grade_plan(case, rows)["exact"]
