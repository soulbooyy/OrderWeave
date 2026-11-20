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
