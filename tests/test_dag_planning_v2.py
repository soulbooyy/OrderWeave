import asyncio
import json
from types import SimpleNamespace

import pytest
from core.dag_planning import parse_plan, PlanError
from evaluation.harness.deepseek_holdout import ROOT, load_gold
from evaluation.harness.dag_alignment import grade_plan


def wire(case):
    rows = [n.planned() for n in case.nodes]
    for r, n in zip(rows, case.nodes):
        if n.condition:
            r["run_if"] = {"source": n.after[0], "branch": "else" if n.quote in n.condition.split("otherwise ")[1] else "then"}
    return {"intent": "query", "confidence": 0.91, "subtasks": rows}


def parse(data, case):
    return parse_plan(json.dumps(data), case.message)["subtasks"]


def test_historical_errors_are_explained():
    responses = json.loads((ROOT/"plan-probe-results.json").read_text())["records"]
    cases = {c.key:c for c in load_gold()}
    for r in responses:
        if r["mode"] != "dag" or r["key"] not in {"H02","H05","H06"}:
            continue
        raw = r["response"]["content"][0]["text"]
        with pytest.raises(PlanError) as error:
            parse_plan(raw, cases[r["key"]].message)
        paths = [e["path"] for e in error.value.issues]
        assert ("subtasks" if r["key"] == "H02" else "subtasks.0.output") in paths


@pytest.mark.parametrize("case",load_gold(),ids=lambda c:c.key)
def test_all_regression_shapes_compile(case):
    assert parse(wire(case), case) == [n.planned() for n in case.nodes]


@pytest.mark.parametrize("change",["missing","wrong","ambiguous","field","source","target","cycle","quote","extra","empty"])
def test_invalid_graphs(change):
    case = load_gold()[0]
    d = wire(case)
    a,b,c = d["subtasks"]
    if change == "missing": d["subtasks"].pop()
    if change == "wrong": b["run_if"]["branch"] = "else"
    if change == "ambiguous":
        from dataclasses import replace
        case = replace(case, message=case.message+" "+case.nodes[1].condition+".")
    if change == "field": a["output"] = "item_ids"
    if change == "source": b["run_if"]["source"] = c["id"]
    if change == "target": a["target"] = b["target"]
    if change == "cycle": a["depends_on"] = [c["id"]]
    if change == "quote": a["quote"] = "fiction"
    if change == "extra": b["authorized"] = True
    if change == "empty": d["subtasks"] = []
    with pytest.raises(PlanError) as exc:
        parse(d, case)
    assert exc.value.issues[0]["path"] and exc.value.issues[0]["code"]


def test_json_and_empty_are_strict():
    valid = '{"intent":"greeting","confidence":1,"subtasks":[]}'
    assert parse_plan(valid,"hello")["subtasks"] == []
    for text in ["json:"+valid, valid+valid, "```json\n"+valid+"\n```",valid.replace('1,','"1",')]:
        with pytest.raises(PlanError): parse_plan(text,"hello")


def test_versioned_alignment():
    for case in load_gold()[2:4]:
        rows = [n.planned() for n in case.nodes]
        order = next(n for n in rows if n["kind"]=="order_lookup")
        order["target"] = order["target"][1:]
        assert not grade_plan(case,rows,version="v1")["exact"]
        assert grade_plan(case,rows,version="v2")["exact"]
        order["target"] = "W9999999"
        assert not grade_plan(case,rows,version="v2")["exact"]


@pytest.mark.parametrize("corrected",[True,False])
def test_normal_route_retry_and_stop(corrected):
    from tests.test_retail_dispatch import router
    from agents.agent_orchestrator import Request
    case = load_gold()[0]
    app = router("dag",[])
    calls=[]
    async def reply(**kw):
        calls.append(kw)
        assert not ({"tools","tool_choice"} & kw.keys())
        text = json.dumps(wire(case)) if corrected and len(calls)==2 else '{"intent":"query","confidence":1}'
        return SimpleNamespace(content=[{"type":"text","text":text}])
    app._intent_recognizer.client.messages.create = reply
    out = asyncio.run(app.prepare_route(Request(case.message,"u","c"),schedule=True))
    assert len(calls)==2 and "missing" in str(calls[1]["messages"])
    if corrected:
        from agents.retail_dag_runner import DagRunner
        assert isinstance(out,DagRunner)
    else:
        assert not out.success and not out.role_traces and out.routing_reason.startswith("dag_contract:")


def test_correction_budget_stops_before_second_response():
    from tests.test_retail_dispatch import router
    from evaluation.retail_budget import BudgetStopExceeded
    app = router("dag", [])
    replies = iter(['{}', BudgetStopExceeded("test ledger refused reservation")])
    async def limited(**kwargs):
        answer = next(replies)
        if isinstance(answer, Exception):
            raise answer
        return SimpleNamespace(content=[{"type":"text","text":answer}])
    app._intent_recognizer.client.messages.create = limited
    with pytest.raises(BudgetStopExceeded) as stopped:
        asyncio.run(app._intent_recognizer.recognize(load_gold()[0].message))
    assert stopped.value.plan_status == "budget_stop"
    assert stopped.value.plan_attempts[0]["status"] == "structure_error"


def test_ambiguous_producer_duplicate_keys_and_write_target():
    case = load_gold()[0]
    data = wire(case)
    data["subtasks"].append({**data["subtasks"][0],"id":"second"})
    with pytest.raises(PlanError,match="ambiguous_condition_source"):
        parse(data,case)
    with pytest.raises(PlanError,match="invalid_json"):
        parse_plan('{"confidence":0,"confidence":1}',"hello")
    case = load_gold()[4]
    data = wire(case)
    data["subtasks"][1]["target"] = data["subtasks"][0]["target"]
    with pytest.raises(PlanError,match="expected_order_id"): parse(data,case)


def test_wrong_output_identifies_the_producing_node():
    case = load_gold()[0]
    data = wire(case)
    data["subtasks"][0]["output"] = "status"
    with pytest.raises(PlanError) as error:
        parse(data, case)
    assert error.value.issues[0]["path"] == "subtasks.0.output"
