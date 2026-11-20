"""Run the frozen offline comparison through RetailService and real SQLite tools."""
import argparse
import asyncio
from collections import Counter
import hashlib
import json
from math import ceil
from pathlib import Path
from tempfile import TemporaryDirectory
import time
from unittest.mock import patch

from agents.agent_orchestrator import AgentOrchestrator
from agents.retail_agent import RetailService
from agents.retail_multi_agent import RetailMultiAgent
from agents.retail_tools import retail_tools
from core.intent_recognizer import IntentRecognizer
from core.retail_subtasks import OWNERS
from evaluation.harness.config import SEED_PATH
from evaluation.harness.dag_alignment import grade_plan
from evaluation.harness.dag_cases import CASES, MODES, REPEATS, SEED_SHA256
from evaluation.harness.dag_scoring import grade
from evaluation.harness.dag_script import Script, role_classes
from evaluation.retail_budget import BudgetExceeded
from mcp_tools.retail_store import RetailStore


def observation(trace, turn, uid):
    args = trace.get("input") if isinstance(trace.get("input"), dict) else {}
    tool = trace["tool_name"]
    arg_key = {"get_item_details":"item_id", "get_product_details":"product_id"}.get(tool, "order_id")
    target = args.get(arg_key)
    result = trace.get("result") if isinstance(trace.get("result"), dict) else {}
    facts = {}
    phase = "done"
    if (target and trace.get("result_success") is True and result.get("success") is True
            and trace.get("tool_use_id") and trace.get("success") is not False
            and not any(trace.get(flag) for flag in ("not_executed", "skipped", "blocked_by_guard"))):
        if tool == "get_order_details":
            order = result.get("order")
            if isinstance(order, dict) and uid and order.get("user_id") == uid and order.get("order_id") == target:
                items = order.get("items")
                if isinstance(items, list) and all(isinstance(i, dict) and i.get("item_id") for i in items):
                    facts["purchased_item_count"] = len(items)
                    facts["item_ids"] = [item["item_id"] for item in items]
                if isinstance(order.get("status"), str):
                    facts["status"] = order["status"]
        elif tool == "get_product_details":
            product = result.get("product")
            if isinstance(product, dict) and product.get("product_id") == target and isinstance(product.get("variants"), dict):
                facts["variants"] = sorted(product["variants"])
        elif tool == "get_item_details":
            item = result.get("item")
            if isinstance(item, dict) and item.get("item_id") == target and isinstance(item.get("options"), dict):
                facts = {k:v for k,v in item["options"].items() if k in ("size", "color", "ventilation")}
        elif tool == "return_delivered_order_items" and result.get("op_id"):
            if result.get("action_status") == "executed":
                facts = {"executed": True}
            elif result.get("action_status") == "pending_confirmation":
                phase = "prepared"
    return dict(id=trace.get("task_id", ""), role=trace["agent_type"], tool=tool,
                target=target, facts=facts, turn=turn, phase=phase,
                **({"op_id": result["op_id"]} if tool == "return_delivered_order_items" and result.get("op_id") else {}))


async def run_case(case, mode, rep, root, seed, source, frozen=False):
    entry = "frozen_dag" if frozen else mode
    store = RetailStore(root / f"{entry}-{case.key}-{rep}.sqlite", seed, language="en")
    uid = source["orders"][case.order]["user_id"]
    client, conv = "offline", case.key
    assert store.find_user_by_email(client, conv, source["users"][uid]["email"])["success"]
    script = Script(mode, [n.planned() for n in case.nodes] if frozen else None)
    router = AgentOrchestrator(client=script, model="offline", routing_mode=mode, language="en",
                               retail_mode=True, retail_role_classes=role_classes(),
                               intent_recognizer=IntentRecognizer(client=script, model="offline", language="en"))
    router.set_shared_tools(retail_tools(store, language="en"))
    service = RetailService(store, RetailMultiAgent(router, language="en"), workflow_mode="langgraph", language="en")
    row = dict(case=case.key, mode=entry, repeat=rep, calls=[], waiting=[], replies=[], events=[], latency_ms=[])
    seen = set()
    try:
        for turn, message in enumerate(case.turns, start=1):
            start = time.perf_counter()
            try:
                result = await service.handle_turn(message, client, conv)
            except BudgetExceeded:
                break
            finally:
                row["latency_ms"].append(round(1000 * (time.perf_counter() - start), 2))
            row["replies"].append(result.response)
            for trace in result.tool_traces:
                row["calls"].append(observation(trace, turn, uid))
            if any(p["status"] == "displayed" and p["displayed_turn"] == turn and p["display_text"] in result.response
                   for p in store.active_pending(client, conv)):
                row["waiting"].append(turn)
            for e in store.list_events(client, conv):
                if e["event_id"] in seen:
                    continue
                seen.add(e["event_id"])
                if e["event_type"] == "return_requested":
                    args = e["payload"]["args"]
                    row["events"].append(dict(turn=turn, order=e["order_id"], item=",".join(args["item_ids"]),
                                              payment=args["payment_method_id"]))
                elif e["event_type"].endswith("_executed") or e["event_type"] == "handoff_requested":
                    row["events"].append(dict(turn=turn, unexpected=e["event_type"]))
    finally:
        await service.workflow.close()
    row["requests"] = dict(script.calls)
    row["cutoff"] = script.cutoff
    if mode != "keyword":
        row.update(plan=script.first_plan, plan_mode=mode, plan_grade=grade_plan(case, script.first_plan, mode))
    row["grade"] = grade(case, row)
    status = store.get_order_details(client, conv, case.order)["order"]["status"]
    row["final_order_status"] = status
    expected = "return requested" if case.write_args else source["orders"][case.order]["status"]
    row["pending_at_end"] = len(store.active_pending(client, conv))
    if status != expected or row["pending_at_end"]:
        row["grade"].update(success=0, same_ok=0, business_ok=False)
    return row


def summarize(rows):
    totals = Counter()
    requests = Counter()
    latencies = []
    for row in rows:
        totals.update(row["grade"])
        requests.update(row["requests"])
        latencies.extend(row["latency_ms"])
    return {**totals, "sessions": len(rows), "turns": len(latencies),
            "partial_sessions": sum(not r["grade"]["success"] and r["grade"]["proven"] > 0 for r in rows),
            "p95_ms": sorted(latencies)[ceil(len(latencies)*0.95)-1], "requests": dict(requests),
            "input_tokens": None, "output_tokens": None, "billed_model_cost_cny": None, "spent_cny": 0}


async def evaluate(seed):
    raw = seed.read_bytes()
    if hashlib.sha256(raw).hexdigest() != SEED_SHA256:
        raise ValueError("frozen seed hash mismatch")
    source = json.loads(raw)
    rows = []
    with TemporaryDirectory() as folder, patch.dict(OWNERS, {"order_lookup": "billing"}):
        for rep in range(REPEATS):
            for i, case in enumerate(CASES):
                start = (rep+i) % len(MODES)
                for mode in MODES[start:] + MODES[:start]:
                    rows.append(await run_case(case, mode, rep+1, Path(folder), seed, source))
                rows.append(await run_case(case, "dag", rep+1, Path(folder), seed, source, frozen=True))
    return {"freeze": "71eb362", "scoring": "semantic-alignment-v2", "seed_sha256": SEED_SHA256, "model": "offline-script",
            "summary": {m: summarize([r for r in rows if r["mode"] == m]) for m in (*MODES, "frozen_dag")},
            "rows": rows}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=Path, default=SEED_PATH)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    report = asyncio.run(evaluate(args.seed))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(report["summary"], indent=2))
