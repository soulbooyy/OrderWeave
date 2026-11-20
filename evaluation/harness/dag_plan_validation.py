"""Prospective DAG planning probe. Only the score subcommand reads independent gold."""
import argparse
import asyncio
import hashlib
import json
import math
import os
import statistics
import subprocess
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from core.intent_recognizer import IntentRecognizer
from evaluation.harness import config
from evaluation.harness.dag_alignment import grade_plan
from evaluation.harness.dag_cases import Case, Gold
from evaluation.retail_budget import BudgetStopExceeded, TurnRequestLimitExceeded
from evaluation.retail_ledger import RetailLedger, build_budgeted_client

ROOT = Path(__file__).with_name("deepseek-plan-v2")
OUT = config.ROOT / "data/evaluation/deepseek-plan-v2"
FROZEN = ("inputs.json", "gold.json", "PROTOCOL.md")
PREVIOUS = Decimal("4.95901620")
CAP = Decimal("10")


def read(path):
    return json.loads(path.read_text())


def save(path, obj):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2)+"\n")
    tmp.replace(path)


async def run():
    for name, digest in read(ROOT/"manifest.json").items():
        if hashlib.sha256((ROOT/name).read_bytes()).hexdigest() != digest:
            raise ValueError(f"Changed frozen input: {name}")
    config.load_env()
    settings = config.model_settings()
    model = settings["model"]
    if model != "deepseek-flash" or settings["base_url"] != "https://api.deepseek.com/anthropic":
        raise ValueError("Expected the official DeepSeek Flash endpoint")
    OUT.mkdir(exist_ok=False)  # Partial runs require ledger reconciliation before another paid run.
    ledger = RetailLedger(OUT/"ledger.sqlite", float(CAP-PREVIOUS), 2, 8, "dag-plan-v2", model,
                          settlement_policy=settings["settlement_policy"])
    client = build_budgeted_client(os.environ["ANTHROPIC_API_KEY"], settings["base_url"], ledger, "intent",
                                   count_turn=True, thinking={"type":"disabled"})
    batch = {"commit":subprocess.check_output(["git","rev-parse","HEAD"],text=True).strip(), "cases":[]}
    try:
        for case in read(ROOT/"inputs.json"):
            record = {"key":case["key"], "responses":[]}
            batch["cases"].append(record)
            ledger.begin_turn(case["key"], max_requests=2)
            offset = len(ledger.entries())
            async def capture(**kwargs):
                response = await client.messages.create(**kwargs)
                record["responses"].append(response.model_dump(mode="json"))
                return response
            recognizer = IntentRecognizer(model=model, language=case["language"], retail_mode=True,
                                          client=SimpleNamespace(messages=SimpleNamespace(create=capture)))
            recognizer.routing_mode = "dag"
            start = time.monotonic()
            try:
                result = await recognizer.recognize(case["message"])
                record.update(status=result.diagnostics["plan_status"], attempts=result.diagnostics["attempts"],
                              compiled=result.subtasks)
            except (BudgetStopExceeded, TurnRequestLimitExceeded) as stop:
                record.update(status=stop.plan_status, attempts=stop.plan_attempts, reason=str(stop))
            finally:
                record["seconds"] = time.monotonic()-start
                record["ledger"] = ledger.entries()[offset:]
                save(OUT/"raw.json",batch)
            print(record["key"],record["status"],flush=True)
            if record["status"] in {"budget_stop","request_limit","request_error"}:
                break
    finally:
        await client.aclose()


def task_case(input_row, rows):
    nodes = tuple(Gold(r["id"], r["kind"], r["quote"], r["target"], r["output"], None,
                       tuple(r["depends_on"]), r.get("run_if",{}).get("quote","")) for r in rows)
    return Case(input_row["key"],input_row["message"],"",nodes,())


def grades(case, record):
    rows = record.get("compiled")
    valid = record.get("status") in {"first_pass", "corrected"}
    if rows is None and record.get("responses"):
        from core.llm_utils import extract_text_content
        try:
            obj = json.loads(extract_text_content(record["responses"][-1]["content"]))
            rows = obj.get("subtasks") if isinstance(obj,dict) else None
        except ValueError:
            pass
    results = {}
    for version in ("v1","v2"):
        g = grade_plan(case,rows,version=version)
        g["exact"] = bool(g["exact"] and valid)
        results[version] = g
    return results


def score():
    batch = read(OUT/"raw.json")
    gold = read(ROOT/"gold.json")
    observed = {r["key"]:r for r in batch["cases"]}
    expected = [i for i in read(ROOT/"inputs.json") if gold[i["key"]] is not None]
    rows = []
    for i in expected:
        rec = observed.get(i["key"],{})
        attempts = rec.get("attempts",[])
        first = attempts[0]["status"] if attempts else "missing"
        rows.append(dict(key=i["key"], first=first, final=rec.get("status","missing"),
                         grades=grades(task_case(i,gold[i["key"]]), rec)))
    rejected = {k:observed.get(k,{}).get("status","missing") for k,v in gold.items() if v is None}
    g = [r["grades"]["v2"] for r in rows]
    metrics = {}
    for name in ("nodes","edges","conditions"):
        c,p,e = (sum(x[name+"_"+s] for x in g) for s in ("correct","predicted","expected"))
        metrics[name] = dict(correct=c,predicted=p,expected=e,precision=c/p if p else 0,
                             recall=c/e if e else 0,f1=2*c/(p+e) if p+e else 0)
    first_json = sum(r["first"] in {"ok","semantic_error","structure_error"} for r in rows)
    first_structure = sum(r["first"] in {"ok","semantic_error"} for r in rows)
    first_semantic = sum(r["first"]=="ok" for r in rows)
    passed = sum(r["final"] in {"first_pass","corrected"} for r in rows)
    exact = sum(x["exact"] for x in g)
    gate = (first_semantic>=7 and passed==8 and exact>=7 and metrics["conditions"]["recall"]==1
            and all(v in {"structure_error","semantic_error"} for v in rejected.values()))
    attempts = [e for r in batch["cases"] for e in r["ledger"]]
    spent = sum(Decimal(str(e["actual_cny"] if e["actual_cny"] is not None else e["reserved_cny"]))
                for e in attempts).quantize(Decimal("0.00000001"))
    latency = sorted(r["seconds"] for r in batch["cases"])
    report = dict(commit=batch["commit"],supported=len(rows),first_json=first_json,first_structure=first_structure,
                  first_semantic=first_semantic,final_passed=passed,exact=exact,metrics=metrics,rejected=rejected,
                  gate=gate,rows=rows,requests=len(attempts),spent=str(spent),cumulative=str(PREVIOUS+spent),
                  remaining=str(CAP-PREVIOUS-spent),p50=statistics.median(latency),
                  p95=latency[math.ceil(.95*len(latency))-1])
    for col in ("input_tokens","output_tokens","cache_read_tokens"):
        report[col] = sum(e[col] or 0 for e in attempts)
    save(OUT/"summary.json",report)
    print(json.dumps({k:v for k,v in report.items() if k!="rows"},indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action",choices=("run","score"))
    if parser.parse_args().action == "run":
        asyncio.run(run())
    else:
        score()
