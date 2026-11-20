"""Run three budget-reserved paired real-service sessions at a time."""
import argparse
import asyncio
import fcntl
import hashlib
import json
import sqlite3
import time
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from agents.retail_runtime import RetailRuntimeConfig, build_retail_runtime
from core.retail_subtasks import OWNERS
from evaluation.harness import config
from evaluation.harness.dag_ablation import observation
from evaluation.harness.dag_cases import MODES, SEED_SHA256
from evaluation.harness.deepseek_holdout import HoldoutCase, HoldoutNode, load_gold, verify_freeze
from evaluation.retail_budget import BudgetExceeded, TurnRequestLimitExceeded
from mcp_tools.retail_domain import apply_write
from mcp_tools.retail_store import normalize_order_id
from monitor import live_events

ROOT = Path(__file__).with_name("e2e-v1")
OUT = config.ROOT / "data/evaluation/e2e-v1"
AVAILABLE = Decimal("5.00871660")


def read(p):
    return json.loads(p.read_text())


def save(p, data):
    temp = p.with_suffix(".tmp")
    temp.write_text(json.dumps(data, ensure_ascii=False, indent=2)+"\n")
    temp.replace(p)


def cases(stage):
    if stage in {"smoke","replay"}:
        return [c for c in load_gold() if c.key in {"H01","H03","H05"}]
    result = []
    for c in read(ROOT/"formal.json"):
        nodes = tuple(HoldoutNode(**{**n,"after":tuple(n["after"])}) for n in c["nodes"])
        result.append(HoldoutCase(c["key"],c["turns"][0],c["order"],nodes,(),tuple(c["write_args"]),
                                  tuple(c["turns"]),c["initial_status"],c["expected_status"]))
    return result


def spend():
    used = Decimal(0)
    for db in OUT.glob("*/*/ledger.sqlite"):
        with sqlite3.connect(db) as conn:
            for amt, in conn.execute("select coalesce(actual_cny,reserved_cny) from entries"):
                used += Decimal(str(amt))
    return used


async def session(case, mode, rep, stage, cap):
    key = f"{case.key}-{rep}-{mode}"
    folder = OUT/stage/key
    folder.mkdir(parents=True,exist_ok=False)
    conf = replace(RetailRuntimeConfig.from_env(),language="en",agent_mode="multi",workflow_mode="langgraph",
                   enable_knowledge=False,enable_memory=False,semantic_routing=False,support_enabled=False,
                   model_thinking="disabled",context_mode="compiled",skill_mode="on_demand",task_facts=False,
                   request_plan=True,publish_revision=True,routing_mode=mode,profile_enabled=False,
                   db_path=folder/"retail.sqlite",checkpoint_path=folder/"checkpoint.sqlite",
                   ledger_path=folder/"ledger.sqlite",ledger_settlement_policy="deepseek_flash_cache_20261024",
                   input_price_cny_per_mtok=2,output_price_cny_per_mtok=8,event_log_path=folder/"events.jsonl",
                   mcp_stderr_log=folder/"mcp.log",budget_cny=float(cap),case_id=key,run_id=key,
                   seed_path=config.SEED_PATH,policy_path=config.POLICY_PATH)
    app = build_retail_runtime(conf)
    store = app.store
    client, conv = "paired-customer",key
    row = dict(key=key,mode=mode,rep=rep,case=case.key,stage=stage,user_turns=[],replies=[],calls=[],
               cards=[],events=[],seconds=[],steps=[],cutoff=False)
    try:
        await app.start()
        before = store.business_snapshot()
        uid = before["orders"][case.order]["user_id"]
        assert store.find_user_by_email(client,conv,before["users"][uid]["email"])["success"]
        seen = set()
        for t, message in enumerate(case.turns,1):
            if t>1 and (not row["cards"] or row["events"]):
                break
            row["user_turns"].append(message)
            intents = []
            def capture(e):
                if (e.get("type"),e.get("kind"),e.get("name"))==("end","model","intent"):
                    intents.append(e["data"].get("output",[]))
            start = time.monotonic()
            try:
                with live_events.capture(capture):
                    result = await app.service.handle_turn(message,client,conv)
            except (BudgetExceeded,TurnRequestLimitExceeded) as exc:
                row.update(cutoff=True,reason=str(exc))
                break
            finally:
                row["seconds"].append(time.monotonic()-start)
            row["replies"].append(result.response)
            row["steps"].append(dict(reason=result.routing_reason,intent_responses=intents,
                                     dag=result.semantic_routing.get("dag"),roles=result.agent_types,
                                     traces=result.tool_traces,workflow=result.workflow.get("node_path",[])))
            for trace in result.tool_traces:
                if trace["tool_name"]=="load_skill":
                    continue
                if trace["tool_name"]=="get_order_details":
                    args = trace.get("input") or {}
                    trace = {**trace,"input":{**args,"order_id":normalize_order_id(args.get("order_id"))}}
                call = observation(trace,t,uid)
                row["calls"].append(call)
            for card in store.active_pending(client,conv):
                if (card["status"]=="displayed" and card["displayed_turn"]==t and card["display_text"]
                        and result.response.count(card["display_text"])==1):
                    row["cards"].append(dict(turn=t,op_id=card["op_id"],text=card["display_text"],displayed=True))
            for e in store.list_events(client,conv):
                if e["event_id"] in seen:
                    continue
                seen.add(e["event_id"])
                if e["event_type"]=="return_requested":
                    args = e["payload"]["args"]
                    row["events"].append(dict(turn=t,op_id=e["op_id"],order=e["order_id"],
                                              item=",".join(args["item_ids"]),payment=args["payment_method_id"]))
                elif e["event_type"].endswith("_executed") or e["event_type"]=="handoff_requested":
                    row["events"].append(dict(turn=t,unexpected=e["event_type"]))
        after = store.business_snapshot()
        final = after["orders"][case.order]
        if case.write_args:
            item, payment = case.write_args
            expected,_,_ = apply_write("return",dict(order_id=case.order,item_ids=[item],payment_method_id=payment),
                                       before["orders"][case.order],before["users"][uid],before["products"])
            before["orders"][case.order] = expected
        row.update(final_order_status=final["status"],final_return_items=final.get("return_items",[]),
                   final_return_payment=final.get("return_payment_method_id"),
                   pending_at_end=len(store.active_pending(client,conv)),unexpected_mutations=int(after!=before))
    finally:
        row["usage"] = app.ledger.entries()
        await app.close()
        save(folder/"trace.json",row)
    print(key, len(row["usage"]), "calls",flush=True)


async def run(stage):
    config.load_env()
    settings = config.model_settings()
    assert settings["model"]=="deepseek-flash" and settings["base_url"]=="https://api.deepseek.com/anthropic"
    assert hashlib.sha256(config.SEED_PATH.read_bytes()).hexdigest()==SEED_SHA256
    verify_freeze()
    OUT.mkdir(exist_ok=True)
    with (OUT/"paired.lock").open("a") as lock, patch.dict(OWNERS,{"order_lookup":"billing","product_info":"technical"}):
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        for rep in range(1,3 if stage=="formal" else 2):
            for i,case in enumerate(cases(stage)):
                j = (i+rep-1)%3
                modes = MODES[j:]+MODES[:j]
                paths = [OUT/stage/f"{case.key}-{rep}-{m}" for m in modes]
                if all((p/"trace.json").exists() for p in paths):
                    continue
                if any(p.exists() for p in paths):
                    raise RuntimeError("Partial triplet requires inspection")
                cap = min(Decimal(".65"), (AVAILABLE-spend())//Decimal(".0003")*Decimal(".0001"))
                if cap<Decimal(".10"):
                    print("STOP: insufficient triplet reserve",flush=True)
                    return
                for mode in modes:
                    await session(case,mode,rep,stage,cap)


if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("stage",choices=("smoke","formal","replay"))
    asyncio.run(run(ap.parse_args().stage))
