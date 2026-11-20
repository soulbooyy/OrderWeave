"""Grade paired goals, structural execution and blinded user-visible answers separately."""
import argparse
from collections import Counter
from copy import deepcopy
from decimal import Decimal
import hashlib
import math
import statistics

from core.dag_planning import parse_plan, PlanError
from core.llm_utils import extract_text_content
from evaluation.harness.dag_scoring import _verified_value
from evaluation.harness.deepseek_holdout import grade_session
from evaluation.harness.retail_e2e import OUT, ROOT, cases, read, save
from mcp_tools.retail_store import normalize_order_id


def samples():
    for stage in ("smoke","replay","formal"):
        labels = {c.key:c for c in cases(stage)}
        for path in sorted((OUT/stage).glob("*/trace.json")):
            row = read(path)
            yield hashlib.sha256(f"{stage}/{row['key']}".encode()).hexdigest()[:10],labels[row["case"]],row


def checklist(case):
    keys = ["requested_facts","no_contradiction"]
    if any(n.kind=="return_change" for n in case.nodes):
        keys.append("selection_explained")
    if case.write_args:
        keys.extend(["basis_before_card","submission_acknowledged"])
    return keys


def blind():
    sheets = {}
    for key,case,row in samples():
        sheets[key] = dict(message=case.message, expected=[(n.target,n.field,n.value) for n in case.nodes if n.applies
                                       and n.field not in {"variants","item_ids"}], replies=row["replies"],
                           card_text=[c["text"] for c in row["cards"]], checks=checklist(case))
    save(OUT/"blind.json",dict(sorted(sheets.items())))


def grade(case, row, review):
    row = deepcopy(row)
    if row["mode"]=="dag" and row["steps"]:
        responses=row["steps"][0]["intent_responses"]
        if responses:
            try:
                plan=parse_plan(extract_text_content(responses[-1]),case.message)["subtasks"]
                for r in plan:
                    if r["target"].lstrip("#").upper().startswith("W"):
                        r["target"]=normalize_order_id(r["target"])
                row["plan"] = plan
            except PlanError:
                pass
    for i,call in enumerate(row["calls"]):
        # A matched-receipt request projects purchased IDs through a previously verified catalog list.
        for node in case.nodes:
            if (call["tool"]==node.tool and call["target"]==node.target and node.field=="item_ids"
                    and "item_ids" in call["facts"]):
                for parent in case.nodes:
                    if parent.id in node.after and parent.field=="variants":
                        lists = [c["facts"]["variants"] for c in row["calls"][:i] if c["tool"]==parent.tool
                                 and c["target"]==parent.target and c["facts"].get("variants")]
                        if lists:
                            call["facts"]["item_ids"]=[v for v in call["facts"]["item_ids"] if v in lists[-1]]
    result=grade_session(case,row,review)
    # User goal does not depend on planner-local IDs, efficiency or a specific role name.
    def proven(node):
        return any(c["tool"]==node.tool and c["target"]==node.target and c["phase"]=="done"
                   and _verified_value(node.field,c["facts"].get(node.field),node.value) for c in row["calls"])
    off_branch=any(c["tool"]==n.tool and c["target"]==n.target for n in case.nodes if not n.applies
                   for c in row["calls"] if n.kind!="order_lookup" or not case.write_args)
    result["delivered"]=bool(result["answer_ok"] and result["business_ok"] and result["turns_ok"]
                             and result["confirmation_ok"] and not row["cutoff"])
    result["goal"]=bool(result["delivered"] and not off_branch and all(proven(n) for n in case.nodes if n.applies))
    result["full_order"]=result["edges"]==result["edges_ok"]
    return result


def summarize():
    reviews=read(ROOT/"reviews.json")
    rows=[]
    for key,case,r in samples():
        usage=r["usage"]
        rows.append(dict(key=r["key"],case=case.key,rep=r["rep"],mode=r["mode"],stage=r["stage"],review_id=key,
                         grade=grade(case,r,reviews[key]),seconds=r["seconds"],requests=len(usage),
                         **{k:sum(e[k] or 0 for e in usage) for k in ("input_tokens","output_tokens","cache_read_tokens")},
                         cny=round(sum(e["actual_cny"] if e["actual_cny"] is not None else e["reserved_cny"] for e in usage),8)))
    summary={}
    for stage,mode in sorted({(r["stage"],r["mode"]) for r in rows}):
        group=[r for r in rows if (r["stage"],r["mode"])==(stage,mode)]
        g=Counter()
        for r in group:
            g.update(r["grade"])
        times=sorted(t for r in group for t in r["seconds"])
        session_times=sorted(sum(r["seconds"]) for r in group)
        summary[f"{stage}/{mode}"]=dict(sessions=len(group),turns=len(times),**dict(g),
                  **{k:sum(r[k] for r in group) for k in ("requests","input_tokens","output_tokens","cache_read_tokens")},
                  cost=round(sum(r["cny"] for r in group),8),p50=statistics.median(times),
                  p95=times[math.ceil(.95*len(times))-1],session_p50=statistics.median(session_times),
                  session_p95=session_times[math.ceil(.95*len(group))-1])
    total=sum(Decimal(str(r["cny"])) for r in rows)
    save(ROOT/"scores.json",dict(summary=summary,rows=rows,spent=str(total),cumulative=str(Decimal("4.99128340")+total),
                                  remaining=str(Decimal("5.00871660")-total)))


if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("action",choices=("blind","grade"))
    blind() if ap.parse_args().action=="blind" else summarize()
