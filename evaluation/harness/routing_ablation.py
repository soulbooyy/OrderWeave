"""Replay frozen classifier responses through the real retail recognizer and LangGraph role nodes."""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from agents.agent_orchestrator import AgentOrchestrator
from agents.retail_agent import RetailService
from agents.retail_multi_agent import (
    RetailBillingAgent, RetailEscalationAgent, RetailGeneralAgent, RetailMultiAgent, RetailTechnicalAgent,
)
from agents.retail_tools import retail_tools
from core.context_engine import ContextEngine
from core.intent_recognizer import IntentRecognizer
from core.retail_subtasks import MODES
from evaluation.harness.routing_cases import CASES
from mcp_tools.retail_store import RetailStore


ROLES = {cls.agent_type.value: cls for cls in (
    RetailGeneralAgent, RetailTechnicalAgent, RetailBillingAgent, RetailEscalationAgent)}


class ReplayClient:
    def __init__(self, text):
        self.text = text
        self.messages = self
        self.calls = 0

    async def create(self, **kwargs):
        self.calls += 1
        return SimpleNamespace(content=[SimpleNamespace(type="text", text=self.text)],
                               stop_reason="end_turn", usage=SimpleNamespace(input_tokens=0, output_tokens=0))


def classifier_reply(case, mode):
    reply = {"intent": case.intent, "confidence": 0.98, "reasoning": "offline fixture"}
    if mode != "keyword":
        nodes = []
        for index, (kind, quote, *parents) in enumerate(case.nodes):
            row = {"id": f"t{index + 1}", "kind": kind, "quote": quote}
            if mode == "dag":
                row["depends_on"] = [f"t{i + 1}" for i in parents]
            nodes.append(row)
        reply["subtasks"] = nodes
    return json.dumps(reply, ensure_ascii=False)


async def observe(case, mode, store, checkpoint):
    language = "en" if case.message.isascii() else "zh"
    classifier = ReplayClient(classifier_reply(case, mode))
    router = AgentOrchestrator(
        client=ReplayClient("OK."), model="offline", language=language, retail_mode=True,
        intent_recognizer=IntentRecognizer(model="offline", client=classifier, language=language),
        retail_role_classes=ROLES, routing_mode=mode,
    )
    router.set_shared_tools(retail_tools(store, language=language))
    agent = RetailMultiAgent(router, language=language)
    service = RetailService(store, agent, language=language, workflow_mode="langgraph",
                            checkpoint_path=checkpoint, context_engine=ContextEngine(store, language=language))
    try:
        result = await service.handle_turn(case.message, "offline", f"{case.key}-{mode}")
    finally:
        await service.workflow.close()
    visited = [r for r in result.workflow["node_path"] if r in ROLES]
    return {"primary": result.primary_agent, "supporting": result.supporting_agents, "executed": visited,
            "reason": result.routing_reason, "classifier_calls": classifier.calls, "completed": result.success}


def grade_dispatch(case, row):
    primary = row["primary"] == case.primary
    expected = set(case.helpers)
    helpers = set(row["supporting"])
    visited = row["executed"]
    covered = all(a in visited and b in visited for a, b in case.before)
    ordered = covered and all(visited.index(a) < visited.index(b) for a, b in case.before)
    followed = visited == [*row["supporting"], row["primary"]]
    return {"primary_wrong": not primary, "extra_helpers": len(helpers - expected),
            "missing_helpers": len(expected - helpers), "requires_order": bool(case.before),
            "endpoints_present": covered, "order_ok": ordered,
            "exact": row["completed"] and primary and helpers == expected and ordered and followed}


async def evaluate(seed):
    rows, summary = [], {}
    with TemporaryDirectory(prefix="orderweave-routing-") as directory:
        root = Path(directory)
        store = RetailStore(root / "retail.sqlite", seed)
        for mode in MODES:
            grades = []
            for case in CASES:
                row = await observe(case, mode, store, root / "checkpoint.sqlite")
                grade = grade_dispatch(case, row)
                rows.append({"id": case.key, "mode": mode,
                             "expected": {"primary": case.primary, "supporting": case.helpers, "before": case.before},
                             **row, **grade})
                grades.append(grade)
            constrained = [g for g in grades if g["requires_order"]]
            summary[mode] = {
                "cases": len(grades), "exact": sum(g["exact"] for g in grades),
                "primary_wrong": sum(g["primary_wrong"] for g in grades),
                "extra_helpers": sum(g["extra_helpers"] for g in grades),
                "missing_helpers": sum(g["missing_helpers"] for g in grades),
                "order_cases": len(constrained), "order_ok": sum(g["order_ok"] for g in constrained),
                "order_missing_endpoint": sum(not g["endpoints_present"] for g in constrained),
                "order_reversed": sum(g["endpoints_present"] and not g["order_ok"] for g in constrained),
            }
    return {"source": "synthetic classifier replay and scripted role replies", "summary": summary, "rows": rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=Path, default=Path("data/retail-assets/db.json"))
    parser.add_argument("--output", type=Path, help="Optional per-case JSON output")
    args = parser.parse_args()
    report = asyncio.run(evaluate(args.seed))
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()
