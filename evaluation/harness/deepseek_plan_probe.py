"""Run the frozen first-turn plan probe once. Score only after collecting the batch."""
import asyncio
import json
import os
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from core.intent_recognizer import IntentRecognizer
from evaluation.harness import config
from evaluation.harness.dag_alignment import grade_plan
from evaluation.harness.deepseek_holdout import ROOT, load_gold, load_inputs, verify_freeze
from evaluation.retail_ledger import RetailLedger, build_budgeted_client

OUT = config.ROOT / "data/evaluation/deepseek-holdout-v1-plan"


def write(name, data):
    (OUT / name).write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")


async def run():
    verify_freeze()
    config.load_env()
    settings = config.model_settings()
    model = settings["model"]
    if model != "deepseek-flash" or settings["base_url"] != "https://api.deepseek.com/anthropic":
        raise ValueError("Expected the official DeepSeek Flash endpoint")
    budget = json.loads((ROOT / "budget.json").read_text())
    balance = Decimal(budget["cumulative_cap"]) - Decimal(budget["settled_at_freeze"])
    OUT.mkdir(exist_ok=False)  # Never repeat a partially billed batch.
    ledger = RetailLedger(OUT / "ledger.sqlite", float(balance), 2, 8, "holdout-v1-plan", model,
                          settlement_policy=settings["settlement_policy"])
    client = build_budgeted_client(os.environ["ANTHROPIC_API_KEY"], settings["base_url"], ledger, "intent",
                                   count_turn=True, thinking={"type": "disabled"})
    records = []
    try:
        for index, case in enumerate(load_inputs()):
            for mode in (("subintent", "dag") if index % 2 == 0 else ("dag", "subintent")):
                tag = f"{case['key']}-{mode}"
                ledger.begin_turn(tag, max_requests=1)
                attempts = []

                async def capture(**kwargs):
                    response = await client.messages.create(**kwargs)
                    attempts.append(response.model_dump(mode="json"))
                    write(tag + "-raw.json", attempts)
                    return response

                recognizer = IntentRecognizer(model=model, language="en", retail_mode=True,
                                              client=SimpleNamespace(messages=SimpleNamespace(create=capture)))
                recognizer.routing_mode = mode
                result = await recognizer.recognize(case["turns"][0])
                records.append(dict(key=case["key"], mode=mode, status=result.recognition_status,
                                    subtasks=result.subtasks, latency_ms=result.latency_ms))
                write("observations.json", records)
                print(tag, result.recognition_status, flush=True)
                if result.recognition_status in {"parse_error", "request_error", "unknown_label"}:
                    return
    finally:
        await client.aclose()
        write("usage.json", ledger.entries())
    gold = {c.key: c for c in load_gold()}
    scored = [{**row, "grade": grade_plan(gold[row["key"]], row["subtasks"], row["mode"])}
              for row in records]
    write("scores.json", scored)


if __name__ == "__main__":
    asyncio.run(run())
