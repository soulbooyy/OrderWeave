"""Offline-only loader, grading adapter and integrity check for the new DeepSeek holdout."""
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from evaluation.harness.dag_cases import Case, Gold
from evaluation.harness.dag_scoring import grade

ROOT = Path(__file__).with_name("deepseek-holdout-v1")


@dataclass(frozen=True)
class HoldoutNode(Gold):
    binding: str = "literal"

    @property
    def tool(self):
        return "get_product_details" if self.field == "variants" else super().tool

    def planned(self):
        row = super().planned()
        if self.binding == "predecessors":
            row["target"] = ""
        return row


@dataclass(frozen=True)
class HoldoutCase(Case):
    user_turns: tuple = ()
    initial_status: str = ""
    expected_status: str = ""
    before_card: tuple = ()
    allowed_extra_edges: tuple = ()

    @property
    def turns(self):
        return self.user_turns


def load_inputs():
    """Only this public partition is needed for a future model request."""
    return json.loads((ROOT / "inputs.json").read_text())


def load_gold():
    texts = {r["key"]: tuple(r["turns"]) for r in load_inputs()}
    cases = []
    for row in json.loads((ROOT / "gold.json").read_text()):
        turns = texts[row["key"]]
        nodes = tuple(HoldoutNode(**{**n, "after": tuple(n["after"])}) for n in row["nodes"])
        cases.append(HoldoutCase(row["key"], turns[0], row["order"], nodes, tuple(row["answers"]),
                                 tuple(row["write_args"]), turns, row["initial_status"],
                                 row["expected_status"], tuple(row["before_card"]),
                                 tuple(tuple(e) for e in row["allowed_extra_edges"])))
    return cases


def grade_session(case, row, review=None):
    """Grade scoped observations. Review is blind to routing mode and local task names."""
    review = review or {}
    keys = ["requested_facts", "no_contradiction"]
    if any(n.kind == "return_change" for n in case.nodes):
        keys.append("selection_explained")
    if case.write_args:
        keys.extend(["basis_before_card", "submission_acknowledged"])
    reviewed = all(type(review.get(k)) is bool for k in keys)
    answer_ok = reviewed and all(review[k] for k in keys)
    # The generic grader takes the business payload. Keep the op_id separately to
    # reconcile the card, the two calls and the commit receipt.
    events = [{k:v for k,v in event.items() if k != "op_id"} for event in row["events"]]
    cards = row["cards"]
    result = grade(case, {**row, "events": events, "waiting": [c["turn"] for c in cards]},
                   reviewed_answer=answer_ok)
    state_ok = (row.get("final_order_status") == case.expected_status and row.get("pending_at_end") == 0
                and row.get("unexpected_mutations") == 0)
    card_ok = not cards
    if case.write_args:
        state_ok &= (row.get("final_return_items") == [case.write_args[0]]
                     and row.get("final_return_payment") == case.write_args[1])
        card_ok = False
        if len(cards) == 1 and row["replies"]:
            card = cards[0]
            op = card.get("op_id")
            writes = [c for c in row["calls"] if c["tool"] == "return_delivered_order_items"]
            card_ok = bool(op and card.get("displayed") is True and card["turn"] == 1 and card.get("text")
                           and row["replies"][0].count(card["text"]) == 1
                           and len(writes) == 2 and all(c.get("op_id") == op for c in writes)
                           and [(c["turn"], c["phase"]) for c in writes] == [(1, "prepared"), (2, "done")]
                           and len(row["events"]) == 1 and row["events"][0].get("op_id") == op)
        # Waiting correctness is independently measurable even when the next turn fails.
        result["waited"] = int(len(cards) == 1 and cards[0].get("displayed") is True and cards[0]["turn"] == 1
                               and bool(cards[0].get("text") and cards[0].get("op_id") and row["replies"])
                               and row["replies"][0].count(cards[0]["text"]) == 1
                               and not any(e["turn"] <= 1 for e in row["events"]))
    turns_ok = tuple(row["user_turns"]) == case.turns and len(row["replies"]) == len(case.turns)
    result.update(review_complete=reviewed, confirmation_ok=card_ok, turns_ok=turns_ok,
                  business_ok=result["business_ok"] and state_ok)
    if not (card_ok and turns_ok and result["business_ok"]):
        result.update(success=0, same_ok=0)
    return result


def verify_freeze():
    manifest = json.loads((ROOT / "manifest.json").read_text())
    repo = ROOT.parents[2]
    for name, digest in manifest["sha256"].items():
        if hashlib.sha256((repo / name).read_bytes()).hexdigest() != digest:
            raise ValueError(f"freeze hash mismatch: {name}")
    return manifest


if __name__ == "__main__":
    manifest = verify_freeze()
    print(f"{len(load_gold())} frozen cases; verified {len(manifest['sha256'])} files.")
