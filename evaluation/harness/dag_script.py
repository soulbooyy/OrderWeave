"""Offline English fixture model. It has no access to grading labels or the business database."""
from collections import Counter
from contextvars import ContextVar
import json
import re
from types import SimpleNamespace

from evaluation.harness.routing_ablation import ROLES
from evaluation.retail_budget import BudgetExceeded

_VIEW = ContextVar("offline_role_view", default=None)
_ACTION = re.compile(r"count the items in order (?P<order>#W\d+)|check the (?P<field>color|size) of variant (?P<item>\d+)"
                     r"|return item (?P<returned>\d+) from order (?P<source>#W\d+) to (?P<method>\w+)", re.I)


def parse(text):
    nodes = []
    conditional = re.search(r"\bIf .+?; otherwise [^.]+", text, re.I)
    for match in _ACTION.finditer(text):
        quote = match[0]
        field = match["field"]
        kind = "product_info" if field else "return_change" if match["returned"] else "order_lookup"
        prefix = text[:match.start()].lower()
        parents = []
        condition = None
        if conditional and conditional.start() <= match.start() < conditional.end():
            parents = [n["id"] for n in nodes if n["kind"] == "order_lookup"][-1:]
            if parents:
                condition = {"source": parents[0], "quote": conditional[0]}
        elif prefix.endswith("after both, "):
            parents = [n["id"] for n in nodes]
        elif prefix.endswith("then ") and nodes:
            parents = [nodes[-1]["id"]]
        node = dict(id=f"t{len(nodes)+1}", kind=kind, quote=quote, depends_on=parents,
                    target=match["order"] or match["item"] or match["source"],
                    output=field or ("executed" if match["returned"] else "purchased_item_count"))
        if condition:
            node["run_if"] = condition
        nodes.append(node)
    return nodes


def role_classes():
    class ScriptView:
        async def handle(self, req):
            # The adapter exposes the same assignment, pending operation and previous role note
            # as the normal prompt. Scripted roles never inspect another role's tool payload.
            role = self.agent_type.value
            assigned = req.dag_assignment
            tasks = [assigned] if assigned else parse(req.message)
            if req.dispatch and not assigned:
                quotes = {t.quote for t in req.dispatch.roles.get(role, ())}
                tasks = [t for t in tasks if t["quote"] in quotes]
            pending = None
            if req.message.lower().startswith("yes") and req.retail_pending:
                pending = req.retail_pending[0]
            note = ((req.retail_handoff or {}).get("previous_role") or {}).get("note", "")
            view = dict(tasks=tasks, pending=pending, note=note, done=[], replies=[])
            token = _VIEW.set(view)
            try:
                return await super().handle(req)
            finally:
                _VIEW.reset(token)

    return {name: type(f"Offline{cls.__name__}", (ScriptView, cls), {}) for name, cls in ROLES.items()}


def response(block):
    return SimpleNamespace(content=[SimpleNamespace(**block)], stop_reason="tool_use" if block["type"] == "tool_use" else "end_turn",
                           usage=SimpleNamespace(input_tokens=0, output_tokens=0))


class Script:
    def __init__(self, mode, frozen=None):
        self.mode = mode
        self.frozen = frozen
        self.first_plan = None
        self.messages = self
        self.calls = Counter()
        self.cutoff = False

    async def create(self, **kw):
        messages = kw["messages"]
        view = _VIEW.get()
        if view is not None:
            stage = "role"
        elif "Candidate results:" in messages[-1]["content"]:
            stage = "composer"
        else:
            stage = "classifier"
        if sum(self.calls.values()) >= 48:
            self.cutoff = True
            raise BudgetExceeded("offline conversation call limit")
        self.calls[stage] += 1
        if stage == "role":
            return self.role(view, messages, {t["name"] for t in kw.get("tools", [])})
        prompt = messages[-1]["content"]
        if stage == "composer":
            body = prompt.partition("Candidate results:")[2]
            answer = re.sub(r"\[[^\]\n]+ Agent output\]", "", body).strip()
        else:
            match = re.search(r'^User message: "(.*)"$', prompt, re.M)
            if not match:
                raise ValueError("unrecognized offline classifier prompt")
            text = match[1]
            rows = parse(text) if self.frozen is None else self.frozen
            packet = {"intent": "refund" if "return item" in text.lower() else "query", "confidence": 1.0}
            if self.mode != "keyword":
                packet["subtasks"] = rows if self.mode == "dag" else [
                    {k: n[k] for k in ("id", "kind", "quote")} for n in rows]
                if self.calls["classifier"] == 1:
                    self.first_plan = packet["subtasks"]
                if self.mode == "dag":
                    packet["subtasks"] = [
                        {**row, "run_if": {"source": row["run_if"]["source"], "branch":
                         "else" if row["run_if"]["quote"].endswith("otherwise "+row["quote"]) else "then"}}
                        if "run_if" in row else row for row in rows]
            answer = json.dumps(packet)
        return response(dict(type="text", text=answer))

    def role(self, view, messages, allowed):
        last = messages[-1]["content"]
        if isinstance(last, list) and last and last[0].get("type") == "tool_result":
            data = json.loads(last[0]["content"])
            if data.get("success"):
                if "order" in data:
                    o = data["order"]
                    view["replies"].append(f'Order {o["order_id"]} contains {len(o["items"])} items.')
                elif "item" in data:
                    item = data["item"]
                    field = view["field"]
                    view["replies"].append(f'{item["item_id"]} {field}: {item["options"].get(field, "unknown")}.')
                elif "action_status" in data:
                    view["replies"].append(data.get("display_text") or "Return submitted.")
        tasks = view["tasks"]
        pending = view["pending"]
        if pending and not tasks:
            tasks = [dict(kind="return_change", id="confirmation", quote="", target=pending["order_id"])]
        for task in tasks:
            if task["id"] in view["done"]:
                continue
            view["done"].append(task["id"])
            if task.get("run_if"):
                count = re.search(r"contains (\d+) items", " ".join([view["note"], *view["replies"]]))
                if not count:
                    continue
                yes = task["quote"] in task["run_if"]["quote"].split(";")[0]
                if yes != (int(count[1]) >= 3):
                    continue
            kind = task["kind"]
            if kind == "order_lookup":
                tool, args = "get_order_details", {"order_id": task["target"]}
            elif kind == "product_info":
                tool, args = "get_item_details", {"item_id": task["target"]}
                view["field"] = task["output"]
            else:
                tool = "return_delivered_order_items"
                if pending:
                    args = pending["args"]
                else:
                    match = _ACTION.fullmatch(task["quote"])
                    args = dict(item_ids=[match["returned"]], order_id=task["target"], payment_method_id=match["method"])
            if tool in allowed:
                return response(dict(type="tool_use", id=f'call-{sum(self.calls.values())}', name=tool, input=args))
        return response(dict(type="text", text=" ".join(view["replies"]) or "I could not verify the requested details."))
