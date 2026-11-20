"""One authorized conversation window per external turn, shared by planner and task attempts."""
import json
import re
from itertools import groupby

from core.context_engine import STATE_MARK
from core.retail_dag import Status
from core.retail_subtasks import OWNERS, PRIVATE


DEFAULT_RECENT_TURNS = 4
ORDER = re.compile(r"(?<![A-Za-z0-9_])#?(W[0-9]+)(?![A-Za-z0-9_])", re.I)
CATALOG_ID = re.compile(r"(?<![A-Za-z0-9_])[0-9]{6,16}(?![A-Za-z0-9_])")


def target_type(kind, output):
    if kind in PRIVATE:
        return "order"
    if kind == "product_info":
        return "product" if output == "variants" else "item"
    return ""


def choice_value(kind, message):
    text = message.strip()
    if kind == "order" and (match := ORDER.fullmatch(text)):
        return "#" + match[1].upper()
    if kind in {"item", "product"} and re.fullmatch(r"[0-9]{6,16}", text):
        return text
    return None


def verified_entity(store, client, conv, kind, target, bound):
    if kind == "order":
        if not bound or store.bound_user_id(client, conv) != bound:
            return {}
        order = store.get_order_details(client, conv, target).get("order") or {}
        return order if order.get("user_id") == bound else {}
    if kind in {"product", "item"}:
        read = store.get_product_details if kind == "product" else store.get_item_details
        return read(client, conv, target).get(kind) or {}
    return {}


def identifiers(kind, text):
    if kind == "order":
        return {"#" + m[1].upper() for m in ORDER.finditer(text)}
    return set(CATALOG_ID.findall(text)) if kind in {"product", "item"} else set()


def user_snapshot(rows, turn):
    users = [r for r in rows if r.get("kind") == "user" and r.get("role") == "user"]
    if len(users) != 1:
        return None
    try:
        blocks = users[0]["content"]
        if (blocks[0]["type"] != "text" or not blocks[0]["text"].startswith(STATE_MARK)
                or blocks[1]["type"] != "text" or not isinstance(blocks[1]["text"], str)):
            return None
        state = json.loads(blocks[0]["text"].partition("\n")[2])
        if isinstance(state, dict) and state.get("turn") == turn and "bound_user_id" in state:
            return users[0]["seq"], blocks[1]["text"], state
    except (KeyError, IndexError, TypeError, AttributeError, ValueError):
        pass
    return None


def published_reply(rows, user_seq):
    finals = [r for r in rows if r.get("kind") == "assistant" and r.get("role") == "assistant"]
    if len(finals) != 1:
        return None
    final = finals[0]
    blocks = final.get("content")
    if (final.get("agent") or final["seq"] <= user_seq or not isinstance(blocks, list) or len(blocks) != 1
            or not isinstance(blocks[0], dict) or blocks[0].get("type") != "text"):
        return None
    text = blocks[0].get("text")
    return text if isinstance(text, str) and text.strip() else None


class ConversationContextBuilder:
    def __init__(self, store, client, conv, turn, bound, rows, cstate, window=DEFAULT_RECENT_TURNS, *, displayed=False):
        if window not in (3, 4, 5):
            raise ValueError("Recent conversation window must be 3, 4 or 5 external turns")
        self.store, self.client, self.conv = store, client, conv
        self.turn, self.bound, self.cstate = turn, bound, cstate
        self.business_state = {"identity_verified": bool(bound), "has_displayed_operation": displayed}
        self.proposals = {}
        self.audience = None
        self.completed = []
        self.boundary = 0
        for n, group in groupby(sorted((r for r in rows if r["turn"] < turn), key=lambda r: r["turn"]),
                                key=lambda r: r["turn"]):
            entries = list(group)
            if not any(r.get("kind") in {"user", "assistant"} for r in entries):
                continue
            snapshot = user_snapshot(entries, n)
            state = snapshot[2] if snapshot else {}
            # Older anonymous snapshots cannot establish who saw the final reply.
            if ("bound_user_id" not in state or state["bound_user_id"] != bound
                    or (not bound and "final_bound_user_id" not in state)
                    or state.get("final_bound_user_id", bound) != bound):
                self.completed.clear()
                self.boundary = n
                continue
            seq, user, _ = snapshot
            reply = published_reply(entries, seq)
            if user.strip() and reply:
                self.completed.append(dict(turn=n, audience=state.get("dialogue_audience"),
                                           user=user, assistant=reply))
        self.recent = self.completed[-window:]

    def summary_scope(self, cutoff):
        scopes = [t["audience"] for t in self.completed if t["turn"] <= cutoff]
        return dict(format="visible-v1", bound=self.bound, since=self.boundary,
                    audience=scopes[0] if scopes and all(s == scopes[0] for s in scopes) else None)

    def background(self, audience=None):
        summary = self.cstate.get("summary") or {}
        scope = summary.get("scope") if isinstance(summary, dict) else None
        background = {}
        if (isinstance(scope, dict) and scope.get("format") == "visible-v1" and scope.get("bound") == self.bound
                and scope.get("since") == self.boundary and isinstance(summary.get("upto_turn"), int)
                and self.boundary < summary["upto_turn"] < self.turn and isinstance(summary.get("text"), str)
                and (audience is None or scope.get("audience") == audience) and summary["text"].strip()):
            background["history_summary"] = summary["text"]
        background["recent_conversation"] = [
            dict(turn=t["turn"], user=t["user"], assistant=t["assistant"])
            for t in self.recent if audience is None or t["audience"] == audience
        ]
        return background

    def candidates(self, typ, bound):
        if bound != self.bound:
            return []
        # Match identifier syntax only. Intent, corrections and references belong to the planner.
        ids = set()
        for t in self.recent:
            ids.update(identifiers(typ, t["user"]))
            ids.update(identifiers(typ, t["assistant"]))
        return sorted(v for v in ids if self.record(typ, v, bound))

    def record(self, kind, target, bound):
        return verified_entity(self.store, self.client, self.conv, kind, target, bound)

    def permits_inputs(self, execution, task, tools, target):
        for parent in execution.plan.nodes:
            if parent.id not in task.depends_on:
                continue
            run = execution.runs[parent.id]
            if run.status == Status.SKIPPED:
                continue  # Execution.next_node already checked the complementary branch.
            if run.status != Status.SUCCEEDED or not run.refs or parent.output not in run.facts:
                return False
            if parent.kind not in PRIVATE:
                continue
            if parent.kind in {"order_lookup", "billing_lookup"} and not self.record(
                    "order", execution.target(parent)[0], execution.bound_user):
                return False
            if (task.kind in PRIVATE and "get_order_details" in tools
                    and execution.target(parent)[0] == target
                    and (parent.output != "payment_type" or OWNERS[task.kind] == "billing")):
                continue
            if (task.kind == "product_info" and task.output != "variants"
                    and parent.output == "item_ids" and "get_item_details" in tools
                    and target in run.facts["item_ids"]):
                continue
            if task.run_if and task.run_if.source == parent.id and parent.output == "purchased_item_count":
                continue
            return False
        return True

    def messages(self, req, task, role, tools, packet):
        if (role != OWNERS[task.kind] or (req.user_id, req.conv_id, req.turn) != (self.client, self.conv, self.turn)
                or req.retail_user_id != self.store.bound_user_id(self.client, self.conv)):
            raise ValueError("conversation scope changed")
        assignment = packet.pop("assignment")
        audience = [role, task.kind, assignment["target"]]
        background = self.background(audience) if req.retail_user_id == self.bound else {}
        packet["permitted_tools"] = sorted(tools)
        if req.retail_skills and "load_skill" in tools:
            packet["action_skills"] = {n: skills for n in sorted(tools) if (skills := req.retail_skills.bound_skills(n))}
        packet["turn"] = req.turn
        packet["assignment"] = {**assignment, "depends_on": list(task.depends_on)}
        if task.quote in req.message:
            packet["user_input"] = task.quote
        elif choice_value(target_type(task.kind, task.output), req.message) == assignment["target"]:
            packet["user_input"] = req.message
        elif assignment.get("operation"):
            packet["current_user_action"] = "confirmed_displayed_operation"
        blocks = [dict(type="text", text=json.dumps(value, ensure_ascii=False)) for value in (background, packet)]
        return [dict(role="user", content=blocks)]
