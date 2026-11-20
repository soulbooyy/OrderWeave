"""Host-owned, turn-local sources for scoped retail DAG requests."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass

from core.context_engine import STATE_MARK
from core.retail_dag import Status
from core.retail_subtasks import OWNERS, PRIVATE, WRITES


ORDER = re.compile(r"(?<![A-Za-z0-9_])#?(W[0-9]+)(?![A-Za-z0-9_])", re.I)
CATALOG = re.compile(r"(?:\b(product|variant|item)(?: id)?\s*[:#]?\s*|(商品|产品|变体)(?:编号)?\s*)([0-9]{6,16})(?![0-9])", re.I)
ORDER_REF = re.compile(r"\b(?:that|the (?:previous|same|earlier)) order\b|(?:刚才|之前|上次)(?:的|那个|那笔|那张)?订单|那(?:个|笔|张)订单", re.I)
PRODUCT_REF = re.compile(r"\b(?:that|the previous) product\b|(?:刚才的|之前的|那个)商品", re.I)
ITEM_REF = re.compile(r"\b(?:that|the previous) (?:variant|item)\b|(?:刚才的|之前的|那个)变体", re.I)
COLOR_REF = re.compile(r"\b(?:previous|earlier|same) colou?r\b|之前(?:说过|说|选|提到)?的颜色|刚才(?:说|选)?的颜色", re.I)
COLORS = {"红色": "red", "绿色": "green", "蓝色": "blue", "黑色": "black", "白色": "white",
          "黄色": "yellow", "紫色": "purple", "粉色": "pink", "灰色": "gray"}
COLOR = re.compile(r"\b(red|green|blue|black|white|yellow|purple|pink|gray|silver|gold)\b|"
                   + "|".join(COLORS), re.I)
# Bind only a direct, first-person preference to one explicitly named entity.
ENTITY_LABEL = r"(?:(?:for|regarding)\s+)?(?:order\s*|订单\s*)?@"
PREFERENCE = re.compile(
    rf"(?:{ENTITY_LABEL}[，,：:\s]*)?(?:I (?:prefer|want|choose)\s+|我(?:更喜欢|喜欢|想要|选择))"
    rf"(?P<color>{COLOR.pattern})(?:\s+(?:for|on)\s+(?:order\s*)?@)?[.!。！\s]*", re.I)


TARGET_TOOL = {"order": "get_order_details", "product": "get_product_details", "item": "get_item_details"}


def target_type(kind, output):
    if kind in PRIVATE:
        return "order"
    if kind == "product_info":
        return "product" if output == "variants" else "item"
    return ""


def reference_type(kind, output, quote):
    if kind in WRITES and COLOR_REF.search(quote) and re.search(
            r"换成|改成|换为|\b(?:exchange|swap|change|switch)\b", quote, re.I):
        return "order"
    if kind in PRIVATE and ORDER_REF.search(quote):
        return "order"
    if kind == "product_info":
        if output == "variants" and PRODUCT_REF.search(quote):
            return "product"
        if output != "variants" and ITEM_REF.search(quote):
            return "item"
    return ""


def entities(text):
    ids = [("order", "#" + match[1].upper()) for match in ORDER.finditer(text)]
    for match in CATALOG.finditer(text):
        label = (match[1] or match[2]).lower()
        ids.append(("product" if label in {"product", "商品", "产品"} else "item", match[3]))
    return ids


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


@dataclass(frozen=True)
class Mention:
    kind: str
    target: str
    turn: int
    seq: int
    color: str = ""


@dataclass(frozen=True)
class ViewBlock:
    source: str
    sensitivity: str
    required: bool
    refs: tuple
    content: dict


@dataclass(frozen=True)
class TaskView:
    blocks: tuple[ViewBlock, ...]
    resolution: dict | None

    def messages(self):
        return [{"role": "user", "content": [
            {"type": "text", "text": json.dumps(block.content, ensure_ascii=False)}
            for block in self.blocks]}]


class TaskContextCompiler:
    """Only verified snapshot metadata and the actual user block can supply mentions."""

    def __init__(self, store, client, conv, turn, bound, message, rows, cstate, recent_turns):
        self.store = store
        self.client = client
        self.conv = conv
        self.turn = turn
        self.bound = bound
        self.message = message
        self.recent_since = turn - recent_turns
        self.has_reference = any(pattern.search(message) for pattern in (ORDER_REF, PRODUCT_REF, ITEM_REF, COLOR_REF))
        self.resolutions = {}
        self.mentions = []
        summary = cstate.get("summary") or {}
        summary_clauses = re.split(r"[.!?。！？;；\n]", summary.get("text", ""))
        for row in rows:
            if row["kind"] != "user" or row["role"] != "user" or row["turn"] >= turn:
                continue
            blocks = row["content"]
            # ContextEngine.user_blocks writes [trusted snapshot, original user text, ...extras].
            if (not isinstance(blocks, list) or len(blocks) < 2 or not isinstance(blocks[0], dict)
                    or blocks[0].get("type") != "text" or not isinstance(blocks[0].get("text"), str)
                    or not blocks[0]["text"].startswith(STATE_MARK)):
                self.mentions.clear()
                continue
            try:
                state = json.loads(blocks[0]["text"].partition("\n")[2])
            except ValueError:
                state = None
            if (not isinstance(state, dict) or state.get("turn") != row["turn"]
                    or "bound_user_id" not in state or state["bound_user_id"] != bound):
                self.mentions.clear()
                continue
            if (not isinstance(blocks[1], dict) or blocks[1].get("type") != "text"
                    or not isinstance(blocks[1].get("text"), str)):
                continue
            text = blocks[1]["text"]
            ids = set(entities(text))
            color = literal = ""
            if len(ids) == 1:
                marked = CATALOG.sub("@", ORDER.sub("@", text)).strip()
                match = PREFERENCE.fullmatch(marked)
                if match:
                    literal = match["color"].casefold()
                    color = COLORS.get(match["color"], literal)
            if COLOR.search(text):
                # A later color statement also invalidates an older preference when
                # its wording is too ambiguous to project as a new preference.
                self.mentions = [m for m in self.mentions if not m.color or (m.kind, m.target) not in ids]
            for kind, target in sorted(ids):
                # Beyond the recent window, admit only a summary preference corroborated
                # by its original user row. It may never produce an implicit target.
                if row["turn"] < self.recent_since and not (
                    color and row["turn"] <= summary.get("upto_turn", 0)
                    and any((kind, target) in entities(clause)
                            and [c[0].casefold() for c in COLOR.finditer(clause)] == [literal]
                            for clause in summary_clauses)
                ):
                    continue
                self.mentions.append(Mention(kind, target, row["turn"], row["seq"], color))

    def supports(self, kind, output, quote):
        typ = reference_type(kind, output, quote)
        if typ in {"item", "product"} and re.search(r"(?<![\w])\d{6,16}(?![\w])", quote):
            return False
        return bool(typ and not any(k == typ for k, _ in entities(self.message)))

    def record(self, kind, target, bound):
        return verified_entity(self.store, self.client, self.conv, kind, target, bound)

    def candidates(self, typ, bound):
        ids = dict.fromkeys(m.target for m in self.mentions
                            if m.kind == typ and m.turn >= self.recent_since)
        if bound != self.bound:
            return []
        return [target for target in ids if self.record(typ, target, bound)]

    def note_resolution(self, task, typ, target):
        # A quote span in the current turn or a fresh, explicit answer to choose_target.
        explicit = choice_value(typ, self.message) == target
        span_text = self.message.strip() if explicit else task.quote
        start = self.message.find(span_text)
        source = next((m for m in reversed(self.mentions)
                       if m.kind == typ and m.target == target and m.turn >= self.recent_since), None)
        self.resolutions[task.id] = dict(target=target, quote=task.quote, turn=self.turn,
                                         span=(start, start + len(span_text)) if start >= 0 else None,
                                         source=(self.turn, None) if explicit else (source.turn, source.seq))

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

    def wants_saved_color(self, task):
        return COLOR_REF.search(task.quote) and not COLOR.search(self.message)

    def write_preference(self, task, target):
        order = self.record("order", target, self.bound)
        return self._preference("order", target, task, order) if order else None

    def _preference(self, typ, target, task, record):
        items = []
        sources = {(typ, target)}
        if typ == "order":
            # A multi-product order needs an explicit purchased item for color attribution.
            items = record.get("items", [])
            mentioned = {v for k, v in entities(task.quote) if k == "item"}
            if mentioned:
                items = [item for item in items if item.get("item_id") in mentioned]
            products = {item["product_id"] for item in items if item.get("product_id")}
            if len(products) != 1:
                return None
            product = products.pop()
            sources.add(("product", product))
            sources.update(("item", item["item_id"]) for item in items)
            record = self.record("product", product, self.bound)
        eligible = [m for m in self.mentions if m.color and (m.kind, m.target) in sources]
        if not eligible:
            return None
        m = eligible[-1]
        if typ == "item":
            valid = record.get("options", {}).get("color") == m.color
        else:
            valid = any(v.get("available") and v.get("options", {}).get("color") == m.color
                        for v in record.get("variants", {}).values())
        if valid:
            preference = dict(source="user_preference", turn=m.turn, seq=m.seq, color=m.color)
            if task.kind in WRITES:
                if len(items) != 1:
                    return None
                original = items[0]
                options = dict(original.get("options", {}), color=m.color)
                matches = [key for key, variant in record.get("variants", {}).items()
                           if variant.get("available") and variant.get("options") == options
                           and key != original.get("item_id")]
                if len(matches) != 1:
                    return None
                preference["matching_variant"] = matches[0]
            return preference

    def compile(self, req, task, role, tools, packet):
        """Project attributable fields into fresh blocks. Metadata stays on the host."""
        if role != OWNERS[task.kind] or (self.client, self.conv, self.turn) != (req.user_id, req.conv_id, req.turn):
            raise ValueError("task context scope mismatch")
        assignment = packet["assignment"]
        target = assignment["target"]
        typ = target_type(task.kind, task.output)
        blocks = []
        history = []
        if TARGET_TOOL.get(typ) in tools and req.retail_user_id == self.bound:
            recent = [m for m in self.mentions if m.kind == typ and m.target == target
                      and m.turn >= self.recent_since]
            wants_color = not assignment.get("operation") and self.wants_saved_color(task)
            record = self.record(typ, target, self.bound) if recent or wants_color else {}
            if record and recent:
                m = recent[-1]
                history.append(dict(source="user_mention", turn=m.turn, seq=m.seq, target=target))
            if record and wants_color:
                preference = self._preference(typ, target, task, record)
                if preference:
                    history.append(preference)
        sensitivity = "account" if task.kind in PRIVATE else "catalog"
        if history:
            required = any(h["source"] == "user_preference" for h in history)
            blocks.append(ViewBlock("history", "account" if typ == "order" else "user", required,
                                    tuple(h["seq"] for h in history), {"history": history}))
        packet["turn"] = req.turn
        if task.quote in req.message:
            packet["user_input"] = task.quote
        elif choice_value(typ, req.message) == target:
            packet["user_input"] = req.message
        elif assignment.get("operation"):
            packet["current_user_action"] = "confirmed_displayed_operation"
        # The complete current input remains available to the server confirmation guard.
        packet["permitted_tools"] = sorted(tools)
        if req.retail_skills and "load_skill" in tools:
            required = {name: req.retail_skills.bound_skills(name) for name in sorted(tools)}
            packet["action_skills"] = {tool: names for tool, names in required.items() if names}
        refs = tuple(ref for values in assignment["input_refs"].values() for ref in values)
        blocks.append(ViewBlock("assignment", sensitivity, True, refs, packet))
        return TaskView(tuple(blocks), self.resolutions.get(task.id))
