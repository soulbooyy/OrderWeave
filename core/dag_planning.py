"""Compile one untrusted JSON object into the existing executable DAG representation."""
import json
import re

from pydantic import ValidationError

from core.dag_schema import DagResponse, OUTPUTS
from core.retail_dag import CONDITIONAL, Plan, condition_sentences

ORDER = re.compile(r"[#＃]?W\d+", re.I)
CATALOG_ID = re.compile(r"\d+")
CONDITION_MARKER = re.compile(r"\b(?:if|unless|otherwise|else)\b|如果|若|否则", re.I)


class PlanError(ValueError):
    def __init__(self, stage, issues):
        self.stage = stage
        self.issues = issues
        super().__init__(issues[0]["code"])


def reject(code, path, message):
    raise PlanError("semantic_error", [dict(code=code, path=path, reason=message)])


def target_outputs(kind, output):
    if kind == "product_info":
        return set() if output == "variants" else {"item_ids", "variants"}
    return {"order_id"}


def compile_nodes(data, message, *, proposals=None):
    rows = data["subtasks"]
    if not rows:
        if CONDITION_MARKER.search(message) or data["intent"] in {"order_status", "refund"}:
            reject("missing_actions", "subtasks", "Provide the requested actions and conditional branches.")
        return
    clauses = [(part, match.groups()) for part in condition_sentences(message)
               if (match := CONDITIONAL.fullmatch(part))]
    for _, (predicate, yes, no) in clauses:
        if any(CONDITION_MARKER.search(part) for part in (predicate, yes, no) if part):
            reject("nested_condition", "subtasks", "Nested conditionals cannot be represented by one branch guard.")
    producers = {row["id"]: row["output"] for row in rows}
    seen = set()
    attached = set()
    for i, row in enumerate(rows):
        path = f"subtasks.{i}"
        kind, quote, target = row["kind"], row["quote"], row["target"]
        if row["id"] in seen:
            reject("duplicate_id", path+".id", "Node IDs must be unique.")
        seen.add(row["id"])
        if any(parent not in producers for parent in row["depends_on"]):
            reject("unknown_dependency", path+".depends_on", "Reference an existing task.")
        if len(set(row["depends_on"])) != len(row["depends_on"]):
            reject("duplicate_dependency", path+".depends_on", "List each direct predecessor once.")
        if quote not in message:
            reject("unverified_subtask_quote", path+".quote", "Quote the current user message verbatim.")
        if kind in {"order_lookup", "billing_lookup", "return_change", "order_change"}:
            # The default-address tool targets a user instead of an order.
            profile = kind == "order_change" and re.search(r"default address|默认地址", quote, re.I)
            if target and not profile and not ORDER.fullmatch(target):
                reject("expected_order_id", path+".target", "Use the order ID, not the purchased item or payment ID.")
        if target and not re.search(r"(?<![a-zA-Z0-9_-])"+re.escape(target)+r"(?![a-zA-Z0-9_-])", quote):
            reject("unverified_target", path+".target", "The quoted action must contain the complete literal target.")
        if kind == "product_info" and target:
            if not CATALOG_ID.fullmatch(target):
                reject("expected_catalog_id", path+".target", "Use a product or variant identifier.")
            label = re.search(r"(?:\b(product|variant|item)(?:\s+id)?|(?:商品|变体)(?:编号)?)\s*[:#]?\s*"
                              + re.escape(target) + r"(?!\d)", quote, re.I)
            if label and (row["output"] == "variants") != (label[0].lower().startswith(("product", "商品"))):
                reject("wrong_catalog_target", path+".target", "Use the product for variants and the variant for its specification.")
        history_target = proposals is not None and row["id"] in proposals
        if history_target:
            from core.task_context import identifiers, target_type
            typ = target_type(kind, row["output"])
            allowed = target_outputs(kind, row["output"])
            if (not typ or target or identifiers(typ, quote)
                    or any(producers.get(p) in allowed for p in row["depends_on"])):
                reject("conflicting_history_target", path+".history_target", "Use one target source.")
        if not target and kind not in {"return_policy", "general_policy"}:
            allowed = target_outputs(kind, row["output"])
            has_source = any(producers.get(p) in allowed for p in row["depends_on"])
            if not has_source and not history_target:
                reject("unbound_target", path+".target", "Supply a literal target or a compatible direct predecessor.")
        guard = row.get("run_if")
        if guard is None:
            if any(action and action in quote for _, (_, yes, no) in clauses for action in (yes, no)):
                reject("unguarded_branch_action", path+".run_if", "A conditional action cannot also run unconditionally.")
            continue
        side = 1 if guard["branch"] == "then" else 2
        matches = [(part, parts) for part, parts in clauses if parts[side] == quote]
        if len(matches) != 1:
            reject("unverified_branch", path+".run_if", "The exact action must identify one clause and the declared branch.")
        clause = matches[0][0]
        identity = (clause, side)
        if identity in attached:
            reject("duplicate_branch", path+".run_if", "Only one action may occupy this branch.")
        attached.add(identity)
        if guard["source"] not in row["depends_on"]:
            reject("invalid_condition_source", path+".run_if.source", "The source must be a direct predecessor.")
        if sum(other["output"] == producers[guard["source"]] and not other.get("run_if") for other in rows) > 1:
            reject("ambiguous_condition_source", path+".run_if.source",
                   "The predicate does not distinguish multiple producers of this field.")
        row["run_if"] = {"source": guard["source"], "quote": clause}
    for clause, (_, yes, no) in clauses:
        for side, action in ((1, yes), (2, no)):
            if action and (clause, side) not in attached:
                reject("missing_branch_action", "subtasks", "Every stated branch needs its own guarded action.")
    try:
        Plan.for_execution(rows, message, "validation", "validation")
    except ValueError as exc:
        code = str(exc)
        path = "subtasks"
        if code == "cyclic_dependencies" and exc.__cause__:
            cycle = exc.__cause__.args[1]
            path = f"subtasks.{next(i for i,r in enumerate(rows) if r['id'] == cycle[0])}.depends_on"
        if hasattr(exc, "task_id"):
            index = next(i for i, row in enumerate(rows) if row["id"] == exc.task_id)
            path = f"subtasks.{index}.run_if"
            if code == "unavailable_condition_field":
                source = rows[index]["run_if"]["source"]
                path = f"subtasks.{next(i for i,r in enumerate(rows) if r['id'] == source)}.output"
        reject(code, path, "Executable DAG validation failed: "+code)


def unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key: "+key)
        result[key] = value
    return result


def parse_plan(raw, message, *, conversation_context=None):
    try:
        obj = json.loads(raw, object_pairs_hook=unique_keys, parse_constant=invalid_constant)
    except (ValueError, RecursionError) as exc:
        raise PlanError("json_error", [dict(code="invalid_json", path="$", reason=str(exc))]) from exc
    proposals = {}
    if conversation_context is not None and isinstance(obj, dict) and isinstance(obj.get("subtasks"), list):
        for i, row in enumerate(obj["subtasks"]):
            if isinstance(row, dict) and "history_target" in row and isinstance(row.get("id"), str):
                value = row.pop("history_target")
                if not isinstance(value, str) or len(value) > 128:
                    reject("invalid_history_target", f"subtasks.{i}.history_target", "Expected an optional ID string.")
                proposals[row["id"]] = value
    try:
        data = DagResponse.model_validate(obj).model_dump(exclude_none=True, exclude_unset=True)
    except ValidationError as exc:
        issues = []
        for error in exc.errors(include_input=False, include_url=False):
            # Pydantic inserts the discriminator tag into the location; it isn't a JSON key.
            loc = list(error["loc"])
            if len(loc) >= 3 and loc[0] == "subtasks" and loc[2] in OUTPUTS:
                del loc[2]
            issues.append(dict(code=error["type"], path=".".join(map(str,loc)) or "$", reason=error["msg"]))
        raise PlanError("structure_error", issues) from exc
    compile_nodes(data, message, proposals=proposals)
    if conversation_context is not None:
        conversation_context.proposals = proposals
    return data


def invalid_constant(value):
    raise ValueError("Invalid JSON number: "+value)


def output_instruction(*, history_targets=False):
    schema = DagResponse.model_json_schema()
    if history_targets:
        for node in schema["$defs"].values():
            if "target" in node.get("properties", {}):
                node["properties"]["history_target"] = {"type": "string", "maxLength": 128,
                                                        "description": "Proposed historical ID. Empty if ambiguous."}
    example = DagResponse.model_validate({
        "intent": "query", "confidence": 0.95, "reasoning": "A rating selects one of two lookups.",
        "subtasks": [
            dict(id="a", kind="product_info", quote="Read the ventilation of variant 10101",
                 depends_on=[], output="ventilation", target="10101"),
            dict(id="b", kind="order_lookup", quote="check the status of order #W42",
                 depends_on=["a"], output="status", target="#W42", run_if={"source":"a","branch":"then"}),
            dict(id="c", kind="billing_lookup", quote="find the payment type for order #W42",
                 depends_on=["a"], output="payment_type", target="#W42", run_if={"source":"a","branch":"else"}),
        ]})
    return ("Return exactly one JSON object, without Markdown or commentary. Use this schema and example. "
            "Plan only current requests. Exclude negated or abandoned actions. A policy question is a read, "
            "not a transaction. For an explicit conditional request retain every branch, including a conditional write. "
            "Quote each branch action exactly, without its predicate, separators or final punctuation. "
            "Reference the fact-producing predecessor and mark then or else. Never infer the actual branch outcome. "
            "Reporting facts to the user is not another lookup. An unsupported or social request may have an empty list.\n"
            + json.dumps(schema, ensure_ascii=False, separators=(",",":"))
            + "\nExample for: Read the ventilation of variant 10101. If the ventilation level is high, check "
              "the status of order #W42; otherwise find the payment type for order #W42.\n"+example.model_dump_json(exclude_unset=True))
