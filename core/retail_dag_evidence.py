"""Verify a single task attempt from server tool traces and scoped business events."""
from __future__ import annotations

from dataclasses import dataclass

from core.retail_dag import Node
from mcp_tools.retail_domain import WRITE_KINDS, DomainError, canonical_args
from mcp_tools.retail_store import RetailStore, normalize_order_id


@dataclass(frozen=True)
class Evidence:
    facts: dict
    refs: tuple[str, ...]


def _order_value(field, order, result):
    if field in {"order_id", "status"}:
        return order.get(field)
    if field == "payment_type":
        return result.get("original_payment_description")
    items = order.get("items")
    if not isinstance(items, list):
        return None
    if field == "purchased_item_count":
        return len(items)
    if field == "item_ids" and all(isinstance(i, dict) and i.get("item_id") for i in items):
        return [i["item_id"] for i in items]
    return None


def _catalog_value(field, name, args, result, target):
    if name == "get_product_details":
        product = result.get("product") or {}
        if product.get("product_id") != args.get("product_id") or not product.get("product_id"):
            return None
        variants = product.get("variants") or {}
        if field == "variants" and target == product["product_id"]:
            return list(variants) if variants else None
        item = variants.get(target) or {}
    elif name == "get_item_details" and args.get("item_id") == target:
        item = result.get("item") or {}
    else:
        return None
    if item.get("item_id") == target and field in {"ventilation", "sensor_type", "size", "color"}:
        value = (item.get("options") or {}).get(field.replace("_", " "))
        return value if isinstance(value, str) and value.strip() else None
    return None


def _write_evidence(task, name, args, result, events, target, operation):
    kind = WRITE_KINDS.get(name)
    if not kind or result.get("kind") != kind or result.get("action_status") != "executed":
        return None
    if (kind in {"return", "exchange"}) != (task.kind == "return_change"):
        return None
    op_id = result.get("op_id")
    if (not op_id or not operation or operation.get("op_id") != op_id
            or operation.get("kind") != kind):
        return None
    try:
        canonical = canonical_args(kind, args)
    except DomainError:
        return None
    if operation.get("args") != canonical:
        return None
    if kind == "modify_user_address":
        if result.get("user_id") != target:
            return None
    elif normalize_order_id(target) != canonical.get("order_id"):
        return None
    for event in events:
        payload = event.get("payload") or {}
        if (event.get("event_id") and event.get("op_id") == op_id
                and event.get("event_type") == RetailStore.executed_event(kind)
                and event.get("order_id") == canonical.get("order_id")
                and payload.get("kind") == kind and payload.get("args") == canonical):
            return Evidence({"executed": True, "op_id": op_id}, (f"event:{event['event_id']}",))
    return None


def verify(task: Node, traces, *, bound_user=None, target="", events=(), operation=None):
    """Caller supplies this attempt's traces/events from the same authorized conversation.

    A resolved prerequisite may supply an otherwise unknown target. It cannot
    override a literal target already locked into the user's task. For writes,
    operation is the expected op_id, kind and canonical args from the server.
    """
    if not task.output or (target and task.target and target != task.target):
        return None
    target = task.target or target
    if not target:
        return None
    for trace in traces:
        result, args = trace.get("result"), trace.get("input")
        if (not trace.get("tool_use_id") or trace.get("result_success") is not True
                or trace.get("success") is False or trace.get("blocked_by_guard") or trace.get("skipped")
                or trace.get("not_executed") or not isinstance(result, dict)
                or result.get("success") is not True or not isinstance(args, dict)):
            continue
        name = trace.get("tool_name")
        value = None
        if task.kind in {"order_lookup", "billing_lookup"} and name == "get_order_details":
            order = result.get("order") or {}
            if (bound_user and bound_user == order.get("user_id") and args.get("order_id")
                    and normalize_order_id(target) == normalize_order_id(args["order_id"]) == order.get("order_id")):
                value = _order_value(task.output, order, result)
        elif task.kind == "product_info":
            value = _catalog_value(task.output, name, args, result, target)
        elif task.kind in {"return_policy", "general_policy"} and name == "search_knowledge_base":
            for entry in result.get("results") or []:
                if entry.get("evidence_id") == target and entry.get("content"):
                    return Evidence({"policy": entry["content"]}, (f"knowledge:{target}",))
        elif task.kind in {"return_change", "order_change"} and bound_user:
            found = _write_evidence(task, name, args, result, events, target, operation)
            if found:
                return found
        if value is not None:
            return Evidence({task.output: value}, (f"tool:{trace['tool_use_id']}",))
    return None
