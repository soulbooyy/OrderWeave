"""Compose a retail answer from scoped task evidence, including unfinished requests."""
from core.retail_dag import Status


_LABELS = {
    "purchased_item_count": ("商品件数", "item count"), "item_ids": ("商品编号", "item IDs"),
    "order_id": ("订单编号", "order ID"), "status": ("订单状态", "order status"),
    "payment_type": ("付款方式", "payment method"), "variants": ("商品变体", "variants"),
    "ventilation": ("通风等级", "ventilation"), "sensor_type": ("传感器类型", "sensor type"),
    "color": ("颜色", "color"), "size": ("尺寸", "size"), "policy": ("适用政策", "policy"),
    "executed": ("业务操作", "requested change"),
}


def compose_tasks(state, language, note, *, by_task=None):
    """Use only the declared output of a verified node. Preserve the server's card verbatim."""
    english = language == "en"
    lines = []
    for task in state.plan.nodes:
        start = len(lines)
        run = state.runs[task.id]
        target, _ = state.target(task)
        label = _LABELS.get(task.output, ("所请求的信息", "requested information"))[english]
        subject = f"{target} " if target else ""
        if run.status == Status.SKIPPED:
            continue
        selected = task.run_if.evaluate(state.runs[task.run_if.source]) if task.run_if else None
        if selected is False and run.status in (Status.PENDING, Status.BLOCKED, Status.READY):
            continue
        if selected is True:
            if run.status == Status.WAITING_USER and state.operation.get("kind") in {"return", "exchange"}:
                kind = state.operation["kind"]
                label = kind if english else {"return": "退货", "exchange": "换货"}[kind]
            parent = state.runs[task.run_if.source]
            field = task.run_if.field
            basis = _LABELS[field][english]
            lines.append((f"With {basis} {parent.facts[field]}, your condition selects {subject}{label}."
                          if english else f"根据已核实的{basis}（{parent.facts[field]}），本次选择{subject}{label}。"))
        if run.status == Status.SUCCEEDED and run.refs and task.output in run.facts:
            value = run.facts[task.output]
            if task.output == "purchased_item_count":
                line = f"Order {target} contains {value} items." if english else f"订单 {target} 有 {value} 件商品。"
            elif task.output == "executed":
                line = (f"The requested change for {target} was submitted." if english else f"{target} 的业务申请已提交。")
            else:
                display = ", ".join(map(str, value)) if isinstance(value, list) else str(value)
                line = f"{subject}{label}: {display}." if english else f"{subject}的{label}为 {display}。"
            lines.append(line)
        elif run.status != Status.WAITING_USER:
            lines.append(f"{subject}{label} is still unverified." if english else f"{subject}的{label}尚未核实。")
        if by_task is not None:
            by_task[task.id] = lines[start:]
    if note:
        lines.append(note)
    return "\n".join(lines)
