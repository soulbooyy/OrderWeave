"""零售业务挂入工具循环的钩子插件。

每个钩子只负责一条不变量；单 Agent 与多 Agent 角色按需组合同一组插件，
不再在各角色类里分散实现。钩子只读写同一真实请求的共享 state，不能授权写入。
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import json
from typing import Optional

from core.agent_context_budget import ContextLimit, valid_tool_protocol
from core.retail_dag import Node
from core.retail_dag_evidence import verify
from core.retail_subtasks import WRITES

from agents.agent_loop import LoopHook
from agents.retail_tools import RETAIL_STATE_TOOLS, RETAIL_WRITE_TOOLS, is_execution_attempt


def _en(ctx) -> bool:
    return getattr(ctx.agent, "language", "zh") == "en"


class UnknownResultHook(LoopHook):
    """状态工具结果不明后，本请求不再调用任何工具，防止重放未知写入。"""

    name = "unknown_result"

    def pre_tool(self, ctx, name, args) -> Optional[str]:
        if not ctx.state.get("unknown_result"):
            return None
        return ("A previous tool result is unknown in this request; do not call more tools. "
                "Report the uncertainty and the safe next step." if _en(ctx) else
                "本次请求此前已有结果不明的工具调用，不能继续调用工具；请说明不确定性并给出安全的下一步。")

    def on_tool_error(self, ctx, name, args, error) -> None:
        if name in RETAIL_STATE_TOOLS:
            ctx.state["state_op_entered"] = True
            ctx.state["unknown_result"] = True


class SameTurnExecutionHook(LoopHook):
    """同一轮已生成待确认详情后，禁止执行任何已展示动作（展示与执行必须跨轮）。"""

    name = "same_turn_execution"

    def pre_tool(self, ctx, name, args) -> Optional[str]:
        if not (ctx.state.get("prepared") and is_execution_attempt(name, args, ctx.req.retail_pending)):
            return None
        return ("Pending-confirmation details were generated in this turn; do not submit or perform other "
                "business actions in the same turn. Show the complete details and wait for the user's "
                "explicit confirmation." if _en(ctx) else
                "本轮已生成待确认详情，不能在同一轮提交或执行其他业务操作；请展示完整详情并等待用户明确确认。")

    def post_tool(self, ctx, name, args, result) -> None:
        if (name in RETAIL_WRITE_TOOLS and isinstance(result, dict) and result.get("success") is True
                and result.get("action_status") == "pending_confirmation"):
            ctx.state["prepared"] = True
            if ctx.req.dag_assignment:
                ctx.state["stop_response"] = result["display_text"]


class TaskPermissionHook(LoopHook):
    """Check the role and node scope again immediately before invoking a tool."""

    name = "task_permission"

    def pre_tool(self, ctx, name, args) -> Optional[str]:
        assignment = ctx.req.dag_assignment
        if name in ctx.agent._tools_for_request(ctx.req):
            if assignment is None:
                if (ctx.req.dag_tool_limit is not None and ctx.req.dispatch is None
                        and name in RETAIL_WRITE_TOOLS and not is_execution_attempt(name, args, ctx.req.retail_pending)):
                    return ("Use the exact displayed operation." if _en(ctx) else "只能续办参数与已展示详情一致的操作。")
                return None
            if name == "modify_user_address":
                actual_target = ctx.req.retail_user_id
            elif name in RETAIL_WRITE_TOOLS or name == "get_order_details":
                actual_target = args.get("order_id")
            elif name in {"get_item_details", "get_product_details"}:
                actual_target = args.get("item_id" if name == "get_item_details" else "product_id")
            else:
                return None
            if actual_target and actual_target == assignment.get("target"):
                return None
        return ("This role or task does not permit that tool or target." if _en(ctx)
                else "当前角色或任务无权对该目标调用此工具。")


class ConfirmationStopHook(LoopHook):
    """确认被服务端拒绝后立即结束本角色：本轮用户消息不变，继续调用不能取得新授权。"""

    name = "confirmation_stop"

    def post_tool(self, ctx, name, args, result) -> None:
        if (name in RETAIL_WRITE_TOOLS and isinstance(result, dict)
                and result.get("error_code") == "confirmation_required"):
            ctx.state["stop_response"] = result["confirmation_hint"]

    def stop_response(self, ctx) -> Optional[str]:
        return ctx.state.get("stop_response")


class WriteStateHook(LoopHook):
    """记录本请求是否进入状态操作、已执行写入或已交接，供编排降级判断。"""

    name = "write_state"

    def post_tool(self, ctx, name, args, result) -> None:
        if name in RETAIL_STATE_TOOLS:
            ctx.state["state_op_entered"] = True
        success = isinstance(result, dict) and result.get("success") is True
        if name in RETAIL_WRITE_TOOLS and success and result.get("action_status") != "pending_confirmation":
            ctx.state["submitted"] = True
        elif name == "transfer_to_human_agents" and success:
            ctx.state["handoff"] = True


class SkillGateHook(LoopHook):
    """写工具绑定 Skill：本会话尚未加载对应 Skill 时拒绝调用，确保执行前读过该动作的政策原文。"""

    name = "skill_gate"

    def pre_tool(self, ctx, name, args) -> Optional[str]:
        library = getattr(ctx.req, "retail_skills", None)
        if library is None:
            return None
        bound = library.bound_skills(name)
        loaded = set(getattr(ctx.req, "loaded_skills", None) or ()) | set(ctx.state.get("loaded_skills") or ())
        if not bound or loaded & set(bound):
            return None
        from core.retail_skills import SKILL_TOOL

        return (f"Call {SKILL_TOOL} with name '{bound[0]}' first to read this action's policy and procedure; "
                "nothing was changed." if _en(ctx) else
                f"请先调用 {SKILL_TOOL}（name='{bound[0]}'）阅读该动作的政策与流程；本次未做任何修改。")

    def post_tool(self, ctx, name, args, result) -> None:
        from core.retail_skills import SKILL_TOOL

        if name == SKILL_TOOL and isinstance(result, dict) and result.get("success"):
            loaded = ctx.state.setdefault("loaded_skills", [])
            if result["skill"] not in loaded:
                loaded.append(result["skill"])


class OpenRequestsHandoffHook(LoopHook):
    """仍有可办理的请求时，转人工前先提醒一次；同一组请求已提醒过、用户仍坚持时放行。

    只是建议性拦截：被拦下的调用确定未执行，不影响任何授权判断。
    """

    name = "open_requests_handoff"
    advisory = True

    def pre_tool(self, ctx, name, args) -> Optional[str]:
        if name != "transfer_to_human_agents":
            return None
        goals = [g for g in getattr(ctx.req, "retail_plan", None) or [] if not g.get("nudged")]
        if not goals or ctx.state.get("plan_handoff_nudged"):
            return None
        from agents.retail_plan import handoff_feedback

        ctx.state["plan_handoff_nudged"] = [g["id"] for g in goals]
        # 记在请求上（单 Agent 无共享 state），轮末持久化为已提醒，用户坚持时下一次放行。
        ctx.req.plan_handoff_nudged = list(ctx.state["plan_handoff_nudged"])
        return handoff_feedback(goals, _en(ctx))


_FEEDBACK = {
    "unverified_labeled_order_fact": "It states order facts (order ids, amounts or statuses) that do not match this "
        "customer's verified order records. Mention only the verified customer's orders, use amounts and statuses "
        "exactly as recorded, and do not repeat details of orders that are not theirs.",
    "inconsistent_order_status": "It gives an order status that differs from the order record.",
    "ambiguous_order_status": "It gives an order status without making clear which order it belongs to.",
    "unsupported_submission_claim": "It says an action was submitted or carried out, but no such action is recorded "
        "in this conversation.",
    "unsupported_handoff_claim": "It says a human handoff was recorded, but none is recorded in this conversation.",
    "unsupported_identity_claim": "It says the identity is verified, but no identity is verified in this conversation.",
    "unverified_external_status": "It asserts refund arrival, warehouse receipt or email delivery, which cannot be "
        "verified here; say these are not verified.",
}
REVISABLE = frozenset(_FEEDBACK)


class PublishCheckHook(LoopHook):
    """发布前校验：用与发布守卫相同的不变量检查草稿，发现问题时给出具体反馈让模型改写一次。

    守卫仍在服务层作为最后一道防线；这里只把“替换成固定话术”前移为“说明问题、由模型修订”。
    """

    name = "publish_check"

    def pre_publish(self, ctx, text, tool_traces) -> Optional[str]:
        check = getattr(ctx.req, "publish_check", None)
        if check is None or not text.strip():
            return None
        findings = [f for f in check(text, tool_traces) if f in REVISABLE]
        if not findings:
            return None
        problems = "\n".join(f"- {_FEEDBACK[f]}" for f in dict.fromkeys(findings))
        return ("[Reply check by the system, not the customer] Your draft reply was not sent. Problems:\n"
                f"{problems}\nRewrite the complete reply to the customer, fixing only these points.")


class AgentContextBudgetHook(LoopHook):
    """One instance per role loop. Only a successful send publishes its private prefix."""

    name = "agent_context_budget"

    def pre_model(self, ctx, request):
        transport = ctx.agent._client.messages
        guard = getattr(transport, "context_guard", None)
        if guard is None or guard.mode == "off":
            return
        raw = request["messages"]
        view = ctx.visible_prefix + raw[ctx.raw_cursor:] if ctx.visible_prefix is not None else raw
        kwargs = transport.final_kwargs(request)

        def measure(messages):
            return guard.measure({**kwargs, "messages": messages})

        before = current = measure(view)
        actions = []
        if not valid_tool_protocol(view):
            current = replace(current, reason="invalid_tool_protocol")
        elif current.reason == "window_exceeded" and getattr(ctx.req, "dag_assignment", None):
            view = deepcopy(view)
            allowed = {tool["name"] for tool in request.get("tools", [])}
            for action in self._reductions(ctx, view, allowed):
                actions.append(action)
                current = measure(view)
                if current.reason != "window_exceeded":
                    break
        event = guard.decision(transport.stage, current, "role", before=before, actions=actions,
                               details=self._scope_metrics(ctx.req, view))
        ctx.spans.append(event)
        if guard.mode == "enforce":
            if current.reason:
                raise ContextLimit(event)
            if actions or ctx.visible_prefix is not None:
                request["messages"] = view
            ctx.remember_view = True
            ctx.projected_view = ctx.projected_view or bool(actions)

    @staticmethod
    def _scope_metrics(req, view):
        """UTF-8 sizes give attribution. Only the complete request determines the token limit."""
        assignment = getattr(req, "dag_assignment", None)
        if not assignment or not view or not all(isinstance(m, dict) for m in view):
            return {}
        def size(value):
            return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())

        metrics = {"task": size({k: v for k, v in assignment.items() if k not in {"inputs", "input_refs"}}),
                   "dependencies": size([assignment.get("inputs", {}), assignment.get("input_refs", {})]),
                   "tool_exchanges": sum(size(c) + size(r) for c, r, _ in _read_pairs(view))}
        background = _background(req, view)
        if background:
            metrics["recent_conversation"] = size(background.get("recent_conversation", []))
            metrics["persistent_summary"] = size(background.get("history_summary", ""))
        ticket = getattr(req, "dag_ticket", None)
        return dict(component_bytes=metrics, plan_id=ticket[0] if ticket else None,
                    task_id=assignment.get("id"), attempt=getattr(req, "dag_attempt", None))

    @staticmethod
    def _reductions(ctx, view, tool_names):
        req = ctx.req
        assignment = req.dag_assignment
        if assignment["kind"] in WRITES:
            return
        background = _background(req, view)
        if background and not req.retail_pending:
            block = view[0]["content"][0]
            if background.pop("history_summary", None):
                background["omitted"] = True
                block["text"] = json.dumps(background, ensure_ascii=False)
                yield "omit_summary"
            turns = background.get("recent_conversation", [])
            while turns:
                turns.pop(0)
                background["omitted"] = True
                block["text"] = json.dumps(background, ensure_ascii=False)
                yield "omit_oldest_turn"
        if not assignment.get("output"):
            return
        node = Node(assignment["id"], assignment["kind"], assignment["quote"], (),
                    assignment["output"], assignment["target"], None)
        allowed = tool_names.intersection(req.dag_tool_limit or (), _PROJECTABLE_READS)
        reads = [(call, block, current) for call, block, current in _read_pairs(view) if call["name"] in allowed]
        for call, block, _ in reads:
            projection = _verified_projection(node, call, block, req.retail_user_id)
            if projection and _shorten(block, projection):
                yield "project_read_result"
        if len(reads) > 1 and req.dag_log_mode == "j":
            yield from _reference_duplicates(ctx, node, reads, allowed)


_PROJECTABLE_READS = {"get_order_details", "get_product_details", "get_item_details", "search_knowledge_base"}


def _background(req, view):
    # Only block zero of a host-compiled Phase 1 task packet has this provenance.
    blocks = view[0].get("content") if view and isinstance(view[0], dict) else None
    if not (getattr(req, "conversation_context", None) and isinstance(blocks, list) and len(blocks) == 2
            and isinstance(blocks[0], dict) and blocks[0].get("type") == "text"):
        return None
    try:
        data = json.loads(blocks[0]["text"])
    except (KeyError, ValueError, TypeError):
        return None
    if (isinstance(data, dict) and set(data) <= {"recent_conversation", "history_summary", "omitted"}
            and isinstance(data.get("recent_conversation", []), list)):
        return data


def _read_pairs(view):
    for assistant, user in zip(view, view[1:]):
        if assistant.get("role") != "assistant" or user.get("role") != "user":
            continue
        uses, results = assistant.get("content"), user.get("content")
        if isinstance(uses, list) and isinstance(results, list):
            calls = [b for b in uses if isinstance(b, dict) and b.get("type") == "tool_use"]
            for call, result in zip(calls, results):
                if isinstance(result, dict) and result.get("tool_use_id") == call.get("id"):
                    yield call, result, user is view[-1]


def _verified_projection(node, call, block, bound):
    try:
        trace = dict(tool_use_id=call["id"], tool_name=call["name"], input=call["input"],
                     result_success=True, result=json.loads(block["content"]))
        evidence = verify(node, [trace], bound_user=bound, target=node.target)
    except (KeyError, TypeError, ValueError, AttributeError):
        return None  # An unfamiliar read result has no safe projection.
    if evidence:
        return dict(omitted=True, source=call["name"], target=node.target, facts=evidence.facts, refs=evidence.refs)


def _shorten(block, projection):
    compact = json.dumps(projection, ensure_ascii=False, separators=(",", ":"))
    if len(compact.encode()) >= len(block["content"].encode()):
        return False
    block["content"] = compact
    return True


def _reference_duplicates(ctx, node, reads, allowed):
    req = ctx.req
    # Re-check the live claim before using an older result by reference. The
    # reader's original is used for verification only and never enters the view.
    stored = req.retail_store.task_tool_messages(req.user_id, req.conv_id, req.dag_ticket,
                                                 ctx.agent.agent_type.value, allowed)
    originals = {call["id"]: (call, block) for call, block, _ in _read_pairs(stored)}
    seen, duplicates = set(), []
    for call, block, current in reversed(reads):
        original = originals.get(call["id"])
        if not original or original[0] != call:
            continue
        projection = _verified_projection(node, call, original[1], req.retail_user_id)
        if not projection:
            continue
        key = json.dumps([call["name"], call["input"], projection["facts"]], sort_keys=True)
        if key in seen and not current:
            duplicates.append((call, block))
        seen.add(key)
    for call, block in reversed(duplicates):
        if _shorten(block, dict(omitted=True, source=call["name"], ref="tool:" + call["id"])):
            yield "reference_old_read"



def single_agent_hooks():
    return [SkillGateHook(), SameTurnExecutionHook(), OpenRequestsHandoffHook(), ConfirmationStopHook(),
            PublishCheckHook(), AgentContextBudgetHook()]


def role_hooks():
    # 顺序即拒绝优先级：未知结果最先，其次跨轮执行，再角色权限；清单提醒只在其余检查通过后生效。
    return [UnknownResultHook(), SameTurnExecutionHook(), TaskPermissionHook(), SkillGateHook(),
            OpenRequestsHandoffHook(), ConfirmationStopHook(), WriteStateHook(), PublishCheckHook(), AgentContextBudgetHook()]


__all__ = ["UnknownResultHook", "SameTurnExecutionHook", "TaskPermissionHook", "ConfirmationStopHook",
           "WriteStateHook", "SkillGateHook", "OpenRequestsHandoffHook", "PublishCheckHook", "REVISABLE",
           "AgentContextBudgetHook", "single_agent_hooks", "role_hooks"]
