"""Execute one verified retail DAG node at a time on either workflow backend."""
from dataclasses import replace
import json
import re
import time

from agents.agent_orchestrator import AgentType, OrchestratorResult, _stop_exception
from agents.retail_tools import TASK_TOOLS
from core.retail_dag import Status
from core.intent_recognizer import IntentCategory
from core.retail_dag_evidence import verify
from core.retail_subtasks import OWNERS, PRIVATE, WRITES
from mcp_tools.retail_domain import WRITE_KINDS


class DagRunner:
    def __init__(self, orchestrator, req, execution):
        self.router = orchestrator
        self.req = req
        self.state = execution
        self.task = None
        self.responses = []
        self.started = time.monotonic()
        self.note = ""

    @classmethod
    def resume(cls, orchestrator, req):
        """Only a validated reply to the specific outstanding question resumes work."""
        from mcp_tools.retail_store import evaluate_confirmation
        store = req.retail_store
        text = req.message.strip()
        for state in store.open_dags(req.user_id, req.conv_id):
            driver = cls(orchestrator, req, state)
            if state.bound_user and state.bound_user != req.retail_user_id:
                if state.operation:
                    store.cancel_dag_operation(req.user_id, req.conv_id, state.operation["op_id"])
                    req.retail_pending = [p for p in req.retail_pending if p["op_id"] != state.operation["op_id"]]
                state.stop = "binding_changed"
                driver.save()
                continue
            flow = ((req.retail_support or {}).get("return_flow") or {}).get("action")
            human = flow == "human" or re.fullmatch(
                r"(?:请)?(?:转人工(?:客服)?|找人工)|(?:please )?(?:transfer me to (?:a )?|speak to (?:a )?)?human(?: agent)?[.!]?",
                text, re.I)
            if human:
                if state.operation:
                    store.cancel_dag_operation(req.user_id, req.conv_id, state.operation["op_id"])
                state.stop = "handoff"
                driver.save()
                req.intent = IntentCategory.HUMAN_HANDOFF
                continue
            if any(r.status == Status.RUNNING for r in state.runs.values()) or state.stop == "unknown_result":
                driver.reconcile()
                if state.stop == "unknown_result" or text.lower() in {"continue", "继续"}:
                    return driver
                continue
            waiting = next((r for r in state.runs.values() if r.status == Status.WAITING_USER), None)
            if not waiting:
                if not state.stop and text.lower() in {"continue", "继续"}:
                    return driver
                continue
            if waiting.reason == "confirmation":
                op = next((p for p in req.retail_pending if p["op_id"] == state.operation.get("op_id")), None)
                if not op:
                    waiting.move(Status.BLOCKED, reason="operation_no_longer_pending")
                    state.stop = "superseded"
                    driver.save()
                    continue
                if op["status"] == "prepared" and op["prepared_turn"] < req.turn:
                    store.cancel_dag_operation(req.user_id, req.conv_id, op["op_id"])
                    waiting.move(Status.BLOCKED, reason="interrupted_before_display")
                    state.stop = "interrupted_before_display"
                    driver.note = ("Please request fresh confirmation details. The previous details expired."
                                   if orchestrator.language == "en" else "上次确认详情已失效，请重新发起办理。")
                    driver.save()
                    return driver
                if re.fullmatch(r"(?:no|cancel|cancel (?:the )?request|取消|撤回|不用了)[.!。！]?", text, re.I):
                    store.cancel_dag_operation(req.user_id, req.conv_id, op["op_id"])
                    waiting.move(Status.BLOCKED, reason="cancelled")
                    state.stop = "cancelled"
                    driver.note = "Request withdrawn." if orchestrator.language == "en" else "已撤回待确认操作。"
                    driver.save()
                    return driver
                redirect = re.match(r"(?:please\s+)?(?:use|switch to)\s+(?:order\s+)?(?P<order>\#?W\d+)\b"
                                    r"|(?:改用|改查|换到)(?:订单)?\s*(?P<zh_order>\#?W\d+)", text, re.I)
                new_order = (redirect["order"] or redirect["zh_order"]) if redirect else ""
                different_order = new_order and new_order.lstrip("#").upper() != (op.get("order_id") or "").lstrip("#")
                if (different_order or flow == "switch_to_troubleshooting" or re.search(
                        r"改为|改成|换成|\binstead\b|^change (?:it|the (?:item|payment|refund))", text, re.I)):
                    store.cancel_dag_operation(req.user_id, req.conv_id, op["op_id"])
                    waiting.move(Status.BLOCKED, reason="revised")
                    state.stop = "superseded"
                    driver.save()
                    req.retail_pending = [p for p in req.retail_pending if p["op_id"] != op["op_id"]]
                    continue
                judged = op.get("confirmation_approved")
                approved = judged if judged is not None else evaluate_confirmation(
                    text, op, req.retail_pending, language=orchestrator.language).approved
                if not (approved and op["status"] == "displayed" and op["displayed_turn"] < req.turn
                        and op["args"] == state.operation["args"]):
                    continue
            elif waiting.reason == "choose_target":
                choices = waiting.facts.get("choices", [])
                task = next(t for t in state.plan.nodes if t.id == waiting.task_id)
                valid_id = (r"#W[0-9]+" if task.kind in PRIVATE else r"[0-9]{6,16}")
                if (choices and text not in choices) or (not choices and not re.fullmatch(valid_id, text)):
                    continue
                state.selected[waiting.task_id] = text
            elif waiting.reason == "identity_required":
                if req.retail_user_id and text.lower() not in {"continue", "继续"} and not re.fullmatch(
                        r"[^\s@]+@[^\s@]+\.[^\s@]+", text):
                    continue
                if not req.retail_user_id:
                    if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", text):
                        continue
                    binding = store.find_user_by_email(req.user_id, req.conv_id, text)
                    if not binding.get("success"):
                        return driver
                    req.retail_user_id = binding["user_id"]
                state.bound_user = req.retail_user_id
            else:
                continue
            state.stop = ""
            waiting.move(Status.READY)
            return driver
        return None

    def reconcile(self):
        """After an interruption only an authoritative event may complete a write."""
        state, req = self.state, self.req
        run = next((r for r in state.runs.values() if r.status == Status.RUNNING
                    or (r.status == Status.BLOCKED and r.reason in {"unknown_result", "interrupted"})), None)
        if run is None:
            return
        task = next(t for t in state.plan.nodes if t.id == run.task_id)
        if task.kind not in WRITES:
            if run.status == Status.RUNNING:
                run.move(Status.FAILED, reason="interrupted_read")
            state.stop = ""
            self.save()
            return
        op = state.operation
        store = req.retail_store
        if not op:
            recorded = store.prepared_for_task(state.plan.id, run.task_id, run.turn)
            pending = next((p for p in store.active_pending(req.user_id,req.conv_id) if p["op_id"] == recorded), None)
            if pending:
                state.operation = {k: pending[k] for k in ("kind","args","op_id")}
                if pending["status"] == "displayed":
                    if run.status == Status.BLOCKED:
                        run.move(Status.PENDING)
                        run.move(Status.READY)
                    self.wait(task, "confirmation")
                else:
                    # The user may never have seen the card. Retire it and require fresh preparation.
                    store.cancel_dag_operation(req.user_id, req.conv_id, recorded)
                    if run.status == Status.RUNNING:
                        run.move(Status.FAILED, reason="interrupted_before_display")
                    state.stop = "interrupted_before_display"
                self.save()
                return
        matched = next((e for e in store.list_events(req.user_id, req.conv_id)
                        if op and e["op_id"] == op["op_id"] and e["event_type"] == store.executed_event(op["kind"])
                        and e["payload"].get("args") == op["args"] and e["payload"].get("kind") == op["kind"]), None)
        if matched:
            run.reconcile(op["op_id"], matched["event_id"])
            for child in state.plan.nodes:
                downstream = state.runs[child.id]
                if task.id in child.depends_on and downstream.reason == "dependency":
                    downstream.move(Status.PENDING)
            state.operation = {}
            state.stop = ""
        else:
            if run.status == Status.RUNNING:
                run.move(Status.BLOCKED, reason="unknown_result")
            state.stop = "unknown_result"
        self.save()

    def save(self):
        self.state.record_attempts()
        if self.req.retail_store is not None:
            self.req.retail_store.save_dag(self.state)

    def next_role(self):
        while task := self.state.next_node(self.req.turn):
            run = self.state.runs[task.id]
            target, choices = self.state.target(task)
            if not task.output:
                run.move(Status.BLOCKED, reason="missing_output_contract")
                continue
            if task.kind in PRIVATE and not self.req.retail_user_id:
                self.wait(task, "identity_required")
                break
            if choices is None:
                run.move(Status.BLOCKED, reason="no_matching_target")
                continue
            if not target:
                self.wait(task, "choose_target", choices)
                break
            role = AgentType(OWNERS[task.kind])
            actor = self.router._best_agent(role)
            if not actor or self.router._monitor_degraded(role):
                run.move(Status.BLOCKED, reason="role_unavailable")
                continue
            scope = replace(self.req, dag_assignment={"id": task.id}, dispatch_role=role.value,
                            dag_tool_limit=TASK_TOOLS[task.kind])
            available = actor._tools_for_request(scope)
            if task.kind in WRITES:
                required = WRITE_KINDS.keys() & TASK_TOOLS[task.kind]
                if self.state.operation:
                    required = {name for name in required if WRITE_KINDS[name] == self.state.operation["kind"]}
            elif task.kind == "product_info":
                required = {"get_product_details" if task.output == "variants" else "get_item_details"}
            elif task.kind in PRIVATE:
                required = {"get_order_details"}
            else:
                required = {"search_knowledge_base"}
            if required.isdisjoint(available):
                run.move(Status.BLOCKED, reason="capability_unavailable")
                continue
            self.task = task
            run.claim(role.value, self.req.turn)
            self.save()
            return role.value
        if not self.state.stop and not self.state.complete:
            self.state.stop = "no_ready_task"
        self.save()
        return None

    def wait(self, task, reason, choices=()):
        run = self.state.runs[task.id]
        run.move(Status.WAITING_USER, reason=reason, facts={"choices": list(choices)})
        self.state.stop = reason
        blocked = {task.id}
        for child in self.state.plan.nodes:
            downstream = self.state.runs[child.id]
            if blocked.intersection(child.depends_on):
                if downstream.status == Status.PENDING:
                    downstream.move(Status.BLOCKED, reason="dependency")
                blocked.add(child.id)

    async def execute(self):
        req, task = self.req, self.task
        run = self.state.runs[task.id]
        target, _ = self.state.target(task)
        assignment = {"id": task.id, "kind": task.kind, "quote": task.quote, "output": task.output,
                      "target": target, "inputs": self.state.inputs(task)}
        assignment["input_refs"] = {key: list(self.state.runs[key].refs) for key in assignment["inputs"]}
        if self.state.operation and task.kind in WRITES:
            assignment["operation"] = self.state.operation
        actor = self.router._best_agent(AgentType(run.role))
        # Each node gets a fresh view. The tool wrappers still see the real user
        # message, session binding and shared confirmation guard.
        def view(agent, tools):
            return [{"role": "user", "content": [{"type": "text", "text": json.dumps(
                {"assignment": assignment}, ensure_ascii=False)}]}]
        scoped = replace(req, dag_assignment=assignment, dag_tool_limit=TASK_TOOLS[task.kind],
                         dispatch_role=run.role, context_view=view, context="", history=[], entities={},
                         dag_ticket=(self.state.plan.id, task.id, self.state.revision, req.turn),
                         retail_handoff=None, transcript=None, spans=req.spans)
        try:
            response = await actor.handle(scoped)
        except Exception as exc:
            unknown = task.kind in WRITES
            run.move(Status.BLOCKED if unknown else Status.FAILED, reason="interrupted")
            if unknown or _stop_exception(exc):
                self.state.stop = "unknown_result" if unknown else "budget_stop"
            self.save()
            if _stop_exception(exc):
                raise
            return
        self.responses.append(response)
        traces = response.tool_traces
        guard = req.retail_guard or {}
        store = req.retail_store
        if guard.get("unknown_result"):
            run.move(Status.BLOCKED, reason="unknown_result")
            self.state.stop = "unknown_result"
        elif guard.get("handoff"):
            run.move(Status.BLOCKED, reason="handoff")
            self.state.stop = "handoff"
        elif task.kind in WRITES and store and self._prepared(task, traces):
            self.wait(task, "confirmation")
        else:
            events = store.list_events(req.user_id, req.conv_id) if store and task.kind in WRITES else ()
            evidence = verify(task, traces, bound_user=req.retail_user_id, target=target,
                              events=events, operation=self.state.operation)
            if evidence and task.output == "item_ids":
                for dep in assignment["inputs"].values():
                    if "variants" in dep:
                        evidence.facts["item_ids"] = [v for v in evidence.facts["item_ids"] if v in dep["variants"]]
            if evidence:
                run.move(Status.SUCCEEDED, facts=evidence.facts, refs=evidence.refs)
                if task.kind in WRITES:
                    self.state.operation = {}
            else:
                run.move(Status.FAILED, reason="missing_evidence")
        self.save()

    def _prepared(self, task, traces):
        req = self.req
        recorded = req.retail_store.prepared_for_task(self.state.plan.id, task.id, req.turn)
        ids = {(t.get("result") or {}).get("op_id") for t in traces
               if t.get("tool_name") in WRITE_KINDS and t.get("result_success")
               and (t.get("result") or {}).get("action_status") == "pending_confirmation"}
        for op in req.retail_store.active_pending(req.user_id, req.conv_id):
            if op["op_id"] == recorded and recorded in ids and op["prepared_turn"] == req.turn:
                self.state.operation = {k: op[k] for k in ("op_id", "args", "kind")}
                self.note = op["display_text"]
                return True
        return False

    def finish(self):
        runs = list(self.state.runs.values())
        self.req.retail_routing["dag"] = {"plan_id": self.state.plan.id, "stop": self.state.stop,
            "attempts": list(self.state.attempts),
            "tasks": [{"id": r.task_id, "role": r.role, "status": r.status.value,
                       "reason": r.reason, "refs": list(r.refs), "turn": r.turn} for r in runs]}
        facts = [f"{field}: {value}" for r in runs if r.status == Status.SUCCEEDED
                 for field,value in r.facts.items() if field != "op_id"]
        if self.state.stop == "choose_target":
            choices = next((r.facts.get("choices") for r in runs if r.status == Status.WAITING_USER), ())
            self.note = ("Please specify the target ID: " if self.router.language == "en" else "请指定目标编号：") + ", ".join(choices or ())
        elif self.state.stop == "identity_required":
            self.note = ("Please provide your email to verify your identity." if self.router.language == "en"
                         else "请提供本人邮箱以核实身份。")
        elif self.state.stop == "unknown_result":
            self.note = ("The previous operation is unverified. No operation was replayed." if self.router.language == "en"
                         else "上次操作结果尚未核实，本轮未重放操作。")
        if not self.state.complete and not self.note:
            self.note = ("Some requested steps remain unresolved." if self.router.language == "en" else "部分任务尚未完成。")
        result = OrchestratorResult(
            request_id=self.req.request_id, intent=self.req.intent, agent_type=AgentType.RETAIL,
            response="\n".join(facts + ([self.note] if self.note else [])), success=self.state.complete,
            routing_reason="dag_tasks", agent_types=[r.agent_type for r in self.responses],
            tools_used=list(dict.fromkeys(t for r in self.responses for t in r.tools_used)),
            tool_traces=[t for r in self.responses for t in r.tool_traces],
            role_traces=[self.router._role_trace(r) for r in self.responses],
            latency_ms=(time.monotonic()-self.started)*1000)
        self.router._record_tool_trace(result)
        return result
