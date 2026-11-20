"""Execute one verified retail DAG node at a time on either workflow backend."""
from dataclasses import asdict, replace
import json
import re
import time

from agents.agent_orchestrator import AgentType, OrchestratorResult, _stop_exception
from agents.retail_tools import TASK_TOOLS
from core.retail_dag import Status
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
                state.stop = "binding_changed"
                driver.save()
                continue
            if any(r.status == Status.RUNNING for r in state.runs.values()) or state.stop == "unknown_result":
                driver.reconcile()
                return driver
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
                if re.fullmatch(r"(?:no|cancel|cancel (?:the )?request|取消|撤回|不用了)[.!。！]?", text, re.I):
                    store.cancel_dag_operation(req.user_id, req.conv_id, op["op_id"])
                    waiting.move(Status.BLOCKED, reason="cancelled")
                    state.stop = "cancelled"
                    driver.note = "Request withdrawn." if orchestrator.language == "en" else "已撤回待确认操作。"
                    driver.save()
                    return driver
                judged = op.get("confirmation_approved")
                approved = judged if judged is not None else evaluate_confirmation(
                    text, op, req.retail_pending, language=orchestrator.language).approved
                if not (approved and op["status"] == "displayed" and op["displayed_turn"] < req.turn
                        and op["args"] == state.operation["args"]):
                    continue
            elif waiting.reason == "choose_target":
                choices = waiting.facts.get("choices", [])
                if text not in choices:
                    continue
                state.selected[waiting.task_id] = text
            elif waiting.reason == "identity_required":
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
                    or r.reason in {"unknown_result", "interrupted"}), None)
        if run is None:
            return
        op = state.operation
        store = req.retail_store
        matched = next((e for e in store.list_events(req.user_id, req.conv_id)
                        if op and e["op_id"] == op["op_id"] and e["event_type"] == store.executed_event(op["kind"])
                        and e["payload"].get("args") == op["args"] and e["payload"].get("kind") == op["kind"]), None)
        if matched:
            run.reconcile(op["op_id"], matched["event_id"])
            state.operation = {}
            state.stop = ""
        else:
            if run.status == Status.RUNNING:
                run.move(Status.BLOCKED, reason="unknown_result")
            state.stop = "unknown_result"
        self.save()

    def save(self):
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
            if not target:
                self.wait(task, "choose_target", choices)
                break
            role = AgentType(OWNERS[task.kind])
            actor = self.router._best_agent(role)
            if not actor or self.router._monitor_degraded(role):
                run.move(Status.BLOCKED, reason="role_unavailable")
                continue
            self.task = task
            run.claim(role.value, self.req.turn)
            self.save()
            return role.value
        self.save()
        return None

    def wait(self, task, reason, choices=()):
        run = self.state.runs[task.id]
        run.move(Status.WAITING_USER, reason=reason, facts={"choices": list(choices)})
        self.state.stop = reason

    async def execute(self):
        req, task = self.req, self.task
        run = self.state.runs[task.id]
        target, _ = self.state.target(task)
        assignment = {"id": task.id, "kind": task.kind, "quote": task.quote, "output": task.output,
                      "target": target, "inputs": self.state.inputs(task)}
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
            run.move(Status.WAITING_USER, reason="confirmation")
            self.state.stop = "confirmation"
        else:
            events = store.list_events(req.user_id, req.conv_id) if store and task.kind in WRITES else ()
            evidence = verify(task, traces, bound_user=req.retail_user_id, target=target,
                              events=events, operation=self.state.operation)
            if evidence:
                run.move(Status.SUCCEEDED, facts=evidence.facts, refs=evidence.refs)
                if task.kind in WRITES:
                    self.state.operation = {}
            else:
                run.move(Status.FAILED, reason="missing_evidence")
        self.save()

    def _prepared(self, task, traces):
        req = self.req
        ids = {(t.get("result") or {}).get("op_id") for t in traces
               if t.get("tool_name") in WRITE_KINDS and t.get("result_success")
               and (t.get("result") or {}).get("action_status") == "pending_confirmation"}
        for op in req.retail_store.active_pending(req.user_id, req.conv_id):
            if op["op_id"] in ids and op["prepared_turn"] == req.turn:
                self.state.operation = {k: op[k] for k in ("op_id", "args", "kind")}
                self.note = op["display_text"]
                return True
        return False

    def finish(self):
        runs = list(self.state.runs.values())
        self.req.retail_routing["dag"] = {"plan_id": self.state.plan.id, "stop": self.state.stop,
            "tasks": [{"id": r.task_id, "role": r.role, "status": r.status.value,
                       "reason": r.reason, "refs": list(r.refs), "turn": r.turn} for r in runs]}
        facts = [f"{field}: {value}" for r in runs if r.status == Status.SUCCEEDED
                 for field,value in r.facts.items() if field != "op_id"]
        if self.state.stop == "choose_target":
            choices = next((r.facts.get("choices") for r in runs if r.status == Status.WAITING_USER), ())
            self.note = "Please specify the target / 请指定目标：" + ", ".join(choices or ())
        elif self.state.stop == "identity_required":
            self.note = "Please provide your email to verify your identity / 请提供本人邮箱以核实身份。"
        if not self.state.complete and not self.note:
            self.note = "Some requested steps remain unresolved / 部分任务尚未完成。"
        return OrchestratorResult(
            request_id=self.req.request_id, intent=self.req.intent, agent_type=AgentType.RETAIL,
            response="\n".join(facts + ([self.note] if self.note else [])), success=self.state.complete,
            routing_reason="dag_tasks", agent_types=[r.agent_type for r in self.responses],
            tools_used=list(dict.fromkeys(t for r in self.responses for t in r.tools_used)),
            tool_traces=[t for r in self.responses for t in r.tool_traces],
            role_traces=[self.router._role_trace(r) for r in self.responses],
            latency_ms=(time.monotonic()-self.started)*1000)
