"""Execute one verified retail DAG node at a time on either workflow backend."""
from dataclasses import replace
import json
import re
import time

from agents.agent_orchestrator import AgentResponse, AgentType, OrchestratorResult, _stop_exception
from agents.retail_dag_composer import compose_tasks
from agents.retail_tools import TASK_TOOLS
from core.agent_context_budget import ContextLimit
from core.retail_dag import Status
from evaluation.retail_budget import BudgetExceeded, TurnRequestLimitExceeded
from core.intent_recognizer import IntentCategory
from core.retail_dag_evidence import verify
from core.retail_subtasks import OWNERS, PRIVATE, WRITES
from core.dag_tool_log import JournalConflict, input_digest
from core.dag_planning import target_outputs
from core.task_context import identifiers, choice_value, target_type, verified_entity
from mcp_tools.retail_domain import WRITE_KINDS


def task_context(assignment, req):
    packet = {"assignment": assignment}
    if assignment["kind"] in PRIVATE:
        packet["session"] = {"identity_verified": bool(req.retail_user_id)}
        packet["instruction"] = ("Use the current server-verified session for the assigned task. "
                                 "Obtain the requested evidence with the permitted tools.")
    if assignment["kind"] in WRITES:
        packet["session"]["write_phase"] = ("confirmed_operation" if assignment.get("operation")
                                             else "prepare_for_confirmation")
        if assignment.get("operation"):
            packet["instruction"] += " Submit exactly the displayed operation using its recorded arguments."
        else:
            packet["instruction"] += " Prepare the requested change and return its server confirmation card."
    return packet


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
                state.selected.clear()
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
                if (state.stop in {"context_limit", "budget_stop", "request_limit"} and not state.operation
                        and text.lower() in {"continue", "继续"}):
                    state.stop = ""
                    return driver
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
                typ = target_type(task.kind, task.output)
                scoped_choice = req.conversation_context or driver.reference_needed(task)
                answer = choice_value(typ, text) if scoped_choice else text
                if req.conversation_context and choices:
                    choices = [v for v in choices if req.conversation_context.record(typ, v, req.retail_user_id)]
                    if choices != waiting.facts["choices"]:
                        waiting.facts["choices"] = choices
                        driver.save()
                if choices and answer not in choices:
                    if req.conversation_context and answer:
                        return driver
                    continue
                if not choices and not re.fullmatch(valid_id, answer or ""):
                    continue
                if (typ and scoped_choice
                        and not verified_entity(store, req.user_id, req.conv_id, typ, answer, req.retail_user_id)):
                    return driver
                driver.select_target(task, answer, req.turn, explicit=True)
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
        terminal = store.unpublished_terminal(req.user_id, req.conv_id, req.turn - 1,
                                              message_hash=None if text.lower() in {"continue", "继续"} else input_digest(text))
        if terminal and (terminal.bound_user or None) == req.retail_user_id:
            return cls(orchestrator, req, terminal)
        return None

    def reconcile(self):
        """After an interruption only an authoritative event may complete a write."""
        state, req = self.state, self.req
        run = next((r for r in state.runs.values() if r.status == Status.RUNNING
                    or (r.status == Status.BLOCKED and r.reason in {"unknown_result", "interrupted"})), None)
        if run is None:
            return
        task = next(t for t in state.plan.nodes if t.id == run.task_id)
        store = req.retail_store
        batches = store.attempt_batches(req.user_id, req.conv_id, state.plan.id, run)
        if task.kind not in WRITES:
            traces = []
            target = state.target(task)[0]
            bound = store.bound_user_id(req.user_id, req.conv_id)
            for batch in batches:
                if batch["bound"] != bound or (state.bound_user or None) != bound:
                    continue
                for call in batch["calls"]:
                    if call["state"] != "returned":
                        continue
                    tool, result = call["tool_use"], call["result"]
                    if tool["id"] == result["tool_use_id"] and tool["name"] in TASK_TOOLS[task.kind]:
                        traces.append(dict(tool_name=tool["name"], tool_use_id=tool["id"], input=tool["input"],
                                           result=json.loads(result["content"]), result_success=call["outcome"] == "success"))
            evidence = verify(task, traces, bound_user=bound, target=target)
            if evidence and task.kind in PRIVATE and not verified_entity(store, req.user_id, req.conv_id, "order", target, bound):
                evidence = None
            if evidence and run.status == Status.RUNNING:
                self.accept_evidence(task, run, evidence)
            elif run.status == Status.RUNNING:
                run.move(Status.FAILED, reason="interrupted_read")
            state.stop = ""
            self.save()
            return
        op = state.operation
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
                    store.retire_unpublished_card(state, recorded)
                    if run.status == Status.RUNNING:
                        run.move(Status.FAILED, reason="interrupted_before_display")
                    state.stop = "interrupted_before_display"
                self.save()
                return
        if batches:
            event_id = store.reconcile_tool_write(req.user_id, req.conv_id, state.plan.id, run,
                                                  state.target(task)[0], op, batches)
        elif state.source:
            event_id = store.dag_commit_event(state, task, state.target(task)[0], op)
        else:
            # Older plans predate turn provenance and may lack an operation-to-task row.
            matched = next((e for e in store.list_events(req.user_id, req.conv_id)
                            if op and e["op_id"] == op["op_id"] and e["event_type"] == store.executed_event(op["kind"])
                            and e["payload"].get("args") == op["args"] and e["payload"].get("kind") == op["kind"]), None)
            event_id = matched["event_id"] if matched else None
        if event_id:
            run.reconcile(op["op_id"], event_id)
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
        if self.req.conversation_context and self.req.conversation_context.audience and self.state.source:
            self.state.source["audience"] = self.req.conversation_context.audience
        self.state.record_attempts()
        if self.req.retail_store is not None:
            self.req.retail_store.save_dag(self.state)

    def reference_needed(self, task):
        if task.target:
            return ""
        sources = target_outputs(task.kind, task.output)
        if any(parent.id in task.depends_on and parent.output in sources for parent in self.state.plan.nodes):
            return ""
        return target_type(task.kind, task.output)

    def select_target(self, task, value, turn, *, explicit=False):
        self.state.selected[task.id] = value
        self.state.selection_sources[task.id] = {"turn": turn, "bound": self.req.retail_user_id,
                                                 "explicit": explicit}

    def resolve_reference(self, task, typ):
        req = self.req
        conversation = req.conversation_context
        selected = self.state.selected.get(task.id)
        candidates = conversation.candidates(typ, req.retail_user_id) if conversation else []
        if selected:
            explicit = choice_value(typ, req.message) == selected
            source = self.state.selection_sources.get(task.id)
            # Old plans have no provenance. Require a fresh explicit selection.
            witnessed = bool(source and conversation and source["bound"] == req.retail_user_id and any(
                t["turn"] == source["turn"] and selected in identifiers(typ, t["user"] + " " + t["assistant"])
                for t in conversation.recent))
            if source and not conversation and source["bound"] == req.retail_user_id:
                published = req.retail_store.dag_publication(req.user_id, req.conv_id, source["turn"])
                witnessed = (published.get("dag_receipt", {}).get("bound") == req.retail_user_id and
                             selected in identifiers(typ, published.get("user", "")))
            if conversation and source and not source.get("explicit") and candidates != [selected]:
                witnessed = False
            conflicting = identifiers(typ, req.message) - {selected}
            if not conflicting and (explicit or witnessed) and verified_entity(
                    req.retail_store, req.user_id, req.conv_id, typ, selected, req.retail_user_id):
                return selected
            self.discard_selection(task)
        elif conversation and len(candidates) == 1 and candidates[0] == choice_value(
                typ, conversation.proposals.get(task.id, "")):
            witness = next(t for t in reversed(conversation.recent)
                           if candidates[0] in identifiers(typ, t["user"] + " " + t["assistant"]))
            self.select_target(task, candidates[0], witness["turn"])
            return candidates[0]
        self.wait(task, "choose_target", candidates)
        return ""

    def discard_selection(self, task):
        self.state.selected.pop(task.id, None)
        self.state.selection_sources.pop(task.id, None)
        if self.state.operation:
            req = self.req
            op_id = self.state.operation["op_id"]
            req.retail_store.cancel_dag_operation(req.user_id, req.conv_id, op_id)
            req.retail_pending = [op for op in req.retail_pending if op["op_id"] != op_id]
            self.state.operation = {}

    def next_role(self):
        if self.req.dag_source and self.state.source.get("turn") != self.req.turn:
            self.state.source = self.req.dag_source.copy()
        while task := self.state.next_node(self.req.turn):
            run = self.state.runs[task.id]
            target, choices = self.state.target(task)
            if not task.output:
                run.move(Status.BLOCKED, reason="missing_output_contract")
                continue
            if task.kind in PRIVATE and not self.req.retail_user_id:
                self.wait(task, "identity_required")
                break
            conversation = self.req.conversation_context
            if conversation and self.req.retail_store.bound_user_id(self.req.user_id, self.req.conv_id) != self.req.retail_user_id:
                run.move(Status.BLOCKED, reason="binding_changed")
                self.state.stop = "binding_changed"
                break
            if (conversation and task.kind in PRIVATE and task.target and choice_value("order", task.target)
                    and not conversation.record("order", target, self.req.retail_user_id)):
                run.move(Status.BLOCKED, reason="unverified_target")
                continue
            typ = self.reference_needed(task)
            if typ:
                target = self.resolve_reference(task, typ)
                if not target:
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
            scope = replace(self.req, dag_assignment={"id": task.id, "kind": task.kind}, dispatch_role=role.value,
                            dag_tool_limit=TASK_TOOLS[task.kind])
            available = actor._tools_for_request(scope)
            if (conversation and task.kind not in PRIVATE and any(
                    other.kind in PRIVATE and other.quote in task.quote for other in self.state.plan.nodes)):
                run.move(Status.BLOCKED, reason="quote_scope")
                continue
            if conversation and not conversation.permits_inputs(self.state, task, available, target):
                run.move(Status.BLOCKED, reason="dependency_scope")
                continue
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
        if req.conversation_context:
            store = req.retail_store
            problem = ""
            if store.bound_user_id(req.user_id, req.conv_id) != req.retail_user_id:
                problem = "binding_changed"
            elif task.id in self.state.selected and not req.conversation_context.record(
                    target_type(task.kind, task.output), target, req.retail_user_id):
                self.discard_selection(task)
                self.wait(task, "choose_target")
                self.save()
                return
            elif self.state.operation and task.kind in WRITES:
                expected = self.state.operation
                pending = store.active_pending(req.user_id, req.conv_id)
                if not any(op["status"] == "displayed" and op["displayed_turn"] < req.turn
                           and op["retail_user_id"] == req.retail_user_id
                           and store.prepared_for_task(self.state.plan.id, task.id, op["prepared_turn"]) == op["op_id"]
                           and all(op[key] == expected[key] for key in ("op_id", "kind", "args"))
                           for op in pending):
                    problem = "operation_changed"
            if problem:
                run.move(Status.BLOCKED, reason=problem)
                self.state.stop = problem
                self.save()
                return
        assignment = {"id": task.id, "kind": task.kind, "quote": task.quote, "output": task.output,
                      "target": target, "inputs": self.state.inputs(task)}
        assignment["input_refs"] = {key: list(self.state.runs[key].refs) for key in assignment["inputs"]}
        if self.state.operation and task.kind in WRITES:
            assignment["operation"] = self.state.operation
        actor = self.router._best_agent(AgentType(run.role))
        # Each node gets a fresh view. The tool wrappers still see the real user
        # message, session binding and shared confirmation guard.
        def view(agent, tools):
            packet = task_context(assignment, req)
            if req.conversation_context:
                return req.conversation_context.messages(req, task, agent.agent_type.value, tools, packet)
            return [{"role": "user", "content": [{"type": "text", "text": json.dumps(
                packet, ensure_ascii=False)}]}]
        scoped = replace(req, dag_assignment=assignment, dag_tool_limit=TASK_TOOLS[task.kind],
                         dispatch_role=run.role, context_view=view, context="", history=[], entities={},
                         dag_ticket=(self.state.plan.id, task.id, self.state.revision, req.turn),
                         retail_handoff=None, dag_attempt=run.attempt,
                         transcript=req.transcript if req.dag_log_mode != "off" else None, spans=req.spans)
        stop_reason = ""
        hard_stop = None
        try:
            response = await actor.handle(scoped)
        except ContextLimit as exc:
            stop_reason = "context_limit"
            response = AgentResponse.context_stopped(exc, req, actor.agent_type, self.router.language)
        except (BudgetExceeded, TurnRequestLimitExceeded) as exc:
            hard_stop = exc
            stop_reason = "budget_stop" if isinstance(exc, BudgetExceeded) else "request_limit"
            req.retail_routing["hard_stop"] = stop_reason
            partial = getattr(exc, "partial", None)
            response = AgentResponse(actor.agent_type, stop_reason, False,
                                     tool_traces=partial.tool_traces if partial else [],
                                     tools_used=partial.tools_used if partial else [])
        except JournalConflict:
            # Preserve the durable claim for evidence-only recovery.
            raise
        except Exception as exc:
            unknown = task.kind in WRITES
            run.move(Status.BLOCKED if unknown else Status.FAILED, reason="interrupted")
            if unknown or _stop_exception(exc):
                self.state.stop = "unknown_result" if unknown else "budget_stop"
            self.save()
            if _stop_exception(exc):
                raise
            return
        response.meta = {**(response.meta or {}), "task_id": task.id}
        response.tool_traces = [{**trace, "task_id": task.id} for trace in response.tool_traces]
        self.responses.append(response)
        traces = response.tool_traces
        guard = req.retail_guard or {}
        store = req.retail_store
        if (stop_reason and task.kind in WRITES and not self.state.operation and store
                and self._prepared(task, traces, allow_stored=True)):
            self.wait(task, "confirmation")
        elif guard.get("unknown_result") and stop_reason and task.kind in WRITES and self.state.operation:
            self.reconcile()
        elif guard.get("unknown_result"):
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
            if evidence:
                self.accept_evidence(task, run, evidence)
                if (req.conversation_context and len(self.state.plan.nodes) == 1 and task.quote == req.message
                        and task.kind not in WRITES and not req.retail_pending):
                    req.conversation_context.audience = [run.role, task.kind, target]
                if task.kind in WRITES:
                    self.state.operation = {}
            elif (stop_reason and task.kind in WRITES and self.state.operation and not traces
                  and store and any(op["status"] == "displayed" and op["op_id"] == self.state.operation["op_id"]
                                    for op in store.active_pending(req.user_id, req.conv_id))):
                # Confirmation belongs to this real input. A later attempt needs a new one.
                self.wait(task, "confirmation")
            elif stop_reason and task.kind in WRITES and any(
                    t["tool_name"] in WRITE_KINDS and not (t.get("not_executed") or t.get("blocked_by_guard")
                    or t.get("skipped")) and t.get("result_success") is not False for t in traces):
                run.move(Status.BLOCKED, reason="unknown_result")
                self.state.stop = "unknown_result"
            else:
                run.move(Status.FAILED, reason=stop_reason or "missing_evidence")
        if stop_reason and not self.state.stop:
            self.state.stop = stop_reason
            self.note = response.content
        if stop_reason and self.state.source:
            self.state.source["stop_reason"] = stop_reason
            if stop_reason == "context_limit":
                self.state.source["context_limit"] = req.retail_routing["context_limit"]
        self.save()
        if hard_stop is not None:
            hard_stop.dag_runner = self
            raise hard_stop

    def accept_evidence(self, task, run, evidence):
        if task.output == "item_ids":
            for dep in self.state.inputs(task).values():
                if "variants" in dep:
                    evidence.facts["item_ids"] = [v for v in evidence.facts["item_ids"] if v in dep["variants"]]
        run.move(Status.SUCCEEDED, facts=evidence.facts, refs=evidence.refs)

    def _prepared(self, task, traces, *, allow_stored=False):
        req = self.req
        recorded = req.retail_store.prepared_for_task(self.state.plan.id, task.id, req.turn)
        ids = {(t.get("result") or {}).get("op_id") for t in traces
               if t.get("tool_name") in WRITE_KINDS and t.get("result_success")
               and (t.get("result") or {}).get("action_status") == "pending_confirmation"}
        for op in req.retail_store.active_pending(req.user_id, req.conv_id):
            if op["op_id"] == recorded and (allow_stored or recorded in ids) and op["prepared_turn"] == req.turn:
                self.state.operation = {k: op[k] for k in ("op_id", "args", "kind")}
                self.note = op["display_text"]
                return True
        return False

    def finish(self):
        runs = list(self.state.runs.values())
        self.req.retail_routing["dag"] = {"plan_id": self.state.plan.id, "revision": self.state.revision, "stop": self.state.stop,
            "attempts": list(self.state.attempts),
            "tasks": [{"id": r.task_id, "role": r.role, "status": r.status.value,
                       "reason": r.reason, "refs": list(r.refs), "turn": r.turn} for r in runs]}
        if self.state.stop in {"budget_stop", "request_limit", "context_limit"}:
            self.note = (f"Further processing stopped ({self.state.stop})." if self.router.language == "en"
                         else f"后续处理已停止（{self.state.stop}）。")
        elif self.state.stop == "interrupted_before_display":
            self.note = ("Please request fresh confirmation details." if self.router.language == "en"
                         else "上次详情未发布，请重新申请确认详情。")
        elif self.state.stop == "choose_target":
            choices = next((r.facts.get("choices") for r in runs if r.status == Status.WAITING_USER), ())
            self.note = ("Please specify the target ID: " if self.router.language == "en" else "请指定目标编号：") + ", ".join(choices or ())
        elif self.state.stop == "identity_required":
            self.note = ("Please provide your email to verify your identity." if self.router.language == "en"
                         else "请提供本人邮箱以核实身份。")
        elif self.state.stop == "unknown_result":
            self.note = ("The previous operation is unverified. No operation was replayed." if self.router.language == "en"
                         else "上次操作结果尚未核实，本轮未重放操作。")
        elif self.state.stop == "confirmation" and not self.note and self.req.retail_store:
            pending = self.req.retail_store.active_pending(self.req.user_id, self.req.conv_id)
            self.note = next((op["display_text"] for op in pending
                              if op["op_id"] == self.state.operation.get("op_id")), "")
        by_task = {}
        body = compose_tasks(self.state, self.router.language, "", by_task=by_task)
        events = [r.refs[0] for r in runs if r.turn == self.req.turn and r.facts.get("executed") and r.refs]
        if events:
            # The guard renders each write from its own event. Keep the other declared
            # outputs and unfinished nodes once, attached to the last verified write.
            self.req.dag_post_write[events[-1]] = [
                line for task in self.state.plan.nodes if not self.state.runs[task.id].facts.get("executed")
                for line in by_task.get(task.id, ())]
        if self.state.stop == "confirmation" and self.state.operation and body:
            self.req.dag_confirmation_basis = (self.state.operation["op_id"], body)
        content = "\n".join(filter(None, (body, self.note)))
        result = OrchestratorResult(
            request_id=self.req.request_id, intent=self.req.intent, agent_type=AgentType.RETAIL,
            response=content, composer_output=content,
            success=self.state.complete and not self.req.retail_routing.get("hard_stop"),
            routing_reason="dag_tasks", agent_types=[r.agent_type for r in self.responses],
            tools_used=list(dict.fromkeys(t for r in self.responses for t in r.tools_used)),
            tool_traces=[t for r in self.responses for t in r.tool_traces],
            role_traces=[self.router._role_trace(r) for r in self.responses],
            latency_ms=(time.monotonic()-self.started)*1000)
        self.router._record_tool_trace(result)
        return result
