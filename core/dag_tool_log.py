"""Host-owned records of a claimed task's tool calls. No model messages or drafts."""
from copy import deepcopy
import hashlib
import json
import sqlite3


KIND = "dag_exchange_v1"


class JournalConflict(RuntimeError):
    """The stored evidence disagrees with this writer or its claim."""


def input_digest(message):
    return hashlib.sha256(message.strip().encode("utf-8")).hexdigest()


def batch_key(batch):
    return tuple(batch[k] for k in ("turn", "plan_id", "task_id", "attempt", "batch_no"))


def decode(row):
    if row.get("kind") != KIND:
        return None
    content = row.get("content")
    if isinstance(content, list) and len(content) == 1 and isinstance(content[0], dict):
        batch = content[0]
        if (batch.get("version") == 1 and batch.get("turn") == row["turn"] and isinstance(batch.get("calls"), list)
                and {"plan_id", "task_id", "attempt", "batch_no", "role", "revision", "bound"} <= batch.keys()):
            return batch
    return None


def protocol_pairs(calls, results):
    """Only complete, ordered, unambiguous provider exchanges may be replayed."""
    if not all(isinstance(b, dict) for b in calls):
        return []
    ids = [b.get("id") for b in calls]
    if (not ids or not all(isinstance(i, str) and i for i in ids) or len(set(ids)) != len(ids)
            or any(b.get("type") != "tool_use" for b in calls)
            or any(not isinstance(r, dict) or r.get("type") != "tool_result" for r in results)
            or [r.get("tool_use_id") for r in results] != ids):
        return []
    return list(zip(calls, results))


def authorized_exchange(batch, *, role, bound, plan_id, task_id, attempt, tools):
    if (batch["role"], batch["bound"], batch["plan_id"], batch["task_id"], batch["attempt"]) != (
            role, bound, plan_id, task_id, attempt):
        return []
    calls = [c["tool_use"] for c in batch["calls"]]
    if any(c["name"] not in tools for c in calls):
        return []
    pairs = protocol_pairs(calls, [c.get("result") for c in batch["calls"]])
    if not pairs:
        return []
    return deepcopy([{"role": "assistant", "content": calls},
                     {"role": "user", "content": [r for _, r in pairs]}])


def transition(call, action, result=None):
    if action == "started":
        if call["state"] != "queued":
            raise JournalConflict("tool call already dispatched or closed")
        call.update(state="started", outcome="unknown_result", receipt="raw_result_missing")
    elif action == "blocked":
        if call["state"] == "blocked":
            return False
        if call["state"] != "queued":
            raise JournalConflict("cannot reject a dispatched call")
        call.update(state="blocked", outcome="not_executed", receipt="not_dispatched")
    elif action == "returned":
        if call["state"] == "returned" and call["result"] == result:
            return False
        if call["state"] != "started" or result["tool_use_id"] != call["tool_use"]["id"]:
            raise JournalConflict("conflicting or unexpected tool receipt")
        value = json.loads(result["content"])
        failed = isinstance(value, dict) and value.get("success") is False
        call.update(state="returned", outcome="business_failure" if failed else "success",
                    receipt="original", result=deepcopy(result))
    else:
        raise JournalConflict("unknown tool log transition")
    return True


class ToolBatch:
    def __init__(self, req, role, number, blocks):
        plan, task, revision, turn = req.dag_ticket
        calls = [deepcopy(b) for b in blocks if b["type"] == "tool_use"]
        ids = [c["id"] for c in calls]
        if not all(isinstance(i, str) and i for i in ids) or len(set(ids)) != len(ids):
            raise JournalConflict("missing or duplicate tool_use_id")
        self.req = req
        self.batch = dict(version=1, turn=turn, plan_id=plan, task_id=task, attempt=req.dag_attempt,
                          batch_no=number, revision=revision, role=role, bound=req.retail_user_id,
                          operation_id=(req.dag_assignment.get("operation") or {}).get("op_id"),
                          user_input_hash=input_digest(req.message),
                          calls=[dict(tool_use=c, state="queued", outcome="not_executed", receipt="not_dispatched")
                                 for c in calls])
        if req.dag_log_mode == "m":
            for exchange in req.transcript:
                prior = exchange.get(KIND)
                if prior and batch_key(prior)[:-1] == batch_key(self.batch)[:-1]:
                    if set(ids).intersection(c["tool_use"]["id"] for c in prior["calls"]):
                        raise JournalConflict("reused tool_use_id within a task attempt")
            req.transcript.append({KIND: self.batch})

    def step(self, index, action, result=None):
        if self.req.dag_log_mode == "j":
            try:
                self.req.retail_store.record_tool_step(self.req.user_id, self.req.conv_id, self.batch, index, action, result)
            except sqlite3.Error as exc:
                raise JournalConflict("could not persist DAG tool boundary") from exc
        transition(self.batch["calls"][index], action, result)


def loaded_skills(rows, bound):
    """Only a real, matched load_skill receipt from a closed turn can satisfy the skill gate."""
    closed = {r["turn"] for r in rows if r["kind"] == "assistant"}
    names = []
    for pos, row in enumerate(rows):
        if row["turn"] not in closed:
            continue
        pairs = []
        batch = decode(row)
        if batch and batch["bound"] == bound:
            pairs = [(c["tool_use"], c["result"]) for c in batch["calls"] if c["state"] == "returned"
                     and c["result"]["tool_use_id"] == c["tool_use"]["id"]]
        elif row["kind"] == "tool_call" and pos + 1 < len(rows):
            following = rows[pos + 1]
            if (following["kind"] == "tool_result" and following["turn"] == row["turn"]
                    and following.get("agent") == row.get("agent")):
                pairs = protocol_pairs([b for b in row["content"] if b.get("type") == "tool_use"], following["content"])
        for tool, result in pairs:
            if tool["name"] != "load_skill":
                continue
            try:
                value = json.loads(result["content"])
            except (KeyError, TypeError, ValueError):
                continue
            name = tool.get("input", {}).get("name")
            if (name and isinstance(value, dict) and value.get("success") is True and value.get("skill") == name
                    and value.get("playbook") and name not in names):
                names.append(name)
    return names
