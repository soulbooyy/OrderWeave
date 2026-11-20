import json
from types import SimpleNamespace

import pytest

from agents.agent_orchestrator import Request
from core.retail_dag import Plan
from core.retail_scheduler import Execution
from evaluation.harness.config import SEED_PATH
from mcp_tools.retail_protocol import build_context_meta, parse_context_meta, RetailContextError
from mcp_tools.retail_store import RetailStore


def test_real_mcp_write_restores_claim_and_links_the_confirmation(tmp_path):
    import asyncio
    from agents.retail_tools import retail_tools
    from mcp_tools.retail_server import _call_tool

    source = json.loads(SEED_PATH.read_text())
    order = next(o for o in source["orders"].values() if o["status"] == "delivered")
    oid,uid = order["order_id"],order["user_id"]
    store = RetailStore(tmp_path/"test.sqlite",SEED_PATH,language="en")
    store.find_user_by_email("u","c",source["users"][uid]["email"])
    turn = store.begin_turn("u","c")
    q = "return from "+oid
    plan = Plan.for_execution([dict(id="w",kind="return_change",quote=q,target=oid,output="executed",depends_on=[])],q,"u","c")
    state = Execution.start(plan,uid)
    state.next_node(turn)
    state.runs["w"].claim("billing",turn)
    store.save_dag(state)
    claim = (plan.id,"w",state.revision,turn)
    args = dict(order_id=oid,item_ids=[order["items"][0]["item_id"]],payment_method_id=order["payment_history"][0]["payment_method_id"])
    req=SimpleNamespace(params=SimpleNamespace(name="return_delivered_order_items",arguments=args))
    def call(ticket):
        meta=build_context_meta("u","c","req",turn,q,"en",dag_ticket=ticket)
        return asyncio.run(_call_tool(req,retail_tools(store),store,"en",meta))
    from mcp.shared.exceptions import McpError
    with pytest.raises(McpError): call((plan.id,"wrong",state.revision,turn))
    assert not store.active_pending("u","c")
    call(claim)
    pending,=store.active_pending("u","c")
    assert store.prepared_for_task(plan.id,"w",turn)==pending["op_id"]
    assert not any(e["event_type"]=="return_requested" for e in store.list_events("u","c"))
    for malformed in [None,[],[plan.id,"w",True,turn]]:
        meta=build_context_meta("u","c","req",turn,q,"en")
        meta["dag_ticket"]=malformed
        with pytest.raises(RetailContextError):parse_context_meta(meta,"en")


def test_private_task_view_carries_verified_binding_without_identifier():
    from agents.retail_dag_runner import task_context
    assignment=dict(kind="order_lookup",target="#W123",id="a",output="status")
    data=task_context(assignment,Request("hi","u","c",retail_user_id="secret_customer"))
    assert data["session"]["identity_verified"] is True
    assert "secret_customer" not in json.dumps(data)
    assert not task_context(assignment,Request("hi","u","c"))["session"]["identity_verified"]
    assert "session" not in task_context({**assignment,"kind":"product_info"},Request("hi","u","c"))
