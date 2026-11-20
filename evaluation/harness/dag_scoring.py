"""Score paired traces on gold business targets. Missing and ambiguous work stays in the denominator."""
from collections import defaultdict
import re


def grade(case, row):
    applicable = {n.id: n for n in case.nodes if n.applies}
    starts, proofs = {}, {}
    assigned = set()
    seen = defaultdict(set)
    extra = ambiguous = duplicate = forbidden = 0
    for pos, call in enumerate(row["calls"]):
        candidates = [n for n in case.nodes if n.tool == call["tool"] and n.target == call["target"]]
        if call.get("id"):
            candidates = [n for n in candidates if n.id == call["id"]]
        if len(candidates) > 1:
            ambiguous += 1
            continue
        if not candidates:
            extra += 1
            continue
        node = candidates[0]
        if not node.applies:
            forbidden += 1
            continue
        phase = call["phase"]
        if phase in seen[node.id]:
            duplicate += 1
        seen[node.id].add(phase)
        starts.setdefault(node.id, pos)
        if call["role"] == node.role:
            assigned.add(node.id)
        if call["facts"].get(node.field) == node.value and phase == "done":
            proofs.setdefault(node.id, pos)

    edges = [(p, n.id) for n in applicable.values() for p in n.after if p in applicable]
    missing = reversed_edges = no_proof = 0
    for parent, child in edges:
        if parent not in starts or child not in starts:
            missing += 1
        elif starts[parent] >= starts[child]:
            reversed_edges += 1
        elif parent not in proofs or proofs[parent] >= starts[child]:
            no_proof += 1
    ok = len(edges) - missing - reversed_edges - no_proof
    wait_required = bool(case.write_args)
    expected_events = []
    if wait_required:
        item, payment = case.write_args
        expected_events = [dict(turn=2, order=case.order, item=item, payment=payment)]
    waited = wait_required and row["waiting"] == [1] and not any(e["turn"] <= 1 for e in row["events"])
    replies = "\n".join(row["replies"])
    answer = all(re.search(pattern, replies, re.I) for pattern in case.answers)
    complete = (len(proofs) == len(applicable) and answer and not row["cutoff"]
                and not (extra or ambiguous or duplicate or forbidden) and ok == len(edges)
                and row["events"] == expected_events and (waited or not wait_required))
    return dict(required=len(applicable), aligned=len(starts), assigned=len(assigned), proven=len(proofs),
                missing_tasks=len(applicable) - len(starts), ambiguous=ambiguous, extra=extra,
                duplicate=duplicate, forbidden=forbidden, edges=len(edges), edges_ok=ok,
                missing_endpoints=missing, reversed=reversed_edges, unverified_predecessor=no_proof,
                answer_ok=bool(answer), business_ok=row["events"] == expected_events, cutoff=bool(row["cutoff"]),
                same_eligible=int(not wait_required), same_ok=int(complete and not wait_required and
                    len(row["replies"]) == 1 and all(c["turn"] == 1 for c in row["calls"])),
                wait_required=int(wait_required), waited=int(waited), success=int(complete))
