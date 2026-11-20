"""Align model-local task names to gold semantics. Execution order is graded separately."""
from collections import Counter

from core.retail_dag import Plan
from core.retail_subtasks import _ordered_tasks


def _ids(values):
    return {v for v in values if isinstance(v, str)} if isinstance(values, (tuple, list)) else set()


def _quote_matches(actual, expected):
    # Both quotes must already be literal spans of the same user turn.
    return bool(actual and (actual in expected or expected in actual))


def align_plan(case, raw, mode):
    rows = raw if isinstance(raw, list) else []
    counts = Counter(r["id"] for r in rows if isinstance(r, dict) and isinstance(r.get("id"), str))
    unique = {r["id"]: r for r in rows if isinstance(r, dict) and isinstance(r.get("id"), str)
              and counts[r["id"]] == 1}
    gold = {n.id: n.planned() for n in case.nodes}
    candidates = {}
    for key, row in unique.items():
        quote = row.get("quote")
        choices = set()
        if isinstance(quote, str) and quote.strip() and quote in case.message:
            for gid, ref in gold.items():
                if ref["kind"] != row.get("kind"):
                    continue
                if mode == "dag":
                    if (ref["target"], ref["output"]) != (row.get("target", ""), row.get("output")):
                        continue
                elif (not _quote_matches(quote, ref["quote"]) or
                        (ref["target"] and ref["target"] not in quote) or
                        (not ref["target"] and ref["quote"] not in quote and ref["output"] not in quote.lower())):
                    continue
                choices.add(gid)
        if choices:
            quoted = {gid for gid, ref in gold.items() if _quote_matches(quote, ref["quote"])}
            if quoted:
                choices.intersection_update(quoted)
        candidates[key] = choices

    aligned = {}
    while candidates:
        # Disambiguate repeated semantics only through already-identified neighbours.
        # Edges between unique nodes are never a prerequisite for matching those nodes.
        for key, choices in candidates.items():
            choices.difference_update(aligned.values())
            if mode != "dag" or len(choices) < 2 or not aligned:
                continue
            parents = _ids(unique[key].get("depends_on"))
            children = {k for k in aligned if key in _ids(unique[k].get("depends_on"))}
            incoming = {aligned[k] for k in parents if k in aligned}
            outgoing = {aligned[k] for k in children}
            if not incoming and not outgoing:
                continue
            anchors = set(aligned.values())
            consistent = {g for g in choices if set(gold[g]["depends_on"]) & anchors == incoming
                          and {a for a in anchors if g in gold[a]["depends_on"]} == outgoing}
            if consistent:
                candidates[key] = consistent
        singletons = {key: next(iter(opts)) for key, opts in candidates.items() if len(opts) == 1}
        frequency = Counter(singletons.values())
        new = {key: g for key, g in singletons.items() if frequency[g] == 1}
        if not new:
            break
        aligned.update(new)
        for key in new:
            del candidates[key]
    return aligned, {key for key, opts in candidates.items() if opts}


def grade_plan(case, rows, mode="dag"):
    if mode not in {"dag", "subintent"}:
        raise ValueError("keyword routing does not emit a task plan")
    reason = ""
    try:
        if mode == "dag":
            Plan.for_execution(rows, case.message, "evaluation", "plan")
        else:
            _ordered_tasks(rows, case.message, mode)
    except (ValueError, TypeError) as exc:
        reason = str(exc)
    mapping, ambiguous = align_plan(case, rows, mode)
    rows = rows if isinstance(rows, list) else []
    gold = {n.id: n for n in case.nodes}
    result = dict(nodes_expected=len(gold), nodes_predicted=len(rows), nodes_correct=len(mapping),
                  ambiguous=len(ambiguous), contract_ok=not reason, contract_reason=reason,
                  exact=not reason and len(mapping) == len(gold) == len(rows))
    for metric in ("edges", "conditions"):
        for suffix in ("expected", "predicted", "correct"):
            result[f"{metric}_{suffix}"] = None
    if mode == "subintent":
        return result
    expected_edges = {(p, n.id) for n in case.nodes for p in n.after}
    observed = set()
    conditions_correct = edge_count = condition_count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = row.get("id")
        target = mapping.get(key) if isinstance(key, str) else None
        parents = _ids(row.get("depends_on"))
        edge_count += len(parents)
        observed.update((mapping[p], target) for p in parents if p in mapping and target)
        if "run_if" in row:
            condition_count += 1
            node = gold.get(target)
            cond = row["run_if"]
            if (node and node.condition and isinstance(cond, dict) and set(cond) == {"source", "quote"}
                    and isinstance(cond["quote"], str) and cond["quote"].rstrip("。！？.!?") == node.condition.rstrip("。！？.!?")
                    and row.get("quote") == node.quote
                    and isinstance(cond["source"], str) and cond["source"] in parents
                    and mapping.get(cond["source"]) == node.after[0]):
                conditions_correct += 1
    expected_conditions = sum(bool(n.condition) for n in case.nodes)
    correct_edges = len(expected_edges & observed)
    result.update(edges_expected=len(expected_edges), edges_predicted=edge_count, edges_correct=correct_edges,
                  conditions_expected=expected_conditions, conditions_predicted=condition_count,
                  conditions_correct=conditions_correct)
    accepted_extra = len(observed.intersection(getattr(case, "allowed_extra_edges", ())))
    result["allowed_extra_edges"] = accepted_extra
    result["exact"] &= (len(expected_edges) == correct_edges == edge_count - accepted_extra and
                        expected_conditions == conditions_correct == condition_count)
    return result
