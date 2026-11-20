"""Deterministic serial scheduling state shared by both retail executors."""
from dataclasses import asdict, dataclass, field, replace

from core.retail_dag import Condition, Node, Plan, Run, Status


@dataclass
class Execution:
    plan: Plan
    runs: dict[str, Run]
    bound_user: str = ""
    revision: int = 0
    operation: dict = field(default_factory=dict)
    selected: dict = field(default_factory=dict)
    stop: str = ""

    @classmethod
    def start(cls, plan, bound_user=""):
        return cls(plan, {t.id: Run(plan.id, t.id) for t in plan.nodes}, bound_user or "")

    def next_node(self, turn, limit=12):
        if self.stop or any(r.status == Status.RUNNING for r in self.runs.values()):
            return None
        claimed = sum(r.turn == turn for r in self.runs.values())
        nodes = {t.id: t for t in self.plan.nodes}
        for task in self.plan.nodes:
            run = self.runs[task.id]
            if run.status == Status.BLOCKED and run.reason == "dependency":
                run.move(Status.PENDING)
            if run.status == Status.PENDING:
                if task.run_if:
                    match = task.run_if.evaluate(self.runs[task.run_if.source])
                    if match is False:
                        run.move(Status.SKIPPED, reason="condition_false", refs=self.runs[task.run_if.source].refs)
                        continue
                    if match is None:
                        run.move(Status.BLOCKED, reason="dependency")
                        continue
                for dep in task.depends_on:
                    parent = self.runs[dep]
                    if parent.status == Status.SUCCEEDED:
                        continue
                    # An explicitly complementary successful sibling satisfies a branch join.
                    cond = nodes[dep].run_if
                    if (parent.status == Status.SKIPPED and cond and any(
                            nodes[p].run_if == replace(cond, otherwise=not cond.otherwise)
                            and self.runs[p].status == Status.SUCCEEDED for p in task.depends_on)):
                        continue
                    run.move(Status.BLOCKED, reason="dependency")
                    break
                else:
                    run.move(Status.READY)
            if run.status == Status.READY and run.turn != turn:
                if claimed >= min(limit, len(nodes)):
                    self.stop = "dispatch_limit"
                    return None
                return task
        return None

    def inputs(self, task):
        return {dep: dict(self.runs[dep].facts) for dep in task.depends_on
                if self.runs[dep].status == Status.SUCCEEDED}

    def target(self, task):
        if task.target or task.id in self.selected:
            return task.target or self.selected[task.id], []
        inputs = self.inputs(task)
        if task.kind == "product_info":
            candidates = [set(f[field]) for f in inputs.values() for field in ("item_ids", "variants") if field in f]
            if candidates:
                values = sorted(set.intersection(*candidates))
                return (values[0], []) if len(values) == 1 else ("", values)
        orders = {f["order_id"] for f in inputs.values() if f.get("order_id")}
        return (orders.pop(), []) if len(orders) == 1 else ("", [])

    @property
    def complete(self):
        return all(r.status in (Status.SUCCEEDED, Status.SKIPPED) for r in self.runs.values())

    def dump(self):
        return asdict(self)

    @classmethod
    def load(cls, data):
        plan = dict(data["plan"])
        if plan["version"] != 1:
            raise ValueError("unknown DAG version")
        plan["nodes"] = tuple(Node(**{**n, "depends_on": tuple(n["depends_on"]),
                                     "run_if": Condition(**n["run_if"]) if n["run_if"] else None})
                              for n in plan["nodes"])
        runs = {key: Run(**{**r, "status": Status(r["status"]), "refs": tuple(r["refs"])})
                for key, r in data["runs"].items()}
        return cls(**{**data, "plan": Plan(**plan), "runs": runs})
