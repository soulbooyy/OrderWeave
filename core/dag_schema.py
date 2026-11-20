"""Wire format for model-generated retail plans. The executor consumes compiled nodes."""
from typing import Annotated, Literal, get_args

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

NodeID = Annotated[str, StringConstraints(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,31}$")]
Span = Annotated[str, StringConstraints(min_length=1, max_length=512)]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Branch(StrictModel):
    source: NodeID = Field(description="Direct predecessor providing the tested fact.")
    branch: Literal["then", "else"] = Field(description="Polarity of this exact action in the original conditional.")


class TaskFields(StrictModel):
    id: NodeID
    quote: Span = Field(description="Verbatim user span. For conditional actions use exactly the action without if, then, otherwise, or terminal punctuation.")
    depends_on: Annotated[list[NodeID], Field(max_length=12)] = Field(description="Direct prerequisites only. [] for an independent action.")
    target: Annotated[str, StringConstraints(max_length=128)] = Field(description="Literal identifier from the span. For order reads and order transactions use the order ID, for variants lists the product ID, for a specification the item/variant ID. Use an empty string for a target supplied by predecessors.")
    run_if: Branch | None = Field(default=None, description="Required on both conditional actions. Omit on unconditional actions.")


class OrderRead(TaskFields):
    kind: Literal["order_lookup"]
    output: Literal["order_id", "status", "purchased_item_count", "item_ids"]


class BillingRead(TaskFields):
    kind: Literal["billing_lookup"]
    output: Literal["order_id", "status", "purchased_item_count", "item_ids", "payment_type"]


class Catalog(TaskFields):
    kind: Literal["product_info"]
    output: Literal["variants", "ventilation", "sensor_type", "size", "color"]


class Policy(TaskFields):
    kind: Literal["general_policy", "return_policy"]
    output: Literal["policy"]


class Change(TaskFields):
    kind: Literal["order_change", "return_change"]
    output: Literal["executed"]


TaskData = Annotated[OrderRead | BillingRead | Catalog | Policy | Change, Field(discriminator="kind")]


class DagResponse(StrictModel):
    intent: Literal["order_status", "refund", "query", "human_handoff", "greeting", "other"]
    confidence: Annotated[float, Field(ge=0, le=1, allow_inf_nan=False)]
    reasoning: Annotated[str, StringConstraints(max_length=512)] = ""
    subtasks: Annotated[list[TaskData], Field(max_length=12)]


OUTPUTS = {kind: set(get_args(cls.model_fields["output"].annotation))
           for cls in get_args(TaskData.__origin__)
           for kind in get_args(cls.model_fields["kind"].annotation)}
