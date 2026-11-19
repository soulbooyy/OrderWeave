"""Synthetic dispatch cases. Expected roles and required precedence are independent annotations."""
from dataclasses import dataclass


@dataclass(frozen=True)
class Case:
    key: str
    message: str
    intent: str
    # Frozen simulated provider output, in mention order. Dependency indices are zero-based.
    nodes: tuple
    primary: str
    helpers: tuple = ()
    before: tuple = ()


CASES = (
    Case("tracking", "只查一下订单物流。", "order_status",
         (("order_lookup", "查一下订单物流"),), "general"),
    Case("negated_refund", "不用退款，只查一下订单物流。", "order_status",
         (("order_lookup", "查一下订单物流"),), "general"),
    Case("negated_payment", "No payment change please. Where is my parcel?", "order_status",
         (("order_lookup", "Where is my parcel?"),), "general"),
    Case("background_error", "页面原来报错，现在好了。看看配送到了哪。", "order_status",
         (("order_lookup", "看看配送到了哪"),), "general"),
    Case("catalog", "耳机支持哪种接口？", "query",
         (("product_info", "耳机支持哪种接口"),), "technical"),
    Case("cancel", "I ordered the wrong thing. Please cancel the order.", "refund",
         (("order_change", "Please cancel the order"),), "general"),
    Case("return", "Please return the headphones.", "refund",
         (("return_change", "return the headphones"),), "billing"),
    Case("policy", "配送政策是什么？", "query",
         (("general_policy", "配送政策是什么"),), "general"),
    Case("product_then_policy", "先查这款的接口，再根据规格解释退货条款。", "query",
         (("product_info", "查这款的接口"), ("return_policy", "解释退货条款", 0)),
         "billing", ("technical",), (("technical", "billing"),)),
    Case("policy_then_product", "First explain the return policy; based on that explain the relevant catalog specs.",
         "query", (("return_policy", "explain the return policy"), ("product_info", "explain the relevant catalog specs", 0)),
         "technical", ("billing",), (("billing", "technical"),)),
    Case("reverse_mention", "告诉我退货条款，但先查接口规格，因为条款取决于型号。", "query",
         (("return_policy", "告诉我退货条款", 1), ("product_info", "先查接口规格")),
         "billing", ("technical",), (("technical", "billing"),)),
    Case("read_before_write", "先核对耳机规格，再帮我办理退货。", "refund",
         (("product_info", "核对耳机规格"), ("return_change", "帮我办理退货", 0)),
         "billing", ("technical",), (("technical", "billing"),)),
    Case("write_barrier", "Cancel the order. Only after cancellation, explain the compatible accessory specs.",
         "refund", (("order_change", "Cancel the order"), ("product_info", "explain the compatible accessory specs", 0)),
         "general"),
    Case("same_role", "查一下配送进度，再告诉我订单状态。", "order_status",
         (("order_lookup", "查一下配送进度"), ("order_lookup", "告诉我订单状态", 0)), "general"),
    Case("handoff", "请转人工客服。", "human_handoff", (), "escalation"),
    Case("missing_extraction", "不用退款，我只问配送进度。", "order_status", (), "general"),
    # Deliberately inaccurate but well-formed extraction. Verbatim evidence cannot catch this.
    Case("bad_extraction", "No refund please. Can you track my parcel?", "order_status",
         (("return_change", "No refund please"),), "general"),
)
