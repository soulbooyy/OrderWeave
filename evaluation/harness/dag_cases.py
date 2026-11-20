"""Frozen stage-three inputs, independent annotations and evaluation protocol."""
from dataclasses import dataclass

SEED_SHA256 = "413a65160adbdb5fde0ffc0015c49b6d70250b10c18128de169b597af7766765"
REPEATS = 2
MODES = ("keyword", "subintent", "dag")
CONFIRM = "Yes, please proceed."
TOOLS = {"order_lookup": "get_order_details", "product_info": "get_item_details",
         "return_change": "return_delivered_order_items"}


@dataclass(frozen=True)
class Gold:
    id: str
    kind: str
    quote: str
    target: str
    field: str
    value: object
    after: tuple = ()
    condition: str = ""
    applies: bool = True

    @property
    def role(self):
        return "technical" if self.kind == "product_info" else "billing"

    @property
    def tool(self):
        return TOOLS[self.kind]

    def planned(self):
        row = dict(id=self.id, kind=self.kind, quote=self.quote, target=self.target, output=self.field,
                   depends_on=list(self.after))
        if self.condition:
            row["run_if"] = {"source": self.after[0], "quote": self.condition}
        return row


@dataclass(frozen=True)
class Case:
    key: str
    message: str
    order: str
    nodes: tuple
    # Required facts in the published conversation. Match attributes with their values.
    answers: tuple
    write_args: tuple = ()

    @property
    def turns(self):
        return (self.message, CONFIRM) if self.write_args else (self.message,)


def branch(key, order, count):
    count_quote = f"Count the items in order {order}"
    yes = "check the color of variant 8722653925"
    no = "check the size of variant 5253880258"
    condition = f"If the order has at least 3 items, {yes}; otherwise {no}"
    return Case(key, f"{count_quote}. {condition}.", order, (
        Gold("t1", "order_lookup", count_quote, order, "purchased_item_count", count),
        Gold("t2", "product_info", yes, "8722653925", "color", "white", ("t1",), condition, count >= 3),
        Gold("t3", "product_info", no, "5253880258", "size", "XXL", ("t1",), condition, count < 3),
    ), (rf"\b{count} items?\b", r"color.{0,12}\bwhite\b" if count >= 3 else r"size.{0,12}\bXXL\b"))


# The repeated specialist and the join use distinct targets. Read steps have no side effects.
first = "Check the color of variant 2143041831"
count = "count the items in order #W9318778"
last = "check the size of variant 6342039236"
read = "Count the items in order #W7303089"
write = "return item 2492465580 from order #W7303089 to credit_card_4387170"
CASES = (
    branch("two-shirts", "#W9549057", 2), branch("four-thermostat", "#W8935389", 4),
    Case("repeat-specialist", f"{first}. Then {count}. Then {last}.", "#W9318778", (
        Gold("t1", "product_info", first, "2143041831", "color", "black"),
        Gold("t2", "order_lookup", count, "#W9318778", "purchased_item_count", 5, ("t1",)),
        Gold("t3", "product_info", last, "6342039236", "size", "full size", ("t2",)),
    ), (r"color.{0,12}\bblack\b", r"\b5 items?\b", r"size.{0,12}\bfull size\b")),
    Case("two-inputs", f"{first}. Also {count}. After both, {last}.", "#W9318778", (
        Gold("t1", "product_info", first, "2143041831", "color", "black"),
        Gold("t2", "order_lookup", count, "#W9318778", "purchased_item_count", 5),
        Gold("t3", "product_info", last, "6342039236", "size", "full size", ("t1", "t2")),
    ), (r"color.{0,12}\bblack\b", r"\b5 items?\b", r"size.{0,12}\bfull size\b")),
    Case("confirmed-backpack", f"{read}. Then {write}.", "#W7303089", (
        Gold("t1", "order_lookup", read, "#W7303089", "purchased_item_count", 2),
        Gold("t2", "return_change", write, "#W7303089", "executed", True, ("t1",)),
    ), (r"\b2 items?\b", r"(?:requested|submitted|return request recorded)"),
         ("2492465580", "credit_card_4387170")),
)
