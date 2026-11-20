"""Full-request context accounting shared by the retail loop and transport."""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import logging
from typing import Callable, Mapping

from core.dag_tool_log import protocol_pairs
from evaluation.retail_budget import INPUT_COUNT_BASIS, request_input_upper_tokens


class ContextLimit(RuntimeError):
    """A local refusal before quota, reservation or dispatch."""

    plan_status = "context_limit"

    def __init__(self, decision):
        self.decision = decision
        self.partial = None
        super().__init__(f"context_limit ({decision['stage']}, {decision['reason']})")


@dataclass(frozen=True)
class ModelProfile:
    """Scoped to one transport. count must bound its *fully rendered* input.

    The transport must guarantee that max_tokens caps all generated tokens,
    including thinking. Leave count unset for an unverified deployment.
    """
    model: str
    window: int
    margin: int
    revision: str
    count: Callable[[dict], int] | None = None

    def __post_init__(self):
        if (not self.model or not self.revision or type(self.window) is not int or type(self.margin) is not int
                or not 0 <= self.margin < self.window or (self.count is not None and not callable(self.count))):
            raise ValueError("invalid context window profile")


@dataclass(frozen=True)
class Measurement:
    model: str
    basis: str
    input: int | None
    output: int
    window: int | None
    margin: int | None
    revision: str | None
    verified: bool
    reason: str


def _log(event):
    logging.getLogger(__name__).info("model_context_budget %s", event)


class ModelRequestBudgetGuard:
    def __init__(self, mode="off", profiles: Mapping[str, ModelProfile] | None = None, report=_log):
        if mode not in {"off", "observe", "enforce"}:
            raise ValueError("RETAIL_MODEL_CONTEXT_MODE must be off, observe or enforce")
        self.mode = mode
        self.profiles = dict(profiles or {})
        if any(key != profile.model for key, profile in self.profiles.items()):
            raise ValueError("profile key must match model")
        self.report = report
        self.invalid_profiles = set()

    @classmethod
    def for_endpoint(cls, mode, base_url):
        profiles = {}
        if (base_url or "").rstrip("/") == "https://api.deepseek.com/anthropic":
            for name, rev in (("deepseek-flash", "V4.1-Flash-1017"), ("deepseek-v4-pro", "V4-Pro-0813")):
                profiles[name] = ModelProfile(name, 1_000_000, 1024, rev)
        return cls(mode, profiles)

    def measure(self, request):
        model = request.get("model", "")
        profile = self.profiles.get(model)
        output = request.get("max_tokens")
        verified = bool(profile and profile.count and model not in self.invalid_profiles)
        reason = ""
        try:
            counter = profile.count if verified else request_input_upper_tokens
            size = counter(request)
            if type(size) is not int or size < 0:
                raise ValueError("invalid input count")
        except Exception:  # A counter failure must never authorize a send.
            size, reason = None, "uncountable_request"
        if type(output) is not int or output <= 0:
            output, reason = 0, "unknown_output_reserve"
        if (set(request.get("extra_body") or {}) - {"thinking"}
                or request.get("extra_headers") or request.get("extra_query")):
            reason = "unverified_provider_override"
        if not reason:
            if not profile:
                reason = "unknown_model_window"
            elif not verified and self.mode == "enforce":
                reason = "unverified_counter"
            elif size + output + profile.margin > profile.window:
                reason = "window_exceeded"
        basis = getattr(counter, "__name__", type(counter).__name__) if verified else INPUT_COUNT_BASIS
        return Measurement(model, basis, size, output, profile.window if profile else None,
                           profile.margin if profile else None, profile.revision if profile else None,
                           verified, reason)

    def decision(self, stage, measure, layer, *, before=None, actions=(), details=None):
        event = dict(kind="context_budget", mode=self.mode, stage=stage, layer=layer,
                     **asdict(measure), before=before.input if before else measure.input,
                     actions=list(actions), action="allow", **(details or {}))
        if measure.reason:
            event["action"] = "would_stop" if self.mode == "observe" else "stop"
        elif actions:
            event["action"] = "would_trim" if self.mode == "observe" else "trim"
        self.report(event)
        return event

    def final(self, stage, request):
        if self.mode == "off":
            return None
        measurement = self.measure(request)
        if not valid_tool_protocol(request.get("messages") or []):
            measurement = replace(measurement, reason="invalid_tool_protocol")
        decision = self.decision(stage, measurement, "final")
        if self.mode == "enforce" and measurement.reason:
            raise ContextLimit(decision)
        return decision

    def usage(self, stage, event, usage):
        if not event or usage is None:
            return
        read = usage.get if isinstance(usage, dict) else lambda name: getattr(usage, name, None)
        if read("input_tokens") is None:
            return
        actual = sum(read(key) or 0 for key in ("input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"))
        delta = event["input"] - actual if event["input"] is not None else None
        if event["verified"] and (delta is not None and delta < 0 or (read("output_tokens") or 0) > event["output"]):
            self.invalid_profiles.add(event["model"])
        self.report(dict(kind="context_budget_usage", stage=stage, model=event["model"], actual_input=actual,
                         difference=delta, invalidated=event["model"] in self.invalid_profiles))


def valid_tool_protocol(messages):
    """A tool batch is immediately followed by its corresponding user results."""
    pending, seen = [], set()
    for message in messages:
        if not isinstance(message, dict):
            return False
        content = message.get("content")
        blocks = content if isinstance(content, list) else []
        if not all(isinstance(b, dict) for b in blocks):
            return False
        calls = [b for b in blocks if b.get("type") == "tool_use"]
        results = [b for b in blocks if b.get("type") == "tool_result"]
        if pending or results:
            if (message.get("role") != "user" or blocks[:len(results)] != results
                    or not protocol_pairs(pending, results)):
                return False
        if calls:
            ids = [b.get("id") for b in calls]
            if (message.get("role") != "assistant" or not all(isinstance(i, str) and i for i in ids)
                    or len(set(ids)) != len(ids) or seen.intersection(ids)):
                return False
            seen.update(ids)
        pending = calls
    return not pending
