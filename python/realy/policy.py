"""The decision policy: ONE Python definition of a rule set and its precedence, used by the verifier,
what-ifs, explanations and the Explorer. It mirrors io.realy.scoring.rules.RuleSet (Java), which the
pipeline runs; `realy verify` proves the two agree on every decision of a run.

A rule set (rules/*.yaml):

    version: baseline-v4
    thresholds: {review: 0.5, challenge: 0.5, decline: 0.9}   # challenge is optional
    challenge:                     # which payments CAN be stepped up (CEL over `event`); optional
      when: "!event.recurring && event.entry_mode in ['ECOM_3DS', 'ECOM_NON_3DS', 'CHIP', 'BANK_APP']"
    rules:
      - {id: CARD_TESTING, action: DECLINE, when: "f.card_small_cnp_count_10m >= 3"}
      - {id: ATO_NEW_DEVICE, action: CHALLENGE, when: "..."}

Precedence (first that applies wins):
    DECLINE rule > ALLOW rule > score >= decline
    > CHALLENGE rule (REVIEW when the payment cannot be stepped up)
    > score >= challenge (challengeable payments only)
    > REVIEW rule > score >= review > APPROVE
A missing score (rules-only fallback) skips the score branches. Without a `challenge` threshold the
challenge band does not exist; without a `challenge.when` every payment is challengeable.
"""
from __future__ import annotations

import math
from typing import Any

import yaml

APPROVE, REVIEW, DECLINE, CHALLENGE = 1, 2, 3, 4
NAMES = {0: "UNSPECIFIED", APPROVE: "APPROVE", REVIEW: "REVIEW", DECLINE: "DECLINE", CHALLENGE: "CHALLENGE"}
CODES = {v: k for k, v in NAMES.items()}
ACTIONS = ("DECLINE", "REVIEW", "CHALLENGE", "ALLOW", "APPROVE")
FLAGGED = (REVIEW, DECLINE, CHALLENGE)      # any intervention on the payment


class RuleSet:
    def __init__(self, yaml_text: str) -> None:
        import cel
        doc = yaml.safe_load(yaml_text) or {}
        self.doc = doc
        self.version = doc.get("version")
        th = doc.get("thresholds") or {}
        self.review = float(th.get("review", 0.5))
        self.decline = float(th.get("decline", 0.9))
        ch = th.get("challenge")
        self.challenge = float(ch) if ch not in (None, 0) else None
        self.challenge_when = ((doc.get("challenge") or {}).get("when") or "").strip() or None
        self._ch = cel.compile(self.challenge_when) if self.challenge_when else None
        self.rules = []
        for r in doc.get("rules") or []:
            act = r.get("action", "REVIEW")
            if act not in ACTIONS:
                raise ValueError(f"rule {r.get('id')}: unknown action {act}")
            self.rules.append((r["id"], act, cel.compile(r["when"])))

    def with_thresholds(self, review: float | None = None, decline: float | None = None,
                        challenge: float | None = None) -> "RuleSet":
        """Live-tuning overrides (ScoringConfig.*_threshold): a falsy value keeps the rule set's own."""
        import copy
        out = copy.copy(self)
        out.review = review or self.review
        out.decline = decline or self.decline
        out.challenge = challenge or self.challenge
        return out

    @property
    def uses_challenge(self) -> bool:
        return self.challenge is not None or any(a == "CHALLENGE" for _, a, _ in self.rules)

    def challengeable(self, ev: dict[str, Any]) -> bool:
        if self._ch is None:
            return True
        import cel
        ctx = cel.Context()
        ctx.add_variable("event", ev)
        return bool(self._ch.execute(ctx))

    def evaluate(self, f: dict[str, float], ev: dict[str, Any]) -> list[tuple[str, str]]:
        import cel
        ctx = cel.Context()
        ctx.add_variable("f", f)
        ctx.add_variable("event", ev)
        return [(rid, act) for rid, act, p in self.rules if p.execute(ctx)]

    def decide(self, hits: list[tuple[str, str]], score: float, challengeable: bool = True) -> int:
        return decide({a for _, a in hits}, score, self.review, self.decline, self.challenge, challengeable)[0]


def decide(acts: set[str], score: float | None, review: float, decline: float, challenge: float | None,
           challengeable: bool) -> tuple[int, str]:
    """(outcome, branch) under the precedence above."""
    has = score is not None and not (isinstance(score, float) and math.isnan(score))
    if "DECLINE" in acts:
        return DECLINE, "decline rule"
    if "ALLOW" in acts or "APPROVE" in acts:
        return APPROVE, "allow rule"
    if has and score >= decline:
        return DECLINE, "score decline"
    if "CHALLENGE" in acts:
        return (CHALLENGE, "challenge rule") if challengeable else (REVIEW, "challenge rule, not challengeable")
    if has and challenge is not None and score >= challenge and challengeable:
        return CHALLENGE, "score challenge"
    if "REVIEW" in acts:
        return REVIEW, "review rule"
    if has and score >= review:
        return REVIEW, "score review"
    return APPROVE, "approve"
