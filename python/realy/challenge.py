"""Simulated step-up (CHALLENGE) results: the instrument's side of a 3DS / SCA challenge, a PIN
prompt or an in-app confirmation.

When the policy answers CHALLENGE, a real payment continues only if the customer completes the
step-up. Who completes it is a property of the PERSON paying, not of the pipeline: a genuine customer
usually passes (and sometimes gives up: a lost sale), a card tester has no OTP or PIN and fails, a
phished account-takeover passes a third of the time, an APP-scam victim authorises their own transfer.

So every event carries a latent result, drawn by the instrument and never sent to the system under
test. It is a pure function of the event id, the scenario seed and the population's rates
(scenario `fraud.challenge`, else the typology's `challenge` block), so it is:
  * identical for the report, the verifier, what-ifs and the Explorer (nothing stored, nothing to drift);
  * defined for EVERY event, so a what-if with a different rule set can replay step-ups the run
    never issued;
  * independent of generation order and of every other random stream (adding it changed no event).
"""
from __future__ import annotations

from typing import Any

import numpy as np

PASS, FAIL, ABANDON = 0, 1, 2
RESULTS = {PASS: "pass", FAIL: "fail", ABANDON: "abandon"}
# the payment's fate (instrument-only codes beyond the pipeline's Outcome enum)
APPROVED, REVIEWED, DECLINED, CHALLENGED, ABANDONED, REVIEWED_PAID = 1, 2, 3, 4, 5, 6
FATE = {APPROVED: "approved", REVIEWED: "held for review", DECLINED: "declined", ABANDONED: "abandoned at step-up",
        REVIEWED_PAID: "approved, reviewed afterwards"}
PAID = (APPROVED, REVIEWED_PAID)                 # the money moved
STOPPED = (REVIEWED, DECLINED, ABANDONED)        # the money did not move (a held review is worked first)

_M = (1 << 64) - 1


def _u01(event_ids: np.ndarray, seed: int) -> np.ndarray:
    """A uniform [0, 1) per event: splitmix64 of the event id, salted by the scenario seed."""
    with np.errstate(over="ignore"):
        z = np.asarray(event_ids, np.uint64) ^ np.uint64((seed * 0x9E3779B97F4A7C15 + 0xC4A11E46E) & _M)
        z = z + np.uint64(0x9E3779B97F4A7C15)
        z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
        z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
        z = z ^ (z >> np.uint64(31))
    return (z >> np.uint64(11)).astype(np.float64) / float(1 << 53)


def rates(scenario: Any, registry: Any) -> tuple[tuple[float, float], dict[int, tuple[float, float]]]:
    """(legitimate (pass, abandon), {fraud_type: (pass, abandon)}); fail is the remainder."""
    spec = scenario.fraud.challenge
    lg = (spec.legitimate.passed, spec.legitimate.abandon)
    by: dict[int, tuple[float, float]] = {}
    for t in registry.all:
        r = spec.typologies.get(t.name) or t.challenge
        if r is not None:
            by[t.id] = (r.passed, r.abandon)
    return lg, by


def draw(event_ids: np.ndarray, is_fraud: np.ndarray, fraud_type: np.ndarray, seed: int,
         legit: tuple[float, float], by_type: dict[int, tuple[float, float]]) -> np.ndarray:
    """Latent step-up result per event (PASS / FAIL / ABANDON). Friendly fraud and other label-only
    noise is a genuine customer (is_fraud false) and uses the legitimate rates. A typology without
    rates fails (no OTP, no PIN)."""
    u = _u01(event_ids, seed)
    p = np.full(u.shape, legit[0])
    a = np.full(u.shape, legit[1])
    fr = np.asarray(is_fraud, bool)
    ft = np.asarray(fraud_type)
    p[fr], a[fr] = 0.0, 0.0
    for t, (tp, ta) in by_type.items():
        m = fr & (ft == t)
        p[m], a[m] = tp, ta
    out = np.full(u.shape, FAIL, np.int8)
    out[u < p] = PASS
    out[(u >= p) & (u < p + a)] = ABANDON
    return out


def final(outcome: np.ndarray, result: np.ndarray, review_holds: np.ndarray | None = None) -> np.ndarray:
    """The payment's fate: CHALLENGE + pass -> APPROVED, + fail -> DECLINED, + abandon -> ABANDONED;
    REVIEW -> REVIEWED where a review holds the payment, else REVIEWED_PAID (approved, worked
    afterwards). `review_holds` None: every review holds (the pipeline's own view)."""
    o = np.asarray(outcome).astype(np.int8).copy()
    ch = o == CHALLENGED
    r = np.asarray(result)
    rv = o == REVIEWED
    o[ch & (r == PASS)] = APPROVED
    o[ch & (r == FAIL)] = DECLINED
    o[ch & (r == ABANDON)] = ABANDONED
    if review_holds is not None:
        o[rv & ~np.asarray(review_holds, bool)] = REVIEWED_PAID
    return o


def review_holds(payment_type: np.ndarray, config: dict | None = None) -> np.ndarray:
    """Per event: does a REVIEW hold this payment (contracts/insights.yaml `review_holds`)?"""
    from .features.derived import PAYMENT_TYPE
    if config is None:
        from .report.insights import load_config
        config = load_config()
    names = set(config.get("review_holds", ["TRANSFER"]))
    codes = [i for i, n in enumerate(PAYMENT_TYPE) if n in names]
    return np.isin(np.asarray(payment_type), codes)


def for_run(run_dir: Any, event_ids: np.ndarray, is_fraud: np.ndarray, fraud_type: np.ndarray) -> np.ndarray:
    """Latent results for a finished run's events, with the scenario and typology definitions the run
    snapshotted."""
    import json
    from pathlib import Path

    from .scenario import Scenario
    from .usecases import for_run as reg_for_run
    sc = Scenario.model_validate(json.loads((Path(run_dir) / "scenario.json").read_text()))
    lg, by = rates(sc, reg_for_run(run_dir))
    return draw(event_ids, is_fraud, fraud_type, sc.seed, lg, by)
