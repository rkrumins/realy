"""Expand the generator's transfers into the messages of a multi-message transaction
(contracts/transactions/<name>.yaml, `generate:`).

The unified transfer the generator already draws (with its customer, payee, amount, device, IP, typology
and labels) is the ground truth: its parts are drawn so that they assemble back into exactly that event
(the initiation reuses its event id, so the assembled transaction keeps it, and labels and the collector
join on it unchanged). What the parts add is the flow itself: the payee check and its result, the device
and session signals, authentication, the submission (perhaps to another payee) and the scheme's status,
each at its own time, with fraud typologies shaping them (an APP scam victim proceeds despite a payee
mismatch while on a call; account takeover shows remote access and failed authentication).

Every record keeps its own event time; parts that fall after the batch's window wait for a later batch,
so the emitter sends everything in event-time order (one customer's messages stay ordered on its
partition, which is what makes assembly deterministic).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from ..bindings import event_type
from ..bindings.codec import message_class
from ..bindings.transactions import Transaction

# unified channel -> a2a channel (the contract maps it back)
_A2A_CHANNEL = {"IN_APP": "A2A_MOBILE_APP", "ONLINE_BANKING": "A2A_ONLINE_BANKING", "POS": "A2A_BRANCH"}


@dataclass
class Record:
    """One message to send: its event time, type ("" = the default unified type), payload and key; `scored`
    marks the record whose event id names the decision (a unified event, or a transaction's trigger)."""
    t_us: int
    type: str
    payload: bytes
    key: bytes
    event_id: int
    scored: bool
    order: int = 0


def _pick(rng: np.random.Generator, dist: dict[str, float]) -> str:
    names = list(dist)
    p = np.asarray([dist[n] for n in names], np.float64)
    return names[int(rng.choice(len(names), p=p / p.sum()))]


class FlowExpander:
    """Expands one transaction contract's flows; `expand` is called once per generated batch."""

    def __init__(self, txn: Transaction, rng: np.random.Generator, fraud_names: dict[int, str], gen_id: int,
                 score_stages: bool = False) -> None:
        g = txn.generate or {}
        if not g:
            raise ValueError(f"transaction {txn.name} has no `generate:` section")
        self.txn = txn
        self.rng = rng
        self.fraud_names = fraud_names
        self.gen_id = gen_id
        self.payment_type = g.get("payment_type", "TRANSFER")
        self.delays = {k: tuple(v) for k, v in (g.get("delays_ms") or {}).items()}
        self.rates = dict(g.get("rates") or {})
        self.fraud = {k: dict(v or {}) for k, v in (g.get("fraud") or {}).items()}
        self.codecs = {role: event_type(t).codec for role, t in txn.roles.items()}
        self.classes = {role: message_class(event_type(t).message) for role, t in txn.roles.items()}
        self.score_stages = score_stages   # scored lifecycle stages are decisions of their own (expected by the collector)
        self.pending: list[Record] = []
        self.seq = 0
        self.parts_sent = 0

    # ---------------------------------------------------------------- helpers
    def _rate(self, ft: int, key: str) -> Any:
        over = self.fraud.get(self.fraud_names.get(ft, ""), {})
        return over[key] if key in over else self.rates.get(key)

    def _delay_us(self, part: str) -> int:
        lo, hi = self.delays.get(part, (0, 0))
        return int(self.rng.uniform(lo, hi) * 1000)

    def _enum(self, role: str, field: str, name: str) -> int:
        f = self.codecs[role].by_name[field]
        for n, nm in f.enum_names:
            if nm == name:
                return n
        raise ValueError(f"{role}.{field}: no enum value {name}")

    def _part_id(self) -> int:
        """Event id of a part: the generator's id space with bit 47 set (unified events use seq < 2^47)."""
        self.seq += 1
        return (self.gen_id << 48) | (1 << 47) | self.seq

    def _record(self, role: str, t_us: int, fields: dict[str, Any], key: bytes, event_id: int, scored: bool) -> Record:
        msg = self.classes[role](**fields)
        return Record(t_us, self.txn.roles[role], msg.SerializeToString(), key, event_id, scored)

    # ---------------------------------------------------------------- expansion
    def expand(self, cols: dict[str, np.ndarray], payloads: list[bytes], keys: list[bytes], t1: int) -> list[Record]:
        """All records of a batch in event-time order: its unified events, with each transfer replaced by the
        parts that fall before `t1` (later parts wait for the batch they fall in)."""
        from ..features.derived import CHANNEL, PAYMENT_TYPE
        pt = PAYMENT_TYPE.index(self.payment_type)
        out: list[Record] = []
        n = len(payloads)
        ptype = cols["payment_type"]
        for i in range(n):
            if int(ptype[i]) != pt:
                out.append(Record(int(cols["event_time_us"][i]), "", payloads[i], keys[i], int(cols["event_id"][i]), True))
                continue
            out += self._flow(cols, i, keys[i], CHANNEL)
        due = [r for r in self.pending if r.t_us < t1]
        self.pending = [r for r in self.pending if r.t_us >= t1]
        later = [r for r in out if r.t_us >= t1]
        out = [r for r in out if r.t_us < t1] + due
        self.pending += later
        for j, r in enumerate(out):
            r.order = j
        out.sort(key=lambda r: (r.t_us, r.order))
        self.parts_sent += sum(1 for r in out if r.type)
        return out

    def drain(self) -> list[Record]:
        """Parts still waiting at the end of the run (sent last, in order)."""
        out, self.pending = sorted(self.pending, key=lambda r: r.t_us), []
        return out

    def _flow(self, c: dict[str, np.ndarray], i: int, key: bytes, channel_names: list[str]) -> list[Record]:
        rng = self.rng
        eid = int(c["event_id"][i])
        t0 = int(c["event_time_us"][i])
        cust, acct, payee = int(c["customer_id"][i]), int(c["account_id"][i]), int(c["payee_id"][i])
        amount_minor, amount_usd, currency = int(c["amount_minor"][i]), int(c["amount_usd_minor"][i]), str(c["currency"][i])
        ft = int(c["fraud_type"][i])
        base = {"payment_id": eid, "customer_id": cust}
        recs: list[Record] = []
        ch = _A2A_CHANNEL.get(channel_names[int(c["channel"][i])], "A2A_ONLINE_BANKING")
        recs.append(self._record("initiation", t0, {
            **base, "event_id": eid, "event_time_us": t0, "account_id": acct, "payee_id": payee,
            "amount_minor": amount_minor, "currency": currency, "amount_usd_minor": amount_usd,
            "channel": self._enum("initiation", "channel", ch), "new_payee": bool(rng.random() < self._rate(ft, "new_payee")),
            "lat": float(c["lat"][i]), "lon": float(c["lon"][i])}, key, eid, True))
        if rng.random() < self._rate(ft, "device"):
            lo, hi = self._rate(ft, "session_age_s")
            t = t0 + self._delay_us("device")
            recs.append(self._record("device", t, {
                **base, "event_id": self._part_id(), "event_time_us": t, "device_id": int(c["device_id"][i]),
                "ip_v4": int(c["ip_v4"][i]), "ip_country": str(c["ip_country"][i]), "ip_asn": int(c["ip_asn"][i]),
                "session_age_s": int(rng.uniform(lo, hi)), "remote_access": bool(rng.random() < self._rate(ft, "remote_access")),
                "emulator": bool(rng.random() < self._rate(ft, "emulator")),
                "on_call": bool(rng.random() < self._rate(ft, "on_call"))}, key, 0, False))
        result = None
        t_check = t0 + self._delay_us("payee_check")
        if rng.random() < self._rate(ft, "payee_check"):
            result = _pick(rng, self._rate(ft, "payee_result"))
            recs.append(self._record("payee_check", t_check, {
                **base, "event_id": self._part_id(), "event_time_us": t_check, "payee_id": payee,
                "result": self._enum("payee_check", "result", result),
                "account_type_mismatch": bool(rng.random() < self._rate(ft, "account_type_mismatch"))}, key, 0, False))
        if result is not None and rng.random() < float((self._rate(ft, "abandon") or {}).get(result, 0.0)):
            return recs                    # warned, the customer gave up: no authentication, no submission
        t = t_check + self._delay_us("authentication")
        attempts = 2 if rng.random() < self._rate(ft, "sca_fail") else 1
        method = self._enum("authentication", "method", "SCA_APP_PUSH" if rng.random() < 0.7 else "SCA_SMS_OTP")
        for a in range(1, attempts + 1):
            recs.append(self._record("authentication", t, {
                **base, "event_id": self._part_id(), "event_time_us": t, "method": method,
                "passed": a == attempts, "attempt": a}, key, 0, False))
            t += int(rng.uniform(5_000, 40_000) * 1000) if a < attempts else 0
        t += self._delay_us("submission")
        sub_payee = payee
        if rng.random() < self._rate(ft, "payee_changed"):
            sub_payee = payee + 1 + int(rng.integers(0, 1000))
        sub_id = self._part_id()
        recs.append(self._record("submission", t, {
            **base, "event_id": sub_id, "event_time_us": t, "account_id": acct, "payee_id": sub_payee,
            "amount_minor": amount_minor, "currency": currency, "amount_usd_minor": amount_usd,
            "scheme": self._enum("submission", "scheme", "A2A_INSTANT")}, key, sub_id,
            self.score_stages and any(s.scored and s.role == "submission" for s in self.txn.stages)))
        t += self._delay_us("status")
        code = _pick(rng, self._rate(ft, "status"))
        recs.append(self._record("status", t, {
            **base, "event_id": self._part_id(), "event_time_us": t, "code": self._enum("status", "code", code)},
            key, 0, False))
        return recs
