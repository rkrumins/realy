"""Expand the generator's payments into the messages of multi-message transactions, declaratively
(contracts/transactions/<name>.yaml, `generate:`).

The unified payment the generator already draws (customer, card, payee, amount, device, IP, typology and
labels) is the ground truth. A contract's `generate.expands` picks which payments become its flow, and
`generate.steps` lists the messages, in order:

    steps:
      - role: initiation                    # the trigger: the payment's own event id and time
        set: { new_payee: { p: new_payee } }
      - role: payee_check
        after: initiation                   # its time: that step's time + `delay_ms`
        delay_ms: payee_check               # [lo, hi] ms, or a key of `delays_ms`
        p: payee_check                      # sent with this probability (else skipped; its time still counts)
        set: { result: { pick: payee_result } }
        stop: { field: result, p: abandon } # then the flow may end, with a probability per value
      - role: authentication
        repeat: { p: sca_fail, max: 2, gap_ms: [5000, 40000], counter: attempt,
                  earlier: { passed: false }, last: { passed: true } }
      - role: capture
        needs: [authorization]              # only if those steps were sent; `unless:` only if they were not
        if: { three_ds: { not: [THREE_DS_NONE] } }   # only for payments whose fields match (names or numbers)

Every message gets the link id, the partition key, its own event id (a part id: bit 47 of the generator's
id space) and its event time without being listed. Every field the assembly maps (`assemble.fields`, a
stage's `fields`) is filled from the payment by inverting that mapping, so the parts assemble back into
exactly the payment the generator drew. A field named like a payment field, of the same type, that no
mapping fills is taken from the payment too (a capture's card, merchant and amount). `set` adds the flow's
own fields, or overrides those:

    literal                     a value (an enum field takes its name)
    { p: R }                    true with probability R
    { pick: R }                 one of R's names, by weight (enum or string fields)
    { uniform: R }              uniform in R = [lo, hi] (integer fields round down)
    { col: name }               the payment's own field
    { alter: R }                with probability R, another value than the mapped one (another id; an
                                amount x U(1.1, 3)): what malware rewriting a submission looks like
                                (`col:` beside it names the base value)

R is a number / list / mapping, or the name of a `rates` entry; `fraud.<typology>` overrides rates by name,
so typologies shape the flow (an APP-scam victim proceeds despite a payee mismatch; card testing is
authorised but never captured; friendly fraud is captured then charged back).

Every record keeps its own event time; parts that fall after the batch's window wait for a later batch,
so the emitter sends everything in event-time order (one customer's messages stay ordered on its
partition, which is what makes assembly deterministic). Parts after the end of the run are not sent.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np

from ..bindings import event_type
from ..bindings.codec import message_class
from ..bindings.transactions import Transaction


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


@dataclass
class Step:
    role: str
    after: str | None = None
    delay: Any = None
    p: Any = None
    set: dict[str, Any] = field(default_factory=dict)
    stop: dict[str, Any] | None = None
    repeat: dict[str, Any] | None = None
    needs: tuple[str, ...] = ()
    unless: tuple[str, ...] = ()
    when: dict[str, Any] = field(default_factory=dict)


STEP_KEYS = {"role", "after", "delay_ms", "p", "set", "stop", "repeat", "needs", "unless", "if"}
VALUE_KINDS = ("p", "pick", "uniform", "col", "alter", "const")


def parse_steps(txn: Transaction) -> list[Step]:
    g = txn.generate or {}
    out = []
    for i, raw in enumerate(g.get("steps") or []):
        bad = set(raw) - STEP_KEYS
        if bad:
            raise ValueError(f"{txn.name}: generate.steps[{i}]: unknown keys {sorted(bad)} (one of {sorted(STEP_KEYS)})")
        out.append(Step(role=raw["role"], after=raw.get("after"), delay=raw.get("delay_ms"), p=raw.get("p"),
                        set=dict(raw.get("set") or {}), stop=raw.get("stop"), repeat=raw.get("repeat"),
                        needs=tuple(raw.get("needs") or ()), unless=tuple(raw.get("unless") or ()),
                        when=dict(raw.get("if") or {})))
    return out


def validate_generate(txn: Transaction) -> list[str]:
    """Problems with a contract's `generate:` section (empty: it can be generated)."""
    g = txn.generate or {}
    if not g:
        return []
    p: list[str] = []
    try:
        steps = parse_steps(txn)
    except (ValueError, KeyError) as e:
        return [str(e)]
    if not steps:
        return [f"{txn.name}: generate has no steps"]
    names = [s.role for s in steps]
    if txn.trigger not in names:
        p.append(f"{txn.name}: generate.steps never sends the trigger {txn.trigger!r}")
    seen: set[str] = set()
    pay = event_type(txn.into).codec
    for s in steps:
        if s.role not in txn.roles:
            p.append(f"{txn.name}: generate step {s.role!r} is not one of the roles {txn.role_names}")
            continue
        codec = event_type(txn.roles[s.role]).codec
        for ref in (s.after, *s.needs, *s.unless):
            if ref is not None and ref not in seen and ref != txn.trigger:
                p.append(f"{txn.name}: step {s.role} refers to {ref!r}, which is not an earlier step")
        for f, spec in s.set.items():
            if f not in codec.by_name:
                p.append(f"{txn.name}: step {s.role} sets {f!r}, which is not a field of {codec.full_name}")
            if isinstance(spec, dict) and not (set(spec) & set(VALUE_KINDS)):
                p.append(f"{txn.name}: step {s.role}.{f}: one of {VALUE_KINDS}")
        for f in s.when:
            if f not in pay.by_name:
                p.append(f"{txn.name}: step {s.role}: `if` names {f!r}, which is not a field of the payment")
        seen.add(s.role)
    for f in (g.get("expands") or {}):
        if f not in pay.by_name:
            p.append(f"{txn.name}: generate.expands names {f!r}, which is not a field of the payment")
    return p


def _pick(rng: np.random.Generator, dist: dict[str, float]) -> str:
    names = list(dist)
    w = np.asarray([dist[n] for n in names], np.float64)
    return names[int(rng.choice(len(names), p=w / w.sum()))]


def _native(v: Any) -> Any:
    return v.item() if hasattr(v, "item") else v


class FlowExpander:
    """One transaction contract's flows: `flow(cols, i, key)` gives the messages of payment `i`."""

    def __init__(self, txn: Transaction, rng: np.random.Generator, fraud_names: dict[int, str], gen_id: int,
                 score_stages: bool = False) -> None:
        g = txn.generate or {}
        if not g:
            raise ValueError(f"transaction {txn.name} has no `generate:` section")
        problems = validate_generate(txn)
        if problems:
            raise ValueError("; ".join(problems))
        self.txn = txn
        self.rng = rng
        self.fraud_names = fraud_names
        self.gen_id = gen_id
        self.steps = parse_steps(txn)
        self.delays = {k: tuple(v) for k, v in (g.get("delays_ms") or {}).items()}
        self.rates = dict(g.get("rates") or {})
        self.fraud = {k: dict(v or {}) for k, v in (g.get("fraud") or {}).items()}
        self.codecs = {role: event_type(t).codec for role, t in txn.roles.items()}
        self.classes = {role: message_class(event_type(t).message) for role, t in txn.roles.items()}
        self.pay = event_type(txn.into).codec
        from ..bindings.pipeline import pipeline
        part_entity = pipeline().partition_key
        self.part_field = {r: event_type(t).entities.get(part_entity) for r, t in txn.roles.items()}
        self.pay_part_field = event_type(txn.into).entities.get(part_entity)
        self.id_field = {r: event_type(t).id for r, t in txn.roles.items()}
        self.time_field = {r: event_type(t).time for r, t in txn.roles.items()}
        self.scored_roles = {s.role for s in txn.stages if s.scored} if score_stages else set()
        self.score_stages = score_stages
        self.inverse = self._inverse()
        self.expands = self._conditions(g.get("expands") or {})
        self.when = [self._conditions(s.when) for s in self.steps]
        self.seq = 0

    # ---------------------------------------------------------------- set-up
    def _inverse(self) -> dict[str, list[tuple[str, str, dict[int, int] | None]]]:
        """role -> [(its field, payment field, payment enum number -> its enum number)]: the assembly's
        mappings read backwards (the first source of a mapped name wins)."""
        out: dict[str, list] = {r: [] for r in self.txn.roles}
        seen: set[tuple[str, str]] = set()
        maps = list(self.txn.fields) + [f for s in self.txn.stages for f in s.fields]
        for fm in maps:
            if fm.role is None or (fm.role, fm.field) in seen:
                continue
            if fm.field in (self.id_field[fm.role], self.time_field[fm.role], self.txn.link):
                continue                    # set for every message
            seen.add((fm.role, fm.field))
            enum = None
            if fm.enum_map:
                src, dst = self.codecs[fm.role].by_name[fm.field], self.pay.by_name[fm.target]
                src_num = {nm: n for n, nm in src.enum_names}
                dst_num = {nm: n for n, nm in dst.enum_names}
                enum = {}
                for s_name, d_name in fm.enum_map:      # the contract's order: the first source of a name wins
                    d = dst_num.get(d_name)
                    if d is not None and d not in enum and s_name in src_num:
                        enum[d] = src_num[s_name]
            out[fm.role].append((fm.field, fm.target, enum))
        # a field named like a payment field, of the same type, that no mapping fills: from the payment too
        # (a capture's card, merchant and amount)
        for role in self.txn.roles:
            fixed = {self.id_field[role], self.time_field[role], self.txn.link, self.part_field[role]}
            for f in self.codecs[role].fields:
                pf = self.pay.by_name.get(f.name)
                if f.name in fixed or (role, f.name) in seen or pf is None or pf.kind != f.kind \
                        or (f.kind == "enum" and pf.enum_names != f.enum_names):
                    continue
                out[role].append((f.name, f.name, None))
        return out

    def _conditions(self, raw: dict[str, Any]) -> list[tuple[str, set[int], bool]]:
        """payment field -> allowed (or, with `not`, excluded) values, as numbers."""
        out = []
        for f, spec in raw.items():
            neg = isinstance(spec, dict) and "not" in spec
            vals = spec["not"] if neg else spec
            vals = vals if isinstance(vals, list) else [vals]
            fd = self.pay.by_name[f]
            nums = set()
            for v in vals:
                if fd.kind == "enum" and isinstance(v, str):
                    n = next((n for n, nm in fd.enum_names if nm == v), None)
                    if n is None:
                        raise ValueError(f"{self.txn.name}: {f} has no value {v!r}")
                    nums.add(n)
                elif fd.kind == "bool":
                    nums.add(int(bool(v)))
                else:
                    nums.add(int(v))
            out.append((f, nums, neg))
        return out

    def mask(self, cols: dict[str, np.ndarray]) -> np.ndarray:
        """Which of a batch's payments this contract expands."""
        n = len(cols["event_id"])
        m = np.ones(n, bool)
        for f, nums, neg in self.expands:
            hit = np.isin(np.asarray(cols[f]).astype(np.int64), list(nums))
            m &= ~hit if neg else hit
        return m

    def _matches(self, conds, cols: dict[str, np.ndarray], i: int) -> bool:
        for f, nums, neg in conds:
            v = int(_native(cols[f][i]))
            if (v in nums) == neg:
                return False
        return True

    # ---------------------------------------------------------------- values
    def _rate(self, ft: int, ref: Any) -> Any:
        if not isinstance(ref, str):
            return ref
        over = self.fraud.get(self.fraud_names.get(ft, ""), {})
        if ref in over:
            return over[ref]
        if ref not in self.rates:
            raise KeyError(f"{self.txn.name}: no rate {ref!r} in generate.rates")
        return self.rates[ref]

    def _delay_us(self, ref: Any) -> int:
        if ref is None:
            return 0
        lo, hi = self.delays[ref] if isinstance(ref, str) else tuple(ref)
        return int(self.rng.uniform(lo, hi) * 1000)

    def _enum(self, role: str, fname: str, name: str) -> int:
        for n, nm in self.codecs[role].by_name[fname].enum_names:
            if nm == name:
                return n
        raise ValueError(f"{role}.{fname}: no enum value {name}")

    def _typed(self, role: str, fname: str, v: Any) -> Any:
        f = self.codecs[role].by_name[fname]
        if f.kind == "enum":
            return self._enum(role, fname, v) if isinstance(v, str) else int(v)
        if f.kind == "bool":
            return bool(v)
        if f.kind == "float":
            return float(v)
        if f.kind == "string":
            return str(v)
        return int(v)

    def _value(self, role: str, fname: str, spec: Any, ft: int, cols, i: int, current: Any) -> Any:
        rng = self.rng
        if not isinstance(spec, dict):
            return self._typed(role, fname, spec)
        if "const" in spec:
            return self._typed(role, fname, spec["const"])
        if "p" in spec:
            return bool(rng.random() < float(self._rate(ft, spec["p"])))
        if "pick" in spec:
            return self._typed(role, fname, _pick(rng, self._rate(ft, spec["pick"])))
        if "uniform" in spec:
            lo, hi = self._rate(ft, spec["uniform"])
            return self._typed(role, fname, rng.uniform(lo, hi))
        if "alter" in spec:
            if "col" in spec:
                current = _native(cols[spec["col"]][i])
            if rng.random() < float(self._rate(ft, spec["alter"])):
                kind = self.codecs[role].by_name[fname].kind
                if kind == "float":
                    return float(current) * float(rng.uniform(1.1, 3.0))
                if fname.startswith("amount"):
                    return int(int(current) * float(rng.uniform(1.1, 3.0)))
                return int(current) + 1 + int(rng.integers(0, 1000))
            return current
        if "col" in spec:
            return self._typed(role, fname, _native(cols[spec["col"]][i]))
        raise ValueError(f"{self.txn.name}: {role}.{fname}: one of {VALUE_KINDS}")

    def _part_id(self) -> int:
        """Event id of a part: the generator's id space with bit 47 set (unified events use seq < 2^47)."""
        self.seq += 1
        return (self.gen_id << 48) | (1 << 47) | self.seq

    # ---------------------------------------------------------------- one flow
    def flow(self, c: dict[str, np.ndarray], i: int, key: bytes) -> list[Record]:
        rng = self.rng
        eid = int(c["event_id"][i])
        t0 = int(c["event_time_us"][i])
        # the typology that shapes the flow: the payment's own, or a label-only one (friendly fraud: a
        # genuine purchase the cardholder later disputes)
        ft = int(c["fraud_type"][i]) or (int(c["label_type"][i]) if "label_type" in c else 0)
        txn = self.txn
        times: dict[str, int] = {txn.trigger: t0}
        sent: set[str] = set()
        recs: list[Record] = []
        for si, s in enumerate(self.steps):
            if any(n not in sent for n in s.needs) or any(u in sent for u in s.unless) \
                    or not self._matches(self.when[si], c, i):
                times.setdefault(s.role, times.get(s.after, t0) if s.after else t0)
                continue
            is_trigger = s.role == txn.trigger
            t = t0 if is_trigger else (times.get(s.after, t0) if s.after else t0) + self._delay_us(s.delay)
            times[s.role] = t
            if s.p is not None and not is_trigger and not (rng.random() < float(self._rate(ft, s.p))):
                continue
            fields: dict[str, Any] = {txn.link: eid}
            pf = self.part_field[s.role]
            if pf is not None and self.pay_part_field is not None:
                fields[pf] = int(_native(c[self.pay_part_field][i]))
            for fname, target, enum in self.inverse[s.role]:
                v = _native(c[target][i])
                if enum is not None:
                    v = enum.get(int(v), 0)
                fields[fname] = self._typed(s.role, fname, v)
            for fname, spec in s.set.items():
                fields[fname] = self._value(s.role, fname, spec, ft, c, i, fields.get(fname))
            attempts = 1
            if s.repeat:
                r = s.repeat
                while attempts < int(r.get("max", 2)) and rng.random() < float(self._rate(ft, r["p"])):
                    attempts += 1
            for a in range(1, attempts + 1):
                fa = dict(fields)
                if s.repeat:
                    r = s.repeat
                    if r.get("counter"):
                        fa[r["counter"]] = a
                    for fname, v in ((r.get("last") if a == attempts else r.get("earlier")) or {}).items():
                        fa[fname] = self._typed(s.role, fname, v)
                scored = is_trigger or s.role in self.scored_roles
                pid = eid if is_trigger else self._part_id()
                fa[self.id_field[s.role]] = pid
                fa[self.time_field[s.role]] = t
                msg = self.classes[s.role](**fa)
                recs.append(Record(t, txn.roles[s.role], msg.SerializeToString(), key, pid if scored else 0, scored))
                if a < attempts:
                    lo, hi = s.repeat.get("gap_ms", (0, 0))
                    t += int(rng.uniform(lo, hi) * 1000)
            times[s.role] = t
            sent.add(s.role)
            if s.stop:
                v = fields.get(s.stop["field"])
                name = dict(self.codecs[s.role].by_name[s.stop["field"]].enum_names).get(v, v) \
                    if self.codecs[s.role].by_name[s.stop["field"]].kind == "enum" else v
                ps = self._rate(ft, s.stop["p"]) or {}
                pv = ps.get(name, 0.0) if isinstance(ps, dict) else float(ps)
                if pv and rng.random() < float(pv):
                    break
        return recs


class Flows:
    """Every flow of a run: each payment becomes the flow of the first contract that expands it (or stays a
    one-message payment). One random stream for all of them, consumed in event order (deterministic)."""

    def __init__(self, txns: list[Transaction], rng: np.random.Generator, fraud_names: dict[int, str], gen_id: int,
                 score_stages: bool | None = None) -> None:
        self.expanders = [FlowExpander(t, rng, fraud_names, gen_id,
                                       score_stages=any(s.scored for s in t.stages) if score_stages is None else score_stages)
                          for t in txns]
        self.seq = 0
        for e in self.expanders:            # one part-id sequence for every flow (ids stay unique)
            e._part_id = self._part_id
        self.pending: list[Record] = []
        self.parts_sent = 0

    def _part_id(self) -> int:
        self.seq += 1
        return (self.expanders[0].gen_id << 48) | (1 << 47) | self.seq

    def expand(self, cols: dict[str, np.ndarray], payloads: list[bytes], keys: list[bytes], t1: int) -> list[Record]:
        """All records of a batch in event-time order: its unified events, with each expanded payment
        replaced by its messages that fall before `t1` (later ones wait for the batch they fall in)."""
        n = len(payloads)
        which = np.full(n, -1, np.int64)
        for k, e in enumerate(self.expanders):
            m = e.mask(cols) & (which < 0)
            which[m] = k
        out: list[Record] = []
        for i in range(n):
            k = int(which[i])
            if k < 0:
                out.append(Record(int(cols["event_time_us"][i]), "", payloads[i], keys[i], int(cols["event_id"][i]), True))
            else:
                out += self.expanders[k].flow(cols, i, keys[i])
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
        """Parts still waiting at the end of a stream (the golden sets send them; a run does not)."""
        out, self.pending = sorted(self.pending, key=lambda r: r.t_us), []
        return out
