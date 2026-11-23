"""Messages of a pipeline's standalone event types (logins, profile changes, card lifecycle, device telemetry):
the types that are neither the default type nor a transaction part, each carrying a subset of the keyed
entities (contracts/events/<type>.yaml).

`traffic.event_types: {login: 0.6, ...}` gives messages of each type per payment generated, so the load
mix is explicit. Each message is drawn around one payment of the same batch and takes that payment's
entities through the type's binding (a login takes its customer, device and IP; device telemetry its
device and IP), at a time shortly before the payment (a login precedes the payment of its session). Any
bound type works this way; the four example messages also get realistic field values, skewed before a
fraudulent payment the way account takeover looks: failed logins from a new device, a credential change,
a card token provisioned to a new wallet, a tampered device.

One random stream of its own (the generator's other streams are unchanged), consumed in batch order, so
a run is deterministic.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import numpy as np

from .flows import Record

US_MIN = 60_000_000

# how long before its payment a message of the type is drawn (uniform in [0, lead))
LEAD_US = {"realy.identity.v1.LoginEvent": 5 * US_MIN, "realy.identity.v1.ProfileChangeEvent": 30 * US_MIN,
           "realy.cards.v1.CardLifecycleEvent": 60 * US_MIN, "realy.devices.v1.DeviceTelemetry": 2 * US_MIN}
DEFAULT_LEAD_US = 10 * US_MIN
FRAUD_WEIGHT = 10.0        # a fraudulent payment is this many times likelier to anchor a message


@dataclass
class _Type:
    name: str
    message: str
    cls: Any
    id_field: str
    time_field: str
    time_unit_us: int
    fields: dict[str, str]          # message field -> the payment column it copies (the shared entities)
    needs: list[str]                # payment columns that must be non-zero for the payment to anchor one
    rate: float
    lead_us: int
    fill: Callable[[Any, np.random.Generator, bool], None] | None


def _login(m: Any, rng: np.random.Generator, fraud: bool) -> None:
    from ..identity.v1 import identity_pb2 as pb
    m.outcome = pb.LOGIN_FAILED_PASSWORD if rng.random() < (0.55 if fraud else 0.04) else pb.LOGIN_SUCCESS
    m.method = int(rng.choice([pb.LOGIN_PASSWORD, pb.LOGIN_BIOMETRIC, pb.LOGIN_OTP], p=[0.5, 0.4, 0.1]))
    m.new_device = bool(rng.random() < (0.8 if fraud else 0.03))


def _profile(m: Any, rng: np.random.Generator, fraud: bool) -> None:
    from ..identity.v1 import identity_pb2 as pb
    if fraud:
        m.change = int(rng.choice([pb.PROFILE_PASSWORD, pb.PROFILE_EMAIL, pb.PROFILE_PHONE, pb.PROFILE_MFA_RESET]))
    else:
        m.change = int(rng.choice([pb.PROFILE_ADDRESS, pb.PROFILE_PAYEE_ADDED, pb.PROFILE_PASSWORD, pb.PROFILE_EMAIL],
                                  p=[0.4, 0.4, 0.15, 0.05]))
    m.channel = int(rng.choice([pb.PROFILE_APP, pb.PROFILE_WEB, pb.PROFILE_CALL_CENTRE], p=[0.6, 0.3, 0.1]))
    m.verified = bool(rng.random() < (0.2 if fraud else 0.9))


def _card(m: Any, rng: np.random.Generator, fraud: bool) -> None:
    from ..cards.v1 import cards_pb2 as pb
    if fraud and rng.random() < 0.7:
        m.action = pb.CARD_TOKEN_PROVISIONED
        m.wallet = int(rng.choice([pb.WALLET_APPLE, pb.WALLET_GOOGLE]))
        m.step_up_passed = bool(rng.random() < 0.6)
        return
    m.action = int(rng.choice([pb.CARD_ACTIVATED, pb.CARD_PIN_CHANGED, pb.CARD_TOKEN_PROVISIONED, pb.CARD_BLOCKED,
                               pb.CARD_UNBLOCKED], p=[0.3, 0.2, 0.3, 0.1, 0.1]))
    if m.action == pb.CARD_TOKEN_PROVISIONED:
        m.wallet = int(rng.choice([pb.WALLET_APPLE, pb.WALLET_GOOGLE, pb.WALLET_MERCHANT], p=[0.45, 0.45, 0.1]))
    m.step_up_passed = bool(rng.random() < 0.97)
    if m.action != pb.CARD_TOKEN_PROVISIONED:
        m.device_id = 0


def _telemetry(m: Any, rng: np.random.Generator, fraud: bool) -> None:
    m.risk_score = float(rng.beta(5, 2) if fraud else rng.beta(1, 9))
    m.emulator = bool(rng.random() < (0.3 if fraud else 0.005))
    m.rooted = bool(rng.random() < (0.2 if fraud else 0.01))
    m.ip_reputation_risk = float(rng.beta(4, 3) if fraud else rng.beta(1, 12))
    m.vpn = bool(rng.random() < (0.5 if fraud else 0.05))


FILL = {"realy.identity.v1.LoginEvent": _login, "realy.identity.v1.ProfileChangeEvent": _profile,
        "realy.cards.v1.CardLifecycleEvent": _card, "realy.devices.v1.DeviceTelemetry": _telemetry}


class Sessions:
    """`expand(cols, t0, t1)` gives a batch's messages of the standalone types, as flow Records (typed,
    not scored), each keyed by its topic's partition entity."""

    def __init__(self, rates: dict[str, float], rng: np.random.Generator, gen_id: int, pipe=None) -> None:
        from ..bindings import event_type
        from ..bindings.codec import message_class
        from ..bindings.pipeline import pipeline
        from ..bindings.registry import registry
        pipe = pipe or pipeline()
        reg = registry()
        pay = event_type(pipe.default_type)
        self.rng, self.gen_id, self.seq = rng, gen_id, 0
        self.key_col: dict[str, str] = {}
        self.types: list[_Type] = []
        for name, rate in rates.items():
            if float(rate) <= 0:
                continue
            if not pipe.standalone(name):
                raise ValueError(f"traffic.event_types: {name} is not a standalone event type of pipeline {pipe.name} "
                                 f"(the default type and transaction parts are generated as payments and flows)")
            t = event_type(name)
            fields = {}
            for ent, f in t.entities.items():
                if ent in pay.entities:
                    fields[f] = pay.entities[ent]
                elif reg.get(ent) is None:
                    raise ValueError(f"{name}: unknown entity {ent}")
            if t.location:
                for k in ("lat", "lon"):
                    if t.location.get(k) and pay.location and pay.location.get(k):
                        fields[t.location[k]] = pay.location[k]
            key_ent = pipe.partition_key_of(name)
            if key_ent not in t.entities or key_ent not in pay.entities:
                raise ValueError(f"{name}: carries no {key_ent} (its topic's partition key) a payment can lend it")
            self.key_col[name] = pay.entities[key_ent]
            # the payment must have every entity the message carries (a login anchors on an online payment,
            # with a device and an IP; a card's lifecycle on a card payment)
            needs = sorted({pay.entities[e] for e in t.entities if e in pay.entities})
            unit = {"us": 1, "ms": 1000, "s": 1_000_000}.get(t.time_unit or "us", 1)
            self.types.append(_Type(name, t.message, message_class(t.message), t.id, t.time, unit, fields, needs,
                                    float(rate), LEAD_US.get(t.message, DEFAULT_LEAD_US), FILL.get(t.message)))
        self.sent: dict[str, int] = {t.name: 0 for t in self.types}

    def __bool__(self) -> bool:
        return bool(self.types)

    def _id(self) -> int:
        self.seq += 1
        return (self.gen_id << 48) | (1 << 46) | self.seq

    def expand(self, cols: dict[str, np.ndarray], t0: int, t1: int) -> list[Record]:
        n = int(cols["event_time_us"].shape[0])
        out: list[Record] = []
        if n == 0:
            return out
        fraud = cols["is_fraud"].astype(bool) if "is_fraud" in cols else np.zeros(n, bool)
        times = cols["event_time_us"]
        for ty in self.types:
            ok = np.ones(n, bool)
            for c in ty.needs:
                ok &= cols[c] != 0
            idx = np.flatnonzero(ok)
            k = int(self.rng.poisson(ty.rate * n))
            if k == 0 or idx.size == 0:
                continue
            w = np.where(fraud[idx], FRAUD_WEIGHT, 1.0)
            pick = idx[self.rng.choice(idx.size, size=k, p=w / w.sum())]
            lead = (self.rng.random(k) * ty.lead_us).astype(np.int64)
            when = np.maximum(times[pick].astype(np.int64) - lead, t0)
            for j, i in enumerate(pick.tolist()):
                m = ty.cls()
                eid = self._id()
                setattr(m, ty.id_field, eid)
                setattr(m, ty.time_field, int(when[j]) // ty.time_unit_us)
                for f, c in ty.fields.items():
                    v = cols[c][i]
                    setattr(m, f, v.item() if hasattr(v, "item") else v)
                if ty.fill is not None:
                    ty.fill(m, self.rng, bool(fraud[i]))
                key = str(int(cols[self.key_col[ty.name]][i])).encode()
                # order -1: a message at the same microsecond as a payment goes first (it is drawn before it)
                out.append(Record(int(when[j]), ty.name, m.SerializeToString(), key, eid, False, -1))
            self.sent[ty.name] += len(pick)
        return out
