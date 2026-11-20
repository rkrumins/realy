"""The `steps` pattern: define a fraud typology in YAML, with no code.

A campaign picks its victims, an actor (whose device and network the events come from) and walks an
ordered list of steps; each step emits a burst of events with a role. Built from the same helpers as
the built-in generators (CampaignEngine), so the events are indistinguishable in shape and every
downstream consumer (features, rules, labels, reports, the Explorer story) works unchanged.

    pattern: steps
    victims: {kind: card | customer, count: [1, 1], credit_only: false}
    actor:   {device: fraud | victim | shared_pool, ip: fraud | victim, pool: 3}
    steps:
      - role: probe                       # declared in `roles`
        count: [3, 6]                     # events per victim
        after_s: [0, 0]                   # after the previous step's last event (first step: campaign start)
        gap_s: [5, 30]                    # between this step's events
        probability: 1.0                  # chance the step happens (per victim)
        payment_type: purchase | atm | transfer
        channel: ecom | in_app | pos | atm | online_banking
        entry_mode: non3ds                # or a mix: {non3ds: 0.7, cof: 0.3}
        merchant: {categories: [HIGH_RISK_RESALE], where: online | home_city | foreign_city | same}
        amount_usd: [0.5, 1.99]           # uniform; or {ratio_to_avg: [8, 15]}; or {fraction_of_limit: [0.2, 0.4]}
        payee: mule | fresh | same        # transfers
        cvv_match: 0.7                    # P(true); avs_match likewise
        fraud: true                       # false: a legitimate step of the story (campaign + role, no label)

Parameters in a scenario's campaign `params` override `victims` (count) and any step field by
`<role>.<field>` (e.g. `probe.count: [8, 12]`).
"""
from __future__ import annotations

from typing import Any, Literal

import numpy as np
from pydantic import BaseModel, ConfigDict, Field

from ..world import mcc as M
from ..world.build import ACCOUNT_BASE
from ..world.geo import geo
from .batch import (
    CH_ATM, CH_ECOM, CH_IN_APP, CH_ONLINE_BANKING, CH_POS, EM_3DS, EM_BANK_APP, EM_CHIP, EM_COF, EM_CONTACTLESS,
    EM_MAGSTRIPE, EM_NON3DS, EM_WALLET, PT_ATM, PT_PURCHASE, PT_TRANSFER, concat_chunks, make_chunk,
)
from .behaviour import US

PAYMENT = {"purchase": PT_PURCHASE, "atm": PT_ATM, "transfer": PT_TRANSFER}
CHANNEL = {"ecom": CH_ECOM, "in_app": CH_IN_APP, "pos": CH_POS, "atm": CH_ATM, "online_banking": CH_ONLINE_BANKING}
ENTRY = {"chip": EM_CHIP, "contactless": EM_CONTACTLESS, "magstripe": EM_MAGSTRIPE, "wallet": EM_WALLET, "3ds": EM_3DS,
         "non3ds": EM_NON3DS, "cof": EM_COF, "bank_app": EM_BANK_APP}
REMOTE = {"ecom", "in_app", "online_banking"}


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Victims(_M):
    kind: Literal["card", "customer"] = "card"
    count: int | list[int] = 1
    credit_only: bool = False


class Actor(_M):
    device: Literal["fraud", "victim", "shared_pool"] = "fraud"
    ip: Literal["fraud", "victim"] = "fraud"
    pool: int = Field(3, ge=1, le=50)


class Merchant(_M):
    categories: list[str] = []
    where: Literal["online", "home_city", "foreign_city", "same"] = "online"


class Step(_M):
    role: str
    count: int | list[int] = 1
    after_s: float | list[float] = 0.0
    gap_s: float | list[float] = 30.0
    probability: float = Field(1.0, ge=0, le=1)
    payment_type: Literal["purchase", "atm", "transfer"] = "purchase"
    channel: Literal["ecom", "in_app", "pos", "atm", "online_banking"] = "ecom"
    entry_mode: str | dict[str, float] = "non3ds"
    merchant: Merchant = Merchant()
    amount_usd: list[float] | dict[str, list[float]] = [10.0, 100.0]
    payee: Literal["mule", "fresh", "same"] = "fresh"
    cvv_match: float = Field(1.0, ge=0, le=1)
    avs_match: float = Field(1.0, ge=0, le=1)
    fraud: bool = True

    def model_post_init(self, _ctx: Any) -> None:
        modes = [self.entry_mode] if isinstance(self.entry_mode, str) else list(self.entry_mode)
        bad = [m for m in modes if m not in ENTRY]
        if bad:
            raise ValueError(f"step {self.role}: unknown entry_mode {bad}; known: {sorted(ENTRY)}")
        if isinstance(self.amount_usd, dict) and set(self.amount_usd) - {"ratio_to_avg", "fraction_of_limit"}:
            raise ValueError(f"step {self.role}: amount_usd is a [lo, hi] range, or ratio_to_avg / fraction_of_limit")
        for c in self.merchant.categories:
            _categories([c])        # raises on an unknown category / group


class StepsSpec(_M):
    victims: Victims = Victims()
    actor: Actor = Actor()
    steps: list[Step]


def _categories(names: list[str]) -> list[str]:
    """Category names or group constants of world.mcc (e.g. HIGH_RISK_RESALE)."""
    out: list[str] = []
    for n in names:
        grp = getattr(M, n, None) if n.isupper() else None
        if isinstance(grp, (tuple, list)):
            out.extend(grp)
        else:
            out.append(n)
    M.cats(*out)                    # validates every name
    return out


def _rng_int(rng: np.random.Generator, v: int | list[int]) -> int:
    lo, hi = (v, v) if isinstance(v, (int, float)) else (int(v[0]), int(v[-1]))
    return int(rng.integers(int(lo), int(hi) + 1))


def _rng_u(rng: np.random.Generator, v: float | list[float], n: int | None = None):
    lo, hi = (v, v) if isinstance(v, (int, float)) else (float(v[0]), float(v[-1]))
    return rng.uniform(lo, hi, n) if n is not None else float(rng.uniform(lo, hi))


def validate(doc: dict[str, Any]) -> StepsSpec:
    spec = StepsSpec.model_validate({k: doc.get(k) for k in ("victims", "actor", "steps") if doc.get(k) is not None})
    roles = set(doc.get("roles") or [])
    for st in spec.steps:
        if st.role not in roles:
            raise ValueError(f"step role {st.role!r} is not declared in `roles` {sorted(roles)}")
    return spec


def _apply_params(spec: StepsSpec, params: dict[str, Any]) -> StepsSpec:
    if not params:
        return spec
    d = spec.model_dump()
    if "victims" in params:
        d["victims"]["count"] = params["victims"]
    for k, v in params.items():
        if "." in k:
            role, field = k.split(".", 1)
            for st in d["steps"]:
                if st["role"] == role:
                    st[field] = v
    return StepsSpec.model_validate(d)


def build_steps(eng: Any, typ: Any, T: int, params: dict[str, Any]) -> dict[str, np.ndarray]:
    """Generate one campaign of a `steps` typology starting at simulation time T (µs)."""
    spec = _apply_params(validate(typ.model_dump(by_alias=True)), params)
    rng, g, c, w = eng.rng, geo(), eng.c, eng.w
    cid = eng.next_campaign_id()
    n_v = max(1, _rng_int(rng, spec.victims.count))
    if spec.victims.kind == "card":
        cards = eng.pick_cards(n_v, credit_only=spec.victims.credit_only)
        custs = eng.cards["customer_row"][cards].astype(np.int64)
    else:
        custs = eng.pick_customers(n_v).astype(np.int64)
        cards = c["card_offset"][custs].astype(np.int64)
    home0 = int(c["home_country"][custs[0]])
    f_ip, f_ipc, f_asn = eng.fraud_ip(home0)
    f_lat, f_lon = g.city_lat[g.main_city[f_ipc]], g.city_lon[g.main_city[f_ipc]]
    pool = eng.fraud_device(spec.actor.pool if spec.actor.device == "shared_pool" else 1)
    mule = int(eng.mules[rng.integers(0, eng.mules.shape[0])]) if eng.mules.shape[0] else None
    chunks = []
    for vi in range(n_v):
        cr, card = int(custs[vi]), int(cards[vi])
        t_prev, prev_m, prev_payee = T, None, None
        for st in spec.steps:
            if rng.random() > st.probability:
                continue
            k = max(1, _rng_int(rng, st.count))
            start = t_prev + int(_rng_u(rng, st.after_s) * US)
            gaps = np.concatenate([[0.0], _rng_u(rng, st.gap_s, k - 1)]) if k > 1 else np.zeros(1)
            t = (start + np.cumsum(gaps) * US).astype(np.int64)
            remote = st.channel in REMOTE
            if isinstance(st.entry_mode, str):
                em = np.full(k, ENTRY[st.entry_mode])
            else:
                names, p = list(st.entry_mode), np.asarray(list(st.entry_mode.values()), float)
                em = np.asarray([ENTRY[n] for n in names])[rng.choice(len(names), k, p=p / p.sum())]
            ev: dict[str, Any] = dict(event_time_us=t, payment_type=PAYMENT[st.payment_type], channel=CHANNEL[st.channel],
                                      entry_mode=em, cvv_match=rng.random(k) < st.cvv_match,
                                      avs_match=rng.random(k) < st.avs_match)
            # ---- amount
            a = st.amount_usd
            if isinstance(a, dict) and "ratio_to_avg" in a:
                usd = max(5.0, float(c["avg_ticket_usd"][cr])) * _rng_u(rng, a["ratio_to_avg"], k)
            elif isinstance(a, dict) and "fraction_of_limit" in a:
                usd = max(500.0, float(eng.cards["credit_limit_usd"][card])) * _rng_u(rng, a["fraction_of_limit"], k)
            else:
                usd = _rng_u(rng, a, k)
            if st.payment_type == "transfer":
                if st.payee == "mule" and mule is not None:
                    payee = mule
                elif st.payee == "same" and prev_payee is not None:
                    payee = prev_payee
                else:
                    payee = int(rng.integers(0, w.payees["payee_id"].shape[0]))
                prev_payee = payee
                ev.update(customer_id=np.uint64(cr + 1), account_id=np.uint64(ACCOUNT_BASE + cr * 4),
                          payee_id=w.payees["payee_id"][payee])
                ev.update(eng.amounts(usd, np.full(k, c["home_country"][cr])))
            else:
                mrow = _merchants(eng, st, k, cr, prev_m)
                prev_m = mrow
                ev.update(eng.card_fields(np.full(k, card)))
                ev.update(eng.merchant_fields(mrow, not remote))
                ev.update(eng.amounts(usd, w.merchants["country"][mrow], round_atm=st.payment_type == "atm"))
            if remote:
                if spec.actor.device == "victim":
                    dev = w.devices["device_id"][c["device_offset"][cr]]
                else:
                    dev = pool[rng.integers(0, pool.shape[0], k)] if pool.shape[0] > 1 else pool[0]
                if spec.actor.ip == "victim":
                    ip, ipc, asn, lat, lon = c["home_ip"][cr], c["home_country"][cr], c["home_asn"][cr], c["home_lat"][cr], c["home_lon"][cr]
                else:
                    ip, ipc, asn, lat, lon = f_ip, f_ipc, f_asn, f_lat, f_lon
                ev.update(eng.remote_ctx(k, dev, ip, ipc, asn, lat, lon))
            ev.update(eng.label_cols(k, typ.name, cid, st.role) if st.fraud else eng.context_cols(typ.name, cid, st.role))
            chunks.append(make_chunk(k, **ev))
            t_prev = int(t[-1])
    return concat_chunks(chunks)


def _merchants(eng: Any, st: Step, k: int, cr: int, prev: np.ndarray | None) -> np.ndarray:
    rng, g, c = eng.rng, geo(), eng.c
    where = st.merchant.where
    if where == "same" and prev is not None:
        return np.full(k, prev[0])
    cats = _categories(st.merchant.categories) if st.merchant.categories else []
    if st.payment_type == "atm" or where in ("home_city", "foreign_city"):
        if where == "foreign_city":
            home = int(c["home_country"][cr])
            wgt = g.city_global_weight * (g.city_country != home)
            city = int(rng.choice(g.n_cities, p=wgt / wgt.sum())) if wgt.sum() > 0 else int(c["home_city"][cr])
        else:
            city = int(c["home_city"][cr])
        grp = np.full(k, city, np.int64)
        if st.payment_type == "atm":
            return eng.mi.atm_by_city.sample(grp, rng.random(k))
        if cats:
            idx = eng.mi.pos_in_city_cats(M.cats(*cats))
            if idx.has(grp[:1])[0]:
                return idx.sample(grp, rng.random(k))
        return eng.mi.pos_by_city.sample(grp, rng.random(k))
    idx = eng.mi.online_in_cats(M.cats(*(cats or list(M.HIGH_RISK_RESALE))))
    return idx.sample(np.zeros(k, np.int64), rng.random(k))
