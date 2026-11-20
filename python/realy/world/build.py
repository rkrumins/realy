"""Deterministic synthetic world: customers, accounts, cards, devices, merchants (+ATMs), payees.

ID ranges (uint64, disjoint per entity type, all < 2**63 so they are also valid int64):

    customer   row + 1                                    (1 .. N)
    account    1<<40 + customer_row*4 + k                 (implicit, no table)
    card       2<<40 + card_row
    device     3<<40 + device_row
      new legit devices (phone upgrades)   3<<40 + 1<<39 + shard<<28 + counter
    merchant   4<<40 + merchant_row        (ATMs are merchants with mcc 6011)
    terminal   5<<40 + terminal_index      (merchant.terminal_offset + k)
    payee      6<<40 + payee_row
    fraudster devices                     7<<40 + shard<<28 + counter

Every table is a dict of numpy column arrays. `world.json` carries a content hash computed over
the arrays themselves (independent of the Parquet encoder) so determinism can be asserted.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ..scenario import WorldSpec
from . import mcc as M
from .geo import geo

CUSTOMER_BASE = 0
ACCOUNT_BASE = 1 << 40
CARD_BASE = 2 << 40
DEVICE_BASE = 3 << 40
NEW_DEVICE_BASE = DEVICE_BASE + (1 << 39)
MERCHANT_BASE = 4 << 40
TERMINAL_BASE = 5 << 40
PAYEE_BASE = 6 << 40
FRAUD_DEVICE_BASE = 7 << 40

SEGMENTS = ("student", "mass", "affluent", "sme")
SEGMENT_P = np.array([0.15, 0.55, 0.20, 0.10])
CARD_DEBIT, CARD_CREDIT, CARD_PREPAID, CARD_VIRTUAL = 1, 2, 3, 4  # realy.payments.v1.CardType
N_HABITS = 6
N_PAYEES = 3
CUSTOMER_FILE_ROWS = 1_000_000
WORLD_FORMAT = 1


@dataclass
class World:
    dir: Path | None
    seed: int
    spec: dict
    customers: dict[str, np.ndarray]
    cards: dict[str, np.ndarray]
    devices: dict[str, np.ndarray]
    merchants: dict[str, np.ndarray]
    payees: dict[str, np.ndarray]
    content_hash: str = ""
    meta: dict = field(default_factory=dict)

    # ------------------------------------------------------------------ sizes / lookups
    @property
    def n_customers(self) -> int:
        return int(self.customers["customer_id"].shape[0])

    @property
    def n_merchants(self) -> int:
        return int(self.merchants["merchant_id"].shape[0])

    def customer_row(self, ids: np.ndarray | int) -> np.ndarray:
        return np.asarray(ids, dtype=np.int64) - 1

    def card_row(self, ids):
        return np.asarray(ids, dtype=np.int64) - CARD_BASE

    def merchant_row(self, ids):
        return np.asarray(ids, dtype=np.int64) - MERCHANT_BASE

    def payee_row(self, ids):
        return np.asarray(ids, dtype=np.int64) - PAYEE_BASE

    def account_id(self, customer_row: np.ndarray, k: np.ndarray | int = 0) -> np.ndarray:
        return (ACCOUNT_BASE + np.asarray(customer_row, dtype=np.int64) * 4 + k).astype(np.uint64)

    # ------------------------------------------------------------------ profile export
    def customer_profile_arrays(self) -> dict[str, np.ndarray]:
        c = self.customers
        return {
            "customer_id": c["customer_id"],
            "tenure_days": c["tenure_days"].astype(np.float32),
            "segment": c["segment"].astype(np.float32),
            "avg_ticket_usd": c["avg_ticket_usd"].astype(np.float32),
            "home_lat": c["home_lat"].astype(np.float32),
            "home_lon": c["home_lon"].astype(np.float32),
            "home_country_idx": c["home_country"].astype(np.float32),
            "tz_offset_min": c["tz_offset_min"].astype(np.float32),
        }

    def merchant_profile_arrays(self) -> dict[str, np.ndarray]:
        m = self.merchants
        return {"merchant_id": m["merchant_id"], "risk_tier": m["risk_tier"].astype(np.float32),
                "fraud_rate_30d": m["fraud_rate_30d"].astype(np.float32)}

    # ------------------------------------------------------------------ persistence
    def save(self, out_dir: Path) -> None:
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        n = self.n_customers
        for i, lo in enumerate(range(0, max(n, 1), CUSTOMER_FILE_ROWS)):
            hi = min(n, lo + CUSTOMER_FILE_ROWS)
            _write(out_dir / f"customers-{i:05d}.parquet", {k: v[lo:hi] for k, v in self.customers.items()})
        _write(out_dir / "cards.parquet", self.cards)
        _write(out_dir / "devices.parquet", self.devices)
        _write(out_dir / "merchants.parquet", self.merchants)
        _write(out_dir / "payees.parquet", self.payees)
        manifest = {"format": WORLD_FORMAT, "seed": self.seed, "spec": self.spec, "content_hash": self.content_hash,
                    "counts": {"customers": n, "cards": len(self.cards["card_id"]), "devices": len(self.devices["device_id"]),
                               "merchants": self.n_merchants, "payees": len(self.payees["payee_id"])},
                    "id_ranges": {"customer": "1..N", "account": hex(ACCOUNT_BASE), "card": hex(CARD_BASE),
                                  "device": hex(DEVICE_BASE), "merchant": hex(MERCHANT_BASE),
                                  "terminal": hex(TERMINAL_BASE), "payee": hex(PAYEE_BASE),
                                  "fraud_device": hex(FRAUD_DEVICE_BASE)},
                    **self.meta}
        (out_dir / "world.json").write_text(json.dumps(manifest, indent=2))
        self.dir = out_dir

    @classmethod
    def load(cls, d: str | Path) -> "World":
        d = Path(d)
        manifest = json.loads((d / "world.json").read_text())
        parts = sorted(d.glob("customers-*.parquet"))
        cust_tables = [_read(p) for p in parts]
        customers = {k: np.concatenate([t[k] for t in cust_tables]) for k in cust_tables[0]}
        w = cls(dir=d, seed=manifest["seed"], spec=manifest["spec"], customers=customers,
                cards=_read(d / "cards.parquet"), devices=_read(d / "devices.parquet"),
                merchants=_read(d / "merchants.parquet"), payees=_read(d / "payees.parquet"),
                content_hash=manifest["content_hash"],
                meta={k: v for k, v in manifest.items() if k in ("build_seconds",)})
        return w

    def compute_hash(self) -> str:
        h = hashlib.sha256()
        for tname in ("customers", "cards", "devices", "merchants", "payees"):
            t = getattr(self, tname)
            for col in sorted(t):
                a = np.ascontiguousarray(t[col])
                h.update(f"{tname}.{col}:{a.dtype.str}:{a.shape}".encode())
                h.update(a.tobytes())
        return h.hexdigest()


def _write(path: Path, cols: dict[str, np.ndarray]) -> None:
    pq.write_table(pa.table({k: pa.array(v) for k, v in cols.items()}), path, compression="zstd")


def _read(path: Path) -> dict[str, np.ndarray]:
    t = pq.read_table(path)
    return {name: t.column(name).to_numpy() for name in t.column_names}


def load_or_build(spec: WorldSpec, seed: int, out_dir: Path) -> World:
    out_dir = Path(out_dir)
    if (out_dir / "world.json").exists():
        m = json.loads((out_dir / "world.json").read_text())
        if m.get("seed") == seed and m.get("spec") == spec.model_dump(mode="json") and m.get("format") == WORLD_FORMAT:
            return World.load(out_dir)
    return build_world(spec, seed, out_dir)


# ====================================================================== build
def build_world(spec: WorldSpec, seed: int, out_dir: Path | None) -> World:
    t_start = time.perf_counter()
    g = geo()
    ss = np.random.SeedSequence([seed, 0x5EED_0001])
    r_merch, r_cust, r_card, r_dev, r_payee = (np.random.Generator(np.random.PCG64(s)) for s in ss.spawn(5))

    merchants = _build_merchants(spec.merchants, r_merch)
    payees = _build_payees(spec.payees, spec.mule_fraction, r_payee)
    customers = _build_customers(spec.customers, merchants, payees, r_cust)
    cards = _build_cards(customers, r_card)
    devices = _build_devices(customers, r_dev)

    w = World(dir=None, seed=seed, spec=spec.model_dump(mode="json"), customers=customers, cards=cards,
              devices=devices, merchants=merchants, payees=payees)
    w.content_hash = w.compute_hash()
    w.meta["build_seconds"] = round(time.perf_counter() - t_start, 2)
    if out_dir is not None:
        w.save(Path(out_dir))
    _ = g
    return w


def _zipf_weights(n: int, s: float, rng: np.random.Generator) -> np.ndarray:
    ranks = rng.permutation(n) + 1
    return 1.0 / ranks.astype(np.float64) ** s


def _build_merchants(n_merch: int, rng: np.random.Generator) -> dict[str, np.ndarray]:
    g = geo()
    n_merch = max(n_merch, 200)
    n_atm = max(3 * g.n_cities, n_merch // 10)
    n_online = int(n_merch * 0.25)
    n_pos = n_merch - n_online

    # --- card-present merchants: guaranteed minimum per city so travellers can shop anywhere
    min_per_city = int(max(2, min(20, n_pos // (2 * g.n_cities))))
    extra = n_pos - min_per_city * g.n_cities
    pos_city = np.concatenate([np.repeat(np.arange(g.n_cities), min_per_city),
                               rng.choice(g.n_cities, size=max(extra, 0), p=g.city_global_weight)]).astype(np.int16)
    pos_cat = rng.choice(M.N_CAT, size=pos_city.shape[0], p=M.POS_W / M.POS_W.sum()).astype(np.int16)
    pos_pop = np.empty(pos_city.shape[0])
    for c in range(g.n_cities):
        idx = np.flatnonzero(pos_city == c)
        pos_pop[idx] = _zipf_weights(idx.shape[0], 0.8, rng)

    # --- online merchants: domestic businesses (country by home weight), heavy global Zipf
    on_country = rng.choice(g.n_countries, size=n_online, p=g.home_weight).astype(np.int16)
    on_city = g.main_city[on_country].astype(np.int16)
    on_cat = rng.choice(M.N_CAT, size=n_online, p=M.REMOTE_W / M.REMOTE_W.sum()).astype(np.int16)
    on_pop = _zipf_weights(n_online, 1.1, rng)

    # --- ATMs: per city by weight (minimum 3)
    atm_city = np.concatenate([np.repeat(np.arange(g.n_cities), 3),
                               rng.choice(g.n_cities, size=n_atm - 3 * g.n_cities, p=g.city_global_weight)]).astype(np.int16)
    atm_pop = np.empty(atm_city.shape[0])
    for c in range(g.n_cities):
        idx = np.flatnonzero(atm_city == c)
        atm_pop[idx] = _zipf_weights(idx.shape[0], 0.5, rng)

    city = np.concatenate([pos_city, on_city, atm_city])
    cat = np.concatenate([pos_cat, on_cat, np.full(n_atm, -1, dtype=np.int16)])
    online = np.concatenate([np.zeros(n_pos, bool), np.ones(n_online, bool), np.zeros(n_atm, bool)])
    atm = np.concatenate([np.zeros(n_pos + n_online, bool), np.ones(n_atm, bool)])
    pop = np.concatenate([pos_pop, on_pop, atm_pop])
    n = city.shape[0]
    country = g.city_country[city].astype(np.int16)
    lat = g.city_lat[city] + rng.normal(0, 0.06, n)
    lon = g.city_lon[city] + rng.normal(0, 0.08, n)
    # A small share of card-present merchants report their HQ location instead of the store's
    # (a real source of impossible-travel false positives).
    misattr = (~online) & (~atm) & (rng.random(n) < 0.003) & (city != g.main_city[country])
    lat = np.where(misattr, g.city_lat[g.main_city[country]], lat)
    lon = np.where(misattr, g.city_lon[g.main_city[country]], lon)
    mcc = np.where(atm, M.ATM_MCC, M.MCC[np.maximum(cat, 0)]).astype(np.uint32)
    risk = np.where(atm, 1, M.RISK[np.maximum(cat, 0)] + online.astype(np.int8)).clip(0, 3).astype(np.int8)
    base_rate = np.array([0.0005, 0.0015, 0.004, 0.012])
    fraud_rate = (base_rate[risk] * rng.lognormal(0, 0.5, n)).astype(np.float32)
    n_term = np.where(online, 0, np.where(atm, 1, np.minimum(rng.geometric(0.35, n), 12))).astype(np.int16)
    term_off = np.concatenate([[0], np.cumsum(n_term[:-1].astype(np.int64))]).astype(np.int64)
    return {
        "merchant_id": (MERCHANT_BASE + np.arange(n, dtype=np.int64)).astype(np.uint64),
        "cat": cat, "mcc": mcc, "country": country, "city": city, "lat": lat, "lon": lon,
        "online": online, "atm": atm, "popularity": pop, "risk_tier": risk, "fraud_rate_30d": fraud_rate,
        "n_terminals": n_term, "terminal_offset": term_off, "geo_misattributed": misattr,
    }


def _build_payees(n: int, mule_fraction: float, rng: np.random.Generator) -> dict[str, np.ndarray]:
    g = geo()
    n = max(n, 100)
    country = rng.choice(g.n_countries, size=n, p=g.home_weight).astype(np.int16)
    mule = rng.random(n) < mule_fraction
    k = max(5, int(n * mule_fraction))
    if mule.sum() < k:
        mule[rng.choice(n, size=k, replace=False)] = True
    return {"payee_id": (PAYEE_BASE + np.arange(n, dtype=np.int64)).astype(np.uint64), "country": country, "is_mule": mule}


def _sample_weighted_within(groups: np.ndarray, group_of_item: np.ndarray, weights: np.ndarray, k: int,
                            rng: np.random.Generator) -> np.ndarray:
    """For each element of `groups` (a group id), draw k items whose group == that id, by weight."""
    out = np.full((groups.shape[0], k), -1, dtype=np.int32)
    order = np.argsort(group_of_item, kind="stable")
    sorted_groups = group_of_item[order]
    for gid in np.unique(groups):
        who = np.flatnonzero(groups == gid)
        lo, hi = np.searchsorted(sorted_groups, [gid, gid + 1])
        items = order[lo:hi]
        if items.shape[0] == 0:
            continue
        cw = np.cumsum(weights[items])
        u = rng.random((who.shape[0], k)) * cw[-1]
        out[who] = items[np.minimum(np.searchsorted(cw, u, side="right"), items.shape[0] - 1)]
    return out


def _build_customers(n: int, merchants: dict, payees: dict, rng: np.random.Generator) -> dict[str, np.ndarray]:
    g = geo()
    seg = rng.choice(4, size=n, p=SEGMENT_P).astype(np.int8)
    country = rng.choice(g.n_countries, size=n, p=g.home_weight).astype(np.int16)
    city = np.empty(n, dtype=np.int16)
    for ci in range(g.n_countries):
        who = np.flatnonzero(country == ci)
        cc = g.country_cities[ci]
        p = g.city_weight[cc] / g.city_weight[cc].sum()
        city[who] = cc[rng.choice(cc.shape[0], size=who.shape[0], p=p)]
    lat = g.city_lat[city] + rng.normal(0, 0.07, n)
    lon = g.city_lon[city] + rng.normal(0, 0.09, n)
    tz = g.city_tz[city].astype(np.int16)
    tenure = np.where(seg == 0, rng.lognormal(np.log(500), 0.8, n), rng.lognormal(np.log(1400), 0.9, n)).clip(7, 15000).astype(np.int32)
    act_med = np.array([1.6, 2.1, 2.8, 5.5])[seg]
    activity = (act_med * rng.lognormal(0, 0.55, n)).clip(0.05, 40).astype(np.float32)
    ticket = (np.array([0.55, 1.0, 1.9, 2.3])[seg] * rng.lognormal(0, 0.3, n)).astype(np.float32)
    ecom_mean = np.array([0.42, 0.30, 0.36, 0.40])[seg]
    ecom = rng.beta(ecom_mean * 6, (1 - ecom_mean) * 6).astype(np.float32)
    us = g.countries.index("US") if "US" in g.countries else -1
    cl_mean = np.where(country == us, 0.35, 0.75)
    contactless = rng.beta(cl_mean * 8, (1 - cl_mean) * 8).astype(np.float32)
    atm = (np.array([0.06, 0.05, 0.02, 0.02])[seg] * rng.uniform(0.5, 1.5, n)).astype(np.float32)
    transfer = (np.array([0.04, 0.05, 0.06, 0.15])[seg] * rng.uniform(0.5, 1.5, n)).astype(np.float32)
    trips = (np.array([1.0, 1.5, 4.0, 3.0])[seg] * rng.lognormal(0, 0.5, n)).astype(np.float32)
    card_p = np.array([[0.8, 0.2, 0.0, 0.0], [0.55, 0.35, 0.10, 0.0], [0.2, 0.45, 0.25, 0.10], [0.3, 0.4, 0.2, 0.1]])
    u = rng.random(n)
    n_cards = (1 + (u[:, None] > np.cumsum(card_p[seg], axis=1)).sum(axis=1)).clip(1, 4).astype(np.int8)
    n_dev = rng.choice([1, 2, 3], size=n, p=[0.5, 0.4, 0.1]).astype(np.int8)
    card_off = np.concatenate([[0], np.cumsum(n_cards[:-1].astype(np.int64))])
    dev_off = np.concatenate([[0], np.cumsum(n_dev[:-1].astype(np.int64))])
    home_ip = ((g.ip_first_octet[country].astype(np.uint64) << np.uint64(24))
               | rng.integers(1, 1 << 24, n).astype(np.uint64)).astype(np.uint32)
    asn = np.empty(n, dtype=np.uint32)
    for ci in range(g.n_countries):
        who = np.flatnonzero(country == ci)
        asn[who] = g.country_asns[ci][rng.integers(0, g.country_asns[ci].shape[0], who.shape[0])]

    # habitual merchants: card-present merchants in the home city, by popularity
    pos_mask = (~merchants["online"]) & (~merchants["atm"])
    pos_rows = np.flatnonzero(pos_mask)
    hab_local = _sample_weighted_within(city.astype(np.int64), merchants["city"][pos_rows].astype(np.int64),
                                        merchants["popularity"][pos_rows], N_HABITS, rng)
    habits = np.where(hab_local >= 0, pos_rows[np.maximum(hab_local, 0)], -1).astype(np.int32)
    # regular payees (domestic, never mules)
    legit_payees = np.flatnonzero(~payees["is_mule"])
    pay = legit_payees[rng.integers(0, legit_payees.shape[0], (n, N_PAYEES))].astype(np.int32)

    cols = {
        "customer_id": np.arange(1, n + 1, dtype=np.uint64), "segment": seg, "home_country": country,
        "home_city": city, "home_lat": lat, "home_lon": lon, "tz_offset_min": tz, "tenure_days": tenure,
        "activity_per_day": activity, "ticket_mult": ticket, "avg_ticket_usd": (ticket * 34.0).astype(np.float32),
        "ecom_prop": ecom, "contactless_prop": contactless, "atm_prop": atm, "transfer_prop": transfer,
        "trips_per_year": trips, "card_offset": card_off, "n_cards": n_cards, "device_offset": dev_off,
        "n_devices": n_dev, "home_ip": home_ip, "home_asn": asn,
    }
    for k in range(N_HABITS):
        cols[f"hab{k}"] = habits[:, k]
    for k in range(N_PAYEES):
        cols[f"payee{k}"] = pay[:, k]
    return cols


def _build_cards(c: dict, rng: np.random.Generator) -> dict[str, np.ndarray]:
    g = geo()
    n_cards = c["n_cards"].astype(np.int64)
    owner = np.repeat(np.arange(n_cards.shape[0], dtype=np.int32), n_cards)
    total = owner.shape[0]
    first = np.zeros(total, bool)
    first[c["card_offset"]] = True
    seg = c["segment"][owner]
    other = rng.choice([CARD_DEBIT, CARD_CREDIT, CARD_VIRTUAL, CARD_PREPAID], size=total, p=[0.3, 0.5, 0.15, 0.05])
    primary = np.where(rng.random(total) < 0.7, CARD_DEBIT, CARD_CREDIT)
    ctype = np.where(first, primary, other).astype(np.int8)
    lim_med = np.array([1000, 4000, 12000, 15000])[seg]
    limit = np.where(ctype == CARD_CREDIT, np.round(lim_med * rng.lognormal(0, 0.5, total), -2), 0).clip(0, 100000).astype(np.uint32)
    cvm = g.cvm_usd_minor[c["home_country"][owner]].astype(np.int64)
    return {"card_id": (CARD_BASE + np.arange(total, dtype=np.int64)).astype(np.uint64), "customer_row": owner,
            "card_type": ctype, "credit_limit_usd": limit, "cvm_usd_minor": cvm, "primary": first}


def _build_devices(c: dict, rng: np.random.Generator) -> dict[str, np.ndarray]:
    n_dev = c["n_devices"].astype(np.int64)
    owner = np.repeat(np.arange(n_dev.shape[0], dtype=np.int32), n_dev)
    return {"device_id": (DEVICE_BASE + np.arange(owner.shape[0], dtype=np.int64)).astype(np.uint64),
            "customer_row": owner, "os": rng.choice(3, size=owner.shape[0], p=[0.45, 0.45, 0.10]).astype(np.int8)}
