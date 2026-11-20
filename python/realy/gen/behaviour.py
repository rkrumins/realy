"""Legitimate customer behaviour (vectorised).

Arrivals: per batch, the expected share of each customer is activity_per_day x diurnal(local hour,
segment) x weekday(segment) x payday. Customers are drawn by inverse-CDF over a cumulative weight
array sorted by (timezone, segment) group, so one batch is a handful of vectorised numpy calls.

State kept per customer (row-indexed arrays): travel itinerary (depart / arrive / until /
return-arrive, destination city) and an optional upgraded phone. State changes take effect for the
customer's *subsequent* events (the event that triggers a trip happens at home).
"""
from __future__ import annotations

import numpy as np

from ..world import NEW_DEVICE_BASE, TERMINAL_BASE, World
from ..world import mcc as M
from ..world.build import ACCOUNT_BASE, CARD_CREDIT
from ..world.geo import geo, haversine_km
from .batch import (
    CH_ATM, CH_ECOM, CH_IN_APP, CH_ONLINE_BANKING, CH_POS, EM_3DS, EM_BANK_APP, EM_CHIP, EM_COF, EM_CONTACTLESS,
    EM_MAGSTRIPE, EM_NON3DS, EM_WALLET, LS_CHARGEBACK, PT_ATM, PT_PURCHASE, PT_TRANSFER, make_chunk,
)

US = 1_000_000
HOUR = 3600 * US
DAY = 24 * HOUR

_DIURNAL = np.array([
    # student
    [0.40, 0.30, 0.15, 0.08, 0.05, 0.05, 0.10, 0.30, 0.60, 0.90, 1.00, 1.20, 1.40, 1.30, 1.20, 1.20, 1.30, 1.50, 1.60, 1.60, 1.50, 1.30, 1.00, 0.70],
    # mass
    [0.15, 0.08, 0.05, 0.04, 0.05, 0.12, 0.35, 0.70, 1.00, 1.10, 1.20, 1.40, 1.60, 1.40, 1.30, 1.30, 1.40, 1.60, 1.70, 1.50, 1.20, 0.90, 0.60, 0.30],
    # affluent
    [0.12, 0.06, 0.04, 0.03, 0.04, 0.10, 0.30, 0.75, 1.10, 1.20, 1.25, 1.40, 1.60, 1.40, 1.30, 1.30, 1.40, 1.60, 1.70, 1.55, 1.25, 0.90, 0.55, 0.25],
    # sme (business hours)
    [0.05, 0.03, 0.02, 0.02, 0.03, 0.10, 0.30, 0.80, 1.50, 1.90, 2.00, 1.90, 1.70, 1.90, 1.90, 1.80, 1.60, 1.20, 0.80, 0.50, 0.30, 0.20, 0.10, 0.07],
])
DIURNAL = _DIURNAL / _DIURNAL.mean(axis=1, keepdims=True)
_WEEK = np.array([[0.95, 0.95, 0.97, 1.00, 1.10, 1.10, 0.93]] * 3 + [[1.10, 1.10, 1.10, 1.10, 1.05, 0.60, 0.40]])
WEEKDAY = _WEEK / _WEEK.mean(axis=1, keepdims=True)  # Monday = 0
HABIT_W = np.cumsum([0.35, 0.25, 0.15, 0.10, 0.08, 0.07])
PAYEE_W = np.cumsum([0.5, 0.3, 0.2])


def local_hour_weekday(t_us: np.ndarray | int, tz_min: np.ndarray | int):
    local_s = np.asarray(t_us) // US + np.asarray(tz_min, dtype=np.int64) * 60
    day = local_s // 86400
    return (local_s % 86400) // 3600, (day + 3) % 7, day


class _CumIndex:
    """Weighted sampling of items inside groups via one global cumulative array."""

    def __init__(self, items: np.ndarray, group: np.ndarray, weight: np.ndarray, n_groups: int) -> None:
        order = np.lexsort((items, group))
        self.items = items[order]
        g = group[order]
        self.cum = np.cumsum(weight[order].astype(np.float64))
        bounds = np.searchsorted(g, np.arange(n_groups + 1))
        self.lo, self.hi = bounds[:-1], bounds[1:]
        cum0 = np.concatenate([[0.0], self.cum])
        self.start = cum0[self.lo]
        self.total = cum0[self.hi] - self.start

    def has(self, groups: np.ndarray) -> np.ndarray:
        return self.total[groups] > 0

    def sample(self, groups: np.ndarray, u: np.ndarray) -> np.ndarray:
        """Items for each group id in `groups` (groups must have total > 0)."""
        x = self.start[groups] + u * self.total[groups]
        i = np.searchsorted(self.cum, x, side="right")
        i = np.clip(i, self.lo[groups], self.hi[groups] - 1)
        return self.items[i]


class MerchantIndex:
    """Popularity-weighted merchant lookups shared by behaviour and campaigns."""

    def __init__(self, world: World) -> None:
        g = geo()
        m = world.merchants
        rows = np.arange(world.n_merchants, dtype=np.int64)
        pop = m["popularity"]
        pos = (~m["online"]) & (~m["atm"])
        self.pos_by_city = _CumIndex(rows[pos], m["city"][pos].astype(np.int64), pop[pos], g.n_cities)
        self.atm_by_city = _CumIndex(rows[m["atm"]], m["city"][m["atm"]].astype(np.int64), pop[m["atm"]], g.n_cities)
        on = m["online"]
        self.online_by_country = _CumIndex(rows[on], m["country"][on].astype(np.int64), pop[on], g.n_countries)
        self.online_global = _CumIndex(rows[on], np.zeros(on.sum(), dtype=np.int64), pop[on], 1)
        self.m = m
        self.rows = rows
        self._cat_cache: dict[tuple, _CumIndex] = {}

    def online_in_cats(self, cat_idx: np.ndarray) -> _CumIndex:
        key = ("on", tuple(sorted(cat_idx.tolist())))
        if key not in self._cat_cache:
            sel = self.m["online"] & np.isin(self.m["cat"], cat_idx)
            if not sel.any():
                sel = self.m["online"]
            self._cat_cache[key] = _CumIndex(self.rows[sel], np.zeros(sel.sum(), dtype=np.int64), self.m["popularity"][sel], 1)
        return self._cat_cache[key]

    def pos_in_city_cats(self, cat_idx: np.ndarray) -> _CumIndex:
        key = ("pos", tuple(sorted(cat_idx.tolist())))
        if key not in self._cat_cache:
            g = geo()
            sel = (~self.m["online"]) & (~self.m["atm"]) & np.isin(self.m["cat"], cat_idx)
            self._cat_cache[key] = _CumIndex(self.rows[sel], self.m["city"][sel].astype(np.int64), self.m["popularity"][sel], g.n_cities)
        return self._cat_cache[key]

    def terminal(self, mrow: np.ndarray, u: np.ndarray) -> np.ndarray:
        nt = self.m["n_terminals"][mrow].astype(np.int64)
        k = np.minimum((u * np.maximum(nt, 1)).astype(np.int64), np.maximum(nt - 1, 0))
        return np.where(nt > 0, TERMINAL_BASE + self.m["terminal_offset"][mrow] + k, 0).astype(np.uint64)


def money(usd_minor: np.ndarray, cur_idx: np.ndarray, round_atm: np.ndarray | None = None):
    """USD cents -> (local minor amount, consistent USD cents) for currency of country `cur_idx`."""
    g = geo()
    local = usd_minor / 100.0 / g.fx_usd[cur_idx]
    if round_atm is not None and round_atm.any():
        step = 10.0 ** np.floor(np.log10(np.maximum(local, 1.0))) / 2.0
        rounded = np.maximum(step, np.round(local / step) * step)
        local = np.where(round_atm, rounded, local)
    minor = np.maximum(1, np.round(local * g.minor_mult[cur_idx])).astype(np.int64)
    usd = np.maximum(1, np.round(minor / g.minor_mult[cur_idx] * g.fx_usd[cur_idx] * 100.0)).astype(np.int64)
    return minor, usd


class LegitEngine:
    def __init__(self, world: World, shard_rows: np.ndarray, shard: int, rng: np.random.Generator,
                 merchants: MerchantIndex, friendly_per_1k: float, cb_delay_days: tuple[float, float]) -> None:
        self.w = world
        self.c = world.customers
        self.cards = world.cards
        self.dev = world.devices
        self.mi = merchants
        self.rng = rng
        self.shard = shard
        self.rows = shard_rows
        self.friendly_p = friendly_per_1k / 1000.0
        from ..usecases import registry
        ff = registry().by_name.get("friendly_fraud")      # label-only typology: id and label source from its YAML
        self.friendly_type = ff.id if ff else 10
        self.friendly_source = ff.label_source if ff else LS_CHARGEBACK
        self.cb_delay = cb_delay_days
        c = self.c
        seg = c["segment"][shard_rows].astype(np.int64)
        tz = c["tz_offset_min"][shard_rows].astype(np.int64)
        self.tz_values, tz_idx = np.unique(tz, return_inverse=True)
        self.group_tz = np.repeat(self.tz_values, 4)
        self.group_seg = np.tile(np.arange(4), self.tz_values.shape[0])
        group = tz_idx * 4 + seg
        act = c["activity_per_day"][shard_rows].astype(np.float64)
        self.arrivals = _CumIndex(shard_rows, group, act, self.group_tz.shape[0])
        self.natural_rate = act.sum() / 86400.0   # events per simulated second (diurnal mean = 1)
        n = world.n_customers
        self.depart = np.zeros(n, np.int64)
        self.arrive = np.zeros(n, np.int64)
        self.until = np.zeros(n, np.int64)
        self.ret = np.zeros(n, np.int64)
        self.dest = np.full(n, -1, np.int32)
        self.new_dev = np.zeros(n, np.uint64)
        self.last_cp_t = np.full(n, -(1 << 62), np.int64)
        self.last_cp_m = np.full(n, -1, np.int64)
        self.new_dev_counter = 0
        self.p_trip = (c["trips_per_year"] / (np.maximum(c["activity_per_day"], 0.05) * 365.0)).astype(np.float64)
        self.p_trip *= 0.5  # half of the trips are announced through card activity abroad

    # -------------------------------------------------------------- arrivals
    def sample_customers(self, n: int, t0: int, t1: int) -> tuple[np.ndarray, np.ndarray]:
        rng = self.rng
        t_mid = (t0 + t1) // 2
        hour, wd, day = local_hour_weekday(t_mid, self.group_tz)
        dom = _day_of_month(day)
        payday = np.where((dom == 1) | ((dom >= 25) & (dom <= 28)), 1.12, 1.0)
        w = self.arrivals.total * DIURNAL[self.group_seg, hour] * WEEKDAY[self.group_seg, wd] * payday
        cw = np.cumsum(w)
        grp = np.minimum(np.searchsorted(cw, rng.random(n) * cw[-1], side="right"), cw.shape[0] - 1)
        rows = self.arrivals.sample(grp, rng.random(n))
        times = np.sort(rng.integers(t0, t1, n, dtype=np.int64)) if t1 > t0 else np.full(n, t0, np.int64)
        return rows, times

    # -------------------------------------------------------------- events
    def generate(self, n: int, t0: int, t1: int, order0: int) -> dict[str, np.ndarray]:
        if n <= 0:
            return make_chunk(0)
        g = geo()
        rng = self.rng
        c = self.c
        cr, t = self.sample_customers(n, t0, t1)
        u = rng.random((n, 16))

        # ---- travel status (state before this batch) and new trips
        in_flight = ((t >= self.depart[cr]) & (t < self.arrive[cr])) | ((t >= self.until[cr]) & (t < self.ret[cr]))
        abroad = (t >= self.arrive[cr]) & (t < self.until[cr])
        at_home = ~(in_flight | abroad)
        start = at_home & (t >= self.ret[cr]) & (u[:, 0] < self.p_trip[cr])
        if start.any():
            self._start_trips(cr[start], t[start])
        home_city = c["home_city"][cr].astype(np.int64)
        city = np.where(abroad, self.dest[cr], home_city).astype(np.int64)
        country = g.city_country[city].astype(np.int64)
        home_country = c["home_country"][cr].astype(np.int64)

        # ---- payment type
        atm_p = c["atm_prop"][cr]
        tr_p = c["transfer_prop"][cr]
        ptype = np.where(u[:, 1] < atm_p, PT_ATM, np.where(u[:, 1] < atm_p + tr_p, PT_TRANSFER, PT_PURCHASE))
        ptype = np.where(in_flight, PT_PURCHASE, ptype)
        atm_city_ok = self.mi.atm_by_city.has(city)
        ptype = np.where((ptype == PT_ATM) & ~atm_city_ok, PT_PURCHASE, ptype)
        is_p, is_atm, is_tr = ptype == PT_PURCHASE, ptype == PT_ATM, ptype == PT_TRANSFER
        remote = is_p & ((u[:, 2] < c["ecom_prop"][cr]) | in_flight | ~self.mi.pos_by_city.has(city))
        pos = is_p & ~remote

        # ---- merchant
        mrow = np.full(n, -1, np.int64)
        if remote.any():
            r_idx = np.flatnonzero(remote)
            dom_ok = self.mi.online_by_country.has(home_country[r_idx]) & (u[r_idx, 3] < 0.75)
            sel = np.empty(r_idx.shape[0], np.int64)
            if dom_ok.any():
                sel[dom_ok] = self.mi.online_by_country.sample(home_country[r_idx][dom_ok], u[r_idx[dom_ok], 4])
            if (~dom_ok).any():
                sel[~dom_ok] = self.mi.online_global.sample(np.zeros((~dom_ok).sum(), np.int64), u[r_idx[~dom_ok], 4])
            mrow[r_idx] = sel
        if pos.any():
            p_idx = np.flatnonzero(pos)
            hk = np.searchsorted(HABIT_W, u[p_idx, 5] * HABIT_W[-1], side="right").clip(0, 5)
            habit = np.choose(hk, [c[f"hab{k}"][cr[p_idx]] for k in range(6)]).astype(np.int64)
            use_habit = at_home[p_idx] & (u[p_idx, 6] < 0.6) & (habit >= 0)
            local = self.mi.pos_by_city.sample(city[p_idx], u[p_idx, 4])
            mrow[p_idx] = np.where(use_habit, habit, local)
        if is_atm.any():
            a_idx = np.flatnonzero(is_atm)
            mrow[a_idx] = self.mi.atm_by_city.sample(city[a_idx], u[a_idx, 4])
        m = self.w.merchants
        cp_cand = pos | is_atm
        if cp_cand.any():
            mrow = self._same_store_for_quick_repeats(cr, t, mrow, cp_cand, is_atm)
        has_m = mrow >= 0
        mr = np.maximum(mrow, 0)
        cat = np.where(has_m & ~is_atm, m["cat"][mr], 0).astype(np.int64)
        cat = np.maximum(cat, 0)

        # ---- card / account
        nc = c["n_cards"][cr].astype(np.int64)
        k = np.where(u[:, 7] < 0.65, 0, (u[:, 8] * nc).astype(np.int64).clip(0, None) % nc)
        k = np.where(is_atm, 0, k)
        card_row = c["card_offset"][cr] + k
        ctype = self.cards["card_type"][card_row]
        credit = ctype == CARD_CREDIT
        card_id = np.where(is_tr, 0, self.cards["card_id"][card_row]).astype(np.uint64)
        account = (ACCOUNT_BASE + cr * 4 + np.where(credit & ~is_tr, 1, 0)).astype(np.uint64)

        # ---- amounts
        tm = c["ticket_mult"][cr].astype(np.float64)
        z = rng.standard_normal(n)
        usd = np.exp(np.log(M.MEDIAN_USD[cat] * tm) + M.SIGMA[cat] * z)
        usd = np.where(is_atm, np.exp(np.log(80.0 * np.sqrt(tm)) + 0.6 * z).clip(10, 1000), usd)
        usd = np.where(is_tr, np.exp(np.log(150.0 * tm) + 1.1 * z).clip(1, 50000), usd)
        usd_minor = np.round(usd.clip(0.5, 25000.0) * 100).astype(np.int64)
        cur = np.where(is_tr | ~has_m, home_country, m["country"][mr]).astype(np.int64)
        amount_minor, usd_minor = money(usd_minor, cur, round_atm=is_atm)

        # ---- channel / entry mode
        cvm = self.cards["cvm_usd_minor"][card_row]
        channel = np.where(is_atm, CH_ATM, np.where(is_tr, CH_ONLINE_BANKING,
                           np.where(remote, np.where(u[:, 9] < 0.3, CH_IN_APP, CH_ECOM), CH_POS)))
        contactless = pos & (usd_minor <= cvm) & (u[:, 10] < c["contactless_prop"][cr])
        mag_p = np.where(home_country == (g.countries.index("US") if "US" in g.countries else -1), 0.015, 0.004)
        recurring = remote & (u[:, 11] < M.RECURRING[cat])
        entry = np.where(contactless, np.where(u[:, 12] < 0.15, EM_WALLET, EM_CONTACTLESS),
                         np.where(u[:, 12] < mag_p, EM_MAGSTRIPE, EM_CHIP))
        entry = np.where(remote, np.where(recurring, EM_COF, np.where(u[:, 12] < 0.65, EM_3DS,
                                                                    np.where(u[:, 12] < 0.95, EM_NON3DS, EM_COF))), entry)
        entry = np.where(is_atm, EM_CHIP, entry)
        entry = np.where(is_tr, EM_BANK_APP, entry)
        card_present = pos | is_atm

        # ---- device / ip (remote + transfers)
        needs_dev = remote | is_tr
        nd = c["n_devices"][cr].astype(np.int64)
        dk = np.where(u[:, 13] < 0.75, 0, (u[:, 14] * nd).astype(np.int64) % nd)
        device = self.dev["device_id"][c["device_offset"][cr] + dk]
        upgrade = needs_dev & (u[:, 15] < 0.0015) & (self.new_dev[cr] == 0)
        if upgrade.any():
            up_rows = cr[upgrade]
            ids = NEW_DEVICE_BASE + (self.shard << 28) + self.new_dev_counter + np.arange(up_rows.shape[0], dtype=np.int64)
            self.new_dev_counter += up_rows.shape[0]
            self.new_dev[up_rows] = ids.astype(np.uint64)
        has_new = self.new_dev[cr] != 0
        device = np.where(has_new & (u[:, 13] < 0.8), self.new_dev[cr], device)
        device = np.where(needs_dev, device, 0).astype(np.uint64)
        mobile = u[:, 14] < 0.2
        rnd_ip = (g.ip_first_octet[country].astype(np.uint64) << np.uint64(24)) | rng.integers(1, 1 << 24, n).astype(np.uint64)
        ip = np.where(abroad | mobile, rnd_ip, c["home_ip"][cr].astype(np.uint64))
        ip_country = np.where(abroad, country, home_country)
        asn0 = np.array([a[0] for a in g.country_asns], dtype=np.uint32)
        asn = np.where(abroad | mobile, asn0[ip_country], c["home_asn"][cr])
        ip = np.where(needs_dev, ip, 0).astype(np.uint32)
        asn = np.where(needs_dev, asn, 0).astype(np.uint32)
        ip_country = np.where(needs_dev, ip_country, -1)

        # ---- payee
        pk = np.searchsorted(PAYEE_W, rng.random(n) * PAYEE_W[-1], side="right").clip(0, 2)
        reg_payee = np.choose(pk, [c[f"payee{k}"][cr] for k in range(3)]).astype(np.int64)
        new_payee = rng.integers(0, self.w.payees["payee_id"].shape[0], n)
        new_payee = np.where(self.w.payees["is_mule"][new_payee], reg_payee, new_payee)
        payee = np.where(rng.random(n) < 0.85, reg_payee, new_payee)
        payee_id = np.where(is_tr, self.w.payees["payee_id"][payee], 0).astype(np.uint64)

        # ---- location
        lat = np.where(card_present, m["lat"][mr], np.where(abroad, g.city_lat[city], c["home_lat"][cr]))
        lon = np.where(card_present, m["lon"][mr], np.where(abroad, g.city_lon[city], c["home_lon"][cr]))
        terminal = np.where(card_present, self.mi.terminal(mr, rng.random(n)), 0).astype(np.uint64)

        cvv = np.where(remote, rng.random(n) < 0.995, True)
        avs = np.where(remote, rng.random(n) < 0.97, True)

        # ---- label noise: friendly fraud chargebacks on legit remote purchases
        friendly = remote & (rng.random(n) < self.friendly_p)
        lo, hi = self.cb_delay
        delay = (rng.uniform(lo, hi, n) * DAY).astype(np.int64)

        return make_chunk(
            n, event_time_us=t, payment_type=ptype, customer_id=(cr + 1).astype(np.uint64), account_id=account,
            card_id=card_id, card_type=np.where(is_tr, 0, ctype), amount_minor=amount_minor, currency_idx=cur,
            amount_usd_minor=usd_minor,
            merchant_id=np.where(has_m, m["merchant_id"][mr], 0).astype(np.uint64),
            mcc=np.where(has_m, m["mcc"][mr], 0), merchant_country_idx=np.where(has_m, m["country"][mr], -1),
            terminal_id=terminal, channel=channel, entry_mode=entry, card_present=card_present, lat=lat, lon=lon,
            device_id=device, ip_v4=ip, ip_country_idx=ip_country, ip_asn=asn, payee_id=payee_id,
            cvv_match=cvv, avs_match=avs, recurring=recurring,
            credit_limit_usd=np.where(is_tr, 0, self.cards["credit_limit_usd"][card_row]),
            label_delay_us=np.where(friendly, delay, -1), label_is_fraud=friendly, label_type=np.where(friendly, self.friendly_type, 0),
            label_source=np.where(friendly, self.friendly_source, 0), order=order0 + np.arange(n, dtype=np.int64),
        )

    def _same_store_for_quick_repeats(self, cr, t, mrow, cp, is_atm, window_us: int = 5 * 60 * US):
        """A card-present payment within 5 min of the customer's previous one happens at the same
        place (same store / same ATM) -- people do not teleport across town in a minute."""
        idx = np.flatnonzero(cp)
        o = idx[np.lexsort((t[idx], cr[idx]))]
        s_cr, s_t, s_m = cr[o], t[o], mrow[o].copy()
        first = np.ones(o.shape[0], bool)
        first[1:] = s_cr[1:] != s_cr[:-1]
        prev_t = np.empty_like(s_t)
        prev_m = np.empty_like(s_m)
        prev_t[1:], prev_m[1:] = s_t[:-1], s_m[:-1]
        prev_t[first], prev_m[first] = self.last_cp_t[s_cr[first]], self.last_cp_m[s_cr[first]]
        atm_flag = self.w.merchants["atm"]
        ok = (prev_m >= 0) & (s_t - prev_t < window_us)
        ok &= atm_flag[np.maximum(prev_m, 0)] == is_atm[o]
        s_m = np.where(ok, prev_m, s_m)
        mrow = mrow.copy()
        mrow[o] = s_m
        last = np.ones(o.shape[0], bool)
        last[:-1] = s_cr[1:] != s_cr[:-1]
        self.last_cp_t[s_cr[last]] = s_t[last]
        self.last_cp_m[s_cr[last]] = s_m[last]
        return mrow

    def _start_trips(self, rows: np.ndarray, t: np.ndarray) -> None:
        g = geo()
        rng = self.rng
        k = rows.shape[0]
        home_city = self.c["home_city"][rows].astype(np.int64)
        home_country = self.c["home_country"][rows].astype(np.int64)
        foreign = rng.random(k) < 0.65
        dest = rng.choice(g.n_cities, size=k, p=g.city_global_weight)
        # domestic trips: another city of the home country when possible
        dom = np.array([_other_city(g, hc, h, rng) for hc, h in zip(home_country.tolist(), home_city.tolist())], dtype=np.int64) \
            if (~foreign).any() else np.zeros(k, np.int64)
        dest = np.where(foreign, dest, dom)
        same = dest == home_city
        dest = np.where(same, g.main_city[(home_country + 1) % g.n_countries], dest)
        dist = haversine_km(self.c["home_lat"][rows], self.c["home_lon"][rows], g.city_lat[dest], g.city_lon[dest])
        flight = ((dist / 800.0 + 1.5) * HOUR).astype(np.int64)
        depart = t + (rng.uniform(0.5, 6.0, k) * HOUR).astype(np.int64)
        stay = (np.exp(rng.normal(np.log(4.0), 0.6, k)).clip(1, 21) * DAY).astype(np.int64)
        self.depart[rows] = depart
        self.arrive[rows] = depart + flight
        self.until[rows] = depart + flight + stay
        self.ret[rows] = depart + 2 * flight + stay
        self.dest[rows] = dest


def _other_city(g, country: int, home_city: int, rng: np.random.Generator) -> int:
    cc = g.country_cities[country]
    others = cc[cc != home_city]
    if others.shape[0] == 0:
        return int(g.main_city[(country + 1) % g.n_countries])
    return int(others[rng.integers(0, others.shape[0])])


def _day_of_month(day_since_epoch: np.ndarray) -> np.ndarray:
    d = np.asarray(day_since_epoch, dtype="datetime64[D]")
    return (d - d.astype("datetime64[M]")).astype(np.int64) + 1
