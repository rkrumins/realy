"""Fraud campaign scripts (ground-truth labelled).

A campaign is scripted completely when it starts: `build(kind, start_us, params)` returns a chunk of
future events (absolute simulation times). The generator merges due events into each batch.
Every script is shaped so its signature trips the intended rule of rules/baseline.yaml under the
windows of contracts/features/feature_schema.yaml (see the table in docs / the plan).

Parameters (all optional, defaults in brackets). For scenario-timeline campaigns `cards`/`victims`
are totals across all generator shards; `Generator.schedule_campaign(split=True)` divides them.

  card_testing      cards [1], tests [4..12], cashout [true], spread_min [5 for waves]
  account_takeover  purchases [1..4], transfer_prob [0.5]
  lost_stolen       extra_txns [0..2]
  counterfeit       pos [2..6], atm [0..3]
  merchant_breach   victims [15], delay_days [0.2..2.0]
  atm_cashout       cards [200], atms [20], duration_min [15], withdrawals [3..6], foreign_prob [0.5]
                    (probability the cash-out city is abroad for the cards: cross-border unlimited operation)
  app_scam_mule     victims [8], window_h [4]
  bust_out          purchases [3..6], span_hours [20], buildup_hours [0], buildup_purchases [6..12]
                    (build-up: ordinary on-profile purchases that ramp up before the bust; they are
                    LEGITIMATE events - only the bust is first-party fraud, as charge-offs record it)
  bot_ecom          cards [10], attempts/card [1..2], duration_min [5..30]
"""
from __future__ import annotations

import numpy as np

from ..world import FRAUD_DEVICE_BASE, World
from ..world import mcc as M
from ..world.build import ACCOUNT_BASE, CARD_CREDIT
from ..world.geo import HOSTING_ASNS, geo, haversine_km
from .batch import (
    CH_ATM, CH_ECOM, CH_IN_APP, CH_ONLINE_BANKING, CH_POS, EM_BANK_APP, EM_CHIP, EM_COF, EM_CONTACTLESS,
    EM_MAGSTRIPE, EM_NON3DS, LS_CHARGEBACK, LS_CUSTOMER_REPORT, LS_INVESTIGATION, PT_ATM, PT_PURCHASE, PT_TRANSFER,
    concat_chunks, make_chunk,
)
from .behaviour import DAY, HOUR, US, MerchantIndex, money

MIN = 60 * US
FRAUD_TYPE = {"card_testing": 1, "account_takeover": 2, "lost_stolen": 3, "counterfeit": 4, "merchant_breach": 5,
              "atm_cashout": 6, "app_scam_mule": 7, "bust_out": 8, "bot_ecom": 9}


def _rng_range(rng: np.random.Generator, v, default: tuple[int, int]) -> int:
    lo, hi = (v if isinstance(v, (list, tuple)) else (v, v)) if v is not None else default
    return int(rng.integers(int(lo), int(hi) + 1))


class CampaignEngine:
    def __init__(self, world: World, shard_rows: np.ndarray, shard: int, n_shards: int, rng: np.random.Generator,
                 merchants: MerchantIndex, unreported: float, cb_delay_days: tuple[float, float]) -> None:
        self.w, self.rows, self.shard, self.n_shards, self.rng, self.mi = world, shard_rows, shard, n_shards, rng, merchants
        self.unreported = unreported
        self.cb_delay = cb_delay_days
        self.counter = 0
        self.dev_counter = 0
        c = world.customers
        cards = world.cards
        shard_mask = np.zeros(world.n_customers, bool)
        shard_mask[shard_rows] = True
        self.shard_cards = np.flatnonzero(shard_mask[cards["customer_row"]])
        self.shard_credit = self.shard_cards[cards["card_type"][self.shard_cards] == CARD_CREDIT]
        self.mules = np.flatnonzero(world.payees["is_mule"])
        self.c, self.cards = c, cards
        g = geo()
        self.fraud_countries = g.fraud_source

    # ------------------------------------------------------------------ helpers
    def next_campaign_id(self) -> int:
        self.counter += 1
        return (self.shard << 40) | self.counter

    def fraud_device(self, k: int = 1) -> np.ndarray:
        ids = FRAUD_DEVICE_BASE + (self.shard << 28) + self.dev_counter + np.arange(k, dtype=np.int64)
        self.dev_counter += k
        return ids.astype(np.uint64)

    def fraud_ip(self, avoid_country: int) -> tuple[int, int, int]:
        g = geo()
        rng = self.rng
        choices = self.fraud_countries[self.fraud_countries != avoid_country]
        country = int(choices[rng.integers(0, choices.shape[0])])
        ip = (int(g.ip_first_octet[country]) << 24) | int(rng.integers(1, 1 << 24))
        asn = int(HOSTING_ASNS[rng.integers(0, HOSTING_ASNS.shape[0])]) if rng.random() < 0.6 else int(g.country_asns[country][0])
        return ip, country, asn

    def pick_cards(self, k: int, credit_only: bool = False) -> np.ndarray:
        pool = self.shard_credit if credit_only and self.shard_credit.shape[0] else self.shard_cards
        k = min(k, pool.shape[0])
        return pool[self.rng.choice(pool.shape[0], size=k, replace=False)]

    def pick_customers(self, k: int) -> np.ndarray:
        k = min(k, self.rows.shape[0])
        return self.rows[self.rng.choice(self.rows.shape[0], size=k, replace=False)]

    def label_cols(self, n: int, kind: str, campaign_id: int, source: int, delay_scale: float = 1.0) -> dict:
        rng = self.rng
        lo, hi = self.cb_delay
        reported = rng.random() >= self.unreported
        base = rng.uniform(lo, hi) * DAY * delay_scale
        delay = (base * rng.uniform(0.9, 1.1, n)).astype(np.int64)
        ft = FRAUD_TYPE[kind]
        return dict(is_fraud=True, fraud_type=ft, campaign_id=np.uint64(campaign_id),
                    label_delay_us=np.where(reported, delay, -1), label_is_fraud=True, label_type=ft, label_source=source)

    def card_fields(self, card_rows: np.ndarray) -> dict:
        cards = self.cards
        cr = cards["customer_row"][card_rows].astype(np.int64)
        ctype = cards["card_type"][card_rows]
        return dict(customer_id=(cr + 1).astype(np.uint64), card_id=cards["card_id"][card_rows], card_type=ctype,
                    account_id=(ACCOUNT_BASE + cr * 4 + (ctype == CARD_CREDIT)).astype(np.uint64),
                    credit_limit_usd=cards["credit_limit_usd"][card_rows])

    def merchant_fields(self, mrow: np.ndarray, card_present: bool | np.ndarray) -> dict:
        m = self.w.merchants
        cp = np.broadcast_to(np.asarray(card_present), mrow.shape)
        term = np.where(cp, self.mi.terminal(mrow, self.rng.random(mrow.shape[0])), 0)
        return dict(merchant_id=m["merchant_id"][mrow], mcc=m["mcc"][mrow], merchant_country_idx=m["country"][mrow],
                    terminal_id=term, lat=m["lat"][mrow], lon=m["lon"][mrow], card_present=cp)

    def amounts(self, usd: np.ndarray, cur_idx: np.ndarray, round_atm: bool = False) -> dict:
        minor, usd_minor = money(np.round(np.asarray(usd) * 100).astype(np.int64), np.asarray(cur_idx, np.int64),
                                 round_atm=np.full(np.shape(usd), round_atm))
        return dict(amount_minor=minor, amount_usd_minor=usd_minor, currency_idx=cur_idx)

    def remote_ctx(self, n: int, device, ip, ip_country, asn, lat, lon) -> dict:
        return dict(device_id=device, ip_v4=ip, ip_country_idx=ip_country, ip_asn=asn, lat=lat, lon=lon)

    # ------------------------------------------------------------------ dispatcher
    def build(self, kind: str, start: int, params: dict) -> dict[str, np.ndarray]:
        fn = getattr(self, f"_{kind}")
        return fn(start, params or {})

    # ------------------------------------------------------------------ typologies
    def _card_testing(self, T: int, p: dict) -> dict:
        rng = self.rng
        g = geo()
        n_cards = max(1, int(p.get("cards", 1)))
        cid = self.next_campaign_id()
        cards = self.pick_cards(n_cards)
        test_m = self.mi.online_in_cats(M.cats(*M.CARD_TESTING_TARGETS))
        n_test_m = 1 if n_cards > 1 else int(rng.integers(1, 3))
        test_merchants = test_m.sample(np.zeros(n_test_m, np.int64), rng.random(n_test_m))
        home = self.c["home_country"][self.cards["customer_row"][cards[0]]]
        ip, ipc, asn = self.fraud_ip(int(home))
        dev = self.fraud_device(1)[0]
        spread = float(p.get("spread_min", 5 if n_cards > 1 else 0)) * MIN
        chunks = []
        cash_m = self.mi.online_in_cats(M.cats(*M.HIGH_RISK_RESALE))
        for j, card in enumerate(cards.tolist()):
            k = _rng_range(rng, p.get("tests"), (4, 12) if n_cards == 1 else (3, 6))
            t = T + int(rng.uniform(0, spread)) + np.cumsum((rng.exponential(25.0, k) + 3.0) * US).astype(np.int64)
            mrow = test_merchants[rng.integers(0, test_merchants.shape[0], k)]
            usd = rng.uniform(0.5, 1.99, k)
            ev = dict(event_time_us=t, payment_type=PT_PURCHASE, channel=CH_ECOM, entry_mode=EM_NON3DS,
                      cvv_match=rng.random(k) > 0.3, avs_match=rng.random(k) > 0.4)
            ev.update(self.card_fields(np.full(k, card)))
            ev.update(self.merchant_fields(mrow, False))
            ev.update(self.amounts(usd, self.w.merchants["country"][mrow]))
            ev.update(self.remote_ctx(k, dev, ip, ipc, asn, g.city_lat[g.main_city[ipc]], g.city_lon[g.main_city[ipc]]))
            ev.update(self.label_cols(k, "card_testing", cid, LS_CHARGEBACK))
            chunks.append(make_chunk(k, **ev))
            if p.get("cashout", True) and (n_cards == 1 or rng.random() < 0.2):
                kc = int(rng.integers(1, 4))
                tc = t[-1] + int(rng.uniform(5, 30) * MIN) + np.cumsum(rng.uniform(2, 10, kc) * MIN).astype(np.int64)
                mc = cash_m.sample(np.zeros(kc, np.int64), rng.random(kc))
                ev = dict(event_time_us=tc, payment_type=PT_PURCHASE, channel=CH_ECOM, entry_mode=EM_NON3DS)
                ev.update(self.card_fields(np.full(kc, card)))
                ev.update(self.merchant_fields(mc, False))
                ev.update(self.amounts(rng.uniform(300, 1500, kc), self.w.merchants["country"][mc]))
                ev.update(self.remote_ctx(kc, dev, ip, ipc, asn, g.city_lat[g.main_city[ipc]], g.city_lon[g.main_city[ipc]]))
                ev.update(self.label_cols(kc, "card_testing", cid, LS_CHARGEBACK))
                chunks.append(make_chunk(kc, **ev))
        return concat_chunks(chunks)

    def _account_takeover(self, T: int, p: dict) -> dict:
        rng = self.rng
        g = geo()
        cid = self.next_campaign_id()
        cr = int(self.pick_customers(1)[0])
        card = int(self.c["card_offset"][cr])
        ip, ipc, asn = self.fraud_ip(int(self.c["home_country"][cr]))
        dev = self.fraud_device(1)[0]
        k = _rng_range(rng, p.get("purchases"), (1, 4))
        t = T + np.cumsum(rng.uniform(3, 20, k) * MIN).astype(np.int64)
        mrow = self.mi.online_in_cats(M.cats(*M.HIGH_RISK_RESALE)).sample(np.zeros(k, np.int64), rng.random(k))
        usd = rng.uniform(150, 2000, k)
        lat, lon = g.city_lat[g.main_city[ipc]], g.city_lon[g.main_city[ipc]]
        ev = dict(event_time_us=t, payment_type=PT_PURCHASE, channel=np.where(rng.random(k) < 0.3, CH_IN_APP, CH_ECOM),
                  entry_mode=np.where(rng.random(k) < 0.6, EM_COF, EM_NON3DS))
        ev.update(self.card_fields(np.full(k, card)))
        ev.update(self.merchant_fields(mrow, False))
        ev.update(self.amounts(usd, self.w.merchants["country"][mrow]))
        ev.update(self.remote_ctx(k, dev, ip, ipc, asn, lat, lon))
        ev.update(self.label_cols(k, "account_takeover", cid, LS_CUSTOMER_REPORT))
        chunks = [make_chunk(k, **ev)]
        if rng.random() < float(p.get("transfer_prob", 0.5)) and self.mules.shape[0]:
            mule = int(self.mules[rng.integers(0, self.mules.shape[0])])
            tt = np.array([t[-1] + int(rng.uniform(2, 15) * MIN)])
            ev = dict(event_time_us=tt, payment_type=PT_TRANSFER, channel=CH_ONLINE_BANKING, entry_mode=EM_BANK_APP,
                      customer_id=np.uint64(cr + 1), account_id=np.uint64(ACCOUNT_BASE + cr * 4),
                      payee_id=self.w.payees["payee_id"][mule])
            ev.update(self.amounts(rng.uniform(500, 5000, 1), np.array([self.c["home_country"][cr]])))
            ev.update(self.remote_ctx(1, dev, ip, ipc, asn, lat, lon))
            ev.update(self.label_cols(1, "account_takeover", cid, LS_CUSTOMER_REPORT))
            chunks.append(make_chunk(1, **ev))
        return concat_chunks(chunks)

    def _lost_stolen(self, T: int, p: dict) -> dict:
        rng = self.rng
        cid = self.next_campaign_id()
        card = int(self.pick_cards(1)[0])
        cr = int(self.cards["customer_row"][card])
        cvm = int(self.cards["cvm_usd_minor"][card])
        k = int(np.clip(np.ceil(21000 / (0.93 * cvm)), 4, 8)) + _rng_range(rng, p.get("extra_txns"), (0, 2))
        k = min(k, 9)
        t = T + np.cumsum(rng.uniform(1.5, 3.2, k) * MIN).astype(np.int64)
        city = int(self.c["home_city"][cr])
        idx = self.mi.pos_in_city_cats(M.cats(*M.STOLEN_CARD_POS))
        grp = np.full(k, city, np.int64)
        mrow = idx.sample(grp, rng.random(k)) if idx.has(grp[:1])[0] else self.mi.pos_by_city.sample(grp, rng.random(k))
        usd = cvm / 100.0 * rng.uniform(0.92, 0.99, k)
        # pay in the card's home currency so the converted amount stays just under the CVM limit
        cur = np.full(k, self.c["home_country"][cr], np.int64)
        ev = dict(event_time_us=t, payment_type=PT_PURCHASE, channel=CH_POS, entry_mode=EM_CONTACTLESS)
        ev.update(self.card_fields(np.full(k, card)))
        ev.update(self.merchant_fields(mrow, True))
        ev.update(self.amounts(usd, cur))
        ev.update(self.label_cols(k, "lost_stolen", cid, LS_CUSTOMER_REPORT, delay_scale=0.3))
        return make_chunk(k, **ev)

    def _counterfeit(self, T: int, p: dict) -> dict:
        rng = self.rng
        g = geo()
        cid = self.next_campaign_id()
        card = int(self.pick_cards(1)[0])
        cr = int(self.cards["customer_row"][card])
        hlat, hlon = float(self.c["home_lat"][cr]), float(self.c["home_lon"][cr])
        dist = haversine_km(hlat, hlon, g.city_lat, g.city_lon)
        far = np.flatnonzero((dist > 1500) & (g.city_country != self.c["home_country"][cr]))
        city = int(far[rng.integers(0, far.shape[0])])
        # genuine cardholder transaction at home shortly before the clone is used abroad (not fraud)
        hab = int(self.c["hab0"][cr])
        anchor = dict(event_time_us=np.array([T]), payment_type=PT_PURCHASE, channel=CH_POS, entry_mode=EM_CHIP)
        anchor.update(self.card_fields(np.array([card])))
        anchor.update(self.merchant_fields(np.array([hab]), True))
        anchor.update(self.amounts(rng.uniform(10, 60, 1), self.w.merchants["country"][[hab]]))
        chunks = [make_chunk(1, **anchor)]
        t_first = T + int(rng.uniform(10, 80) * MIN)
        n_pos = _rng_range(rng, p.get("pos"), (2, 6))
        n_atm = _rng_range(rng, p.get("atm"), (0, 3))
        k = n_pos + n_atm
        t = t_first + np.concatenate([[0], np.cumsum(rng.uniform(10, 40, k - 1) * MIN)]).astype(np.int64)
        is_atm = np.zeros(k, bool)
        is_atm[rng.choice(np.arange(1, k), size=n_atm, replace=False) if n_atm and k > 1 else []] = True
        grp = np.full(k, city, np.int64)
        idx = self.mi.pos_in_city_cats(M.cats(*M.COUNTERFEIT_POS))
        pos_m = idx.sample(grp, rng.random(k)) if idx.has(grp[:1])[0] else self.mi.pos_by_city.sample(grp, rng.random(k))
        atm_m = self.mi.atm_by_city.sample(grp, rng.random(k))
        mrow = np.where(is_atm, atm_m, pos_m)
        usd = np.where(is_atm, rng.uniform(200, 500, k), np.exp(rng.normal(np.log(450), 0.6, k)).clip(150, 1500))
        ev = dict(event_time_us=t, payment_type=np.where(is_atm, PT_ATM, PT_PURCHASE), channel=np.where(is_atm, CH_ATM, CH_POS),
                  entry_mode=EM_MAGSTRIPE)
        ev.update(self.card_fields(np.full(k, card)))
        ev.update(self.merchant_fields(mrow, True))
        cur = self.w.merchants["country"][mrow]
        a1 = self.amounts(usd, cur, round_atm=False)
        a2 = self.amounts(usd, cur, round_atm=True)
        ev.update({key: np.where(is_atm, a2[key], a1[key]) for key in a1})
        ev.update(self.label_cols(k, "counterfeit", cid, LS_CUSTOMER_REPORT, delay_scale=0.5))
        chunks.append(make_chunk(k, **ev))
        return concat_chunks(chunks)

    def _merchant_breach(self, T: int, p: dict) -> dict:
        rng = self.rng
        g = geo()
        cid = self.next_campaign_id()
        k_vict = max(1, int(p.get("victims", 15)))
        seed_cust = int(self.pick_customers(1)[0])
        merchant = int(self.c["hab0"][seed_cust])
        hab = np.stack([self.c[f"hab{j}"][self.rows] for j in range(6)], axis=1)
        exposed = self.rows[np.any(hab == merchant, axis=1)]
        victims = exposed[rng.choice(exposed.shape[0], size=min(k_vict, exposed.shape[0]), replace=False)]
        if victims.shape[0] < k_vict:
            victims = np.concatenate([victims, self.pick_customers(k_vict - victims.shape[0])])
        lo, hi = p.get("delay_days", (0.2, 2.0))
        devs = self.fraud_device(3)
        ip, ipc, asn = self.fraud_ip(-1)
        cash_m = self.mi.online_in_cats(M.cats(*M.HIGH_RISK_RESALE))
        chunks = []
        for v in victims.tolist():
            kk = int(rng.integers(1, 4))
            t = T + int(rng.uniform(lo, hi) * DAY) + np.concatenate([[0], np.cumsum(rng.uniform(0.5, 8, kk - 1) * HOUR)]).astype(np.int64)
            card = int(self.c["card_offset"][v])
            mrow = cash_m.sample(np.zeros(kk, np.int64), rng.random(kk))
            ev = dict(event_time_us=t, payment_type=PT_PURCHASE, channel=CH_ECOM, entry_mode=EM_NON3DS)
            ev.update(self.card_fields(np.full(kk, card)))
            ev.update(self.merchant_fields(mrow, False))
            ev.update(self.amounts(rng.uniform(80, 900, kk), self.w.merchants["country"][mrow]))
            ev.update(self.remote_ctx(kk, devs[rng.integers(0, 3, kk)], ip, ipc, asn,
                                      g.city_lat[g.main_city[ipc]], g.city_lon[g.main_city[ipc]]))
            ev.update(self.label_cols(kk, "merchant_breach", cid, LS_CHARGEBACK))
            chunks.append(make_chunk(kk, **ev))
        return concat_chunks(chunks)

    def _atm_cashout(self, T: int, p: dict) -> dict:
        rng = self.rng
        g = geo()
        cid = self.next_campaign_id()
        n_cards = max(1, int(p.get("cards", 200)))
        cards = self.pick_cards(n_cards)
        # unlimited operations cash out abroad far more often than at home (cloned cards are shipped to
        # mule crews in another country); foreign_prob picks the side for the campaign
        homes = self.c["home_country"][self.cards["customer_row"][cards]]
        home = int(np.bincount(homes).argmax())
        abroad = rng.random() < float(p.get("foreign_prob", 0.5))
        w = g.city_global_weight * ((g.city_country != home) if abroad else (g.city_country == home))
        w = w if w.sum() > 0 else g.city_global_weight
        city = int(rng.choice(g.n_cities, p=w / w.sum()))
        n_atms = int(p.get("atms", 20))
        grp = np.full(n_atms, city, np.int64)
        atms = np.unique(self.mi.atm_by_city.sample(grp, rng.random(n_atms)))
        dur = float(p.get("duration_min", 15)) * MIN
        w_lo, w_hi = p.get("withdrawals", (3, 6))
        k_per = rng.integers(int(w_lo), int(w_hi) + 1, cards.shape[0])
        total = int(k_per.sum())
        card_rep = np.repeat(cards, k_per)
        spacing = rng.uniform(1, 4, total) * MIN
        first = np.repeat(rng.uniform(0, dur, cards.shape[0]), k_per)
        # cumulative spacing within each card
        cs = np.cumsum(spacing)
        starts = np.repeat(np.concatenate([[0], np.cumsum(k_per)[:-1]]), k_per)
        within = cs - np.concatenate([[0.0], cs])[starts]
        t = (T + first + within).astype(np.int64)
        mrow = atms[rng.integers(0, atms.shape[0], total)]
        usd = rng.uniform(300, 500, total)
        ev = dict(event_time_us=t, payment_type=PT_ATM, channel=CH_ATM,
                  entry_mode=np.where(rng.random(total) < 0.7, EM_MAGSTRIPE, EM_CHIP))
        ev.update(self.card_fields(card_rep))
        ev.update(self.merchant_fields(mrow, True))
        ev.update(self.amounts(usd, self.w.merchants["country"][mrow], round_atm=True))
        ev.update(self.label_cols(total, "atm_cashout", cid, LS_INVESTIGATION, delay_scale=0.2))
        return make_chunk(total, **ev)

    def _app_scam_mule(self, T: int, p: dict) -> dict:
        rng = self.rng
        cid = self.next_campaign_id()
        if not self.mules.shape[0]:
            return make_chunk(0)
        mule = int(self.mules[rng.integers(0, self.mules.shape[0])])
        k = max(6, int(p.get("victims", 8)))
        senders = self.pick_customers(k)
        window = float(p.get("window_h", 4)) * HOUR
        t = (T + np.sort(rng.uniform(0, window, k))).astype(np.int64)
        c = self.c
        devs = self.w.devices["device_id"][c["device_offset"][senders]]
        usd = np.exp(rng.normal(np.log(2500), 0.6, k)).clip(1000, 9000)
        ev = dict(event_time_us=t, payment_type=PT_TRANSFER, channel=CH_ONLINE_BANKING, entry_mode=EM_BANK_APP,
                  customer_id=(senders + 1).astype(np.uint64), account_id=(ACCOUNT_BASE + senders * 4).astype(np.uint64),
                  payee_id=self.w.payees["payee_id"][mule])
        ev.update(self.amounts(usd, c["home_country"][senders]))
        ev.update(self.remote_ctx(k, devs, c["home_ip"][senders], c["home_country"][senders], c["home_asn"][senders],
                                  c["home_lat"][senders], c["home_lon"][senders]))
        ev.update(self.label_cols(k, "app_scam_mule", cid, LS_CUSTOMER_REPORT, delay_scale=0.3))
        return make_chunk(k, **ev)

    def _bust_out(self, T: int, p: dict) -> dict:
        rng = self.rng
        cid = self.next_campaign_id()
        card = int(self.pick_cards(1, credit_only=True)[0])
        cr = int(self.cards["customer_row"][card])
        limit = max(500.0, float(self.cards["credit_limit_usd"][card]))
        k = _rng_range(rng, p.get("purchases"), (3, 6))
        share = rng.dirichlet(np.full(k, 2.0)) * limit * rng.uniform(0.9, 1.1)
        build_h = float(p.get("buildup_hours", 0) or 0)
        T_bust = T + int(build_h * HOUR)
        t = T_bust + np.sort(rng.uniform(0, float(p.get("span_hours", 20)), k) * HOUR).astype(np.int64)
        remote = rng.random(k) < 0.6
        on = self.mi.online_in_cats(M.cats(*M.HIGH_RISK_RESALE, "airlines", "travel_agency")).sample(np.zeros(k, np.int64), rng.random(k))
        city = np.full(k, self.c["home_city"][cr], np.int64)
        idx = self.mi.pos_in_city_cats(M.cats("electronics", "jewellery", "department_store"))
        pos = idx.sample(city, rng.random(k)) if idx.has(city[:1])[0] else self.mi.pos_by_city.sample(city, rng.random(k))
        mrow = np.where(remote, on, pos)
        c = self.c
        dev = self.w.devices["device_id"][c["device_offset"][cr]]
        ev = dict(event_time_us=t, payment_type=PT_PURCHASE, channel=np.where(remote, CH_ECOM, CH_POS),
                  entry_mode=np.where(remote, EM_COF, EM_CHIP))
        ev.update(self.card_fields(np.full(k, card)))
        mf = self.merchant_fields(mrow, ~remote)
        mf["lat"] = np.where(remote, c["home_lat"][cr], mf["lat"])
        mf["lon"] = np.where(remote, c["home_lon"][cr], mf["lon"])
        ev.update(mf)
        ev.update(self.amounts(share, self.w.merchants["country"][mrow]))
        ev.update(dict(device_id=np.where(remote, dev, 0), ip_v4=np.where(remote, c["home_ip"][cr], 0),
                       ip_country_idx=np.where(remote, c["home_country"][cr], -1), ip_asn=np.where(remote, c["home_asn"][cr], 0)))
        ev.update(self.label_cols(k, "bust_out", cid, LS_INVESTIGATION, delay_scale=1.5))
        bust = make_chunk(k, **ev)
        if build_h <= 0:
            return bust
        return concat_chunks([self._bust_out_buildup(T, build_h, card, cr, p), bust])

    def _bust_out_buildup(self, T: int, hours: float, card: int, cr: int, p: dict) -> dict:
        """The account looks like a good customer: on-profile purchases at home, ramping up in
        frequency (more of them towards the bust), near the customer's usual ticket. Legitimate."""
        rng = self.rng
        n = _rng_range(rng, p.get("buildup_purchases"), (6, 12))
        # stratified quantiles of a density growing linearly in time: ~3/4 of the build-up lands in its second half
        t = (T + np.sqrt((np.arange(n) + rng.random(n)) / n) * hours * HOUR).astype(np.int64)
        c = self.c
        city = np.full(n, c["home_city"][cr], np.int64)
        mrow = self.mi.pos_by_city.sample(city, rng.random(n))
        usd = np.exp(rng.normal(np.log(max(5.0, float(c["avg_ticket_usd"][cr]) * 1.3)), 0.4, n))
        ev = dict(event_time_us=t, payment_type=PT_PURCHASE, channel=CH_POS, entry_mode=EM_CHIP)
        ev.update(self.card_fields(np.full(n, card)))
        ev.update(self.merchant_fields(mrow, True))
        ev.update(self.amounts(usd, self.w.merchants["country"][mrow]))
        return make_chunk(n, **ev)

    def _bot_ecom(self, T: int, p: dict) -> dict:
        rng = self.rng
        g = geo()
        cid = self.next_campaign_id()
        cards = self.pick_cards(max(4, int(p.get("cards", 10))))
        merchant = int(self.mi.online_in_cats(M.cats(*M.BOT_TARGETS)).sample(np.zeros(1, np.int64), rng.random(1))[0])
        dur = _rng_range(rng, p.get("duration_min"), (5, 30)) * MIN
        attempts = rng.integers(1, 3, cards.shape[0])
        card_rep = rng.permutation(np.repeat(cards, attempts))
        k = card_rep.shape[0]
        t = (T + np.sort(rng.uniform(0, dur, k))).astype(np.int64)
        dev = self.fraud_device(1)[0]
        ip, ipc, asn = self.fraud_ip(-1)
        ev = dict(event_time_us=t, payment_type=PT_PURCHASE, channel=CH_ECOM, entry_mode=EM_NON3DS,
                  cvv_match=rng.random(k) > 0.2)
        ev.update(self.card_fields(card_rep))
        ev.update(self.merchant_fields(np.full(k, merchant), False))
        ev.update(self.amounts(rng.uniform(5, 80, k), np.full(k, self.w.merchants["country"][merchant])))
        ev.update(self.remote_ctx(k, dev, ip, ipc, asn, g.city_lat[g.main_city[ipc]], g.city_lon[g.main_city[ipc]]))
        ev.update(self.label_cols(k, "bot_ecom", cid, LS_CHARGEBACK))
        return make_chunk(k, **ev)
