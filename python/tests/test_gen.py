import hashlib
import time

import numpy as np
import pytest

from realy.gen import Generator, auto_time_scale, sim_start_us
from realy.scenario import Scenario, WorldSpec
from realy.world import build_world, haversine_km
from realy.payments.v1 import transaction_pb2

MIN = 60_000_000
HOUR = 60 * MIN
SPEC = WorldSpec(customers=20_000, merchants=2_000, payees=5_000)


@pytest.fixture(scope="module")
def world():
    return build_world(SPEC, 5, None)


def _scenario(**fraud):
    return Scenario(name="t", seed=5, world=SPEC, fraud=fraud or {})


def run(gen, batches, n=200, t0=None, dt=None):
    t = t0 if t0 is not None else sim_start_us(gen.scenario)
    dt = dt or int(30e6)
    out = []
    for _ in range(batches):
        out.append(gen.generate(n, t, t + dt))
        t += dt
    return out


def merged(batches):
    cols = {k: np.concatenate([b.columns[k] for b in batches]) for k in batches[0].columns}
    return cols


def test_deterministic_payloads(world):
    def digest():
        g = Generator(world, _scenario(), 0, 1)
        g.schedule_campaign("card_testing", sim_start_us(g.scenario) + 60_000_000)
        h = hashlib.sha256()
        for b in run(g, 30):
            for p in b.payloads:
                h.update(p)
        return h.hexdigest()
    assert digest() == digest()


def test_ids_order_and_times(world):
    g = Generator(world, _scenario(), 1, 3)
    bs = run(g, 20)
    c = merged(bs)
    seq = c["event_id"] & np.uint64((1 << 48) - 1)
    assert np.all(seq == np.arange(seq.shape[0]))
    assert np.all((c["event_id"] >> np.uint64(48)) == 1)
    assert np.all(c["customer_id"] % 3 == 1)
    assert np.all(np.diff(c["event_time_us"]) >= 0)
    # per-customer monotone across batches (global order is time-sorted, so this follows)
    for b in bs:
        assert len(b.payloads) == len(b.keys) == b.event_ids.shape[0] >= 200
    ev = transaction_pb2.TransactionEvent()
    ev.ParseFromString(bs[0].payloads[0])
    assert ev.event_id == int(bs[0].event_ids[0]) and str(ev.customer_id).encode() == bs[0].keys[0]


def test_field_invariants(world):
    g = Generator(world, _scenario(background_per_1k=2.0), 0, 1)
    c = merged(run(g, 40, n=500))
    tr = c["payment_type"] == 3
    assert np.all(c["card_id"][tr] == 0) and np.all(c["payee_id"][tr] != 0) and np.all(c["merchant_id"][tr] == 0)
    assert np.all(c["card_id"][~tr] != 0)
    cp = c["card_present"]
    assert np.all(c["terminal_id"][cp] != 0) and np.all(c["device_id"][cp & ~tr] == 0)
    remote = ~cp
    assert np.all(c["device_id"][remote] != 0) and np.all(c["ip_v4"][remote] != 0)
    assert np.all(c["amount_minor"] > 0) and np.all(c["amount_usd_minor"] > 0)
    atm = c["payment_type"] == 2
    assert np.all(c["mcc"][atm] == 6011)
    credit = c["card_type"] == 2
    assert np.all(c["credit_limit_usd"][credit] > 0)
    assert set(np.unique(c["currency"]).tolist()) <= {"USD", "GBP", "EUR", "PLN", "SEK", "CAD", "MXN", "BRL", "INR",
                                                        "JPY", "AUD", "SGD", "AED", "NGN", "ZAR", "TRY", "RON", "UAH", "CNY"}


def _campaign_stream(world, kind, params=None, batches=60, dt=int(60e6), n=100):
    g = Generator(world, _scenario(background_per_1k=0.0), 0, 1)
    t0 = sim_start_us(g.scenario)
    assert g.schedule_campaign(kind, t0 + 30_000_000, params or {}) > 0
    bs = run(g, batches, n=n, t0=t0, dt=dt)
    c = merged(bs)
    labels = sum(len(b.labels) for b in bs)
    return c, labels


def _window_count(t, mask, window):
    """For each event i (mask), number of masked events in (t_i - window, t_i]."""
    tt = t[mask]
    return np.searchsorted(tt, tt, side="right") - np.searchsorted(tt, tt - window, side="right")


def test_card_testing_trips_rule(world):
    c, labels = _campaign_stream(world, "card_testing")
    f = c["is_fraud"] & (c["fraud_type"] == 1)
    assert f.sum() >= 5 and labels >= 0
    card = c["card_id"][f][0]
    sel = (c["card_id"] == card) & (c["payment_type"] == 1) & ~c["card_present"] & (c["amount_usd_minor"] < 200)
    assert _window_count(c["event_time_us"], sel, 10 * MIN).max() >= 3


def test_card_testing_wave_merchant(world):
    c, _ = _campaign_stream(world, "card_testing", {"cards": 40})
    f = c["is_fraud"] & (c["fraud_type"] == 1) & (c["amount_usd_minor"] < 200)
    m = np.unique(c["merchant_id"][f])
    assert m.shape[0] == 1
    sel = (c["merchant_id"] == m[0]) & (c["amount_usd_minor"] < 200) & ~c["card_present"]
    assert _window_count(c["event_time_us"], sel, 5 * MIN).max() >= 25


def test_atm_cashout_trips_rules_and_spikes(world):
    c, _ = _campaign_stream(world, "atm_cashout", {"cards": 300})
    f = c["is_fraud"] & (c["fraud_type"] == 6)
    assert f.sum() >= 900  # load spike on top of the base rate
    for card in np.unique(c["card_id"][f])[:20]:
        sel = (c["card_id"] == card) & (c["payment_type"] == 2)
        assert _window_count(c["event_time_us"], sel, HOUR).max() >= 3
    term = c["terminal_id"][f][0]
    sel = (c["terminal_id"] == term) & (c["payment_type"] == 2)
    tt = c["event_time_us"][sel]
    amt = np.cumsum(c["amount_usd_minor"][sel])
    i = np.searchsorted(tt, tt - HOUR, side="right")
    window_sum = amt - np.concatenate([[0], amt])[i]
    assert window_sum.max() > 500_000


def test_mule_fan_in(world):
    c, _ = _campaign_stream(world, "app_scam_mule", {"victims": 8}, batches=300)
    f = c["is_fraud"] & (c["fraud_type"] == 7)
    payee = c["payee_id"][f][0]
    sel = c["payee_id"] == payee
    assert np.unique(c["customer_id"][sel]).shape[0] >= 6
    assert (c["event_time_us"][sel].max() - c["event_time_us"][sel].min()) < 24 * HOUR
    assert np.all(c["amount_usd_minor"][f] >= 100_000)


def test_bot_device_many_cards(world):
    c, _ = _campaign_stream(world, "bot_ecom")
    f = c["is_fraud"] & (c["fraud_type"] == 9)
    dev = c["device_id"][f][0]
    sel = c["device_id"] == dev
    assert np.unique(c["card_id"][sel]).shape[0] >= 4
    assert (c["event_time_us"][sel].max() - c["event_time_us"][sel].min()) < HOUR


def test_counterfeit_impossible_travel(world):
    c, _ = _campaign_stream(world, "counterfeit", batches=200)
    f = c["is_fraud"] & (c["fraud_type"] == 4)
    first = np.flatnonzero(f)[0]
    card = c["card_id"][first]
    prev = np.flatnonzero((c["card_id"] == card) & c["card_present"] & (np.arange(len(f)) < first))
    assert prev.shape[0] >= 1
    p = prev[-1]
    km = haversine_km(c["lat"][p], c["lon"][p], c["lat"][first], c["lon"][first])
    hours = max((c["event_time_us"][first] - c["event_time_us"][p]) / 3.6e9, 60 / 3600)
    assert km / hours > 900
    assert np.all(c["entry_mode"][f] == 3)


def test_lost_stolen_contactless_split(world):
    c, _ = _campaign_stream(world, "lost_stolen")
    f = c["is_fraud"] & (c["fraud_type"] == 3)
    card = c["card_id"][f][0]
    sel = (c["card_id"] == card) & (c["entry_mode"] == 2)
    t = c["event_time_us"][sel]
    a = np.cumsum(c["amount_usd_minor"][sel])
    i = np.searchsorted(t, t - 30 * MIN, side="right")
    counts = np.arange(1, t.shape[0] + 1) - i
    sums = a - np.concatenate([[0], a])[i]
    assert ((counts >= 4) & (sums > 20_000)).any()


def test_account_takeover_signals(world):
    c, _ = _campaign_stream(world, "account_takeover")
    f = np.flatnonzero(c["is_fraud"] & (c["fraud_type"] == 2))
    i = f[0]
    cust = c["customer_id"][i]
    earlier = (c["customer_id"] == cust) & (np.arange(len(c["customer_id"])) < i)
    assert c["device_id"][i] not in set(c["device_id"][earlier].tolist())
    home = world.customers["home_country"][cust - 1]
    from realy.world.geo import geo
    assert c["ip_country"][i] != geo().countries[home]
    assert c["amount_usd_minor"][i] > 10_000 and not c["card_present"][i]


def test_bust_out_utilisation(world):
    c, _ = _campaign_stream(world, "bust_out", batches=1500)
    f = c["is_fraud"] & (c["fraud_type"] == 8)
    card = c["card_id"][f][0]
    limit = c["credit_limit_usd"][f][0]
    sel = c["card_id"] == card
    t = c["event_time_us"][sel]
    a = np.cumsum(c["amount_usd_minor"][sel])
    i = np.searchsorted(t, t - 24 * HOUR, side="right")
    sums = a - np.concatenate([[0], a])[i]
    assert (sums / (limit * 100.0)).max() > 0.8


def test_bust_out_buildup_is_legit_and_ramps(world):
    c, _ = _campaign_stream(world, "bust_out", {"buildup_hours": 6, "span_hours": 2, "buildup_purchases": [10, 10]},
                            batches=600)
    f = c["is_fraud"] & (c["fraud_type"] == 8)
    card = c["card_id"][f][0]
    sel = c["card_id"] == card
    t_bust = c["event_time_us"][f].min()
    build = sel & ~c["is_fraud"] & (c["event_time_us"] < t_bust)
    assert build.sum() >= 10                                   # on-profile history before the bust ...
    bt = np.sort(c["event_time_us"][build])
    half = bt[0] + (t_bust - bt[0]) / 2
    assert (bt >= half).sum() > (bt < half).sum()              # ... ramping up towards it
    assert (c["amount_usd_minor"][build].mean() < c["amount_usd_minor"][f].mean() / 3)   # bust tickets dwarf it


def test_atm_cashout_cross_border(world):
    from realy.world.geo import geo
    g = geo()
    home_of = {}
    for fp, expect in ((1.0, True), (0.0, False)):
        c, _ = _campaign_stream(world, "atm_cashout", {"cards": 60, "foreign_prob": fp})
        f = c["is_fraud"] & (c["fraud_type"] == 6)
        mc = c["merchant_country"][f]
        # the cards' home countries (majority) vs where the ATMs are
        rows = c["customer_id"][f].astype(np.int64) - 1
        home = g.countries[int(np.bincount(world.customers["home_country"][rows]).argmax())]
        abroad = float((mc != home).mean())
        home_of[fp] = abroad
        assert (abroad > 0.5) == expect, (fp, abroad)


def test_merchant_breach_and_labels(world):
    sc = Scenario(name="t", seed=5, world=SPEC, fraud={"background_per_1k": 0.0, "unreported_fraction": 0.0,
                                                       "chargeback_delay_days": (1.0, 2.0)})
    g = Generator(world, sc, 0, 1)
    t0 = sim_start_us(sc)
    g.schedule_campaign("merchant_breach", t0, {"victims": 10, "delay_days": (0.01, 0.05)})
    bs = run(g, 200, n=50, t0=t0, dt=int(60e6))
    c = merged(bs)
    f = c["is_fraud"] & (c["fraud_type"] == 5)
    assert np.unique(c["customer_id"][f]).shape[0] >= 5
    labels = [lt for b in bs for lt, _ in b.labels]
    assert len(labels) == f.sum()
    from realy.payments.v1 import label_pb2
    lab = label_pb2.FraudLabel()
    lab.ParseFromString(bs[-1].labels[-1][1] if bs[-1].labels else [x for b in bs for x in b.labels][-1][1])
    assert lab.is_fraud and lab.fraud_type == 5 and lab.label_time_us - lab.event_time_us >= 0.9 * 86_400_000_000


def test_legit_false_positive_rates_low(world):
    g = Generator(world, _scenario(background_per_1k=0.0, friendly_fraud_per_1k=0.0), 0, 1)
    ts = auto_time_scale(world, 1000.0)
    bs = run(g, 300, n=100, dt=int(ts * 0.1e6))  # 30 s of a 1k TPS run
    c = merged(bs)
    assert not c["is_fraud"].any()
    # card testing signature on legit cards
    small = (c["payment_type"] == 1) & ~c["card_present"] & (c["amount_usd_minor"] < 200)
    order = np.lexsort((c["event_time_us"], c["card_id"]))
    cards, t = c["card_id"][order], c["event_time_us"][order]
    sm = small[order]
    hits = 0
    for card in np.unique(cards[sm]):
        s = (cards == card) & sm
        if _window_count(t, s, 10 * MIN).max() >= 3:
            hits += 1
    assert hits / max(1, np.unique(c["card_id"]).shape[0]) < 0.001
    # bot signature on legit devices
    dev = c["device_id"] != 0
    d_cards = {}
    for d, cd in zip(c["device_id"][dev].tolist(), c["card_id"][dev].tolist()):
        if cd:
            d_cards.setdefault(d, set()).add(cd)
    assert max((len(v) for v in d_cards.values()), default=0) <= 4


def test_throughput(world):
    sc = _scenario()
    g = Generator(world, sc, 0, 1)
    ts = auto_time_scale(world, 10_000.0)
    t = sim_start_us(sc)
    dt = int(ts * 0.1e6)  # 100 ms of wall time per batch at 10k TPS -> 1000 events
    g.generate(1000, t, t + dt)
    n, st = 0, time.perf_counter()
    for i in range(20):
        t += dt
        n += len(g.generate(1000, t, t + dt))
    rate = n / (time.perf_counter() - st)
    print(f"generator throughput: {rate:,.0f} events/s")
    assert rate > 15_000
