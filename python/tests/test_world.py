import numpy as np
import pytest

from realy.scenario import Scenario, WorldSpec
from realy.world import (ACCOUNT_BASE, CARD_BASE, DEVICE_BASE, MERCHANT_BASE, PAYEE_BASE, World, build_world)
from realy.world.geo import geo


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    d = tmp_path_factory.mktemp("world")
    return build_world(WorldSpec(customers=20_000, merchants=2_000, payees=5_000), 11, d)


def test_deterministic_hash(world):
    again = build_world(WorldSpec(customers=20_000, merchants=2_000, payees=5_000), 11, None)
    assert again.content_hash == world.content_hash
    other = build_world(WorldSpec(customers=20_000, merchants=2_000, payees=5_000), 12, None)
    assert other.content_hash != world.content_hash


def test_roundtrip(world):
    loaded = World.load(world.dir)
    assert loaded.content_hash == world.content_hash
    assert loaded.compute_hash() == world.content_hash


def test_id_ranges_disjoint(world):
    c = world.customers["customer_id"]
    assert c.min() == 1 and c.max() == world.n_customers
    assert world.cards["card_id"].min() >= CARD_BASE and world.cards["card_id"].max() < DEVICE_BASE
    assert world.devices["device_id"].min() >= DEVICE_BASE
    assert world.merchants["merchant_id"].min() >= MERCHANT_BASE
    assert world.payees["payee_id"].min() >= PAYEE_BASE
    assert c.max() < ACCOUNT_BASE
    assert np.all(world.customer_row(c) == np.arange(world.n_customers))


def test_cards_devices_habits(world):
    cu = world.customers
    assert cu["n_cards"].min() >= 1 and cu["n_devices"].min() >= 1
    owner = world.cards["customer_row"]
    assert np.all(owner[cu["card_offset"]] == np.arange(world.n_customers))
    m = world.merchants
    for k in range(6):
        h = cu[f"hab{k}"]
        assert h.min() >= 0
        assert not m["online"][h].any() and not m["atm"][h].any()
        assert np.all(m["city"][h] == cu["home_city"])
    credit = world.cards["card_type"] == 2
    assert np.all(world.cards["credit_limit_usd"][credit] > 0)
    assert np.all(world.cards["credit_limit_usd"][~credit] == 0)


def test_profile_arrays(world):
    p = world.customer_profile_arrays()
    assert set(p) >= {"customer_id", "tenure_days", "segment", "avg_ticket_usd", "home_lat", "home_lon",
                      "home_country_idx", "tz_offset_min"}
    assert all(v.shape[0] == world.n_customers for v in p.values())
    mp = world.merchant_profile_arrays()
    assert mp["risk_tier"].max() <= 3 and mp["fraud_rate_30d"].min() > 0
    assert len(geo().countries) == int(p["home_country_idx"].max()) + 1 or p["home_country_idx"].max() < len(geo().countries)


def test_profile_report_runs(world):
    from realy.world.profile import profile
    rep = profile(world, Scenario(name="p"), 20_000)
    assert 0.5 < rep["txns_per_customer_per_day"]["mean"] < 10
    assert 0.4 < rep["card_present_share"] < 0.8
    assert rep["payment_types"]["CARD_PURCHASE"] > 0.8
