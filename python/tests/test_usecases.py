"""Use cases as data: the typology registry, user CRUD, the `steps` DSL, and run snapshots."""
from __future__ import annotations

import json

import numpy as np
import pytest
import yaml

from realy import usecases as uc


def test_builtin_registry_matches_the_former_hard_coded_tables():
    r = uc.registry()
    names = r.fraud_type_names()
    assert names[0] == "none" and names[1] == "card_testing" and names[9] == "bot_ecom" and names[10] == "friendly_fraud"
    assert r.default_mix() == {"card_testing": 0.22, "account_takeover": 0.15, "lost_stolen": 0.12, "counterfeit": 0.10,
                               "merchant_breach": 0.08, "atm_cashout": 0.03, "app_scam_mule": 0.12, "bust_out": 0.06,
                               "bot_ecom": 0.12}
    assert "friendly_fraud" not in r.names()                      # label-only: never a campaign
    t = r.by_name["lost_stolen"]
    assert (t.label_source, t.labels.delay_scale) == (uc.LABEL_SOURCES["customer_report"], 0.3)
    assert r.role_name(t.role_code("spree_payment")) == ("lost_stolen", "spree_payment")
    assert r.role_name(0) == (None, None)


def test_user_typologies_create_update_override_delete(tmp_path):
    builtin, user = uc.BUILTIN_DIR, tmp_path / "user"
    doc = yaml.safe_load((builtin / "investment_scam.yaml").read_text())
    doc.update(name="pig_butchering", id=uc.next_id(uc.registry(builtin, user)), title="Pig butchering")
    t = uc.save_user(doc, user=user)
    reg = uc.registry(builtin, user)
    assert reg.by_name["pig_butchering"].source == "user" and t.id not in (1, 9, 10, 11)
    doc["mix_weight"] = 0.05                                       # update in place
    uc.save_user(doc, user=user)
    assert uc.registry(builtin, user).by_name["pig_butchering"].mix_weight == 0.05
    clash = dict(doc, name="other_name")                           # ids stay unique
    with pytest.raises(ValueError, match="already used"):
        uc.save_user(clash, user=user)
    off = yaml.safe_load((builtin / "bot_ecom.yaml").read_text())  # a user copy disables a built-in
    off["disabled"] = True
    uc.save_user(off, user=user)
    assert "bot_ecom" not in uc.registry(builtin, user).names()
    assert uc.delete_user("pig_butchering", user=user) and uc.delete_user("bot_ecom", user=user)
    assert "pig_butchering" not in uc.registry(builtin, user).by_name and not uc.delete_user("nope", user=user)


@pytest.mark.parametrize("bad, match", [
    ({"steps": [{"role": "nope"}]}, "not declared"),
    ({"steps": [{"role": "first_transfer", "entry_mode": "telepathy"}]}, "entry_mode"),
    ({"steps": [{"role": "first_transfer", "merchant": {"categories": ["NOT_A_GROUP"]}}]}, "NOT_A_GROUP"),
    ({"pattern": "unknown_generator"}, "pattern"),
    ({"name": "Bad Name"}, "name"),
])
def test_invalid_definitions_are_rejected_at_save_time(tmp_path, bad, match):
    doc = yaml.safe_load((uc.BUILTIN_DIR / "investment_scam.yaml").read_text())
    doc.update({"name": "probe_typology", "id": 100})
    doc.update(bad)
    with pytest.raises(Exception, match=match):
        uc.save_user(doc, user=tmp_path)


def test_steps_typology_generates_its_story_without_code():
    from realy.gen import Generator
    from realy.scenario import load_scenario
    from realy.world import load_or_build
    sc = load_scenario("smoke")
    sc.fraud.background_per_1k = 0.0
    w = load_or_build(sc.world, sc.seed, sc.world_dir)
    g = Generator(w, sc, 0, 1)
    t0 = 1_800_000_000_000_000
    for i in range(6):
        g.schedule_campaign("investment_scam", t0 + i * 1_000_000)
    b = g.generate(5_000, t0, t0 + 10 * 86_400_000_000)
    c = b.columns
    fraud = np.asarray(c["is_fraud"])
    reg = uc.registry()
    t = reg.by_name["investment_scam"]
    assert fraud.sum() >= 6 * 3 and set(np.asarray(c["fraud_type"])[fraud]) == {t.id}
    roles = {reg.role_name(int(x))[1] for x in np.asarray(b.columns.get("campaign_role", np.zeros(0)))[fraud]} \
        if "campaign_role" in c else None
    # each campaign: one first transfer to a fresh payee, then escalating transfers to the SAME payee
    camp = np.asarray(c["campaign_id"])[fraud]
    payee = np.asarray(c["payee_id"])[fraud]
    amt = np.asarray(c["amount_usd_minor"])[fraud] / 100
    for cid in np.unique(camp):
        m = camp == cid
        assert len(set(payee[m].tolist())) == 1 and m.sum() >= 3
        assert amt[m].min() < 1000 <= amt[m].max()
    assert np.all(np.asarray(c["payment_type"])[fraud] == 3) and np.all(np.asarray(c["device_id"])[fraud] > 0)
    if roles is not None:
        assert roles == {"first_transfer", "escalating_transfer"}


def test_runs_read_their_own_snapshot(tmp_path):
    snap = uc.registry().snapshot()
    snap["typologies"] = [x for x in snap["typologies"] if x["name"] != "investment_scam"]
    (tmp_path / "usecases.json").write_text(json.dumps(snap))
    assert "investment_scam" not in uc.for_run(tmp_path).by_name           # the run predates the typology
    assert "investment_scam" in uc.for_run(None).by_name
