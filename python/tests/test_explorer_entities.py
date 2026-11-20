"""Explorer for every use case: compound search over every id kind, generic entity lists and trails
(customer, card, device, IP, payee, merchant, ...), per-payment-type context columns from
contracts/explorer.yaml, scores that are never a fallback's 0.0, and the views contract itself."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from realy.control import app as app_mod
from realy.control import explorer as ex_mod
from realy.features import load_schema

IP = (10 << 24) | 1          # 10.0.0.1


def _client(tmp_path: Path) -> TestClient:
    runs = tmp_path / "runs"
    runs.mkdir(exist_ok=True)
    return TestClient(app_mod.create_app(runs_dir=runs, kafka=False, background=False, run_fn=lambda *a, **k: {},
                                         publish_fn=lambda msgs: None))


def _run(runs: Path) -> str:
    """Ten measured events and two history events:
    - customers 7, 8 and 9 pay online by card from one device (500), the first two from one IP;
    - 9, 10 and 11 send transfers: 9 and 10 to payee 600 (a payee with two senders), 11 to 601;
    - event 2 was decided by the rules alone (the model missed its deadline: stored score 0.0);
    - events 3, 4 are card testing, event 7 an APP scam transfer."""
    rid = "entities-run"
    d = runs / rid
    d.mkdir(parents=True)
    n = 12
    ids = np.arange(1, n + 1, dtype=np.uint64)
    pt = np.array([1, 1, 1, 1, 1, 3, 3, 3, 1, 1, 1, 1], np.int32)            # 3 = TRANSFER
    transfer = pt == 3
    backfill = np.zeros(n, bool)
    backfill[10:] = True
    cols = {
        "event_id": ids, "event_time_us": np.arange(n, dtype=np.int64) * 60_000_000 + 1_800_000_000_000_000,
        "payment_type": pt, "customer_id": np.array([7, 7, 8, 8, 9, 9, 10, 11, 12, 12, 7, 7], np.uint64),
        "account_id": np.array([71, 71, 81, 81, 91, 91, 101, 111, 121, 121, 71, 71], np.uint64),
        "card_id": np.array([70, 70, 80, 80, 90, 0, 0, 0, 120, 121, 70, 70], np.uint64),
        "card_type": np.where(transfer, 0, 1).astype(np.int32), "amount_minor": np.full(n, 150),
        "currency": ["GBP"] * n, "amount_usd_minor": np.full(n, 15000),
        "merchant_id": np.where(transfer, 0, 1000).astype(np.uint64), "mcc": np.where(transfer, 0, 5411).astype(np.uint32),
        "merchant_country": ["GB"] * n, "terminal_id": np.zeros(n, np.uint64),
        "channel": np.where(transfer, 6, 2).astype(np.int32), "entry_mode": np.where(transfer, 0, 7).astype(np.int32),
        "card_present": np.zeros(n, bool), "lat": np.zeros(n), "lon": np.zeros(n),
        "device_id": np.array([500, 500, 500, 500, 500, 0, 0, 0, 500, 500, 0, 0], np.uint64),
        "ip_v4": np.array([IP, IP, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], np.uint32), "ip_country": ["GB"] * n,
        "ip_asn": np.zeros(n, np.uint32), "payee_id": np.array([0, 0, 0, 0, 0, 600, 600, 601, 0, 0, 0, 0], np.uint64),
        "cvv_match": np.ones(n, bool), "avs_match": np.ones(n, bool), "recurring": np.zeros(n, bool),
        "credit_limit_usd": np.zeros(n, np.uint32), "three_ds": np.full(n, 2, np.int32),
        "is_fraud": np.isin(ids, [3, 4, 7]), "fraud_type": np.where(np.isin(ids, [3, 4]), 1, np.where(ids == 7, 7, 0)).astype(np.int32),
        "campaign_id": np.where(np.isin(ids, [3, 4]), 42, np.where(ids == 7, 43, 0)).astype(np.uint64),
        "label_delay_us": np.where(np.isin(ids, [3, 4, 7]), 86_400_000_000, -1).astype(np.int64),
        "label_is_fraud": np.isin(ids, [3, 4, 7]), "t_intended_us": np.arange(n, dtype=np.int64), "backfill": backfill}
    pq.write_table(pa.table(cols), d / "events-0.parquet")
    m = 10                                   # history events have no decision
    did = ids[:m]
    path = np.zeros(m, np.int8)
    path[1] = 1                              # event 2: RULES_ONLY_FALLBACK
    score = np.array([0.01, 0.0, 0.97, 0.99, 2e-5, 0.02, 0.91, 0.01, 0.003, 0.004], np.float32)
    outcome = np.array([1, 1, 3, 3, 1, 1, 2, 1, 1, 4], np.int8)
    pq.write_table(pa.table({
        "event_id": did, "revision": np.zeros(m, np.uint8), "outcome": outcome, "score": score, "path": path,
        "degraded": np.zeros(m, bool), "ingress_mode": np.full(m, 3, np.int8), "t_intended_us": np.full(m, 1000, np.int64),
        "t_sent_us": np.full(m, 1100, np.int64), "t_ingest_us": np.full(m, 3000, np.int64),
        "t_decision_us": np.full(m, 15000, np.int64), "t_collected_us": np.full(m, 21000, np.int64),
        "reason_codes": [""] * m, "rule_hits": ["", "", "CARD_TESTING", "CARD_TESTING", "", "", "MULE_FAN_IN", "", "", ""],
        "model_version": ["v9"] * m}), d / "decisions-0.parquet")
    schema = load_schema()
    nf = len(schema.features)
    idx = {f.name: f.index for f in schema.features}
    vals = np.zeros((m, nf), np.float32)
    vals[:, idx["payee_distinct_senders_24h"]] = [0, 0, 0, 0, 0, 1, 2, 1, 0, 0]
    vals[:, idx["dev_distinct_cards_1h"]] = [1, 1, 2, 2, 3, 0, 0, 0, 4, 5]
    vals[:, idx["cust_transfer_sum_24h"]] = [0, 0, 0, 0, 0, 15000, 15000, 15000, 0, 0]
    card_bits = [f.index for f in schema.features if f.entity == "card"]

    def missing(row: int) -> bytes:
        bm = np.zeros((nf + 7) // 8, np.uint8)
        if transfer[row]:                    # a transfer has no card: card features were defaulted
            for i in card_bits:
                bm[i >> 3] |= 1 << (i & 7)
        return bm.tobytes()
    pq.write_table(pa.table({"event_id": did, "values": pa.array(vals.tolist(), pa.list_(pa.float32())),
                             "missing": pa.array([missing(i) for i in range(m)], pa.binary())}), d / "features-0.parquet")
    (d / "scenario.json").write_text(json.dumps({"name": "x"}))
    (d / "manifest.json").write_text(json.dumps({"run_id": rid, "scenario": "x", "topology": "gateway-kafka"}))
    return rid


def _ids(rows):
    return [r["event_id"] for r in rows]


def test_views_contract_is_valid_and_served(tmp_path):
    c = _client(tmp_path)
    v = c.get("/api/explorer/views").json()
    assert v["problems"] == []
    assert {"customer", "card", "account", "device", "ip", "merchant", "terminal", "payee"} <= set(v["entities"])
    assert [x["name"] for x in v["contexts"]][:4] == ["transfer", "atm", "card_present", "online_card"]
    # money aggregates are cents in the schema: the views say so, whatever the file wrote
    tr = {x["key"]: x for x in v["contexts"][0]["columns"]}
    assert tr["cust_transfer_sum_24h"]["format"] == "usd_minor" and tr["payee_id"]["link"] == "payee"
    assert any(ln["entity"] == "payee" for ln in v["lenses"])
    assert c.get("/api/usecases").json()["typologies"][0]["name"]


def test_views_loader_reports_bad_entries(tmp_path):
    p = tmp_path / "explorer.yaml"
    p.write_text("entities:\n  thing: {key: not_a_column}\n  card: {key: card_id, features: [no_such_feature]}\n"
                 "contexts:\n  - {name: x, match: {colour: [red]}, columns: [{field: lat}, {feature: nope}]}\n")
    v = ex_mod.load_views(p)
    joined = " | ".join(v["problems"])
    for frag in ("thing: key not_a_column", "unknown feature no_such_feature", "cannot match on colour",
                 "lat is not an indexed event field", "unknown feature nope"):
        assert frag in joined
    ex_mod._views_cache.clear()


def test_search_every_id_kind_and_compound_terms(tmp_path):
    c = _client(tmp_path)
    rid = _run(tmp_path / "runs")
    q = lambda s: c.get(f"/api/runs/{rid}/search", params={"q": s})  # noqa: E731
    assert _ids(q("payee:600").json()["results"]) == ["6", "7"]
    assert _ids(q("device:500").json()["results"]) == ["1", "2", "3", "4", "5", "9", "10"]
    assert _ids(q("ip:10.0.0.1").json()["results"]) == ["1", "2"]                  # dotted address
    assert _ids(q(f"ip:{IP}").json()["results"]) == ["1", "2"]
    assert _ids(q("account:91").json()["results"]) == ["5", "6"]
    assert _ids(q("channel:ONLINE_BANKING").json()["results"]) == ["6", "7", "8"]
    assert _ids(q("type:TRANSFER outcome:REVIEW").json()["results"]) == ["7"]      # terms are ANDed
    assert _ids(q("outcome:DECLINE,CHALLENGE").json()["results"]) == ["3", "4", "10"]   # commas are OR
    assert _ids(q("rule:MULE_FAN_IN").json()["results"]) == ["7"]
    assert _ids(q("fraud:any").json()["results"]) == ["3", "4", "7"]
    assert _ids(q("path:RULES_ONLY_FALLBACK").json()["results"]) == ["2"]
    assert _ids(q("label:fraud").json()["results"]) == ["3", "4", "7"]
    # a bare id is tried as every kind of id, and says what it matched
    r = q("7").json()
    assert set(_ids(r["results"])) == {"1", "2", "7"} and r["matched"] == {"event": 1, "customer": 2}
    assert q("11").json()["matched"] == {"customer": 1}                              # history events excluded
    bad = q("color:red")
    assert bad.status_code == 400 and "payee" in bad.json()["detail"] and "ip" in bad.json()["detail"]
    assert q("channel:SPACESHIP").status_code == 400
    assert q("payee:abc").status_code == 400


def test_rows_never_show_a_fallback_score_and_keep_tiny_scores(tmp_path):
    c = _client(tmp_path)
    rid = _run(tmp_path / "runs")
    rows = {r["event_id"]: r for r in c.get(f"/api/runs/{rid}/search", params={"q": "customer:7,9"}).json()["results"]}
    assert rows["2"]["score"] is None and rows["2"]["path"] == "RULES_ONLY_FALLBACK"
    assert abs(rows["5"]["score"] - 2e-5) < 1e-9 and rows["5"]["model_version"] == "v9"
    assert rows["6"]["card_id"] is None and rows["6"]["payee_id"] == "600"           # a transfer has no card
    assert rows["1"]["ip"] == "10.0.0.1"
    d = c.get(f"/api/runs/{rid}/decisions/2").json()
    assert d["decision"]["score"] is None and "deadline" in d["decision"]["score_note"]
    t = c.get(f"/api/runs/{rid}/decisions/6").json()
    assert t["event"]["card_id"] is None and t["event"]["card_type"] is None and t["event"]["mcc"] is None


def test_payee_fan_in_and_shared_device(tmp_path):
    c = _client(tmp_path)
    rid = _run(tmp_path / "runs")
    payees = c.get(f"/api/runs/{rid}/entities/payee", params={"show": "shared"}).json()
    assert [r["id"] for r in payees["rows"]] == ["600"] and payees["rows"][0]["distinct"]["customer"] == 2
    assert payees["stats"]["entities"] == 2 and payees["stats"]["shared"] == 1
    dev = c.get(f"/api/runs/{rid}/entities/device", params={"sort": "shared"}).json()["rows"][0]
    assert dev["id"] == "500" and dev["distinct"]["card"] == 5 and dev["distinct"]["customer"] == 4
    assert dev["fraud_events"] == 2 and dev["fraud_types"] == ["card_testing"]
    ips = c.get(f"/api/runs/{rid}/entities/ip").json()["rows"]
    assert ips[0]["display"] == "10.0.0.1"
    # a search narrows what is counted: devices of the card-testing events only
    sub = c.get(f"/api/runs/{rid}/entities/device", params={"q": "fraud:card_testing"}).json()["rows"][0]
    assert sub["events"] == 2
    assert c.get(f"/api/runs/{rid}/entities/planet").status_code == 404
    assert c.get(f"/api/runs/{rid}/entities/device", params={"show": "nope"}).status_code == 400


def test_entity_trail_context_columns_pivots_and_paging(tmp_path):
    c = _client(tmp_path)
    rid = _run(tmp_path / "runs")
    t = c.get(f"/api/runs/{rid}/entities/payee/600").json()
    assert _ids(t["events"]) == ["6", "7"] and t["total"] == 2 and not t["truncated"]
    ev = t["events"][1]
    assert ev["context"] == "transfer" and ev["ctx"]["payee_id"] == "600"
    assert ev["ctx"]["payee_distinct_senders_24h"] == 2.0 and ev["ctx"]["cust_transfer_sum_24h"] == 15000.0
    assert ev["state"]["payee_distinct_senders_24h"] == 2.0 and ev["features_recorded"]
    assert ev["label"] == {"fraud": True, "after_days": 1.0}
    assert t["related"]["customer"]["distinct"] == 2 and {x["id"] for x in t["related"]["customer"]["top"]} == {"9", "10"}
    assert t["summary"]["events"] == 2 and t["summary"]["fraud_events"] == 1 and t["summary"]["review"] == 1
    # online card payments pick the online context; a defaulted feature reads n/a (None), never 0
    d = c.get(f"/api/runs/{rid}/entities/device/500").json()
    first = d["events"][0]
    assert first["context"] == "online_card" and first["ctx"]["device_id"] == "500" and first["ctx"]["ip_v4"] == "10.0.0.1"
    tr = c.get(f"/api/runs/{rid}/entities/customer/9").json()
    transfer_row = [e for e in tr["events"] if e["event_id"] == "6"][0]
    assert transfer_row["context"] == "transfer"
    # paging: the summary counts everything, the page holds `limit`
    p = c.get(f"/api/runs/{rid}/entities/device/500", params={"limit": 3}).json()
    assert len(p["events"]) == 3 and p["total"] == 7 and p["truncated"] and p["summary"]["events"] == 7
    p2 = c.get(f"/api/runs/{rid}/entities/device/500", params={"limit": 3, "offset": 6}).json()
    assert _ids(p2["events"]) == ["10"] and not p2["truncated"]
    # an entity without measured events still answers
    none = c.get(f"/api/runs/{rid}/entities/payee/999").json()
    assert none["events"] == [] and "no measured events" in none["note"]
    assert c.get(f"/api/runs/{rid}/entities/ip/10.0.0.1").json()["total"] == 2


def test_campaigns_count_stops_and_story(tmp_path):
    c = _client(tmp_path)
    rid = _run(tmp_path / "runs")
    camps = {x["campaign_id"]: x for x in c.get(f"/api/runs/{rid}/campaigns").json()["campaigns"]}
    ct = camps["42"]
    assert ct["events"] == 2 and ct["declined"] == 2 and ct["stopped"] == 2 and ct["approved_before_first_stop"] == 0
    assert ct["usd_paid"] == 0.0 and ct["usd_total"] == 300.0
    app = camps["43"]          # a reviewed transfer is held (review_holds: TRANSFER)
    assert app["stopped"] == 1 and app["usd_paid"] == 0.0
    ev = c.get(f"/api/runs/{rid}/campaigns/42").json()["events"]
    assert _ids(ev) == ["3", "4"]
