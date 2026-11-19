"""Console backend additions: auth, explorer, topology, docs, environment probes, SPA fallback."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from realy.control import app as app_mod
from realy.control.topology import TOPOLOGIES, build


def make_client(tmp_path: Path, **kw) -> TestClient:
    runs = tmp_path / "runs"
    runs.mkdir(exist_ok=True)
    a = app_mod.create_app(runs_dir=runs, kafka=False, background=False, run_fn=lambda *a, **k: {},
                           publish_fn=lambda msgs: None, **kw)
    return TestClient(a)


def test_auth_off_by_default(tmp_path, monkeypatch):
    monkeypatch.delenv("REALY_CONSOLE_TOKEN", raising=False)
    c = make_client(tmp_path)
    assert c.get("/api/auth/me").json() == {"auth_required": False, "authenticated": True}
    assert c.get("/api/runs").status_code == 200


def test_auth_token_cookie_and_bearer(tmp_path, monkeypatch):
    monkeypatch.setenv("REALY_CONSOLE_TOKEN", "s3cret-token")
    c = make_client(tmp_path)
    assert c.get("/api/runs").status_code == 401
    assert c.get("/api/health").status_code == 200
    assert c.get("/api/auth/me").json() == {"auth_required": True, "authenticated": False}
    assert c.post("/api/auth/login", json={"token": "wrong"}).status_code == 401
    r = c.post("/api/auth/login", json={"token": "s3cret-token"})
    assert r.status_code == 200 and "realy_session" in r.cookies
    assert "s3cret-token" not in r.headers.get("set-cookie", "")
    assert c.get("/api/runs").status_code == 200
    assert c.get("/api/auth/me").json()["authenticated"] is True
    c.post("/api/auth/logout")
    c.cookies.clear()
    assert c.get("/api/runs").status_code == 401
    assert c.get("/api/runs", headers={"Authorization": "Bearer s3cret-token"}).status_code == 200
    with pytest.raises(Exception):
        with c.websocket_connect("/api/live") as ws:
            ws.receive_json()


@pytest.mark.parametrize("topo", TOPOLOGIES)
def test_topology_edges_reference_nodes(topo):
    t = build(topo)
    ids = {n["id"] for n in t["nodes"]}
    assert all(e["from"] in ids and e["to"] in ids for e in t["edges"])
    for n in t["nodes"]:
        for ref in (n["rate"], n["p99"]):
            assert ref is None or ref["source"] in ("emitter", "collector", "gateway", "model", "flink")


def _fixture_run(runs: Path) -> str:
    rid = "fixture-run"
    d = runs / rid
    d.mkdir(parents=True)
    n = 6
    ids = np.arange(1, n + 1, dtype=np.uint64)
    pq.write_table(pa.table({
        "event_id": ids, "event_time_us": np.arange(n, dtype=np.int64) * 1_000_000 + 1_800_000_000_000_000,
        "payment_type": np.ones(n, np.int32), "customer_id": np.array([7, 7, 8, 8, 9, 9], np.uint64),
        "account_id": np.ones(n, np.uint64), "card_id": np.array([70, 70, 80, 80, 90, 90], np.uint64),
        "card_type": np.ones(n, np.int32), "amount_minor": np.full(n, 150), "currency": ["GBP"] * n,
        "amount_usd_minor": np.full(n, 150), "merchant_id": np.ones(n, np.uint64), "mcc": np.full(n, 5411, np.uint32),
        "merchant_country": ["GB"] * n, "terminal_id": np.zeros(n, np.uint64), "channel": np.full(n, 2, np.int32),
        "entry_mode": np.full(n, 6, np.int32), "card_present": np.zeros(n, bool), "lat": np.zeros(n), "lon": np.zeros(n),
        "device_id": np.zeros(n, np.uint64), "ip_v4": np.zeros(n, np.uint32), "ip_country": [""] * n,
        "ip_asn": np.zeros(n, np.uint32), "payee_id": np.zeros(n, np.uint64), "cvv_match": np.ones(n, bool),
        "avs_match": np.ones(n, bool), "recurring": np.zeros(n, bool), "credit_limit_usd": np.zeros(n, np.uint32),
        "is_fraud": np.array([0, 0, 1, 1, 0, 0], bool), "fraud_type": np.array([0, 0, 1, 1, 0, 0], np.int32),
        "campaign_id": np.array([0, 0, 42, 42, 0, 0], np.uint64), "t_intended_us": np.arange(n, dtype=np.int64),
        "backfill": np.zeros(n, bool)}), d / "events-0.parquet")
    pq.write_table(pa.table({
        "event_id": ids, "revision": np.zeros(n, np.uint8), "outcome": np.array([1, 1, 2, 3, 1, 1], np.int8),
        "score": np.full(n, 0.1, np.float32), "path": np.zeros(n, np.int8), "degraded": np.zeros(n, bool),
        "ingress_mode": np.full(n, 3, np.int8), "t_intended_us": np.full(n, 1000, np.int64),
        "t_sent_us": np.full(n, 1100, np.int64), "t_ingest_us": np.full(n, 3000, np.int64),
        "t_decision_us": np.full(n, 15000, np.int64), "t_collected_us": np.full(n, 21000, np.int64),
        "reason_codes": ["", "", "CARD_TESTING", "CARD_TESTING", "", ""],
        "rule_hits": ["", "", "CARD_TESTING", "CARD_TESTING", "", ""], "model_version": ["v1"] * n,
        "span_stage": pa.array([[10, 30, 40, 70]] * n, pa.list_(pa.int16())),
        "span_attr": pa.array([[3, 7, 0, 1]] * n, pa.list_(pa.int32())),
        "span_domain": pa.array([[1, 1, 1, 1]] * n, pa.list_(pa.uint32())),
        "span_start_us": pa.array([[3000, 3100, 6000, 15000]] * n, pa.list_(pa.int64())),
        "span_dur_us": pa.array([[0, 2900, 8800, 0]] * n, pa.list_(pa.int32()))}), d / "decisions-0.parquet")
    from realy.features import load_schema
    nf = len(load_schema().features)
    pq.write_table(pa.table({"event_id": ids, "values": pa.array([[0.0] * nf] * n, pa.list_(pa.float32())),
                             "missing": pa.array([b"\x01"] * n, pa.binary())}), d / "features-0.parquet")
    (d / "scenario.json").write_text(json.dumps({"name": "x"}))
    (d / "manifest.json").write_text(json.dumps({"run_id": rid, "scenario": "x", "topology": "gateway-kafka"}))
    return rid


def test_explorer_search_decision_campaigns(tmp_path):
    c = make_client(tmp_path)
    rid = _fixture_run(tmp_path / "runs")
    rows = c.get(f"/api/runs/{rid}/search", params={"q": "7"}).json()["results"]
    assert {r["event_id"] for r in rows} == {"1", "2"}  # customer 7 (no event/card/campaign id 7)
    assert c.get(f"/api/runs/{rid}/search", params={"q": "fraud:card_testing"}).json()["results"][0]["fraud_type"] == "card_testing"
    assert len(c.get(f"/api/runs/{rid}/search", params={"q": "outcome:DECLINE"}).json()["results"]) == 1
    d = c.get(f"/api/runs/{rid}/decisions/3").json()
    assert d["decision"]["outcome"] == "REVIEW" and d["decision"]["rule_hits"] == ["CARD_TESTING"]
    assert [s["stage_name"] for s in d["spans"]] == ["INGEST", "FEATURE_LOOKUP", "MODEL_CALL", "DECISION"]
    assert d["timing"]["e2e_ms"] == 20.0 and d["label"]["fraud_type"] == "card_testing"
    assert d["features"][0]["missing"] is True and d["event"]["channel"] == "ECOM"
    camps = c.get(f"/api/runs/{rid}/campaigns").json()["campaigns"]
    assert camps[0]["campaign_id"] == "42" and camps[0]["events"] == 2 and camps[0]["approved_before_first_decline"] == 1
    assert c.get(f"/api/runs/{rid}/decisions/999").status_code == 404
    assert c.get("/api/runs/../etc/search").status_code in (400, 404)


def test_docs_index_page_and_traversal(tmp_path):
    c = make_client(tmp_path)
    idx = c.get("/api/docs").json()
    paths = [i["path"] for s in idx["sections"] for i in s["items"]]
    assert "architecture.md" in paths
    page = c.get("/api/docs/page", params={"path": "architecture.md"}).json()
    assert page["title"] and "#" in page["markdown"]
    assert c.get("/api/docs/page", params={"path": "../../etc/passwd"}).status_code == 400
    assert "properties" in c.get("/api/reference/scenario-schema").json()
    assert c.get("/api/openapi.json").json()["info"]["title"] == "realy console"


def test_env_reports_down_components_quickly(tmp_path, monkeypatch):
    for var, val in (("KAFKA_BOOTSTRAP", "127.0.0.1:1"), ("REDIS_URI", "redis://127.0.0.1:1"),
                     ("CLICKHOUSE_URL", "http://127.0.0.1:1"), ("FLINK_REST", "http://127.0.0.1:1"),
                     ("GATEWAY_ADMIN", "http://127.0.0.1:1"), ("MODEL_ADMIN", "http://127.0.0.1:1")):
        monkeypatch.setenv(var, val)
    c = make_client(tmp_path)
    comps = {x["id"]: x for x in c.get("/api/env").json()["components"]}
    assert comps["redis"]["status"] == "down" and comps["gateway"]["status"] == "down"
    assert comps["console"]["status"] == "up"


def test_spa_fallback(tmp_path, monkeypatch):
    static = tmp_path / "static"
    (static / "assets").mkdir(parents=True)
    (static / "index.html").write_text("<html>console</html>")
    (static / "assets" / "app.js").write_text("console.log(1)")
    monkeypatch.setattr(app_mod, "STATIC", static)
    c = make_client(tmp_path)
    assert "console" in c.get("/runs/some-run").text
    assert c.get("/assets/app.js").text == "console.log(1)"
    assert c.get("/api/nope").status_code == 404
