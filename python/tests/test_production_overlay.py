"""The production overlay (deploy/k8s/overlays/production) is complete and consistent: every file it names
exists, every environment variable it sets is one the code reads (a typo would be silently ignored), every
secret file it points at is in the secret's shape, and the gateway is deployed the way the guarantees in
docs/production-pipeline.md need."""
from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
OVERLAY = ROOT / "deploy" / "k8s" / "overlays" / "production"


def _docs(path: Path) -> list[dict]:
    return [d for d in yaml.safe_load_all(path.read_text()) if d]


def _env_file() -> dict[str, str]:
    out = {}
    for line in (OVERLAY / "config" / "pipeline.env").read_text().splitlines():
        if line.strip() and not line.lstrip().startswith("#"):
            k, _, v = line.partition("=")
            out[k.strip()] = v.strip()
    return out


def test_every_file_the_kustomization_names_exists():
    k = yaml.safe_load((OVERLAY / "kustomization.yaml").read_text())
    for r in k["resources"]:
        assert (OVERLAY / r).resolve().exists(), r
    for g in k["configMapGenerator"]:
        for f in g.get("files", []) + g.get("envs", []):
            assert (OVERLAY / f).exists(), f


def test_every_variable_set_is_one_the_code_reads():
    known = set(re.findall(r"^\| `([A-Z0-9_]+)` \|", (ROOT / "docs" / "reference" / "environment.md").read_text(), re.M))
    unknown = sorted(set(_env_file()) - known)
    assert not unknown, f"not read by any service (typo?): {unknown}"


def test_secret_files_are_in_the_secret_and_the_profile_is_production():
    env = _env_file()
    assert env["REALY_PROFILE"] == "production" and env["CONTROL_ENABLED"] == "false"
    secret = _docs(OVERLAY / "secrets.example.yaml")[0]
    keys = set(secret["stringData"])
    for k, v in env.items():
        if k.endswith("_FILE") and v.startswith("/secrets/"):
            assert v.removeprefix("/secrets/") in keys, (k, v)


def test_the_gateway_is_deployed_for_its_guarantees():
    docs = {d["kind"]: d for d in _docs(OVERLAY / "gateway.yaml")}
    sts = docs["StatefulSet"]
    c = sts["spec"]["template"]["spec"]["containers"][0]
    assert c["readinessProbe"]["httpGet"]["path"] == "/ready" and c["livenessProbe"]["httpGet"]["path"] == "/health"
    spool = next(v for v in c["volumeMounts"] if v["name"] == "spool")
    assert _env_file()["AUDIT_SPOOL"].startswith(spool["mountPath"])
    assert sts["spec"]["volumeClaimTemplates"][0]["metadata"]["name"] == "spool"     # a persistent spool per pod
    drain_s = int(_env_file()["DRAIN_TIMEOUT_MS"]) / 1000
    assert sts["spec"]["template"]["spec"]["terminationGracePeriodSeconds"] > drain_s
    assert docs["HorizontalPodAutoscaler"]["spec"]["scaleTargetRef"]["kind"] == "StatefulSet"
    # scoring config: valid for the scoring core, nothing benchmark-only
    import json
    cfg = json.loads((OVERLAY / "config" / "scoring.json").read_text())
    assert not cfg.get("echo") and cfg.get("featureSource") != "mock"
    assert "featureLookupDelay" not in cfg and "modelExtraDelay" not in cfg


def test_flink_assembles_into_the_topic_the_gateway_scores():
    job = _docs(OVERLAY / "flink.yaml")[0]["spec"]["job"]["args"]
    topic = job[job.index("--assembled-topic") + 1]
    assert _env_file()["INGRESS_TOPIC"] == topic
    from realy.bindings import pipeline
    assert topic == pipeline().topics["assembled"]
    contracts = job[job.index("--assemble") + 1].split(",")
    assert set(contracts) <= set(pipeline().transactions)
