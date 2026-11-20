"""Deterministic hand-set proxy model (single sigmoid layer) used until `realy train` has produced a
trained MLP. Same JSON format the mock model server and the verifier load."""
from __future__ import annotations

import json
from pathlib import Path

from ..features import load_schema
from ..scenario import REPO_ROOT

# feature -> (scale, weight): contribution = weight * value / scale
WEIGHTS: dict[str, tuple[float, float]] = {
    "card_small_cnp_count_10m": (1.0, 1.2),
    "card_count_5m": (2.0, 0.6),
    "card_distinct_merchants_10m": (2.0, 0.5),
    "card_geo_speed_kmh": (500.0, 0.8),
    "cust_device_novel": (1.0, 1.5),
    "cust_ip_country_novel": (1.0, 1.5),
    "dev_distinct_cards_1h": (1.0, 1.0),
    "payee_distinct_senders_24h": (2.0, 0.8),
    "cust_payee_novel": (1.0, 0.8),
    "card_atm_count_1h": (1.0, 0.9),
    "card_contactless_count_30m": (2.0, 0.5),
    "is_foreign": (1.0, 0.8),
    "amount_to_avg_ratio": (5.0, 0.6),
    "merch_risk_tier": (1.0, 0.4),
    "cust_tenure_days": (365.0, -0.5),
    "is_cnp": (1.0, 0.5),
    "credit_utilisation_24h": (0.3, 0.5),
}
BIAS = -5.0


def build() -> dict:
    schema = load_schema()
    n = len(schema.features)
    std = [WEIGHTS.get(f.name, (1.0, 0.0))[0] for f in schema.features]
    w = [WEIGHTS.get(f.name, (1.0, 0.0))[1] for f in schema.features]
    return {"name": "fraud_mlp", "version": "default-v1", "n_features": n,
            "feature_names": schema.names, "feature_schema_version": schema.version,
            "mean": [0.0] * n, "std": std,
            "layers": [{"W": [w], "b": [BIAS], "activation": "sigmoid"}]}


def write(path: Path = REPO_ROOT / "models" / "proxy" / "default.weights.json") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(build(), indent=1))
    return path


if __name__ == "__main__":
    print(write())
