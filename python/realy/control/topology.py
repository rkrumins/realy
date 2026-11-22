"""Live pipeline topology map: nodes/edges per topology (from the steps in contracts/topologies.yaml), each
annotated with the bench.metrics source that feeds it, so the UI can paint events/s and p99 on every hop
from the 1 s snapshots it already receives. A metric ref is {"source": <emitter|collector|gateway|model|
flink>, "counter": name} (rate = sum of that counter across instances) or {"source", "stage"} (rate =
stage sample count, p99 = max p99)."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from ..topologies import FRESHNESS_SLO_MS, SLO_STAGES, graph, names  # noqa: F401 (re-exported)

router = APIRouter(tags=["topology"])
TOPOLOGIES = names()


def build(topology: str, flows: bool = False) -> dict[str, Any]:
    """The map of `topology`, built from its steps in contracts/topologies.yaml; `flows`: the run has
    multi-message transactions (Flink assembles them first; on gateway-kafka the gateway consumes the
    assembled topic)."""
    if topology not in TOPOLOGIES:
        raise HTTPException(400, f"topology must be one of {TOPOLOGIES}")
    return graph(topology, flows)


@router.get("/api/topology")
def topology(topology: str = "gateway-kafka", flows: bool = False) -> dict[str, Any]:
    return build(topology, flows)
