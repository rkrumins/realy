"""GKE machine catalog and node allocatable arithmetic (what a pod can actually request)."""
from __future__ import annotations

from dataclasses import dataclass

# Per-node overhead of GKE system DaemonSets (kube-proxy / dataplane, logging, metrics, CSI, ...).
DAEMONSET_CPU = 0.4
DAEMONSET_MEM_GIB = 0.8


@dataclass(frozen=True)
class Machine:
    name: str
    vcpu: int
    mem_gib: float
    gpus: int = 0
    gpu: str = ""
    local_ssd_gb: int = 0
    note: str = ""

    @property
    def alloc_cpu(self) -> float:
        """GKE reserves 6% of the 1st core, 1% of the 2nd, 0.5% of the 3rd-4th, 0.25% above."""
        c = self.vcpu
        reserved = 0.06 + (0.01 if c > 1 else 0) + 0.005 * max(0, min(c, 4) - 2) + 0.0025 * max(0, c - 4)
        return c - reserved - DAEMONSET_CPU

    @property
    def alloc_mem_gib(self) -> float:
        """25% of the first 4 GiB, 20% of the next 4, 10% of the next 8, 6% of the next 112, 2% above,
        plus the 100 MiB eviction threshold."""
        m, left, reserved = self.mem_gib, self.mem_gib, 0.0
        for size, frac in ((4, 0.25), (4, 0.20), (8, 0.10), (112, 0.06), (1e9, 0.02)):
            take = min(left, size)
            reserved += take * frac
            left -= take
            if left <= 0:
                break
        return m - reserved - 0.1 - DAEMONSET_MEM_GIB


GKE: dict[str, Machine] = {m.name: m for m in (
    Machine("n2-standard-8", 8, 32, note="Kafka brokers (pd-ssd), ClickHouse"),
    Machine("n2-highmem-8", 8, 64, local_ssd_gb=375, note="Flink TaskManagers (RocksDB on local NVMe)"),
    Machine("c3-standard-8", 8, 32, note="scoring gateway, bench driver"),
    Machine("c3-standard-22", 22, 88, note="bench driver above ~40k TPS"),
    Machine("g2-standard-8", 8, 32, gpus=1, gpu="nvidia-l4", note="Triton on one L4"),
)}

# Node pool -> machine type (docs/k8s-l4.md); ClickHouse gets its own pool so merges never steal CPU
# from the hot path.
POOLS: dict[str, str] = {
    "realy-kafka": "n2-standard-8",
    "realy-flink": "n2-highmem-8",
    "realy-serving": "c3-standard-8",
    "realy-gpu": "g2-standard-8",
    "realy-clickhouse": "n2-standard-8",
    "realy-driver": "c3-standard-8",
}
