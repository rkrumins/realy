"""Driver clock domain. Emitter and collector run on the same host, so end-to-end latency
(t_collected - t_intended) is measured in a single clock domain (CLOCK_REALTIME, microseconds)."""
from __future__ import annotations

import os
import socket
import time
import zlib


def now_us() -> int:
    return time.time_ns() // 1000


DRIVER_DOMAIN = zlib.crc32(f"{socket.gethostname()}/driver".encode()) & 0xFFFFFFFF


def process_domain() -> int:
    return zlib.crc32(f"{socket.gethostname()}/{os.getpid()}".encode()) & 0xFFFFFFFF
