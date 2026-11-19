"""Event column chunks, EventBatch and protobuf serialization.

Internally every producer (legit behaviour engine, fraud campaigns) emits a *chunk*: a dict of
equal-length numpy arrays using the INTERNAL schema below (country/currency as indices). Chunks
are concatenated, sorted by event time, assigned event ids, then serialized.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

from ..payments.v1 import label_pb2, transaction_pb2
from ..world.geo import geo

# internal column -> (dtype, default)
INTERNAL: dict[str, tuple[np.dtype, object]] = {
    "event_time_us": (np.dtype(np.int64), 0),
    "payment_type": (np.dtype(np.int8), 1),
    "customer_id": (np.dtype(np.uint64), 0),
    "account_id": (np.dtype(np.uint64), 0),
    "card_id": (np.dtype(np.uint64), 0),
    "card_type": (np.dtype(np.int8), 0),
    "amount_minor": (np.dtype(np.int64), 0),
    "currency_idx": (np.dtype(np.int16), 0),
    "amount_usd_minor": (np.dtype(np.int64), 0),
    "merchant_id": (np.dtype(np.uint64), 0),
    "mcc": (np.dtype(np.uint32), 0),
    "merchant_country_idx": (np.dtype(np.int16), -1),
    "terminal_id": (np.dtype(np.uint64), 0),
    "channel": (np.dtype(np.int8), 0),
    "entry_mode": (np.dtype(np.int8), 0),
    "card_present": (np.dtype(bool), False),
    "lat": (np.dtype(np.float64), 0.0),
    "lon": (np.dtype(np.float64), 0.0),
    "device_id": (np.dtype(np.uint64), 0),
    "ip_v4": (np.dtype(np.uint32), 0),
    "ip_country_idx": (np.dtype(np.int16), -1),
    "ip_asn": (np.dtype(np.uint32), 0),
    "payee_id": (np.dtype(np.uint64), 0),
    "cvv_match": (np.dtype(bool), True),
    "avs_match": (np.dtype(bool), True),
    "recurring": (np.dtype(bool), False),
    "credit_limit_usd": (np.dtype(np.uint32), 0),
    # ground truth (never serialized into the event)
    "is_fraud": (np.dtype(bool), False),
    "fraud_type": (np.dtype(np.int8), 0),
    "campaign_id": (np.dtype(np.uint64), 0),
    "label_delay_us": (np.dtype(np.int64), -1),   # -1 => no label is ever published
    "label_is_fraud": (np.dtype(bool), False),     # observed (noisy) label value
    "label_type": (np.dtype(np.int8), 0),
    "label_source": (np.dtype(np.int8), 0),
    "order": (np.dtype(np.int64), 0),              # tie-breaker for deterministic sorting
}

# Output column names (= proto field names) for EventBatch.columns
EVENT_FIELDS = [f.name for f in transaction_pb2.TransactionEvent.DESCRIPTOR.fields]
TRUTH_FIELDS = ["is_fraud", "fraud_type", "campaign_id"]

# realy.payments.v1 enums
PT_PURCHASE, PT_ATM, PT_TRANSFER, PT_REFUND = 1, 2, 3, 4
CH_POS, CH_ECOM, CH_ATM, CH_MOTO, CH_IN_APP, CH_ONLINE_BANKING = 1, 2, 3, 4, 5, 6
EM_CHIP, EM_CONTACTLESS, EM_MAGSTRIPE, EM_MANUAL, EM_WALLET, EM_3DS, EM_NON3DS, EM_COF, EM_BANK_APP = 1, 2, 3, 4, 5, 6, 7, 8, 9
LS_CHARGEBACK, LS_CUSTOMER_REPORT, LS_INVESTIGATION = 1, 2, 3


def make_chunk(n: int, **cols) -> dict[str, np.ndarray]:
    out: dict[str, np.ndarray] = {}
    for name, (dt, default) in INTERNAL.items():
        if name in cols:
            v = np.asarray(cols.pop(name))
            out[name] = (np.full(n, v, dtype=dt) if v.ndim == 0 else v.astype(dt, copy=False))
        else:
            out[name] = np.full(n, default, dtype=dt)
    if cols:
        raise KeyError(f"unknown chunk columns: {sorted(cols)}")
    return out


def concat_chunks(chunks: list[dict[str, np.ndarray]]) -> dict[str, np.ndarray]:
    chunks = [c for c in chunks if c["event_time_us"].shape[0]]
    if not chunks:
        return make_chunk(0)
    return {k: np.concatenate([c[k] for c in chunks]) for k in INTERNAL}


def take(chunk: dict[str, np.ndarray], idx: np.ndarray) -> dict[str, np.ndarray]:
    return {k: v[idx] for k, v in chunk.items()}


@dataclass
class EventBatch:
    """Serialized events of one `Generator.generate` call, ordered by event time.

    payloads[i] is a serialized realy.payments.v1.TransactionEvent; keys[i] is the Kafka key
    (customer_id as decimal ASCII). `columns` holds every TransactionEvent field (proto field names,
    enums as int codes, countries/currency as ISO strings) plus ground truth `is_fraud`, `fraud_type`
    (FraudType enum) and `campaign_id`. `labels` are (label_time_us, serialized FraudLabel) for
    events of this batch that will ever be labelled; publish each once simulation time passes
    label_time_us.
    """
    payloads: list[bytes]
    keys: list[bytes]
    event_ids: np.ndarray
    event_time_us: np.ndarray
    columns: dict[str, np.ndarray]
    labels: list[tuple[int, bytes]] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.payloads)


def finalize(chunk: dict[str, np.ndarray], gen_id: int, seq0: int) -> EventBatch:
    """Sort a merged chunk by (event_time, order), assign event ids, serialize."""
    g = geo()
    n = chunk["event_time_us"].shape[0]
    idx = np.lexsort((chunk["order"], chunk["event_time_us"]))
    c = take(chunk, idx)
    seq = np.arange(seq0, seq0 + n, dtype=np.uint64)
    event_ids = (np.uint64(gen_id) << np.uint64(48)) | seq
    countries = np.asarray(g.countries + [""], dtype="U2")  # index -1 -> ""
    currencies = np.asarray(g.currency, dtype="U3")
    merchant_country = countries[c["merchant_country_idx"]]
    ip_country = countries[c["ip_country_idx"]]
    currency = currencies[c["currency_idx"]]
    cols: dict[str, np.ndarray] = {
        "event_id": event_ids, "event_time_us": c["event_time_us"], "payment_type": c["payment_type"].astype(np.int32),
        "customer_id": c["customer_id"], "account_id": c["account_id"], "card_id": c["card_id"],
        "card_type": c["card_type"].astype(np.int32), "amount_minor": c["amount_minor"], "currency": currency,
        "amount_usd_minor": c["amount_usd_minor"], "merchant_id": c["merchant_id"], "mcc": c["mcc"],
        "merchant_country": merchant_country, "terminal_id": c["terminal_id"], "channel": c["channel"].astype(np.int32),
        "entry_mode": c["entry_mode"].astype(np.int32), "card_present": c["card_present"], "lat": c["lat"], "lon": c["lon"],
        "device_id": c["device_id"], "ip_v4": c["ip_v4"], "ip_country": ip_country, "ip_asn": c["ip_asn"],
        "payee_id": c["payee_id"], "cvv_match": c["cvv_match"], "avs_match": c["avs_match"], "recurring": c["recurring"],
        "credit_limit_usd": c["credit_limit_usd"],
        "is_fraud": c["is_fraud"], "fraud_type": c["fraud_type"].astype(np.int32), "campaign_id": c["campaign_id"],
    }
    payloads = serialize_events(cols, n)
    # what the institution LEARNS, and when (ground truth above is what really happened): the event log
    # keeps both so training can use labels as known at training time (realy train --labels observed)
    cols.update(label_delay_us=c["label_delay_us"], label_is_fraud=c["label_is_fraud"],
                label_type=c["label_type"].astype(np.int32), label_source=c["label_source"].astype(np.int32))
    keys = [str(x).encode() for x in cols["customer_id"].tolist()]
    labels: list[tuple[int, bytes]] = []
    lab = np.flatnonzero(c["label_delay_us"] >= 0)
    if lab.shape[0]:
        FL = label_pb2.FraudLabel
        lt = (c["event_time_us"][lab] + c["label_delay_us"][lab]).tolist()
        for j, i in enumerate(lab.tolist()):
            labels.append((lt[j], FL(
                event_id=int(event_ids[i]), label_time_us=lt[j], event_time_us=int(c["event_time_us"][i]),
                is_fraud=bool(c["label_is_fraud"][i]), fraud_type=int(c["label_type"][i]),
                campaign_id=int(c["campaign_id"][i]), source=int(c["label_source"][i]),
                customer_id=int(c["customer_id"][i]), card_id=int(c["card_id"][i]),
                amount_usd_minor=int(c["amount_usd_minor"][i])).SerializeToString()))
    return EventBatch(payloads=payloads, keys=keys, event_ids=event_ids, event_time_us=c["event_time_us"],
                      columns=cols, labels=labels)


def serialize_events(cols: dict[str, np.ndarray], n: int) -> list[bytes]:
    lists = {k: (cols[k].tolist()) for k in EVENT_FIELDS}
    TE = transaction_pb2.TransactionEvent
    keys = EVENT_FIELDS
    vals = [lists[k] for k in keys]
    return [TE(**dict(zip(keys, row))).SerializeToString() for row in zip(*vals)] if n else []
