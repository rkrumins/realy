"""The rich blocks of a payment (TransactionEvent fields 40-47): its card, account, merchant, device, payee,
authentication, basket and attributes as structured sub-messages, drawn only when a scenario asks for them
(`traffic.event_blocks: [card_info, ...]`).

Every value comes from the world (a merchant's risk tier and city, a customer's home country and tenure, a
payee's country, a device's OS) or from the payment itself (its card, channel, 3-D Secure result), plus
draws hashed from the event id (like the authorisation signals: no random stream moves, so the same
payments draw the same blocks in every run). Fraud skews the context the way it looks in practice: a card
already reported blocked, a foreign BIN, an emulator, a first payment to a payee whose name does not match,
several authentication attempts, gift cards in the basket.

The event log keeps every block leaf as a column named by its dotted path (`card_info.status`), lists and
maps as Arrow list / map columns; `ARROW_TYPES` fixes their types so every batch writes the same schema.
"""
from __future__ import annotations

from typing import Any

import numpy as np
import pyarrow as pa

from ..challenge import _u01

BLOCKS = ("card_info", "account_info", "merchant_info", "device_info", "payee_info", "authentication", "items",
          "attributes")
# realy.payments.v1 enums (transaction.proto)
CS_ACTIVE, CS_BLOCKED, CS_LOST, CS_STOLEN = 1, 2, 3, 4
AT_CURRENT, AT_CREDIT = 1, 3
TDS_NONE = 0
CH_POS, CH_ECOM, CH_ATM, CH_MOTO, CH_IN_APP, CH_ONLINE_BANKING = 1, 2, 3, 4, 5, 6
EM_WALLET = 5
OS = np.asarray(["ANDROID", "IOS", "WEB"])
CITIES = np.asarray(["London", "Manchester", "Paris", "Berlin", "Madrid", "Dublin", "Amsterdam", "Lisbon", "Rome",
                     "Warsaw", "New York", "Toronto", "Lagos", "Singapore"])
CATEGORIES = np.asarray(["ELECTRONICS", "GROCERY", "APPAREL", "HOME", "TRAVEL", "GIFT_CARD"])

ARROW_TYPES: dict[str, pa.DataType] = {
    "items": pa.list_(pa.struct([("sku", pa.string()), ("qty", pa.int64()), ("amount_usd_minor", pa.int64()),
                                 ("category", pa.string())])),
    "account_info.flags": pa.list_(pa.string()),
    "attributes": pa.map_(pa.string(), pa.string()),
}


class Blocks:
    def __init__(self, world, enabled: list[str], seed: int) -> None:
        bad = sorted(set(enabled) - set(BLOCKS))
        if bad:
            raise ValueError(f"traffic.event_blocks: unknown block(s) {bad} (one of {list(BLOCKS)})")
        self.enabled = set(enabled)
        self.seed = int(seed)
        from ..world.geo import geo
        self.countries = np.asarray(geo().countries + [""], dtype="U2")
        cust = world.customer_profile_arrays()
        self.cust_ids, self.cust_country = cust["customer_id"], cust["home_country_idx"].astype(np.int64)
        self.cust_tenure = cust["tenure_days"].astype(np.int64)
        self.world = world
        m = world.merchants
        self.m_ids, self.m_tier = np.asarray(m["merchant_id"], np.uint64), np.asarray(m["risk_tier"], np.int64)
        self.m_city = np.asarray(m["city"]) if "city" in m else None
        p = world.payees
        self.p_ids, self.p_country = np.asarray(p["payee_id"], np.uint64), np.asarray(p["country"], np.int64)
        d = world.devices
        self.d_ids, self.d_os = np.asarray(d["device_id"], np.uint64), np.asarray(d["os"], np.int64)

    def _u(self, ids: np.ndarray, salt: int) -> np.ndarray:
        return _u01(ids, self.seed * 1_000_003 + salt)

    @staticmethod
    def _rows(ref: np.ndarray, q: np.ndarray) -> np.ndarray:
        pos = np.clip(np.searchsorted(ref, q), 0, max(0, ref.size - 1))
        return np.where((ref.size > 0) & (ref[pos] == q) & (q != 0), pos, -1)

    def draw(self, c: dict[str, np.ndarray], cols: dict[str, np.ndarray], ids: np.ndarray) -> dict[str, Any]:
        """Dotted block columns for one batch (`cols`: the flat event columns)."""
        n = ids.shape[0]
        fraud = c["is_fraud"].astype(bool)
        out: dict[str, Any] = {}
        e = self.enabled
        if "card_info" in e:
            card = cols["card_id"]
            u = self._u(ids, 1)
            crow = self._rows(np.asarray(self.cust_ids, np.uint64), np.asarray(cols["customer_id"], np.uint64))
            home = np.where(crow >= 0, self.cust_country[np.maximum(crow, 0)], -1)
            # a fraudster's card is more often foreign to where it is used; a few attempts hit a reported card
            foreign = u < np.where(fraud, 0.35, 0.03)
            bin_country = np.where(foreign, (home + 1 + (u * 7).astype(np.int64)) % max(1, self.countries.size - 1), home)
            status = np.full(n, CS_ACTIVE, np.int32)
            u2 = self._u(ids, 2)
            status[(u2 < np.where(fraud, 0.06, 0.0002))] = CS_BLOCKED
            status[(u2 < np.where(fraud, 0.02, 0.0))] = CS_STOLEN
            tok = cols["entry_mode"] == EM_WALLET
            out.update({
                "card_info.id": card,
                "card_info.bin": np.char.add(np.where(card % 2 == 0, "4", "5"),
                                             np.char.zfill((card % 99_999).astype(str), 5)),
                "card_info.bin_country": self.countries[bin_country],
                "card_info.status": np.where(card != 0, status, 0).astype(np.int32),
                "card_info.network": np.where(card == 0, "", np.where(card % 2 == 0, "VISA", "MASTERCARD")),
                "card_info.tokenized": tok,
                "card_info.wallet": np.where(tok, np.where(u > 0.5, "APPLE_PAY", "GOOGLE_PAY"), ""),
                "card_info.issued_at_us": np.where(card != 0, cols["event_time_us"]
                                                   - (30 + (u2 * 1500).astype(np.int64)) * 86_400_000_000, 0),
            })
        if "account_info" in e:
            u = self._u(ids, 3)
            crow = self._rows(np.asarray(self.cust_ids, np.uint64), np.asarray(cols["customer_id"], np.uint64))
            bal = np.round(np.exp(7 + 2.0 * u) * 100 - np.where(fraud, 50_000, 2_000)).astype(np.int64)
            aml = self._u(ids, 4) < np.where(fraud, 0.03, 0.001)
            flags = np.empty(n, dtype=object)
            for i in range(n):
                f = []
                if bal[i] < 0:
                    f.append("OVERDRAWN")
                if aml[i]:
                    f.append("AML_HOLD")
                flags[i] = f
            out.update({
                "account_info.id": cols["account_id"],
                "account_info.type": np.where(cols["card_type"] == 2, AT_CREDIT, AT_CURRENT).astype(np.int32),
                "account_info.balance_usd_minor": bal,
                "account_info.age_days": np.where(crow >= 0, self.cust_tenure[np.maximum(crow, 0)], 0).astype(np.int64),
                "account_info.flags": flags,
            })
        if "merchant_info" in e:
            mid = np.asarray(cols["merchant_id"], np.uint64)
            mrow = self._rows(self.m_ids, mid)
            city = (self.m_city[np.maximum(mrow, 0)] if self.m_city is not None and self.m_city.dtype.kind in "US"
                    else CITIES[(mid % np.uint64(CITIES.size)).astype(np.int64)])
            out.update({
                "merchant_info.id": cols["merchant_id"],
                "merchant_info.name": np.where(mid != 0, np.char.add("merchant-", mid.astype(str)), ""),
                "merchant_info.mcc": cols["mcc"],
                "merchant_info.address.country": np.where(mid != 0, cols["merchant_country"], ""),
                "merchant_info.address.city": np.where(mid != 0, city.astype(str), ""),
                "merchant_info.address.postcode": np.full(n, "", dtype="U1"),
                "merchant_info.risk_tier": np.where(mrow >= 0, self.m_tier[np.maximum(mrow, 0)], 0).astype(np.int64),
            })
        if "device_info" in e:
            did = np.asarray(cols["device_id"], np.uint64)
            drow = self._rows(self.d_ids, did)
            u = self._u(ids, 5)
            os_ = np.where(drow >= 0, OS[np.clip(self.d_os[np.maximum(drow, 0)], 0, 2)], OS[(u * 2).astype(np.int64)])
            out.update({
                "device_info.id": cols["device_id"],
                "device_info.os": np.where(did != 0, os_, ""),
                "device_info.app_version": np.where(did != 0, "7." + (u * 9).astype(np.int64).astype(str), ""),
                "device_info.emulator": (did != 0) & (self._u(ids, 6) < np.where(fraud, 0.25, 0.003)),
                "device_info.rooted": (did != 0) & (self._u(ids, 7) < np.where(fraud, 0.15, 0.01)),
                "device_info.risk_score": np.where(did != 0, np.where(fraud, 0.4 + 0.6 * u, 0.3 * u * u), 0.0),
            })
        if "payee_info" in e:
            pid = np.asarray(cols["payee_id"], np.uint64)
            prow = self._rows(self.p_ids, pid)
            u = self._u(ids, 8)
            new = (pid != 0) & (u < np.where(fraud, 0.7, 0.08))
            nm = np.where(pid == 0, "", np.where(self._u(ids, 9) < np.where(fraud, 0.4, 0.02), "NO_MATCH",
                                                 np.where(self._u(ids, 10) < 0.05, "CLOSE_MATCH", "MATCH")))
            out.update({
                "payee_info.id": cols["payee_id"],
                "payee_info.bank_country": np.where(prow >= 0, self.countries[np.maximum(self.p_country[np.maximum(prow, 0)], -1)], ""),
                "payee_info.new_payee": new,
                "payee_info.name_match": nm,
            })
        if "authentication" in e:
            remote = np.isin(cols["channel"], (CH_ECOM, CH_IN_APP, CH_ONLINE_BANKING))
            u = self._u(ids, 11)
            att = np.where(remote, 1 + (u < np.where(fraud, 0.5, 0.05)) + (u < np.where(fraud, 0.2, 0.01)), 0)
            out.update({
                "authentication.three_ds": np.where(remote, cols["three_ds"], TDS_NONE).astype(np.int32),
                "authentication.attempts": att.astype(np.int64),
                "authentication.method": np.where(remote, np.where(u > 0.6, "BIOMETRIC", "OTP"), ""),
                "authentication.sca_exempt": remote & (cols["amount_usd_minor"] < 3_000),
            })
        if "items" in e:
            online = np.isin(cols["channel"], (CH_ECOM, CH_IN_APP))
            u = self._u(ids, 12)
            nitems = np.where(online, 1 + (u * 4).astype(np.int64), 0)
            gift = self._u(ids, 13) < np.where(fraud, 0.45, 0.02)
            amt = cols["amount_usd_minor"]
            items = np.empty(n, dtype=object)
            for i in range(n):
                k = int(nitems[i])
                if k == 0:
                    items[i] = []
                    continue
                share = int(amt[i]) // k
                items[i] = [{"sku": f"SKU-{(int(ids[i]) >> (j * 5)) % 9973}", "qty": 1 + ((int(ids[i]) >> (j * 3)) % 3),
                             "amount_usd_minor": share if j < k - 1 else int(amt[i]) - share * (k - 1),
                             "category": "GIFT_CARD" if gift[i] and j == 0 else str(CATEGORIES[(int(ids[i]) >> j) % 5])}
                            for j in range(k)]
            out["items"] = items
        if "attributes" in e:
            ch = cols["channel"]
            attrs = np.empty(n, dtype=object)
            for i in range(n):
                attrs[i] = ({"channel_app": "ios" if int(ids[i]) % 2 else "android"} if int(ch[i]) == CH_IN_APP
                            else {"channel_app": "web"} if int(ch[i]) == CH_ECOM else {})
            out["attributes"] = attrs
        # a block that does not apply to a payment (a transfer's card) is absent from its message, so its
        # columns hold the defaults a decoder reads for it: the event log and the payload agree
        for block, key in PRESENT.items():
            if block not in e:
                continue
            absent = ~np.asarray(out[key] != 0) if key in out else None
            if absent is None or not absent.any():
                continue
            for k in [k for k in out if k.startswith(block + ".")]:
                col = out[k]
                if col.dtype == object:
                    col = col.copy()
                    for i in np.flatnonzero(absent):
                        col[i] = []
                    out[k] = col
                elif col.dtype.kind in "US":
                    out[k] = np.where(absent, "", col)
                elif col.dtype.kind == "b":
                    out[k] = np.where(absent, False, col)
                else:
                    out[k] = np.where(absent, 0, col).astype(col.dtype)
        return out


# a block is present (in the message) when this leaf is non-zero
PRESENT = {"card_info": "card_info.id", "account_info": "account_info.id", "merchant_info": "merchant_info.id",
           "device_info": "device_info.id", "payee_info": "payee_info.id", "authentication": "authentication.attempts"}


def block_messages(cols: dict[str, Any], i: int, enabled: set[str]) -> dict[str, Any]:
    """The protobuf kwargs of event `i`'s blocks (only blocks with content: an empty one stays absent, so a
    payment the block does not apply to - a transfer's card - is byte-identical to one without it)."""
    from ..payments.v1 import transaction_pb2 as pb
    kw: dict[str, Any] = {}

    def g(k):
        v = cols[k][i]
        return v.item() if hasattr(v, "item") else v
    if "card_info" in enabled and g("card_info.id"):
        kw["card_info"] = pb.CardInfo(id=g("card_info.id"), bin=g("card_info.bin"), bin_country=g("card_info.bin_country"),
                                      status=g("card_info.status"), network=g("card_info.network"),
                                      tokenized=g("card_info.tokenized"), wallet=g("card_info.wallet"),
                                      issued_at_us=g("card_info.issued_at_us"))
    if "account_info" in enabled and g("account_info.id"):
        kw["account_info"] = pb.AccountInfo(id=g("account_info.id"), type=g("account_info.type"),
                                            balance_usd_minor=g("account_info.balance_usd_minor"),
                                            age_days=g("account_info.age_days"), flags=list(cols["account_info.flags"][i]))
    if "merchant_info" in enabled and g("merchant_info.id"):
        kw["merchant_info"] = pb.MerchantInfo(id=g("merchant_info.id"), name=g("merchant_info.name"), mcc=g("merchant_info.mcc"),
                                              address=pb.Address(country=g("merchant_info.address.country"),
                                                                 city=g("merchant_info.address.city")),
                                              risk_tier=g("merchant_info.risk_tier"))
    if "device_info" in enabled and g("device_info.id"):
        kw["device_info"] = pb.DeviceInfo(id=g("device_info.id"), os=g("device_info.os"), app_version=g("device_info.app_version"),
                                          emulator=g("device_info.emulator"), rooted=g("device_info.rooted"),
                                          risk_score=g("device_info.risk_score"))
    if "payee_info" in enabled and g("payee_info.id"):
        kw["payee_info"] = pb.PayeeInfo(id=g("payee_info.id"), bank_country=g("payee_info.bank_country"),
                                        new_payee=g("payee_info.new_payee"), name_match=g("payee_info.name_match"))
    if "authentication" in enabled and g("authentication.attempts"):
        kw["authentication"] = pb.Authentication(three_ds=g("authentication.three_ds"), attempts=g("authentication.attempts"),
                                                 method=g("authentication.method"), sca_exempt=g("authentication.sca_exempt"))
    if "items" in enabled and len(cols["items"][i]):
        kw["items"] = [pb.LineItem(**x) for x in cols["items"][i]]
    if "attributes" in enabled and cols["attributes"][i]:
        kw["attributes"] = dict(cols["attributes"][i])
    return kw
