"""Authorisation signals: what the issuer sees in the request besides the payment itself.

  three_ds          online card payments: AUTHENTICATED / ATTEMPTED / NONE (EMV 3DS result)
  pin_verified      chip and ATM: the PIN was entered and verified
  cryptogram_valid  chip, contactless and wallet: the EMV / network-token cryptogram checked out
                    (magstripe and keyed entry never carry one)
  scheme_score      card network risk score 0-99 (Visa / Mastercard style), 0 when none
  (cvv_match / avs_match are drawn by the behaviour and campaign models, as before)

Each is drawn per event from its population's rates: scenario `fraud.auth.legitimate` for genuine
payments, else the typology's `auth` block (contracts/usecases), overridable per scenario. The draws
are a pure function of the event id and the scenario seed, salted per signal, and are made after the
events exist: no other random stream moves, so every event that existed before keeps all its values
(measured numbers stay reproducible) and the instrument can recompute them for any run.
"""
from __future__ import annotations

from typing import Any

import numpy as np

from ..challenge import _u01
from ..features.derived import ENTRY_MODE, PAYMENT_TYPE

THREE_DS = ["THREE_DS_NONE", "THREE_DS_ATTEMPTED", "THREE_DS_AUTHENTICATED"]
_EM = {n: i for i, n in enumerate(ENTRY_MODE)}
_PT = {n: i for i, n in enumerate(PAYMENT_TYPE)}
_SALT = {"three_ds": 0x3D5, "pin": 0x919, "crypto": 0xC2E, "score_a": 0x5C0A, "score_b": 0x5C0B}


class AuthSignals:
    def __init__(self, scenario: Any, registry: Any) -> None:
        spec = scenario.fraud.auth
        self.seed = int(scenario.seed)
        self.legit = spec.legitimate
        self.by_type = {}
        for t in registry.all:
            r = spec.typologies.get(t.name) or t.auth
            if r is not None:
                self.by_type[t.id] = r

    def _rates(self, is_fraud: np.ndarray, fraud_type: np.ndarray) -> dict[str, np.ndarray]:
        n = is_fraud.shape[0]
        L = self.legit
        out = {"three_ds": np.full(n, L.three_ds_authenticated), "pin": np.full(n, L.pin_verified),
               "crypto": np.full(n, L.cryptogram_valid), "mu": np.full(n, L.scheme_score.mean),
               "sd": np.full(n, L.scheme_score.sd)}
        for t, r in self.by_type.items():
            m = is_fraud & (fraud_type == t)
            if m.any():
                out["three_ds"][m], out["pin"][m], out["crypto"][m] = r.three_ds_authenticated, r.pin_verified, r.cryptogram_valid
                out["mu"][m], out["sd"][m] = r.scheme_score.mean, r.scheme_score.sd
        return out

    def draw(self, c: dict[str, np.ndarray], event_ids: np.ndarray) -> dict[str, np.ndarray]:
        """The four signal columns for a finalised chunk (internal columns, sorted, ids assigned)."""
        n = event_ids.shape[0]
        if n == 0:
            return {"three_ds": np.zeros(0, np.int8), "pin_verified": np.zeros(0, bool),
                    "cryptogram_valid": np.zeros(0, bool), "scheme_score": np.zeros(0, np.uint32)}
        fr = c["is_fraud"].astype(bool)
        r = self._rates(fr, c["fraud_type"])
        em, pt = c["entry_mode"].astype(np.int64), c["payment_type"].astype(np.int64)
        u = {k: _u01(event_ids, self.seed ^ s) for k, s in _SALT.items()}
        card = np.isin(pt, [_PT["CARD_PURCHASE"], _PT["ATM_WITHDRAWAL"], _PT["CARD_REFUND"]])
        # 3DS: a 3DS-capable checkout authenticates or only attempts; a non-3DS one has none
        ecom3 = em == _EM["ECOM_3DS"]
        three = np.where(ecom3, np.where(u["three_ds"] < r["three_ds"], 2, 1), 0).astype(np.int8)
        # PIN: chip in store and ATM withdrawals ask for it
        pin_cap = (em == _EM["CHIP"]) | (pt == _PT["ATM_WITHDRAWAL"])
        pin = pin_cap & (u["pin"] < r["pin"])
        # cryptogram: chip, contactless and wallet tokens carry one (ATM chip too); magstripe / keyed never
        crypto_cap = np.isin(em, [_EM["CHIP"], _EM["CONTACTLESS"], _EM["WALLET_TOKEN"]])
        crypto = crypto_cap & (u["crypto"] < r["crypto"])
        # network risk score for card payments: a clipped normal (Box-Muller from two uniforms)
        z = np.sqrt(-2.0 * np.log(np.maximum(u["score_a"], 1e-12))) * np.cos(2 * np.pi * u["score_b"])
        score = np.where(card, np.clip(np.rint(r["mu"] + r["sd"] * z), 1, 99), 0).astype(np.uint32)
        return {"three_ds": three, "pin_verified": pin, "cryptogram_valid": crypto, "scheme_score": score}
