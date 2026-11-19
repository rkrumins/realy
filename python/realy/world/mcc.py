"""Merchant category table (ISO 18245 MCCs) with behavioural parameters.

Columns: mcc, name, pos_weight, remote_weight, median_usd, sigma, risk_tier, recurring_prob
  pos_weight / remote_weight: share of merchants of this category among card-present / online
  merchants (popularity then skews actual traffic).
"""
from __future__ import annotations

import numpy as np

_MCC = [
    # mcc,  name,                 pos,   remote, median, sigma, risk, recurring
    (5411, "grocery",             0.20,  0.03,   35.0,   0.80, 0, 0.00),
    (5812, "restaurants",         0.12,  0.00,   32.0,   0.70, 0, 0.00),
    (5814, "fast_food",           0.11,  0.02,    9.5,   0.50, 0, 0.00),
    (5541, "fuel",                0.07,  0.00,   48.0,   0.50, 0, 0.00),
    (4111, "transit",             0.05,  0.01,    3.5,   0.50, 0, 0.02),
    (5912, "pharmacy",            0.05,  0.01,   18.0,   0.70, 0, 0.00),
    (5311, "department_store",    0.05,  0.05,   62.0,   0.90, 1, 0.00),
    (5651, "clothing",            0.06,  0.10,   55.0,   0.80, 1, 0.00),
    (5732, "electronics",         0.02,  0.06,  160.0,   1.00, 2, 0.00),
    (5944, "jewellery",           0.01,  0.01,  260.0,   1.00, 2, 0.00),
    (5999, "misc_retail",         0.08,  0.05,   25.0,   1.00, 1, 0.00),
    (5942, "books",               0.02,  0.03,   18.0,   0.60, 0, 0.00),
    (5815, "digital_media",       0.00,  0.09,   10.0,   0.70, 1, 0.10),
    (5816, "digital_games",       0.00,  0.04,   15.0,   0.80, 1, 0.05),
    (4899, "subscriptions",       0.00,  0.07,   13.0,   0.30, 0, 0.90),
    (4121, "rideshare_taxi",      0.03,  0.06,   18.0,   0.60, 0, 0.00),
    (5964, "online_marketplace",  0.00,  0.22,   35.0,   1.00, 1, 0.02),
    (4722, "travel_agency",       0.01,  0.02,  420.0,   0.80, 1, 0.00),
    (4511, "airlines",            0.00,  0.02,  310.0,   0.70, 1, 0.00),
    (7011, "hotels",              0.02,  0.02,  180.0,   0.60, 1, 0.00),
    (5947, "gift_cards",          0.01,  0.01,   50.0,   0.60, 2, 0.00),
    (7995, "gambling",            0.00,  0.01,   40.0,   1.00, 3, 0.00),
    (6051, "quasi_cash_crypto",   0.00,  0.005, 200.0,   1.00, 3, 0.00),
    (8398, "charity",             0.00,  0.005,  20.0,   0.80, 1, 0.20),
    (4829, "money_transfer",      0.005, 0.005, 180.0,   1.00, 2, 0.00),
]

MCC = np.array([r[0] for r in _MCC], dtype=np.uint32)
NAMES = [r[1] for r in _MCC]
POS_W = np.array([r[2] for r in _MCC], dtype=np.float64)
REMOTE_W = np.array([r[3] for r in _MCC], dtype=np.float64)
MEDIAN_USD = np.array([r[4] for r in _MCC], dtype=np.float64)
SIGMA = np.array([r[5] for r in _MCC], dtype=np.float64)
RISK = np.array([r[6] for r in _MCC], dtype=np.int8)
RECURRING = np.array([r[7] for r in _MCC], dtype=np.float64)
N_CAT = len(_MCC)
ATM_MCC = 6011


def cat_index(name: str) -> int:
    return NAMES.index(name)


def cats(*names: str) -> np.ndarray:
    return np.array([NAMES.index(n) for n in names], dtype=np.int16)


# category groups used by behaviour / fraud scripts
HIGH_RISK_RESALE = ("electronics", "jewellery", "gift_cards", "online_marketplace", "quasi_cash_crypto")
CARD_TESTING_TARGETS = ("charity", "digital_media", "digital_games")
BOT_TARGETS = ("online_marketplace", "digital_media", "digital_games", "gift_cards")
STOLEN_CARD_POS = ("grocery", "pharmacy", "fuel", "misc_retail", "fast_food", "clothing")
COUNTERFEIT_POS = ("electronics", "jewellery", "department_store", "clothing")
