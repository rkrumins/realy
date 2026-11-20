"""Realism profile: generate a sample of the event stream and summarise its distributions."""
from __future__ import annotations

import numpy as np

from ..scenario import Scenario
from .build import World
from . import mcc as M

PT = {1: "CARD_PURCHASE", 2: "ATM_WITHDRAWAL", 3: "TRANSFER", 4: "CARD_REFUND"}
CH = {1: "POS", 2: "ECOM", 3: "ATM", 4: "MOTO", 5: "IN_APP", 6: "ONLINE_BANKING"}
EM = {1: "CHIP", 2: "CONTACTLESS", 3: "MAGSTRIPE", 4: "MANUAL", 5: "WALLET_TOKEN", 6: "ECOM_3DS", 7: "ECOM_NON_3DS",
      8: "CREDENTIAL_ON_FILE", 9: "BANK_APP"}



def _share(values: np.ndarray, names: dict[int, str]) -> dict[str, float]:
    u, c = np.unique(values, return_counts=True)
    tot = c.sum()
    return {names.get(int(k), str(k)): round(float(v) / tot, 4) for k, v in sorted(zip(u, c), key=lambda x: -x[1])}


def sample_stream(world: World, scenario: Scenario, events: int, sim_hours_per_batch: float = 1.0):
    """Generate `events` events at the population's natural rate (time_scale irrelevant here)."""
    from ..gen import Generator, sim_start_us
    g = Generator(world, scenario, 0, 1)
    rate = g.natural_rate_per_sim_s()
    t = sim_start_us(scenario)
    dt = int(sim_hours_per_batch * 3600 * 1e6)
    per = max(1, int(rate * sim_hours_per_batch * 3600))
    cols: list[dict] = []
    labels = 0
    got = 0
    while got < events:
        b = g.generate(min(per, events - got), t, t + dt)
        t += dt
        got += len(b)
        labels += len(b.labels)
        cols.append(b.columns)
    merged = {k: np.concatenate([c[k] for c in cols]) for k in cols[0]}
    return merged, labels, (t - sim_start_us(scenario)) / 1e6, g


def profile(world: World, scenario: Scenario, events: int = 200_000) -> dict:
    c, n_labels, sim_s, gen = sample_stream(world, scenario, events)
    n = c["event_id"].shape[0]
    days = sim_s / 86400.0
    cust = world.customers
    rows = world.customer_row(c["customer_id"])
    per_cust = np.bincount(rows, minlength=world.n_customers)[gen.rows] / max(days, 1e-9)
    usd = c["amount_usd_minor"] / 100.0
    tz = cust["tz_offset_min"][rows].astype(np.int64)
    local_hour = ((c["event_time_us"] // 1_000_000 + tz * 60) % 86400) // 3600
    hour_curve = np.bincount(local_hour, minlength=24) / n
    home_cc = np.asarray(world_countries(), dtype="U2")[cust["home_country"][rows]]
    has_m = c["merchant_id"] != 0
    foreign = has_m & (c["merchant_country"] != home_cc)
    # amount by MCC (top categories)
    mcc_stats = {}
    mu, mc = np.unique(c["mcc"][has_m], return_counts=True)
    for code, cnt in sorted(zip(mu.tolist(), mc.tolist()), key=lambda x: -x[1])[:16]:
        a = usd[c["mcc"] == code]
        name = "atm" if code == M.ATM_MCC else (M.NAMES[list(M.MCC).index(code)] if code in M.MCC else str(code))
        mcc_stats[f"{code} {name}"] = {"share": round(cnt / n, 4), "p50_usd": round(float(np.median(a)), 2),
                                       "p90_usd": round(float(np.quantile(a, 0.9)), 2)}
    fraud = c["is_fraud"]
    ft = c["fraud_type"]
    by_type = {}
    for k in np.unique(ft[fraud]).tolist():
        sel = fraud & (ft == k)
        from ..usecases import registry
        by_type[registry().fraud_type_names().get(int(k), str(k))] = {"events": int(sel.sum()), "share_of_events": round(float(sel.mean()), 6),
                          "usd": round(float(usd[sel].sum()), 2)}
    return {
        "events": n, "sim_days": round(days, 3), "customers_in_shard": int(gen.rows.shape[0]),
        "txns_per_customer_per_day": {"mean": round(float(per_cust.mean()), 3), "p50": round(float(np.median(per_cust)), 3),
                                      "p90": round(float(np.quantile(per_cust, 0.9)), 3),
                                      "p99": round(float(np.quantile(per_cust, 0.99)), 3),
                                      "active_share": round(float((per_cust > 0).mean()), 4)},
        "payment_types": _share(c["payment_type"], PT),
        "channels": _share(c["channel"], CH),
        "entry_modes": _share(c["entry_mode"], EM),
        "card_present_share": round(float(c["card_present"].mean()), 4),
        "foreign_merchant_share": round(float(foreign.sum() / max(has_m.sum(), 1)), 4),
        "amount_usd": {q: round(float(np.quantile(usd, v)), 2) for q, v in (("p10", .1), ("p50", .5), ("p90", .9), ("p99", .99))},
        "amount_by_mcc": mcc_stats,
        "local_hour_curve": [round(float(x), 4) for x in hour_curve],
        "fraud": {"events": int(fraud.sum()), "rate": round(float(fraud.mean()), 5),
                  "usd": round(float(usd[fraud].sum()), 2), "by_type": by_type, "labels_emitted": n_labels},
    }


def world_countries() -> list[str]:
    from .geo import geo
    return geo().countries


def print_profile(rep: dict) -> None:
    from rich.console import Console
    from rich.table import Table
    con = Console()
    con.print(f"[bold]events[/] {rep['events']:,}  sim days {rep['sim_days']}  customers {rep['customers_in_shard']:,}")
    con.print("txns/customer/day", rep["txns_per_customer_per_day"])
    for key in ("payment_types", "channels", "entry_modes"):
        t = Table(title=key)
        t.add_column("value")
        t.add_column("share", justify="right")
        for k, v in rep[key].items():
            t.add_row(k, f"{v:.2%}")
        con.print(t)
    t = Table(title="amount by MCC (USD)")
    for col in ("mcc", "share", "p50", "p90"):
        t.add_column(col, justify="right" if col != "mcc" else "left")
    for k, v in rep["amount_by_mcc"].items():
        t.add_row(k, f"{v['share']:.2%}", f"{v['p50_usd']:.2f}", f"{v['p90_usd']:.2f}")
    con.print(t)
    curve = rep["local_hour_curve"]
    peak = max(curve) or 1
    con.print("local hour-of-day:")
    for h, v in enumerate(curve):
        con.print(f"  {h:02d} {'#' * int(40 * v / peak)} {v:.3f}")
    con.print(f"card-present {rep['card_present_share']:.1%}  foreign merchant {rep['foreign_merchant_share']:.1%}  amount {rep['amount_usd']}")
    t = Table(title=f"fraud: {rep['fraud']['events']:,} events ({rep['fraud']['rate']:.3%}), labels {rep['fraud']['labels_emitted']:,}")
    for col in ("typology", "events", "share", "usd"):
        t.add_column(col)
    for k, v in rep["fraud"]["by_type"].items():
        t.add_row(k, f"{v['events']:,}", f"{v['share_of_events']:.4%}", f"{v['usd']:,.0f}")
    con.print(t)
