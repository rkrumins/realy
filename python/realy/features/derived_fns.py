"""Derived-function plugins for `source: derived, fn: <name>` features: the Python twin of
io.realy.scoring.features.DerivedFns (Java).

The built-in library (amount_usd, is_cnp, dist_from_home_km, ...) is computed by realy.features.derived on
the payment record. A plugin function sees what a CEL expression feature sees: `event`, the event's view (every
field by name, enums as their names) and `f`, every feature computed before it. Register one with

    from realy.features.derived_fns import register
    register("log_amount", lambda event, f: math.log1p(event["amount_usd_minor"] / 100.0))

and its Java twin (a DerivedFn in a jar in PLUGINS_DIR). Both pass the fixture this module writes
(contracts/testdata/derived/<name>.json; checked in Java by DerivedFnConformance). docs/dev/extending.md.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

Fn = Callable[[dict[str, Any], dict[str, float]], float]

BUILTINS = ("amount_usd", "is_cnp", "is_contactless", "is_atm", "is_transfer", "is_foreign", "dist_from_home_km",
            "amount_to_avg_ratio", "local_hour", "credit_utilisation_24h")
_REGISTRY: dict[str, Fn] = {}
FIXTURES = Path(__file__).resolve().parents[3] / "contracts" / "testdata" / "derived"


def register(name: str, fn: Fn) -> Fn:
    if name in BUILTINS:
        raise ValueError(f"derived fn {name} is a built-in")
    prev = _REGISTRY.get(name)
    if prev is not None and prev is not fn:
        raise ValueError(f"derived fn {name} is already registered")
    _REGISTRY[name] = fn
    return fn


def _ensure() -> None:
    from .. import plugins
    plugins.load()


def has(name: str) -> bool:
    if name in BUILTINS or name in _REGISTRY:
        return True
    _ensure()
    return name in _REGISTRY


def get(name: str) -> Fn:
    if name not in _REGISTRY:
        _ensure()
    fn = _REGISTRY.get(name)
    if fn is None:
        raise KeyError(f"unknown derived fn {name!r} (registered: {', '.join(names())})")
    return fn


def names() -> list[str]:
    _ensure()
    return sorted(set(BUILTINS) | set(_REGISTRY))


def extensions() -> list[str]:
    _ensure()
    return sorted(_REGISTRY)


# ------------------------------------------------------------------------------- conformance fixtures
def sample_rows(n: int = 40, seed: int = 11) -> list[dict[str, Any]]:
    """Event views and feature maps with the shapes a plugin sees: integers, floats, enum names, text."""
    import random
    rnd = random.Random(seed)
    types = ["CARD_PURCHASE", "ATM_WITHDRAWAL", "TRANSFER"]
    rows = []
    for _ in range(n):
        ev = {"amount_usd_minor": rnd.choice([0, 1, 99, 1_250, 49_999, 250_000, 9_999_999]),
              "payment_type": rnd.choice(types), "card_present": rnd.random() < 0.4,
              "lat": round(rnd.uniform(-60, 60), 4), "lon": round(rnd.uniform(-170, 170), 4),
              "merchant_country": rnd.choice(["US", "GB", "NG", ""]), "event_time_us": 1_800_000_000_000_000 + rnd.randint(0, 10**12)}
        f = {"cust_avg_ticket_usd": round(rnd.uniform(0, 400), 3), "card_amount_sum_24h": float(rnd.randint(0, 10**6))}
        rows.append({"event": ev, "f": f})
    return rows


def conformance(fns: list[str], rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    rows = rows or sample_rows()
    return {"cases": [{"fn": n, "rows": [{**r, "value": float(get(n)(r["event"], r["f"]))} for r in rows]} for n in fns]}


def write_fixture(fns: list[str], path: Path | None = None) -> Path:
    path = path or FIXTURES / f"{fns[0]}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(conformance(fns), separators=(",", ":")) + "\n")
    return path


def _main(argv: list[str] | None = None) -> int:
    """`python -m realy.features.derived_fns conformance --plugin MOD --fn NAME [--out FILE]` | `list`."""
    import argparse
    from .. import plugins
    ap = argparse.ArgumentParser(prog="python -m realy.features.derived_fns")
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("conformance", help="write a conformance fixture for plugin functions")
    c.add_argument("--plugin", action="append", default=[], help="twin module to import")
    c.add_argument("--fn", action="append", required=True, help="function name")
    c.add_argument("--out", type=Path, default=None)
    sub.add_parser("list", help="the registered functions")
    a = ap.parse_args(argv)
    if a.cmd == "conformance" and a.plugin:
        plugins.load(a.plugin)
    if a.cmd == "list":
        for n in names():
            print(f"{n:24s} {'built-in' if n in BUILTINS else 'plugin'}")
        return 0
    print(write_fixture(a.fn, a.out))
    return 0


if __name__ == "__main__":
    from realy.features import derived_fns as _canonical
    raise SystemExit(_canonical._main())
