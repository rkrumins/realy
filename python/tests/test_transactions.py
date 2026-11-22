"""Transactions formed by linked messages: the a2a_transfer contract and the reference assembler."""
from __future__ import annotations

import numpy as np

from realy.bindings import event_type
from realy.bindings.transactions import Assembler, default_context, transaction, transactions

T0 = 1_800_000_000_000_000
CUST, PAY = 42, 9001


def msg(type_name: str, dt_ms: float, **fields):
    """A row of one flow message (the event-log shape), from its protobuf message."""
    from realy.bindings.codec import message_class
    m = message_class(event_type(type_name).message)(event_id=abs(hash((type_name, dt_ms))) % (1 << 60),
                                                     event_time_us=T0 + int(dt_ms * 1000), customer_id=CUST,
                                                     payment_id=PAY, **fields)
    return event_type(type_name).codec.row(m)


def test_contract_is_valid():
    t = transaction("a2a_transfer")
    assert t.validate() == []
    assert t.role_names[0] == "initiation" and t.bit("payee_check") == 4
    assert set(transactions()) >= {"a2a_transfer"}


def test_assembles_into_the_unified_event_with_context_and_stages():
    a = Assembler([transaction("a2a_transfer")])
    out = a.process("payment_initiation", msg("payment_initiation", 0, account_id=7, payee_id=555, amount_minor=125000,
                                              currency="GBP", amount_usd_minor=158000, channel=1, new_payee=True))
    assert out == []                                   # waits for the required payee check
    assert a.process("device_context", msg("device_context", 20, device_id=77, ip_v4=167772161, ip_country="GB",
                                           remote_access=True)) == []
    out = a.process("payee_check", msg("payee_check", 60, payee_id=555, result=3))
    assert len(out) == 1 and out[0].kind == "assembled"
    ev, ctx = out[0].row, out[0].context
    pay = event_type("payment").codec
    assert pay.enum_name("payment_type", ev["payment_type"]) == "TRANSFER"
    assert pay.enum_name("channel", ev["channel"]) == "IN_APP"           # A2A_MOBILE_APP mapped
    assert pay.enum_name("entry_mode", ev["entry_mode"]) == "BANK_APP"
    assert ev["customer_id"] == CUST and ev["payee_id"] == 555 and ev["amount_usd_minor"] == 158000
    assert ev["device_id"] == 77 and ev["ip_country"] == "GB"            # the optional part that came first
    assert ctx["payee_check"] == "PAYEE_NO_MATCH" and ctx["remote_access"] is True and ctx["new_payee"] is True
    assert ctx["transaction"] == "a2a_transfer" and ctx["stage"] == "assembled" and ctx["partial"] is False
    assert out[0].parts_present == 0b111
    # the customer proceeds anyway: SCA, then the submission (to another payee) is scored again
    assert a.process("sca_result", msg("sca_result", 9000, method=2, passed=True, attempt=1)) == []
    out = a.process("payment_submission", msg("payment_submission", 15000, payee_id=556, amount_minor=125000,
                                              amount_usd_minor=158000, scheme=1))
    assert len(out) == 1 and out[0].kind == "stage" and out[0].stage == "submitted" and out[0].read_only
    assert out[0].row["payee_id"] == 556 and out[0].row["event_time_us"] == T0 + 15_000_000
    c = out[0].context
    assert c["payee_changed"] is True and c["amount_changed"] is False and c["sca_attempts"] == 1
    assert abs(c["secs_to_submit"] - 15.0) < 1e-9 and c["sca_passed"] is True and c["stage"] == "submitted"
    assert a.process("payment_status", msg("payment_status", 60000, code=2)) == []   # settled: not scored


def test_missing_required_part_assembles_partial_at_the_deadline():
    a = Assembler([transaction("a2a_transfer")])
    a.process("payment_initiation", msg("payment_initiation", 0, payee_id=5, amount_usd_minor=100))
    # the customer's next message comes after the 150 ms wait: the transfer is assembled without the check
    out = a.process("payment", {"event_id": 1, "event_time_us": T0 + 400_000, "customer_id": CUST})
    assert [o.kind for o in out] == ["assembled", "passthrough"]
    assert out[0].partial and out[0].parts_present == 0b1 and out[0].context["payee_check"] == "PAYEE_CHECK_UNSPECIFIED"
    assert out[1].context == default_context()


def test_a_recorded_partial_assembly_is_replayed():
    """The pipeline assembled without the payee check (its wall-clock wait expired); the twin reproduces
    that from the decision's parts_present although the check arrived in time by event time."""
    a = Assembler([transaction("a2a_transfer")])
    a.force("a2a_transfer", PAY, 0b1)
    out = a.process("payment_initiation", msg("payment_initiation", 0, payee_id=5))
    assert len(out) == 1 and out[0].parts_present == 0b1


def test_lifecycle_message_before_assembly_is_held():
    a = Assembler([transaction("a2a_transfer")])
    a.process("payment_initiation", msg("payment_initiation", 0, payee_id=5))
    assert a.process("payment_submission", msg("payment_submission", 50, payee_id=5)) == []
    out = a.process("payee_check", msg("payee_check", 80, payee_id=5, result=1))
    assert [o.kind for o in out] == ["assembled", "stage"]


def test_default_context_has_every_name_with_its_default():
    d = default_context()
    assert d["transaction"] == "" and d["payee_check"] == "PAYEE_CHECK_UNSPECIFIED" and d["sca_attempts"] == 0
    assert d["remote_access"] is False and d["secs_to_submit"] == 0.0


def _flow_stream(n_batches=25):
    from realy.gen.generator import Generator, sim_start_us
    from realy.scenario import Scenario, WorldSpec
    from realy.world import build_world
    spec = WorldSpec(customers=3000, merchants=400, payees=1500)
    sc = Scenario(name="t", seed=11, world=spec, traffic={"flows": ["a2a_transfer"]},
                  fraud={"background_per_1k": 8.0, "mix": {"app_scam_mule": 1.0, "account_takeover": 1.0}})
    g = Generator(build_world(spec, 11, None), sc, 0, 1)
    t = sim_start_us(sc)
    dt = int(200 / g.natural_rate_per_sim_s() * 1e6)
    unified, records = {}, []
    for _ in range(n_batches):
        b = g.generate(200, t, t + dt)
        t += dt
        cols = b.columns
        for i in range(len(b)):
            unified[int(cols["event_id"][i])] = {k: cols[k][i] for k in cols}
        records += b.records
    records += g.flows.drain()
    return unified, records


def test_generated_parts_assemble_back_into_the_unified_transfers():
    """The generator draws each transfer as a unified event, then sends it as its messages: assembling
    those messages (the twin, in the order the pipeline receives them) gives back exactly that event."""
    from realy.bindings.codec import message_class
    from realy.features.derived import PAYMENT_TYPE
    unified, records = _flow_stream()
    a = Assembler([transaction("a2a_transfer")])
    pay = event_type("payment").codec
    assembled = {}
    for r in records:
        if not r.type:
            row = pay.row(message_class(pay.full_name).FromString(r.payload))
            outs = a.process("payment", row)
        else:
            c = event_type(r.type).codec
            outs = a.process(r.type, c.row(message_class(c.full_name).FromString(r.payload)))
        for o in outs:
            if o.kind == "assembled":
                assembled[int(o.row["event_id"])] = o
    for o in a.close():
        assembled[int(o.row["event_id"])] = o
    transfers = {e: u for e, u in unified.items() if int(u["payment_type"]) == PAYMENT_TYPE.index("TRANSFER")}
    assert len(transfers) > 200 and set(assembled) == set(transfers)
    fields = [f.name for f in pay.fields]
    for eid, u in transfers.items():
        got = assembled[eid].row
        for f in fields:
            want = u[f]
            if f in ("device_id", "ip_v4", "ip_country", "ip_asn") and not assembled[eid].parts_present & 0b10:
                want = pay.by_name[f].default          # no device context sent: those stay default
            assert got[f] == (want.item() if hasattr(want, "item") else want), (eid, f, got[f], want)
    # the flow's own signals: scams show up in the context
    scams = [o for o in assembled.values() if int(unified[int(o.row["event_id"])]["fraud_type"]) == 7]
    legit = [o for o in assembled.values() if int(unified[int(o.row["event_id"])]["fraud_type"]) == 0]
    assert scams and legit
    share = lambda xs, k: sum(1 for o in xs if o.context[k]) / len(xs)   # noqa: E731
    assert share(scams, "new_payee") > 0.6 > 0.2 > share(legit, "new_payee")


def test_verifier_replays_the_assembly_order_and_contexts(tmp_path):
    """verify on a flows run: the unified event log plus the parts log, through the twin, give every event
    once, transfers at their assembly with their context, and a recorded partial replayed as recorded."""
    import pyarrow as pa
    from realy.verify.verify import flow_order
    unified, records = _flow_stream(12)
    cols = {}
    rows = sorted(unified.values(), key=lambda r: (int(r["event_time_us"]), int(r["event_id"])))
    for k in rows[0]:
        cols[k] = np.asarray([r[k] for r in rows])
    parts = pa.table({"type": [r.type for r in records if r.type],
                      "event_time_us": [r.t_us for r in records if r.type],
                      "payload": [r.payload for r in records if r.type]})
    transfers = [int(r["event_id"]) for r in rows if int(r["payment_type"]) == 3]
    forced_id = transfers[0]
    dec = pa.table({"event_id": np.asarray([forced_id], np.uint64), "txn_partial": [True], "txn_parts": [1],
                    "txn": ["a2a_transfer/assembled"]})
    order, ctx, info, rows_as_assembled = flow_order(cols, parts, dec)
    assert set(rows_as_assembled) == set(transfers)
    # the forced partial had no payee check; an optional device context that came late is absent
    for eid, row in rows_as_assembled.items():
        if not ctx[eid]["parts"] & 0b10:
            assert row["device_id"] == 0
    n_stage = info["stage_decisions"]
    assert n_stage > 100                                              # re-scored submissions, read-only
    assert sorted(order.tolist()) == list(range(len(rows) + n_stage))  # every event exactly once
    stage_ids = [int(e) for e in cols["event_id"][len(rows):]]
    assert all(ctx[e]["stage"] == "submitted" for e in stage_ids)
    assert set(ctx) == set(transfers) | set(int(e) for e in cols["event_id"][len(rows):]) and info["assembled"] == len(transfers)
    assert ctx[forced_id]["partial"] is True and ctx[forced_id]["parts"] & 0b101 == 0b001   # no payee check
    assert info["partial_replayed_from_decisions"] == 1
    # a card payment keeps its place relative to its own customer's other events
    pos = {int(cols["event_id"][i]): p for p, i in enumerate(order.tolist())}
    by_cust: dict = {}
    for r in rows:
        by_cust.setdefault(int(r["customer_id"]), []).append(int(r["event_id"]))
    for evs in by_cust.values():
        non_tr = [e for e in evs if e not in ctx]
        assert [pos[e] for e in non_tr] == sorted(pos[e] for e in non_tr)


def test_report_summarises_transactions(tmp_path):
    """The report's transactions section: assembled, partial, the assembly wait, and decisions by payee
    check result with ground truth, from the decisions' txn columns."""
    import json

    import pyarrow as pa
    import pyarrow.parquet as pq
    from realy.report.report import transactions_summary
    n = 6
    pq.write_table(pa.table({
        "event_id": np.arange(1, n + 1, dtype=np.uint64), "revision": np.zeros(n, np.uint8),
        "outcome": np.asarray([1, 2, 1, 3, 1, 1], np.int8),
        "txn": ["a2a_transfer/assembled"] * 4 + ["", ""],
        "txn_parts": np.asarray([7, 7, 5, 1, -1, -1], np.int32),
        "txn_partial": np.asarray([False, False, False, True, False, False]),
        "txn_context": [json.dumps({"payee_check": "PAYEE_MATCH"}), json.dumps({"payee_check": "PAYEE_NO_MATCH"}),
                        json.dumps({"payee_check": "PAYEE_MATCH"}), json.dumps({}), "", ""],
        "st_txn_assembly": np.asarray([20_000, 30_000, 40_000, 150_000, -1, -1], np.int32)}), tmp_path / "decisions-0.parquet")
    pq.write_table(pa.table({"event_id": np.arange(1, n + 1, dtype=np.uint64),
                             "is_fraud": np.asarray([False, True, False, True, False, False])}), tmp_path / "events-0.parquet")
    (tmp_path / "emit-0.json").write_text(json.dumps({"parts_sent": 17}))
    s = transactions_summary(tmp_path)
    assert s["assembled"] == 4 and s["partial"] == 1 and s["parts_sent"] == 17
    assert s["by_payee_check"]["PAYEE_NO_MATCH"] == {"decided": 1, "fraud": 1, "stopped": 1, "fraud_stopped": 1}
    assert s["by_payee_check"]["PAYEE_CHECK_UNSPECIFIED"]["fraud_stopped"] == 1      # the partial one, declined
    assert s["assembly_wait_ms"]["p50"] > 20
