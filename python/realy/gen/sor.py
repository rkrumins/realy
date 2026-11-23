"""Simulated systems of record: who owns the status of the entities a payment touches, and the changes they
publish (scenario `traffic.status`, pipeline payments-status).

  card management   a card blocked by the issuer's own monitoring (at a compromised card's k-th fraudulent
                    payment, a few minutes later), or reported lost / stolen when its fraud is reported (the
                    fraud label's time); temporary locks a customer sets in the app and lifts again (noise)
  core banking      an account frozen when an account takeover is reported; AML holds set and released (noise)
  fraud operations  a payee listed as a mule at its k-th fraud label
  at the start      a share of cards already blocked, lost or stolen, of accounts on an AML hold, and of the
                    world's mule payees already listed

Each change is a typed message (card_status_changed, account_status_changed: keyed by customer on the events
topic; payee_listing_changed: keyed by payee on its own topic) at the simulated time it happens, merged with the
payments in event-time order, so the pipeline folds it into the entity's state before any later payment reads it.

The world reacts: a customer does not pay with a blocked or locked card, so genuine payments on it stop while
it is blocked; fraudsters holding the card's details keep trying, which is what a blocked-card rule catches.

One random stream of its own, consumed in batch order (the generator's other streams are unchanged), so a run
is deterministic; each generator shard simulates its own customers' cards and accounts.
"""
from __future__ import annotations

import collections
import heapq
from typing import Any

import numpy as np

from .flows import Record

CARD, ACCOUNT, PAYEE = "card_status_changed", "account_status_changed", "payee_listing_changed"
TYPES = (CARD, ACCOUNT, PAYEE)
FOREVER = 1 << 62
# fraud types whose report means the account itself was taken over (its owner is not the one paying)
ACCOUNT_FRAUD = ("ACCOUNT_TAKEOVER", "BUST_OUT")


class SystemOfRecord:
    """`observe(chunk, t0, t1)` takes a batch's payments before they are finalised: it schedules the changes they
    cause and returns which to keep (genuine payments on a blocked card are dropped). `due(t1)` gives the changes
    that happen before t1 as typed Records."""

    def __init__(self, spec: Any, world: Any, rows: np.ndarray, rng: np.random.Generator, gen_id: int,
                 pipe: Any = None) -> None:
        from ..bindings.pipeline import pipeline
        from ..accounts.v1 import accounts_pb2
        from ..cards.v1 import cards_pb2
        from ..payees.v1 import payees_pb2
        from ..payments.v1 import label_pb2, transaction_pb2
        pipe = pipe or pipeline()
        missing = [t for t in TYPES if t not in pipe.event_types]
        if missing:
            raise ValueError(f"traffic.status: pipeline {pipe.name} has no event types {', '.join(missing)} "
                             f"(use a pipeline with the status change feeds, e.g. payments-status)")
        self.spec, self.world, self.rows, self.rng, self.gen_id = spec, world, rows, rng, gen_id
        self.pb_card, self.pb_account, self.pb_payee = cards_pb2, accounts_pb2, payees_pb2
        self.cs = transaction_pb2.CardStatus
        self.lost_stolen = label_pb2.FraudType.Value("LOST_STOLEN")
        self.account_fraud = {label_pb2.FraudType.Value(n) for n in ACCOUNT_FRAUD}
        self.report_sources = {label_pb2.LabelSource.Value("CUSTOMER_REPORT"), label_pb2.LabelSource.Value("CHARGEBACK")}
        self.key_entity = {t: pipe.partition_key_of(t) for t in TYPES}
        self.heap: list[tuple[int, int, str, bytes, bytes]] = []
        self.seq = 0
        self.started = False
        self.card_block: dict[int, tuple[int, int]] = {}    # card -> (from, until) of its latest block (until: unblock)
        self.frozen: set[int] = set()
        self.aml_until: dict[int, int] = {}                 # account -> end of its AML hold
        self.fraud_hits: collections.Counter = collections.Counter()
        self.payee_labels: collections.Counter = collections.Counter()
        self.counts: collections.Counter = collections.Counter()   # (type, reason) -> changes scheduled
        self.dropped = 0                                    # genuine payments not made on a blocked card

    # ------------------------------------------------------------------ scheduling
    def _id(self) -> int:
        self.seq += 1
        return (self.gen_id << 48) | (1 << 45) | self.seq

    def _push(self, t: int, ty: str, msg: Any, key: int, reason: str) -> None:
        eid = self._id()
        msg.event_id = eid
        msg.event_time_us = int(t)
        heapq.heappush(self.heap, (int(t), eid, ty, msg.SerializeToString(), str(int(key)).encode()))
        self.counts[(ty, reason)] += 1

    def _card(self, t: int, customer: int, card: int, status: int, reason: str, source: str, until: int = FOREVER) -> None:
        if status != self.cs.Value("CARD_STATUS_ACTIVE"):
            cur = self.card_block.get(card)
            if cur is not None and cur[1] == FOREVER and cur[0] <= t:
                return                                       # already blocked for good: nothing changes
            self.card_block[card] = (int(t), int(until))
        m = self.pb_card.CardStatusChanged(customer_id=int(customer), card_id=int(card), status=status, reason=reason,
                                           source=source)
        self._push(t, CARD, m, customer if self.key_entity[CARD] == "customer" else card, reason)

    def _account(self, t: int, customer: int, account: int, status: int, flags: list[str], reason: str, source: str) -> None:
        m = self.pb_account.AccountStatusChanged(customer_id=int(customer), account_id=int(account), status=status,
                                                 flags=flags, reason=reason, source=source)
        self._push(t, ACCOUNT, m, customer if self.key_entity[ACCOUNT] == "customer" else account, reason)

    def _payee(self, t: int, payee: int, reason: str) -> None:
        m = self.pb_payee.PayeeListingChanged(payee_id=int(payee), listed=True, list="MULE", reason=reason, source="FRAUD_OPS")
        self._push(t, PAYEE, m, payee, reason)

    def _initial(self, t0: int) -> None:
        """The statuses already in place when the simulation starts (this shard's customers)."""
        s, w = self.spec, self.world
        cust_ids = w.customers["customer_id"]
        if s.initial_blocked_cards > 0:
            owner = w.cards["customer_row"].astype(np.int64)
            mine = np.flatnonzero(np.isin(owner, self.rows))
            pick = mine[self.rng.random(mine.size) < s.initial_blocked_cards]
            statuses = self.rng.choice([self.cs.Value("CARD_STATUS_BLOCKED"), self.cs.Value("CARD_STATUS_LOST"),
                                        self.cs.Value("CARD_STATUS_STOLEN")], size=pick.size, p=[0.5, 0.25, 0.25])
            for i, st in zip(pick.tolist(), statuses.tolist()):
                self._card(t0, int(cust_ids[owner[i]]), int(w.cards["card_id"][i]), int(st), "INITIAL", "CARD_MANAGEMENT")
        if s.initial_listed_mules > 0 and self.gen_id == 0:
            # payees belong to no customer: the first shard lists the known mules for every shard
            mules = np.flatnonzero(w.payees["is_mule"])
            for i in mules[self.rng.random(mules.size) < s.initial_listed_mules].tolist():
                self._payee(t0, int(w.payees["payee_id"][i]), "INITIAL")
                self.payee_labels[int(w.payees["payee_id"][i])] = max(s.list_payee_after, 1)   # listed: not again
        if s.initial_aml_holds > 0:
            pick = self.rows[self.rng.random(self.rows.size) < s.initial_aml_holds]
            for r in pick.tolist():
                acct = int(w.account_id(r))
                self.aml_until[acct] = FOREVER
                self._account(t0, int(cust_ids[r]), acct, self.pb_account.ACCOUNT_STATUS_OPEN, ["AML_HOLD"], "INITIAL", "AML")

    # ------------------------------------------------------------------ per batch
    def observe(self, c: dict[str, np.ndarray], t0: int, t1: int) -> np.ndarray:
        """Schedule the changes this batch's payments cause; the rows to keep (genuine payments on a card that is
        blocked at their time are not made)."""
        s = self.spec
        if not self.started:
            self.started = True
            self._initial(t0)
        n = int(c["event_time_us"].shape[0])
        if n == 0:
            return np.ones(0, bool)
        t = c["event_time_us"].astype(np.int64)
        card, cust = c["card_id"].astype(np.int64), c["customer_id"].astype(np.int64)
        acct, payee = c["account_id"].astype(np.int64), c["payee_id"].astype(np.int64)
        fraud = c["is_fraud"].astype(bool)
        order = np.argsort(t, kind="stable")
        # the issuer's monitoring: a compromised card blocked at its k-th fraudulent payment
        if s.issuer_block_after > 0:
            for i in order[fraud[order] & (card[order] != 0)].tolist():
                k = int(card[i])
                self.fraud_hits[k] += 1
                if self.fraud_hits[k] == s.issuer_block_after and self.rng.random() < s.issuer_block_rate:
                    self._card(t[i] + int(s.issuer_block_delay_s * 1e6), cust[i], k, self.cs.Value("CARD_STATUS_BLOCKED"),
                               "ISSUER_MONITORING", "FRAUD_OPS")
        # reported fraud, at its label's time: the card blocked (lost / stolen), an account taken over frozen, a
        # payee listed at its k-th label
        lab = np.flatnonzero((c["label_delay_us"] >= 0) & c["label_is_fraud"].astype(bool))
        for i in lab[np.argsort(t[lab], kind="stable")].tolist():
            lt = int(t[i] + c["label_delay_us"][i])
            ftype = int(c["fraud_type"][i])
            if card[i] != 0 and s.report_blocks and int(c["label_source"][i]) in self.report_sources:
                st = "CARD_STATUS_STOLEN" if ftype == self.lost_stolen else "CARD_STATUS_BLOCKED"
                self._card(lt, cust[i], int(card[i]), self.cs.Value(st), "CUSTOMER_REPORT", "CARD_MANAGEMENT")
            if (card[i] == 0 and acct[i] != 0 and ftype in self.account_fraud and int(acct[i]) not in self.frozen
                    and self.rng.random() < s.freeze_on_report):
                self.frozen.add(int(acct[i]))
                self._account(lt, cust[i], int(acct[i]), self.pb_account.ACCOUNT_STATUS_FROZEN, [], "FRAUD_CONFIRMED",
                              "FRAUD_OPS")
            if payee[i] != 0 and s.list_payee_after > 0:
                self.payee_labels[int(payee[i])] += 1
                if self.payee_labels[int(payee[i])] == s.list_payee_after:
                    self._payee(lt, int(payee[i]), "FRAUD_LABELS")
        # noise: temporary card locks and AML holds, at a random time of the batch, lifted later
        genuine = np.flatnonzero(~fraud)
        for rate, col, kind in ((s.temporary_locks_per_1k, card, "lock"), (s.aml_holds_per_1k, acct, "aml")):
            k = int(self.rng.poisson(rate * n / 1000.0)) if rate > 0 else 0
            cand = genuine[col[genuine] != 0]
            if k == 0 or cand.size == 0:
                continue
            for i, at in zip(self.rng.choice(cand, size=k).tolist(), self.rng.integers(t0, t1, size=k).tolist()):
                if kind == "lock":
                    if int(card[i]) in self.card_block and self.card_block[int(card[i])][1] > at:
                        continue
                    until = at + int(s.lock_duration_s * 1e6)
                    self._card(at, cust[i], int(card[i]), self.cs.Value("CARD_STATUS_BLOCKED"), "TEMPORARY_LOCK",
                               "MOBILE_APP", until)
                    self._card(until, cust[i], int(card[i]), self.cs.Value("CARD_STATUS_ACTIVE"), "UNLOCK", "MOBILE_APP")
                else:
                    a = int(acct[i])
                    if self.aml_until.get(a, 0) > at or a in self.frozen:
                        continue
                    until = at + int(s.aml_hold_duration_s * 1e6)
                    self.aml_until[a] = until
                    open_ = self.pb_account.ACCOUNT_STATUS_OPEN
                    self._account(at, cust[i], a, open_, ["AML_HOLD"], "AML_REVIEW", "AML")
                    self._account(until, cust[i], a, open_, [], "RELEASED", "AML")
        keep = np.ones(n, bool)
        if s.genuine_stop_on_block and self.card_block:
            blocked = np.fromiter(self.card_block.keys(), np.int64, len(self.card_block))
            for i in np.flatnonzero(~fraud & np.isin(card, blocked)).tolist():
                lo, hi = self.card_block[int(card[i])]
                if lo <= t[i] < hi:
                    keep[i] = False
            self.dropped += int((~keep).sum())
        return keep

    def due(self, t1: int) -> list[Record]:
        """The changes that happen before t1, as typed Records (order -2: before a payment at the same microsecond)."""
        out = []
        while self.heap and self.heap[0][0] < t1:
            t, eid, ty, payload, key = heapq.heappop(self.heap)
            out.append(Record(t, ty, payload, key, eid, False, -2))
        return out

    def summary(self) -> dict[str, Any]:
        return {"changes": {f"{ty}:{reason}": n for (ty, reason), n in sorted(self.counts.items())},
                "genuine_payments_not_made": self.dropped, "pending": len(self.heap)}
