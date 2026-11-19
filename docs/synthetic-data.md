# Synthetic payments world and fraud scenarios

`realy world build` creates a deterministic world (same seed ⇒ byte-identical content hash):
customers (segment, home city/country/timezone, tenure, income-driven ticket size, activity rate,
diurnal/weekly curves, channel preferences, travel propensity), 1–3 accounts and 1–4 cards each
(debit/credit/virtual/wallet, credit limits, contactless CVM limits), 1–3 devices with usual IPs/ASNs,
merchants with a realistic MCC mix and Zipf popularity (ATMs are merchants with MCC 6011), terminals,
and payees including mule accounts. 2M customers build in ~6 s.

## Legitimate behaviour (so false positives are real)

Per-customer non-homogeneous Poisson arrivals scaled to the target TPS with **time compression**
(`traffic.time_scale: auto` maps the population's natural rate onto the target TPS, so each customer
behaves realistically and "last 10 minutes" means 10 simulated minutes). Habitual / local / Zipf
online merchant choice, lognormal amounts per MCC × income, round ATM amounts, FX by merchant
country, chip/contactless/magstripe/e-commerce(3DS)/wallet entry modes, CVV/AVS, recurring billing,
travel episodes abroad, new phones, large one-off purchases and new payees.

## Fraud typologies (campaigns with ground truth)

| Typology | Pattern | Rules it should trip |
|---|---|---|
| card_testing | burst of < $2 card-not-present auths on 1–3 merchants, then cash-out | `CARD_TESTING`, `CARD_TESTING_MERCHANT` |
| account_takeover | new device + new IP country + high-value e-commerce | `ATO_NEW_DEVICE_*` |
| lost_stolen | card-present spree, contactless just under the CVM limit | `CONTACTLESS_SPLIT` |
| counterfeit | magstripe abroad while the genuine customer transacts at home | `IMPOSSIBLE_TRAVEL`, `MAGSTRIPE_FOREIGN` |
| merchant_breach | cards seen at a compromised merchant defrauded later | (weak by design — model only) |
| atm_cashout | thousands of cards × 3–10 ATM withdrawals in minutes (a load spike) | `ATM_VELOCITY`, `ATM_TERMINAL_CASHOUT` |
| app_scam_mule | large transfers to new payees fanning into mules | `APP_SCAM_NEW_PAYEE`, `MULE_FAN_IN` |
| bust_out | gradual credit build-up then maxing out | `BUST_OUT_UTILISATION` |
| bot_ecom | one device/IP rotating many cards at one merchant | `BOT_DEVICE_MANY_CARDS` |

Labels go to `payments.labels` with a chargeback delay (simulated days), with label noise
(unreported fraud, friendly-fraud chargebacks). Scenario timelines schedule campaigns at wall-clock
offsets (`fraud.campaigns`), on top of Poisson background campaigns (`fraud.background_per_1k`).

## Warm start

`traffic.backfill_days` replays simulated history unpaced (`x-backfill`, untraced) so Flink's keyed
state — device/payee/IP-country novelty in particular — is realistic before the measured run. Cold
state makes every first device "new": the false-positive rate drops from ~16% (cold) to ~6% with one
simulated day of history in `mixed-realistic`.

## Reproducibility

The world hash, the per-run dataset SHA-256 (sync rail) and the scenario fingerprint are recorded in
`runs/<id>/manifest.json`; generator output is byte-identical for the same seed and call sequence.
