# PAY-264 — a third of the legacy card feed does not say what currency it is in

Finance is restating two years of card money for the close, and the legacy processor is the
part that will not go through. `raw.pay_halcyon_settlements` carries a `currency` column and
about a third of its rows arrive with it empty.

For most of the feed's life that cost nothing. Copperline sold in the United States only, so
an empty currency and a dollar amount said the same thing. The international launch on
2025-04-07 ended that. From then on some of what Halcyon settled is pounds and euros, and it
lands in that same empty column beside the dollars.

Finance cannot convert an amount that does not say what it is, and reading the empty ones as
dollars converts a pound at one to one. Halcyon is gone, the feed is frozen, and nobody is
going to resend it. What the row does not say has to come off what it does.

Work it out once, into the warehouse, so that everything reading this feed reads the same
currency.

`docs/billing-integration.md` says what the fields mean.

## What to build

A one-off job, `halcyon_currency_backfill`, in `projects/commerce/dags/`. It fills
`ops.halcyon_settlement_currency`, one row for every settlement in
`raw.pay_halcyon_settlements`:

| column | what it holds |
|---|---|
| `txn_id` | the settlement, as the feed spells it |
| `currency_code` | the currency that settlement was taken in |
| `amount_cents` | what the settlement is worth, in that currency |
| `settled_on` | the day the money settled |

The table is not in the warehouse yet. The feed is frozen and read-only, so nothing writes
back to it.

## What it has to hold

- One row per settlement, over the whole feed. Every row, whatever state it ended in and
  whatever date it carries: a reversal and a payment still pending were both taken in a
  currency.
- `currency_code` is never null.
- Check what you fill against the rows that came filled. About two thirds of the feed states
  its currency. For any merchant account, what you fill in has to be what that account's own
  filled rows already say. Where the two disagree, the fill is wrong.
- `amount_cents` is an integer number of cents. `CONVENTIONS.md` says money is cents in every
  table at every step, and this feed is the one place in the estate that lands it as text.
- It is a one-off over a frozen feed, so triggering it with no configuration does the whole
  feed, and a second run leaves one copy of each row.

Finance reads this straight into the conversion. A row in the wrong currency is money in the
wrong place, not a label.
