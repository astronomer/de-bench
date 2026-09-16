# Carrier rate policy

Maintained by supply-chain analytics with finance-eng. Last reviewed 2026-04-21. Source tables: `raw.carrier_rate_cards`, `raw.carrier_invoices`, `raw.shipments`.

Copperline ships with three carriers — Brightline Freight, Pallas Parcel and Merriweather Logistics — and rates every package itself rather than taking the carrier's invoice as read. This document says which card applies, how the lookup works, and how the arithmetic rounds. It exists because the carriers reissue cards mid-year and the invoices arrive rated on whichever card the carrier believed was in force.

## RATE-1 Which card is in force

This policy states which rate-card version applies over which era, and it decides. `raw.carrier_rate_cards` carries `effective_from` and `effective_to` values supplied by the carriers, and those values have been wrong twice: once when Pallas backdated a card by a month, once when Brightline shipped a card with no end date at all. Where the table and this policy disagree, the policy governs and the table is corrected.

| Carrier | Version | In force |
|---|---|---|
| Brightline Freight | `BL-2024A` | 2024-02-04 to 2025-01-31 |
| Brightline Freight | `BL-2025A` | 2025-02-01 to 2026-01-31 |
| Brightline Freight | `BL-2026A` | from 2026-02-01 |
| Pallas Parcel | `PP-24Q1` | 2024-02-04 to 2024-12-31 |
| Pallas Parcel | `PP-25Q1` | 2025-01-01 to 2025-12-31 |
| Pallas Parcel | `PP-26Q1` | from 2026-01-01 |
| Merriweather Logistics | `MW-2024` | 2024-02-04 to 2025-06-30 |
| Merriweather Logistics | `MW-2025H2` | from 2025-07-01 |

## RATE-2 Boundaries

`effective_from` is inclusive and `effective_to` is exclusive. A shipment on a card's `effective_to` date is rated on the next card, not on the one ending. Both values are dates, not timestamps; there is no hour at which a card changes.

## RATE-3 The lookup

A package rates on its zone and its weight break. The zone comes from the origin facility and the destination postal prefix, through the carrier's own zone matrix on the card. The weight break is the first break whose upper bound is greater than or equal to the billable weight, so a package exactly on a break rates in the lower band, not the higher one. Billable weight is the greater of actual weight and dimensional weight, computed with the divisor on the card in force.

## RATE-4 Rounding

Compute the charge per package, round half up to the cent, then sum. Never sum unrounded amounts and round the total, and never round to anything coarser than a cent at an intermediate step. Surcharges are computed on the rounded package charge and are themselves rounded half up to the cent before they are added.

## RATE-5 Re-rating

A re-rate uses the card in force on the ship date, never the card in force when the re-rate runs. Re-rating ninety days of shipments in June does not rate them on June's card. This is the whole point of RATE-1: a re-rate that reads today's card produces a number that has never been true of any shipment.

## Where the cost lands

`sc_shipping_cost_daily` builds `marts.fct_shipping_costs`, one partition a day. `fin_accrual_freight` reads the supply team's monthly accrual off the back of it. The finance workbook a person maintains by hand is a third path and is covered by `docs/memos/fy26-cost-restatement.md`, not here.

## Surcharges, and the one that moved

Fuel is the surcharge that matters. It is a percentage of the base charge, set weekly by each carrier, and it runs between 9% and 19% over the range in the cards. Accessorials — residential delivery, oversize, address correction — are flat amounts and are stable.

The fuel surcharge is also where the finance workbook and the warehouse stopped agreeing, because the workbook's `freight_cost` column changed meaning at the FY2026 boundary. `docs/memos/fy26-cost-restatement.md` §MEMO-1 states which side is gross of fuel and which is net. This document rates packages; it does not say what the workbook's column means.

## Open items

- The zone matrices are stored as a nested JSON blob on the card row. It works and nobody enjoys it. TODO: flatten to a table when someone has a week.
- Merriweather's `MW-2025H2` card arrived without a dimensional divisor and the value in the table was typed in from the contract PDF. It is right; it is not sourced.
- The carrier invoices are not a rate authority and are not reconciled to the penny. Differences under a dollar a package are written off without investigation, which is a decision from 2023 that nobody has revisited.
- TODO: the rate-card intake DAG lands whatever the carrier sends. It does not check the version against this table, so a backdated card lands silently and stays until someone notices a cost step.
