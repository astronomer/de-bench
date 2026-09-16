# Comparable-store policy

Maintained by finance-eng with the retail operations team. Last reviewed 2026-02-24. Source tables: `raw.stores`, `raw.fiscal_calendar`.

This is the only authority on comparability at Copperline. `docs/finance-policy.md` governs recognition and never defines which stores are comparable; this document governs which stores are comparable and never defines what a sale is worth. Neither one overrides the other, because they answer different questions.

Comp sales is the number the board reads first and the number the trade press quotes. It is also the number where a reasonable, uniform rule gives the wrong answer, because Copperline's convention is not the industry's. Read all four clauses before you compute one.

## CMP-1 The opening test

A store is comparable for a fiscal period once it has been open for 13 full fiscal months at the end of that period. Thirteen, not twelve.

## CMP-2 Closures restate both years

A store that closes during a fiscal year leaves comp for every period of that year, current year and prior year alike, and figures already published are restated. A comp figure recomputed after a closure legitimately differs from the figure published before it.

## CMP-3 Remodels

A store shut for remodel for more than 21 consecutive days leaves comp for the affected fiscal period and for the same period in the prior year, then returns.

## CMP-4 Acquired stores

An acquired store enters comp 13 full fiscal months after the acquisition close, not 13 months after its own opening date.

## Why thirteen

The extra month is Copperline's own convention and it dates from the builders'-yard era, when a new store's first year ran on contractor word of mouth and the twelfth month still looked nothing like a settled one. Finance kept the thirteenth month when the estate grew and has never been persuaded to drop it. Every retailer you might compare against uses twelve. Copperline uses thirteen, and a figure computed on twelve is not a Copperline comp figure.

## Why the acquisition date and not the opening date

The 44 stores Copperline bought with Northwave Supply are between three and fifteen years old. Read `opened_on` alone and every one of them clears CMP-1 on the day the deal closed, which puts 44 stores into comp about a year early. The total still looks plausible — it moves the headline by roughly four points — and every per-region figure is wrong.

CMP-4 exists because the comparison is against Copperline's own prior year, and Copperline had no prior year for those stores. `raw.stores` carries `opened_on` and `acquired_from` for exactly this reason. With one of the two columns the rule cannot be applied at all.

## Restatement, and which figure is current

CMP-2 means a published comp figure has a shelf life. When a store closes, the periods already reported are recomputed without it, on both sides of the comparison, and the recomputed figure replaces the published one. The published figure was right when it was published and is not right now. Anyone quoting a comp number should say when it was computed.

## What `raw.stores` carries

| Column | What it holds |
|---|---|
| `store_id` | `S-####` |
| `opened_on` | the date the store first traded, under any owner |
| `closed_on` | NULL while trading |
| `acquired_from` | `'northwave'` on the acquired estate, NULL on the rest |
| `remodel_start`, `remodel_end` | the shut window, NULL when there was none |
| `region`, `tz_name` | region for reporting, IANA zone for the POS batches |

The table is SCD2, so a store that moved region has more than one row. Comparability is a property of the store, not of the row: take the store once.

## Open items

- CMP-3 counts consecutive days shut, not the number of days the remodel was budgeted for. Two stores in FY2025 ran over budget by a week and dropped out of comp because of it. Operations asked for a materiality floor and finance said no. TODO: revisit if it happens a third time.
- Nothing here covers a store that changes format — a yard converted to a full store. There have been two. Both were treated as continuing stores. That is a precedent, not a clause.
- The retail operations wiki page on store lifecycle states the twelve-month rule. It is wrong and it predates this document; the link from the operations handbook has been dead since the intranet move and nobody has missed it.
