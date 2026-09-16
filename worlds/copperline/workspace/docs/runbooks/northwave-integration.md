# Runbook: the Northwave Supply integration

Owner: customer, with the integration team. Review date 2026-03-24. Advisory, per `docs/change-management.md` §CM-3.

Copperline bought Northwave Supply on 2025-02-03: 44 stores, 1,400 trade accounts, its own account numbering and its own product hierarchy. Northwave's account book landed in the warehouse on 2025-03-17 and has sat beside Copperline's ever since. This runbook is the state of the integration, which is unfinished, and it is unfinished on purpose rather than by neglect — the international launch took the team and nothing came back.

## What is in the warehouse

| Table | What it holds |
|---|---|
| `raw.nwv_accounts` | 1,400 rows, `NWA-` ids, Northwave's own book as it stood |
| `ops.merge_candidates` | 300 reviewed pairs |
| `raw.stores.acquired_from` | `'northwave'` on 44 rows |
| `raw.orders.source_system` | `'nwv'` on the acquired book's orders |
| the `nwv` schema | frozen copies of Northwave's own reporting tables |

The two books are not merged. Every customer-facing model carries `source_book` for that reason, and a join across the two goes through the crosswalk rather than on a natural key.

## NWI-1 Merge decisions

`ops.merge_candidates` is the source of record for which Copperline account and which Northwave account are the same customer. It holds `customer_id`, `nwv_account_id`, `confidence`, `decided_by` and `decided_on`, and the rows in it were decided by people, not by a matcher. Take the merge from that table. Do not re-derive a merge by matching names, addresses or domains: the review is the decision, and matching produces a different set.

The population it was built from is genuinely hard. Some of the same customers trade under different names on the two books, some share a domain and are not the same business, and married names, `info@` addresses and resellers all appear. That is why it was reviewed by hand once and has not been re-run.

## NWI-2 The frozen namespace

The `nwv` schema stopped refreshing on 2025-09-30. It is a frozen copy of Northwave's own reporting tables as they stood on that date, kept so that pre-acquisition figures can still be produced. Nothing writes to it, and any figure taken from it is a September 2025 figure whatever date you ask it for. It is not a live source and must not be joined to current data as though it were.

## Comparable stores

The 44 acquired stores enter comp on the acquisition clock, not their own. `docs/comp-store-policy.md` §CMP-4 owns that rule and this runbook does not restate it. The stores are old — the oldest opened in 2011 — so the wrong reading is the plausible one.

## The shape of the merge list

`ops.merge_candidates` holds 300 pairs. Every one was looked at by a person during the integration review and carries who decided it and when.

| Column | What it holds |
|---|---|
| `customer_id` | the Copperline account, `C-######` |
| `nwv_account_id` | the Northwave account, `NWA-#####` |
| `confidence` | the reviewer's own rating, 0 to 1; 1.0000 on the tax-id matches |
| `method` | `deterministic_tax_id` where the federal tax ids agreed, `manual_review` for the rest |
| `decided_by` | the reviewer |
| `decided_on` | the review date; the tax-id matches cleared in the first week after the book landed, the manual ones ran into late June 2025 |
| `note` | the reviewer's one line on what carried the decision |

`confidence` records how sure the reviewer was, not how well two strings matched. A low-confidence row is still a decision.

## Working with the two books

- Every customer-facing model carries `source_book`, either `copperline` or `northwave`.
- A join across the books goes through the crosswalk staging model, never on a name, an address or a domain.
- A model that reads dates before 2025-02-03 states the era in its description, because the acquired book's history predates Copperline's ownership of it.
- `raw.orders.source_system = 'nwv'` marks the acquired book's orders. It is not the same thing as `source_book` on the account, and an account can have orders on both.

## What was never done

- The account books were never merged.
- The product hierarchies were never mapped. Merchandising finance did the department mapping for the FY2026 valuation change and that is as far as it went.
- Northwave's own reporting was never retired; it was frozen.
- The trade terms on the acquired accounts were never conformed to Copperline's.

## Open items

- Nobody owns the merge any more. The integration team was disbanded into the market-setup work.
- TODO: `ops.merge_candidates` has no row for accounts reviewed and rejected, so an account absent from the table might have been looked at or might not.
- The acquisition wiki space is gone. The deal documents are with legal.
