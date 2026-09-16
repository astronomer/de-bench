# Contract: privacy queue

Schema: `contracts/privacy_surfaces.yml`. Consumer C-12 in `docs/report-registry.md`. Owner: platform. Publisher: `plat_privacy_sweep`. Agreed 2023-01-10, last amended 2026-02-03.

Deletion requests arrive in `ops/privacy/deletion_requests.csv` and are worked across every surface personal data reached. This contract holds the list of surfaces. Nothing else does — several of them are invisible to dbt and to the Airflow graph, and a sweep that finds its own surfaces finds the warehouse and stops.

## PRV-1 The surface list

A deletion reaches all of these:

| Surface | Where |
|---|---|
| customer and account tables | `raw.customers`, `raw.nwv_accounts`, `marts.dim_customer` |
| the 360 table and its published view | `marts.customer_360` |
| support tickets and their contents | `raw.support_tickets` |
| the feature exports | `exports/features/{ds}.parquet` |
| the audience files | `exports/audiences/{ds}/`, and the two ad platforms |
| the partner-share extract | `exports/partner_share/` |
| the service-desk sync | `exports/service_desk/{ds}.json` |
| the soft-delete audit table | `audit.deleted_rows` |
| landing files still inside their retention window | the paths in `docs/retention-policy.md` |

The five surfaces below the warehouse rows are the ones a code-first sweep misses. They are the reason this list is prose and is the contract.

## PRV-2 Thirty days, and a receipt

A request is completed within 30 days of receipt, per `docs/retention-policy.md` §RET-4, and every completed request writes a receipt naming the surfaces reached and the date. A surface that could not be reached is named on the receipt as unreached, with the reason. A silent partial sweep is the failure this contract exists to prevent.

## PRV-3 What is not deleted

Published aggregates are not rebuilt to satisfy a deletion, per `docs/retention-policy.md` §RET-3. The row-level tables are deleted from and the aggregate keeps its published figure.

Records exempt under `docs/retention-policy.md` §RET-5 — the finance ledger, the invoice tables behind it, and anything under legal hold — are not deleted. The exemption is recorded on the request and on the receipt. Do not delete an exempt record and do not skip it without saying so.

## Notes

- Requests arrive from the service desk and from the web privacy form, and both land in the same CSV.
- The sweep runs daily at 13:00 and works whatever is outstanding.
- Deletion receipts are kept indefinitely, which is the point of a receipt.

## Open items

- TODO: nothing confirms the ad platforms actually removed a hashed identifier. The push is one-way.
- The landing files are in scope while they are in the retention window and out of scope once archived, which is a line nobody enjoys explaining.
- The list above is maintained by hand. A new export is on it when somebody remembers.
