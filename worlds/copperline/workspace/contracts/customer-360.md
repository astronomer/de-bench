# Contract: customer 360

Schema: `contracts/customer_360.yml`. Consumer C-8 in `docs/report-registry.md`. Owner: customer. Publisher: `cus_customer_360_daily`, published by `cus_360_publish`. Agreed 2023-08-30, last amended 2026-03-02.

One row per trade account with contact, order and payment attributes. Support reads it all day, from the warehouse view and from the service desk.

## CU-1 One row per real account

The grain is one row per real trade account. Not one per account record: where two records are the same customer, the merge decision in `ops.merge_candidates` applies and the account appears once. Where two records are different customers that look alike, both appear. `docs/runbooks/northwave-integration.md` §NWI-1 says the reviewed table is the source of that decision and a re-derived match is not.

An account that was deleted at source is not silently dropped. It leaves the published view and stays reconstructable, per `docs/retention-policy.md` §RET-6.

## CU-2 Slowly-changing attributes and same-day ties

Account attributes come from a slowly-changing dimension. The row published for a day carries the version in force at the end of that day. Where two versions of an attribute carry the same effective date, take the last by `updated_at`, and where those tie as well, take the source with the higher priority: Copperline OMS, then Halyard, then the acquired book.

Ties happen. The rule is written down so that two rebuilds of the same day produce the same row.

## CU-3 Columns

`customer_id`, `account_name`, `source_book`, `status`, `first_order_date`, `last_order_date`, `orders_12m`, `net_sales_cents`, `open_disputes`, `primary_contact_hash`, `region`, `updated_at`.

`net_sales_cents` is the trailing twelve months and is defined in `docs/semantic-definitions.md`. Contact detail is hashed in the published view; the unhashed values stay in the warehouse and are covered by the privacy contract.

## Notes

- The warehouse count of accounts and the source API's own count do not agree. Deletes have never flowed through the sync, so the warehouse holds accounts the source no longer has.
- `source_book` is `copperline` or `northwave` and it stays on the row after a merge, carrying the book the surviving record came from.
- This is the slowest model in the estate: eight joins over two years of events, and no incremental build.

## Open items

- TODO: the deletes problem above has been known since the acquisition and has never been sized. Somebody should count it.
- `orders_12m` counts orders, not order lines, and merchandising ask about it every few months.
- The service-desk sync is a full replace every night. There is no delta path and nobody has needed one.
