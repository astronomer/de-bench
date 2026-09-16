# Runbook: store POS ingestion

Owner: commerce. Review date 2026-05-05. Reads: `landing/pos/dt=<ds>/store_<S-####>.csv`. Writes: `raw.pos_sales_header`, `raw.pos_sales_lines`, `raw.pos_batch_manifest`.

This runbook covers the nightly store batches from the Northgate registers: what arrives, what to do when a store is missing, and how the till reconciliation is worked. It is advisory. Where it and the code disagree, `docs/change-management.md` §CM-3 says the code is right.

## What arrives

One CSV per trading store per night, named for the store, under the day's landing partition. 268 stores, so a complete night is 268 files. `store_pos_intake` runs hourly and picks up whatever has landed; `store_pos_late_catchup` runs at 04:00 and takes the stragglers.

`raw.pos_batch_manifest` holds one row per store per business date with the file name, the row count the store claims and the row count we loaded. It is the first place to look when a number is wrong.

## POS-1 Missing and late stores

A store that has not sent by 04:00 is late, not missing. A store's batch file can arrive up to three days after its business date, and when it does, every row in that file arrives at once — the rows themselves carry no lag of their own. Do not backfill a store that is merely late, and do not treat a manifest row with no file as a zero-sales day.

A store that has sent nothing for three business days is escalated to the store systems team. Check `raw.market_calendar` first: a store in a market with `feed_expected = false` sent nothing because it was shut.

## POS-2 Till reconciliation

Work the till reconciliation off the manifest, store by store, comparing the register's declared takings to the sum of the loaded lines. Differences of a few cents are float rounding at the register and are written off. Differences at the level of a whole transaction are a partial file, and the fix is a reload of that store's file rather than a repair of the rows.

Two things about the timestamps make this awkward and it is worth knowing both. The registers do not post transactions as they happen: the software holds the day and posts one close batch after the store shuts, so every line in a pre-cutover batch carries the batch time, `23:05:00`, in the store's own local wall clock rather than the time of the sale. From the UTC standardization on 2025-11-03 the registers began sending per-transaction times and `event_time_utc` is populated from that date, while `event_time_local` stays on the row for the store teams. So a reconciliation over a window that spans that date is reading two different kinds of timestamp, and the pre-cutover side has no intraday detail to reconcile against at all.

`raw.stores.tz_name` carries the IANA zone for every store and has done since the table was written.

## Reloading a store's day

1. Confirm the file is complete: the manifest's claimed count against the file's line count.
2. Ask the store systems team to resend if it is not.
3. Reload through the intake DAG for that store and business date. Do not hand-load into the table.
4. Re-check the manifest row afterwards.

A reload replaces the store-day. It does not append. If a store-day's totals double after a reload, the load path appended and that is a defect worth raising, not a data problem to clean up.

## The tables, and what each is for

| Table | Grain | What it is for |
|---|---|---|
| `raw.pos_sales_header` | one row per transaction | the transaction and its stamps |
| `raw.pos_sales_lines` | one row per line | what was sold |
| `raw.pos_batch_manifest` | one row per store per business date | what the store says it sent, and what we loaded |
| `raw.pos_sales_daily` | one row per store per day | the summary slab for years whose detail has aged out |

`raw.pos_sales_daily` is not built from the other three. It is a summary that was kept when the detail behind it aged out under `docs/retention-policy.md` §RET-2, and it covers years the detail tables do not reach. Comp figures for those years come from it.

## Who to call

| Symptom | Who |
|---|---|
| a store sending nothing | store systems |
| a file that parses but has the wrong columns | store systems, and raise it as a release change |
| totals that doubled after a reload | the commerce rotation |
| a manifest count that does not match the file | store systems |
| a store missing from `raw.stores` | the customer team, who own the dimension |

## Things that look like problems and are not

- A whole region reporting nothing on the same day. Check the market calendar; it is usually a holiday.
- 23:05 on every row of an old day. That is the close batch, and it is what the registers sent.
- A store with two rows in `raw.stores`. The table is SCD2 and the store moved region.
- Sunday counts well below Saturday's. That is the weekly shape, not a feed problem.

## Open items

- The manifest has no row at all when a store sends no file, so "late" and "never sent" look the same until three days have passed.
- TODO: the intake reads the file name for the store id rather than a column, so a renamed file lands under the wrong store. It has happened once.
- The register software version is not recorded anywhere. When a store's format changes we find out from the load.
