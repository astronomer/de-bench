# LF-1042 — why the replayed settlement week went out double

**What doubles it.** `projects/commerce/sql/settlement_publish.sql` is a bare
`INSERT INTO copperline.marts.settlement_weekly`. Nothing removes the week that
is already in the mart, so every run of a week adds a second set of rows under
the same `week_start`. `CONVENTIONS.md`, "Writing to the warehouse", says it in
as many words: a bare INSERT into a dated table is a defect, and the second run
of an interval puts the rows in twice. `contracts/settlement-summary.md` SS-3
says the same from the other side — a re-run for a week that has already gone
out replaces that week's rows and does not add to them.

`publish_week` now reads that statement as a plain SELECT and writes the rows
through `warehouse.delete_insert`, keyed on `week_start`. So a replay replaces
the week it was asked for, still corrects the figures on it, and leaves every
other week where it was.

**One week, not seven.** Seller-ops re-ran 2 to 8 February and the run history
holds nine runs across those seven dates. Every one of them published the same
thing: the fiscal week that opened on **2026-02-01**. `week_bounds` takes the
week from `raw.fiscal_calendar` for the day before `ds`, and 2026-02-01 to
2026-02-07 is one fiscal week — FY2026 W1, the first week of the year. So the
replay was nine copies of one week rather than one copy of each of seven weeks,
which is why exactly one week's sellers are on the phone.

**The two-feed overlap is not the cause.** `apply_processor_boundary` cannot
remove anything at all: `settlement_week_lines.sql` stamps every row it builds
`'marketplace'`, and the DELETE only looks at rows whose feed is `'processor'`.
The step deletes nothing and never has. That is worth its own ticket, but it is
not this. The overlap it was written for ended on 2025-09-01
(`docs/billing-integration.md` B-8), five months before the week that doubled,
and the summary is built from the marketplace remittance rather than from either
processor feed. A single feed cannot double itself.

**Where else.** The same shape sits on every publish commerce owns.
`economics_publish.sql`, `close_publish_economics.sql`, `returns_publish.sql`,
`refunds_publish.sql`, `cart_abandon_publish.sql`, `recon_publish.sql` and
`payments_restore_day.sql` are all bare INSERTs into a mart, and three of them
carry a comment saying they replace the partition. Nothing has replayed those
marts the way seller-ops replayed this one, which is the only reason nobody has
seen it. One ticket each rather than a sweep — every one of them keys on a
different column.
