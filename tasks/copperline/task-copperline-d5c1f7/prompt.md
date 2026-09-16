# REP-204 — the marketplace page will not stay in the report registry

Finance want a fifth rendered report: the marketplace day, by day and by seller.
How many orders, the gross value, and our commission.

Priya set it up on Tuesday the 2nd. She put `marketplace_daily_summary` into
`ops/report_registry.yml`, which is the list the ops tooling reads, and the
tooling listed it that same afternoon. Two things then went wrong. No file has
ever landed for it. And by Wednesday lunchtime the entry had gone from the
registry. She put it back on Thursday and it was gone again on Friday.

Set the report up so that it publishes, and so that it stays in the registry.

- Call it `marketplace_daily_summary`. It reads `marts.gmv_daily`.
- Day and seller is the grain. The order count, the gross value and the
  commission are the figures. Use the mart's own column names.
- It lands where the other four rendered reports land, under its own name.
- Finance owns it, the same as the other four.
- The four reports that publish today must not change: same names, same tables,
  same files.
