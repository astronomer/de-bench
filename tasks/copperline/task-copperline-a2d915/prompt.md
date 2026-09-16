# FIN-524 — the rate intake has never landed a rate

`fin_fx_rates_intake` runs at 05:00 and it goes red every morning. Ironwood's
file arrives, the wait clears at about 05:12, and `land` is the step that dies.
This is what it says:

```
Invalid Input Error: Error when sniffing file "landing/fx/dt=.../rates.csv"
It was not possible to automatically detect the CSV parsing dialect
...
* Columns are set as: "columns = { ... }", and they contain: 4 columns. It
  does not match the number of columns found by the sniffer: 5. Verify the
  columns parameter is correctly set.
```

The controller's office has been loading the file by hand every morning since
the job was rendered, which is why `raw.fx_rates` looks healthy and why nobody
has chased this. The person who does it is on maternity leave from the 6th.

And `ops.fx_rate_coverage` has never held a row, so nobody can see whether a
morning's file came in whole. Leave the table itself alone: nothing in the
estate creates a rollup's target and platform have that on their list, which is
a hole of its own and not this ticket. The step is ours, and the step has never
run.

Get the job landing the file, and get the coverage step counting.

- `raw.fx_rates` is the record of what Ironwood sends. Every converting model
  reads it through `stg_reference__fx_rates`, REV-7 pins the unit, and the
  table is not moving. The job has to agree with the table, not the other way
  round.
- The load keeps replacing the day it is loading rather than appending to it.
  The file is a delivery and it gets re-sent.
- The coverage row is what somebody reads to see a morning arrived whole, so
  `currency_count` has to be the number of currencies the day carries. Keep
  that column name — it is what the coverage table is for.
- Do not write a currency into the job. We add one every time we open a market
  and a list in a yaml file is a list that goes stale.
- It stays a rendered job with the four steps it has.
  `include/lib/blueprint/` is the platform team's shared library; drive it, do
  not rewrite it.
- `dbt/`, `contracts/` and `fixtures/` are not this ticket. Neither is anybody
  else's project directory.

— M. Feld, finance-eng
