# GRO-418 — the mornings we replayed all came back with the newest morning's numbers

Finance landed a run of late FX rates on Thursday. The mornings of 11 to 15 May
had been reported with currencies we held no rate for, so we replayed those five
mornings of `gro_marketing_spend_intake` through the backfill broker to recount
the gaps. Every run finished green.

The delivery report the campaign team read on Monday now shows the same five
currency gaps against all five mornings, and the same restatement list against
all five. It is the answer for the last morning the intake had run before the
replay, repeated five times. The report is built from the run record — the
`restatements` and `fx_coverage` entries the DAG writes for the morning it ran.

Nobody can show from the record that those five mornings re-landed at all.

Find out why a replayed morning comes back with another morning's answers, and
fix it. After the fix:

- A run for a morning records that morning's restatements and that morning's
  missing rates. It records them whatever the DAG has run since and whatever
  order the mornings are run in.
- A run for a morning lands that morning's three deliveries. A morning whose
  rows are not in `raw.ads_spend_daily` gets them back by being run again.
- The morning-after-morning path does not change. Tonight's run must land and
  record what tonight's run lands and records today.

`projects/growth/lib/ads.py` is right and is not the place to change this.
