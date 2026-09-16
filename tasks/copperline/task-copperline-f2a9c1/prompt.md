# SC-1204 — shrink has read zero since February

Loss prevention pulled the shrink pack for the quarter and every line on it is
0.00. They went back through the weeks and the last one carrying money closed in
January. Nobody caught it in four months, because a zero on that page reads as a
good week.

The counts are landing. `sc_shrink_weekly` runs every Thursday, it finishes
green, `marts.shrink_weekly` still gets a row per location per department, and
`counted_delta` still moves week to week. Only `shrink_cents` is dead.

Work out why, and fix the job so the money comes back.

Four things have to hold when you are done.

- The weeks that closed before February keep the numbers they have now. Finance
  closed those periods on the figures they were given and nobody is reopening
  them.
- The weeks since have to be valued the way the business values stock today.
  What the feed sends changed at the turn of the fiscal year and the new rule is
  not in the data — someone wrote it down at the time, and that write-up is the
  only statement of it.
- Shrink is a loss. `shrink_cents` comes out negative, the way it always has.
- The table keeps its shape. The pack selects `week_start`, `location_id`,
  `dept_code`, `counted_delta` and `shrink_cents` by name.

Two notes from K. Duffy, who owns the job.

- The columns the feed sends now describe the whole position sitting in the bin,
  not one unit of it. A counted delta is units, so something has to bring the
  two onto the same footing before either means anything.
- Leave the match alone. A count only picks up a snapshot row when the two land
  on the same day, and most of them do not — that is SC-1188 and merchandising
  owns it. This ticket is only about what a matched row is worth.

For scale: through the autumn the whole estate ran a couple of hundred thousand
dollars of shrink a week. Whatever February comes out at, it is not zero and it
is not tens of millions.

This is one job. The dbt project and the other teams' trees are not ours this
week, however tempting anything you find in them looks.
