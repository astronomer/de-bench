# MIG-84 — the run-calendar gate wave 2 owes the boxes it moves

Wave 2 takes the tie-outs, the distribution packs and the month end off the old
scheduler. Every one of those boxes runs on a run calendar today, and nothing in
this deployment can answer what that calendar says about a night. I want the
gate written before the first box comes across, and I want to know what that
calendar has been doing to us while it sat over there.

`legacy/autosys/copperline.jil` is the scheduler's own export and
`legacy/autosys/calendars/retail_2026.cal` is the calendar the boxes obey.
`CONVENTIONS.md` says `legacy/` is read-only. Read all of it. Do not edit any of
it and do not add anything to it.

`sc_autosys_bridge` has a calendar reader of its own and a ticket of its own.
Leave that DAG alone. What wave 2 needs is a gate its converted DAGs can call,
and it has to answer for every night this deployment runs: that export was cut
for one year, we were running before it and we will be running after it. The
same fact is already in this warehouse, for the home market, for the years the
export does not cover. Find it and read that.

## 1. The gate

`projects/supply/lib/run_calendar.py`. Two functions, these names, because the
converted DAGs are going to import them:

- `calendared_jobs(calendar)` — every job in the export that the named run
  calendar governs. AutoSys starts a job inside a box only when its box starts,
  so a calendar on a box reaches everything that box holds: `box_name:` is the
  edge and the export is a tree. Answer for any calendar name the export
  mentions, not only the one wave 2 cares about.
- `is_run_day(ds)` — whether those jobs may start. `ds` is an ISO date and it
  means what `CONVENTIONS.md` says `ds` means. A date the calendar holds no row
  for is not a no: raise, and name the date. A gate that answers "not a trading
  day" for a night it has never heard of stops the estate and does it quietly.

Read the calendar once and hold it if you like. A night asks about one date and
a backfill asks about hundreds.

## 2. Three answers, in `RESPONSE.md` at the root of the working tree

- **How many jobs the gate has to cover** — the ones the trading-day calendar
  governs — and how many of those carry it on a line of their own. Both numbers.
- **Which business dates the tie-outs have never run for.** Say it in days of
  the week, and say what makes it so. This is the half I care about: finance
  read those tie-outs every morning, and I want to know what has never been in
  them before we carry the behaviour across.
- **`ops.load_control.history_complete`.** I was told that column says whether a
  night's history closed. Can wave 2 use it to tell which of these nights the
  old chain actually finished? Say what it holds.

Keep it short. It goes in front of the wave 2 plan on Friday.
