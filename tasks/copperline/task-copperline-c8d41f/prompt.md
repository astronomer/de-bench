# MIG-71 — the history bracket will not close a repaired night

`sc_pdi_history_complete` is what we trigger by hand when a night's Pentaho
loads have been re-run outside the schedule. It closes the history bracket for
that night and nothing else does.

It will not close one. Point it at 2026-03-11, a night `ops.load_control` shows
finished clean with every job `complete`, and it comes back saying a job did not
complete. It has behaved like that since the guard was rewritten in April.

The rewrite had the right idea. Closing over whatever rows the night happened to
write says nothing about a job that never started, so the guard now asks
`legacy/autosys/copperline.jil` what the night owed. What it asks the export for
is not what a night owes.

The failures since April are a separate ticket and somebody else has them. What
I need is this DAG working before those loads are repaired, because the morning
they are, we have two months of nights to close by hand.

Make it right:

- A night whose legacy loads all finished closes. Every night this deployment
  has run — the January cutover onward — that the control table shows finished
  has to close.
- A night with a failed load does not close. Neither does a night that is short
  a job it owed. Neither does a night the control table has no record of at all:
  a night that started nothing is not a night whose history is complete.
- What a night owed is not what a night wrote. `due_jobs(ds)` answers from the
  export, for any night it is handed, including one with no rows behind it. A
  roster read back off `ops.load_control` closes every night that never ran,
  which is the one night this guard exists to refuse.
- Keep `due_jobs(ds)` and `check_loads_finished(ds)` where they are and keep
  their names. The next person closing a repair night looks for them in that
  DAG.

`legacy/` is read-only, as `CONVENTIONS.md` says. Read the export. Do not edit
it and do not add anything to it.

`ops.load_control` is read-only too, and it is the estate's own record of what
each night ran. It is what to settle an answer against.
