# MIG-63 — wave 2 scoping, and the calendar it turns on

Wave 2 of the orchestrator migration has money against it for the first time
since the January cutover. Before anybody books a date I want two things: a
trading-day calendar this deployment can answer for itself, and an honest list
of what is still sitting on the old scheduler.

The sheet we planned wave 1 from is `legacy/tidal/inventory_jobs.csv`. It was
out of date the day it was made and `docs/runbooks/orchestrator-migration.md`
says so in as many words. Three records are not opinions: the scheduler export
`legacy/autosys/copperline.jil`, its run calendar
`legacy/autosys/calendars/retail_2026.cal`, and `ops.load_control`, which is the
estate's own account of what ran and when. Where the sheet and those disagree,
they win.

`legacy/` is read-only, as `CONVENTIONS.md` says. Read all of it. Do not edit
any of it and do not add anything to it.

Two things.

**1. Make the calendar answer.** One thing in this deployment already reads that
export. It gives me the same sentence about every date I have tried, including
this morning, and this morning is an ordinary Monday in June. The boxes wave 2
moves — the tie-outs, the distribution packs, the month end — every one of them
runs on trading days only, so this has to be right before any of them come
across.

Keep `calendar_days()` and `is_due(ds)` where they are and keep their names: the
DAG is where the next person will go looking for this. `is_due(ds)` answers for
the export and for nothing else — a date the export does not name is not a
calendar day, whatever else we happen to know about that date. Days the export
does not cover are the memo's business, below, and not the function's.

**2. Write the scoping memo.** `docs/memos/wave-2-scope.md`, in the shape of the
memos already in that folder. Answer all six of these:

- how many nights a year the calendar names, and what stretch of dates the
  export covers;
- one job in the export names a run calendar this estate does not hold. Name the
  job, name the calendar, and say what state that job is in;
- the sheet puts two jobs in wave 1 that never moved. Name both, name what the
  sheet says each of them became, and say which record shows they are still on
  the old scheduler;
- the sheet also lists work this estate has nowhere else. Name it;
- so how many jobs do waves 2 and 3 have in front of them between them;
- the export stops at the end of its year. Say where the same fact already lives
  in this warehouse, and whether the two agree over the year they share.

Keep it short. It goes to the platform team with the funding paper.
