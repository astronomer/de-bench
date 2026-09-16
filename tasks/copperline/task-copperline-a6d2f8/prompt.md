# MIG-96 — the bridge is a day out and finance have been reading it

The note I sent the platform team with the wave 2 plan says the tie-out for a
Friday has never existed, and that the Sunday tie-out runs on the Monday. It went
round to finance, who came back and said the opposite has been true all year:
they have had a Friday tie-out every week since January and they have not had a
Sunday one at all.

Both accounts are right. The note describes the old scheduler and the profile it
ran from. Since the cutover the nightly window is started from this deployment,
and what we start it with is not what that profile set.

`legacy/autosys/copperline.jil` says at the top what `BUSDATE` is. Do not take
that on trust — it is a comment in an export somebody cut in December.
`ops.load_control` is the estate's own record of every night it has ever run,
and it has a column that settles the question. Settle it there first, then read
what `sc_autosys_bridge` hands the runner.

`legacy/` is read-only, as `CONVENTIONS.md` says. Read it. Do not edit any of it
and do not add anything to it.

## 1. The one thing that moves

Fix what `trigger_nightly` tells the estate. `CONVENTIONS.md` says under **Dates**
how a daily job that owns the day before writes it, and there is a house spelling
for it. Use that one.

Three things in that DAG do not move, and each of them for its own reason:

- `report_calendar_day` asks the run calendar about the day the box **starts**.
  A run calendar governs starting, not the business date the box carries. Leave
  it on the day the run fires.
- `check_jobs_wrote` reads the control table for the same `ds` and always has.
  Moving it changes what the morning log says, and finance are told before that
  happens, not after. Leave it where it is and put it in the write-up.
- `calendar_days()` and `is_due()` have a ticket of their own and somebody else
  has it. Leave them alone. They are also why `report_calendar_day` has said the
  same sentence about every night since January, which is not this ticket.

## 2. The audit, in `RESPONSE.md` at the root of the working tree

Finance want to know what they have been reading since January. Answer every one
of these, with the numbers:

- **What `BUSDATE` is meant to be**, and what in the estate's own record says so
  rather than a header comment. Give both counts: how many rows in
  `ops.load_control` bear the rule out, and how many break it.
- **How many nights the bridge has named the wrong business date.** Count from
  the cutover to the last business date the control table holds.
- **How many of those nights started the tie-outs and the distribution packs.**
  Those boxes are the ones on the run calendar. The calendar reader in that DAG
  cannot tell you and is not yours to repair — the same fact is in this
  warehouse, for the home market.
- **Which business dates the tie-outs should have covered and never did.** The
  count, the days of the week, and what makes each kind of day one of them.
- **Which business dates they covered that they never should have.** The count
  and the day of the week. This is the half finance recognised.
- **Why nothing has gone red over any of it.** Name the step that has been
  agreeing with the wrong night, and say what it reads.

Keep it short. It goes back to finance beside the note they queried.
