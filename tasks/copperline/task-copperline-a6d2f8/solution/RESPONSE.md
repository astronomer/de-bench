# MIG-96 — the night the bridge has been asking for

## What `BUSDATE` is

The day before the day the box starts. `copperline.jil` says so at the top, and
`ops.load_control` settles it without the comment: all **15,577** rows in the
table started in the 01:00 hour of the day after their own business date, and
**none** break the rule — before the cutover and since.

`sc_autosys_bridge.trigger_nightly` handed the runner `{{ ds }}`, which under a
trigger timetable is the day the run fires. So the estate has been told to work
the night that had not closed yet, and the night that had just closed was never
asked for. The trigger now carries `TARGET_DS`, the house spelling of the day the
run owns.

## How many nights

**161** — 2026-01-05, the cutover, to 2026-06-14, the last business date the
control table holds. Every one of them named the wrong night.

**112** of those 161 started the tie-outs and the distribution packs. Those boxes
carry `run_calendar: retail_2026`, and the calendar names 112 of the 161 days.
The reader in the bridge cannot say so — it has answered "not a calendar day" to
every date since January and it is another ticket — but `raw.market_calendar` at
`market_code = 'US'` names the same days, all 251 of them for 2026.

## The nights finance never got

**23** business dates the tie-outs should have covered and never did:

- **20 Sundays.** A Sunday is tied out on the Monday, and the Monday now ties
  out itself.
- **3 Mondays** — 2026-01-19, 2026-02-16 and 2026-05-25. The calendar leaves each
  of those out as a holiday, so the Tuesday after it used to carry the Monday.
  Now the Tuesday carries the Tuesday.

The earliest is 2026-01-04, the night before the cutover. The latest is
2026-06-07.

## The nights finance got that never used to exist

**23 Fridays**, 2026-01-09 to 2026-06-12. A Friday is tied out on the Saturday,
and Saturday is not a calendar day, so under the old profile a Friday tie-out was
never produced at all. That is the half finance recognised, and it is why they
and the wave 2 note both read true: the note describes the profile, and the
estate has not been run from the profile since January.

Both accounts come to the same 112 nights. 23 came off the front and 23 went on
the back.

## Why nothing has gone red

`check_jobs_wrote` reads `ops.load_control` for the same `{{ ds }}` the trigger
passed. The step that asks and the step that checks have been out by the same
day, so the morning log has always agreed with itself. It stays on `{{ ds }}`
here on purpose: moving it changes what supply-chain read every morning, and
finance are told before that happens.
