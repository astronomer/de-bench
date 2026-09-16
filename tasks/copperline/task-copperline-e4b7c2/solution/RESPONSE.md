# MIG-84 — the run calendar wave 2 inherits

## How many jobs the gate has to cover

`retail_2026` governs **18** of the jobs in `legacy/autosys/copperline.jil`.
Only **8** carry `run_calendar: retail_2026` on a line of their own:
`cpl.nightly.recon`, `cpl.nightly.dist`, `cpl.recon.gl`, `cpl.dist.stores`,
`cpl.dist.merch`, `cpl.dist.finance`, `cpl.month_end` and
`cpl.month_end.gl_close`.

The other ten sit inside those boxes and a job in a box starts only when its box
starts, so they are on the calendar without saying so: `cpl.recon.stock` and
`cpl.recon.freight`, the three tie steps, the four pack and send steps under
`cpl.nightly.dist`, and `cpl.month_end.gl_close.run`. A grep for
`run_calendar:` sees eight of the eighteen.

The same grep finds one more line that is not ours: `cpl.util.archive.logs`
names `retail_2025`, a calendar this estate does not hold, and the job carries
`status: OI`.

## The nights the tie-outs have never run for

**Fridays and Saturdays**, every week, and the day before each market holiday.

`cpl.nightly` starts at 01:00 and `nightly.profile` sets `BUSDATE` to yesterday,
so the box that starts on a Monday ties out Sunday. The calendar gates the day
the box **starts**, not the business date it carries. Saturday and Sunday are not
calendar days, so the runs that would have carried Friday and Saturday never
start at all. From 2026-01-04 to 2026-06-14 that is 49 of the 162 business
dates: 23 Fridays, 23 Saturdays and the 3 Sundays that fall before a Monday
holiday.

Nothing has ever failed over it. The box is not late, it is not scheduled, and
the tie-out for a Friday has never existed.

Wave 2 inherits this the moment a converted DAG gates on `ds`, because `ds` is
the day the run fires and the day it owns is `ds - 1`. Gating on the day it owns
instead would close the hole and move every other night by one, which changes
what finance read in the morning — so `is_run_day` answers about `ds` and the
decision to keep the hole or close it is yours.

## `ops.load_control.history_complete`

No. It tells us nothing `load_status` does not.

On every row in the table `history_complete` is true exactly where `load_status`
is `complete` and false exactly where it is `failed`. The two columns are the
same fact spelled twice, and the column is not a record of a night's history
closing whatever `stg_ops__load_control` says about backfills.

So the only account of a night is `load_status`, and it cannot tell a night that
failed from a night that never started — a night nothing ran for has no row at
all.
