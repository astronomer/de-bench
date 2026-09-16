# LF-1121 — why the Monday settlement summary was empty and the payout tie was red

Two defects, both in `payment_settlement_weekly`. Neither is the fee schedule.

## What a scheduled Monday run covered

One day: the Sunday before it.

`week_bounds(ds)` took the day before the run and asked the calendar for its
week, then used that answer for both ends of the window — start and end came
back as the same date. On a Monday `ds`, the day before is a Sunday, and a
fiscal week opens on a Sunday, so `calendar.week_start` returned that Sunday
itself. The window was one day long instead of seven.

That one day is the worst day it could have been. Every marketplace payout in
`raw.marketplace_payouts` falls on a Monday, so a Sunday-only window holds the
one day of the week that never carries a payout. `build_week` loaded no lines,
`publish_week` wrote no rows, and `check_grain` was happy with it — nought rows
for nought sellers is one row per seller. `tie_to_payouts` is the first step that
compares the summary against anything outside itself, so it is the first step
that could notice, and it has been red every Monday since.

Every run in the list is a scheduled one, and `CONVENTIONS.md` "Dates" says why
that matters: a bare cron string is a trigger timetable, so `ds` is the Monday
the run fires. The window collapses on a Monday and only on a Monday. A run
triggered by hand mid-week covers a part-week and comes out with figures, which
is why the February replay looked fine to everyone who checked it.

## Which days it should cover

The fiscal week that closed on the Saturday before the run: Sunday to Saturday,
seven days, ending two days before `ds` rather than one.

Both ends now come from `raw.fiscal_calendar` — the `week_start` and `week_end`
of a day inside the closed week — and not from weekday arithmetic.
`docs/retail-calendar.md` owns the 4-5-4 pattern and
`contracts/settlement-summary.md` SS-1 and SS-3 own the grain: one row per
seller per fiscal week, for the seven days of that week. `sc_shrink_weekly` and
`sc_supplier_scorecard_weekly` land inside their own closed week the same way,
each with its own lag from its own run day.

## The fee schedule is not what broke the tie

Ruled out. `settlement_payout_tie.sql` closed the summary side on one
`week_start` and opened the payout side at that same date with nothing above it
— `payout_date >= ?` and no upper bound — so it compared one week's summary
against every payout from that week onwards. No fee schedule, right or stale,
can make those two sides agree; the payout side was several months of money and
the summary side was one week of it, or in this case none.

The payout side is now closed at both ends on the same week the summary covers.
With the window seven days long and the tie comparing like with like, the two
sides agree cent for cent: `raw.marketplace_payouts.net_paid_cents` is rolled up
from the same settlement lines the summary sums, so the tie is an identity once
both sides read the same week.

## What changed

- `projects/commerce/dags/payment_settlement_weekly.py` — `week_bounds` returns
  the closed fiscal week, both ends from the calendar; `tie_to_payouts` passes
  the end of the week through.
- `projects/commerce/sql/settlement_payout_tie.sql` — an upper bound on the
  payout side.

`include/lib/calendar.py` is right and is untouched: a fiscal week does open on
a Sunday, and the caller was asking it the wrong question.
