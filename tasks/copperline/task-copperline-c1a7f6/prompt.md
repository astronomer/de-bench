# GRO-447 — the guest stitch has never stitched

Growth reads its per-customer web numbers off `marts.fct_web_sessions`. A
session that converted while signed out is meant to get its customer back from
the order it placed: `gro_sessionize_daily` builds the day, then runs
`stitch_orders` over it, and the library says outright that the order carries
the customer so the session can be resolved after the fact.

Nothing is being resolved. R. Adeyemi rebuilt three days by hand last week and
counted, before the stitch and after it, the sessions that carry an order and no
customer. The two counts were the same every time, on every day she tried. More
than half the converting sessions on an ordinary day arrive signed out, and
every one of them is still signed out in the published table. As far as we can
tell this has been true since the step was written.

Nobody caught it because the step reports a number every night that looks like
work. It moves with the volume, it has never been zero, and on the days she ran
by hand it had nothing to do with what the step actually did.

Find out why nothing is being resolved, and make it resolve.

## What we already know

- It is not one bad day and it is not an empty feed. `raw.orders` carries the
  day's web orders and the sessions carry the order ids that name them — the
  two sides find each other, and the step still leaves the sessions as it found
  them.
- Rebuilding the day first makes no difference. She rebuilt each day before
  stitching it and got the same nothing.

## What has to be true when you are done

- A session that converted, and whose order names somebody, comes out naming
  that same somebody. The id that order carries — not one made up for the
  occasion, and not another order's.
- A session that arrived with a customer on it keeps the customer it arrived
  with. The stitch is for the ones that arrived without one.
- Guest checkout is real and some orders name nobody at all. Where that is the
  case the session stays unresolved, and the row says so rather than guessing.
  An id invented for a sale that named nobody is worse than a null, because the
  next reader counts it.
- A session that did not convert is not the stitch's business and does not move.
- **The step returns the rows it changed.** That is what its docstring has said
  since it was written and it is not what it returns; the number it does return
  is the reason this ran for two years without anybody looking at it. Once it is
  honest a second run over the same day reports nothing changed, and that is
  correct — there is nothing left to resolve.
- One day's partition at a time, and running that day twice leaves the same
  rows.
- The day is still built from the event stream alone. `build_day` reads the
  clickstream and the stitch reads the order feed after it; four DAGs build
  through that library and only one of them runs the stitch, so keep the two
  apart.
- `raw.*` is landing and read-only. Whatever is wrong here, writing to a landing
  table is not the fix.
- The published session grain does not change: same table, same columns, one row
  per session per day.

The model side of the session definition has the same gap and is not in this
ticket. `int_sessions_funnel` is read by every team, so it moves through
`docs/change-management.md` once this one is settled, not before.

Write down in the function's own docstring what you found and what the link is.
Whoever reads it next will ask the same question, and the answer should be
sitting there.
