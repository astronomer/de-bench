# CUS-544 — the replayed 360 has autumn churn going the wrong way

Marketing asked for last autumn's churn cohorts and the days had never been run.
`cus_customer_360_daily` is `catchup=False` like everything else here, so a day
nobody asks for is a day that never happens. Platform put the range through
`plat_backfill_broker`: **2025-10-17 to 2026-01-16 inclusive**, one day at a
time, in order, each day handing its own date down the chain as
`--vars '{ds: <that day>}'`.

Every run was green. The series is not.

- The churned-party count on the last day of the window is several times the
  count on the first day. Everything else we have says autumn churn fell — the
  support queue, the win-back sends, the trade team's own list.
- Rows in `int_customer_lifecycle` on the replayed days carry a **negative**
  `days_since_last_order`.
- The model publishes the **same number of rows on every day of the window**.
  The first day and the last day of a three-month range have the same party
  count, to the row.

The nightly has never shown any of this, and nobody is claiming it has.

## What to do

### 1. The model answers as of the day it was given

`int_customer_lifecycle` is the model behind all three symptoms. Make it answer
as of the day it is handed: a build for `ds` counts an order if it happened on
or before `ds`, and counts nothing else. First order, last order, tenure, the
four rolling counts, the money and the three flags — all of them as of that day.

Three things constrain the fix.

- **The order spine keeps the whole book.** `int_orders_enriched` publishes
  every order there is on every run, and ten other models and one data test read
  it. The bound belongs to the model that has an as-of question to answer, not
  to the spine underneath it.
- **The nightly does not move.** A build for the day the nightly runs has to
  publish exactly what it publishes today: same rows, same counts, same flags.
  A fix that changes tonight's numbers is not this fix.
- **The grain does not move.** One row per party, `trade`, `loyalty` and
  `guest`, the same as now.

### 2. RESPONSE.md, at the root of the working tree

The audit somebody can take to marketing. Keep it short. It has to carry these.
Every count below is over every row the model publishes, whatever its
`party_kind`.

**The churn series, both ways.** For **2025-10-17** and for **2026-01-16** — the
first and the last day of the replayed window — give the churned-party count the
replay published, and the churned-party count a build that answers as of its own
day publishes. Four numbers. Say which pair is the true shape of the autumn.

**What the 2025-10-17 build was holding.** Three counts, off that one day's
build, as the replay published it:

- how many of the parties it published had not placed an order by 2025-10-17
- how many rows carry a negative `days_since_last_order`
- how many of the orders it counted had not happened by 2025-10-17

**Why the nightly is clean.** One line, and say what the nightly build publishes
differently once the model is bounded.

Nothing else moves this week. Somebody will re-run the backfill when this lands;
that is not your job.
