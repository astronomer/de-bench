# FIN-311: what May 2026 is made of

May 2026 is **FY2026-P04**. It runs 2026-05-03 to 2026-05-30 on the shipped 4-5-4 calendar
in `raw.fiscal_calendar`, and it closed on 2026-06-05, the 5th business day of June. It is
not calendar May: 2026-05-01 and 2026-05-02 belong to P3 and 2026-05-31 opens P5. Working
the calendar month instead puts 10,005,977,807 cents in step 3 rather than 9,006,594,420.

Recognized revenue: 8,916,119,657 cents

## The build-up

Every figure is integer cents for the whole company, worked from the tables
`docs/finance-policy.md` names. Nothing here reads `raw.finance_ledger`.

| Step | What it is | Cents |
|---|---|---|
| 1 | current-era ratable lines, allocated daily (REV-1, REV-2) | 3,712,115 |
| 2 | legacy-era plans at fiscal-month grain (REV-11) | 4,503,826 |
| 3 | goods and freight, in full on the invoice date (REV-1) | 9,006,594,420 |
| 4 | marketplace commission and fees, net of returns (REV-14) | 79,621,294 |
| 5 | gift cards: redemptions and breakage (REV-12) | 44,755,895 |
| 6 | credit memos issued in the month (REV-5) | -217,620,158 |
| 7 | write-offs of disputes settled in the customer's favour (REV-10) | -5,329,569 |
| 8 | schedules voided inside the 14-day window (REV-6) | -118,166 |
| | **Total** | **8,916,119,657** |

Step 9 of the worked month books nothing. It is the instruction not to convert a second
time, and every amount above was converted once at the date its own clause names.

Inside step 5: redemptions are 24,147,257 cents and breakage is 20,608,638 cents.

## The tie

The eight steps add to 8,916,119,657, and that agrees with Ironwood entity by entity. No
difference, in either direction, on any of the five.

| Entity | Worked from the feeds | `raw.finance_ledger` | Difference |
|---|---|---|---|
| CL-US | 6,118,617,928 | 6,118,617,928 | 0 |
| CL-GB | 1,361,266,578 | 1,361,266,578 | 0 |
| CL-DE | 870,280,323 | 870,280,323 | 0 |
| CL-IE | 556,308,593 | 556,308,593 | 0 |
| CL-MX | 9,646,235 | 9,646,235 | 0 |

This is the first month the tie has actually run, so it is worth saying what it means: the
warehouse can reproduce the ledger from the source feeds, and the two models can be built
against these eight targets rather than against the total alone.

## What each step had to decide

**Step 1 and step 2 — the grain.** `raw.plan_lines.billing_frequency` decides which legacy
lines land whole and which spread. A legacy line billed `monthly` lands entirely on the
first day of the fiscal month its service period starts in (4,433,501 cents); a legacy line
billed `annual` is a prepay and spreads across its term at month grain (70,325 cents). The
term length decides nothing. Cutting the legacy lines to daily grain, which is what
`int_recognition_schedule.sql` does today, gives 4,550,633 instead — and the worked month
names this as the difference the tie usually breaks on.

**Step 3 — the amount, and the month.** `raw.invoice_lines.line_total_cents`, whole. Tax is
not inside that column and `tax_cents` sits beside it, so subtracting it is wrong; it would
give 8,162,397,121. The header is not a recognition input at all, which matters here
because `net_cents` does not equal the sum of its lines on an order-linked invoice. Each
line converts once at its invoice-date rate; not converting at all gives 8,820,828,042.

**Step 4 — which orders, and which day.** A cancelled marketplace order never shipped, so
it recognizes nothing however much commission the row carries; leaving the cancelled orders
in gives 80,418,718. `ship_confirmed_at` and the settlements' `posted_at` are UTC
timestamps and convert to the head-office business date before they meet a month — reading
the UTC calendar day off them gives 79,589,117. The returns leg is the `refund` line of
`raw.marketplace_settlements`, in the period it posted, at the rate of its order's
ship-confirmation date.

**Step 5 — which cards break.** Breakage is the value left on the card at 24 months, not
the value it was sold for, and a card issued in a jurisdiction whose `escheat_applies` is
true escheats instead and never books breakage. 20 of the 66 rows of
`raw.gift_card_jurisdictions` carry the column, and ignoring it more than doubles the
breakage to 43,761,918. Every amount on a card takes the entity and the rate struck when
the card was sold, so a card sold in Germany in 2024 is still CL-US.

**Step 6 — which rate.** A credit memo converts at the rate its invoice converted at, never
at its own issue-date rate. It books on its own issue date and restates nothing earlier.

**Step 7 — which status.** `settled` is the only one of the four that books, at
`settled_cents`. Fifteen `upheld` and ten `rejected` disputes resolved inside the month and
none of them books; treating `upheld` as the customer's favour and writing off
`disputed_cents` would give 10,559,955 instead of 5,329,569.

**Step 8 — which side of day 14.** The refund request is `raw.plan_lines.cancelled_on` and
that column is the whole population. On or before day 14 the schedule voids and everything
recognized to date reverses on the day of the request; on day 15 or later it only stops
going forward and what recognized stands. Reading credit memos or `raw.returns` as this
step's population instead leaves it at zero and the month at 8,916,237,823.

Nothing in the pipelines was changed for this.
