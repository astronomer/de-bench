# CUS-544 — the replayed 360 counted orders that had not happened yet

`int_customer_lifecycle` read the whole order book on every build and dated it
against `var('ds')`. Its `orders` CTE had no upper bound on `order_date`, so a
build for a past day aggregated every order to the end of the feed and then
measured tenure, recency and churn from a day those orders are after. The fix is
one clause on that CTE: `where order_date <= {{ ds() }}`. The order spine is
untouched — `int_orders_enriched` still publishes the whole book, which is what
its ten other readers need.

## The churn series, both ways

Churned parties in `int_customer_lifecycle` — no order in the 180 days ending
`ds`:

| ds | the replay published | as of its own day |
|---|---|---|
| 2025-10-17 | 1,758 | 34,906 |
| 2026-01-16 | 4,928 | 22,911 |

The second column is the true shape. Autumn churn **fell**, from 34,906 parties
to 22,911 over the window. The replay's series rises because the further back
the day, the more of the future it was holding: on 2025-10-17 almost every party
in the book had ordered "since", so almost nobody could look churned.

## What the 2025-10-17 build was holding

That day's build published 295,148 parties — the same count it published on
every other day of the window, because the count was never a function of the
day.

| | rows |
|---|---|
| parties published that had not ordered by 2025-10-17 | 23,688 |
| rows carrying a negative `days_since_last_order` | 281,883 |
| orders counted that had not happened by 2025-10-17 | 1,087,389 |

The 23,688 are parties whose first order is still in the future on that day;
they carry a negative `tenure_days` as well. The 281,883 are every party whose
last order lands after the day the build was given, which on a day eight months
before the end of the feed is most of the book. The 1,087,389 is against
2,325,989 orders that had actually happened — the build counted 3,413,378.

## Why the nightly is clean

The nightly runs for the current day and no order is dated after it, so the
bound removes nothing. A build for the nightly's own `ds` publishes exactly what
it published before the fix — same 295,148 rows, same counts, same flags,
nothing changes. The fault is only reachable by replaying an old day, which is
what the broker was doing.
