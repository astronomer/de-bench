# CUS-509

**What the row was carrying.** Refunds the customer had not asked for yet. The
table summed `net_sales_cents` off `int_orders_enriched`, and that column is
gross less discounts less **every** return ever matched to the order, whenever
it was initiated — `docs/semantic-definitions.md` defines it that way and four
teams read it that way. A row dated 4 May therefore had a return initiated on
20 May already netted out of it. Median return lag here is about a fortnight,
so a rebuilt row carries roughly the last two to six weeks of returns it could
not have known about. The netting now happens in the model, at line grain,
against `stg_sales__returns.initiated_date` bounded by the row's own `ds`.

**Which columns.** `net_sales_cents` carries exactly the same thing and is fixed
the same way. `orders_30d`, `orders_90d`, `orders_7d` and `booked_cents_30d` do
not: an order counts on its order date and `booked_cents` never moves again, so
they were right. `last_order_date` and `days_since_last_order` were right for
the same reason. The account attributes are a different question and not this
one.

**The files.** The ones on disk are the good copies and the rebuilt ones are the
bad copies, which is the opposite of what it looks like. A day's file is written
the morning after that day, so it can only carry the returns initiated in those
few hours — near enough none. Rebuilding that day now nets against everything
that has arrived since, which is where the leak comes from. So the model that
scored beautifully offline was trained on a training set that knew the future,
and it had nothing to reproduce live. Rebuilding a day does not help and never
did; with the fix in, a rebuilt day now matches the file that was written at the
time, which is the only reason a rebuild is safe at all.
