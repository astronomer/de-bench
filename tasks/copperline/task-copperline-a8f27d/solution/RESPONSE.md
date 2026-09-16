# TAX-258 — what `marts.tax_daily` would publish, against what we charged

Working paper. Sources: `raw.orders`, `raw.invoices`, `raw.invoice_lines`, and the model file
`dbt/copperline_analytics/models/finance/tax_daily.sql`. The mart is not in this warehouse, so
the figures below are the model's own SQL run over the landed feeds. Range 2024-02-04 to
2026-06-14, all nine markets, both books.

## The two numbers

    marts.tax_daily.tax_cents, whole range    81,329,623,679 cents
    output tax we charged                     35,469,202,859 cents
    the mart over the charge                  45,860,420,820 cents, 2.29 times

The mart's own halves are 35,442,956,632 from the order side and 45,886,667,047 from the
invoice side. The order side is right. Everything wrong with the number is on the invoice side.

## What we charged

Order tax over the order spine — `stg_sales__orders`, so no `is_test` rows and no soft
deletes — is 35,442,956,632 cents over 3,413,378 orders, every channel. Add the tax on the
invoices that bill no order, 26,246,227 cents, and the estate charged 35,469,202,859.

Nothing else is added. The rest of the invoice book is order tax again, in a second place.

## Where the two books meet

Every one of the 172,674 goods invoices carries an `order_id`, and it resolves to exactly one
row of `raw.orders` — 172,674 invoices, 172,674 distinct orders, no orphans on either side.
Every one of those orders is a `trade` order, and `invoice_date` is the order's own
`local_order_date` on all 172,674.

Summed by invoice, `raw.invoice_lines.tax_cents` equals `raw.orders.tax_cents` on all 172,674,
to the cent: **18,400,194,743 cents**. That is the whole goods book and it is the whole of the
trade order book's tax. It is one charge to one customer, and `tax_daily` puts it on both
sides of an addition.

## The invoice side of the mart

The invoice book's own line tax is 18,426,440,970 cents. The mart's invoice side is
45,886,667,047. The two are apart in two directions at once.

**Smaller: 6,846,249,989 cents reach no row of the mart.** `raw.invoices.currency_code` is
NULL on 92,971 invoices — the US and CA book from 2024-02-04 to 2025-04-06, before the
currency cutover. `fct_ar_invoices` passes the column through as `currency_key` without
filling it. In `tax_daily`, the `days` CTE unions those keys in, because a UNION treats two
NULLs as the same key, but the left join back does not, because `NULL = NULL` is not true. So
those rows land in the mart with `invoice_tax_cents` of nought and 6,846,249,989 cents of
invoice tax is in the model's output nowhere at all. It leaves 11,580,190,981 cents of invoice
tax that can join.

**Larger: the join fans that remainder out about four times, to 45,886,667,047.** `order_tax`
is grouped by day, market, **channel** and currency. `invoice_tax` is grouped by day, market,
**entity** and currency. The final join is on day, market and currency only. A market has
exactly one entity, so `invoice_tax` has one row per join key — but `order_tax` has one row per
channel trading that day, and four channels trade on most of them. Each invoice row therefore
meets four order rows, and `sum(i.invoice_tax_cents)` adds that invoice tax four times.
The mart's own `invoice_count` shows it: 412,587 against 103,783 invoices that can join.

The order side does not fan out in return, because the invoice side contributes one row.
`sum(o.order_tax_cents)` comes to 35,442,956,632, which is the order tax exactly.

## The invoice tax that is genuinely ours to add

The 24,080 plan invoices bill no order — `order_id` is NULL on all of them and every line on
them is `line_kind = 'plan'`. They carry **26,246,227 cents** of tax, and that is the only
invoice tax in the book that is not already an order's. It is 0.14 per cent of the invoice
book. Adding the invoice book to the order book to get a group figure is out by the other
99.86 per cent.

## Market by market

| Market | Mart reports | We charged | Mart over |
|---|---:|---:|---:|
| US | 34,263,388,122 | 17,078,018,557 | +17,185,369,565 |
| GB | 15,981,365,474 | 5,958,405,560 | +10,022,959,914 |
| DE | 13,213,733,917 | 4,980,124,332 | +8,233,609,585 |
| IE | 7,379,445,126 | 2,658,053,957 | +4,721,391,169 |
| CA | 8,836,334,877 | 4,268,060,974 | +4,568,273,903 |
| BR | 696,904,877 | 220,756,754 | +476,148,123 |
| MX | 458,434,907 | 146,733,407 | +311,701,500 |
| PL | 346,880,712 | 110,782,065 | +236,098,647 |
| ID | 153,135,667 | 48,267,253 | +104,868,414 |

Every market is overstated; nothing cancels. **The US is overstated by the most:
17,185,369,565 cents.** The US is also the mildest in proportion, at 2.01 times, because the
blank-currency book that never joins is all US and CA — the two faults work against each other
there and only there. ID, PL, MX and BR come out at about 3.15 times, which is the fan-out with
nothing taken back off it.

No return can be filed off this table until the mart adds the trade book once and joins the
invoice side on a key both sides carry.
