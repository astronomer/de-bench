# TAX-241 — output tax on the trade invoice book

Working paper. Source: `raw.invoices`, `raw.invoice_lines` and `raw.orders`. Population: all
196,754 invoices in the AR export, invoice dates 2024-02-04 to 2026-06-14, every market, the
172,674 goods invoices and the 24,080 plan invoices together.

## The two totals

    Tax the headers say    21,416,521,280 cents
    Tax the lines say      18,426,440,970 cents
    Difference              2,990,080,310 cents, the headers over the lines

## The line tax is the tax we charged

Summed by invoice, the line tax equals `raw.orders.tax_cents` on all 172,674 goods invoices,
to the cent. The header total equals `raw.orders.grand_total_cents` on the same 172,674. The
order feed is not written by the AR export, so it is an independent record of what the
customer was charged, and it agrees with the lines and not with the headers.

The rates the line detail produces are market rates: 5.40 per cent across the US book, 15.52
on the British one, 17.85 on the Irish one, 14.81 on the German one, 10.01 on the Canadian.

**File 18,426,440,970 cents.**

## What the header number is

`raw.invoices.tax_cents` is a flat 8.33 per cent of the invoice, rounded to the cent. On a
goods invoice it is 8.33 per cent of `total_cents`; on a plan invoice it is 8.33 per cent of
`net_cents`. One of those two holds on every row of the table — 172,674 on the total, 24,080
on the net, all 196,754 invoices. The rate does not vary by market, by entity, by currency or
by year.

It is not a tax calculation. A number that comes out at 8.33 per cent of the invoice in
Portland and 8.33 per cent in Dublin is a plug.

## Market by market

| Market | Header tax | Line tax | Header less line |
|---|---:|---:|---:|
| US | 15,461,403,085 | 9,780,180,680 | +5,681,222,405 |
| ID | 36,468,989 | 35,163,499 | +1,305,490 |
| MX | 76,304,697 | 103,900,500 | −27,595,803 |
| PL | 43,103,768 | 79,888,310 | −36,784,542 |
| BR | 109,531,746 | 158,716,041 | −49,184,295 |
| CA | 2,241,759,848 | 2,517,611,009 | −275,851,161 |
| IE | 633,708,020 | 1,181,736,659 | −548,028,639 |
| DE | 1,298,213,844 | 2,060,472,056 | −762,258,212 |
| GB | 1,516,027,283 | 2,508,772,216 | −992,744,933 |

The US is the market the headers overstate by the most: 5,681,222,405 cents, which is larger
than the whole group difference. Every VAT market is understated — Great Britain by
992,744,933, Germany by 762,258,212, Ireland by 548,028,639, Canada by 275,851,161. The group
figure of 2,990,080,310 is what is left after the two directions cancel, so it is smaller than
the error on any return we file.

## The plan invoices

On all 24,080 plan invoices the header tax equals the line tax exactly. That is no assurance.
The plan line carries the same plug the header carries — 8.33 per cent of the plan amount on
both — so both sides of the tie are the same number reached the same way. The tie is the rate
agreeing with itself and it tells us nothing about the tax on the plan book.

## What is left over

Put the line tax on the header and the header still does not come to its own lines. It is
short on 38,108 invoices, by 7,037,686,339 cents across the book.

The money is `raw.orders.order_discount_cents`, the order-level discount. The invoice header
total is the order's grand total, which is net of that discount. The invoice lines carry the
goods and a freight line and no line carries the order-level discount, so the discount is on
the header only. Header total less line totals less line tax equals minus the order discount,
to the cent, over the whole book.
