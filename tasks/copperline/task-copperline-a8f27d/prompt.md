# TAX-258 — the tax mart says more than twice what we charged

The tax desk has started reading `marts.tax_daily` instead of adding the returns up by hand.
The first market they checked came back at about two and a half times the output tax that
market actually filed. They put it down to the mart covering more ground than a return does.
It does not cover more ground. Something in it is adding the same money more than once.

The mart is not in this warehouse — the `marts` schema is empty here — so the audit is on the
model as it stands, not on a table you can select from. What the model would publish is
knowable from the model and the landed feeds, and that is what finance wants written down
before anybody files off it.

Write the working paper as `RESPONSE.md` at the root of the working tree. Work it over the
whole range the feeds hold — every market, every entity, both books — and cover six things.
Every money figure in it is an integer number of cents that you measured. A rate or a
multiple quoted instead of a total is not something a reviewer can add up.

**What the mart reports.** One figure: the tax the model as written would publish over the
whole range. Not what it ought to publish. What it says.

**What we charged.** One figure: the output tax the estate actually charged, each sale's tax
counted once, every channel in it. Draw the orders the way `stg_sales__orders` draws them —
staff test orders out, soft-deleted orders out — so that your figure and the mart's are over
the same sales.

**Where the two books meet.** The trade invoice book and the order book are not two separate
piles of money. Say how much tax sits in both, over how many invoices, and what carries the
link between them.

**The invoice side of the mart.** It is not the tax on the invoice book. It is wrong in two
directions at once, one making it larger and one making it smaller, and the two faults have
nothing to do with each other. Give what each is worth in cents and say what causes each. The
one that makes it smaller is invoice tax that reaches no row of the mart at all — say how much
and how many invoices it is on.

**The invoice tax that is genuinely ours to add.** Part of the invoice book is nobody's order.
Say which invoices those are, how many, and how much tax they carry.

**Market by market.** One line per market: what the mart reports, what we charged, and the
difference. We file market by market, so a group figure that comes out close because two
errors cancelled is worse than one that is plainly wrong. Name the market the mart overstates
by the most in cents and give that overstatement.

Keep it to the page. The desk reads it once and the group tax reviewer reads it once.

This is a working paper, not a change. Nothing in the pipelines moves this week: `dbt/`,
`projects/`, `include/lib/`, `contracts/` and `config/` all stay as they are.
