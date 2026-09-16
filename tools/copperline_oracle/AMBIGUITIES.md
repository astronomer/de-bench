# Where the policy does not decide the answer

`recognition.py` is written from `docs/finance-policy.md`, `docs/retail-calendar.md`
and `docs/semantic-definitions.md`, plus the raw tables. Nothing else was read.

This file lists every point where the policy and the data together leave more
than one defensible answer, what the model does, and what a reader who
disagrees would have to change. The clause ids are the policy's own.

Most of the list used to be longer. The reconciliation against the controller's
ledger turned each disagreement into either a fixture change or a sentence in
the policy, and the second half of this file records the ones that closed and
what closed them. The rule that governed it is spec 07 section 8: a
disagreement resolves in the fixtures or in the policy text, never by an edit
to the ledger to make it agree.

---

## A-2 REV-8: which days are business days

**The clause.** "Copperline closes a fiscal month on the 5th business day of
the following month."

**The question.** Are public holidays business days? The world ships no
holiday calendar and the policy names none.

**Chosen: Monday to Friday, no holidays.** The one close date the policy writes
down comes out right: FY2026 P5 opens on Sunday 2026-05-31, the count runs 1 to
5 June, and May 2026 closes on 2026-06-05 — "the 5th business day of June",
which is what the worked month says.

**Cost of the alternative.** A holiday inside the first five weekdays of a
period would push the close a day later. It changes nothing in this world:
every month's close is more than a week clear of the as-at date.

Also read here: "the following month" is the following **fiscal** month. The
policy counts in fiscal months everywhere else, and for the worked example the
two readings give the same day.

## A-4 Which day the books are read as at

**The question.** REV-8 decides which months are closed against a date, and the
world ships no clock.

**Chosen: the latest `loaded_at` across the recognition feeds**, converted to a
head-office business date. That is 2026-06-15, which makes FY2026 P4 the last
closed month — the same answer the policy's worked month gives ("May 2026 is
the most recent closed month").

**Why not an event date.** A dispute in this world carries a resolution date in
July 2026 and a marketplace order a ship confirmation past the last load.
Reading the clock off events would close FY2026 P5, a month the warehouse has
no data for, and would then run off the end of `raw.fx_rates`.

`--as-of` overrides it.

## A-9 REV-12: a card issued on the 31st

**The question.** Breakage falls "24 months after issue". A card issued on
31 August has no anniversary in a 30-day month.

**Chosen: clamp to the last day of the target month.** It has not come up in
either profile, and the alternative — rolling into the next month — would move
the amount a period only for cards issued at a month end.

## A-17 REV-13: where the discount stacking is

**The clause.** "Discounts apply in this order, each to the amount remaining
after the one before it: employee discount, trade-program discount,
promotional markdown, coupon... A line whose stacked discounts exceed its gross
amount floors at zero cents."

**The question.** REV-13 books nothing of its own. Does the model have to
re-stack the discounts on an invoice line?

**Chosen: no.** `line_total_cents` is `qty * unit_price_cents -
line_discount_cents` on every row of the world, and no row is negative, so the
stacking and the floor have already been applied where REV-13 says they
apply — per line, upstream of recognition. REV-13's subject is
`net_sales_cents`, an order-line number defined in
`docs/semantic-definitions.md`, and this model computes `recognized_cents`.

REV-16 is used and books nothing either: the fiscal calendar is read from
`raw.fiscal_calendar`, never computed, and `comp_date_ly` belongs to
comparisons, which the policy hands to `docs/comp-store-policy.md`.

---

# What the reconciliation closed

Each of these was a fork in the first draft of this file. The ledger read one
way, the model read the other, and the tie could not hold while both readings
stood. The column says where the fix landed.

| Was | Now | Where it was fixed |
|---|---|---|
| A-1 REV-11's grain: the month's share of a daily allocation, or the whole billing period in one fiscal month | the whole billing period, in the fiscal month its service period starts in; `billing_frequency` separates a monthly billing from an annual prepay | REV-11 |
| A-3 what "would have belonged to a closed month" means | the `loaded_at` business date, and a row that arrived on the close day is late | REV-8 |
| A-5 which entity a marketplace order belongs to | `raw.marketplace_orders.entity_code`, a real column now; the currency never decides it | `extracts/marketplace.py` and REV-14 |
| A-6 which column is "a refund requested" | `raw.plan_lines.cancelled_on`, which now carries a date inside the term rather than the term's own end | `extracts/ar.py` and REV-6 |
| A-7 a non-ratable line on a legacy invoice | it books in full on the invoice date; the worked month lists it as its own step | "A worked month" |
| A-8 which rate a credit memo converts at | the invoice's invoice-date rate, and a dispute write-off with it | REV-5 |
| A-10 which dispute status is the customer's favour | `settled`, at `settled_cents`; `upheld` and `rejected` book nothing | REV-10 |
| A-11 a cancelled marketplace order | recognizes nothing | REV-14 |
| A-12 which rate a gift card converts at | the issue-date rate, for redemption and breakage alike, and the issuing entity with it | REV-12 |
| A-13 fiscal or calendar month for breakage | fiscal | REV-12 |
| A-14 the escheat list is not in the world | it ships, and `escheat_applies` is the test rather than the presence of a row | REV-12 |
| A-15 which rows are store credit | none of them; store credit is not in `raw.gift_cards` and lives on the return that issued it | REV-12 |
| A-16 what the monthly figure is made of | the nine steps of the worked month, which now name every source that books | "A worked month" |
| the entity table has no first-traded date | `raw.entities.first_traded_on`, and a market with no local entity yet books to CL-US | "The five entities" |
| the invoice header does not equal its lines | recognition reads `line_total_cents` and never the header; tax is not inside it | REV-1 |

Two of those needed the fixture changed rather than the policy explained,
because the data itself was saying something untrue:

1. **`raw.gift_card_ledger` stamped entities that did not exist.** It carried
   `CL-GB`, `CL-IE` and `CL-DE` on cards issued from 2024-02-04, more than a
   year before those entities began trading. Every entry on a card now carries
   the entity that sold it, which is CL-US for a card sold in a market with no
   local entity yet. Until that was fixed the two books could not even agree
   on how many rows the ledger had: 74 against 130.

2. **`raw.plan_lines.cancelled_on` was not a cancellation.** It was stamped
   with the line's own term end, so REV-6 either did nothing or reversed a
   handful of very short lines for no reason. It now carries a date inside the
   term, and both branches of REV-6 have a population.

# Things the data says that the policy does not

Not ambiguities — places where the two disagree, and the model follows the
policy because the policy says to: "Where code and this policy disagree about
recognition, the policy is the statement of intent and the code is the defect."

1. **`raw.invoices.net_cents` does not equal the sum of its lines** on an
   order-linked invoice. The order-level discount never reaches the invoice
   lines, so the header and the lines disagree by that discount. It does not
   move a recognized figure — REV-1 says recognition reads the lines — but it
   is the first thing a reader who reconciles the header will trip over. The
   plan invoices do tie, on all 24,080 of them.

2. **`raw.marketplace_orders.order_id` joins nothing.** It is an integer that
   is neither the order book's `order_id` nor its `order_ref`. With
   `entity_code` on the feed the join is no longer needed for recognition.

3. **A dispute resolves after the last load.** One row carries a resolution
   date weeks past the last load. It is past the horizon and books nothing.

# Tables the policy names that the world ships at bake time

`raw.entities`, `raw.gift_card_jurisdictions` and `raw.finance_ledger` are
hand-authored files under `worlds/copperline/workspace/fixtures/`, landed by
`tools/gen_copperline_load_fixtures.py` rather than by the generator. The
model reads the first two. It never reads `raw.finance_ledger`: REV-9 states
that the tie runs one way, and a model of the policy that took a figure from
the ledger would have stopped being a check on it.
