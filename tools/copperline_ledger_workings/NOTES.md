# The controller's working for `ledger_monthly.csv`

Repo-side. Nothing here ships. The one file that ships is
`worlds/copperline/workspace/fixtures/finance/ledger_monthly.csv`, and the
generator neither writes it nor reads it — `tests/test_copperline_ledger.py`
holds that line.

What I read to build it: `docs/finance-policy.md` (the only recognition
authority), `docs/retail-calendar.md`, `docs/semantic-definitions.md`, and the
raw warehouse at both profiles. I did not read the generator, the dbt project,
the tasks, `PLANTED.md` or `planted.yaml`. Every number below comes from the
policy applied to the raw tables.

Databases used:

    tools $ PYTHONPATH=. python -m gen_copperline \
        --timeline ../worlds/copperline/timeline.yaml \
        --planted ../worlds/copperline/planted.yaml \
        --db /tmp/ledgA9-full.duckdb --landing /tmp/ledgA9-lf --profile full
    $ python tools/gen_copperline_load_fixtures.py \
        --fixtures worlds/copperline/workspace/fixtures \
        --db /tmp/ledgA9-full.duckdb --landing /tmp/ledgA9-lf

    (and the same with --profile small for spot checks)

The loader is not optional. `raw.entities`, `raw.gift_card_jurisdictions` and
`raw.finance_ledger` are hand-authored files that land at bake time rather than
being generated, and REV-9 and REV-12 both read the first two.

The working script is `close.py` beside this file. It prints every step of the
worked month, per entity, for any month you name.

## What changed at the reconciliation

This file was written twice. The first version was worked against the policy as
it then stood, and it disagreed with `tools/copperline_oracle` — the model
written independently from the same document — on every one of the 74 rows. The
adjudication is section 7. Sixteen questions were settled, thirteen of them by
a sentence added to `docs/finance-policy.md` and three by a fixture that was
saying something untrue. Section 3 below is the rulings as they now stand,
after that.

---

## 1. What the ledger covers

`docs/finance-policy.md` names its systems of record in the first line:
`raw.invoices`, `raw.invoice_lines`, `raw.credit_memos`, `raw.disputes`,
`raw.plan_lines`, `raw.fx_rates`, `raw.gift_cards`,
`raw.gift_card_jurisdictions`, `raw.marketplace_orders`,
`raw.fiscal_calendar`. That list, and the nine steps of "A worked month", are
what the ledger is built from. `raw.orders`, `raw.pos_sales_daily` and
`raw.returns` are not on the list and are not in the ledger.

Three tables I used that the list does not name, because the policy asks for a
number the named tables cannot give:

- `raw.gift_card_ledger` — `raw.gift_cards` holds issues only. REV-12
  recognises on redemption and on breakage, and the ledger table is the only
  record of either.
- `raw.marketplace_settlements` — REV-14 puts a marketplace return "in the
  period of the return". `raw.marketplace_orders` marks an order `refunded`
  and carries no return date. The settlement feed's `refund` rows carry one.
- `raw.entities` — REV-9's coverage rule turns on `first_traded_on`.

Checked: of the 196,754 invoices at the full profile, 172,674 carry an
`order_id`, and every one of those orders is on the `trade` channel. The other
24,080 are the plan invoices, billed to a trade account with no order behind
them. So the invoice book is the trade book, which is the population the
policy is written around, and no store or web sale reaches it.

## 2. The steps, as the policy sets them out

"A worked month" lists nine sources and says they are the whole of the figure.
I ran them in that order and kept each one separately so the working can be
read back.

| Step | Clause | What it takes |
|---|---|---|
| 1 | REV-1, REV-2 | current-era ratable invoice lines, daily, summed into the month |
| 2a | REV-3 with REV-11 | legacy lines billed annually, allocated, at month grain |
| 2b | REV-11 | legacy lines billed monthly, whole, in one fiscal month |
| 3 | REV-1 last sentence | goods and freight lines, in full on the invoice date |
| 4a | REV-14 | marketplace commission and fulfilment fees, on ship confirmation |
| 4b | REV-14 | marketplace returns, in the period of the return |
| 5 | REV-12 | gift-card redemptions and breakage |
| 6 | REV-5 | credit memos, negative, on the issue date |
| 7 | REV-10 | dispute write-offs in the customer's favour, on the close date |
| 8 | REV-6 | the reversal of a schedule voided inside the 14-day window |

Step 3 is most of the ledger: 1,346,678 of the 1,370,758 invoice lines have no
`service_end`. They are the `goods` and `freight` lines, on revenue accounts
4000 and 4400.

Step 7 is separate from step 6 because the two populations do not overlap: no
`dispute_settlement` credit memo shares an invoice with a `raw.disputes` row.

## 3. Rulings

Where the policy did not decide something, I decided it and wrote down the
number the other reading would have moved.

### A1 — The line is the grain, not the invoice header

`recognized_cents` is defined in `docs/semantic-definitions.md` at "invoice
line day" grain, and REV-1 says it outright: "Recognition reads the lines and
never the invoice header." Recognition is off `raw.invoice_lines`.

### A2 — The header does not tie to its lines, and it does not have to

On an order-linked invoice the order-level discount never reaches the invoice
lines, so `net_cents` and the sum of `line_total_cents` differ by that
discount on all 172,674 of them. It is a real fault in the feed and it is worth
knowing about, but it moves nothing here: neither book reads the header. The
24,080 plan invoices do tie, on every row.

### A3 — Tax is not revenue, and it is not inside the line amount

REV-1: "The amount that recognizes is `raw.invoice_lines.line_total_cents`. It
is the line's own extended amount, `qty * unit_price_cents -
line_discount_cents`, on every row of the book. Tax is not revenue, and it is
not inside that column." I take `line_total_cents` whole.

**Swing.** The first version of this file took `line_total_cents - tax_cents`,
on the strength of the plan invoices, whose header then read as though the line
amount were tax-inclusive. That was 18,780,814,554 cents across the closed
months, 7.7% of the ledger, and it was the largest single difference between
the two books. The clause settled the reading and the plan invoice header was
corrected to match it.

### A4 — Payment state does not matter

`issued`, `partially_paid` and `paid` invoices all recognise. REV-3 says the
payment date has no effect on recognition.

### L1 — Legacy grain: the whole billing period in one fiscal month

REV-11: "the whole of its amount lands on the first day of the fiscal month its
service period starts in... nothing of it reaches the next one." The service
periods are calendar months and only 434 of 14,664 legacy monthly lines start
on a fiscal period's first day, so the two grains give different month totals,
which is exactly what the clause means by a daily figure being wrong at the
month as well as at the day.

`billing_frequency` decides which legacy lines those are. A legacy line billed
`annual` is a prepay under REV-3 and spreads across its term at month grain
(step 2a); a legacy line billed `monthly` lands whole (step 2b). The first
version of this file cut on the term length instead, at 31 days, which put four
amended lines of 54, 101, 202 and 355 days on the wrong side. The column does
the job the day count was standing in for.

### L2 — A cancellation is a refund request, and REV-6 reads it

`raw.plan_lines.cancelled_on` carries the day the customer asked for the money
back, inside the term the line bills. 473 invoiced plan lines carry one, 289 of
them inside REV-6's 14-day window. Inside the window the schedule voids:
recognition to date reverses on the day of the request and the line comes out
at nothing. Outside it the schedule stops on that day and what recognized
stands.

The first version of this file ruled that REV-6 had no population. It was right
about the data it had — `cancelled_on` was then stamped with the line's own
term end, which is not a cancellation — and wrong about the clause. The fixture
was corrected and the ruling with it.

### R1 — `raw.returns` is not REV-6's population

REV-6 names its own: "The refund request is `raw.plan_lines.cancelled_on`, and
that column is the whole of REV-6's population... `raw.returns` is merchandise
coming back against a retail order, which never reaches the trade book." Goods
returns reach the ledger as credit memos with `reason_code = 'return'` under
REV-5.

### C1 — "The following month" is the following fiscal period

REV-8 closes a month "on the 5th business day of the following month". Read as
the following fiscal period, FY2026-P04 closes on 2026-06-05, which is the date
the policy's own worked month gives. Read as the following calendar month it
also gives 2026-06-05, so the worked example does not separate the two — but it
does separate them elsewhere (FY2024-P11 ends 2025-01-04, and the two readings
give 2025-01-10 and 2025-02-07). The closed set is the same either way:
FY2024-P01 through FY2026-P04, with FY2026-P05 open.

### C2 — A business day is Monday to Friday

The warehouse ships no head-office holiday calendar. `raw.market_calendar` is
a store trading calendar, not a finance one. No holiday is applied.

### C3 — The month is spelled `FY2026-P04`

`docs/retail-calendar.md` gives the year as `FY2026` and the period as a
number. Ironwood spells the pair `FY2024-P01` in `raw.invoices.posted_period`,
and the ledger is an Ironwood extract, so the ledger spells it that way too.

The fiscal month of every amount is derived by joining the date to
`raw.fiscal_calendar`, never from `posted_period`: 7,519 of 196,754 invoices at
the full profile carry a `posted_period` that disagrees with the calendar for
their own invoice date.

### C4 — A UTC timestamp is converted before it is dated

"Business dates are America/Los_Angeles... Event timestamps in the source feeds
are UTC and are converted once, on the way in." Four recognition columns are
timestamps — `ship_confirmed_at`, `posted_at`, `issued_at`, `occurred_at` — and
every one goes through the conversion before it meets a fiscal month. Reading
the UTC calendar day off `ship_confirmed_at` puts about one marketplace order
in seven in the wrong month; on the gift-card columns it happens to change
nothing, because those events are stamped in the middle of the UTC day.

### E1 — Before a market had a local entity, CL-US carried it

The policy's entity table gives each entity a first traded date and
`raw.entities.first_traded_on` carries it as data. A market with no local
entity yet is a cross-border market, and CL-US covers those. The feeds now
stamp that: 27,132 gift-card entries in GB, IE, DE, PL and MX markets carry
`CL-US` because they are dated before the local entity opened.

The first version of the gift-card feed stamped `CL-GB`, `CL-IE` and `CL-DE` on
cards from 2024-02-04, more than a year before those entities began trading.
That single fault was the difference between a 74-row ledger and a 130-row one,
and it was fixed in the feed rather than worked around here.

### E2 — Market to entity is the map the reference table carries

`raw.market_config` gives every market its entity and `raw.invoices` and
`raw.gift_cards` agree with it: US, CA, BR and ID to CL-US; GB to CL-GB; IE
**and PL** to CL-IE; DE to CL-DE; MX to CL-MX. Poland is on EU VAT OSS through
the Irish entity, which is what the policy table says and what the data now
does. An earlier build had Poland on CL-DE; that was the fixture, and it was
corrected.

### E3 — Coverage starts where the entity has something to recognise

REV-9: "An entity's coverage starts at the first closed month in which it
recognized anything, and never before the month it began trading; from there it
runs unbroken to the last closed month. A closed month with nothing in it has
no row." The fiscal calendar reaches back to FY2022 and the facts do not, so
without the second half of that rule CL-US would carry two years of zero rows.

### M1 — Marketplace entity is a column

`raw.marketplace_orders.entity_code` says which entity the order billed
through. The currency cannot: CL-IE and CL-DE both bill in euro, and there is
no honest way to split a shared currency between two entities.

The first version of this file booked all euro marketplace revenue to CL-DE and
the model held it in an `EUR-UNATTRIBUTED` bucket, which is two guesses at a
question neither book could answer. The extract carries the entity now, so
neither has to guess: 9,816 of the marketplace orders are CL-IE and 19,949 are
CL-DE.

### M2 — A cancelled marketplace order recognises nothing

They carry a `ship_confirmed_at` and a commission, and not one has a commission
settlement row. Nothing shipped, so nothing recognises. `refunded` orders do
recognise and are then reversed under step 4b, which is what REV-14 asks for.

### G1 — Gift-card breakage lands in the fiscal month it ages out in

REV-12 dates breakage "the last day of the fiscal month the card ages out". The
last day of a fiscal month is inside that fiscal month, so at ledger grain the
month is the one the age-out date falls in.

### G2 — Breakage is the value left, and the escheat column decides

REV-12: "Breakage is the value left on the card on the day it ages out, not the
value it was sold for", and "A card issued in a jurisdiction where
`escheat_applies` is true escheats instead and never recognizes breakage." The
feed writes a breakage entry for every card with a balance at 24 months, and
this working drops the ones in escheat jurisdictions: at the full profile
18,286 breakage entries, of which 9,949 escheat and 8,337 recognise.

Both halves of that were faults in the feed before the reconciliation. It broke
only the cards with no redemption at all, so a card spent down to a stub never
broke; and it broke cards in escheat jurisdictions along with the rest, which
made the 66-row reference table say nothing.

### G3 — A card is one entity and one rate, struck at the sale

REV-12: "Every amount on a card converts at the rate for the card's issue date,
redemptions and breakage alike", and "Every amount on a card books to the
entity that sold it." One rate and one entity per card is what keeps a card's
redemptions and its breakage from adding up to more or less than the card.

### D1 — `settled` is the customer's favour and nothing else is

REV-10 names the four statuses and the column: `settled` books `settled_cents`
as negative revenue on `resolved_on`; `upheld` and `rejected` are Copperline's
favour and book nothing; `open` stays recognised. `disputed_cents` is what the
customer claimed and never books.

**Swing.** The first version of this file read `upheld` as upheld for the
customer and wrote off `disputed_cents` on those 399 rows. That was
231,772,215 cents of negative revenue that should not have been there.

### F1 — The ledger is in USD cents

REV-7 converts a line "in a currency other than the entity's functional
currency". The only rate `raw.fx_rates` carries is `rate_to_usd_ppm`, and
`raw.invoices.fx_rate_ppm` is 1,000,000 on every USD row, so the currency
every amount converts into is USD. `currency_code` NULL, on every row before
the 2025-04-07 cutover, is USD by construction, per REV-7.

`raw.invoices.fx_rate_ppm` agrees with `raw.fx_rates` for the invoice date on
all 26,817 non-USD invoices at the full profile. I still take the rate from
`raw.fx_rates`, which is what the clause names.

### F2 — Conversion rounds half up to the cent

REV-7 gives no rounding rule and REV-2's floor is paired with a remainder that
recovers the lost cents. FX has no remainder to put anything in, so a floor
would lose money on every line. Half up, away from zero:

    usd_cents = sign(c) * ((abs(c) * ppm + 500000) // 1000000)

all in integers, no float at any step, per "Money, days and time zones".

### F3 — A reversal converts at the rate of the thing it reverses

REV-5: "A credit converts at the rate its invoice converted at... A dispute
write-off under REV-10 is a credit memo for this purpose and follows the same
rate." REV-14 says the same for a marketplace return: the rate of its order's
ship-confirmation date. Gift-card amounts convert at the card's issue date,
per G3.

The credit-memo currency equals the invoice currency on every row of the full
profile (11,528 memos). 219 of the 1,583 disputes sit on a non-USD invoice, so
the rate chosen for a dispute does move money.

### R2 — REV-8 fires, on the memos that arrive late

REV-8 pushes an amount that belongs to a closed month onto the first day of the
earliest open month, and it names the arrival: "The arrival is `loaded_at`, as
a business date... A row that arrived on the close day itself is late." The
invoices and marketplace orders never arrive that late — the widest gap between
a transaction date and its `loaded_at` is 5 days, and a month never closes
sooner than the 5th business day after it ends — but the credit-memo feed
carries a population issued 45 to 84 days after their invoices, and some of
those land after the close of the month they were issued in.

## 4. Working the months

74 rows: 5 entities across the months each has been live and closed.

| Entity | First closed month | Months | Recognised, cents |
|---|---|---|---|
| CL-US | FY2024-P01 | 28 | 202,468,243,057 |
| CL-GB | FY2025-P03 | 14 | 19,597,945,401 |
| CL-IE | FY2025-P03 | 14 | 7,215,588,683 |
| CL-DE | FY2025-P03 | 14 | 14,313,472,846 |
| CL-MX | FY2026-P01 | 4 | 42,307,777 |
| | | **74** | **243,637,557,764** |

Every plan line in the world belongs to CL-US, so steps 1, 2a, 2b and 8 are a
CL-US story throughout. The other four entities are goods, freight,
marketplace, gift cards, credit memos and dispute write-offs.

By step, over the 74 rows:

| Step | Cents |
|---|---|
| 1 ratable, current era | 137,446,565 |
| 2a legacy prepay, annual | 3,254,200 |
| 2b legacy month billing | 170,603,687 |
| 3 goods and freight | 245,730,130,856 |
| 4a marketplace commission | 2,499,643,850 |
| 4b marketplace returns | -42,453,914 |
| 5 gift cards | 641,490,006 |
| 6 credit memos | -5,258,423,334 |
| 7 dispute write-offs | -241,263,131 |
| 8 refunds voided under REV-6 | -2,871,021 |

### FY2024-P01 — the first month

2024-02-04 to 2024-03-02, closed 2024-03-08. One entity.

    1 ratable, current era      CL-US         1,408,212
    2a legacy prepay, annual    CL-US             4,721
    2b legacy month billing     CL-US         3,650,230
    3 goods and freight         CL-US     7,244,194,841
    4a marketplace commission   CL-US        55,991,581
    5 gift cards                CL-US           512,890
    6 credit memos              CL-US       -72,145,549
    8 refunds voided            CL-US           -75,196
    total                       CL-US     7,233,541,730

No dispute write-off: the first dispute in the world had not closed yet. No
marketplace return either, because a refund posts with the payout run three
weeks after the sale and the first of those falls in P02. Gift cards are small
because the world is 24 months short of its first breakage and the cards issued
in the first weeks had barely been redeemed.

### FY2025-P03 — three entities open, and the first non-USD invoice

2025-04-06 to 2025-05-03, closed 2025-05-09. The hardest month in the file.
Three entities begin, the currency cutover falls on 2025-04-07 inside it, and
the first EUR and GBP invoices and marketplace orders land in the same week.

    1/2a/2b ratable             CL-US        12,632,609
    3 goods and freight         CL-DE       850,372,862
                                CL-GB     1,249,343,370
                                CL-IE       438,494,134
                                CL-US     5,702,357,963
    4a marketplace commission   CL-DE         7,225,069
                                CL-GB        10,620,425
                                CL-IE         3,509,971
                                CL-US        53,529,057
    4b marketplace returns      CL-US        -1,326,203
    5 gift cards                CL-DE            45,002
                                CL-GB            31,533
                                CL-IE            15,702
                                CL-US        24,215,815
    6 credit memos              CL-DE        -9,636,600
                                CL-GB       -11,435,501
                                CL-IE        -7,517,399
                                CL-US      -136,139,504
    7 dispute write-offs        CL-US       -10,387,479
    8 refunds voided            CL-US          -213,830

    total                       CL-DE       848,006,333
                                CL-GB     1,248,559,827
                                CL-IE       434,502,408
                                CL-US     5,644,668,428

Three things had to be got right here. The month is the first where
`currency_code` stops being NULL, so a rule that treats NULL as a missing rate
rather than as USD breaks on the row before the cutover, not on the row after.
The gift-card amounts are the first ones that stay with their own entity
instead of booking to CL-US under E1, and they are tiny for exactly that
reason: a card sold in Germany in 2024 is a CL-US card for life, so the new
entities start from nothing. And CL-IE has a marketplace line from its first
month, which it could not have had while the euro was being split by currency.

### FY2026-P04 — the last closed month

2026-05-03 to 2026-05-30, closed 2026-06-05, which is the date the policy's own
worked month gives. FY2026-P05 runs 2026-05-31 to 2026-07-01 and has not
closed, so it has no row: "The ledger covers a month only once that month has
closed."

    1 ratable, current era      CL-US         3,712,115
    2a legacy prepay, annual    CL-US            70,325
    2b legacy month billing     CL-US         4,433,501
    3 goods and freight         CL-DE       885,430,489
                                CL-GB     1,371,898,447
                                CL-IE       569,574,138
                                CL-MX         9,748,159
                                CL-US     6,169,943,187
    4a marketplace commission   CL-DE         8,566,999
                                CL-GB        12,420,344
                                CL-IE         5,377,893
                                CL-MX           116,136
                                CL-US        54,588,849
    4b marketplace returns      CL-DE          -162,223
                                CL-GB          -181,273
                                CL-IE           -64,874
                                CL-MX            -5,314
                                CL-US        -1,035,243
    5 gift cards                CL-DE         2,643,675
                                CL-GB         3,386,266
                                CL-IE         1,093,812
                                CL-MX               141
                                CL-US        37,632,001
    6 credit memos              CL-DE       -25,872,944
                                CL-GB       -25,770,807
                                CL-IE       -18,832,536
                                CL-MX          -212,887
                                CL-US      -146,930,984
    7 dispute write-offs        CL-DE          -325,673
                                CL-GB          -486,399
                                CL-IE          -839,840
                                CL-US        -3,677,657
    8 refunds voided            CL-US          -118,166

    total                       CL-DE       870,280,323
                                CL-GB     1,361,266,578
                                CL-IE       556,308,593
                                CL-MX         9,646,235
                                CL-US     6,118,617,928

CL-MX's gift-card line is 141 cents: one redemption on one card. It is the
smallest true number in the file and a good check that nothing rounds a small
amount away.

## 5. Checks I ran on the working

- **REV-2 holds.** Allocated daily and summed back over every fiscal month
  each line touches, all 24,080 ratable lines return their own amount to the
  cent. That is the property the policy says the tie depends on.
- **One month by hand.** CL-GB FY2026-P04 at the small profile: goods and
  freight, marketplace commission and gift cards and nothing else, which is
  what `close.py` prints.
- **Row count at both profiles.** 74 rows at small and at full, from the same
  entity first months. The trade agreement book does not scale between
  profiles (24,080 plan lines in both), so steps 1, 2a, 2b and 8 are identical
  at the two profiles; every other step scales about twentyfold.
- **74 is the whole of it.** Every one of the 74 entity-months holds activity —
  none is a zero row — and no closed entity-month at or after an entity's first
  month is missing.
- **The other book agrees.** `tools/copperline_oracle` and this working land on
  the same number on all 74 rows, at the small profile and at the full one.
  `tests/test_copperline_reconciliation.py` is the gate; section 7 says what it
  took to get there.

## 6. Redoing it

    python tools/copperline_ledger_workings/close.py --db <world.duckdb> \
        --csv worlds/copperline/workspace/fixtures/finance/ledger_monthly.csv

The CSV is byte-pinned in `tests/test_copperline_ledger.py`. If the world's
raw data changes, that test fails, and it should: someone has to work the
months again and decide whether the ledger moved or the pipeline did.

## 7. The reconciliation

Spec 07 section 8: a difference between this working and
`tools/copperline_oracle` resolves by a change to the fixtures or a change to
the policy text, **never by an edit to the ledger to make it agree.** Sixteen
differences were found. None was closed by moving a number.

| What the two books disagreed about | Ruling | How it was fixed |
|---|---|---|
| Is tax inside `line_total_cents`? | No. The line's extended amount is `qty * unit_price - discount` on every row, and `tax_cents` sits beside it. | policy: REV-1; fixture: the plan invoice header now reads net + tax |
| Legacy grain | The whole billing period in the fiscal month it starts in, split by `billing_frequency`. | policy: REV-11 |
| REV-6's population | `raw.plan_lines.cancelled_on`, which now holds a date inside the term. | policy: REV-6; fixture: `extracts/ar.py` |
| Which dispute status writes off | `settled`, at `settled_cents`. | policy: REV-10 |
| Which entity a marketplace order belongs to | The one on the order. | fixture: `entity_code` on `raw.marketplace_orders`; policy: REV-14 |
| When a marketplace return posts | With its own payout run, three weeks after the sale. | fixture: `posted_at` on the settlement line |
| Which rate a marketplace return converts at | The order's ship-confirmation rate. | policy: REV-14 |
| Which entity a pre-go-live gift card belongs to | CL-US, and for every entry on that card. | fixture: `extracts/promos.py`; policy: REV-12 and the entity table |
| Which rate a gift card converts at | The issue-date rate, redemptions and breakage alike. | policy: REV-12 |
| What breaks | The value left on the card, whether or not it was ever redeemed. | fixture: `extracts/promos.py`; policy: REV-12 |
| Which cards escheat | The ones whose jurisdiction has `escheat_applies`. | fixture: the feed no longer breaks them for recognition purposes; policy: REV-12 |
| Which rows are store credit | None; store credit is not in `raw.gift_cards`. | policy: REV-12 |
| Which rate a credit memo converts at | The invoice's. | policy: REV-5 |
| Where an entity's coverage starts | The first closed month it recognised anything in, never before it began trading. | policy: REV-9 |
| Whether a UTC timestamp is converted | Yes, once, on the way in, and the policy names the four columns. | policy: "Money, days and time zones" |
| What the monthly figure is made of | The nine steps above, and nothing else. | policy: "A worked month" |

Two of those needed the fixture corrected because the data was making a claim
that was not true: the gift-card feed stamped entities that did not exist yet,
and `cancelled_on` was stamped with the line's own term end. A third, the plan
invoice header, was corrected so that the world stops arguing with the clause
that governs it.

The one difference that was neither and stays open is A2, the header against
the lines on an order-linked invoice. It is a real fault in the AR feed, it is
worth 4% of the invoice book, and it moves no recognized figure because REV-1
says recognition reads the lines. It stays as it is.
