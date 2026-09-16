# Revenue recognition policy

Maintained by finance-eng. Systems of record: `raw.invoices`, `raw.invoice_lines`, `raw.credit_memos`, `raw.disputes`, `raw.plan_lines`, `raw.fx_rates`, `raw.gift_cards`, `raw.gift_card_jurisdictions`, `raw.marketplace_orders`, `raw.fiscal_calendar`.

Comparable-store rules live in `docs/comp-store-policy.md`. This policy does not define comparability.

Where this policy and a consumer contract disagree, this policy governs recognition. Contracts govern presentation only, including which populations a consumer excludes. See `contracts/README.md`.

Last reviewed 2026-05-11 by finance-eng and the controller's office. Next review at the FY2027 planning cycle. Questions go to #finance-eng; the older wiki page at `wiki.copperline.internal/finance/rev-rec-v2` is retired and the redirect has been broken since the intranet move.

## What is in scope

Copperline recognizes three kinds of revenue ratably: trade programs (the annual and multi-year agreements the ~4,000 named trade accounts buy on terms), marketplace seller subscriptions, and fixture leases to trade customers. Everything else — a bag of screws over the counter, a web order, a marketplace commission — recognizes on its own event and is covered by REV-13 and REV-14.

The trade book is the reason this document is longer than a retailer's usually is. About a third of company revenue moves through it, the agreements run for a year or more, and finance closes against a hand-kept ledger in Ironwood every month. The rules below are what the two have to agree on.

## Who reads this, and when

Three groups read this document, and they read different halves of it.

The close team reads REV-8, REV-9 and REV-11 in the first week of every month, because that is the week the month closes and the tie has to hold. They read the rest once a year.

The analytics engineers read REV-1 through REV-7 whenever they touch a model under `models/finance/`. The clause ids are cited from model descriptions and from tickets, which is why they do not move.

Everybody else reads REV-12 through REV-16 when a retail question arrives — a gift card that aged out, a stacked promotion that came out at zero, a year-on-year comparison that is off by a week. Those four clauses answer more questions than the first eleven put together.

## The five entities

Copperline bills through five legal entities, and recognition is per entity. `raw.entities` holds these five rows and no others, and `first_traded_on` on that row is the day the entity opened.

| Entity | Covers | First closed month |
|---|---|---|
| `CL-US` | the United States, Canada, and cross-border markets with no local entity | FY2024 P1 |
| `CL-GB` | Great Britain | FY2025 P3 |
| `CL-IE` | Ireland, and Poland under EU VAT treatment | FY2025 P3 |
| `CL-DE` | Germany | FY2025 P3 |
| `CL-MX` | Mexico | FY2026 P1 |

An entity has no ledger coverage before the month it began trading, so a per-entity tie over a period that predates the entity has nothing to tie to. That is not a defect; it is an entity that did not exist.

A market has no local entity until the entity `raw.market_config` gives it has traded. Until that day the market is a cross-border market with no local entity, and everything it bills recognizes in CL-US. The day is `raw.entities.first_traded_on`, never the market's own `launched_on`, and the amount books to CL-US whatever entity code its feed carries.

## Money, days and time zones

Every rule below counts in integer cents. There is no step at which an amount is a float, and no step at which an amount is rounded to anything other than a cent. A money column carrying a float is a defect, not a rounding style.

Business dates are America/Los_Angeles, the zone of the Portland head office. Event timestamps in the source feeds are UTC and are converted once, on the way in. A "day" in this policy is a business date in the head-office zone; it is not a UTC day and it is not a store-local day.

Four recognition feeds carry a UTC timestamp rather than a date, and every one of them converts: `raw.marketplace_orders.ship_confirmed_at`, `raw.marketplace_settlements.posted_at`, `raw.gift_cards.issued_at` and `raw.gift_card_ledger.occurred_at`. Reading the UTC calendar day off one of those columns puts about one marketplace order in seven in the wrong month. Every other date in the recognition feeds is already a business date and needs no conversion.

## REV-1 Ratable recognition

A ratable line recognizes in equal daily amounts across its service period. The service period runs from `service_start` to `service_end` inclusive: both end days are inside the period, so a period from the 1st to the 31st is 31 days, not 30. A line with no `service_end` is not ratable and recognizes in full on the invoice date.

The amount that recognizes is `raw.invoice_lines.line_total_cents`. It is the line's own extended amount, `qty * unit_price_cents - line_discount_cents`, on every row of the book. Tax is not revenue, and it is not inside that column: `tax_cents` sits beside it and never recognizes. Recognition reads the lines and never the invoice header, so `raw.invoices.net_cents` is not a recognition input and a header that disagrees with its lines does not move a recognized figure.

## REV-2 The daily amount, and the remainder

The daily amount is `floor(line_cents / days)`. The remainder is recognized in full on the final day of the service period, never spread and never dropped. Nothing recognizes a fraction of a cent at any step.

A 4,097-cent line over a 31-day period therefore recognizes 132 cents a day for the first 30 days and 137 cents on the 31st. Summing the daily rows returns 4,097 cents exactly, which is the property the ledger tie depends on.

## REV-3 Annual prepays

An agreement paid in advance for a term of twelve months or longer recognizes across the whole term under REV-1, not on the payment date. The cash sits as a contract liability until it is recognized. The payment date has no effect on recognition and never moves an amount between periods.

## REV-4 Mid-period changes

An upgrade, a downgrade or a quantity change closes the old line on the day before the change and opens a new line on the day of the change. Recognition to date on the closed line stands and is not restated. The new line recognizes under REV-1 and REV-2 over its own service period. Never re-rate a line in place.

## REV-5 Credit memos

A credit memo recognizes as negative revenue on its issue date, at the credited amount in cents, against the same entity and the same fiscal period the issue date falls in. A credit memo never restates the invoice it credits, and never restates a period earlier than its own issue date. Credit memos issued against a legacy-era plan follow REV-11 for grain.

A credit converts at the rate its invoice converted at, which is the invoice-date rate, and never at the rate of its own issue date. It offsets an amount that has already been converted, and REV-7 allows one conversion per amount. A dispute write-off under REV-10 is a credit memo for this purpose and follows the same rate.

## REV-6 The refund boundary

A refund requested within 14 days of the invoice date voids the schedule: the whole recognized-to-date amount reverses and the line recognizes nothing. A refund requested after day 14 cancels the schedule forward: recognition to date stands and nothing further recognizes.

The 14-day test is by calendar date in America/Los_Angeles, and day 14 is inside the window. A refund on day 14 voids; a refund on day 15 cancels. The test never uses elapsed hours, and never uses the refund's own load time.

The refund request is `raw.plan_lines.cancelled_on`, and that column is the whole of REV-6's population. It carries the day the customer asked for the money back, which is inside the term the line bills. A line with no `cancelled_on` ran to its term and REV-6 does not touch it. Nothing else is a refund request: `raw.credit_memos` is REV-5's subject and books on its own issue date, and `raw.returns` is merchandise coming back against a retail order, which never reaches the trade book. Counting a credit memo under both clauses books it twice.

## REV-7 Foreign currency

A line in a currency other than the entity's functional currency converts at the invoice-date rate, once, at line level, before the daily allocation of REV-2. The rate is the value in `raw.fx_rates` for the invoice date, held as integer parts per million on the row. There is no second conversion, no re-translation on the recognition date, and no re-translation at close.

Rows dated before the currency cutover carry `currency_code` NULL and are USD by construction. Treat them as USD; do not treat NULL as a missing rate.

## REV-8 Closed books

Copperline closes a fiscal month on the 5th business day of the following month. A closed month is final. Nothing restates a closed month, and no adjustment is booked into one.

An amount that would have belonged to a closed month is booked instead on the 1st day of the earliest open fiscal month, carrying a reason of its own. `docs/reconciliation-policy.md` §R-5 applies this rule to processor corrections and names the row it writes.

An amount belongs to a closed month when its own date falls in one and it reached the warehouse after that month closed. The arrival is `loaded_at`, as a business date, on the feeds that carry one; a feed with no `loaded_at` is taken to have arrived on the day of the event. A row that arrived on the close day itself is late.

## REV-9 The ledger tie

`raw.finance_ledger` is the general ledger extract from Ironwood, at legal entity by closed fiscal month grain, and it is the authority a closed month is measured against. A published monthly figure for a closed month must equal the ledger to the cent, per entity. A difference is a defect in the pipeline until it is shown to be a defect in the ledger, which has happened twice since FY2024 and both times took a journal entry to fix.

The ledger covers a month once that month has closed and the entity has something to recognize in it. The open month has no ledger row and no tie. An entity's coverage starts at the first closed month in which it recognized anything, and never before the month it began trading; from there it runs unbroken to the last closed month. A closed month with nothing in it has no row.

The tie runs one way. Recognition is computed from the clauses above and the source tables they name, and the answer is then compared to the ledger. Nothing in REV-1 to REV-14 reads `raw.finance_ledger`, and a recognition model that takes a figure from it has stopped being a check and started agreeing with itself. The comparison belongs to the pipeline; recognition belongs to the clauses.

## REV-10 Disputes

A disputed amount stays recognized while the dispute is open. Recognition does not pause, and a provision is not netted against revenue. When a dispute closes in the customer's favour, the write-off books on the close date under REV-5. When it closes in Copperline's favour, nothing books at all.

`raw.disputes` carries `raised_on`, `resolved_on` and `dispute_status`. A row with `resolved_on` NULL is open, whatever its age.

`dispute_status` holds four values and only one of them books. `settled` is the customer's favour: the write-off is `settled_cents`, the amount Copperline gave up, and it books as negative revenue on `resolved_on`. `upheld` means the invoice was upheld and `rejected` means the dispute was rejected; both are Copperline's favour and book nothing. `open` books nothing and stays recognized. `disputed_cents` is what the customer claimed, never what was given up, and it never books.

## REV-11 Legacy-era plans

Agreements signed before 2025-07-01 carry `billing_era = 'legacy'` and were written on a monthly-grain plan model. A legacy line billed monthly recognizes at fiscal-month grain: the whole of its amount lands on the first day of the fiscal month its service period starts in, and there are no daily rows behind it. The service periods are calendar months, so a line that runs from the 27th to the 26th lands whole in the fiscal month that holds the 27th, and nothing of it reaches the next one. They are not re-cut to daily grain, and a daily figure that includes them is wrong at the day and wrong at the month.

`raw.plan_lines.billing_frequency` says which is which. A legacy line billed `monthly` follows the paragraph above. A legacy line billed `annual` is a prepay under REV-3 and recognizes across its whole term, allocated under REV-1 and REV-2 and then dated to the first day of each fiscal month it touches. The term length decides nothing; the billing frequency does.

Contracts signed on or after that date carry `billing_era = 'current'` and follow REV-1 and REV-2.

## REV-12 Gift cards and store credit

A gift card sale is a liability, never revenue. Revenue recognizes on redemption, on the redemption date, for the redeemed amount in cents. Unredeemed value recognizes as breakage 24 months after issue, as one amount dated the last day of the fiscal month the card ages out. Breakage is the value left on the card on the day it ages out, not the value it was sold for, so a card spent down to a stub breaks for the stub and a card spent in full breaks for nothing.

`raw.gift_card_jurisdictions` lists all 66 jurisdictions and decides each one on its `escheat_applies` column. A card issued in a jurisdiction where `escheat_applies` is true escheats instead and never recognizes breakage; its redemptions still recognize. The column is the test, not the presence of a row.

Every amount on a card converts at the rate for the card's issue date, redemptions and breakage alike. The liability is struck in the card's currency when the card is sold, and REV-7 forbids a second conversion on the recognition date. One rate per card is also what keeps a card's redemptions and its breakage from adding up to more or less than the card.

The entity is struck at the sale in the same way. Every amount on a card books to the entity that sold it, whether it redeems the same week or breaks two years later. A card sold in Germany before CL-DE opened was sold by CL-US, and its breakage is CL-US revenue.

Store credit issued as a refund remedy is not a gift card and never recognizes breakage. It is not in `raw.gift_cards` at all — that table is gift cards and nothing else — and it is recorded on the return that issued it, as `raw.returns.refund_method = 'store_credit'`. A gift card issued as a refund remedy is a gift card and REV-12 applies to it in full.

## REV-13 Promotion stacking

Discounts apply in this order, each to the amount remaining after the one before it: employee discount, trade-program discount, promotional markdown, coupon. Loyalty points are a settlement method, not a discount: they reduce the amount collected and never reduce gross or net sales. A line whose stacked discounts exceed its gross amount floors at zero cents; the excess is not carried to another line.

## REV-14 Marketplace

GMV is the seller's order value and is never Copperline revenue. Copperline recognizes commission and fulfilment fees, on the seller's ship-confirmation date, in cents. A marketplace return reduces commission in the period of the return and never restates the original period, including inside a closed month. Marketplace seller subscription fees follow REV-1.

The entity is `raw.marketplace_orders.entity_code`. The order currency does not decide it: CL-IE and CL-DE both bill in euro, and splitting a shared currency by any rule at all invents an allocation. A cancelled order never shipped, so it recognizes nothing and carries no settlement line. The return leg is the `refund` line of `raw.marketplace_settlements`, in the period of its own `posted_at`, at the entity of the order it belongs to; `principal` is the seller's GMV and never books. A return reverses an amount that has already been converted, so it converts at the rate of its order's ship-confirmation date, per REV-7, and never at the rate of the day it posted.

## REV-16 Fiscal calendar

The fiscal calendar is 4-5-4 and lives in `raw.fiscal_calendar`. FY2026 begins Sunday 2026-02-01. FY2023 held 53 weeks, so FY2024 week W compares to FY2023 week W+1; `comp_date_ly` holds it. Never compare by calendar-date offset.

## Why the numbering has a hole

There is no REV-15. The draft that became this version had a clause on comparable-store revenue between the marketplace clause and the calendar clause. Comparability is not this document's subject, so the clause moved to `docs/comp-store-policy.md` and the id was retired rather than reused. Clause ids are cited from tickets, models and the reconciliation policy, so renumbering the tail would have broken every citation for the sake of a tidy sequence. The ids run REV-1 to REV-14 and then REV-16.

## A worked month

May 2026 is the most recent closed month. It closed on 2026-06-05, the 5th business day of June. The sequence finance runs, in order. These nine sources are the whole of the monthly figure; nothing else books, and none of them is left out.

1. Take every current-era ratable line whose service period touches the month, allocate under REV-1 and REV-2, and sum the daily rows that fall inside the month.
2. Add the legacy-era plans at their fiscal-month grain, per REV-11.
3. Add every line with no `service_end` whose invoice date falls in the month, in full, per REV-1. That is the goods and freight book, and it is most of the number.
4. Add marketplace commission and fulfilment fees at their ship-confirmation dates, and subtract the marketplace returns posted in the month, per REV-14.
5. Add gift-card redemptions and any breakage aging out in the month, per REV-12.
6. Subtract credit memos issued in the month, per REV-5.
7. Subtract the write-offs of disputes that closed in the customer's favour in the month, per REV-10.
8. Reverse any line REV-6 voided in the month, and stop any line REV-6 cancelled forward.
9. Convert nothing. Every amount was converted once, at the date its own clause names, per REV-7.

Then compare the per-entity totals to `raw.finance_ledger`, per REV-9. That comparison is the step that fails. When it does, the difference is nearly always step 1 against step 2: a legacy-era plan cut to daily grain, or a current-era line rolled up as though it were monthly.

## The three questions people actually ask

**"The daily rows do not sum to the invoice."** They do, once REV-2's remainder is on the final day. A daily amount computed as `line_cents / days` and rounded per row drifts by up to a cent a day, and over a 31-day line that is up to 31 cents. Over the trade book it is a number the controller notices.

**"Which rate did we use?"** The invoice-date rate, once, before allocation. Not the recognition-date rate, not the month-end rate, not an average. If the number moves when the FX table is reloaded, something is converting twice.

**"Can we put this in last month?"** Not if last month has closed. REV-8 says where it goes instead, and the reconciliation policy says what the row looks like.

## Terms used here

| Term | What it means in this document |
|---|---|
| ratable line | an invoice line with a service period, recognized under REV-1 |
| service period | `service_start` to `service_end`, both days inside |
| billing era | `legacy` for agreements signed before 2025-07-01, `current` on or after |
| closed month | a fiscal month past its 5th-business-day close |
| open month | any fiscal month that has not closed; the current one always is |
| the tie | the per-entity comparison of a published monthly figure to `raw.finance_ledger` |
| breakage | unredeemed gift-card value recognized under REV-12 |

## What this policy does not cover

- Comparability. `docs/comp-store-policy.md` owns it, and this policy never defines it.
- Inventory valuation and cost. `docs/inventory-policy.md` owns the method and its effective date.
- Carrier rating and freight cost. `docs/rate-policy.md` owns the rate cards.
- What any one report shows. A consumer contract may exclude a population from its own output; that is presentation, and it does not change what recognized.
- Tax. The tax team keeps its own guidance and it is not held here.

## Where this runs

The policy is prose; these are the places it turns into code. The list is here so that a change to a clause has an address, not so that the code is read as the policy. Where code and this policy disagree about recognition, the policy is the statement of intent and the code is the defect.

| Clause | Where it lands |
|---|---|
| REV-1, REV-2 | `models/finance/` recognition models, built by `fin_close_monthly` |
| REV-5, REV-6 | `refunds_daily` and the credit-memo staging models |
| REV-7 | `fin_fx_rates_intake` lands the rates; conversion happens at line level upstream of allocation |
| REV-8 | `fin_close_monthly` and `fin_restatement_apply` |
| REV-9 | `fin_ledger_tie`, which writes `ops.tie_breaks` |
| REV-12 | the gift-card tender models under `models/finance/` |
| REV-14 | `marketplace_settlement_intake` and the commission models |
| REV-16 | `raw.fiscal_calendar`, read through `dim_date` |

`marts.revenue_recognized_daily` and `marts.revenue_recognized_monthly` are the two outputs the close file is built from. They are named in `contracts/finance-close.md` and in `docs/lineage.md`.

## History

| Version | Date | What changed |
|---|---|---|
| v1 | 2019-11-04 | first written, trade programs only |
| v2 | 2022-06-13 | marketplace added when the marketplace launched |
| v3 | 2025-05-19 | currency cutover; REV-7 rewritten for the invoice-date rate |
| v3.1 | 2025-08-25 | REV-11 added when the plan model split at the billing-era boundary |
| v3.2 | 2026-03-09 | REV-15 retired to the comp-store policy; wording of REV-6 tightened |
| v3.3 | 2026-05-11 | the close-tie review: every clause the FY2026 P3 tie broke on now names its own column, and the worked month lists all nine sources |

## Open items

- REV-3 says nothing about a term that ends mid-day on a leap day. It has not happened. TODO: decide before FY2028.
- The controller wants a worked example for REV-4 with a mid-period quantity change. Not written yet.
- `raw.gift_card_jurisdictions` is maintained by hand. The second-wave markets were added when they launched, so the 66 rows are complete, but `dormancy_months` on the escheat rows has not been checked against the statutes since FY2025.
- REV-11 describes the legacy plan model as it was written, not as anyone would write it now. The plan lines behind it are frozen and nothing new lands in them, so the clause is a reading rule rather than a design.
- The escheat column in REV-12 is jurisdictional and the treasury team owns it. Finance-eng owns the clause, not the values.
- Nobody has written down what happens to a marketplace commission when the seller's ship confirmation arrives after the month it was ordered in and before the month closes. REV-14 says the ship-confirmation date decides, which is the answer, but the question keeps coming back.

## Contact

Owner: finance-eng. Reviewers: the controller's office, and the analytics engineer on the finance rotation. Raise a change through `docs/change-management.md`; a clause that a consumer contract cites cannot be edited without telling that consumer's owner first.
