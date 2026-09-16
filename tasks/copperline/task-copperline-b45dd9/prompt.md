# PAY-288 — put a size on the order reference before we sign the control off

Audit sat with the payments squad in the walkthrough last week and asked one question
nobody could answer. The control narrative finance signed says every Meridian settlement
is attributed to an order through the reference the processor sends back. Audit asked how
we know it is the right order, and how often it is not.

The only figure anyone has is in `docs/billing-integration.md` §B-6, which says a join on
that reference "matches about 96% of rows and is wrong". Nobody here knows where 96 came
from, what "wrong" means in orders, or what it costs. The refund recovery work is being
rebuilt on the same reference this quarter and finance will not sign the plan off until
the risk has a number on it.

Measure it. Nothing is fixed on this ticket — the repairs have tickets of their own.

Today is 2026-06-15.

## The populations, so that two people working this get one answer

- **The feed** is `raw.pay_meridian_settlements`, every row it carries, of every type and
  in every state. Do not filter it, do not deduplicate it, and leave in the rows the
  processor has since deleted and the ones a later event restates.
- **The order book** is `raw.orders`, whole. Test orders stay in and soft-deleted orders
  stay in. This is an audit of a join, not a revenue figure.
- **A settlement's own order**, where you need one, is reached through the attempt behind
  the event — `raw.payment_intents` on `intent_id` — and is the order that carries the
  attempt's reference and was placed within a week of the attempt's `created_at`. Over the
  whole feed that pairing gives one order per event and leaves none over. Say so if you
  find otherwise.
- **Money** means `amount_cents` added up as it stands, signs and all, over whatever rows
  are under discussion. It prices the join. It is not a revenue number and nobody will
  read it as one.
- **A period** is a fiscal period out of `raw.fiscal_calendar`, and an event belongs to the
  period its `event_time_utc` falls in.

## What to write

`RESPONSE.md` at the root of the working tree. Whole numbers throughout.

- **How often the reference repeats, and why.** The cadence, in whole days. Then the
  arithmetic behind it: how many references the format can hold, against how many order
  keys the OMS hands out in a day. Give both of those numbers, not only the cadence they
  produce.
- **The collision population in the order book.** How many orders the book holds, how many
  distinct references they carry between them, how many of those orders share a reference
  with at least one other order, and the most orders any single reference names.
- **What a reference-alone join does to the feed.** Join the feed to the order book on the
  reference and nothing else. Rows in, rows out.
- **How the feed splits under it.** How many events the reference resolves to exactly one
  order, how many to more than one, and how many to none at all. Then say what those three
  numbers make of §B-6's 96%.
- **What it misattributes.** How many distinct orders a reference-alone join hangs a card
  event on, against how many orders actually took one. Price both: the money on the rows the
  reference-alone join returns, and the money on the feed itself.
- **By period.** One row per period the feed touches: the events in it, the rows a
  reference-alone join returns for them, and how many of those events resolve to exactly
  one order. That last column is not shaped the way anyone would guess. Account for it —
  the reason is in the order book and it sits on one named day.
- **The key the documents send us to.** §B-6 and the header on
  `stg_payments__payment_intents` both point a reader at `raw.payment_intents`. Say whether
  the column on it that names an order does what it says. Give the range of that column,
  the range of the order book's own key, and how many rows the two pair up on.

Keep it to a page. It goes to audit behind the control narrative, and the first thing they
will do is try one of the figures themselves.

This is a question, not a change. Nothing in the pipelines moves for it.
