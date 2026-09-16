# FIN-402 — three revenue numbers for the same month, and one page to settle them

Monday's board pack, the close file and the flash each carried a revenue figure
for the same fiscal month, and the three do not agree. The meeting spent twenty
minutes on which one was right. The answer is that all three are, and nobody
could show it, because there is nowhere the three sit side by side.

`docs/semantic-definitions.md` reserves the three names and says what question
each one answers. Build the page that puts them next to each other with the
difference named.

## What to build

`marts.revenue_definitions_monthly` — one row per fiscal month, company wide.
Not per entity: the entity split belongs to the close file and the pack, and
this page belongs to the meeting.

| column | type | |
|---|---|---|
| `fiscal_month` | VARCHAR | `FY2026-P02`, the way `posted_period` spells it |
| `booked_cents` | BIGINT | |
| `recognized_cents` | BIGINT | |
| `reported_cents` | BIGINT | |
| `plan_cents` | BIGINT | the part of `recognized_cents` that came off the trade agreements, reversals and all |
| `legacy_plan_cents` | BIGINT | the part of `plan_cents` the pack leaves out |
| `credit_cents` | BIGINT | the credits the month carries, positive — it comes off |

`reported_cents` is `recognized_cents` less `legacy_plan_cents` less
`credit_cents`, and we will check that it is.

## The population

The trade book and nothing else: the trade orders, the invoices they raise and
the trade agreements — `raw.orders`, `raw.invoices`, `raw.invoice_lines`,
`raw.plan_lines`, `raw.credit_memos`. Marketplace commission and the gift-card
tender are the close file's own steps and are not on this page.

Both account books. The invoices that carry an `nwv_account_id` instead of a
customer reference are the acquired book's, and they are company revenue like
every other invoice; the crosswalk between the two id formats is somebody
else's ticket and this page does not need it.

Work from `raw`. `marts.revenue_recognized_daily` and
`marts.revenue_recognized_monthly` were never built, `marts.account_rollup`
needs them, and that is the whole reason this is being asked for now. Take the
rows staging takes.

## What each column answers

- `booked_cents` — what the trade arm sold that month: the order, on the day it
  was taken, gross. Nothing that happened to it afterwards moves it. Not a
  cancellation, not a return, not a credit.
- `recognized_cents` — what we earned that month, `docs/finance-policy.md`
  applied to the invoice book. Both halves of that book: the lines that carry a
  service period and the lines that do not.
- `reported_cents` — what the board pack shows. `contracts/board-pack.md` §BP-2
  is the exclusion, and it excludes the legacy-era plans — not the goods a
  legacy-era account bought.

Three rulings from finance, because the policy leaves them open and we would
rather you did not have to guess:

- USD cents throughout. Convert once, on the row, at the rate the row carries,
  rounded half up to the cent, before anything is allocated. §REV-7 says once
  and says when; it does not say which way to round.
- The credits come off in `reported_cents` and nowhere else. That is the
  difference the meeting is asking about, which is why `recognized_cents` on
  this page is gross of them. Every credit the month carries comes off,
  whatever it was raised against: the memo names a line on barely half its
  rows, so nobody allocates a credit to a line here.
- This page is not the close file. It does not tie to Ironwood and it does not
  restate: build every month from the rows whose own dates fall in it, and
  leave §REV-8's redirection to the close. We are not chasing rounding either.
  Every difference this page exists to show is a whole population, thousands of
  times larger than a hundred dollars.

## Where it goes

`projects/finance/lib/definitions.py`, holding
`build_monthly(fiscal_month: str) -> int` — the rows written. Rebuilding a
month replaces that month and touches no other.

No DAG yet. The schedule comes with the pack's next amendment and the builder
is what we need first. Leave `contracts/` alone for the same reason: we will
contract the page once the meeting has used it twice.

Build `FY2026-P02` and `FY2026-P03` for Monday. It has to work for any closed
month.

## Two lines for the slide

`RESPONSE.md` at the root of the working tree:

- the two things that separate the pack's number from the close's, named
- which of the three columns a refund moves, and which of them it does not

Two lines. It goes on the slide behind the page.
