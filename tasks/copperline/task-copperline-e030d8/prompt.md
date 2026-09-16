# CUS-441 — the repeat-purchase line, and what it is divided by

The growth pack has carried a "repeat rate" since January and the chart calls
it the repeat rate of the business. It is not. It came out of a spreadsheet
built off the loyalty export, the loyalty export only holds the orders that
named a member, and a large part of every trading day names nobody at all —
the store and web channels take guest checkout and the order carries neither a
trade account nor a loyalty id. Those orders have never been in the top or the
bottom of that fraction, and nothing on the chart says so.

We are not arguing about whether a guest is a customer. We are asking the
warehouse to publish the split with the populations named on the row, so that
the next person who quotes the number can see what went into it.

## What to build

`marts.repeat_purchase_daily` — one row per channel per day.

| column | type | |
|---|---|---|
| `ds` | DATE | the local order date, the way the order spine keeps it |
| `channel` | VARCHAR | |
| `order_count` | BIGINT | every order the channel took that day, whatever became of it afterwards |
| `guest_orders` | BIGINT | the orders with no account on them |
| `member_orders` | BIGINT | the rest |
| `new_member_orders` | BIGINT | member orders where this day is the member's first |
| `repeat_member_orders` | BIGINT | member orders where the member had bought before |
| `distinct_members` | BIGINT | the members who traded that day, counted once each |
| `repeat_order_share_bps` | INTEGER | `repeat_member_orders` over `member_orders`, in basis points |

`order_count` is `guest_orders` plus `member_orders`, and `member_orders` is
`new_member_orders` plus `repeat_member_orders`. We will check both.

A channel that took no orders that day gets no row.

## The population

The three consumer channels — store, web and marketplace — and nothing else.
The trade channel is out: a trade order always names an account, the
active-account count is NW-214's question, and the two id formats the re-key
left behind are a crosswalk this table does not need.

Work from `raw.orders`. Take the rows staging takes and drop the rows staging
drops.

## The rules, because they are what the meeting will argue about

- **A guest order is an order without an account.** It counts in
  `order_count`. It is not a new member and it is not a repeat one, so it is
  in neither of those columns and it is not in `distinct_members`. Guests do
  not share an identity with each other and they do not get one each: there is
  nothing on the row to hang a lifecycle on, which is the whole reason this
  table names them separately.
- **One population, read twice.** Whatever you drop from the day, drop from
  the first-order lookup as well. An order that is not an order is not
  somebody's first order either.
- **The first order is the earliest the feed holds**, over the whole history —
  not over the day, and not over whatever range somebody happens to be
  rebuilding. A member who first bought in 2024 is a repeat buyer today.
- **Same-day ties go to new.** Where a member's first order date is the day
  being built, every order they placed on that day is new. Two rebuilds of a
  day, and the days either side of it, have to agree.
- **`repeat_order_share_bps` is divided by the member orders**, not by the
  day's orders. Both are defensible, they are about fifteen hundred basis
  points apart, and the column name is the one that carries the population.

## Where it goes

`projects/customer/lib/repeat.py`, holding
`build_daily(ds: str) -> int` — the rows written. Rebuilding a day replaces
that day and touches no other.

No DAG yet, and leave the nightly dbt build alone. `marts.loyalty_daily`
already answers the member-and-guest half of this and it answers it well;
putting a whole-history first-order lookup into a model every team reads is a
change we will make once we have agreed the definition, through
`docs/change-management.md`. Build it beside, and we will fold it in when the
pack has quoted it twice.

Build the week of 8 March 2026 for Monday. It has to work for any day the
spine holds.

## Three lines for the pack

`RESPONSE.md` at the root of the working tree:

- the share of that week's orders that carry no account, as a percentage
- which population `repeat_order_share_bps` is divided by, named
- the columns on the page a guest order reaches, named

Three lines. It goes in the footnote under the chart.
