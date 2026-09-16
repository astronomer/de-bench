# CUS-402 — the location dimension

`marts.dim_location` has been on the model list since the acquisition and has
never been built. The merge review wants it. Two accounts at one address are the
first thing a reviewer looks at, and today they read the address text by eye.

`cus_address_normalize_daily` is where it comes from. That DAG already gathers
the addresses on both books, normalises them and keys them.

Build `marts.dim_location` in that DAG, off the keyed addresses in
`ops.address_keyed`. One row per place per day:

    ds
    address_key
    country_code, region_code, postal_code, locality, street_line, unit
    parts_present
    accounts, accounts_copperline, accounts_northwave

- A place is an address key, not an account. Accounts that share a key are
  counted, not repeated.
- `address_key` is the key the run has already written. The review joins the
  dimension to `ops.address_keyed` on that column, so a second key worked out
  here from the same parts is a key nobody can join to.
- The six parts ride along, as the run wrote them.
- `accounts` is how many accounts sit at the place. The two book columns split
  it; `projects/customer/CONVENTIONS.md` says why both books stay visible.
- The acquired book holds a city and a state and nothing finer, so a lot of its
  accounts land on one key. That is the truth about that book and the review has
  to be able to see it: `parts_present` is what separates a street address from
  a city.
- A day is replaced, not added to. The nightly gets re-run.

Every account on both books goes in. Do not drop a book, a row, or a part of the
key to get the run finishing.
