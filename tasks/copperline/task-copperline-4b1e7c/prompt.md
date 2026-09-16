# CUS-233 — trade order history begins at the replatform

Two trade accounts called support this week to say the order history we show
them starts in November 2024. Both have bought from us since 2022, and both are
right. Everything we hold against an account is keyed on the account id, and
the OMS was not writing today's ids before the replatform.

Support has been answering these by hand out of `raw.orders` and getting a
different answer each time. Sales want September to December 2024 first,
because that is the window the FY2024 volume review argues about.

Land the attribution once, in the warehouse, so everything that needs it reads
the same answer.

## What to build

A one-off job, `cus_crosswalk_backfill`, in `projects/customer/dags/`. It fills
`ops.customer_ref_resolved` for every order placed between **2024-09-01** and
**2024-12-31** that carries a customer reference:

| column | what it holds |
|---|---|
| `order_id` | the order |
| `customer_ref` | the reference the OMS wrote on it |
| `customer_id` | the account that reference belongs to today |
| `ds` | the order's date, and the partition key |

The table is not in the warehouse yet.

## What it has to hold

- One row per order. Every order in the window that carries a reference gets
  one — the rows flagged as test and the ones the OMS has since deleted
  included. This is an attribution table, not a revenue table, and it has to
  account for every reference that was ever written.
- `customer_id` is never null. Every reference the OMS wrote belongs to an
  account, including the accounts that never made it onto the new platform.
- It is a one-off over one window, so triggering it with no configuration does
  the whole window.

Support reads this straight back to a customer, so an account's history has to
come out whole and come out under one id.
