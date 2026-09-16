# CUS-441 — the footnote

- 16.3% of the orders in the week of 8 March 2026 carry no account at all:
  4,184 guest orders out of 25,617 on the store, web and marketplace channels.
- `repeat_order_share_bps` is divided by `member_orders` — the orders that name
  a loyalty member — and not by the day's orders. Against the day it reads
  about fifteen hundred basis points lower.
- A guest order reaches `order_count` and `guest_orders` and no other column on
  the page. It is not a new member, it is not a repeat one, and it is not in
  `distinct_members`.
