# FIN-402

- The pack's number is the close's number less two whole populations: the
  legacy-era plans that `contracts/board-pack.md` §BP-2 keeps out of the deck,
  and the credit memos issued in the month. For FY2026-P02 that is 6,399,418
  cents of legacy plan and 241,461,991 cents of credits — 247,861,409 cents
  between `recognized_cents` and `reported_cents`, and none of it is rounding.
- A refund moves `recognized_cents` and `reported_cents`, because §REV-6 either
  reverses the schedule or stops it. It never moves `booked_cents`: the order
  was booked on the day it was taken and nothing after that day touches it.
