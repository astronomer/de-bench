# Ticket FIN-311: what is May 2026 made of?

The close file has been assembled by hand from Ironwood every month since FY2024, because
`marts.revenue_recognized_daily` and `marts.revenue_recognized_monthly` were never built.
That means the cent-exact tie the close file promises has never actually run. We copy the
ledger into the file and then compare the file to the ledger, and it agrees every time.

May 2026 is closed and the controller has signed it. Before analytics builds the two
models, she wants that month worked from the source feeds and taken apart, so the build has
a target for each part of the figure rather than only for the total — and so we find out,
for the first time, whether the warehouse can reproduce Ironwood at all.

`docs/finance-policy.md` is the authority and "A worked month" in it is the shape she wants
the answer in. Work every step from the tables the policy names. The ledger is what you
check the finished total against; it is not where any part of the answer comes from.

Write it up in `RESPONSE.md` at the root of the working tree:

- the fiscal month you worked, the two dates it runs between, and the day it closed
- the build-up: one line per step of the worked month that books anything, in the policy's
  own numbering, for the whole company. Each step is one net figure in integer cents — a
  step with two sides nets to one number on its line.
- the gift-card step split into the two things inside it, because the breakage figure is
  the one the auditors ask about every year and it has to be readable on its own
- the total, on a line of its own, written `Recognized revenue: <cents> cents`
- for each step that took a decision rather than a sum, one sentence saying what you
  decided and which clause decided it

The build-up has to add to the total, and the total has to agree with Ironwood entity by
entity. If it does not, say so and say by how much. Do not close the gap by moving a
figure: a difference we can see is worth more to us than a build-up that adds up.

Cents throughout, and integers. A figure in dollars is no use to the close.

Keep it short. It goes in the close pack behind the file.

This is a question, not a change. Nothing in the pipelines moves for it.
