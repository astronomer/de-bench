# Ticket FIN-419: the credits are sitting in the wrong month

Last Monday's deck showed the same credit memo twice. The top-twenty account page carried it
in one fiscal period and the GL page, two slides later, carried it in the next one. Both read
the same memo book. One of them is wrong, and `docs/finance-policy.md` says which.

Nobody is changing a model this week. The controller wants the size of it first, because a
restatement paper has to say which periods move before it says what to do about them.

## What to work out

Where every credit memo belongs, and where the account page puts it. Work from
`raw.credit_memos` and the invoices they credit; the policy is the authority on where a memo
belongs, and the account page is only evidence of where it currently sits.

Write it up in `RESPONSE.md` at the root of the working tree.

- On a line of its own, over the whole memo book:
  `Credit memos filed to the wrong month: <n> of <total>`
- Which way a single memo can move. It can only land on one side of where it belongs, never
  the other, and there is a fact about the memo book that makes it so. Name the fact.
- The interim review covers the first half of FY2025 — periods 1 to 6. One row per period,
  with three figures: the credit total the period should carry, the credit total the account
  page files under it, and the movement between them.
- The one of the six that moves furthest, named, with its figure.

## Four rulings, so you do not have to guess

- Company wide. The entity split belongs to the pack; this is one figure per period.
- USD cents, and integers. Convert every memo once, at the rate the policy names for a
  credit, rounded half up to the cent. Convert **both** of the two totals the same way, so
  the only thing that moves between them is the month. The account page's own arithmetic
  converts nothing at all — that is a separate ticket and this one does not touch it.
- Fiscal periods, from `raw.fiscal_calendar`. FY2025 P1 is not February.
- Movement is the corrected total less what the page files. A positive figure means the page
  is carrying too little credit in that period.

We are not chasing cents. Every figure this page exists to show is thousands of times larger
than a rounding difference.

Keep it short. It goes to the controller behind the restatement paper.

This is a question, not a change. Nothing in the pipelines moves for it.
