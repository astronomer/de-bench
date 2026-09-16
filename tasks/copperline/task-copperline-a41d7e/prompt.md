# Ticket PAY-207: the card month the switch falls inside

The payments squad reconciled the migration quarter in a spreadsheet and signed it off. The
spreadsheet is on a drive nobody can reach. The only month of that quarter anyone still asks
about is the one the switchover sits inside, September 2025, and three people have produced a
card total for it. No two of them agree.

Finance wants that month worked once, from the feeds, and written down, so that the next
person who asks gets the same answer and can see how it was reached.

`docs/runbooks/processor-migration.md` says what the migration did.
`docs/billing-integration.md` says what the fields mean. `docs/reconciliation-policy.md` says
who wins. Between them they decide every figure below. Work from the tables they name.

Write it up in `RESPONSE.md` at the root of the working tree:

- the fiscal month, the two dates it runs between, and which processor is authoritative for
  which part of it. Say where you read the boundary and give the date it moves on
- how many payments the month holds, taking the authoritative feed for each date and nothing
  else. Give the whole month's count. Give the count for the part of the month the old
  processor still owns
- the money, in integer cents, one line per transaction currency. Do not add the currencies
  together. A figure across three currencies is no use to anybody
- the rows you left out because the processor that sent them was not authoritative that day.
  How many, and how they split between the two feeds
- how many of the month's payments a later correction restates. Say which date a correction
  belongs to and name the clause that decides it
- how many of the month's rows the sending processor has since deleted, what happens to them,
  and whether they sit inside the figures above or outside
- how many orders sit behind the month's payments, and why that is not the count of payments

Date every payment by when the processor took it, on that processor's own clock. Not by the
day the file reached us, and not by the day the money moved.

Count every payment the authoritative feed carries for its date, whatever state it ended in.
A reversal and a payment still pending are both payments the processor took.

Cents throughout, and integers. A figure in dollars is no use to the close.

Keep it short. It goes in the close pack behind the migration file.

This is a question, not a change. Nothing in the pipelines moves for it.
