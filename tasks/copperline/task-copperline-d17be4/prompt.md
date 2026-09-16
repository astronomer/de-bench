# CRD-140 — credit control has no dispute position, and the table they were promised is empty

Credit control works the disputed book off a spreadsheet one of them keeps by
hand. `ops.dispute_open_daily` is the table they were told to read, and it has
never held a row.

`fin_invoice_dispute_daily` is the job that fills it. The step that writes it
reads `entity` and `open_disputes` off `marts.dispute_daily`, and that mart
carries neither. It is commerce's table, cut by dispute status and reason code
on the day a dispute was raised, and it does not know which entity an invoice
bills through. Rebuilding that mart is a separate ticket and not this one.

Fill the collections table from the dispute record itself. One row per entity
per day:

    ds                  the day the row is for
    entity              the entity the disputed invoice bills through
    open_disputes       how many disputes were open
    disputed_cents      what those open disputes claim
    written_off_cents   what we gave up on the day

Four things have to hold.

**Every day is that day's position.** Credit control will run this back over
the last few months to get a trend, and each day has to say what was open
*then* rather than what is open this morning. A dispute raised in February and
closed in April was open through March, and March has to say so. A dispute
closed on a day is closed on that day: it leaves the open figure and its
write-off lands on the same row.

**`written_off_cents` is what §REV-10 books and nothing else.** The clause is
in `docs/finance-policy.md`. Read it. It says which of the four dispute
statuses book, on which date they book, and at which amount.

**A day rerun replaces itself.** They will run the same day more than once
while they build the trend, and the second run has to leave one copy of it.

**The table does not exist.** Create it where you write it, the way the other
`ops` tables in this tree are created.

The job keeps the four steps it has. The mart rebuild, the cents check and the
publish belong to the other ticket, and `dbt/` is not ours this week.

One more thing. The note in the job about the open figure being a count rather
than a sum was written when this table had a single column. Credit control have
asked for the amount as well, so both go in.
