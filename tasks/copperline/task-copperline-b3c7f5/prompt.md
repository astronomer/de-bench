# CRD-141 — the dispute mart the job checks and publishes is not the mart it was written against

`fin_invoice_dispute_daily` builds `marts.dispute_daily` at 10:00, checks the
money in it, and publishes the day out to `include/data/marts/`. Not one day of
that mart has ever reached a file: the publish sorts by two columns the mart has
not got and raises before it writes a line. The cents check ahead of it goes
green every morning and has never looked at a write-off.

Both steps were written against a dispute mart finance drew up and commerce
never built. What got built is commerce's model, at commerce's grain, and it is
the mart the job has. Read the model. `dbt/` is not ours: commerce will not
re-cut their model so that finance's job can keep the shape it assumed.

Reconcile the check and the publish to the mart that is there.

**The check has to check something.** `assert_integer_cents` takes a list of
column names, and a name the table has not got costs nothing and says nothing —
read it. Hand it the money the mart holds for an invoice dispute: what the
customer claimed, and what Copperline gave up.

**The published file has to come out the same every time.** Credit control keep
the files and diff one day against the next, so two runs of the same day may not
land two different orderings of the same rows. Whatever the publish sorts by has
to give every row of a day a place of its own.

The job keeps the four steps it has, and it still publishes the whole partition
rather than a cut of it.

Leave `open_count` alone. CRD-140 has the collections table and the open figure,
and the two tickets land on the same file.
