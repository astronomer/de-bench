# INV-317 — the weekly shrink pack has no money in it

Loss prevention have asked for the shrink pack three times this quarter and
have not had one. Two things are in the way and we think they are separate.

**The job cannot finish.** `sc_shrink_weekly` dies on the step that builds the
pack. Run it for any Thursday and it dies there.

**When it does finish, the money is wrong.** K. pointed the build at a copy of
the warehouse to see what would come out. Two things came back.

- Every week from the start of FY2026 prices at zero. Not low — zero, on every
  location and every department.
- The weeks before that price something, but only about one count in seven of
  them. The rest arrive under a row with no department against it, carrying
  most of the units and no money at all.

What loss prevention are asking is whether the losses got worse, so they want
the FY2026 weeks and the FY2025 weeks side by side. A column of zeros answers
nothing, and neither does a week that priced one count in seven.

## What the pack has to hold

- One row per location per department per fiscal week, carrying the units the
  counts moved and what those units were worth. Those are the columns the job
  already names and the ones loss prevention already read. Do not rename them
  and do not change the grain.
- Every count in the week is priced, not only the counts that fall on a day the
  warehouse happened to take a position.
- Every week is priced on the basis that was in force for that week.
- Money is an integer number of cents.

## One thing K. worked out and wrote down

A position row carries the retail value of everything on hand at that site that
day. It is not the price of one unit.

## What to write down

Leave a short note in `RESPONSE.md` for loss prevention. Say what changed under
us and when, whether the two halves of the pack answer the same question, why a
count taken on an ordinary day was priced at nothing, and which weeks the pack
has been wrong for.

## After this

Once the job is right we will put the weeks loss prevention asked for through
`plat_backfill_broker`. So the answer has to hold for a week nobody has asked
about yet, not only for the weeks in the request.
