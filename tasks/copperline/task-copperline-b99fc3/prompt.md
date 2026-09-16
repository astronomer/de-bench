# SUP-1174 — the follow-up on the file that came twice

`ops/incidents/2025-11-03-vendor-file-doubled.md` closes with three follow-ups.
Two were done. The third was not:

> TODO: work out how many other loads would silently double on a repeated file.
> Nobody has looked.

The collector that sent the file twice has timed out twice more this quarter, so
a re-send is a matter of when rather than whether. Supply want the answer before
the Q2 close.

Go through the loads under `projects/` and write up what you find in
`RESPONSE.md` at the root of the working tree. Four things go in it.

**What made 3 November possible.** Both loads reported success and the day came
out at twice the amount. Say what in the shared load path lets that happen. Then
name the second, quieter way the same thing happens — the one where the call
reads as though it is already doing the right thing.

**Which loads append, and whether each of them is safe.** Parts of this estate
append on purpose. For each pattern you find, say whether a repeated file leaves
one copy or two, and what makes it so. A pattern you call unsafe and cannot say
why is worse than not naming it.

**The one job that the two arguments cannot save.** There is a feed here whose
shape defeats the usual fix. Name the job. Say what it is about the files it
reads that defeats it, and give the figure from those files that shows it.

**What you would do about that one.** At the call site: `include/lib/` is the
platform team's, and a change there is their review and their release.

Keep it short. Supply will paste it under the note.

This is a question, not a change. Nothing in the pipelines moves this week.
