# RPT-556 — the channel split is wrong for the week of 25 May

Finance sent the board pack back. The channel split for the week beginning
2026-05-25 is light against what the order spine says for the same week, and the
day-over-day figures for that week do not add up.

Monday 2026-05-25 is the day that is wrong. Every run that day and that week
finished green. Nothing failed, nothing retried, nothing paged, and the on-call
notes close the week as "no failures".

Work out why the 25th came out the way it did, and fix it. The fix holds two
rules:

- A day no market was due to send anything publishes nothing at all — no mart
  rows and no partition file. A published day with nothing in it reads as a real
  zero to everybody downstream. A day that is not there reads as not there.
- A day where only some markets were due still publishes what those markets
  sent. Partial days are ordinary and there are several a month.

No other day may change. `docs/retention-policy.md` RET-3 says a published
partition is immutable, so the empty one has to not go out in the first place.
