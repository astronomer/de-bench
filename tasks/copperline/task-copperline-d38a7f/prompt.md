# OPS-412 — the channel split moved on 2 March and nobody moved a definition

Finance pulled the published daily revenue files for the fortnight to Sunday
8 March for the Q1 review. The channel mix steps on Monday 2 March: `trade`
goes up, `store` comes down, and the two moves are the same size to the cent.
It never steps back.

Nothing about that week looks broken from here. Every run finished green.
Nothing failed, nothing retried, nothing paged. The day's total is right — it
ties to the order spine to the cent, which is what `tie_to_orders` checks, and
it passed every night that week. All four channels landed a row. The only thing
that moved is which channel the money sits under.

Nobody changed a channel definition. `docs/semantic-definitions.md` has not been
edited this year and neither has any consumer contract.

Work out why the split moved and fix it, so that tonight's `fin_revenue_daily`
publishes the right one. Two rules the fix holds:

- the day's total still ties to the order spine to the cent
- every channel still lands a row for the day

Then write finance four short lines in `RESPONSE.md` at the root of the working
tree:

- which two channels moved, and what population moved between them
- which published days are wrong — finance only pulled the fortnight
- what re-publishing those days as they stand would have given
- nothing else. It goes straight into the Q1 pack.
