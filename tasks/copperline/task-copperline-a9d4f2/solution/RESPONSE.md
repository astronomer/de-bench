# FIN-457 — board pack revenue line

## Which number the page has to show

`reported_cents`. `contracts/board-pack.md` §BP-1 names the pack's four columns and that is
one of them, and `docs/semantic-definitions.md` §SD-2 makes the name reserved: `reported_cents`
is `recognized_cents` net of refunds with legacy-era plans removed, and it lives in
`marts.account_rollup`. The same contract's notes say it in as many words — the two are not
the same number, and the difference is whole rows rather than pennies.

## What the line was showing instead

`sum(recognized_cents)` out of `marts.revenue_recognized_monthly`, written under the
`reported_cents` header. That is C-2's mart and C-2's measure. The companion accounts page in
the same deck has always read `reported_cents` off `marts.account_rollup`, so the headline and
the page under it have never been the same measure.

## What the difference is made of

Two things, both of them whole rows rather than rounding:

- the legacy-era book. §BP-2 excludes `billing_era = 'legacy'` from the pack, and that is
  about half the invoice book, which is why the headline came to roughly twice the rollup.
- credit memos. `reported_cents` is net of refunds; `recognized_cents` is not.

## Delivered

`fin_board_pack_weekly.company_revenue` now sums `reported_cents` out of
`marts.account_rollup` to fiscal month and entity. The grain, the columns and the range of
months on the page are unchanged, and so is the accounts page — it was the half of the deck
that was already right.

The pack no longer reads `marts.revenue_recognized_monthly` at all, so its row came off that
table in `docs/lineage.md` and off the C-1 line in `docs/report-registry.md`. §LIN-1 says that
list is the checklist.

Nothing under `dbt/` moved. The exclusion stays in the mart, where §BP-2 puts it, so the pack
and every other reader of that mart agree about it; and `marts.revenue_recognized_monthly` is
the close file's before it is the pack's, which §CM-2 says is corrected downstream rather than
in the shared model.

Relabelling the header to `recognized_cents` was the other way to close this. It is not the
pack's to do: §BP-1 pins the columns, finance owns C-1, and §CM-1 puts an amendment before the
code. It would also have left the deck holding two measures, which is what the ticket asked to
end.
