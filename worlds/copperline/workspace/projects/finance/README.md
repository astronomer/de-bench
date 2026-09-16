# finance-analytics

N. Brandt. `finance-eng@copperline.example`.

The ledger, recognition, the P&L, working capital, and the comparable-store
numbers the board reads. Sixteen models under `dbt/copperline_analytics/
models/finance/` and seventeen DAGs here.

`docs/finance-policy.md` is the authority on recognition and this project is
where it turns into code. Where the code and the policy disagree about
recognition, the policy is the statement of intent and the code is the defect.

## What we publish

| Output | Built by | Contract |
|---|---|---|
| the daily flash, out by 06:00 | `fin_daily_flash` | `contracts/daily-flash.md` |
| the close file | `fin_close_monthly` | `contracts/finance-close.md` |
| the Monday board pack | `fin_board_pack_weekly` | `contracts/board-pack.md` |
| the rendered reports | `fin_report_publish`, from `config/reports.yml` | none — see the registry's open items |

## What we do not rebuild

Revenue, cost of goods and the receipt match. Those definitions are shared, they
live in `int` under platform ownership, and commerce and supply-chain consume
the same ones we do. If finance rebuilt them the P&L and the sales dashboard
would disagree and both would be defensible, which is worse than a wrong number.

`int_gl_postings_unified` is shared and platform-owned, not ours, whatever its
subject looks like.

## No incremental models in `models/finance/`

Every model in this folder is a full rebuild. That is the rule and it is not
about size.

A restated month has to restate. An incremental model quietly keeps the figures
it already wrote, so a correction that lands after a build is a correction
nobody sees: the row is right in the source, right in a fresh build, and wrong
in the table the close reads. We have been on the wrong side of that once, in
FY2024, and it took a journal entry to fix.

`contracts/finance-close.md` states the same rule from the consumer's side.

Two of these marts are large. `fct_gl_lines` is about 6M journal lines and
`fct_invoice_line` about 1.05M invoice lines, so a full rebuild of either holds
the warehouse's single writer for minutes rather than seconds. That is a real
cost and it is the cost we have chosen. `projects/platform/README.md` states the
opposite rule for shared infrastructure; `docs/change-management.md` says which
one governs where the two meet, and a change that neither allows needs the
conflict written down before it ships.

## The close

A fiscal month closes on the 5th business day of the following month
(§REV-8). Before that the month is open and nothing is final; after it, nothing
restates. An amount that would have belonged to a closed month books on the 1st
of the earliest open month with a reason of its own.

`fin_restatement_apply` is the only sanctioned way to touch a closed month, it
runs on request rather than on a schedule, and it writes what it did.

## The tie

A published figure is not correct until `fin_ledger_tie` agrees with it.
`raw.finance_ledger` is the general ledger extract from Ironwood, at legal
entity by closed fiscal month grain, and it is the authority. A difference is a
defect in the pipeline until somebody shows it is a defect in the ledger, which
has happened twice.

The tie reads its subject table from `config/recon.yml` rather than naming it,
which is a leftover from when it ran over four subjects. One survived.

## Two things here dbt cannot see

`fin_ledger_tie` assembles its table name from config at run time, and
`fin_gl_export` holds forty lines of SQL and no `ref()`. Both read
`marts.order_economics` at order grain, both are listed in `docs/lineage.md`,
and neither shows up in a lineage graph or in a `dbt ls`. They are the two most
likely to be missed when that mart changes shape, because neither publishes
anything anybody would think to ask about.

## The retail edges

Three things bite this team more often than the recognition rules do.

- The fiscal calendar is 4-5-4 and FY2023 held 53 weeks, so a year-on-year
  comparison in FY2024 is off by a week if it uses a date offset. Read
  `comp_date_ly` from `dim_date`. Never `ds - 364`.
- Comparability is `docs/comp-store-policy.md`'s subject, not the finance
  policy's. Thirteen full fiscal months, closures restate both years, and an
  acquired store dates from the acquisition close.
- Legacy-era plans recognise at fiscal-month grain and have no daily rows
  behind them (§REV-11). A daily figure that includes them is wrong at the day
  and right at the month.

## Open

- `marts.revenue_recognized_daily` and `marts.revenue_recognized_monthly` are
  named in `contracts/finance-close.md`, in `docs/lineage.md` and by
  `fin_close_monthly`, and neither is built. The close is assembled by hand from
  the ledger every month until they are.
- `marts.budget_variance_weekly` waits on a budget extract that does not exist.
  `fin_budget_variance_weekly` publishes plan-against-actual from the plan lines
  in the meantime, which is not the same thing and is labelled as not the same
  thing.
