# TRD-318 — no trade statement since the November close

The desk sends every trade account a statement when a fiscal period closes:
what the account ordered in the period and what it was billed, in the currency
it was billed in. `fin_trade_statement_monthly` builds it.

The last one that went out was for the period that closed on 29 November.
Every close since has died in the same place, and the on-call note carries the
same paste each time:

```
Invalid Input Error: Failed to read file
"landing/orders_export/dt=2026-03-02/part-000.parquet": schema mismatch in
glob: column "customer_ref" was read from the original file
"landing/orders_export/dt=2026-03-01/part-000.parquet", but could not be found
in file "landing/orders_export/dt=2026-03-02/part-000.parquet". [...]
```

Nothing in the job changed. Nobody has been in `projects/finance/` since
October.

Get the statement building again, for the periods it has missed as well as for
the next close. Four things hold when you are done.

- It stays built from the parquet orders export under `landing/`. That is the
  point of it: when an account queries a line we answer out of the same file
  the OMS sent, so the statement carries the OMS's own figures and not a
  number we derived somewhere else.
- Every trade order in a period sits under the account that placed it, and no
  line goes out under a blank account. An account's period total is every
  order it placed in that period, whatever the export called things that
  month.
- The periods that still build come out with the figures they carry today.
  Whatever the fix is, it does not move a statement we have already sent.
- The desk also wants the promotion the OMS allocates to an order, which the
  export carries as `promo_allocation_cents`. Put it on the statement under
  that name. The desk nets this figure off the account, so an empty cell and a
  zero are different claims: where the export did not carry the figure for the
  whole of a period, that period's cell is empty on every line — not zero, and
  not the part of it that happens to be there.

The export itself is the OMS's and we read what it sends. `fixtures/`,
`legacy/`, `include/lib/` and `CONVENTIONS.md` are not ours to change either.
