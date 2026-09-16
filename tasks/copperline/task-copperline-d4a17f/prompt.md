# PLAT-468 — the checklist has never been checked

`docs/lineage.md` §LIN-1 says it holds every consumer of every mart, including the ones dbt and
Airflow cannot see, and that it is the list to work before a mart changes. `docs/report-registry.md`
§REG-2 says the `Reads` column beside each consumer is the full read set. Both documents were
signed off on 2026-05-18. Neither has ever been read back against the code.

Three marts are being re-cut next sprint, and we are not working a list we have not checked:

- `marts.order_economics`
- `marts.sell_through_daily`
- `marts.feature_customer_daily`

Settle their rows both ways round. For every reader those three marts claim, open the file and
decide whether it reads the mart. Then go the other way and sweep the repository for anything
that reads one of the three and is not on the list. The two misses this document was written
after were both readers nobody had written down, so the second half is the half that matters.

**What counts as reading a mart.** The reader's own file, or a module of the reader's own
project that the file imports, or SQL of its own project that the file names. `include/lib/` is
the shared platform library and is nobody's read. A job that assembles the table's name at run
time reads it exactly as much as one that spells it out — the name not being greppable is the
reason this document exists, not a reason to drop a row.

**Scope, so this ticket does not become the whole estate.** In scope: the rows that name a job
under `projects/` or a dbt model. Out of scope: the rows that point at a dashboard file, an
export path, a config file or a contract list. Those belong to the BI tool, to growth's alerting
and to privacy, and each has its own ticket open. Leave every one of them exactly as it stands —
do not remove one, reword one or add one.

Correct both documents.

- `docs/lineage.md`: take out the rows that are not true and put in the readers that are
  missing. Every row you add names a file that exists in the tree, and says how the read is
  expressed.
- `docs/report-registry.md`: §REG-2 is a claim about the `Reads` column. A consumer whose
  publisher reads one of the three marts has to say so.

Nothing under `projects/`, `dbt/`, `config/`, `contracts/`, `scripts/`, `plugins/`, `ops/` or
`tools/` moves. This is a documents ticket, and the code is the record we are checking against.

Write the audit up in `RESPONSE.md` at the root of the working tree. A table, one row per
lineage row you worked:

| Mart | Reader | Verdict | Evidence |

`Verdict` is `keep`, `remove` or `add`. `Evidence` is a file and a line number, so the next
person can check the work without doing it again. Under the table, do the same for the registry:
which rows you changed, and what you added to each.

Keep it short. It is a record, not an essay.
