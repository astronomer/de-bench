# POS-402 — the silent-store escalation has never named a store

`store_pos_late_catchup` runs at 04:00 and its last step, `escalate_silent_stores`,
is the one that tells store systems which stores have stopped sending.
It has never told them anything. `ops.pos_escalations` is not in the warehouse at
all, the step goes red, and the alert for it was muted long before I got here.

Store systems have spent a fortnight asking how long one of their tills has been
off the air, so I pasted `projects/commerce/sql/pos_silent_stores.sql` into a
prompt to see what the step would have said. For last night it names all 268
stores. For every other night I tried it names all 268 too, less whichever
markets were shut that day. Nobody would have read a list like that even if the
step had finished.

Get it running, and make what it writes mean what POS-1 means. We will be asking
it about nights in the past as well as tonight's, because "how long" is the
question store systems keep asking.

The rules:

- One row per escalated store per run date in `ops.pos_escalations`, carrying the
  last business date one of that store's batches arrived for. Running a date a
  second time replaces it rather than doubling it.
- `raw.pos_batch_manifest.status` is the record of what happened to a batch. The
  words it uses are the words the landed table holds — read them off it. Every
  statement in `projects/commerce/sql/` that writes that column or reads it has
  to use those same words, and none of them may add another. The till
  reconciliation and the store teams read that column too.
- Escalate a store when the last business date it delivered a batch for, on or
  before the run's date, is further back than `LOOKBACK_DAYS`.
- A store that has never delivered a batch is not escalated. It has not opened
  yet; the store list carries a site from the day the lease is signed.
- A run for a date in the past answers for that date. What has arrived since is
  not what that night knew.
- Keep POS-1's calendar check. A store whose market was not expecting a feed that
  night sent nothing because it was shut.
- Escalate on what the feed shows, not on what `raw.stores` says about the store.
  Whether a site is shut, remodelling or gone for good is what store systems come
  back and tell us, and that is the point of sending them the list. A store that
  has stopped sending goes on the list whatever the dimension says about it.
- The manifest keeps its columns and its grain, and the catch-up keeps loading
  what it loads. `docs/runbooks/pos-ingestion.md` describes both.

— J. Mwangi, commerce
