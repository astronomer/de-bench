# Legacy Pentaho nightly failing since 7 April 2026

Written 2026-06-15 by K. Duffy, supply-chain. Raised as SC-1477.

## What happened

`sc_pdi_nightly_load` has failed every night since 2026-04-07 and had not been
looked at until the quarter-end pack showed the old supply-chain reports flat.
The last night that loaded is 2026-04-06. Thirteen legacy jobs are affected —
twelve every night, plus `nightly_store_targets`, which runs weekly — and
`ops.load_control` carries a `failed` row for each of them for every night
since.

Nothing in `raw` is fed by these jobs, so no mart moved and no daily number is
wrong. What is missing is the old-schema supply-chain reporting for the whole
window.

## The cause

The job hands its load context down a chain of steps, and one link stopped
carrying a value.

| Step | What it does | What it did after 6 April |
|---|---|---|
| `set_constants` | reads the site settings and exports `STAGING_SCHEMA` and `WATERMARK_COLUMN` | exported no watermark column |
| `resolve_load_control` | reads the night's control row and exports the load context | resolved a context with an empty `LOAD_FROM_COLUMN` |
| `load_*` | filter on `${LOAD_FROM_COLUMN} > TIMESTAMP '${LOAD_FROM}'` | filter on nothing, and exit non-zero |

`legacy/pdi/trn/set_constants.ktr` does not hold the watermark column itself. It
takes it from `${COPPERLINE_WATERMARK_COL}`, a site setting in
`kettle.properties` on the PDI box, so that the same job file can be pointed at
a copy of the warehouse — `legacy/pdi/README.txt` says as much. That setting
stopped reaching the job on 2026-04-06.

The 6th still loaded because `resolve_load_control` falls back a night:
`coalesce(p.watermark_column, '${WATERMARK_COLUMN}')`, where `p` is the previous
night's control row. On the 6th the previous row was the 5th, which carries a
watermark column, so the loads ran. The row the 6th wrote carries none, because
the same step stamps the row with what it was given. On the 7th the fallback
found a null and an empty variable behind it, and there was nothing left to cut
from.

So the last row in `ops.load_control` that carries a `watermark_column` is
2026-04-05, the last night with `load_status = 'complete'` is 2026-04-06, and
the first `failed` night is 2026-04-07. The one-day gap between the two is the
whole tell.

## The value

`watermark_column` is `loaded_at` on every control row from the start of the
range to 2026-04-05 — 14,727 rows, one value, no exceptions. That is the mark
the load steps have always cut from, and it is what the context needs.

## What we ruled out

- **The Airflow side.** Nothing under `projects/supply/` changed between
  January and April. `set_constants` and `resolve_load_control` exit zero on
  every failed night; the red starts at `check_control_context` and at the
  first load step, which is downstream of the seam.
- **The three retries on the SSH step.** They are there because the old box
  drops a session about once a week. A dropped session is transient and this is
  not: all four attempts fail the same way, on every job, on every night since
  April. The retries neither caused this nor hid it.
- **The January change in `ops.load_control`.** The table carries twenty-two
  jobs a night up to 2026-01-04 and thirteen from 2026-01-05. That is wave 1 of
  the orchestrator migration, which moved nine jobs to Airflow on the cutover —
  `docs/runbooks/orchestrator-migration.md` names all nine. Their history lives
  in this deployment from that date. It is a migration, not a failure, and it is
  three months before anything went wrong.

## The fix

The PDI box is frozen until wave 2, so the setting now comes from here.
`sc_pdi_nightly_load` passes `COPPERLINE_WATERMARK_COL=loaded_at` to
`set_constants` as a parameter, the way `legacy/autosys/copperline.jil` passes
`JOB_NAME` and `BUSINESS_DATE` to `kitchen.sh`. Nothing under `legacy/` was
edited and the runner still defaults nothing.

## The repair

- Re-run the loads for every night from 2026-04-07 to 2026-06-14, oldest first.
  Each night cuts from the mark the night before it reached, so the order is not
  optional.
- Close the bracket for each repaired night with `sc_pdi_history_complete`. It
  is a DAG of its own for exactly this: the bracket belongs to the job, and a
  night repaired outside the schedule still has to be marked complete.
  `check_loads_finished` refuses a night whose loads did not finish, so the
  bracket cannot close over a repair that only half worked.

## Follow-ups

- Nothing watches `ops.load_control`. Ten weeks of `failed` rows sat there and
  the first person to notice was reading a quarter-end pack. Raised, not built.
- The other site settings still come from the box. When wave 2 is funded they
  come here too, or they break the same way.
