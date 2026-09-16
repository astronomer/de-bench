# Runbook: the legacy orchestrator migration

Owner: supply-chain, with data platform. Review date 2026-04-20. Advisory, per `docs/change-management.md` §CM-3.

Before 2021 the reporting estate ran on Pentaho: 22 nightly jobs a contractor built, on a schedule kept in AutoSys, with an inventory spreadsheet in Tidal that was already out of date when it was made. Wave 1 of the migration moved nine of those jobs. The other thirteen still run.

## ORC-1 What moved in wave 1

Wave 1 cut over on 2026-01-05. These nine jobs moved off Pentaho and now run as Airflow DAGs:

| Legacy job | Now |
|---|---|
| `nightly_supplier_master` | `sc_ssis_supplier_master` |
| `nightly_inventory_report` | `sc_tidal_inventory_report` |
| `nightly_dc_transfer` | `sc_dc_transfer_daily` |
| `nightly_shrink_weekly` | `sc_shrink_weekly` |
| `nightly_lane_performance` | `sc_lane_performance_daily` |
| `nightly_backorder` | `sc_backorder_daily` |
| `nightly_dc_capacity` | `sc_dc_capacity_daily` |
| `nightly_scorecard` | `sc_supplier_scorecard_weekly` |
| `nightly_fill_rate` | `sc_fill_rate_daily` |

The remaining thirteen run under Pentaho, and Airflow wraps some of them: a DAG calls the runner over SSH, step by step, and waits. Waves 2 and 3 are unscheduled and have been since the cutover.

This runbook says which jobs moved and when. It does not describe how any of them work. The job definitions under `legacy/` are the only description of that, and they are complete.

## What still runs where

| Artifact | What it is |
|---|---|
| `legacy/pdi/nightly_load.kjb` | the surviving nightly job and its child transformations |
| `legacy/autosys/copperline.jil` | the run calendar for the jobs nobody converted |
| `legacy/tidal/inventory_jobs.csv` | a planning inventory, out of date when it was written |
| `include/lib/legacy_runner.py` | the stub that runs a legacy step over SSH |
| `ops.load_control` | the warehouse table the legacy control step reads |

`sc_pdi_nightly_load` runs the legacy job step by step, one Airflow task per step, so that a step can be retried on its own. `sc_pdi_history_complete` closes the history bracket the job opens.

## Before the platform deployment

Run history exists from 2026-01-05. There is no run history before that date, because the deployment the estate runs on now was created for the migration. Anything about what ran before then has to come from the legacy artifacts or from the data.

## Open items

- Waves 2 and 3 have no date and no owner. The thirteen remaining jobs are a permanent state until somebody funds the work.
- TODO: the Tidal inventory is kept because it lists jobs nothing else lists. It is also wrong about several of them.
- Nobody on the current team wrote the Pentaho jobs. The contractor left in 2021.
- The AutoSys calendar and the Airflow schedules overlap for two jobs. They have not collided yet.
