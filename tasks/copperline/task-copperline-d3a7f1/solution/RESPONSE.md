# PLAT-402: what the dbt 1.6 to 1.10 move costs

It is not one afternoon. Three things fail on the first run after the pins change, and four
more change numbers or start writing rows without failing. Monday's clean report covers none
of them.

## What stops the move outright

**`dbt/copperline_analytics/dbt_project.yml:14`** — `require-dbt-version: [">=1.6.0", "<1.7.0"]`.
dbt refuses the project before it compiles a model. One line to widen, and it is the only
finding here that announces itself.

**`dbt/copperline_analytics/models/`, 15 call sites of `dbt_utils.surrogate_key`.** The macro
does not exist at dbt-utils 1.x. Every one of them is a compile error on the first run. That
error is the anaesthetic, not the problem — see below.

**`dbt/northwave_reporting/packages.yml:2-3`** — `dbt deps` cannot resolve
`fishtown-analytics/dbt_utils`. That project is covered under its own heading.

## What changes numbers without failing

**Surrogate keys, and this is the one that matters.** `dbt_utils.surrogate_key` is renamed
`generate_surrogate_key` at 1.x, and the new macro hashes NULL as the literal string `NULL`
where 0.8.6 hashed it as the empty string. A mechanical rename compiles and changes the value
of every key it computes.

Fifteen call sites, one per file:

| Where | Files |
|---|---|
| `models/shared/staging/` | 6 |
| `models/shared/intermediate/` | 2 |
| `models/shared/dims/` | 1 |
| `models/supply/` | 3 |
| `models/commerce/` | 1 |
| `models/finance/intermediate/` | 1 |
| `models/customer/` | 1 |

Six of the seven model directories, including `dim_carrier`, `int_gl_postings_unified` and
`fct_shipping_costs`. Anything incremental that keys on one of these reshuffles: the old rows
carry the old hash, the new rows carry the new one, and the `unique_key` match stops finding
its own history. Nothing fails while that happens.

The fix is the var the project already sets —
`surrogate_key_treat_nulls_as_empty_strings: true`, `dbt_project.yml:24`. dbt-utils 1.x still
reads it and it reproduces the 0.8.6 hashing. Do not hand-roll a replacement macro and do not
turn the var off; `dbt/README.md` says so at lines 48-53 and it is right.

**No `flags:` block anywhere in `dbt_project.yml`.** Every behaviour flag that changed its
default between 1.6 and 1.10 takes the new default here on the first run, silently. Setting
the ones we care about explicitly, before the pins move, is what makes the change reviewable.

## The scheduled jobs

**`plat_dbt_source_freshness` starts writing audit rows.** At 1.10 `dbt source freshness` runs
the project's `on-run-start` and `on-run-end` hooks. It did not at 1.6. Our hooks
(`dbt_project.yml:29-33` → `macros/audit.sql`) are the run audit: they insert an `open` and a
`close` row into `ops_run.dbt_run_events`. The job is scheduled `15 * * * *`
(`projects/platform/dags/plat_dbt_source_freshness.py:44`), so that is **24 invocations a day**
appearing in `models/shared/ops/ops_audit_log.sql` as runs of the warehouse.

They are not runs of the warehouse. `ops_audit_log` is incremental and its header says it must
never be full-refreshed, so once the rows are in there is no clean way to take them out. Every
count of builds, and anything watching for an open row with no close, reads 24 extra runs a day
that built nothing.

**`plat_dbt_analytics_daily` renders from a stale artifact.** Cosmos does not run dbt to work
out the graph; it reads the committed manifest at
`dbt/copperline_analytics/manifest/manifest.json`
(`projects/platform/dags/plat_dbt_analytics_daily.py:40` and `:65`). That file is stamped
`https://schemas.getdbt.com/dbt/manifest/v10.json`, written by dbt 1.6.14. The new core writes
a different schema version. The manifest has to be regenerated as part of the change and
`plat_manifest_check` at 03:45 is what tells us it was not.

**Everything shells `/opt/dbt-venv/bin/dbt`.** dbt is not in the Airflow environment and cannot
be (`dbt/README.md:21-34`). The pins live in that virtualenv, so the upgrade is a rebuild of
the image, not a `pip install` on the deployment.

**`plat_dbt_test_nightly`.** Twelve tests fail on this tree today. After the move we lose the
ability to say whether a thirteenth is the upgrade or the backlog, so record the failing set
before the pins change.

## The other dbt project

`dbt/northwave_reporting/` is **41 models** — 20 under `models/legacy/`, 11 marts, 10 staging —
against the `northwave` profile and the frozen file. It is not part of the analytics graph, so
nothing about it shows up in a `copperline_analytics` build.

It is already broken and has been since before this ticket:

- `packages.yml` asks for `fishtown-analytics/dbt_utils` at **0.6.4**. That namespace was
  renamed to `dbt-labs` and 0.6.4 predates dbt 1.0 entirely. `dbt deps` in this project cannot
  succeed today, at 1.6, never mind at 1.10.
- `dbt_project.yml` uses `source-paths`, `data-paths` and `clean-targets: dbt_modules` — the
  key names from before the dbt 1.0 rename to `model-paths`, `seed-paths` and `dbt_packages`.
- It carries **no `require-dbt-version`**. The analytics project stops the new core at the
  door; this one lets it in. The project that would break worst is the one with no guard.
- `models/legacy/` is configured `enabled: true`, so nothing here is switched off.

Recommendation: this project does not move with the pins. Establish what still calls it, from
`ops/lineage.json` and the scheduler, before spending anything on it. `README.md` in that
directory says the person who knew left in November.

## The pre-flight

`tools/check_upgrade.py` walks the files under `dags/` for **imports** and compares them
against a hardcoded list of symbols removed at the pinned versions. `docs/runbooks/upgrade.md`
lines 22-24 already state the limits, in the runbook's own words: it does not run anything,
does not render a DAG, **it does not read the dbt project** and does not look at configuration.

So `no blocking issues found` means one thing — no DAG file imports a symbol on the list. Every
finding above is outside that sentence:

| Finding | Where it lives | In the pre-flight's scope? |
|---|---|---|
| `require-dbt-version` | `dbt/` | no |
| `surrogate_key` | a macro call in a model | no |
| freshness runs the hooks | a changed default | no |
| the committed manifest | a JSON artifact | no |
| `northwave_reporting` | a second project | no |

Two more things about the report. Line 43 of the same runbook records that the symbol list is
hardcoded and that nobody updates it during an upgrade, so it knows about the pins it was
written for and not about 1.10. And the script it names is not in this repo — the weekly
`plat_upgrade_check` DAG points at `tools/check_upgrade.py` and there is no `tools/` directory.
Whatever produced Monday's line, it was not code we can read.

The clean report is not evidence. Take it out of the change ticket.

## Order

`docs/runbooks/upgrade.md` UPG-1: providers, then the orchestrator core, then dbt-core and
dbt-duckdb **together**, then the rendering layer. dbt-utils moves with dbt-core. Set the var
and widen `require-dbt-version` in the same change as the core, regenerate the manifest, and
keep the freshness job paused until the hook behaviour is decided.

Nothing in the pipelines was changed for this.
