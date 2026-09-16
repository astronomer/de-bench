"""The daily build of `dbt/copperline_analytics`, one task per model.

Cosmos renders the project from its committed manifest, so the graph in the
Airflow UI is the graph in dbt: a model that fails is one red square, it
retries on its own, and the models that did not depend on it carry on. That is
the whole reason this is not a `BashOperator` around `dbt build`.

It runs at 04:00 and it holds the warehouse's single writer while it does. A
`dbt run` started against the live file while this is in flight fails on the
lock; take a snapshot and work against that instead. `dbt/README.md` has the
commands.

Owned by data-platform. When this stops, every mart in the company is a day
old and the 05:45 flash publishes yesterday twice.
"""

from __future__ import annotations

import pendulum
from cosmos import (
    DbtDag,
    ExecutionConfig,
    ExecutionMode,
    LoadMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
    TestBehavior,
)

from include.lib import workspace_root
from include.lib.notify import notify

DBT_ROOT = workspace_root() / "dbt" / "copperline_analytics"

#: The committed manifest. Rendering from it means no `dbt ls` at parse, which
#: is the difference between a parse in a second and a parse in a minute. It
#: also means the graph is whatever the manifest last held: `plat_manifest_check`
#: runs at 03:45 and says so when the manifest and the models have drifted.
MANIFEST = DBT_ROOT / "manifest" / "manifest.json"

#: dbt runs from its own interpreter. The pinned dbt-core does not install on
#: the deployment's Python, so the image builds a second virtualenv for it and
#: the operators shell into that. `docs/runbooks/upgrade.md` says what moves
#: when the pin does.
DBT_EXECUTABLE = "/opt/dbt-venv/bin/dbt"

#: The day the build owns. A bare cron string is a trigger timetable, so `ds`
#: is the day the run fires and the day being built is the one before it. See
#: `CONVENTIONS.md`.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

profile_config = ProfileConfig(
    profile_name="copperline",
    target_name="prod",
    profiles_yml_filepath=DBT_ROOT / "profiles.yml",
)

plat_dbt_analytics_daily = DbtDag(
    dag_id="plat_dbt_analytics_daily",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    project_config=ProjectConfig(DBT_ROOT, manifest_path=MANIFEST),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL,
        dbt_executable_path=DBT_EXECUTABLE,
    ),
    # Rendering from the committed manifest runs no dbt at all, so the render
    # takes no executable. The executable belongs on the execution config,
    # where the tasks that do run dbt pick it up.
    #
    # The build runs models and no tests. `plat_dbt_test_nightly` at 05:30 runs
    # the selection each team tags, and running every test twice would double
    # the graph and hold the writer for the second half of it.
    render_config=RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,
        test_behavior=TestBehavior.NONE,
    ),
    operator_args={
        "vars": {"run_date": TARGET_DS},
        "install_deps": False,
        "pool": "dbt_build",
    },
    default_args={
        "owner": "platform",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=3),
        "on_failure_callback": notify(
            "platform", "the daily model build failed; marts are a day old"
        ),
    },
    tags=["platform", "dbt", "build"],
    doc_md=__doc__,
)
