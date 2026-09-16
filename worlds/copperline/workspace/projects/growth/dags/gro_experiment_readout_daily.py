"""The experiment readout: what each running test did yesterday.

Cron rather than an asset, because the readout has two upstreams on two
clocks — the session marts, which fire on an asset, and commerce's GMV, which
lands with the nightly build — and 10:00 is comfortably after both. That is
the exception `CONVENTIONS.md` allows and this docstring is the reason for
it.

**It owns yesterday.** A bare cron schedule makes `{{ ds }}` the day the run
fires, so the day being read out is `{{ macros.ds_add(ds, -1) }}`. It is
written that way in every task rather than in one place, because a readout
that quietly moved to the wrong day would look exactly like a quiet day.

**The readout is not a decision.** It publishes the arms, their exposures and
their conversion, and it does not call a winner: there is no stopping rule in
this DAG and there never has been. Whoever reads the table applies one.

Owned by growth.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.growth.lib import dbt, experiments

DEFAULT_ARGS = {
    "owner": "growth",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("growth", "experiment readout failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="gro_experiment_readout_daily",
    schedule="0 10 * * *",
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["growth", "experiments", "mart"],
    doc_md=__doc__,
)
def gro_experiment_readout_daily():

    @lake_task
    def running_experiments(day: str) -> list[dict]:
        """The experiments that were live on the day being read out.

        An experiment that ended yesterday is still read out for yesterday,
        because it was running then. Reading the day rather than the clock is
        what makes a re-run of an old day reproduce that day's readout.
        """
        return experiments.running_on(day)

    @lake_task
    def exposures(day: str) -> int:
        """Sessions per arm, from the session fact."""
        return experiments.build_exposures(day)

    @lake_task
    def outcomes(day: str) -> int:
        """Orders and net sales per arm, from the order spine."""
        return experiments.build_outcomes(day)

    readout = dbt.selection("readout", "experiment_readout",
                            variables={"run_date": "{{ macros.ds_add(ds, -1) }}"})

    @lake_task
    def arms_balanced(day: str, live: list[dict]) -> dict[str, int]:
        """Each experiment's arms are within the split it was set up with.

        A split that has drifted means the assignment is broken, and every
        number in the readout is then a comparison of two different
        populations rather than of two treatments.
        """
        skewed = experiments.skewed_arms(day, live)
        if skewed:
            raise Reject(f"{len(skewed)} arm(s) outside the agreed split",
                         rows=skewed)
        return {"experiments": len(live)}

    @task
    def publish_readout(day: str) -> str:
        """Write the readout file the experiment review reads."""
        return experiments.publish(day)

    day = "{{ macros.ds_add(ds, -1) }}"
    live = running_experiments(day)
    live >> [exposures(day), outcomes(day)] >> readout
    readout >> arms_balanced(day, live) >> publish_readout(day)


gro_experiment_readout_daily()
