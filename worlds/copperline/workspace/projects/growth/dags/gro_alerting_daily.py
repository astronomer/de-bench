"""Freshness and volume alerting over the feeds and the marts.

Consumer C-10, bound by `contracts/alerting.md`. It pages growth's on-call
and it is the one consumer in the estate whose read set is a list of strings:
every table it watches is a name in `config/alerts.yml`, with the check, the
threshold and the owning team beside it.

**The config is the definition.** §AL-3. Nothing is defined in this file — no
table, no threshold, no owner. A threshold changed here rather than in the
config would be invisible to everybody who reads the config to find out what
is watched, which is everybody.

**A quiet day is not an anomaly.** §AL-2. `ops/calendar/quiet-days.yml` holds
the windows Copperline agreed in advance: planned closures, planned
migrations, anything where low or absent volume is the expected outcome. A
subject inside one of those windows is measured and recorded and does not
page.

It runs at 11:00 and has never claimed to be a real-time path.

Owned by growth.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.lib.notify import notify
from include.lib.pipeline import lake_task
from projects.growth.lib import alerts

#: The checks that fail high. `row_count` fails low; everything else fails
#: when the measured number is above the threshold.
FAIL_HIGH = ("freshness", "null_rate")

DEFAULT_ARGS = {
    "owner": "growth",
    "retries": 1,
    "on_failure_callback": notify("growth", "the alerting run itself failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects", "retry": {"attempts": 1}}


@dag(
    dag_id="gro_alerting_daily",
    schedule="0 11 * * *",
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["growth", "alerting", "ops"],
    doc_md=__doc__,
)
def gro_alerting_daily():

    @task
    def watched() -> list[dict]:
        """Every subject in `config/alerts.yml`, in file order.

        Mapped over, so it is a plain task and its list is an ordinary XCom.
        """
        return alerts.subjects()

    @lake_task
    def unowned(subjects: list[dict]) -> list[dict]:
        """Subjects with no owning team, or an owner that is not one.

        §AL-1: a table with no owner is not watched, because an alert nobody
        owns wakes the wrong person and gets muted. They are reported rather
        than paged on, and fixing one is a change to the config.
        """
        return alerts.unowned(subjects)

    @lake_task(map_index_template="{{ task.op_kwargs['subject']['table'] }}")
    def evaluate(subject: dict, day: str) -> dict:
        """Measure one subject and say whether it breached.

        The quiet-day check comes before the comparison, not after it: a
        subject inside an agreed window is measured, recorded and left alone.
        """
        quiet = alerts.is_quiet(day)
        measured = alerts.measure(subject, day)
        threshold = float(subject["threshold"])
        breached = (measured > threshold if subject["check"] in FAIL_HIGH
                    else measured < threshold)
        return {"table": subject["table"], "check": subject["check"],
                "owner": subject.get("owner"), "measured": measured,
                "threshold": threshold, "quiet_day": quiet,
                "breached": bool(breached and not quiet)}

    @lake_task
    def record(results: list[dict], day: str) -> int:
        """Write every evaluation to `ops.alert_history`, breach or not.

        The runs that did not fire are the record that says whether a
        threshold is doing anything, which is the open item
        `contracts/alerting.md` names.
        """
        return alerts.record(results, day)

    @task
    def page(results: list[dict]) -> list[str]:
        """Page the owning team for each breach, one alert per subject.

        The pager is the house notifier, so an alert from here lands the same
        way a DAG failure does and leaves the same record.
        """
        fired = [result for result in results
                 if result["breached"] and not alerts.unowned([result])]
        for result in fired:
            notify(result["owner"],
                   f"{result['table']}: {result['check']} is "
                   f"{result['measured']:g} against {result['threshold']:g}",
                   runbook="ops/runbooks/alerting.md")({"table": result["table"]})
        return [result["table"] for result in fired]

    subjects = watched()
    results = evaluate.partial(day="{{ ds }}").expand(subject=subjects)
    unowned(subjects) >> results
    results >> record(results, day="{{ ds }}") >> page(results)


gro_alerting_daily()
