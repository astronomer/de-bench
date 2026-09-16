"""CUS-318: what the wait renders into, after the fix.

Every question here is asked of the OPERATOR the blueprint factory builds, not
of the YAML text. That is deliberate. `include/lib/blueprint/render.py` ignores
a key it does not know — its own docstring says so — so `deferrable: true`,
`execution_timeout: 3600` or a misspelled `timeouts:` render a step that does
exactly what it did before. Reading the rendered task is the only way to tell a
fix from a file that looks like one.

Nothing here holds an authored table name, an authored date or an authored
number of seconds:

* which table the wait may name is read from the file's own steps, and whether
  that table can answer is read from the warehouse the scorer rebuilt from the
  image, against the world's clock (`WORLD_TODAY`);
* the seven-day leak is read from Airflow's own configuration, which is where
  the default comes from;
* the ceiling on the wait is the DAG's own schedule, computed from the cron the
  file carries.
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

import duckdb
import pytest
import yaml

# The scorer lays the tree at /work and runs pytest there. The override exists
# so this file can be exercised against a tree on a laptop; nothing in a trial
# or in scoring sets it.
WORKDIR = Path(os.environ.get("DE_BENCH_WORKDIR") or "/work")
DB = WORKDIR / os.environ.get("DUCKDB_PATH", "include/data/copperline.duckdb")
BLUEPRINT = WORKDIR / "projects" / "customer" / "dags" / "cus_support_sla_daily.dag.yaml"
DAG_ID = "cus_support_sla_daily"
STEP = "wait_for_tickets"

#: The world's clock, the same value every DAG in this tree reads.
TODAY = dt.date.fromisoformat(os.environ.get("WORLD_TODAY", "2026-06-15"))

#: How far behind the world's clock a daily feed may be and still count as
#: arriving. The service desk lands tickets hourly, so the feed is a day behind
#: at worst; a week is nowhere near that edge and nowhere near a table that
#: holds nothing at all.
ARRIVING_WITHIN_DAYS = 7

#: The blueprint's own line on mode: "reschedule by default, so a long wait
#: releases its worker slot. poke for a wait under five minutes."
POKE_IS_FOR_WAITS_UNDER = 300


@pytest.fixture(scope="module")
def wait():
    """The rendered wait task, from a DagBag over the tree under test."""
    from airflow.models.dagbag import DagBag

    root = str(WORKDIR / "projects")
    try:
        bag = DagBag(root, include_examples=False)
    except TypeError:  # Airflow 3.3 dropped the kwarg
        bag = DagBag(root)
    dag = bag.dags.get(DAG_ID)
    assert dag is not None, (
        f"{DAG_ID} is not in the DagBag. Import errors: {bag.import_errors}"
    )
    assert STEP in dag.task_dict, (
        f"{DAG_ID} has no {STEP!r} step; its tasks are {sorted(dag.task_dict)}"
    )
    return dag, dag.get_task(STEP)


def is_sensor(task) -> bool:
    """Whether the rendered task is a sensor, whichever path the pin exposes
    `BaseSensorOperator` under."""
    for module, name in (
        ("airflow.sdk.bases.sensor", "BaseSensorOperator"),
        ("airflow.sensors.base", "BaseSensorOperator"),
    ):
        try:
            base = getattr(__import__(module, fromlist=[name]), name)
        except Exception:  # noqa: BLE001 - a pin that moved the class
            continue
        return isinstance(task, base)
    # No base class to test against: fall back to the two attributes every
    # sensor carries and no other operator does.
    return hasattr(task, "poke_interval") and hasattr(task, "mode")


def steps() -> dict:
    """The file's steps, as it spells them."""
    document = yaml.safe_load(BLUEPRINT.read_text(encoding="utf-8")) or {}
    return document.get("steps") or {}


def published_tables() -> set:
    """What this DAG writes: the marts its dbt selections build, and whatever
    its export step publishes. Read out of the file so the set moves with it."""
    published = set()
    for name, step in steps().items():
        if name == STEP:
            continue
        if step.get("blueprint") == "dbt_select" and step.get("select"):
            published.add(f"marts.{step['select']}")
        if step.get("blueprint") == "partition_export" and step.get("source"):
            published.add(str(step["source"]))
        if step.get("target"):
            published.add(str(step["target"]))
    return published


def day_named(template, run_day: dt.date):
    """The day a `partition_value` names for a run on `run_day`, or None when
    the template is one this file does not read.

    Only the two forms the tree itself uses are read — `{{ ds }}` and
    `{{ macros.ds_add(ds, -n) }}`. Anything else falls back to the weaker
    question in the caller rather than failing the fix for its spelling.
    """
    import re

    text = str(template or "").strip()
    if not text:
        return None
    if re.fullmatch(r"\{\{\s*ds\s*\}\}", text):
        return run_day
    shift = re.fullmatch(r"\{\{\s*macros\.ds_add\(\s*ds\s*,\s*(-?\d+)\s*\)\s*\}\}", text)
    if shift:
        return run_day + dt.timedelta(days=int(shift.group(1)))
    try:
        return dt.date.fromisoformat(text)
    except ValueError:
        return None


def holds(table: str, column: str, day: dt.date) -> bool:
    """The question the sensor asks the warehouse, asked the same way."""
    with duckdb.connect(str(DB), read_only=True) as con:
        found = con.execute(
            f"SELECT 1 FROM {table} WHERE {column} = ? LIMIT 1", [str(day)]
        ).fetchone()
    return bool(found)


def newest_partition(table: str, column: str):
    """The newest day the table the wait names holds, or None when it has none."""
    schema, _, bare = table.rpartition(".")
    with duckdb.connect(str(DB), read_only=True) as con:
        exists = con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, bare],
        ).fetchone()
        if not exists:
            return None
        newest = con.execute(f"SELECT max({column}) FROM {table}").fetchone()[0]
    if newest is None:
        return None
    return newest.date() if isinstance(newest, dt.datetime) else newest


def cron_interval_seconds(cron: str) -> int:
    """Seconds between two fires of a five-field cron, by scanning it.

    Written out rather than imported so the ceiling on the wait comes from the
    file's own schedule and not from whatever scheduling library happens to be
    installed. Anything this cannot read falls back to a day, which is the
    cadence every rendered DAG in this project runs at.
    """
    fields = cron.split()
    if len(fields) != 5:
        return 86400

    def matches(spec: str, value: int, low: int, high: int) -> bool:
        for part in spec.split(","):
            step = 1
            if "/" in part:
                part, _, raw = part.partition("/")
                if not raw.isdigit():
                    return False
                step = int(raw)
            if part in ("*", "?"):
                start, end = low, high
            elif "-" in part:
                a, _, b = part.partition("-")
                if not (a.isdigit() and b.isdigit()):
                    return False
                start, end = int(a), int(b)
            elif part.isdigit():
                start = end = int(part)
            else:
                return False
            if start <= value <= end and (value - start) % step == 0:
                return True
        return False

    def fires(when: dt.datetime) -> bool:
        dom_star = fields[2] in ("*", "?")
        dow_star = fields[4] in ("*", "?")
        dom = matches(fields[2], when.day, 1, 31)
        dow = matches(fields[4], (when.weekday() + 1) % 7, 0, 6)
        day = (dom and dow) if (dom_star or dow_star) else (dom or dow)
        return (
            matches(fields[0], when.minute, 0, 59)
            and matches(fields[1], when.hour, 0, 23)
            and matches(fields[3], when.month, 1, 12)
            and day
        )

    at = dt.datetime(TODAY.year, TODAY.month, TODAY.day)
    hits = []
    for minute in range(0, 60 * 24 * 40):
        moment = at + dt.timedelta(minutes=minute)
        if fires(moment):
            hits.append(moment)
            if len(hits) == 2:
                return int((hits[1] - hits[0]).total_seconds())
    return 86400


def airflow_default_timeout() -> float:
    """Airflow's own sensor default — the seven days that is not a timeout."""
    try:
        from airflow.configuration import conf

        return float(conf.getfloat("sensors", "default_timeout"))
    except Exception:  # noqa: BLE001 - a pin that moved the option
        return 7 * 24 * 60 * 60.0


def test_the_step_is_still_a_wait(wait):
    """Deleting the wait, or swapping it for something that always succeeds, is
    the cheapest way to make a stuck DAG finish, and it is the one thing the
    ticket cannot mean: the job reads a feed that lands through the day."""
    _, task = wait
    assert is_sensor(task), (
        f"{STEP} renders as {type(task).__name__}, which is not a sensor"
    )


def test_the_wait_is_not_on_what_this_dag_writes(wait):
    """The find. The step waits on the mart this DAG's own selection builds and
    its own export publishes, so the run waits for itself: the table appears
    only if the run finishes, and the run cannot finish until the table
    appears. `table_has_partition` answers `no` to a table that is not there
    rather than raising, which is why nothing ever failed.

    The set is read out of the file's other steps, so it follows the file.
    """
    _, task = wait
    table = str((dict(getattr(task, "op_kwargs", None) or {})).get("table") or "")
    if not table:
        return  # a file wait; the next test covers what it waits for
    assert table not in published_tables(), (
        f"{STEP} waits for {table}, which this DAG builds and publishes. The "
        "run cannot finish until the table is there and the table is not there "
        "until the run finishes"
    )


def test_the_wait_is_on_something_the_warehouse_can_answer(wait):
    """A wait is only a wait if the thing it asks about turns up. This asks the
    warehouse the question the sensor asks it, at the world's own clock."""
    _, task = wait
    kwargs = dict(getattr(task, "op_kwargs", None) or {})
    table, column = kwargs.get("table"), kwargs.get("partition_col")

    if not table:
        # A wait rewritten as a file drop. Graded leniently: the shipped step
        # waits on a warehouse asset and the fix is which table, not which kind
        # of wait, so this branch only asks that the drop it names exists.
        path = str(getattr(task, "filepath", "") or getattr(task, "path", ""))
        assert path, f"{STEP} waits on neither an asset nor a file"
        root = Path(path.split("{{")[0]).parent
        assert root.exists(), f"{STEP} waits for a file under {root}, which is not there"
        return

    assert column, (
        f"{STEP} waits for {table} to exist at all. The job owns one day per "
        "run, so the wait is for that day's rows"
    )
    newest = newest_partition(table, column)
    assert newest is not None, (
        f"{STEP} waits on {table}.{column}, which the warehouse cannot answer: "
        "the table is missing or holds nothing. A sensor reads that as `not "
        "yet`, not as an error"
    )
    behind = (TODAY - newest).days
    assert behind <= ARRIVING_WITHIN_DAYS, (
        f"{STEP} waits on {table}, whose newest {column} is {newest} — "
        f"{behind} days behind {TODAY}. Nothing is landing there"
    )

    # And the whole predicate, for the last three mornings this DAG would have
    # run. A column that holds a timestamp where the day is compared, or a day
    # the feed never carries, clears the test above and still never clears the
    # wait. One quiet day is allowed; three is not a feed.
    days = [day_named(kwargs.get("partition_value"), TODAY - dt.timedelta(days=n))
            for n in range(3)]
    asked = [day for day in days if day is not None]
    if asked:
        assert any(holds(table, column, day) for day in asked), (
            f"{STEP} asks {table} for {column} = "
            f"{', '.join(str(day) for day in asked)} and gets nothing back on "
            "any of them, so the wait never clears"
        )


def test_the_wait_ends_by_itself(wait):
    """CONVENTIONS.md §9: every sensor sets a timeout, and Airflow's own default
    of seven days is not one. The band is wide on purpose — no document in this
    tree fixes a house default and `docs/blueprints.md` says as much — but it
    has two ends. A wait may not outlive the run's own day, because a rendered
    DAG is `max_active_runs=1` and the next day's run waits behind this one,
    and it may not be shorter than a single poke, because a wait that cannot
    poke twice is not a wait.
    """
    dag, task = wait
    timeout = getattr(task, "timeout", None)
    leak = airflow_default_timeout()
    assert timeout is not None, f"{STEP} sets no timeout"
    assert float(timeout) < leak, (
        f"{STEP} still carries Airflow's own default of {leak:.0f}s. That is "
        "the leak CONVENTIONS.md §9 names, not a timeout"
    )
    assert float(timeout) > 0, f"{STEP} has a timeout of {timeout}, which never waits"
    interval = cron_interval_seconds(str(getattr(dag.timetable, "summary", "") or "0 7 * * *"))
    assert float(timeout) <= interval, (
        f"{STEP} waits up to {timeout:.0f}s on a DAG that runs every "
        f"{interval}s, so a stuck run is still holding the slot when the next "
        "one is due"
    )
    poke = float(getattr(task, "poke_interval", 0) or 0)
    assert float(timeout) >= poke, (
        f"{STEP} gives up after {timeout:.0f}s while poking every {poke:.0f}s"
    )


def test_the_mode_matches_the_length_of_the_wait(wait):
    """`sensor_wait`'s own docstring: reschedule by default so a long wait
    releases its worker slot, poke for a wait under five minutes. The house
    library ships `reschedule` as the default, so this fails only for a fix
    that chose the slot-holding mode for a wait measured in hours.
    """
    _, task = wait
    timeout = float(getattr(task, "timeout", 0) or 0)
    mode = getattr(task, "mode", None)
    if timeout > POKE_IS_FOR_WAITS_UNDER:
        assert mode == "reschedule", (
            f"{STEP} waits up to {timeout:.0f}s in {mode!r} mode, which holds a "
            "worker slot for the whole wait"
        )


def test_the_dag_still_pages(wait):
    """The other half of the ticket. A rendered DAG pages the team that owns its
    directory because `render_file` builds the callback; the file does not say
    so. A fix that renders with `notify: false`, or takes the notifier off,
    leaves the failure unheard, which is the state being complained about.
    """
    dag, _ = wait
    assert getattr(dag, "on_failure_callback", None), (
        f"{DAG_ID} renders with no failure callback, so a failure reaches nobody"
    )


def test_the_wait_is_configured_in_the_dag_file():
    """The ticket says the fix goes in the DAG's own file. The team loader is
    two lines that render every customer blueprint, and a timeout injected
    there would land on DAGs this ticket never looked at."""
    loader = WORKDIR / "projects" / "customer" / "dags" / "blueprints.py"
    text = loader.read_text(encoding="utf-8") if loader.exists() else ""
    assert "timeout" not in text, (
        "the customer blueprint loader now sets a timeout; that changes every "
        "rendered customer DAG, not this one"
    )
