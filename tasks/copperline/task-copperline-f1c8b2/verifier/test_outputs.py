"""PS-274: what the Kestrel wait does, after the fix.

Every question about the wait is asked of the OPERATOR the tree builds and then
of the wait's own answer — the rendered task is poked, twice, against a
warehouse this file stands up. Reading the file text cannot tell a wait that
clears from a wait that looks like one, and the shipped fault is exactly that
shape: a `FileSensor` on a name pattern nothing writes returns `False` for ever
and reads, in the file, like an ordinary wait.

Nothing here holds an authored date, table or number of seconds:

* the week under test is computed from the world's clock (`WORLD_TODAY`) and
  the DAG's own Friday cadence — the last Friday the DAG fired, and the logical
  date of the run that fired then;
* the rows the mart is stood up with are aggregated out of `raw.*` in this
  session, so no figure is typed here;
* the ceiling on the wait is the DAG's own schedule interval;
* the PS-1 column list is read from the module the DAG builds its tasks from,
  not restated.

WHY THE MART IS STOOD UP IN A SCRATCH WAREHOUSE
-----------------------------------------------
The shipped warehouse holds `raw` and `ops` and nothing above them, so
`marts.sell_through_daily` is not there on any tree, fixed or not. A wait that
asks the warehouse for it therefore answers "not yet" on both arms, and the
question this task grades — does the wait clear when the week IS published —
cannot be asked without publishing one. So the week is built here, into a
warehouse of this file's own, and `DUCKDB_PATH` is pointed at it for the poke:
the house library reads that variable on every open, so a wait written the
house way follows it. A wait that opens the warehouse by a hard-coded path does
not, and `CONVENTIONS.md` puts that outside `include/lib/` in as many words.
"""

from __future__ import annotations

import copy
import datetime as dt
import os
import re
from pathlib import Path

import duckdb
import pytest

# The scorer lays the tree at /work and runs pytest there. The override exists
# so this file can be exercised against a tree on a laptop; nothing in a trial
# or in scoring sets it.
WORKDIR = Path(os.environ.get("DE_BENCH_WORKDIR") or "/work")
LIVE_DB = WORKDIR / os.environ.get("DUCKDB_PATH", "include/data/copperline.duckdb")

DAG_ID = "sc_partner_share_kestrel"
STEP = "wait_for_sell_through"
MART = "marts.sell_through_daily"

#: The world's clock, the same value every DAG in this tree reads.
TODAY = dt.date.fromisoformat(os.environ.get("WORLD_TODAY", "2026-06-15"))

#: The documents that carry how this consumer is coupled, and the claim they
#: stop being allowed to make once it moves. `docs/report-registry.md` REG-2
#: and `docs/lineage.md` LIN-1 are the rules; both files say they are written
#: together.
REGISTRY = WORKDIR / "docs" / "report-registry.md"
LINEAGE = WORKDIR / "docs" / "lineage.md"
PATTERN_CLAIM = re.compile(r"pattern|filesensor|sell_through_\*", re.IGNORECASE)


def friday_on_or_before(day: dt.date) -> dt.date:
    """The most recent Friday at or before `day`. Friday is weekday 4."""
    return day - dt.timedelta(days=(day.weekday() - 4) % 7)


#: The Friday the DAG last fired, and the logical date of the run that fired
#: then — a weekly cron's run owns the interval that opened a week earlier.
LAST_FIRED = friday_on_or_before(TODAY)
DELIVERED_DS = LAST_FIRED - dt.timedelta(days=7)

#: The fiscal week that run delivers: the Sunday five days before its own date,
#: through the Saturday after it. `week_start` in the DAG does the same sum.
WEEK_START = DELIVERED_DS - dt.timedelta(days=5)
WEEK_DAYS = [WEEK_START + dt.timedelta(days=n) for n in range(7)]

#: A run whose week the mart holds nothing for — the next Friday's, one week on.
UNPUBLISHED_DS = LAST_FIRED


# --------------------------------------------------------------------------
# rendering: the two template forms this tree writes, without Airflow's
# rendering machinery, so the test does not move when that machinery does.
# --------------------------------------------------------------------------

class Macros:
    """The `macros` a template in this tree reaches for."""

    date = dt.date
    datetime = dt.datetime
    timedelta = dt.timedelta

    @staticmethod
    def ds_add(ds: str, days: int) -> str:
        return (dt.date.fromisoformat(str(ds)[:10]) + dt.timedelta(days=int(days))).isoformat()

    @staticmethod
    def ds_format(ds: str, input_format: str, output_format: str) -> str:
        return dt.datetime.strptime(str(ds), input_format).strftime(output_format)


def render_context(ds: dt.date) -> dict:
    """A run's context, as much of it as a wait ever reads."""
    start = dt.datetime(ds.year, ds.month, ds.day, 9, 0)
    return {
        "ds": ds.isoformat(),
        "ds_nodash": ds.strftime("%Y%m%d"),
        "macros": Macros,
        "params": {},
        "logical_date": start,
        "data_interval_start": start,
        "data_interval_end": start + dt.timedelta(days=7),
        "run_id": f"scheduled__{ds.isoformat()}T09:00:00+00:00",
        "try_number": 1,
    }


def render_value(value, context: dict):
    """One template field, rendered. Jinja is used directly rather than through
    the DAG's environment so that a wait written any way at all renders the
    same, and a field that is not a template is returned as it is."""
    import jinja2

    if isinstance(value, str):
        if "{{" not in value and "{%" not in value:
            return value
        return jinja2.Environment(undefined=jinja2.Undefined).from_string(value).render(**context)
    if isinstance(value, dict):
        return {k: render_value(v, context) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        rendered = [render_value(v, context) for v in value]
        return type(value)(rendered) if isinstance(value, tuple) else rendered
    return value


def poked(task, ds: dt.date):
    """Ask the wait, for a run on `ds`, and give back what it answered.

    The task's template fields are saved and put back afterwards, because
    rendering replaces them and `PythonSensor.poke` replaces `op_kwargs` with
    the arguments it resolved. `FileSensor` caches the path it built from
    `filepath`, so that cache goes too. The answer comes back as the sensor
    gave it — a bool, a `PokeReturnValue`, or the exception it raised; a wait
    that raises has not cleared either, and `cleared` below flattens the three.
    """
    fields = tuple(getattr(task, "template_fields", ()) or ())
    saved = {name: copy.deepcopy(getattr(task, name)) for name in fields if hasattr(task, name)}
    context = render_context(ds)
    try:
        for name, value in saved.items():
            setattr(task, name, render_value(copy.deepcopy(value), context))
        task.__dict__.pop("path", None)
        try:
            return task.poke(dict(context))
        except Exception as exc:  # noqa: BLE001 - a raise is an answer here
            return exc
    finally:
        for name, value in saved.items():
            setattr(task, name, value)
        task.__dict__.pop("path", None)


def cleared(answer) -> bool:
    """Whether that answer means the wait is over.

    `FileSensor.poke` gives a bool and `PythonSensor.poke` wraps its callable's
    answer in a `PokeReturnValue`, whose `is_done` is the same fact under
    another name. A raise is not a clear.
    """
    if isinstance(answer, BaseException):
        return False
    done = getattr(answer, "is_done", None)
    return bool(answer) if done is None else bool(done)


# --------------------------------------------------------------------------
# the warehouse the wait is asked against
# --------------------------------------------------------------------------

def build_week(scratch: Path) -> int:
    """Publish one fiscal week of `marts.sell_through_daily`, and only that week.

    The rows are the week's store sales at the mart's own grain, aggregated out
    of `raw.orders` and `raw.order_lines` in this session, with the region taken
    from the current version of the store. The columns are the six
    `contracts/sell_through_daily.yml` names. Nothing about the figures is
    graded; what is graded is that the week is there, and that no other week is.
    """
    scratch.parent.mkdir(parents=True, exist_ok=True)
    if scratch.exists():
        scratch.unlink()
    con = duckdb.connect(str(scratch))
    try:
        con.execute(f"ATTACH '{LIVE_DB}' AS live (READ_ONLY)")
        con.execute("CREATE SCHEMA IF NOT EXISTS marts")
        con.execute(
            """
            CREATE TABLE marts.sell_through_daily AS
            SELECT o.local_order_date                       AS ds,
                   l.sku                                    AS sku,
                   o.store_id                               AS store_id,
                   s.region_code                            AS store_region,
                   cast(sum(l.qty) AS INTEGER)              AS demand_units,
                   cast(sum(l.line_total_cents) AS BIGINT)  AS net_sales_cents
            FROM live.raw.orders o
            JOIN live.raw.order_lines l ON l.order_id = o.order_id
            JOIN live.raw.stores s ON s.store_id = o.store_id AND s.is_current
            WHERE o.store_id IS NOT NULL
              AND NOT o.is_test
              AND o.local_order_date BETWEEN ?::DATE AND ?::DATE
            GROUP BY 1, 2, 3, 4
            """,
            [WEEK_DAYS[0].isoformat(), WEEK_DAYS[-1].isoformat()],
        )
        days = [row[0] for row in con.execute(
            "SELECT DISTINCT ds FROM marts.sell_through_daily ORDER BY 1").fetchall()]
        rows = con.execute("SELECT count(*) FROM marts.sell_through_daily").fetchone()[0]
    finally:
        con.execute("DETACH live")
        con.close()
    assert [str(d) for d in days] == [d.isoformat() for d in WEEK_DAYS], (
        f"the week {WEEK_START} was stood up holding {days}, not the seven days "
        "of it — the graded week has to be a complete one"
    )
    return int(rows)


@pytest.fixture(scope="module")
def wait():
    """The rendered wait, and a warehouse holding one published week.

    The DagBag is built once. `DUCKDB_PATH` is pointed at the scratch warehouse
    for the whole module, so every poke below reads the week this file
    published and nothing else.
    """
    from airflow.models.dagbag import DagBag

    scratch = WORKDIR / "include" / "data" / "_ps274" / "copperline.duckdb"
    build_week(scratch)
    os.environ["DUCKDB_PATH"] = str(scratch)

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


def dag_globals(dag) -> dict:
    """The module the DAG's tasks were built in, as a namespace.

    Read off a callable the DAG carries rather than imported by name, so it is
    the module this DagBag actually loaded.
    """
    for task in dag.tasks:
        callable_ = getattr(task, "python_callable", None)
        if callable_ is not None and getattr(callable_, "__globals__", None):
            names = callable_.__globals__
            if "COLUMNS" in names or "SUPPLIER" in names:
                return names
    return {}


def dag_source() -> str:
    """The DAG's own file, or an empty string if it has been moved. Only ever a
    fallback: every question here is asked of the rendered task first."""
    path = WORKDIR / "projects" / "supply" / "dags" / f"{DAG_ID}.py"
    return path.read_text(encoding="utf-8") if path.exists() else ""


def week_interval_seconds(dag) -> int:
    """Seconds between two fires of this DAG, from its own schedule.

    Falls back to a week, which is the cadence `contracts/partner-share.md`
    PS-3 fixes and the shipped cron runs at.
    """
    summary = str(getattr(dag.timetable, "summary", "") or "")
    fields = summary.split()
    if len(fields) == 5 and fields[2] in ("*", "?") and fields[4] not in ("*", "?"):
        return 7 * 24 * 60 * 60
    if len(fields) == 5 and fields[2] in ("*", "?") and fields[4] in ("*", "?"):
        return 24 * 60 * 60
    return 7 * 24 * 60 * 60


# --------------------------------------------------------------------------
# the wait
# --------------------------------------------------------------------------

def test_the_step_is_still_a_wait(wait):
    """Deleting the wait, or swapping it for a step that always succeeds, is the
    cheapest way to make a stuck DAG finish and it is the one thing this ticket
    cannot mean: the mart is built by another team's DAG on another schedule,
    and the share may not be cut before the week is in it."""
    _, task = wait
    assert is_sensor(task), (
        f"{STEP} renders as {type(task).__name__}, which is not a sensor"
    )


def test_the_wait_clears_when_the_week_is_published(wait):
    """The find, put to the wait directly.

    `marts.sell_through_daily` now holds the whole of the week the last Friday
    run delivers. A wait on the mart clears. The shipped wait does not, because
    it matches `sell_through_*_<week>.csv` under `include/data/marts/` and no
    DAG, model or script in this repository writes a file of that name — the
    freight glob beside it in the same module has a producer and this one never
    had one.
    """
    _, task = wait
    answer = poked(task, DELIVERED_DS)
    assert not isinstance(answer, BaseException), (
        f"{STEP} raised for the run of {DELIVERED_DS}, whose week "
        f"({WEEK_START}) is published in {MART}: {answer!r}"
    )
    assert cleared(answer), (
        f"{STEP} does not clear for the run of {DELIVERED_DS}. The week that "
        f"run delivers ({WEEK_START} to {WEEK_DAYS[-1]}) is in {MART}, so the "
        "wait is still on something that does not turn up"
    )


def test_the_wait_holds_for_a_week_that_has_not_been_published(wait):
    """And it is still a wait. The next Friday's run delivers the week after the
    published one, which the mart holds no row for. A step that clears for that
    run is not waiting for this run's week — it is waiting for the table to
    exist, or for nothing at all, and it would cut the share on a week that is
    not there. `build_share` calls that a failed delivery in as many words.
    """
    _, task = wait
    answer = poked(task, UNPUBLISHED_DS)
    assert not cleared(answer), (
        f"{STEP} clears for the run of {UNPUBLISHED_DS}, whose week "
        f"({UNPUBLISHED_DS - dt.timedelta(days=5)}) is not in {MART} at all"
    )


def test_the_wait_ends_by_itself(wait):
    """CONVENTIONS.md §9: every sensor sets a `timeout`, and Airflow's own
    default of seven days is not one. The band is wide — no document here fixes
    a house default — but it has two ends. A wait may not outlive the run's own
    week, because `max_active_runs=1` means the next Friday queues behind this
    one, and it may not be shorter than a single poke.
    """
    dag, task = wait
    timeout = getattr(task, "timeout", None)
    assert timeout is not None, f"{STEP} sets no timeout"
    interval = week_interval_seconds(dag)
    assert 0 < float(timeout) < interval, (
        f"{STEP} waits up to {float(timeout):.0f}s on a DAG that fires every "
        f"{interval}s, so a stuck run is still holding the slot when the next "
        "delivery is due"
    )
    poke = float(getattr(task, "poke_interval", 0) or 0)
    assert float(timeout) >= poke, (
        f"{STEP} gives up after {float(timeout):.0f}s while poking every {poke:.0f}s"
    )


def test_the_dag_still_says_so_when_it_fails(wait):
    """The delivery is a commitment to a third party, and the only record of a
    missed one is the alert. A fix that drops the notifier leaves the next
    missed Friday as quiet as the twenty-two before it."""
    dag, task = wait
    reached = getattr(dag, "on_failure_callback", None) or getattr(
        task, "on_failure_callback", None)
    assert reached, (
        f"{DAG_ID} has no failure callback on the DAG or on {STEP}, so a failed "
        "delivery reaches nobody"
    )


# --------------------------------------------------------------------------
# the delivery itself, which the ticket does not license changing
# --------------------------------------------------------------------------

def test_the_file_still_carries_the_agreed_columns(wait):
    """`contracts/partner-share.md` PS-1, read off the module the DAG builds its
    tasks from. Kestrel's loader is positional: a column dropped or added moves
    everything after it.

    Falls back to the file's own text when the column list is not a module
    global any more, so a fix that restructured the module is graded on the
    columns rather than on where it keeps them.
    """
    dag, _ = wait
    agreed = ("week_start", "sku", "store_region", "demand_units", "net_sales_cents")
    columns = tuple(dag_globals(dag).get("COLUMNS") or ())
    if columns:
        assert columns == agreed, (
            f"the share is built with columns {columns}, and PS-1 fixes "
            f"{list(agreed)} in that order"
        )
        return
    text = dag_source()
    between = r"[\"']\s*,\s*[\"']"
    in_order = re.search(rf"[\"']{between.join(agreed)}[\"']", text)
    assert in_order, (
        f"{DAG_ID} no longer states the five PS-1 columns in order: {list(agreed)}"
    )


def test_the_share_is_still_scoped_to_the_one_vendor(wait):
    """The share carries one vendor's sell-through and no one else's. Widening
    it — dropping the supplier predicate to make the query return rows — sends
    every supplier's figures to a third party, which is the largest thing that
    can go wrong on this path and the easiest to do by accident.
    """
    dag, _ = wait
    names = dag_globals(dag)
    supplier = str(names.get("SUPPLIER") or "")
    share = str(names.get("SHARE") or "")
    text = dag_source()
    assert "Kestrel Outdoor" in supplier or "Kestrel Outdoor" in text, (
        "the share no longer names Kestrel Outdoor; the vendor this file is for "
        "is not a detail to be replaced with an id somebody guessed"
    )
    scoped = re.search(r"supplier", share or text, re.IGNORECASE)
    assert scoped, (
        "the share query no longer scopes to a supplier, so it would send every "
        "supplier's sell-through to one vendor"
    )


# --------------------------------------------------------------------------
# the two documents that carry the coupling
# --------------------------------------------------------------------------

def registry_rows(path: Path, marker: str) -> list:
    """The table rows in `path` whose first cell is `marker`."""
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        first = stripped.strip("|").split("|")[0].strip()
        if first.startswith(marker):
            rows.append(stripped)
    return rows


def test_the_registry_row_no_longer_sells_the_filename_pattern():
    """REG-2: the registry is the read set, and its `Lineage` column is what a
    person reads before changing a mart. Two of its rows say C-11 is picked up
    by a name-pattern sensor and is invisible because of it. Once the coupling
    moves, both are false, and a document that is false about the one consumer
    that stopped delivering is worse than no document."""
    rows = registry_rows(REGISTRY, "C-11")
    assert rows, "docs/report-registry.md no longer has a C-11 row at all"
    stale = [r for r in rows if PATTERN_CLAIM.search(r)]
    assert not stale, (
        "docs/report-registry.md still tells the next reader C-11 is coupled by "
        f"a filename pattern: {stale}"
    )


def test_the_lineage_row_moves_with_it():
    """LIN-1: the same facts by table, and the list to work before a mart
    changes. The `marts.sell_through_daily` section names C-11's coupling and
    the glob it matched."""
    rows = registry_rows(LINEAGE, "C-11")
    assert rows, "docs/lineage.md no longer lists C-11 as a reader of any mart"
    stale = [r for r in rows if PATTERN_CLAIM.search(r)]
    assert not stale, (
        "docs/lineage.md still records C-11's read as a name-pattern sensor on "
        f"a file: {stale}"
    )
