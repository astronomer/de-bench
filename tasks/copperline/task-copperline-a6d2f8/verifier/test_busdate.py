"""MIG-96 — does the bridge tell the estate the night it owns?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it loads the patched DAG module the same way a DagBag would.

**No authored dates.** Every expectation below is `ds` minus one day, worked out
in this session from the `ds` the test hands the DAG. The rule itself comes from
two read-only records — the `BUSDATE` line at the top of
`legacy/autosys/copperline.jil`, and `ops.load_control`, where every row started
at 01:00 on the day after its own business date — and both sit under the world's
`do_not_modify` floor, so nothing here can drift away from what the world says.

**Why a verifier rather than a run.** `trigger_nightly` shells `ssh` to a box
that is not in the container. A `dag_runs` check would be red whatever `BUSDATE`
says and would grade the network. So the runner is stubbed and the step is
called with the arguments the DAG itself passes it, rendered.

**Why the arguments are rendered rather than read.** `CONVENTIONS.md` puts the
shift in the template — `TARGET_DS = "{{ macros.ds_add(ds, -1) }}"`, named once
at the top of the file — and a fix inside the callable is the same fix spelled
differently. Rendering the task's own `op_kwargs` and calling the callable with
them grades what the estate is actually told and takes no view on which of the
two spellings the writer chose.

**What the five tests separate.**

  the trigger fixed                        test 1
  every `{{ ds }}` in the file shifted     tests 2 and 3 — the run calendar
                                           governs the day the box STARTS, and
                                           the ticket says both stay
  the calendar reader repaired or moved    test 4; that is MIG-63's surface and
                                           the ticket says twice to leave it
  the trigger rewritten and the "nothing   test 5
    due" exit code turned back into a
    failure

**The sixth test grades the world, not the patch.** The six figures the ticket
asks for in `RESPONSE.md` are matched by pattern, and a pattern cannot tell a
stale number from a wrong one. Test 6 recomputes all six from the warehouse and
the export in this session and asserts they are still what `checks.yaml` looks
for, so a timeline change fails loudly here instead of quietly grading an answer
the world no longer gives.
"""

from __future__ import annotations

import datetime as dt
import importlib
import sys
import importlib.util
import re
import types

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the tree root the same way every DAG in the world does —
# from its own file, whatever the working directory happens to be.
from include.lib import warehouse, workspace_root

BRIDGE = workspace_root() / "projects" / "supply" / "dags" / "sc_autosys_bridge.py"
CALENDAR = (
    workspace_root() / "legacy" / "autosys" / "calendars" / "retail_2026.cal"
)
DB = str(warehouse.warehouse_path())

#: The cutover. `docs/runbooks/orchestrator-migration.md` ORC-1: wave 1 moved on
#: this date and run history exists from it, because the deployment the estate
#: runs on now was made for the migration.
CUTOVER = dt.date(2026, 1, 5)

#: Run days to ask about. One ordinary night, one that crosses a month end and
#: one that crosses a year end, because a shift written with string slicing
#: rather than a date gets the first right and the other two wrong.
RUN_DAYS = ("2026-06-15", "2026-03-01", "2026-01-01")

#: The exit code an AutoSys box uses for "nothing due today". It is an outcome,
#: not a failure, and the shipped DAG says so.
NOTHING_DUE = 4


def _macros() -> types.ModuleType | types.SimpleNamespace:
    """Airflow's `macros`, under whichever name this Airflow keeps it.

    The house spelling is `{{ macros.ds_add(ds, -1) }}` and eight DAGs in this
    world already use it, so the module is there; the fallback exists so a
    rename in a future Airflow fails the fix and not the harness.
    """
    for name in ("airflow.sdk.execution_time.macros", "airflow.macros"):
        try:
            return importlib.import_module(name)
        except Exception:  # noqa: BLE001 - any import trouble means try the next
            continue

    def ds_add(ds: str, days: int) -> str:
        return (dt.date.fromisoformat(ds) + dt.timedelta(days=int(days))).isoformat()

    def ds_format(ds: str, input_format: str, output_format: str) -> str:
        return dt.datetime.strptime(ds, input_format).strftime(output_format)

    return types.SimpleNamespace(
        ds_add=ds_add, ds_format=ds_format,
        datetime=dt.datetime, timedelta=dt.timedelta, time=dt.time,
    )


MACROS = _macros()


@pytest.fixture(scope="module")
def bridge():
    """The patched DAG module, loaded from its file.

    Loading it builds the DAG object, which is what a parse does.
    """
    spec = importlib.util.spec_from_file_location("sc_autosys_bridge_under_test", BRIDGE)
    assert spec and spec.loader, f"FAIL: {BRIDGE} is not importable"
    module = importlib.util.module_from_spec(spec)
    # Register the module before executing it: dataclasses resolves string
    # annotations through sys.modules, and a hand-loaded module that is not
    # there fails on any @dataclass it defines. A fair repair lost a trial
    # to this before the line below.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def dag_of(module):
    """The DAG the module defines."""
    dag = getattr(module, "dag", None)
    if dag is None:
        dag = next(
            (v for v in vars(module).values() if hasattr(v, "get_task") and hasattr(v, "dag_id")),
            None,
        )
    assert dag is not None, "FAIL: sc_autosys_bridge no longer defines a DAG"
    return dag


def context(dag, ds: str) -> dict:
    """What a run of this DAG is told about its day.

    A bare cron string is a trigger timetable, so `data_interval_start` and
    `data_interval_end` are both the moment the run fires — `CONVENTIONS.md`,
    Dates. Everything a writer might reach for to name the day before is in
    here, so a fix spelled any of the usual ways renders.
    """
    day = dt.date.fromisoformat(ds)
    fires = dt.datetime(day.year, day.month, day.day, tzinfo=dt.timezone.utc)
    return {
        "ds": ds,
        "ds_nodash": ds.replace("-", ""),
        "prev_ds": (day - dt.timedelta(1)).isoformat(),
        "next_ds": (day + dt.timedelta(1)).isoformat(),
        "logical_date": fires,
        "data_interval_start": fires,
        "data_interval_end": fires,
        "ts": fires.isoformat(),
        "ts_nodash": fires.strftime("%Y%m%dT%H%M%S"),
        "run_id": f"scheduled__{fires.isoformat()}",
        "macros": MACROS,
        "params": dict(getattr(dag, "params", None) or {}),
        "dag": dag,
    }


def arguments(module, task_id: str, ds: str) -> dict:
    """One step's arguments for a run on `ds`, rendered."""
    dag = dag_of(module)
    task = dag.get_task(task_id)
    try:
        env = dag.get_template_env()
    except Exception:  # noqa: BLE001 - a plain environment renders the same string
        import jinja2

        env = jinja2.Environment()
    ctx = context(dag, ds)
    rendered = {}
    for name, value in dict(getattr(task, "op_kwargs", None) or {}).items():
        if isinstance(value, str) and "{{" in value:
            try:
                value = env.from_string(value).render(**ctx)
            except Exception as exc:  # noqa: BLE001 - the blame is the finding
                raise AssertionError(
                    f"FAIL: {task_id} passes {name}={value!r}, which does not render "
                    f"for a run on {ds}: {exc!r}"
                ) from exc
        rendered[name] = value
    return rendered


def callable_of(module, task_id: str):
    """The function one step calls."""
    task = dag_of(module).get_task(task_id)
    function = getattr(task, "python_callable", None)
    assert function is not None, f"FAIL: the {task_id} step no longer calls a python function"
    return function


def told(module, ds: str, exit_code: int = 0) -> dict:
    """What the estate is handed when the bridge starts the night for `ds`.

    The runner is stubbed: `include/lib/legacy_runner.py` shells `ssh` to a box
    that is not in this container, and what is being graded is the variable
    block, not the network. The stub goes on the house module and on the DAG
    module both, so it lands whether the DAG holds `legacy_runner` or the bare
    `run_step` name.
    """
    from include.lib import legacy_runner

    seen: dict = {}

    def fake_run_step(artifact, step, *, variables=None, check=True, **rest):
        seen["artifact"] = artifact
        seen["step"] = step
        seen["variables"] = dict(variables or {})
        return legacy_runner.StepResult(
            step=step, artifact=str(artifact), exit_code=exit_code,
        )

    holders = [legacy_runner]
    if hasattr(module, "run_step"):
        holders.append(module)
    real = [(holder, holder.run_step) for holder in holders]
    for holder in holders:
        holder.run_step = fake_run_step
    try:
        seen["returned"] = callable_of(module, "trigger_nightly")(
            **arguments(module, "trigger_nightly", ds)
        )
    finally:
        for holder, function in real:
            holder.run_step = function
    return seen


def test_the_trigger_asks_the_estate_for_the_night_the_run_owns(bridge):
    """`BUSDATE` is the day before the day the run fires.

    The export's header says it and every row of `ops.load_control` bears it
    out. The shipped DAG hands the runner `{{ ds }}`, which is the day the run
    fires, so the estate has been told to work a night that has not closed.
    """
    for ds in RUN_DAYS:
        want = (dt.date.fromisoformat(ds) - dt.timedelta(1)).isoformat()
        variables = told(bridge, ds)["variables"]
        assert "BUSDATE" in variables, (
            f"FAIL: the run on {ds} started the night without a BUSDATE at all; "
            f"the runner was handed {sorted(variables)}"
        )
        assert str(variables["BUSDATE"]) == want, (
            f"FAIL: the run on {ds} told the estate BUSDATE={variables['BUSDATE']!r}; "
            f"the night that run owns is {want}"
        )


def test_the_calendar_step_still_asks_about_the_day_the_box_starts(bridge):
    """A run calendar governs starting, not the business date.

    The ticket says this in as many words. A fix that shifts every `{{ ds }}`
    in the file moves the gate a day as well, and then the log answers Monday's
    question with Sunday's answer.
    """
    for ds in RUN_DAYS:
        shifted = (dt.date.fromisoformat(ds) - dt.timedelta(1)).isoformat()
        given = arguments(bridge, "report_calendar_day", ds)
        assert ds in given.values(), (
            f"FAIL: report_calendar_day was given {given} for a run on {ds}; the run "
            "calendar answers about the day the box starts, which is the day the run "
            "fires"
        )
        assert shifted not in given.values(), (
            f"FAIL: report_calendar_day was given {shifted} for a run on {ds}; the "
            "run calendar gates the day the box starts, not the night it carries"
        )


def test_the_control_table_step_still_reads_the_day_it_always_read(bridge):
    """`check_jobs_wrote` stays on `ds`.

    The ticket says to leave it and to write it up instead: what it reports is
    what supply-chain read every morning, and moving it is a change finance are
    told about first. It is the reason nothing has gone red, not a second bug
    to fix in the same patch.
    """
    for ds in RUN_DAYS:
        shifted = (dt.date.fromisoformat(ds) - dt.timedelta(1)).isoformat()
        given = arguments(bridge, "check_jobs_wrote", ds)
        assert ds in given.values(), (
            f"FAIL: check_jobs_wrote was given {given} for a run on {ds}; the ticket "
            "says that step does not move in this change"
        )
        assert shifted not in given.values(), (
            f"FAIL: check_jobs_wrote was given {shifted} for a run on {ds}; the ticket "
            "says to leave that step alone and to write it up instead"
        )


def test_the_calendar_reader_is_still_where_the_next_reader_looks(bridge):
    """`calendar_days` and `is_due` keep their names and their home.

    They are MIG-63's surface. This ticket says twice to leave them alone, and
    the cheap wrong answer to "the log says the same thing every night" is to
    go and repair the reader instead of the variable.
    """
    for name in ("calendar_days", "is_due"):
        assert hasattr(bridge, name), (
            f"FAIL: sc_autosys_bridge no longer has {name}(); the calendar reader is "
            "another ticket and this one does not touch it"
        )


def test_a_night_with_nothing_due_is_still_not_a_failure(bridge):
    """The exit code the boxes use for "nothing due today" is still an outcome.

    A rewrite of the trigger that drops `check=False` turns every night the
    calendar leaves out into a red task, which is a bigger change than the one
    the ticket asked for and nothing else here would notice.
    """
    ds = RUN_DAYS[0]
    told(bridge, ds, exit_code=0)
    told(bridge, ds, exit_code=NOTHING_DUE)
    try:
        told(bridge, ds, exit_code=17)
    except AssertionError:
        raise
    except Exception:  # noqa: BLE001 - any raise is the right answer here
        return
    pytest.fail(
        "FAIL: an exit code that is neither success nor 'nothing due' came back as a "
        "clean night; a box that failed has to fail the task"
    )


def export_days() -> set[dt.date]:
    """The days `retail_2026.cal` names.

    An `autocal_asc` export: `/* ... */` comments, a `calendar:` line, then the
    dates, six to a line, US-style. The header comment lists the holidays that
    were left out, written the same way, so the comments come out before
    anything is read as a day.
    """
    body = re.sub(r"/\*.*?\*/", " ", CALENDAR.read_text(encoding="utf-8"), flags=re.S)
    return {
        dt.date(int(y), int(m), int(d))
        for m, d, y in re.findall(r"\b(\d{2})/(\d{2})/(\d{4})\b", body)
    }


def test_the_figures_the_note_asks_for_are_still_the_ones_the_world_gives():
    """The six answers, recomputed from the warehouse and the export.

    This one says nothing about the patch. It guards the six patterns in
    `checks.yaml`, which hold numbers that a change to the world's timeline
    would move without anything else noticing.
    """
    con = duckdb.connect(DB, read_only=True)
    try:
        rows, breaks, last = con.execute(
            """
            SELECT count(*),
                   count(*) FILTER (
                       WHERE started_at::DATE <> business_date + INTERVAL 1 DAY),
                   max(business_date)
            FROM ops.load_control
            """
        ).fetchone()
    finally:
        con.close()

    assert (rows, breaks) == (15577, 0), (
        f"FAIL: ops.load_control now holds {rows} row(s) with {breaks} that break the "
        "day-after rule; checks.yaml looks for 15,577 and none"
    )

    nights = [CUTOVER + dt.timedelta(n) for n in range((last - CUTOVER).days + 1)]
    assert len(nights) == 161, (
        f"FAIL: the cutover to {last} is now {len(nights)} night(s); checks.yaml "
        "looks for 161"
    )

    calendared = [d for d in nights if d in export_days()]
    assert len(calendared) == 112, (
        f"FAIL: the run calendar now names {len(calendared)} of those nights; "
        "checks.yaml looks for 112"
    )

    owed = {d - dt.timedelta(1) for d in calendared}
    dropped = sorted(owed - set(calendared))
    gained = sorted(set(calendared) - owed)
    assert len(dropped) == 23 and len(gained) == 23, (
        f"FAIL: the shift now drops {len(dropped)} business date(s) and adds "
        f"{len(gained)}; checks.yaml looks for 23 and 23"
    )
    weekdays = {d.strftime("%A") for d in dropped}
    assert weekdays == {"Sunday", "Monday"}, (
        f"FAIL: the dropped dates now fall on {sorted(weekdays)}; checks.yaml looks "
        "for Sundays and Mondays"
    )
    assert {d.strftime("%A") for d in gained} == {"Friday"}, (
        "FAIL: the added dates are no longer all Fridays; checks.yaml looks for "
        "23 Fridays"
    )
