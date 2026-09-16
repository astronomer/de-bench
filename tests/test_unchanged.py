"""Differential grading: the untouched world is the expected result.

Nothing here needs Modal or a world. A toy DuckDB in tmp_path plays both arms,
so the comparator is exercised on its own — which is the point of keeping the
logic in scoring.py as plain functions.
"""

import duckdb
import pytest

from de_bench.scoring import (
    capture_relations,
    unchanged_plan,
    unchanged_verdict,
    read_snapshot,
    relation_snapshot,
    warehouse_db,
)
from de_bench.tasks import CHECK_KINDS


def make_tree(tmp_path, name, statements):
    """A minimal workspace: a warehouse where the world keeps one, nothing else."""
    workdir = tmp_path / name
    db = workdir / "include" / "data" / "lodestone.duckdb"
    db.parent.mkdir(parents=True)
    con = duckdb.connect(str(db))
    for sql in statements:
        con.execute(sql)
    con.close()
    return str(workdir)


PRISTINE = [
    "create schema marts",
    "create table marts.fct_shipping_costs as "
    "select * from (values ('CR-1', 42.0, 3), ('CR-2', 17.5, 1)) t(carrier_id, eur, legs)",
]


def arms(original_tree, patched_tree, checks, out_dir=None):
    """Both arms, the way the scorer runs them: same plan, no DAGs to execute."""
    plan = unchanged_plan(checks)
    a = capture_relations(original_tree, plan, {}, baseline_dags=[], replay_dags=[],
                             out_dir=out_dir, arm="original")
    b = capture_relations(patched_tree, plan, {}, baseline_dags=[], replay_dags=[], arm="patched")
    return a, b


def test_identical_output_passes(tmp_path):
    check = {"kind": "unchanged", "relations": ["marts.fct_shipping_costs"]}
    original = make_tree(tmp_path, "a", PRISTINE)
    patched = make_tree(tmp_path, "b", PRISTINE)
    a, b = arms(original, patched, [check])
    verdict = unchanged_verdict(check, a, b)
    assert verdict["passed"] is True, verdict["detail"]
    assert "2 relation" not in verdict["detail"]


def test_one_changed_value_fails(tmp_path):
    check = {"kind": "unchanged", "relations": ["marts.fct_shipping_costs"]}
    original = make_tree(tmp_path, "a", PRISTINE)
    patched = make_tree(tmp_path, "b", [
        "create schema marts",
        "create table marts.fct_shipping_costs as "
        "select * from (values ('CR-1', 42.0, 3), ('CR-2', 17.6, 1)) t(carrier_id, eur, legs)",
    ])
    a, b = arms(original, patched, [check])
    verdict = unchanged_verdict(check, a, b)
    assert verdict["passed"] is False
    assert "17.5" in verdict["detail"] and "17.6" in verdict["detail"]


def test_a_duplicated_row_fails_on_the_count(tmp_path):
    """EXCEPT both ways is empty here — the same distinct rows on both sides.
    Only the count catches a row emitted twice, which is what a re-run that
    appends instead of replacing looks like."""
    check = {"kind": "unchanged", "relations": ["marts.fct_shipping_costs"]}
    original = make_tree(tmp_path, "a", PRISTINE)
    patched = make_tree(tmp_path, "b", PRISTINE + [
        "insert into marts.fct_shipping_costs values ('CR-1', 42.0, 3)",
    ])
    a, b = arms(original, patched, [check])
    rows_a = a["2026-03-01"]["relations"]["marts.fct_shipping_costs"]["rows"]
    rows_b = b["2026-03-01"]["relations"]["marts.fct_shipping_costs"]["rows"]
    assert {tuple(r) for r in rows_a} == {tuple(r) for r in rows_b}, "the set test must be blind here"
    verdict = unchanged_verdict(check, a, b)
    assert verdict["passed"] is False
    assert "3 row(s)" in verdict["detail"] and "produces 2" in verdict["detail"]


def test_a_dropped_row_fails(tmp_path):
    check = {"kind": "unchanged", "relations": ["marts.fct_shipping_costs"]}
    original = make_tree(tmp_path, "a", PRISTINE)
    patched = make_tree(tmp_path, "b", PRISTINE + [
        "delete from marts.fct_shipping_costs where carrier_id = 'CR-2'",
    ])
    a, b = arms(original, patched, [check])
    verdict = unchanged_verdict(check, a, b)
    assert verdict["passed"] is False
    assert "missing" in verdict["detail"]


def test_a_column_that_can_move_on_its_own_is_rejected(tmp_path):
    """A wall-clock column would make the two arms differ for reasons the agent
    had nothing to do with. Reject it loudly; allow_columns is the author
    saying it is pinned after all."""
    statements = [
        "create schema marts",
        "create table marts.fct_shipping_costs as "
        "select 'CR-1' as carrier_id, 42.0 as eur, timestamp '2026-03-01 00:00:00' as loaded_at",
    ]
    original = make_tree(tmp_path, "a", statements)
    patched = make_tree(tmp_path, "b", statements)
    check = {"kind": "unchanged", "relations": ["marts.fct_shipping_costs"]}
    a, b = arms(original, patched, [check])

    verdict = unchanged_verdict(check, a, b)
    assert verdict["passed"] is False
    assert "loaded_at" in verdict["detail"] and "allow_columns" in verdict["detail"]

    allowed = {**check, "allow_columns": ["loaded_at"]}
    assert unchanged_verdict(allowed, a, b)["passed"] is True


@pytest.mark.parametrize("column", ["run_uuid", "random_seed", "created_at"])
def test_the_guard_covers_every_spelling_it_claims(tmp_path, column):
    statements = [
        "create schema marts",
        f"create table marts.t as select 1 as k, 2 as {column}",
    ]
    check = {"kind": "unchanged", "relations": ["marts.t"]}
    a, b = arms(make_tree(tmp_path, "a", statements), make_tree(tmp_path, "b", statements), [check])
    assert unchanged_verdict(check, a, b)["passed"] is False
    assert unchanged_verdict({**check, "allow_columns": [column]}, a, b)["passed"] is True


def test_a_column_named_after_a_real_word_is_not_rejected(tmp_path):
    """`_at` anchors at the end of the name — `attribute` and `latency` are not
    wall clocks, and failing them would make the guard unusable."""
    statements = [
        "create schema marts",
        "create table marts.t as select 1 as attribute, 2 as latency_ms, 3 as at_risk",
    ]
    check = {"kind": "unchanged", "relations": ["marts.t"]}
    a, b = arms(make_tree(tmp_path, "a", statements), make_tree(tmp_path, "b", statements), [check])
    assert unchanged_verdict(check, a, b)["passed"] is True


def test_grading_twice_gives_the_same_verdict(tmp_path):
    check = {"kind": "unchanged", "relations": ["marts.fct_shipping_costs"]}
    original = make_tree(tmp_path, "a", PRISTINE)
    patched = make_tree(tmp_path, "b", PRISTINE + [
        "insert into marts.fct_shipping_costs values ('CR-3', 9.25, 2)",
    ])
    first = unchanged_verdict(check, *arms(original, patched, [check]))
    second = unchanged_verdict(check, *arms(original, patched, [check]))
    assert first == second
    assert first["passed"] is False


def test_a_missing_original_arm_fails_rather_than_skips(tmp_path):
    """There is no authored expected value to fall back on, so a missing
    original snapshot has to read as a fault, never as agreement."""
    check = {"kind": "unchanged", "relations": ["marts.fct_shipping_costs"]}
    patched = make_tree(tmp_path, "b", PRISTINE)
    _, b = arms(make_tree(tmp_path, "a", PRISTINE), patched, [check])
    verdict = unchanged_verdict(check, None, b)
    assert verdict["passed"] is False
    assert "original" in verdict["detail"]


def test_a_relation_the_patch_removed_fails(tmp_path):
    check = {"kind": "unchanged", "relations": ["marts.fct_shipping_costs"]}
    a, b = arms(make_tree(tmp_path, "a", PRISTINE), make_tree(tmp_path, "b", ["create schema marts"]), [check])
    verdict = unchanged_verdict(check, a, b)
    assert verdict["passed"] is False
    assert "does not exist" in verdict["detail"]


def test_a_renamed_column_fails_before_the_rows_are_read(tmp_path):
    patched = [
        "create schema marts",
        "create table marts.fct_shipping_costs as "
        "select * from (values ('CR-1', 42.0, 3), ('CR-2', 17.5, 1)) t(carrier_id, euros, legs)",
    ]
    check = {"kind": "unchanged", "relations": ["marts.fct_shipping_costs"]}
    a, b = arms(make_tree(tmp_path, "a", PRISTINE), make_tree(tmp_path, "b", patched), [check])
    verdict = unchanged_verdict(check, a, b)
    assert verdict["passed"] is False
    assert "columns" in verdict["detail"]


def test_the_snapshot_survives_the_trip_through_disk(tmp_path):
    """The original arm writes its dump to a scratch directory outside the
    workspace and the scorer reads it back; JSON must not change a verdict."""
    check = {"kind": "unchanged", "relations": ["marts.fct_shipping_costs"]}
    out = tmp_path / "original"
    a, b = arms(make_tree(tmp_path, "a", PRISTINE), make_tree(tmp_path, "b", PRISTINE), [check],
                out_dir=str(out))
    assert unchanged_verdict(check, a, b) == unchanged_verdict(check, read_snapshot(str(out)), b)
    assert read_snapshot(str(tmp_path / "nowhere")) is None


def test_rows_are_canonicalized_not_compared_as_written(tmp_path):
    """Insert order and float spelling are not part of the answer; the 4dp
    rounding every other check uses applies here too."""
    check = {"kind": "unchanged", "relations": ["marts.t"]}
    a, b = arms(
        make_tree(tmp_path, "a", [
            "create schema marts",
            "create table marts.t as select * from (values ('a', 1.000004), ('b', 2.0)) v(k, x)",
        ]),
        make_tree(tmp_path, "b", [
            "create schema marts",
            "create table marts.t as select * from (values ('b', 2.0), ('a', 1.0)) v(k, x)",
        ]),
        [check],
    )
    assert unchanged_verdict(check, a, b)["passed"] is True


def test_the_plan_groups_by_date_and_unions_the_work():
    plan = unchanged_plan([
        {"kind": "unchanged", "relations": ["marts.b", "marts.a"], "run": ["build_marts"]},
        {"kind": "unchanged", "relations": ["marts.a"], "run": ["build_marts", "load_raw"]},
        {"kind": "unchanged", "relations": ["marts.c"], "ds": "2026-04-01"},
        {"kind": "dag_runs", "dag_id": "ignored"},
    ])
    assert sorted(plan) == ["2026-03-01", "2026-04-01"]
    assert plan["2026-03-01"]["relations"] == ["marts.a", "marts.b"]
    assert plan["2026-03-01"]["run"] == ["build_marts", "load_raw"]
    assert plan["2026-04-01"]["relations"] == ["marts.c"]


def test_the_warehouse_is_read_from_the_workspace_by_default(tmp_path):
    assert warehouse_db("/work") == "/work/include/data/lodestone.duckdb"
    assert warehouse_db("/work", {"db": "warehouse/copperline.duckdb"}) == "/work/warehouse/copperline.duckdb"
    assert relation_snapshot(str(tmp_path / "gone.duckdb"), "marts.t")["error"].startswith("no warehouse")


def test_the_stage_is_wired_into_the_scorer(tmp_path):
    """The scorer reads the snapshot the caller left, and grades the patched
    tree against it. No snapshot means a failed check, not a quiet pass."""
    from de_bench.scoring import score_workspace

    check = {"kind": "unchanged", "relations": ["marts.fct_shipping_costs"]}
    out = tmp_path / "original"
    capture_relations(make_tree(tmp_path, "a", PRISTINE), unchanged_plan([check]), {},
                         baseline_dags=[], replay_dags=[], out_dir=str(out), arm="original")
    patched = make_tree(tmp_path, "b", PRISTINE)

    def score(original_dir):
        return score_workspace(patched, [check], [], "diff --git a/x b/x\n",
                               baseline_dags=[], replay_dags=[], original_dir=original_dir)

    graded = score(str(out))
    assert graded["pass"] is True
    assert graded["checks"]["unchanged:marts.fct_shipping_costs"]["passed"] is True
    assert score(None)["pass"] is False


def test_the_check_kind_is_declared():
    assert CHECK_KINDS["unchanged"] == ("relations",)


def test_a_malformed_unchanged_check_fails_at_load(tmp_path):
    """A bare string for relations would grade the characters of the name."""
    import yaml

    from de_bench.tasks import load_task

    (tmp_path / "worlds" / "toy").mkdir(parents=True)
    (tmp_path / "worlds" / "toy" / "world.yaml").write_text(yaml.safe_dump({"name": "toy"}))
    task_dir = tmp_path / "tasks" / "toy" / "t1"
    task_dir.mkdir(parents=True)
    (task_dir / "task.yaml").write_text(yaml.safe_dump({"id": "t1", "world": "toy"}))

    def load(check):
        (task_dir / "checks.yaml").write_text(yaml.safe_dump({"checks": [check]}))
        return load_task(task_dir, {}, tmp_path)

    load({"kind": "unchanged", "relations": ["marts.a"], "run": ["build_marts"]})
    with pytest.raises(ValueError, match="missing relations"):
        load({"kind": "unchanged"})
    with pytest.raises(ValueError, match="relations must be a list"):
        load({"kind": "unchanged", "relations": "marts.a"})
    with pytest.raises(ValueError, match="run must be a list"):
        load({"kind": "unchanged", "relations": ["marts.a"], "run": "build_marts"})
