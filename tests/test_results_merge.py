"""Re-running one column must not delete the rest of the run."""

import json

from de_bench.cli import _write_results


def row(config, task, cost=1.0, trial=0):
    return {"config": config, "task_id": task, "trial": trial,
            "telemetry": {"cost_usd": cost}, "wall_seconds": 1.0}


def test_a_rerun_keeps_the_columns_it_did_not_touch(tmp_path):
    _write_results(tmp_path, [row("pi-opus", "t1"), row("pi-opus", "t2"), row("otto-opus", "t1")])
    _write_results(tmp_path, [row("opencode-opus", "t1")])  # the subset re-run
    out = json.loads((tmp_path / "results.json").read_text())
    assert {(r["config"], r["task_id"]) for r in out} == {
        ("pi-opus", "t1"), ("pi-opus", "t2"), ("otto-opus", "t1"), ("opencode-opus", "t1")}


def test_a_rerun_replaces_its_own_rows(tmp_path):
    _write_results(tmp_path, [row("opencode-opus", "t1", cost=9.0)])
    _write_results(tmp_path, [row("opencode-opus", "t1", cost=2.0)])
    out = json.loads((tmp_path / "results.json").read_text())
    assert len(out) == 1 and out[0]["telemetry"]["cost_usd"] == 2.0


def test_trials_of_the_same_cell_are_distinct_rows(tmp_path):
    _write_results(tmp_path, [row("pi-opus", "t1", trial=0), row("pi-opus", "t1", trial=1)])
    assert len(json.loads((tmp_path / "results.json").read_text())) == 2


def test_unreadable_history_does_not_lose_this_run(tmp_path):
    (tmp_path / "results.json").write_text("{ not json")
    out = _write_results(tmp_path, [row("pi-opus", "t1")])
    assert len(out) == 1
