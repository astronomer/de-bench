"""The report's arithmetic: what counts as a trial, and what sits on the frontier."""

import json

import pytest

from de_bench.report import ConfigStats, aggregate, build, chart_png, pareto_frontier, render


def cfg(name, cost, rate, n=10, agent="pi"):
    c = ConfigStats(name=name, agent=agent, model="m", thinking="high")
    c.scored = n
    c.passes = round(rate * n)
    c.costs = [cost] * n
    return c


def test_frontier_keeps_the_undominated():
    cheap_bad = cfg("cheap-bad", 0.05, 0.3)
    cheap_good = cfg("cheap-good", 0.06, 0.6)
    dear_best = cfg("dear-best", 1.50, 0.9)
    dominated = cfg("dominated", 0.80, 0.5)  # dearer than cheap-good, no better
    front = pareto_frontier([cheap_bad, cheap_good, dear_best, dominated])
    assert front == {"cheap-bad", "cheap-good", "dear-best"}


def test_frontier_keeps_both_sides_of_a_tie():
    a, b = cfg("a", 0.10, 0.5), cfg("b", 0.10, 0.5)
    assert pareto_frontier([a, b]) == {"a", "b"}


def test_frontier_ignores_unscored_configs():
    scored = cfg("scored", 0.5, 0.5)
    unscored = ConfigStats(name="unscored", agent="pi", model="m", thinking="high")
    assert pareto_frontier([scored, unscored]) == {"scored"}


def _run(outcome="scored", passed=True, cost=0.10):
    results = [{
        "task_id": "t1", "config": "c1", "agent": "pi", "trial": 0,
        "model": "claude-haiku-4-5", "thinking": "low", "wall_seconds": 10.0,
        "telemetry": {"cost_usd": cost, "input_tokens": 5, "output_tokens": 3},
        "outcome": outcome,
    }]
    scores = [{
        "config": "c1", "task_id": "t1", "trial": 0, "pass": passed,
        "tiers": {
            "delivered": {"passed": True}, "do_not_modify": {"passed": True},
            "parses": {"passed": passed}, "runs": {"passed": None},
            "idempotent": {"passed": None},
        },
    }]
    return results, scores, {"configs": [{"name": "c1", "agent": "pi"}]}


def test_infra_failures_leave_the_numbers_alone():
    agg = aggregate(*_run(outcome="infra_failed"))
    c = agg["configs"][0]
    assert (c.trials, c.scored, c.infra, c.total_cost) == (1, 0, 1, 0.0)
    assert not agg["scored_any"]


def test_failure_is_blamed_on_the_first_broken_tier():
    agg = aggregate(*_run(passed=False))
    assert agg["configs"][0].tier_fails == {"parses": 1}


def test_cost_per_pass_is_none_without_a_pass():
    assert aggregate(*_run(passed=False))["configs"][0].cost_per_pass is None
    assert aggregate(*_run(passed=True))["configs"][0].cost_per_pass == pytest.approx(0.10)


def test_unscored_run_still_aggregates():
    results, _, meta = _run()
    agg = aggregate(results, [], meta)
    assert agg["scored_any"] is False
    assert agg["configs"][0].total_cost == pytest.approx(0.10)
    assert "de-bench score" in render(agg, "r", meta)


def test_one_config_says_so_instead_of_plotting():
    agg = aggregate(*_run())
    assert "no frontier to plot" in render(agg, "r", {})


def test_chart_is_drawn_for_two_or_more_configs(tmp_path):
    cfgs = [cfg("cheap", 0.05, 0.4, agent="pi"), cfg("dear", 1.5, 0.9, agent="otto")]
    agg = {"configs": cfgs, "frontier": pareto_frontier(cfgs)}
    png = tmp_path / "c.png"
    assert chart_png(agg, png) is True
    assert png.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"


def test_chart_is_skipped_when_there_is_nothing_to_compare(tmp_path):
    """One point is a stat, not a scatter — and no file should appear."""
    agg = {"configs": [cfg("only", 0.05, 0.4)], "frontier": {"only"}}
    png = tmp_path / "c.png"
    assert chart_png(agg, png) is False
    assert not png.exists()


def test_chart_handles_a_crowd_without_raising(tmp_path):
    """Many configs, several sharing a pass rate — the label nudging must hold."""
    cfgs = [cfg(f"c{i}", 0.05 + i * 0.13, 0.30 + i * 0.02, agent=f"a{i % 5}") for i in range(19)]
    agg = {"configs": cfgs, "frontier": pareto_frontier(cfgs)}
    assert chart_png(agg, tmp_path / "c.png") is True


def _two_task_run(configs=("c1", "c2"), tasks=("task-authoring-aaa", "task-upgrade-bbb")):
    """One trial per (config, task), every one a pass."""
    results, scores = [], []
    for i, name in enumerate(configs):
        for task in tasks:
            results.append({
                "task_id": task, "config": name, "agent": "pi", "trial": 0,
                "model": "m", "thinking": "high", "wall_seconds": 5.0,
                "telemetry": {"cost_usd": 0.10 * (i + 1)}, "outcome": "scored",
            })
            scores.append({"config": name, "task_id": task, "trial": 0, "pass": True,
                           "tiers": {"parses": {"passed": True}}})
    return results, scores, {"configs": [{"name": n, "agent": "pi"} for n in configs]}


def test_report_builds_without_a_world_map(tmp_path):
    results, scores, meta = _two_task_run()
    for name, blob in (("results", results), ("scores", scores), ("meta", meta)):
        (tmp_path / f"{name}.json").write_text(json.dumps(blob))
    markdown, _, _ = build(tmp_path, tmp_path / "report.md")
    assert "## Configs" in markdown


def _write_run(root, name, date, world_sha, cost, passed, task="t1"):
    d = root / name
    d.mkdir(parents=True)
    (d / "results.json").write_text(json.dumps([{
        "task_id": task, "config": "c1", "agent": "pi", "trial": 0,
        "model": "m", "thinking": "high", "wall_seconds": 5.0,
        "telemetry": {"cost_usd": cost}, "outcome": "scored",
    }]))
    (d / "scores.json").write_text(json.dumps([{
        "config": "c1", "task_id": task, "trial": 0, "pass": passed,
        "tiers": {"parses": {"passed": passed}},
    }]))
    (d / "meta.json").write_text(json.dumps({
        "date": date, "worlds": {"lodestone": world_sha},
        "configs": [{"name": "c1", "agent": "pi"}],
    }))
    return d


def test_snapshot_takes_the_newest_result_per_cell(tmp_path):
    from de_bench.report import collect

    _write_run(tmp_path, "2026-01-01-old", "2026-01-01", "abc", 0.99, False)
    _write_run(tmp_path, "2026-02-01-new", "2026-02-01", "abc", 0.11, True)
    results, scores, _, info = collect(tmp_path, {"lodestone": "abc"})
    assert len(results) == 1
    assert results[0]["telemetry"]["cost_usd"] == pytest.approx(0.11)
    assert scores[0]["pass"] is True
    assert info["runs"] == {"2026-01-01-old": 1, "2026-02-01-new": 1}


def test_snapshot_keeps_cells_a_newer_run_did_not_touch(tmp_path):
    """Re-running two configs must not wipe the rest of a matrix."""
    from de_bench.report import collect

    _write_run(tmp_path, "2026-01-01-matrix", "2026-01-01", "abc", 0.5, True, task="t1")
    _write_run(tmp_path, "2026-02-01-patch", "2026-02-01", "abc", 0.5, True, task="t2")
    results, _, _, _ = collect(tmp_path, {"lodestone": "abc"})
    assert {r["task_id"] for r in results} == {"t1", "t2"}


def test_snapshot_drops_runs_from_a_changed_world(tmp_path):
    from de_bench.report import collect

    _write_run(tmp_path, "2026-01-01-stale", "2026-01-01", "old-sha", 0.5, True)
    results, _, meta, info = collect(tmp_path, {"lodestone": "new-sha"})
    assert results == [] and info["stale"] == {"2026-01-01-stale": 1}
    assert meta["worlds"] == {}  # nothing contributed, so nothing is claimed


def test_snapshot_skips_unscored_trials(tmp_path):
    from de_bench.report import collect

    d = _write_run(tmp_path, "2026-01-01-run", "2026-01-01", "abc", 0.5, True)
    (d / "scores.json").write_text("[]")
    results, _, _, info = collect(tmp_path, {"lodestone": "abc"})
    assert results == [] and info["unscored"] == 1


def test_build_returns_markdown(tmp_path):
    results, scores, meta = _run()
    (tmp_path / "results.json").write_text(json.dumps(results))
    (tmp_path / "scores.json").write_text(json.dumps(scores))
    (tmp_path / "meta.json").write_text(json.dumps(meta))
    markdown, chart, agg = build(tmp_path, tmp_path / "report.md")
    assert markdown.startswith("# de-bench —") and agg["configs"][0].passes == 1
    assert "| config |" in markdown
    assert "<" not in markdown  # markdown, not smuggled HTML
    assert chart is None  # a single config gets prose, not a plot
