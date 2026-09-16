"""`--no-cache` leaves the cache alone — the local half included.

Only `vol` gated the store in `_cache_store`, and that is the remote half, so the
flag skipped the volume upload but still wrote the local cell. A later run without
the flag then read that cell back as an ordinary sample, and nothing in the results
said otherwise.
"""

from __future__ import annotations

import argparse
import contextlib
from pathlib import Path

import pytest

from de_bench.cli import CACHE_DIR, _cmd_run
from de_bench.tasks import load_tasks, runs_root


@pytest.fixture(autouse=True)
def isolate_cache(monkeypatch, tmp_path):
    """A cwd of this test's own, and no route to the shared volume.

    `cache/` is a relative path, so chdir is enough for it. Runs are not: they
    live outside every checkout now, so without DE_BENCH_RUNS this test writes a
    fake run into the real store. It did exactly that once.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DE_BENCH_RUNS", str(tmp_path / "runs"))

    def no_volume():
        raise AssertionError("--no-cache must not reach for the shared volume")

    monkeypatch.setattr("de_bench.cli._cache_volume", no_volume)


@pytest.fixture
def task():
    """A task from a world that ships its data. Modal is faked here, but a
    generated world still reaches for its baked image before the fake takes
    over."""
    return next(t for t in load_tasks() if not t.history and t.world.name == "upgrade-v2")


class _FakeTrial:
    """run_trial without Modal: the artifact set _cmd_run unpacks, so the
    bookkeeping around the cache write runs for real."""

    def map(self, specs, order_outputs=False, return_exceptions=False):
        for s in specs:
            yield {
                "task_id": s["task_id"], "config": s["config"], "agent": s["agent"],
                "trial": s["trial"], "model": s["model"], "thinking": s["thinking"],
                "status": "completed", "exit_code": 0, "wall_seconds": 1.0,
                "telemetry": {"turns": 1, "tool_calls": 1, "input_tokens": 10, "output_tokens": 2,
                              "cache_read_tokens": 0, "cache_write_tokens": 0, "cost_usd": 0.001},
                "diff_stat": " x | 1 +\n",
                "artifacts": {"transcript.jsonl": b'{"type":"turn_end"}\n', "stderr.txt": b"",
                              "changes.patch": b"diff --git a/x b/x\n+one\n",
                              "deleted.txt": b"", "session.jsonl": b""},
            }


@pytest.fixture
def fake_modal(monkeypatch):
    import modal

    import de_bench.modal_app as modal_app

    monkeypatch.setattr(modal, "enable_output", contextlib.nullcontext)
    monkeypatch.setattr(
        modal_app, "app", type("App", (), {"run": lambda self, **kw: contextlib.nullcontext()})()
    )
    monkeypatch.setattr(modal_app, "run_trial", _FakeTrial())


def test_run_writes_no_cell(task, fake_modal):
    rc = _cmd_run(argparse.Namespace(
        agents=["pi"], world=None, tag=None, name="experiment", tasks=[task.id], limit=None,
        model="claude-haiku-4-5", thinking="low", trials=1, no_cache=True, wall=None, matrix=None,
    ))

    assert rc == 0
    run = next(iter(runs_root().glob("*-experiment")))
    assert (run / "pi" / task.id / "t0" / "changes.patch").exists(), "the run directory is the record"
    assert not CACHE_DIR.exists(), f"--no-cache wrote {list(CACHE_DIR.rglob('*'))}"
