"""The generated world: the tar the harness ships, and the setup step that runs it.

The generator is the world's answer key, so the whole point of these tests is the
negative: after setup the warehouse is full and every trace of the generator is
gone — nothing under the workdir, nothing left in the staging area. They build a
toy world and a stub generator in `tmp_path`, so no real world has to exist yet.
"""

from __future__ import annotations

import io
import tarfile
import tempfile
from pathlib import Path

import duckdb
import pytest

from de_bench.modal_app import _prepare_workspace_extras
from de_bench.tasks import generated_tar, load_task

TODAY = "2026-06-15"

# A stand-in for tools/gen_copperline: reads the same command line, writes one
# table recording what it was given, so a test can assert the contract held.
STUB_GENERATOR = """
import argparse
import os
import pathlib

import duckdb

p = argparse.ArgumentParser()
p.add_argument("--timeline", required=True)
p.add_argument("--planted", required=True)
p.add_argument("--db", required=True)
p.add_argument("--landing", required=True)
p.add_argument("--profile", required=True)
a = p.parse_args()

landing = pathlib.Path(a.landing) / "toy" / "dt=2026-06-01"
landing.mkdir(parents=True, exist_ok=True)
(landing / "part-000.csv").write_text("id\\n1\\n")

con = duckdb.connect(a.db)
con.execute("CREATE SCHEMA IF NOT EXISTS raw")
con.execute("CREATE OR REPLACE TABLE raw.calls (k VARCHAR, v VARCHAR)")
con.executemany("INSERT INTO raw.calls VALUES (?, ?)", [
    ("timeline", pathlib.Path(a.timeline).read_text().strip()),
    ("planted", pathlib.Path(a.planted).read_text().strip()),
    ("landing", a.landing),
    ("profile", a.profile),
    ("cwd", os.getcwd()),
    ("world_today", os.environ.get("WORLD_TODAY", "")),
    ("importable", str(__import__("gen_toy").MARK)),
])
con.close()
"""

FAILING_GENERATOR = """
import sys

print("no timeline for that date", file=sys.stderr)
sys.exit(3)
"""


def _toy_world(root: Path, generator_body: str) -> Path:
    """A world with a `generated:` block, its config, and a generator package."""
    world = root / "worlds" / "toy"
    (world / "workspace").mkdir(parents=True)
    (world / "workspace" / "README.md").write_text("the tree the agent gets\n")
    (world / "world.yaml").write_text(
        "name: toy\n"
        "generated:\n"
        "  generator: tools/gen_toy\n"
        "  timeline: timeline.yaml\n"
        "  planted: planted.yaml\n"
    )
    (world / "timeline.yaml").write_text("today: 2026-06-15\n")
    (world / "planted.yaml").write_text("[]\n")

    package = root / "tools" / "gen_toy"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text('MARK = "gen_toy imported from the staging dir"\n')
    (package / "__main__.py").write_text(generator_body)

    task_dir = root / "tasks" / "toy" / "t1"
    task_dir.mkdir(parents=True)
    (task_dir / "task.yaml").write_text("id: t1\nworld: toy\n")
    (task_dir / "prompt.md").write_text("do the thing\n")
    return task_dir


def _spec(root: Path, workdir: Path, db_path: Path) -> dict:
    task = load_task(_toy_world(root, STUB_GENERATOR), {}, root=root)
    return {
        "generated_tar": generated_tar(task),
        # Pairs, not a dict: that is what _runtime_spec_fields puts in the spec.
        "env": [["DUCKDB_PATH", str(db_path)], ["WORLD_TODAY", TODAY]],
    }


def _tree(path: Path) -> set[str]:
    return {str(p.relative_to(path)) for p in path.rglob("*")}


def test_generated_tar_holds_the_generator_and_its_config(tmp_path):
    task = load_task(_toy_world(tmp_path, STUB_GENERATOR), {}, root=tmp_path)
    blob = generated_tar(task)
    assert blob is not None
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tar:
        names = set(tar.getnames())
    assert {"gen_toy", "gen_toy/__main__.py", "timeline.yaml", "planted.yaml"} <= names


def test_a_baked_world_ships_a_sha_instead_of_the_generator(tmp_path, monkeypatch):
    """With generated.baked, the spec carries the expected bake sha and no
    generator tar — the image did the work, and the container asserts the
    two agree. The cache key is untouched by the delivery mode: it reads the
    same config hashes either way."""
    from de_bench.cli import _runtime_spec_fields, _trial_key
    from de_bench.tasks import _cached_tree_sha, load_task

    task_dir = _toy_world(tmp_path, STUB_GENERATOR)
    monkeypatch.setenv("DE_BENCH_ROOT", str(tmp_path))
    _cached_tree_sha.cache_clear()
    plain = load_task(task_dir, {}, root=tmp_path)
    key_plain = _trial_key("pi", "m", "low", plain, 0, "sys")
    assert "generated_tar" in _runtime_spec_fields(plain)

    world_yaml = tmp_path / "worlds" / "toy" / "world.yaml"
    world_yaml.write_text(world_yaml.read_text().replace(
        "generated:\n", "generated:\n  baked: true\n"))
    _cached_tree_sha.cache_clear()
    baked = load_task(task_dir, {}, root=tmp_path)
    fields = _runtime_spec_fields(baked)
    assert "generated_tar" not in fields
    assert len(fields["baked_sha"]) == 64
    assert _trial_key("pi", "m", "low", baked, 0, "sys") == key_plain


def test_a_world_with_no_fixtures_block_ships_no_tar(tmp_path):
    world = tmp_path / "worlds" / "plain"
    (world / "workspace").mkdir(parents=True)
    (world / "world.yaml").write_text("name: plain\n")
    task_dir = tmp_path / "tasks" / "plain" / "t1"
    task_dir.mkdir(parents=True)
    (task_dir / "task.yaml").write_text("id: t1\nworld: plain\n")
    assert generated_tar(load_task(task_dir, {}, root=tmp_path)) is None


def test_setup_generates_the_warehouse_and_leaves_nothing_behind(tmp_path, monkeypatch):
    staging_root = tmp_path / "staging"
    staging_root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(staging_root))

    workdir = tmp_path / "work"
    (workdir / "dags").mkdir(parents=True)
    (workdir / "dags" / "one.py").write_text("# a dag\n")
    db_path = workdir / "database" / "toy.duckdb"
    spec = _spec(tmp_path, workdir, db_path)

    before = _tree(workdir)
    _prepare_workspace_extras(str(workdir), spec)

    con = duckdb.connect(str(db_path))
    calls = dict(con.execute("SELECT k, v FROM raw.calls").fetchall())
    con.close()
    assert calls["timeline"] == "today: 2026-06-15"
    assert calls["planted"] == "[]"
    assert calls["profile"] == "full"
    assert calls["landing"] == str(workdir / "landing")
    assert calls["world_today"] == TODAY
    assert calls["importable"] == "gen_toy imported from the staging dir"

    # The staging directory the generator ran in is gone, and so is every other.
    assert not Path(calls["cwd"]).exists()
    assert list(staging_root.iterdir()) == []

    # Only the warehouse and the landing tree appeared under the workdir.
    # Nothing named after the generator or its config reached the tree the
    # agent will see.
    new = _tree(workdir) - before
    assert new <= {
        "database", "database/toy.duckdb", "database/toy.duckdb.wal",
        "landing", "landing/toy", "landing/toy/dt=2026-06-01",
        "landing/toy/dt=2026-06-01/part-000.csv",
    }
    assert (workdir / "landing" / "toy" / "dt=2026-06-01" / "part-000.csv").exists()
    assert not any("gen_toy" in n or "timeline" in n or "planted" in n for n in _tree(workdir))


def test_the_warehouse_clock_reads_world_today_in_a_later_session(tmp_path, monkeypatch):
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    workdir = tmp_path / "work"
    workdir.mkdir()
    db_path = workdir / "toy.duckdb"
    _prepare_workspace_extras(str(workdir), _spec(tmp_path, workdir, db_path))

    # A fresh connection, as dbt-duckdb or a DAG's own would be.
    con = duckdb.connect(str(db_path))
    today, week_ago, stamp = con.execute(
        "SELECT current_date::VARCHAR, (current_date - 7)::VARCHAR, CAST(now() AS DATE)::VARCHAR"
    ).fetchone()
    con.close()
    assert (today, week_ago, stamp) == (TODAY, "2026-06-08", TODAY)


def test_a_generator_that_fails_fails_the_setup(tmp_path, monkeypatch):
    staging_root = tmp_path / "staging"
    staging_root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(staging_root))

    task = load_task(_toy_world(tmp_path, FAILING_GENERATOR), {}, root=tmp_path)
    workdir = tmp_path / "work"
    workdir.mkdir()
    spec = {
        "generated_tar": generated_tar(task),
        "env": [["DUCKDB_PATH", str(workdir / "toy.duckdb")], ["WORLD_TODAY", TODAY]],
    }
    with pytest.raises(RuntimeError, match="exited 3"):
        _prepare_workspace_extras(str(workdir), spec)
    assert list(staging_root.iterdir()) == []
