"""`tools/check_docs.py` keeps the copperline documents layer consistent.

The shipped layer has to pass it, and the checks themselves have to bite —
a consistency tool that cannot fail is furniture.
"""

import importlib.util
from pathlib import Path

import pytest

from de_bench.tasks import repo_root

WORLD = repo_root() / "worlds" / "copperline"


def _load():
    spec = importlib.util.spec_from_file_location(
        "check_docs", repo_root() / "tools" / "check_docs.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


check_docs = _load()


def _world_at(tmp_path: Path, planted: str) -> Path:
    """A world whose workspace is the real one and whose planted.yaml is ours."""
    world = tmp_path / "worlds" / "copperline"
    world.mkdir(parents=True)
    (world / "workspace").symlink_to(WORLD / "workspace")
    (world / "planted.yaml").write_text(planted, encoding="utf-8")
    return tmp_path


def test_shipped_layer_is_consistent():
    assert check_docs.check("copperline") == []


def test_every_clause_id_is_unique_and_counted():
    """The ids do not run without gaps — finance-policy skips REV-15 — so the
    count is declared per file and matched by id."""
    body = (WORLD / "workspace" / "docs" / "finance-policy.md").read_text()
    ids = check_docs._clause_ids(body)
    assert "REV-14" in ids and "REV-16" in ids
    assert "REV-15" not in ids
    assert len(ids) == check_docs.DOCS["docs/finance-policy.md"]


def test_every_consumer_has_a_contract_and_a_schema():
    for consumer, contract in check_docs.CONSUMERS.items():
        assert (WORLD / "workspace" / contract).is_file(), consumer
    for schema in check_docs.SCHEMAS:
        assert (WORLD / "workspace" / schema).is_file(), schema


def test_every_precedence_pair_is_stated():
    """Eighteen pairs of documents could disagree. None is left open."""
    assert len(check_docs.PRECEDENCE) == 18


def test_an_undeclared_document_is_reported(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    (root / "worlds" / "copperline").mkdir(parents=True)
    workspace = root / "worlds" / "copperline" / "workspace"
    workspace.mkdir()
    for top in ("docs", "contracts", "ops"):
        (workspace / top).symlink_to(WORLD / "workspace" / top)
    (root / "worlds" / "copperline" / "planted.yaml").write_text("[]\n")
    monkeypatch.setattr(check_docs, "ROOT", root)
    assert check_docs.check("copperline") == []

    # An extra file under docs/ has to be reported, or the layer grows
    # documents nothing knows about.
    monkeypatch.setitem(check_docs.DOCS, "docs/finance-policy.md", 99)
    fail = check_docs.check("copperline")
    assert any("declares 99 clauses" in line for line in fail)


@pytest.mark.parametrize("clause,expected", [
    ("docs/finance-policy.md#REV-6", None),
    ("docs/finance-policy.md#REV-15", "and REV-15 is in no document"),
    ("docs/comp-store-policy.md#REV-6", "and REV-6 is in docs/finance-policy.md"),
    ("docs/finance-policy.md", "which is not <document>#<clause>"),
])
def test_a_planted_row_must_cite_a_clause_that_exists(tmp_path, monkeypatch, clause, expected):
    row = (f"- id: e1\n  table: raw.invoice_lines\n  ds: 2026-05-04\n"
           f"  tasks: [FIN-311]\n  clause: {clause}\n")
    monkeypatch.setattr(check_docs, "ROOT", _world_at(tmp_path, row))
    fail = check_docs.check("copperline")
    if expected is None:
        assert not any(line.startswith("2: planted row") for line in fail)
    else:
        assert any(expected in line for line in fail), fail


def test_coverage_is_ready_and_runs_once_rows_exist(tmp_path, monkeypatch):
    """With planted.yaml empty the coverage check stays quiet. The moment a row
    lands it reports every clause that has none."""
    monkeypatch.setattr(check_docs, "ROOT", _world_at(tmp_path, "[]\n"))
    assert not any("has no planted row" in line for line in check_docs.check("copperline"))

    row = ("- id: e1\n  table: raw.invoice_lines\n  ds: 2026-05-04\n"
           "  tasks: [FIN-311]\n  clause: docs/finance-policy.md#REV-2\n")
    monkeypatch.setattr(check_docs, "ROOT", _world_at(tmp_path / "armed", row))
    fail = check_docs.check("copperline")
    uncovered = [line for line in fail if "has no planted row" in line]
    assert len(uncovered) == 127  # every clause but REV-2
    assert not any("REV-2 has no planted row" in line for line in fail)
