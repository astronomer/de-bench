"""`STAGING_VIEWS` is a hand-copy of a dbt model, so it can drift.

The views in `tools/gen_copperline_load_fixtures.py` exist because a trial is
scored against the baked warehouse with no dbt run of its own, so a prompt that
names a staging view points at nothing unless the bake creates it (finding 018).
The body is copied from the model rather than compiled — cheap, but it means an
edit to the model silently stops matching the view a check reads.

These tests fail on that drift.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


sys.path.insert(0, str(repo_root() / "tools"))
from gen_copperline_load_fixtures import STAGING_VIEWS  # noqa: E402

MODELS = repo_root() / "worlds/copperline/workspace/dbt/copperline_analytics/models"

#: view name -> the model file it is copied from
SOURCES = {
    "stg.stg_sales__orders": MODELS / "shared/staging/stg_sales__orders.sql",
}


def _select_columns(sql: str) -> list[str]:
    """The output column names of a single-SELECT model, in order.

    Takes the aliased name where there is one (`x as y` -> y), the bare name
    otherwise, and ignores dbt's config block and Jinja comments.
    """
    sql = re.sub(r"\{\{.*?\}\}", "SOURCE", sql, flags=re.DOTALL)
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.DOTALL)
    body = re.search(r"\bselect\b(.*?)\bfrom\b", sql, re.DOTALL | re.IGNORECASE)
    assert body, f"no SELECT ... FROM found in {sql[:120]!r}"
    columns = []
    depth = 0
    current = ""
    for ch in body.group(1):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            columns.append(current)
            current = ""
        else:
            current += ch
    columns.append(current)
    out = []
    for col in columns:
        col = col.strip()
        if not col:
            continue
        alias = re.search(r"\bas\s+([A-Za-z_][A-Za-z0-9_]*)\s*$", col, re.IGNORECASE)
        out.append(alias.group(1) if alias else col.split(".")[-1].strip())
    return out


@pytest.mark.parametrize("view", sorted(SOURCES))
def test_view_matches_its_model_columns(view: str) -> None:
    """The copied view publishes the model's columns, in the model's order."""
    model = SOURCES[view]
    assert model.exists(), f"{model} is gone — update SOURCES in this test"
    assert _select_columns(STAGING_VIEWS[view]) == _select_columns(model.read_text())


def _where_clause(sql: str) -> str:
    """Everything after the model's FROM, normalized to single spaces.

    Jinja comments go first: their prose contains the words this would
    otherwise match on ("...dropped in that view and nowhere else").
    """
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.DOTALL)
    sql = re.sub(r"\{\{.*?\}\}", "SOURCE", sql, flags=re.DOTALL)
    tail = re.search(r"\bfrom\b.*", sql, re.DOTALL | re.IGNORECASE)
    return re.sub(r"\s+", " ", tail.group(0) if tail else "").strip().lower()


@pytest.mark.parametrize("view", sorted(SOURCES))
def test_view_keeps_the_models_filters(view: str) -> None:
    """The WHERE clause is what the view exists for: a staging model that drops
    rows is the only reason reading `raw` directly is not equivalent."""
    model_tail = _where_clause(SOURCES[view].read_text())
    view_tail = _where_clause(STAGING_VIEWS[view])
    # the model reads a dbt source, the view the table it resolves to
    assert model_tail.replace("source", "raw.orders") == view_tail


def test_every_view_declares_a_schema() -> None:
    """`create_staging_views` splits on the first dot to make the schema."""
    for name in STAGING_VIEWS:
        assert "." in name, f"{name} needs a schema prefix"
