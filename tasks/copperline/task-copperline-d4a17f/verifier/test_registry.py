"""`docs/report-registry.md` after PLAT-468: §REG-2 made true for three marts.

The registry's `Reads` column claims to be the full read set of each consumer.
This file recomputes it. For every consumer row, it takes the publishers the
row names, applies the same rule `test_lineage.py` applies — the DAG's own file,
the modules of its own project it imports, the SQL of its own project it names —
and asserts the `Reads` cell says every audited mart the publisher touches.

Nothing is authored except the three marts under audit, which the ticket names.
Which consumer moves and what it gains is worked out from the tree, so a world
change that moves a read fails this file rather than grading a stale answer.

Consumers whose publisher reads none of the three are untouched by this ticket
and are not graded here.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

WORKDIR = Path(os.environ.get("DE_BENCH_WORKDIR") or "/work")
REGISTRY = WORKDIR / "docs" / "report-registry.md"
PROJECTS = WORKDIR / "projects"

#: The three marts PLAT-468 puts up. The ticket's only authored input.
AUDITED = ("marts.order_economics", "marts.sell_through_daily",
           "marts.feature_customer_daily")


def dag_source(dag_id: str) -> Path | None:
    for parent in PROJECTS.glob("*/dags"):
        for name in (f"{dag_id}.py", f"{dag_id}.dag.yaml"):
            candidate = parent / name
            if candidate.exists():
                return candidate
    return None


def reader_text(dag_id: str) -> list[tuple[Path, str]]:
    """A DAG's own file and everything of its own project it reaches for."""
    src = dag_source(dag_id)
    if src is None:
        return []
    body = src.read_text(errors="replace")
    out = [(src, body)]
    team = src.relative_to(PROJECTS).parts[0]
    lib = PROJECTS / team / "lib"
    for match in re.finditer(rf"from projects\.{team}\.lib import ([^\n]+)", body):
        for name in match.group(1).replace("(", "").replace(")", "").split(","):
            module = lib / f"{name.strip()}.py"
            if module.exists():
                out.append((module, module.read_text(errors="replace")))
    for match in re.finditer(rf"from projects\.{team}\.lib\.(\w+) import ([^\n]+)", body):
        module = lib / f"{match.group(1)}.py"
        if not module.exists():
            continue
        module_body = module.read_text(errors="replace")
        for name in match.group(2).replace("(", "").replace(")", "").split(","):
            token = re.escape(name.strip())
            for line in module_body.splitlines():
                if re.match(rf"\s*{token}\s*[:=]", line):
                    out.append((module, line))
    sql_dir = PROJECTS / team / "sql"
    if sql_dir.exists():
        for statement in sorted(sql_dir.glob("*.sql")):
            if statement.stem in body:
                out.append((statement, statement.read_text(errors="replace")))
    return out


def reads(dag_id: str, mart: str) -> Path | None:
    bare = mart.split(".", 1)[1]
    for path, text in reader_text(dag_id):
        if bare in text:
            return path
    return None


def consumer_rows() -> dict[str, list[str]]:
    """The registry table, keyed by consumer number, as its cells."""
    assert REGISTRY.exists(), "docs/report-registry.md is gone"
    out: dict[str, list[str]] = {}
    for line in REGISTRY.read_text(errors="replace").splitlines():
        if not line.lstrip().startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) >= 5 and re.fullmatch(r"C-\d+", cells[0]):
            out.setdefault(cells[0], cells)
    return out


def publishers(cells: list[str]) -> list[str]:
    """The DAGs a consumer row names as publishing it."""
    return [name for name in re.findall(r"`([A-Za-z0-9_]+)`", cells[3])
            if dag_source(name) is not None]


def expected() -> dict[str, set[str]]:
    """Per consumer, the audited marts its publishers read. Recomputed."""
    out: dict[str, set[str]] = {}
    for number, cells in consumer_rows().items():
        found = set()
        for publisher in publishers(cells):
            for mart in AUDITED:
                if reads(publisher, mart) is not None:
                    found.add(mart)
        if found:
            out[number] = found
    return out


def test_the_tree_still_puts_an_unlisted_mart_on_a_consumer():
    """The oracle checks itself. If no consumer's publisher reads an audited
    mart any more, there is nothing to grade and this file is stale."""
    assert expected(), "no registry consumer's publisher reads any audited mart"


def test_every_consumer_reads_what_its_publisher_reads():
    """§REG-2 for the three marts. A consumer whose publisher touches one of
    them says so in its `Reads` cell.

    Two consumers move. Both are customer jobs that take order history off
    `marts.order_economics` through a module in their own project rather than
    in the DAG file, which is why nobody has ever written it down."""
    rows = consumer_rows()
    wrong = []
    for number, marts in expected().items():
        cell = rows[number][4]
        for mart in sorted(marts):
            bare = mart.split(".", 1)[1]
            if bare not in cell:
                wrong.append(f"{number} reads {mart} and does not say so")
    assert not wrong, "; ".join(wrong)
