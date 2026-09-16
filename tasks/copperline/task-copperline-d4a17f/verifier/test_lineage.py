"""`docs/lineage.md` after PLAT-468, for the three marts the ticket names.

Nothing here holds an authored verdict. Every verdict is recomputed from the
tree the trial leaves behind, by the rule the ticket states: a reader reads a
mart when the mart's name appears in the reader's own file, in a module of the
reader's own project that the file imports, or in SQL of its own project that
the file names. `include/lib/` is excluded, because the ticket excludes it.

The rule resolves imported symbols rather than whole modules for
`from projects.<team>.lib.<mod> import NAME`. A module of asset constants is
imported by every DAG that touches one of them, so grepping the whole module
would make every growth DAG a reader of every growth asset.

What IS authored is the list of reader names to put the rule to — three to add,
four to take out, six to leave alone. Each one is cross-checked against the tree
before it grades anything, in the first three tests, so a world change that
moves a read fails this file loudly instead of grading a stale answer.

One reader is graded by a second recomputation. `fin_ledger_tie` never spells
the table out: `config/recon.yml` carries a schema, a subject and a grain, and
the DAG assembles the three. The rule above finds nothing, so the test rebuilds
the name from the config file and grades the row against that.

Rows outside the ticket's scope — the dashboard files, the export paths, the
config strings and the contract lists — are graded only on being left where
they were. Readers the audit adds beyond the three (the platform profiler, the
asset republisher, the commerce jobs that both write and read the mart) are
neither asked for nor convicted: they are defensible either way and the ticket
does not settle them.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

WORKDIR = Path(os.environ.get("DE_BENCH_WORKDIR") or "/work")
LINEAGE = WORKDIR / "docs" / "lineage.md"
PROJECTS = WORKDIR / "projects"
MODELS = WORKDIR / "dbt" / "copperline_analytics" / "models"

#: The three marts PLAT-468 puts up.
AUDITED = ("marts.order_economics", "marts.sell_through_daily",
           "marts.feature_customer_daily")

#: Readers the sweep finds and the document misses. Every one is a
#: cross-project read that lives in a lib module rather than in the DAG file,
#: which is why a grep of `projects/*/dags/` alone does not find it.
MUST_ADD = {
    "marts.order_economics": (
        "cus_customer_360_daily",
        "cus_features_daily",
        "gro_experiment_readout_daily",
    ),
}

#: Rows the document claims and the code does not support.
MUST_REMOVE = {
    "marts.order_economics": ("fin_margin_daily",),
    "marts.sell_through_daily": ("sc_shrink_weekly", "sc_supplier_scorecard_weekly"),
    "marts.feature_customer_daily": ("cus_churn_scores_weekly",),
}

#: Rows the code supports, which have to survive the audit.
MUST_KEEP = {
    "marts.order_economics": ("fin_gl_export", "commerce_export_partner"),
    "marts.sell_through_daily": ("sc_partner_share_kestrel",),
    "marts.feature_customer_daily": ("cus_features_daily",),
}

#: dbt models that reference an audited mart, which have to survive too.
MUST_KEEP_MODELS = {"marts.order_economics": ("category_margin",)}

#: Out of scope by the ticket's own words, and graded only on still being
#: there. A token that appears in the row anywhere is enough.
MUST_NOT_TOUCH = {
    "marts.order_economics": ("dashboards/merch",),
    "marts.sell_through_daily": ("dashboards/merch",),
    "marts.feature_customer_daily": ("exports/features", "contracts/privacy.md"),
}

#: The one reader whose name is assembled at run time. Graded against
#: `config/recon.yml` rather than against a grep.
ASSEMBLED = ("fin_ledger_tie", "marts.order_economics")


# --------------------------------------------------------------------------
# the rule


def dag_source(dag_id: str) -> Path | None:
    """The file that declares a DAG, hand-written or blueprint."""
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
    """Where a DAG names a mart, or None."""
    bare = mart.split(".", 1)[1]
    for path, text in reader_text(dag_id):
        if bare in text:
            return path
    return None


def model_refs(model: str, mart: str) -> Path | None:
    """Where a dbt model references a mart, or None."""
    bare = mart.split(".", 1)[1]
    for path in sorted(MODELS.rglob(f"{model}.sql")):
        body = path.read_text(errors="replace")
        if re.search(rf"ref\(\s*['\"]{bare}['\"]\s*\)", body) or f"marts.{bare}" in body:
            return path
    return None


def assembled_subject() -> str:
    """The table `fin_ledger_tie` ties, rebuilt from `config/recon.yml`."""
    config = (WORKDIR / "config" / "recon.yml").read_text(errors="replace")
    keys = {}
    for key in ("schema", "subject", "grain"):
        match = re.search(rf"^{key}:\s*(\S+)\s*$", config, re.M)
        assert match, f"config/recon.yml has no {key} key"
        keys[key] = match.group(1)
    return f"{keys['schema']}.{keys['subject']}_{keys['grain']}"


# --------------------------------------------------------------------------
# the document


def sections() -> dict[str, list[str]]:
    """Each `## \\`marts.x\\`` heading, with the table rows under it."""
    assert LINEAGE.exists(), "docs/lineage.md is gone; it is the checklist"
    mart = None
    out: dict[str, list[str]] = {}
    for line in LINEAGE.read_text(errors="replace").splitlines():
        heading = re.match(r"^#+\s+`?(marts\.[a-z_0-9]+)`?\s*$", line)
        if heading:
            mart = heading.group(1)
            out.setdefault(mart, [])
            continue
        if line.startswith("#"):
            mart = None
            continue
        if mart is None or not line.lstrip().startswith("|"):
            continue
        if set(line.strip()) <= set("|- :"):
            continue
        out[mart].append(line)
    return out


def rows(mart: str) -> list[str]:
    found = sections()
    assert mart in found, f"docs/lineage.md has no section for {mart}"
    return found[mart]


def mentions(mart: str, token: str) -> list[str]:
    """The table rows of a mart's section that name a token."""
    pattern = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(token)}(?![A-Za-z0-9_])")
    return [row for row in rows(mart) if pattern.search(row)]


# --------------------------------------------------------------------------
# the oracle checks itself against the tree before it grades anything


def test_the_tree_still_supports_every_reader_this_audit_adds():
    """Each missing reader really does read the mart, and reads it somewhere a
    grep of the DAG files alone would not find."""
    missing = []
    for mart, readers in MUST_ADD.items():
        for reader in readers:
            where = reads(reader, mart)
            if where is None:
                missing.append(f"{reader} no longer reads {mart}")
            elif where.parent.name == "dags":
                missing.append(f"{reader} now reads {mart} in its own DAG file")
    assert not missing, "; ".join(missing)


def test_the_tree_still_gives_no_support_to_the_rows_this_audit_removes():
    """Each stale row's reader names the mart nowhere the ticket's rule looks."""
    supported = []
    for mart, readers in MUST_REMOVE.items():
        for reader in readers:
            assert dag_source(reader) is not None, f"{reader} is not a DAG in this tree"
            where = reads(reader, mart)
            if where is not None:
                supported.append(f"{reader} now reads {mart} at {where}")
    assert not supported, "; ".join(supported)


def test_the_tree_still_supports_every_row_this_audit_keeps():
    """The true rows, and the one whose table name is assembled from config."""
    unsupported = []
    for mart, readers in MUST_KEEP.items():
        for reader in readers:
            if reads(reader, mart) is None:
                unsupported.append(f"{reader} no longer reads {mart}")
    for mart, models in MUST_KEEP_MODELS.items():
        for model in models:
            if model_refs(model, mart) is None:
                unsupported.append(f"model {model} no longer references {mart}")
    reader, mart = ASSEMBLED
    if reads(reader, mart) is not None:
        unsupported.append(f"{reader} now spells {mart} out; it used to assemble it")
    if assembled_subject() != mart:
        unsupported.append(f"config/recon.yml now assembles {assembled_subject()}, not {mart}")
    assert not unsupported, "; ".join(unsupported)


# --------------------------------------------------------------------------
# the audit


def test_the_rows_the_code_does_not_support_are_gone():
    """The four claims that are not true. `sc_shrink_weekly` values a counted
    delta off the movement feed, `sc_supplier_scorecard_weekly` scores off
    matched receipts, `fin_margin_daily` builds category margin from the two
    shared `int` models its own docstring names, and `cus_churn_scores_weekly`
    builds its features into `ops.churn_features` and reads them back."""
    left = []
    for mart, readers in MUST_REMOVE.items():
        for reader in readers:
            for row in mentions(mart, reader):
                left.append(f"{mart}: {row.strip()[:110]}")
    assert not left, "still listed: " + " | ".join(left)


def test_the_readers_the_sweep_finds_are_listed():
    """The three cross-project reads of `marts.order_economics`. Each one sits
    in a lib module of another team's project, which is why the list has never
    had them: two customer jobs profile order history and behaviour windows off
    the mart, and growth's experiment readout takes `net_sales_cents` from it
    rather than writing a second definition of the number."""
    absent = []
    for mart, readers in MUST_ADD.items():
        for reader in readers:
            if not mentions(mart, reader):
                absent.append(f"{mart}: nothing lists {reader}")
    assert not absent, "; ".join(absent)


def test_every_row_the_code_supports_survived():
    """A purge of everything that does not grep is not an audit. The rows the
    code stands behind stay, and that includes the one reader whose table name
    is three keys in `config/recon.yml` rather than a string in the DAG."""
    dropped = []
    for mart, readers in MUST_KEEP.items():
        for reader in readers:
            if not mentions(mart, reader):
                dropped.append(f"{mart}: {reader} was taken out")
    for mart, models in MUST_KEEP_MODELS.items():
        for model in models:
            if not mentions(mart, model):
                dropped.append(f"{mart}: model {model} was taken out")
    reader, mart = ASSEMBLED
    if not mentions(mart, reader):
        dropped.append(f"{mart}: {reader} was taken out, and it does read the mart")
    assert not dropped, "; ".join(dropped)


def test_the_rows_the_ticket_puts_out_of_scope_were_left_alone():
    """The dashboard files, the export paths and the contract lists. The ticket
    says leave them; a sweep that removes every row it cannot open in the tree
    takes them with it."""
    dropped = []
    for mart, tokens in MUST_NOT_TOUCH.items():
        for token in tokens:
            if not any(token in row for row in rows(mart)):
                dropped.append(f"{mart}: the {token} row is gone")
    assert not dropped, "; ".join(dropped)


def test_every_row_added_names_a_file_that_exists():
    """The ticket asks for a path on every row it adds, and the value of this
    document is the path. A row pointing at a file nobody can open is the state
    the audit was called to fix."""
    wrong = []
    for mart, readers in MUST_ADD.items():
        for reader in readers:
            for row in mentions(mart, reader):
                paths = [
                    candidate
                    for candidate in re.findall(r"`([^`]+)`", row)
                    if "/" in candidate and "{" not in candidate and "*" not in candidate
                ]
                real = [p for p in paths if (WORKDIR / p).exists()
                        or (WORKDIR / "dbt" / "copperline_analytics" / p).exists()]
                if not real:
                    wrong.append(f"{mart}/{reader}: {paths or 'no path'}")
    assert not wrong, "rows naming no file that is in the tree: " + "; ".join(wrong)
