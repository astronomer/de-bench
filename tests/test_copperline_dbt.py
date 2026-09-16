"""The copperline dbt project: it parses, it obeys its own six rules, and the
list of tests that fail on the shipped tree is exactly the twelve the answer key
names.

The parse and the rule checks need no warehouse: they read the manifest that
`dbt parse` writes. The run-and-test check needs a built warehouse and is
skipped unless COPPERLINE_DUCKDB points at one — building it takes about ninety
seconds and does not belong in the unit suite. To run it:

    python -m gen_copperline --profile small --db /tmp/cw.duckdb ...
    COPPERLINE_DUCKDB=/tmp/cw.duckdb pytest tests/test_copperline_dbt.py
"""

import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest


def repo_root():
    return Path(__file__).resolve().parents[1]


PROJECT = repo_root() / "worlds/copperline/workspace/dbt/copperline_analytics"
NORTHWAVE = repo_root() / "worlds/copperline/workspace/dbt/northwave_reporting"
NOTES = repo_root() / "worlds/copperline/DBT-NOTES.md"

# The twelve. Four on inventory and WMS staging, three on gift-card staging,
# five reconciliation ties on commerce marts. DBT-NOTES.md splits them nine
# stale against three real and says why for each.
EXPECTED_FAILURES = {
    "relationships_stg_inventory__snapshots_sku__sku__ref_dim_product_",
    "relationships_stg_inventory__wms_movements_sku__sku__ref_dim_product_",
    "not_null_stg_inventory__snapshots_unit_cost_cents",
    "not_null_stg_inventory__wms_movements_from_location",
    "accepted_values_stg_sales__gift_cards_status__active__redeemed__void",
    "accepted_values_stg_sales__gift_card_ledger_entry_type__issue__redeem__reload__adjust",
    "not_null_stg_sales__gift_card_ledger_order_id",
    "tie_channel_daily_store_pos",
    "tie_settlement_processor_overlap",
    "tie_gmv_settlement_reconstructs_order",
    "tie_order_economics_promo_allocation",
    "tie_channel_mix_daily_channel_coverage",
}

# The two rule-2 crossings this world plants on purpose. Both are named in the
# model headers, in dbt/README.md and in DBT-NOTES.md, and both appear as `ref`
# rows in the shipped docs/lineage.md. A third one is a bug.
ALLOWED_CROSS_TEAM_MART_REFS = {
    ("finance", "category_margin", "commerce", "order_economics"),
    ("growth", "channel_roi_daily", "commerce", "gmv_daily"),
}

# Conformed dimensions may be read by any team. Two of them live in a team
# directory because their move to the platform team never finished.
CONFORMED_DIMS = {
    "dim_date", "dim_product", "dim_customer", "dim_store", "dim_warehouse",
    "dim_supplier", "dim_channel", "dim_promotion", "dim_carrier",
    "dim_geography", "dim_account", "dim_cost_center", "dim_currency",
}

TEAMS = {"commerce", "growth", "supply", "finance", "customer"}


def dbt_executable():
    """The pinned dbt, if this machine has one. The world runs dbt from its own
    virtualenv at /opt/dbt-venv because dbt-core 1.6 and Airflow 3 cannot share
    an environment; any 1.6.x install will do for parsing."""
    for candidate in (
        os.environ.get("COPPERLINE_DBT"),
        "/opt/dbt-venv/bin/dbt",
        shutil.which("dbt"),
    ):
        if not candidate or not Path(candidate).exists():
            continue
        try:
            out = subprocess.run(
                [candidate, "--version"], capture_output=True, text=True, timeout=120
            ).stdout
        except (OSError, subprocess.SubprocessError):
            continue
        if re.search(r"installed:\s*1\.6\.", out):
            return candidate
    return None


def run_dbt(args, duckdb_path, cwd=PROJECT):
    dbt = dbt_executable()
    env = {
        **os.environ,
        "COPPERLINE_HOME": str(Path(duckdb_path).parent),
        "DUCKDB_PATH": Path(duckdb_path).name,
        "WORLD_TODAY": "2026-06-15",
        "DBT_PROFILES_DIR": str(PROJECT),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    return subprocess.run(
        [dbt, *args], cwd=cwd, env=env, capture_output=True, text=True, timeout=1800
    )


@pytest.fixture(scope="module")
def manifest():
    """Parse the project and hand back its manifest."""
    if dbt_executable() is None:
        pytest.skip("no dbt-core 1.6.x on this machine")
    if not (PROJECT / "dbt_packages" / "dbt_utils").is_dir():
        pytest.skip("dbt deps has not been run for copperline_analytics")

    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "target"
        env_target = {"DBT_TARGET_PATH": str(target), "DBT_LOG_PATH": str(Path(tmp) / "logs")}
        os.environ.update(env_target)
        try:
            result = run_dbt(["parse", "--no-partial-parse"], Path(tmp) / "parse.duckdb")
            assert result.returncode == 0, f"dbt parse failed:\n{result.stdout}\n{result.stderr}"
            path = target / "manifest.json"
            assert path.is_file(), f"dbt parse wrote no manifest:\n{result.stdout}"
            yield json.loads(path.read_text())
        finally:
            for key in env_target:
                os.environ.pop(key, None)


# --------------------------------------------------------------------- shape


def test_project_files_are_present():
    for rel in ("dbt_project.yml", "profiles.yml", "packages.yml", "macros/schemas.sql"):
        assert (PROJECT / rel).is_file(), f"copperline_analytics is missing {rel}"
    assert (PROJECT.parent / "README.md").is_file(), "dbt/README.md is missing"
    assert NOTES.is_file(), "the repo-side answer key is missing"


def test_pins_are_the_ones_the_world_states():
    project = (PROJECT / "dbt_project.yml").read_text()
    assert 'require-dbt-version: [">=1.6.0", "<1.7.0"]' in project
    packages = (PROJECT / "packages.yml").read_text()
    assert "dbt-labs/dbt_utils" in packages
    assert "version: 0.8.6" in packages


def test_profile_holds_no_absolute_path():
    profiles = (PROJECT / "profiles.yml").read_text()
    assert "env_var('DUCKDB_PATH'" in profiles
    for line in profiles.splitlines():
        if line.strip().startswith("path:"):
            assert "/Users/" not in line and not line.split("path:")[1].strip().startswith('"/')


def test_northwave_ships_forty_one_models_and_says_nothing_about_which_live():
    models = sorted(NORTHWAVE.glob("models/**/*.sql"))
    assert len(models) == 41, f"northwave_reporting has {len(models)} models, expected 41"
    # Nothing in the tree may name which three still run. That is world-spec
    # item 1: an orphaned stack whose live subset is not written down anywhere.
    # The README may say that some of it runs — it may not say which.
    names = {p.stem for p in models}
    for path in (NORTHWAVE / "README.md", NORTHWAVE / "dbt_project.yml"):
        body = path.read_text()
        named = sorted(n for n in names if n in body)
        assert not named, f"{path.name} names models: {named}"


# --------------------------------------------------------------------- parse


def test_project_parses(manifest):
    models = [n for n in manifest["nodes"].values() if n["resource_type"] == "model"]
    assert len(models) == 141, f"expected 141 models, parsed {len(models)}"


def test_the_failing_tests_exist_and_are_exactly_twelve(manifest):
    names = {n["name"] for n in manifest["nodes"].values() if n["resource_type"] == "test"}
    missing = EXPECTED_FAILURES - names
    assert not missing, f"the answer key names tests the project does not define: {sorted(missing)}"

    notes = NOTES.read_text()
    named_in_notes = {t for t in EXPECTED_FAILURES if t in notes}
    assert named_in_notes == EXPECTED_FAILURES, (
        f"DBT-NOTES.md does not name every expected failure: "
        f"{sorted(EXPECTED_FAILURES - named_in_notes)}"
    )
    assert len(EXPECTED_FAILURES) == 12


# ----------------------------------------------------------- the six rules


def _model_index(manifest):
    out = {}
    for node in manifest["nodes"].values():
        if node["resource_type"] != "model":
            continue
        parts = Path(node["path"]).parts
        top = parts[0]
        team = "platform" if top == "shared" else top
        if top == "shared":
            layer = {"staging": "stg", "intermediate": "int", "dims": "dim", "ops": "ops"}[parts[1]]
        elif len(parts) > 1 and parts[-2] == "intermediate":
            layer = "int"
        else:
            layer = "mart"
        out[node["unique_id"]] = {
            "name": node["name"],
            "team": team,
            "layer": layer,
            "depends_on": node["depends_on"]["nodes"],
            "path": node["path"],
        }
    return out


def test_rule_1_marts_never_read_raw(manifest):
    models = _model_index(manifest)
    offenders = []
    for node in models.values():
        if node["layer"] not in ("mart", "dim"):
            continue
        if any(dep.startswith("source.") for dep in node["depends_on"]):
            offenders.append(node["name"])
    assert not offenders, f"marts reading a landed table: {sorted(offenders)}"


def test_rule_1b_only_staging_reads_sources(manifest):
    models = _model_index(manifest)
    offenders = [
        n["name"]
        for n in models.values()
        if n["layer"] not in ("stg", "ops")
        and any(dep.startswith("source.") for dep in n["depends_on"])
    ]
    assert not offenders, f"non-staging models reading a source: {sorted(offenders)}"


def test_rule_2_marts_do_not_read_other_teams_marts(manifest):
    models = _model_index(manifest)
    crossings = set()
    for node in models.values():
        if node["layer"] not in ("mart", "dim") or node["team"] == "platform":
            continue
        for dep in node["depends_on"]:
            parent = models.get(dep)
            if parent is None or parent["layer"] not in ("mart", "dim"):
                continue
            if parent["team"] in (node["team"], "platform"):
                continue
            if parent["name"] in CONFORMED_DIMS:
                continue  # a conformed dim is readable by every team
            crossings.add((node["team"], node["name"], parent["team"], parent["name"]))
    assert crossings == ALLOWED_CROSS_TEAM_MART_REFS, (
        f"unexpected cross-team mart reads: {sorted(crossings - ALLOWED_CROSS_TEAM_MART_REFS)}; "
        f"planted ones that vanished: {sorted(ALLOWED_CROSS_TEAM_MART_REFS - crossings)}"
    )


def test_rule_3_no_team_int_model_is_read_across_a_boundary(manifest):
    """A team-local `int` model is that team's own. A definition two teams use
    belongs in models/shared/intermediate/ — that is rule 4."""
    models = _model_index(manifest)
    offenders = []
    for node in models.values():
        for dep in node["depends_on"]:
            parent = models.get(dep)
            if parent is None or parent["layer"] != "int" or parent["team"] == "platform":
                continue
            if parent["team"] != node["team"]:
                offenders.append((node["name"], parent["name"]))
    assert not offenders, f"team-local int models read across a boundary: {sorted(offenders)}"


def test_rule_4_shared_int_models_live_in_the_shared_directory(manifest):
    models = _model_index(manifest)
    shared = {n["name"] for n in models.values() if n["team"] == "platform" and n["layer"] == "int"}
    assert len(shared) == 11, f"expected 11 shared int models, found {len(shared)}"
    for node in models.values():
        if node["layer"] == "int" and node["team"] == "platform":
            assert node["path"].startswith("shared/intermediate/"), node["path"]


def test_rule_5_every_conformed_dimension_is_built_once(manifest):
    models = _model_index(manifest)
    dims = [n for n in models.values() if n["name"].startswith("dim_")]
    names = [n["name"] for n in dims]
    assert len(names) == len(set(names)), f"a dimension is built twice: {names}"
    built = set(names)
    assert built == CONFORMED_DIMS - {"dim_employee"}, (
        f"unexpected dimensions: {sorted(built - CONFORMED_DIMS)}; "
        f"missing: {sorted(CONFORMED_DIMS - {'dim_employee'} - built)}"
    )
    # The two that never finished moving to the platform team.
    by_name = {n["name"]: n for n in dims}
    assert by_name["dim_product"]["team"] == "commerce"
    assert by_name["dim_customer"]["team"] == "customer"


def _strip_comments(sql):
    """Jinja block comments and SQL line comments, gone. The rule checks read
    what runs, not what the author wrote about it."""
    sql = re.sub(r"\{#.*?#\}", " ", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n]*", " ", sql)
    return sql


def test_rule_5b_staging_is_one_to_one_with_its_landed_table():
    """A staging model casts, renames, dedupes and filters. It does not join.

    "Does not join" means it reads one relation: exactly one source and no
    model. A self-join back over the same source — which is how
    stg_payments__meridian_settlements marks a superseded event — reads one
    relation and is not a join in the sense the rule means.
    """
    for path in sorted((PROJECT / "models/shared/staging").glob("stg_*.sql")):
        body = _strip_comments(path.read_text())
        sources = re.findall(r"\{\{\s*source\('([a-z_]+)',\s*'([a-z_]+)'\)", body)
        assert len(set(sources)) == 1, f"{path.name} reads {set(sources)}; staging is 1:1"
        assert "ref(" not in body, f"{path.name} refs a model; staging reads its source only"


def test_every_landed_table_has_exactly_one_staging_model(manifest):
    """One view per landed table, and no landed table read twice."""
    staged = {}
    for node in manifest["nodes"].values():
        if node["resource_type"] != "model" or not node["name"].startswith("stg_"):
            continue
        for dep in node["depends_on"]["nodes"]:
            if dep.startswith("source."):
                staged.setdefault(dep, []).append(node["name"])
    doubled = {k: v for k, v in staged.items() if len(v) > 1}
    assert not doubled, f"a landed table is staged twice: {doubled}"
    # ops_run.dbt_run_events is written by this project's own run hooks, not by
    # a feed. ops_audit_log reads it directly and there is nothing to stage.
    sources = {
        s["unique_id"] for s in manifest["sources"].values() if s["source_name"] != "ops_run"
    }
    unstaged = sources - set(staged)
    assert not unstaged, f"landed tables with no staging model: {sorted(unstaged)}"


def test_rule_6_money_is_an_integer_in_minor_units():
    """No float anywhere near a money column, and every conversion goes through
    macros/money.sql."""
    float_cast = re.compile(r"(::\s*(float|double|real)\b|as\s+(float|double precision|real)\b)", re.I)
    for path in sorted(PROJECT.glob("models/**/*.sql")) + sorted(PROJECT.glob("tests/**/*.sql")):
        body = path.read_text()
        found = float_cast.search(_strip_comments(body))
        assert not found, f"{path.name} casts to a float: {found.group(0)!r}"


def test_rule_6b_converting_facts_carry_the_rate_and_its_date():
    converting = [
        "models/shared/intermediate/int_orders_enriched.sql",
        "models/shared/intermediate/int_gl_postings_unified.sql",
        "models/finance/fct_gl_postings.sql",
        "models/commerce/fct_order.sql",
        "models/finance/fct_invoice_line.sql",
        "models/finance/fct_ar_invoices.sql",
        "models/commerce/order_economics.sql",
        "models/growth/channel_roi_daily.sql",
    ]
    for rel in converting:
        body = (PROJECT / rel).read_text()
        assert "fx_rate_ppm" in body, f"{rel} converts without naming the rate"
        assert "fx_rate_date" in body, f"{rel} converts without naming the rate's date"


def test_no_model_reads_the_wall_clock():
    """The warehouse clock is pinned. A model that reads the wall clock answers
    differently on a replay than it did on the night it ran."""
    banned = re.compile(r"\b(current_date|current_timestamp|now\(\)|today\(\))", re.I)
    for path in sorted(PROJECT.glob("models/**/*.sql")) + sorted(PROJECT.glob("tests/**/*.sql")):
        found = banned.search(_strip_comments(path.read_text()))
        assert not found, f"{path.name} reads the wall clock: {found.group(0)!r}"


# ------------------------------------------------------- the expensive check


@pytest.mark.skipif(
    not os.environ.get("COPPERLINE_DUCKDB"),
    reason="set COPPERLINE_DUCKDB to a built small-profile warehouse to run this",
)
def test_exactly_the_twelve_fail_against_a_built_warehouse(tmp_path):
    if dbt_executable() is None:
        pytest.skip("no dbt-core 1.6.x on this machine")
    warehouse = Path(os.environ["COPPERLINE_DUCKDB"])
    assert warehouse.is_file(), warehouse

    # Build into a copy: the run writes stg, int, marts and the snapshots.
    working = tmp_path / warehouse.name
    shutil.copy(warehouse, working)
    os.environ["DBT_TARGET_PATH"] = str(tmp_path / "target")
    os.environ["DBT_LOG_PATH"] = str(tmp_path / "logs")

    # Snapshots first: dim_product is built from the product snapshot pair and
    # there is nothing to read on a warehouse that has never snapshotted.
    snap = run_dbt(["snapshot"], working)
    assert snap.returncode == 0, f"dbt snapshot failed:\n{snap.stdout[-4000:]}"

    run = run_dbt(["run"], working)
    assert run.returncode == 0, f"dbt run failed:\n{run.stdout[-4000:]}"

    test = run_dbt(["test"], working)

    results = json.loads((tmp_path / "target" / "run_results.json").read_text())
    # A long test name is truncated in the unique_id and a hash is appended, so
    # the name has to come from the manifest rather than off the end of the id.
    built = json.loads((tmp_path / "target" / "manifest.json").read_text())
    name_of = {uid: node["name"] for uid, node in built["nodes"].items()}
    failed = {
        name_of.get(r["unique_id"], r["unique_id"])
        for r in results["results"]
        if r["status"] in ("fail", "error")
    }
    assert failed == EXPECTED_FAILURES, (
        f"unexpected failures: {sorted(failed - EXPECTED_FAILURES)}; "
        f"expected failures that passed: {sorted(EXPECTED_FAILURES - failed)}"
    )
    skipped = [r["unique_id"] for r in results["results"] if r["status"] == "skipped"]
    assert not skipped, f"dbt test skipped {len(skipped)} tests: {skipped[:3]}"
    assert test.returncode != 0, "dbt test should exit non-zero with twelve failures"
