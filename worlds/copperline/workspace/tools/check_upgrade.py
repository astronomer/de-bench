#!/usr/bin/env python3
"""The upgrade pre-flight. Run it before moving any pinned package.

Walks every DAG file under `projects/*/dags/` and compares what it imports
against the symbols the target versions removed. `docs/runbooks/upgrade.md`
says when to run it and what to do with a finding.

Usage:
    python tools/check_upgrade.py            # check the whole tree
    python tools/check_upgrade.py --project finance
"""
from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

#: Import paths removed at or before the versions the platform pins. A DAG
#: importing one of these does not survive the upgrade. Maintained by hand
#: against the release notes; last reviewed 2026-03-30.
REMOVED_IMPORTS = {
    # Gone in Airflow 3: the 2.x module layout and the context globals.
    "airflow.operators.python_operator",
    "airflow.operators.bash_operator",
    "airflow.operators.dummy",
    "airflow.operators.dummy_operator",
    "airflow.operators.subdag",
    "airflow.operators.subdag_operator",
    "airflow.contrib",
    "airflow.hooks.base_hook",
    "airflow.hooks.dbapi",
    "airflow.sensors.external_task_sensor",
    "airflow.utils.dates",
    "airflow.www",
    # Gone from dbt's python surface after 1.6.
    "dbt.main",
    "dbt.clients.system",
}


def imports_of(path: Path) -> set[str]:
    """Every dotted module path a file imports, at any depth."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError as exc:
        print(f"  cannot parse {path.relative_to(ROOT)}: {exc}")
        return set()
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module)
    return found


def blocking(found: set[str]) -> set[str]:
    """The imports that match the removed list, by exact name or prefix."""
    hits = set()
    for name in found:
        for removed in REMOVED_IMPORTS:
            if name == removed or name.startswith(removed + "."):
                hits.add(name)
    return hits


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", help="check one team project only")
    args = parser.parse_args()

    projects = ROOT / "projects"
    pattern = f"{args.project}/dags/**/*.py" if args.project else "*/dags/**/*.py"
    files = sorted(projects.glob(pattern))
    if not files:
        print(f"no DAG files under projects/{args.project or '*'}/dags/")
        return 2

    findings = 0
    for path in files:
        hits = blocking(imports_of(path))
        for name in sorted(hits):
            print(f"  BLOCKING {path.relative_to(ROOT)}: imports {name}")
            findings += 1

    print(f"checked {len(files)} DAG file(s)")
    if findings:
        print(f"{findings} blocking issue(s) found")
        return 1
    print("no blocking issues found")
    return 0


if __name__ == "__main__":
    sys.exit(main())
