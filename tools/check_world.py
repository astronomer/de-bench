"""Load a world the way the scorer does, and say what parsed.

Every `dag_parses` check asserts the whole DagBag is free of import errors, so one
DAG that fails to import fails every task in the world — not just its own. That
makes this the check worth running before anything is committed.

It unpacks the world's shipped tar rather than reading `worlds/<name>/workspace/`,
because for a variant those are not the same thing: the tar is the parent's tree
with the variant's laid over it, and the tar is what an agent actually gets.

    python tools/check_world.py copperline

Needs Airflow importable. Run it in an environment that has the version the world
declares, or the answer is about your machine rather than about the world.
"""

from __future__ import annotations

import argparse
import io
import os
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

CHILD = '''
import json, os, sys
from pathlib import Path

ws = Path(sys.argv[1])
os.environ.setdefault("AIRFLOW_HOME", sys.argv[2])
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
sys.path.insert(0, str(ws))
os.chdir(ws)

from airflow.models.dagbag import DagBag

# Airflow 3.3 dropped DagBag's include_examples kwarg; the env var above
# already keeps examples out on every version.
try:
    bag = DagBag(str(ws / "projects"), include_examples=False)
except TypeError:
    bag = DagBag(str(ws / "projects"))
print("__RESULT__" + json.dumps({
    "dags": sorted(bag.dags),
    "errors": {str(k): str(v).strip().splitlines()[-1] for k, v in bag.import_errors.items()},
}))
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("world")
    parser.add_argument("--python", default=sys.executable,
                        help="interpreter with Airflow installed (default: this one)")
    parser.add_argument("--airflow-home", default=None,
                        help="an AIRFLOW_HOME with a migrated database. Without one, "
                             "cosmos DAGs report an import error about the database "
                             "rather than about the world")
    args = parser.parse_args()

    sys.path.insert(0, str(ROOT / "src"))
    from de_bench.tasks import load_tasks, load_world, workspace_tar

    world = load_world(args.world)
    task = next((t for t in load_tasks() if t.world.name == world.root_name), None)
    if task is None:
        print(f"no task binds to {world.root_name}", file=sys.stderr)
        return 1
    from dataclasses import replace

    tar_bytes = workspace_tar(replace(task, world=world))

    with tempfile.TemporaryDirectory() as tmp:
        ws = Path(tmp) / "workspace"
        ws.mkdir()
        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:gz") as tar:
            tar.extractall(ws, filter="data")
        script = Path(tmp) / "load.py"
        script.write_text(CHILD, encoding="utf-8")
        env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
        proc = subprocess.run(
            [args.python, str(script), str(ws), args.airflow_home or str(Path(tmp) / "airflow-home")],
            capture_output=True, text=True, env=env,
        )

    marker = [ln for ln in proc.stdout.splitlines() if ln.startswith("__RESULT__")]
    if not marker:
        print(proc.stdout[-4000:], file=sys.stderr)
        print(proc.stderr[-4000:], file=sys.stderr)
        print("the DagBag load produced no result", file=sys.stderr)
        return 1

    import json

    result = json.loads(marker[0][len("__RESULT__"):])
    print(f"{args.world}: {len(result['dags'])} dags")
    for dag_id in result["dags"]:
        print("  ", dag_id)
    if result["errors"]:
        print(f"\n{len(result['errors'])} import error(s):", file=sys.stderr)
        for path, message in result["errors"].items():
            print(f"  {path}\n     {message}", file=sys.stderr)
        return 1
    print("\nno import errors")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
