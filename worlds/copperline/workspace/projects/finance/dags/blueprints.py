"""Render this team's `*.dag.yaml` files into Airflow DAGs.

Five of the seventeen DAGs here are rendered. They are the ones whose shape is
the ordinary one — land a file, build a selection, check the money, write the
partition out — and keeping them as YAML is what stops five files drifting into
five slightly different opinions about the same four steps.

The twelve that are hand-written are hand-written because their shape is their
own. `docs/blueprints.md` says where the line is.
"""

from __future__ import annotations

from pathlib import Path

from include.lib.blueprint import render_all

globals().update(render_all(Path(__file__).parent))
