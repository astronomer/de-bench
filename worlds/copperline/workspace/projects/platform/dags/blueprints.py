"""Render this team's `*.dag.yaml` files into Airflow DAGs.

Two lines of machinery and no logic. `include/lib/blueprint/` walks each file,
builds one operator per step and wires `depends_on`; `docs/blueprints.md` is
the guide. Every rendered DAG lands in this module's globals, which is where
the parser looks for them.

A YAML error here is an import error, so a broken blueprint shows up as a
broken file in the DAG list rather than as a DAG that quietly went missing.
"""

from __future__ import annotations

from pathlib import Path

from include.lib.blueprint import render_all

globals().update(render_all(Path(__file__).parent))
