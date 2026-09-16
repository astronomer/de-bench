"""The blueprint loader for supply-chain.

Renders every `projects/supply/dags/*.dag.yaml` into a DAG and puts each one in
this module's globals, where the parser finds it. `docs/blueprints.md` is the
guide and `include/lib/blueprint/` is the machinery.

Five of supply's DAGs are rendered. They are the ones that land a file, group
it and publish it, which is the shape the factory is for. The rest are not:
anything that drives the legacy estate, waits on a name pattern, or maps over
locations with `depends_on_past` on is written in Python, because expressing it
as YAML would be a worse file.
"""

from __future__ import annotations

from pathlib import Path

from include.lib.blueprint import render_all

globals().update(render_all(Path(__file__).parent))
