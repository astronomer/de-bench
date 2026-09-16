"""The blueprint loader for commerce.

Renders every `projects/commerce/dags/*.dag.yaml` into a DAG and puts each one
in this module's globals, where the parser finds it. `docs/blueprints.md` is the
guide and `include/lib/blueprint/` is the machinery.

Commerce hand-writes most of its DAGs, because most of them are not the shape
the factory renders. The five here are: they land a file, run a statement or
two, and publish. A DAG that would need a sixth blueprint kind to express is a
DAG better written in Python.
"""

from __future__ import annotations

from pathlib import Path

from include.lib.blueprint import render_all

globals().update(render_all(Path(__file__).parent))
