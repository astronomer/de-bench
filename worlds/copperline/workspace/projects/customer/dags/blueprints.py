"""Render every `*.dag.yaml` in this folder into an Airflow DAG.

Three of this project's DAGs are a file drop, a rollup and a dbt selection
wired in that order, which is what the blueprint factory is for.
`docs/blueprints.md` is the guide and `include/lib/blueprint/` is the
machinery. Everything else here is hand-written, because its shape is its
own.

A blueprint's `default_args` comes out of YAML, so it can carry a string and
a number and never a Python object. The failure notifier is a Python object,
so it is attached here, once, to everything this folder renders. Without this
loop a rendered DAG would fail quietly and page nobody.
"""

from __future__ import annotations

from pathlib import Path

from include.lib.blueprint import render_all

# The factory pages the owning team for every DAG it renders; see
# include/lib/blueprint/render.py and the `notify` key in docs/blueprints.md.
globals().update(render_all(Path(__file__).parent))
