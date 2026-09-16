"""Render every `*.dag.yaml` in this folder into an Airflow DAG.

The blueprint factory does the work; this file is the few lines that put the
DAG objects in a module the scheduler parses. `docs/blueprints.md` is the
guide and `include/lib/blueprint/` is the machinery.

A YAML error here is an import error on this file, so a broken blueprint
shows up in the DAG list as one broken file rather than as a DAG that
silently went missing.

A blueprint's `default_args` comes out of YAML, so it can carry a string and a
number and never a Python object. The failure notifier is a Python object, so
it is attached here, once, to everything this folder renders. Without this
loop a rendered DAG would fail quietly and page nobody.
"""

from __future__ import annotations

from pathlib import Path

from include.lib.blueprint import render_all

# The factory pages the owning team for every DAG it renders; see
# include/lib/blueprint/render.py and the `notify` key in docs/blueprints.md.
globals().update(render_all(Path(__file__).parent))
