"""Loader: turns dags.yml into one DAG per top-level entry."""

from pathlib import Path

from dagfactory import DagFactory

_CONFIG = Path(__file__).parent / "dags.yml"

dag_factory = DagFactory(str(_CONFIG))
dag_factory.generate_dags(globals())
