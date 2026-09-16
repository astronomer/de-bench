"""Builds the audience-export DAG from audience_export.yml."""

from pathlib import Path

from dagfactory import DagFactory

_CONFIG = Path(__file__).parent / "audience_export.yml"

dag_factory = DagFactory(str(_CONFIG))
dag_factory.generate_dags(globals())
