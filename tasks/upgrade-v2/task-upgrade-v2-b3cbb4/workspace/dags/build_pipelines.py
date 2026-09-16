"""Generates the payments pipelines from pipelines.yml."""

from pathlib import Path

from dagfactory import DagFactory

_CONFIG = Path(__file__).parent / "pipelines.yml"

factory = DagFactory(str(_CONFIG))
factory.generate_dags(globals())
