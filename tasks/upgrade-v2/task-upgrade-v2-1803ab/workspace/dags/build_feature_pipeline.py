"""Builds the feature-engineering DAG from feature_pipeline.yml."""

from pathlib import Path

from dagfactory import DagFactory

_CONFIG = Path(__file__).parent / "feature_pipeline.yml"

dag_factory = DagFactory(str(_CONFIG))
dag_factory.generate_dags(globals())
