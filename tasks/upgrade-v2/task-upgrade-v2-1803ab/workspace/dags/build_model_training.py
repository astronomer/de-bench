"""Builds the model-training DAG from model_training.yml."""

from pathlib import Path

from dagfactory import DagFactory

_CONFIG = Path(__file__).parent / "model_training.yml"

dag_factory = DagFactory(config_filepath=str(_CONFIG))
dag_factory.generate_dags(globals())
