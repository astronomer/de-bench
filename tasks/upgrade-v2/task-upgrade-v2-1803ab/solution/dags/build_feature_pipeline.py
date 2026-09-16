"""Builds the feature-engineering DAG from feature_pipeline.yml."""

from pathlib import Path

from dagfactory import load_yaml_dags

_CONFIG = Path(__file__).parent / "feature_pipeline.yml"

load_yaml_dags(globals_dict=globals(), config_filepath=str(_CONFIG))
