"""Loader: turns dags.yml into one DAG per top-level entry."""

from pathlib import Path

from dagfactory import load_yaml_dags

_CONFIG = Path(__file__).parent / "dags.yml"

load_yaml_dags(globals_dict=globals(), config_filepath=str(_CONFIG))
