"""Loader for the scheduler/dataset DAGs."""

from pathlib import Path

from dagfactory import load_yaml_dags

_CONFIG = Path(__file__).parent / "scheduler.yml"

load_yaml_dags(globals_dict=globals(), config_filepath=str(_CONFIG))
