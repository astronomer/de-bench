"""Builds the audience-export DAG from audience_export.yml."""

from pathlib import Path

from dagfactory import load_yaml_dags

_CONFIG = Path(__file__).parent / "audience_export.yml"

load_yaml_dags(globals_dict=globals(), config_filepath=str(_CONFIG))
