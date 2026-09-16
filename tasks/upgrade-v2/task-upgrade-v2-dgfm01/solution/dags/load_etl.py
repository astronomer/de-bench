"""ETL pipelines: generated from etl.yml."""

from pathlib import Path

from dagfactory import load_yaml_dags

_CONFIG = Path(__file__).parent / "etl.yml"

load_yaml_dags(globals_dict=globals(), config_filepath=str(_CONFIG))
