"""Reporting pipelines: generated from reporting.yml. Carries
project-wide default args supplied at load time."""

from datetime import datetime
from pathlib import Path

from dagfactory import load_yaml_dags

_CONFIG = Path(__file__).parent / "reporting.yml"

_defaults_config_dict = {
    "default_args": {
        "owner": "data-team",
        "start_date": datetime(2025, 1, 1),
    }
}

load_yaml_dags(
    globals_dict=globals(),
    config_filepath=str(_CONFIG),
    defaults_config_dict=_defaults_config_dict,
)
