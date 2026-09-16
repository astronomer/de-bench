"""Reporting pipelines: generated from reporting.yml. Carries
project-wide default args supplied at load time."""

from datetime import datetime
from pathlib import Path

from dagfactory import DagFactory

_CONFIG = Path(__file__).parent / "reporting.yml"

_default_args_config_dict = {
    "default_args": {
        "owner": "data-team",
        "start_date": datetime(2025, 1, 1),
    }
}

dag_factory = DagFactory(
    str(_CONFIG),
    default_args_config_dict=_default_args_config_dict,
)
dag_factory.generate_dags(globals())
dag_factory.clean_dags(globals())
