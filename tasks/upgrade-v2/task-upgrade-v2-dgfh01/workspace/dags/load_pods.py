"""Loader for the K8s pod pipelines."""

from datetime import datetime
from pathlib import Path

from dagfactory import DagFactory

_CONFIG = Path(__file__).parent / "k8s.yml"

_default_args_config_dict = {
    "default_args": {
        "owner": "platform",
        "start_date": datetime(2025, 1, 1),
    }
}

dag_factory = DagFactory(
    str(_CONFIG),
    default_args_config_dict=_default_args_config_dict,
)
dag_factory.generate_dags(globals())
dag_factory.clean_dags(globals())
