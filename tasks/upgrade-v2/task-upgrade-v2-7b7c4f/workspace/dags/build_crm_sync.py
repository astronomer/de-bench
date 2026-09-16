"""Builds the CRM-sync DAGs from crm_sync.yml."""

from pathlib import Path

from dagfactory import DagFactory

_CONFIG = Path(__file__).parent / "crm_sync.yml"

dag_factory = DagFactory(config_filepath=str(_CONFIG))
dag_factory.generate_dags(globals())
dag_factory.clean_dags(globals())
