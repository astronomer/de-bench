"""ETL pipelines: generated from etl.yml."""

from pathlib import Path

from dagfactory import DagFactory

_CONFIG = Path(__file__).parent / "etl.yml"

dag_factory = DagFactory(str(_CONFIG))
dag_factory.generate_dags(globals())
dag_factory.clean_dags(globals())
