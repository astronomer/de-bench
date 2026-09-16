"""Concrete scoring primitives registered with the scoring registry.

Importing this package executes the `@scoring_primitive` decorators and
makes the primitives discoverable by `get_primitive(name)`.

Vendored from airflow-bench (src/airflow_bench/scoring/) at commit
97f7e69a735aac128ace24ddd4ef69d8b14559ed, same commit worlds/upgrade-v2's
tasks were vendored from (see tasks/upgrade-v2/*/task.yaml `provenance: kb`
entries of the earlier upgrade world). `runtime_checks.py` was
dropped: its one primitive, `airflow_dag_loads`, shells out to
airflow-bench's own `airflow-bench/runtime:<X-Y>-base` Docker images via
`adapters._docker_runtime.run_docker_run`, neither of which exist here, and
no upgrade-v2 task's `scoring.metrics` references it.
"""

from de_bench.upgrade_v2_scoring.primitives import authoring as _authoring
from de_bench.upgrade_v2_scoring.primitives import cosmos as _cosmos
from de_bench.upgrade_v2_scoring.primitives import dag_factory as _dag_factory
from de_bench.upgrade_v2_scoring.primitives import refusal as _refusal
from de_bench.upgrade_v2_scoring.primitives import upgrades as _upgrades
from de_bench.upgrade_v2_scoring.primitives import version_awareness as _version_awareness

__all__ = [
    "_authoring",
    "_cosmos",
    "_dag_factory",
    "_refusal",
    "_upgrades",
    "_version_awareness",
]
