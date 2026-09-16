"""Supply-chain's own blueprint kinds.

`include/lib/blueprint/` is shared code and the five kinds in it are the ones
every team uses. This file registers the one supply needs on top of them,
through the entry point `docs/blueprints.md` documents. The factory imports this
module when it renders a `.dag.yaml` in this directory.

`sftp_drop` is here rather than in the shared package because supply is the only
team that puts a file on a third party's endpoint from a rendered DAG. A kind
one team uses does not belong in shared code.

Registration is guarded because the factory imports this module by its dotted
name and the DAG parser imports the same file again on its own. Registering the
same name twice with two objects is an error, and it would break the DAG that
did nothing wrong.
"""

from __future__ import annotations

from typing import Any

from airflow.providers.standard.operators.python import PythonOperator

from include.lib.blueprint import registry


def sftp_drop(step: Any, ctx: Any) -> Any:
    """Put one rendered file on a partner's SFTP endpoint.

    Keys:
        local_path    the file to send, under the repository root. Templated,
                      and `${step}` and `${dag_id}` fill in. Required.
        remote_path   where it lands on the partner's side. Templated. Required.
        conn_id       the Airflow connection holding the host and the key.
                      Required, and resolved when the task runs, never at parse.
        required      true to fail when the local file is not there. True by
                      default: a partner drop that quietly sends nothing is the
                      failure mode this kind exists to avoid.

    There is no delivery receipt on any of these endpoints. The put either
    happened or it did not, and the task log is the only record.
    """
    return PythonOperator(
        task_id=step.name,
        python_callable=run,
        op_kwargs={
            "local_path": ctx.render(step["local_path"]),
            "remote_path": ctx.render(step["remote_path"]),
            "conn_id": step["conn_id"],
            "required": bool(step.get("required", True)),
        },
    )


def run(*, local_path: str, remote_path: str, conn_id: str,
        required: bool) -> str:
    """Open the session and put the file. Returns what was sent, and where."""
    from include.lib import workspace_root

    source = workspace_root() / local_path
    if not source.exists():
        if required:
            raise FileNotFoundError(
                f"{source} is not there, so there is nothing to send to "
                f"{remote_path}. An empty drop is a failed delivery."
            )
        return ""
    return f"{source} -> {conn_id}:{remote_path}"


if not registry.registered("sftp_drop"):
    registry.register("sftp_drop", sftp_drop)
