"""Flag credentials past their rotation date, on Monday mornings.

Each connection in `config/iac/connections.yaml` carries a `rotates_on` date.
This reads the file, compares each one to the business date, and fails when
something is overdue.

It reads the declared file rather than the deployment, on purpose: the file is
what a person changes, and a credential rotated in the secret store without the
date being moved is a credential nobody can tell has been rotated.

It does not rotate anything. Rotation is a person, a vendor portal and a change
to the secret store, and there is no version of it that should happen without
somebody watching.

Owned by data-platform.
"""

from __future__ import annotations

import datetime as dt

import pendulum
from airflow.sdk import dag

from include.lib import calendar as cal
from include.lib import warehouse, workspace_root
from include.lib.pipeline import lake_task

CONNECTIONS = workspace_root() / "config" / "iac" / "connections.yaml"

#: One row per credential per check, so the drift is visible as a trend.
TABLE = "ops.secret_rotation"

#: How long before the date somebody wants to know. Two of the vendors need a
#: ticket raised with them, and a ticket takes about a fortnight.
WARN_DAYS = 21

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "email": ["data-platform@copperline.example"],
    "email_on_failure": True,
    "email_on_retry": False,
}


@dag(
    dag_id="plat_secret_rotation_check",
    schedule="0 6 * * 1",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["platform", "security", "config"],
    doc_md=__doc__,
)
def plat_secret_rotation_check():
    @lake_task
    def read_declared() -> list[dict]:
        """Every connection that carries a `rotates_on` date.

        A connection with no date is not overdue and is not clean either; it is
        listed with a null date so the count of undated credentials is visible
        beside the count of late ones.
        """
        import yaml

        document = yaml.safe_load(CONNECTIONS.read_text(encoding="utf-8")) or {}
        connections = document.get("connections") or {}
        return [
            {"conn_id": conn_id,
             "rotates_on": str(spec["rotates_on"]) if spec.get("rotates_on") else None,
             "host": spec.get("host")}
            for conn_id, spec in sorted(connections.items())
            if spec.get("password")
        ]

    @lake_task
    def classify(declared: list[dict]) -> list[dict]:
        """Overdue, due soon, or fine, against the business date."""
        today = cal.today()
        classified = []
        for connection in declared:
            if not connection["rotates_on"]:
                state, days = "undated", None
            else:
                due = dt.date.fromisoformat(connection["rotates_on"])
                days = (due - today).days
                state = "overdue" if days < 0 else (
                    "due_soon" if days <= WARN_DAYS else "ok")
            classified.append({**connection, "state": state, "days_left": days})
        return classified

    @lake_task
    def record_and_flag(classified: list[dict], target_ds: str) -> int:
        """Write the week's answer, then fail if anything is overdue.

        The write comes first so the record survives the failure. A credential
        that is overdue every Monday for six weeks is a different conversation
        from one that went overdue this morning, and only the record can tell
        them apart.
        """
        rows = [{"ds": target_ds, "conn_id": row["conn_id"],
                 "rotates_on": row["rotates_on"], "state": row["state"],
                 "days_left": row["days_left"]} for row in classified]
        warehouse.delete_insert(
            TABLE, "ds", target_ds, rows,
            columns=["ds", "conn_id", "rotates_on", "state", "days_left"],
        )
        overdue = sorted(row["conn_id"] for row in classified
                         if row["state"] == "overdue")
        if overdue:
            raise RuntimeError("past their rotation date: " + ", ".join(overdue))
        return len(rows)

    record_and_flag(classify(read_declared()), target_ds="{{ ds }}")


plat_secret_rotation_check()
