"""Disputes opened and closed, every morning at 10:00.

Builds `marts.dispute_daily` and writes the day's dispute position per entity
to `ops.dispute_open_daily`, which is what the collections team reads.

§REV-10 is the rule and it is the one people expect to be otherwise. A disputed
amount **stays recognised** while the dispute is open: recognition does not
pause, and a provision is not netted against revenue. When a dispute closes in
the customer's favour the write-off books on the close date under §REV-5, and
when it closes in Copperline's favour nothing books at all.

A row in `raw.disputes` with `closed_on` NULL is open, whatever its age.

**`marts.dispute_daily` is commerce's mart and it is cut commerce's way.** One
row per dispute status per reason code per day, dated on the day the dispute was
raised, plus one chargeback row a day off the settlement feed. It does not know
which entity an invoice bills through, and the money it holds for a dispute is
`disputed_cents`, the claim, and `dispute_settled_cents`, the amount given up.
There is no `entity`, no `dispute_state` and no `written_off_cents`, and there
was never going to be: the model is `models/commerce/dispute_daily.sql`, it
answers commerce's question, and a dispute is a payment problem until it
settles. So the check names the money the mart holds, and the publish sorts by
the status and the reason code together, which is what names a row of a day.
The same day published twice is then the same file.

**The collections table is a position, not a status.** Each row says what was
open on that day, not what is open this morning, so the day is replayable: a
dispute raised in February and resolved in April is open on every day of March
and on none of May. `dispute_status` is current state and answers a different
question. The population comes off `raised_on` and `resolved_on` instead, and a
dispute closed on a day leaves the open figure on that day and books its
write-off on the same row.

Owned by finance-analytics.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse, workspace_root
from include.lib.notify import notify
from projects.finance.lib import warehouse_checks

DBT_ROOT = workspace_root() / "dbt" / "copperline_analytics"

#: The day this run owns. See `CONVENTIONS.md`.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

TABLE = "marts.dispute_daily"
OPEN_TABLE = "ops.dispute_open_daily"

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("finance", "the dispute mart did not rebuild"),
}


#: The collections table. Created here because nothing else makes it: it is an
#: `ops` table rather than a mart, so no dbt model owns it.
OPEN_DDL = """CREATE TABLE IF NOT EXISTS {table} (
    ds                DATE,
    entity            VARCHAR,
    open_disputes     BIGINT,
    disputed_cents    BIGINT,
    written_off_cents BIGINT
)"""

#: The day's position, per entity.
#:
#: `open` is the point-in-time population: raised on or before the day, and not
#: resolved by the end of it. `disputed_cents` is what those disputes claim, and
#: §REV-10 is clear that a claim never books.
#:
#: The write-off is §REV-10's, and only `settled` is the customer's favour. It
#: books `settled_cents`, the amount given up, on `resolved_on`. `upheld` and
#: `rejected` are Copperline's favour and book nothing, and `disputed_cents` is
#: the amount asked for rather than the amount given up.
#:
#: An entity with no open dispute and no write-off on the day is left out.
POSITION_SQL = """
WITH dispute AS (
    SELECT i.entity_code AS entity,
           d.raised_on,
           d.resolved_on,
           d.dispute_status,
           d.disputed_cents,
           d.settled_cents
    FROM {disputes} d
    JOIN {invoices} i ON i.invoice_id = d.invoice_id
), position AS (
    SELECT entity,
           count(*) FILTER (
               WHERE raised_on <= CAST(? AS DATE)
                 AND (resolved_on IS NULL OR resolved_on > CAST(? AS DATE))
           )::BIGINT AS open_disputes,
           coalesce(sum(disputed_cents) FILTER (
               WHERE raised_on <= CAST(? AS DATE)
                 AND (resolved_on IS NULL OR resolved_on > CAST(? AS DATE))
           ), 0)::BIGINT AS disputed_cents,
           coalesce(sum(settled_cents) FILTER (
               WHERE dispute_status = 'settled'
                 AND resolved_on = CAST(? AS DATE)
           ), 0)::BIGINT AS written_off_cents
    FROM dispute
    GROUP BY entity
)
SELECT entity, open_disputes, disputed_cents, written_off_cents
FROM position
WHERE open_disputes > 0 OR written_off_cents > 0
ORDER BY entity
"""


def open_by_entity(target_ds: str) -> int:
    """Replace the day's dispute position. Returns the rows written."""
    with warehouse.connect() as con:
        con.execute(OPEN_DDL.format(table=warehouse.qualify(OPEN_TABLE)))
        rows = con.execute(
            POSITION_SQL.format(
                disputes=warehouse.qualify("raw.disputes"),
                invoices=warehouse.qualify("raw.invoices"),
            ),
            [target_ds] * 5,
        ).fetchall()
        return warehouse.delete_insert(
            OPEN_TABLE, "ds", target_ds, [(target_ds, *row) for row in rows],
            columns=["ds", "entity", "open_disputes", "disputed_cents",
                     "written_off_cents"],
            con=con,
        )


with DAG(
    dag_id="fin_invoice_dispute_daily",
    schedule="0 10 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "disputes", "mart"],
    doc_md=__doc__,
) as dag:
    build = BashOperator(
        task_id="build",
        bash_command=(
            "/opt/dbt-venv/bin/dbt run --target prod --profiles-dir . "
            "--select marts.dispute_daily "
            "--vars '{\"run_date\": \"" + TARGET_DS + "\"}'"
        ),
        cwd=str(DBT_ROOT),
        pool="warehouse_write",
    )

    check = PythonOperator(
        task_id="check",
        python_callable=warehouse_checks.assert_integer_cents,
        op_kwargs={"table": TABLE,
                   "columns": ["disputed_cents", "dispute_settled_cents"]},
    )

    open_count = PythonOperator(
        task_id="open_count",
        python_callable=open_by_entity,
        op_kwargs={"target_ds": TARGET_DS},
        pool="warehouse_write",
    )

    publish = PythonOperator(
        task_id="publish",
        python_callable=warehouse_checks.publish_partition,
        op_kwargs={"table": TABLE, "partition_col": "ds",
                   "partition_value": TARGET_DS, "name": "dispute_daily",
                   "order_by": ["dispute_status", "reason_code"]},
    )

    build >> check >> open_count >> publish
