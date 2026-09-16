# -*- coding: utf-8 -*-
"""Northwave stock feed.

Reads the branch stock counts off the old AS/400 gateway over ssh, lands them,
and builds the availability table the trade site reads.

t.hallam 2022-03-07
r.iyer   2023-06-19  split the per-region work into a subdag, the flat version
                     was 90 tasks and the tree view was unusable
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.helpers import chain

REGIONS = ["north", "midlands", "south", "west"]

DEFAULT_ARGS = {
    "owner": "northwave-data",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

PARENT_DAG_NAME = "nwv_stock_feed"


def build_region_subdag(parent_dag_name, child_dag_name, args):
    sub = DAG(
        dag_id="%s.%s" % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval="15 3 * * *",
    )
    for region in REGIONS:
        SSHOperator(
            task_id="pull_%s" % region,
            ssh_conn_id="as400_gw",
            command=(
                "/qsys/bin/stockdump REGION={region} DATE={{{{ ds_nodash }}}} "
                "> /mnt/share/stock/{region}_{{{{ ds_nodash }}}}.txt"
            ).format(region=region),
            dag=sub,
        )
    return sub


dag = DAG(
    PARENT_DAG_NAME,
    default_args=DEFAULT_ARGS,
    schedule_interval="15 3 * * *",
    catchup=False,
    max_active_runs=1,
)

start = DummyOperator(task_id="start", dag=dag)

pull_regions = SubDagOperator(
    task_id="pull_regions",
    subdag=build_region_subdag(PARENT_DAG_NAME, "pull_regions", DEFAULT_ARGS),
    dag=dag,
)


def land_counts(**context):
    ds = context["ds"]
    hook = PostgresHook(postgres_conn_id="nwv_dw")
    for region in REGIONS:
        path = "/mnt/share/stock/%s_%s.txt" % (region, ds.replace("-", ""))
        # the gateway pads the sku to 15 with spaces. it has done that since
        # 2009 and it will keep doing it. strip on the way in.
        hook.copy_expert(
            "COPY nwv_stage.stock_counts FROM STDIN WITH (FORMAT text)", path
        )
    hook.run("UPDATE nwv_stage.stock_counts SET sku = trim(sku)")


land = PythonOperator(
    task_id="land_counts",
    python_callable=land_counts,
    provide_context=True,
    dag=dag,
)


def build_availability(**context):
    ds = context["ds"]
    hook = PostgresHook(postgres_conn_id="nwv_dw")
    hook.run(
        """
        DELETE FROM nwv_dw.availability WHERE count_date = '%s';
        INSERT INTO nwv_dw.availability (count_date, branch_no, sku, on_hand)
        SELECT '%s', branch_no, sku, sum(qty)
          FROM nwv_stage.stock_counts
         GROUP BY branch_no, sku;
        """
        % (ds, ds)
    )


availability = PythonOperator(
    task_id="build_availability",
    python_callable=build_availability,
    provide_context=True,
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

chain(start, pull_regions, land, availability, end)
