# -*- coding: utf-8 -*-
# Northwave Supply
# trade account sync - CRM to the warehouse
# t.hallam 2021-11-02
#
# The account numbers are ours (NWA-nnnnnn). Do not translate them here.
# Whoever owns the merge downstream owns the translation.

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.utils.trigger_rule import TriggerRule

DROP_DIR = Variable.get("nwv_crm_drop", default_var="/mnt/share/crm")

args = {
    "owner": "northwave-data",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

dag = DAG(
    "nwv_account_sync",
    default_args=args,
    schedule_interval="@daily",
    catchup=False,
)

wait_for_file = FileSensor(
    task_id="wait_for_crm_drop",
    filepath=DROP_DIR + "/accounts_{{ ds_nodash }}.csv",
    poke_interval=300,
    timeout=60 * 60 * 3,
    mode="poke",
    dag=dag,
)

stage = PostgresOperator(
    task_id="stage_accounts",
    postgres_conn_id="nwv_dw",
    sql="""
        TRUNCATE TABLE nwv_stage.accounts;
        COPY nwv_stage.accounts
          FROM '{{ params.drop }}/accounts_{{ ds_nodash }}.csv'
          WITH (FORMAT csv, HEADER true);
    """,
    params={"drop": DROP_DIR},
    dag=dag,
)


def pick_path(**kwargs):
    """Full reload on the first of the month, otherwise the delta."""
    execution_date = kwargs["execution_date"]
    if execution_date.day == 1:
        return "full_reload"
    return "apply_delta"


branch = BranchPythonOperator(
    task_id="pick_path",
    python_callable=pick_path,
    provide_context=True,
    dag=dag,
)

full_reload = PostgresOperator(
    task_id="full_reload",
    postgres_conn_id="nwv_dw",
    sql="""
        TRUNCATE TABLE nwv_dw.accounts;
        INSERT INTO nwv_dw.accounts SELECT * FROM nwv_stage.accounts;
    """,
    dag=dag,
)

apply_delta = PostgresOperator(
    task_id="apply_delta",
    postgres_conn_id="nwv_dw",
    sql="""
        UPDATE nwv_dw.accounts d
           SET account_name = s.account_name,
               terms_code   = s.terms_code,
               status       = s.status,
               updated_on   = s.updated_on
          FROM nwv_stage.accounts s
         WHERE s.account_no = d.account_no
           AND s.updated_on > d.updated_on;

        INSERT INTO nwv_dw.accounts
        SELECT s.* FROM nwv_stage.accounts s
         WHERE NOT EXISTS (SELECT 1 FROM nwv_dw.accounts d
                            WHERE d.account_no = s.account_no);
    """,
    dag=dag,
)

done = DummyOperator(
    task_id="done",
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)

wait_for_file >> stage >> branch
branch >> full_reload >> done
branch >> apply_delta >> done
