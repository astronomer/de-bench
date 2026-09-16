# -*- coding: utf-8 -*-
"""
Northwave Supply - daily sales rollup
t.hallam, 2021-08-30
last change 2023-02-14 (added the branch dimension)

Runs after the tills settle. Everything here is store local time; the
warehouse is UTC and nobody has fixed that.
"""

from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'northwave-data',
    'depends_on_past': True,
    'start_date': datetime(2021, 9, 1),
    'email': ['data@northwave-supply.example'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'nwv_daily_sales',
    default_args=default_args,
    description='daily sales rollup for the 44 branches',
    schedule_interval='0 2 * * *',
    catchup=True,
    max_active_runs=1,
    concurrency=4,
)

start = DummyOperator(task_id='start', dag=dag)


def extract_tills(**kwargs):
    """Pull the till files the branches drop overnight."""
    ds = kwargs['ds']
    hook = PostgresHook(postgres_conn_id='nwv_dw')
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM nwv_stage.till_lines WHERE business_date = %s", (ds,))
    cur.execute(
        """
        INSERT INTO nwv_stage.till_lines
        SELECT * FROM nwv_src.till_export WHERE business_date = %s
        """,
        (ds,),
    )
    conn.commit()
    print("staged tills for " + ds)


extract = PythonOperator(
    task_id='extract_tills',
    python_callable=extract_tills,
    provide_context=True,
    dag=dag,
)


def rollup(**kwargs):
    ds = kwargs['ds']
    ti = kwargs['ti']
    hook = PostgresHook(postgres_conn_id='nwv_dw')
    # branch dimension came in 2023-02, before that everything rolled to region
    sql = """
        INSERT INTO nwv_dw.sales_daily (business_date, branch_no, dept, net_amount)
        SELECT business_date, branch_no, dept, sum(line_amount)
        FROM nwv_stage.till_lines
        WHERE business_date = '{ds}'
        GROUP BY business_date, branch_no, dept
    """.format(ds=ds)
    hook.run(sql)
    rows = hook.get_first(
        "SELECT count(*) FROM nwv_dw.sales_daily WHERE business_date = '%s'" % ds
    )[0]
    ti.xcom_push(key='rows', value=rows)


rollup_task = PythonOperator(
    task_id='rollup_sales',
    python_callable=rollup,
    provide_context=True,
    dag=dag,
)

publish = BashOperator(
    task_id='publish_extract',
    bash_command=(
        'psql -h $NWV_DW_HOST -d nwv -c "\\copy (SELECT * FROM nwv_dw.sales_daily '
        'WHERE business_date = \'{{ ds }}\') TO \'/mnt/share/sales_{{ ds_nodash }}.csv\' CSV HEADER"'
    ),
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag, trigger_rule='all_done')

start.set_downstream(extract)
extract.set_downstream(rollup_task)
rollup_task.set_downstream(publish)
publish.set_downstream(end)
