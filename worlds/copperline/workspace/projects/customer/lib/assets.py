"""The one asset this team schedules on, and the one it publishes.

Customer runs on cron. `cus_360_publish` is the exception, because
`marts.customer_360` is the slowest model in the estate — eight joins over
two years of events — and a clock guess would either wait too long every
morning or publish half a table on a bad one.

Rendered DAGs carry no outlets, so `projects/customer/dags/*.dag.yaml`
publishes nothing here.
"""

from __future__ import annotations

from airflow.sdk import Asset

__all__ = ["CUSTOMER_360", "DIM_CUSTOMER"]

#: The 360 table, published by `cus_customer_360_daily` once it lands.
CUSTOMER_360 = Asset("duckdb://warehouse/marts.customer_360")

#: The conformed customer dimension, built here under a platform contract.
DIM_CUSTOMER = Asset("duckdb://warehouse/marts.dim_customer")
