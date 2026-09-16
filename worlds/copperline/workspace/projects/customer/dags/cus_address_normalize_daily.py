"""Normalise and key the addresses on both account books.

Addresses arrive as free text from three places — Halyard, the acquired book,
and whatever the order carried — and no two of them punctuate a street the
same way. This normalises the parts, then keys them, so that two records of
one place get one key.

**The key's columns are ordered, and the order is part of the key.** Country,
region, postal code, locality, street line, unit. Two models that list the
same columns in a different order produce two different keys for one address
and nothing says which is which. `CONVENTIONS.md` fixes the rule and
`projects/customer/lib/addresses.py` fixes this dimension's order.

**Empty is not the same as absent, but the key cannot tell.** A missing unit
number and an empty one hash to the same key, so a normalised part is written
as an empty string rather than left null and the difference is carried in
`parts_present` beside the key.

There is no location dimension yet. `marts.dim_location` is declared and
unbuilt, and this DAG is what it would be built from.

Owned by customer.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag

from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.customer.lib import addresses

DEFAULT_ARGS = {
    "owner": "customer",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("customer", "address normalisation failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="cus_address_normalize_daily",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["customer", "identity", "address"],
    doc_md=__doc__,
)
def cus_address_normalize_daily():

    @lake_task
    def collect(day: str) -> int:
        """Gather every address on both books into `ops.address_inbox`."""
        return addresses.collect(day)

    @lake_task
    def normalize(day: str) -> int:
        """Case, punctuation, abbreviations and whitespace, part by part."""
        return addresses.normalize(day)

    @lake_task
    def key_addresses(day: str) -> int:
        """Key each normalised address, columns in the fixed order."""
        return addresses.key_addresses(day)

    @lake_task
    def check_keys(day: str) -> dict[str, int]:
        """Every address got a key, and one place has one key.

        Two records of one address that key differently is the failure this
        DAG exists to prevent, so it is checked rather than assumed.
        """
        problems = addresses.key_problems(day)
        if problems:
            raise Reject(f"{len(problems)} address key(s) look wrong",
                         rows=problems)
        return addresses.key_counts(day)

    day = "{{ ds }}"
    collect(day) >> normalize(day) >> key_addresses(day) >> check_keys(day)


cus_address_normalize_daily()
