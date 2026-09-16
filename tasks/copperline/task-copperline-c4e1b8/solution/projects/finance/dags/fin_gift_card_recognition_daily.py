"""Build `marts.gift_card_recognition`: what the card book recognized in a day.

Redemptions, breakage, and the value that escheats instead of breaking, at
entity and card-currency grain. The close's gift-card line comes off the first
two and treasury's escheat filing off the third, so the two of them argue about
the definition once instead of about the number every month. FIN-455.

`docs/finance-policy.md` §REV-12 decides all of it: which entries recognize,
which day a breakage lands on, which jurisdictions escheat instead, and which
rate and which entity every amount on a card carries. The module docstring on
`projects/finance/lib/gift_cards.py` says what each of those means here.

**A run owns one recognition day.** The redemptions are that day's. The
breakage is not: REV-12 dates it to the last day of the fiscal month the card
ages out, so the run that owns a fiscal month end reaches back over the month
and every other run of that month writes no breakage at all.

Owned by finance-analytics. When this stops, the close's gift-card line goes
back to the spreadsheet.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib.notify import notify
from projects.finance.lib import gift_cards

#: The day this run owns. A bare cron string is a trigger timetable, so `ds` is
#: the day the run fires and the day being built is the one before it —
#: `CONVENTIONS.md`, and `docs/late-data-policy.md` §LD-3.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("finance", "gift-card recognition did not build"),
}

with DAG(
    dag_id="fin_gift_card_recognition_daily",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "mart", "gift-cards"],
    doc_md=__doc__,
) as dag:

    recognize = PythonOperator(
        task_id="recognize",
        python_callable=gift_cards.recognize,
        op_kwargs={"ds": TARGET_DS},
        pool="warehouse_write",
        doc_md="Rebuild the recognition day whole, in integer cents. The write "
               "is a delete-insert on the day, so a second run of the same day "
               "leaves one copy and no other day is touched.",
    )

    tie_cards = PythonOperator(
        task_id="tie_cards",
        python_callable=gift_cards.tie_cards,
        op_kwargs={"ds": TARGET_DS},
        doc_md="The day's rows account for every ledger entry the day "
               "recognizes. An escheat filed as revenue is quiet otherwise: the "
               "row is there and the total still looks like a day.",
    )

    recognize >> tie_cards
