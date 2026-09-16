"""Shared order-enrichment helpers.

Imported by the hourly orders ETL (and other DAGs) as
``from common.order_utils import ...``. Pure functions only — no
Airflow models touched.
"""

import pendulum

# Window used when back-filling enrichment for late-arriving accounts.
_BACKFILL_FLOOR = pendulum.today("UTC").subtract(days=7)


def fetch_orders(region):
    """Return the raw orders the upstream API exposed for the region."""
    return [
        {"order_id": 1, "region": region, "account_id": "acc-1"},
        {"order_id": 2, "region": region, "account_id": "acc-2"},
    ]


def enrich_accounts(rows):
    """Attach a derived tier to each row based on its account id."""
    enriched = []
    for row in rows:
        tier = "gold" if row["account_id"].endswith("1") else "silver"
        enriched.append({**row, "tier": tier})
    return enriched
