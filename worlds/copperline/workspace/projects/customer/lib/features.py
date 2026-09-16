"""Compass features: the as-of join, the trailing windows, and the export.

`contracts/feature-store.md` binds this and two of its clauses shape every
function here.

**§FS-1, values are as of the event time.** Account attributes come from a
slowly-changing dimension, and the version a feature row carries is the one
in force at that row's event time — the version whose validity window
contains it. Filtering the dimension to its current version produces a full
table and better scores and is wrong: a feature built from a value the
account did not yet have will not exist when the model is asked to predict.

**§FS-2, trailing windows end on the day, inclusive.** Seven, thirty and
ninety days. Compass was trained that way, so changing it means retraining.

The 90-day window makes a full rebuild expensive and nobody has made it
incremental. That is an open item in the contract, not a fault here.
"""

from __future__ import annotations

import datetime as dt

from include.lib import warehouse, workspace_root

__all__ = ["WINDOWS", "EXPORT_DIR", "attributes_as_of", "behaviour_windows",
           "as_of_leaks", "feature_counts", "write_export", "contract_breaks"]

#: The trailing windows, in days. Each one ends on the feature day inclusive.
WINDOWS = (7, 30, 90)

#: Where the modelling team pick the files up.
EXPORT_DIR = "exports/features"


def attributes_as_of(ds: str | dt.date) -> int:
    """The dimension version in force at each account's event time.

    The event time is the end of the feature day, because a feature row
    describes what was true when the day closed. The join takes the version
    whose validity window contains that instant — `valid_from <= at` and
    `valid_to` either open or after it.
    """
    day = _as_date(ds)
    dim = warehouse.qualify("marts.dim_customer")
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            WITH at AS (SELECT DATE '{day}' + INTERVAL 1 DAY AS event_time)
            SELECT d.customer_id,
                   d.region_code                  AS region_as_of,
                   d.status                       AS status_as_of,
                   d.tier                         AS tier_as_of,
                   d.valid_from,
                   DATE '{day}'                   AS ds
            FROM {dim} d, at
            WHERE d.valid_from <= at.event_time
              AND (d.valid_to IS NULL OR d.valid_to > at.event_time)
            ORDER BY d.customer_id
            """
        ).fetchall()
        return warehouse.delete_insert(
            "ops.feature_attributes", "ds", day, rows,
            columns=["customer_id", "region_as_of", "status_as_of",
                     "tier_as_of", "valid_from", "ds"],
            con=con,
        )


def behaviour_windows(ds: str | dt.date) -> int:
    """Orders and net sales over each trailing window. Returns the rows.

    Every window ends on the day inclusive, so the seven-day window covers
    the day itself and the six before it.
    """
    day = _as_date(ds)
    resolved = warehouse.qualify("ops.customer_ref_resolved")
    economics = warehouse.qualify("marts.order_economics")
    counts = ", ".join(
        f"""count(DISTINCT e.order_id) FILTER (
                e.order_date > DATE '{day}' - INTERVAL {window - 1} DAY
                AND e.order_date <= DATE '{day}')     AS orders_{window}d"""
        for window in WINDOWS
    )
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            SELECT r.customer_id, {counts},
                   coalesce(sum(e.net_sales_cents) FILTER (
                       e.order_date > DATE '{day}' - INTERVAL 89 DAY
                       AND e.order_date <= DATE '{day}'), 0)::BIGINT
                                                      AS net_sales_cents,
                   DATE '{day}'                       AS ds
            FROM {resolved} r
            JOIN {economics} e ON e.order_id = r.order_id
            GROUP BY r.customer_id ORDER BY r.customer_id
            """
        ).fetchall()
        return warehouse.delete_insert(
            "ops.feature_behaviour", "ds", day, rows,
            columns=["customer_id", *[f"orders_{window}d" for window in WINDOWS],
                     "net_sales_cents", "ds"],
            con=con,
        )


def as_of_leaks(ds: str | dt.date) -> list[dict]:
    """Feature rows carrying an attribute version later than the event time.

    A row that matches only the dimension's current version is a leak from
    the future: the model would be trained on something that had not
    happened. This is the check that catches the shortest way to a full
    table.
    """
    day = _as_date(ds)
    features = warehouse.qualify("marts.feature_customer_daily")
    dim = warehouse.qualify("marts.dim_customer")
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT f.customer_id, f.region_as_of, d.valid_from
            FROM {features} f
            JOIN {dim} d ON d.customer_id = f.customer_id
                        AND d.region_code = f.region_as_of
            WHERE f.ds = DATE '{day}'
              AND d.valid_from > DATE '{day}' + INTERVAL 1 DAY
            ORDER BY f.customer_id
            """
        ).fetchall()
    return [{"customer_id": customer, "region_as_of": region,
             "version_valid_from": str(valid_from)}
            for customer, region, valid_from in rows]


def feature_counts(ds: str | dt.date) -> dict[str, int]:
    """Rows in the day's feature table, and how many have any behaviour."""
    day = _as_date(ds)
    table = warehouse.qualify("marts.feature_customer_daily")
    with warehouse.connect(read_only=True) as con:
        row = con.execute(
            f"SELECT count(*), count(*) FILTER (orders_90d > 0), "
            f"count(DISTINCT customer_id) FROM {table} WHERE ds = DATE '{day}'"
        ).fetchone()
    return {"rows": int(row[0]), "with_orders_90d": int(row[1]),
            "customers": int(row[2])}


def write_export(ds: str | dt.date) -> str:
    """Write `exports/features/<ds>.parquet` and return the path.

    A day is written once and a re-run replaces its file. The write goes to a
    temporary name and is renamed over the target, so the modelling team
    never read half a file.
    """
    day = _as_date(ds)
    table = warehouse.qualify("marts.feature_customer_daily")
    root = workspace_root() / EXPORT_DIR
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{day.isoformat()}.parquet"
    partial = path.with_name(path.name + ".partial")
    with warehouse.connect(read_only=True) as con:
        con.execute(
            f"COPY (SELECT * FROM {table} WHERE ds = DATE '{day}' "
            f"ORDER BY customer_id) TO '{partial}' (FORMAT PARQUET)"
        )
    partial.replace(path)
    return str(path)


def contract_breaks() -> list[str]:
    """The mart against `contracts/feature_customer_daily.yml`."""
    from include.lib import contracts

    return [str(violation) for violation in contracts.check("feature_customer_daily")]


def _as_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
