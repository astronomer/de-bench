"""Supply's file names, in one place.

Every path this team reads or waits on is written here rather than in the DAG
that uses it, so a name changes once. `CONVENTIONS.md` states the rule.

`include.lib.warehouse` still owns the warehouse and still writes every
partition. This module only says what things are called.

Two shapes matter and they are not the same shape:

**The landing tree** is what a source sent us. The carriers key their subtree by
carrier rather than by date, the WMS lands two files a night under a dated
directory, and the rate cards arrive as one file a quarter. `landing_dir` from
the house library is the root; the helpers below are the leaves.

**The published partitions** are what we wrote. They live under
`include/data/<layer>/<name>_<ds>.csv`, which is the layout the platform's
readers know, and `warehouse.partition_path` is the one function that builds
them. The globs below match that layout and nothing else — a partition that
lands under another name is a partition nobody reads, and nothing fails.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from include.lib import landing_dir, workspace_root

__all__ = [
    "CARRIERS",
    "RATE_CARD_TEMPLATE",
    "FUEL_TEMPLATE",
    "WMS_MOVEMENTS_TEMPLATE",
    "WMS_SNAPSHOT_TEMPLATE",
    "SHIPPING_COST_GLOB",
    "SELL_THROUGH_GLOB",
    "carrier_packages",
    "carrier_invoices",
    "wms_movements",
    "wms_snapshot",
    "wms_network_snapshot",
    "rate_card",
    "supplier_drop",
    "under_root",
]

#: The three carriers whose feeds we take, by their SCAC codes. The landing
#: subtree is the code in lower case.
CARRIERS = ("BRFR", "PLPC", "MWLG")

#: The same paths, as Airflow templates, for an operator that renders its own
#: source at run time. The functions below parse their date and so cannot be
#: called with `{{ ds }}` at parse; these are what an operator argument takes.
RATE_CARD_TEMPLATE = "landing/carriers/rate_cards/cards_{{ ds }}.csv"
FUEL_TEMPLATE = "landing/carriers/rate_cards/fuel_{{ ds }}.csv"
WMS_MOVEMENTS_TEMPLATE = "landing/wms/dt={{ ds }}/movements.csv"
WMS_SNAPSHOT_TEMPLATE = "landing/wms/dt={{ ds }}/snapshot_aclass.csv"


def carrier_packages(carrier: str, week_start: str | dt.date) -> Path:
    """`landing/carriers/<carrier>/packages_<week>.csv`.

    One file a week per carrier, named for the Sunday the week opens.
    """
    week = _as_date(week_start).isoformat()
    return landing_dir("carriers") / carrier.lower() / f"packages_{week}.csv"


def carrier_invoices(carrier: str) -> Path:
    """`landing/carriers/<carrier>/invoices.csv`.

    One file per carrier, rewritten each month with the periods still in
    retention. It is not dated, so a load takes the whole file and scopes the
    write to the invoice dates in it.
    """
    return landing_dir("carriers") / carrier.lower() / "invoices.csv"


def wms_movements(ds: str | dt.date) -> Path:
    """`landing/wms/dt=<ds>/movements.csv` — the night's movement log."""
    return landing_dir("wms") / f"dt={_as_date(ds)}" / "movements.csv"


def wms_snapshot(ds: str | dt.date) -> Path:
    """`landing/wms/dt=<ds>/snapshot_aclass.csv` — the nightly A-class count."""
    return landing_dir("wms") / f"dt={_as_date(ds)}" / "snapshot_aclass.csv"


def wms_network_snapshot(ds: str | dt.date) -> Path:
    """`landing/wms/dt=<ds>/snapshot_network.csv` — the weekly whole-network
    count the long tail of SKUs gets. Present on one night a week only."""
    return landing_dir("wms") / f"dt={_as_date(ds)}" / "snapshot_network.csv"


def rate_card(ds: str | dt.date) -> Path:
    """`landing/carriers/rate_cards/cards_<ds>.csv` — the card drop.

    The carriers reissue mid-year and the file is named for the day it arrived,
    not for the day the card takes effect. `docs/rate-policy.md` RATE-1 decides
    which version is in force, and it is the authority when the file's own
    `effective_from` disagrees.
    """
    return landing_dir("carriers") / "rate_cards" / f"cards_{_as_date(ds)}.csv"


def supplier_drop() -> Path:
    """The SSIS interface drop, as this side sees it.

    The package writes `supplier_master.csv` to the Windows interface share and
    the share is mounted here. The file is not dated: each night overwrites it,
    which is why a night that writes nothing leaves yesterday's file in place
    and a reader that only checks the file exists sees a green run.
    """
    return landing_dir("suppliers") / "supplier_master.csv"


#: The freight-cost partitions for a day, as a glob a sensor waits on.
#: `sc_freight_accrual_workbook` matches this pattern rather than the task that
#: writes it, because the cost build and the accrual are not wired together.
SHIPPING_COST_GLOB = "include/data/marts/shipping_costs_*_{{ ds }}.csv"

#: The sell-through partitions for a fiscal week, one file per region.
#: `sc_partner_share_kestrel` waits on this before it opens the SFTP session.
#: The DAG runs on a Friday and a fiscal week opens on the Sunday five days
#: before it, which is why the offset is a constant rather than a lookup.
SELL_THROUGH_GLOB = "include/data/marts/sell_through_*_{{ macros.ds_add(ds, -5) }}.csv"


def under_root(pattern: str) -> str:
    """A repository-relative pattern as an absolute one, for a sensor.

    The template inside it is left alone; the operator renders it against the
    run's own context.
    """
    return str(workspace_root() / pattern)


def _as_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
