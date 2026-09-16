"""The landing layer: every `raw.*` and `ops.*` table, and the dated files
under `landing/`, derived from the sim tables with the delivery mess applied
at this boundary (spec chapter 03). One module per source family; ORDER is
the build order, and everything here runs after the whole of `upstream.ORDER`.

EVERY SHIPPED ORDER REFERENCE COMES FROM `_util.order_keys`. `ORD-########`
is eight digits, the simulated key runs to 181,000,000, and `lpad` truncates
rather than widening — `lpad('121600050', 8, '0')` is `'12160005'` — so
spelling the reference from the key puts ten orders under one string from
2025-05-04 on. `_boundary` renumbers the orders densely in date order instead
and `_util.order_keys` maps one to the other. Call `_boundary.ensure_keys(ctx)`
and join that table on the simulated key.

`OE-#######` is the opposite case and is not renumbered: it stays
`upstream.external.ORDER_REF`, ambiguous on purpose, because that ambiguity
is what NLO-4 grades.
"""

from . import (
    ads,
    ar,
    calendars,
    carriers,
    clickstream,
    crm,
    fx,
    halcyon,
    marketplace,
    meridian,
    northwave_frozen,
    ops_control,
    orders,
    orders_export,
    pim,
    pos,
    promos,
    wms,
)

ORDER = (
    calendars,    # the reference layer; meridian reads raw.fiscal_calendar
    fx,           # raw.fx_rates from sim_core.exchange_rates
    pos,          # S3 — builds raw.stores, which later sources join to;
    #             # reads raw.market_calendar, so after calendars
    meridian,     # S2a: settlements, intents, hourly JSONL landing
    halcyon,      # S2b: decimal-string CSVs, the armed currency NULLs
    marketplace,  # S2c: orders, settlements, payouts
    crm,          # S8: the two account books, tickets with the derived
    #             # party pair, ops.merge_candidates — before anything that
    #             # reads raw.customers (S13's invoices do)
    clickstream,  # S4: raw.web_events + the hourly parquet window
    ads,          # S7: spend with restatements, email events
    pim,          # S9: the product change feed
    # The OMS and AR family. `promos` leaves the per-order gift-card
    # redemption in `_util.gift_card_applied` for `orders`, and `orders`
    # leaves the 45 old-format rows in `_util.e2_late_orders` for the export;
    # nothing else here has an edge.
    promos,        # S12: promotions, applications, gift cards, returns
    orders,        # S1: the order spine, its lines, the crosswalk, statuses
    ar,            # S13: trade invoicing
    orders_export,  # S11: the parquet export and its silent shape change
    wms,          # S6a: movements and snapshots from sim_inventory
    carriers,     # S6b: the package-grain draw; builds raw.lanes itself
    ops_control,  # ops.load_control: the legacy control chain
    # Last, and the only module that writes outside the warehouse file: it
    # compares its own tables against the finished `raw` layer, so every
    # module that owns a shared name has to have run first.
    northwave_frozen,  # the frozen nwv sibling file
)
