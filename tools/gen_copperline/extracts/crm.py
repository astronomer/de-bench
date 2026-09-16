"""S8 — the two account books and the decided merges: `raw.customers`,
`raw.nwv_accounts`, `ops.merge_candidates` and `raw.support_tickets`.

`upstream/northwave.py` builds both identity books together, because the
300/90/60 pair populations are only exact if one author writes both sides of
every pair. This module is the extract: it puts the era-shaped id formats,
the delivery bookkeeping and the CRM's own vocabulary on top, and it decides
what reaches the warehouse at all.

**The 60 decoys never reach `ops.merge_candidates`.** `sim_identity.
merge_truth` holds 360 rows; this module writes the 300 whose `kind` starts
`true_`. That absence is load-bearing: it is what makes the flagged table
trustworthy and a fuzzy answer wrong.

## No landing files

Halyard exports through a paginated API, not a dated file (spec 03 section
2). The tables land in `raw` and the stub the ingest DAG calls,
`include/lib/halyard_api.py`, is world source — it belongs to the platform
phase, not to the generator, which never writes anything under
`workspace/`.

## What it reads, and what it falls back to

  `sim_identity.trade_accounts`  the trade book. 4,000 rows, 3,960 active.
  `sim_nwv.accounts`             the acquired book. 1,400 rows, 1,380 active.
  `sim_identity.merge_truth`     the 360 decided rows.
  `sim_customer.customers`       **optional.** The customer module owns
                                 `customer_id` and joins the trade book by
                                 `trade_seq`. Where it has not built, the
                                 `C-######` id comes from `trade_seq` through
                                 `CUSTOMER_ID_BASE`, which is the same
                                 arithmetic that module should use.
  `sim_support.tickets`          Copperline's service desk. It carries a
                                 `customer_id` and no party columns, so this
                                 module derives the armed pair at the
                                 boundary and augments the feed with the
                                 acquired book's tickets, which `sim_support`
                                 does not simulate. Without it both legs are
                                 drawn. See `_support_tickets`.

## Two era columns whose values are drawn, and why

`legacy_id` and `billing_era` both look like they should follow a date, and
neither can. Every trade account was opened before the fixture range starts,
so a rule keyed on `created_on` would put all 4,000 on one side of E2 and
all 4,000 on one side of E5, and both armed populations would be empty.

  * `legacy_id` is held by the `LEGACY_KEYED` accounts that existed under
    the old scheme, by ordinal. S1's `raw.customer_id_map` carries 2,600 of
    those 2,660; the other 60 are the orphans D7 grades, and this module and
    that one have to agree on the same 2,660.
  * `billing_era` is a property of the billing contract, not of the account:
    an account moves to `current` when its contract is re-signed. It is
    drawn, and about 46% of the book has moved.
"""

from __future__ import annotations

import datetime as dt

from ..config import Context
from ..streams import draw, pick, uniform
from ..upstream.logistics import ORDER_ID_BASE, SLOTS_PER_DAY

# `C-######`, the current scheme. The customer module owns `customer_id`;
# this is the arithmetic to use where it has not built.
CUSTOMER_ID_BASE = 400_000

# The accounts that existed under `CUST####`. S1 maps 2,600 of them and
# leaves 60 orphans, which is the population D7 grades.
LEGACY_KEYED = 2_660

BILLING_ERA_CURRENT_PCT = 46

MARKET_BY_COUNTRY = {"US": "US", "CA": "CA", "GB": "GB", "IE": "IE", "DE": "DE"}

PAYMENT_TERMS = {"platinum": "NET60", "gold": "NET45", "standard": "NET30"}

TICKETS = 90_000

# The two vocabularies come from `sim_support`, so a drawn ticket and a real
# one cannot be told apart by the words in them. These are the fallback for a
# build without the support module, and they are its own values.
TICKET_CHANNELS = (("chat", 34), ("email", 31), ("phone", 27), ("social", 8))
TICKET_CATEGORIES = (
    "Delivery - Late Arrival", "Delivery - Damaged in Transit",
    "Delivery - Missing Parcel", "Order - Wrong Item Shipped",
    "Returns - Refund Not Received", "Trade - Invoice Query",
    "Account - Login Problem", "Payment - Declined Card",
)

# Acquired-book tickets a day, once the book landed. Halyard holds the
# Northwave service desk too, and `sim_support` does not simulate it.
NWV_TICKETS_PER_DAY = 12
# Tickets that cite an order. An acquired account's orders never reached
# `raw.orders`, so an `nwv` ticket carries no order reference at all, which is
# the other half of what the swapped books hide.
ORDER_TICKET_PCT = 58
ORDER_LOOKBACK_DAYS = 20

# Ticket-id bands, disjoint from each other and from `sim_support`, which
# numbers from 6,600,000.
DRAWN_OMS_BASE = 8_000_000
DRAWN_NWV_BASE = 9_000_000


def _has(ctx: Context, schema: str, table: str) -> bool:
    return bool(ctx.sql(
        "SELECT count(*) FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
    ).fetchone()[0])


def _customers(ctx: Context) -> None:
    """`raw.customers` — trade accounts only.

    Copperline's dedup problem is a trade-account problem, which is where it
    is real. Consumer orders carry an anonymous `loyalty_id` and never enter
    this table.
    """
    s = ctx.seed
    h_era = draw(s, "'crm_billing_era'", "t.trade_seq")
    h_upd = draw(s, "'crm_updated'", "t.trade_seq")
    h_del = draw(s, "'crm_deleted'", "t.trade_seq")
    markets = " ".join(f"WHEN '{c}' THEN '{m}'"
                       for c, m in MARKET_BY_COUNTRY.items())
    terms = " ".join(f"WHEN '{t}' THEN '{code}'"
                     for t, code in PAYMENT_TERMS.items())

    if _has(ctx, "sim_customer", "customers"):
        ids = """
            SELECT t.trade_seq, c.customer_code AS customer_id
            FROM sim_identity.trade_accounts t
            JOIN sim_customer.customers c ON c.trade_seq = t.trade_seq
        """
    else:
        ids = f"""
            SELECT t.trade_seq,
                   'C-' || lpad(({CUSTOMER_ID_BASE} + t.trade_seq)::VARCHAR, 6, '0')
                       AS customer_id
            FROM sim_identity.trade_accounts t
        """
    # The ordinal-to-id map, kept because the merge extract needs it too and
    # `raw.customers` may not carry a column that leads back to the ordinal.
    ctx.sql(f"CREATE OR REPLACE TABLE _util.trade_customer_ids AS {ids}")

    ctx.sql(f"""
        CREATE OR REPLACE TABLE raw.customers AS
        SELECT
            k.customer_id,
            CASE WHEN t.trade_seq <= {LEGACY_KEYED}
                 THEN 'CUST' || lpad(t.trade_seq::VARCHAR, 4, '0') END AS legacy_id,
            t.legal_name                                     AS account_name,
            t.contact_name,
            t.contact_email                                  AS email,
            t.phone,
            t.address_line1,
            t.city,
            t.region_code,
            t.postal_code,
            t.country_code,
            (CASE t.country_code {markets} ELSE 'US' END)    AS market_code,
            t.tax_id,
            t.created_on,
            t.status,
            CASE WHEN ({h_era}) % 100 < {BILLING_ERA_CURRENT_PCT}
                 THEN 'current' ELSE 'legacy' END            AS billing_era,
            t.tier,
            (CASE t.tier {terms} ELSE 'NET30' END)           AS payment_terms_code,
            CASE WHEN t.status = 'deleted'
                 THEN (DATE '{ctx.start}'
                       + INTERVAL 1 DAY * ({uniform(h_del, 30, 800)}))::TIMESTAMP
                 END                                         AS deleted_at,
            (DATE '{ctx.start}'
             + INTERVAL 1 DAY * ({uniform(h_upd, 1, (ctx.end - ctx.start).days)})
            )::TIMESTAMP + INTERVAL 1 MINUTE * (({h_upd}) % 1440) AS updated_at,
            'copperline'                                     AS source_brand
        FROM sim_identity.trade_accounts t
        JOIN _util.trade_customer_ids k ON k.trade_seq = t.trade_seq
        ORDER BY t.trade_seq
    """)


def _nwv_accounts(ctx: Context) -> None:
    """`raw.nwv_accounts` — the one-off load the integration team ran by hand
    at E3. It has not refreshed since, and it carries no load clock because
    there was only ever one load."""
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.nwv_accounts AS
        SELECT nwv_account_id, account_name, primary_contact, contact_email,
               phone, billing_city, billing_state, country, tax_id, opened_on,
               status, owner, legacy_crm_id
        FROM sim_nwv.accounts
        ORDER BY nwv_seq
    """)


def _merge_candidates(ctx: Context) -> None:
    """`ops.merge_candidates` — the 300 decided pairs, and only those."""
    ctx.sql(f"""
        CREATE OR REPLACE TABLE ops.merge_candidates AS
        SELECT c.customer_id,
               m.nwa_id                                      AS nwv_account_id,
               m.confidence,
               m.method,
               m.decided_by,
               m.decided_on,
               m.note
        FROM sim_identity.merge_truth m
        JOIN _util.trade_customer_ids c ON c.trade_seq = m.trade_seq
        WHERE m.is_true_pair
        ORDER BY m.trade_seq
    """)


def _support_tickets(ctx: Context) -> None:
    """`raw.support_tickets` — one row per ticket, against either book.

    `party_ref` and `party_source` are the armed pair, and **both are made
    here**. `sim_support.tickets` carries a `customer_id` and nothing else:
    Halyard's own service desk has one kind of party, because inside Halyard
    there is one kind of party. The two-book shape is a property of the
    extract, which is where the acquired book meets Copperline's, so this
    module derives the pair rather than reading it:

      * a Copperline ticket takes the `C-######` code the customer module
        minted for its `customer_id`, and `party_source = 'oms'`;
      * an acquired-book ticket takes an `NWA-#####` id and
        `party_source = 'nwv'`.

    **The acquired leg is a boundary augmentation.** `sim_support` simulates
    Copperline's service desk alone, so the Northwave tickets do not exist
    upstream and are drawn here, on the days after the book landed, against
    the accounts `sim_nwv` holds. If a Northwave service desk is ever
    simulated, this leg reads it instead and nothing else moves. The
    vocabulary is `sim_support`'s own, so a drawn ticket and a real one
    cannot be told apart by the words in them.

    A consumer's `C-` code resolves against nothing that ships: `raw.customers`
    holds trade accounts only, which spec 03 section 13 states outright. That
    is true of the world and not an accident of this module.
    """
    if _has(ctx, "sim_support", "tickets"):
        _tickets_from_support(ctx)
    else:
        _tickets_drawn(ctx, "_util.tickets_oms", "oms", ctx.start,
                       round(ctx.scale(TICKETS) / _days(ctx)), DRAWN_OMS_BASE)
    book_lands = dt.date.fromisoformat(
        str(ctx.era("E3_northwave_acquisition")["book_lands"]))
    _tickets_drawn(ctx, "_util.tickets_nwv", "nwv", book_lands,
                   ctx.scale(NWV_TICKETS_PER_DAY), DRAWN_NWV_BASE)

    ctx.sql("""
        CREATE OR REPLACE TABLE raw.support_tickets AS
        SELECT ticket_id, party_ref, party_source, opened_at, closed_at,
               channel, category, order_id, csat
        FROM (SELECT * FROM _util.tickets_oms
              UNION ALL SELECT * FROM _util.tickets_nwv)
        ORDER BY opened_at, ticket_id
    """)


def _days(ctx: Context) -> int:
    return (ctx.end - ctx.start).days + 1


def _tickets_from_support(ctx: Context) -> None:
    """`_util.tickets_oms` from the real service desk.

    The party pair is derived: the code comes from the customer module, which
    owns it, and the source is `oms` because every ticket Halyard holds is
    Copperline's. The order reference takes the extract's `ORD-########`
    format, and a ticket that names no order keeps its NULL.
    """
    ctx.sql("""
        CREATE OR REPLACE TABLE _util.tickets_oms AS
        SELECT t.ticket_number                               AS ticket_id,
               c.customer_code                               AS party_ref,
               'oms'                                         AS party_source,
               t.opened_at,
               t.closed_at,
               t.contact_channel                             AS channel,
               cat.name                                      AS category,
               ok.raw_order_id                               AS order_id,
               t.csat_score                                  AS csat
        FROM sim_support.tickets t
        JOIN sim_customer.customers c ON c.customer_id = t.customer_id
        LEFT JOIN _util.order_keys ok ON ok.order_id = t.order_id
        LEFT JOIN sim_support.ticket_categories cat
               ON cat.category_id = t.category_id
    """)


def _tickets_drawn(ctx: Context, table: str, book: str, first_day: dt.date,
                   per_day: int, id_base: int) -> None:
    """One leg of the feed, drawn against a book of accounts.

    Used for the acquired book always, and for Copperline's own where the
    support module has not built.
    """
    s = ctx.seed
    per_day = max(1, per_day)
    h = {name: draw(s, f"'tkt_{book}_{name}'", "d.ds", "g.i")
         for name in ("party", "open", "close", "channel", "category", "csat",
                      "lag", "slot", "order")}

    if book == "nwv":
        pool = ("SELECT (row_number() OVER (ORDER BY nwv_account_id) - 1)::BIGINT"
                " AS i, nwv_account_id AS party_ref FROM raw.nwv_accounts")
    else:
        pool = ("SELECT (row_number() OVER (ORDER BY customer_id) - 1)::BIGINT"
                " AS i, customer_id AS party_ref FROM raw.customers")
    ctx.sql(f"CREATE OR REPLACE TABLE _util.ticket_pool_{book} AS {pool}")
    n = ctx.sql(f"SELECT count(*) FROM _util.ticket_pool_{book}").fetchone()[0]

    if _has(ctx, "sim_support", "ticket_categories"):
        categories = ("SELECT (row_number() OVER (ORDER BY category_id) - 1)"
                      "::BIGINT AS i, name FROM sim_support.ticket_categories "
                      "WHERE parent_category_id IS NOT NULL")
    else:
        categories = " UNION ALL ".join(
            f"SELECT {i}::BIGINT AS i, '{name}' AS name"
            for i, name in enumerate(TICKET_CATEGORIES))
    ctx.sql(f"CREATE OR REPLACE TABLE _util.ticket_categories AS {categories}")
    n_cat = ctx.sql("SELECT count(*) FROM _util.ticket_categories").fetchone()[0]

    # Only Copperline's own orders reached `raw.orders`, so only a ticket
    # against Copperline's book can cite one. The cited reference is the
    # SHIPPED one: derive the simulated key from the day-block formula, then
    # map it through _util.order_keys — spelling it with lpad truncates.
    order_ref = "NULL::VARCHAR"
    if book == "oms":
        order_ref = f"""
            CASE WHEN ({h['order']}) % 100 < {ORDER_TICKET_PCT}
                 THEN (SELECT ok.raw_order_id FROM _util.order_keys ok
                       WHERE ok.order_id = {ORDER_ID_BASE} + o.dn * {SLOTS_PER_DAY}
                             + 1 + ({h['slot']}) % o.orders) END
        """

    ctx.sql(f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT
            'TK-' || ({id_base} + row_number() OVER (
                ORDER BY opened_at, party_ref))::VARCHAR       AS ticket_id,
            party_ref, party_source, opened_at, closed_at, channel, category,
            order_id, csat
        FROM (
            SELECT
                p.party_ref,
                '{book}'                                       AS party_source,
                d.ds::TIMESTAMP + INTERVAL 1 MINUTE * (({h['open']}) % 1440)
                                                               AS opened_at,
                CASE WHEN ({h['close']}) % 100 < 91
                     THEN d.ds::TIMESTAMP
                          + INTERVAL 1 MINUTE * (({h['close']}) % 5760 + 60)
                     END                                       AS closed_at,
                {pick(h['channel'], [(f"'{c}'", w) for c, w in TICKET_CHANNELS])}
                                                               AS channel,
                cat.name                                       AS category,
                {order_ref}                                    AS order_id,
                CASE WHEN ({h['csat']}) % 100 < 63
                     THEN 1 + ({h['csat']} >> 8) % 5 END        AS csat
            FROM _util.days d,
                 LATERAL (SELECT unnest(generate_series(1, {per_day})) AS i) g
            JOIN _util.days o
              ON o.ds = greatest(DATE '{ctx.start}',
                                 d.ds - INTERVAL 1 DAY
                                        * (1 + ({h['lag']}) % {ORDER_LOOKBACK_DAYS}))
            JOIN _util.ticket_pool_{book} p ON p.i = ({h['party']}) % {max(1, n)}
            JOIN _util.ticket_categories cat
              ON cat.i = ({h['category']}) % {max(1, n_cat)}
            WHERE d.ds >= DATE '{first_day}'
        )
    """)


def _check(ctx: Context) -> None:
    total, active, legacy = ctx.sql("""
        SELECT count(*), count(*) FILTER (WHERE status = 'active'),
               count(legacy_id)
        FROM raw.customers
    """).fetchone()
    assert (total, active) == (4_000, 3_960), (total, active)
    assert legacy == LEGACY_KEYED, f"{legacy} legacy ids, not {LEGACY_KEYED}"

    total, active = ctx.sql(
        "SELECT count(*), count(*) FILTER (WHERE status = 'active') "
        "FROM raw.nwv_accounts").fetchone()
    assert (total, active) == (1_400, 1_380), (total, active)

    pairs, customers, accounts = ctx.sql("""
        SELECT count(*), count(DISTINCT customer_id),
               count(DISTINCT nwv_account_id)
        FROM ops.merge_candidates
    """).fetchone()
    assert pairs == 300, f"{pairs} decided pairs, not 300"
    assert (customers, accounts) == (300, 300), (customers, accounts)

    # Both id formats reach `party_ref`, and only `party_source` says which
    # book to look in.
    mixed = dict(ctx.sql(
        "SELECT party_source, count(*) FROM raw.support_tickets GROUP BY 1"
    ).fetchall())
    assert set(mixed) == {"oms", "nwv"}, mixed
    assert min(mixed.values()) > 0, mixed

    shape = ctx.sql("""
        SELECT count(*) FILTER (WHERE party_source = 'oms'
                                  AND party_ref NOT LIKE 'C-%'),
               count(*) FILTER (WHERE party_source = 'nwv'
                                  AND party_ref NOT LIKE 'NWA-%'),
               count(DISTINCT ticket_id), count(*)
        FROM raw.support_tickets
    """).fetchone()
    assert shape[:2] == (0, 0), f"{shape[:2]} ticket(s) hold the wrong id format"
    assert shape[2] == shape[3], "two tickets share an id"

    # Every acquired-book reference resolves. A Copperline one resolves only
    # where the party is a trade account: `raw.customers` is the trade book
    # and a shopper never enters it, which spec 03 section 13 states outright.
    orphan = ctx.sql("""
        SELECT count(*) FROM raw.support_tickets t
        WHERE t.party_source = 'nwv'
          AND t.party_ref NOT IN (SELECT nwv_account_id FROM raw.nwv_accounts)
    """).fetchone()[0]
    assert not orphan, f"{orphan} acquired-book ticket(s) point at no account"

    trade = ctx.sql("""
        SELECT count(*) FROM raw.support_tickets t
        JOIN raw.customers c ON c.customer_id = t.party_ref
        WHERE t.party_source = 'oms'
    """).fetchone()[0]
    assert trade, "no ticket reaches the trade book at all"


def build(ctx: Context) -> None:
    from ._boundary import ensure_keys_or_stub

    ensure_keys_or_stub(ctx)  # ticket order refs come from _util.order_keys
    _customers(ctx)
    _nwv_accounts(ctx)
    _merge_candidates(ctx)
    _support_tickets(ctx)
    _check(ctx)
