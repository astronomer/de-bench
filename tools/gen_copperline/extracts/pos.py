"""S3 — the Northgate store POS batches (spec chapter 03 section 8).

Four tables and one landing tree. Everything here hangs off two facts about
the register software, and both are eras rather than defects:

**E1, to 2025-11-02.** The gen-2 registers do not post transactions as they
happen. They hold the day and post one close batch at 23:05, stamped in the
store's own local wall clock, with no offset recorded and no UTC column
populated. Reading `event_time_local` as UTC pushes the whole close batch of
every Americas store into the next day, and leaves the GB, IE and DE stores
right, because 23:05 at UTC+0 or UTC+1 is still the same UTC day. 166 of the
268 stores sit in Americas zones, so the error is region-shaped: a fleet-wide
count is 62% wrong while per-store-day counts are wrong for two thirds of the
estate and right for the rest.

**E6, from 2025-11-03.** The registers send per-transaction times and
`event_time_utc` is populated from the store's zone. `event_time_local` stays
on the row for the store teams, so the column's meaning changes underneath a
reader who only knows its name.

**The two armed fall-backs.** `raw.pos_batch_manifest.stamped_at` is written
by the same store server, so it carries the same local clock through E1. The
server writes the file some time after the 23:05 close, which puts a share of
the stamps in the small hours of the next day — and on 2024-11-03 and
2025-11-02 the Americas clocks run 01:00 to 01:59 twice. Two stores in one
zone can therefore carry the identical `stamped_at` string an hour apart in
real time, which is what `received_at` records. America/Phoenix never moves,
so its stores are never ambiguous: the fix that applies one rule to the whole
estate is wrong for them too.

**What the batch lag is a lag on.** `late_tail.store_card` is `d0: 1.0` — no
row is ever late. The *file* can be up to three days late, and when it comes
every row inside arrives at once. A lookback tuned on row lag and a lookback
tuned on file arrival are different numbers (`docs/runbooks/pos-ingestion.md`
POS-1).

**The slab.** `raw.pos_sales_daily` is store x day summary for the whole of
FY2023, 371 days by the 191 stores Copperline held then: 70,861 rows. It is
not built from the transaction tables and never can be — the line detail aged
out under `docs/retention-policy.md` RET-2 long before anyone wanted it. It
exists so the 53-week comp has a prior year to land on. It does not scale
with the profile: a comp answer has to exist at every profile.
"""

from __future__ import annotations

import datetime as dt
from collections import defaultdict

from .. import streams
from ..config import Context
from . import _landing

# The whole of FY2023, the year before the fact range opens.
SLAB_START = dt.date(2023, 1, 29)
SLAB_END = dt.date(2024, 2, 3)
SLAB_STORES = range(101, 292)          # the 191 stores held through FY2023

# Delivery incidents over the manifest, spec chapter 03 section 8. These are
# store-day counts, and store-days do not scale, so neither do these.
LATE_BATCHES = 6_500
MISSING_BATCHES = 990

# How long the store server takes to write the file after the 23:05 close, in
# five-minute marks: 0 to 175 minutes, so a share of the stamps land after
# midnight local.
WRITE_MARKS = 36
MARK_MINUTES = 5

# The marks that fall inside a repeated fall-back hour: 23:05 + 115 minutes
# is 01:00 the next day, 23:05 + 170 is 01:55.
AMBIGUOUS_FIRST_MARK = 23
AMBIGUOUS_MARKS = 12

# The market a store reports into, from its IANA zone. Both are fixed by
# sim_store.ZONES, so the mapping is exact and needs no cross-schema read.
TZ_MARKET = {
    "America/New_York": "US", "America/Chicago": "US", "America/Denver": "US",
    "America/Los_Angeles": "US", "America/Phoenix": "US",
    "America/Toronto": "CA", "America/Vancouver": "CA",
    "Europe/London": "GB", "Europe/Dublin": "IE", "Europe/Berlin": "DE",
}
# Arizona keeps standard time all year, so no fall-back is ambiguous there.
NO_DST_ZONES = ("America/Phoenix",)

# The landing horizon. One file per store per night makes the full 400-day
# RET-2 window 107,200 files; fourteen days plus the key dates is the same
# tree at a size trial setup can pay for. See extracts/_landing.py.
LANDING_DAYS = 14

FILE_COLUMNS = ["pos_txn_id", "register_id", "business_date", "event_time_local",
                "order_id", "customer_ref", "tender_type", "gross_cents",
                "discount_cents", "tax_cents", "net_cents", "return_flag",
                "void_flag"]


def _h(ctx: Context, stream: str, *parts: str) -> str:
    return streams.draw(ctx.seed, f"'{stream}'", *parts)


def _market_case(column: str) -> str:
    arms = " ".join(f"WHEN '{zone}' THEN '{market}'"
                    for zone, market in TZ_MARKET.items())
    return f"(CASE {column} {arms} ELSE 'US' END)"


def _utc(local: str, zone: str) -> str:
    """A store-local stamp as the UTC instant it was. DuckDB resolves an
    ambiguous fall-back stamp to the second occurrence, so the first is that
    answer less an hour — which is what the fold column below picks."""
    return f"(({local}) AT TIME ZONE ({zone})) AT TIME ZONE 'UTC'"


def build(ctx: Context) -> None:
    from ._boundary import ensure_keys_or_stub

    ensure_keys_or_stub(ctx)  # raw order refs come from _util.order_keys, never lpad
    _stores(ctx)
    _slab(ctx)
    _batches(ctx)
    _headers(ctx)
    _manifest(ctx)
    _files(ctx)
    ctx.sql("DROP TABLE IF EXISTS _pos_batch")


# --- raw.stores, the SCD2 store dimension ---------------------------------

def _stores(ctx: Context) -> None:
    """268 current versions and 22 prior ones, 290 rows. The prior versions
    are what makes "region as of the charge date" a different join from
    "region as of now", and they carry the CMP-3 remodel span."""
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.stores (
            store_id VARCHAR NOT NULL,
            store_name VARCHAR NOT NULL,
            store_format VARCHAR NOT NULL,
            region_code VARCHAR NOT NULL,
            market_code VARCHAR NOT NULL,
            tz_name VARCHAR NOT NULL,
            opened_on DATE NOT NULL,
            closed_on DATE,
            acquired_from VARCHAR,
            status VARCHAR NOT NULL,
            valid_from DATE NOT NULL,
            valid_to DATE,
            is_current BOOLEAN NOT NULL,
            PRIMARY KEY (store_id, valid_from)
        )
    """)
    region_code = ("coalesce('D-' || upper(replace(replace(r.name, ' ', '-'), '''', '')), "
                   "'D-' || lpad(v.region_id::VARCHAR, 6, '0'))")
    if not _has(ctx, "sim_core", "regions"):
        region_code = "'D-' || lpad(v.region_id::VARCHAR, 6, '0')"
        join = ""
    else:
        join = "LEFT JOIN sim_core.regions r ON r.region_id = v.region_id"
    ctx.sql(f"""
        INSERT INTO raw.stores
        SELECT s.store_code,
               s.name,
               v.store_format,
               {region_code},
               {_market_case('s.timezone')},
               s.timezone,
               s.open_date,
               s.close_date,
               s.acquired_from,
               v.status,
               v.valid_from,
               v.valid_to,
               v.is_current
        FROM sim_store.store_versions v
        JOIN sim_store.stores s USING (store_id)
        {join}
        ORDER BY s.store_id, v.version_no
    """)


def _has(ctx: Context, schema: str, table: str) -> bool:
    return bool(ctx.sql(
        "SELECT count(*) FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
    ).fetchone()[0])


# --- raw.pos_sales_daily, the FY2023 slab ---------------------------------

def _slab(ctx: Context) -> None:
    """371 days by 191 stores. Summary grain only: no transactions, no line
    items, no orders, no payments — that is the retention policy showing
    through, and the disaster-recovery task depends on it."""
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.pos_sales_daily (
            store_id VARCHAR NOT NULL,
            business_date DATE NOT NULL,
            txn_count INTEGER NOT NULL,
            gross_cents BIGINT NOT NULL,
            discount_cents BIGINT NOT NULL,
            tax_cents BIGINT NOT NULL,
            net_cents BIGINT NOT NULL,
            PRIMARY KEY (store_id, business_date)
        )
    """)
    shape = list(ctx.cfg["volumes"]["weekday_shape"])
    arms = " ".join(f"WHEN {i} THEN {factor}" for i, factor in enumerate(shape))
    draw = _h(ctx, "raw.pos_sales_daily", "d.ds", "s.store_id")
    # FY2023 is the year before the range, so it trades a year of growth
    # below it. Everything else is the same weekly shape the range carries.
    growth = 1 + float(ctx.cfg["volumes"]["annual_growth_pct"]) / 100
    ctx.sql(f"""
        INSERT INTO raw.pos_sales_daily
        WITH b AS (
            SELECT 'S-' || lpad(s.store_id::VARCHAR, 4, '0') AS store_id,
                   d.ds AS business_date,
                   greatest(1, round(7.4 / {growth}
                       * (CASE dayofweek(d.ds) {arms} END)
                       * (0.72 + ({draw} % 57) / 100.0)))::INTEGER AS txn_count,
                   (3200 + ({draw} >> 12) % 9400)::BIGINT AS ticket_cents,
                   (380 + ({draw} >> 28) % 520)::BIGINT AS discount_bps,
                   (600 + ({draw} >> 40) % 320)::BIGINT AS tax_bps
            FROM (SELECT unnest(generate_series(DATE '{SLAB_START}', DATE '{SLAB_END}',
                                                INTERVAL 1 DAY))::DATE AS ds) d
            CROSS JOIN (SELECT unnest(generate_series({SLAB_STORES.start},
                                                      {SLAB_STORES.stop - 1})) AS store_id) s
        ), g AS (
            SELECT *, txn_count * ticket_cents AS gross_cents FROM b
        ), n AS (
            SELECT *, (gross_cents * discount_bps / 10000)::BIGINT AS discount_cents FROM g
        )
        SELECT store_id, business_date, txn_count, gross_cents, discount_cents,
               ((gross_cents - discount_cents) * tax_bps / 10000)::BIGINT AS tax_cents,
               gross_cents - discount_cents
                   + ((gross_cents - discount_cents) * tax_bps / 10000)::BIGINT AS net_cents
        FROM n
        ORDER BY store_id, business_date
    """)


# --- the batch population, which the headers and the manifest both read ----

def _batches(ctx: Context) -> None:
    """One row per store-day a batch was owed on, with its delivery outcome.

    A store owes a batch on a day it traded: after it opened, before it
    closed, outside a remodel span, and on a day its market's feed was
    expected. The late and missing counts are exact, handed out day by day so
    that regenerating one day never moves another's.
    """
    days = (ctx.end - ctx.start).days + 1
    quota = [
        (ctx.start + dt.timedelta(days=n), n,
         MISSING_BATCHES * (n + 1) // days - MISSING_BATCHES * n // days,
         LATE_BATCHES * (n + 1) // days - LATE_BATCHES * n // days)
        for n in range(days)
    ]
    ctx.sql("CREATE OR REPLACE TABLE _pos_quota "
            "(ds DATE, dn INTEGER, n_missing INTEGER, n_late INTEGER)")
    ctx.con.executemany("INSERT INTO _pos_quota VALUES (?,?,?,?)", quota)

    def draw(stream: str) -> str:
        return _h(ctx, stream, "business_date", "store_id")

    e1_end = str(ctx.era("E1_pos_local_stamp")["to"])
    no_dst = ", ".join(f"'{z}'" for z in NO_DST_ZONES)
    fallbacks = ", ".join(
        f"(DATE '{day}', '{market}')"
        for day, which, state, moves in _fallbacks()
        for market in moves
    )

    # The stamp mark. On the armed fall-back nights half the estate's stores
    # are pushed into the repeated hour, so the ambiguity has a population
    # rather than a handful of rows.
    mark = "d_mark"
    normal_mark = f"({mark} % {WRITE_MARKS})"
    armed_mark = f"({AMBIGUOUS_FIRST_MARK} + ({mark} >> 8) % {AMBIGUOUS_MARKS})"

    ctx.sql(f"""
        CREATE OR REPLACE TABLE _pos_batch AS
        WITH owed AS (
            SELECT d.ds AS business_date, d.dn, d.n_missing, d.n_late,
                   v.store_id, s.store_code, s.timezone,
                   {_market_case('s.timezone')} AS market_code
            FROM _pos_quota d
            JOIN sim_store.store_versions v
              ON d.ds >= v.valid_from AND (v.valid_to IS NULL OR d.ds <= v.valid_to)
            JOIN sim_store.stores s USING (store_id)
            WHERE v.status = 'open'
              AND d.ds >= s.open_date
        ), traded AS (
            SELECT o.* FROM owed o
            JOIN raw.market_calendar m
              ON m.market_code = o.market_code AND m.calendar_date = o.business_date
            WHERE m.feed_expected
        ), drawn AS (
            SELECT *,
                   {draw('raw.pos_batch_manifest.rank')} AS d_rank,
                   {draw('raw.pos_batch_manifest.lag')} AS d_lag,
                   {draw('raw.pos_batch_manifest.mark')} AS d_mark,
                   {draw('raw.pos_batch_manifest.fold')} AS d_fold,
                   {draw('raw.pos_batch_manifest.push')} AS d_push,
                   {draw('raw.pos_batch_manifest.partial')} AS d_partial
            FROM traded
        ), ranked AS (
            SELECT *, row_number() OVER (PARTITION BY business_date
                                         ORDER BY d_rank) AS rk
            FROM drawn
        ), outcome AS (
            SELECT *,
                   CASE WHEN rk <= n_missing THEN 'missing'
                        WHEN rk <= n_missing + n_late THEN 'late'
                        ELSE 'ok' END AS status,
                   CASE WHEN rk <= n_missing THEN NULL
                        WHEN rk <= n_missing + n_late THEN 1 + d_lag % 3
                        ELSE 0 END AS lag_days
            FROM ranked
        ), stamped AS (
            SELECT o.*,
                   f.market_code IS NOT NULL
                       AND o.timezone NOT IN ({no_dst}) AS on_armed_fallback,
                   CASE WHEN f.market_code IS NOT NULL
                             AND o.timezone NOT IN ({no_dst})
                             AND (o.d_mark >> 32) % 2 = 0
                        THEN {armed_mark.replace(mark, 'o.d_mark')}
                        ELSE {normal_mark.replace(mark, 'o.d_mark')} END AS write_mark,
                   (o.d_fold >> 4) % 2 AS fold
            FROM outcome o
            LEFT JOIN (SELECT * FROM (VALUES {fallbacks}) AS v(fb_date, market_code)) f
                   ON f.market_code = o.market_code
                  AND f.fb_date = o.business_date + INTERVAL 1 DAY
        ), local_stamp AS (
            SELECT *,
                   business_date::TIMESTAMP + INTERVAL 23 HOUR + INTERVAL 5 MINUTE
                       + INTERVAL {MARK_MINUTES} MINUTE * write_mark AS stamped_local,
                   write_mark >= {AMBIGUOUS_FIRST_MARK}
                       AND write_mark < {AMBIGUOUS_FIRST_MARK + AMBIGUOUS_MARKS}
                       AND on_armed_fallback AS ambiguous
            FROM stamped
        ), instant AS (
            SELECT business_date, dn, store_id, store_code, timezone, market_code,
                   status, lag_days, stamped_local, ambiguous, fold,
                   -- The close batch, in UTC. This is the instant a reader
                   -- who treats the 23:05 local stamp as UTC gets wrong, and
                   -- the error is the store's offset, not a constant.
                   {_utc("business_date::TIMESTAMP + INTERVAL 23 HOUR + INTERVAL 5 MINUTE",
                         "timezone")} AS close_utc,
                   -- The stamp as a real instant. On an ambiguous stamp the
                   -- fold bit picks which of the two it was; DuckDB resolves
                   -- to the second occurrence, so fold 0 is an hour earlier.
                   {_utc("stamped_local", "timezone")}
                       - CASE WHEN ambiguous AND fold = 0
                              THEN INTERVAL 1 HOUR ELSE INTERVAL 0 HOUR END
                       AS stamped_utc,
                   (d_push % 10)::INTEGER AS push_minutes,
                   (d_partial % 1000 < 3) AS partial_file,
                   (1 + (d_partial >> 12) % 3)::INTEGER AS partial_short,
                   business_date <= DATE '{e1_end}' AS in_e1
            FROM local_stamp
        )
        SELECT *,
               CASE WHEN status <> 'missing'
                    THEN stamped_utc + INTERVAL 1 DAY * lag_days
                         + INTERVAL 1 MINUTE * push_minutes END AS received_at
        FROM instant
    """)
    ctx.sql("DROP TABLE _pos_quota")


def _fallbacks() -> list[tuple]:
    """The armed fall-back transitions, from the calendar module's table."""
    from .calendars import DST_TRANSITIONS

    return [row for row in DST_TRANSITIONS if row[2] == "armed"]


# --- raw.pos_sales_header --------------------------------------------------

def _headers(ctx: Context) -> None:
    """One row per POS transaction. A store-day whose batch never arrived has
    no rows at all — that is what `status = 'missing'` means, and treating a
    manifest row with no file as a zero-sales day is the mistake POS-1 warns
    about."""
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.pos_sales_header (
            pos_txn_id VARCHAR NOT NULL PRIMARY KEY,
            store_id VARCHAR NOT NULL,
            register_id VARCHAR NOT NULL,
            business_date DATE NOT NULL,
            event_time_local TIMESTAMP NOT NULL,
            event_time_utc TIMESTAMP,
            received_at TIMESTAMP NOT NULL,
            order_id VARCHAR,
            customer_ref VARCHAR,
            tender_type VARCHAR NOT NULL,
            gross_cents BIGINT NOT NULL,
            discount_cents BIGINT NOT NULL,
            tax_cents BIGINT NOT NULL,
            net_cents BIGINT NOT NULL,
            return_flag BOOLEAN NOT NULL,
            void_flag BOOLEAN NOT NULL,
            loaded_at TIMESTAMP NOT NULL
        )
    """)
    keyed = _h(ctx, "raw.pos_sales_header.keyed", "o.order_id")
    void = _h(ctx, "raw.pos_sales_header.void", "o.order_id")
    gift = _h(ctx, "raw.pos_sales_header.gift", "o.order_id")
    e2_at = str(ctx.era("E2_customer_rekey")["at"])

    # Tender follows what the order was paid with. Two live payments is a
    # split tender; a gift card leaves no trace upstream, so the extract
    # draws it, at the share the till reports.
    tender = (
        "CASE WHEN p.live > 1 THEN 'split' "
        f"WHEN ({gift}) % 1000 < 48 THEN 'gift_card' "
        "WHEN p.first_method = 1 THEN 'cash' ELSE 'card' END"
    )
    # The trade account code in the format of its era. E2 re-keys the book on
    # 2024-11-04; the POS writes whatever the account carried that night.
    # The till captures a trade account number on about six transactions in a
    # hundred, and only where the OMS never got a customer at all: the
    # builder gave the counter their account and nothing keyed it to a
    # shopper record. The number is written in the format of its era, so E2's
    # re-key is visible in this feed as well as in S1.
    n_trade = ctx.sql(
        "SELECT count(*) FROM sim_customer.customers WHERE trade_seq IS NOT NULL"
    ).fetchone()[0] or 1
    account = _h(ctx, "raw.pos_sales_header.account", "o.order_id")
    customer_ref = (
        f"CASE WHEN c.trade_seq IS NULL THEN NULL "
        f"WHEN o.business_date < DATE '{e2_at}' "
        f"THEN 'CUST' || lpad(c.trade_seq::VARCHAR, 4, '0') "
        f"ELSE c.customer_code END"
    )
    # A store trades 07:00 to 22:00 local and the batch closes at 23:05, so
    # the per-transaction time E6 starts sending sits inside trading hours.
    # The upstream draws a time over the whole day; this maps it onto them,
    # which is also what keeps `loaded_at >= event_time_utc` true on every row.
    trading_time = (
        "o.business_date::TIMESTAMP + INTERVAL 7 HOUR + INTERVAL 1 MINUTE * "
        "((date_part('hour', o.order_datetime) * 60 "
        " + date_part('minute', o.order_datetime)) * 900 / 1440)::INTEGER"
    )
    ctx.sql(f"""
        INSERT INTO raw.pos_sales_header
        WITH pay AS (
            SELECT order_id,
                   count(*) FILTER (WHERE status <> 'failed') AS live,
                   min(payment_method_id) FILTER (WHERE status <> 'failed')
                       AS first_method
            FROM sim_sales.payments GROUP BY order_id
        ), ret AS (
            SELECT DISTINCT order_id FROM sim_sales.returns
        ), tr AS (
            SELECT customer_code, trade_seq,
                   row_number() OVER (ORDER BY trade_seq) - 1 AS n
            FROM sim_customer.customers WHERE trade_seq IS NOT NULL
        ), t AS (
            SELECT o.order_id, o.order_datetime, o.register_id, o.customer_id,
                   o.subtotal_amount, o.order_discount_amount, o.tax_amount,
                   b.store_id, b.store_code, b.timezone, b.business_date,
                   b.in_e1, b.received_at
            FROM sim_sales.orders o
            JOIN _pos_batch b
              ON b.store_id = o.store_id
             AND b.business_date = o.order_datetime::DATE
            WHERE o.channel_id = 1 AND b.status <> 'missing'
        ), m AS (
            SELECT t.*,
                   round(t.subtotal_amount * 100)::BIGINT AS gross_cents,
                   round(t.order_discount_amount * 100)::BIGINT AS discount_cents,
                   round(t.tax_amount * 100)::BIGINT AS tax_cents,
                   CASE WHEN t.customer_id IS NULL
                         AND ({account.replace('o.order_id', 't.order_id')}) % 100 < 40
                        THEN ({account.replace('o.order_id', 't.order_id')} >> 8)
                             % {n_trade} END AS trade_pick
            FROM t
        )
        SELECT 'PT-' || lpad(o.order_id::VARCHAR, 10, '0'),
               o.store_code,
               o.store_code || '-' || coalesce(r.terminal_code, 'POS-01'),
               o.business_date,
               -- E1 stamps the close batch, one constant for the whole
               -- estate and the whole era. E6 stamps the transaction.
               CASE WHEN o.in_e1
                    THEN o.business_date::TIMESTAMP + INTERVAL 23 HOUR + INTERVAL 5 MINUTE
                    ELSE {trading_time} END,
               CASE WHEN o.in_e1 THEN NULL
                    ELSE {_utc(trading_time, "o.timezone")} END,
               o.received_at,
               CASE WHEN ({keyed}) % 100 < 4 THEN NULL
                    ELSE ok.raw_order_id END,
               {customer_ref},
               {tender},
               o.gross_cents, o.discount_cents, o.tax_cents,
               o.gross_cents - o.discount_cents + o.tax_cents,
               ret.order_id IS NOT NULL,
               ({void}) % 1000 < 18,
               date_trunc('hour', o.received_at) + INTERVAL 1 HOUR
        FROM m o
        LEFT JOIN _util.order_keys ok ON ok.order_id = o.order_id
        LEFT JOIN pay p ON p.order_id = o.order_id
        LEFT JOIN ret ON ret.order_id = o.order_id
        LEFT JOIN sim_store.registers r ON r.register_id = o.register_id
        LEFT JOIN tr c ON c.n = o.trade_pick
        ORDER BY o.business_date, o.store_id, o.order_id
    """)


# --- raw.pos_batch_manifest ------------------------------------------------

def _manifest(ctx: Context) -> None:
    """One row per store per business date. `claimed_row_count` is what the
    register declared and `row_count` is what the load took, which is the
    pair POS-2's till reconciliation works off; a partial file is the case
    where they differ by a whole transaction."""
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.pos_batch_manifest (
            batch_id VARCHAR NOT NULL PRIMARY KEY,
            store_id VARCHAR NOT NULL,
            business_date DATE NOT NULL,
            file_name VARCHAR NOT NULL,
            claimed_row_count INTEGER,
            row_count INTEGER,
            stamped_at TIMESTAMP,
            received_at TIMESTAMP,
            status VARCHAR NOT NULL
        )
    """)
    ctx.sql("""
        INSERT INTO raw.pos_batch_manifest
        WITH n AS (
            SELECT store_id, business_date, count(*)::INTEGER AS rows_sent
            FROM raw.pos_sales_header
            GROUP BY store_id, business_date
        )
        SELECT 'B-' || strftime(b.business_date, '%Y%m%d') || '-' || b.store_code,
               b.store_code,
               b.business_date,
               'store_' || b.store_code || '.csv',
               CASE WHEN b.status = 'missing' THEN NULL
                    ELSE coalesce(n.rows_sent, 0) END,
               CASE WHEN b.status = 'missing' THEN NULL
                    WHEN b.partial_file
                     THEN greatest(0, coalesce(n.rows_sent, 0) - b.partial_short)
                    ELSE coalesce(n.rows_sent, 0) END,
               CASE WHEN b.status = 'missing' THEN NULL ELSE b.stamped_local END,
               b.received_at,
               b.status
        FROM _pos_batch b
        LEFT JOIN n ON n.store_id = b.store_code AND n.business_date = b.business_date
        ORDER BY b.business_date, b.store_code
    """)
    # stamped_at is the store server's local clock through E1 and UTC after
    # it, because the server learned about zones at the same release the
    # registers did. The column's meaning changes on 2025-11-03.
    ctx.sql("""
        UPDATE raw.pos_batch_manifest m
        SET stamped_at = b.stamped_utc
        FROM _pos_batch b
        WHERE b.store_code = m.store_id AND b.business_date = m.business_date
          AND NOT b.in_e1 AND m.status <> 'missing'
    """)


# --- the landing tree ------------------------------------------------------

def _files(ctx: Context) -> None:
    """`landing/pos/dt=<ds>/store_<S-####>.csv`, one per store per night.

    The file is what the store server wrote, so it carries the store's own
    columns and none of the warehouse's: no `event_time_utc`, which the
    server has no way to compute under E1 and does not send after it either,
    and no `received_at` or `loaded_at`, which are the warehouse's record of
    handling it.
    """
    dates = _landing.window(ctx, LANDING_DAYS)
    rows = ctx.sql(f"""
        SELECT h.business_date, h.store_id, h.pos_txn_id, h.register_id,
               h.event_time_local, h.order_id, h.customer_ref, h.tender_type,
               h.gross_cents, h.discount_cents, h.tax_cents, h.net_cents,
               h.return_flag, h.void_flag
        FROM raw.pos_sales_header h
        WHERE h.business_date IN {_landing.sql_dates(dates)}
        ORDER BY h.business_date, h.store_id, h.pos_txn_id
    """).fetchall()

    grouped: dict[tuple, list] = defaultdict(list)
    for row in rows:
        grouped[(row[0], row[1])].append([
            _landing.cell(value) for value in
            (row[2], row[3], row[0], row[4], row[5], row[6], row[7],
             row[8], row[9], row[10], row[11], row[12], row[13])
        ])
    root = ctx.landing_dir("pos")
    for (business_date, store_id), file_rows in sorted(grouped.items()):
        _landing.write_csv(
            root / f"dt={business_date}" / f"store_{store_id}.csv",
            FILE_COLUMNS, file_rows)
