"""The extract boundary for the OMS and AR: S1, S11, S12, S13.

These four modules turn the sim into what the agent sees, so what is asserted
here is what the boundary promises and what a task will grade against: the two
id formats and the exact forty-five rows that carry the old one after the
cutover, the sixty legacy ids that never migrated, the two clocks on every
order, the shape of the landing tree, and two builds of the same config being
the same world.

`extracts.crm` (S8) builds alongside them because the E2 crosswalk and
`raw.customers.legacy_id` are two halves of one fact and have to agree; what
that module owns is tested in `test_gen_copperline_x4.py`.

The upstream is built directly rather than through the CLI. It costs about
four seconds at the small profile, against eighty for the whole world, and it
keeps the assertions about this code.
"""

import re
import sys
from pathlib import Path

import duckdb
import pytest

from de_bench.tasks import repo_root

sys.path.insert(0, str(repo_root() / "tools"))

from gen_copperline import config                                # noqa: E402
from gen_copperline.extracts import (                            # noqa: E402
    _boundary, ar, crm, orders, orders_export, promos,
)
from gen_copperline.upstream import (                            # noqa: E402
    _util, core, customer, finance, finance_reference, hr, northwave, product,
    sales, store,
)

WORLD = repo_root() / "worlds" / "copperline"

# Everything the four extracts read, and nothing else. Inventory, logistics,
# ecommerce, support and the outside systems belong to other sources.
UPSTREAM = (core, hr, finance_reference, product, store, northwave, customer,
            sales, finance)
MINE = (promos, orders, ar, orders_export)
EXTRACTS = (crm,) + MINE

E2_AT = "2024-11-04"
E2_TAIL_UNTIL = "2024-11-17"
SCHEMA_CHANGE = "2026-01-01"


def _build(tmp_path: Path) -> tuple[Path, Path]:
    """One world at the small profile: the db, and the landing tree."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    db, landing = tmp_path / "x1.duckdb", tmp_path / "landing"
    cfg = config.load_timeline(WORLD / "timeline.yaml")
    con = duckdb.connect(str(db))
    ctx = config.Context(con=con, cfg=cfg, profile="small", landing=landing)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    _util.ensure_days(ctx)
    for module in UPSTREAM + EXTRACTS:
        module.build(ctx)
    con.close()
    return db, landing


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    db, landing = _build(tmp_path_factory.mktemp("x1") / "a")
    con = duckdb.connect(str(db), read_only=True)
    yield con, landing
    con.close()


def one(con, sql, *args):
    return con.execute(sql, *args).fetchone()


def _cfg():
    """A context that only answers date questions — `live_from` needs the end
    of the fixture range and nothing else."""
    return config.Context(con=None, cfg=config.load_timeline(
        WORLD / "timeline.yaml"), profile="small", landing=WORLD)


# --- the shipped order reference ------------------------------------------

def test_padding_the_simulated_key_would_collide(world):
    """The reason the reference is renumbered, as a fact rather than a claim.

    `lpad` truncates: in DuckDB, as in Postgres, `lpad('121600050', 8, '0')`
    is `'12160005'`. So `'ORD-' || lpad(order_id, 8, '0')` puts every ten
    consecutive orders above 99,999,999 under one string, and any feed that
    spells the reference that way joins the wrong order — or, on this table,
    could not be a key at all.
    """
    con, _ = world
    assert one(con, "SELECT lpad('121600050', 8, '0')")[0] == "12160005"
    orders_now, spelled = one(con, """
        SELECT count(*), count(DISTINCT 'ORD-' || lpad(order_id::VARCHAR, 8, '0'))
        FROM _util.order_keys
    """)
    assert spelled < orders_now
    assert one(con, "SELECT count(DISTINCT raw_order_id) FROM _util.order_keys")[0] \
        == orders_now


def test_the_order_reference_holds_its_eight_digits(world):
    """`ORD-########` cannot hold the simulated key, so the shipped reference
    is derived from (ds, slot). It has to be unique, dense, and in date
    order."""
    con, _ = world
    n, refs, lo, hi = one(con, """
        SELECT count(*), count(DISTINCT order_id), min(order_id), max(order_id)
        FROM raw.orders
    """)
    assert n == refs
    assert lo == "ORD-00000001"
    assert hi == f"ORD-{n:08d}"
    assert not one(con, r"""
        SELECT count(*) FROM raw.orders
        WHERE NOT regexp_matches(order_id, '^ORD-[0-9]{8}$')
    """)[0]
    # In date order, which is what an OMS counter looks like.
    assert not one(con, """
        SELECT count(*) FROM (
            SELECT local_order_date,
                   lag(local_order_date) OVER (ORDER BY order_id) AS prev
            FROM raw.orders
        ) WHERE prev > local_order_date
    """)[0]


def test_the_processor_reference_is_the_ambiguous_one(world):
    """`OE-#######` is not renumbered. It is `upstream.external.ORDER_REF`,
    which every processor feed spells the same way, and seven digits cannot
    hold the key — so it names several orders and the bridge is the only
    honest link (NLO-4)."""
    con, _ = world
    assert one(con, r"""
        SELECT count(*) FROM raw.orders
        WHERE NOT regexp_matches(order_ref, '^OE-[0-9]{7}$')
    """)[0] == 0
    wrong = one(con, """
        SELECT count(*) FROM raw.orders o
        JOIN _util.order_keys k ON k.raw_order_id = o.order_id
        WHERE o.order_ref <> 'OE-' || lpad((k.order_id % 10000000)::VARCHAR, 7, '0')
    """)[0]
    assert wrong == 0, "the reference must be the formula every feed uses"
    shared = one(con, """
        SELECT count(*) FROM (
            SELECT order_ref FROM raw.orders GROUP BY 1 HAVING count(*) > 1)
    """)[0]
    assert shared > 0


# --- E2, the re-key -------------------------------------------------------

def test_the_id_format_flips_at_the_cutover(world):
    con, _ = world
    before = one(con, f"""
        SELECT count(*) FILTER (WHERE customer_ref LIKE 'CUST%'),
               count(*) FILTER (WHERE customer_ref LIKE 'C-%')
        FROM raw.orders WHERE customer_ref IS NOT NULL
          AND local_order_date < DATE '{E2_AT}'
    """)
    assert before[1] == 0, "no current-format id may predate the re-key"
    assert before[0] > 0

    after = one(con, f"""
        SELECT count(*) FILTER (WHERE customer_ref LIKE 'CUST%'),
               count(*) FILTER (WHERE customer_ref LIKE 'C-%')
        FROM raw.orders WHERE customer_ref IS NOT NULL
          AND local_order_date >= DATE '{E2_AT}'
    """)
    assert after == (_boundary.E2_LATE_ROWS, after[1])
    assert after[1] > 0

    # Both formats on the cutover date itself.
    both = one(con, f"""
        SELECT count(*) FILTER (WHERE customer_ref LIKE 'CUST%'),
               count(*) FILTER (WHERE customer_ref LIKE 'C-%')
        FROM raw.orders WHERE local_order_date = DATE '{E2_AT}'
    """)
    assert both[0] > 0 and both[1] > 0


def test_the_forty_five_late_rows_are_exactly_that(world):
    """A date-based reading of the crosswalk gets these and only these wrong,
    so the count is graded and the tail has a last day."""
    con, _ = world
    rows = con.execute(f"""
        SELECT local_order_date, count(*) FROM raw.orders
        WHERE customer_ref LIKE 'CUST%' AND local_order_date >= DATE '{E2_AT}'
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    assert sum(n for _, n in rows) == 45
    assert [n for _, n in rows] == list(_boundary.E2_LATE_QUOTA)
    assert str(rows[0][0]) == E2_AT
    assert str(rows[-1][0]) == E2_TAIL_UNTIL


def test_the_crosswalk_ships_sixty_orphans(world):
    con, _ = world
    n, mapped = one(con, """
        SELECT count(*), count(customer_id) FROM raw.customer_id_map
    """)
    assert (mapped, n - mapped) == (2600, 60)

    # The unmapped rows say why.
    assert one(con, """
        SELECT count(*) FROM raw.customer_id_map
        WHERE customer_id IS NULL AND note IS NULL
    """)[0] == 0

    # The crosswalk and the CRM agree on which accounts were ever keyed the
    # old way, in both directions, and on what the current id is.
    assert one(con, """
        SELECT count(*) FROM raw.customer_id_map m
        LEFT JOIN raw.customers c ON c.legacy_id = m.legacy_id
        WHERE c.customer_id IS NULL
    """)[0] == 0
    assert one(con, """
        SELECT count(*) FROM raw.customers
        WHERE legacy_id IS NOT NULL
          AND legacy_id NOT IN (SELECT legacy_id FROM raw.customer_id_map)
    """)[0] == 0
    assert one(con, """
        SELECT count(*) FROM raw.customer_id_map m
        JOIN raw.customers c ON c.legacy_id = m.legacy_id
        WHERE m.customer_id IS NOT NULL AND m.customer_id <> c.customer_id
    """)[0] == 0
    dropped = one(con, f"""
        SELECT count(*) FROM raw.orders o
        JOIN raw.customer_id_map m ON m.legacy_id = o.customer_ref
        WHERE m.customer_id IS NULL AND o.local_order_date < DATE '{E2_AT}'
    """)[0]
    assert dropped > 0


# --- the two clocks -------------------------------------------------------

def test_two_clocks_on_every_order(world):
    con, _ = world
    assert one(con, """
        SELECT count(*) FROM raw.orders WHERE loaded_at < event_time_utc
    """)[0] == 0
    assert one(con, """
        SELECT count(*) FROM raw.orders WHERE loaded_at < updated_at
    """)[0] == 0
    assert one(con, """
        SELECT count(*) FROM raw.orders
        WHERE date_part('second', event_time_utc) <> 0
           OR date_part('second', loaded_at) <> 0
           OR date_part('second', event_time_local) <> 0
    """)[0] == 0

    # The tail: nothing later than day 5, and day 5 always happens. The
    # measured lookback is the whole of NLO-2, so it has to read back as the
    # number the timeline declares.
    tail = dict(con.execute("""
        SELECT datediff('day', event_time_utc, loaded_at), count(*)
        FROM raw.orders WHERE event_time_utc IS NOT NULL GROUP BY 1 ORDER BY 1
    """).fetchall())
    assert set(tail) == {0, 1, 2, 3, 4, 5}
    total = sum(tail.values())
    assert 0.90 < tail[0] / total < 0.94


def test_the_store_clock_before_the_utc_cutover(world):
    """`event_time_utc` is NULL on store rows through E1, and the business
    date is not the UTC date for an evening sale in the Americas."""
    con, _ = world
    missing, present = one(con, """
        SELECT count(*) FILTER (WHERE channel = 'store' AND event_time_utc IS NULL),
               count(*) FILTER (WHERE channel <> 'store' AND event_time_utc IS NULL)
        FROM raw.orders
    """)
    assert missing > 0 and present == 0
    assert one(con, """
        SELECT count(*) FROM raw.orders
        WHERE channel = 'store' AND local_order_date >= DATE '2025-11-03'
          AND event_time_utc IS NULL
    """)[0] == 0
    # A NULL population a check could filter on is worth at least a tenth of
    # the table (spec 03 section 1).
    share = one(con, """
        SELECT count(*) FILTER (WHERE event_time_utc IS NULL) / count(*)::DOUBLE
        FROM raw.orders
    """)[0]
    assert share > 0.10
    crossed = one(con, """
        SELECT count(*) FROM raw.orders
        WHERE event_time_utc IS NOT NULL
          AND event_time_utc::DATE <> local_order_date
    """)[0]
    assert crossed > 0


def test_currency_and_the_rate_are_null_before_the_cutover(world):
    con, _ = world
    before = one(con, """
        SELECT count(*), count(currency_code), count(fx_rate_ppm)
        FROM raw.orders WHERE local_order_date < DATE '2025-04-07'
    """)
    assert before[1] == 0 and before[2] == 0 and before[0] > 0
    after = one(con, """
        SELECT count(*), count(currency_code), count(fx_rate_ppm),
               count(DISTINCT currency_code)
        FROM raw.orders WHERE local_order_date >= DATE '2025-04-07'
    """)
    assert after[0] == after[1] == after[2]
    assert after[3] > 1, "the point of E4 is more than one currency"
    # A USD row converts at one, and every other rate is a real number of
    # parts per million, so the landed row alone re-derives the conversion.
    assert one(con, """
        SELECT count(*) FROM raw.orders
        WHERE currency_code = 'USD' AND fx_rate_ppm <> 1000000
    """)[0] == 0
    assert one(con, """
        SELECT count(*) FROM raw.orders
        WHERE currency_code IS NOT NULL AND fx_rate_ppm <= 0
    """)[0] == 0


def test_money_is_integer_cents_everywhere(world):
    con, _ = world
    bad = con.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema IN ('raw', 'ops') AND column_name LIKE '%\\_cents'
          ESCAPE '\\' AND data_type <> 'BIGINT'
    """).fetchall()
    assert not bad, bad
    # The header discount stays on the header: no line carries it.
    assert one(con, """
        SELECT count(*) FROM raw.orders o
        JOIN raw.order_lines l USING (order_id)
        WHERE o.order_discount_cents > 0 AND l.line_discount_cents > 0
    """)[0] == 0


# --- the populations a naive total swallows -------------------------------

def test_the_flagged_populations_exist(world):
    con, _ = world
    test_orders, deleted = one(con, """
        SELECT count(*) FILTER (WHERE is_test),
               count(*) FILTER (WHERE deleted_at IS NOT NULL) FROM raw.orders
    """)
    assert test_orders == 900          # 18,000 at full, over the divisor
    assert deleted == 260              # 5,200 at full
    # A soft-deleted row is still visible, and it is dated after the order.
    assert one(con, """
        SELECT count(*) FROM raw.orders
        WHERE deleted_at IS NOT NULL AND deleted_at < event_time_local
    """)[0] == 0


def test_the_pos_line_detail_lands_on_the_order_lines(world):
    """`docs/runbooks/pos-ingestion.md` sends the reader to the POS line
    detail, and chapter 03 section 8 puts it here rather than in a table of
    its own. So the value the runbook names has to exist, on the store rows
    and only on them."""
    con, _ = world
    by_source = dict(con.execute("""
        SELECT source_system, count(*) FROM raw.order_lines GROUP BY 1
    """).fetchall())
    assert by_source["pos_replay"] > 0
    assert set(by_source) <= {"oms", "pos_replay", "marketplace_sync", "nwv"}
    # A line reads the same source as its own order, always.
    assert one(con, """
        SELECT count(*) FROM raw.order_lines l JOIN raw.orders o USING (order_id)
        WHERE l.source_system <> o.source_system
    """)[0] == 0
    assert one(con, """
        SELECT count(*) FROM raw.order_lines l JOIN raw.orders o USING (order_id)
        WHERE l.source_system = 'pos_replay' AND o.channel <> 'store'
    """)[0] == 0


def test_the_status_feed_keeps_the_retention_window(world):
    con, _ = world
    lo, hi, feeds = one(con, """
        SELECT min(changed_at)::DATE, max(changed_at)::DATE,
               count(DISTINCT feed) FROM raw.order_status_history
    """)
    assert feeds == 2
    assert (hi - lo).days <= orders.STATUS_HISTORY_DAYS + 10
    assert one(con, """
        SELECT count(*) FROM raw.order_status_history h
        LEFT JOIN raw.orders o USING (order_id) WHERE o.order_id IS NULL
    """)[0] == 0


# --- S11, the two shapes --------------------------------------------------

def test_the_export_changes_shape_and_nothing_errors(world):
    con, landing = world
    old = f"{landing}/orders_export/dt=2025-12-31/part-000.parquet"
    new = f"{landing}/orders_export/dt={SCHEMA_CHANGE}/part-000.parquet"
    old_columns = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{old}')").fetchall()]
    new_columns = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{new}')").fetchall()]
    assert "customer_ref" in old_columns and "customer_id" not in old_columns
    assert "customer_id" in new_columns and "customer_ref" not in new_columns
    assert "promo_allocation_cents" not in old_columns
    assert "promo_allocation_cents" in new_columns

    # Read together with union_by_name the rename splits into two
    # half-populated columns and the new column NULL-fills. Nothing errors,
    # which is what makes W4 hard to see.
    both, ref, cid, filled = one(con, f"""
        SELECT count(*), count(customer_ref), count(customer_id),
               count(promo_allocation_cents)
        FROM read_parquet('{landing}/orders_export/dt=*/part-000.parquet',
                          union_by_name = true)
    """)
    # The tree holds the live window, so it is the orders of those days.
    assert both == one(con, f"""
        SELECT count(*) FROM raw.orders
        WHERE local_order_date >= DATE '{_boundary.live_from(_cfg())}'
    """)[0]
    assert ref > 0 and cid > 0 and 0 < filled < both


# --- the landing tree -----------------------------------------------------

def test_the_landing_tree_has_the_shape_the_spec_names(world):
    con, landing = world
    day = "2026-04-14"
    assert sorted(p.name for p in (landing / "oms" / f"dt={day}").iterdir()) == [
        "gift_card_ledger.csv", "order_lines.csv", "orders.csv",
        "promo_applications.csv", "returns.csv",
    ]
    assert (landing / "orders_export" / f"dt={day}" / "part-000.parquet").exists()
    assert {p.name for p in (landing / "ar" / f"dt={day}").iterdir()} <= {
        "invoices.csv", "invoice_lines.csv", "credit_memos.csv",
        "disputes.csv", "plan_lines.csv",
    }
    # One partition per day of the RET-2 live window — 400 days back from the
    # end of the range — and none outside it. `raw` still holds the whole
    # history; the tree holds what the ingest DAGs still read.
    days = sorted(p.name for p in (landing / "oms").iterdir())
    assert days[0] == f"dt={_boundary.live_from(_cfg())}"
    assert days[-1] == "dt=2026-06-14"
    assert len(days) == 400
    assert len(list((landing / "orders_export").iterdir())) == 400

    # The file is the table: what landed for a day is what `raw` holds for it.
    landed = one(con, f"""
        SELECT count(*) FROM read_csv('{landing}/oms/dt={day}/orders.csv')
    """)[0]
    assert landed == one(con, f"""
        SELECT count(*) FROM raw.orders WHERE local_order_date = DATE '{day}'
    """)[0]


# --- AR ------------------------------------------------------------------

def test_the_invoice_book_carries_both_books_and_both_id_formats(world):
    con, _ = world
    goods, plan, nwv = one(con, """
        SELECT count(*) FILTER (WHERE order_id IS NOT NULL),
               count(*) FILTER (WHERE service_start IS NOT NULL),
               count(*) FILTER (WHERE nwv_account_id IS NOT NULL)
        FROM raw.invoices
    """)
    assert goods > 0 and plan > 0 and nwv > 0
    # An invoice names one book or the other, never both and never neither.
    assert one(con, """
        SELECT count(*) FROM raw.invoices
        WHERE (customer_ref IS NULL) = (nwv_account_id IS NULL)
    """)[0] == 0
    assert one(con, f"""
        SELECT count(*) FROM raw.invoices
        WHERE customer_ref LIKE 'C-%' AND invoice_date < DATE '{E2_AT}'
    """)[0] == 0
    # The posting month is not always the month of the invoice date (REV-8).
    off = one(con, """
        SELECT count(*) FROM raw.invoices v
        JOIN _util.fiscal_month f ON f.cal_date = v.invoice_date
        WHERE v.posted_period <> f.period_label
    """)[0]
    assert off > 0
    # Partial settlement is the aging case, and nothing is over-settled.
    assert one(con, """
        SELECT count(*) FROM raw.invoices WHERE settled_cents > total_cents
    """)[0] == 0
    assert one(con, """
        SELECT count(*) FROM raw.invoices
        WHERE invoice_status = 'partially_paid'
          AND (settled_cents = 0 OR settled_cents = total_cents)
    """)[0] == 0
    # The trade account an invoice names is the one the CRM shipped, and the
    # billing era both tables carry for it is one fact, not two.
    assert one(con, """
        SELECT count(*) FROM raw.invoices v
        LEFT JOIN raw.customers c ON c.customer_id = v.customer_ref
        WHERE v.customer_ref LIKE 'C-%' AND c.customer_id IS NULL
    """)[0] == 0
    assert one(con, """
        SELECT count(*) FROM raw.invoices v
        JOIN raw.customers c ON c.customer_id = v.customer_ref
        WHERE v.billing_era <> c.billing_era
    """)[0] == 0


def test_a_mid_term_plan_change_closes_one_line_and_opens_another(world):
    con, _ = world
    changed = one(con, """
        SELECT count(*) FROM raw.plan_lines WHERE changed_from_plan_line_id IS NOT NULL
    """)[0]
    assert changed > 0
    # The line it replaces exists, ends the day before, and belongs to the
    # same plan: summing both without reading the dates double-counts.
    assert one(con, """
        SELECT count(*) FROM raw.plan_lines n
        JOIN raw.plan_lines p ON p.plan_line_id = n.changed_from_plan_line_id
        WHERE n.changed_from_plan_line_id IS NOT NULL
          AND (p.plan_id <> n.plan_id OR p.term_end <> n.term_start - 1)
    """)[0] == 0


def test_an_open_dispute_has_no_settlement(world):
    con, _ = world
    assert one(con, """
        SELECT count(*) FROM raw.disputes
        WHERE dispute_status = 'open'
          AND (resolved_on IS NOT NULL OR settled_cents <> 0)
    """)[0] == 0
    assert one(con, """
        SELECT count(*) FROM raw.disputes
        WHERE dispute_status = 'settled' AND settled_cents = 0
    """)[0] == 0


# --- S12 -----------------------------------------------------------------

def test_the_side_tables_join_the_spine(world):
    con, _ = world
    for sql in (
        "SELECT count(*) FROM raw.promo_applications a "
        "LEFT JOIN raw.promotions p USING (promo_id) WHERE p.promo_id IS NULL",
        "SELECT count(*) FROM raw.promo_applications a "
        "LEFT JOIN raw.orders o USING (order_id) WHERE o.order_id IS NULL",
        "SELECT count(*) FROM raw.returns r "
        "LEFT JOIN raw.order_lines l ON l.order_id = r.order_id "
        "AND l.order_line_id = r.order_line_id WHERE l.order_id IS NULL",
        "SELECT count(*) FROM raw.gift_card_ledger g "
        "LEFT JOIN raw.gift_cards c USING (card_id) WHERE c.card_id IS NULL",
        "SELECT count(*) FROM raw.invoice_lines l "
        "LEFT JOIN raw.invoices v USING (invoice_id) WHERE v.invoice_id IS NULL",
    ):
        assert one(con, sql)[0] == 0, sql

    # The gift card applied to an order is what the ledger says it redeemed,
    # and never more than the order was worth.
    assert one(con, """
        SELECT count(*) FROM raw.orders
        WHERE gift_card_applied_cents > grand_total_cents
    """)[0] == 0
    assert one(con, """
        SELECT count(*) FROM (
            SELECT o.order_id, o.gift_card_applied_cents AS on_order,
                   coalesce(sum(-g.amount_cents), 0) AS in_ledger
            FROM raw.orders o
            LEFT JOIN raw.gift_card_ledger g
                   ON g.order_id = o.order_id AND g.entry_type = 'redeem'
            GROUP BY 1, 2
        ) WHERE on_order <> in_ledger
    """)[0] == 0

    # Two promotions share a stack priority, and the applied order is not the
    # priority order.
    assert one(con, """
        SELECT count(*) FROM (
            SELECT stack_priority FROM raw.promotions
            GROUP BY 1 HAVING count(*) > 1)
    """)[0] == 1
    assert one(con, """
        SELECT count(*) FROM raw.promo_applications GROUP BY order_id
        HAVING count(*) > 1 LIMIT 1
    """)[0] > 1

    # A return that came back through another channel keeps the company total
    # identical and moves the channel one.
    cross = one(con, """
        SELECT count(*) FROM raw.returns r JOIN raw.orders o USING (order_id)
        WHERE r.return_channel <> o.channel
    """)[0]
    assert cross > 0
    assert one(con, """
        SELECT count(DISTINCT refund_method) FROM raw.returns
    """)[0] == 3


# --- the contract ---------------------------------------------------------

def test_no_extract_reads_another_extract_across_sources(world):
    """`raw.orders.gift_card_applied_cents` sums the gift-card ledger, and
    the promos module hands it over through `_util`, not through `raw`. The
    rule is easy to break by accident, so it is grepped."""
    source = Path(orders.__file__).read_text()
    assert "raw.gift_card_ledger" not in source
    assert "_util.gift_card_applied" in Path(_boundary.__file__).read_text()


def test_no_wall_clock_in_the_extract_modules():
    pattern = re.compile(r"datetime\.now|date\.today|time\.time\(|utcnow")
    for module in MINE + (_boundary,):
        text = Path(module.__file__).read_text()
        assert not pattern.search(text), module.__name__


def test_no_module_spells_a_shipped_order_reference_of_its_own():
    """Eight digits cannot hold the simulated key, so `ORD-########` is
    renumbered in `_boundary` and every other feed joins `_util.order_keys`
    for it. A module that spells the reference from the key writes nine digits
    after 2025-05-04 and joins nothing."""
    for module in MINE:
        text = Path(module.__file__).read_text()
        assert "'ORD-'" not in text, module.__name__


def test_two_builds_are_the_same_world(tmp_path):
    fingerprints = []
    for run in ("a", "b"):
        db, landing = _build(tmp_path / run)
        con = duckdb.connect(str(db), read_only=True)
        tables = con.execute("""
            SELECT table_schema, table_name FROM information_schema.tables
            WHERE table_schema IN ('raw', 'ops') ORDER BY 1, 2
        """).fetchall()
        fingerprints.append({
            f"{schema}.{table}": con.execute(
                f"SELECT count(*), coalesce(sum(hash(t)), 0) FROM {schema}.{table} t"
            ).fetchone() for schema, table in tables
        } | {
            "landing": con.execute(f"""
                SELECT count(*), sum(hash(t)) FROM read_parquet(
                    '{landing}/orders_export/dt=*/part-000.parquet',
                    union_by_name = true) t
            """).fetchone()
        })
        con.close()
    assert fingerprints[0] == fingerprints[1]
    assert len(fingerprints[0]) > 15
