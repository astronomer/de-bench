"""The two upstream modules that sit beside Copperline: `northwave` (the
acquired book and the NW-214 pair truth) and `external` (the processors, the
marketplace and the advertising platforms).

`external` settles what the sales module charged, and the sales module is
built elsewhere, so these tests stub `sim_sales` and `sim_store` against the
column lists spec 02 states. The stub is small and dense on purpose: every
era boundary these modules care about falls inside it.
"""

import datetime as dt
from pathlib import Path

import duckdb
import pytest

from de_bench.tasks import repo_root

import sys

sys.path.insert(0, str(repo_root() / "tools"))

from gen_copperline import config                      # noqa: E402
from gen_copperline.upstream import external, northwave  # noqa: E402

WORLD = repo_root() / "worlds" / "copperline"

# What the stub stands in for. Spec 02 sections 6 and 8.
_STUB = """
CREATE SCHEMA IF NOT EXISTS sim_store;
CREATE SCHEMA IF NOT EXISTS sim_sales;

CREATE OR REPLACE TABLE sim_store.channels AS
SELECT * FROM (VALUES
    (1, 'Retail Store', 'store', true), (2, 'Web Store', 'web', true),
    (3, 'Marketplace', 'marketplace', true), (4, 'Trade', 'trade', true)
) t(channel_id, name, channel_type, is_active);

CREATE OR REPLACE TABLE sim_sales.payment_methods AS
SELECT * FROM (VALUES
    (1, 'Visa Credit', 'card', 'Meridian Pay', 2, 0.0229),
    (2, 'Mastercard',  'card', 'Meridian Pay', 2, 0.0215),
    (3, 'Cash',        'cash', 'Copperline',   0, 0.0000)
) t(payment_method_id, name, method_type, provider, settlement_days, fee_pct);

CREATE OR REPLACE TABLE sim_sales.orders AS
SELECT
    d.daynum * 1000 + g.i                              AS order_id,
    'W-' || strftime(d.ds, '%Y') || '-' || (d.daynum * 1000 + g.i)::VARCHAR AS order_number,
    CASE WHEN g.i % 7 = 0 THEN NULL ELSE 400000 + (d.daynum * 1000 + g.i) % 5000 END AS customer_id,
    1 + g.i % 4                                        AS channel_id,
    d.ds::TIMESTAMP + INTERVAL 1 HOUR * (9 + g.i % 9)  AS order_datetime,
    CASE WHEN d.ds < DATE '2025-04-07' THEN 'USD'
         WHEN g.i % 10 = 0 THEN 'GBP' WHEN g.i % 10 = 1 THEN 'EUR'
         ELSE 'USD' END                                AS currency_code,
    round(20 + (g.i * 37 % 900) / 3.0, 2)::DECIMAL(18,4) AS grand_total
FROM (SELECT range::DATE AS ds,
             date_diff('day', DATE '1970-01-01', range::DATE) AS daynum
      FROM range(DATE '{start}', DATE '{end}' + INTERVAL 1 DAY, INTERVAL 1 DAY)) d
CROSS JOIN generate_series(1, 12) g(i);

CREATE OR REPLACE TABLE sim_sales.payments AS
SELECT
    o.order_id                                         AS payment_id,
    o.order_id,
    1 + o.order_id % 2                                 AS payment_method_id,
    o.grand_total                                      AS amount,
    o.currency_code,
    'captured'                                         AS status,
    o.order_datetime                                   AS authorized_at,
    o.order_datetime + INTERVAL 3 HOUR                 AS captured_at,
    CASE WHEN o.order_datetime::DATE < DATE '2025-07-01' THEN 'hp_' ELSE 'mp_' END
        || lpad(o.order_id::VARCHAR, 12, '0')          AS gateway_reference
FROM sim_sales.orders o
JOIN sim_store.channels ch ON ch.channel_id = o.channel_id
WHERE ch.channel_type <> 'trade';
"""


def _build(profile: str = "small") -> duckdb.DuckDBPyConnection:
    cfg = config.load_timeline(WORLD / "timeline.yaml")
    con = duckdb.connect()
    ctx = config.Context(con=con, cfg=cfg, profile=profile,
                         landing=Path("/nonexistent"))
    northwave.build(ctx)
    con.execute(_STUB.format(start=ctx.start, end=ctx.end))
    external.build(ctx)
    return con


@pytest.fixture(scope="module")
def con():
    c = _build()
    yield c
    c.close()


def _one(con, sql):
    return con.execute(sql).fetchone()


# --- the NW-214 truth -------------------------------------------------------

def test_the_pair_truth_is_exactly_300_90_and_60(con):
    """The three counts the whole dedup task turns on. They are exact by
    construction, so an inexact one means a role boundary moved."""
    kinds = dict(con.execute(
        "SELECT kind, count(*) FROM sim_identity.merge_truth GROUP BY 1").fetchall())
    assert kinds == {"true_visible": 210, "true_fuzzy_invisible": 90,
                     "decoy_fuzzy_attractive": 60}

    n_true, n_pairs, n_nwa = _one(con, """
        SELECT count(*) FILTER (WHERE kind LIKE 'true_%'),
               count(DISTINCT trade_seq), count(DISTINCT nwa_id)
        FROM sim_identity.merge_truth""")
    assert n_true == 300
    assert n_pairs == n_nwa == 360, "a book may appear in at most one pair"

    # The populations the three wrong answers are computed from.
    assert _one(con, "SELECT count(*) FROM sim_identity.trade_accounts") == (4000,)
    assert _one(con, "SELECT count(*) FROM sim_nwv.accounts") == (1400,)
    assert _one(con, "SELECT count(*) FROM sim_identity.trade_accounts "
                     "WHERE status = 'active'") == (3960,)
    assert _one(con, "SELECT count(*) FROM sim_nwv.accounts "
                     "WHERE status = 'active'") == (1380,)
    # Every pair and every decoy is active on both sides, so the board deck's
    # naive union of active rows is 5,340 and the truth is 5,040.
    assert _one(con, """
        SELECT count(*) FROM sim_identity.merge_truth m
        JOIN sim_identity.trade_accounts t USING (trade_seq)
        JOIN sim_nwv.accounts a ON a.nwv_account_id = m.nwa_id
        WHERE t.status = 'active' AND a.status = 'active'""") == (360,)


def test_the_exact_rules_find_205_true_and_43_false(con):
    """`tax_id` is the deterministic key on 205 of the 300, and the CRM's
    own rule removes 248 — 205 true by e-mail and 43 false by shared
    domain. Those two numbers are what make the CRM export 5,092."""
    def matched(predicate):
        return _one(con, f"""
            SELECT count(*),
                   count(*) FILTER (WHERE m.kind LIKE 'true_%'),
                   count(*) FILTER (WHERE m.kind = 'decoy_fuzzy_attractive')
            FROM sim_identity.trade_accounts t
            JOIN sim_nwv.accounts a ON {predicate}
            LEFT JOIN sim_identity.merge_truth m
                   ON m.trade_seq = t.trade_seq AND m.nwa_id = a.nwv_account_id""")

    assert matched("t.tax_id = a.tax_id") == (205, 205, 0), \
        "a tax id must never match by accident"
    assert matched("t.contact_email = a.contact_email") == (205, 205, 0)
    assert matched("t.email_domain = split_part(a.contact_email, '@', 2)") \
        == (248, 205, 43)


def test_the_90_invisible_pairs_share_nothing_a_matcher_could_use(con):
    """Different legal name, moved city, married surname, an `info@`
    dealer-group address and no tax id. If any of these ever matched, the
    pair would stop being invisible and the 210 answer would move."""
    assert _one(con, """
        SELECT count(*) FROM sim_identity.merge_truth m
        JOIN sim_identity.trade_accounts t USING (trade_seq)
        JOIN sim_nwv.accounts a ON a.nwv_account_id = m.nwa_id
        WHERE m.kind = 'true_fuzzy_invisible'
          AND (t.legal_name = a.account_name OR t.trading_name = a.account_name
               OR t.city = a.billing_city OR t.region_code = a.billing_state
               OR t.contact_email = a.contact_email
               OR t.email_domain = split_part(a.contact_email, '@', 2)
               OR t.tax_id = a.tax_id OR t.phone = a.phone
               OR t.contact_name = a.primary_contact
               OR t.address_line1 = a.billing_street)""") == (0,)

    # A shared `info@` address across a dealer group, so the e-mail is not
    # even distinguishing within the Northwave book.
    assert _one(con, """
        SELECT count(*) FROM sim_nwv.accounts
        WHERE pair_role = 'pair_invisible' AND contact_email NOT LIKE 'info@%'""") == (0,)
    shared = _one(con, """
        SELECT count(*) FROM (
            SELECT contact_email FROM sim_nwv.accounts
            WHERE pair_role = 'pair_invisible' GROUP BY 1 HAVING count(*) > 1)""")[0]
    assert shared > 0, "the info@ addresses must actually be shared"

    # A NULL tax id cannot give the hidden pairs away: a third of the book
    # carries one.
    null_share = _one(con, """
        SELECT round(100.0 * count(*) FILTER (WHERE tax_id IS NULL) / count(*))
        FROM sim_nwv.accounts WHERE pair_role = 'plain'""")[0]
    assert 25 <= null_share <= 45, null_share


def test_the_60_decoys_look_like_the_best_match_in_the_book(con):
    """Same account name, same city, and 43 of them the same e-mail domain —
    but a different company. They must be absent from any true set."""
    same_name, same_city, same_tax, same_person = _one(con, """
        SELECT count(*) FILTER (WHERE t.legal_name = a.account_name),
               count(*) FILTER (WHERE t.city = a.billing_city),
               count(*) FILTER (WHERE t.tax_id = a.tax_id),
               count(*) FILTER (WHERE t.contact_name = a.primary_contact)
        FROM sim_identity.merge_truth m
        JOIN sim_identity.trade_accounts t USING (trade_seq)
        JOIN sim_nwv.accounts a ON a.nwv_account_id = m.nwa_id
        WHERE m.kind = 'decoy_fuzzy_attractive'""")
    assert (same_name, same_city) == (60, 60)
    assert (same_tax, same_person) == (0, 0)
    assert _one(con, "SELECT count(*) FROM sim_identity.merge_truth "
                     "WHERE kind = 'decoy_fuzzy_attractive' AND is_true_pair") == (0,)


def test_the_two_books_share_no_identifier(con):
    """Northwave numbers `NWA-#####` and Copperline `C-######`, so no join
    by natural key can accidentally work. The merge list is the only bridge."""
    assert _one(con, "SELECT count(*) FROM sim_nwv.accounts "
                     "WHERE nwv_account_id NOT SIMILAR TO 'NWA-[0-9]{5}'") == (0,)
    # Nothing in the Copperline identity ever holds an NWA id, in any column.
    cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'sim_identity' AND table_name = 'trade_accounts' "
        "AND data_type = 'VARCHAR'").fetchall()]
    assert cols
    for col in cols:
        assert _one(con, f"""
            SELECT count(*) FROM sim_identity.trade_accounts t
            JOIN sim_nwv.accounts a ON a.nwv_account_id = t."{col}" """) == (0,), col
    # And the bridge joins on the ordinal, never on a natural key.
    assert _one(con, """
        SELECT count(*) FROM sim_identity.merge_truth m
        JOIN sim_identity.trade_accounts t USING (trade_seq)""") == (360,)


def test_the_trade_account_contract_holds(con):
    """The customer module joins this table by `trade_seq` and carries these
    columns onto `raw.customers`, so the names are a contract."""
    cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'sim_identity' AND table_name = 'trade_accounts'"
    ).fetchall()]
    assert set(northwave.TRADE_ACCOUNT_COLUMNS) <= set(cols), sorted(cols)
    lo, hi, n = _one(con, "SELECT min(trade_seq), max(trade_seq), "
                          "count(DISTINCT trade_seq) FROM sim_identity.trade_accounts")
    assert (lo, hi, n) == (1, 4000, 4000)


# --- Northwave's own clock --------------------------------------------------

def test_nothing_northwave_owns_outlives_the_freeze(con):
    """The `nwv` namespace stopped refreshing on 2025-09-30 and nothing in
    the tree says so. A row dated after it would tell the agent."""
    frozen = dt.date(2025, 9, 30)
    checked = set()
    for table in northwave.NWV_OWNED_TABLES:
        cols = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'sim_nwv' AND table_name = ? "
            "AND data_type IN ('DATE', 'TIMESTAMP')", [table]).fetchall()
        for (col,) in cols:
            latest = _one(con, f'SELECT max("{col}") FROM sim_nwv."{table}"')[0]
            checked.add((table, col))
            if latest is None:
                continue
            assert dt.date.fromisoformat(str(latest)[:10]) <= frozen, (table, col, latest)
    assert {t for t, _ in checked} >= {"accounts", "stores", "orders",
                                       "stock_ledger", "gl_journal"}, checked
    assert _one(con, "SELECT count(*) FROM sim_nwv.orders "
                     f"WHERE order_date > DATE '{frozen}'") == (0,)
    assert _one(con, "SELECT max(order_date) FROM sim_nwv.orders")[0] == frozen


def test_northwave_stores_predate_the_acquisition(con):
    """All 44 opened before the close, which is what the comparable-store
    clause turns on."""
    n, latest = _one(con, "SELECT count(*), max(opened_on) FROM sim_nwv.stores")
    assert n == 44
    assert latest < dt.date(2025, 2, 3)


# --- the second-system seam -------------------------------------------------

def test_gateway_reference_is_the_whole_of_the_shared_identity(con):
    """Meridian's settlements join Copperline's payments on the gateway
    reference and on nothing else — there is no order id out there."""
    joined, total = _one(con, """
        SELECT count(*), (SELECT count(*) FROM sim_ext.meridian_events)
        FROM sim_ext.meridian_events e
        JOIN sim_sales.payments p USING (gateway_reference)""")
    assert total > 0 and joined == total

    for table in ("meridian_events", "halcyon_settlements"):
        cols = {r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'sim_ext' AND table_name = ?", [table]).fetchall()}
        assert "order_id" not in cols, table
        assert "gateway_reference" in cols, table

    # The bridge is the exception: it is the OMS's own attempt log.
    assert "order_id" in {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'sim_ext' AND table_name = 'meridian_intents'").fetchall()}


def test_the_recycled_order_reference_matches_but_is_wrong(con):
    """Meridian reuses one `order_ref` across every retry of an order, so a
    join on it matches nearly everything and cannot say which attempt paid."""
    retried = _one(con, """
        SELECT count(*) FROM (
            SELECT order_ref FROM sim_ext.meridian_intents
            GROUP BY 1 HAVING count(*) > 1)""")[0]
    assert retried > 0
    assert _one(con, """
        SELECT count(*) FROM (
            SELECT order_ref FROM sim_ext.meridian_intents
            GROUP BY 1 HAVING count(DISTINCT order_id) > 1)""") == (0,), \
        "one reference must belong to one order, however many attempts it had"
    share = _one(con, """
        SELECT round(100.0 * count(*) FILTER (WHERE n > 1) / count(*))
        FROM (SELECT order_id, count(*) AS n FROM sim_ext.meridian_intents GROUP BY 1)""")[0]
    assert 10 <= share <= 20, share


def test_meridian_restates_and_soft_deletes(con):
    """Both populations exist only outside Copperline, and the restatement
    window is the number a reconciliation policy has to name."""
    lo, hi, n = _one(con, """
        SELECT min(date_diff('day', settlement_date, event_time_utc::DATE)),
               max(date_diff('day', settlement_date, event_time_utc::DATE)),
               count(*)
        FROM sim_ext.meridian_events WHERE restates_event_id IS NOT NULL""")
    assert n > 0 and 20 <= lo and hi <= 35, (lo, hi, n)
    assert _one(con, "SELECT count(*) FROM sim_ext.meridian_events "
                     "WHERE deleted_at IS NOT NULL")[0] > 0
    assert _one(con, "SELECT count(*) FROM sim_ext.meridian_events "
                     "WHERE loaded_at < event_time_utc") == (0,)


def test_the_processor_windows_are_honoured(con):
    """Meridian is silent before the dual-feed quarter opens, Halcyon is
    silent after it closes, and Halcyon's September is a thinner file."""
    assert _one(con, "SELECT count(*) FROM sim_ext.meridian_intents "
                     "WHERE created_at < DATE '2025-07-01'") == (0,)
    assert _one(con, "SELECT count(*) FROM sim_ext.meridian_events "
                     "WHERE event_time_utc < DATE '2025-07-01'") == (0,)
    assert _one(con, "SELECT count(*) FROM sim_ext.halcyon_settlements "
                     "WHERE settled_on > DATE '2025-10-05'") == (0,)

    # Both feeds carry the same payments for the overlap quarter, which is
    # what a naive union double-counts.
    both = _one(con, """
        SELECT count(DISTINCT h.gateway_reference)
        FROM sim_ext.halcyon_settlements h
        JOIN sim_ext.meridian_events e USING (gateway_reference)""")[0]
    assert both > 0

    per_month = dict(con.execute("""
        SELECT strftime(settled_on, '%Y-%m'), count(*)
        FROM sim_ext.halcyon_settlements
        WHERE settled_on BETWEEN DATE '2025-07-01' AND DATE '2025-09-30'
        GROUP BY 1""").fetchall())
    assert per_month["2025-09"] < per_month["2025-08"] * 0.8, per_month


def test_halcyon_learns_about_currency_only_after_e4(con):
    """The feed never learned about currency. Before the international
    launch that is harmless — everything was USD by construction — and after
    it, it is the trap."""
    assert _one(con, "SELECT count(*) FROM sim_ext.halcyon_settlements "
                     "WHERE settlement_currency <> 'USD' "
                     "AND txn_local_ts < DATE '2025-04-07'") == (0,)
    after = _one(con, """
        SELECT count(*), count(*) FILTER (WHERE settlement_currency <> 'USD')
        FROM sim_ext.halcyon_settlements WHERE txn_local_ts >= DATE '2025-04-07'""")
    assert after[0] > 0 and after[1] > 0, after


def test_the_marketplace_is_seller_facing(con):
    """The principal is money out and signs negative from the operator's
    side, and every payout nets to what the settlement lines say."""
    assert _one(con, "SELECT count(*) FROM sim_ext.marketplace_settlements "
                     "WHERE line_type = 'principal' AND amount >= 0") == (0,)
    assert _one(con, "SELECT count(*) FROM sim_ext.marketplace_settlements "
                     "WHERE line_type IN ('commission', 'fulfilment_fee') "
                     "AND amount < 0") == (0,)
    assert _one(con, """
        SELECT count(*) FROM sim_ext.marketplace_payouts p
        JOIN (SELECT payout_id, round(-sum(amount), 2) AS net
              FROM sim_ext.marketplace_settlements GROUP BY 1) s USING (payout_id)
        WHERE round(p.net_paid_amount, 2) <> s.net""") == (0,)
    # Every marketplace row belongs to an order on the marketplace channel.
    assert _one(con, """
        SELECT count(*) FROM sim_ext.marketplace_orders m
        JOIN sim_sales.orders o USING (order_id)
        JOIN sim_store.channels c ON c.channel_id = o.channel_id
        WHERE c.channel_type <> 'marketplace'""") == (0,)


def test_the_ad_platforms_bill_in_the_market_currency(con):
    """Their calendar covers every day of the range and their money is the
    market's, so the FX join is needed here too."""
    days, campaigns = _one(con, "SELECT count(DISTINCT report_date), "
                                "count(DISTINCT campaign_id) FROM sim_ext.ads_spend_daily")
    assert campaigns == 120
    assert days == (dt.date(2026, 6, 14) - dt.date(2024, 2, 4)).days + 1
    assert {r[0] for r in con.execute(
        "SELECT DISTINCT platform FROM sim_ext.ads_spend_daily").fetchall()} == \
        {"beacon", "tessera", "solstice"}
    assert len({r[0] for r in con.execute(
        "SELECT DISTINCT currency_code FROM sim_ext.ads_spend_daily").fetchall()}) > 1
    assert _one(con, "SELECT count(*) FROM sim_ext.ads_spend_daily "
                     "WHERE restated_at IS NOT NULL")[0] > 0
    assert _one(con, "SELECT count(*) FROM sim_ext.email_events "
                     "WHERE event_time_utc < DATE '2024-02-04'") == (0,)


# --- the generator contract -------------------------------------------------

def _fingerprint(con) -> dict:
    out = {}
    tables = con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema IN ('sim_identity', 'sim_nwv', 'sim_ext') "
        "AND table_type = 'BASE TABLE' ORDER BY 1, 2").fetchall()
    for schema, table in tables:
        out[f"{schema}.{table}"] = con.execute(
            f'SELECT count(*), coalesce(sum(hash(t)), 0) FROM {schema}."{table}" t'
        ).fetchone()
    return out


def test_two_builds_agree_row_for_row():
    a, b = _build(), _build()
    first, second = _fingerprint(a), _fingerprint(b)
    a.close()
    b.close()
    assert first == second
    assert len(first) > 15, sorted(first)
    assert all(n for n, _ in first.values()), \
        [t for t, (n, _) in first.items() if not n]


def test_both_profiles_carry_every_boundary():
    """The small profile scales fact volumes down and keeps every entity
    population and every era boundary, so an authored check crosses the same
    edges at either size."""
    small, full = _build("small"), _build("full")
    for con in (small, full):
        assert _one(con, "SELECT count(*) FROM sim_identity.trade_accounts") == (4000,)
        assert _one(con, "SELECT count(*) FROM sim_nwv.accounts") == (1400,)
        assert _one(con, "SELECT count(*) FROM sim_identity.merge_truth") == (360,)
        assert _one(con, "SELECT count(*) FROM sim_nwv.stores") == (44,)
        assert _one(con, "SELECT count(*) FROM sim_ext.meridian_events")[0] > 0
        assert _one(con, "SELECT count(*) FROM sim_ext.halcyon_settlements")[0] > 0
    facts = "SELECT count(*) FROM sim_nwv.order_lines"
    assert _one(small, facts)[0] < _one(full, facts)[0]
    small.close()
    full.close()
