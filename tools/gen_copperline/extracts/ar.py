"""S13 — trade invoicing, the nightly AR export out of Ironwood.

    raw.invoices        goods invoices and plan invoices, one row each
    raw.invoice_lines   goods, freight and plan lines
    raw.credit_memos    including the ones that post into a closed month
    raw.disputes        open ones do not reverse; settled ones do
    raw.plan_lines      the contracted service lines behind the plan invoices
    raw.payment_terms   14 rows of reference data
    landing/ar/dt=<ds>/ one file per table per night

`raw.gift_card_jurisdictions` is the seventh table of this source and does not
belong to the generator: spec 03 section 1 rule 7 makes it hand-authored,
along with `raw.entities` and `raw.market_config`. Nothing here writes it. The
gift-card side ships `jurisdiction_code` as `<market>-<nn>`, and the authored
file has to carry the same 66 codes.

## Where the rows come from

A third of Copperline's revenue moves through named trade accounts that buy on
terms, and none of it settles through a card processor. The goods half of that
is `sim_finance.ar_invoices` — one invoice per trade order — and the rest is
the AR module's own: trade programs, marketplace seller subscriptions and
fixture leases, billed on a plan. The upstream model summarizes the domain
into `ar_invoices` and stops there, so the plans, the credit memos and the
disputes are minted at this boundary, which is where they live in life too.

## What is armed here

**`customer_ref` carries the same two id formats as `raw.orders`** and flips
on the same date, so a crosswalk applied by date is wrong in both places.

**`nwv_account_id` is set instead of `customer_ref`** on the acquired book,
which never re-keyed. An invoice carries one or the other and never both.

**`posted_period` is not always the fiscal month of `invoice_date`.** About
one invoice in twenty-five posts to the following month, which is REV-8's
whole content, and `invoice_date` — not the posting month — is the FX date
recognition uses (REV-7).

**`billing_era`.** A legacy contract books on a monthly anniversary and a
current one books daily. The same column sits on `raw.customers`, drawn once
in `_boundary` so the two can only agree.

**`settled_cents` against `total_cents`.** Partial settlement is the aging
case BP-1 reads, and `invoice_status` says `partially_paid` where they differ.

**`changed_from_plan_line_id`.** A mid-term change closes one plan line and
opens another on the same day. Summing both without reading the dates
double-counts the month it happened in.

`line_kind` ships three of the four values chapter 03 lists: an `adjustment`
line has no population here, because an adjustment to an issued invoice is a
credit memo in this world and `raw.credit_memos` carries it.
"""

from __future__ import annotations

from ..config import Context
from ..streams import pick
from . import _boundary as b

# Plans, and their mix. Contract data: 1,720 plans exist at every profile,
# because the recognition clauses need every plan type on both sides of every
# era, and 1,720 rows cost nothing.
TRADE_PROGRAMS = 1_200
SELLER_SUBSCRIPTIONS = 180
FIXTURE_LEASES = 340
PLANS = TRADE_PROGRAMS + SELLER_SUBSCRIPTIONS + FIXTURE_LEASES

# Populations spec 03 section 18 states, before the profile divisor.
CREDIT_MEMOS = 11_700
CREDIT_MEMOS_AFTER_CLOSE = 340
DISPUTES = 1_600
DISPUTES_OPEN = 210

# Share of plans billed under a Northwave account, which never re-keyed.
NWV_PLAN_PERMILLE = 80


def build(ctx: Context) -> None:
    b.ensure_keys(ctx)
    b.ensure_fiscal(ctx)
    _payment_terms(ctx)
    _plan_lines(ctx)
    _invoices(ctx)
    _invoice_lines(ctx)
    _credit_memos(ctx)
    _disputes(ctx)
    _land(ctx)


def _payment_terms(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.payment_terms (
            payment_terms_code VARCHAR PRIMARY KEY,
            description VARCHAR NOT NULL,
            net_days INTEGER NOT NULL,
            discount_pct_bps INTEGER NOT NULL,
            discount_days INTEGER NOT NULL
        )
    """)
    ctx.con.executemany(
        "INSERT INTO raw.payment_terms VALUES (?,?,?,?,?)",
        [row[:5] for row in b.PAYMENT_TERMS])


def _plan_lines(ctx: Context) -> None:
    """One row per plan per billing period. A monthly plan bills on its
    anniversary day and an annual plan bills once a year; a mid-term change
    closes the period line it lands in and opens a second one that day."""
    hp = b.h(ctx, "plan_lines.plan", "g.plan_no")
    hl = b.h(ctx, "plan_lines.line", "s.plan_no", "s.i")
    plan_type = (f"CASE WHEN p.plan_no <= {TRADE_PROGRAMS} THEN 'trade_program' "
                 f"     WHEN p.plan_no <= {TRADE_PROGRAMS + SELLER_SUBSCRIPTIONS}"
                 f"          THEN 'seller_subscription' "
                 f"     ELSE 'fixture_lease' END")
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.plan_lines (
            plan_line_id VARCHAR PRIMARY KEY,
            plan_id VARCHAR NOT NULL,
            customer_ref VARCHAR,
            nwv_account_id VARCHAR,
            plan_type VARCHAR NOT NULL,
            billing_era VARCHAR NOT NULL,
            billing_frequency VARCHAR NOT NULL,
            term_start DATE NOT NULL,
            term_end DATE NOT NULL,
            amount_cents BIGINT NOT NULL,
            currency_code VARCHAR NOT NULL,
            changed_from_plan_line_id VARCHAR,
            cancelled_on DATE
        )
    """)
    # The plan register: who is billed, how often, and over what term.
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util._plans AS
        WITH p AS (
            SELECT g.plan_no, {hp} AS h FROM generate_series(1, {PLANS}) g(plan_no)
        ), t AS (
            SELECT p.*, {plan_type} AS plan_type,
                   CASE WHEN (p.h) % 1000 < {NWV_PLAN_PERMILLE} THEN true
                        ELSE false END AS on_nwv_book,
                   CASE WHEN (p.h >> 8) % 100 < 22 THEN 'annual'
                        ELSE 'monthly' END AS billing_frequency,
                   (DATE '{ctx.start}' - INTERVAL 1 DAY * ((p.h >> 16) % 500)
                    + INTERVAL 1 DAY * ((p.h >> 24) % 700))::DATE AS plan_start,
                   1 + (p.h >> 32) % 4000 AS trade_seq,
                   1 + (p.h >> 40) % 1400 AS nwv_seq
            FROM p
        )
        SELECT t.*,
               -- One, two or three years, whichever way it bills.
               (t.plan_start
                + INTERVAL 12 MONTH * (1 + (t.h >> 48) % 3))::DATE AS plan_end,
               (12000 + (t.h >> 52) % 480000)::BIGINT AS amount_cents
        FROM t
    """)
    def step(alias: str) -> str:
        """Months between billing periods: twelve on an annual plan, one on a
        monthly one."""
        return f"CASE WHEN {alias}.billing_frequency = 'annual' THEN 12 ELSE 1 END"
    ctx.sql(f"""
        INSERT INTO raw.plan_lines
        WITH span AS (
            SELECT p.*,
                   greatest(1, least(
                       datediff('month', p.plan_start,
                                least(p.plan_end, DATE '{ctx.end}'))
                           / {step('p')},
                       36)) AS periods
            FROM _util._plans p
            WHERE p.plan_start <= DATE '{ctx.end}'
        ), s AS (
            SELECT span.*, unnest(range(0, span.periods::INTEGER)) AS i FROM span
        ), l AS (
            SELECT s.*,
                   (s.plan_start + INTERVAL 1 MONTH * (s.i * {step('s')}))::DATE
                       AS period_start,
                   (s.plan_start + INTERVAL 1 MONTH * ((s.i + 1) * {step('s')})
                    - INTERVAL 1 DAY)::DATE AS period_end,
                   {hl} AS hl
            FROM s
        ), c AS (
            -- One period in twenty is amended mid-term: the line closes on the
            -- change date and a second line opens the same day.
            SELECT l.*, (l.hl % 100 < 5 AND l.i > 0) AS amended,
                   (l.period_start + INTERVAL 1 DAY
                    * (5 + (l.hl >> 8) % greatest(
                        datediff('day', l.period_start, l.period_end) - 6, 1)))::DATE
                       AS change_on
            FROM l
        ), e AS (
            SELECT c.*, 0 AS part FROM c
            UNION ALL
            SELECT c.*, 1 AS part FROM c WHERE c.amended
        ), f AS (
            -- The term this line actually bills, once the amendment has cut
            -- it. `cancelled_on` is measured against it below.
            SELECT e.*,
                   (CASE WHEN e.amended AND e.part = 1 THEN e.change_on
                         ELSE e.period_start END)::DATE AS line_start,
                   (CASE WHEN e.amended AND e.part = 0
                         THEN e.change_on - INTERVAL 1 DAY
                         ELSE e.period_end END)::DATE AS line_end
            FROM e
        )
        SELECT 'PL-' || lpad((f.plan_no * 100 + f.i * 2 + f.part)::VARCHAR, 7, '0'),
               'PLAN-' || lpad(f.plan_no::VARCHAR, 5, '0'),
               CASE WHEN NOT f.on_nwv_book THEN ck.customer_ref END,
               CASE WHEN f.on_nwv_book THEN
                    'NWA-' || lpad((10000 + (f.nwv_seq - 1) * 3)::VARCHAR, 5, '0') END,
               f.plan_type,
               coalesce(ck.billing_era,
                        CASE WHEN f.plan_start < DATE '{b.era_date(ctx, "E5_processor_overlap", "from")}'
                             THEN 'legacy' ELSE 'current' END),
               f.billing_frequency,
               f.line_start,
               f.line_end,
               CASE WHEN f.part = 1 THEN (f.amount_cents * 11) / 10
                    ELSE f.amount_cents END,
               'USD',
               CASE WHEN f.part = 1 THEN
                    'PL-' || lpad((f.plan_no * 100 + f.i * 2)::VARCHAR, 7, '0') END,
               -- The day the customer asked for the money back. It sits
               -- inside the term the line bills, because a line that ran to
               -- its term end was not cancelled at all: REV-6 reads this
               -- column and needs both sides of its 14-day boundary.
               CASE WHEN f.hl % 1000 < 18 THEN
                    least(f.line_start + INTERVAL 1 DAY
                          * (2 + (f.hl >> 16) % greatest(
                              datediff('day', f.line_start, f.line_end) - 2, 1)),
                          f.line_end - INTERVAL 1 DAY)::DATE END
        FROM f
        LEFT JOIN _util.customer_keys ck ON ck.trade_seq = f.trade_seq
        WHERE f.period_start <= DATE '{ctx.end}'
    """)
    ctx.sql("DROP TABLE _util._plans")


def _invoices(ctx: Context) -> None:
    e2 = b.era_date(ctx, "E2_customer_rekey")
    e4 = b.era_date(ctx, "E4_halcyon_non_usd")
    hs = b.h(ctx, "invoices.shape", "s.invoice_date", "s.source_key")
    posted = (
        "CASE WHEN (i.h) % 100 < 4 "
        "     THEN (SELECT period_label FROM _util.fiscal_month f2 "
        "           WHERE f2.cal_date = i.invoice_date + INTERVAL 32 DAY) "
        "     ELSE f.period_label END"
    )
    settled = (
        "CASE WHEN i.invoice_status = 'paid' THEN i.gross_cents "
        "     WHEN i.invoice_status = 'partially_paid' "
        "     THEN (i.gross_cents * (20 + (i.h >> 8) % 60)) / 100 "
        "     ELSE 0 END"
    )
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.invoices (
            invoice_id VARCHAR PRIMARY KEY,
            invoice_number VARCHAR NOT NULL,
            customer_ref VARCHAR,
            nwv_account_id VARCHAR,
            order_id VARCHAR,
            entity_code VARCHAR NOT NULL,
            market_code VARCHAR NOT NULL,
            invoice_date DATE NOT NULL,
            service_start DATE,
            service_end DATE,
            due_date DATE NOT NULL,
            payment_terms_code VARCHAR NOT NULL,
            billing_era VARCHAR NOT NULL,
            currency_code VARCHAR,
            fx_rate_ppm BIGINT,
            net_cents BIGINT NOT NULL,
            tax_cents BIGINT NOT NULL,
            total_cents BIGINT NOT NULL,
            settled_cents BIGINT NOT NULL,
            invoice_status VARCHAR NOT NULL,
            posted_period VARCHAR NOT NULL,
            loaded_at TIMESTAMP NOT NULL
        )
    """)
    # Two populations, one table: the goods invoice behind a trade order, and
    # the plan invoice behind a contracted service line.
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util._invoice_src AS
        SELECT 'G' AS kind,
               a.ar_invoice_id AS source_key,
               k.raw_order_id AS order_id,
               NULL::VARCHAR AS plan_line_id,
               ck.customer_ref AS customer_ref_now,
               ck.era_ref_before AS customer_ref_before,
               NULL::VARCHAR AS nwv_account_id,
               ck.payment_terms_code, ck.billing_era,
               k.market_code, k.currency_code,
               a.invoice_date, a.due_date,
               NULL::DATE AS service_start, NULL::DATE AS service_end,
               {b.cents('a.total_amount')} AS total_cents,
               {b.cents('a.amount_settled')} AS settled_amount,
               a.invoice_status AS src_status
        FROM sim_finance.ar_invoices a
        JOIN _util.order_keys k ON k.order_id = a.order_id
        LEFT JOIN _util.customer_keys ck ON ck.customer_id = a.customer_id
        UNION ALL
        SELECT 'P',
               p.plan_no * 1000 + p.i,
               NULL,
               p.plan_line_id,
               p.customer_ref, ck.era_ref_before, p.nwv_account_id,
               coalesce(ck.payment_terms_code, 'NET30'), p.billing_era,
               'US', 'USD',
               p.term_start,
               (p.term_start + INTERVAL 30 DAY)::DATE,
               p.term_start, p.term_end,
               p.amount_cents, 0, 'issued'
        FROM (
            SELECT l.*, ck2.trade_seq,
                   (split_part(l.plan_id, '-', 2))::INTEGER AS plan_no,
                   row_number() OVER (PARTITION BY l.plan_id ORDER BY l.term_start,
                                      l.plan_line_id) - 1 AS i
            FROM raw.plan_lines l
            LEFT JOIN _util.customer_keys ck2 ON ck2.customer_ref = l.customer_ref
        ) p
        LEFT JOIN _util.customer_keys ck ON ck.trade_seq = p.trade_seq
    """)
    # A goods invoice keeps the settlement the sim gave it; a plan invoice is
    # settled by age, because the AR module is where that fact lives.
    status = (
        "CASE WHEN n.kind = 'G' THEN "
        "         CASE n.src_status WHEN 'paid' THEN 'paid' "
        "              WHEN 'partially_paid' THEN 'partially_paid' "
        "              ELSE 'issued' END "
        f"    WHEN n.invoice_date + INTERVAL 45 DAY < DATE '{ctx.end}' "
        "          AND (n.h) % 100 < 88 THEN 'paid' "
        "     WHEN (n.h) % 100 < 94 THEN 'partially_paid' "
        "     ELSE 'issued' END"
    )
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util._invoice_num AS
        WITH n AS (
            SELECT s.*, {hs} AS h,
                   row_number() OVER (ORDER BY s.invoice_date, s.kind,
                                      s.source_key) AS invoice_no,
                   -- A goods invoice arrives from AR gross of tax, so its net
                   -- is the gross less the tax. A plan invoice carries the
                   -- plan's own amount, which is the net, and tax goes on top
                   -- of it. Either way `net_cents` is the revenue and
                   -- `total_cents` is what the customer owes.
                   CASE WHEN s.kind = 'P' THEN s.total_cents
                        ELSE s.total_cents
                             - round(s.total_cents * 0.0833)::BIGINT END
                       AS net_cents,
                   round(s.total_cents * 0.0833)::BIGINT AS tax_cents,
                   CASE WHEN s.kind = 'P'
                        THEN s.total_cents + round(s.total_cents * 0.0833)::BIGINT
                        ELSE s.total_cents END AS gross_cents
            FROM _util._invoice_src s
        )
        SELECT n.*, {status} AS invoice_status,
               'INV-' || lpad(n.invoice_no::VARCHAR, 7, '0') AS invoice_id
        FROM n
    """)
    ctx.sql(f"""
        INSERT INTO raw.invoices
        WITH i AS (SELECT * FROM _util._invoice_num)
        SELECT 'INV-' || lpad(i.invoice_no::VARCHAR, 7, '0'),
               'IN-' || year(i.invoice_date)::VARCHAR || '-'
                     || lpad(i.invoice_no::VARCHAR, 7, '0'),
               CASE WHEN i.nwv_account_id IS NOT NULL THEN NULL
                    WHEN i.invoice_date < DATE '{e2}' THEN i.customer_ref_before
                    ELSE i.customer_ref_now END,
               i.nwv_account_id,
               i.order_id,
               {b.entity_at('i.market_code', 'i.invoice_date')},
               i.market_code,
               i.invoice_date, i.service_start, i.service_end, i.due_date,
               i.payment_terms_code, i.billing_era,
               CASE WHEN i.invoice_date >= DATE '{e4}' THEN i.currency_code END,
               CASE WHEN i.invoice_date >= DATE '{e4}'
                    THEN coalesce(round(fx.rate * 1000000)::BIGINT, 1000000) END,
               i.net_cents,
               i.tax_cents,
               i.gross_cents,
               CASE WHEN i.kind = 'G' THEN i.settled_amount ELSE {settled} END,
               i.invoice_status,
               {posted},
               {b.minute(b.loaded_at(ctx, 'invoices', 'i.invoice_date::TIMESTAMP'
                                     ' + INTERVAL 20 HOUR',
                                     ('i.invoice_date', 'i.source_key')))}
        FROM i
        LEFT JOIN _util.fiscal_month f ON f.cal_date = i.invoice_date
        LEFT JOIN sim_core.exchange_rates fx
               ON fx.rate_date = i.invoice_date
              AND fx.from_currency = i.currency_code AND fx.to_currency = 'USD'
        WHERE i.invoice_date BETWEEN DATE '{ctx.start}' AND DATE '{ctx.end}'
    """)
    # Which plan line each plan invoice bills. The line builder joins this
    # rather than matching on the customer and the term dates, which two plans
    # of one customer can share.
    ctx.sql("""
        CREATE OR REPLACE TABLE _util.invoice_plan_line AS
        SELECT n.invoice_id, n.plan_line_id
        FROM _util._invoice_num n
        JOIN raw.invoices v ON v.invoice_id = n.invoice_id
        WHERE n.plan_line_id IS NOT NULL
    """)
    ctx.sql("DROP TABLE _util._invoice_src")
    ctx.sql("DROP TABLE _util._invoice_num")


def _invoice_lines(ctx: Context) -> None:
    """Goods invoices carry the order's lines and a freight line; a plan
    invoice carries the plan line it bills. Only `plan` lines recognize
    ratably, which is REV-1's whole content."""
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.invoice_lines (
            invoice_line_id VARCHAR NOT NULL,
            invoice_id VARCHAR NOT NULL,
            line_no INTEGER NOT NULL,
            line_kind VARCHAR NOT NULL,
            sku VARCHAR,
            plan_line_id VARCHAR,
            qty DECIMAL(12,3),
            unit_price_cents BIGINT,
            line_discount_cents BIGINT NOT NULL,
            tax_cents BIGINT NOT NULL,
            line_total_cents BIGINT NOT NULL,
            service_start DATE,
            service_end DATE,
            revenue_account_code VARCHAR NOT NULL,
            PRIMARY KEY (invoice_id, invoice_line_id)
        )
    """)
    ctx.sql(f"""
        INSERT INTO raw.invoice_lines
        WITH goods AS (
            SELECT v.invoice_id, l.line_no, 'goods' AS line_kind, s.sku,
                   NULL::VARCHAR AS plan_line_id, l.qty,
                   {b.cents('l.unit_price')} AS unit_price_cents,
                   {b.cents('l.line_discount_amount')} AS line_discount_cents,
                   {b.cents('l.tax_amount')} AS tax_cents,
                   {b.cents('l.line_total')} AS line_total_cents,
                   NULL::DATE AS service_start, NULL::DATE AS service_end,
                   '4000' AS revenue_account_code
            FROM raw.invoices v
            JOIN _util.order_keys k ON k.raw_order_id = v.order_id
            JOIN sim_sales.order_lines l ON l.order_id = k.order_id
            JOIN _util.sku_keys s ON s.variant_id = l.variant_id
            WHERE v.order_id IS NOT NULL
        ), freight AS (
            SELECT v.invoice_id, 90, 'freight', NULL, NULL, NULL,
                   NULL, 0, 0, {b.cents('k.shipping_amount')}, NULL, NULL, '4400'
            FROM raw.invoices v
            JOIN _util.order_keys k ON k.raw_order_id = v.order_id
            WHERE v.order_id IS NOT NULL AND k.shipping_amount > 0
        ), plan AS (
            SELECT m.invoice_id, 1, 'plan', NULL, p.plan_line_id, 1.000,
                   p.amount_cents, 0,
                   round(p.amount_cents * 0.0833)::BIGINT,
                   p.amount_cents, p.term_start, p.term_end, '4100'
            FROM _util.invoice_plan_line m
            JOIN raw.plan_lines p ON p.plan_line_id = m.plan_line_id
        ), u AS (
            SELECT * FROM goods UNION ALL SELECT * FROM freight
            UNION ALL SELECT * FROM plan
        )
        SELECT 'L-' || lpad(u.line_no::VARCHAR, 2, '0'), u.invoice_id, u.line_no,
               u.line_kind, u.sku, u.plan_line_id, u.qty, u.unit_price_cents,
               u.line_discount_cents, u.tax_cents, u.line_total_cents,
               u.service_start, u.service_end, u.revenue_account_code
        FROM u
    """)


def _credit_memos(ctx: Context) -> None:
    """A memo issued after the month closed books to the earliest open month,
    which is REV-5, and the fixture carries rows on both sides of it."""
    n = ctx.scale(CREDIT_MEMOS)
    after_close = ctx.scale(CREDIT_MEMOS_AFTER_CLOSE)
    hm = b.h(ctx, "credit_memos.shape", "v.invoice_id")
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.credit_memos (
            credit_memo_id VARCHAR PRIMARY KEY,
            invoice_id VARCHAR NOT NULL,
            invoice_line_id VARCHAR,
            issued_on DATE NOT NULL,
            reason_code VARCHAR NOT NULL,
            amount_cents BIGINT NOT NULL,
            currency_code VARCHAR NOT NULL,
            applies_to_period VARCHAR NOT NULL,
            approved_by VARCHAR NOT NULL,
            loaded_at TIMESTAMP NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO raw.credit_memos
        WITH picked AS (
            SELECT v.invoice_id, v.invoice_date, v.total_cents, v.currency_code,
                   {hm} AS h,
                   row_number() OVER (ORDER BY {hm}) AS rn
            FROM raw.invoices v
            WHERE v.invoice_status <> 'void'
            QUALIFY rn <= {n}
        ), m AS (
            -- The last {after_close} of the drawn set are issued well after
            -- the month they apply to had closed; the rest land inside it.
            SELECT p.*,
                   (p.invoice_date + INTERVAL 1 DAY
                    * CASE WHEN p.rn > {n - after_close}
                           THEN 45 + (p.h >> 8) % 40
                           ELSE 2 + (p.h >> 8) % 22 END)::DATE AS issued_on
            FROM picked p
        )
        SELECT 'CM-' || lpad(m.rn::VARCHAR, 7, '0'),
               m.invoice_id,
               CASE WHEN (m.h >> 16) % 100 < 62
                    THEN 'L-' || lpad((1 + (m.h >> 20) % 3)::VARCHAR, 2, '0') END,
               m.issued_on,
               {pick('(m.h) >> 24',
                     [("'price_adjustment'", 34), ("'return'", 31),
                      ("'goodwill'", 20), ("'dispute_settlement'", 15)])},
               greatest(100, (m.total_cents * (5 + (m.h >> 32) % 60)) / 100)::BIGINT,
               coalesce(m.currency_code, 'USD'),
               f.period_label,
               'ar.' || lpad(((m.h >> 40) % 8)::VARCHAR, 2, '0'),
               {b.minute(b.loaded_at(ctx, 'credit_memos',
                                     'm.issued_on::TIMESTAMP + INTERVAL 19 HOUR',
                                     ('m.invoice_id',)))}
        FROM m
        LEFT JOIN _util.fiscal_month f ON f.cal_date = m.invoice_date
        WHERE m.issued_on <= DATE '{ctx.end}'
    """)


def _disputes(ctx: Context) -> None:
    """An open dispute does not reverse recognition; a settled one reverses in
    the period it settled, never in the period of the invoice (REV-10)."""
    n = ctx.scale(DISPUTES)
    still_open = ctx.scale(DISPUTES_OPEN)
    hd = b.h(ctx, "disputes.shape", "v.invoice_id")
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.disputes (
            dispute_id VARCHAR PRIMARY KEY,
            invoice_id VARCHAR NOT NULL,
            raised_on DATE NOT NULL,
            resolved_on DATE,
            dispute_status VARCHAR NOT NULL,
            disputed_cents BIGINT NOT NULL,
            settled_cents BIGINT NOT NULL,
            reason_code VARCHAR NOT NULL,
            owner VARCHAR NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO raw.disputes
        WITH picked AS (
            SELECT v.invoice_id, v.invoice_date, v.total_cents, {hd} AS h,
                   row_number() OVER (ORDER BY {hd}) AS rn
            FROM raw.invoices v
            WHERE v.invoice_date <= DATE '{ctx.end}' - 20
            QUALIFY rn <= {n}
        ), dated AS (
            SELECT p.*,
                   (p.invoice_date + INTERVAL 1 DAY * (6 + (p.h >> 8) % 40))::DATE
                       AS raised_on
            FROM picked p
        ), d AS (
            -- The open quota counts rows that survive the raised_on cut, or
            -- a dispute raised past range end would silently eat the quota:
            -- at the full profile that left 207 open where the world says 210.
            SELECT dated.*,
                   row_number() OVER (ORDER BY dated.rn) <= {still_open}
                       AS still_open
            FROM dated
            WHERE dated.raised_on <= DATE '{ctx.end}'
        )
        SELECT 'DSP-' || lpad(d.rn::VARCHAR, 6, '0'),
               d.invoice_id,
               d.raised_on,
               CASE WHEN NOT d.still_open
                    THEN (d.raised_on + INTERVAL 1 DAY * (10 + (d.h >> 16) % 60))::DATE
                    END,
               CASE WHEN d.still_open THEN 'open'
                    ELSE {pick('(d.h) >> 24', [("'settled'", 46), ("'upheld'", 30),
                                               ("'rejected'", 24)])} END,
               greatest(100, (d.total_cents * (10 + (d.h >> 32) % 80)) / 100)::BIGINT,
               0,
               {pick('(d.h) >> 44', [("'price'", 32), ("'delivery_shortfall'", 26),
                                     ("'damaged_goods'", 22), ("'duplicate_billing'", 20)])},
               'credit.' || lpad(((d.h >> 48) % 6)::VARCHAR, 2, '0')
        FROM d
    """)
    # A settled dispute gives back part of what was disputed; nothing else does.
    ctx.sql(f"""
        UPDATE raw.disputes SET settled_cents =
            (disputed_cents * (30 + ({b.h(ctx, 'disputes.settled', 'dispute_id')})
                                     % 60)) / 100
        WHERE dispute_status = 'settled'
    """)
    open_now = ctx.sql(
        "SELECT count(*) FROM raw.disputes WHERE dispute_status = 'open'"
    ).fetchone()[0]
    assert open_now == still_open, (open_now, still_open)


def _land(ctx: Context) -> None:
    """One file per table per night. The AR export is cut on the invoice date
    and the memo and dispute files carry the day they were written."""
    live = b.live_from(ctx)
    b.land(ctx, "ar", "invoices.csv",
           f"SELECT invoice_date AS dt, * FROM raw.invoices "
           f"WHERE invoice_date >= DATE '{live}' "
           "ORDER BY invoice_date, invoice_id")
    b.land(ctx, "ar", "invoice_lines.csv", f"""
        SELECT v.invoice_date AS dt, l.*
        FROM raw.invoice_lines l JOIN raw.invoices v USING (invoice_id)
        WHERE v.invoice_date >= DATE '{live}'
        ORDER BY v.invoice_date, l.invoice_id, l.line_no
    """)
    b.land(ctx, "ar", "credit_memos.csv",
           f"SELECT issued_on AS dt, * FROM raw.credit_memos "
           f"WHERE issued_on >= DATE '{live}' "
           "ORDER BY issued_on, credit_memo_id")
    b.land(ctx, "ar", "disputes.csv",
           f"SELECT raised_on AS dt, * FROM raw.disputes "
           f"WHERE raised_on >= DATE '{live}' "
           "ORDER BY raised_on, dispute_id")
    b.land(ctx, "ar", "plan_lines.csv",
           "SELECT term_start AS dt, * FROM raw.plan_lines "
           f"WHERE term_start BETWEEN DATE '{live}' AND DATE '{ctx.end}' "
           "ORDER BY term_start, plan_line_id")
