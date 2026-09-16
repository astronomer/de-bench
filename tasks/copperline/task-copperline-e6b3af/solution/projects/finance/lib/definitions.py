"""The three revenue definitions on one page, a fiscal month at a time.

`docs/semantic-definitions.md` reserves three money names that look alike and
are not. `booked_cents` counts an order on the day it was taken, gross, and
never moves again. `recognized_cents` follows the recognition schedule of
`docs/finance-policy.md`. `reported_cents` is what the board pack shows, which
is recognised revenue net of credits with the legacy-era plans taken out, per
`contracts/board-pack.md` §BP-2. This module builds the three of them side by
side, with the two amounts that separate the second from the third, so that the
meeting has one page instead of three numbers.

The page is the trade book: the trade orders, the invoices they raise, and the
trade agreements. Marketplace commission and the gift-card tender are the close
file's own steps and are not on it — the same population `marts.account_rollup`
is built from.

Every figure is USD cents. An amount in another currency converts once, at the
rate its own row carries, before anything is allocated (§REV-7), rounded half
up to the cent.

The clauses, and where each one lands below:

* §REV-1  a line with no `service_end` recognises in full on the invoice date;
          a line with one recognises across its service period, both ends in.
* §REV-2  the daily amount is the floor, and the remainder lands on the final
          day of the period. Never a fraction of a cent, and never a per-day
          amount rounded up, which leaves the schedule short of the line.
* §REV-4  nothing is ever re-rated in place.
* §REV-6  a refund asked for within 14 days of the invoice voids the schedule:
          what the line had recognised reverses in the month the refund fell
          in, and it recognises nothing after that. A refund after day 14 stops
          the schedule where it stands, and the days after it never recognise.
* §REV-11 a legacy-era agreement billed monthly recognises at fiscal-month
          grain: the whole amount lands in the fiscal month its service period
          starts in, and no day of it reaches the next one. A legacy-era
          agreement billed annually is a prepay and is allocated like any other.
* §BP-2   the board pack excludes legacy-era plans. It excludes the plans, not
          the goods a legacy-era account bought.
* §REV-5  a credit memo books on its own issue date, at the rate its invoice
          converted at. `applies_to_period` on the row is what the credit was
          raised against, which is a different question.
"""

from __future__ import annotations

from include.lib import warehouse

__all__ = ["TABLE", "COLUMNS", "build_monthly"]

#: The page. One row per fiscal month, and the month is replaced whole.
TABLE = "marts.revenue_definitions_monthly"

COLUMNS = ["fiscal_month", "booked_cents", "recognized_cents", "reported_cents",
           "plan_cents", "legacy_plan_cents", "credit_cents"]

#: §REV-7 holds a rate as integer parts per million. One conversion per row,
#: rounded half up to the cent, before anything is allocated.
_USD = "({amount} * coalesce({ppm}, 1000000) + 500000) // 1000000"

#: §REV-6. Day 14 is inside the window; day 15 is outside it.
_VOID_WINDOW_DAYS = 14


def _sql() -> str:
    """The whole page in one statement. Every name is qualified onto the live
    warehouse: `nwv` sits first on the search path and carries several of them."""
    calendar = warehouse.qualify("raw.fiscal_calendar")
    orders = warehouse.qualify("raw.orders")
    invoices = warehouse.qualify("raw.invoices")
    invoice_lines = warehouse.qualify("raw.invoice_lines")
    plan_lines = warehouse.qualify("raw.plan_lines")
    credit_memos = warehouse.qualify("raw.credit_memos")

    order_usd = _USD.format(amount="o.subtotal_cents", ppm="o.fx_rate_ppm")
    line_usd = _USD.format(amount="l.line_total_cents", ppm="i.fx_rate_ppm")
    credit_usd = _USD.format(amount="m.amount_cents", ppm="i.fx_rate_ppm")

    return f"""
WITH months AS (
    SELECT fiscal_year || '-P' || lpad(CAST(fiscal_period AS VARCHAR), 2, '0') AS fiscal_month,
           min(cal_date) AS month_start,
           max(cal_date) AS month_end
    FROM {calendar}
    GROUP BY 1
),

bounds AS (
    SELECT fiscal_month, month_start, month_end FROM months WHERE fiscal_month = ?
),

-- `booked_cents`: the order on the day it was taken, gross. Nothing that
-- happened to it afterwards moves it — not a cancellation, not a return, not a
-- credit. The two rows staging drops everywhere else are dropped here too.
booked AS (
    SELECT coalesce(sum({order_usd}), 0)::BIGINT AS cents
    FROM {orders} o, bounds b
    WHERE o.channel = 'trade'
      AND o.local_order_date BETWEEN b.month_start AND b.month_end
      AND NOT coalesce(o.is_test, false)
      AND o.deleted_at IS NULL
),

-- §REV-1, last sentence: a line with no service period recognises in full on
-- the invoice date. That is the goods and freight book, and it is most of the
-- number. The invoice date decides, never the period the header was posted to.
point_in_time AS (
    SELECT coalesce(sum({line_usd}), 0)::BIGINT AS cents
    FROM {invoice_lines} l
    JOIN {invoices} i ON i.invoice_id = l.invoice_id, bounds b
    WHERE l.service_end IS NULL
      AND i.invoice_date BETWEEN b.month_start AND b.month_end
),

-- Every ratable line, converted once, with its own clock beside it.
ratable AS (
    SELECT {line_usd} AS amount_cents,
           l.service_start,
           l.service_end,
           date_diff('day', l.service_start, l.service_end) + 1 AS term_days,
           i.invoice_date,
           p.billing_era,
           p.billing_frequency,
           p.cancelled_on
    FROM {invoice_lines} l
    JOIN {invoices} i ON i.invoice_id = l.invoice_id
    JOIN {plan_lines} p ON p.plan_line_id = l.plan_line_id
    WHERE l.service_end IS NOT NULL
),

classified AS (
    SELECT r.*,
           r.billing_era = 'legacy' AND r.billing_frequency = 'monthly' AS month_grain,
           r.cancelled_on IS NOT NULL
               AND r.cancelled_on <= r.invoice_date + {_VOID_WINDOW_DAYS}     AS is_void,
           r.cancelled_on IS NOT NULL
               AND r.cancelled_on > r.invoice_date + {_VOID_WINDOW_DAYS}
               AND r.cancelled_on < r.service_end                             AS is_stopped,
           r.amount_cents // r.term_days                                      AS daily_cents,
           r.amount_cents - (r.amount_cents // r.term_days) * r.term_days     AS remainder_cents,
           (SELECT m.fiscal_month FROM months m
             WHERE r.service_start BETWEEN m.month_start AND m.month_end)     AS start_month,
           (SELECT m.fiscal_month FROM months m
             WHERE r.cancelled_on BETWEEN m.month_start AND m.month_end)      AS void_month,
           (SELECT m.month_start FROM months m
             WHERE r.cancelled_on BETWEEN m.month_start AND m.month_end)      AS void_month_start
    FROM ratable r
),

-- One line's contribution to the month, under §REV-1, §REV-2, §REV-6 and
-- §REV-11. A month-grain line lands whole or not at all. A daily line lands
-- its days, and the final day of a period that ran to the end carries the
-- remainder. A voided line lands its days until the month the refund fell in,
-- and that month carries the reversal of everything it had recognised.
per_line AS (
    SELECT c.billing_era,
           CASE
               WHEN c.month_grain
               THEN CASE WHEN c.start_month = b.fiscal_month THEN c.amount_cents ELSE 0 END
                  - CASE WHEN c.is_void AND c.void_month = b.fiscal_month
                         THEN c.amount_cents ELSE 0 END
               ELSE c.daily_cents * o.days
                  + CASE WHEN o.pays_remainder THEN c.remainder_cents ELSE 0 END
                  - CASE WHEN o.reverses THEN o.recognised_before ELSE 0 END
           END AS cents
    FROM classified c, bounds b,
         LATERAL (
             SELECT
                 -- the days of this month the line still recognises
                 greatest(
                     date_diff('day',
                         greatest(b.month_start, c.service_start),
                         least(
                             b.month_end,
                             CASE WHEN c.is_stopped THEN c.cancelled_on ELSE c.service_end END,
                             CASE WHEN c.is_void THEN c.void_month_start - 1
                                  ELSE DATE '9999-12-31' END
                         )
                     ) + 1, 0) AS days,
                 -- only a period that ran to its final day has a remainder to
                 -- pay, and a voided line pays it only in a month the reversal
                 -- has not reached yet
                 NOT c.is_stopped
                     AND c.service_end BETWEEN b.month_start AND b.month_end
                     AND (NOT c.is_void OR c.service_end < c.void_month_start) AS pays_remainder,
                 c.is_void AND c.void_month = b.fiscal_month                 AS reverses,
                 -- everything the line had recognised before the month of the refund
                 CASE WHEN c.is_void THEN
                     c.daily_cents * greatest(
                         date_diff('day', c.service_start,
                                   least(c.void_month_start - 1, c.service_end)) + 1, 0)
                   + CASE WHEN c.service_end < c.void_month_start
                          THEN c.remainder_cents ELSE 0 END
                 ELSE 0 END                                                  AS recognised_before
         ) AS o
),

plan_book AS (
    SELECT coalesce(sum(cents), 0)::BIGINT AS cents,
           coalesce(sum(cents) FILTER (WHERE billing_era = 'legacy'), 0)::BIGINT AS legacy_cents
    FROM per_line
),

-- §REV-5: the credit books on its issue date, at the rate its invoice
-- converted at. Positive here; it is subtracted on the page.
credits AS (
    SELECT coalesce(sum({credit_usd}), 0)::BIGINT AS cents
    FROM {credit_memos} m
    JOIN {invoices} i ON i.invoice_id = m.invoice_id, bounds b
    WHERE m.issued_on BETWEEN b.month_start AND b.month_end
)

SELECT (SELECT cents FROM booked)::BIGINT                                    AS booked_cents,
       ((SELECT cents FROM point_in_time)
        + (SELECT cents FROM plan_book))::BIGINT                             AS recognized_cents,
       ((SELECT cents FROM point_in_time)
        + (SELECT cents FROM plan_book)
        - (SELECT legacy_cents FROM plan_book)
        - (SELECT cents FROM credits))::BIGINT                               AS reported_cents,
       (SELECT cents FROM plan_book)::BIGINT                                 AS plan_cents,
       (SELECT legacy_cents FROM plan_book)::BIGINT                          AS legacy_plan_cents,
       (SELECT cents FROM credits)::BIGINT                                   AS credit_cents
"""


def build_monthly(fiscal_month: str, table: str = TABLE) -> int:
    """Replace one fiscal month of the page. Returns the rows written.

    `include.lib.warehouse.delete_insert` partitions on a date and this table is
    keyed by a 4-5-4 fiscal period, which is not one, so the delete and the
    insert are written out here in one transaction — the same shape, and the
    same rule, as `ledger.write_breaks`. A second run of a month leaves one row.
    """
    target = warehouse.qualify(table)
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('marts')}")
        con.execute(
            f"""CREATE TABLE IF NOT EXISTS {target} (
                fiscal_month VARCHAR NOT NULL,
                booked_cents BIGINT NOT NULL,
                recognized_cents BIGINT NOT NULL,
                reported_cents BIGINT NOT NULL,
                plan_cents BIGINT NOT NULL,
                legacy_plan_cents BIGINT NOT NULL,
                credit_cents BIGINT NOT NULL)"""
        )
        figures = con.execute(_sql(), [fiscal_month]).fetchone()
        try:
            con.execute("BEGIN TRANSACTION")
            con.execute(f"DELETE FROM {target} WHERE fiscal_month = ?", [fiscal_month])
            con.execute(
                f"INSERT INTO {target} ({', '.join(COLUMNS)}) "
                f"VALUES ({', '.join('?' for _ in COLUMNS)})",
                [fiscal_month, *figures],
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return 1
