"""Recognized revenue per legal entity per closed fiscal month.

This model is written from `worlds/copperline/workspace/docs/finance-policy.md`
and the raw tables alone. Every rule below cites the clause it comes from
(REV-1 .. REV-14, REV-16). Where the policy does not decide a question, the
choice is marked `AMBIGUITY A-n` and `AMBIGUITIES.md` beside this file says
what was chosen and why.

Money
-----
Every amount is an integer number of cents, as the policy's "Money, days and
time zones" section requires. Integers are exact, so the accumulators are
plain `int`. `decimal.Decimal` carries every step where a ratio appears — the
foreign-currency conversion of REV-7 and the daily amount of REV-2 — and the
only rounding modes used are ROUND_HALF_UP (conversion) and ROUND_FLOOR (the
daily amount, which REV-2 names as `floor`). No float is constructed anywhere.

Days
----
A day is a business date in America/Los_Angeles ("Money, days and time
zones"). Feeds that carry a UTC timestamp are converted once, on the way in,
by `business_date`. Feeds that carry a DATE are already business dates.

Output
------
One row per (legal entity, closed fiscal month), sorted. `recognized_cents`
is the number the ledger tie of REV-9 would measure. The other money columns
are the sub-lines the clauses require to defend it, and they add up to it —
except the two balance columns, which are closing balances rather than flows
and are memo columns.

Run it:

    PYTHONPATH=tools python -m copperline_oracle.recognition <copperline.duckdb>
"""

from __future__ import annotations

import argparse
import bisect
import csv
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
from pathlib import Path
from typing import Iterable, Iterator, Optional
from zoneinfo import ZoneInfo

import duckdb

#: The head-office zone. "Business dates are America/Los_Angeles, the zone of
#: the Portland head office" — finance-policy, "Money, days and time zones".
HEAD_OFFICE = ZoneInfo("America/Los_Angeles")

#: REV-7 holds a rate as "integer parts per million on the row".
PPM = Decimal(1_000_000)

#: REV-7: "Rows dated before the currency cutover carry `currency_code` NULL
#: and are USD by construction." USD is the reporting currency: it is the one
#: currency `raw.fx_rates` holds no row for, and every rate in that table is
#: `rate_to_usd_ppm`.
REPORTING_CURRENCY = "USD"

#: REV-8: "Copperline closes a fiscal month on the 5th business day of the
#: following month."
CLOSE_BUSINESS_DAY = 5

#: REV-6: "A refund requested within 14 days of the invoice date voids the
#: schedule... day 14 is inside the window."
VOID_WINDOW_DAYS = 14

#: REV-12: "Unredeemed value recognizes as breakage 24 months after issue".
BREAKAGE_MONTHS = 24

#: The money columns of the output, in order. Every one is a flow into the
#: fiscal month, in cents, and together they sum to `recognized_cents`.
COMPONENTS = (
    "ratable_current_cents",     # REV-1, REV-2, REV-3
    "ratable_legacy_cents",      # REV-11
    "point_in_time_cents",       # REV-1, last sentence
    "marketplace_cents",         # REV-14
    "gift_card_redemption_cents",  # REV-12
    "breakage_cents",            # REV-12
    "credit_memo_cents",         # REV-5
    "dispute_writeoff_cents",    # REV-10
    "refund_reversal_cents",     # REV-6
)

#: The memo columns: balances at the close of the month, not flows into it.
#: They are not part of `recognized_cents`.
BALANCES = (
    "deferred_closing_cents",         # REV-3, the contract liability
    "gift_card_liability_cents",      # REV-12, unredeemed card value
)

COLUMNS = (
    "entity_code",
    "fiscal_month",
    "recognized_cents",
    *COMPONENTS,
    *BALANCES,
)


class PolicyError(RuntimeError):
    """The data cannot be recognized under the policy as written."""


# ---------------------------------------------------------------------------
# Days and money
# ---------------------------------------------------------------------------

def business_date(stamp: datetime) -> date:
    """The head-office business date of a UTC event timestamp.

    "Event timestamps in the source feeds are UTC and are converted once, on
    the way in... A 'day' in this policy is a business date in the head-office
    zone; it is not a UTC day and it is not a store-local day."
    """
    return stamp.replace(tzinfo=timezone.utc).astimezone(HEAD_OFFICE).date()


def floor_div(cents: int, days: int) -> int:
    """`floor(line_cents / days)` of REV-2, in exact arithmetic."""
    if days <= 0:
        raise PolicyError(f"a service period of {days} days has no daily amount")
    return int((Decimal(cents) / Decimal(days)).to_integral_value(ROUND_FLOOR))


def add_months(day: date, months: int) -> date:
    """`day` plus whole months, clamped to the end of the target month.

    REV-12 counts breakage "24 months after issue" and says nothing about a
    card issued on the 31st. Clamping is the only reading that keeps every
    card's anniversary inside a month (AMBIGUITY A-9).
    """
    total = day.month - 1 + months
    year = day.year + total // 12
    month = total % 12 + 1
    last = (date(year + month // 12, month % 12 + 1, 1) - timedelta(days=1)).day
    return date(year, month, min(day.day, last))


# ---------------------------------------------------------------------------
# REV-16 / CAL-2 the fiscal calendar, and REV-8 the close
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FiscalMonth:
    """One fiscal period of `raw.fiscal_calendar`, 4 or 5 weeks long."""

    key: str          # 'FY2026-P04', the shape `posted_period` uses
    start: date
    end: date
    close: Optional[date]  # REV-8; None when the calendar runs out


class Calendar:
    """`raw.fiscal_calendar`, read, never computed.

    CAL-2: "It is the calendar. A model that needs a fiscal attribute joins
    to it... and does not compute one." Nothing here derives a fiscal date by
    arithmetic; the close date of REV-8 is the one exception, and the policy
    states it as a rule rather than shipping it as a column.
    """

    def __init__(self, rows: Iterable[tuple[date, str, int]]):
        spans: dict[str, tuple[date, date]] = {}
        for cal_date, fiscal_year, fiscal_period in rows:
            key = f"{fiscal_year}-P{int(fiscal_period):02d}"
            lo, hi = spans.get(key, (cal_date, cal_date))
            spans[key] = (min(lo, cal_date), max(hi, cal_date))
        ordered = sorted(spans.items(), key=lambda kv: kv[1][0])
        self.months: list[FiscalMonth] = []
        for i, (key, (lo, hi)) in enumerate(ordered):
            following = ordered[i + 1][1][0] if i + 1 < len(ordered) else None
            close = self._close(following) if following else None
            self.months.append(FiscalMonth(key, lo, hi, close))
        self._starts = [m.start for m in self.months]

    @staticmethod
    def _close(following_start: date) -> date:
        """The 5th business day of a fiscal month (REV-8).

        A business day is Monday to Friday. The policy names no holiday
        calendar and the world ships none, so none is applied (AMBIGUITY A-2).
        The worked example checks out: FY2026 P5 opens Sunday 2026-05-31, so
        the count runs 1 June to 5 June and May 2026 closes on 2026-06-05,
        which is what the policy's worked month says.
        """
        day, counted = following_start, 0
        while True:
            if day.weekday() < 5:
                counted += 1
                if counted == CLOSE_BUSINESS_DAY:
                    return day
            day += timedelta(days=1)

    def month_of(self, day: date) -> FiscalMonth:
        i = bisect.bisect_right(self._starts, day) - 1
        if i < 0 or day > self.months[i].end:
            raise PolicyError(f"{day} is outside raw.fiscal_calendar")
        return self.months[i]

    def key_of(self, day: date) -> str:
        return self.month_of(day).key

    def spanning(self, first: date, last: date) -> Iterator[FiscalMonth]:
        """Every fiscal month that touches the inclusive range."""
        i = bisect.bisect_right(self._starts, first) - 1
        if i < 0:
            raise PolicyError(f"{first} is before raw.fiscal_calendar")
        while i < len(self.months) and self.months[i].start <= last:
            if self.months[i].end >= first:
                yield self.months[i]
            i += 1

    def closed_months(self, as_of: date) -> list[FiscalMonth]:
        """REV-8: a fiscal month is closed once its close date has arrived.

        "A closed month is final." REV-9: "The ledger covers a month only once
        that month has closed. The open month has no ledger row and no tie."
        """
        return [m for m in self.months if m.close is not None and m.close <= as_of]

    def earliest_open(self, as_of: date) -> FiscalMonth:
        """REV-8: "the earliest open fiscal month" as at a date."""
        for month in self.months:
            if month.close is None or month.close > as_of:
                return month
        raise PolicyError(f"every fiscal month is closed at {as_of}")


# ---------------------------------------------------------------------------
# REV-7 foreign currency
# ---------------------------------------------------------------------------

class Rates:
    """`raw.fx_rates`, keyed the way REV-7 reads it: by date and currency.

    "The rate is the value in `raw.fx_rates` for the invoice date, held as
    integer parts per million on the row. There is no second conversion, no
    re-translation on the recognition date, and no re-translation at close."
    """

    def __init__(self, rows: Iterable[tuple[date, str, int]]):
        self._ppm = {(rate_date, code): int(ppm) for rate_date, code, ppm in rows}

    def convert(self, cents: int, currency: Optional[str], on: date) -> int:
        """`cents` of `currency`, converted once, at the rate for `on`.

        REV-7: "Rows dated before the currency cutover carry `currency_code`
        NULL and are USD by construction. Treat them as USD; do not treat NULL
        as a missing rate."
        """
        if currency is None or currency == REPORTING_CURRENCY:
            return cents
        ppm = self._ppm.get((on, currency))
        if ppm is None:
            raise PolicyError(f"no {currency} rate in raw.fx_rates for {on}")
        # Half up to the cent. The policy allows no other rounding: "no step
        # at which an amount is rounded to anything other than a cent".
        converted = (Decimal(cents) * Decimal(ppm) / PPM).quantize(
            Decimal(1), rounding=ROUND_HALF_UP)
        return int(converted)


# ---------------------------------------------------------------------------
# REV-1, REV-2, REV-11: the recognition schedule of one line
# ---------------------------------------------------------------------------

def daily_schedule(cents: int, start: date, end: date) -> list[tuple[date, int]]:
    """REV-1 and REV-2 spelled out one day at a time.

    REV-1: "A ratable line recognizes in equal daily amounts across its
    service period. The service period runs from `service_start` to
    `service_end` inclusive: both end days are inside the period, so a period
    from the 1st to the 31st is 31 days, not 30."

    REV-2: "The daily amount is `floor(line_cents / days)`. The remainder is
    recognized in full on the final day of the service period, never spread
    and never dropped."

    This is the readable statement of the rule and the reference the fast
    path is tested against; `periodise` produces the same numbers without
    walking a year of days.
    """
    if cents < 0:
        raise PolicyError("REV-2's floor is written for a positive line amount")
    days = (end - start).days + 1
    per_day = floor_div(cents, days)
    remainder = cents - per_day * days
    out = []
    for offset in range(days):
        day = start + timedelta(days=offset)
        amount = per_day + (remainder if offset == days - 1 else 0)
        out.append((day, amount))
    return out


#: How a ratable line is cut into fiscal months.
DAILY = "daily"                    # REV-1, REV-2
LEGACY_MONTH = "legacy_month"      # REV-11, a legacy line billed monthly
LEGACY_PREPAY = "legacy_prepay"    # REV-3 with REV-11, a legacy annual prepay


def periodise(
    calendar: Calendar,
    cents: int,
    start: date,
    end: date,
    *,
    grain: str,
    through: Optional[date] = None,
) -> list[tuple[FiscalMonth, date, int]]:
    """The schedule of one ratable line, as (fiscal month, booking date, cents).

    `grain` is one of three, and REV-11 names the column that chooses between
    the last two: "`raw.plan_lines.billing_frequency` says which is which."

    `DAILY` is REV-1 and REV-2, summed inside each fiscal month the period
    touches, booked on the last recognized day of the month.

    `LEGACY_MONTH` is REV-11: "the whole of its amount lands on the first day
    of the fiscal month its service period starts in... nothing of it reaches
    the next one." There are no daily rows behind it.

    `LEGACY_PREPAY` is REV-3 with REV-11: a legacy line billed annually
    recognizes across its term under REV-1 and REV-2, and each month's share
    is then dated to the first day of that fiscal month.

    `through` is the cutoff REV-6 needs: nothing recognizes after a refund.
    A booking counts when its booking date is on or before the cutoff, so a
    legacy month lands whole or not at all, while a current-era month is cut
    on the day.
    """
    if cents < 0:
        raise PolicyError("REV-2's floor is written for a positive line amount")
    if grain == LEGACY_MONTH:
        month = calendar.month_of(start)
        if through is not None and month.start > through:
            return []
        return [(month, month.start, cents)]

    days = (end - start).days + 1
    per_day = floor_div(cents, days)
    remainder = cents - per_day * days
    out: list[tuple[FiscalMonth, date, int]] = []
    for month in calendar.spanning(start, end):
        if grain == LEGACY_PREPAY:
            # The month's share of the term, dated the 1st of the fiscal
            # month. A cutoff drops the month or leaves it whole.
            booked_on = month.start
            if through is not None and booked_on > through:
                continue
            last = min(month.end, end)
        else:
            # REV-1 and REV-2 at day grain, summed inside the month. The last
            # recognized day is the booking date, so a cutoff cuts the month.
            last = min(month.end, end) if through is None \
                else min(month.end, end, through)
            booked_on = last
        first = max(month.start, start)
        if last < first:
            continue
        covered = (last - first).days + 1
        # The remainder of REV-2 goes on the final day of the service period,
        # so it belongs to whichever month holds that day.
        amount = per_day * covered + (remainder if last == end else 0)
        out.append((month, booked_on, amount))
    return out


# ---------------------------------------------------------------------------
# The books
# ---------------------------------------------------------------------------

@dataclass
class Books:
    """Every booking, by (entity, fiscal month, component).

    `book` is the one door into the ledger, so REV-8's redirection is applied
    once, in one place.
    """

    calendar: Calendar
    #: The last day of the last closed fiscal month. Nothing dated after it
    #: can reach a published row: REV-8 only ever moves an amount forward,
    #: into a month that is still open. Dropping those bookings keeps the
    #: model off the end of `raw.fx_rates`, which stops on the last day the
    #: world has data for while a ship confirmation can be dated later.
    horizon: date
    amounts: dict[tuple[str, str, str], int] = field(
        default_factory=lambda: defaultdict(int))
    balances: dict[tuple[str, str, str], int] = field(
        default_factory=lambda: defaultdict(int))
    redirected: int = 0

    def book(
        self,
        entity: str,
        component: str,
        on: date,
        cents: int,
        *,
        arrived: Optional[date] = None,
    ) -> None:
        """Book `cents` into the fiscal month `on` falls in.

        REV-8: "An amount that would have belonged to a closed month is booked
        instead on the 1st day of the earliest open fiscal month, carrying a
        reason of its own." `arrived` is the business date the row reached the
        warehouse; an amount that arrived after its own month closed moves.
        Feeds with no load timestamp pass `arrived=None` and are taken to have
        arrived on the day of the event (AMBIGUITY A-3).
        """
        if on > self.horizon:
            return
        month = self.calendar.month_of(on)
        if arrived is not None and month.close is not None and month.close <= arrived:
            month = self.calendar.earliest_open(arrived)
            self.redirected += 1
        self.amounts[(entity, month.key, component)] += cents

    def move_balance(self, entity: str, balance: str, on: date, cents: int) -> None:
        """A change in a liability balance, in the fiscal month of `on`.

        REV-3: "The cash sits as a contract liability until it is recognized."
        REV-12: "A gift card sale is a liability, never revenue." The two are
        different liabilities and are reported apart.
        """
        if on > self.horizon:
            return
        self.balances[(entity, self.calendar.key_of(on), balance)] += cents


# ---------------------------------------------------------------------------
# Reading the world
# ---------------------------------------------------------------------------

def _entities(con) -> dict[str, date]:
    """The legal entities and the day each began trading.

    "`raw.entities` holds these five rows and no others, and `first_traded_on`
    on that row is the day the entity opened." An entity has no ledger
    coverage before the month it began trading, so this is what decides which
    rows a per-entity table has.
    """
    rows = con.execute(
        "SELECT entity_code, first_traded_on FROM raw.entities "
        "ORDER BY entity_code").fetchall()
    if not rows:
        raise PolicyError("raw.entities is empty; recognition is per entity")
    return {code: first for code, first in rows}


def _as_of(con) -> date:
    """The day the books are read as at.

    REV-8 decides which months are closed against a date, and the world ships
    no clock. The last time a recognition feed landed is the closest thing the
    data holds to today (AMBIGUITY A-4). An event date will not do: a dispute
    carries a resolution date weeks past the last load, and a ship
    confirmation runs days past it, so reading the clock off events would
    close a month the warehouse has no data for.

    The answer this returns agrees with the policy's own worked month, which
    says May 2026 — FY2026 P4 — is the most recent closed month.
    """
    stamp = con.execute("""
        SELECT MAX(t) FROM (
            SELECT MAX(loaded_at) AS t FROM raw.invoices
            UNION ALL SELECT MAX(loaded_at) FROM raw.credit_memos
            UNION ALL SELECT MAX(loaded_at) FROM raw.marketplace_orders
            UNION ALL SELECT MAX(loaded_at) FROM raw.marketplace_settlements
        )""").fetchone()[0]
    if stamp is None:
        raise PolicyError("the world holds no loaded rows")
    return business_date(stamp)


# ---------------------------------------------------------------------------
# The clauses, one at a time
# ---------------------------------------------------------------------------

def _invoice_lines(con, books: Books, rates: Rates) -> None:
    """REV-1 to REV-7 and REV-11: the trade book.

    One pass over `raw.invoice_lines`. A line with a `service_end` is ratable
    (REV-1); a line without one "is not ratable and recognizes in full on the
    invoice date" (REV-1). REV-1 names the amount: "The amount that recognizes
    is `raw.invoice_lines.line_total_cents`... Tax is not revenue, and it is
    not inside that column: `tax_cents` sits beside it and never recognizes."
    The header is not read at all, which the same clause states.
    """
    rows = con.execute("""
        SELECT i.entity_code,
               i.invoice_date,
               i.currency_code,
               i.billing_era,
               CAST(i.loaded_at AS DATE) AS arrived,
               l.line_total_cents,
               l.service_start,
               l.service_end,
               p.cancelled_on,
               p.term_end,
               p.billing_frequency
        FROM raw.invoice_lines l
        JOIN raw.invoices i USING (invoice_id)
        LEFT JOIN raw.plan_lines p USING (plan_line_id)
        ORDER BY i.invoice_id, l.line_no
    """).fetchall()

    for (entity, invoice_date, currency, billing_era, arrived, cents,
         service_start, service_end, cancelled_on, term_end,
         billing_frequency) in rows:
        # REV-7: one conversion, at line level, at the invoice-date rate,
        # before the daily allocation of REV-2.
        cents = rates.convert(int(cents), currency, invoice_date)

        if service_end is None:
            # REV-1: "A line with no `service_end` is not ratable and
            # recognizes in full on the invoice date." REV-11 is a rule about
            # plan lines, so a goods or freight line on a legacy invoice still
            # books on its own date (AMBIGUITY A-7).
            books.book(entity, "point_in_time_cents", invoice_date, cents,
                       arrived=arrived)
            continue

        legacy = billing_era == "legacy"
        component = "ratable_legacy_cents" if legacy else "ratable_current_cents"
        # REV-11: "`raw.plan_lines.billing_frequency` says which is which... A
        # legacy line billed `annual` is a prepay under REV-3... The term
        # length decides nothing; the billing frequency does."
        if not legacy:
            grain = DAILY
        elif billing_frequency == "annual":
            grain = LEGACY_PREPAY
        else:
            grain = LEGACY_MONTH

        # REV-3: an annual prepay recognizes across its term, never on the
        # payment date, so nothing here reads `settled_cents` or `due_date`.
        # REV-4: an upgrade or downgrade already arrives as a closed line and
        # a new line — `raw.plan_lines` holds no chain whose successor starts
        # before its parent ends, so there is nothing to close here. The check
        # below is what keeps that true.
        if term_end is not None and term_end != service_end:
            raise PolicyError(
                "REV-4: plan term and invoiced service period disagree "
                f"({term_end} vs {service_end})")

        # REV-6: the refund boundary. "The refund request is
        # `raw.plan_lines.cancelled_on`, and that column is the whole of
        # REV-6's population." Day 14 is inside the window: "A refund on day
        # 14 voids; a refund on day 15 cancels."
        cutoff = None
        voids = False
        if cancelled_on is not None:
            elapsed = (cancelled_on - invoice_date).days
            voids = elapsed <= VOID_WINDOW_DAYS
            cutoff = cancelled_on

        schedule = periodise(books.calendar, cents, service_start, service_end,
                             grain=grain, through=cutoff)
        # REV-3's contract liability: the cash lands on the invoice date and
        # runs down as the schedule recognizes.
        books.move_balance(entity, "deferred_closing_cents",
                           invoice_date, cents)
        recognized = 0
        for _month, booked_on, amount in schedule:
            books.book(entity, component, booked_on, amount, arrived=arrived)
            books.move_balance(entity, "deferred_closing_cents",
                               booked_on, -amount)
            recognized += amount

        if cancelled_on is None:
            continue

        if voids:
            # REV-6: "the whole recognized-to-date amount reverses and the
            # line recognizes nothing." The reversal books on the refund date;
            # REV-8 forbids restating the closed months the recognition sat in.
            # A cancellation carries no load timestamp of its own, so the
            # refund date is both the event and the arrival (AMBIGUITY A-3).
            books.book(entity, "refund_reversal_cents", cancelled_on,
                       -recognized)
            books.move_balance(entity, "deferred_closing_cents",
                               cancelled_on, recognized - cents)
        else:
            # REV-6: "recognition to date stands and nothing further
            # recognizes." The unrecognized rest is refunded, so the liability
            # goes with it.
            books.move_balance(entity, "deferred_closing_cents",
                               cancelled_on, -(cents - recognized))


def _credit_memos(con, books: Books, rates: Rates) -> None:
    """REV-5: a credit memo is negative revenue on its issue date.

    "A credit memo recognizes as negative revenue on its issue date, at the
    credited amount in cents, against the same entity and the same fiscal
    period the issue date falls in. A credit memo never restates the invoice
    it credits, and never restates a period earlier than its own issue date."

    The entity is the invoice's. The rate is the invoice's invoice-date rate,
    not the issue-date rate: REV-7 allows one conversion per amount and
    forbids "re-translation on the recognition date" (AMBIGUITY A-8).
    """
    rows = con.execute("""
        SELECT i.entity_code,
               cm.issued_on,
               COALESCE(cm.currency_code, i.currency_code) AS currency_code,
               i.invoice_date,
               cm.amount_cents,
               CAST(cm.loaded_at AS DATE) AS arrived
        FROM raw.credit_memos cm
        JOIN raw.invoices i USING (invoice_id)
        ORDER BY cm.credit_memo_id
    """).fetchall()
    for entity, issued_on, currency, invoice_date, cents, arrived in rows:
        amount = rates.convert(int(cents), currency, invoice_date)
        books.book(entity, "credit_memo_cents", issued_on, -amount,
                   arrived=arrived)


def _disputes(con, books: Books, rates: Rates) -> None:
    """REV-10: a dispute changes nothing until it closes.

    "A disputed amount stays recognized while the dispute is open. Recognition
    does not pause, and a provision is not netted against revenue. When a
    dispute closes in the customer's favour, the write-off books on the close
    date under REV-5. When it closes in Copperline's favour, nothing books at
    all."

    The policy names `opened_on`, `closed_on` and `outcome`; the table carries
    `raised_on`, `resolved_on` and `dispute_status`. `settled` is the only
    status that carries a settled amount, and a write-off needs an amount, so
    `settled` is the customer's favour and `upheld` and `rejected` are
    Copperline's (AMBIGUITY A-10). A row with `resolved_on` NULL is open,
    "whatever its age".
    """
    rows = con.execute("""
        SELECT i.entity_code, d.resolved_on, i.currency_code, i.invoice_date,
               d.settled_cents
        FROM raw.disputes d
        JOIN raw.invoices i USING (invoice_id)
        WHERE d.resolved_on IS NOT NULL
          AND d.dispute_status = 'settled'
          AND d.settled_cents > 0
        ORDER BY d.dispute_id
    """).fetchall()
    for entity, resolved_on, currency, invoice_date, cents in rows:
        amount = rates.convert(int(cents), currency, invoice_date)
        books.book(entity, "dispute_writeoff_cents", resolved_on, -amount)


def _marketplace(con, books: Books, rates: Rates) -> None:
    """REV-14: commission and fees, on the seller's ship-confirmation date.

    "GMV is the seller's order value and is never Copperline revenue.
    Copperline recognizes commission and fulfilment fees, on the seller's
    ship-confirmation date, in cents. A marketplace return reduces commission
    in the period of the return and never restates the original period,
    including inside a closed month."

    "The entity is `raw.marketplace_orders.entity_code`. The order currency
    does not decide it: CL-IE and CL-DE both bill in euro."

    `ship_confirmed_at` is a UTC timestamp, so it converts to a head-office
    business date before anything else happens — about one order in seven
    lands on the day before its UTC date. A cancelled order never shipped and
    carries no settlement, so nothing recognizes for it.
    """
    rows = con.execute("""
        SELECT entity_code, currency_code, ship_confirmed_at, commission_cents,
               fulfilment_fee_cents, CAST(loaded_at AS DATE) AS arrived
        FROM raw.marketplace_orders
        WHERE order_state <> 'cancelled'
        ORDER BY marketplace_order_id
    """).fetchall()
    for entity, currency, shipped_at, commission, fee, arrived in rows:
        on = business_date(shipped_at)
        if on > books.horizon:
            continue
        # One conversion, at the date REV-14 names for the amount.
        amount = rates.convert(int(commission) + int(fee), currency, on)
        books.book(entity, "marketplace_cents", on, amount, arrived=arrived)

    # The return leg. `line_type = 'refund'` is the only settlement line that
    # moves Copperline's take back; `principal` is the seller's GMV, which
    # REV-14 says is never Copperline revenue. The refund reverses an amount
    # already converted, so it converts at the order's ship-confirmation rate:
    # REV-7 allows one conversion per amount, and a return at a second rate
    # would leave a foreign-exchange difference inside a revenue line.
    refunds = con.execute("""
        SELECT m.entity_code, m.currency_code, m.ship_confirmed_at, s.posted_at,
               s.amount_cents, CAST(s.loaded_at AS DATE) AS arrived
        FROM raw.marketplace_settlements s
        JOIN raw.marketplace_orders m USING (marketplace_order_id)
        WHERE s.line_type = 'refund'
        ORDER BY s.settlement_id
    """).fetchall()
    for entity, currency, shipped_at, posted_at, cents, arrived in refunds:
        on = business_date(posted_at)
        if on > books.horizon:
            continue
        amount = rates.convert(abs(int(cents)), currency,
                               business_date(shipped_at))
        # "in the period of the return", so the arrival date is not allowed to
        # move it: REV-14 states the period itself, closed or not.
        books.book(entity, "marketplace_cents", on, -amount)


def _gift_cards(con, books: Books, rates: Rates, as_of: date) -> None:
    """REV-12: a card is a liability until it is redeemed or ages out.

    "A gift card sale is a liability, never revenue. Revenue recognizes on
    redemption, on the redemption date, for the redeemed amount in cents.
    Unredeemed value recognizes as breakage 24 months after issue, as one
    amount dated the last day of the fiscal month the card ages out. Breakage
    is the value left on the card on the day it ages out, not the value it was
    sold for."

    The escheat test is a column: "A card issued in a jurisdiction where
    `escheat_applies` is true escheats instead and never recognizes breakage;
    its redemptions still recognize."

    Every amount converts at the card's issue-date rate, redemptions and
    breakage alike, which REV-12 states and REV-7 requires. Store credit is
    not in `raw.gift_cards` at all, so there is nothing here to exclude.
    """
    escheat = _escheat_jurisdictions(con)

    cards = con.execute("""
        SELECT g.card_id, g.issued_at, g.initial_cents, g.currency_code,
               g.jurisdiction_code, l.entity_code
        FROM raw.gift_cards g
        JOIN raw.gift_card_ledger l
          ON l.card_id = g.card_id AND l.entry_type = 'issue'
        ORDER BY g.card_id
    """).fetchall()
    issued: dict[str, tuple[date, date, int, Optional[str], Optional[str], str]] = {}
    for card_id, issued_at, initial, currency, jurisdiction, entity in cards:
        issue_day = business_date(issued_at)
        issued[card_id] = (issue_day, add_months(issue_day, BREAKAGE_MONTHS),
                           int(initial), currency, jurisdiction, entity)
        books.move_balance(
            entity, "gift_card_liability_cents", issue_day,
            rates.convert(int(initial), currency, issue_day))

    redeemed: dict[str, int] = defaultdict(int)
    rows = con.execute("""
        SELECT card_id, occurred_at, amount_cents
        FROM raw.gift_card_ledger
        WHERE entry_type = 'redeem'
        ORDER BY entry_id
    """).fetchall()
    for card_id, occurred_at, cents in rows:
        # Every event on a card books to the entity that sold it, which is
        # what REV-12 means by the liability being struck when the card is
        # sold. The entity comes from the issue entry, never from the event.
        issue_day, ages_out, _initial, currency, _j, entity = issued[card_id]
        on = business_date(occurred_at)
        # The ledger holds a redemption as a fall in the liability; the
        # revenue is the same number the other way up.
        native = abs(int(cents))
        if on <= ages_out:
            # REV-12 measures the unredeemed value at the day the card ages
            # out, so a later redemption cannot reduce the breakage.
            redeemed[card_id] += native
        amount = rates.convert(native, currency, issue_day)
        books.book(entity, "gift_card_redemption_cents", on, amount)
        books.move_balance(entity, "gift_card_liability_cents", on, -amount)

    for card_id, card in issued.items():
        issue_day, ages_out, initial, currency, jurisdiction, entity = card
        if jurisdiction in escheat:
            continue
        if ages_out > as_of:
            continue
        unredeemed = initial - redeemed.get(card_id, 0)
        if unredeemed <= 0:
            continue
        # "as one amount dated the last day of the month the card ages out".
        # Every other "month" in this policy is a fiscal month, so this one is
        # too, and reading it that way keeps the amount in the period it aged
        # out in (AMBIGUITY A-13).
        month = books.calendar.month_of(ages_out)
        amount = rates.convert(unredeemed, currency, issue_day)
        books.book(entity, "breakage_cents", month.end, amount)
        books.move_balance(entity, "gift_card_liability_cents",
                           month.end, -amount)


def _escheat_jurisdictions(con) -> set[str]:
    """The jurisdictions REV-12 keeps out of breakage.

    "`raw.gift_card_jurisdictions` lists all 66 jurisdictions and decides each
    one on its `escheat_applies` column... The column is the test, not the
    presence of a row."
    """
    return {r[0] for r in con.execute(
        "SELECT jurisdiction_code FROM raw.gift_card_jurisdictions "
        "WHERE escheat_applies").fetchall()}


# ---------------------------------------------------------------------------
# The model
# ---------------------------------------------------------------------------

def recognize(db: Path | str, as_of: Optional[date] = None) -> list[dict]:
    """Recognized revenue per legal entity per closed fiscal month.

    Returns one dict per row, sorted by (entity, fiscal month), with the
    columns of `COLUMNS`. Every value is an exact integer number of cents.
    """
    con = duckdb.connect(str(db), read_only=True)
    try:
        calendar = Calendar(con.execute(
            "SELECT cal_date, fiscal_year, fiscal_period "
            "FROM raw.fiscal_calendar ORDER BY cal_date").fetchall())
        rates = Rates(con.execute(
            "SELECT rate_date, currency_code, rate_to_usd_ppm "
            "FROM raw.fx_rates").fetchall())
        entities = _entities(con)
        when = as_of or _as_of(con)
        closed = calendar.closed_months(when)
        if not closed:
            return []

        books = Books(calendar, closed[-1].end)
        _invoice_lines(con, books, rates)
        _credit_memos(con, books, rates)
        _disputes(con, books, rates)
        _marketplace(con, books, rates)
        _gift_cards(con, books, rates, when)
    finally:
        con.close()

    return _rows(books, calendar, when, entities)


def _rows(books: Books, calendar: Calendar, as_of: date,
          entities: dict[str, date]) -> list[dict]:
    """Lay the books out per entity and closed fiscal month.

    REV-9: only a closed month has a ledger row and a tie, so only a closed
    month is published. The entity table of the policy says an entity "has no
    ledger coverage before the month it began trading", and
    `raw.entities.first_traded_on` is the day it began. Coverage starts at the
    first closed month in which the entity recognized something, and never
    before the month it began trading: "The ledger covers a month once that
    month has closed and the entity has something to recognize in it."
    """
    closed = calendar.closed_months(as_of)
    out: list[dict] = []
    for entity in sorted(entities):
        traded = entities[entity]
        opened = (calendar.key_of(traded) if traded >= calendar.months[0].start
                  else calendar.months[0].key)
        booked = sorted(month for (code, month, _c), cents
                        in books.amounts.items() if code == entity and cents)
        if not booked:
            continue
        first = max(opened, booked[0])
        running = dict.fromkeys(BALANCES, 0)
        for month in closed:
            # A balance is cumulative, so it is carried across every month
            # from the start of the calendar and published from the entity's
            # first month.
            for balance in BALANCES:
                running[balance] += books.balances.get(
                    (entity, month.key, balance), 0)
            if first is None or month.key < first:
                continue
            row = {"entity_code": entity, "fiscal_month": month.key}
            total = 0
            for component in COMPONENTS:
                cents = books.amounts.get((entity, month.key, component), 0)
                row[component] = cents
                total += cents
            row["recognized_cents"] = total
            row.update(running)
            out.append({column: row[column] for column in COLUMNS})
    out.sort(key=lambda r: (r["entity_code"], r["fiscal_month"]))
    return out


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Recognized revenue per entity per closed fiscal month, "
                    "from docs/finance-policy.md.")
    parser.add_argument("db", type=Path, help="a copperline DuckDB file")
    parser.add_argument("--as-of", type=date.fromisoformat, default=None,
                        help="the day the books are read as at; the default "
                             "is the last dated row in the world")
    args = parser.parse_args(argv)

    writer = csv.DictWriter(sys.stdout, fieldnames=list(COLUMNS))
    writer.writeheader()
    for row in recognize(args.db, args.as_of):
        writer.writerow(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
