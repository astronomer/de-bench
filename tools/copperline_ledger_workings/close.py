"""The controller's close working.

This is the hand-worked side of the copperline ledger. It reads the raw
warehouse and applies docs/finance-policy.md, one step of the worked month at a
time, and prints the numbers behind each step. It writes nothing into the world.
The shipped file, worlds/copperline/workspace/fixtures/finance/ledger_monthly.csv,
is typed from what this prints; the generator never reads it and never writes it.

Usage:
    python tools/copperline_ledger_workings/close.py --db /tmp/ledgA9-full.duckdb \
        [--month FY2026-P04] [--csv path]

The warehouse must have been through tools/gen_copperline_load_fixtures.py:
`raw.entities` and `raw.gift_card_jurisdictions` are hand-authored files that
land at bake time, and REV-9 and REV-12 both read them.

Every ruling this makes is written down in NOTES.md beside it.
"""

import argparse
import csv
import datetime as dt
import sys

import duckdb

TODAY = dt.date(2026, 6, 15)  # worlds/copperline/timeline.yaml

# REV-6: "A refund requested within 14 days of the invoice date voids the
# schedule... day 14 is inside the window."
VOID_WINDOW_DAYS = 14


# Who ran each close, in the fiction. The controller's office handed over after
# FY2025-P05 and took cover for one month in FY2025-P11.
def preparer(fiscal_month, order):
    if order[fiscal_month] <= order["FY2025-P05"]:
        return "JMR"
    if fiscal_month == "FY2025-P11":
        return "AKT"
    return "PDV"


# The notes the controller left on a row. A close file carries a handful.
MEMOS = {
    ("CL-US", "FY2024-P01"): "first close on the new ledger",
    ("CL-GB", "FY2025-P03"): "first close for the entity; first invoices in GBP",
    ("CL-IE", "FY2025-P03"): "first close for the entity",
    ("CL-DE", "FY2025-P03"): "first close for the entity; first invoices in EUR",
    ("CL-US", "FY2025-P07"): "JE-2025-0117 posted before close",
    ("CL-MX", "FY2026-P01"): "first close for the entity",
    ("CL-US", "FY2026-P01"): "first gift-card breakage",
    ("CL-GB", "FY2026-P02"): "trade book rechecked after the P1 query",
}


def fiscal_months(con):
    """The fiscal months, their bounds and their close dates. CAL-2, REV-8."""
    rows = con.execute(
        """
        select fiscal_year,
               fiscal_period,
               fiscal_year || '-P' || lpad(fiscal_period::varchar, 2, '0') as fiscal_month,
               min(cal_date) as first_day,
               max(cal_date) as last_day
        from raw.fiscal_calendar
        group by 1, 2
        order by first_day
        """
    ).fetchall()
    out = []
    for i, (fy, fp, label, first_day, last_day) in enumerate(rows):
        nxt = rows[i + 1][3] if i + 1 < len(rows) else None
        out.append(
            {
                "fiscal_month": label,
                "first_day": first_day,
                "last_day": last_day,
                "close_on": fifth_business_day(nxt) if nxt else None,
            }
        )
    return out


def fifth_business_day(start):
    """REV-8: a month closes on the 5th business day of the following month.

    Business day is Monday to Friday. The warehouse ships no holiday calendar
    for the head office, so none is applied. See NOTES.md ruling C2.
    """
    day, seen = start, 0
    while True:
        if day.weekday() < 5:
            seen += 1
            if seen == 5:
                return day
        day += dt.timedelta(days=1)


SOURCES = [
    "invoices", "invoice_lines", "credit_memos", "disputes", "plan_lines",
    "fx_rates", "gift_cards", "gift_card_ledger", "gift_card_jurisdictions",
    "marketplace_orders", "marketplace_settlements", "entities",
    "fiscal_calendar",
]


def open_world(path):
    """The warehouse, read only, behind plain raw.* names."""
    con = duckdb.connect(":memory:")
    con.execute(f"attach '{path}' as cw (read_only)")
    con.execute("create schema raw")
    for t in SOURCES:
        con.execute(f"create view raw.{t} as select * from cw.raw.{t}")
    return con


def setup(con, months):
    """Rates, the fiscal months, and the recognisable amount of every line."""
    con.execute(
        """
        create or replace macro usd(cents, ppm) as
            case when cents >= 0
                 then (cents * ppm + 500000) // 1000000
                 else -((-cents * ppm + 500000) // 1000000) end
        """
    )
    # "Business dates are America/Los_Angeles... Event timestamps in the source
    # feeds are UTC and are converted once, on the way in." Four recognition
    # columns are timestamps and every one of them goes through this.
    con.execute(
        """
        create or replace macro bizdate(ts) as
            (ts AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles')::DATE
        """
    )
    con.execute(
        """
        create or replace temp table rates as
        select rate_date, currency_code, rate_to_usd_ppm from raw.fx_rates
        union all
        select cal_date, 'USD', 1000000 from raw.fiscal_calendar
        """
    )
    con.execute(
        """
        create or replace temp table fm (
            fiscal_month varchar, first_day date, last_day date, close_on date
        )
        """
    )
    con.executemany(
        "insert into fm values (?, ?, ?, ?)",
        [(m["fiscal_month"], m["first_day"], m["last_day"], m["close_on"])
         for m in months],
    )
    # REV-8: an amount that arrived after its own month had closed books to the
    # first day of the earliest month still open on the day it arrived. One row
    # per calendar day says which month that is.
    con.execute(
        """
        create or replace temp table open_at as
        select c.cal_date as arrived,
               (select f.fiscal_month from fm f
                 where f.close_on is null or f.close_on > c.cal_date
                 order by f.first_day limit 1) as fiscal_month
        from (select distinct cal_date from raw.fiscal_calendar) c
        """
    )
    # REV-7: convert once, at line level, at the invoice-date rate, before the
    # daily allocation. REV-1 names the amount: `line_total_cents`, which is the
    # line's own extended amount and does not hold the tax.
    con.execute(
        """
        create or replace temp table lines as
        select il.invoice_id,
               il.line_no,
               i.entity_code,
               i.invoice_date,
               i.billing_era,
               p.billing_frequency,
               il.line_kind,
               il.service_start,
               il.service_end,
               p.cancelled_on,
               cast(i.loaded_at as date) as arrived,
               usd(il.line_total_cents, r.rate_to_usd_ppm) as usd_cents
        from raw.invoice_lines il
        join raw.invoices i using (invoice_id)
        left join raw.plan_lines p on p.plan_line_id = il.plan_line_id
        join rates r
          on r.rate_date = i.invoice_date
         and r.currency_code = coalesce(i.currency_code, 'USD')
        """
    )
    # One row per ratable line with everything REV-1, REV-2, REV-6 and REV-11
    # need: the per-day amount, the remainder, and the last day that recognizes.
    con.execute(
        """
        create or replace temp table ratable as
        select l.*,
               l.usd_cents // (l.service_end - l.service_start + 1) as per_day,
               l.usd_cents - (l.usd_cents // (l.service_end - l.service_start + 1))
                   * (l.service_end - l.service_start + 1) as remainder,
               -- REV-6 cancels the schedule forward at the refund request.
               least(coalesce(l.cancelled_on, l.service_end), l.service_end)
                   as recog_end,
               -- REV-6 voids it outright inside the 14-day window.
               (l.cancelled_on is not null
                and datediff('day', l.invoice_date, l.cancelled_on)
                    <= {void}) as voids
        from lines l
        where l.service_end is not null
        """.format(void=VOID_WINDOW_DAYS)
    )


# --- the worked month, step by step -----------------------------------------

# One row per ratable line per fiscal month it recognizes in: the schedule the
# rest of the trade book is read off. Three branches, because REV-11 gives a
# legacy line a different grain from a current-era one and REV-6 bites
# differently on each.
SCHEDULE = """
create or replace temp table sched as
-- REV-1, REV-2: current-era lines at day grain, cut at the refund request.
-- The remainder sits on the final day of the period, so it books only when
-- that day still recognizes.
select r.invoice_id, r.line_no, r.entity_code, r.billing_era,
       r.billing_frequency, r.arrived,
       r.cancelled_on, r.voids, f.fiscal_month, f.close_on,
       r.per_day
         * (least(r.recog_end, f.last_day) - greatest(r.service_start, f.first_day) + 1)
       + case when r.service_end between f.first_day and f.last_day
                   and r.recog_end >= r.service_end
              then r.remainder else 0 end as cents
from ratable r
join fm f on r.service_start <= f.last_day and r.recog_end >= f.first_day
where r.billing_era = 'current'
union all
-- REV-3 with REV-11: a legacy line billed annually recognizes across its whole
-- term, and each month's share is dated to the first day of that fiscal month.
-- REV-6 drops a month whole; it never cuts one, because the month's amount
-- landed on its first day.
select r.invoice_id, r.line_no, r.entity_code, r.billing_era,
       r.billing_frequency, r.arrived,
       r.cancelled_on, r.voids, f.fiscal_month, f.close_on,
       r.per_day
         * (least(r.service_end, f.last_day) - greatest(r.service_start, f.first_day) + 1)
       + case when r.service_end between f.first_day and f.last_day
              then r.remainder else 0 end as cents
from ratable r
join fm f on r.service_start <= f.last_day and r.service_end >= f.first_day
where r.billing_era = 'legacy' and r.billing_frequency = 'annual'
  and (r.cancelled_on is null or f.first_day <= r.cancelled_on)
union all
-- REV-11: a legacy line billed monthly lands whole in the fiscal month its
-- service period starts in. There are no daily rows behind it, and REV-6 never
-- cuts it: the whole amount landed on a day at or before the refund request.
select r.invoice_id, r.line_no, r.entity_code, r.billing_era,
       r.billing_frequency, r.arrived,
       r.cancelled_on, r.voids, f.fiscal_month, f.close_on, r.usd_cents
from ratable r
join fm f on r.service_start between f.first_day and f.last_day
where r.billing_era = 'legacy' and r.billing_frequency = 'monthly'
"""

# The fiscal month an amount books into, once REV-8 has had its say. A feed
# with no load timestamp passes `arrived` as NULL and never moves.
BOOK_MONTH = """
       coalesce(case when {arrived} is not null and {close} is not null
                      and {arrived} >= {close}
                     then o.fiscal_month end, {month})
"""
REDIRECT_JOIN = "left join open_at o on o.arrived = {arrived}"

FROM_SCHEDULE = """
select s.entity_code, {month} as fiscal_month, sum(s.cents) as cents
from sched s
{redirect}
where {era_test}
group by 1, 2
""".replace("{month}", BOOK_MONTH.format(arrived="s.arrived", close="s.close_on",
                                        month="s.fiscal_month")) \
   .replace("{redirect}", REDIRECT_JOIN.format(arrived="s.arrived"))

# REV-1, REV-2: current-era ratable lines.
STEP1 = FROM_SCHEDULE.format(era_test="s.billing_era = 'current'")

# REV-3 with REV-11: the legacy annual prepays.
STEP2_LONG = FROM_SCHEDULE.format(
    era_test="s.billing_era = 'legacy' and s.billing_frequency = 'annual'")

# REV-11: the legacy monthly billings.
STEP2_SHORT = FROM_SCHEDULE.format(
    era_test="s.billing_era = 'legacy' and s.billing_frequency = 'monthly'")

# REV-1 last sentence: a line with no service_end is not ratable and recognises
# in full on the invoice date. That is every goods and freight line.
STEP3 = """
select l.entity_code, {month} as fiscal_month, sum(l.usd_cents) as cents
from lines l
join fm f on l.invoice_date between f.first_day and f.last_day
{redirect}
where l.service_end is null
group by 1, 2
""".replace("{month}", BOOK_MONTH.format(arrived="l.arrived", close="f.close_on",
                                        month="f.fiscal_month")) \
   .replace("{redirect}", REDIRECT_JOIN.format(arrived="l.arrived"))

# REV-14: commission and fulfilment fees, on the seller's ship-confirmation
# date, at the entity the order billed through.
STEP4_COMMISSION = """
select m.entity_code, {month} as fiscal_month,
       sum(usd(m.commission_cents + m.fulfilment_fee_cents, r.rate_to_usd_ppm)) as cents
from raw.marketplace_orders m
join fm f on bizdate(m.ship_confirmed_at) between f.first_day and f.last_day
left join open_at o on o.arrived = cast(m.loaded_at as date)
join rates r
  on r.rate_date = bizdate(m.ship_confirmed_at)
 and r.currency_code = m.currency_code
where m.order_state <> 'cancelled'
group by 1, 2
""".replace("{month}", BOOK_MONTH.format(arrived="cast(m.loaded_at as date)",
                                        close="f.close_on", month="f.fiscal_month"))

# REV-14: a marketplace return reduces commission in the period of the return
# and never restates the original period, so REV-8 does not move it. The refund
# converts at the order's own ship-date rate, so the reversal offsets what was
# recognised (REV-7).
STEP4_RETURNS = """
select m.entity_code, f.fiscal_month,
       sum(usd(s.amount_cents, r.rate_to_usd_ppm)) as cents
from raw.marketplace_settlements s
join raw.marketplace_orders m using (marketplace_order_id)
join fm f on bizdate(s.posted_at) between f.first_day and f.last_day
join rates r
  on r.rate_date = bizdate(m.ship_confirmed_at)
 and r.currency_code = m.currency_code
where s.line_type = 'refund'
group by 1, 2
"""

# REV-12: revenue on redemption, at the redeemed amount; breakage as one amount
# when the card ages out, at the value left on it. Both convert at the card's
# issue-date rate and both book to the entity that sold the card. A card issued
# where `escheat_applies` never books breakage. Issues are a liability and are
# not here.
STEP5 = """
select e.entity_code, f.fiscal_month,
       sum(usd(-l.amount_cents, r.rate_to_usd_ppm)) as cents
from raw.gift_card_ledger l
join raw.gift_cards g using (card_id)
join raw.gift_card_jurisdictions j
  on j.jurisdiction_code = g.jurisdiction_code
join (select card_id, entity_code from raw.gift_card_ledger
       where entry_type = 'issue') e using (card_id)
join fm f on bizdate(l.occurred_at) between f.first_day and f.last_day
join rates r
  on r.rate_date = bizdate(g.issued_at)
 and r.currency_code = g.currency_code
where (l.entry_type = 'redeem'
       or (l.entry_type = 'breakage' and not j.escheat_applies))
group by 1, 2
"""

# REV-5: a credit memo is negative revenue on its issue date, against the same
# entity, at the rate the invoice it credits was converted at (REV-7).
STEP6 = """
select i.entity_code, {month} as fiscal_month,
       -sum(usd(cm.amount_cents, r.rate_to_usd_ppm)) as cents
from raw.credit_memos cm
join raw.invoices i using (invoice_id)
join fm f on cm.issued_on between f.first_day and f.last_day
left join open_at o on o.arrived = cast(cm.loaded_at as date)
join rates r
  on r.rate_date = i.invoice_date
 and r.currency_code = coalesce(i.currency_code, 'USD')
group by 1, 2
""".replace("{month}", BOOK_MONTH.format(arrived="cast(cm.loaded_at as date)",
                                        close="f.close_on", month="f.fiscal_month"))

# REV-10: a dispute closing in the customer's favour books its write-off on the
# close date under REV-5. `settled` is that outcome and `settled_cents` is the
# amount; `upheld`, `rejected` and `open` book nothing.
STEP7 = """
select i.entity_code, f.fiscal_month,
       -sum(usd(d.settled_cents, r.rate_to_usd_ppm)) as cents
from raw.disputes d
join raw.invoices i using (invoice_id)
join fm f on d.resolved_on between f.first_day and f.last_day
join rates r
  on r.rate_date = i.invoice_date
 and r.currency_code = coalesce(i.currency_code, 'USD')
where d.dispute_status = 'settled'
group by 1, 2
"""

# REV-6: a refund inside the 14-day window voids the schedule, so everything
# recognised to date reverses on the day of the request. A legacy month landed
# whole on a day at or before that request, so the whole of it reverses.
STEP8 = """
select s.entity_code, f.fiscal_month, -sum(s.cents) as cents
from sched s
join fm f on s.cancelled_on between f.first_day and f.last_day
where s.voids
group by 1, 2
"""

STEPS = [
    ("1 ratable, current era", STEP1),
    ("2a legacy prepay, annual", STEP2_LONG),
    ("2b legacy month billing", STEP2_SHORT),
    ("3 goods and freight", STEP3),
    ("4a marketplace commission", STEP4_COMMISSION),
    ("4b marketplace returns", STEP4_RETURNS),
    ("5 gift cards", STEP5),
    ("6 credit memos", STEP6),
    ("7 dispute write-offs", STEP7),
    ("8 refunds voided under REV-6", STEP8),
]


def run(con, sql):
    return con.execute(sql).fetchall()


def coverage(con, months, order):
    """Which entity-months the ledger holds.

    REV-9: "An entity's coverage starts at the first closed month in which it
    recognized anything, and never before the month it began trading; from
    there it runs unbroken to the last closed month."
    """
    out = {}
    for entity, first_traded in con.execute(
            "select entity_code, first_traded_on from raw.entities "
            "order by entity_code").fetchall():
        opened = months[0]["fiscal_month"]
        for m in months:
            if m["first_day"] <= first_traded <= m["last_day"]:
                opened = m["fiscal_month"]
                break
        else:
            if first_traded > months[-1]["last_day"]:
                continue
        out[entity] = opened
    return out


def derive(db):
    """The whole close, as (entity, fiscal_month, cents) and its working.

    Returns the ledger rows in entity then month order, the per-step numbers
    behind them, and the fiscal calendar the two are keyed on.
    """
    con = open_world(db)
    try:
        months = fiscal_months(con)
        setup(con, months)
        con.execute(SCHEDULE)

        closed = [m for m in months if m["close_on"] and m["close_on"] <= TODAY]
        order = {m["fiscal_month"]: i for i, m in enumerate(months)}

        # Every step's numbers, kept per step so the working can be read back.
        per_step = {}
        for name, sql in STEPS:
            per_step[name] = {}
            for entity, month, cents in run(con, sql):
                if cents:
                    per_step[name][(entity, month)] = \
                        per_step[name].get((entity, month), 0) + cents

        total = {}
        for rows in per_step.values():
            for key, cents in rows.items():
                total[key] = total.get(key, 0) + cents

        opened = coverage(con, months, order)
    finally:
        con.close()

    ledger = []
    for entity, first in opened.items():
        live = [m["fiscal_month"] for m in closed
                if order[m["fiscal_month"]] >= order[first]
                and total.get((entity, m["fiscal_month"]))]
        if not live:
            continue
        start = order[min(live, key=order.get)]
        for m in closed:
            if order[m["fiscal_month"]] >= start:
                ledger.append((entity, m["fiscal_month"],
                               total.get((entity, m["fiscal_month"]), 0)))
    ledger.sort(key=lambda r: (r[0], order[r[1]]))
    return ledger, per_step, months, closed, order, opened


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--month", help="print the step detail for one fiscal month")
    ap.add_argument("--csv", help="write the ledger here")
    args = ap.parse_args()

    ledger, per_step, months, closed, order, opened = derive(args.db)
    closed_set = {m["fiscal_month"] for m in closed}
    close_on = {m["fiscal_month"]: m["close_on"] for m in months}

    if args.month:
        print(f"== {args.month}")
        for name, _ in STEPS:
            rows = {k: v for k, v in per_step[name].items() if k[1] == args.month}
            for (entity, _), cents in sorted(rows.items()):
                print(f"   {name:32s} {entity} {cents:>16,d}")
        print("   " + "-" * 60)
        for entity, month, cents in ledger:
            if month == args.month:
                print(f"   {'total':32s} {entity} {cents:>16,d}")
        return

    print(f"closed months: {len(closed)}  first {closed[0]['fiscal_month']} "
          f"last {closed[-1]['fiscal_month']} (closed {closed[-1]['close_on']})")
    print(f"ledger rows: {len(ledger)}")
    print("\nper entity:")
    for entity in sorted(opened):
        rows = [r for r in ledger if r[0] == entity]
        if rows:
            print(f"   {entity}  {len(rows):>3d} months  "
                  f"{sum(r[2] for r in rows):>18,d}")
    print("\nper step:")
    for name, _ in STEPS:
        rows = per_step[name]
        kept = sum(c for (e, m), c in rows.items()
                   if m in closed_set and e in opened
                   and order[m] >= order[opened[e]])
        print(f"   {name:32s} {kept:>18,d}")
    print(f"\ngrand total {sum(r[2] for r in ledger):,d}")

    if args.csv:
        # The close file as the controller keeps it: appended each month, so
        # month first and entity within it.
        rows = sorted(ledger, key=lambda r: (order[r[1]], r[0]))
        with open(args.csv, "w", newline="\n") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(["entity_code", "fiscal_month", "recognized_cents",
                        "posted_on", "preparer", "memo"])
            for entity, month, cents in rows:
                w.writerow([entity, month, cents, close_on[month].isoformat(),
                            preparer(month, order), MEMOS.get((entity, month), "")])
        print(f"\nwrote {args.csv}")


if __name__ == "__main__":
    sys.exit(main())
