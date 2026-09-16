"""raw.fiscal_calendar and raw.market_calendar — the two shipped calendars.

`raw.market_calendar` is at the bottom of the file; the fiscal calendar and
its comp answers come first.

## raw.fiscal_calendar — the 4-5-4 retail calendar, comp answers authored in.

The calendar is 1,827 rows and every one is load-bearing, so it is built in
Python where the week arithmetic can be read, not in a SQL expression. The
fiscal year ends on the Saturday nearest Jan 31; FY2023 is the 53-week year
and its extra week goes to period 12 (4-5-5), which is the NRF convention.

`comp_date_ly` is the authored answer to "what does this date compare to":
the same day of week, 52 weeks back — which in the year after a 53-week year
lands on the restated week (FY2024 week W compares to FY2023 week W+1, the
sentence docs/finance-policy.md REV-16 carries). It is NULL where no honest
comparable exists: every FY2022 date (the prior year is outside the
calendar) and the 53rd week itself.

A NOTE FOR TASK AUTHORS, recorded here because the spec has it wrong: spec
chapter 01 claims `ds - interval '364 days'` is "wrong by one week for every
FY2024 date". It is not — 364 days is exactly 52 weeks, which IS the
restated mapping, so ds-364 agrees with comp_date_ly on every FY2024 date.
The shortcut that is genuinely wrong by one week is the same-week-number
join (fiscal_year - 1, same fiscal_week — what a SQL model reaches for when
joining the calendar to itself). Any comp task must convict that join, not
date arithmetic.

## raw.market_calendar — one row per market per calendar date.

Nine markets over the same 1,827 dates, 16,443 rows. The holidays are real
ones, authored as a literal table below rather than computed, because that
is what reference data is: a person can read it line by line, and the
exceptions a rule engine would need anyway (the 2022 Platinum Jubilee
weekend, Ireland's St Brigid's Day from 2023, Poland's Christmas Eve from
2025, Indonesia's lunar dates) are most of the interesting rows.

The two flags say different things, and spec chapter 03 uses both:

- `is_trading_day` is the business-day flag. False on Saturdays, Sundays
  and every holiday the market names. `raw.finance_close_calendar` counts
  the fifth business day against it.
- `feed_expected` is the delivery flag. False only where the market shuts
  hard enough to send nothing, plus the three days the whole company skips.

Those three days — 2024-12-25, 2025-12-25 and 2026-05-25 — are the only
dates in the fact range where no market reports, which is W2's whole
mechanism (spec 01 section 4.2). Christmas falls out of the local calendars
on its own; 2026-05-25 is authored, and it is a real holiday in three of the
nine markets, so it reads as an ordinary Monday to anyone who does not open
the calendar. On the skip days every market is shut, holiday name or not.

A SECOND NOTE FOR TASK AUTHORS: spec chapter 01 names 2026-05-25 the
"Spring bank holiday (GB, IE)". Ireland has no late-May holiday — its May
bank holiday is the first Monday of the month — so `holiday_name` is NULL
for IE that day, as it is for CA, BR, MX, PL and ID. `feed_expected` is
false in all nine markets, which is what W2 grades.
"""

from __future__ import annotations

import datetime as dt

from ..config import Context

_PERIOD_WEEKS = (4, 5, 4) * 4  # 12 periods, 52 weeks; the 53rd goes to P12


def _nearest_saturday(d: dt.date) -> dt.date:
    ahead = (5 - d.weekday()) % 7  # Monday=0 .. Saturday=5
    after = d + dt.timedelta(days=ahead)
    before = after - dt.timedelta(days=7)
    return after if (after - d) <= (d - before) else before


def fiscal_year_start(label_year: int) -> dt.date:
    """FY N runs from the day after the Saturday nearest Jan 31 of year N."""
    return _nearest_saturday(dt.date(label_year, 1, 31)) + dt.timedelta(days=1)


def _week_of_period(week: int) -> tuple[int, int]:
    """(fiscal_period, fiscal_quarter) for a week number; week 53 stays in P12."""
    acc = 0
    for i, weeks in enumerate(_PERIOD_WEEKS, start=1):
        acc += weeks
        if week <= acc:
            return i, (i - 1) // 3 + 1
    return 12, 4


def rows(cfg: dict) -> list[tuple]:
    years = [int(str(y).removeprefix("FY")) for y in cfg["fiscal"]["emit_years"]]
    bounds = {y: (fiscal_year_start(y), fiscal_year_start(y + 1) - dt.timedelta(days=1))
              for y in years}

    week_of: dict[dt.date, tuple[int, int]] = {}  # date -> (label year, fiscal week)
    for y, (start, end) in bounds.items():
        for offset in range((end - start).days + 1):
            week_of[start + dt.timedelta(days=offset)] = (y, offset // 7 + 1)

    out = []
    for cal_date, (y, week) in sorted(week_of.items()):
        start = bounds[y][0]
        day_of_week = (cal_date - start).days % 7 + 1
        week_start = cal_date - dt.timedelta(days=day_of_week - 1)
        period, quarter = _week_of_period(week)
        comp = cal_date - dt.timedelta(days=364)
        comp_ok = comp in week_of and week != 53
        out.append((
            cal_date, f"FY{y}", quarter, period, week,
            week_start, week_start + dt.timedelta(days=6), day_of_week,
            comp if comp_ok else None,
            week_of[comp][1] if comp_ok else None,
            week == 53,
        ))
    return out


# --- raw.market_calendar ------------------------------------------------

# Real public holidays for the nine markets over the calendar range,
# 2022-01-30 to 2027-01-30. Sourced by hand and kept as a literal: the
# fixture range 2024-02-04 to 2026-06-14 is the part that has to be right.
_HOLIDAYS: dict[str, dict[str, str]] = {
    "US": {
        "2022-02-21": "Presidents Day",
        "2022-05-30": "Memorial Day",
        "2022-06-19": "Juneteenth",
        "2022-07-04": "Independence Day",
        "2022-09-05": "Labor Day",
        "2022-10-10": "Columbus Day",
        "2022-11-11": "Veterans Day",
        "2022-11-24": "Thanksgiving Day",
        "2022-12-25": "Christmas Day",
        "2023-01-01": "New Year's Day",
        "2023-01-16": "Martin Luther King Jr. Day",
        "2023-02-20": "Presidents Day",
        "2023-05-29": "Memorial Day",
        "2023-06-19": "Juneteenth",
        "2023-07-04": "Independence Day",
        "2023-09-04": "Labor Day",
        "2023-10-09": "Columbus Day",
        "2023-11-11": "Veterans Day",
        "2023-11-23": "Thanksgiving Day",
        "2023-12-25": "Christmas Day",
        "2024-01-01": "New Year's Day",
        "2024-01-15": "Martin Luther King Jr. Day",
        "2024-02-19": "Presidents Day",
        "2024-05-27": "Memorial Day",
        "2024-06-19": "Juneteenth",
        "2024-07-04": "Independence Day",
        "2024-09-02": "Labor Day",
        "2024-10-14": "Columbus Day",
        "2024-11-11": "Veterans Day",
        "2024-11-28": "Thanksgiving Day",
        "2024-12-25": "Christmas Day",
        "2025-01-01": "New Year's Day",
        "2025-01-20": "Martin Luther King Jr. Day",
        "2025-02-17": "Presidents Day",
        "2025-05-26": "Memorial Day",
        "2025-06-19": "Juneteenth",
        "2025-07-04": "Independence Day",
        "2025-09-01": "Labor Day",
        "2025-10-13": "Columbus Day",
        "2025-11-11": "Veterans Day",
        "2025-11-27": "Thanksgiving Day",
        "2025-12-25": "Christmas Day",
        "2026-01-01": "New Year's Day",
        "2026-01-19": "Martin Luther King Jr. Day",
        "2026-02-16": "Presidents Day",
        "2026-05-25": "Memorial Day",
        "2026-06-19": "Juneteenth",
        "2026-07-04": "Independence Day",
        "2026-09-07": "Labor Day",
        "2026-10-12": "Columbus Day",
        "2026-11-11": "Veterans Day",
        "2026-11-26": "Thanksgiving Day",
        "2026-12-25": "Christmas Day",
        "2027-01-01": "New Year's Day",
        "2027-01-18": "Martin Luther King Jr. Day",
    },
    "CA": {
        "2022-04-15": "Good Friday",
        "2022-05-23": "Victoria Day",
        "2022-07-01": "Canada Day",
        "2022-08-01": "Civic Holiday",
        "2022-09-05": "Labour Day",
        "2022-09-30": "National Day for Truth and Reconciliation",
        "2022-10-10": "Thanksgiving Day",
        "2022-11-11": "Remembrance Day",
        "2022-12-25": "Christmas Day",
        "2022-12-26": "Boxing Day",
        "2023-01-01": "New Year's Day",
        "2023-04-07": "Good Friday",
        "2023-05-22": "Victoria Day",
        "2023-07-01": "Canada Day",
        "2023-08-07": "Civic Holiday",
        "2023-09-04": "Labour Day",
        "2023-09-30": "National Day for Truth and Reconciliation",
        "2023-10-09": "Thanksgiving Day",
        "2023-11-11": "Remembrance Day",
        "2023-12-25": "Christmas Day",
        "2023-12-26": "Boxing Day",
        "2024-01-01": "New Year's Day",
        "2024-03-29": "Good Friday",
        "2024-05-20": "Victoria Day",
        "2024-07-01": "Canada Day",
        "2024-08-05": "Civic Holiday",
        "2024-09-02": "Labour Day",
        "2024-09-30": "National Day for Truth and Reconciliation",
        "2024-10-14": "Thanksgiving Day",
        "2024-11-11": "Remembrance Day",
        "2024-12-25": "Christmas Day",
        "2024-12-26": "Boxing Day",
        "2025-01-01": "New Year's Day",
        "2025-04-18": "Good Friday",
        "2025-05-19": "Victoria Day",
        "2025-07-01": "Canada Day",
        "2025-08-04": "Civic Holiday",
        "2025-09-01": "Labour Day",
        "2025-09-30": "National Day for Truth and Reconciliation",
        "2025-10-13": "Thanksgiving Day",
        "2025-11-11": "Remembrance Day",
        "2025-12-25": "Christmas Day",
        "2025-12-26": "Boxing Day",
        "2026-01-01": "New Year's Day",
        "2026-04-03": "Good Friday",
        "2026-05-18": "Victoria Day",
        "2026-07-01": "Canada Day",
        "2026-08-03": "Civic Holiday",
        "2026-09-07": "Labour Day",
        "2026-09-30": "National Day for Truth and Reconciliation",
        "2026-10-12": "Thanksgiving Day",
        "2026-11-11": "Remembrance Day",
        "2026-12-25": "Christmas Day",
        "2026-12-26": "Boxing Day",
        "2027-01-01": "New Year's Day",
    },
    "GB": {
        "2022-04-15": "Good Friday",
        "2022-04-18": "Easter Monday",
        "2022-05-02": "Early May bank holiday",
        "2022-06-02": "Spring bank holiday",
        "2022-06-03": "Platinum Jubilee bank holiday",
        "2022-08-29": "Summer bank holiday",
        "2022-09-19": "State Funeral of Queen Elizabeth II",
        "2022-12-25": "Christmas Day",
        "2022-12-26": "Boxing Day",
        "2022-12-27": "Christmas Day (substitute day)",
        "2023-01-01": "New Year's Day",
        "2023-01-02": "New Year's Day (substitute day)",
        "2023-04-07": "Good Friday",
        "2023-04-10": "Easter Monday",
        "2023-05-01": "Early May bank holiday",
        "2023-05-08": "Coronation bank holiday",
        "2023-05-29": "Spring bank holiday",
        "2023-08-28": "Summer bank holiday",
        "2023-12-25": "Christmas Day",
        "2023-12-26": "Boxing Day",
        "2024-01-01": "New Year's Day",
        "2024-03-29": "Good Friday",
        "2024-04-01": "Easter Monday",
        "2024-05-06": "Early May bank holiday",
        "2024-05-27": "Spring bank holiday",
        "2024-08-26": "Summer bank holiday",
        "2024-12-25": "Christmas Day",
        "2024-12-26": "Boxing Day",
        "2025-01-01": "New Year's Day",
        "2025-04-18": "Good Friday",
        "2025-04-21": "Easter Monday",
        "2025-05-05": "Early May bank holiday",
        "2025-05-26": "Spring bank holiday",
        "2025-08-25": "Summer bank holiday",
        "2025-12-25": "Christmas Day",
        "2025-12-26": "Boxing Day",
        "2026-01-01": "New Year's Day",
        "2026-04-03": "Good Friday",
        "2026-04-06": "Easter Monday",
        "2026-05-04": "Early May bank holiday",
        "2026-05-25": "Spring bank holiday",
        "2026-08-31": "Summer bank holiday",
        "2026-12-25": "Christmas Day",
        "2026-12-26": "Boxing Day",
        "2027-01-01": "New Year's Day",
    },
    "IE": {
        "2022-03-17": "St Patrick's Day",
        "2022-04-18": "Easter Monday",
        "2022-05-02": "May bank holiday",
        "2022-06-06": "June bank holiday",
        "2022-08-01": "August bank holiday",
        "2022-10-31": "October bank holiday",
        "2022-12-25": "Christmas Day",
        "2022-12-26": "St Stephen's Day",
        "2022-12-27": "Christmas Day (substitute day)",
        "2023-01-01": "New Year's Day",
        "2023-01-02": "New Year's Day (substitute day)",
        "2023-02-06": "St Brigid's Day",
        "2023-03-17": "St Patrick's Day",
        "2023-04-10": "Easter Monday",
        "2023-05-01": "May bank holiday",
        "2023-06-05": "June bank holiday",
        "2023-08-07": "August bank holiday",
        "2023-10-30": "October bank holiday",
        "2023-12-25": "Christmas Day",
        "2023-12-26": "St Stephen's Day",
        "2024-01-01": "New Year's Day",
        "2024-02-05": "St Brigid's Day",
        "2024-03-17": "St Patrick's Day",
        "2024-04-01": "Easter Monday",
        "2024-05-06": "May bank holiday",
        "2024-06-03": "June bank holiday",
        "2024-08-05": "August bank holiday",
        "2024-10-28": "October bank holiday",
        "2024-12-25": "Christmas Day",
        "2024-12-26": "St Stephen's Day",
        "2025-01-01": "New Year's Day",
        "2025-02-03": "St Brigid's Day",
        "2025-03-17": "St Patrick's Day",
        "2025-04-21": "Easter Monday",
        "2025-05-05": "May bank holiday",
        "2025-06-02": "June bank holiday",
        "2025-08-04": "August bank holiday",
        "2025-10-27": "October bank holiday",
        "2025-12-25": "Christmas Day",
        "2025-12-26": "St Stephen's Day",
        "2026-01-01": "New Year's Day",
        "2026-02-02": "St Brigid's Day",
        "2026-03-17": "St Patrick's Day",
        "2026-04-06": "Easter Monday",
        "2026-05-04": "May bank holiday",
        "2026-06-01": "June bank holiday",
        "2026-08-03": "August bank holiday",
        "2026-10-26": "October bank holiday",
        "2026-12-25": "Christmas Day",
        "2026-12-26": "St Stephen's Day",
        "2027-01-01": "New Year's Day",
    },
    "DE": {
        "2022-04-15": "Good Friday",
        "2022-04-18": "Easter Monday",
        "2022-05-01": "Labour Day",
        "2022-05-26": "Ascension Day",
        "2022-06-06": "Whit Monday",
        "2022-10-03": "German Unity Day",
        "2022-12-25": "Christmas Day",
        "2022-12-26": "Second Christmas Day",
        "2023-01-01": "New Year's Day",
        "2023-04-07": "Good Friday",
        "2023-04-10": "Easter Monday",
        "2023-05-01": "Labour Day",
        "2023-05-18": "Ascension Day",
        "2023-05-29": "Whit Monday",
        "2023-10-03": "German Unity Day",
        "2023-12-25": "Christmas Day",
        "2023-12-26": "Second Christmas Day",
        "2024-01-01": "New Year's Day",
        "2024-03-29": "Good Friday",
        "2024-04-01": "Easter Monday",
        "2024-05-01": "Labour Day",
        "2024-05-09": "Ascension Day",
        "2024-05-20": "Whit Monday",
        "2024-10-03": "German Unity Day",
        "2024-12-25": "Christmas Day",
        "2024-12-26": "Second Christmas Day",
        "2025-01-01": "New Year's Day",
        "2025-04-18": "Good Friday",
        "2025-04-21": "Easter Monday",
        "2025-05-01": "Labour Day",
        "2025-05-29": "Ascension Day",
        "2025-06-09": "Whit Monday",
        "2025-10-03": "German Unity Day",
        "2025-12-25": "Christmas Day",
        "2025-12-26": "Second Christmas Day",
        "2026-01-01": "New Year's Day",
        "2026-04-03": "Good Friday",
        "2026-04-06": "Easter Monday",
        "2026-05-01": "Labour Day",
        "2026-05-14": "Ascension Day",
        "2026-05-25": "Whit Monday",
        "2026-10-03": "German Unity Day",
        "2026-12-25": "Christmas Day",
        "2026-12-26": "Second Christmas Day",
        "2027-01-01": "New Year's Day",
    },
    "BR": {
        "2022-03-01": "Carnival Tuesday",
        "2022-04-15": "Good Friday",
        "2022-04-21": "Tiradentes Day",
        "2022-05-01": "Labour Day",
        "2022-06-16": "Corpus Christi",
        "2022-09-07": "Independence Day",
        "2022-10-12": "Our Lady of Aparecida",
        "2022-11-02": "All Souls' Day",
        "2022-11-15": "Republic Day",
        "2022-12-25": "Christmas Day",
        "2023-01-01": "New Year's Day",
        "2023-02-21": "Carnival Tuesday",
        "2023-04-07": "Good Friday",
        "2023-04-21": "Tiradentes Day",
        "2023-05-01": "Labour Day",
        "2023-06-08": "Corpus Christi",
        "2023-09-07": "Independence Day",
        "2023-10-12": "Our Lady of Aparecida",
        "2023-11-02": "All Souls' Day",
        "2023-11-15": "Republic Day",
        "2023-12-25": "Christmas Day",
        "2024-01-01": "New Year's Day",
        "2024-02-13": "Carnival Tuesday",
        "2024-03-29": "Good Friday",
        "2024-04-21": "Tiradentes Day",
        "2024-05-01": "Labour Day",
        "2024-05-30": "Corpus Christi",
        "2024-09-07": "Independence Day",
        "2024-10-12": "Our Lady of Aparecida",
        "2024-11-02": "All Souls' Day",
        "2024-11-15": "Republic Day",
        "2024-11-20": "Black Consciousness Day",
        "2024-12-25": "Christmas Day",
        "2025-01-01": "New Year's Day",
        "2025-03-04": "Carnival Tuesday",
        "2025-04-18": "Good Friday",
        "2025-04-21": "Tiradentes Day",
        "2025-05-01": "Labour Day",
        "2025-06-19": "Corpus Christi",
        "2025-09-07": "Independence Day",
        "2025-10-12": "Our Lady of Aparecida",
        "2025-11-02": "All Souls' Day",
        "2025-11-15": "Republic Day",
        "2025-11-20": "Black Consciousness Day",
        "2025-12-25": "Christmas Day",
        "2026-01-01": "New Year's Day",
        "2026-02-17": "Carnival Tuesday",
        "2026-04-03": "Good Friday",
        "2026-04-21": "Tiradentes Day",
        "2026-05-01": "Labour Day",
        "2026-06-04": "Corpus Christi",
        "2026-09-07": "Independence Day",
        "2026-10-12": "Our Lady of Aparecida",
        "2026-11-02": "All Souls' Day",
        "2026-11-15": "Republic Day",
        "2026-11-20": "Black Consciousness Day",
        "2026-12-25": "Christmas Day",
        "2027-01-01": "New Year's Day",
    },
    "MX": {
        "2022-02-07": "Constitution Day",
        "2022-03-21": "Benito Juarez's Birthday",
        "2022-05-01": "Labour Day",
        "2022-09-16": "Independence Day",
        "2022-11-21": "Revolution Day",
        "2022-12-25": "Christmas Day",
        "2023-01-01": "New Year's Day",
        "2023-02-06": "Constitution Day",
        "2023-03-20": "Benito Juarez's Birthday",
        "2023-05-01": "Labour Day",
        "2023-09-16": "Independence Day",
        "2023-11-20": "Revolution Day",
        "2023-12-25": "Christmas Day",
        "2024-01-01": "New Year's Day",
        "2024-02-05": "Constitution Day",
        "2024-03-18": "Benito Juarez's Birthday",
        "2024-05-01": "Labour Day",
        "2024-09-16": "Independence Day",
        "2024-10-01": "Transfer of Federal Executive Power",
        "2024-11-18": "Revolution Day",
        "2024-12-25": "Christmas Day",
        "2025-01-01": "New Year's Day",
        "2025-02-03": "Constitution Day",
        "2025-03-17": "Benito Juarez's Birthday",
        "2025-05-01": "Labour Day",
        "2025-09-16": "Independence Day",
        "2025-11-17": "Revolution Day",
        "2025-12-25": "Christmas Day",
        "2026-01-01": "New Year's Day",
        "2026-02-02": "Constitution Day",
        "2026-03-16": "Benito Juarez's Birthday",
        "2026-05-01": "Labour Day",
        "2026-09-16": "Independence Day",
        "2026-11-16": "Revolution Day",
        "2026-12-25": "Christmas Day",
        "2027-01-01": "New Year's Day",
    },
    "PL": {
        "2022-04-17": "Easter Sunday",
        "2022-04-18": "Easter Monday",
        "2022-05-01": "Labour Day",
        "2022-05-03": "Constitution Day",
        "2022-06-05": "Pentecost Sunday",
        "2022-06-16": "Corpus Christi",
        "2022-08-15": "Assumption Day",
        "2022-11-01": "All Saints' Day",
        "2022-11-11": "Independence Day",
        "2022-12-25": "Christmas Day",
        "2022-12-26": "Second Day of Christmas",
        "2023-01-01": "New Year's Day",
        "2023-01-06": "Epiphany",
        "2023-04-09": "Easter Sunday",
        "2023-04-10": "Easter Monday",
        "2023-05-01": "Labour Day",
        "2023-05-03": "Constitution Day",
        "2023-05-28": "Pentecost Sunday",
        "2023-06-08": "Corpus Christi",
        "2023-08-15": "Assumption Day",
        "2023-11-01": "All Saints' Day",
        "2023-11-11": "Independence Day",
        "2023-12-25": "Christmas Day",
        "2023-12-26": "Second Day of Christmas",
        "2024-01-01": "New Year's Day",
        "2024-01-06": "Epiphany",
        "2024-03-31": "Easter Sunday",
        "2024-04-01": "Easter Monday",
        "2024-05-01": "Labour Day",
        "2024-05-03": "Constitution Day",
        "2024-05-19": "Pentecost Sunday",
        "2024-05-30": "Corpus Christi",
        "2024-08-15": "Assumption Day",
        "2024-11-01": "All Saints' Day",
        "2024-11-11": "Independence Day",
        "2024-12-25": "Christmas Day",
        "2024-12-26": "Second Day of Christmas",
        "2025-01-01": "New Year's Day",
        "2025-01-06": "Epiphany",
        "2025-04-20": "Easter Sunday",
        "2025-04-21": "Easter Monday",
        "2025-05-01": "Labour Day",
        "2025-05-03": "Constitution Day",
        "2025-06-08": "Pentecost Sunday",
        "2025-06-19": "Corpus Christi",
        "2025-08-15": "Assumption Day",
        "2025-11-01": "All Saints' Day",
        "2025-11-11": "Independence Day",
        "2025-12-24": "Christmas Eve",
        "2025-12-25": "Christmas Day",
        "2025-12-26": "Second Day of Christmas",
        "2026-01-01": "New Year's Day",
        "2026-01-06": "Epiphany",
        "2026-04-05": "Easter Sunday",
        "2026-04-06": "Easter Monday",
        "2026-05-01": "Labour Day",
        "2026-05-03": "Constitution Day",
        "2026-05-24": "Pentecost Sunday",
        "2026-06-04": "Corpus Christi",
        "2026-08-15": "Assumption Day",
        "2026-11-01": "All Saints' Day",
        "2026-11-11": "Independence Day",
        "2026-12-24": "Christmas Eve",
        "2026-12-25": "Christmas Day",
        "2026-12-26": "Second Day of Christmas",
        "2027-01-01": "New Year's Day",
        "2027-01-06": "Epiphany",
    },
    "ID": {
        "2022-02-01": "Chinese New Year",
        "2022-04-15": "Good Friday",
        "2022-05-02": "Idul Fitri",
        "2022-05-03": "Idul Fitri Holiday",
        "2022-08-17": "Independence Day",
        "2022-12-25": "Christmas Day",
        "2023-01-01": "New Year's Day",
        "2023-01-22": "Chinese New Year",
        "2023-04-07": "Good Friday",
        "2023-04-22": "Idul Fitri",
        "2023-04-23": "Idul Fitri Holiday",
        "2023-08-17": "Independence Day",
        "2023-12-25": "Christmas Day",
        "2024-01-01": "New Year's Day",
        "2024-02-10": "Chinese New Year",
        "2024-03-29": "Good Friday",
        "2024-04-10": "Idul Fitri",
        "2024-04-11": "Idul Fitri Holiday",
        "2024-08-17": "Independence Day",
        "2024-12-25": "Christmas Day",
        "2025-01-01": "New Year's Day",
        "2025-01-29": "Chinese New Year",
        "2025-03-31": "Idul Fitri",
        "2025-04-01": "Idul Fitri Holiday",
        "2025-04-18": "Good Friday",
        "2025-08-17": "Independence Day",
        "2025-12-25": "Christmas Day",
        "2026-01-01": "New Year's Day",
        "2026-02-17": "Chinese New Year",
        "2026-03-20": "Idul Fitri",
        "2026-03-21": "Idul Fitri Holiday",
        "2026-04-03": "Good Friday",
        "2026-08-17": "Independence Day",
        "2026-12-25": "Christmas Day",
        "2027-01-01": "New Year's Day",
    },
}

_EVERY_HOLIDAY = "*"

# The holidays a market shuts for. It trades on none of them and its feed
# sends nothing that day. Every other holiday above is a day the stores
# work through — a US home retailer sells hard on Memorial Day, and a UK
# bank holiday is a sale day — so it is a non-trading day that still
# delivers. `_EVERY_HOLIDAY` means the market shuts on every holiday it
# has, which is what German and Polish shop-closing law says.
_CLOSES: dict[str, frozenset[str] | str] = {
    "US": frozenset({"Thanksgiving Day", "Christmas Day"}),
    "CA": frozenset({"New Year's Day", "Good Friday", "Christmas Day"}),
    "GB": frozenset({"New Year's Day", "New Year's Day (substitute day)",
                     "Good Friday", "Easter Monday",
                     "State Funeral of Queen Elizabeth II", "Christmas Day",
                     "Christmas Day (substitute day)", "Boxing Day"}),
    "IE": frozenset({"New Year's Day", "New Year's Day (substitute day)",
                     "Easter Monday", "Christmas Day",
                     "Christmas Day (substitute day)", "St Stephen's Day"}),
    "DE": _EVERY_HOLIDAY,
    "BR": frozenset({"New Year's Day", "Carnival Tuesday", "Good Friday",
                     "Christmas Day"}),
    "MX": frozenset({"New Year's Day", "Christmas Day"}),
    "PL": _EVERY_HOLIDAY,
    "ID": frozenset({"New Year's Day", "Idul Fitri", "Idul Fitri Holiday",
                     "Christmas Day"}),
}

# The days the whole company skips, whatever the local calendar says
# (spec 01 section 4.2). The two Christmases are all-market under the
# local calendars anyway; 2026-05-25 is the authored one W2 grades, and
# six markets have no holiday of their own on it.
_ALL_MARKET_SKIP = ("2024-12-25", "2025-12-25", "2026-05-25")

# The DST transitions inside the fixture range (spec 01 section 4.5) and
# the markets each one moves. No shipped column carries them: DST lives in
# the event times and in `raw.stores.tz_name`. The map is here because it
# is calendar reference data, and because the absences are load-bearing.
# Brazil abolished DST in 2019, Mexico in 2022 and Indonesia never had it,
# so none of those three ever moves, and a fix that applies the northern
# calendar everywhere is wrong for them.
DST_TRANSITIONS: tuple[tuple[str, str, str, tuple[str, ...]], ...] = (
    ("2024-03-10", "us_spring_forward", "unarmed", ("US", "CA")),
    ("2024-03-31", "eu_spring_forward", "unarmed", ("GB", "IE", "DE", "PL")),
    ("2024-10-27", "eu_fall_back", "unarmed", ("GB", "IE", "DE", "PL")),
    ("2024-11-03", "us_fall_back", "armed", ("US", "CA")),
    ("2025-03-09", "us_spring_forward", "unarmed", ("US", "CA")),
    ("2025-03-30", "eu_spring_forward", "unarmed", ("GB", "IE", "DE", "PL")),
    ("2025-10-26", "eu_fall_back", "unarmed", ("GB", "IE", "DE", "PL")),
    ("2025-11-02", "us_fall_back", "armed", ("US", "CA")),
    ("2026-03-08", "us_spring_forward", "graded", ("US", "CA")),
    ("2026-03-29", "eu_spring_forward", "graded", ("GB", "IE", "DE", "PL")),
)


def dst_transitions(market: str | None = None) -> list[tuple[dt.date, str, str]]:
    """(date, which, state) for a market, or for every market at once."""
    return [
        (dt.date.fromisoformat(day), which, state)
        for day, which, state, moves in DST_TRANSITIONS
        if market is None or market in moves
    ]


def markets(cfg: dict) -> list[str]:
    """The five live markets, then the four the second wave added."""
    return list(cfg["markets"]["live"]) + list(cfg["markets"]["second_wave"])


def _shuts(market: str, holiday_name: str | None) -> bool:
    if holiday_name is None:
        return False
    closes = _CLOSES[market]
    return closes == _EVERY_HOLIDAY or holiday_name in closes


def market_rows(cfg: dict) -> list[tuple]:
    dates = [row[0] for row in rows(cfg)]
    out = []
    for market in markets(cfg):
        holidays = _HOLIDAYS[market]
        for cal_date in dates:
            day = cal_date.isoformat()
            holiday_name = holidays.get(day)
            shut = day in _ALL_MARKET_SKIP or _shuts(market, holiday_name)
            trading = not shut and holiday_name is None and cal_date.weekday() < 5
            out.append((market, cal_date, trading, holiday_name, not shut))
    return out


def build(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.fiscal_calendar (
            cal_date DATE PRIMARY KEY,
            fiscal_year VARCHAR NOT NULL,
            fiscal_quarter TINYINT NOT NULL,
            fiscal_period TINYINT NOT NULL,
            fiscal_week TINYINT NOT NULL,
            week_start DATE NOT NULL,
            week_end DATE NOT NULL,
            day_of_fiscal_week TINYINT NOT NULL,
            comp_date_ly DATE,
            comp_week_ly TINYINT,
            is_53rd_week BOOLEAN NOT NULL
        )
    """)
    ctx.con.executemany(
        "INSERT INTO raw.fiscal_calendar VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        rows(ctx.cfg),
    )

    ctx.sql("""
        CREATE OR REPLACE TABLE raw.market_calendar (
            market_code VARCHAR NOT NULL,
            calendar_date DATE NOT NULL,
            is_trading_day BOOLEAN NOT NULL,
            holiday_name VARCHAR,
            feed_expected BOOLEAN NOT NULL,
            PRIMARY KEY (market_code, calendar_date)
        )
    """)
    ctx.con.executemany(
        "INSERT INTO raw.market_calendar VALUES (?,?,?,?,?)",
        market_rows(ctx.cfg),
    )
