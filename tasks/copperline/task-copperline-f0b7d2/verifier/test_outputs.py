"""GRO-241 — does the new clickstream volume alert fire when it had to and stay
quiet the rest of the time?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it reads the config through `projects.growth.lib.alerts` — the
same reader `gro_alerting_daily` uses — and the warehouse through
`include.lib.warehouse`.

**No authored threshold.** Nothing here says what the number should be. The
alert is replayed day by day over `raw.web_events` as the warehouse holds it,
and graded on which days it would have paged. Any number that pages on the
second day of the January decay and on no ordinary day passes; the band that
satisfies both is narrow, and the only way into it is to measure the feed.

**Why the count is grouped once rather than measured per day.** `alerts.measure`
opens the warehouse per subject per day, and this grades 220-odd days. The
histogram below is the same count in one pass, and
`test_the_reader_counts_what_this_grades_on` proves the two agree before any
verdict rests on it.

**The direction.** `row_count` fails LOW — the header of `config/alerts.yml`
says so, and `gro_alerting_daily.FAIL_HIGH` holds the two checks that do not.
A quiet day is measured and recorded and does not page (§AL-2), so a day is a
page when the count is under the threshold and the day is not an agreed one.

**The days that are graded neither way, and why.**

    2026-01-11        decay day one. 2,638 events is inside the ordinary range;
                      the ticket says outright that nobody is asking for it.
    2026-01-15..22    the recovery and the replay. The feed came back at 14:00
                      on the 14th, so the 15th is missing the nightly batch
                      behind a day that sent nothing, and the 20th to the 22nd
                      carry the replay on top of their own volume. Neither is
                      an ordinary day and neither is the decay.
    2026-03-20        the migration Friday. `ops/calendar/quiet-days.yml` names
                      the 21st and the 22nd and says the feeds pause from 18:00
                      on the Friday, so that day is short by an evening and is
                      not in the agreed window. Paging on it is defensible and
                      so is not paging on it; this grades neither.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest

from include.lib import warehouse
from projects.growth.lib import alerts

TABLE = warehouse.qualify("raw.web_events")
DB = str(warehouse.warehouse_path())

#: `row_count` fails low. See the module docstring.
FAIL_LOW = "row_count"

#: The window the alert is replayed over. The ticket states both ends, and both
#: come from the postmortem: its baseline is "the 90 days to 2026-01-10", which
#: opens on 13 October, and the end of May is the last full month before the
#: world's today. Neither end reaches a reserved window.
WINDOW_END_OF_BASELINE = dt.date(2026, 1, 10)
WINDOW_START = WINDOW_END_OF_BASELINE - dt.timedelta(days=89)
WINDOW_END = dt.date(2026, 5, 31)

#: The days the alert has to page on. `ops/incidents/2026-01-14-feed-decay.md`
#: prints the four decay days; these are the three the ticket asks for, day one
#: being the one it excuses.
MUST_PAGE = (dt.date(2026, 1, 12), dt.date(2026, 1, 13), dt.date(2026, 1, 14))

#: Days around the incident that are neither the decay nor an ordinary day.
#: See the module docstring.
UNGRADED = tuple(
    dt.date(2026, 1, 11) + dt.timedelta(days=n) for n in range(12)
) + (dt.date(2026, 3, 20),)

#: The agreed quiet windows, as `ops/calendar/quiet-days.yml` ships them. They
#: are authored here rather than read from the file because the file is one of
#: the things an answer can get wrong: an alert that pages on the migration
#: weekend can be silenced by deleting the window, and then a check that read
#: the file would agree with the deletion.
AGREED_QUIET = (dt.date(2026, 2, 16), dt.date(2026, 3, 21), dt.date(2026, 3, 22))

#: Every (table, check) pair `config/alerts.yml` shipped with. The ticket says
#: to leave the rest of the config alone, so all thirteen have to survive.
SHIPPED_SUBJECTS = (
    ("raw.web_events", "freshness"),
    ("ops.session_daily", "row_count"),
    ("raw.ads_spend_daily", "freshness"),
    ("raw.comp_prices", "row_count"),
    ("marts.gmv_daily", "row_count"),
    ("marts.comp_sales_daily", "row_count"),
    ("raw.pos_sales_header", "freshness"),
    ("raw.carrier_scans", "null_rate"),
    ("marts.inventory_position", "row_count"),
    ("marts.customer_360", "null_rate"),
    ("raw.email_events", "freshness"),
    ("marts.audience_segments", "row_count"),
    ("raw.seo_rankings", "row_count"),
)


def days(first: dt.date, last: dt.date) -> list[dt.date]:
    return [first + dt.timedelta(days=n) for n in range((last - first).days + 1)]


@pytest.fixture(scope="module")
def subject() -> dict:
    """The volume subject the ticket asks for, out of the agent's config.

    Read through the reader the DAG reads with, so a config the DAG could not
    load fails here first and says so.
    """
    watched = alerts.subjects()
    volume = [
        s for s in watched
        if str(s.get("table", "")).strip() == "raw.web_events"
        and str(s.get("check", "")).strip() == FAIL_LOW
    ]
    assert volume, (
        "config/alerts.yml holds no row_count subject on raw.web_events; the "
        f"feed is watched by {[s.get('check') for s in watched if s.get('table') == 'raw.web_events']}"
    )
    assert len(volume) == 1, (
        f"config/alerts.yml holds {len(volume)} volume subjects on raw.web_events; "
        "the file is the definition and two thresholds on one table is two answers"
    )
    return volume[0]


@pytest.fixture(scope="module")
def counted(subject: dict) -> dict[dt.date, int]:
    """Rows in `raw.web_events` per day, on the clock the subject counts by.

    The same count `alerts.measure` makes for a `row_count` check, in one pass
    instead of one connection per day.
    """
    column = str(subject.get("partition_column", "ds"))
    con = duckdb.connect(DB, read_only=True)
    try:
        try:
            rows = con.execute(
                f"SELECT {column}::DATE AS day, count(*) FROM {TABLE} GROUP BY 1"
            ).fetchall()
        except duckdb.Error as error:
            pytest.fail(
                f"the subject counts raw.web_events by {column!r} and the alert "
                f"cannot run: {error}"
            )
    finally:
        con.close()
    return {day: int(n) for day, n in rows}


def pages(subject: dict, counted: dict[dt.date, int], day: dt.date) -> bool:
    """Would the alert have paged on `day`?

    `gro_alerting_daily.evaluate` in one line: a `row_count` subject breaches
    when the day's count is under the threshold, and a breach on an agreed
    quiet day is recorded rather than paged.
    """
    if alerts.is_quiet(day):
        return False
    return counted.get(day, 0) < float(subject["threshold"])


def test_the_feed_is_watched_by_a_volume_alert_somebody_owns(subject: dict):
    """§AL-1: a table with no owning team is not watched, because an alert
    nobody owns wakes the wrong person and gets muted. The threshold also has
    to be a number, since the evaluator compares it as one."""
    assert not alerts.unowned([subject]), (
        f"the volume subject is owned by {subject.get('owner')!r}, which is not a team"
    )
    float(subject["threshold"])


def test_the_rest_of_the_alert_config_survived():
    """The ticket adds a subject; it does not rewrite the file. An answer that
    tightens the whole config, or drops the freshness alert this feed already
    had, takes other teams' pages down with it."""
    held = {(str(s.get("table", "")), str(s.get("check", ""))) for s in alerts.subjects()}
    missing = sorted(pair for pair in SHIPPED_SUBJECTS if pair not in held)
    assert not missing, f"config/alerts.yml no longer watches {missing}"


def test_the_reader_counts_what_this_grades_on(subject: dict, counted: dict[dt.date, int]):
    """The oracle checks itself against the world's own reader before it grades
    anything. `alerts.measure` is what the DAG calls; if the histogram above
    disagreed with it, every verdict below would be about the wrong number."""
    sample = list(MUST_PAGE) + [WINDOW_START, dt.date(2025, 11, 27), WINDOW_END]
    for day in sample:
        assert alerts.measure(subject, day) == float(counted.get(day, 0)), (
            f"the reader and this histogram disagree about {day}"
        )


def test_the_alert_pages_on_the_second_day_of_the_january_decay(
    subject: dict, counted: dict[dt.date, int]
):
    """The whole point of the follow-up. The feed lost volume in even daily
    steps from the 11th and nothing fired; the ticket asks for a rule that has
    the on-call awake on the 12th, before the second wrong flash.

    A threshold read off the postmortem's prose without checking it against the
    feed is the ordinary way to fail this, and so is counting the feed on
    `event_time_utc`: the January window was replayed on the 20th to the 22nd,
    so on the event clock the four decay days now read at or above a normal day
    and the decay is not in that column at all.
    """
    silent = [(day, counted.get(day, 0)) for day in MUST_PAGE
              if not pages(subject, counted, day)]
    assert not silent, (
        f"the alert sleeps through {silent} against a threshold of {subject['threshold']}"
    )


def test_the_alert_pages_on_no_ordinary_day(subject: dict, counted: dict[dt.date, int]):
    """Every day from 13 October to the end of May that is not the decay and
    not an agreed quiet window. The postmortem is explicit that this half
    matters as much as the other: the four days outside its two-sigma band were
    two known peaks and two regional closures, none of them a defect, and a
    band tight enough to catch the decay would have paged on all four and been
    muted before January.

    This is what convicts a threshold set from the baseline rather than from
    the feed. The lowest ordinary day is the ceiling, and it is not the
    baseline less two sigma.
    """
    ordinary = [day for day in days(WINDOW_START, WINDOW_END)
                if day not in MUST_PAGE
                and day not in UNGRADED
                and day not in AGREED_QUIET]
    fired = sorted(((counted.get(day, 0), day) for day in ordinary
                    if pages(subject, counted, day)))
    assert not fired, (
        f"a threshold of {subject['threshold']} pages on {len(fired)} ordinary day(s), "
        f"lowest first: {fired[:6]}"
    )


def test_the_agreed_quiet_windows_stay_quiet(subject: dict, counted: dict[dt.date, int]):
    """§AL-2. The migration weekend is absent by arrangement, so any volume
    alert breaches on it and none of them may page. The evaluator already
    exempts an agreed day, which leaves one way to fail this: deleting the
    window from `ops/calendar/quiet-days.yml` to silence something else."""
    agreed = alerts.quiet_days()
    dropped = [day for day in AGREED_QUIET if day not in agreed]
    assert not dropped, f"ops/calendar/quiet-days.yml no longer agrees {dropped}"
    paged = [day for day in AGREED_QUIET if pages(subject, counted, day)]
    assert not paged, f"the alert pages on the agreed quiet window {paged}"
