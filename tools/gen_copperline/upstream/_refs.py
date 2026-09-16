"""Lookups and draw helpers the entity modules share.

Two jobs. First, the reference data that `sim_core` owns — country ids,
district regions, address ids — is read here through one function each, so a
module never spells the lookup twice and never breaks when it runs without
core (the tests stub core, and an authoring run may build one schema alone).
Where core is absent the fallback hands back ids from a reserved band that
no core row can occupy, which keeps a partial build coherent instead of
NULL-shaped.

Second, the two things every fact table needs: the day series it draws over,
and the per-day row count that `ctx.scale()` implies. Both live here so the
volume arithmetic reads the same in five modules.
"""

from __future__ import annotations

import datetime as dt
import hashlib

from ..config import Context

# The nine live countries in the order chapter 02 lists them. Core owns the
# table; this mapping is what the entity modules assume when core is absent.
DEFAULT_COUNTRY_IDS = {
    "US": 1, "CA": 2, "GB": 3, "IE": 4, "DE": 5,
    "BR": 6, "MX": 7, "PL": 8, "ID": 9,
}

# Reserved bands. Core numbers regions and addresses from its own sequences,
# well below these, so a fallback id can never collide with a real one.
SYNTHETIC_REGION_BASE = 990_000
SYNTHETIC_ADDRESS_BASE = 7_900_000
DISTRICTS_PER_COUNTRY = 12


def has_table(ctx: Context, schema: str, table: str) -> bool:
    return bool(ctx.sql(
        "SELECT count(*) FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
    ).fetchone()[0])


def py_draw(seed: int, *parts: object) -> int:
    """The SQL `streams.draw` discipline, computed in Python.

    Reference tables of a few hundred rows are built in Python loops, and
    they still need draws that no other stream can move. Same shape as the
    SQL helper: one stream per (table, column, key), stable across runs and
    across platforms because blake2b is.
    """
    key = "|".join([str(seed), *(str(p) for p in parts)])
    return int.from_bytes(hashlib.blake2b(key.encode(), digest_size=8).digest(), "big") >> 1


def shuffled(seed: int, stream: str, items: list) -> list:
    """The items in a stable draw order. Used to hand out a fixed population
    — the country mix of the estate, say — without any item's place depending
    on any other's."""
    return sorted(items, key=lambda item: py_draw(seed, stream, item))


def country_ids(ctx: Context) -> dict[str, int]:
    if has_table(ctx, "sim_core", "countries"):
        rows = ctx.sql("SELECT iso2, country_id FROM sim_core.countries").fetchall()
        if rows:
            return {iso2: cid for iso2, cid in rows}
    return dict(DEFAULT_COUNTRY_IDS)


def districts_by_country(ctx: Context) -> dict[int, list[int]]:
    """The district-level region ids, per country id. Districts are the level
    a store sits at, and the level the one re-districted store moves within."""
    out: dict[int, list[int]] = {}
    if has_table(ctx, "sim_core", "regions"):
        rows = ctx.sql(
            "SELECT country_id, region_id FROM sim_core.regions "
            "WHERE level = 'district' ORDER BY country_id, region_id"
        ).fetchall()
        for country_id, region_id in rows:
            out.setdefault(int(country_id), []).append(int(region_id))
    for iso2, country_id in DEFAULT_COUNTRY_IDS.items():
        if not out.get(country_id):
            out[country_id] = [
                SYNTHETIC_REGION_BASE + country_id * 100 + k
                for k in range(DISTRICTS_PER_COUNTRY)
            ]
    return out


def address_ids(ctx: Context, stream: str, n: int) -> list[int]:
    """`n` address ids for a set of physical places. Real ones when core has
    built the address book, reserved-band ones otherwise."""
    if has_table(ctx, "sim_core", "addresses"):
        rows = ctx.sql(
            f"SELECT address_id FROM sim_core.addresses "
            f"ORDER BY hash({ctx.seed}, '{stream}', address_id) LIMIT {n}"
        ).fetchall()
        if len(rows) == n:
            return [int(r[0]) for r in rows]
    return [SYNTHETIC_ADDRESS_BASE + py_draw(ctx.seed, stream, i) % 90_000 for i in range(n)]


def days(ctx: Context) -> int:
    return (ctx.end - ctx.start).days + 1


def day_series(ctx: Context) -> str:
    """SQL for the fixture range, one row per day, column `ds`."""
    return (
        f"SELECT unnest(generate_series(DATE '{ctx.start}', DATE '{ctx.end}', "
        f"INTERVAL 1 DAY))::DATE AS ds"
    )


def week_series(ctx: Context) -> str:
    """SQL for the fixture range, one row per week, column `ds` — the Sunday
    that opens the fiscal week, because the range starts on one."""
    return (
        f"SELECT unnest(generate_series(DATE '{ctx.start}', DATE '{ctx.end}', "
        f"INTERVAL 7 DAY))::DATE AS ds"
    )


def per_day(ctx: Context, total: int) -> int:
    """Rows a day for a fact table whose whole-range volume the spec fixes.
    The profile divides the total, never the day count, so the small profile
    still crosses every date boundary the shipped one does."""
    return max(1, round(ctx.scale(total) / days(ctx)))


def fiscal_year_start(label_year: int) -> dt.date:
    """FY N runs from the day after the Saturday nearest Jan 31 of year N —
    the rule the calendar module owns, restated here so the upstream never
    imports the extract layer."""
    d = dt.date(label_year, 1, 31)
    ahead = (5 - d.weekday()) % 7  # Monday=0 .. Saturday=5
    after = d + dt.timedelta(days=ahead)
    before = after - dt.timedelta(days=7)
    return (after if (after - d) <= (d - before) else before) + dt.timedelta(days=1)


_PERIOD_WEEKS = (4, 5, 4) * 4  # 12 periods of the 4-5-4 year


def fiscal_periods(first_year: int, last_year: int) -> list[tuple[int, dt.date, dt.date]]:
    """(period_id, start, end) for each 4-5-4 period, period_id as YYYYPP."""
    out = []
    for year in range(first_year, last_year + 1):
        cursor = fiscal_year_start(year)
        for period, weeks in enumerate(_PERIOD_WEEKS, start=1):
            end = cursor + dt.timedelta(days=7 * weeks - 1)
            out.append((year * 100 + period, cursor, end))
            cursor = end + dt.timedelta(days=1)
    return out
