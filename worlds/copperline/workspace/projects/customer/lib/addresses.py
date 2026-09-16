"""Normalising an address, and keying it.

Addresses reach us as free text from three places and no two of them
punctuate a street the same way. `Suite 4`, `Ste. 4` and `STE 4` are one
place. So the parts are normalised first — case, punctuation, the common
abbreviations, whitespace — and only then keyed.

**The key's columns are ordered and the order is part of the key.**
`KEY_COLUMNS` is that order, and it is the same order the location dimension
would carry. A model that lists the same columns differently produces a
different key for the same address, and nothing downstream can tell the two
apart.

**Empty is not absent, and the key cannot tell.** A missing unit number and
an empty one hash the same. So every normalised part is written as an empty
string rather than as null, and `parts_present` beside the key records how
many parts the address actually had. Two addresses with the same key and
different `parts_present` are worth a look before they are treated as one
place.
"""

from __future__ import annotations

import datetime as dt

from include.lib import warehouse

__all__ = ["INBOX", "KEY_COLUMNS", "ABBREVIATIONS", "collect", "normalize",
           "key_addresses", "key_problems", "key_counts"]

#: Where the collected addresses sit while they are worked on.
INBOX = "ops.address_inbox"

#: The key's columns, in order. Widest first, so that a partial key still
#: narrows: country, then region, then postal code, then the street.
KEY_COLUMNS = ("country_code", "region_code", "postal_code", "locality",
               "street_line", "unit")

#: The abbreviations worth folding. Kept short on purpose: a long list starts
#: to change addresses rather than tidy them.
ABBREVIATIONS = {
    "street": "st", "road": "rd", "avenue": "ave", "boulevard": "blvd",
    "drive": "dr", "suite": "ste", "apartment": "apt", "floor": "fl",
    "north": "n", "south": "s", "east": "e", "west": "w",
}


def collect(ds: str | dt.date) -> int:
    """Gather both books' addresses into the inbox. Returns the rows.

    One row per account per book. The acquired book carries no street line —
    Northwave's CRM held a city and a state and nothing finer — so its rows
    key coarsely and say so in `parts_present`.
    """
    day = _as_date(ds)
    inbox = warehouse.qualify(INBOX)
    copperline = warehouse.qualify("raw.customers")
    northwave = warehouse.qualify("raw.nwv_accounts")
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {inbox} AS
            SELECT customer_id                      AS party_id,
                   'copperline'                     AS source_book,
                   country_code, region_code, postal_code,
                   city                             AS locality,
                   address_line1                    AS street_line,
                   NULL                             AS unit,
                   DATE '{day}'                     AS ds
            FROM {copperline}
            UNION ALL
            SELECT nwv_account_id, 'northwave',
                   country, billing_state, NULL, billing_city, NULL, NULL,
                   DATE '{day}'
            FROM {northwave}
            """
        )
        rows = con.execute(f"SELECT count(*) FROM {inbox}").fetchone()
    return int(rows[0] or 0)


def normalize(ds: str | dt.date) -> int:
    """Fold case, punctuation and the abbreviations, part by part.

    Returns the rows normalised. Every part comes out as a string, empty
    where the source had nothing, so that the key is defined for every row.
    """
    day = _as_date(ds)
    inbox = warehouse.qualify(INBOX)
    replacements = " ".join(
        f"WHEN '{long}' THEN '{short}'" for long, short in ABBREVIATIONS.items()
    )
    parts = ", ".join(
        f"""{column} = coalesce(
                list_reduce(
                    list_transform(
                        string_split(
                            regexp_replace(lower(trim({column})),
                                           '[.,]', '', 'g'),
                            ' '),
                        word -> CASE word {replacements} ELSE word END),
                    (a, b) -> a || ' ' || b), '')"""
        for column in ("locality", "street_line", "unit")
    )
    with warehouse.connect() as con:
        con.execute(
            f"""
            UPDATE {inbox} SET
                country_code = upper(coalesce(trim(country_code), '')),
                region_code  = upper(coalesce(trim(region_code), '')),
                postal_code  = upper(replace(coalesce(trim(postal_code), ''),
                                             ' ', '')),
                {parts}
            WHERE ds = DATE '{day}'
            """
        )
        rows = con.execute(
            f"SELECT count(*) FROM {inbox} WHERE ds = DATE '{day}'"
        ).fetchone()
    return int(rows[0] or 0)


def key_addresses(ds: str | dt.date) -> int:
    """Key each normalised address and write `ops.address_keyed`.

    The key is a hash of `KEY_COLUMNS` in that order, joined by a separator
    that cannot occur in a part, so two different splits of the same text
    cannot collide.
    """
    day = _as_date(ds)
    inbox = warehouse.qualify(INBOX)
    keyed = ", ".join(KEY_COLUMNS)
    joined = " || '|' || ".join(KEY_COLUMNS)
    present = " + ".join(f"CASE WHEN {column} <> '' THEN 1 ELSE 0 END"
                         for column in KEY_COLUMNS)
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            SELECT party_id, source_book, {keyed},
                   md5({joined})       AS address_key,
                   {present}           AS parts_present,
                   DATE '{day}'        AS ds
            FROM {inbox} WHERE ds = DATE '{day}'
            ORDER BY party_id
            """
        ).fetchall()
        return warehouse.delete_insert(
            "ops.address_keyed", "ds", day, rows,
            columns=["party_id", "source_book", *KEY_COLUMNS, "address_key",
                     "parts_present", "ds"],
            con=con,
        )


def key_problems(ds: str | dt.date) -> list[dict]:
    """Keys that cannot be right: absent, or shared by two shapes.

    A key shared by rows with different `parts_present` means one address was
    fuller than the other and they were treated as one place anyway. It is
    not always wrong — a unit number nobody typed is still the same building
    — so it is reported rather than failed on.
    """
    day = _as_date(ds)
    keyed = warehouse.qualify("ops.address_keyed")
    with warehouse.connect(read_only=True) as con:
        missing = con.execute(
            f"SELECT party_id FROM {keyed} WHERE ds = DATE '{day}' "
            "AND (address_key IS NULL OR address_key = '') ORDER BY 1"
        ).fetchall()
        mixed = con.execute(
            f"SELECT address_key, count(DISTINCT parts_present) FROM {keyed} "
            f"WHERE ds = DATE '{day}' GROUP BY address_key "
            "HAVING count(DISTINCT parts_present) > 1 ORDER BY 1"
        ).fetchall()
    return ([{"problem": "no key", "party_id": party} for (party,) in missing]
            + [{"problem": "one key, two shapes", "address_key": key,
                "shapes": int(count)} for key, count in mixed])


def key_counts(ds: str | dt.date) -> dict[str, int]:
    """Addresses keyed, and how many distinct places they came to."""
    day = _as_date(ds)
    keyed = warehouse.qualify("ops.address_keyed")
    with warehouse.connect(read_only=True) as con:
        row = con.execute(
            f"SELECT count(*), count(DISTINCT address_key), "
            "count(*) FILTER (source_book = 'northwave') "
            f"FROM {keyed} WHERE ds = DATE '{day}'"
        ).fetchone()
    return {"addresses": int(row[0]), "places": int(row[1]),
            "from_acquired_book": int(row[2])}


def _as_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
