"""FIN-455 — gift-card recognition, off the card book and the ledger.

One row per recognition day, entity and card currency in
`marts.gift_card_recognition`: what redeemed, what broke, and what escheats
instead of breaking. The close reads the first two and treasury reads the
third, and the point of one table is that they cannot disagree about which
cards are behind either.

`docs/finance-policy.md` §REV-12 is the whole specification and four of its
sentences do the work here.

**A day is not a day.** A redemption recognizes on the redemption date, but
breakage recognizes "as one amount dated the last day of the fiscal month the
card ages out". So a card that aged out on the 6th of a fiscal month
recognizes at the end of it, and the run that owns a fiscal month end has to
reach back over the whole month for the age-outs. The fiscal month is 4-5-4
and lives in `raw.fiscal_calendar`; it is not a calendar month, and in FY2026
P02 ends on 4 April and P03 on 2 May.

**The tender module knows nothing about escheat.** It writes a breakage entry
for every card still holding value at 24 months, in every jurisdiction.
`raw.gift_card_jurisdictions.escheat_applies` is what decides whether that
entry is revenue or an escheat liability, and about half the card book sits in
a jurisdiction where it is not revenue. A card that escheats still recognizes
its redemptions — the clause says so — so this cannot be done by dropping the
card.

**One rate per card, struck at the sale.** Every amount on a card converts at
the rate for the card's issue date, redemptions and breakage alike, because
REV-7 forbids a second conversion on the recognition date. `raw.fx_rates`
carries no USD row; USD is the base and takes 1,000,000.

**The entity is struck at the sale too**, and the ledger carries it on every
entry the card ever has. A card sold in Germany before CL-DE opened was sold
by CL-US and its breakage is CL-US revenue two years later.

The ledger is a liability ledger: an issue is positive, a redemption and a
breakage are negative, and an issue never recognizes anything.
"""

from __future__ import annotations

from include.lib import warehouse

__all__ = ["TABLE", "PARTITION_COL", "COLUMNS", "recognize", "tie_cards"]

#: The table FIN-455 asked for, and the column a run's write is scoped to.
TABLE = "marts.gift_card_recognition"
PARTITION_COL = "ds"

#: The order `_build_sql` selects in.
COLUMNS = ["ds", "entity_code", "currency_code", "redeemed_cents",
           "breakage_cents", "escheated_cents", "redeemed_base_cents",
           "breakage_base_cents", "cards"]

_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    ds DATE NOT NULL,
    entity_code VARCHAR NOT NULL,
    currency_code VARCHAR NOT NULL,
    redeemed_cents BIGINT NOT NULL,
    breakage_cents BIGINT NOT NULL,
    escheated_cents BIGINT NOT NULL,
    redeemed_base_cents BIGINT NOT NULL,
    breakage_base_cents BIGINT NOT NULL,
    cards BIGINT NOT NULL
)
"""

#: Every entry that recognizes anything, priced and dated, for one day.
#:
#: `period_end` is the last day of the entry's own fiscal month and it is what
#: dates a breakage. A redemption keeps its own date. The rate is the card's,
#: at the card's issue date, and USD is minted at par because the rate table
#: does not carry the base currency.
_ENTRIES = """
WITH period AS (
    SELECT cal_date,
           max(cal_date) OVER (PARTITION BY fiscal_year, fiscal_period)
                                                    AS period_end
    FROM {fiscal}
),
card AS (
    SELECT g.card_id,
           g.currency_code,
           g.issued_at::DATE                        AS issue_date,
           j.escheat_applies
    FROM {cards} g
    JOIN {jurisdictions} j ON j.jurisdiction_code = g.jurisdiction_code
),
entry AS (
    SELECT l.card_id,
           l.entry_type,
           l.entity_code,
           -l.amount_cents                          AS cents,
           CASE WHEN l.entry_type = 'redeem' THEN l.occurred_at::DATE
                ELSE p.period_end END               AS ds
    FROM {ledger} l
    LEFT JOIN period p ON p.cal_date = l.occurred_at::DATE
    WHERE l.entry_type IN ('redeem', 'breakage')
)
SELECT e.ds,
       e.entity_code,
       c.currency_code,
       e.card_id,
       e.entry_type,
       e.cents,
       c.escheat_applies,
       coalesce(r.rate_to_usd_ppm, 1000000)         AS rate_ppm
FROM entry e
JOIN card c ON c.card_id = e.card_id
LEFT JOIN {rates} r ON r.currency_code = c.currency_code
                   AND r.rate_date = c.issue_date
WHERE e.ds = ?::DATE
"""

#: Half-up on the single division, in integer arithmetic — `macros/money.sql`,
#: and CONVENTIONS.md "Money is an integer number of cents".
_BASE = "((cents * rate_ppm + 500000) // 1000000)"

_BUILD = f"""
SELECT ds,
       entity_code,
       currency_code,
       coalesce(sum(cents) FILTER (entry_type = 'redeem'), 0)::BIGINT
                                                    AS redeemed_cents,
       coalesce(sum(cents) FILTER (entry_type = 'breakage'
                                   AND NOT escheat_applies), 0)::BIGINT
                                                    AS breakage_cents,
       coalesce(sum(cents) FILTER (entry_type = 'breakage'
                                   AND escheat_applies), 0)::BIGINT
                                                    AS escheated_cents,
       coalesce(sum({_BASE}) FILTER (entry_type = 'redeem'), 0)::BIGINT
                                                    AS redeemed_base_cents,
       coalesce(sum({_BASE}) FILTER (entry_type = 'breakage'
                                     AND NOT escheat_applies), 0)::BIGINT
                                                    AS breakage_base_cents,
       count(DISTINCT card_id)::BIGINT              AS cards
FROM ({{entries}})
GROUP BY 1, 2, 3
ORDER BY 2, 3
"""

_TIE = """
SELECT entity_code, currency_code,
       sum(redeemed_cents + breakage_cents + escheated_cents)::BIGINT,
       count(*)::BIGINT
FROM {table} WHERE ds = ?::DATE
GROUP BY 1, 2 ORDER BY 1, 2
"""

_TIE_SOURCE = """
SELECT entity_code, currency_code, sum(cents)::BIGINT, 1::BIGINT
FROM ({entries})
GROUP BY 1, 2 ORDER BY 1, 2
"""


def _entries_sql() -> str:
    return _ENTRIES.format(
        fiscal=warehouse.qualify("raw.fiscal_calendar"),
        cards=warehouse.qualify("raw.gift_cards"),
        jurisdictions=warehouse.qualify("raw.gift_card_jurisdictions"),
        ledger=warehouse.qualify("raw.gift_card_ledger"),
        rates=warehouse.qualify("raw.fx_rates"),
    )


def recognize(ds: str) -> int:
    """Rebuild one recognition day of `marts.gift_card_recognition`.

    Returns the rows written. The write is a delete-insert on the day, so the
    day it is given is the only day it touches and a second run of it leaves
    one copy.
    """
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('marts')}")
        con.execute(_DDL.format(table=warehouse.qualify(TABLE)))
        rows = con.execute(
            _BUILD.format(entries=_entries_sql()), [ds],
        ).fetchall()
        return warehouse.delete_insert(
            TABLE, PARTITION_COL, ds, rows, columns=COLUMNS, con=con,
        )


def tie_cards(ds: str) -> int:
    """The day's rows account for every entry the day recognizes.

    Returns the number of (entity, currency) pairs checked. A break here is a
    day that dropped an escheat into the revenue column or lost one on the way,
    and both are quiet: the row is still there and it still looks like a day.
    """
    with warehouse.connect(read_only=True) as con:
        held = con.execute(
            _TIE.format(table=warehouse.qualify(TABLE)), [ds],
        ).fetchall()
        owed = con.execute(
            _TIE_SOURCE.format(entries=_entries_sql()), [ds],
        ).fetchall()
    doubled = [row for row in held if row[3] != 1]
    if doubled:
        raise ValueError(
            f"{ds}: {doubled[0][0]}/{doubled[0][1]} holds {doubled[0][3]} rows. "
            "One row per entity and currency."
        )
    if held != owed:
        missing = sorted(set(owed) - set(held))
        raise ValueError(
            f"{ds}: {len(missing)} entity/currency pair(s) do not account for "
            f"the ledger. First: {missing[0] if missing else held[0]}."
        )
    return len(held)
