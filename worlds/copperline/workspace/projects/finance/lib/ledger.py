"""The tie: a published figure against the general ledger.

`raw.finance_ledger` is the extract from Ironwood, at legal entity by closed
fiscal month grain, and §REV-9 makes it the authority a closed month is measured
against. A difference is a defect in the pipeline until somebody shows it is a
defect in the ledger, which has happened twice since FY2024 and both times took
a journal entry.

Two things this module knows that the DAGs above it do not.

**A month with no ledger row is not a break.** The ledger covers a month once
that month has closed, and an entity has no coverage before the month it began
trading. A tie over a period that predates the entity has nothing to tie to.

**The tolerance is for the alert, not for the figure.** §REV-9 says the tie is
to the cent and it means it. `config/recon.yml` carries a floor under which a
break is recorded and not raised, because the first month of the tie paged
somebody at midnight over two cents in an entity with eleven rows.
"""

from __future__ import annotations

from typing import Any

from include.lib import warehouse

__all__ = ["subject_table", "tie_to_ledger", "write_breaks", "fail_on_breaks"]


def subject_table(config: dict[str, Any]) -> str:
    """The table this tie runs over, from the reconciliation config.

    Assembled from three keys rather than written out. That is a leftover from
    when the tie ran over four subjects and the DAG was rendered once per
    subject; one subject survived and the assembly did not get taken out.
    """
    return f"{config['schema']}.{config['subject']}_{config['grain']}"


def tie_to_ledger(table: str, column: str, ledger: str, fiscal_month: str,
                  *, tolerance_cents: int = 0,
                  skip_before_first_close: bool = True) -> list[dict]:
    """Compare a published figure to the ledger, per entity, for one month.

    Returns one row per entity: the published amount, the ledger amount, the
    difference in cents, and whether it is outside the tolerance. An entity the
    ledger does not cover for that month is returned with a null ledger amount
    and is never a break.
    """
    with warehouse.connect(read_only=True) as con:
        published = dict(con.execute(
            f"SELECT entity, coalesce(sum({column}), 0)::BIGINT "
            f"FROM {warehouse.qualify(table)} WHERE fiscal_month = ? GROUP BY entity",
            [fiscal_month],
        ).fetchall())
        booked = dict(con.execute(
            f"SELECT entity, coalesce(sum(amount_cents), 0)::BIGINT "
            f"FROM {warehouse.qualify(ledger)} WHERE fiscal_month = ? GROUP BY entity",
            [fiscal_month],
        ).fetchall())

    rows = []
    for entity in sorted(set(published) | set(booked)):
        ledger_amount = booked.get(entity)
        if ledger_amount is None and skip_before_first_close:
            rows.append({"entity": entity, "fiscal_month": fiscal_month,
                         "published_cents": published.get(entity, 0),
                         "ledger_cents": None, "difference_cents": None,
                         "is_break": False})
            continue
        difference = published.get(entity, 0) - (ledger_amount or 0)
        rows.append({"entity": entity, "fiscal_month": fiscal_month,
                     "published_cents": published.get(entity, 0),
                     "ledger_cents": ledger_amount,
                     "difference_cents": difference,
                     "is_break": abs(difference) > tolerance_cents})
    return rows


def write_breaks(rows: list[dict], table: str, fiscal_month: str) -> int:
    """Replace the month's rows in the breaks table. Returns the rows written.

    Every entity is written, not only the breaks. A month where nothing broke
    is a fact worth being able to show, and an empty table cannot be told apart
    from a tie that never ran.

    `include.lib.warehouse.delete_insert` is the house write and it partitions
    on a date; this table is keyed by fiscal month, which is a 4-5-4 period and
    not a date. So the delete and the insert are written out here, in one
    transaction, with the same rule behind them: the month is replaced whole
    and a second run of a month leaves one copy.
    """
    columns = ["fiscal_month", "entity", "published_cents", "ledger_cents",
               "difference_cents", "is_break"]
    target = warehouse.qualify(table)
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('ops')}")
        con.execute(
            f"""CREATE TABLE IF NOT EXISTS {target} (
                fiscal_month VARCHAR NOT NULL, entity VARCHAR NOT NULL,
                published_cents BIGINT, ledger_cents BIGINT,
                difference_cents BIGINT, is_break BOOLEAN)"""
        )
        try:
            con.execute("BEGIN TRANSACTION")
            con.execute(f"DELETE FROM {target} WHERE fiscal_month = ?", [fiscal_month])
            con.executemany(
                f"INSERT INTO {target} ({', '.join(columns)}) VALUES (?, ?, ?, ?, ?, ?)",
                [[fiscal_month, row["entity"], row["published_cents"],
                  row["ledger_cents"], row["difference_cents"], row["is_break"]]
                 for row in rows],
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return len(rows)


def fail_on_breaks(rows: list[dict]) -> int:
    """Raise when any entity is outside the tolerance. Returns the break count.

    Call it after the breaks are written. The record is what the close team
    reads in the morning and it has to exist whichever way this goes.
    """
    breaks = [row for row in rows if row["is_break"]]
    if breaks:
        raise ValueError(
            "the ledger does not tie: "
            + "; ".join(
                f"{row['entity']} {row['fiscal_month']} off by "
                f"{row['difference_cents']} cents"
                for row in sorted(breaks, key=lambda r: r["entity"])
            )
        )
    return 0
