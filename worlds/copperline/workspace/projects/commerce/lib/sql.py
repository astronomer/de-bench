"""Reading commerce's SQL out of files.

Commerce keeps its SQL in `projects/commerce/sql/`, one statement per file, so
that the statement a DAG runs is the statement a person can paste into a prompt
when the number is wrong. `CONVENTIONS.md` states the rule; this is the reader.

    from projects.commerce.lib import sql

    con.execute(sql.read("economics_publish"), [ds])

Values are bound as parameters. Nothing here formats a date into the text.
"""

from __future__ import annotations

from pathlib import Path

__all__ = ["SQL_DIR", "read", "names"]


def SQL_DIR() -> Path:
    """`projects/commerce/sql/`, resolved from this file rather than from the
    working directory."""
    return Path(__file__).resolve().parents[1] / "sql"


def read(name: str) -> str:
    """The statement in `projects/commerce/sql/<name>.sql`.

    Raises `FileNotFoundError` naming the file, because the usual cause is a
    statement that was renamed on one side only.
    """
    path = SQL_DIR() / f"{name}.sql"
    if not path.exists():
        raise FileNotFoundError(f"no commerce statement at {path}")
    return path.read_text(encoding="utf-8")


def names() -> list[str]:
    """Every statement commerce ships, sorted. For the review tooling."""
    return sorted(path.stem for path in SQL_DIR().glob("*.sql"))
