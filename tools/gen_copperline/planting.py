"""The planting pass: apply planted.yaml on top of the base draw.

Two mechanisms and only two (spec 07 section 4):

- insert: the row's keys come from a reserved block the base draw never
  touches, so a planted row is identical at every volume and can never
  collide with a drawn one. The entry carries the full column values.
- update: lands on an existing row named by rank — row_number() over the
  day's rows in key order — which is deterministic under the seed and
  unaffected by any other planted row on any other day.

Entries apply top to bottom. Every entry names the clause it tests and the
tasks that grade it; config.load_planted enforces that before this runs.
"""

from __future__ import annotations

from .config import Context


def _quote(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def apply(ctx: Context, planted: list[dict]) -> None:
    problems = validate(planted)
    if problems:
        raise SystemExit(
            "the planting pass refuses these rows:\n  - " + "\n  - ".join(problems)
        )
    for row in planted:
        action = row.get("action", "insert")
        if action == "insert":
            _insert(ctx, row)
        elif action == "update":
            _update(ctx, row)
        else:
            raise ValueError(f"planted row {row['id']!r}: unknown action {action!r}")


def validate(planted: list[dict]) -> list[str]:
    """Checks that must hold before any row applies — after would mean a
    half-planted world behind a clean-looking error."""
    problems: list[str] = []
    seen: dict[tuple, str] = {}
    for row in planted:
        # No two planted rows may touch the same target. For inserts the
        # values are the identity; for updates it is (table, ds, rank).
        if row.get("action", "insert") == "update":
            key = (row["table"], str(row["ds"]), row.get("rank"))
        else:
            key = (row["table"], tuple(sorted((row.get("values") or {}).items())))
        if key in seen:
            problems.append(f"planted rows {seen[key]!r} and {row['id']!r} touch the same target")
        seen[key] = row["id"]

        if row.get("changes_row_count") and row.get("action", "insert") != "insert":
            problems.append(f"planted row {row['id']!r}: only inserts change a row count")
    return problems


def _insert(ctx: Context, row: dict) -> None:
    values = dict(row.get("values") or {})
    if not values:
        raise ValueError(f"planted row {row['id']!r}: insert with no values")
    cols = ", ".join(values)
    vals = ", ".join(_quote(v) for v in values.values())
    ctx.sql(f"INSERT INTO {row['table']} ({cols}) VALUES ({vals})")


def _update(ctx: Context, row: dict) -> None:
    values = dict(row.get("values") or {})
    rank = row.get("rank")
    key = row.get("key")  # the column rank orders by, e.g. order_ref
    ds_column = row.get("ds_column", "ds")
    if not (values and rank and key):
        raise ValueError(f"planted row {row['id']!r}: update needs values, rank and key")
    sets = ", ".join(f"{c} = {_quote(v)}" for c, v in values.items())
    target = ctx.sql(f"""
        SELECT {key} FROM (
            SELECT {key}, row_number() OVER (ORDER BY {key}) AS rn
            FROM {row['table']} WHERE {ds_column} = '{row['ds']}'
        ) WHERE rn = {int(rank)}
    """).fetchone()
    if target is None:
        raise ValueError(
            f"planted row {row['id']!r}: rank {rank} exceeds the day's rows — "
            "a volume change silently unarming a row is exactly what this refuses"
        )
    ctx.sql(
        f"UPDATE {row['table']} SET {sets} "
        f"WHERE {ds_column} = '{row['ds']}' AND {key} = {_quote(target[0])}"
    )
