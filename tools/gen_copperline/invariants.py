"""The refuse-to-write pass. Any failure here deletes the output.

These are the day-zero data properties (day-zero-invariants.md) checked
generically over whatever tables exist, plus the planting checks from spec
07 section 4. Table-specific assertions live with the module that builds the
table; this pass is the floor every table stands on.
"""

from __future__ import annotations

from .config import Context

# Column pairs that make a table a fact feed: when both exist, the two-clock
# rules apply to it.
_EVENT_COLUMNS = ("event_time", "event_time_utc")


def _tables(ctx: Context, schema: str) -> list[str]:
    return [r[0] for r in ctx.sql(
        f"SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' ORDER BY table_name"
    ).fetchall()]


def _columns(ctx: Context, schema: str, table: str) -> dict[str, str]:
    return dict(ctx.sql(
        f"SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
    ).fetchall())


def run(ctx: Context) -> list[str]:
    failures: list[str] = []
    for schema in ("raw", "ops"):
        for table in _tables(ctx, schema):
            cols = _columns(ctx, schema, table)
            qualified = f"{schema}.{table}"

            # Money is integer minor units. The two armed exceptions arrive
            # as VARCHAR (Halcyon's decimal strings, the workbook), so any
            # float-typed money column is a generator bug, not an edge.
            for name, dtype in cols.items():
                if name.endswith("_cents") and dtype in ("FLOAT", "DOUBLE"):
                    failures.append(f"{qualified}.{name} is {dtype}; money is BIGINT cents")

            # Two clocks, ordered, minute-grain events.
            event = next((c for c in _EVENT_COLUMNS if c in cols), None)
            if event and "loaded_at" in cols:
                n = ctx.sql(f"SELECT count(*) FROM {qualified} "
                            f"WHERE loaded_at < {event}").fetchone()[0]
                if n:
                    failures.append(f"{qualified}: {n} row(s) loaded before they happened")
                n = ctx.sql(f"SELECT count(*) FROM {qualified} "
                            f"WHERE date_part('second', {event}) <> 0").fetchone()[0]
                if n:
                    failures.append(f"{qualified}.{event}: {n} row(s) carry seconds")

    failures += _late_tail(ctx)
    failures += _references(ctx)
    return failures


# Feeds whose measured lag distribution must match the declared tail, and the
# tail that governs each. The tolerance is half a point per bucket: the draw
# is exact in expectation and a real drift here means a module stopped
# reading the config.
_TAILED = (
    ("raw.orders", "default"),
    ("raw.pay_meridian_settlements", "default"),
    ("raw.marketplace_settlements", "marketplace"),
)


def _late_tail(ctx: Context) -> list[str]:
    failures: list[str] = []
    for table, tail_name in _TAILED:
        schema, name = table.split(".")
        cols = _columns(ctx, schema, name)
        event = next((c for c in _EVENT_COLUMNS if c in cols), None)
        if event is None or "loaded_at" not in cols:
            continue
        tail = ctx.cfg["late_tail"][tail_name]
        rows = ctx.sql(f"""
            SELECT least(datediff('day', {event}, loaded_at), 9) AS lag,
                   count(*) AS n
            FROM {table} WHERE {event} IS NOT NULL GROUP BY 1
        """).fetchall()
        total = sum(n for _, n in rows) or 1
        measured = {lag: n / total for lag, n in rows}
        top = max(int(d[1:]) for d in tail)
        if any(lag > top for lag in measured):
            failures.append(f"{table}: rows load later than day {top}")
        if measured.get(top, 0) == 0:
            failures.append(f"{table}: nothing loads on day {top}, and something always does")
        for day, share in tail.items():
            d = int(day[1:])
            if abs(measured.get(d, 0) - share) > 0.005 + 0.02 * share:
                failures.append(
                    f"{table}: day-{d} lag share {measured.get(d, 0):.4f} "
                    f"vs the declared {share}")
    return failures


# Cross-extract references: (referencing column, referenced column). A
# shipped reference that joins nothing is a world bug — this is the check
# that catches a module spelling an id its neighbour never minted.
_REFERENCES = (
    ("raw.support_tickets.order_id", "raw.orders.order_id"),
    ("raw.pos_sales_header.order_id", "raw.orders.order_id"),
    ("raw.disputes.invoice_id", "raw.invoices.invoice_id"),
    ("raw.payment_intents.order_ref", "raw.orders.order_ref"),
)


def _references(ctx: Context) -> list[str]:
    failures: list[str] = []
    for source, target in _REFERENCES:
        s_schema, s_table, s_col = source.split(".")
        t_schema, t_table, t_col = target.split(".")
        if s_col not in _columns(ctx, s_schema, s_table):
            failures.append(f"{source} does not exist to check")
            continue
        if t_col not in _columns(ctx, t_schema, t_table):
            failures.append(f"{target} does not exist to check")
            continue
        n, dangling = ctx.sql(f"""
            SELECT count(s.{s_col}),
                   count(s.{s_col}) FILTER (WHERE t.{t_col} IS NULL)
            FROM {s_schema}.{s_table} s
            LEFT JOIN {t_schema}.{t_table} t ON t.{t_col} = s.{s_col}
        """).fetchone()
        if n and dangling:
            failures.append(
                f"{source}: {dangling} of {n} reference(s) join nothing in {target}")
    return failures
