"""Deterministic draws as SQL expressions.

Every random-looking value in the world comes from DuckDB's hash() over the
seed and a stream name, so each (table, day) is its own stream: regenerating
one day never moves another, and adding a table never moves an existing one.
These helpers build the SQL text; nothing here executes anything.

hash() returns UBIGINT; shifting one bit right before the BIGINT cast keeps
every draw in [0, 2^63) without overflow.
"""

from __future__ import annotations


def draw(seed: int, *parts: str) -> str:
    """A BIGINT draw from the named stream. Parts are SQL expressions —
    quote literals yourself: draw(seed, "'orders'", "d.ds", "g.i")."""
    return f"(hash({seed}, {', '.join(parts)}) >> 1)::BIGINT"


def uniform(h: str, lo: int, hi: int) -> str:
    """An integer in [lo, hi], uniform, from a draw."""
    return f"({lo} + ({h}) % {hi - lo + 1})"


def pick(h: str, weighted: list[tuple[str, int]]) -> str:
    """One of the values, by integer weight, from a draw.

    pick(h, [("'web'", 31), ("'store'", 58), ("'marketplace'", 11)])
    """
    total = sum(w for _, w in weighted)
    edges, acc = [], 0
    for value, weight in weighted[:-1]:
        acc += weight
        edges.append(f"WHEN ({h}) % {total} < {acc} THEN {value}")
    return f"(CASE {' '.join(edges)} ELSE {weighted[-1][0]} END)"


def lag_days(h: str, tail: dict[str, float]) -> str:
    """Days of load lag from a per-mille tail like {d0: 0.924, d1: 0.041, ...}.

    The tail is cumulative per-mille under the hood so the SQL stays integer.
    Nothing may arrive later than day 5, and the invariant pass asserts
    something always does arrive on the last declared day.
    """
    acc = 0
    arms = []
    days = sorted(tail, key=lambda k: int(k[1:]))
    for day in days[:-1]:
        acc += round(tail[day] * 1000)
        arms.append(f"WHEN ({h}) % 1000 < {acc} THEN {int(day[1:])}")
    return f"(CASE {' '.join(arms)} ELSE {int(days[-1][1:])} END)"


def minute_time(h: str, ds: str = "ds") -> str:
    """A timestamp inside the day with zero seconds, so any downstream
    minute-truncation is a no-op (generator contract, spec 07 section 5)."""
    return (
        f"({ds}::TIMESTAMP + INTERVAL 1 SECOND * "
        f"((({h}) % 86340) - (({h}) % 86340) % 60))"
    )
