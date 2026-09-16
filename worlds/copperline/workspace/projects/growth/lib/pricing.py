"""The pricing marts' two lists, and the checks over what they produce.

**Two lists of markets, and they are not the same list.**
`seeds/markets.csv` is what the models build from — the markets that are in
the marts. `raw.market_config` is the market-setup team's export and the
record of which markets exist commercially, whatever reporting has caught up
with. `docs/runbooks/market-setup.md` §MKT-1 owns the distinction and says
which is the system of record for a market's attributes.

`gro_market_seed_build` writes the seed from the table each Monday, over the
market list in its own blueprint. Nothing checks the two against each other,
which is an open item in that runbook rather than a fault in this module.

Everything here reads. The models do the writing.
"""

from __future__ import annotations

from include.lib import warehouse, workspace_root

__all__ = ["SEED_PATH", "seeded_markets", "configured_markets",
           "pricing_problems"]

#: The committed seed the pricing models build from.
SEED_PATH = "dbt/copperline_analytics/seeds/markets.csv"


def seeded_markets() -> list[str]:
    """The market codes in `seeds/markets.csv`, in file order.

    Read from the file rather than from the warehouse, because a run that
    happens before the nightly seed load has to see what the models will
    build from and not what they built from yesterday.
    """
    import csv

    path = workspace_root() / SEED_PATH
    with path.open(encoding="utf-8", newline="") as handle:
        return [row["market_code"] for row in csv.DictReader(handle)]


def configured_markets() -> list[dict]:
    """Every market in `raw.market_config`, with its currency and entity.

    This is the system of record for a market's attributes. It is longer than
    the seed, and the difference is the markets that exist and are in no
    mart.
    """
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            "SELECT market_code, billing_currency, entity_code, tax_regime, "
            f"launched_on, is_active FROM {warehouse.qualify('raw.market_config')} "
            "ORDER BY market_code"
        ).fetchall()
    return [{"market_code": code, "billing_currency": currency,
             "entity_code": entity, "tax_regime": regime,
             "launched_on": str(launched), "is_active": bool(active)}
            for code, currency, entity, regime, launched, active in rows]


def pricing_problems(markets: list[str]) -> list[dict]:
    """Seeded markets missing from the mart, and margins that cannot be real.

    Two failures with one shape. A market that drops out of the mart reads as
    a market with no sales, which somebody will believe. A margin outside
    minus one hundred to one hundred per cent is arithmetic going wrong, not
    a trading result.
    """
    if not markets:
        return []
    pricing = warehouse.qualify("marts.market_pricing")
    quoted = ", ".join(f"'{market}'" for market in markets)
    with warehouse.connect(read_only=True) as con:
        present = {row[0] for row in con.execute(
            f"SELECT DISTINCT market_code FROM {pricing}"
        ).fetchall()}
        impossible = con.execute(
            f"""
            SELECT market_code, ds, margin_bps FROM {pricing}
            WHERE market_code IN ({quoted})
              AND (margin_bps < -10000 OR margin_bps > 10000)
            ORDER BY ds, market_code
            """
        ).fetchall()
    problems = [{"market_code": market, "problem": "missing from the mart"}
                for market in markets if market not in present]
    problems += [{"market_code": market, "ds": str(ds), "problem": "margin_bps "
                  f"is {margin}"} for market, ds, margin in impossible]
    return problems
