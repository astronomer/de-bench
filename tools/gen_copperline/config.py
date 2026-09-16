"""timeline.yaml loading and the build context every module receives."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

import yaml

# Fact volumes divide by this under --profile small. Entity tables (stores,
# customers, calendars) never scale: boundary populations must exist at every
# profile, and 4,000 trade accounts is already small.
SMALL_DIVISOR = 20

_REQUIRED = ("world", "seed", "today", "range", "fiscal", "eras", "volumes", "late_tail")


def load_timeline(path: str | Path) -> dict:
    cfg = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    missing = [k for k in _REQUIRED if k not in cfg]
    if missing:
        raise ValueError(f"timeline.yaml is missing {', '.join(missing)}")
    start = dt.date.fromisoformat(str(cfg["range"]["start"]))
    end = dt.date.fromisoformat(str(cfg["range"]["end"]))
    today = dt.date.fromisoformat(str(cfg["today"]))
    if not start < end:
        raise ValueError(f"range start {start} is not before end {end}")
    if today != end + dt.timedelta(days=1):
        # The whole history exists as of today; a gap would mean days the
        # world is silent about, an overlap would mean data from the future.
        raise ValueError(f"today {today} must be the day after range end {end}")
    for era_id, era in (cfg["eras"] or {}).items():
        if not any(k in era for k in ("at", "from")):
            raise ValueError(f"era {era_id} has neither 'at' nor 'from'")
    return cfg


def load_planted(path: str | Path) -> list[dict]:
    doc = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or []
    if not isinstance(doc, list):
        raise ValueError("planted.yaml must be a list of planted rows")
    for row in doc:
        for field in ("id", "table", "ds"):
            if not row.get(field):
                raise ValueError(f"planted row {row.get('id', '?')!r} is missing {field!r}")
        if not row.get("tasks"):
            raise ValueError(f"planted row {row['id']!r} names no task")
        if not row.get("clause"):
            raise ValueError(f"planted row {row['id']!r} names no clause")
    ids = [r["id"] for r in doc]
    if len(ids) != len(set(ids)):
        raise ValueError("planted row ids are not unique")
    return doc


@dataclass
class Context:
    """What every build module gets. Modules read config and write SQL; they
    never read the wall clock and never read another module's output except
    through the sim tables it declared."""

    con: object  # duckdb.DuckDBPyConnection
    cfg: dict
    profile: str
    landing: Path

    @property
    def seed(self) -> int:
        return int(self.cfg["seed"])

    @property
    def start(self) -> dt.date:
        return dt.date.fromisoformat(str(self.cfg["range"]["start"]))

    @property
    def end(self) -> dt.date:
        return dt.date.fromisoformat(str(self.cfg["range"]["end"]))

    @property
    def today(self) -> dt.date:
        return dt.date.fromisoformat(str(self.cfg["today"]))

    def era(self, era_id: str) -> dict:
        return self.cfg["eras"][era_id]

    def scale(self, n: int | float) -> int:
        """A fact volume at this profile. Entities do not go through this."""
        if self.profile == "full":
            return int(n)
        return max(1, round(n / SMALL_DIVISOR))

    def sql(self, statement: str) -> object:
        return self.con.execute(statement)

    def landing_dir(self, source: str) -> Path:
        d = self.landing / source
        d.mkdir(parents=True, exist_ok=True)
        return d
