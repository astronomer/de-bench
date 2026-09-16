"""Reading and enforcing `contracts/<mart>.yml`.

Every published mart has a contract file. The yml is the machine half —
columns, types, grain, the tests — and the `.md` beside it is the prose half,
which governs meaning. `plat_contracts_enforce` runs the yml against the
warehouse every morning; nothing runs the prose. Where the two disagree that
is a bug in one of them, and the fix is to the pair.

A contract is a promise to a named consumer. Changing a mart's grain or
dropping one of its columns amends the file FIRST, before the model — see
`contracts/README.md` and `docs/lineage.md`, which lists the readers dbt
cannot see.

    from include.lib import contracts

    contract = contracts.load("order_economics")
    violations = contracts.check(contract)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence

from . import contracts_dir, warehouse

__all__ = [
    "Contract",
    "Violation",
    "ContractError",
    "load",
    "load_all",
    "names",
    "check",
    "enforce",
]

#: The contract types, and the warehouse types each one accepts.
TYPES: dict[str, tuple[str, ...]] = {
    "string": ("VARCHAR", "TEXT", "STRING", "CHAR"),
    "date": ("DATE",),
    "timestamp": ("TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "DATETIME"),
    "integer": ("INTEGER", "INT", "INT4", "SIGNED"),
    "bigint": ("BIGINT", "INT8", "LONG"),
    "boolean": ("BOOLEAN", "BOOL", "LOGICAL"),
}

#: Warehouse types a money column may never have. `no_floats_in` looks for
#: these, because a float in a money column is a rounding error waiting for a
#: close.
FLOAT_TYPES = ("FLOAT", "REAL", "FLOAT4", "DOUBLE", "FLOAT8", "DECIMAL", "NUMERIC")


class ContractError(Exception):
    """One or more contract violations. Carries them on `.violations`."""

    def __init__(self, violations: Sequence["Violation"]):
        super().__init__("; ".join(str(v) for v in violations))
        self.violations = list(violations)


@dataclass(frozen=True)
class Violation:
    """One broken promise: which contract, which rule, and what was found."""

    model: str
    rule: str
    detail: str

    def __str__(self) -> str:
        return f"{self.model}: {self.rule}: {self.detail}"


@dataclass(frozen=True)
class Contract:
    """One `contracts/<name>.yml`, read.

    `model` is the mart it governs, `schema.table`. `grain` is the columns one
    row is unique by — read it before you change a model's group-by, because
    it is the promise the consumer built on. `columns` is the published
    surface, in order, each one a dict with `name`, `type`, `required` and
    sometimes `accepted_values`. `tests` is the list of assertions
    `plat_contracts_enforce` runs.

    `partition_by` and `freshness` are optional and most contracts carry
    neither. `grain` and the `unique` test usually agree and are allowed not
    to: `account_rollup` is unique at a finer grain than it publishes.

    Two files under `contracts/` govern a document rather than a mart —
    `privacy_surfaces.yml` and `alert_subjects.yml`. They load, `model` is
    None, and `check` says it has nothing to run against.
    """

    name: str
    path: Path
    raw: dict[str, Any]
    model: str | None
    consumer: str | None
    owner: str | None
    grain: list[str] = field(default_factory=list)
    columns: list[dict[str, Any]] = field(default_factory=list)
    tests: list[dict[str, Any]] = field(default_factory=list)
    partition_by: str | None = None
    freshness: Any = None

    @property
    def column_names(self) -> list[str]:
        return [c["name"] for c in self.columns]

    def column(self, name: str) -> dict[str, Any] | None:
        for candidate in self.columns:
            if candidate["name"] == name:
                return candidate
        return None


def load(mart: str) -> Contract:
    """Read one contract.

    `mart` is the file's stem, and a qualified model name works too:
    `load("order_economics")` and `load("marts.order_economics")` read the
    same file.
    """
    import yaml

    name = mart.rsplit(".", 1)[-1]
    path = contracts_dir() / f"{name}.yml"
    if not path.exists():
        raise FileNotFoundError(f"no contract at {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return Contract(
        name=name,
        path=path,
        raw=raw,
        model=raw.get("model"),
        consumer=raw.get("consumer"),
        owner=raw.get("owner"),
        grain=list(raw.get("grain") or []),
        columns=[dict(c) for c in raw.get("columns") or []],
        tests=[dict(t) for t in raw.get("tests") or []],
        partition_by=raw.get("partition_by"),
        freshness=raw.get("freshness"),
    )


def names() -> list[str]:
    """Every contract file's stem, sorted."""
    return sorted(p.stem for p in contracts_dir().glob("*.yml"))


def load_all() -> list[Contract]:
    """Every contract under `contracts/`."""
    return [load(name) for name in names()]


def check(contract: Contract | str, con: Any = None) -> list[Violation]:
    """Run a contract against the warehouse and return what it found.

    An empty list means the mart satisfies its contract. Each violation names
    the rule it broke. Six rules are checked, and they are the six the yml can
    state:

    - the model exists;
    - every column in `columns` is present, and no column is missing;
    - each column's warehouse type matches its contract type;
    - `unique` holds at the columns it names;
    - `not_null` holds at the columns it names;
    - `no_floats_in` finds no float, double or decimal column, and
      `non_negative` finds no negative value, and `row_count_min` finds at
      least that many rows;
    - `accepted_values` holds where a column declares one.

    Extra columns the contract does not name are NOT a violation. A mart may
    publish more than it promised; it may not publish less, or differently.

    Reads through `connect(read_only=True)` unless you pass a connection, so
    the morning enforcement run does not queue behind the build.
    """
    contract = load(contract) if isinstance(contract, str) else contract
    if not contract.model:
        return [Violation(contract.name, "model", "the contract names no model")]

    owned = con is None
    con = con or warehouse.connect(read_only=True)
    try:
        return list(_run(contract, con))
    finally:
        if owned:
            con.close()


def enforce(contract: Contract | str, con: Any = None) -> None:
    """`check`, and raise `ContractError` if anything came back."""
    violations = check(contract, con)
    if violations:
        raise ContractError(violations)


# --- the checks themselves -------------------------------------------------

def _run(contract: Contract, con: Any) -> Iterable[Violation]:
    model = contract.model
    table = warehouse.qualify(model)
    schema, _, bare = model.rpartition(".")
    live = con.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ?",
        [schema, bare],
    ).fetchall()
    if not live:
        yield Violation(model, "model", "the table does not exist")
        return

    types = {name: _base_type(kind) for name, kind in live}
    for column in contract.columns:
        name, want = column["name"], str(column.get("type", "")).lower()
        if name not in types:
            yield Violation(model, "columns", f"{name} is missing")
            continue
        accepts = TYPES.get(want)
        if accepts and types[name] not in accepts:
            yield Violation(
                model, "columns",
                f"{name} is {types[name]}, and the contract says {want}",
            )

    for test in contract.tests:
        for rule, argument in test.items():
            yield from _test(contract, con, table, types, rule, argument)

    for column in contract.columns:
        allowed = column.get("accepted_values")
        if not allowed or column["name"] not in types:
            continue
        marks = ", ".join("?" for _ in allowed)
        found = con.execute(
            f"SELECT DISTINCT {column['name']} FROM {table} "
            f"WHERE {column['name']} IS NOT NULL AND {column['name']} NOT IN ({marks})",
            list(allowed),
        ).fetchall()
        if found:
            yield Violation(
                model, "accepted_values",
                f"{column['name']} holds {sorted(str(r[0]) for r in found)}",
            )


def _test(contract: Contract, con: Any, table: str, types: dict[str, str],
          rule: str, argument: Any) -> Iterable[Violation]:
    model = contract.model
    if rule == "unique":
        columns = ", ".join(argument)
        extra = con.execute(
            f"SELECT count(*) FROM (SELECT {columns} FROM {table} "
            f"GROUP BY {columns} HAVING count(*) > 1)"
        ).fetchone()[0]
        if extra:
            yield Violation(model, "unique", f"{extra} duplicate keys at ({columns})")
    elif rule == "not_null":
        for name in argument:
            if name not in types:
                continue
            nulls = con.execute(
                f"SELECT count(*) FROM {table} WHERE {name} IS NULL"
            ).fetchone()[0]
            if nulls:
                yield Violation(model, "not_null", f"{name} is null on {nulls} rows")
    elif rule == "no_floats_in":
        for name in argument:
            if types.get(name) in FLOAT_TYPES:
                yield Violation(
                    model, "no_floats_in",
                    f"{name} is {types[name]}; money is integer cents",
                )
    elif rule == "non_negative":
        for name in argument:
            if name not in types:
                continue
            below = con.execute(
                f"SELECT count(*) FROM {table} WHERE {name} < 0"
            ).fetchone()[0]
            if below:
                yield Violation(model, "non_negative", f"{name} is negative on {below} rows")
    elif rule == "row_count_min":
        rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        if rows < int(argument):
            yield Violation(model, "row_count_min", f"{rows} rows, and it promises {argument}")
    # Any other rule belongs to a document contract rather than to a mart, and
    # the DAG that owns that document runs it. Unknown rules pass here.


def _base_type(kind: str) -> str:
    """A warehouse type without its width: `DECIMAL(18,2)` is `DECIMAL`."""
    return str(kind).split("(")[0].strip().upper()
