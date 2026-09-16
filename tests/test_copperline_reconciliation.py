"""The two revenue books have to agree, cent for cent.

Copperline keeps two independent readings of `docs/finance-policy.md`:

* **the ledger** — `worlds/copperline/workspace/fixtures/finance/ledger_monthly.csv`,
  hand-worked by the controller, its working in `tools/copperline_ledger_workings/`;
* **book three** — `tools/copperline_oracle/recognition.py`, an exact-`Decimal`
  model written from the policy text and the raw tables.

The ledger is the oracle the recognized-revenue flagship grades against, so it
is worth nothing unless the other reading lands on the same numbers. Spec 07
section 8 states the rule that governs a disagreement: it resolves by a change
to the fixtures or a change to the policy text, **never by an edit to the
ledger to make it agree.**

Two gates, because the shipped ledger is a full-profile file and a full build
takes about twenty minutes.

**The small-profile tie runs every time.** It builds a small world, derives the
ledger from it with `close.py` and runs book three over the same world, and
requires the two to agree on every entity-month. Neither side is the shipped
file: this is the two derivations checking each other, which is the property
that actually has to hold.

**The full-profile tie is the pre-ship gate**, and it also ties both books to
the CSV that ships. It is skipped unless `DE_BENCH_COPPERLINE_FULL=1`:

    DE_BENCH_COPPERLINE_FULL=1 PYTHONPATH=src python -m pytest \\
        tests/test_copperline_reconciliation.py -q

What always runs instead is `test_the_shipped_ledger_holds_the_agreed_totals`,
which pins the full-profile per-entity totals both books produced when that
gate was last run. It costs nothing and it fails the moment somebody edits the
ledger by hand.

One thing this gate does not prove any more. The two books were written by two
authors who had not seen each other's work, and that is what made the first
tie worth something: sixteen real disagreements came out of it. The rulings
that settled them were then applied to both books by one hand, so from here the
tie is a regression test rather than a second opinion. The second opinion is
`tools/copperline_ledger_workings/NOTES.md` section 7, which is the record of
what the two authors actually disagreed about and how each one was decided.
"""

import csv
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

from de_bench.tasks import repo_root

sys.path.insert(0, str(repo_root() / "tools"))

from copperline_oracle.recognition import recognize  # noqa: E402


def _load_working():
    """`close.py` by path: it is a script beside its notes, not a package."""
    path = repo_root() / "tools" / "copperline_ledger_workings" / "close.py"
    spec = importlib.util.spec_from_file_location("copperline_close", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


close = _load_working()

WORLD = repo_root() / "worlds" / "copperline"
LEDGER = WORLD / "workspace" / "fixtures" / "finance" / "ledger_monthly.csv"
FIXTURES = WORLD / "workspace" / "fixtures"

FULL = os.environ.get("DE_BENCH_COPPERLINE_FULL") == "1"

#: What the two books agreed on at the full profile, per entity: the number of
#: closed months and the recognized cents over them. Both books produced these
#: independently. Move them only when the full-profile gate has been run again
#: and both books still agree.
AGREED = {
    "CL-US": (28, 202_468_243_057),
    "CL-GB": (14, 19_597_945_401),
    "CL-IE": (14, 7_215_588_683),
    "CL-DE": (14, 14_313_472_846),
    "CL-MX": (4, 42_307_777),
}


def build(tmp_path: Path, profile: str) -> Path:
    """A copperline world through the real CLI, with the hand-authored fixtures.

    `raw.entities` and `raw.gift_card_jurisdictions` ship as source and land at
    bake time, and REV-9 and REV-12 both read them, so a world that has not been
    through the loader is not a world either book can be run against.
    """
    db = tmp_path / "copperline.duckdb"
    landing = tmp_path / "landing"
    env = {"PATH": "/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1",
           "PYTHONPATH": str(repo_root() / "tools")}
    proc = subprocess.run(
        [sys.executable, "-m", "gen_copperline",
         "--timeline", str(WORLD / "timeline.yaml"),
         "--planted", str(WORLD / "planted.yaml"),
         "--db", str(db), "--landing", str(landing),
         "--profile", profile],
        cwd=repo_root() / "tools", capture_output=True, text=True, env=env)
    assert proc.returncode == 0, proc.stderr[-2000:]
    proc = subprocess.run(
        [sys.executable, str(repo_root() / "tools" / "gen_copperline_load_fixtures.py"),
         "--fixtures", str(FIXTURES), "--db", str(db), "--landing", str(landing)],
        capture_output=True, text=True, env=env)
    assert proc.returncode == 0, proc.stderr[-2000:]
    return db


def both_books(db: Path):
    """The ledger's numbers and book three's, keyed the same way."""
    ledger, _steps, _months, _closed, _order, _opened = close.derive(str(db))
    left = {(entity, month): cents for entity, month, cents in ledger}
    right = {(row["entity_code"], row["fiscal_month"]): row["recognized_cents"]
             for row in recognize(db)}
    return left, right


def report(left: dict, right: dict) -> str:
    """Every entity-month the two books disagree on, in cents."""
    lines = []
    for key in sorted(set(left) | set(right)):
        a, b = left.get(key), right.get(key)
        if a != b:
            lines.append(f"{key[0]} {key[1]}: ledger {a}, book three {b}, "
                         f"difference {None if None in (a, b) else b - a}")
    return "\n".join(lines)


@pytest.fixture(scope="module")
def small(tmp_path_factory) -> Path:
    return build(tmp_path_factory.mktemp("copperline-small"), "small")


@pytest.fixture(scope="module")
def shipped() -> list[dict]:
    with LEDGER.open(newline="") as fh:
        return list(csv.DictReader(fh))


# ---------------------------------------------------------------------------
# The tie that runs every time
# ---------------------------------------------------------------------------

def test_the_two_books_tie_at_the_small_profile(small):
    """Two readings of one policy, over one world, to the cent.

    A failure here is a disagreement between the books, not a broken test.
    Spec 07 section 8: resolve it in the fixtures or in the policy text, and
    never by editing the ledger to agree.
    """
    left, right = both_books(small)
    assert not report(left, right), "the two books disagree:\n" + report(left, right)
    assert left, "neither book produced a row"


def test_the_small_profile_covers_the_same_entity_months_as_the_ship_file(small, shipped):
    """The trade book does not scale between profiles, so the shape does not
    either: the same five entities over the same closed months."""
    left, _right = both_books(small)
    assert {(row["entity_code"], row["fiscal_month"]) for row in shipped} == set(left)


def test_the_shipped_ledger_holds_the_agreed_totals(shipped):
    """The full-profile numbers both books last agreed on, pinned.

    This is the cheap half of the gate. It does not rebuild anything; it says
    that the file on disk is still the file the two books tied on.
    """
    per_entity: dict[str, list[int]] = {}
    for row in shipped:
        got = per_entity.setdefault(row["entity_code"], [0, 0])
        got[0] += 1
        got[1] += int(row["recognized_cents"])
    assert {k: tuple(v) for k, v in per_entity.items()} == AGREED


# ---------------------------------------------------------------------------
# The pre-ship gate
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not FULL, reason="set DE_BENCH_COPPERLINE_FULL=1; ~20 minutes")
def test_the_two_books_tie_at_the_full_profile_and_the_ship_file_is_one_of_them(
        tmp_path_factory, shipped):
    """The gate the world ships behind.

    Book three, the controller's working and the CSV that ships all carry the
    same 74 numbers.
    """
    db = build(tmp_path_factory.mktemp("copperline-full"), "full")
    left, right = both_books(db)
    assert not report(left, right), "the two books disagree:\n" + report(left, right)

    on_disk = {(row["entity_code"], row["fiscal_month"]): int(row["recognized_cents"])
               for row in shipped}
    assert on_disk == left, (
        "the shipped ledger is not what the working derives. Work the months "
        "again and write the numbers up in "
        "tools/copperline_ledger_workings/NOTES.md.")
