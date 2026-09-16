"""PLAT-455 — do the three copies of C-10's watch list agree, and did anything
stop being watched to make them agree?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it reads `config/alerts.yml` through
`projects.growth.lib.alerts` — the reader `gro_alerting_daily` uses — and
`contracts/alert_subjects.yml` through `include.lib.contracts`, the reader
`plat_contracts_enforce` uses. A file either of those two could not load fails
here first and says which one.

**Almost nothing is authored.** The relations below are computed from the config
as the agent left it: the schema has to name every table the config watches, and
the two have to agree about the owner and the check per table. Three things are
authored, and each is a shipped fact rather than a judgment:

    the thirteen (table, check) pairs the world ships     the anti-deletion floor
    the ten owners the config already had                  unchanged is the answer
    the three owners the reorganisation orphaned           see ORPHANED below

**The direction the reconciliation runs, and why it is not a coin toss.**
`contracts/alert_subjects.yml:7` says `source_of_truth: config/alerts.yml`, and
`contracts/alerting.md:22-23` §AL-3 says an alert exists because it is in the
config, which holds "the table name as a string, the check, the threshold and
the owner". So the config defines the membership, the owner and the check, and
the schema's list is a copy that fell behind. The schema is the side that moves.

The one place the config is the wrong side is §AL-1
(`contracts/alerting.md:9`): the owner has to be a team that exists. That is a
rule the config must satisfy, not a fact the config gets to define, and three
subjects break it.

**`raw.web_events` is not graded here at all.** GRO-241 has that feed open and
the ticket says to leave its rows alone in both files.
"""

from __future__ import annotations

import re

import pytest
import yaml

from include.lib import contracts, workspace_root
from include.lib.notify import TEAMS
from projects.growth.lib import alerts

#: The feed GRO-241 owns. Excluded from every comparison below.
GRO_241 = "raw.web_events"

#: Every (table, check) pair `config/alerts.yml` ships with. The ticket says a
#: table that stops being watched stops waking anybody, so all thirteen have to
#: survive the reconciliation. Extra subjects are fine: adding coverage is more
#: than the ticket asks and it breaks nothing.
SHIPPED_SUBJECTS = (
    ("raw.web_events", "freshness"),
    ("ops.session_daily", "row_count"),
    ("raw.ads_spend_daily", "freshness"),
    ("raw.comp_prices", "row_count"),
    ("marts.gmv_daily", "row_count"),
    ("marts.comp_sales_daily", "row_count"),
    ("raw.pos_sales_header", "freshness"),
    ("raw.carrier_scans", "null_rate"),
    ("marts.inventory_position", "row_count"),
    ("marts.customer_360", "null_rate"),
    ("raw.email_events", "freshness"),
    ("marts.audience_segments", "row_count"),
    ("raw.seo_rankings", "row_count"),
)

#: The ten owners `config/alerts.yml` already had right, as it ships them
#: (`config/alerts.yml:17-76`). Every one of them is the team whose project
#: builds the table, which is the pattern the file keeps. Unchanged is the
#: answer for all ten, and `marts.gmv_daily` is the one that costs something to
#: leave alone: `contracts/alert_subjects.yml:16` says finance about the same
#: table, because finance READS it for the 06:00 flash. Commerce builds it
#: (`dbt/copperline_analytics/models/commerce/gmv_daily.sql`) and commerce is
#: who a broken feed wakes.
SHIPPED_OWNERS = {
    "raw.web_events": "growth",
    "ops.session_daily": "growth",
    "raw.ads_spend_daily": "growth",
    "raw.comp_prices": "growth",
    "marts.gmv_daily": "commerce",
    "marts.comp_sales_daily": "finance",
    "raw.pos_sales_header": "commerce",
    "raw.carrier_scans": "supply",
    "marts.inventory_position": "supply",
    "marts.customer_360": "customer",
}

#: The three the reorganisation orphaned (`config/alerts.yml:78-96`, and the
#: open item at `contracts/alerting.md:35`). `lifecycle` and `acquisition` are
#: not in `include.lib.notify.TEAMS`, so `alerts.unowned` filters them out of
#: the paging step and they have woken nobody since.
#:
#: Growth for all three, and not a guess: growth runs every one of the feeds.
#: `projects/growth/dags/gro_email_engagement_daily.dag.yaml:17,24` lands
#: `raw.email_events`; `projects/growth/dags/gro_seo_rank_intake.dag.yaml:19,28`
#: lands `raw.seo_rankings`; `marts.audience_segments` is built from
#: `dbt/copperline_analytics/models/growth/audience_segments.sql` and published
#: by C-6, which `docs/report-registry.md:28` gives to growth.
ORPHANED = {
    "raw.email_events": "growth",
    "marts.audience_segments": "growth",
    "raw.seo_rankings": "growth",
}

#: The schema's word for each of the config's checks. The schema spells them
#: `volume` and `freshness` (`contracts/alert_subjects.yml:9-16`) and the config
#: spells the first one `row_count` (`config/alerts.yml:13-15`, "rows landed for
#: the day"), so either word is taken for a count. `null_rate` has no word in the
#: schema's vocabulary at all, so whatever an answer invents for the two
#: null-rate subjects passes — there is nothing in the tree to derive it from.
KIND_WORDS = {"row_count": {"volume", "row_count"}, "freshness": {"freshness"}}


def config_subjects() -> list[dict]:
    """`config/alerts.yml`, through the reader the alerting DAG reads with."""
    return [dict(s) for s in alerts.subjects()]


def by_table(subjects: list[dict]) -> dict[str, dict]:
    return {str(s.get("table", "")).strip(): s for s in subjects}


@pytest.fixture(scope="module")
def watched() -> dict[str, dict]:
    """What the config watches, keyed by table."""
    return by_table(config_subjects())


@pytest.fixture(scope="module")
def schema() -> dict:
    """`contracts/alert_subjects.yml`, through `include.lib.contracts`.

    The header is checked here rather than in a test of its own: an answer that
    reconciled the two files by turning the schema into something that is no
    longer C-10's schema has not reconciled anything.
    """
    contract = contracts.load("alert_subjects")
    assert contract.raw.get("consumer") == "C-10", (
        f"contracts/alert_subjects.yml is the schema for {contract.raw.get('consumer')!r}, "
        "and it was C-10"
    )
    assert str(contract.raw.get("source_of_truth", "")).strip() == "config/alerts.yml", (
        "contracts/alert_subjects.yml no longer names config/alerts.yml as its "
        "source of truth, which is the line the whole reconciliation turns on"
    )
    listed = contract.raw.get("subjects")
    assert isinstance(listed, list) and listed, (
        "contracts/alert_subjects.yml holds no subject list; the ticket says the "
        "schema stays a list, because a list that names nothing cannot say the "
        "config grew a subject nobody agreed"
    )
    return contract.raw


@pytest.fixture(scope="module")
def named(schema: dict) -> dict[str, dict]:
    """What the schema names, keyed by table."""
    entries = [dict(s) for s in schema["subjects"] if isinstance(s, dict)]
    tables = [str(s.get("table", "")).strip() for s in entries]
    duplicates = sorted({t for t in tables if tables.count(t) > 1})
    assert not duplicates, (
        f"contracts/alert_subjects.yml names {duplicates} more than once, and two "
        "entries for one table is two answers"
    )
    return by_table(entries)


def test_nothing_stopped_being_watched():
    """The floor. The schema names eight tables and the config watches thirteen,
    so the cheap way to make the two agree is to delete the ten subjects the
    schema never named — which takes ten live alerts down with it.

    `contracts/alert_subjects.yml:7` is what makes that wrong rather than merely
    unkind: the schema itself says the config is the source of truth, so a table
    the config watches and the schema does not name is a gap in the schema.
    """
    held = {(str(s.get("table", "")), str(s.get("check", ""))) for s in config_subjects()}
    missing = sorted(pair for pair in SHIPPED_SUBJECTS if pair not in held)
    assert not missing, f"config/alerts.yml no longer watches {missing}"


def test_every_watched_table_has_a_team_behind_it(watched: dict[str, dict]):
    """§AL-1, `contracts/alerting.md:9`. An alert nobody owns wakes the wrong
    person and gets muted, and `alerts.unowned` is the world's own test for it —
    an owner outside `include.lib.notify.TEAMS` counts as no owner, which is
    exactly the state the three orphaned subjects are in.

    Nothing is authored here: the six live team names come from the library.
    """
    orphans = alerts.unowned(list(watched.values()))
    assert not orphans, (
        "config/alerts.yml still holds subjects nobody is paged for: "
        + str(sorted((o["table"], o.get("owner"), o["reason"]) for o in orphans))
        + f"; the teams that exist are {list(TEAMS)}"
    )


def test_the_three_orphaned_subjects_went_to_the_team_that_runs_the_feed(
    watched: dict[str, dict]
):
    """See ORPHANED. Growth builds and runs all three feeds, so growth is the
    rotation a broken one should reach. An answer that picked a live team at
    random satisfies AL-1 and still wakes the wrong people."""
    wrong = {
        table: watched.get(table, {}).get("owner")
        for table, team in ORPHANED.items()
        if watched.get(table, {}).get("owner") != team
    }
    assert not wrong, (
        f"config/alerts.yml pages {wrong} for feeds growth builds and runs; "
        f"expected {ORPHANED}"
    )


def test_the_owners_the_config_already_had_did_not_move(watched: dict[str, dict]):
    """The other ten. §AL-3 makes the config the definition of who is paged, so
    where the schema disagrees with it the schema is what moves.

    This is what convicts the tempting shortcut on `marts.gmv_daily`: the two
    files name different teams, and editing the config to match the schema makes
    them agree while moving a live page from the team that builds the mart to
    the team that reads it. Same shape for any answer that retuned the owner
    column while it was in the file.
    """
    moved = {
        table: watched.get(table, {}).get("owner")
        for table, team in SHIPPED_OWNERS.items()
        if watched.get(table, {}).get("owner") != team
    }
    assert not moved, (
        f"config/alerts.yml now pages {moved}; it shipped "
        + str({t: SHIPPED_OWNERS[t] for t in moved})
    )


def test_the_schema_names_every_table_the_config_watches(
    watched: dict[str, dict], named: dict[str, dict]
):
    """The enforcement sentence in the schema's own header, made true. Computed
    from the config as the agent left it, so an answer that also added coverage
    has to name that too."""
    absent = sorted(set(watched) - set(named))
    assert not absent, (
        "contracts/alert_subjects.yml does not name "
        f"{absent}, and config/alerts.yml watches them; the schema's own header "
        "says enforcement checks the config holds nothing this file does not name"
    )


def test_the_two_files_agree_about_who_is_paged(
    watched: dict[str, dict], named: dict[str, dict]
):
    """Per table, for every table in both. One file saying commerce and the
    other saying finance about the same page is the disagreement the ticket
    exists to settle, and copying a retired name into the schema is the other
    way to fail this."""
    disagree = {
        table: (watched[table].get("owner"), named[table].get("owner"))
        for table in sorted(set(watched) & set(named))
        if table != GRO_241
        and str(watched[table].get("owner", "")).strip()
        != str(named[table].get("owner", "")).strip()
    }
    assert not disagree, (
        "config/alerts.yml and contracts/alert_subjects.yml name different teams "
        f"for {disagree} (config first)"
    )


def test_the_two_files_agree_about_what_is_checked(
    watched: dict[str, dict], named: dict[str, dict]
):
    """Per table, under the schema's own vocabulary. `marts.comp_sales_daily` is
    the shipped case: the schema calls it freshness and the config counts rows.

    The two null-rate subjects are skipped — the schema has no word for that
    check and nothing in the tree says what it should be.
    """
    wrong = {}
    for table in sorted(set(watched) & set(named)):
        check = str(watched[table].get("check", "")).strip()
        if table == GRO_241 or check not in KIND_WORDS:
            continue
        kind = str(named[table].get("kind", "")).strip().lower()
        if kind not in KIND_WORDS[check]:
            wrong[table] = (check, named[table].get("kind"))
    assert not wrong, (
        "contracts/alert_subjects.yml describes "
        f"{wrong} the wrong way (the config's check first); the schema's words "
        "are volume and freshness, and the config's row_count is a volume check"
    )


def test_the_third_copy_of_the_list_reaches_every_watched_mart(watched: dict[str, dict]):
    """`docs/lineage.md` is the same facts by table, and REG-2
    (`docs/report-registry.md:13`) makes its read set the complete one. C-10 is
    listed there under two marts and the config watches five, so three marts
    carry a reader nobody working that list would find.

    Only marts are graded: `docs/lineage.md:161` says outright that staging and
    raw tables are not in it, so the eight `raw.` and `ops.` subjects are out of
    scope and no answer is asked to add them.
    """
    text = (workspace_root() / "docs" / "lineage.md").read_text(encoding="utf-8")
    sections = {}
    current = None
    for line in text.splitlines():
        heading = re.match(r"^##\s+`([A-Za-z0-9_.]+)`\s*$", line)
        if heading:
            current = heading.group(1)
            sections[current] = []
        elif line.startswith("## "):
            current = None
        elif current:
            sections[current].append(line)

    marts = sorted(t for t in watched if t.startswith("marts."))
    unlisted = []
    for mart in marts:
        body = "\n".join(sections.get(mart, []))
        if not body:
            unlisted.append(f"{mart} (no section)")
        elif "config/alerts.yml" not in body and "C-10" not in body:
            unlisted.append(mart)
    assert not unlisted, (
        f"docs/lineage.md does not list C-10 as a reader of {unlisted}, and "
        "config/alerts.yml watches them"
    )


def test_the_schema_is_still_yaml_the_enforcement_run_can_read(schema: dict):
    """Belt and braces on the file `plat_contracts_enforce` globs every morning.
    `contracts.load` already parsed it to get here; this checks the two list
    checks the schema declares survived the amendment, since they are what an
    enforcement step would run.
    """
    raw = yaml.safe_load(
        (workspace_root() / "contracts" / "alert_subjects.yml").read_text(encoding="utf-8")
    )
    tests = {key for entry in (raw.get("tests") or []) if isinstance(entry, dict) for key in entry}
    assert "every_subject_has_owner" in tests, (
        "contracts/alert_subjects.yml no longer declares every_subject_has_owner, "
        f"which is half of what its header says enforcement does; it declares {sorted(tests)}"
    )
