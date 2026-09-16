"""The hand-authored half of copperline: the reference tables, the dbt seed,
the carrier rate cards and the finance workbook.

Extract convention 7 (spec 03 section 1) keeps these out of the generator and
ships them as source. Nothing else checks them, because nothing else builds
them — so this module holds every count the chapters fix, the arithmetic the
rate cards have to reproduce, the eight mess features of the workbook, and the
guard that keeps the generator's hands off all of it.
"""

from __future__ import annotations

import csv
import datetime as dt
import re
import sys
from pathlib import Path

import pytest

from de_bench.tasks import repo_root

pytest.importorskip("duckdb")
import duckdb  # noqa: E402

WORLD = repo_root() / "worlds" / "copperline"
WORKSPACE = WORLD / "workspace"
FIXTURES = WORKSPACE / "fixtures"
REFERENCE = FIXTURES / "reference"
SEED = WORKSPACE / "dbt" / "copperline_analytics" / "seeds" / "markets.csv"
WORKBOOK = FIXTURES / "finance" / "logistics_cost_workbook_fy26q1.csv"
GEN = repo_root() / "tools" / "gen_copperline"
LOADER = repo_root() / "tools" / "gen_copperline_load_fixtures.py"

RANGE = (dt.date(2024, 2, 4), dt.date(2026, 6, 14))

#: file -> rows under the header. Every one is fixed by a spec chapter.
ROW_COUNTS = {
    REFERENCE / "market_config.csv": 9,
    REFERENCE / "entities.csv": 5,
    REFERENCE / "pay_processor_windows.csv": 2,
    REFERENCE / "gift_card_jurisdictions.csv": 66,
    REFERENCE / "carrier_rate_cards.csv": 80,
    REFERENCE / "carrier_fuel_surcharges.csv": 372,
    SEED: 5,
}

#: The two FY2026 cards, which carry the reprice. Every other card is the
#: version the carriers actually billed on.
REPRICED = {"BL-2026A", "PP-26Q1"}


def rows(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def carriers_module():
    sys.path.insert(0, str(repo_root() / "tools"))
    from gen_copperline.extracts import carriers

    return carriers


# --- the counts every chapter fixes ----------------------------------------

@pytest.mark.parametrize("path,expected", sorted(ROW_COUNTS.items(),
                                                 key=lambda kv: str(kv[0])))
def test_each_file_parses_at_its_stated_count(path, expected):
    assert path.exists(), f"{path} is a rule-7 file and has to ship"
    assert len(rows(path)) == expected


# --- S10: markets, entities and the processor windows ----------------------

def test_market_config_carries_the_rows_that_contradict_the_prior():
    """Chapter 01 section 3.4. Four of the nine rows say what a model is sure
    is wrong, and the CA row is the breadcrumb that proves the table."""
    config = {row["market_code"]: row for row in rows(REFERENCE / "market_config.csv")}
    assert set(config) == {"US", "CA", "GB", "IE", "DE", "BR", "MX", "PL", "ID"}

    assert config["CA"]["billing_currency"] == "USD"
    assert config["CA"]["entity_code"] == "CL-US"
    assert config["BR"]["billing_currency"] == "USD"
    assert config["BR"]["entity_code"] == "CL-US"
    assert config["ID"]["billing_currency"] == "USD"
    assert config["ID"]["entity_code"] == "CL-US"
    assert config["PL"]["billing_currency"] == "EUR"
    assert config["PL"]["entity_code"] == "CL-IE"
    assert config["PL"]["tax_regime"] == "eu_vat_oss"
    # The one row that matches the prior, so the naive answer is four fifths
    # wrong rather than wholly wrong.
    assert config["MX"]["billing_currency"] == "MXN"
    assert config["MX"]["entity_code"] == "CL-MX"

    assert {row["launched_on"] for row in
            (config["BR"], config["MX"], config["PL"], config["ID"])} == {"2026-02-01"}
    for market in ("GB", "IE", "DE"):
        assert config[market]["launched_on"] == "2025-04-07"


def test_market_config_agrees_with_the_currency_the_generator_stamps():
    """`core.BILLING_CURRENCY` is what every generated row carries. The table
    is the system of record; a disagreement here is an unplanted fault."""
    sys.path.insert(0, str(repo_root() / "tools"))
    from gen_copperline.extracts import _boundary
    from gen_copperline.upstream import core

    by_market = {_boundary.MARKET_OF_COUNTRY[country]: code
                 for country, code in core.BILLING_CURRENCY.items()}
    for row in rows(REFERENCE / "market_config.csv"):
        assert row["billing_currency"] == by_market[row["market_code"]]
        assert row["entity_code"] == _boundary.ENTITY_OF_MARKET[row["market_code"]]


def test_the_seed_is_the_five_live_markets_and_agrees_with_the_table():
    """NLO-3 turns on the seed being a shorter list than the table, and on the
    rows they share saying the same thing."""
    seed = rows(SEED)
    assert [row["market_code"] for row in seed] == ["CA", "DE", "GB", "IE", "US"]
    assert list(seed[0]) == ["market_code", "currency", "entity"]

    config = {row["market_code"]: row for row in rows(REFERENCE / "market_config.csv")}
    for row in seed:
        assert row["currency"] == config[row["market_code"]]["billing_currency"]
        assert row["entity"] == config[row["market_code"]]["entity_code"]


def test_entities_holds_the_five_the_generator_books_to():
    sys.path.insert(0, str(repo_root() / "tools"))
    from gen_copperline.upstream import _common

    authored = {row["entity_code"]: row for row in rows(REFERENCE / "entities.csv")}
    assert set(authored) == {"CL-US", "CL-GB", "CL-IE", "CL-DE", "CL-MX"}
    for _id, code, name, country, currency, since in _common.LEGAL_ENTITIES:
        assert authored[code]["entity_name"] == name
        assert authored[code]["country_code"] == country
        assert authored[code]["functional_currency"] == currency
        assert authored[code]["first_traded_on"] == since


def test_the_go_live_dates_the_feeds_stamp_are_the_ones_the_table_states():
    """Every feed that carries an entity code goes through
    `_boundary.entity_at`, which books a market to CL-US until the entity that
    covers it has opened. The day it opens is `first_traded_on` here. A
    disagreement would put revenue in an entity that did not exist yet, which
    is what `docs/finance-policy.md`'s entity table forbids."""
    sys.path.insert(0, str(repo_root() / "tools"))
    from gen_copperline.extracts import _boundary

    authored = {row["entity_code"]: row["first_traded_on"]
                for row in rows(REFERENCE / "entities.csv")}
    assert _boundary.ENTITY_FIRST_TRADED == authored
    assert set(_boundary.ENTITY_OF_MARKET.values()) <= set(authored)


def test_processor_windows_hand_the_switchover_over_as_data():
    """E5. Halcyon is authoritative through 2025-08-31, Meridian from the
    first of September, and the second row has no end."""
    windows = {row["processor"]: row for row in
               rows(REFERENCE / "pay_processor_windows.csv")}
    assert set(windows) == {"halcyon", "meridian"}
    assert windows["halcyon"]["authoritative_to"] == "2025-08-31"
    assert windows["meridian"]["authoritative_from"] == "2025-09-01"
    assert windows["meridian"]["authoritative_to"] == ""
    assert (dt.date.fromisoformat(windows["halcyon"]["authoritative_from"])
            <= RANGE[0])


# --- S12: the gift-card jurisdictions --------------------------------------

def test_the_jurisdictions_are_the_66_codes_the_generator_mints():
    sys.path.insert(0, str(repo_root() / "tools"))
    from gen_copperline.extracts import promos

    expected = {f"{market}-{n:02d}"
                for market, count in promos.JURISDICTIONS.items()
                for n in range(1, count + 1)}
    assert len(expected) == 66
    authored = rows(REFERENCE / "gift_card_jurisdictions.csv")
    assert {row["jurisdiction_code"] for row in authored} == expected
    for row in authored:
        assert row["jurisdiction_code"].startswith(row["market_code"] + "-")


def test_escheat_and_breakage_are_the_two_sides_of_one_clause():
    """REV-12: an unspent card recognizes breakage 24 months after issue,
    except where the jurisdiction escheats instead. Both sides carry rows, and
    no row carries both or neither."""
    authored = rows(REFERENCE / "gift_card_jurisdictions.csv")
    escheat = [row for row in authored if row["escheat_applies"] == "true"]
    breakage = [row for row in authored if row["breakage_allowed"] == "true"]
    assert len(escheat) + len(breakage) == len(authored)
    assert 10 <= len(escheat) <= 30, "both sides have to be worth finding"

    sys.path.insert(0, str(repo_root() / "tools"))
    from gen_copperline.extracts import promos

    for row in authored:
        if row["escheat_applies"] == "true":
            assert row["breakage_allowed"] == "false"
            assert int(row["dormancy_months"]) > promos.BREAKAGE_MONTHS
            assert row["escheat_to"] and not row["breakage_after_months"]
        else:
            assert int(row["breakage_after_months"]) == promos.BREAKAGE_MONTHS
            assert not row["dormancy_months"] and not row["escheat_to"]

    # A "US escheats, nobody else does" shortcut is wrong on both sides.
    assert any(row["market_code"] == "US" and row["escheat_applies"] == "false"
               for row in authored)
    assert any(row["market_code"] != "US" and row["escheat_applies"] == "true"
               for row in authored)


# --- S6b: the rate cards ---------------------------------------------------

def test_version_1_reproduces_the_price_the_carrier_billed():
    """`carriers.py` bills every package on one card, and FIN-388's answer is
    the difference between that and the card in force. Twenty sampled cells,
    priced both ways."""
    module = carriers_module()
    cards = {(row["card_version"], row["carrier_code"], row["service_level"],
              int(row["break_seq"])): row
             for row in rows(REFERENCE / "carrier_rate_cards.csv")}

    # Twenty cells spread over the whole version-1 card set, each at a zone
    # from the 2-to-8 range `raw.shipment_packages` draws.
    cells = [key for key in sorted(cards) if key[0] not in REPRICED]
    sample = [(cells[n * len(cells) // 20], 2 + n % 7) for n in range(20)]
    assert len({key for key, _ in sample}) == 20

    for (version, carrier, service, seq), zone in sample:
        row = cards[(version, carrier, service, seq)]
        expected = (module.RATE_BASE_CENTS[service]
                    + zone * module.ZONE_STEP_CENTS[carrier]
                    + (module.WEIGHT_BREAKS_G[seq - 1] // 1000)
                    * module.WEIGHT_STEP_CENTS[service])
        priced = (int(row["base_cents"])
                  + zone * int(row["zone_step_cents"])
                  + int(row["break_kg"]) * int(row["weight_step_cents"]))
        assert priced == expected, (version, carrier, service, seq, zone)


def test_every_version_1_card_carries_the_generator_constants():
    module = carriers_module()
    for row in rows(REFERENCE / "carrier_rate_cards.csv"):
        if row["card_version"] in REPRICED:
            continue
        service = row["service_level"]
        assert int(row["base_cents"]) == module.RATE_BASE_CENTS[service]
        assert int(row["weight_step_cents"]) == module.WEIGHT_STEP_CENTS[service]
        assert int(row["zone_step_cents"]) == module.ZONE_STEP_CENTS[row["carrier_code"]]


def test_the_weight_breaks_are_the_generator_ladder_and_ascend():
    """`sc_rate_card_intake.check_break_order`: every card's breaks ascend from
    one with no gaps, and `sc_shipping_cost_daily` picks the first break at or
    above the billable weight off them."""
    module = carriers_module()
    by_card: dict[tuple, list] = {}
    for row in rows(REFERENCE / "carrier_rate_cards.csv"):
        key = (row["card_version"], row["carrier_code"], row["service_level"])
        by_card.setdefault(key, []).append(row)
    assert len(by_card) == 16, "eight card versions, two service levels each"
    for key, card in by_card.items():
        seqs = sorted(int(row["break_seq"]) for row in card)
        assert seqs == [1, 2, 3, 4, 5], key
        bounds = [int(row["upper_bound_g"]) for row in
                  sorted(card, key=lambda row: int(row["break_seq"]))]
        assert bounds == list(module.WEIGHT_BREAKS_G), key
        for row in card:
            assert int(row["break_kg"]) * 1000 == int(row["upper_bound_g"])


def test_the_repricing_card_steps_two_carriers_and_leaves_the_third():
    """`docs/rate-policy.md` RATE-1. Brightline reissues at the fiscal year and
    Pallas at the calendar year; Merriweather does not reissue in FY2026 at
    all, so one carrier's cost is flat across the boundary and two are not."""
    cards = rows(REFERENCE / "carrier_rate_cards.csv")
    module = carriers_module()

    stepped = {row["carrier_code"] for row in cards
               if row["card_version"] in REPRICED}
    assert stepped == {"BRFR", "PLPC"}
    for row in cards:
        if row["card_version"] not in REPRICED:
            continue
        assert int(row["base_cents"]) > module.RATE_BASE_CENTS[row["service_level"]]
        assert int(row["zone_step_cents"]) > module.ZONE_STEP_CENTS[row["carrier_code"]]
        assert (int(row["weight_step_cents"])
                > module.WEIGHT_STEP_CENTS[row["service_level"]])
    assert not any(row["carrier_code"] == "MWLG" and row["effective_from"] >= "2026-01-01"
                   for row in cards)


def test_the_card_versions_are_the_ones_the_policy_names():
    """RATE-1 names eight versions and their windows; RATE-2 makes the end
    exclusive. The table is corrected to the policy, so the two agree."""
    windows = {}
    for row in rows(REFERENCE / "carrier_rate_cards.csv"):
        key = (row["card_version"], row["carrier_code"])
        span = (row["effective_from"], row["effective_to"])
        assert windows.setdefault(key, span) == span, key
    assert windows == {
        ("BL-2024A", "BRFR"): ("2024-02-04", "2025-02-01"),
        ("BL-2025A", "BRFR"): ("2025-02-01", "2026-02-01"),
        ("BL-2026A", "BRFR"): ("2026-02-01", ""),
        ("PP-24Q1", "PLPC"): ("2024-02-04", "2025-01-01"),
        ("PP-25Q1", "PLPC"): ("2025-01-01", "2026-01-01"),
        ("PP-26Q1", "PLPC"): ("2026-01-01", ""),
        ("MW-2024", "MWLG"): ("2024-02-04", "2025-07-01"),
        ("MW-2025H2", "MWLG"): ("2025-07-01", ""),
    }


def test_the_fuel_series_covers_every_day_of_the_range_inside_the_band():
    """rate-policy.md: a weekly percentage of the base charge, 9% to 19%.
    `sc_shipping_cost_daily` matches one week per ship date, so the weeks have
    to be contiguous or a package rates with no surcharge or with two."""
    series = rows(REFERENCE / "carrier_fuel_surcharges.csv")
    assert {row["carrier_code"] for row in series} == {"BRFR", "PLPC", "MWLG"}
    for row in series:
        assert 900 <= int(row["fuel_pct_bps"]) <= 1900

    weeks = sorted({dt.date.fromisoformat(row["week_start"]) for row in series})
    assert weeks[0] <= RANGE[0] and weeks[-1] + dt.timedelta(days=6) >= RANGE[1]
    assert all(late - early == dt.timedelta(days=7)
               for early, late in zip(weeks, weeks[1:]))
    assert len(series) == len(weeks) * 3


# --- S6c: the finance workbook ---------------------------------------------

@pytest.fixture(scope="module")
def workbook() -> list[list[str]]:
    with WORKBOOK.open(encoding="utf-8", newline="") as handle:
        return list(csv.reader(handle))


HEADER = ["Carrier Code", "Lane", "Fiscal Week", "Ship Date", "freight_cost",
          "Fuel Surcharge", "Accessorials", "Unnamed: 7", "Notes"]


def body(sheet: list[list[str]]) -> list[list[str]]:
    """Every line under the title block that is not a repeated header."""
    return [row for row in sheet[3:] if row != HEADER]


def test_the_workbook_is_the_size_the_chapter_states(workbook):
    """1,180 data rows, 12 subtotals and a total: 1,193 lines, plus the title
    block, the real header and the header the second export brought with it."""
    assert len(workbook) == 1197
    lines = body(workbook)
    assert len(lines) == 1193
    data = [row for row in lines if row[3]]
    assert len(data) == 1180


def test_feature_1_the_title_block_hides_the_real_header(workbook):
    assert workbook[0][0].startswith("Logistics cost")
    assert not any(workbook[0][1:])
    assert not any(workbook[1])
    assert workbook[2] == HEADER


def test_feature_2_the_header_repeats_at_row_412(workbook):
    repeats = [n for n, row in enumerate(workbook, start=1)
               if row == HEADER and n != 3]
    assert repeats == [412]


def test_feature_3_forty_eight_rows_carry_a_trailing_space(workbook):
    spaced = [row for row in body(workbook) if row[0] != row[0].rstrip()]
    assert len(spaced) == 48
    assert {row[0] for row in spaced} == {"BRFR "}


def test_feature_4_twelve_subtotals_and_one_total(workbook):
    lines = body(workbook)
    subtotals = [row for row in lines if row[1] == "Subtotal"]
    totals = [row for row in lines if row[0] == "Total"]
    assert len(subtotals) == 12
    assert len(totals) == 1
    assert len(subtotals) + len(totals) == 13
    assert lines[-1][0] == "Total", "the total sits at the foot"


def test_feature_5_freight_cost_changes_meaning_at_the_fiscal_year(workbook):
    """Gross of the fuel surcharge before FY2026 and net of it after, which is
    visible as the surcharge column arriving with the new fiscal year."""
    before = [row for row in body(workbook) if row[2].startswith("FY2025")]
    after = [row for row in body(workbook) if row[2].startswith("FY2026")]
    assert before and after
    assert all(row[5] == "" for row in before)
    assert all(row[5] != "" for row in after)


def test_feature_6_five_hundred_and_seventeen_locale_formatted_lines(workbook):
    locale = [row for row in body(workbook) if re.fullmatch(r"[\d.]+,\d\d", row[4])]
    assert len(locale) == 517
    assert all(row[2].startswith("FY2025") for row in locale)
    plain = [row for row in body(workbook) if re.fullmatch(r"\d+\.\d\d", row[4])]
    assert len(plain) == 1193 - 517


def test_feature_7_two_date_formats_one_per_block(workbook):
    dated = [row for row in body(workbook) if row[3]]
    day_first = [row for row in dated if re.fullmatch(r"\d\d/\d\d/\d{4}", row[3])]
    iso = [row for row in dated if re.fullmatch(r"\d{4}-\d\d-\d\d", row[3])]
    assert len(day_first) + len(iso) == len(dated)
    assert all(row[2].startswith("FY2025") for row in day_first)
    assert all(row[2].startswith("FY2026") for row in iso)
    assert day_first and iso


def test_feature_8_the_unnamed_column_and_the_notes(workbook):
    lines = body(workbook)
    assert all(row[7] == "" for row in lines)
    assert sum(1 for row in lines if "\n" in row[8]) == 1
    assert sum(1 for row in lines if "," in row[8]) > 100


def test_the_workbook_ships_as_source_and_not_under_landing():
    """`landing/` is generated and diff-excluded. The workbook is authored, so
    it ships under `fixtures/` and the loader copies it into the landing tree
    `plat_workbook_inbox` globs."""
    assert WORKBOOK.exists()
    assert not (WORKSPACE / "landing").exists()


# --- the loader ------------------------------------------------------------

def _loader():
    sys.path.insert(0, str(repo_root() / "tools"))
    import gen_copperline_load_fixtures

    return gen_copperline_load_fixtures


def test_the_loader_round_trips_every_fixture_with_its_declared_types(tmp_path):
    loader = _loader()
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    try:
        landed = loader.load(con, FIXTURES)
        assert landed["raw.market_config"] == 9
        assert landed["raw.entities"] == 5
        assert landed["raw.pay_processor_windows"] == 2
        assert landed["raw.gift_card_jurisdictions"] == 66
        assert landed["raw.carrier_rate_cards"] == 80
        assert landed["raw.carrier_fuel_surcharges"] == 372

        for table, columns in loader.SPECS.values():
            if not columns or table not in landed:
                continue
            found = {name: kind for name, kind, *_ in
                     con.execute(f"DESCRIBE {table}").fetchall()}
            assert found == columns, table

        # The trap rows survive the trip, typed.
        assert con.execute(
            "SELECT billing_currency, entity_code, launched_on FROM raw.market_config "
            "WHERE market_code = 'PL'").fetchone() == ("EUR", "CL-IE", dt.date(2026, 2, 1))
        assert con.execute(
            "SELECT authoritative_to FROM raw.pay_processor_windows "
            "WHERE processor = 'meridian'").fetchone() == (None,)
    finally:
        con.close()


def test_the_loaded_cards_leave_exactly_one_version_in_force_a_day(tmp_path):
    """`sc_shipping_cost_daily.check_card_in_force` stops the run when a
    carrier has two cards in force. Over the whole range, none ever does."""
    loader = _loader()
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    try:
        loader.load(con, FIXTURES)
        overlaps = con.execute("""
            WITH d AS (SELECT unnest(generate_series(?::DATE, ?::DATE,
                                                     INTERVAL 1 DAY))::DATE AS ds)
            SELECT d.ds, c.carrier_code, count(DISTINCT c.card_version) AS versions
            FROM d
            JOIN raw.carrier_rate_cards c
              ON c.effective_from <= d.ds
             AND (c.effective_to IS NULL OR c.effective_to > d.ds)
            GROUP BY 1, 2
            HAVING count(DISTINCT c.card_version) <> 1
               OR count(*) <> 10
        """, [RANGE[0], RANGE[1]]).fetchall()
        assert not overlaps
        gaps = con.execute("""
            WITH d AS (SELECT unnest(generate_series(?::DATE, ?::DATE,
                                                     INTERVAL 1 DAY))::DATE AS ds)
            SELECT d.ds FROM d
            LEFT JOIN raw.carrier_rate_cards c
              ON c.effective_from <= d.ds
             AND (c.effective_to IS NULL OR c.effective_to > d.ds)
            GROUP BY 1 HAVING count(DISTINCT c.carrier_code) <> 3
        """, [RANGE[0], RANGE[1]]).fetchall()
        assert not gaps
    finally:
        con.close()


def test_the_loader_lands_the_workbook_where_the_inbox_globs_for_it(tmp_path):
    loader = _loader()
    written = loader.land_workbook(FIXTURES, tmp_path)
    assert written == tmp_path / "finance" / "logistics_cost_workbook_fy26q1.csv"
    assert written.read_bytes() == WORKBOOK.read_bytes()


def test_the_bake_hashes_the_fixtures_as_well_as_the_generator():
    """`world_sha` folds `workspace/fixtures/` in already, because it sits
    inside the shipped tree — so the trial cache keys on it. The bake does not
    see it that way, so the image build carries its own hash of the tree."""
    from de_bench import modal_app

    assert (repo_root() / modal_app.COPPERLINE_FIXTURES) == FIXTURES
    first = modal_app._copperline_fixtures_sha()
    assert first and first == modal_app._copperline_fixtures_sha()

    # The bake runs the loader against the warehouse it just built, and the
    # fixtures hash rides in the command so a fixture edit rebuilds the layer.
    source = (repo_root() / "src" / "de_bench" / "modal_app.py").read_text()
    assert "load_fixtures.py" in source
    assert "_copperline_fixtures_sha()" in source


# --- extract convention 7 --------------------------------------------------

#: The tables and files the generator may not write, name in a write, or ship.
PROTECTED_TABLES = ("market_config", "carrier_rate_cards", "carrier_fuel_surcharges",
                    "gift_card_jurisdictions", "pay_processor_windows",
                    "entities", "finance_ledger")
PROTECTED_FILES = ("entities.csv", "markets.csv", "market_config.csv",
                   "carrier_rate_cards.csv", "carrier_fuel_surcharges.csv",
                   "gift_card_jurisdictions.csv", "pay_processor_windows.csv",
                   "logistics_cost_workbook", "ledger_monthly")

WRITE = re.compile(
    r"(CREATE\s+(OR\s+REPLACE\s+)?TABLE|INSERT\s+INTO|UPDATE|DELETE\s+FROM|COPY)"
    r"\s+raw\.(" + "|".join(PROTECTED_TABLES) + r")\b", re.IGNORECASE)


def test_the_generator_writes_none_of_the_hand_authored_tables():
    """Extract convention 7. The generator may explain in prose why a table is
    not its business — several modules do — and it may not write one."""
    offenders = [f"{path.relative_to(GEN)}: {match.group(0)}"
                 for path in sorted(GEN.rglob("*.py"))
                 for match in WRITE.finditer(path.read_text())]
    assert not offenders, offenders


def test_the_generator_never_names_a_hand_authored_file():
    offenders = [f"{path.relative_to(GEN)}: {name}"
                 for path in sorted(GEN.rglob("*.py"))
                 for name in PROTECTED_FILES
                 if name in path.read_text()]
    assert not offenders, offenders


def test_the_loader_lives_outside_the_generator_package():
    assert LOADER.exists()
    assert GEN not in LOADER.parents
    assert "gen_copperline_load_fixtures" not in "".join(
        path.read_text() for path in GEN.rglob("*.py"))
