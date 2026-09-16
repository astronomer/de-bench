"""MER-612 — is `marts.dim_product` the feed's history, or the newest row per SKU?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it reads the patched statement through `projects.commerce.lib.
sql` and opens the warehouse through `include.lib.warehouse`, exactly as the
DAG's step does.

**Why a verifier rather than a DAG run.** `AGENTS.md`, "The warehouse": the
world ships the landed half only, so `marts.*` is not on disk. Three of
`product_snapshot_daily`'s four steps shell out to the analytics venv against a
warehouse a nightly has already filled — two dbt snapshots and a dbt run — so
`airflow dags test` on this DAG stops there whatever the agent writes. The
ticket says so and puts them out of scope. `run_build` below makes the `marts`
schema and drives the one step the ticket does name, straight off the statement
file the ticket names.

**No authored numbers, no authored SKUs and no authored dates.** The expected
history is computed from `raw.pim_product_versions` in the same session, by the
contract the ticket states. Every SKU this grades is found by asking the feed
which SKUs carry the thing under test — the arrivals that overtook each other,
the dates that carry two changes, the deletes with nothing after them, the keys
that came back — and the ticket names none of them. The as-of probe is every
change date in the feed and the day after each one.

**The first test is this file's own guard.** Each population has to exist and
each has to separate a correct build from the two readings that are wrong about
it, or the tests below grade nothing.
"""

from __future__ import annotations

import duckdb

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse the same way every DAG in the world does —
# `DUCKDB_PATH` against the root that holds `include/`, whatever the working
# directory happens to be.
from include.lib import warehouse

DB = str(warehouse.warehouse_path())

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` and `CONVENTIONS.md` rule 4 both give: the frozen
# `nwv` copy sits first on the search path and carries twelve tables under
# Copperline's own names.
FEED = warehouse.qualify("raw.pim_product_versions")
MARTS_SCHEMA = warehouse.qualify("marts")
MART = warehouse.qualify("marts.dim_product")

#: The statement the ticket asks for, by the name the ticket gives it.
STATEMENT = "product_history_build"

#: The nine columns of the contract, in the order the ticket lists them.
COLUMNS = (
    "sku",
    "product_name",
    "category_id",
    "brand",
    "supplier_id",
    "list_price_cents",
    "status",
    "valid_from",
    "valid_to",
)

#: The contract's own tie-break: pim_ui, then supplier_feed, then bulk_load.
SOURCE_RANK = (
    "CASE source WHEN 'pim_ui' THEN 1 WHEN 'supplier_feed' THEN 2 "
    "WHEN 'bulk_load' THEN 3 ELSE 4 END"
)

#: The history the contract describes, from the feed. Ordered on `updated_at`;
#: one row per SKU per date, the strongest source first and the later stamp
#: after it; a `delete` closes the span in force and contributes no row of its
#: own, which is why `operation` survives the window and is filtered after it.
TRUTH = f"""
SELECT sku, product_name, category_id, brand, supplier_id,
       CAST(list_price_cents AS BIGINT) AS list_price_cents, status,
       valid_from, valid_to
FROM (
    SELECT sku, product_name, category_id, brand, supplier_id,
           list_price_cents, status, operation,
           change_date AS valid_from,
           lead(change_date) OVER (PARTITION BY sku ORDER BY change_date)
               AS valid_to
    FROM (
        SELECT *, CAST(updated_at AS DATE) AS change_date,
               row_number() OVER (
                   PARTITION BY sku, CAST(updated_at AS DATE)
                   ORDER BY {SOURCE_RANK}, updated_at DESC, change_id) AS seq
        FROM {FEED}
    )
    WHERE seq = 1
)
WHERE operation = 'upsert'
"""

#: The built table, read as the contract's nine columns. Types are coerced
#: rather than policed: a build that spells `list_price_cents` as an integer of
#: another width, or the dates as timestamps at midnight, is not wrong about the
#: history. `test_an_as_of_read_returns_the_record_in_force` reads the columns
#: as they were built, which is where a date that is secretly a mid-afternoon
#: timestamp stops being harmless.
BUILT = f"""
SELECT CAST(sku AS VARCHAR)              AS sku,
       CAST(product_name AS VARCHAR)     AS product_name,
       CAST(category_id AS VARCHAR)      AS category_id,
       CAST(brand AS VARCHAR)            AS brand,
       CAST(supplier_id AS VARCHAR)      AS supplier_id,
       CAST(list_price_cents AS BIGINT)  AS list_price_cents,
       CAST(status AS VARCHAR)           AS status,
       CAST(valid_from AS DATE)          AS valid_from,
       CAST(valid_to AS DATE)            AS valid_to
FROM {MART}
"""

#: Every SKU whose feed carries an arrival that overtook an earlier change: a
#: row that reached us after a row with a later source stamp had already
#: reached us. A build ordered on `received_at` splices these into the wrong
#: place.
OUT_OF_ORDER = f"""
SELECT DISTINCT a.sku FROM {FEED} a
WHERE EXISTS (SELECT 1 FROM {FEED} b
              WHERE b.sku = a.sku
                AND b.updated_at > a.updated_at
                AND b.received_at < a.received_at)
"""

#: Every SKU that carries two changes on one date, which is where the source
#: tie-break has to decide and where a build that skips it opens a span it
#: closes the same day.
SAME_DAY = f"""
SELECT DISTINCT sku FROM (
    SELECT sku FROM {FEED}
    GROUP BY sku, CAST(updated_at AS DATE) HAVING count(*) > 1)
"""

#: Every SKU whose last change is a delete: retired, and entitled to no open
#: row at all.
RETIRED = f"""
SELECT sku FROM {FEED}
GROUP BY sku
HAVING max(updated_at) = max(updated_at) FILTER (WHERE operation = 'delete')
"""

#: Every SKU that comes back: an upsert with a delete before it.
RESURRECTED = f"""
SELECT DISTINCT a.sku FROM {FEED} a
WHERE a.operation = 'upsert'
  AND EXISTS (SELECT 1 FROM {FEED} b
              WHERE b.sku = a.sku AND b.operation = 'delete'
                AND b.updated_at < a.updated_at)
"""

#: The two readings the ticket rules out, as tables, so the guard can show that
#: the graded populations tell them apart from the contract's answer. The first
#: orders on arrival. The second drops the deletes before the span ends are
#: computed, which is the natural way to write it and the one that swallows
#: every retirement and every gap.
BY_ARRIVAL = f"""
SELECT sku, product_name, category_id, brand, supplier_id,
       CAST(list_price_cents AS BIGINT) AS list_price_cents, status,
       CAST(updated_at AS DATE) AS valid_from,
       lead(CAST(updated_at AS DATE)) OVER (PARTITION BY sku ORDER BY received_at)
           AS valid_to
FROM {FEED}
"""

DELETES_DROPPED_FIRST = f"""
SELECT sku, product_name, category_id, brand, supplier_id,
       CAST(list_price_cents AS BIGINT) AS list_price_cents, status,
       change_date AS valid_from,
       lead(change_date) OVER (PARTITION BY sku ORDER BY change_date) AS valid_to
FROM (
    SELECT *, CAST(updated_at AS DATE) AS change_date,
           row_number() OVER (
               PARTITION BY sku, CAST(updated_at AS DATE)
               ORDER BY {SOURCE_RANK}, updated_at DESC, change_id) AS seq
    FROM {FEED} WHERE operation = 'upsert')
WHERE seq = 1
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the build
    under test opens its own, so nothing here may hold the file open."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def run_build() -> None:
    """Run the statement the ticket asks for, the way the DAG's step does.

    The `marts` schema is made first: `AGENTS.md` says the shipped warehouse
    carries the landed half only, and a run of the nightly would have made it
    by now. The statement is executed as it was written — a file that carries
    its own `CREATE SCHEMA` alongside the build runs here too.
    """
    from projects.commerce.lib import sql

    try:
        text = sql.read(STATEMENT)
    except FileNotFoundError as exc:
        raise AssertionError(
            f"FAIL: {exc}. The ticket asks for the build to live in "
            f"projects/commerce/sql/{STATEMENT}.sql"
        ) from None
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MARTS_SCHEMA}")
        try:
            con.execute(text)
        except Exception as exc:  # noqa: BLE001
            raise AssertionError(
                f"FAIL: projects/commerce/sql/{STATEMENT}.sql did not run: {exc}"
            ) from None


_BUILT: list[bool] = []


def ensure_built() -> None:
    """Build once for the whole file, and blame the statement rather than the
    fixture when it cannot be run at all."""
    if not _BUILT:
        run_build()
        _BUILT.append(True)


def spans(scope: str = "") -> list[tuple]:
    return query(f"SELECT * FROM ({BUILT}) {scope} ORDER BY sku, valid_from")


def truth(scope: str = "") -> list[tuple]:
    return query(f"SELECT * FROM ({TRUTH}) {scope} ORDER BY sku, valid_from")


def difference(built: str, expected: str, scope: str = "") -> list[tuple]:
    """Up to six rows one side holds and the other does not, each labelled with
    the side that holds it. `scope` is a WHERE clause both sides take."""
    return query(f"""
        SELECT 'missing' AS side, * FROM (
            SELECT * FROM ({expected}) {scope}
            EXCEPT SELECT * FROM ({built}) {scope})
        UNION ALL
        SELECT 'unwanted', * FROM (
            SELECT * FROM ({built}) {scope}
            EXCEPT SELECT * FROM ({expected}) {scope})
        LIMIT 6
    """)


def in_population(population: str) -> str:
    return f"WHERE sku IN ({population})"


def check_population(name: str, population: str) -> None:
    """The spans of one population, against the history the feed says it has."""
    ensure_built()
    scope = in_population(population)
    covered = query(f"SELECT count(*) FROM ({population})")[0][0]
    assert covered, f"FAIL: {name}: the feed carries no such SKU, so nothing is graded"
    wrong = difference(BUILT, TRUTH, scope)
    assert not wrong, (
        f"FAIL: {name}: {covered} SKU(s) carry it and the history does not match "
        f"the feed. First differences (side, then the contract's columns): {wrong}"
    )


def test_the_feed_still_carries_what_this_grades():
    """This file's own guard, and it runs first and reads no built table.

    Each population has to exist, and the contract's answer has to differ from
    both readings the ticket rules out. A world that stopped shipping late
    arrivals, or same-day pairs, or retirements, fails here loudly rather than
    grading nothing.
    """
    for name, population in (
        ("late arrivals", OUT_OF_ORDER),
        ("same-day pairs", SAME_DAY),
        ("retirements", RETIRED),
        ("resurrections", RESURRECTED),
    ):
        n = query(f"SELECT count(*) FROM ({population})")[0][0]
        assert n, f"FAIL: the feed carries no {name}, so this task grades nothing"

    for reading, name, population, group in (
        (BY_ARRIVAL, "ordering on arrival", OUT_OF_ORDER, "the late arrivals"),
        (BY_ARRIVAL, "ordering on arrival", SAME_DAY, "the same-day pairs"),
        (DELETES_DROPPED_FIRST, "dropping the deletes first", RETIRED, "the retirements"),
        (DELETES_DROPPED_FIRST, "dropping the deletes first", RESURRECTED, "the resurrections"),
    ):
        assert difference(reading, TRUTH, in_population(population)), (
            f"FAIL: {name} already agrees with the contract on {group}, so that "
            "population cannot tell the two apart"
        )


def test_the_table_carries_the_columns_the_contract_names():
    """Nine columns, by name. Extra columns are the build's own business; a
    missing one is a table the close cannot read."""
    ensure_built()
    found = {row[0] for row in query(f"DESCRIBE {MART}")}
    missing = [c for c in COLUMNS if c not in found]
    assert not missing, f"FAIL: {MART} has no {', '.join(missing)}"


def test_the_history_is_the_whole_feed_spanned():
    """Every span of every SKU, against the feed read by the contract."""
    ensure_built()
    wrong = difference(BUILT, TRUTH)
    assert not wrong, (
        f"FAIL: the history and the feed disagree. Built {len(spans())} spans "
        f"against {len(truth())}. First differences: {wrong}"
    )


def test_a_change_that_arrived_late_sits_where_its_own_stamp_puts_it():
    """Contract rule 2. A build ordered on `received_at` puts the overtaken
    change at the end of the history instead of splicing it into the middle,
    and every as-of read from that day on is wrong."""
    check_population("the late arrivals", OUT_OF_ORDER)


def test_two_changes_on_one_date_leave_one_row():
    """Contract rule 3. The stronger source stands, the loser leaves no row,
    and no span opens and closes on the same date."""
    check_population("the same-day pairs", SAME_DAY)
    degenerate = query(
        f"SELECT sku, valid_from FROM ({BUILT}) WHERE valid_to = valid_from LIMIT 5"
    )
    assert not degenerate, (
        f"FAIL: {len(degenerate)}+ span(s) open and close on the same date, so no "
        f"reader can ever match them: {degenerate}"
    )
    doubled = query(f"""
        SELECT sku, valid_from, count(*) FROM ({BUILT})
        GROUP BY sku, valid_from HAVING count(*) > 1 LIMIT 5
    """)
    assert not doubled, f"FAIL: two spans open on one date for the same SKU: {doubled}"


def test_a_retired_sku_has_no_open_row():
    """Contract rule 4. A delete with nothing after it closes the record, and
    an as-of read of any later date has to return nothing for that SKU."""
    check_population("the retirements", RETIRED)
    open_after_delete = query(f"""
        SELECT sku FROM ({BUILT}) WHERE valid_to IS NULL
          AND sku IN ({RETIRED}) LIMIT 5
    """)
    assert not open_after_delete, (
        "FAIL: a SKU whose last change is a delete still carries an open row: "
        f"{[row[0] for row in open_after_delete]}"
    )


def test_a_resurrected_sku_comes_back_after_a_gap():
    """Contract rule 5. The delete closes the span in force on its date, the
    upsert that follows opens a fresh one, and the days between belong to no
    span at all."""
    check_population("the resurrections", RESURRECTED)
    unbroken = query(f"""
        WITH gone AS (
            SELECT sku, CAST(updated_at AS DATE) AS day FROM {FEED}
            WHERE operation = 'delete' AND sku IN ({RESURRECTED}))
        SELECT g.sku, g.day FROM gone g
        WHERE EXISTS (SELECT 1 FROM ({BUILT}) p
                      WHERE p.sku = g.sku AND p.valid_from <= g.day
                        AND (p.valid_to IS NULL OR p.valid_to > g.day))
        LIMIT 5
    """)
    assert not unbroken, (
        "FAIL: a span is still in force on the date the SKU was deleted, so the "
        f"gap before it came back was never recorded: {unbroken}"
    )


def test_an_as_of_read_returns_the_record_in_force():
    """Contract rule 1, read the way `close_price_book` and `enrich_product`
    read it, and against the columns as they were built rather than as this
    file coerces them elsewhere. The probe is every date the feed changed
    something and the day after each one.
    """
    ensure_built()
    probes = query(f"""
        WITH probe AS (
            SELECT DISTINCT sku, CAST(updated_at AS DATE) AS d FROM {FEED}
            UNION
            SELECT DISTINCT sku, CAST(updated_at AS DATE) + INTERVAL 1 DAY FROM {FEED}
        ),
        want AS (
            SELECT p.sku, p.d, t.list_price_cents, t.status, t.category_id
            FROM probe p LEFT JOIN ({TRUTH}) t
              ON t.sku = p.sku AND p.d >= t.valid_from
             AND (t.valid_to IS NULL OR p.d < t.valid_to)
        ),
        got AS (
            SELECT p.sku, p.d,
                   count(b.sku)               AS matched,
                   max(b.list_price_cents)    AS list_price_cents,
                   max(b.status)              AS status,
                   max(CAST(b.category_id AS VARCHAR)) AS category_id
            FROM probe p LEFT JOIN {MART} b
              ON b.sku = p.sku AND p.d >= b.valid_from
             AND (b.valid_to IS NULL OR p.d < b.valid_to)
            GROUP BY p.sku, p.d
        )
        SELECT w.sku, w.d, w.list_price_cents, g.list_price_cents, g.matched
        FROM want w JOIN got g ON g.sku = w.sku AND g.d = w.d
        WHERE g.matched > 1
           OR w.list_price_cents IS DISTINCT FROM CAST(g.list_price_cents AS BIGINT)
           OR w.status IS DISTINCT FROM g.status
           OR w.category_id IS DISTINCT FROM g.category_id
        LIMIT 6
    """)
    assert not probes, (
        "FAIL: an as-of read of the history does not return the record the feed "
        "says was in force (sku, date, wanted price, got price, rows matched): "
        f"{probes}"
    )


def test_a_second_run_leaves_the_table_as_the_first_left_it():
    """Contract rule 6. The nightly runs this every night and a rerun is
    ordinary; a build that appends doubles the catalogue on the second one."""
    ensure_built()
    once = spans()
    run_build()
    assert spans() == once, (
        f"FAIL: the second run changed the table: {len(once)} spans became "
        f"{len(spans())}"
    )
