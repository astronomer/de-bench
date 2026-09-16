"""GRO-418 — does a replayed morning of the ad-spend intake come out as that
morning?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so `include.lib` resolves the warehouse the way every DAG does.

**Why this drives the runs itself.** The fault only shows when a morning is run
after a later morning has already run — that is what a replay is. The check
stages have no way to order two dates that way, because they run a date set in
ascending order, so the sequence is built here: one forward morning, then two
earlier ones, in that order, with the run record cleared first so the sequence
is the whole history the DAG has.

**No authored numbers.** Every expected result is computed from
`raw.ads_spend_daily` and `raw.fx_rates` in this session, before anything runs,
with the two queries `projects/growth/lib/ads.py` states as the definition of a
morning's restatements and a morning's missing rates. A patch that rewrites
those definitions is still graded against the definitions — which is the other
reason the ticket puts that module out of scope.

**Three mornings, on purpose.**

    2026-05-29  the forward morning: the newest morning run here, so its own
                answer is also the newest answer and the fault cannot show on
                it. This is the "nothing moved" arm.
    2026-04-21  a replay, five weeks earlier.
    2026-03-24  a replay, four weeks earlier again, run after 21 April, so a
                fix that reaches for the previous run's morning fails here.

The ticket names 11 to 15 May and none of the three. All three sit inside the
window the landing tree keeps and outside the world's reserved windows.

**What each morning is measured on.**

1. The run record for that morning's own interval holds that morning's
   restatement list and that morning's missing rates.
2. `raw.ads_spend_daily` holds that morning's three deliveries. Each replayed
   morning's rows are removed first, so the replay has to land them again —
   which is the half of the ticket the record cannot answer.
"""

from __future__ import annotations

import json
import os
import subprocess

import pytest

from include.lib import data_dir, warehouse, workspace_root

DAG_ID = "gro_marketing_spend_intake"
ROOT = str(workspace_root())
MANIFEST = data_dir() / "_manifest" / f"{DAG_ID}.json"

SPEND = warehouse.qualify("raw.ads_spend_daily")
RATES = warehouse.qualify("raw.fx_rates")

#: The morning the DAG runs first and never runs again. See the docstring.
FORWARD = "2026-05-29"
#: The mornings replayed after it, in the order they are replayed.
REPLAYS = ("2026-04-21", "2026-03-24")

#: The platforms that drop a spend file. Written out here rather than imported
#: so that a patch cannot move the expectation by editing the list.
PLATFORMS = ("beacon", "solstice", "tessera")


# --- the definitions, as `projects/growth/lib/ads.py` states them ------------

RESTATED_SQL = f"""
SELECT r.platform, r.report_date, r.campaign_id,
       f.spend_cents AS first_spend_cents,
       r.spend_cents AS restated_spend_cents
FROM {SPEND} r
JOIN {SPEND} f
  ON f.platform = r.platform
 AND f.report_date = r.report_date
 AND f.campaign_id = r.campaign_id
 AND f.restated_at IS NULL
WHERE r.restated_at IS NOT NULL AND r.loaded_at::DATE = DATE '{{day}}'
ORDER BY r.platform, r.report_date, r.campaign_id
"""

MISSING_RATES_SQL = f"""
SELECT s.currency_code, s.report_date, count(*) AS deliveries
FROM {SPEND} s
LEFT JOIN {RATES} r
  ON r.currency_code = s.currency_code
 AND r.rate_date = s.report_date
WHERE s.loaded_at::DATE = DATE '{{day}}' AND r.currency_code IS NULL
GROUP BY 1, 2 ORDER BY 1, 2
"""

DELIVERIES_SQL = f"""
SELECT platform, count(*), sum(spend_cents), sum(impressions), sum(clicks)
FROM {SPEND}
WHERE loaded_at::DATE = DATE '{{day}}'
GROUP BY platform ORDER BY platform
"""

RESTATED_KEYS = ("platform", "report_date", "campaign_id",
                 "first_spend_cents", "restated_spend_cents")
RATE_KEYS = ("currency_code", "report_date", "deliveries")


def read(sql: str) -> list[tuple]:
    with warehouse.connect(read_only=True) as con:
        return [tuple(str(cell) for cell in row)
                for row in con.execute(sql).fetchall()]


def from_query(sql: str, day: str, keys: tuple[str, ...]) -> list[tuple]:
    """A query result as `key=value` pairs, sorted, so neither side of the
    comparison is trusted about column or row order."""
    return sorted(tuple(f"{k}={v}" for k, v in sorted(zip(keys, row)))
                  for row in read(sql.format(day=day)))


def from_record(rows: list) -> list[tuple]:
    """A recorded list of dicts in the same shape."""
    return sorted(tuple(f"{k}={v}" for k, v in sorted(row.items()))
                  for row in rows)


def deliveries_of(day: str) -> list[tuple]:
    return read(DELIVERIES_SQL.format(day=day))


def drop_deliveries(day: str) -> None:
    """Take one morning's landed rows out, so a replay has to land them again."""
    with warehouse.connect() as con:
        con.execute(f"DELETE FROM {SPEND} WHERE loaded_at::DATE = DATE '{day}'")


# --- the run record ---------------------------------------------------------

def document() -> dict:
    if not MANIFEST.exists():
        pytest.fail(f"no run record at {MANIFEST.relative_to(workspace_root())}")
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def recorded(task_id: str, day: str) -> list[tuple]:
    """What the DAG recorded for `task_id` on the interval that morning owns.

    The manifest files an entry under the run's `data_interval_start`, which
    for this DAG's schedule is the morning itself. Matched on the date rather
    than on the whole timestamp, so the check does not turn on how an offset is
    spelled.
    """
    entries = (document().get("entries") or {}).get(task_id) or {}
    hits = [entry for interval, entry in sorted(entries.items())
            if str(interval).startswith(day)]
    if not hits:
        pytest.fail(f"the run record holds no {task_id} entry for {day}; it "
                    f"holds {sorted(entries) or 'nothing for that task'}")
    entry = hits[-1]
    if entry.get("state") != "ok":
        pytest.fail(f"{task_id} for {day} is recorded {entry.get('state')}: "
                    f"{entry.get('reason')}")
    return from_record(entry.get("value") or [])


# --- driving the DAG --------------------------------------------------------

def run_morning(day: str) -> None:
    proc = subprocess.run(["airflow", "dags", "test", DAG_ID, day], cwd=ROOT,
                          capture_output=True, text=True, timeout=900)
    if proc.returncode != 0:
        blob = (proc.stdout or "") + (proc.stderr or "")
        tail = [line.strip() for line in blob.splitlines()
                if "Error" in line or "Exception" in line][-3:]
        pytest.fail(f"the run for {day} did not finish: "
                    + " | ".join(tail or [blob[-400:]]))


@pytest.fixture(scope="module")
def expected() -> dict:
    """One forward morning, then two replays, with every expected answer taken
    from the warehouse before anything runs."""
    answers = {
        day: {
            "restatements": from_query(RESTATED_SQL, day, RESTATED_KEYS),
            "fx_coverage": from_query(MISSING_RATES_SQL, day, RATE_KEYS),
            "deliveries": deliveries_of(day),
        }
        for day in (FORWARD, *REPLAYS)
    }
    for day, answer in answers.items():
        assert answer["deliveries"], f"the world lands no deliveries on {day}"
        assert answer["restatements"], f"the world restates nothing on {day}"

    MANIFEST.unlink(missing_ok=True)
    subprocess.run(["airflow", "dags", "reserialize"], cwd=ROOT,
                   capture_output=True, text=True, check=False, timeout=600)

    run_morning(FORWARD)
    for day in REPLAYS:
        drop_deliveries(day)
        run_morning(day)
    return answers


def test_forward_morning_records_its_own_answer(expected):
    """The morning-after-morning path. Nothing about the fix may move it."""
    assert recorded("restatements", FORWARD) == expected[FORWARD]["restatements"]
    assert recorded("fx_coverage", FORWARD) == expected[FORWARD]["fx_coverage"]


def test_forward_morning_deliveries_are_untouched(expected):
    """A forward run lands the morning it is given, and the two replays behind
    it left that morning's rows alone."""
    assert deliveries_of(FORWARD) == expected[FORWARD]["deliveries"]


@pytest.mark.parametrize("day", REPLAYS)
def test_replayed_morning_records_its_own_restatements(expected, day):
    """The half the campaign team reads: the morning's own restatement list."""
    assert recorded("restatements", day) == expected[day]["restatements"]


@pytest.mark.parametrize("day", REPLAYS)
def test_replayed_morning_records_its_own_missing_rates(expected, day):
    """The half GRO-418 was replayed for: the morning's own currency gaps."""
    assert recorded("fx_coverage", day) == expected[day]["fx_coverage"]


@pytest.mark.parametrize("day", REPLAYS)
def test_replayed_morning_lands_its_own_deliveries(expected, day):
    """The half the record cannot answer: the morning's rows are back."""
    landed = deliveries_of(day)
    assert [row[0] for row in landed] == sorted(PLATFORMS)
    assert landed == expected[day]["deliveries"]


def test_every_run_files_under_its_own_morning(expected):
    """A run files its entries under its own interval, forward or replayed.

    The guard against getting the numbers right by filing them somewhere else:
    the delivery report looks a morning up by its date.
    """
    entries = document().get("entries") or {}
    wanted = sorted([FORWARD, *REPLAYS])
    for task_id in ("restatements", "fx_coverage"):
        filed = sorted({str(i)[:10] for i in entries.get(task_id) or {}})
        assert filed == wanted, f"{task_id} is filed under {filed}"


def test_the_warehouse_read_is_the_scored_tree():
    """A guard on the verifier itself."""
    assert os.path.exists(str(warehouse.warehouse_path()))
