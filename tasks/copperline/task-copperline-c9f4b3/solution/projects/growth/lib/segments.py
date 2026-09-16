"""Audience segments: what they are, and what each destination wants.

`marts.audience_segments` is one row per customer per segment per day. Two
reverse-ETL DAGs push it out and one DAG writes it to files. What differs
between them is not the rows, it is the column names each destination insists
on and the identifier each one matches on, so the destinations live in
`projects/growth/config/segments.yml` and this module reads it.

`contracts/audience-sync.md` binds the delivery. Two of its clauses are the
reason this module has the shape it does:

- **AS-1** an empty segment is a failure, not a delivery. Pushing zero rows
  clears the audience at the platform and stops the campaign, so
  `rows_by_segment` is checked before anything is sent.
- **AS-3** a customer without current marketing consent is not in a segment
  file, and neither is one with a deletion request in flight. Consent is a
  column on the mart. The deletion flag is not, and never was: it rides on
  `marts.consent_daily`, which is where `cus_consent_sync_daily` puts it and
  where `projects/customer/lib/consent.py` says it belongs. `SELECT_SQL`
  joins it here rather than in each caller, so that no caller can forget it.

No unhashed contact detail leaves the warehouse on this path. The mart holds
`contact_hash` and nothing else that identifies a person.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from include.lib import warehouse, workspace_root

__all__ = ["CONFIG_PATH", "CONSENT_CHANNEL", "destinations", "rows_by_segment", "segment_rows",
           "empty_segments", "latest_segment_day", "push", "export_dir",
           "stage", "delivered_counts", "destinations_for"]

#: The destination map. Read at run time, never at parse.
CONFIG_PATH = "projects/growth/config/segments.yml"

#: The channel the audience files go out on. `marts.consent_daily` is one row
#: per account per channel, so the join is scoped to the one this path uses;
#: without it an account with a second channel comes back twice.
CONSENT_CHANNEL = "email"

#: The one read of the mart every destination shares. Consent and deletion are
#: filtered here so that no caller can forget them.
#:
#: The deletion flag is not a column on the audience mart and asking for one is
#: what broke this read. `marts.consent_daily` holds it, one row per account per
#: channel per day, and the join is what brings it in. An account with no
#: consent row for the day is not in the file: not being asked is not consent.
SELECT_SQL = """
    SELECT a.ds, a.segment_id, a.customer_id, a.contact_hash, a.consent_state
    FROM {table} a
    JOIN {consent} c
      ON c.customer_id = a.customer_id
     AND c.ds = a.ds
     AND c.channel = '{channel}'
    WHERE a.ds = DATE '{ds}'
      AND a.consent_state = 'granted'
      AND c.deletion_pending = false
    ORDER BY a.segment_id, a.customer_id
"""


def destinations() -> dict[str, dict[str, Any]]:
    """Every destination in the segment config, by name.

    Each entry carries the platform's own name for the identifier column, the
    segments it takes, and the connection it goes out on.
    """
    import yaml

    document = yaml.safe_load(
        (workspace_root() / CONFIG_PATH).read_text(encoding="utf-8")
    ) or {}
    return dict(document.get("destinations") or {})


def segment_rows(ds: str | dt.date, segments: list[str] | None = None) -> list[dict]:
    """The day's segment membership, consent applied, as dicts.

    `segments` narrows it to one destination's list; without it every segment
    comes back.
    """
    sql = SELECT_SQL.format(table=warehouse.qualify("marts.audience_segments"),
                            consent=warehouse.qualify("marts.consent_daily"),
                            channel=CONSENT_CHANNEL,
                            ds=_as_date(ds))
    with warehouse.connect(read_only=True) as con:
        result = con.execute(sql)
        names = [description[0] for description in result.description]
        rows = [dict(zip(names, row)) for row in result.fetchall()]
    if segments is None:
        return rows
    wanted = set(segments)
    return [row for row in rows if row["segment_id"] in wanted]


def rows_by_segment(rows: list[dict]) -> dict[str, int]:
    """How many rows each segment has in `rows`."""
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["segment_id"]] = counts.get(row["segment_id"], 0) + 1
    return counts


def empty_segments(rows: list[dict], expected: list[str]) -> list[str]:
    """The segments in `expected` that `rows` has nothing for.

    AS-1 makes this a failure rather than a delivery: nothing downstream can
    tell an empty audience from an audience that was never built.
    """
    present = rows_by_segment(rows)
    return [segment for segment in expected if not present.get(segment)]


def latest_segment_day() -> str:
    """The newest day `marts.audience_segments` holds.

    An asset-triggered run has no interval to read, so the day being pushed
    is the day that has just been built. Reading the mart rather than a clock
    means a run that fires late pushes the day it was fired for.
    """
    with warehouse.connect(read_only=True) as con:
        row = con.execute(
            f"SELECT max(ds) FROM {warehouse.qualify('marts.audience_segments')}"
        ).fetchone()
    if not row or row[0] is None:
        raise RuntimeError("marts.audience_segments is empty, so there is "
                           "nothing to publish")
    return _as_date(row[0]).isoformat()


def push(destination: str, spec: dict[str, Any], rows: list[dict]) -> int:
    """Upsert the membership at a destination. Returns the rows sent.

    The write is keyed on `(segment_id, customer_id)`, so a run that fires
    twice sends the same membership twice and the destination holds one copy.
    There is no delta path, because membership changes every day and there is
    no history to diff against.

    The destination's own column names come from the config, which is why
    two platforms that want the same rows under different headers need no
    code between them.
    """
    identifier = spec.get("identifier", "contact_hash")
    payload = [{spec["segment_field"]: row["segment_id"],
                spec["identifier_field"]: row[identifier]} for row in rows]
    base = _base_url(spec.get("conn_id", "copperline_api"))
    _post(f"{base}/{spec['endpoint']}",
          {"destination": destination, "rows": payload})
    return len(payload)


def _base_url(conn_id: str) -> str:
    """The destination's host, from an Airflow connection. Read when the task
    runs; nothing here touches a connection at parse time."""
    from airflow.sdk import BaseHook

    conn = BaseHook.get_connection(conn_id)
    port = f":{conn.port}" if conn.port else ""
    return f"{conn.schema or 'http'}://{conn.host}{port}".rstrip("/")


def _post(url: str, body: dict[str, Any]) -> None:
    """Send one upsert. The stub answers on loopback; nothing leaves the box."""
    import json
    import urllib.request

    request = urllib.request.Request(
        url, data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(request) as response:  # noqa: S310 - loopback
        response.read()


def export_dir(ds: str | dt.date) -> Path:
    """`exports/audiences/<ds>/`, created if it is not there."""
    path = workspace_root() / "exports" / "audiences" / _as_date(ds).isoformat()
    path.mkdir(parents=True, exist_ok=True)
    return path


def stage(destination: str, spec: dict[str, Any], ds: str | dt.date,
          rows: list[dict] | None = None) -> list[str]:
    """Write one destination's files where the publish script looks.

    One file per segment, under `exports/audiences/<ds>/<destination>/`, with
    the destination's own column names on the header. The write is atomic and
    a re-run replaces the file rather than appending to it.
    """
    import csv

    rows = segment_rows(ds, spec["segments"]) if rows is None else rows
    identifier = spec.get("identifier", "contact_hash")
    header = [spec["segment_field"], spec["identifier_field"]]
    root = export_dir(ds) / destination
    root.mkdir(parents=True, exist_ok=True)
    written = []
    for segment in spec["segments"]:
        path = root / f"{segment}.csv"
        partial = path.with_suffix(".csv.partial")
        with partial.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(header)
            writer.writerows([row["segment_id"], row[identifier]]
                             for row in rows if row["segment_id"] == segment)
        partial.replace(path)
        written.append(str(path))
    return written


def delivered_counts(destinations: tuple[str, ...] | list[str],
                     ds: str | dt.date) -> dict[str, int]:
    """How many rows each destination actually holds for the day.

    Read back from the platform rather than taken from the publish script's
    exit status, which is what `contracts/audience-sync.md` §AS-1 asks for:
    the script reports success on nothing.
    """
    wanted = destinations_for(destinations)
    counts = {}
    for name, spec in wanted.items():
        base = _base_url(spec.get("conn_id", "copperline_api"))
        counts[name] = _get_count(f"{base}/{spec['endpoint']}/count",
                                  {"destination": name,
                                   "as_of": _as_date(ds).isoformat()})
    return counts


def destinations_for(names: tuple[str, ...] | list[str]) -> dict[str, dict]:
    """The config entries for a list of destination names."""
    everything = destinations()
    return {name: everything[name] for name in names}


def _get_count(url: str, params: dict[str, Any]) -> int:
    import json
    import urllib.parse
    import urllib.request

    query = urllib.parse.urlencode(params)
    with urllib.request.urlopen(f"{url}?{query}") as response:  # noqa: S310
        payload = json.loads(response.read().decode("utf-8"))
    return int(payload.get("rows") or 0)


def _as_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
