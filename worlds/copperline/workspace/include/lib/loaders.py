"""The two house load operators: files in, and the fixture API in.

Both land rows in a warehouse table and both take the same `mode` /
`partition_col` pair, which is the only thing that decides whether a second
run of the same interval is safe.

The landing tree they read is dated and hive-style. `landing/<source>/dt=<ds>/`
holds one directory per day for most sources — `landing/oms/dt=2026-06-14/
orders.csv`, `landing/pos/dt=2026-06-14/store_S-0117.csv` — with an hour level
under it for the clickstream and the Meridian feed, and the carrier feeds keyed
by carrier instead of by date. `docs/retention-policy.md` says how long each
source keeps its files.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from airflow.sdk import BaseOperator

from . import landing_dir, warehouse, workspace_root

__all__ = ["CsvToWarehouseOperator", "JsonApiToWarehouseOperator", "PAGE_TOKEN_KEY"]

#: The fixture API's cursor. Present on every page but the last.
PAGE_TOKEN_KEY = "next_page_token"


class CsvToWarehouseOperator(BaseOperator):
    """Load CSV files into a warehouse table.

    mode="append" is the DEFAULT and it is not what the public operators of
    this name do. History tables were this operator's first use and the default
    never moved. An append load of the same file twice puts the rows in twice;
    there is no key check and no dedup.

    mode="replace" scopes the delete to ONE PARTITION and needs `partition_col`
    to know which column carries it. Passing mode="replace" without
    `partition_col` is accepted, logs a line at INFO, and falls back to append
    — the guard exists because a table-wide delete on the shared warehouse
    locked it for nine minutes once.

    So the rerun-safe call is both arguments together:

        CsvToWarehouseOperator(
            task_id="load",
            source="landing/carriers/brfr/invoices.csv",
            table="raw.carrier_invoices",
            mode="replace",
            partition_col="invoice_date",
        )

    Args:
        source: a path or a glob, under the repository root unless it is
            absolute; templated, so `landing/oms/dt={{ ds }}/orders.csv` reads
            the run's own day and `landing/pos/dt={{ ds }}/store_*.csv` reads
            every store's file for it.
        table: `schema.table`, qualified onto the live warehouse before the
            load runs. Created from the CSV header if it is absent.
        mode: "append" (default) or "replace".
        partition_col: the column the replace deletes on. Required for replace.
        partition_value: the partition the replace owns. Templated, and
            `{{ ds }}` by default, which is the run's own day.
        columns: explicit column types, `{"invoice_date": "DATE", ...}`.
            Without it the CSV sniffer guesses, and the guess is not stable
            across DuckDB minors.

    Returns the number of rows loaded.
    """

    template_fields: Sequence[str] = ("source", "table", "partition_value")
    ui_color = "#d9c3a5"

    def __init__(
        self,
        *,
        source: str,
        table: str,
        mode: str = "append",
        partition_col: str | None = None,
        partition_value: str = "{{ ds }}",
        columns: Mapping[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.source = source
        self.table = table
        self.mode = mode
        self.partition_col = partition_col
        self.partition_value = partition_value
        self.columns = dict(columns) if columns else None

    def execute(self, context: Any) -> int:
        relation = _read_csv_sql(self.source, self.columns)
        return _load(self, relation)


class JsonApiToWarehouseOperator(BaseOperator):
    """Load JSON rows into a warehouse table, from the fixture API or from a
    JSONL file in the landing tree.

    THE API PAGES, AND IT DOES NOT SAY HOW FAR. Every listing endpoint returns
    at most fifty rows and a `next_page_token`. The token is present on every
    page except the last, and `total` is always null, so the only way to know
    you have the whole answer is to follow the token until it is gone. This
    operator does that. Anything that calls the API by hand — `scripts/cpl`
    pages the same way — has to do it too, and a first-page answer looks
    exactly like a complete one.

    `mode` and `partition_col` mean what they mean on
    `CsvToWarehouseOperator`, including the append default: the same page
    loaded twice lands twice.

        JsonApiToWarehouseOperator(
            task_id="poll",
            endpoint="carriers/scans",
            params={"since": "{{ ds }}"},
            table="raw.carrier_scans",
            mode="replace",
            partition_col="scan_date",
        )

    Args:
        endpoint: a listing path on the fixture API, without a leading slash.
            One of `endpoint` or `source`.
        source: a JSONL path or glob under the repository root, for the feeds
            that arrive as files rather than over the API — the Meridian
            events land at `landing/meridian/dt={{ ds }}/hr=*/events.jsonl`.
            Templated. One of `endpoint` or `source`.
        params: query parameters, templated. The page token is added to them;
            do not pass one.
        conn_id: the Airflow connection that holds the API's host and port.
            Resolved when the task runs, never at parse.
        base_url: an explicit base URL, which skips the connection. For a local
            run against a stub you started yourself.
        items_key: the response key holding the page's rows. "items".
        page_limit: a stop after this many pages, in case an endpoint ever
            returns a token that does not advance. Raises when it trips, rather
            than loading a truncated answer quietly.
        table, mode, partition_col, partition_value, columns: as
            `CsvToWarehouseOperator`.

    Returns the number of rows loaded, across every page.
    """

    #: `params` is the caller's word and stays the caller's word, but the
    #: templated attribute is `query_params`: Airflow reserves `params` on
    #: every operator and refuses to serialize a DAG that templates it.
    template_fields: Sequence[str] = ("endpoint", "source", "query_params", "table",
                                      "partition_value")
    ui_color = "#a5c3d9"

    def __init__(
        self,
        *,
        table: str,
        endpoint: str | None = None,
        source: str | None = None,
        params: Mapping[str, Any] | None = None,
        conn_id: str = "copperline_api",
        base_url: str | None = None,
        items_key: str = "items",
        page_limit: int = 1000,
        mode: str = "append",
        partition_col: str | None = None,
        partition_value: str = "{{ ds }}",
        columns: Mapping[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if bool(endpoint) == bool(source):
            raise ValueError("give exactly one of `endpoint` or `source`")
        self.table = table
        self.endpoint = endpoint
        self.source = source
        self.query_params = dict(params or {})
        self.conn_id = conn_id
        self.base_url = base_url
        self.items_key = items_key
        self.page_limit = page_limit
        self.mode = mode
        self.partition_col = partition_col
        self.partition_value = partition_value
        self.columns = dict(columns) if columns else None

    def execute(self, context: Any) -> int:
        import tempfile

        if self.source:
            return _load(self, _read_json_sql(self.source, self.columns))
        rows = self.fetch()
        if not rows:
            self.log.info("%s returned no rows", self.endpoint)
            return _load(self, None)
        # The pages go to one JSONL file and load from there, so the API path
        # and the landing-tree path take the same route into the warehouse.
        with tempfile.TemporaryDirectory() as scratch:
            page_file = Path(scratch) / "page.jsonl"
            page_file.write_text(
                "\n".join(json.dumps(row, default=str) for row in rows),
                encoding="utf-8",
            )
            return _load(self, _read_json_sql(str(page_file), self.columns))

    def fetch(self) -> list[dict]:
        """Every row the endpoint has, following `next_page_token` to the end."""
        import urllib.parse
        import urllib.request

        base = (self.base_url or _connection_url(self.conn_id)).rstrip("/")
        query = dict(self.query_params)
        rows: list[dict] = []
        for page in range(1, self.page_limit + 1):
            url = f"{base}/{self.endpoint}?{urllib.parse.urlencode(query)}"
            with urllib.request.urlopen(url) as response:  # noqa: S310 - a loopback stub
                payload = json.loads(response.read().decode("utf-8"))
            rows.extend(payload.get(self.items_key) or [])
            token = payload.get(PAGE_TOKEN_KEY)
            self.log.info("%s page %s: %s rows so far", self.endpoint, page, len(rows))
            if not token:
                return rows
            if token == query.get("page_token"):
                raise RuntimeError(
                    f"{self.endpoint}: the page token stopped advancing at page {page}"
                )
            query["page_token"] = token
        raise RuntimeError(
            f"{self.endpoint}: still paging after {self.page_limit} pages"
        )


# --- the load both operators share -----------------------------------------

def _load(operator: Any, relation: str | None) -> int:
    """Create the table if it is absent, then append or replace one partition.

    `relation` is a SELECT over the incoming rows, or None when the source had
    none — an empty replace still clears the partition it owns, because a day
    with no rows is an answer.

    The delete and the insert are one transaction, so a load that dies halfway
    leaves the table as it found it.
    """
    table = warehouse.qualify(operator.table)
    mode = operator.mode
    partition_col = operator.partition_col
    if mode == "replace" and not partition_col:
        # The guard from the docstring: a table-wide delete on the shared
        # warehouse is not something this operator will do.
        operator.log.info(
            "%s: mode=replace without partition_col, loading as append", table
        )
        mode = "append"
    if mode not in ("append", "replace"):
        raise ValueError(f"mode is 'append' or 'replace', not {mode!r}")

    loaded = 0
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {table.rsplit('.', 1)[0]}")
        if relation is not None:
            con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS {relation} LIMIT 0")
        try:
            con.execute("BEGIN TRANSACTION")
            if mode == "replace":
                con.execute(
                    f"DELETE FROM {table} WHERE {partition_col} = ?",
                    [_as_date(operator.partition_value)],
                )
            if relation is not None:
                counted = con.execute(f"INSERT INTO {table} BY NAME {relation}").fetchall()
                loaded = int(counted[0][0]) if counted and counted[0] else 0
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return loaded


def _read_csv_sql(source: str, columns: Mapping[str, str] | None) -> str:
    """A SELECT over the CSV files `source` matches."""
    options = ["header = true", "union_by_name = true"]
    if columns:
        types = ", ".join(f"'{name}': '{sql}'" for name, sql in columns.items())
        options.append(f"columns = {{{types}}}")
    return f"SELECT * FROM read_csv('{_resolve(source)}', {', '.join(options)})"


def _read_json_sql(source: str, columns: Mapping[str, str] | None) -> str:
    """A SELECT over the newline-delimited JSON files `source` matches."""
    options = ["format = 'newline_delimited'", "union_by_name = true"]
    if columns:
        types = ", ".join(f"'{name}': '{sql}'" for name, sql in columns.items())
        options.append(f"columns = {{{types}}}")
    return f"SELECT * FROM read_json('{_resolve(source)}', {', '.join(options)})"


def _resolve(source: str) -> Path:
    """A source path, under the repository root unless it is absolute.

    A bare source name — `pos`, `carriers` — is taken as that source's landing
    subtree, so `landing_dir()` and this agree on where a feed lands.
    """
    path = Path(source)
    if path.is_absolute():
        return path
    if len(path.parts) == 1 and not path.suffix:
        return landing_dir(source)
    return workspace_root() / path


def _as_date(value: Any) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])


def _connection_url(conn_id: str) -> str:
    """The fixture API's base URL, from an Airflow connection. Read when the
    task runs; nothing here touches a connection at parse time."""
    from airflow.sdk import BaseHook

    conn = BaseHook.get_connection(conn_id)
    scheme = conn.schema or "http"
    port = f":{conn.port}" if conn.port else ""
    return f"{scheme}://{conn.host}{port}"
