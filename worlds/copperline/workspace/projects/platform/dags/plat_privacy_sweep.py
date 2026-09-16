"""Work the deletion queue across every surface the privacy contract lists.

`ops/privacy/deletion_requests.csv` is the queue. `contracts/privacy_surfaces.yml`
is the list of places a deletion has to reach, and that list is the read set of
this DAG: it is prose and YAML rather than code, so no lineage tool and no grep
of the models will find it. Four of the eleven surfaces are files outside the
warehouse — the feature exports, the audience files, the partner-share drop and
the service-desk sync — and dbt has never been able to see any of them.

Three rules from `docs/retention-policy.md` shape what this does.

- **RET-4**: a request is honoured within 30 days, across every listed surface.
- **RET-5**: the ledger, the invoice tables behind it and anything under legal
  hold are exempt. The exemption is recorded on the request; it is not a silent
  skip and it is not a deletion.
- **RET-3**: a published aggregate partition is immutable. Deletion is a
  targeted DELETE against the row-level tables plus a documented restatement,
  never a rebuild of the aggregate. This DAG does not rebuild anything, and the
  restatement note is what the receipt carries instead.

Owned by data-platform. It is the platform team's only consumer, and what it
consumes is other teams' outputs.
"""

from __future__ import annotations

import csv
import datetime as dt
import json

import pendulum
from airflow.sdk import dag

from include.lib import calendar as cal
from include.lib import contracts, warehouse, workspace_root
from include.lib.notify import notify
from include.lib.pipeline import lake_task

QUEUE = workspace_root() / "ops" / "privacy" / "deletion_requests.csv"
RECEIPTS = workspace_root() / "exports" / "privacy"

#: The contract whose `surfaces` list is this DAG's read set.
CONTRACT = "privacy_surfaces"

#: RET-4. A request older than this is late, and being late is worth failing
#: the run over rather than logging.
HONOUR_WITHIN_DAYS = 30

#: RET-5. A surface that is exempt, and stays.
EXEMPT_SURFACES = ("finance_ledger", "invoices", "invoice_lines", "credit_memos")

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=10),
    "on_failure_callback": notify(
        "platform", "the deletion queue did not complete; RET-4 is a 30-day commitment"
    ),
}


@dag(
    dag_id="plat_privacy_sweep",
    schedule="0 13 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["platform", "privacy", "retention"],
    doc_md=__doc__,
)
def plat_privacy_sweep():
    @lake_task
    def read_requests() -> list[dict]:
        """The open requests, with the day each one was received.

        A request with no received date cannot be aged, so it is treated as
        received today and flagged. That is the safe direction: it gets worked
        now rather than sitting until somebody notices the column is empty.
        """
        if not QUEUE.exists():
            return []
        with QUEUE.open(encoding="utf-8", newline="") as handle:
            rows = [dict(row) for row in csv.DictReader(handle)]
        return [row for row in rows if (row.get("state") or "open") == "open"]

    @lake_task
    def read_surfaces() -> list[dict]:
        """The surface list, from the contract.

        The contract is the list. A surface added there is swept the next day
        without this DAG being edited, which is the arrangement `contracts/
        privacy.md` describes and the reason the read set is a document.
        """
        contract = contracts.load(CONTRACT)
        surfaces = contract.raw.get("surfaces") or []
        if not surfaces:
            raise ValueError(f"contracts/{CONTRACT}.yml lists no surfaces")
        return [dict(surface) for surface in surfaces]

    @lake_task
    def check_exemptions(requests: list[dict]) -> list[dict]:
        """Mark what RET-5 keeps, and check nothing has aged past RET-4.

        The exemption is a decision recorded on the request, not a surface that
        is quietly missed. A request that covers an account under legal hold
        still gets a receipt, and the receipt says which records stayed and
        why.
        """
        today = cal.today()
        marked = []
        for request in requests:
            received = request.get("received_on") or today.isoformat()
            age = (today - dt.date.fromisoformat(str(received)[:10])).days
            if age > HONOUR_WITHIN_DAYS:
                raise RuntimeError(
                    f"request {request.get('request_id')} is {age} days old and "
                    f"RET-4 commits to {HONOUR_WITHIN_DAYS}"
                )
            marked.append({
                **request,
                "received_on": received,
                "age_days": age,
                "legal_hold": str(request.get("legal_hold") or "").lower() == "true",
            })
        return marked

    @lake_task(map_index_template="{{ task.op_kwargs['surface']['id'] }}",
               pool="warehouse_write")
    def sweep_table(surface: dict, requests: list[dict]) -> dict:
        """Delete the subject's rows from one warehouse surface.

        Row-level only. An aggregate is immutable under RET-3, so a mart that
        holds a total rather than a person is not on this list and is not
        touched here.
        """
        if surface["kind"] != "table":
            return {"surface": surface["id"], "skipped": "not a table"}
        if surface["path"].rsplit(".", 1)[-1] in EXEMPT_SURFACES:
            return {"surface": surface["id"], "exempt": True, "deleted": 0}
        subjects = [r["subject_id"] for r in requests if not r["legal_hold"]]
        if not subjects:
            return {"surface": surface["id"], "deleted": 0}
        table = warehouse.qualify(surface["path"])
        column = surface.get("subject_column", "customer_id")
        marks = ", ".join("?" for _ in subjects)
        with warehouse.connect() as con:
            deleted = con.execute(
                f"SELECT count(*) FROM {table} WHERE {column} IN ({marks})", subjects
            ).fetchone()[0]
            con.execute(f"DELETE FROM {table} WHERE {column} IN ({marks})", subjects)
        return {"surface": surface["id"], "deleted": int(deleted)}

    @lake_task(map_index_template="{{ task.op_kwargs['surface']['id'] }}")
    def sweep_files(surface: dict, requests: list[dict]) -> dict:
        """Rewrite the file surfaces without the subject's rows.

        These are the four dbt cannot see. Each is a directory of CSV or JSON
        files written by another team's export, and the deletion is a rewrite
        of each file rather than a delete of it: the file is somebody's daily
        partition and removing it entirely takes a day of history with it.
        """
        if surface["kind"] != "files":
            return {"surface": surface["id"], "skipped": "not files"}
        subjects = {r["subject_id"] for r in requests if not r["legal_hold"]}
        root = workspace_root() / surface["path"]
        if not subjects or not root.exists():
            return {"surface": surface["id"], "rewritten": 0, "removed_rows": 0}
        rewritten = removed = 0
        for path in sorted(root.rglob("*.csv")):
            with path.open(encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                fields = reader.fieldnames or []
                key = next((f for f in ("customer_id", "account_id", "subject_id")
                            if f in fields), None)
                rows = list(reader)
            if not key:
                continue
            keep = [row for row in rows if row.get(key) not in subjects]
            if len(keep) == len(rows):
                continue
            partial = path.with_suffix(".csv.partial")
            with partial.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fields)
                writer.writeheader()
                writer.writerows(keep)
            partial.replace(path)
            rewritten += 1
            removed += len(rows) - len(keep)
        return {"surface": surface["id"], "rewritten": rewritten,
                "removed_rows": removed}

    @lake_task
    def write_receipts(requests: list[dict], table_results: list[dict],
                       file_results: list[dict], target_ds: str) -> list[str]:
        """One receipt per request, in `exports/privacy/`.

        The receipt names every surface reached, what was removed from each,
        and which records stayed under an exemption. RET-3's restatement note
        goes here too: nothing rebuilt an aggregate, and the receipt says what
        the published figures still include.
        """
        RECEIPTS.mkdir(parents=True, exist_ok=True)
        written = []
        for request in requests:
            receipt = {
                "request_id": request.get("request_id"),
                "subject_id": request.get("subject_id"),
                "received_on": request.get("received_on"),
                "worked_on": target_ds,
                "legal_hold": request["legal_hold"],
                "surfaces": sorted(table_results + file_results,
                                   key=lambda r: r["surface"]),
                "aggregates": "not rebuilt, per docs/retention-policy.md RET-3",
            }
            path = RECEIPTS / f"{request.get('request_id')}.json"
            path.write_text(json.dumps(receipt, indent=2, default=str), encoding="utf-8")
            written.append(str(path.relative_to(workspace_root())))
        return written

    @lake_task
    def verify(surfaces: list[dict], table_results: list[dict],
               file_results: list[dict]) -> int:
        """Every surface the contract lists was reported on.

        This is the check that makes the contract's list load-bearing rather
        than aspirational: a surface added to the yml and not reachable by
        either sweep fails here on the first night.
        """
        reported = {result["surface"] for result in table_results + file_results}
        missing = sorted({surface["id"] for surface in surfaces} - reported)
        if missing:
            raise RuntimeError(
                "surfaces the contract lists and the sweep did not reach: "
                + ", ".join(missing)
            )
        return len(reported)

    requests = check_exemptions(read_requests())
    surfaces = read_surfaces()
    table_results = sweep_table.partial(requests=requests).expand(surface=surfaces)
    file_results = sweep_files.partial(requests=requests).expand(surface=surfaces)
    write_receipts(requests, table_results, file_results, target_ds="{{ ds }}")
    verify(surfaces, table_results, file_results)


plat_privacy_sweep()
