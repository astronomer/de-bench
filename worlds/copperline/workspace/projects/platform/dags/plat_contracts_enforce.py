"""Run every `contracts/<mart>.yml` against the warehouse, every morning.

A contract is what a consumer's owner and the team that builds the model
agreed: the grain, the columns, the types, and the assertions that go with
them. `include.lib.contracts` reads the file and checks it; this DAG runs it
over every contract in the folder and records what came back.

It runs at 06:00, after the 04:00 build, and it reads through the snapshot so
it never queues behind one. A violation is recorded for every contract before
the run fails, so one broken mart does not hide the other eleven.

The prose contract beside each yml governs meaning and nothing runs it. Where
the two disagree that is a bug in the pair — `contracts/README.md` §CON-1.

Owned by data-platform.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag

from include.lib import contracts, warehouse
from include.lib.notify import notify
from include.lib.pipeline import lake_task

#: Where the morning's violations land. Replaced per run day.
TABLE = "ops.contract_violations"

#: The day the check speaks for. A bare cron string is a trigger timetable, so
#: `ds` is the day this fires and the marts it reads are the day before's.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify(
        "platform", "a published mart is outside its contract"
    ),
}


@dag(
    dag_id="plat_contracts_enforce",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["platform", "contracts", "quality"],
    doc_md=__doc__,
)
def plat_contracts_enforce():
    @lake_task
    def contract_names() -> list[str]:
        """Every contract file's stem, sorted.

        The folder is the list. A contract added there is enforced the next
        morning without anybody editing this DAG, which is the arrangement the
        registry's step 3 assumes.
        """
        return contracts.names()

    @lake_task(map_index_template="{{ task.op_kwargs['name'] }}")
    def check(name: str) -> dict:
        """Check one contract and return what it found.

        Returns rather than raises. A contract that names no model — the two
        that govern a document rather than a mart — comes back with one
        violation saying so, and the summary below knows to expect them.
        """
        contract = contracts.load(name)
        return {
            "contract": name,
            "model": contract.model,
            "violations": [{"model": v.model, "rule": v.rule, "detail": v.detail}
                           for v in contracts.check(contract)],
        }

    @lake_task
    def record(results: list[dict], target_ds: str) -> int:
        """Replace the day's rows in `ops.contract_violations`."""
        rows = [
            {"ds": target_ds, "contract": result["contract"],
             "model": violation["model"], "rule": violation["rule"],
             "detail": violation["detail"]}
            for result in results for violation in result["violations"]
        ]
        return warehouse.delete_insert(
            TABLE, "ds", target_ds, rows,
            columns=["ds", "contract", "model", "rule", "detail"],
        )

    @lake_task
    def by_owner(results: list[dict]) -> dict[str, list[str]]:
        """Group the morning's violations by the team that owns the contract,
        so the log names who has to act rather than what broke."""
        summary: dict[str, list[str]] = {}
        for result in results:
            if not result["violations"]:
                continue
            owner = contracts.load(result["contract"]).owner or "unowned"
            summary.setdefault(owner, []).append(result["contract"])
        return {owner: sorted(set(names)) for owner, names in sorted(summary.items())}

    @lake_task
    def fail_on_violations(results: list[dict]) -> int:
        """Fail the run when any mart is outside its contract.

        Last, and after the record, so the morning's evidence is written
        whatever this does. A contract that names no model governs a document
        rather than a mart — `alert_subjects` and `privacy_surfaces` — and the
        DAG that owns each document runs its own list check, so those are
        skipped here rather than counted as broken.
        """
        broken = [
            f"{result['contract']}: {v['rule']}: {v['detail']}"
            for result in results if result["model"]
            for v in result["violations"]
        ]
        if broken:
            raise RuntimeError("contracts broken:\n" + "\n".join(sorted(broken)))
        return 0

    results = check.expand(name=contract_names())
    record(results, target_ds=TARGET_DS) >> fail_on_violations(results)
    by_owner(results)


plat_contracts_enforce()
