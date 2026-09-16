"""Keep the copperline documents layer from drifting apart.

The documents are the world's authority surface: policies, contracts, runbooks
and the ops notes beside them. Tasks cite their clauses by id, so the layer has
to stay internally consistent — a clause that loses its anchor, a consumer that
loses its contract, or a rule stated twice with no precedence all turn a graded
decision into a coin flip.

Five things are asserted:

1. every document in the inventory exists, and no document exists that the
   inventory does not name;
2. every clause id is unique, every file carries the clause count it declares,
   and every planted row cites a clause that exists — matched by id, never by
   assuming the ids run without gaps;
3. every consumer in the report registry appears in the lineage document and
   has both a prose contract and the machine schema that contract names;
4. every money or unit column a prose contract names is defined in the semantic
   definitions, or declared by that contract as its own;
5. every pair of documents that could disagree states its precedence.

    python tools/check_docs.py            # the copperline world
    python tools/check_docs.py --world X  # any world laid out the same way

Exits non-zero and prints one line per failure.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]

# --- 1. the inventory ------------------------------------------------------
#
# Path under workspace/, and the number of load-bearing clauses it carries. The
# clause bodies are the numbered sections; everything else is prose that
# restates, gives history or works an example.

DOCS: dict[str, int] = {
    "docs/finance-policy.md": 15,          # REV-1..REV-14, REV-16 — not contiguous
    "docs/comp-store-policy.md": 4,
    "docs/retail-calendar.md": 3,
    "docs/rate-policy.md": 5,
    "docs/inventory-policy.md": 4,
    "docs/billing-integration.md": 8,
    "docs/reconciliation-policy.md": 7,
    "docs/semantic-definitions.md": 11,    # 9 column definitions, SD-1, SD-2
    "docs/change-management.md": 4,
    "docs/retention-policy.md": 6,
    "docs/late-data-policy.md": 3,
    "docs/lineage.md": 1,
    "docs/report-registry.md": 3,
    "docs/blueprints.md": 0,               # mechanism only
    "docs/memos/fy26-cost-restatement.md": 2,
    "docs/memos/2026-04-quality-contract-revision.md": 3,
    "docs/runbooks/pos-ingestion.md": 2,
    "docs/runbooks/customer-id-migration.md": 3,
    "docs/runbooks/northwave-integration.md": 2,
    "docs/runbooks/processor-migration.md": 2,
    "docs/runbooks/market-setup.md": 1,
    "docs/runbooks/orchestrator-migration.md": 1,
    "docs/runbooks/upgrade.md": 1,
    "contracts/README.md": 1,
    "contracts/board-pack.md": 3,
    "contracts/finance-close.md": 3,
    "contracts/settlement-summary.md": 3,
    "contracts/merch-dashboards.md": 3,
    "contracts/daily-flash.md": 3,
    "contracts/audience-sync.md": 3,
    "contracts/replenishment.md": 3,
    "contracts/customer-360.md": 3,
    "contracts/feature-store.md": 3,
    "contracts/alerting.md": 3,
    "contracts/partner-share.md": 3,
    "contracts/privacy.md": 3,
}

SCHEMAS = {
    "contracts/account_rollup.yml",
    "contracts/revenue_recognized_monthly.yml",
    "contracts/settlement_weekly.yml",
    "contracts/order_economics.yml",
    "contracts/comp_sales_daily.yml",
    "contracts/audience_segments.yml",
    "contracts/inventory_position.yml",
    "contracts/customer_360.yml",
    "contracts/feature_customer_daily.yml",
    "contracts/alert_subjects.yml",
    "contracts/sell_through_daily.yml",
    "contracts/privacy_surfaces.yml",
}

OPS = {
    "ops/incidents/2025-11-03-vendor-file-doubled.md",
    "ops/incidents/2026-01-14-feed-decay.md",
    "ops/incidents/2026-01-23-event-replay.md",
    "ops/incidents/2026-02-10-settlement-replay.md",
    "ops/incidents/2026-02-16-archive-gap.md",
    "ops/calendar/quiet-days.yml",
    "ops/runbooks/warehouse-cleanup.md",
    "ops/runbooks/alerting.md",
}

# --- 3. the consumers ------------------------------------------------------

CONSUMERS = {
    "C-1": "contracts/board-pack.md",
    "C-2": "contracts/finance-close.md",
    "C-3": "contracts/settlement-summary.md",
    "C-4": "contracts/merch-dashboards.md",
    "C-5": "contracts/daily-flash.md",
    "C-6": "contracts/audience-sync.md",
    "C-7": "contracts/replenishment.md",
    "C-8": "contracts/customer-360.md",
    "C-9": "contracts/feature-store.md",
    "C-10": "contracts/alerting.md",
    "C-11": "contracts/partner-share.md",
    "C-12": "contracts/privacy.md",
}

# --- 5. precedence ---------------------------------------------------------
#
# Every pair of documents that could disagree, and where the sentence that
# settles it lives. A pair with no row here is a flat contradiction, which
# grades a coin flip. Each entry is (pair, [(path, clause or None, phrases)]).

PRECEDENCE: list[tuple[str, list[tuple[str, str | None, list[str]]]]] = [
    ("finance policy vs any contract", [
        ("docs/finance-policy.md", None,
         ["this policy governs recognition", "Contracts govern presentation only"]),
        ("contracts/board-pack.md", "BP-2",
         ["finance-policy governs recognition and this contract governs what the pack shows"]),
    ]),
    ("finance policy vs comp-store policy", [
        ("docs/finance-policy.md", None,
         ["This policy does not define comparability"]),
        ("docs/comp-store-policy.md", None,
         ["only authority on comparability", "never defines which stores are comparable"]),
    ]),
    ("comp-store policy vs a published comp figure", [
        ("docs/comp-store-policy.md", "CMP-2",
         ["figures already published are restated", "legitimately differs"]),
    ]),
    ("retail calendar vs date arithmetic in a model", [
        ("docs/retail-calendar.md", "CAL-3", ["`comp_date_ly` holds the answer"]),
        ("docs/finance-policy.md", "REV-16", ["Never compare by calendar-date offset"]),
    ]),
    ("rate policy vs raw.carrier_rate_cards", [
        ("docs/rate-policy.md", "RATE-1",
         ["the policy governs and the table is corrected"]),
    ]),
    ("inventory policy vs the data", [
        ("docs/inventory-policy.md", "INV-1", ["The data cannot supply this rule"]),
    ]),
    ("cost-restatement memo vs semantic definitions", [
        ("docs/memos/fy26-cost-restatement.md", "MEMO-1",
         ["this memo governs the workbook's column"]),
    ]),
    ("quality-contract revision vs a shipped test", [
        ("docs/memos/2026-04-quality-contract-revision.md", "QC-3",
         ["this memo governs and the test is rewritten"]),
    ]),
    ("reconciliation vs billing integration", [
        ("docs/reconciliation-policy.md", None,
         ["That document defines the terms; this one governs which side wins"]),
        ("docs/billing-integration.md", "B-1", ["governs which side wins"]),
    ]),
    ("reconciliation vs finance policy on closed months", [
        ("docs/reconciliation-policy.md", "R-5",
         ["closed under finance-policy §REV-8"]),
    ]),
    ("ticket vs contract", [
        ("docs/change-management.md", "CM-1", ["A ticket requests; a contract governs"]),
    ]),
    ("ticket vs finance policy", [
        ("docs/change-management.md", "CM-1",
         ["unless the contract it affects has been amended first"]),
    ]),
    ("runbook vs code", [
        ("docs/change-management.md", "CM-3", ["the code is right"]),
    ]),
    ("runbook vs runbook", [
        ("docs/change-management.md", "CM-4", ["the one inside its review date"]),
    ]),
    ("registry vs lineage", [
        ("docs/report-registry.md", "REG-2", ["`docs/lineage.md` governs"]),
    ]),
    ("contract prose vs contract yml", [
        ("contracts/README.md", "CON-1",
         ["The yml is enforced and the prose governs meaning"]),
    ]),
    ("semantic definitions vs a model", [
        ("docs/semantic-definitions.md", "SD-2", ["must not use one of these names"]),
        ("docs/semantic-definitions.md", None, ["the model is wrong and is renamed"]),
    ]),
    ("retention vs a deletion request", [
        ("docs/retention-policy.md", "RET-4",
         ["across every surface listed in `contracts/privacy.md`"]),
        ("docs/retention-policy.md", "RET-5", ["the exemption holds and the record stays"]),
    ]),
]

CLAUSE_HEADING = re.compile(r"^##\s+([A-Z][A-Z0-9]*-\d+)\b")
COLUMN_TOKEN = re.compile(r"`([a-z][a-z0-9_]*(?:_cents|_units))`")
LOCAL_COLUMNS = re.compile(
    r"Columns defined by this contract and not in `docs/semantic-definitions\.md`:\s*(.+)")


def _clause_ids(body: str) -> list[str]:
    return [m.group(1) for line in body.splitlines() if (m := CLAUSE_HEADING.match(line))]


def _semantic_columns(body: str) -> list[str]:
    """The reserved names, read out of the definitions table itself. These nine
    are clauses in their own right: a planted row cites them by column name."""
    names, inside = [], False
    for line in body.splitlines():
        if line.startswith("## "):
            inside = line.startswith("## The reserved names")
            continue
        m = re.match(r"^\|\s*`([a-z][a-z0-9_]+)`\s*\|", line)
        if inside and m:
            names.append(m.group(1))
    return names


def _section(body: str, clause: str) -> str:
    """One clause's body: its heading down to the next heading of any level."""
    lines = body.splitlines()
    for i, line in enumerate(lines):
        m = CLAUSE_HEADING.match(line)
        if m and m.group(1) == clause:
            for j in range(i + 1, len(lines)):
                if lines[j].startswith("#"):
                    return "\n".join(lines[i:j])
            return "\n".join(lines[i:])
    return ""


def check(world: str) -> list[str]:
    root = ROOT / "worlds" / world
    ws = root / "workspace"
    fail: list[str] = []
    bodies: dict[str, str] = {}

    # 1. the inventory names every document, and only documents that exist.
    declared = set(DOCS) | SCHEMAS | OPS
    for rel in sorted(declared):
        path = ws / rel
        if not path.is_file():
            fail.append(f"1: {rel} is in the inventory and does not exist")
        elif rel.endswith(".md"):
            bodies[rel] = path.read_text(encoding="utf-8")
    found = {
        str(p.relative_to(ws))
        for top in ("docs", "contracts", "ops")
        for p in (ws / top).rglob("*")
        if p.is_file()
    }
    for rel in sorted(found - declared):
        fail.append(f"1: {rel} exists and the inventory does not name it")

    # 2. clause ids: unique, counted, and cited only where they exist.
    ids: dict[str, str] = {}
    for rel, expected in sorted(DOCS.items()):
        body = bodies.get(rel)
        if body is None:
            continue
        clauses = _clause_ids(body)
        if rel == "docs/semantic-definitions.md":
            clauses += _semantic_columns(body)
        if len(clauses) != expected:
            fail.append(
                f"2: {rel} declares {expected} clauses and carries "
                f"{len(clauses)}: {', '.join(clauses)}")
        for cid in clauses:
            if cid in ids and ids[cid] != rel:
                fail.append(f"2: clause {cid} is in both {ids[cid]} and {rel}")
            ids[cid] = rel

    planted_path = root / "planted.yaml"
    planted = []
    if planted_path.is_file():
        planted = yaml.safe_load(planted_path.read_text(encoding="utf-8")) or []
    cited: set[str] = set()
    for row in planted:
        ref = str(row.get("clause", ""))
        rel, _, cid = ref.partition("#")
        if not cid:
            fail.append(f"2: planted row {row.get('id', '?')} cites {ref!r}, "
                        "which is not <document>#<clause>")
            continue
        cited.add(cid)
        if ids.get(cid) != rel:
            where = ids.get(cid, "no document")
            fail.append(f"2: planted row {row.get('id', '?')} cites {ref}, "
                        f"and {cid} is in {where}")
    if planted:
        # Coverage is only meaningful once rows exist. Every load-bearing
        # clause needs a row: one with none is furniture that thinks it is
        # load-bearing.
        for cid, rel in sorted(ids.items()):
            if cid not in cited:
                fail.append(f"2: {rel} clause {cid} has no planted row")

    # 3. every consumer: registry row, lineage mention, prose contract, schema.
    registry = bodies.get("docs/report-registry.md", "")
    lineage = bodies.get("docs/lineage.md", "")
    for consumer, contract in CONSUMERS.items():
        if not re.search(rf"^\| {re.escape(consumer)} \|", registry, re.M):
            fail.append(f"3: {consumer} has no row in docs/report-registry.md")
        if consumer not in lineage:
            fail.append(f"3: {consumer} is in the registry and not in docs/lineage.md")
        body = bodies.get(contract)
        if body is None:
            fail.append(f"3: {consumer} names {contract}, which does not exist")
            continue
        first = body.splitlines()[2] if len(body.splitlines()) > 2 else ""
        schema = re.search(r"`(contracts/[a-z0-9_]+\.yml)`", first)
        if not schema:
            fail.append(f"3: {contract} does not name its schema in its first line")
        elif not (ws / schema.group(1)).is_file():
            fail.append(f"3: {contract} names {schema.group(1)}, which does not exist")

    # 4. money and unit columns named by a contract are defined somewhere.
    defined = set(_semantic_columns(bodies.get("docs/semantic-definitions.md", "")))
    for contract in sorted(set(CONSUMERS.values())):
        body = bodies.get(contract, "")
        local = LOCAL_COLUMNS.search(body)
        own = set(re.findall(r"`([a-z][a-z0-9_]+)`", local.group(1))) if local else set()
        for column in sorted(set(COLUMN_TOKEN.findall(body))):
            if column not in defined and column not in own:
                fail.append(
                    f"4: {contract} names `{column}`, which docs/semantic-definitions.md "
                    "does not define and the contract does not claim as its own")

    # 5. every pair that could disagree states its precedence.
    for pair, statements in PRECEDENCE:
        for rel, clause, phrases in statements:
            body = bodies.get(rel)
            if body is None:
                fail.append(f"5: {pair} is stated in {rel}, which does not exist")
                continue
            text = _section(body, clause) if clause else body
            if clause and not text:
                fail.append(f"5: {pair} is stated in {rel} §{clause}, which is not there")
                continue
            for phrase in phrases:
                if phrase not in text:
                    where = f"{rel} §{clause}" if clause else rel
                    fail.append(f"5: {pair}: {where} does not say {phrase!r}")
    return fail


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--world", default="copperline")
    args = parser.parse_args()
    fail = check(args.world)
    for line in fail:
        print(line)
    if fail:
        print(f"\n{len(fail)} problem(s) in the {args.world} documents layer")
        return 1
    print(f"{args.world} documents layer is consistent")
    return 0


if __name__ == "__main__":
    sys.exit(main())
