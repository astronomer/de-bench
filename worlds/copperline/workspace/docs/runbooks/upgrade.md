# Runbook: upgrading the pinned packages

Owner: data platform. Review date 2026-05-26. Advisory, per `docs/change-management.md` §CM-3.

The estate pins its packages and moves them deliberately. This runbook is the order they move in and the pre-flight that runs first.

## UPG-1 The order

Move the pins in this order, one step at a time, with the estate parsing between each:

1. **The provider packages first.** They carry the operator and hook changes and they move independently of the core.
2. **The orchestrator core second.** Nothing else moves in the same change.
3. **The transformation packages third** — the dbt core and the adapter together, never apart, because the adapter is pinned to a core range.
4. **The rendering layer last.** It reads both the orchestrator and the transformation packages and it is the one most likely to reveal what the earlier steps broke.

Never move the core and the transformation packages in one change. When something breaks after a combined move, there is no way to tell which half did it without unpicking the change anyway.

Run `tools/check_upgrade.py` before any of it. It is the pre-flight: point it at the projects and it reports what it knows about.

## What the pre-flight covers

`tools/check_upgrade.py` walks the DAG files for imports and compares them against the symbols removed at the pinned versions. It prints what it finds and exits.

It is a static import check and that is all it is. It does not run anything, does not render a DAG, does not read the dbt project and does not look at configuration. A clean report means the imports it knows about are not the ones that were removed. It is a first pass, not a clearance.

## The order in practice

| Step | What tends to surface |
|---|---|
| providers | import paths that moved between provider versions |
| core | scheduling and parameter changes, and anything a deprecation warning has been saying for a year |
| dbt core and adapter | macro signatures, materialization behaviour, and configuration that has moved position |
| rendering | whatever the earlier steps changed about what a DAG looks like at parse time |

## Before you start

- Read the release notes for every version you are crossing, not only the target.
- Move one estate at a time. The six team projects do not have to move together, and moving them together makes a failure harder to place.
- Behaviour changes matter more than removals. A symbol that disappears fails loudly; a default that changes does not.

## Open items

- TODO: `tools/check_upgrade.py` has a hardcoded symbol list and nobody updates it as part of an upgrade. It knows about the pins it was written for.
- The pins are in three places — the project requirements, the dbt packages file and the rendering layer's own constraint — and nothing keeps them consistent.
- There is no rollback procedure written down. In practice it is a revert of the pin change and a re-parse.
