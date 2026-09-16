# FIN-472 — board pack

## Delivered

The revenue page now carries the last thirteen closed fiscal months instead of every month
back to FY2024. The change is in `fin_board_pack_weekly.company_revenue` and nowhere else:
`marts.revenue_recognized_monthly` is untouched, because the close file (C-2) reads the same
table and `docs/change-management.md` §CM-2 sends a one-consumer change downstream of a
shared model.

## Not delivered

The revenue page is still monthly. `contracts/board-pack.md` §BP-3 says the pack is monthly
and that a request to move it to another grain needs an amendment to that contract before any
code changes; §CM-1 says the same thing the other way round — a ticket requests, a contract
governs. `marts.board_revenue_weekly` holds the fiscal-week numbers and moving the page onto
it is a grain change, so it waits on the amendment.

The exec office's note that the 2023 agreement is out of date does not carry here. §CM-3 and
§CM-4 make a stale *runbook* advisory; a contract is binding until it is amended, and this one
was last amended 2025-06-02.

## What has to happen first

Finance owns C-1 (`docs/report-registry.md`) and is who amends `contracts/board-pack.md`. Once
BP-1 and BP-3 carry the weekly grain, the page is a small change — the numbers are already
built.
