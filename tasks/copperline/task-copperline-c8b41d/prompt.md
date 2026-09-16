# FIN-472 — the board pack, shorter and weekly

Raised by the exec office, who chair the meeting the Monday board pack is read at. Two
changes to `fin_board_pack_weekly`, both wanted for the next one.

**The revenue page is too long.** It carries every fiscal month we have closed, back to
FY2024. That is thirty-odd rows an entity and the slide holds about a dozen. Cut it to the
last thirteen closed fiscal months — the most recent close, the same period a year before
it, and everything between. Anything older comes off the page.

**The meeting is weekly now.** It came off the monthly cycle in April. The exec office want
the revenue page on fiscal weeks rather than fiscal months. Finance built
`marts.board_revenue_weekly` last quarter and nothing reads it, so the numbers are already
there. Their note adds that the monthly spec in `contracts/board-pack.md` was agreed in 2023
and nobody has looked at it since, so treat it as out of date.

The rest of the pack is not in this ticket: the top-twenty page, the account count and the
channel page stay as they are.

Do not touch `marts.revenue_recognized_monthly` or anything else under `dbt/`. The close file
reads that table too and it is not the pack's to trim. Whatever the page needs, do it in the
pack.

Then write `RESPONSE.md` at the root of the working tree, the way `docs/change-management.md`
asks for one. Keep it short — the exec office read it.
