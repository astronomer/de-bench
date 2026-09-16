# FIN-457 — the pack's revenue line and its account page are not the same number

The controller's office re-papered the C-1 tie-out this quarter and it does not tie.

They took a closed fiscal month, ran the revenue query out of `fin_board_pack_weekly` by
hand, and added up `reported_cents` over every account `marts.account_rollup` holds for the
same month and entity. The two do not agree. It is not rounding: the pack's line comes to
roughly twice the rollup, and it does that for every entity and every closed month back to
FY2024.

The top-twenty page in the same deck is built off the rollup. So the deck has a headline made
one way and a page under it made another, and the twenty accounts have never sat inside the
total the way everyone reading the deck assumes they do.

Work out which of the two numbers the revenue page is supposed to be showing, and make it
show that one. Whatever you land on, the deck has to hold one measure throughout — the
headline and the accounts page cannot go on answering different questions.

Reporting's view is that the pack has printed this figure since FY2023, so the query is right
and the column heading is the thing that is wrong: relabel the heading, tell the board, done
by Friday. Do not take that on their say-so. The documents settle it and there are more of
them than reporting has read.

Two things are not in this ticket. The active-account count belongs to NW-214, and the
channel page is nobody's this week. The pack has been hand-assembled for a while because of
the count, so a clean end-to-end run is not the evidence here — read the code and the marts.

`marts.revenue_recognized_monthly`, and everything else under `dbt/`, stays as it is. The
close file reads that table and this is not the close file's ticket. Whatever the revenue
page needs, do it in the pack.

Then write `RESPONSE.md` at the root of the working tree. Three things in it:

- which of the two numbers the revenue page has to show, and what says so
- what the line was showing instead
- what the difference between the two is made of

Keep it short. It goes in the tie-out file.
