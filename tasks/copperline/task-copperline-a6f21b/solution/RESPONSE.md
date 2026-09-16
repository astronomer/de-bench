# FIN-419: the credit memos are filed by the invoice's period, not the issue period

Credit memos filed to the wrong month: 4,866 of 11,528

## What the deck was showing

`stg_finance__credit_memos` selects `applies_to_period as fiscal_month`, and
`raw.credit_memos.applies_to_period` is the fiscal period of the **invoice** date, not of the
memo. `account_rollup.sql` groups its credits by that column, so the account page files every
credit under the period of the invoice it credits.

§REV-5 says the opposite: a memo "recognizes as negative revenue on its issue date... against
the same entity and the same fiscal period the issue date falls in", and it "never restates a
period earlier than its own issue date". The GL page reads the same staging model and gets it
right — `int_gl_postings_unified.sql` posts on `issued_on` at the invoice's rate. Two readers
of one model, and only one of them follows the clause.

## Which way it can run

Only early, never late. No memo in the book is issued before the invoice it credits: on all
11,528 rows `issued_on` is on or after `invoice_date`, so the period the page files under is
always the same as, or earlier than, the period the memo belongs to. The page can pull credit
backwards out of a period and can never push it forward.

That is why a period can still move either way. It loses the credits it should have received
from earlier invoices and gains the credits it should have sent on, and the net of the two
decides the sign.

## The first half of FY2025

Company wide, USD cents, integers. Every memo converts once at the rate its invoice converted
at — the invoice-date rate, §REV-5 and §REV-7 — rounded half up to the cent, in both columns.

| Period | Runs | Should carry | Page files | Movement |
|---|---|---|---|---|
| FY2025-P01 | 2025-02-02 to 2025-03-01 | 164,079,564 | 157,986,739 | +6,092,825 |
| FY2025-P02 | 2025-03-02 to 2025-04-05 | 214,883,881 | 211,087,435 | +3,796,446 |
| FY2025-P03 | 2025-04-06 to 2025-05-03 | 164,712,682 | 179,896,169 | -15,183,487 |
| FY2025-P04 | 2025-05-04 to 2025-05-31 | 171,665,087 | 182,759,420 | -11,094,333 |
| FY2025-P05 | 2025-06-01 to 2025-07-05 | 236,213,375 | 219,141,291 | +17,072,084 |
| FY2025-P06 | 2025-07-06 to 2025-08-02 | 179,791,854 | 185,848,585 | -6,056,731 |
| **Half year** | | **1,131,346,443** | **1,136,719,639** | **-5,373,196** |

A positive movement means the page carries too little credit, so the reported figure for that
period is too high.

**FY2025-P05 moves furthest: +17,072,084 cents.** The page carries 219,141,291 of credit in
it and it should carry 236,213,375.

Nothing is lost in any of this. Over the whole memo book both readings add to the same
5,331,080,655 cents; every one of the 4,866 mis-filed memos is in a period, just not its own.

## Two things that would have changed the answer

**The rate.** §REV-5 converts a memo at the rate its invoice converted at, never at the rate
of its own issue date, because §REV-7 allows one conversion per amount. Using the issue-date
rate gives -15,103,266 for P3 and +16,990,574 for P5; converting nothing at all gives
-11,830,826 and +16,453,971. P1 and P2 are unaffected either way — Great Britain, Ireland and
Germany opened on 2025-04-07, so the first foreign-currency memos land in P3.

**The calendar.** These are 4-5-4 fiscal periods from `raw.fiscal_calendar`. P1 opens
2025-02-02, and P2, P5, P8 and P11 are five weeks long. Calendar months put the boundaries in
different places and no figure above survives.

Nothing in the pipelines was changed for this.
