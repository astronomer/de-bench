# NW-214: the active trade-account count for FY2026 planning

Active trade accounts: 5,040

## The split

| Book | Accounts |
|---|---|
| Copperline | 3,960 |
| Northwave, with no merge decision against a Copperline account | 1,080 |
| **Total** | **5,040** |

## How the number was arrived at

1. Count the accounts on each book whose status is active. Copperline's book holds 4,000
   accounts and 3,960 of them are active. The acquired book holds 1,400 and 1,380 are active.
   This is a status count, which is the definition the board pack has printed since FY2023 —
   `contracts/board-pack.md` §BP-1. An account that has not ordered in a year is active if its
   status is active, and an account that ordered yesterday is not.
2. Take the pairs from the reviewed merge decisions in `ops.merge_candidates`. It holds 300
   pairs, each one looked at by a person during the integration review, and each carries who
   decided it and when. All 300 are active on both books, so each one is a single business
   counted twice in step 1.
3. 3,960 + 1,380 − 300 = 5,040. Of that, 3,960 accounts come from Copperline's book and the
   remaining 1,080 are acquired accounts that no reviewer paired with a Copperline account.

The pairing is the whole question, and it comes from the review rather than from a matcher.
`docs/runbooks/northwave-integration.md` §NWI-1 says the reviewed table is the source of record
and a re-derived match is not. The population is why: the same business trades under two
different names across the two books, married names and shared `info@` dealer-group addresses
hide real pairs, and separate businesses share a name, a city or an e-mail domain often enough
that any of those rules pairs companies that are not the same.

## The two numbers in circulation

**The board pack's 5,340** is the two books added together with nothing merged: 3,960 + 1,380.
The 300 reviewed pairs are counted twice, so the pack runs 300 high.

**The CRM's 5,092** applies a matching rule of its own instead of the review. Matching the two
books on a shared e-mail domain gives 248 pairs, and 5,340 − 248 = 5,092, which is the gap
exactly. Those 248 are wrong in both directions: they find only 205 of the 300 real pairs and
they pair 43 businesses that are not the same company. The number runs 52 high, and the error
would not be visible from the total.

Neither number is the one to plan on. 5,040 is.

Nothing in the pipelines was changed for this.
