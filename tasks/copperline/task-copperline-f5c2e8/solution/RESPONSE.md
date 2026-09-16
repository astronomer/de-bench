# CUS-476

**The buckets.** `int_customer_lifecycle` carries 9 guest rows, one per market
— US, GB, DE, CA, IE, BR, MX, ID and PL — and 515,617 orders sit on them.
GUEST-US holds 327,855 of those on its own, which is why it sorted above every
real account in the book. All nine are still there and still hold every order
they held. What they no longer hold is a first order date, a tenure, a
days-since-last-order or a churn flag.

**Who reads the model, and whether anything moves.** Five models `ref` it, and
none of their numbers move:

| Reader | How it reads the model | Moves? |
|---|---|---|
| `models/customer/dim_customer.sql` | joins `party_key` to a resolved `customer_key` | no — a guest key never matches one |
| `models/customer/agg_customer_rfm.sql` | `where party_kind = 'trade'` | no |
| `models/customer/agg_cohort_retention.sql` | `where party_kind = 'trade'` | no |
| `models/customer/churn_scores_weekly.sql` | `where party_kind = 'trade'` | no |
| `models/growth/audience_segments.sql` | `where party_kind = 'trade'` | no |

Four are ours and the fifth is growth's, so this is a list nobody gets from one
project. No guest row has ever reached a published mart, which is why the
flag has been wrong for a year and nobody saw it. `docs/lineage.md` is the mart
list and this is an `int` model, so the list above came from the `ref`s.

**The rule, as written.** A party has churned when it placed no order in the
180 days ending on the run date the build was given, never on today. A guest
bucket is a denominator and has no churn state, so it is empty there and on the
six other columns that describe a life. `docs/semantic-definitions.md` §SD-3.
