# CUS-476 — the churn flag cites a clause nobody wrote

Two findings out of the same afternoon, and they are the same finding.

**One.** `int_customer_lifecycle` says its churn rule "lives in
docs/semantic-definitions.md". It does not. That document has never carried a
churn clause. It has never said what a party is either. So the one label the
whole company puts on a customer is a threshold one model picked, sitting in a
comment that points at a page where the argument was supposed to happen and
never did. Nobody can disagree with it because there is nothing to disagree
with.

**Two.** Sort the model by `orders_lifetime` and the top of the list is not
customers. Guest checkout writes no account, so every account-less order in a
market lands on one row, `GUEST-<market>`. That much is deliberate and it
stays: the row is the denominator the model was built to hold, and
`agg_customer_rfm` says so in as many words.

What is not deliberate is that the row then answers every question about a
life. It has a first order date, a tenure, a days-since-last-order and a churn
flag. Guest orders land in every market every day, so that flag says none of
those rows has ever churned and none of them ever will — and they are the
oldest, busiest parties in the book by a wide margin. A bucket is not a person
and must not answer as one.

## What to do

### 1. The bucket stops claiming a life

In `int_customer_lifecycle`, a row with `party_kind = 'guest'` comes back empty
on the seven columns that describe one customer's history:

    first_order_date, last_order_date, tenure_days, days_since_last_order,
    is_churned, is_one_and_done, is_new

Empty, not false and not zero. False is a claim about a customer and there is
no customer on that row.

Everything else on a guest row stays exactly as it is. The order counts, the
money, `channels_used` and `average_order_net_cents` are the denominator, and
holding the denominator is the whole reason the row exists. One row per market
before and the same rows after — none of them dropped, none of them split, and
every order still on the one it was on. Nothing changes on a row that is not a
bucket.

### 2. The test moves with the rule

`first_order_date` carries a `not_null` in
`models/shared/intermediate/_intermediate_models.yml`, and it goes red the
moment the buckets go empty. Narrow it to the rows it is still true of. Do not
delete it and do not turn it down to a warning: a column that is legitimately
empty on the bucket rows is a test that gets scoped, not a test that gets
dropped.

`dbt test --select int_customer_lifecycle` has to come back clean afterwards,
and it still has to go red if `first_order_date` ever goes missing on a party
that is not a bucket. Both halves matter. A test that passes because it now
asserts nothing is worse than the red one.

### 3. Write the clause the model says is already there

Into `docs/semantic-definitions.md`, in the form that document uses. Its own
"Adding a name" section is the rule it is asking to be held to: a model that
computes something the definitions do not cover needs the name written down.
Cover three things.

- `is_churned`: the threshold, and what the threshold is counted back from. The
  model measures against the date the build was given, never against today, and
  a reader who assumes today gets a different answer on every backfill. Say so.
- What a party is, and the kinds of party the model publishes. The grain is
  half the definition — this document says that itself.
- That a guest party has no churn state, and which columns are empty on it.

Do not change the meaning of a name that is already in the document.

## RESPONSE.md

At the root of the working tree. Three things, short:

- how many rows in the model are guest buckets, and how many orders sit on them
- every model that reads `int_customer_lifecycle`, and whether this change
  moves any of their numbers. `docs/lineage.md` is the list of who reads a
  **mart**; this is not a mart, so this one you work out yourself, and it runs
  wider than one team
- the churn rule in one line, the way you wrote it into the definitions

The nightly build runs the model the way it always has. Nothing else moves this
week.
