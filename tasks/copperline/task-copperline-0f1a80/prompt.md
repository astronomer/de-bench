# CUS-537 — two churn scores under one name, and a rebuild that dies on its own check

Two findings out of the same afternoon, and they are the same finding.

**One.** Retention asked for the twelve weeks to Friday 5 June scored again, so
they could see which accounts moved. Every week they tried died in the same
place: `check_scores` raised, and it named thousands of scores as out of range.
Nothing is wrong with the accounts. The older the week, the more of the book it
flags, and by the autumn it is nearly all of it.

**Two.** The number in the file retention receives is between nought and one.
The page they were shown when the list was set up is a score out of a hundred,
with a `churn_risk` band of low, medium or high and the rule printed on the row.
Both of those things are called `marts.churn_scores_weekly`. Retention's saved
filter is "70 or more", which is why the list has never had a name on it, and
why nobody was called.

Work out which job writes what, then do the four things below.

## 1. A week that is not this week has to score

The score is five parts, each one scaled to nought-to-one, and the weights are
the whole model. Every part has to come back inside nought-to-one for any week
the order book covers, not only for the week the job happens to run on.

Three things do not move.

- **The weights.** `WEIGHTS` is the model. Arguing with it is a different
  ticket.
- **The population.** Every account the feature step holds still gets one
  score and one row. An account is not dropped for making the arithmetic
  awkward.
- **The source.** `build_features` reads `marts.customer_360` and goes on
  reading it. Whether the denominator should be the lifecycle model's is
  CUS-476's question and it is open.

## 2. One name, one writer

The weekly job stops writing `marts.churn_scores_weekly`. The nightly model
keeps that name: it carries the tests that state its range, and moving a model
means the committed manifest and the graph the nightly build renders from it,
which is a change-management ticket and not this one.

The job writes `marts.customer_churn_weekly` instead, and it creates what it
owns — every other job in the estate that writes a table of its own does.

| column | type | |
|---|---|---|
| `customer_id` | VARCHAR | |
| `source_book` | VARCHAR | which book the account came from, as now |
| `churn_score` | DOUBLE | the weighted sum, as now |
| `score_scale` | VARCHAR | the literal `0-1`, on every row |
| `history_days` | BIGINT | as now |
| `ds` | DATE | the week scored |

`score_scale` is on the row so that a page in front of an account manager says
which of the two numbers it is holding. Rebuilding a week replaces that week and
touches no other.

## 3. The file keeps its name and its place

`include/data/marts/churn_scores_<ds>.csv`. Retention's link points at it, and a
list that lands under a new name is a list nobody opens. It carries the account,
the score and the scale, one line per account, still highest score first.

## 4. The guard stays as strict as it is

`check_scores` is the only written statement of what a score on this table may
be. A rebuild that goes green because the range was widened is a rebuild that
changed the label rather than the number. `score_counts` keeps the counts it
returns; the only thing that moves there is the table it counts.

The DAG keeps the six steps it has.

## RESPONSE.md

At the root of the working tree. Four lines, short — retention reads it:

- every job that writes `marts.churn_scores_weekly` today, and what each one
  puts in it
- which of the two scores retention has been receiving, and how you can tell
- whether both scales have ever been in that table at once, and why
- what the rebuild died on, in one line
