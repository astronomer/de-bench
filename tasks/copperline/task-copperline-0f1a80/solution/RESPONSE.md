# CUS-537 — the churn list

**Three jobs write `marts.churn_scores_weekly`.** The nightly build writes the
dbt model `models/customer/churn_scores_weekly.sql`: rule-based points out of a
hundred, four signals with stated weights, a `churn_risk` band of low, medium
or high, and one row per account keyed on `week_of`. This weekly job wrote it
too, through `churn.build_scores`: a weighted score between nought and one,
with `source_book` and `history_days`, keyed on `ds`. And
`cus_northwave_backfill` replays it out of the frozen Northwave copy through
`backfill.replay`, which puts an account name, `orders_12m` and
`net_sales_cents` on the row and no score at all. Three shapes, one name.

**Retention has been receiving this job's score, the one between nought and
one.** It is the only one of the three that publishes a file:
`churn.publish` writes `include/data/marts/churn_scores_<ds>.csv`, and neither
the model nor the replay writes anything outside the warehouse. A filter of
"70 or more" against a column that never reaches 1 returns nobody, every week.

**The two scales have never been in that table together.** The model is
`materialized='table'`, so the 04:00 build drops it and rebuilds it out of the
model's own columns; the job's write is a delete-insert on `ds`, which is a
column the model's table has not got, so the job's write cannot bind against it
at all. Whichever of the two ran last owned the table, and the other one either
overwrote it whole or died on it.

**The rebuild died because the recency part had no lower bound.** It counts the
days from an account's last order to the week being scored. For any week that
is not the current one, an account that has ordered since counts back a negative
number of days, recency goes below nought, and its weight is subtracted from the
score instead of adding part of itself. `check_scores` then rejects the row, and
the further back the week the more of the book it rejects. Every part is now
held inside nought-to-one, this job writes `marts.customer_churn_weekly` with
`score_scale` on the row, and the file keeps its name.
