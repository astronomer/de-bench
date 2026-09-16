# Contract: Compass feature table

Schema: `contracts/feature_customer_daily.yml`. Consumer C-9 in `docs/report-registry.md`. Owner: customer. Publisher: `cus_features_daily`. Agreed 2024-08-12, last amended 2025-12-08.

Daily features for the Compass propensity model, written as parquet for the modelling team to pick up.

## FS-1 Values are as of the event time

Every feature value on a row is the value that was true at the row's event time, not the value that is true now. Account attributes come from a slowly-changing dimension and the join takes the version in force at the event time, through the dimension's validity window.

Joining the current version of an attribute is wrong even though it runs, produces a full table, and scores better. A feature built from a value the account did not yet have is a feature that will not exist when the model is asked to predict.

## FS-2 Grain and partitioning

One row per customer per day, partitioned by `ds`, written to `exports/features/{ds}.parquet`. A day is written once. Re-running a day replaces its file.

Columns: `ds`, `customer_id`, the account attributes as of the event time, and the behavioural features over trailing windows of 7, 30 and 90 days. Every trailing window ends on `ds` inclusive.

## FS-3 Deletion reaches the exports

The parquet exports are a surface a deletion request must reach, per `docs/retention-policy.md` §RET-4, and they are on the list in `contracts/privacy.md`. They sit outside the warehouse and dbt cannot see them, so nothing finds them except that list.

Feature files are not aggregates in the sense of `docs/retention-policy.md` §RET-3. They are per-customer rows and they are deleted, not restated.

## Notes

- The dimension behind the account attributes carries a validity window per version. Filtering it to the current version is the shortest way to get a table out and it is what FS-1 forbids.
- The modelling team retrain on a rolling window and read whatever files are present. A missing day is a gap in training data and nothing complains about it.
- Trailing windows are inclusive of `ds` because that is how Compass was trained. Changing it means retraining.

## Open items

- TODO: no feature file carries the version of the feature definitions that produced it.
- The 90-day window makes a full rebuild expensive and nobody has made it incremental.
- Deleted customers leave the future files and stay in the ones already written until the sweep reaches them.
