# northwave_reporting

Northwave Supply's reporting project, inherited with the acquisition in February
2025. It builds against `warehouse/northwave.duckdb`, which is the frozen copy
of their warehouse taken at the namespace freeze.

Some of this is still run nightly. Most of it is not.

The last person who knew which was which left in November. Before you change
anything here, check `ops/lineage.json` and the scheduler for what actually
calls it — the model names here do not match anything in
`dbt/copperline_analytics`, and a name that looks the same is a different table.

`profile: northwave`, which is a different profile from the analytics project.
It points at the frozen file and it is read-only for practical purposes: the
source data behind it stopped refreshing at the freeze.
