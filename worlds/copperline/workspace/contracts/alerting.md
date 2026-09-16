# Contract: data health alerts

Schema: `contracts/alert_subjects.yml`. Consumer C-10 in `docs/report-registry.md`. Owner: growth. Publisher: `gro_alerting_daily`. Agreed 2024-10-01, last amended 2026-01-26.

Freshness and volume alerting over the feeds and the marts. It pages growth's on-call.

## AL-1 Every watched table has an owner who is paged

Every table named in `config/alerts.yml` has an owning team recorded beside it, and that team is who the alert pages. A table with no owner is not watched: an alert nobody owns wakes the wrong person and gets muted, which is worse than not alerting at all.

The owner in the config is the team, not a person. The team's rotation is where it lands.

## AL-2 Legitimate quiet days do not page

An alert must not fire on a day Copperline knows will be quiet. `ops/calendar/quiet-days.yml` holds the windows: planned closures, planned migrations, and anything else agreed in advance where a low or absent volume is expected rather than wrong.

Equally, a peak day is not an anomaly. The market calendar carries the trading days and the known peaks, and an alert built on a rolling standard deviation fires on the biggest sales day of the year unless it exempts them.

An alert that pages on a known quiet window is a defect in the alert, not a fact about the feed.

## AL-3 The config is the definition

An alert exists because it is in `config/alerts.yml`. The file holds the table name as a string, the check, the threshold and the owner. Nothing is defined in the DAG, and a threshold changed in code rather than in config is invisible to everyone who reads the config to find out what is watched.

Because the tables are strings, this consumer is invisible to dbt and to the Airflow graph. `docs/lineage.md` lists it.

## Notes

- The alerting runbook under `ops/runbooks/alerting.md` is separate from this contract and covers how failures notify. Check its review date before following it, per `docs/change-management.md` §CM-4.
- Postmortems are where thresholds come from. `ops/incidents/` holds them, and a threshold set without reading the relevant one is a guess.
- The DAG runs at 11:00. It is not a real-time path and has never claimed to be.

## Open items

- TODO: three tables in the config have an owner that is a team that no longer exists under that name.
- There is no way to silence an alert for a window from the config. On-call mute it at the pager instead, which loses the reason.
- Nobody reviews the threshold set. It is changed when something is missed and never when something over-fires.
