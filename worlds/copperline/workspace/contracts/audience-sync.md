# Contract: audience sync

Schema: `contracts/audience_segments.yml`. Consumer C-6 in `docs/report-registry.md`. Owner: growth. Publisher: `gro_reverse_etl_ads`, publishing through `scripts/publish_reports.sh`. Agreed 2024-06-18, last amended 2025-11-24.

Pushes `marts.audience_segments` to Beacon Ads and Tessera Social every morning, so that the day's campaigns target the right people.

## AS-1 An empty segment is a failure

A segment file with zero rows is a failure, not a delivery. Nothing downstream distinguishes "this audience is empty" from "the audience was not built", and an empty push clears the audience at the ad platform, which stops the campaign. A run that produces no rows for a segment fails, loudly, and delivers nothing rather than delivering nothing-shaped-like-something.

The same holds for the whole sync. Zero segments delivered is a failed run, whatever the exit status of the publish step says.

## AS-2 Grain and columns

One row per customer per segment per day. Columns: `ds`, `segment_id`, `customer_id`, and the hashed contact identifiers the platforms match on. No unhashed contact detail leaves the warehouse on this path, ever.

Segments are defined in the growth project's segment config. This contract binds the delivery, not the definitions.

## AS-3 Consent and deletion

A customer without current marketing consent is not in a segment file. A customer with a deletion request in flight is removed from the next delivery and from the platforms, per `docs/retention-policy.md` §RET-4; the audience files are one of the surfaces `contracts/privacy.md` lists, and they are outside anything dbt can see.

## Delivery

Two platforms, two files, one publish step. The step writes to the export root, then pushes. Growth's on-call owns the delivery and the ad platforms' own dashboards are where a missed audience shows up first, usually as a campaign that stops spending.

## Notes

- The publish step is a shell script rather than an operator. It is a leftover from before the reverse-ETL DAGs existed and it has never been rewritten.
- The script reports what it pushed. It reports success on zero segments, which is what AS-1 exists to say is not acceptable.
- Segment membership changes every day and there is no history. Yesterday's audience is not recoverable.

## Open items

- TODO: move the publish step into the DAG as an operator, so a failure is a task failure.
- Nobody has agreed how long a hashed identifier stays valid at the platform after a deletion sweep removes it here.
