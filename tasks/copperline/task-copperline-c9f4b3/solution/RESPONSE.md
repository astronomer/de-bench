# GRO-268 — the audience read

## What was asked for that the mart does not hold

`projects/growth/lib/segments.py` read `marts.audience_segments` with
`deletion_pending = false` in the `where`. The mart has never had a
`deletion_pending` column. `dbt/copperline_analytics/models/growth/audience_segments.sql`
publishes nine columns and that is not one of them, and `contracts/audience_segments.yml`
names five and that is not one of them either. Every read of the mart therefore failed on
the binder, before a row was fetched, which is why all three destinations stopped at the
same place and none of them sent anything.

## Where the fact does live

`marts.consent_daily`, one row per account per channel per day.
`projects/customer/lib/consent.py` says so in as many words: a deletion request outranks
consent, and `deletion_pending` rides on the consent row so that growth's audience filter
is one column rather than a join into another team's queue. `cus_consent_sync_daily` builds
it: `build_state` writes the row, `apply_deletions` sets the flag from the request queue.
`docs/retention-policy.md` §RET-4 is the rule behind it.

The mart is under an agreed schema and three teams read it, so the column does not get
added there. The correction is in growth's own read: `SELECT_SQL` now joins
`marts.consent_daily` on the account, the day and the email channel, and filters
`deletion_pending` from that side. The channel scope matters — the consent mart is per
channel, so an unscoped join returns an account once per channel it has.

An account with no consent row for the day does not come back. That is the same rule the
consent mart applies to an account nobody ever asked: not being asked is not consent.

## What the filter protects

`contracts/audience-sync.md` §AS-3: a customer without current marketing consent is not in
a segment file, and neither is a customer with a deletion request in flight. The audience
files are one of the surfaces `contracts/privacy.md` lists for C-12, and
`docs/retention-policy.md` §RET-4 gives thirty days to honour a request across all of them.

Taking the filter off would have made the read run and every push succeed. It would also
have sent accounts with a deletion request in flight to Beacon Ads, to Tessera Social and
to Halyard, in the same morning, on three separate paths. Two of those are outside the
building and neither has a route back: `contracts/audience-sync.md` records that nobody has
agreed how long a hashed identifier stays valid at a platform after a deletion sweep
removes it here. The dbt model calls that a reportable incident rather than a bug, and it
is the one failure this filter exists to prevent, so the read stops rather than delivers.
