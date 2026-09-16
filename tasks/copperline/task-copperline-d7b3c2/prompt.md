# GRO-455 — the channel review has been adding pounds to dollars

The Monday channel review opens with a spend number. It is `sum(spend_cents)`
over `raw.ads_spend_daily` for the week, and that feed bills in the market's own
currency: the German and Irish campaigns in euro, the British ones in pounds,
the North American ones in dollars. We have been adding the three together and
calling the answer spend since the feed started. Nobody looked twice until
sterling moved and the spend line moved with it in a week when no campaign had
changed.

`marts.channel_roi_daily` was where this was going to be fixed.
`gro_channel_roi_daily` has never finished a run — it selects models that are
not in the dbt project — and getting that DAG working is a bigger job than this
week has room for. The review still needs a number on Monday.

So build the figure in growth's own tree, as `projects/growth/lib/spend.py`:

    build_day(ds) -> int

One report date per call, published to `marts.channel_spend_base_daily`,
returning the rows written. That table is new — nothing in the tree creates it
yet. Nothing calls the build yet either; hanging it off a schedule is a separate
ticket.

One row per platform, campaign, market and channel for that report date, with
these columns:

    ds, platform, campaign_id, market_code, channel, currency_code,
    fx_rate_ppm, fx_rate_date, spend_cents, spend_base_cents,
    attributed_revenue_cents, attributed_revenue_base_cents

Four things have to hold.

- **One figure per campaign per day.** The feed holds every delivery of a report
  date, the first and the restatements, and the review wants the one we believe
  now.
- **Both money columns convert.** What we spent and what the platform claims we
  earned are both in the billing currency. A cost of sale worked from a
  converted spend and an unconverted return is further out than the number the
  review has today.
- **The rate is the report date's.** A day's spend converts at that day's rate,
  whichever morning the platform delivered it, and it converts once. A report
  date rebuilt a week after a restatement has to come out at the same rate it
  did the first time.
- **The row carries the rate and the date it is the rate for**, so that anybody
  can reproduce `spend_base_cents` from the row without going back to the rate
  table. Every converting fact in this warehouse carries those two columns
  beside the result.

`raw.fx_rates` is the rate table. Read it before you join to it, and know what
is in it and what is not. A currency that falls out of that join takes its
campaigns' whole spend with it, the total still looks like money, and nothing
downstream will say so.

Rerunning a report date replaces that date and leaves every other date alone —
`CONVENTIONS.md`, "Writing to the warehouse".
