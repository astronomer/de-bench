# GRO-241 — set the clickstream volume alert

The January postmortem left growth a follow-up: propose a replacement rule for
`raw.web_events`. Nobody did. Seven months on, the feed still has no volume
alert at all. `config/alerts.yml` watches its freshness and nothing else, while
`contracts/alert_subjects.yml` has carried `kind: volume` for that table since
the config was rewritten in January.

Set it. One new subject in `config/alerts.yml`, owned by the team that gets
paged. Stay inside what the alerting reader already does — a `row_count` check
on that table, with whatever it needs to count a day. We are not rebuilding the
evaluator this week; the number is the job.

Two things the number has to do, and you should be able to show both against the
feed as it stands in the warehouse:

- **It pages during the January decay** — and before the feed hit zero, because
  the zero day freshness would have caught anyway. Which decay day is the
  earliest a volume rule can honestly reach is yours to work out from the feed.

- **It does not page on an ordinary day.** Take the window the postmortem
  measured and run it through to the end of May. Every day in there that is not
  the decay itself is a day the on-call must not have been woken, including the
  low ones the postmortem names.

The agreed windows in `ops/calendar/quiet-days.yml` stay quiet, per AL-2 — the
migration weekend is absent by arrangement, not broken.

Leave the rest of the config alone. The retired owner names in it are a
different ticket and nobody has decided who inherits them.
