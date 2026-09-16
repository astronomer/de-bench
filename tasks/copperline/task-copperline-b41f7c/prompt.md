# GRO-431 — sign off the January replay before the quarter goes out

Finance want the Q1 web numbers signed off this week. The last thing standing in
the way is the follow-up nobody did after
`ops/incidents/2026-01-23-event-replay.md`: confirm that what we publish out of
`raw.web_events` counts an event the way the feed's own contract counts one.

I started it and got as far as this. Rebuild a session day the replay touched
and the published sessions account for more events than the feed holds for that
day. Rebuild a day from March and the two tie to the row. Nothing failed on any
of those runs and no alert fired — the days are simply wrong, and only some of
them.

Sort it out. Three things have to be true when you are done.

- For any event day, the events the published sessions account for is the number
  of events the feed holds for that day. One event is one event, however many
  times the collector delivered it and wherever it sits in the stream.
- No day that was already right moves, and no day loses events that only ever
  reached us on the replay. The replay carried recovered events as well as
  repeats, and both belong in the numbers.
- `raw.web_events` keeps every row it has. It is landed source and read-only,
  the collector team audit against the original offsets, and the incident note
  says the replay is additive on purpose.

Fix it where the number is defined, not in one job. `gro_sessionize_daily`,
`gro_funnel_daily`, `gro_attribution_daily` and `gro_event_replay_repair` all
build through the same definition, and a repair that only reaches one of them
leaves the other three publishing the old number.

The published session grain does not change. Same table, same columns, one row
per session per day.
