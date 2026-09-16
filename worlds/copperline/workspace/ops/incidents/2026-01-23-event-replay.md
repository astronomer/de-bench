# Clickstream replay, 20 to 22 January 2026

Written 2026-01-23 by Ines Okafor, growth.

## What happened

After the feed decay earlier in the month (`ops/incidents/2026-01-14-feed-decay.md`), the collector team replayed the events that had been dropped between 11 and 14 January. The replay ran over three days, 20 to 22 January.

The replay was written into fresh partitions with new offsets. The events themselves are the originals and carry their original `event_id` values, their original `event_time`, and a `loaded_at` from the replay window.

## What that means for anything reading the feed

The same event now exists at two partition offsets: the one it landed at in January and the one the replay wrote. The offsets are different, the `event_id` is the same, and the payloads are identical.

So a reader that identifies an event by its position in the stream sees two events. A reader that identifies an event by `event_id` sees one. Neither is a bug in the feed; they are two different ideas of what an event is.

## Volumes

| | Events |
|---|---:|
| replayed | 6,940 |
| of those, already present from January | 1,102 |
| genuinely new to the warehouse | 5,838 |

The 1,102 are events that had flushed successfully before the batch was dropped, and the collector replayed the whole window rather than the missing part of it.

## What we did

Ran the replay. Sessionization for the affected days was rebuilt afterwards.

We did not remove the January partitions. The replay is additive by design and the collector team keep the original offsets for their own auditing.

## Follow-ups

- Growth to confirm that everything downstream of `raw.web_events` identifies events by `event_id`. Not confirmed at the time of writing.
- TODO: `gro_event_replay_repair` exists for exactly this and was not used, because nobody remembered it was there.
- The collector team will replay only the missing offsets next time, which they could not do this time because the drop was not recorded per event.
