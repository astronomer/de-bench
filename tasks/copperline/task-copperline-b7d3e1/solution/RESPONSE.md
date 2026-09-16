# RR-118 — Meridian settlement dates 2026-02-01 to 2026-03-31

## What we can send now

`ops.payments_restored` holds 75,067 Meridian events settled inside the requested range,
rebuilt from `landing/meridian/` and from nothing else. They fall on 58 of the 59 dates;
2026-02-01 has none.

It reaches February at all because the landing tree is dated by **arrival**, not by
settlement. `landing/meridian/dt=<date>/hr=<hour>/events.jsonl` is what Meridian delivered in
that hour, and an event settled on the 6th can arrive on the 9th or in April. The live tree
starts at `dt=2026-03-17` — the 90 days RET-1 keeps — so what it still holds for February and
early March is the late tail: the events that settled then and were delivered after 17 March.
That tail is a real part of the record, but it is a small share of the range and it is not the
whole of any February date.

The rest of the range is not in the live tree. It has not been lost; it has been archived.

## What an archive restore has to cover

To serve 2026-02-01 to 2026-03-31 in full we need the Meridian landing files for **arrival
dates 2026-01-29 to 2026-03-16**. The file window opens before the settlement window does,
because a payment can settle up to three days after Meridian delivers the event.

`docs/retention-policy.md` says the archive is write-once object storage with no index and no
query path: reading it is a restore job, not a query, and a month of payment files takes a
couple of hours. Budget a working day for those six and a half weeks, then run
`fct_payments_restore` over the same settlement range again. It will pick the archived days up
without a change.

## What we cannot serve at all

**2026-02-09, 2026-02-10, 2026-02-11, 2026-02-12, 2026-02-13 and 2026-02-14.** Six days of
Meridian landing files were deleted from the live tree without an archive copy ever being
written (`ops/incidents/2026-02-16-archive-gap.md`). There is no second archive, so those
files exist nowhere. Meridian will not re-send them: their own retention is 90 days and this
window closed months ago.

Be precise with legal about what that means. It is not that settlements dated 9 to 14 February
are gone — some of those settled on those days and arrived later, and they are in what we are
sending. What is gone is everything Meridian delivered on those six days, whatever date it
settled on, and no restore of ours or theirs will bring it back.

We must not close that hole from `raw.pay_meridian_settlements` or from any mart. Both are our
own reading of the files that are missing, so a record patched from them would agree with
itself and prove nothing to Meridian.
