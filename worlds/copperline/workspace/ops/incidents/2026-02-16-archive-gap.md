# Payments landing files not archived, 9 to 14 February 2026

Written 2026-02-16 by Priya Raman, data platform. Reviewed with commerce 2026-02-18.

## What happened

The archive job that moves payments landing files out of the live tree stopped writing to the archive on 9 February and kept reporting success. It was noticed on 14 February when a restore for an unrelated question came back empty for a recent date.

Six days of Meridian Pay landing files — 2026-02-09 to 2026-02-14 inclusive — were removed from the live tree without an archive copy being written. Those files no longer exist anywhere.

## Why the job reported success

The job writes to the archive, verifies the write, then deletes from the live tree. The verification reads back the object listing for the destination prefix. A configuration change on 8 February moved the destination prefix, and the job created the new prefix, wrote nothing to it that it could read back within its listing window, and treated an empty listing as a listing with no errors.

So it deleted six days of files it had not successfully archived, and said so in six green runs.

## What is recoverable, and what is not

| Window | State |
|---|---|
| before 2026-02-09 | archived normally, restorable |
| 2026-02-09 to 2026-02-14 | live copy deleted, no archive copy — **gone** |
| from 2026-02-15 | archive job fixed, archived normally |

The warehouse tables loaded from those files are intact. What is gone is the immutable landing copy that a truncated or corrupted fact table would be rebuilt from. For those six days there is no source to rebuild from.

Meridian's own retention is 90 days on their side, and their window over this gap has since passed.

## What we did

- 14 February: job stopped, prefix corrected.
- 15 February: archiving resumed and was verified by hand for three days.
- 16 February: the gap confirmed as six days, no copies found under either prefix.

## What this means for a rebuild

Any rebuild of a payments fact that reaches into 9 to 14 February 2026 has no landing source for those six days. The right answer is to say so. Do not interpolate the window from a downstream aggregate and do not fill it from a mart that was built from the same missing files — that produces a fact table that agrees with itself and stands for nothing.

`docs/retention-policy.md` says the archive is the only copy once a file leaves the live tree, and there is no second archive.

## Follow-ups

- The verification step now fails on an empty listing. Done.
- TODO: no alert exists on "archive job wrote zero objects". Raised, not built.
- Commerce asked whether the gap can be reconstructed from the processor. Meridian will not re-send beyond their own retention.
- The restore path, `fct_payments_restore`, was written after this and assumes the archive is complete.
