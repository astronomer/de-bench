# Contract: Kestrel vendor share

Schema: `contracts/sell_through_daily.yml`. Consumer C-11 in `docs/report-registry.md`. Owner: supply. Publisher: `sc_partner_share_kestrel`. Agreed 2025-09-08, last amended 2026-04-14.

A weekly SFTP drop of Kestrel Outdoor's own sell-through, so the vendor can plan production. It leaves Copperline and goes to a third party, which is why the column list is exact in both directions.

## PS-1 The required columns

The file carries exactly these columns, in this order: `week_start`, `sku`, `store_region`, `demand_units`, `net_sales_cents`.

All five are required. A file missing a column is not a partial delivery, it is a failed one — Kestrel's own loader is positional and a short file lands wrong rather than failing. Do not add columns either: an extra column shifts everything after it.

## PS-2 Personal data does not leave

No column derived from a source column tagged as personal data appears in this file, in any form. The tags are at source-column level in the governance metadata, and the rule follows the derivation: a column computed from a tagged column is itself tagged, whatever it is called. Exclude it, or hash it where the agreement allows a hash.

This is a rule about what may not be sent. It does not license dropping a required column from PS-1: over-redaction breaks the delivery exactly as under-redaction breaks the agreement. Where a required column appears to derive from tagged data, that is a conflict to raise, not a decision to take alone.

## PS-3 Delivery and naming

One file a week, written as `sell_through_<region>_<week>.csv` and dropped over SFTP on Friday. The DAG's sensor waits on that pattern before it opens the session.

The pattern is the whole coupling. Rename the output or change its layout and the sensor waits until it times out: no error, no failed task, and a vendor who stops receiving a file. Nothing else in the repository names the model that writes it.

## Notes

- Kestrel is a vendor on a partner-share agreement, not a marketplace seller. The commercial terms are with the supply team.
- `store_region` is a region, not a store. Kestrel do not receive store-level figures and the agreement says so.
- The week is a fiscal week, starting Sunday, per `docs/retail-calendar.md` §CAL-1.

## Open items

- TODO: the sensor has a long timeout and a missed week is discovered by Kestrel rather than by us.
- There is no delivery receipt. The SFTP put either happened or it did not, and the log is the only record.
- The governance tags are maintained by hand in the model metadata. Nothing verifies the derivation rule in PS-2.
