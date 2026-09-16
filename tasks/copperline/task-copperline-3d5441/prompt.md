# FIN-388 — the FY2026 Q1 freight reprice does not tie

Supply reworked `sc_shipping_cost_daily` last month for the Q1 reprice. Finance
has been through what came out and will not sign it off. Two findings.

**Days in the window that are too big.** For a handful of dates the package
count in `marts.fct_shipping_costs` is above what the carriers' own package
files hold for that date, on every carrier at once, and the same `package_id`
turns up under two `ds` values. Every one of those runs finished green. The one
finance asked about is 2026-03-08, and it is not the only one.

**A day outside the window that came back wrong.** The accrual team re-ran
2025-12-10 to answer a Pallas query. Brightline came back about seven per cent
above what the December accrual carried and Pallas about four; Merriweather came
back unchanged. The `card_version` on those rows is not the version
`docs/rate-policy.md` puts in force in December.

## What the fix has to hold

- A package is rated once, on the date the carrier bills it on, and lands in
  exactly one day's partition.
- A package rates on the card RATE-1 puts in force on **that package's own ship
  date**, whatever date the run happens on.
- Every other day comes out exactly as it does now. Most of the ninety days from
  2026-02-01 to 2026-05-01 are already right and finance has seen them.
  `docs/retention-policy.md` RET-3 makes a published partition immutable, so a
  day whose numbers do not change must not come out of a rebuild different.

## A worked example

A Brightline `ground_2day` package shipped 2025-12-10, zone 3, billable weight
6,200 g, no accessorial. It rates in the 15 kg band, and `rated_cents` for it is
**1885** — 1683 of base, zone and weight, plus 202 of fuel at that week's rate.

## After this

The affected days get rebuilt through `plat_backfill_broker` once the code is
right. The window is ninety days at about nine thousand packages each, so that
job is booked overnight.
