# Contract: replenishment feed

Schema: `contracts/inventory_position.yml`. Consumer C-7 in `docs/report-registry.md`. Owner: supply. Publisher: `sc_replenishment_daily`. Agreed 2023-02-27, last amended 2025-07-15.

Pushes inventory positions to Ironwood every night, so the ERP can raise purchase orders in the morning. When this is wrong, Copperline buys the wrong stock.

Columns defined by this contract and not in `docs/semantic-definitions.md`: `on_hand_units`.

## RP-1 `demand_units` is the demand signal

`demand_units` means what `docs/semantic-definitions.md` says it means: units ordered at SKU and day, with returns not deducted. Ironwood plans against ordered units, and netting returns into this column understates demand for exactly the SKUs that sell most.

Columns: `ds`, `sku`, `location_id`, `on_hand_units`, `demand_units`. `on_hand_units` is the position at the snapshot; `demand_units` is the day's ordered quantity.

## RP-2 The 06:00 cutoff

The feed is delivered by 06:00. Ironwood's planning run starts at 06:15 and takes whatever is there. A late delivery is not picked up later in the day; it is missed, and that day's purchase orders are raised from the previous position.

## RP-3 Totals reconcile against the source

The delivered total per location equals the source total per location. The final task compares them and fails when they differ. That guard is deliberate and it is the reason a grain change in the upstream mart fails this feed at run time rather than quietly delivering a multiplied number to the ERP.

Do not relax the comparison to a tolerance. A difference here means the feed and the warehouse disagree about what was in the building.

## Where the mart name lives

`config/replenishment.yml` names the mart this feed reads. The DAG assembles the table name from that file rather than referencing it, so dbt cannot see the read and a grep for the mart name does not find this consumer. `docs/lineage.md` lists it.

## Notes

- The DAG maps over open distribution centres and runs with `depends_on_past` on, because a movement processed out of order corrupts the on-hand position.
- Ironwood takes the file over SFTP and acknowledges receipt. The acknowledgement is not checked.
- Closed and seasonal locations drop out of the map automatically. A location that reopens has to be re-added to the config.

## Open items

- TODO: the reconciliation compares totals per location and not per SKU, so two SKUs that swap quantities pass.
- Nobody has written down what supply do when the 06:00 cutoff is missed. In practice they call the planners.
