# Memo: what wave 2 is left with, and the calendar it runs on

To: data platform, supply-chain
From: K. Duffy, supply-chain
Date: 2026-06-15

Six answers for the funding paper. They come from the scheduler export, its run calendar and `ops.load_control`. The Tidal sheet is not one of the sources here; it is one of the findings.

## W2-1 The calendar is 251 nights and it is one year long

`legacy/autosys/calendars/retail_2026.cal` names 251 days, 2026-01-02 to 2026-12-31, with the weekends and the ten weekday holidays taken out. It covers calendar 2026 and nothing else. The estate has run since 2024-02-04 and the export says nothing about any of that, so a question about a trading day before 2026-01-02 has to be asked somewhere else.

`sc_autosys_bridge` reads it correctly now. It used to read the header comments as data, take one date off each line of six, and compare a `MM/DD/YYYY` date with a `YYYY-MM-DD` run date, so `is_due` had never once answered yes.

## W2-2 One job names a calendar we do not hold

`cpl.util.archive.logs` carries `run_calendar: retail_2025`. There is no `retail_2025` calendar under `legacy/autosys/calendars/` and no other job names one. The job also carries `status: OI` — it is on ice, and has been for as long as the export goes back. There is nothing there to move and nothing to fix. Take it off the list.

## W2-3 The sheet puts two jobs in wave 1 that never moved

`legacy/tidal/inventory_jobs.csv` marks `nightly_stock_ledger` as wave 1 against a target of `sc_stock_ledger_daily`, and `nightly_customer_master` as wave 1 against `cus_customer_master_daily`. Neither DAG exists in this repository. Both boxes are still defined in `copperline.jil` — the nine that wave 1 took out are `delete_job` lines and these two are not among them — and both still write a row into `ops.load_control` every night after the 2026-01-05 cutover, which is exactly what the nine that did move stop doing on that date.

## W2-4 The sheet also lists work that is not anywhere

Three rows name jobs no other record has: `nightly_edi_852`, `cpl.stage.erp.costing` and `nwv_planogram_feed`. They are in no JIL, in no `ops.load_control` row and in no DAG folder, ours or Northwave's. Treat them as gone.

## W2-5 Thirteen jobs, waves 2 and 3 between them

Twenty-two nightly Pentaho jobs, nine moved in wave 1, thirteen still running. The runbook says thirteen, `copperline.jil` still defines thirteen `nightly_*` boxes, and `ops.load_control` carries twenty-two job names a night to 2026-01-04 and thirteen a night from 2026-01-05. W2-3 does not add to that count: both of those jobs were inside the thirteen already.

## W2-6 The warehouse already holds the same calendar

`raw.market_calendar` at `market_code = 'US'` with `is_trading_day` true gives 251 days for 2026, and they are the same 251 days the export names. The two agree exactly over the year they share, and the table also covers the years the export was never taken for. Anything that needs a trading day outside 2026 should read the table. Keep the export for what it is: the record of what the boxes themselves obey.
