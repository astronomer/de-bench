# FIN-431 — the FY2024 comparatives the year-in-review has to carry

Every FY2024 week in the board deck shows a blank against last year. The comp
mart has no prior year for FY2024: order detail begins on 2024-02-04, the day
FY2024 opens, and FY2023 was cut back to store-day summary long before anyone
wanted it back, so the prior-year column is zero for the whole year and the
change column divides by nothing.

`fin_comp_restate_fy2024` was written last week to fill that gap out of the
store day book, and it has been run once. Finance read the output and sent it
back with two things:

- the prior-year takings, added up over the year, come to more than the store
  day book holds for the whole of FY2023. A year of comparatives cannot be
  larger than the year it is drawn from.

Work out why and fix the build, so the next run publishes a year finance can
sign. Two rules the fix holds:

- the prior-year takings, summed over the year, are the store day book's own
  takings for the days the comparison covers, and nothing more
- every FY2024 week is compared against the week the calendar names, and every
  row says which week that is

Leave the rest of the job as it is. Same table, `marts.comp_prior_year_weekly`,
with the columns it already has; one row per fiscal week per market; takings net
of discount and before tax, in whole cents; a second run leaves one copy of the
year. Keep the DAG to the three steps it has — the repair belongs in the build,
not in a fourth task.
