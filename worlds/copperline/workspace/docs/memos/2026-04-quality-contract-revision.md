# Memo: the data quality contract, revised

To: all six teams
From: Priya Raman, data platform
Date: 2026-04-13

The test suite has been failing in the same places for two quarters and the failures are not defects. They are tests written against an acceptance rule we no longer hold. This memo replaces that rule. Read QC-1 before you write another test, and QC-3 before you trust an old one.

## QC-1 The acceptance rule, by layer

Acceptance is a property of the layer, not of the column.

- **Staging (`stg_`).** Shape only. A staging model tests uniqueness of its key, non-null on the columns the source guarantees, and nothing else. A staging model never tests a business rule and never tests a value range. The source is what it is; staging says what arrived.
- **Intermediate (`int_`).** Relationships and grain. An intermediate model tests that its grain is what it claims, that its joins do not fan out, and that referenced keys exist. It does not test totals.
- **Marts.** Business rules. A mart tests the rules its consumers' contracts state: grain, required columns, the accepted values a contract pins, and any total a contract commits to. A mart test that is not traceable to a contract clause is a test of somebody's assumption.

A test that fails at the wrong layer moves; it does not get a threshold widened until it passes.

## QC-2 What is retired, and what replaces it

| Retired | Why | Replaced by |
|---|---|---|
| `not_null` on staging columns the source may omit | staging says what arrived | nothing; the mart tests what it needs |
| `accepted_values` on staging status columns | a new source status is not a defect | `accepted_values` on the mart, from the contract; where no mart publishes the column and no contract pins it, the test retires with nothing in its place |
| range tests on staging amounts | the range is a business rule | a mart test where a contract pins the range |
| `relationships` from staging to a dimension | the dimension may not have loaded yet | the same test at `int_` |
| row-count equality between a staging model and its source | breaks on every late arrival | freshness on the source, and `unique` on the key |
| `unique` on `order_ref` in the payment staging models | `order_ref` is recycled across retries | `unique` on `payment_intent_id` |
| totals tested at `int_` grain | intermediate models do not test totals | the mart test the contract asks for |
| `not_null` on `unit_cost_cents` | the column is NULL by design from FY2026 | a mart test on the valuation output |
| `not_null` on `event_time_utc` in the POS staging models | the column is NULL for the whole pre-cutover era | nothing; the mart handles the era |

## QC-3 Old tests are stale until re-derived

A test written before the date of this memo is stale until it has been re-derived from QC-1. Where this memo and a shipped test disagree, this memo governs and the test is rewritten. Do not read a passing old test as agreement with the rule above, and do not read a failing one as a defect until it has been placed at the right layer.

## Why now

The suite grew by accretion. Each test was reasonable when it was added, mostly written at whatever layer the author happened to be working in, and the estate has since collected two eras of NULL columns, a recycled key and a source that deletes rather than cancels. Nothing in the list above was wrong when it was written. All of it is wrong now.

Questions to me, or to your team's rotation.
