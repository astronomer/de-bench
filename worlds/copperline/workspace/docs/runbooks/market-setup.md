# Runbook: setting up a market

Owner: growth, with the market-setup team. Review date 2026-02-16. Advisory, per `docs/change-management.md` §CM-3.

Opening a market means a legal entity, a billing currency, a tax regime and a launch date, plus whatever the market's own rules require. The market-setup team does that work and records the outcome. This runbook says where the record is.

## MKT-1 The system of record

The market-setup export is the system of record for a market's attributes, and it lands as `raw.market_config`. One row per market, carrying `market_code`, `entity_code`, `billing_currency`, `tax_regime`, `launched_on` and `owner`.

Read the market's attributes from that table. This runbook does not restate them and neither does any other document: a market's currency, entity and tax treatment are commercial decisions that change without a code change, and a second copy of them would go stale the first time one moved.

## What the setup covers

1. The legal entity the market bills through. `raw.entities` holds the five.
2. The billing currency, which is a commercial decision and not a geographical fact.
3. The tax regime, which follows the entity rather than the market.
4. The launch date.
5. The market calendar rows, so that the publish DAGs know which days a market trades.

Steps 1 to 4 land in `raw.market_config`. Step 5 lands in `raw.market_calendar`.

## What setting up a market does not do

It does not put the market in any mart. A market exists commercially from its launch date and appears in reporting when a model is changed to include it. Those are separate pieces of work and the second one has been skipped before.

The dbt seed `seeds/markets.csv` is the list the models build from, and it is not the same list as `raw.market_config`. The seed holds the markets that are in the marts. The table holds the markets that exist.

## Notes from the launches so far

- This export was first cut for the 2025-04-07 launch and covers the first five markets as one piece of work, including US and CA, which were already trading. Their rows carry the launch dates they actually traded from.
- The second wave was set up in FY2026 Q1, one market at a time, by whoever was free.
- Two markets were set up before their legal entity existed and were pointed at an existing entity by agreement with finance. That is normal for a cross-border launch and it is why the entity on a row is not always the entity a reader expects.

## Open items

- Nothing checks the seed against `raw.market_config`, so a market can exist for a quarter with nothing reading it.
- TODO: `owner` on the config rows is a person's name typed by hand and two of them have left.
- The market-setup team keeps a checklist in a shared doc. The link from the growth wiki has been dead since the intranet move; ask the team directly.
