-- Stores whose batches have stopped arriving, with the last business date one
-- of them did arrive for. POS-1 escalates these rather than backfilling them,
-- and it says to check the market calendar first: a store in a market with
-- `feed_expected = false` sent nothing because it was shut.
--
-- `status` is the manifest's record of what happened to a batch, and the words
-- it uses are the ones the landed table holds: `ok` and `late` for a batch that
-- arrived, `missing` for one that never did. A `missing` row is not a delivery,
-- and a store with no row at all owed no batch that day.
--
-- Two things about the date this is given, and both were wrong before:
--
-- 1. The window ends at the run's business date. Reading the whole table
--    answers a re-run for an old date with the batches that have arrived since,
--    so the run comes back empty however dark the store was that night.
-- 2. A store that has never delivered a batch is not silent. It has not opened
--    yet; the store list carries it from the day the lease is signed.
WITH run AS (
    SELECT ?::DATE AS ds, CAST(? AS INTEGER) AS lookback_days
),
delivered AS (
    SELECT m.store_id, max(m.business_date) AS last_seen
    FROM copperline.raw.pos_batch_manifest m, run
    WHERE m.status IN ('ok', 'late')
      AND m.business_date <= run.ds
    GROUP BY m.store_id
),
estate AS (
    SELECT DISTINCT store_id, market_code FROM copperline.raw.stores
)
SELECT d.store_id, d.last_seen
FROM delivered d
CROSS JOIN run
JOIN estate s ON s.store_id = d.store_id
LEFT JOIN copperline.raw.market_calendar c
       ON c.market_code = s.market_code AND c.calendar_date = run.ds
WHERE coalesce(c.feed_expected, true)
  AND d.last_seen < run.ds - run.lookback_days
ORDER BY d.store_id
