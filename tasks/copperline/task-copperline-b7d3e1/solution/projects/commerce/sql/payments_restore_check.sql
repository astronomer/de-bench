-- The restored rows against the landing files they came from, settlement date
-- by settlement date. Returns one row per date that disagrees, and nothing at
-- all when the two sides match.
--
-- Both sides count on `settlement_date`, which is the grain the restore
-- replaces. Counting the files by their directory date would compare the
-- delivery to the settlement and disagree on every date in the window.
--
-- Parameters: the file glob, the two ends of the settlement window, then the
-- same two ends again for the restored table.
WITH landed AS (
    SELECT settlement_date::DATE AS settlement_date, count(*) AS events
    FROM read_json(?, format = 'newline_delimited', union_by_name = true)
    WHERE settlement_date::DATE BETWEEN ?::DATE AND ?::DATE
    GROUP BY 1
), rebuilt AS (
    SELECT settlement_date, count(*) AS events
    FROM copperline.ops.payments_restored
    WHERE settlement_date BETWEEN ?::DATE AND ?::DATE
    GROUP BY 1
)
SELECT settlement_date,
       coalesce(landed.events, 0)  AS landed_events,
       coalesce(rebuilt.events, 0) AS rebuilt_events
FROM landed FULL OUTER JOIN rebuilt USING (settlement_date)
WHERE coalesce(landed.events, 0) <> coalesce(rebuilt.events, 0)
ORDER BY settlement_date
