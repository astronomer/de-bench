-- One settlement day, by currency, as the feed stood at the end of a delivery
-- date. Parameters: the settlement day, twice, then the delivery date.
--
-- The whole day is selected, not the part of it that arrived tonight:
-- `include/lib/warehouse.py` says a partition write takes the whole partition,
-- and a day rebuilt from its late rows alone is a day that has lost the rest.
--
-- `loaded_at` bounds the read at the delivery date so that a replayed night
-- reproduces the figures that night could have published.
SELECT ?::DATE                                              AS ds,
       currency_code,
       count(*)                                             AS events,
       coalesce(sum(amount_cents)
                FILTER (event_type = 'captured'), 0)        AS captured_cents,
       coalesce(sum(amount_cents)
                FILTER (event_type = 'refunded'), 0)        AS refunded_cents,
       coalesce(sum(amount_cents)
                FILTER (event_type = 'chargeback'), 0)      AS chargeback_cents
FROM copperline.raw.pay_meridian_settlements
WHERE event_time_utc >= ?::DATE
  AND event_time_utc < ?::DATE + INTERVAL 1 DAY
  AND loaded_at < ?::DATE + INTERVAL 1 DAY
GROUP BY currency_code
ORDER BY currency_code
