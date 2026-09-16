-- Every event the live landing tree holds for the requested settlement window.
--
-- The tree is dated by ARRIVAL — `landing/meridian/dt=<ds>/hr=<hh>/events.jsonl`
-- is what Meridian delivered in that hour, as `payments_intake` says — so a
-- settlement date is not a directory and cannot be read out of one. The whole
-- tree is scanned once and filtered on the column that carries the settlement
-- date, which is the only way to see the events that settled inside the window
-- and arrived outside it.
--
-- Parameters: the file glob, then the two ends of the settlement window.
CREATE OR REPLACE TEMP TABLE restore_source AS
SELECT event_id,
       payment_id,
       intent_id,
       order_ref,
       processor_txn_id,
       event_type,
       amount_cents::BIGINT           AS amount_cents,
       currency_code,
       event_time_utc::TIMESTAMP      AS event_time_utc,
       loaded_at::TIMESTAMP           AS loaded_at,
       settlement_date::DATE          AS settlement_date,
       restates_event_id,
       attempt_no::INTEGER            AS attempt_no,
       deleted_at::TIMESTAMP          AS deleted_at
FROM read_json(?, format = 'newline_delimited', union_by_name = true)
WHERE settlement_date::DATE BETWEEN ?::DATE AND ?::DATE
