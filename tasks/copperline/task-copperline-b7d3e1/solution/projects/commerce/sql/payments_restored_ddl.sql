-- The restored Meridian event record. One row per event, the fourteen columns
-- the landing files carry and nothing else: this table is what a dispute is
-- answered from, so it holds the vendor's own record rather than our reading
-- of it. `settlement_date` is the partition the restore replaces.
CREATE TABLE IF NOT EXISTS copperline.ops.payments_restored (
    event_id VARCHAR NOT NULL,
    payment_id VARCHAR,
    intent_id VARCHAR,
    order_ref VARCHAR,
    processor_txn_id VARCHAR,
    event_type VARCHAR,
    amount_cents BIGINT,
    currency_code VARCHAR,
    event_time_utc TIMESTAMP,
    loaded_at TIMESTAMP,
    settlement_date DATE,
    restates_event_id VARCHAR,
    attempt_no INTEGER,
    deleted_at TIMESTAMP
)
