-- One row per Meridian settlement event, with the order behind it. PAY-233.
--
-- `ds` is the event's own date, so the day a run owns is the day the events
-- happened and not the day they were delivered. The write is scoped to it.
CREATE TABLE IF NOT EXISTS copperline.marts.settlement_attribution (
    ds DATE NOT NULL,
    event_id VARCHAR NOT NULL,
    intent_id VARCHAR NOT NULL,
    order_id VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    amount_cents BIGINT NOT NULL
)
