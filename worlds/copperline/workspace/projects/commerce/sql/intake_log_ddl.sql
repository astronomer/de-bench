-- One row per source per day: what landed, and how much of it.
CREATE TABLE IF NOT EXISTS copperline.ops.intake_log (
    ds DATE NOT NULL,
    source VARCHAR NOT NULL,
    row_count BIGINT NOT NULL
)
