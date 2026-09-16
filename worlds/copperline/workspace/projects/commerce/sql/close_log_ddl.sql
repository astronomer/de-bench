CREATE TABLE IF NOT EXISTS copperline.ops.close_log (
    ds DATE NOT NULL,
    orders BIGINT NOT NULL,
    state VARCHAR NOT NULL
)
