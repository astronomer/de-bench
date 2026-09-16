CREATE TABLE IF NOT EXISTS copperline.ops.restore_log (
    ds DATE NOT NULL,
    table_name VARCHAR NOT NULL,
    reason VARCHAR
)
