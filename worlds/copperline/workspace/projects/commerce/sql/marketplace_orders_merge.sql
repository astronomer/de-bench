-- Merge the region's stage into the order table on the order id. An amended
-- order updates in place; a new one inserts. Neither doubles on a second run.
INSERT OR REPLACE INTO copperline.raw.marketplace_orders BY NAME
SELECT * FROM query_table(?)
