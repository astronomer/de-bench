-- One store's file. The store code and the business date come from the caller,
-- because the file carries neither: the store is in the file name and the day
-- is the partition directory.
INSERT INTO copperline.raw.pos_sales_header BY NAME
SELECT *, ? AS store_id, ?::DATE AS business_date
FROM read_csv(?, header = true, union_by_name = true)
