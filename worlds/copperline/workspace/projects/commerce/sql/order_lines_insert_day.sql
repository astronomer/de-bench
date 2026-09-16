-- The day's line file, as the OMS cut it. The file carries exactly the table's
-- columns, so the insert is by name and a column added upstream lands unnamed
-- rather than shifting the ones after it.
INSERT INTO copperline.raw.order_lines BY NAME
SELECT * FROM read_csv(?, header = true, union_by_name = true)
