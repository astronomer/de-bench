-- The insert half of one settlement date's replace, out of the staged landing
-- rows. The source is the immutable landing copy and never a mart: a rebuild
-- that reads a downstream aggregate produces a table that agrees with itself
-- and stands for nothing.
INSERT INTO copperline.ops.payments_restored BY NAME
SELECT * FROM restore_source WHERE settlement_date = ?::DATE
