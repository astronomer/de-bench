-- Rebuild one day of the payments fact from its own landing files. The source
-- is the immutable landing copy and never a mart: a rebuild that reads a
-- downstream aggregate produces a table that agrees with itself and stands for
-- nothing.
INSERT INTO copperline.marts.fct_payments BY NAME
SELECT * FROM read_json(
    'landing/meridian/dt=' || ?::VARCHAR || '/hr=*/events.jsonl',
    format = 'newline_delimited', union_by_name = true)
WHERE settlement_date = ?
