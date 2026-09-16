SELECT
    (SELECT count(*) FROM read_json(
        'landing/meridian/dt=' || ?::VARCHAR || '/hr=*/events.jsonl',
        format = 'newline_delimited', union_by_name = true)),
    (SELECT count(*) FROM copperline.marts.fct_payments WHERE settlement_date = ?)
