Read `requirements.txt`. Recommend the highest
`apache-airflow-providers-snowflake` major that installs cleanly against the
pinned Airflow version, and write your recommendation to `output.json`:

    { "providers": {"snowflake": "<major-or-version>"}, "notes": "<one-sentence rationale>" }

The version string can be a bare major or a specific release; the bench
grades on the major.
