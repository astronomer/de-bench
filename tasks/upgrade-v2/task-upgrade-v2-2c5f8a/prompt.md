Read `requirements.txt`. The team wants to upgrade the snowflake
provider as far as possible without bumping Airflow. Write your
recommendation to `output.json` in this shape:

    {
      "providers": {"snowflake": "<major-or-version>"},
      "notes": "<one-sentence rationale>"
    }

The version string can be a bare major or a specific release
(`"X"` or `"X.Y.Z"`); the bench grades on the major.
