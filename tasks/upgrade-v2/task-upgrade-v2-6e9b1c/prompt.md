Read `requirements.txt`. The team wants to land on Airflow 3.2.
Audit the pin set and write your full bump plan to `output.json`
in this shape:

    {
      "airflow": "<target version>",
      "providers": {"<name>": "<major-or-version>"},
      "rationale": "<one-sentence rationale>"
    }

Include in `providers` only the entries (if any) whose major must
change so the bumped pin set installs cleanly. Versions can be
bare majors or specific releases (`"X"` or `"X.Y.Z"`); the bench
grades on the major.
