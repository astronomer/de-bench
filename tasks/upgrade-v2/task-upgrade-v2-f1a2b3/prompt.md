The project pins an older Airflow version. The team wants to land on
Airflow 3.0. Read the current pin, work out the correct upgrade route, update
`requirements.txt` to the next version on that route, and write the full
route to `output.json` with keys `path` (an ordered list of version strings
from the current pin to the target) and `rationale` (a short explanation).
