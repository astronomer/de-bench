"""The five kinds every team uses.

Importing this package registers all five. `render_file` imports it, so a
blueprint file never has to. A sixth kind that only one team needs belongs in
that team's `projects/<team>/dags/kinds.py`, not here.
"""

from __future__ import annotations

from . import csv_intake, dbt_select, partition_export, rollup, sensor_wait  # noqa: F401

__all__ = ["csv_intake", "rollup", "dbt_select", "partition_export", "sensor_wait"]
