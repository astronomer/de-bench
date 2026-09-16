"""Marketing dashboard plugin (Airflow 3 surface).

Mounts the externally-hosted marketing dashboard into the Airflow
UI via ``external_views`` so marketing-ops viewers continue to see
the campaign overview and pipeline-status pages from the Airflow
top nav.
"""

from airflow.plugins_manager import AirflowPlugin


class MarketingDashboardPlugin(AirflowPlugin):
    name = "marketing_dashboard"
    external_views = [
        {
            "name": "Campaign overview",
            "category": "Marketing",
            "href": "https://dashboards.example.com/marketing/overview",
        },
        {
            "name": "Pipeline status",
            "category": "Marketing",
            "href": "https://dashboards.example.com/marketing/pipelines/status",
        },
    ]
