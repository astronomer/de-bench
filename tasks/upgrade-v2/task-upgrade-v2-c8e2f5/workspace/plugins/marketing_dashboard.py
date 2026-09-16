"""Marketing dashboard plugin.

Adds a "Marketing" menu to the Airflow UI showing campaign KPI
summaries and pipeline-health snapshots for marketing-ops viewers.
"""

from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint
from flask_appbuilder import BaseView, expose


class CampaignOverviewView(BaseView):
    route_base = "/marketing"
    default_view = "overview"

    @expose("/overview")
    def overview(self):
        return self.render_template(
            "marketing_overview.html",
            title="Campaign overview",
        )

    @expose("/kpi-summary")
    def kpi_summary(self):
        return self.render_template(
            "kpi_summary.html",
            title="KPI summary",
        )


class PipelineStatusView(BaseView):
    route_base = "/marketing/pipelines"
    default_view = "status"

    @expose("/status")
    def status(self):
        return self.render_template(
            "pipeline_status.html",
            title="Pipeline status",
        )


marketing_bp = Blueprint(
    "marketing_dashboard",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/marketing",
)


class MarketingDashboardPlugin(AirflowPlugin):
    name = "marketing_dashboard"
    flask_blueprints = [marketing_bp]
    appbuilder_views = [
        {
            "name": "Campaign overview",
            "category": "Marketing",
            "view": CampaignOverviewView(),
        },
        {
            "name": "Pipeline status",
            "category": "Marketing",
            "view": PipelineStatusView(),
        },
    ]
