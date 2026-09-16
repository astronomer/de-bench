from airflow.sdk import dag
from airflow.sdk.bases.operator import BaseOperator

from triggers.my_http_trigger import MyHttpTrigger


class WaitForHttp(BaseOperator):
    def execute(self, context):
        self.defer(trigger=MyHttpTrigger(), method_name="resume")

    def resume(self, context, event):
        return event


@dag(schedule=None)
def pipeline():
    WaitForHttp(task_id="wait")


pipeline()
