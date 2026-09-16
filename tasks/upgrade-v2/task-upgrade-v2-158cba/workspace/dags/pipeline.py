from airflow.sdk import dag
from airflow.sdk.bases.operator import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent


class MyHttpTrigger(BaseTrigger):
    def serialize(self):
        return "dags.pipeline.MyHttpTrigger", {}

    async def run(self):
        yield TriggerEvent({"ok": True})


class WaitForHttp(BaseOperator):
    def execute(self, context):
        self.defer(trigger=MyHttpTrigger(), method_name="resume")

    def resume(self, context, event):
        return event


@dag(schedule=None)
def pipeline():
    WaitForHttp(task_id="wait")


pipeline()
