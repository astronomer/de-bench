from airflow.triggers.base import BaseTrigger, TriggerEvent


class MyHttpTrigger(BaseTrigger):
    def serialize(self):
        return "triggers.my_http_trigger.MyHttpTrigger", {}

    async def run(self):
        yield TriggerEvent({"ok": True})
