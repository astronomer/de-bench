"""Deferrable trigger for the hourly orders ETL.

Lives outside the dag bundle: the triggerer process runs without any
dag bundle on sys.path, so the trigger (and anything it imports) must
be importable as a standalone package.
"""

from airflow.triggers.base import BaseTrigger, TriggerEvent


class OrdersReadyTrigger(BaseTrigger):
    """Fires once the upstream API reports the hourly extract is ready."""

    def __init__(self, region: str) -> None:
        super().__init__()
        self.region = region

    def serialize(self):
        return (
            "triggers.orders_trigger.OrdersReadyTrigger",
            {"region": self.region},
        )

    async def run(self):
        # Real code awaits the API; the bench just needs the shape.
        yield TriggerEvent({"region": self.region, "ready": True})
