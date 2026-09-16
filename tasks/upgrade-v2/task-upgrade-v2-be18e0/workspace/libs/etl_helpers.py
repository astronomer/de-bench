"""Helpers imported by the DAG factory. Ships from a sibling repo in prod."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

from airflow.utils.dates import days_ago

_REGISTRY: dict[str, Callable[..., None]] = {}


def register(name: str) -> Callable[[Callable[..., None]], Callable[..., None]]:
    def _wrap(fn: Callable[..., None]) -> Callable[..., None]:
        _REGISTRY[name] = fn
        return fn

    return _wrap


def build_callable(handler: str) -> Callable[..., None]:
    return _REGISTRY[handler]


def default_start_date() -> datetime:
    return days_ago(2)


@register("extract_orders")
def _extract_orders(**ctx) -> None:
    _ = ctx["execution_date"]


@register("load_orders")
def _load_orders(**ctx) -> None:
    _ = ctx["yesterday_ds"]


@register("sync_inventory")
def _sync_inventory(**ctx) -> None:
    _ = ctx["next_ds"]
