"""Helpers imported by the DAG factory. Ships from a sibling repo in prod."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta

import pendulum

_REGISTRY: dict[str, Callable[..., None]] = {}


def register(name: str) -> Callable[[Callable[..., None]], Callable[..., None]]:
    def _wrap(fn: Callable[..., None]) -> Callable[..., None]:
        _REGISTRY[name] = fn
        return fn

    return _wrap


def build_callable(handler: str) -> Callable[..., None]:
    return _REGISTRY[handler]


def default_start_date() -> datetime:
    return pendulum.now("UTC").subtract(days=2).replace(hour=0, minute=0, second=0, microsecond=0)


@register("extract_orders")
def _extract_orders(**ctx) -> None:
    _ = ctx["data_interval_start"]


@register("load_orders")
def _load_orders(**ctx) -> None:
    _ = ctx["data_interval_start"] - timedelta(days=1)


@register("sync_inventory")
def _sync_inventory(**ctx) -> None:
    _ = ctx["data_interval_end"]
