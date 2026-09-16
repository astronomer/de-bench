"""Notification helpers for the invoice pipeline."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def page_on_delay(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Emit a paging alert when invoice delivery runs late."""
    logger.warning("invoice dispatch missed its deadline: %s", [s.task_id for s in slas])
