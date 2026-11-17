"""Post-commit historical cache notifications, without extra MySQL writes."""

import datetime

from .tables import month_str, normalize_month_code


def notify_history_change(month: str | datetime.date | None) -> None:
    if isinstance(month, datetime.date):
        month = month.strftime("%Y%m")
    if not month or not normalize_month_code(month) or month == month_str():
        return
    from ..api_cache import invalidate_history

    invalidate_history(month)
