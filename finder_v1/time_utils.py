from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .config import business_timezone_name


def business_tz() -> ZoneInfo:
    return ZoneInfo(business_timezone_name())


def business_now() -> datetime:
    return datetime.now(business_tz())


def today_business_date() -> str:
    return business_now().strftime("%Y-%m-%d")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def next_business_time_iso(hour: int, minute: int = 0) -> str:
    now_local = business_now()
    candidate = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= now_local:
        candidate = candidate + timedelta(days=1)
    return candidate.astimezone(timezone.utc).isoformat()


def next_interval_iso(minutes: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()
