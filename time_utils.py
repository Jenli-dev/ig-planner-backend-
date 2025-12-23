# time_utils.py
from datetime import datetime, timezone


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_to_utc(ts: str) -> datetime:
    # принимает "2025-09-08T12:00:00Z" и "2025-09-08T12:00:00+00:00"
    if ts.endswith("Z"):
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    # если пришло без timezone — считаем UTC
    dt = datetime.fromisoformat(ts)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def sleep_seconds_until(dt: datetime) -> float:
    delta = (dt - now_utc()).total_seconds()
    return max(0.0, delta)
