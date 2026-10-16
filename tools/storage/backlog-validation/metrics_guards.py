"""Tie freshness to the current process as well as the measurement timestamp."""
import datetime as dt


def metrics_current(row):
    writer = row.get('writer') or {}
    service = row.get('services', {}).get('storage-writer', {})
    try:
        observed = dt.datetime.fromisoformat(row['utc'])
        measured = dt.datetime.fromisoformat(writer['time'])
        pid = int(writer['pid'])
        active = int(service['MainPID'])
    except (KeyError, TypeError, ValueError):
        return False
    return (observed.tzinfo is not None and measured.tzinfo is not None
            and -5 <= (observed - measured).total_seconds() <= 45
            and pid > 0 and pid == active and service.get('ActiveState') == 'active')
