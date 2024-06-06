from dataclasses import dataclass
from datetime import time, datetime, timedelta


@dataclass(frozen=True)
class TimeInterval:
    from_time: time
    span: timedelta

    def covers(self, dt: datetime) -> bool:
        from_datetime: datetime = datetime.combine(dt, self.from_time)
        return from_datetime <= dt < from_datetime + self.span
