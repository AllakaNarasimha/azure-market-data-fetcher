from datetime import datetime
import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay


class MarketCalendar:
    """Manages market trading days, weekends, and specific exchange holidays.

    Used for properly aligning lookback windows to actual trading sessions.
    """

    NSE_HOLIDAYS = [
        "2026-01-15",
        "2026-01-26",
        "2026-03-03",
        "2026-03-26",
        "2026-03-31",
        "2026-04-03",
        "2026-04-14",
        "2026-05-01",
        "2026-05-28",
        "2026-06-26",
        "2026-09-14",
        "2026-10-02",
        "2026-10-20",
        "2026-11-10",
        "2026-11-24",
        "2026-12-25",
    ]

    DEFAULT_BDAY = CustomBusinessDay(holidays=NSE_HOLIDAYS)

    @staticmethod
    def get_trading_days_back(
        reference_date: datetime, days_back: int, holidays: list | None = None, max_calendar_span_days: int = 90
    ) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        ref_ts = pd.Timestamp(reference_date).normalize()
        if int(days_back) == 0:
            return [(ref_ts, ref_ts)]

        # Build a descending list of trading dates (newest -> oldest)
        trading_dates: list[pd.Timestamp] = []
        d = ref_ts
        holidays_set = set(holidays) if holidays is not None else None
        while len(trading_dates) < int(days_back):
            is_hol = MarketCalendar.is_holiday(d) if holidays is None else (d.strftime("%Y-%m-%d") in holidays_set)
            if not is_hol:
                trading_dates.append(pd.Timestamp(d))
            d = d - pd.Timedelta(days=1)

        # Chunk trading dates into segments where calendar span <= max_calendar_span_days
        segments: list[tuple[pd.Timestamp, pd.Timestamp]] = []
        seg: list[pd.Timestamp] = []  # holds dates newest->oldest for current segment
        for dt in trading_dates:
            if not seg:
                seg = [dt]
                continue
            # tentative earliest if we add dt
            tentative_earliest = dt
            tentative_span = (seg[0] - tentative_earliest).days + 1
            if tentative_span <= max_calendar_span_days:
                seg.append(dt)
            else:
                # close current segment (oldest, newest)
                segments.append((seg[-1], seg[0]))
                seg = [dt]

        if seg:
            segments.append((seg[-1], seg[0]))

        return segments

    @staticmethod
    def is_holiday(check_date: datetime) -> bool:
        """True if check_date is a weekend or a known NSE holiday."""
        if check_date.weekday() >= 5:
            return True
        return check_date.strftime("%Y-%m-%d") in MarketCalendar.NSE_HOLIDAYS
