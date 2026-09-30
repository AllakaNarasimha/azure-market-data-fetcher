from typing import Union, List, Dict, Any, Optional
import pandas as pd

class DFUtils:
    @staticmethod
    def prepare_ist_candle_dataframe(data: Union[List[Dict[str, Any]], pd.DataFrame, None], index_col: str = "timestamp", tz: str = "Asia/Kolkata") -> pd.DataFrame:
        if data is None or len(data) == 0:
            return pd.DataFrame()

        # 1. Unpack objects (e.g., FyersCandle) or dicts into a DataFrame
        if isinstance(data, list):
            first_item = data[0]
            if hasattr(first_item, "__dict__"):
                data = [vars(x) for x in data]
            elif hasattr(first_item, "_asdict"):  # namedtuple support
                data = [x._asdict() for x in data]
            df = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            df = data.copy()
        else:
            return pd.DataFrame()

        if df.empty or index_col not in df.columns:
            return df

        # 2. Coerce timestamps to numeric (safely parses strings, ints, floats)
        numeric_ts = pd.to_numeric(df[index_col], errors="coerce")
        valid_ts = numeric_ts.dropna()
        if valid_ts.empty:
            return pd.DataFrame()

        # 3. Auto-detect epoch precision using the largest valid timestamp.
        max_timestamp = valid_ts.abs().max()
        if max_timestamp >= 1e17:
            unit = "ns"
        elif max_timestamp >= 1e14:
            unit = "us"
        elif max_timestamp >= 1e11:
            unit = "ms"
        else:
            unit = "s"
        utc_dt = pd.to_datetime(numeric_ts, unit=unit, utc=True)

        # 4. Convert to target timezone and remove tz offset for clean .between_time() usage
        df[index_col] = utc_dt.dt.tz_convert(tz).dt.tz_localize(None)

        # 5. Set DatetimeIndex, drop invalid rows, and sort
        df = df.dropna(subset=[index_col]).set_index(index_col).sort_index()

        return df