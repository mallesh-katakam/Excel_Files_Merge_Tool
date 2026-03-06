# booking_date_normalizer.py
from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Optional

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# Accepts: 11-sep-2024, 11-SEP-2024, 10/aug/2023, 10/Aug/2023
_DD_MMM_YYYY_RE = re.compile(r"^\s*(\d{1,2})\s*[-/]\s*([A-Za-z]{3})\s*[-/]\s*(\d{4})\s*$")


def _to_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    
    # Handle pandas Timestamp
    try:
        import pandas as pd
        if isinstance(value, pd.Timestamp):
            return value.date()
    except ImportError:
        pass
    
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return None


def _parse_with_formats(raw: str, formats: list[str], *, kind: str) -> date:
    last_err: Optional[Exception] = None
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception as e:
            last_err = e

    supported = ", ".join(formats)
    raise ValueError(
        f"Unrecognized {kind} booking date format: {raw!r}. Supported formats: {supported}"
    ) from last_err


def _parse_dd_mmm_yyyy(raw: str) -> Optional[date]:
    m = _DD_MMM_YYYY_RE.match(raw)
    if not m:
        return None
    day_s, mon_s, year_s = m.group(1), m.group(2), m.group(3)
    mon_key = mon_s.strip().lower()
    mon = _MONTHS.get(mon_key)
    if not mon:
        raise ValueError(
            f"Unrecognized month in booking date: {raw!r}. Expected 3-letter month like 'sep', 'Aug', 'DEC'."
        )
    return date(int(year_s), int(mon), int(day_s))


def _try_pandas_datetime(value: Any) -> Optional[date]:
    """Try to parse value using pandas to_datetime for robust handling."""
    try:
        import pandas as pd
        parsed = pd.to_datetime(value, errors='coerce', dayfirst=False)
        if pd.notna(parsed):
            return parsed.date()
    except Exception:
        pass
    return None


def parse_excel_booking_date(value: Any) -> Optional[date]:
    """
    Excel Booking Date formats:
      - YYYY-MM-DD
      - DD-MM-YYYY
      - Excel serial date numbers (floats)
      - pandas Timestamp objects
      - datetime/date objects
    
    Returns None if value is empty/null.
    Raises ValueError for unrecognized non-empty formats.
    """
    if value is None:
        return None
    
    # Handle pandas NA values
    try:
        import pandas as pd
        if pd.isna(value):
            return None
    except ImportError:
        pass
    
    # Handle datetime/date/Timestamp objects first
    d = _to_date(value)
    if d is not None:
        return d
    
    # Handle numeric values (Excel serial dates) - including numpy int64/float64
    try:
        import numpy as np
        is_numeric = isinstance(value, (int, float, np.integer, np.floating))
    except ImportError:
        is_numeric = isinstance(value, (int, float))
    
    if is_numeric:
        try:
            import pandas as pd
            # Excel serial date - convert using pandas
            # Excel uses 1899-12-30 as origin (with the 1900 leap year bug)
            parsed = pd.to_datetime(value, unit='D', origin='1899-12-30', errors='coerce')
            if pd.notna(parsed):
                return parsed.date()
        except Exception:
            pass
    
    raw = str(value).strip()
    if not raw or raw.lower() in ('', 'null', 'none', 'nan', 'nat'):
        return None
    
    # Try pandas datetime parsing first (handles many formats)
    pd_result = _try_pandas_datetime(raw)
    if pd_result is not None:
        return pd_result
    
    # Excel formats required
    return _parse_with_formats(
        raw,
        formats=["%Y-%m-%d", "%d-%m-%Y"],
        kind="excel",
    )


def parse_db_booking_date(value: Any) -> Optional[date]:
    """
    DB Booking_Date formats:
      - DD-MMM-YYYY (month can be lowercase/uppercase/mixed; separator '-' or '/')
      - DD/MM/YYYY
      - DD-MM-YYYY
      - YYYY-MM-DD
      - datetime/date objects
    
    Returns None if value is empty/null.
    Raises ValueError for unrecognized non-empty formats.
    """
    if value is None:
        return None
    
    # Handle pandas NA values
    try:
        import pandas as pd
        if pd.isna(value):
            return None
    except ImportError:
        pass
    
    # Handle datetime/date/Timestamp objects first
    d = _to_date(value)
    if d is not None:
        return d

    raw = str(value).strip()
    if not raw or raw.lower() in ('', 'null', 'none', 'nan', 'nat'):
        return None

    # First handle DD-MMM-YYYY with robust month handling and '-' or '/' separators
    dd_mmm = _parse_dd_mmm_yyyy(raw)
    if dd_mmm is not None:
        return dd_mmm

    # Then numeric formats
    return _parse_with_formats(
        raw,
        formats=["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"],
        kind="db",
    )


def canonical_yyyy_mm_dd(d: Optional[date]) -> Optional[str]:
    """
    Convert a date object to canonical YYYY-MM-DD string format.
    Returns None if input is None.
    """
    if d is None:
        return None
    return d.isoformat()


def normalize_excel_booking_date(value: Any) -> Optional[str]:
    """
    Parse Excel booking date and return canonical YYYY-MM-DD string.
    Returns None if value is empty/null or parsing fails.
    """
    try:
        d = parse_excel_booking_date(value)
        return canonical_yyyy_mm_dd(d)
    except (ValueError, Exception):
        return None


def normalize_db_booking_date(value: Any) -> Optional[str]:
    """
    Parse DB booking date and return canonical YYYY-MM-DD string.
    Returns None if value is empty/null or parsing fails.
    """
    try:
        d = parse_db_booking_date(value)
        return canonical_yyyy_mm_dd(d)
    except (ValueError, Exception):
        return None


def dates_match(excel_value: Any, db_value: Any) -> bool:
    """
    Compare Excel booking date and DB booking date for equality.
    Both are normalized to YYYY-MM-DD format before comparison.
    
    Returns True if both dates are valid and equal.
    Returns False if either date is invalid/empty or they don't match.
    """
    excel_normalized = normalize_excel_booking_date(excel_value)
    db_normalized = normalize_db_booking_date(db_value)
    
    if excel_normalized is None or db_normalized is None:
        return False
    
    return excel_normalized == db_normalized
