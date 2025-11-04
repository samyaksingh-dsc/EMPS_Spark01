import os, re, calendar, asyncio, traceback
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Tuple, Optional, Dict
import uuid, json

import chainlit as cl
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ANALYTICS_ACTIVE_WINDOW_SEC = int(os.getenv("ANALYTICS_ACTIVE_WINDOW_SEC", "120"))

# ─────────────────────────────────────────────────────────────
# Branding: custom avatar (logo)
# Place your logo at: public/avatars/emps.png
# ─────────────────────────────────────────────────────────────
ASSISTANT_AUTHOR = "EMPS_v2"

# ─────────────────────────────────────────────────────────────
# Environment & DB
# ─────────────────────────────────────────────────────────────
load_dotenv(override=True)
DB_URL = os.getenv("DATABASE_URL", "").strip()
DB_HOST = os.getenv("DB_HOST", "").strip()
DB_PORT = int(os.getenv("DB_PORT", os.getenv("PGPORT", "5432")))
DB_NAME = os.getenv("DB_NAME", "").strip()
DB_USER = os.getenv("DB_USER", "").strip()
DB_PASSWORD = os.getenv("DB_PASSWORD", "").strip()
DB_SSLMODE = os.getenv("DB_SSLMODE", "require").strip()

DEFAULT_STAT = os.getenv("DEFAULT_STAT", "twap").strip().lower()
if DEFAULT_STAT not in ("twap", "vwap", "list", "daily_avg"):
    DEFAULT_STAT = "twap"

DATE_MIN_GUARD = date(2010, 1, 1)

# print("DB PATH:", "DATABASE_URL" if DB_URL else "split fields")
# print("DATABASE_URL =", (DB_URL or "<none>"))
# print("DB_USER =", DB_USER)



def _connect():
    keepalive = dict(connect_timeout=10, keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5)
    if DB_URL:
        return psycopg2.connect(DB_URL, sslmode=DB_SSLMODE, **keepalive)
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD, sslmode=DB_SSLMODE, **keepalive
    )
# Disclaimer footer for derivative market focus
DISCLAIMER_FOOTER = """

---

**Primary Service:** MCX/NSE Derivative Market Analysis | **Complementary:** DAM/GDAM/RTM Spot Data  
*For research purposes only • Not financial advice • Consult licensed professionals*

© 2025 Energy Minds Power Solutions Pvt. Ltd.
"""

# ─────────────────────────────────────────────────────────────
# UI helpers
# ─────────────────────────────────────────────────────────────
async def progress_start(text: str = "💭 Interpreting …") -> cl.Message:
    m = cl.Message(author=ASSISTANT_AUTHOR, content=text)
    await m.send()
    return m

async def progress_update(m: cl.Message, text: str) -> None:
    try:
        await m.update(content=text)
    except Exception:
        pass

async def progress_hide(m: cl.Message) -> None:
    try:
        await m.remove()
    except Exception:
        try:
            await m.update(content="")
        except Exception:
            pass

# @cl.on_chat_start
# async def chat_start():
    
#     await cl.Message(author=ASSISTANT_AUTHOR, content="Welcome to SPARK" + "\n").send()

# ─────────────────────────────────────────────────────────────
# Parsing – deterministic pipeline
# ─────────────────────────────────────────────────────────────
MONTHS = {
    "jan":1,"january":1,"feb":2,"february":2,"mar":3,"march":3,"apr":4,"april":4,
    "may":5,"jun":6,"june":6,"jul":7,"july":7,"aug":8,"august":8,"sep":9,"sept":9,"september":9,
    "oct":10,"october":10,"nov":11,"november":11,"dec":12,"december":12
}
MONTH_WORDS = r"(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec|january|february|march|april|june|july|august|september|october|november|december)"
EXCH_RE = re.compile(r"\b(MCX|NSE)\b", re.IGNORECASE)


def normalize(text: str) -> str:
    s = text.strip()
    s = s.replace("–", "-").replace("—", "-").replace("-", "-")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\bbetween\s+(\S.*?)\s+and\s+(\S.*?)\b", r"\1 to \2", s, flags=re.I)
    s = re.sub(r"\b(upto|through|till|until)\b", "to", s, flags=re.I)
    s = re.sub(rf"\b({MONTH_WORDS})\s*[-']\s*(\d{{2}})\b", lambda m: f"{m.group(1)} 20{m.group(2)}", s, flags=re.I)
    return s

def parse_market(text: str) -> str:
    if re.search(r"\b(gdam|green day\.?\s*ahead)\b", text, re.I): return "GDAM"
    if re.search(r"\b(dam)\b", text, re.I): return "DAM"
    return "DAM"

def parse_stat(text: str) -> str:
    s = text.lower()
    if re.search(r"\b(vwap|weighted)\b", s): return "vwap"
    if re.search(r"\bdaily\s+(avg|average)\b", s): return "daily_avg"
    if re.search(r"\b(list|table|rows|detailed)\b", s): return "list"
    if re.search(r"\b(avg|average|mean|twap)\b", s): return "twap"
    return DEFAULT_STAT


def parse_multi_year_months(text: str) -> List[Tuple[date, date]]:
    """
    Robust parser for multi-year month queries.
    Handles:
    - "November 2022, November 2023, November 2024"
    - "November 2022, 2023, 2024"
    - "Nov 2022, 2023, and 2024"
    - "dam november 2022, 2023, 2024"
    - Any combination with commas and 'and'
    
    Returns list of (start_date, end_date) tuples for each month
    """
    s = text.lower().strip()
    results = []
    
    # Pattern 1: "November 2022, 2023, 2024" (month once, multiple years)
    # More precise: month followed by year, then more years separated by commas
    month_match = re.search(
        rf"\b({MONTH_WORDS})\s+(\d{{4}})\b(?:\s*,\s*(?:and\s+)?(\d{{4}}))+", 
        s, re.I
    )
    if month_match:
        month_name = month_match.group(1).lower()
        if month_name in MONTHS:
            month_num = MONTHS[month_name]
            # Get ALL years from the entire matched portion
            full_match = month_match.group(0)
            years = re.findall(r'\b\d{4}\b', full_match)
            
            if len(years) > 1:  # Found multiple years
                for year_str in years:
                    try:
                        year = int(year_str)
                        if 2000 <= year <= 2100:  # Sanity check
                            start = date(year, month_num, 1)
                            end = date(year, month_num, calendar.monthrange(year, month_num)[1])
                            results.append((start, end))
                    except (ValueError, calendar.IllegalMonthError):
                        continue  # Skip invalid dates
                
                if len(results) > 1:  # Only return if we got multiple valid periods
                    return results
    
    # Pattern 2: "November 2022, November 2023, November 2024" (month repeated)
    # More precise: only match complete "Month YYYY" patterns
    pattern = rf"\b({MONTH_WORDS})\s+(\d{{4}})\b"
    matches = re.findall(pattern, s, re.I)
    
    if len(matches) > 1:  # Multiple month-year pairs found
        seen_periods = set()  # Avoid duplicates
        
        for match in matches:
            month_name = match[0].lower()
            year_str = match[1]
            
            if month_name in MONTHS:
                try:
                    month_num = MONTHS[month_name]
                    year = int(year_str)
                    
                    if 2000 <= year <= 2100:  # Sanity check
                        period_key = (year, month_num)
                        if period_key not in seen_periods:
                            seen_periods.add(period_key)
                            start = date(year, month_num, 1)
                            end = date(year, month_num, calendar.monthrange(year, month_num)[1])
                            results.append((start, end))
                except (ValueError, calendar.IllegalMonthError):
                    continue  # Skip invalid dates
        
        if len(results) > 1:  # Only return if we got multiple valid periods
            return results
    
    return []  # Not a multi-year query or couldn't parse

def parse_date_or_range(text: str) -> Tuple[Optional[date], Optional[date]]:
    s = " " + text.lower().strip() + " "
    today = date.today()

    if " yesterday " in s:
        d = today - timedelta(days=1); return (d, d)
    if " today " in s:
        return (today, today)
    if " this month " in s:
        start = date(today.year, today.month, 1)
        end = date(today.year, today.month, calendar.monthrange(today.year, today.month)[1])
        return (start, end)
    if " last month " in s:
        y, m = today.year, today.month - 1
        if m == 0: y, m = y - 1, 12
        return (date(y, m, 1), date(y, m, calendar.monthrange(y, m)[1]))
    # Month-day to month-day range in SAME year: "24 September to 24 October 2025"
    m = re.search(
        rf"(?:from\s+)?(\d{{1,2}})\s+({MONTH_WORDS})\s+(?:to|until|till|-)\s+(\d{{1,2}})\s+({MONTH_WORDS})\s+(\d{{2,4}})",
        s, re.I
    )
    if m:
        d1 = int(m.group(1))
        mon1 = MONTHS[m.group(2).lower()]
        d2 = int(m.group(3))
        mon2 = MONTHS[m.group(4).lower()]
        yr = int(m.group(5))
        if yr < 100:
            yr += 2000
        start, end = date(yr, mon1, d1), date(yr, mon2, d2)
        if start > end:
            start, end = end, start
        return (start, end)

    # word-date RANGE (original pattern continues below)
    m = re.search(rf"\b(?:from\s*)?(\d{{1,2}})\s+{MONTH_WORDS}\s+(\d{{2,4}})\s*(?:to|-)\s*(\d{{1,2}})\s+{MONTH_WORDS}\s+(\d{{2,4}})\b", s, re.I)

    # Month-day to month-day range in SAME year: "24 September to 24 October 2025"
    m = re.search(
        rf"(?:from\s+)?(\d{{1,2}})\s+({MONTH_WORDS})\s+(?:to|until|till|-)\s+(\d{{1,2}})\s+({MONTH_WORDS})\s+(\d{{2,4}})",
        s, re.I
    )
    if m:
        d1 = int(m.group(1))
        mon1 = MONTHS[m.group(2).lower()]
        d2 = int(m.group(3))
        mon2 = MONTHS[m.group(4).lower()]
        yr = int(m.group(5))
        if yr < 100:
            yr += 2000
        start, end = date(yr, mon1, d1), date(yr, mon2, d2)
        if start > end:
            start, end = end, start
        return (start, end)

    # word-date RANGE (original pattern continues below)
    m = re.search(rf"\b(?:from\s*)?(\d{{1,2}})\s+{MONTH_WORDS}\s+(\d{{2,4}})\s*(?:to|-)\s*(\d{{1,2}})\s+{MONTH_WORDS}\s+(\d{{2,4}})\b", s, re.I)

    # word-date RANGE
    m = re.search(rf"\b(?:from\s*)?(\d{{1,2}})\s+{MONTH_WORDS}\s+(\d{{2,4}})\s*(?:to|-)\s*(\d{{1,2}})\s+{MONTH_WORDS}\s+(\d{{2,4}})\b", s, re.I)
    if m:
        d1 = int(m.group(1)); mon1 = MONTHS[m.group(2).lower()]; y1 = int(m.group(3))
        d2 = int(m.group(4)); mon2 = MONTHS[m.group(5).lower()]; y2 = int(m.group(6))
        if y1 < 100: y1 += 2000
        if y2 < 100: y2 += 2000
        start, end = date(y1, mon1, d1), date(y2, mon2, d2)
        if start > end: start, end = end, start
        return (start, end)

    # day range within SAME month (e.g., 10-15 Aug 2025)
    m = re.search(rf"\b(\d{{1,2}})\s*(?:to|-)\s*(\d{{1,2}})\s+{MONTH_WORDS}(?:\s+(\d{{2,4}}))?\b", s, re.I)
    if m:
        d1, d2 = int(m.group(1)), int(m.group(2))
        mon = MONTHS[m.group(3).lower()]
        yr  = int(m.group(4)) if m.group(4) else today.year
        if yr < 100: yr += 2000
        return (date(yr, mon, min(d1, d2)), date(yr, mon, max(d1, d2)))

    # numeric date RANGE
    m = re.search(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\s*(?:to|-)\s*(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b", s, re.I)
    if m:
        d1, m1, y1 = int(m.group(1)), int(m.group(2)), int(m.group(3))
        d2, m2, y2 = int(m.group(4)), int(m.group(5)), int(m.group(6))
        y1 = y1 + 2000 if y1 < 100 else y1
        y2 = y2 + 2000 if y2 < 100 else y2
        start, end = date(y1, m1, d1), date(y2, m2, d2)
        if start > end: start, end = end, start
        return (start, end)

    # single numeric date
    m = re.search(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b", s)
    if m:
        d0, m0, y0 = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y0 < 100: y0 += 2000
        d = date(y0, m0, d0)
        if d >= DATE_MIN_GUARD:
            return (d, d)

    # single day with month word
    m = re.search(rf"\b(\d{{1,2}})\s+{MONTH_WORDS}(?:\s+(\d{{2,4}}))?\b", s, re.I)
    if m:
        d0 = int(m.group(1)); mon0 = MONTHS[m.group(2).lower()]
        y0 = int(m.group(3)) if m.group(3) else today.year
        if y0 < 100: y0 += 2000
        d = date(y0, mon0, d0)
        if d >= DATE_MIN_GUARD:
            return (d, d)

    # month + year
    m = re.search(rf"\b{MONTH_WORDS}\s+(\d{{2,4}})\b", s, re.I)
    if m:
        mon = MONTHS[m.group(1).lower()]
        yr  = int(m.group(2));  yr = yr + 2000 if yr < 100 else yr
        last = calendar.monthrange(yr, mon)[1]
        s1, s2 = date(yr, mon, 1), date(yr, mon, last)
        if s1 >= DATE_MIN_GUARD:
            return (s1, s2)

    # year only (if phrased)
    m = re.search(r"\b(20\d{2})\b", s)
    if m and re.search(r"\b(in|for|year|full\s+year)\b", s):
        y = int(m.group(1)); return (date(y, 1, 1), date(y, 12, 31))

    return (None, None)


def _same_calendar_month(a: date, b: date) -> bool:
    return (a.year == b.year) and (a.month == b.month)

def _is_same_contract_month(target_day: date, cm: date) -> bool:
    if isinstance(cm, datetime):
        cm = cm.date()
    return cm.year == target_day.year and cm.month == target_day.month



def is_month_intent(text: str, start: Optional[date], end: Optional[date]) -> bool:
    if not start or not end:
        return False
    first = date(start.year, start.month, 1)
    last  = date(start.year, start.month, calendar.monthrange(start.year, start.month)[1])
    whole_month = (start == first and end == last)
    yyyymm = bool(re.search(r"\b20\d{2}-(0[1-9]|1[0-2])\b", text or ""))
    has_month_word = bool(re.search(MONTH_WORDS, text or "", re.I))
    return whole_month and has_month_word or yyyymm

# Ranges for hours/slots (unchanged from your current app)
def _fmt_hhmm(total_min: int) -> str:
    # show 24:00 for exact end-of-day instead of wrapping to 00:00
    if total_min == 24 * 60:
        return "24:00"
    h = (total_min // 60) % 24
    m = total_min % 60
    return f"{h:02d}:{m:02d}"

def hour_block_window(b: int) -> str:
    return f"{_fmt_hhmm((b-1)*60)}–{_fmt_hhmm(b*60)}"

def slot_window(s: int) -> str:
    return f"{_fmt_hhmm((s-1)*15)}–{_fmt_hhmm(s*15)}"

def _same_calendar_month(a: date, b: date) -> bool:
    return (a.year == b.year) and (a.month == b.month)


def _compress_ranges(indices: List[int]) -> List[Tuple[int, int]]:
    if not indices:
        return []
    b = sorted(set(indices))
    out = []
    s = p = b[0]
    for x in b[1:]:
        if x == p + 1:
            p = x
        else:
            out.append((s, p))
            s = p = x
    out.append((s, p))
    return out

@dataclass
class QuerySpec:
    market: str
    start_date: date
    end_date: date
    granularity: str            # 'hour' | 'quarter'
    hours: Optional[List[int]]
    slots: Optional[List[int]]
    stat: str                   # 'list' | 'twap' | 'vwap' | 'daily_avg'
    area: str = "ALL"


def _hour_blocks_to_slot_ranges(hranges: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """Convert hour block ranges (1–24) to slot ranges (1–96)."""
    out: List[Tuple[int,int]] = []
    for b1, b2 in hranges:
        s1 = (b1 - 1) * 4 + 1
        s2 = b2 * 4
        out.append((s1, s2))
    return out

def parse_ranges(text: str) -> dict:
    s_orig = normalize(text)
    s = s_orig.lower()

    hours: List[int] = []
    quarters: List[int] = []

    prefer_quarter = bool(re.search(r"\b(blocks?|slots?|quarters?)\b", s))
    prefer_hour    = bool(re.search(r"\b(hours?|hrs?)\b", s))

    any_minute_nonzero = False

    # “whole day”
    if re.search(r"\b(full day|all 24|entire day|whole day)\b", s):
        hours = list(range(1, 25))

    # HH[:MM][am/pm] to HH[:MM][am/pm]
    time_pat = re.compile(
        r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*(?:to|-)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b",
        re.I,
    )
    found_time_range = False

    def _to24_inner(h: int, ampm: Optional[str]) -> int:
        if ampm:
            h = h % 12
            if ampm.lower() == "pm":
                h += 12
        return max(0, min(23, h))

    for m in time_pat.finditer(s):
        found_time_range = True
        h1, m1, a1 = int(m.group(1)), int(m.group(2) or 0), m.group(3)
        h2, m2, a2 = int(m.group(4)), int(m.group(5) or 0), m.group(6)
        h2_raw = int(m.group(4))  # keep raw for “... to 24”

        if m1 > 0 or m2 > 0:
            any_minute_nonzero = True

        H1, H2 = _to24_inner(h1, a1), _to24_inner(h2, a2)

        # Hour blocks (1..24), inclusive
        start_block = min(24, H1 + 1 + (1 if m1 > 0 else 0))
        end_block   = min(24, H2 + (0 if m2 == 0 else 1))
        if m2 == 0:
            end_block = max(1, H2)
        # special case: “to 24” with no minutes or am/pm → include 23–24
        if (h2_raw == 24) and (a2 is None) and (m2 == 0):
            end_block = 24
        if end_block >= start_block:
            hours.extend(range(start_block, end_block + 1))

        # Quarter slots (1..96)
        def ceil_slot(h: int, mm: int) -> int: return (h*60 + mm + 14)//15 + 1
        def end_slot(h: int, mm: int)  -> int: return (h*60 + mm)//15
        sslot = max(1, min(96, ceil_slot(H1, m1)))
        eslot = max(1, min(96, end_slot(H2, m2)))
        if (h2_raw == 24) and (a2 is None) and (m2 == 0):
            eslot = 96
        if eslot >= sslot:
            quarters.extend(range(sslot, eslot + 1))

    # scrub numeric dates so “10-12” inside dates don’t become time ranges
    clean = re.sub(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b", " ", s)

    # “H to H hrs/hours”  (24 is allowed on the right)
    for m in re.finditer(r"\b(\d{1,2})\s*(?:to|-)\s*(\d{1,2})\s*(?:hours?|hrs?)\b", clean, re.I):
        h1 = max(0, min(23, int(m.group(1))))
        h2 = max(0, min(24, int(m.group(2))))
        start_block = min(24, h1 + 1)
        end_block   = 24 if h2 == 24 else max(1, min(24, h2))
        if end_block >= start_block:
            hours.extend(range(start_block, end_block + 1))

    # “blocks/slots A-B”
    for m in re.finditer(r"\b(\d{1,2})\s*(?:to|-)\s*(\d{1,2})\s*(?:blocks?|slots?|quarters?)\b", clean, re.I):
        a, b = int(m.group(1)), int(m.group(2))
        lo, hi = sorted((a, b))
        lo = max(1, lo); hi = min(96, hi)
        quarters.extend(range(lo, hi + 1))
        prefer_quarter = True

    # Fallback: naked “a-b” when we didn’t already capture any clock-range
    if not found_time_range:
        for m in re.finditer(r"\b(\d{1,2})\s*(?:to|-)\s*(\d{1,2})\b", clean, re.I):
            a, b = int(m.group(1)), int(m.group(2))
            lo, hi = sorted((a, b))
            if prefer_quarter or hi > 24:
                lo = max(1, lo); hi = min(96, hi)
                quarters.extend(range(lo, hi + 1))
            else:
                lo = max(1, lo); hi = min(24, hi)
                hours.extend(range(lo, hi + 1))

    hours    = sorted({h for h in hours    if 1 <= h <= 24})
    quarters = sorted({q for q in quarters if 1 <= q <= 96})

    # Decide mode
    if prefer_quarter or any_minute_nonzero:
        gran = "quarter"
    elif prefer_hour:
        gran = "hour"
    else:
        gran = "hour" if hours else "quarter"

    return {"hours": hours, "quarters": quarters, "granularity": gran}

def extract_explicit_time_groups(text: str) -> Dict[str, List[Tuple[int,int]]]:
    """
    Return explicit time groups (do not merge overlaps) and de-duplicate them.
    Prevents duplicates when the same span matches both the generic time regex
    and the '... hrs' regex.
    """
    s = normalize(text).lower()
    hours_groups_raw, slot_groups_raw = [], []

    time_pat = re.compile(
        r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*(?:to|-)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b",
        re.I,
    )

    def _to24(h, ampm):
        h = int(h)
        if ampm:
            h = h % 12
            if ampm.lower() == "pm":
                h += 12
        return max(0, min(23, h))

    # A) HH[:MM][am/pm] to HH[:MM][am/pm]
    for m in time_pat.finditer(s):
        h1, m1, a1 = int(m.group(1)), int(m.group(2) or 0), m.group(3)
        h2, m2, a2 = int(m.group(4)), int(m.group(5) or 0), m.group(6)
        H1, H2 = _to24(h1, a1), _to24(h2, a2)

        # hour blocks 1..24, inclusive
        sb = min(24, H1 + 1 + (1 if m1 > 0 else 0))
        eb = min(24, H2 + (0 if m2 == 0 else 1))
        if m2 == 0:
            eb = max(1, H2)
        # special “to 24”
        if (m.group(4).isdigit() and int(m.group(4)) == 24 and a2 is None and m2 == 0):
            eb = 24
        if eb >= sb:
            hours_groups_raw.append((sb, eb))

        # 15-min slots 1..96
        def ceil_slot(h, mm): return (h*60 + mm + 14)//15 + 1
        def end_slot(h, mm):  return (h*60 + mm)//15
        sslot = max(1, min(96, ceil_slot(H1, m1)))
        eslot = max(1, min(96, end_slot(H2, m2)))
        if (m.group(4).isdigit() and int(m.group(4)) == 24 and a2 is None and m2 == 0):
            eslot = 96
        if eslot >= sslot:
            slot_groups_raw.append((sslot, eslot))

    # B) “H to H hrs/hours”  (24 allowed on right)
    for m in re.finditer(r"\b(\d{1,2})\s*(?:to|-)\s*(\d{1,2})\s*(?:hours?|hrs?)\b", s, re.I):
        h1 = max(0, min(23, int(m.group(1))))
        h2 = max(0, min(24, int(m.group(2))))
        sb = min(24, h1 + 1)
        eb = 24 if h2 == 24 else max(1, min(24, h2))
        if eb >= sb:
            hours_groups_raw.append((sb, eb))

    # C) “blocks/slots A-B”
    for m in re.finditer(r"\b(\d{1,2})\s*(?:to|-)\s*(\d{1,2})\s*(?:blocks?|slots?|quarters?)\b", s, re.I):
        a, b = int(m.group(1)), int(m.group(2))
        lo, hi = sorted((a, b))
        lo = max(1, lo); hi = min(96, hi)
        slot_groups_raw.append((lo, hi))

    # Deduplicate while preserving order
    def dedupe(pairs: List[Tuple[int,int]]) -> List[Tuple[int,int]]:
        seen = set()
        out = []
        for a,b in pairs:
            key = (min(a,b), max(a,b))
            if key not in seen:
                seen.add(key)
                out.append(key)
        return out

        # If we already have explicit hour groups and user did not mention
    # slots/blocks/quarters, suppress slot groups entirely.
    if hours_groups_raw and not re.search(r'\b(slots?|blocks?|quarters?)\b', s):
        slot_groups_raw = []


    return {"hours": dedupe(hours_groups_raw), "slots": dedupe(slot_groups_raw)}


def canonicalize(market: str, start: Optional[date], end: Optional[date], gran: str, hours: List[int], slots: List[int], stat: str) -> Optional[QuerySpec]:
    if not start or not end:
        return None
    if start > end:
        start, end = end, start
    market = "GDAM" if market.upper()=="GDAM" else "DAM"
    gran = "hour" if gran=="hour" else "quarter"
    if gran == "hour":
        hs = sorted(set([h for h in hours if 1<=h<=24])) or list(range(1,25))
        return QuerySpec(market, start, end, "hour", hs, None, stat if stat in ("list","twap","vwap","daily_avg") else "list")
    else:
        qs = sorted(set([q for q in slots if 1<=q<=96])) or list(range(1,97))
        return QuerySpec(market, start, end, "quarter", None, qs, stat if stat in ("list","twap","vwap","daily_avg") else "list")


def dmy(d: date) -> str: return d.strftime("%d %b %Y")

def _block_to_time(b: int) -> str:
    # block 1 => 00:00, block 24 ends at 24:00
    return f"{b-1:02d}:00"

def format_duration(block_start: Optional[int], block_end: Optional[int]) -> str:
    # Full day if blocks not provided or explicitly 1..24
    if not block_start or not block_end or (block_start == 1 and block_end == 24):
        return "00:00–24:00 (24 hrs)"
    hours = max(1, block_end - block_start + 1)
    start_txt = _block_to_time(block_start)   # start of the first block
    end_txt   = f"{block_end:02d}:00"         # end of the last block
    return f"{start_txt}–{end_txt} ({hours} hrs)"


# ─────────────────────────────────────────────────────────────
# DB calls (DAM/GDAM)
# ─────────────────────────────────────────────────────────────

def fetch_hourly(market: str, ds: date, de: date, b1: Optional[int], b2: Optional[int]) -> List[Dict]:
    with _connect() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        if b1 and b2:
            cur.execute("SELECT * FROM public.rpc_get_hourly_prices_range(%s,%s,%s,%s,%s);", (market, ds, de, b1, b2))
        else:
            cur.execute("SELECT * FROM public.rpc_get_hourly_prices_range(%s,%s,%s,NULL,NULL);", (market, ds, de))
        return [dict(r) for r in cur.fetchall()]


def fetch_quarter(market: str, ds: date, de: date, s1: Optional[int], s2: Optional[int]) -> List[Dict]:
    with _connect() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        if s1 and s2:
            cur.execute("SELECT * FROM public.rpc_get_quarter_prices_range(%s,%s,%s,%s,%s);", (market, ds, de, s1, s2))
        else:
            cur.execute("SELECT * FROM public.rpc_get_quarter_prices_range(%s,%s,%s,NULL,NULL);", (market, ds, de))
        return [dict(r) for r in cur.fetchall()]

# ─────────────────────────────────────────────────────────────
# DB calls (Derivatives)
# ─────────────────────────────────────────────────────────────

def fetch_deriv_daily_fallback(target_day: date, exchange: Optional[str]) -> List[Dict]:
    """Returns daily close for nearest prior trading day (<= target_day) per exchange.
       If no rows (i.e., before Jul 2025), the caller renders N/A."""
    with _connect() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT * FROM public.rpc_deriv_daily_with_fallback(%s,%s);", (exchange, target_day))
        return [dict(r) for r in cur.fetchall()]


def fetch_deriv_month_expiry(cm_first: date, exchange: Optional[str]) -> List[Dict]:
    with _connect() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT * FROM public.rpc_deriv_expiry_for_month(%s,%s);", (exchange, cm_first))
        return [dict(r) for r in cur.fetchall()]

# ─────────────────────────────────────────────────────────────
# Math (₹/kWh)
# ─────────────────────────────────────────────────────────────

def twap_kwh(rows: List[Dict], price_key: str, minute_key: str) -> Optional[float]:
    if not rows: return None
    num = sum(float(r[price_key]) * float(r[minute_key]) for r in rows)
    den = sum(float(r[minute_key]) for r in rows)
    return None if den==0 else (num/den)/1000.0


def vwap_kwh(rows: List[Dict], price_key: str, sched_key: str, minute_key: str) -> Optional[float]:
    if not rows: return None
    weights = [float(r.get(sched_key) or 0) * float(r[minute_key]) for r in rows]
    num = sum(float(r[price_key]) * w for r, w in zip(rows, weights))
    den = sum(weights)
    if den>0: return (num/den)/1000.0
    return twap_kwh(rows, price_key, minute_key)


def money(v: Optional[float]) -> str:
    return "—" if v is None else f"₹{v:.4f}"

# ─────────────────────────────────────────────────────────────
# Presentation helpers (selection card + tables)
# ─────────────────────────────────────────────────────────────

def _label_hour_ranges(hours: List[int]) -> Tuple[str, str, int]:
    rngs = _compress_ranges(hours)
    time_parts  = [f"{_fmt_hhmm((s-1)*60)}–{_fmt_hhmm(e*60)}" for s, e in rngs]
    idx_parts   = [f"{s}–{e}" if s != e else f"{s}" for s, e in rngs]
    total_count = sum(e - s + 1 for s, e in rngs)
    return " + ".join(time_parts), ", ".join(idx_parts), total_count


def _label_slot_ranges(slots: List[int]) -> Tuple[str, str, int]:
    rngs = _compress_ranges(slots)
    time_parts  = [f"{_fmt_hhmm((s-1)*15)}–{_fmt_hhmm(e*15)}" for s, e in rngs]
    idx_parts   = [f"{s}–{e}" if s != e else f"{s}" for s, e in rngs]
    total_count = sum(e - s + 1 for s, e in rngs)
    return " + ".join(time_parts), ", ".join(idx_parts), total_count


def _primary_metric_label(stat: str) -> str:
    if stat == "vwap": return "Average price (VWAP)"
    if stat == "twap": return "Average price"
    if stat == "daily_avg": return "Daily average"
    return "Average price"


def _render_selection_card(spec, time_label: str, idx_label: str, count: int) -> str:
    # count = number of items in the selection.
    # For hours it's already "hours"; for 15-min slots convert to hours.
    if spec.granularity == "hour":
        hours_str = f"{count}"
    else:
        hrs = count * 15 / 60.0          # 15-min × count  → hours
        hours_str = f"{hrs:.2f}".rstrip("0").rstrip(".")  # 7, 7.5, 24, etc.

    metric_label = _primary_metric_label(spec.stat)
    return (
        "## Summary\n\n"
        "| **Parameter** | **Value** |\n"
        "|----------------|------------|\n"
        f"| **Market** | {spec.market} |\n"
        f"| **Period** | {dmy(spec.start_date)} to {dmy(spec.end_date)} |\n"
        f"| **Duration** | {time_label} ({hours_str} hrs) |\n"
    )


# Tables for DAM/GDAM

def rows_to_md_hour(rows: List[Dict], limit=120) -> str:
    if not rows: return "_No rows._"
    hdr="| Date | Hour (HH:MM–HH:MM) | Block | Price (₹/kWh) | Sched MW |\n|---|---|---:|---:|---:|"
    show = rows if len(rows)<=limit else rows[:60]+rows[-60:]
    lines=[hdr]
    for r in show:
        dd = r["delivery_date"] if isinstance(r["delivery_date"], str) else dmy(r["delivery_date"])
        b  = int(r["block_index"])
        price_kwh = float(r["price_avg_rs_per_mwh"])/1000.0
        lines.append(f"| {dd} | {hour_block_window(b)} | {b:>2} | {price_kwh:.4f} | {float(r['scheduled_mw_sum'] or 0):.2f} |")
    if len(rows)>limit: lines.insert(1,f"_Showing first 60 and last 60 of {len(rows)} rows (total {len(rows)})._")
    return "\n".join(lines)


def rows_to_md_quarter(rows: List[Dict], limit=120) -> str:
    if not rows: return "_No rows._"
    hdr="| Date | Slot (HH:MM–HH:MM) | Slot # | Price (₹/kWh) | Sched MW |\n|---|---|---:|---:|---:|"
    show = rows if len(rows)<=limit else rows[:60]+rows[-60:]
    lines=[hdr]
    for r in show:
        dd = r["delivery_date"] if isinstance(r["delivery_date"], str) else dmy(r["delivery_date"])
        s  = int(r["slot_index"])
        price_kwh = float(r["price_rs_per_mwh"])/1000.0
        lines.append(f"| {dd} | {slot_window(s)} | {s:>2} | {price_kwh:.4f} | {float(r['scheduled_mw'] or 0):.2f} |")
    if len(rows)>limit: lines.insert(1,f"_Showing first 60 and last 60 of {len(rows)} rows (total {len(rows)})._")
    return "\n".join(lines)

# ─────────────────────────────────────────────────────────────
# Derivatives rendering
# ─────────────────────────────────────────────────────────────

# def render_deriv_companion_for_day(requested_day: date, rows: List[Dict]) -> str:
#     """Side panel for derivatives when user asked a single day for DAM/GDAM."""
#     if not rows:
#         return f"### ⭐ **Derivative Market (MCX/NSE)** — {requested_day.strftime('%d %b %Y')}\n*Primary Service*\n\nN/A (no derivative data before Jul 2025)."

#     # Different exchanges might have different last-open days
#     used_dates = sorted({(r['used_trading_date'].date() if isinstance(r['used_trading_date'], datetime) else r['used_trading_date']) for r in rows})
#     if len(used_dates) == 1 and used_dates[0] != requested_day:
#         note = f" _(market closed; showing last close on {used_dates[0].strftime('%d %b %Y')})_"
#     elif len(used_dates) > 1:
#         note = " _(market closed; showing last close per exchange)_"
#     else:
#         note = ""
#         lines = [f"### ⭐ **Derivative Market (MCX/NSE)** — {requested_day.strftime('%d %b %Y')}\n*Primary Service*{note}\n"]
#     for r in rows:
#         td = r['trading_date']; cm = r['contract_month']
#         if isinstance(td, datetime): td = td.date()
#         if isinstance(cm, datetime): cm = cm.date()
#         tag = "" if td == requested_day else f" _(from {td.strftime('%d %b %Y')})_"
#         lines.append(f"- **{r['exchange']} • {r['commodity']} • {cm.strftime('%b %Y')}** → ₹{float(r['close_price_rs_per_mwh']):.2f}/MWh{tag}")
#     return "\n".join(lines)

def render_deriv_daily_for_contract_month(end_day: date, rows: List[Dict]) -> str:
    """Rows must already be filtered so each row's contract_month == end_day's month."""
    label = end_day.strftime("%b %Y")
    lines = [f"### ⭐ **Derivative Market — {label} contract (Last close as of {end_day.strftime('%d %b %Y')})**\n"]
    for r in rows:
        used = r['used_trading_date']
        if isinstance(used, datetime):
            used = used.date()
        price_kwh = float(r['close_price_rs_per_mwh']) / 1000.0
        lines.append(f"- **{r['exchange']} • {r['commodity']}** → ₹{price_kwh:.2f}/kWh (on {used.strftime('%d %b %Y')})")
    return "\n".join(lines)


def render_deriv_companion_for_day(requested_day: date, rows: List[Dict]) -> str:
    """Side panel for derivatives when user asked a single day for DAM/GDAM."""
    if not rows:
        return f"### **Derivative Market (MCX/NSE)** — {requested_day.strftime('%d %b %Y')}\n\nN/A (no derivative data before Jul 2025)."
    
    # Get all unique used trading dates from the results
    used_dates = sorted([
        r['used_trading_date'].date() if isinstance(r['used_trading_date'], datetime) else r['used_trading_date']
        for r in rows
    ])
    
    # Determine if market was closed and add note
    if len(used_dates) == 1 and used_dates[0] != requested_day:
        note = f"\n\n*Market closed on {requested_day.strftime('%d %b %Y')} — showing last close on {used_dates[0].strftime('%d %b %Y')}*"
    elif len(used_dates) > 1:
        note = "\n\n*Market closed — showing last close per exchange*"
    else:
        note = ""
    
    # Initialize lines BEFORE using it
    lines = [f"### **Derivative Market (MCX/NSE)** — {requested_day.strftime('%d %b %Y')}\n{note}\n"]

    # Add each derivative contract
    for r in rows:
        td = r['trading_date']
        cm = r['contract_month']
        

        # Convert to date if datetime
        if isinstance(td, datetime):
            td = td.date()
        if isinstance(cm, datetime):
            cm = cm.date()
        
        # Add tag if showing previous day's data
        tag = "" if td == requested_day else f" ({td.strftime('%d %b %Y')})"
        
        lines.append(f"- **{r['exchange']} • {r['commodity']} • {cm.strftime('%b %Y')}** → ₹{float(r['close_price_rs_per_mwh']/1000):.2f}/KWh{tag}")
    
    return "\n".join(lines)
def highlight_gdam(text: str) -> str:
    """Highlight GDAM with bold green dot emoji (cross-platform, works in tables)"""
    # This will work inside markdown tables and all markdown blocks
    return re.sub(
        r'\b(GDAM)\b',
        r'🟢 **\1**',
        text
    )

def render_deriv_expiry(cm_first: date, rows: List[Dict]) -> str:
    """Show expiry as a single last-close date, not the whole month."""
    month_label = cm_first.strftime("%b %Y")
    if not rows:
        return f"### ⭐ **Derivative Market — {month_label} (Expiry Close)**\n\n_Expiry not available yet._"

    # Pick the (max) expiry date present in rows (usually same across exchanges)
    from datetime import date as _date
    expiry_dates = []
    for r in rows:
        ed = r["expiry_date"]
        if isinstance(ed, datetime):
            ed = ed.date()
        if isinstance(ed, _date):
            expiry_dates.append(ed)
    if not expiry_dates:
        return f"### ⭐ **Derivative Market — {month_label} (Expiry Close)**\n\n_Expiry date not available._"

    last_close_day = max(expiry_dates)
    lines = [f"### ⭐ **Derivative Market — Expiry Close on {last_close_day.strftime('%d %b %Y')}**\n"]
    for r in rows:
        price_kwh = float(r['expiry_close']) / 1000.0
        lines.append(f"- **{r['exchange']} • {r['commodity']}** → ₹{price_kwh:.2f}/kWh")
    return "\n".join(lines)

def _exec(sql, params=None, fetch="none"):
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params or ())
        if fetch == "one":
            r = cur.fetchone(); return r[0] if r else None
        if fetch == "all":
            return cur.fetchall()
        return None

def analytics_start_session(session_id: str, user_agent=None, referer=None, ip=None):
    _exec("""
      insert into analytics_usage_sessions (id, user_agent, referer, ip)
      values (%s, %s, %s, %s)
      on conflict (id) do update set last_seen = now()
    """, (session_id, user_agent, referer, ip))

def analytics_touch_session(session_id: str):
    _exec("update analytics_usage_sessions set last_seen = now() where id = %s", (session_id,))

def analytics_end_session(session_id: str):
    _exec("update analytics_usage_sessions set ended_at = now(), last_seen = now() where id = %s", (session_id,))

def analytics_log_event(session_id: str, etype: str, payload: dict):
    _exec("insert into analytics_usage_events (session_id, type, payload) values (%s, %s, %s)",
          (session_id, etype, json.dumps(payload)))

def analytics_counts():
    active = _exec(
        "select count(*) from analytics_usage_sessions "
        "where last_seen > now() - make_interval(secs := %s)",
        (ANALYTICS_ACTIVE_WINDOW_SEC,), fetch="one"
    ) or 0
    today_sessions = _exec(
        "select count(*) from analytics_usage_sessions where started_at::date = current_date", fetch="one"
    ) or 0
    total_sessions = _exec("select count(*) from analytics_usage_sessions", fetch="one") or 0
    msgs_today = _exec(
        "select count(*) from analytics_usage_events where type='message' and ts::date = current_date", fetch="one"
    ) or 0
    return dict(active_now=active, today_sessions=today_sessions, total_sessions=total_sessions, messages_today=msgs_today)


@cl.on_chat_start
async def _start():
    import uuid
    sid = str(uuid.uuid4())
    cl.user_session.set("sid", sid)
    analytics_start_session(sid)


# ─────────────────────────────────────────────────────────────
# Main handler
# ─────────────────────────────────────────────────────────────

@cl.on_message
async def on_message(msg: cl.Message):
    text_raw = msg.content.strip()
    sid = cl.user_session.get("sid")
    if sid:
        analytics_touch_session(sid)
        analytics_log_event(sid, "message", {"len": len(text_raw), "text_preview": text_raw[:120]})

    # quick stats command
    if text_raw.lower() in ("/stats", "stats"):
        c = analytics_counts()
        await cl.Message(
            author=ASSISTANT_AUTHOR,
            content=(
                "## Service usage\n"
                f"- **Active now** (last {ANALYTICS_ACTIVE_WINDOW_SEC}s): **{c['active_now']}**\n"
                f"- **Today’s sessions**: **{c['today_sessions']}**\n"
                f"- **Messages today**: **{c['messages_today']}**\n"
                f"- **Total sessions (all-time)**: **{c['total_sessions']}**"
            ),
        ).send()
        return
    s_norm = normalize(text_raw)

    progress = await progress_start("💭 Interpreting …")
    await asyncio.sleep(0.10)
    await progress_update(progress, "🧮 Querying …")

    try:
        market = parse_market(s_norm)
        stat = parse_stat(s_norm)

        periods = parse_multi_year_months(s_norm)
        if not periods:
            start, end = parse_date_or_range(s_norm)
            if not start or not end:
                await progress_hide(progress)
                await cl.Message(
                    author=ASSISTANT_AUTHOR,
                    content=("I couldn't infer a date. Try `31/10/2025`, `30 Sep 2025`, `Oct 2025`, `10–15 Aug 2025`, or `yesterday`."),
                ).send()
                return
            periods = [(start, end)]

        # explicit time groups
        groups = extract_explicit_time_groups(s_norm)
        explicit_groups = []
        if groups["hours"]:
            for sb, eb in groups["hours"]:
                explicit_groups.append({"granularity": "hour", "hours": list(range(sb, eb + 1)), "slots": None})
        if groups["slots"]:
            for s1, s2 in groups["slots"]:
                explicit_groups.append({"granularity": "quarter", "hours": None, "slots": list(range(s1, s2 + 1))})

        if not explicit_groups:
            parsed = parse_ranges(s_norm)
            if parsed["granularity"] == "hour":
                explicit_groups = [{"granularity": "hour", "hours": parsed["hours"], "slots": None}]
            else:
                explicit_groups = [{"granularity": "quarter", "hours": None, "slots": parsed["quarters"]}]

        specs: List[QuerySpec] = []
        for ps, pe in periods:
            if ps and ps < DATE_MIN_GUARD:
                continue
            for g in explicit_groups:
                spec = canonicalize(market, ps, pe, g["granularity"], g.get("hours") or [], g.get("slots") or [], stat)
                if spec:
                    specs.append(spec)
                            # De-duplicate identical specs (same period + same hour/slot ranges)
                    def _spec_key(sp: QuerySpec):
                        hrs = tuple(_compress_ranges(sp.hours or []))
                        sls = tuple(_compress_ranges(sp.slots or []))
                        return (sp.market, sp.start_date, sp.end_date, sp.granularity, hrs, sls, sp.stat, sp.area)

                    uniq, seen = [], set()
                    for sp in specs:
                        k = _spec_key(sp)
                        if k not in seen:
                            seen.add(k)
                            uniq.append(sp)
                    specs = uniq


        if not specs:
            await progress_hide(progress)
            await cl.Message(author=ASSISTANT_AUTHOR, content="Couldn't build a query from your input.").send()
            return

        sections: List[str] = []
        for spec in specs:
            # Header
            if spec.granularity == "hour":
                tlabel, blabel, n = _label_hour_ranges(spec.hours)
                selection_card = _render_selection_card(spec, tlabel, blabel, n)
            else:
                tlabel, slabel, n = _label_slot_ranges(spec.slots)
                selection_card = _render_selection_card(spec, tlabel, slabel, n)

            title  = f"## Spot Market ({spec.market}) — {dmy(spec.start_date)} to {dmy(spec.end_date)}"
            header = f"{title}\n\n{selection_card}"

            # Data & fallback
            kpi, body = "", ""
            if spec.granularity == "hour":
                rows: List[Dict] = []
                for b1, b2 in _compress_ranges(spec.hours):
                    rows += fetch_hourly(spec.market, spec.start_date, spec.end_date, b1, b2)
                if rows:
                    twap = twap_kwh(rows, "price_avg_rs_per_mwh", "duration_min")
                    vwap = vwap_kwh(rows, "price_avg_rs_per_mwh", "scheduled_mw_sum", "duration_min")
                    primary_label = _primary_metric_label(spec.stat)
                    primary_value = vwap if spec.stat == "vwap" else twap
                    kpi  = f"**{primary_label}: {money(primary_value)} /kWh**\n\n"
                    body = rows_to_md_hour(rows) if spec.stat == "list" else ""
                else:
                    qrows: List[Dict] = []
                    for s1, s2 in _hour_blocks_to_slot_ranges(_compress_ranges(spec.hours)):
                        qrows += fetch_quarter(spec.market, spec.start_date, spec.end_date, s1, s2)
                    twap = twap_kwh(qrows, "price_rs_per_mwh", "duration_min")
                    vwap = vwap_kwh(qrows, "price_rs_per_mwh", "scheduled_mw", "duration_min")
                    primary_label = _primary_metric_label(spec.stat) + "*"
                    primary_value = vwap if spec.stat == "vwap" else twap
                    kpi  = f"**{primary_label}: {money(primary_value)} /kWh**  \n_Fallback via 15-min slots_\n\n"
                    body = rows_to_md_quarter(qrows) if spec.stat == "list" else ""
            else:
                qrows: List[Dict] = []
                for s1, s2 in _compress_ranges(spec.slots):
                    qrows += fetch_quarter(spec.market, spec.start_date, spec.end_date, s1, s2)
                twap = twap_kwh(qrows, "price_rs_per_mwh", "duration_min")
                vwap = vwap_kwh(qrows, "price_rs_per_mwh", "scheduled_mw", "duration_min")
                primary_label = _primary_metric_label(spec.stat)
                primary_value = vwap if spec.stat == "vwap" else twap
                kpi  = f"**{primary_label}: {money(primary_value)} /kWh**\n\n"
                body = rows_to_md_quarter(qrows) if spec.stat == "list" else ""

            deriv_block = ""

            if spec.start_date == spec.end_date:
                # Single day → last close as of that day (per exchange), DB handles fallback.
                drows = fetch_deriv_daily_fallback(spec.end_date, None)
                if drows:
                    deriv_block = "\n" + render_deriv_companion_for_day(spec.end_date, drows)

            elif _same_calendar_month(spec.start_date, spec.end_date) or is_month_intent(s_norm, spec.start_date, spec.end_date):
                # Range fully inside one month:
                # 1) Try daily close for the SAME contract month up to the end date.
                drows = fetch_deriv_daily_fallback(spec.end_date, None)

                filtered = []
                seen_ex = set()
                for r in drows:
                    if _is_same_contract_month(spec.end_date, r['contract_month']):
                        ex = r['exchange']
                        if ex not in seen_ex:
                            seen_ex.add(ex)
                            filtered.append(r)

                if filtered:
                    deriv_block = "\n" + render_deriv_daily_for_contract_month(spec.end_date, filtered)
                else:
                    # 2) If no daily rows for that contract month → show that month’s EXPIRY,
                    #    but as a single date (not 01–31) via the renderer above.
                    cm_first = date(spec.end_date.year, spec.end_date.month, 1)
                    mrows = fetch_deriv_month_expiry(cm_first, None)
                    deriv_block = "\n" + render_deriv_expiry(cm_first, mrows)

            else:
                # Cross-month ranges → last close as of end date (per exchange).
                drows = fetch_deriv_daily_fallback(spec.end_date, None)
                if drows:
                    deriv_block = "\n" + render_deriv_companion_for_day(spec.end_date, drows)


            sections.append(header + "\n" + kpi + body + deriv_block)

        await progress_hide(progress)
        final = "\n\n---\n\n".join(sections)
        final = highlight_gdam(final)
        await cl.Message(author=ASSISTANT_AUTHOR, content=final).send()

    except Exception:
        traceback.print_exc()
        try: await progress_hide(progress)
        except Exception: pass
        await cl.Message(author=ASSISTANT_AUTHOR, content="⚠️ Temporary data connection issue. Please try again.").send()
