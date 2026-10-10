"""Employee Attendance & Analytics API  (FastAPI + MongoDB).

Single-file application, see openapi.yaml (contract + rules R1-R10) and DATA_MODEL.md (stored data).

Layout of this file
  1. configuration / database handle / indexes
  2. pure business-rule helpers (R1-R5, R7, R8)  -- stdlib only, no I/O
  3. request validation types and models
  4. serialisation (BSON <-> API)
  5. query / pipeline builders (shared by the real endpoints AND /admin/explain)
  6. endpoints
"""
import calendar
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from datetime import date as date_cls
from datetime import datetime, time as time_cls, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from pydantic import AfterValidator, BaseModel, Field, model_validator
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

load_dotenv()  # override=False (default): real environment variables win over .env

log = logging.getLogger("attendance")

# ============================================================================
# 1. configuration, database handle, indexes
# ============================================================================


class _State:
    client: Optional[MongoClient] = None
    db: Any = None
    indexes_ready: bool = False


STATE = _State()

# Index names are fixed so that create_index is idempotent and `hint` can refer to them.
IX_EMP_CODE = "uq_emp_code"
IX_EMP_DEPT_CODE = "dept_emp_code"
IX_EMP_DEPT_JOINED = "dept_joined_on"
IX_LOG_EMP_DATE = "uq_emp_date"
IX_LOG_DATE_EMP = "date_desc_emp"
IX_LOG_STATUS_DATE = "status_date_emp"
IX_LOG_EMP_PUNCH = "emp_punch_in"


def employees():
    return STATE.db["employees"]


def logs():
    return STATE.db["attendance_logs"]


def ensure_indexes() -> None:
    """Idempotent: same name + same spec is a no-op on every start."""
    e, l = employees(), logs()
    # employees
    e.create_index([("emp_code", 1)], unique=True, name=IX_EMP_CODE)  # identity, duplicate guard, sort for list
    e.create_index([("department", 1), ("emp_code", 1)], name=IX_EMP_DEPT_CODE)  # list by department, sorted
    e.create_index([("department", 1), ("joined_on", 1)], name=IX_EMP_DEPT_JOINED)  # headcount / analytics driver
    # attendance_logs
    l.create_index([("emp_code", 1), ("date", 1)], unique=True, name=IX_LOG_EMP_DATE)  # one record per day (race guard)
    l.create_index([("date", -1), ("emp_code", 1)], name=IX_LOG_DATE_EMP)  # default list order + month ranges
    l.create_index([("status", 1), ("date", -1), ("emp_code", 1)], name=IX_LOG_STATUS_DATE)  # list filtered by status
    l.create_index([("emp_code", 1), ("punch_in", -1)], name=IX_LOG_EMP_PUNCH)  # punch-out: latest punch_in <= t
    STATE.indexes_ready = True


@asynccontextmanager
async def lifespan(_: FastAPI):
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    name = os.getenv("MONGO_DB", "attendance_db")
    # tz_aware=True: every datetime read from MongoDB is an aware UTC datetime (never naive).
    STATE.client = MongoClient(
        uri, tz_aware=True, tzinfo=timezone.utc, maxPoolSize=100, serverSelectionTimeoutMS=3000
    )
    STATE.db = STATE.client[name]
    for attempt in range(1, 4):  # database may still be starting; /health stays 503 until it answers
        try:
            ensure_indexes()
            break
        except PyMongoError as exc:
            log.warning("index creation failed (attempt %d): %s", attempt, exc)
            time.sleep(2)
    yield
    STATE.client.close()


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)

# ============================================================================
# 2. pure business-rule helpers  (stdlib only)
# ============================================================================
# --- BEGIN PURE HELPERS ---

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
GRACE_SECONDS = 10 * 60  # R2
PRESENCE = ("PRESENT", "WFH", "ON_DUTY")  # R6
NO_PUNCH_STATUSES = ("ABSENT", "LEAVE")
ONE_SECOND = timedelta(seconds=1)
ONE_MS = timedelta(milliseconds=1)


def ms_to_dt(ms: int) -> datetime:
    """Epoch ms -> aware UTC datetime, truncated to whole seconds (R1)."""
    return EPOCH + timedelta(milliseconds=ms - ms % 1000)


def aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is not None and dt.tzinfo is None:  # defensive: naive datetimes from MongoDB are UTC
        return dt.replace(tzinfo=UTC)
    return dt


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    """Aware/naive-UTC datetime -> integer epoch ms, using integer arithmetic only."""
    if dt is None:
        return None
    return (aware(dt) - EPOCH) // ONE_MS


def now_seconds() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def now_millis() -> datetime:
    n = datetime.now(UTC)
    return n.replace(microsecond=n.microsecond // 1000 * 1000)  # BSON dates have ms precision


def parse_hhmm(value: str) -> time_cls:
    hh, mm = value.split(":")
    return time_cls(int(hh), int(mm))


def at_ist(day: str, hhmm: str) -> datetime:
    """The instant at which wall-clock `hhmm` IST occurs on calendar `day`."""
    d = date_cls.fromisoformat(day)
    t = parse_hhmm(hhmm)
    return datetime(d.year, d.month, d.day, t.hour, t.minute, tzinfo=IST)


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return shift_end <= shift_start  # zero-padded HH:MM compares correctly as text


def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> str:
    """R1: IST calendar date of the punch-in; overnight shifts that began 'yesterday' belong to yesterday."""
    ist = punch_in.astimezone(IST)
    day = ist.date()
    if is_overnight(shift_start, shift_end) and ist.time() < parse_hhmm(shift_end):
        day -= timedelta(days=1)
    return day.isoformat()


def late_minutes_for(punch_in: datetime, day: str, shift_start: str) -> int:
    """R2: late only if strictly more than 10:00 after shift start; then floor-minutes since SHIFT START."""
    delta = (punch_in - at_ist(day, shift_start)) // ONE_SECOND
    return delta // 60 if delta > GRACE_SECONDS else 0


def overtime_minutes_for(punch_out: datetime, day: str, shift_start: str, shift_end: str) -> int:
    """R3: floor-minutes from shift end to punch-out, only if >= 30. Overnight shifts end on the next day."""
    end = at_ist(day, shift_end)
    if is_overnight(shift_start, shift_end):
        end += timedelta(days=1)
    minutes = ((punch_out - end) // ONE_SECOND) // 60
    return minutes if minutes >= 30 else 0


def work_hours_for(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600 rounded to 2 decimals HALF-UP (exact Decimal arithmetic, not float round())."""
    seconds = (punch_out - punch_in) // ONE_SECOND
    return float((Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def derive(status: str, punch_in: Optional[datetime], punch_out: Optional[datetime],
           day: str, shift_start: str, shift_end: str) -> dict:
    """All derived fields for a record (DATA_MODEL.md, 'For a record your own code writes')."""
    if status not in PRESENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    late = late_minutes_for(punch_in, day, shift_start)
    if punch_out is None:
        return {"work_hours": None, "late_minutes": late, "overtime_minutes": 0, "half_day": False}
    hours = work_hours_for(punch_in, punch_out)
    return {
        "work_hours": hours,
        "late_minutes": late,
        "overtime_minutes": overtime_minutes_for(punch_out, day, shift_start, shift_end),
        "half_day": hours < 4.5,  # R5: on the ROUNDED value
    }


def month_bounds(month: str) -> tuple:
    year, mon = int(month[:4]), int(month[5:7])
    last = calendar.monthrange(year, mon)[1]
    return f"{month}-01", f"{month}-{last:02d}"


def count_working_days(month: str, joined_on: str) -> int:
    """R7: Mon-Fri of the month, counted from joined_on for mid-month joiners (0 if not yet joined)."""
    first, last = month_bounds(month)
    start = max(first, joined_on)  # ISO dates compare correctly as text
    if start > last:
        return 0
    d, end, n = date_cls.fromisoformat(start), date_cls.fromisoformat(last), 0
    while d <= end:
        if d.weekday() < 5:
            n += 1
        d += timedelta(days=1)
    return n


def attendance_pct(present_days: float, working_days: int) -> Optional[float]:
    """present/working*100, 2 decimals half-up, in exact integer arithmetic (present_days is a multiple of 0.5)."""
    if working_days <= 0:
        return None
    p2 = int(round(present_days * 2))
    return ((10000 * p2 + working_days) // (2 * working_days)) / 100


# --- END PURE HELPERS ---

# ============================================================================
# 3. validation types and request models
# ============================================================================

HHMM = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"
MONTH_PATTERN = r"^[0-9]{4}-(0[1-9]|1[0-2])$"
_DATE_SHAPE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


def _check_date(value: str) -> str:
    if not _DATE_SHAPE.match(value):
        raise ValueError("must be a date in YYYY-MM-DD format")
    try:
        date_cls.fromisoformat(value)
    except ValueError:
        raise ValueError("not a valid calendar date")
    return value


DateStr = Annotated[str, AfterValidator(_check_date)]
# Epoch milliseconds: strict int (floats/strings/bools rejected), seconds-looking values rejected by the lower bound.
EpochMs = Annotated[int, Field(strict=True, ge=100_000_000_000, le=4_102_444_800_000)]
StatusT = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatusT = Literal["PRESENT", "WFH", "ON_DUTY"]
PageQ = Annotated[int, Query(ge=1)]
PageSizeQ = Annotated[int, Query(ge=1, le=100)]


def unprocessable(loc: tuple, msg: str):
    """422 in FastAPI's standard validation-error shape (detail is a list)."""
    raise RequestValidationError([{"type": "value_error", "loc": loc, "msg": msg, "input": None}])


class EmployeeCreate(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=HHMM)
    shift_end: str = Field(default="18:30", pattern=HHMM)
    joined_on: DateStr

    @model_validator(mode="after")
    def _shift_differs(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None
    status: PresenceStatusT = "PRESENT"

    @model_validator(mode="before")
    @classmethod
    def _no_null_status(cls, data):
        if isinstance(data, dict) and "status" in data and data["status"] is None:
            raise ValueError("status must not be null")
        return data


class PunchOutRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None


class RegularizeRequest(BaseModel):
    status: Optional[StatusT] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @model_validator(mode="before")
    @classmethod
    def _no_nulls(cls, data):
        if isinstance(data, dict):
            for key in ("status", "punch_in", "punch_out"):
                if key in data and data[key] is None:
                    raise ValueError(f"{key} must not be null (omit it to leave it unchanged)")
        return data


# ============================================================================
# 4. serialisation
# ============================================================================


def serialize_employee(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start", "09:30"),
        "shift_end": doc.get("shift_end", "18:30"),
        "joined_on": doc.get("joined_on"),
        "created_at": dt_to_ms(doc.get("created_at")),
    }


def _serialize_history(entry: dict) -> dict:
    changes = {}
    for key, change in (entry.get("changes") or {}).items():
        if key in ("punch_in", "punch_out"):  # BSON dates inside the nested object -> epoch ms
            changes[key] = {"from": dt_to_ms(change.get("from")), "to": dt_to_ms(change.get("to"))}
        else:
            changes[key] = {"from": change.get("from"), "to": change.get("to")}
    return {"at": dt_to_ms(entry.get("at")), "by": entry.get("by"), "reason": entry.get("reason"), "changes": changes}


def serialize_record(doc: dict) -> dict:
    """Missing legacy fields: late/overtime -> 0, half_day -> false, history -> []."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
        "history": [_serialize_history(h) for h in (doc.get("history") or [])],
    }


def page_of(items: list, total: int, page: int, page_size: int) -> dict:
    return {"items": items, "total": total, "page": page, "page_size": page_size}


def shift_of(emp: dict) -> tuple:
    return emp.get("shift_start", "09:30"), emp.get("shift_end", "18:30")


# ============================================================================
# 5. query / pipeline builders  (the endpoints and /admin/explain both call these)
# ============================================================================

# --- reusable aggregation expressions over a stored attendance_logs document ---
_PRESENT = {"$in": ["$status", list(PRESENCE)]}
_LATE = {"$ifNull": ["$late_minutes", 0]}
_OVERTIME = {"$ifNull": ["$overtime_minutes", 0]}
_WEIGHT = {"$cond": [{"$ifNull": ["$half_day", False]}, 0.5, 1]}  # R5: a half day counts 0.5
_DOW = {"$dayOfWeek": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d", "onError": None}}}  # 1=Sun..7=Sat
_WEEKDAY = {"$and": [{"$gte": [_DOW, 2]}, {"$lte": [_DOW, 6]}]}
_PRESENT_DAYS = {"$cond": [{"$and": [_PRESENT, _WEEKDAY]}, _WEIGHT, 0]}  # R6 + R7 (Mon-Fri only)
_IS_LATE = {"$gt": [_LATE, 0]}
_HAS_HOURS = {"$and": [_PRESENT, {"$ne": [{"$ifNull": ["$work_hours", None]}, None]}]}


def _count_if(cond: dict) -> dict:
    return {"$sum": {"$cond": [cond, 1, 0]}}


def attendance_query(emp_code, date_from, date_to, status, page, page_size):
    if date_from is not None and date_to is not None and date_from > date_to:
        unprocessable(("query", "date_from"), "date_from must not be after date_to")
    flt: dict = {}
    if emp_code is not None:
        flt["emp_code"] = emp_code
    if date_from is not None or date_to is not None:
        rng = {}
        if date_from is not None:
            rng["$gte"] = date_from
        if date_to is not None:
            rng["$lte"] = date_to
        flt["date"] = rng
    if status is not None:
        flt["status"] = status
    sort = [("date", -1), ("emp_code", 1)]
    return flt, sort, (page - 1) * page_size, page_size


def monthly_pipeline(emp_code: str, month: str) -> list:
    first, last = month_bounds(month)
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first, "$lte": last}}},
        {"$group": {
            "_id": None,
            "present_days": {"$sum": _PRESENT_DAYS},
            "leave_days": _count_if({"$eq": ["$status", "LEAVE"]}),
            "late_count": _count_if(_IS_LATE),
            "total_late_minutes": {"$sum": _LATE},          # weekend records still count (R7)
            "total_overtime_minutes": {"$sum": _OVERTIME},
        }},
    ]


def department_summary_pipeline(month: str, department: Optional[str]) -> list:
    """Driven from `employees` so people with zero logs are counted (R9); logs are joined per employee."""
    first, last = month_bounds(month)
    emp_match: dict = {"joined_on": {"$lte": last}}  # R9: joined on or before the end of the month
    if department is not None:
        emp_match["department"] = department
    return [
        {"$match": emp_match},
        {"$lookup": {
            "from": "attendance_logs",
            "let": {"code": "$emp_code"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$emp_code", "$$code"]}, "date": {"$gte": first, "$lte": last}}},
                {"$group": {
                    "_id": None,
                    "present_days": {"$sum": _PRESENT_DAYS},
                    # work hours kept as integer hundredths so the mean can be rounded half-up exactly
                    "work_cents": {"$sum": {"$cond": [_HAS_HOURS, {"$round": [{"$multiply": ["$work_hours", 100]}, 0]}, 0]}},
                    "work_n": _count_if(_HAS_HOURS),
                    "late_count": _count_if(_IS_LATE),
                    "late_total": {"$sum": {"$cond": [_IS_LATE, _LATE, 0]}},
                    "leave_count": _count_if({"$eq": ["$status", "LEAVE"]}),
                    "on_duty_count": _count_if({"$eq": ["$status", "ON_DUTY"]}),
                }},
            ],
            "as": "agg",
        }},
        {"$addFields": {"a": {"$ifNull": [{"$first": "$agg"}, {}]}}},
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "present_days": {"$sum": {"$ifNull": ["$a.present_days", 0]}},
            "work_cents": {"$sum": {"$ifNull": ["$a.work_cents", 0]}},
            "work_n": {"$sum": {"$ifNull": ["$a.work_n", 0]}},
            "late_count": {"$sum": {"$ifNull": ["$a.late_count", 0]}},
            "total_late_minutes": {"$sum": {"$ifNull": ["$a.late_total", 0]}},
            "leave_count": {"$sum": {"$ifNull": ["$a.leave_count", 0]}},
            "on_duty_count": {"$sum": {"$ifNull": ["$a.on_duty_count", 0]}},
        }},
        {"$project": {
            "_id": 0,
            "department": "$_id",
            "headcount": 1,
            "present_days": 1,
            # mean over RECORDS: sum / n, half-up to 2 decimals == floor((2*sum_cents + n) / (2n)) / 100
            "avg_work_hours": {"$cond": [
                {"$gt": ["$work_n", 0]},
                {"$divide": [
                    {"$floor": {"$divide": [{"$add": [{"$multiply": ["$work_cents", 2]}, "$work_n"]},
                                            {"$multiply": ["$work_n", 2]}]}},
                    100]},
                None]},
            "late_count": 1,
            "total_late_minutes": 1,
            "leave_count": 1,
            "on_duty_count": 1,
        }},
        {"$sort": {"department": 1}},
    ]


def leaderboard_pipeline(month: str, limit: int, department: Optional[str]) -> list:
    first, last = month_bounds(month)
    pipeline: list = [
        {"$match": {"date": {"$gte": first, "$lte": last}}},
        {"$group": {
            "_id": "$emp_code",
            "total": {"$sum": {"$cond": [_IS_LATE, _LATE, 0]}},
            "late_count": _count_if(_IS_LATE),
        }},
        {"$match": {"total": {"$gt": 0}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # logs whose employee does not exist are dropped here
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})  # rank inside the department only
    pipeline += [
        # $rank = standard competition ranking: ties share a rank, the next rank is skipped (1, 2, 2, 4)
        {"$setWindowFields": {"sortBy": {"total": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies AFTER ranking
        {"$sort": {"total": -1, "_id": 1}},
        {"$project": {
            "_id": 0, "rank": 1, "emp_code": "$_id", "name": "$emp.name", "department": "$emp.department",
            "total_late_minutes": "$total", "late_count": 1,
        }},
    ]
    return pipeline


def trend_pipeline(department: str, d_from: date_cls, d_to: date_cls) -> list:
    """One row per calendar day, generated inside the database ($range + $dateAdd), with a $setWindowFields
    moving average. Rates are carried as integer ten-thousandths so half-up rounding is exact."""
    f, t = d_from.isoformat(), d_to.isoformat()
    n_days = (d_to - d_from).days + 1
    start = datetime(d_from.year, d_from.month, d_from.day, tzinfo=UTC)
    return [
        {"$match": {"department": department}},
        {"$group": {"_id": None, "joined": {"$push": "$joined_on"}, "codes": {"$push": "$emp_code"}}},
        {"$lookup": {
            "from": "attendance_logs",
            "let": {"codes": "$codes"},
            "pipeline": [
                {"$match": {"date": {"$gte": f, "$lte": t}, "$expr": {"$in": ["$emp_code", "$$codes"]}}},
                {"$group": {
                    "_id": "$date",
                    "present": {"$sum": {"$cond": [_PRESENT, _WEIGHT, 0]}},  # any weekday or weekend
                    "late": _count_if(_IS_LATE),
                }},
            ],
            "as": "daily",
        }},
        {"$project": {"_id": 0, "joined": 1, "daily": 1, "days": {"$map": {
            "input": {"$range": [0, n_days]}, "as": "i",
            "in": {"$dateAdd": {"startDate": start, "unit": "day", "amount": "$$i"}}}}}},
        {"$unwind": "$days"},  # exactly one document per calendar day of the range
        {"$project": {"joined": 1, "daily": 1, "dow": {"$dayOfWeek": "$days"},
                      "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$days"}}}},
        {"$project": {"joined": 1, "dow": 1, "date": 1,
                      "rec": {"$first": {"$filter": {"input": "$daily", "as": "d",
                                                     "cond": {"$eq": ["$$d._id", "$date"]}}}}}},
        {"$project": {
            "date": 1,
            "is_working_day": {"$and": [{"$gte": ["$dow", 2]}, {"$lte": ["$dow", 6]}]},
            "headcount": {"$size": {"$filter": {"input": "$joined", "as": "j", "cond": {"$lte": ["$$j", "$date"]}}}},  # R9
            "present_count": {"$ifNull": ["$rec.present", 0]},
            "late_count": {"$ifNull": ["$rec.late", 0]},
        }},
        # r4 = round_half_up(present / headcount * 10000) = floor((20000*present + headcount) / (2*headcount))
        {"$addFields": {"r4": {"$cond": [
            {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
            {"$floor": {"$divide": [{"$add": [{"$multiply": ["$present_count", 20000]}, "$headcount"]},
                                    {"$multiply": ["$headcount", 2]}]}},
            None]}}},
        {"$addFields": {"r4n": {"$cond": [{"$eq": ["$r4", None]}, 0, 1]}}},
        {"$setWindowFields": {
            "sortBy": {"date": 1},
            "output": {  # current row + up to 6 preceding rows, all inside the requested range
                "win_sum": {"$sum": "$r4", "window": {"documents": [-6, "current"]}},
                "win_n": {"$sum": "$r4n", "window": {"documents": [-6, "current"]}},
            }}},
        {"$project": {
            "_id": 0, "date": 1, "is_working_day": 1, "headcount": 1, "present_count": 1, "late_count": 1,
            "attendance_rate": {"$cond": [{"$eq": ["$r4", None]}, None, {"$divide": ["$r4", 10000]}]},
            "moving_avg_7d": {"$cond": [
                {"$gt": ["$win_n", 0]},
                {"$divide": [{"$floor": {"$divide": [{"$add": [{"$multiply": ["$win_sum", 2]}, "$win_n"]},
                                                     {"$multiply": ["$win_n", 2]}]}}, 10000]},
                None]},
        }},
        {"$sort": {"date": 1}},
    ]


def trend_range(d_from: str, d_to: str) -> tuple:
    a, b = date_cls.fromisoformat(d_from), date_cls.fromisoformat(d_to)
    if b < a:
        unprocessable(("query", "to"), "'to' must not be before 'from'")
    if (b - a).days + 1 > 92:
        unprocessable(("query", "to"), "the range must not exceed 92 days")
    return a, b


# ============================================================================
# 6. endpoints
# ============================================================================


@app.get("/health")
def health():
    try:
        STATE.db.command("ping")
        if not STATE.indexes_ready:
            ensure_indexes()
    except Exception:
        raise HTTPException(status_code=503, detail="database unavailable")
    return {"status": "ok"}


# ---------------------------------------------------------------- employees

@app.post("/employees", status_code=201)
def create_employee(body: EmployeeCreate):
    doc = {
        "emp_code": body.emp_code, "name": body.name, "email": body.email, "department": body.department,
        "shift_start": body.shift_start, "shift_end": body.shift_end, "joined_on": body.joined_on,
        "created_at": now_millis(),
    }
    try:
        employees().insert_one(doc)  # the unique index is the only duplicate guard (no check-then-insert)
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="emp_code already exists")
    return serialize_employee(doc)


@app.get("/employees")
def list_employees(department: Optional[str] = None, page: PageQ = 1, page_size: PageSizeQ = 20):
    flt = {} if department is None else {"department": department}
    total = employees().count_documents(flt)
    cursor = employees().find(flt, {"_id": 0}).sort("emp_code", 1).skip((page - 1) * page_size).limit(page_size)
    return page_of([serialize_employee(d) for d in cursor], total, page, page_size)


# ---------------------------------------------------------------- attendance

@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInRequest):
    emp = employees().find_one({"emp_code": body.emp_code})
    if emp is None:
        raise HTTPException(status_code=404, detail="employee not found")
    start, end = shift_of(emp)
    pin = ms_to_dt(body.punched_at) if body.punched_at is not None else now_seconds()
    day = attendance_date(pin, start, end)  # R1
    doc = {
        "emp_code": body.emp_code, "date": day, "status": body.status,
        "punch_in": pin, "punch_out": None,
        **derive(body.status, pin, None, day, start, end),
        "history": [],
    }
    try:
        logs().insert_one(doc)  # unique (emp_code, date) index: exactly one concurrent request wins
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="already punched in for this date")
    return serialize_record(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutRequest):
    emp = employees().find_one({"emp_code": body.emp_code})
    if emp is None:
        raise HTTPException(status_code=404, detail="employee not found")
    start, end = shift_of(emp)
    pout = ms_to_dt(body.punched_at) if body.punched_at is not None else now_seconds()
    # most recent record whose punch_in is at or before the punch-out (works across midnight)
    rec = logs().find_one({"emp_code": body.emp_code, "punch_in": {"$lte": pout}}, sort=[("punch_in", -1)])
    if rec is None:
        raise HTTPException(status_code=404, detail="no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(status_code=409, detail="already punched out")
    pin = aware(rec["punch_in"])
    if pout <= pin:
        unprocessable(("body", "punched_at"), "punched_at must be after punch_in")
    if pout - pin > timedelta(hours=24):
        unprocessable(("body", "punched_at"), "punched_at is more than 24 hours after punch_in")
    derived = derive(rec["status"], pin, pout, rec["date"], start, end)
    # compare-and-set: only the request that still sees an open record wins; the other gets 409
    updated = logs().find_one_and_update(
        {"_id": rec["_id"], "punch_out": None, "punch_in": rec["punch_in"]},
        {"$set": {"punch_out": pout, **derived}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(status_code=409, detail="already punched out")
    return serialize_record(updated)


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[DateStr] = None,
    date_to: Optional[DateStr] = None,
    status: Optional[StatusT] = None,
    page: PageQ = 1,
    page_size: PageSizeQ = 20,
):
    flt, sort, skip, limit = attendance_query(emp_code, date_from, date_to, status, page, page_size)
    total = logs().count_documents(flt)
    cursor = logs().find(flt, {"_id": 0}).sort(sort).skip(skip).limit(limit)
    return page_of([serialize_record(d) for d in cursor], total, page, page_size)


_REGULARIZE_FIELDS = ("status", "punch_in", "punch_out", "work_hours", "late_minutes", "overtime_minutes", "half_day")


def plan_regularization(rec: dict, emp: dict, body: RegularizeRequest) -> tuple:
    """Validate the correction and return (new_values, changes). Raises 422 for every rule violation."""
    start, end = shift_of(emp)
    cur_in, cur_out = aware(rec.get("punch_in")), aware(rec.get("punch_out"))
    new_status = body.status or rec["status"]
    if new_status in NO_PUNCH_STATUSES:
        if body.punch_in is not None or body.punch_out is not None:
            unprocessable(("body", "punch_in"), f"punch times cannot be supplied with status {new_status}")
        new_in = new_out = None  # ABSENT / LEAVE clear both punch times
    else:
        new_in = ms_to_dt(body.punch_in) if body.punch_in is not None else cur_in
        new_out = ms_to_dt(body.punch_out) if body.punch_out is not None else cur_out
        if new_in is None:
            unprocessable(("body", "punch_in"), f"status {new_status} requires a punch_in")
        if body.punch_in is not None and attendance_date(new_in, start, end) != rec["date"]:
            unprocessable(("body", "punch_in"), "punch_in must stay on the record's attendance date")
        if new_out is not None and (body.punch_in is not None or body.punch_out is not None):
            if new_out <= new_in:
                unprocessable(("body", "punch_out"), "punch_out must be after punch_in")
            if new_out - new_in > timedelta(hours=24):
                unprocessable(("body", "punch_out"), "punch_out must be within 24 hours of punch_in")
    if new_status == rec["status"] and new_in == cur_in and new_out == cur_out:
        unprocessable(("body",), "the request changes nothing")
    new = {"status": new_status, "punch_in": new_in, "punch_out": new_out,
           **derive(new_status, new_in, new_out, rec["date"], start, end)}
    old = {"status": rec["status"], "punch_in": cur_in, "punch_out": cur_out,
           "work_hours": rec.get("work_hours"), "late_minutes": rec.get("late_minutes") or 0,
           "overtime_minutes": rec.get("overtime_minutes") or 0, "half_day": bool(rec.get("half_day", False))}
    changes = {k: {"from": old[k], "to": new[k]} for k in _REGULARIZE_FIELDS if old[k] != new[k]}
    return new, changes


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: DateStr, body: RegularizeRequest):
    emp = employees().find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(status_code=404, detail="employee not found")
    for _ in range(5):
        rec = logs().find_one({"emp_code": emp_code, "date": date})
        if rec is None:
            raise HTTPException(status_code=404, detail="no attendance record for that date")
        new, changes = plan_regularization(rec, emp, body)
        entry = {"at": now_millis(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
        n_history = len(rec.get("history") or [])
        # Optimistic concurrency: apply only if the record is exactly as we read it (same inputs, same history
        # length). A concurrent writer makes this match nothing; we re-read and retry, so no history is lost.
        updated = logs().find_one_and_update(
            {"emp_code": emp_code, "date": date, "status": rec["status"],
             "punch_in": rec.get("punch_in"), "punch_out": rec.get("punch_out"),
             "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, n_history]}},
            {"$set": new, "$push": {"history": entry}},
            return_document=ReturnDocument.AFTER,
        )
        if updated is not None:
            return serialize_record(updated)
    raise HTTPException(status_code=409, detail="record is being modified concurrently, retry")


# ---------------------------------------------------------------- analytics

@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: Annotated[str, Query(pattern=MONTH_PATTERN)]):
    emp = employees().find_one({"emp_code": emp_code}, {"joined_on": 1})
    if emp is None:
        raise HTTPException(status_code=404, detail="employee not found")
    rows = list(logs().aggregate(monthly_pipeline(emp_code, month), hint=IX_LOG_EMP_DATE))
    r = rows[0] if rows else {}
    present = float(r.get("present_days", 0))
    working = count_working_days(month, emp.get("joined_on") or "0000-01-01")
    return {
        "emp_code": emp_code, "month": month, "working_days": working, "present_days": present,
        "leave_days": int(r.get("leave_days", 0)), "late_count": int(r.get("late_count", 0)),
        "total_late_minutes": int(r.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(r.get("total_overtime_minutes", 0)),
        "attendance_pct": attendance_pct(present, working),
    }


@app.get("/analytics/departments/summary")
def department_summary(month: Annotated[str, Query(pattern=MONTH_PATTERN)], department: Optional[str] = None):
    items = list(employees().aggregate(department_summary_pipeline(month, department), hint=IX_EMP_DEPT_JOINED))
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: Annotated[str, Query(pattern=MONTH_PATTERN)],
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    department: Optional[str] = None,
):
    items = list(logs().aggregate(leaderboard_pipeline(month, limit, department), hint=IX_LOG_DATE_EMP))
    return {"month": month, "items": items}


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    from_: Annotated[DateStr, Query(alias="from")],
    to: DateStr,
):
    d_from, d_to = trend_range(from_, to)
    if employees().count_documents({"department": department}, limit=1) == 0:
        raise HTTPException(status_code=404, detail="unknown department")
    items = list(employees().aggregate(trend_pipeline(department, d_from, d_to), hint=IX_EMP_DEPT_JOINED))
    return {"department": department, "items": items}


# ---------------------------------------------------------------- admin / explain

def _require(**params):
    for name, value in params.items():
        if value is None:
            unprocessable(("query", name), "field required")


def _explain_find(collection: str, flt: dict, sort: list, skip: int, limit: int) -> dict:
    cmd = {"explain": {"find": collection, "filter": flt, "sort": dict(sort), "skip": skip, "limit": limit,
                       "projection": {"_id": 0}},
           "verbosity": "executionStats"}
    return STATE.db.command(cmd)


def _explain_aggregate(collection: str, pipeline: list, hint: str) -> dict:
    cmd = {"explain": {"aggregate": collection, "pipeline": pipeline, "cursor": {}, "hint": hint},
           "verbosity": "executionStats"}
    return STATE.db.command(cmd)


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard",
                      "department_trend"],
    emp_code: Optional[str] = None,
    month: Annotated[Optional[str], Query(pattern=MONTH_PATTERN)] = None,
    department: Optional[str] = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    date_from: Optional[DateStr] = None,
    date_to: Optional[DateStr] = None,
    status: Optional[StatusT] = None,
    from_: Annotated[Optional[DateStr], Query(alias="from")] = None,
    to: Optional[DateStr] = None,
    page: PageQ = 1,
    page_size: PageSizeQ = 20,
):
    """executionStats explain of each endpoint's main query, built by the same builders the endpoint uses."""
    if endpoint == "attendance_list":
        flt, sort, skip, lim = attendance_query(emp_code, date_from, date_to, status, page, page_size)
        collection, raw = "attendance_logs", _explain_find("attendance_logs", flt, sort, skip, lim)
    elif endpoint == "employee_monthly":
        _require(emp_code=emp_code, month=month)
        collection = "attendance_logs"
        raw = _explain_aggregate(collection, monthly_pipeline(emp_code, month), IX_LOG_EMP_DATE)
    elif endpoint == "department_summary":
        _require(month=month)
        collection = "employees"
        raw = _explain_aggregate(collection, department_summary_pipeline(month, department), IX_EMP_DEPT_JOINED)
    elif endpoint == "late_leaderboard":
        _require(month=month)
        collection = "attendance_logs"
        raw = _explain_aggregate(collection, leaderboard_pipeline(month, limit, department), IX_LOG_DATE_EMP)
    else:  # department_trend
        _require(department=department, **{"from": from_}, to=to)
        d_from, d_to = trend_range(from_, to)
        collection = "employees"
        raw = _explain_aggregate(collection, trend_pipeline(department, d_from, d_to), IX_EMP_DEPT_JOINED)
    explain = json.loads(json_util.dumps(raw, json_options=json_util.RELAXED_JSON_OPTIONS))
    return {"endpoint": endpoint, "collection": collection, "explain": explain}
