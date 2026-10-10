"""Dev helper (not part of the submission): loads an awkward-but-deterministic dataset into MONGO_DB.

    python dev/seed_awkward.py --yes        # DROPS employees + attendance_logs in $MONGO_DB first

Hand-made cases (July 2026 unless noted):
  EMP0003  Engineering, no logs at all                 EMP0002  mid-month joiner (2026-07-15)
  EMP0006  Sales, joins 2026-08-03 (not in July)       EMP0005  overnight shift 22:00-06:00
  EMP0001 120 late min (incl. a Saturday), EMP0007/8/9 tie on 60, EMP0004 45  -> ranks 1,2,2,2,5
  EMP9999  orphan logs (employee does not exist, 500 late min) -> must be ignored
  ~20% of records are 'legacy' (no half_day / history), some weekdays have no record at all.
"""
import os
import random
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv  # noqa: E402
from pymongo import MongoClient  # noqa: E402

from app.main import IST, attendance_date, derive  # noqa: E402  (reuse the rules to build stored values)

load_dotenv()
UTC = timezone.utc
EMPS = [  # emp_code, name, dept, start, end, joined_on
    ("EMP0001", "Asha Rao", "Engineering", "09:30", "18:30", "2026-01-05"),
    ("EMP0002", "Bilal Khan", "Engineering", "09:30", "18:30", "2026-07-15"),
    ("EMP0003", "Chitra Iyer", "Engineering", "09:30", "18:30", "2026-02-02"),
    ("EMP0004", "Dev Patel", "Sales", "09:30", "18:30", "2026-01-05"),
    ("EMP0005", "Esha Nair", "Sales", "22:00", "06:00", "2026-03-02"),
    ("EMP0006", "Farid Ali", "Sales", "09:30", "18:30", "2026-08-03"),
    ("EMP0007", "Gita Menon", "HR", "09:30", "18:30", "2026-01-05"),
    ("EMP0008", "Hari Om", "HR", "09:30", "18:30", "2026-01-05"),
    ("EMP0009", "Isha Roy", "HR", "09:30", "18:30", "2026-01-05"),
    ("EMP0010", "Jai Singh", "Ops", "10:00", "19:00", "2026-01-05"),
]
NO_LOGS = {"EMP0003", "EMP0006"}
RNG = random.Random(20260710)


def ist_dt(day: date, hh: int, mm: int, ss: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hh, mm, ss, tzinfo=IST).astimezone(UTC)


def build(emp, day: date, status: str, pin=None, pout=None, legacy=False) -> dict:
    code, _, _, ss, se, _ = emp
    d = attendance_date(pin, ss, se) if pin else day.isoformat()
    doc = {"emp_code": code, "date": d, "status": status, "punch_in": pin, "punch_out": pout,
           **derive(status, pin, pout, d, ss, se), "history": []}
    if legacy:
        del doc["history"], doc["half_day"]
    return doc


def main() -> None:
    if "--yes" not in sys.argv:
        sys.exit("refusing to drop collections without --yes")
    db = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"), tz_aware=True)[os.getenv("MONGO_DB", "attendance_db")]
    db.employees.drop(), db.attendance_logs.drop()
    db.employees.insert_many([{"emp_code": c, "name": n, "email": f"{c.lower()}@example.com", "department": dp,
                               "shift_start": s, "shift_end": e, "joined_on": j,
                               "created_at": datetime(2026, 1, 1, tzinfo=UTC)} for c, n, dp, s, e, j in EMPS])
    docs: dict = {}
    day = date(2026, 6, 15)
    while day <= date(2026, 8, 14):
        for emp in EMPS:
            code, _, _, ss, se, joined = emp
            if code in NO_LOGS or day.weekday() >= 5 or day.isoformat() < joined or RNG.random() < 0.10:
                continue  # weekends / before joining / a missing weekday (no record at all)
            h, m = map(int, ss.split(":"))
            pin = ist_dt(day, h, m) - timedelta(minutes=RNG.randint(1, 9))  # before shift start: never late
            roll, legacy = RNG.random(), RNG.random() < 0.2
            if roll < 0.05:
                rec = build(emp, day, "ABSENT", legacy=legacy)
            elif roll < 0.10:
                rec = build(emp, day, "LEAVE", legacy=legacy)
            else:
                status = "WFH" if roll < 0.18 else "ON_DUTY" if roll < 0.22 else "PRESENT"
                if RNG.random() < 0.12:
                    pout = pin + timedelta(hours=3, minutes=RNG.choice([0, 15, 30, 45]))  # half days
                else:
                    end_day = day + timedelta(days=1) if se <= ss else day
                    eh, em = map(int, se.split(":"))
                    pout = ist_dt(end_day, eh, em) + timedelta(minutes=RNG.choice([-40, -5, 0, 10, 29, 30, 45, 90]))
                rec = build(emp, day, status, pin, pout, legacy)
            docs[(code, rec["date"])] = rec
        day += timedelta(days=1)

    def put(code, day_s, in_hhmm, out_hhmm, status="PRESENT"):  # explicit override for the hand-made cases
        emp = next(e for e in EMPS if e[0] == code)
        d = date.fromisoformat(day_s)
        ih, im = map(int, in_hhmm.split(":"))
        oh, om = map(int, out_hhmm.split(":"))
        docs[(code, day_s)] = build(emp, d, status, ist_dt(d, ih, im), ist_dt(d, oh, om))

    put("EMP0001", "2026-07-06", "11:00", "19:00")          # 90 late
    put("EMP0001", "2026-07-11", "10:00", "19:30")          # Saturday: late 30 + overtime 60, no present_days
    put("EMP0007", "2026-07-06", "10:30", "18:30")          # 60
    put("EMP0008", "2026-07-07", "10:30", "18:30")          # 60
    put("EMP0009", "2026-07-08", "10:30", "18:30")          # 60
    put("EMP0004", "2026-07-10", "10:15", "18:30")          # 45
    docs[("EMP9999", "2026-07-06")] = {"emp_code": "EMP9999", "date": "2026-07-06", "status": "PRESENT",
                                       "punch_in": ist_dt(date(2026, 7, 6), 12, 50), "punch_out": None, "work_hours": None,
                                       "late_minutes": 500, "overtime_minutes": 0, "half_day": False, "history": []}
    db.attendance_logs.insert_many(list(docs.values()))
    print(f"seeded {len(EMPS)} employees, {len(docs)} attendance records into '{db.name}'")


if __name__ == "__main__":
    main()
