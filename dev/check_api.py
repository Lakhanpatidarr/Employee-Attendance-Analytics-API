"""Dev helper (not part of the submission). Run AFTER seeding and starting the server:

    python dev/seed_awkward.py --yes
    uvicorn app.main:app --port 8000 &
    python dev/check_api.py [http://localhost:8000]

Part 1 recomputes every analytics endpoint in plain Python straight from the stored documents (an oracle that
shares no code with the aggregation pipelines) and compares. Part 2 exercises rules, errors, concurrency, explain.
"""
import json
import os
import random
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from math import floor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv  # noqa: E402
from pymongo import MongoClient  # noqa: E402

load_dotenv()
BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
IST = timezone(timedelta(hours=5, minutes=30))
PRES = {"PRESENT", "WFH", "ON_DUTY"}
FAILS = []


def call(method, path, body=None, params=None):
    url = BASE + path + ("?" + urllib.parse.urlencode(params) if params else "")
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, raw


def check(name, ok, detail=""):
    print(("PASS  " if ok else "FAIL  ") + name + ("" if ok else f"   {detail}"))
    if not ok:
        FAILS.append(name)


def same(a, b):
    if isinstance(a, float) or isinstance(b, float):
        return a is not None and b is not None and abs(a - b) < 1e-9
    return a == b


def hu(x: Fraction, k: int) -> float:  # half-up rounding to k decimals, exact
    return float(Fraction(floor(x * 10 ** k + Fraction(1, 2)), 10 ** k))


def month_days(month):
    y, m = int(month[:4]), int(month[5:])
    d = date(y, m, 1)
    while d.month == m:
        yield d
        d += timedelta(days=1)


# ----------------------------------------------------------------------------- Part 1: oracle
def part1():
    db = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"), tz_aware=True)[os.getenv("MONGO_DB", "attendance_db")]
    emps = {e["emp_code"]: e for e in db.employees.find()}
    recs = list(db.attendance_logs.find())
    weekday = lambda s: date.fromisoformat(s).weekday() < 5  # noqa: E731
    weight = lambda r: Fraction(1, 2) if r.get("half_day") else Fraction(1)  # noqa: E731
    late = lambda r: r.get("late_minutes") or 0  # noqa: E731

    def in_month(r, month):
        return r["date"].startswith(month)

    for month in ("2026-06", "2026-07", "2026-08"):
        for code, e in sorted(emps.items()):
            rs = [r for r in recs if r["emp_code"] == code and in_month(r, month)]
            working = sum(1 for d in month_days(month) if d.weekday() < 5 and d.isoformat() >= e["joined_on"])
            present = sum((weight(r) for r in rs if r["status"] in PRES and weekday(r["date"])), Fraction(0))
            want = {"emp_code": code, "month": month, "working_days": working, "present_days": float(present),
                    "leave_days": sum(1 for r in rs if r["status"] == "LEAVE"),
                    "late_count": sum(1 for r in rs if late(r) > 0), "total_late_minutes": sum(late(r) for r in rs),
                    "total_overtime_minutes": sum(r.get("overtime_minutes") or 0 for r in rs),
                    "attendance_pct": hu(present / working * 100, 2) if working else None}
            st, got = call("GET", f"/analytics/employees/{code}/monthly", params={"month": month})
            check(f"monthly {code} {month}", st == 200 and all(same(got.get(k), v) for k, v in want.items()), f"{got} != {want}")

    for month in ("2026-06", "2026-07", "2026-08"):
        last = list(month_days(month))[-1].isoformat()
        want = []
        for dept in sorted({e["department"] for e in emps.values()}):
            members = {c for c, e in emps.items() if e["department"] == dept and e["joined_on"] <= last}
            if not members:
                continue
            rs = [r for r in recs if r["emp_code"] in members and in_month(r, month)]
            hours = [Fraction(Decimal(str(r["work_hours"]))) for r in rs if r["status"] in PRES and r.get("work_hours") is not None]
            want.append({"department": dept, "headcount": len(members),
                         "present_days": float(sum((weight(r) for r in rs if r["status"] in PRES and weekday(r["date"])), Fraction(0))),
                         "avg_work_hours": hu(sum(hours) / len(hours), 2) if hours else None,
                         "late_count": sum(1 for r in rs if late(r) > 0), "total_late_minutes": sum(late(r) for r in rs if late(r) > 0),
                         "leave_count": sum(1 for r in rs if r["status"] == "LEAVE"),
                         "on_duty_count": sum(1 for r in rs if r["status"] == "ON_DUTY")})
        st, got = call("GET", "/analytics/departments/summary", params={"month": month})
        ok = st == 200 and len(got["items"]) == len(want) and all(
            all(same(g.get(k), v) for k, v in w.items()) for g, w in zip(got["items"], want))
        check(f"department summary {month}", ok, f"\n got {got}\nwant {want}")

    def board(month, limit, dept):
        tot, cnt = defaultdict(int), defaultdict(int)
        for r in recs:
            if in_month(r, month) and late(r) > 0 and r["emp_code"] in emps:
                tot[r["emp_code"]] += late(r)
                cnt[r["emp_code"]] += 1
        rows = [(c, t) for c, t in tot.items() if t > 0 and (dept is None or emps[c]["department"] == dept)]
        out = [{"rank": 1 + sum(1 for _, t2 in rows if t2 > t), "emp_code": c, "name": emps[c]["name"],
                "department": emps[c]["department"], "total_late_minutes": t, "late_count": cnt[c]} for c, t in rows]
        return sorted([o for o in out if o["rank"] <= limit], key=lambda o: (-o["total_late_minutes"], o["emp_code"]))

    for month, limit, dept in [("2026-07", 10, None), ("2026-07", 1, None), ("2026-07", 2, None), ("2026-07", 3, None),
                               ("2026-07", 5, None), ("2026-07", 3, "HR"), ("2026-07", 50, "Engineering"), ("2026-06", 10, None)]:
        params = {"month": month, "limit": limit, **({"department": dept} if dept else {})}
        st, got = call("GET", "/analytics/leaderboard/late", params=params)
        want = board(month, limit, dept)
        check(f"leaderboard {month} limit={limit} dept={dept}", st == 200 and got["items"] == want, f"\n got {got}\nwant {want}")

    def trend(dept, d_from, d_to):
        a, b = date.fromisoformat(d_from), date.fromisoformat(d_to)
        members = {c for c, e in emps.items() if e["department"] == dept}
        rows, r4s = [], []
        d = a
        while d <= b:
            ds = d.isoformat()
            rs = [r for r in recs if r["emp_code"] in members and r["date"] == ds]
            head = sum(1 for c in members if emps[c]["joined_on"] <= ds)
            present = sum((weight(r) for r in rs if r["status"] in PRES), Fraction(0))
            rate = hu(present / head, 4) if d.weekday() < 5 and head > 0 else None
            r4s.append(None if rate is None else Fraction(Decimal(str(rate))))
            win = [x for x in r4s[-7:] if x is not None]
            rows.append({"date": ds, "is_working_day": d.weekday() < 5, "headcount": head, "present_count": float(present),
                         "late_count": sum(1 for r in rs if late(r) > 0), "attendance_rate": rate,
                         "moving_avg_7d": hu(sum(win) / len(win), 4) if win else None})
            d += timedelta(days=1)
        return rows

    for dept, f, t in [("Engineering", "2026-06-29", "2026-07-31"), ("Sales", "2026-07-25", "2026-08-14"),
                       ("HR", "2026-07-04", "2026-07-06"), ("Ops", "2026-06-15", "2026-08-14"), ("Engineering", "2026-07-11", "2026-07-11")]:
        st, got = call("GET", f"/analytics/departments/{dept}/trend", params={"from": f, "to": t})
        want = trend(dept, f, t)
        ok = st == 200 and len(got["items"]) == len(want) and all(
            all(same(g.get(k), v) for k, v in w.items()) for g, w in zip(got["items"], want))
        check(f"trend {dept} {f}..{t} ({len(want)} rows)", ok, f"\n got {got}\nwant {want}" if st != 200 or len(got['items']) < 8 else "")


# ----------------------------------------------------------------------------- Part 2: behaviour
def ms(y, mo, d, hh, mm, ss=0, frac=0):
    return int(datetime(y, mo, d, hh, mm, ss, tzinfo=IST).timestamp()) * 1000 + frac


def part2():
    tag = random.randint(10000, 99999)
    c1, c2 = f"EMP9{tag}", f"EMP8{tag}"
    dept = f"QA{tag}"

    def mk(code, **kw):
        return call("POST", "/employees", {"emp_code": code, "name": "QA", "email": "qa@example.com", "department": dept,
                                           "joined_on": "2027-01-01", **kw})

    st, e = mk(c1)
    check("create employee 201 + defaults", st == 201 and e["shift_start"] == "09:30" and e["shift_end"] == "18:30" and "_id" not in e, e)
    check("duplicate emp_code 409", mk(c1)[0] == 409)
    check("bad emp_code 422", mk("emp1")[0] == 422 and mk("EMP12")[0] == 422)
    check("equal shifts 422", mk(f"EMP7{tag}", shift_start="09:00", shift_end="09:00")[0] == 422)
    check("overnight employee", mk(c2, shift_start="22:00", shift_end="06:00")[0] == 201)

    # concurrent duplicate creates
    codes = []
    res = []
    def worker():
        res.append(mk(f"EMP6{tag}")[0])
    ts = [threading.Thread(target=worker) for _ in range(12)]
    [t.start() for t in ts]; [t.join() for t in ts]
    check("12 concurrent creates -> one 201, rest 409", sorted(res) == [201] + [409] * 11, res)

    # R2 examples + truncation (shift 09:30)
    for (dd, hh, mm, ss, frac, want) in [(1, 9, 40, 0, 0, 0), (2, 9, 40, 1, 0, 10), (3, 10, 15, 59, 0, 45), (4, 9, 40, 0, 900, 0)]:
        st, r = call("POST", "/attendance/punch-in", {"emp_code": c1, "punched_at": ms(2027, 3, dd, hh, mm, ss, frac)})
        check(f"punch-in 2027-03-0{dd} {hh}:{mm:02d}:{ss:02d} late={want}", st == 201 and r["late_minutes"] == want and r["date"] == f"2027-03-0{dd}"
              and r["punch_in"] % 1000 == 0 and r["history"] == [] and r["work_hours"] is None, r)
    check("duplicate punch-in 409", call("POST", "/attendance/punch-in", {"emp_code": c1, "punched_at": ms(2027, 3, 1, 9, 0)})[0] == 409)
    check("unknown employee 404", call("POST", "/attendance/punch-in", {"emp_code": "EMP0000"})[0] == 404)
    for bad in (1783312500, 1783312500000.5, "1783312500000", True, -5, 99_999_999_999):
        check(f"punched_at {bad!r} -> 422", call("POST", "/attendance/punch-in", {"emp_code": c1, "punched_at": bad})[0] == 422)
    check("status ABSENT rejected on punch-in 422", call("POST", "/attendance/punch-in", {"emp_code": c1, "status": "ABSENT", "punched_at": ms(2027, 3, 8, 9, 0)})[0] == 422)

    # punch-out
    st, r = call("POST", "/attendance/punch-out", {"emp_code": c1, "punched_at": ms(2027, 3, 1, 19, 10)})
    check("punch-out R3/R4", st == 200 and r["overtime_minutes"] == 40 and r["work_hours"] == 9.5 and r["punch_out"] is not None, r)
    check("second punch-out 409", call("POST", "/attendance/punch-out", {"emp_code": c1, "punched_at": ms(2027, 3, 1, 19, 20)})[0] == 409)
    check("punch-out >24h after open record 422", call("POST", "/attendance/punch-out", {"emp_code": c1, "punched_at": ms(2027, 3, 5, 10, 0)})[0] == 422)
    check("punch-out == punch_in 422", call("POST", "/attendance/punch-out", {"emp_code": c1, "punched_at": ms(2027, 3, 4, 9, 40)})[0] == 422)
    check("punch-out with no earlier punch-in 404", call("POST", "/attendance/punch-out", {"emp_code": c1, "punched_at": ms(2027, 2, 1, 9, 0)})[0] == 404)
    st, r = call("POST", "/attendance/punch-out", {"emp_code": c1, "punched_at": ms(2027, 3, 2, 12, 40)})
    check("half day (3h) R5", st == 200 and r["half_day"] is True and r["work_hours"] == 3.0, r)

    # overnight (22:00-06:00)
    st, r = call("POST", "/attendance/punch-in", {"emp_code": c2, "punched_at": ms(2027, 3, 2, 21, 55)})
    check("overnight punch-in date = start day", st == 201 and r["date"] == "2027-03-02" and r["late_minutes"] == 0, r)
    st, r = call("POST", "/attendance/punch-out", {"emp_code": c2, "punched_at": ms(2027, 3, 3, 6, 40)})
    check("overnight punch-out across midnight (40 overtime, 8.75h)", st == 200 and r["overtime_minutes"] == 40 and r["work_hours"] == 8.75 and r["date"] == "2027-03-02", r)
    st, r = call("POST", "/attendance/punch-in", {"emp_code": c2, "punched_at": ms(2027, 3, 4, 5, 30)})
    check("overnight 05:30 punch-in belongs to previous day", st == 201 and r["date"] == "2027-03-03", r)

    # concurrency: punch-in / punch-out
    res = []
    def pi():
        res.append(call("POST", "/attendance/punch-in", {"emp_code": c1, "punched_at": ms(2027, 4, 5, 9, 0)})[0])
    ts = [threading.Thread(target=pi) for _ in range(16)]
    [t.start() for t in ts]; [t.join() for t in ts]
    check("16 concurrent punch-ins -> one 201, rest 409", sorted(res) == [201] + [409] * 15, res)
    res = []
    def po():
        res.append(call("POST", "/attendance/punch-out", {"emp_code": c1, "punched_at": ms(2027, 4, 5, 18, 0)})[0])
    ts = [threading.Thread(target=po) for _ in range(16)]
    [t.start() for t in ts]; [t.join() for t in ts]
    check("16 concurrent punch-outs -> one 200, rest 409", sorted(res) == [200] + [409] * 15, res)

    # regularize
    path = f"/attendance/{c1}/2027-03-03"   # punched in 10:15:59 (late 45), still open
    patch = lambda body, p=path: call("PATCH", p, {"reason": "biometric glitch", "regularized_by": "hr.admin", **body})  # noqa: E731
    st, r = patch({"punch_in": ms(2027, 3, 3, 9, 28), "punch_out": ms(2027, 3, 3, 18, 35)})
    h = r["history"][0] if st == 200 and r["history"] else {}
    check("PATCH recomputes + appends history", st == 200 and r["late_minutes"] == 0 and r["work_hours"] == 9.12 and len(r["history"]) == 1
          and set(h.get("changes", {})) == {"punch_in", "punch_out", "work_hours", "late_minutes"} and h["changes"]["late_minutes"] == {"from": 45, "to": 0}
          and h["changes"]["punch_in"]["to"] == ms(2027, 3, 3, 9, 28) and h["by"] == "hr.admin", r)
    check("PATCH no-op 422", patch({"punch_in": ms(2027, 3, 3, 9, 28)})[0] == 422)
    check("PATCH punch_in on another date 422", patch({"punch_in": ms(2027, 3, 5, 9, 28)})[0] == 422)
    check("PATCH punch_out <= punch_in 422", patch({"punch_out": ms(2027, 3, 3, 9, 0)})[0] == 422)
    check("PATCH reason too short 422", call("PATCH", path, {"reason": "x", "regularized_by": "a", "status": "WFH"})[0] == 422)
    check("PATCH derived field ignored / nothing else changed -> 422", patch({"work_hours": 1})[0] == 422)
    check("PATCH unknown date 404", patch({"status": "WFH"}, f"/attendance/{c1}/2030-01-01")[0] == 404)
    check("PATCH unknown employee 404", patch({"status": "WFH"}, "/attendance/EMP0000/2027-03-03")[0] == 404)
    check("PATCH bad date 422", patch({"status": "WFH"}, f"/attendance/{c1}/2027-13-45")[0] == 422)
    st, r = patch({"status": "LEAVE"})
    check("PATCH to LEAVE clears punches/derived", st == 200 and r["punch_in"] is None and r["punch_out"] is None and r["work_hours"] is None
          and r["late_minutes"] == 0 and len(r["history"]) == 2, r)
    check("LEAVE with punch_in 422", patch({"status": "LEAVE", "punch_in": ms(2027, 3, 3, 9, 0)})[0] == 422)
    check("PRESENT without punch_in 422", patch({"status": "PRESENT"})[0] == 422)
    st, r = patch({"status": "WFH", "punch_in": ms(2027, 3, 3, 9, 0)})
    check("LEAVE -> WFH with punch_in", st == 200 and r["status"] == "WFH" and r["late_minutes"] == 0 and len(r["history"]) == 3, r)

    # concurrent regularizations never lose history
    path2 = f"/attendance/{c1}/2027-03-04"
    ok_count, lock = [], threading.Lock()
    def reg(i):
        st, _ = call("PATCH", path2, {"reason": f"concurrent edit {i}", "regularized_by": f"u{i}", "punch_in": ms(2027, 3, 4, 9, 0, i + 1)})
        with lock:
            ok_count.append(st)
    ts = [threading.Thread(target=reg, args=(i,)) for i in range(8)]
    [t.start() for t in ts]; [t.join() for t in ts]
    st, page = call("GET", "/attendance", params={"emp_code": c1, "date_from": "2027-03-04", "date_to": "2027-03-04"})
    n_hist = len(page["items"][0]["history"])
    check("concurrent PATCHes: history length == number of 200s", n_hist == ok_count.count(200) and set(ok_count) <= {200, 409, 422}, (ok_count, n_hist))

    # list / pagination
    st, page = call("GET", "/attendance", params={"emp_code": c1, "page_size": 2})
    dates = [i["date"] for i in page["items"]]
    check("list sorted by date desc, paginated, total filtered", st == 200 and dates == sorted(dates, reverse=True) and page["page_size"] == 2 and page["total"] >= 5, page)
    check("page_size 101 -> 422", call("GET", "/attendance", params={"page_size": 101})[0] == 422)
    check("page 0 -> 422", call("GET", "/attendance", params={"page": 0})[0] == 422)
    check("date_from > date_to -> 422", call("GET", "/attendance", params={"date_from": "2027-02-02", "date_to": "2027-02-01"})[0] == 422)
    check("bad status -> 422", call("GET", "/attendance", params={"status": "NOPE"})[0] == 422)
    st, page = call("GET", "/employees", params={"department": dept})
    check("employees by department sorted by emp_code", st == 200 and page["total"] >= 3 and [i["emp_code"] for i in page["items"]] == sorted(i["emp_code"] for i in page["items"]), page)

    # analytics errors
    check("monthly unknown employee 404", call("GET", "/analytics/employees/EMP0000/monthly", params={"month": "2026-07"})[0] == 404)
    check("monthly bad month 422", call("GET", f"/analytics/employees/{c1}/monthly", params={"month": "2026-13"})[0] == 422)
    check("trend unknown department 404", call("GET", "/analytics/departments/Nope/trend", params={"from": "2026-07-01", "to": "2026-07-02"})[0] == 404)
    check("trend 92 days ok", call("GET", "/analytics/departments/HR/trend", params={"from": "2026-07-01", "to": "2026-09-30"})[0] == 200)
    check("trend 93 days 422", call("GET", "/analytics/departments/HR/trend", params={"from": "2026-07-01", "to": "2026-10-01"})[0] == 422)
    check("trend to < from 422", call("GET", "/analytics/departments/HR/trend", params={"from": "2026-07-02", "to": "2026-07-01"})[0] == 422)
    check("leaderboard limit 51 -> 422", call("GET", "/analytics/leaderboard/late", params={"month": "2026-07", "limit": 51})[0] == 422)

    # explain: IXSCAN present, COLLSCAN absent
    cases = {"attendance_list": {}, "attendance_list ": {"emp_code": "EMP0001", "date_from": "2026-07-01", "date_to": "2026-07-31", "status": "PRESENT"},
             "employee_monthly": {"emp_code": "EMP0001", "month": "2026-07"}, "department_summary": {"month": "2026-07"},
             "late_leaderboard": {"month": "2026-07"}, "department_trend": {"department": "HR", "from": "2026-07-01", "to": "2026-07-31"}}
    for name, params in cases.items():
        st, out = call("GET", f"/admin/explain/{name.strip()}", params=params)
        text = json.dumps(out)
        check(f"explain {name.strip()} {sorted(params)}: IXSCAN, no COLLSCAN", st == 200 and "IXSCAN" in text and "COLLSCAN" not in text, text[:300])
    check("explain missing params 422", call("GET", "/admin/explain/department_trend", params={"department": "HR"})[0] == 422)
    check("explain unknown endpoint 422", call("GET", "/admin/explain/nope")[0] == 422)
    st, out = call("GET", "/health")
    check("health", st == 200 and out == {"status": "ok"})


if __name__ == "__main__":
    part1()
    part2()
    print(f"\n{len(FAILS)} FAILED" if FAILS else "\nALL CHECKS PASSED")
    sys.exit(1 if FAILS else 0)
