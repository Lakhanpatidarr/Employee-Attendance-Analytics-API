# Employee Attendance & Analytics API

FastAPI + MongoDB. All application code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # MONGO_URI / MONGO_DB (real environment variables take priority over .env)
uvicorn app.main:app --port 8000
```
Indexes are created at startup (idempotent). MongoDB 6.0+ is required (`$setWindowFields`, `$dateAdd`, `$rank`).

## Notes for reviewers
- Analytics are single aggregation pipelines; half-up rounding is done in integer arithmetic inside the pipeline.
- The department trend generates its calendar with `$range` + `$dateAdd` (instead of `$densify`) so headcount per day
  can be computed in the same pipeline; the moving average is a `$setWindowFields` window.
- `GET /admin/explain/{endpoint}` explains the exact query/pipeline the endpoint runs.
- `REVIEW.md` (Part A) lists 26 defects found in the starter, each with a failing example and its fix, plus what I judged fine.

## Optional dev tools (`dev/`, not needed to run the app)
```bash
python dev/seed_awkward.py --yes      # DROPS employees + attendance_logs in MONGO_DB, loads awkward test data
python dev/check_api.py               # recomputes every analytics endpoint in plain Python and compares; plus rule/concurrency/explain checks
```
