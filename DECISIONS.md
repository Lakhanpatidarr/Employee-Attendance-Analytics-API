# DECISIONS.md

1. **Indexes.** Unique `emp_code` on employees and unique `(emp_code, date)` on logs enforce identity and one record per day. `(date desc, emp_code)` serves the default list order and month ranges; `(status, date desc, emp_code)` serves status filters; `(emp_code, punch_in desc)` finds the open record at punch-out; `(department, emp_code)` and `(department, joined_on)` serve the employee list and drive the analytics. Rejected: a covering index on `late_minutes` for the leaderboard, since it slows every write for a modest read gain.
2. **Punch-in race.** Both requests compute the same date and call `insert_one`. The unique `(emp_code, date)` index lets exactly one succeed; the other gets a duplicate-key error and returns 409. No read-before-write.
3. **Ties.** The leaderboard ranks first (`$rank`: 1, 2, 2, 4) and applies `limit` afterwards, so everyone tied at the cutoff is returned and the response can exceed `limit`.
4. **Headcount.** The pipeline starts from `employees` (joined by month end) and looks up each person's logs. Someone with no logs gets an empty lookup and is still counted.
5. **100x data.** Keep a pre-aggregated per-employee, per-month summary updated on writes, and use keyset pagination instead of `skip`.
