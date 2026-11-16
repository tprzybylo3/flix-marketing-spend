# Pipeline details

Materialisations, incremental logic, tests and orchestration. Back to [README](../README.md).

## Materialisations

| Model | Materialised as | Why | In production (Snowflake, years of data) |
|---|---|---|---|
| `stg_*` | view | rename and cast only; no second copy | same |
| `int_*` | table, rebuilt in full on every run | data tests read the result instead of recomputing the dedup and FX join | incremental, rebuilding only the last 14 spend dates: a full rebuild would re-read years of raw data every day, while only recent dates can change |
| `fct_daily_marketing_spend` | incremental (`delete+insert` on `spend_date`) | corrections arrive for up to 9 days | same: each run deletes the last 14 dates and inserts them recalculated |
| `export_daily_spend_eur` | CSV file | the brief asks for a file or local database | BI reads the table directly; a file export stays for teams that need one (e.g. to S3) |

## Incremental logic

`DECISION` The brief only says the latest entry is the most accurate. Making the mart incremental is my choice, so a daily run rebuilds only dates that can still change.

| Run | Dates rebuilt |
|---|---|
| Daily (Airflow passes `run_date = ds`) | `run_date` and the 13 dates before it; never dates after `run_date` |
| Without `run_date` | the newest date in the mart and the 13 dates before it, plus any newer dates in the source |
| First run after missed runs | from 12 days before the newest date in the mart up to `run_date`: the missed dates are filled without a backfill |
| Backfill | `dbt build --vars '{run_date: "2025-01-20", lookback_days: 30}'` |
| Full rebuild | `make full-refresh` |

- **Key = `spend_date`, not the full grain.** A run replaces whole dates, so reruns are idempotent; with a full-grain key, a group that disappears (an account moved to another brand) would keep its old row and be counted twice.
- **Why 14 days** `ASSUMPTION` corrections arrive up to 9 days late (10 for one shifted date), seen only on spend dates 1–15 January; +1 day for the window boundary, +1 if `ds` trails the run (Airflow 2 default), plus margin for a yearly worst case ([details](DATA_QUALITY.md#1c-how-late-corrections-arrive-the-14-day-window)).
- **Older corrections are not applied, but detected:**

| Test (warn) | Compares | Catches |
|---|---|---|
| `warn_source_newer_than_mart` | each date's newest source extraction vs the mart's `last_extracted_at` | a late correction outside the window; cheap, timestamps only |
| `warn_mart_drift_at_full_grain` | every mart row vs a fresh aggregation. It runs in every `dbt build`; `drift_check_days` limits it to the last N days: 40 in the daily DAG (`dags/marketing_spend_dag.py`), the whole history when unset (`dbt_project.yml`) | any change in amounts without a new extraction: revised FX rate, reassigned account, changed logic |

- **Why 40 days** `ASSUMPTION` Finance closes a month in the first days of the next one; on 9 February the last 40 days still cover all of January, so a difference in the month is reported before it is closed. Fewer days (e.g. 35) would drop early January by 5 February; checking 40 instead of 35 days costs almost nothing. The close date should be confirmed with Finance.
- `LIMITATION` a date that disappears entirely from the source keeps its old rows until `make full-refresh`; the drift test reports it.
- `TEST` `scripts/test_incremental.sh` (in CI): a correction inside the window is applied, one outside is left untouched and detected, a full refresh repairs it.

## Tests

| Kind | What | Where |
|---|---|---|
| **Generic** | not_null / unique / relationships / accepted_values on keys, currencies, FX and the mart grain; `unique` campaign-day after the FX join, so a bad FX or account row cannot inflate spend | `models/*/_staging.yml`, `_intermediate.yml`, `_marts.yml` |
| **Error** (stop the build) | source values that do not parse (`assert_source_values_parse`; staging uses `try_cast`, the bad row is stored in `main_dbt_test__audit`) · no negative costs (`assert_no_negative_costs`) · FX rate > 0 and valid_to > valid_from (`assert_fx_rates_valid`) · no overlapping FX windows (`assert_fx_windows_do_not_overlap`) | `tests/assert_*.sql` |
| **Warn** (report, publish anyway) | same-timestamp ties (`warn_same_timestamp_conflicts`) · extracted before the spend date (`warn_extracted_before_spend_date`, `warn_latest_extraction_before_spend_date`) · unmapped country (`warn_unmapped_country`) · late correction outside the window (`warn_source_newer_than_mart`) · mart drift (`warn_mart_drift_at_full_grain`) | `tests/warn_*.sql` |
| **Unit** (dbt ≥ 1.8) | latest per campaign incl. a campaign missing from a newer extraction · account-snapshot alternative · tie-break · monthly FX · conversion with the right day's rate · rate window start inclusive, end exclusive · missing rate stays NULL · CET (+0100/+0200) → UTC parsing | `unit_tests:` in `models/staging/_staging.yml`, `models/intermediate/_intermediate.yml` |
| **Integration** | late corrections inside and outside the window, full refresh | `scripts/test_incremental.sh` (`make test-incremental`) |

All dbt tests run inside `make build`; to run only the tests: `uv run dbt test` (or `--select test_type:unit` for the unit tests).

## Orchestration and CI

- **`dags/marketing_spend_dag.py`** daily `dbt build` with `run_date = {{ ds }}`, 14-day lookback, `drift_check_days: 40`, retries; Airflow 3 with `CronTriggerTimetable`, so the run at 03:00 on 10 January has `ds = 2025-01-10` (under Airflow 2's default it would be 9 January; the window covers both). Not executed.
- **`.github/workflows/ci.yml`** every PR: `dbt parse`, `dbt build`, the incremental test.
