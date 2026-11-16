#!/usr/bin/env bash

# Integration test of the incremental mart, run on a temporary copy of the project.
# Appends two late corrections to the raw costs (one inside the 14-day window, one outside), then checks that:
#   1. the correction inside the window is applied and the date outside is left untouched,
#   2. warn_source_newer_than_mart reports the date outside the window,
#   3. a full refresh applies it and the warning clears.
set -euo pipefail
DBT="$(command -v "${DBT:-dbt}")"
PY="$(command -v "${PY:-python3}")"
case "$DBT" in /*) ;; *) DBT="$PWD/$DBT" ;; esac
case "$PY" in /*) ;; *) PY="$PWD/$PY" ;; esac
SRC="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

cd "$SRC"
tar --exclude=./.venv --exclude=./target --exclude=./logs --exclude='./output/*' -cf - . | tar -xf - -C "$WORK"
cd "$WORK"
export DBT_PROFILES_DIR=.
spend() {
  "$PY" - "$1" <<'PY'
import sys, duckdb
con = duckdb.connect("output/marketing.duckdb", read_only=True)
print(con.execute("select round(sum(spend_eur), 2) from fct_daily_marketing_spend where spend_date = ?", [sys.argv[1]]).fetchone()[0])
PY
}
revise() {
  "$PY" - "$1" <<'PY'
import csv, sys
path = "data/de_case_study_anon_raw_costs.csv"
rows = list(csv.DictReader(open(path)))
row = max((r for r in rows if r["SEGMENTS_DATE"] == sys.argv[1]), key=lambda r: r["LAST_EXTRACTED_AT"])
row = dict(row, LAST_EXTRACTED_AT="2025-02-20 02:00:00.000", METRICS_COST_MICROS=str(int(row["METRICS_COST_MICROS"]) + 1_000_000_000))
with open(path, "a", newline="") as f:
    csv.DictWriter(f, fieldnames=rows[0].keys(), lineterminator="\n").writerow(row)
PY
}
"$DBT" build --quiet
in_window_before=$(spend 2025-01-30)
out_of_window_before=$(spend 2025-01-05)
revise 2025-01-30
revise 2025-01-05
"$DBT" run --quiet --select +fct_daily_marketing_spend
[ "$(spend 2025-01-30)" != "$in_window_before" ] || { echo "FAIL: in-window revision not applied"; exit 1; }
[ "$(spend 2025-01-05)" == "$out_of_window_before" ] || { echo "FAIL: date outside lookback was rebuilt"; exit 1; }
echo "OK: revision inside the lookback applied, date outside left untouched"
if "$DBT" test --quiet --warn-error --select warn_source_newer_than_mart >/dev/null; then
  echo "FAIL: late correction outside the lookback was not detected"; exit 1
fi
echo "OK: warn_source_newer_than_mart detects the late correction outside the lookback"
"$DBT" build --quiet --full-refresh
[ "$(spend 2025-01-05)" != "$out_of_window_before" ] || { echo "FAIL: full refresh did not apply revision"; exit 1; }
"$DBT" test --quiet --warn-error --select warn_source_newer_than_mart >/dev/null \
  || { echo "FAIL: warning still raised after full refresh"; exit 1; }
echo "OK: full refresh repairs it and the warning clears"
