#!/usr/bin/env bash

# Builds the project once per business-rule scenario (dbt variables) on a temporary copy
# and prints a markdown table with rows, total EUR and spend per country for each scenario.
set -euo pipefail

DBT="$(command -v "${DBT:-dbt}")"
PY="$(command -v "${PY:-python3}")"
case "$DBT" in /*) ;; *) DBT="$PWD/$DBT" ;; esac
case "$PY" in /*) ;; *) PY="$PWD/$PY" ;; esac
SRC="$(cd "$(dirname "$0")/.." && pwd)"
SCENARIOS=(
  "A0 default|{}"
  "A1 latest = account snapshot|{latest_grain: account_snapshot}"
  "A2 tie = lowest cost|{tie_break: lowest}"
  "A3 FX = monthly average|{fx_method: monthly_average}"
  "A4 channel = ad platform|{channel_source: ad_platform}"
)

echo "| Scenario | vars | dbt build | Rows | Total EUR | DE | BR | HU | Channels |"
echo "|---|---|---|---|---|---|---|---|---|"
for entry in "${SCENARIOS[@]}"; do
  name="${entry%%|*}"; vars="${entry#*|}"
  work="$(mktemp -d)"
  (cd "$SRC" && tar --exclude=./.venv --exclude=./target --exclude=./logs --exclude='./output/*' -cf - .) | tar -xf - -C "$work"
  status=$(cd "$work" && DBT_PROFILES_DIR=. "$DBT" build --quiet --vars "$vars" >/dev/null 2>&1 && echo pass || echo FAIL)
  (cd "$work" && "$PY" - "$name" "$vars" "$status" <<'PY'
import sys, duckdb
name, vars_, status = sys.argv[1:]
if status != "pass":
    print(f"| {name} | `{vars_}` | FAIL | - | - | - | - | - | - |")
    sys.exit(0)
con = duckdb.connect("output/marketing.duckdb", read_only=True)
rows, total = con.execute("select count(*), round(sum(spend_eur), 2) from fct_daily_marketing_spend").fetchone()
by = dict(con.execute("select country, round(sum(spend_eur), 2) from fct_daily_marketing_spend group by 1").fetchall())
ch = ", ".join(r[0] for r in con.execute("select distinct channel from fct_daily_marketing_spend order by 1").fetchall())
f = lambda x: f"{x:,.2f}" if x is not None else "-"
print(f"| {name} | `{vars_}` | {status} | {rows} | {f(total)} | {f(by.get('DE'))} | {f(by.get('BR'))} | {f(by.get('HU'))} | {ch} |")
PY
  )
  rm -rf "$work"
done
