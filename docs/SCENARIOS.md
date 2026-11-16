# Business-rule decisions and scenarios

The brief and the data leave some rules open. Each one is a **dbt variable**: the default is my `DECISION`, and every alternative is implemented, unit-tested and quantified, so changing a rule is one flag, not a rewrite. Back to [README](../README.md).

```bash
dbt build --vars '{latest_grain: account_snapshot}'   # run one scenario
make scenarios                                        # rebuild every scenario and print the table below
```

An invalid value fails at compile time (`macros/allowed_var.sql`).

## Impact on January 2025

Each row changes one variable; the rest stays at the default.

| Scenario | vars | Total EUR | vs default | DE | BR | HU |
|---|---|---|---|---|---|---|
| **A0 default (submitted)** | `{}` | **345,950.02** | — | 182,192.33 | 82,658.30 | 81,099.39 |
| A1 latest = account snapshot | `{latest_grain: account_snapshot}` | 200,979.15 | **−41.9%** | 103,327.30 | 45,882.19 | 51,769.66 |
| A2 tie = lowest cost | `{tie_break: lowest}` | 344,117.72 | −0.53% | 181,138.01 | 82,535.85 | 80,443.86 |
| A3 FX = monthly average | `{fx_method: monthly_average}` | 346,021.57 | +0.02% | 182,192.33 | 82,729.85 | 81,099.39 |
| A4 channel = ad platform | `{channel_source: ad_platform}` | 345,950.02 | 0% (labels only: `Google`) | 182,192.33 | 82,658.30 | 81,099.39 |

All scenarios build without errors and give 93 rows.

**How A0 is calculated**

| Step | Rule | Variable |
|---|---|---|
| 1 | keep the newest `LAST_EXTRACTED_AT` per (`SEGMENTS_DATE`, `CUSTOMER_ID`, `CAMPAIGN_ID`); older versions discarded, never summed | `latest_grain: campaign` |
| 2 | same newest timestamp, different costs → the higher cost | `tie_break: highest` |
| 3 | cost = `METRICS_COST_MICROS / 1,000,000` | — |
| 4 | EUR = cost / `RATE` of the window containing noon UTC of the spend date; EUR accounts use 1 | `fx_method: daily` |
| 5 | country from `DOMAIN` (seed), brand from `BRAND`, channel from `CHANNEL_TYPE` | `channel_source: channel_type` |
| 6 | sum per date × country × brand × channel, round to cents, count distinct campaigns | — |

---

## Q1. Grain of the latest entry

`latest_grain` · **Impact: high (−42%)** · not a real ambiguity, but an easy trap, so it is tested and quantified.

| Option | Meaning |
|---|---|
| **`campaign`** (default) | newest extraction **per campaign-day**; a campaign missing from a newer extraction keeps its last value |
| `account_snapshot` | the account-day's newest extraction is the whole truth; campaigns missing from it are dropped |

- **Why the default:** the brief says the cost of each *campaign* is extracted multiple times, and the data agrees: each campaign has its own extraction times. HU on 15 Jan: 12 campaigns, but the newest extraction holds one → €17.58 instead of €2,580.99. In January 1,355 of 2,358 campaign-days are missing from their account's newest extraction.
- **When the alternative is right:** if every extraction requested all campaigns of the account, a missing campaign really had zero spend (Google Ads omits zero rows). The source does not say which campaigns each run requested, and this data shows extractions that hold only some campaigns, so the per-campaign rule is the safe one.
- **Key:** (`spend_date`, `account_id`, `campaign_id`). Country, brand and channel are account attributes joined after the dedup, not part of the key.
- **Tests:** `latest_extraction_wins_per_campaign`, `account_snapshot_scenario_drops_campaign_missing_from_newer_extraction`.

## Q2. Ties

`tie_break` · **Impact: low (0.53%, 103 campaign-days)** · `ASSUMPTION`

| Option | Meaning |
|---|---|
| **`highest`** (default) | the higher cost: conservative for spend |
| `lowest` | the lower cost |

- **Why the default:** the brief does not cover ties and the source has no tie-breaker (no sequence number; the CSV is sorted neither by date nor by extraction time). The higher value never understates spend, so cost-per-booking is never flattered. Example: `c5d43633` on 8 Jan, €1,848.89 and €2,119.52, both extracted 10 Jan 02:00 → €2,119.52.
- **Why ties exist and why not the sum:** [DQ #2](DATA_QUALITY.md#2-same-timestamp-ties).
- **Third option, not a variable:** set `warn_same_timestamp_conflicts` to `severity='error'` and stop publishing until the source owner resolves ties. Not the default: it would hold the whole report over 0.5% of spend.

## Q3. Exchange rate

`fx_method` · **Impact: very low (+€71.55, BR only)**

| Option | Meaning |
|---|---|
| **`daily`** (default) | the rate valid at noon UTC of the spend date |
| `monthly_average` | mean of the daily rates of the month (including weekend values) |

- **Why the default:** the FX table is daily with explicit validity windows. Finance often reports with monthly averages, so the alternative is ready.
- `LIMITATION` in a month only partly covered by the FX table, the average uses the available days. `PROD` monthly rates come from Finance as a separate table.
- **Test:** `monthly_average_scenario_uses_mean_rate_of_the_month`.

## Q4. Channel

`channel_source` · **Impact: none on totals, only labels**

| Option | Meaning |
|---|---|
| **`channel_type`** (default) | `CHANNEL_TYPE`: nonbrand, mixed, … |
| `ad_platform` | `AD_PLATFORM`: Google, Bing, YouTube |

**Why the default:** all accounts with spend are on Google, so `ad_platform` gives one value and no breakdown; `CHANNEL_TYPE` is the marketing meaning (brand vs non-brand). Both at once would add a column to the grain: a model change, not a flag.

## Q5. Country

No variable · **Impact: none on totals, only attribution** · `ASSUMPTION`

Country comes from `DOMAIN` via `seeds/domain_country.csv`; unmapped → `UNKNOWN` + warning. Ambiguous domains (`com`, `app`, `us_es`, `gh`) have no spend in the sample.

---

## Changing a rule

| If the business decides | Change |
|---|---|
| each extraction is a full account snapshot | `latest_grain: account_snapshot` in `dbt_project.yml` |
| ties take the lower value | `tie_break: lowest` in `dbt_project.yml` |
| ties must be resolved before reporting | `severity='error'` in `tests/warn_same_timestamp_conflicts.sql` |
| Finance uses monthly average rates | `fx_method: monthly_average` in `dbt_project.yml` |
| channel means platform | `channel_source: ad_platform` in `dbt_project.yml` |
| there is an official account-country mapping | replace `seeds/domain_country.csv` |
| unmapped accounts must never be published | `severity='error'` in `tests/warn_unmapped_country.sql` |
