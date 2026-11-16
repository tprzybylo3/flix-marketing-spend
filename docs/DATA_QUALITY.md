# Data quality findings

Every issue in the January 2025 sample, with real rows, how the pipeline handles it and which test reports it. IDs are shortened; HU (`607cb5`) and DE (`471a02`) are EUR accounts, so amounts are euros. Back to [README](../README.md).

| # | Finding | Size | Handling | Reported by |
|---|---|---|---|---|
| [1](#1-repeated-extractions) | Repeated extractions: Google revises a campaign-day's cost up to 9 days later; the newest extraction holds only some campaigns | 1,392 of 2,358 campaign-days with 2 versions (spend dates 1–15 Jan) | latest per campaign-day, 14-day window | unit test `latest_extraction_wins_per_campaign`; `warn_source_newer_than_mart`, `warn_mart_drift_at_full_grain` |
| [2](#2-same-timestamp-ties) | Two costs with the same timestamp | 103 campaign-days | higher cost `ASSUMPTION` | `warn_same_timestamp_conflicts` |
| [3](#3-spend-dates-67-january-separate-process) | Spend dates 6–7 Jan stamped 10:45:35 by a separate process | 422 rows | no special handling: latest version per campaign-day, like every other date | — |
| [4](#4-extracted-before-the-spend-date) | Extracted before the spend date | 334 rows | superseded later | `warn_extracted_before_spend_date`, `warn_latest_extraction_before_spend_date` |
| [5](#5-exact-duplicates) | Exact duplicate rows | 5 rows | removed by the dedup | — |

List the rows behind the warnings:

```bash
uv run dbt test --select warn_same_timestamp_conflicts warn_extracted_before_spend_date --store-failures
uv run dbt show --limit 200 --inline "select * from main_dbt_test__audit.warn_same_timestamp_conflicts order by spend_date"
```

---

## 1. Repeated extractions

The brief: each campaign's cost "is extracted multiple times as values may vary", and the latest entry is the most accurate. Three properties of the data shape three parts of the pipeline:

| Property | Pipeline decision | Section |
|---|---|---|
| Google revises the cost of a campaign-day in later extractions (we change nothing) | **only the newest version counts, nothing is summed** | [1a](#1a-revised-values-only-the-newest-version-counts) |
| one main extraction, small ones around it | the pattern behind the corrections | [1b](#1b-how-one-spend-date-is-extracted) |
| corrections arrive up to 9 days late | **14-day incremental window** | [1c](#1c-how-late-corrections-arrive-the-14-day-window) |
| an extraction holds only some campaigns | **key = campaign-day**, not account-day | [1d](#1d-partial-extractions-the-key-is-the-campaign-day) |

### 1a. Revised values: only the newest version counts

Google sends a new value for the same campaign-day in a later extraction; it can be higher or lower. One HU campaign, spend date 13 January:

| campaign | SEGMENTS_DATE | LAST_EXTRACTED_AT | cost |
|---|---|---|---|
| c5d43633 (HU) | 2025-01-13 | 2025-01-15 02:00 | 2,291.88 |
| c5d43633 (HU) | 2025-01-13 | 2025-01-22 02:00 | **2,133.08** ← used |

### 1b. How one spend date is extracted

**What this shows:** every spend date gets one main extraction two days later with all its campaigns, and then small extractions of a few campaigns that bring revised values over the next days. This is where the "up to 9 days" in [1c](#1c-how-late-corrections-arrive-the-14-day-window) comes from, and why most campaign-days have two versions.

**Example: spend date 5 January, all three accounts together** (DE 83 campaigns, HU 20, BR 9 = 112 campaign-days). They arrive in 13 extractions, 2–14 January:

| Extracted on (02:00) | 2 Jan | 3 | 4 | 5 | 6 | **7 Jan** | 8 | 9 | 10 | 11 | 12 | 13 | 14 Jan |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **Rows for spend date 5 Jan** | 11 | 11 | 8 | 9 | 9 | **122** | 3 | 11 | 8 | 7 | 7 | 12 | 6 |

- **7 Jan (+2 days) is the main extraction:** every campaign of every account for 5 Jan, 112 campaign-days in 122 rows (10 are ties, [#2](#2-same-timestamp-ties)).
- **Other days are small re-extractions** of a few campaigns each, with revised values; those before 5 Jan should not exist ([#4](#4-extracted-before-the-spend-date)).
- **Result:** 102 campaign-days have two versions, 10 only the main one.

Spend dates 1–15 January all follow this pattern. **Spend dates 16–31 January have only the main extraction**, although the file runs to 2 February: their values are first reads that may still be corrected.

### 1c. How late corrections arrive: the 14-day window

| Extracted after the spend date | Rows |
|---|---|
| 2 days (main extraction) | 2,352 |
| 3–10 days (re-extractions) | 938 |
| 0–1 day | 234 |
| before the spend date | 334 ([#4](#4-extracted-before-the-spend-date)) |

For 1–15 January the last correction comes at **+9 days**; the only +10 rows (16) belong to 6 Jan, whose history is shifted by a day ([#3](#3-spend-dates-67-january-separate-process)).

**Why 14 days, not 10** `ASSUMPTION` The latest revision comes 9 days after the spend date (10 for 6 Jan). The window `(run_date − 14, run_date]` needs one more day than the lag, and under Airflow 2's default timetable `ds` trails the run by another day. Revisions were seen only in half of one month, and Google also credits invalid clicks later, so 14 days adds margin and rounds to two weeks. **The window should be agreed with the business** (how long a number may still change after it is reported) and checked against a longer history.

Too short a window freezes wrong numbers silently; 14 dates cost one delete + insert. A correction outside the window is detected by `warn_source_newer_than_mart` and `warn_mart_drift_at_full_grain`; a backfill or full refresh repairs it.

### 1d. Partial extractions: the key is the campaign-day

Each campaign is extracted on its own schedule, so an extraction is **not a snapshot of the account**. Key: (`SEGMENTS_DATE`, `CUSTOMER_ID`, `CAMPAIGN_ID`), newest `LAST_EXTRACTED_AT` wins.

**Example: HU, spend date 15 January.** 12 campaigns, extracted between 12 and 21 January; the account's newest extraction (22 Jan) holds only one of them, `cf432577` (€17.58).

| Approach | Logic | HU, 15 Jan |
|---|---|---|
| **Latest per campaign (used)** | `partition by spend_date, account_id, campaign_id order by extracted_at desc` | **€2,580.99** |
| Latest per account (trap) | keep only the account-day's newest extraction | €17.58 |

Across January the trap drops 57% of campaign-days and **42% of spend** (scenario `account_snapshot`, [SCENARIOS](SCENARIOS.md#q1-grain-of-the-latest-entry)).

---

## 2. Same-timestamp ties

Two different costs for one campaign-day with identical `LAST_EXTRACTED_AT`, so "latest" cannot decide:

| SEGMENTS_DATE | campaign | LAST_EXTRACTED_AT | cost |
|---|---|---|---|
| 2025-01-08 | c5d43633 (HU) | 2025-01-10 02:00 | 1,848.89 |
| 2025-01-08 | c5d43633 (HU) | 2025-01-10 02:00 | **2,119.52** ← used |

- **Why they exist:** the file has only two clock times, 02:00:00 (3,436 rows) and 10:45:35 (422 rows). `LAST_EXTRACTED_AT` is a **batch start, not a row time**, so two reads in one batch get the same stamp.
- **Two versions, not two halves:** could the two values be parts of one cost that should be added? Compared with the campaign's usual daily cost (median of its days without a tie), the higher value is typically 1.25× the usual cost, while the sum would be 2.25×, roughly double. In 86% of ties a single value is closer to the usual cost than the sum. So the two values are two readings of the same cost: one is taken, never the sum.
- **Rule** `ASSUMPTION` the higher cost: never understates spend. Impact: €344,117.72 (lower) vs **€345,950.02** (higher), 0.5%.
- **Fix:** in the extractor ([what the source should add](#what-the-source-should-add)).

## 3. Spend dates 6–7 January: separate process

All 422 rows of spend dates 6 and 7 Jan are stamped **10:45:35** instead of 02:00, and both histories run on the same 13 days, 4–16 January:

| Extracted (10:45:35) | 4 | 5 | 6 | 7 | 8 | **9** | 10 | 11 | 12 | 13 | 14 | 15 | 16 Jan |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **spend date 6 Jan** | 6 | 10 | 6 | 8 | 2 | **116** | 7 | 10 | 6 | 14 | 5 | 12 | 16 |
| **spend date 7 Jan** | 7 | 7 | 6 | 8 | 5 | **109** | 6 | 7 | 17 | 8 | 11 | 5 | 8 |

- **Main extraction on 9 Jan** (all 211 campaign-days): +2 days for 7 Jan, +3 for 6 Jan, so 6 Jan reaches +10 days.
- **The regular job did not fail:** every day 4–16 Jan has both a 02:00 run (all other spend dates, never 6–7 Jan) and a 10:45:35 run (only 6–7 Jan).
- **Reading:** the two dates were routed to or re-stamped by a separate process; only the source owner can say which. It may be an artefact of preparing the sample.
- **Pipeline:** no special handling, these dates follow the same rules as every other date. 14 campaign-days appear twice in the 9 Jan extraction with different costs: 14 of the 103 ties.

## 4. Extracted before the spend date

334 rows claim to be extracted before the money was spent. HU, spend date 15 Jan:

| campaign | LAST_EXTRACTED_AT | cost |
|---|---|---|
| 0451bb87 | **2025-01-12** 02:00 | 0.42 |
| 0451bb87 | 2025-01-17 02:00 | 0.42 ← used |
| 56251146 | **2025-01-14** 02:00 | 32.57 |
| 56251146 | 2025-01-17 02:00 | 32.57 ← used |

- **Final file:** every such row has a later version, so none reaches the output.
- **Daily feed:** until the main extraction two days later, such a row is the only version of its campaign-day. So both tests are **warnings**: they flag it without stopping the run.
- **Cause:** a time zone explains hours, not 2–3 days. In production a broken extractor timestamp, a question for the source owner; here more likely how the sample was prepared (the extra version falls on a random day between −3 and +9).

## 5. Exact duplicates

5 rows are exact copies of another row (all columns equal), e.g. HU `b8232859`, spend date 2 Jan, extracted 4 Jan, €41.88 twice. Harmless: the dedup keeps one.

---

## Reference data

**Country** comes from the account `DOMAIN` (`seeds/domain_country.csv`); `LANGUAGE` only confirms it (`pt` alone could be Portugal or Brazil).

| account | DOMAIN | LANGUAGE | brand | spend | country |
|---|---|---|---|---|---|
| 471a02 | de | de | FlixTrain | **yes** | DE |
| 607cb5 | hu | hu | FlixBus | **yes** | HU |
| 35d2da | br | pt | FlixBus | **yes** | BR |
| f78773 | br | pt | FlixBus (YouTube) | no | BR |
| 3834ab | us_es | es | FlixBus | no | US |
| 996714 | gh | en | Greyhound | no | US `ASSUMPTION` (also Canada, Mexico) |
| 34e8ae | app | All | FlixBus (App Ads) | no | ambiguous |
| f17fae | com | en | Archived (Bing) | no | ambiguous |

The three accounts with spend are unambiguous. If an ambiguous one starts spending, `warn_unmapped_country` reports it.

**Exchange rates**

| Finding | Example | Handling |
|---|---|---|
| carried over weekends | BRL 4–6 Jan = 6.3478 | used as defined |
| DST change | 2025-03-30 window runs `+0100` → `+0200` (23 hours) | windows converted to UTC, rate at noon UTC |
| obsolete currency | HRK 7.5345 (Croatia uses EUR since 2023) | no account uses it |
| partial coverage | PEN only from 2025-03-15 | a PEN account in January would fail the missing-rate test |

---

## What the source should add

`LAST_EXTRACTED_AT` already orders different extraction runs, but it is the start time of a run, so two reads of the same campaign-day within one run get the same value: the 103 ties ([#2](#2-same-timestamp-ties)). Two extra columns, written by the extractor or Airflow into each row, would make the order unique. They cannot be added in dbt, because they describe the moment of extraction.

| Column | What it is | Solves |
|---|---|---|
| `try_number` | which attempt of the run wrote the row: 1, then 2 after a retry (Airflow `{{ ti.try_number }}`) | a retry beats the failed attempt of the same run |
| `request_seq` | a counter the extractor increases with every API request within one attempt | two reads of one campaign-day in the same run: the ties in this file |

Dedup then becomes `order by last_extracted_at desc, try_number desc, request_seq desc`. Example: the tied campaign from 9 Jan, read twice in the same run:

| campaign | LAST_EXTRACTED_AT | try_number | request_seq | cost |
|---|---|---|---|---|
| 0268591e | 2025-01-09 10:45:35 | 1 | 41 | 20.79 |
| 0268591e | 2025-01-09 10:45:35 | 1 | 87 | **54.07** ← latest |

The `request_seq` values are made up: in that run the campaign was read in the 41st and in the 87th API request. Only the order matters; the higher number is the later read.
