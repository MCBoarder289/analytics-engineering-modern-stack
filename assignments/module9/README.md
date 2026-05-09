# Module 9 Assignment — Data Testing and Validation

## Overview

A dbt model is only as trustworthy as its tests. In this assignment you will add **data quality
assertions** to the `properties.yml` files across all three model layers (staging, data marts, and
ops analysis) and verify that those tests pass in Dagster.

Tests serve as the contract between your pipeline and its consumers. When a test fails, it means a
business assumption has been violated — the data cannot be trusted until the root cause is
understood and resolved.

---

## Setup

```bash
# Install the assignment stubs
uv run python manage.py assignment --module 9

# Restore the finished answer key at any point
uv run python manage.py assignment --module 9 --restore
```

The command above replaces the three `properties.yml` files in your live dbt project with stub
versions that contain `# TODO` comment blocks. Your job is to fill in the missing tests.

---

## Part A — Staging Layer (`staging/properties.yml`)

### Task

Open `call_center/models/staging/properties.yml` and complete the two TODO sections.

**1. `stg_surveys` — Accepted value range tests**

The survey platform records two scores per response:

- **CSAT** — Customer Satisfaction, rated on a **1–5 star scale**. Customers cannot submit a score
  below 1 or above 5. Any value outside this range is a data collection error.
- **NPS** — Net Promoter Score, collected on a **0–10 scale**. A response of 0 means extremely
  unlikely to recommend; 10 means extremely likely. Any value outside 0–10 is invalid.

Add `accepted_values` tests to both columns to enforce these ranges.

**2. `stg_calls` — Temporal and magnitude integrity tests**

The telephony system records a start and end timestamp for every call, as well as a computed
duration in seconds. Two assertions must always hold:

- A call's end timestamp must be strictly after its start timestamp.
- A call's duration must be positive (greater than zero). A zero-second call cannot represent a
  real customer interaction.

Add `dbt_utils.expression_is_true` tests at the **model level** (in a `tests:` block, not under a
column) to enforce these constraints.

---

## Part B — Data Marts Layer (`data_marts/properties.yml`)

### Task

Open `call_center/models/data_marts/properties.yml` and complete the TODO sections in three models.

**1. `mart_calls` — Call record integrity**

Same business rules as `stg_calls`: end must be after start, duration must be positive. Add the
same two model-level `expression_is_true` tests here.

> *Why test the same thing in staging AND in the mart?* Tests at the staging layer catch raw
> ingestion issues. Tests at the mart layer catch transformation bugs — a join or COALESCE could
> silently corrupt a timestamp even if the source data was clean.

**2. `mart_first_call_resolution` — Binary indicator constraints**

FCR is computed as a rate: `sum(fcr_num) / sum(fcr_denom)`. Two columns drive this calculation:

- **`fcr_denom`** — represents one FCR opportunity per call. This field is always `1`. Any other
  value would silently inflate or deflate all downstream FCR rates.
- **`fcr_num`** — binary indicator: `1` if the call was resolved on first contact, `0` if the
  customer called back within 3 days about the same issue. No other values are valid.

Add `accepted_values` tests to both columns to enforce their valid ranges.

**3. `mart_surveys` — NPS classification and temporal integrity**

- **`nps_calc`** — the derived classification used in NPS rate calculations: `1` for Promoters
  (NPS 9–10), `0` for Passives (NPS 7–8), `-1` for Detractors (NPS 0–6). Any other value
  indicates a logic error in the `mart_surveys` model.
- A survey response timestamp must always be on or after the survey's sent timestamp. A customer
  cannot respond to a survey before it is sent.

Add an `accepted_values` test for `nps_calc` and a model-level `expression_is_true` test for the
temporal constraint.

> **Reference:** Look at `mart_crm_callbacks` in the same file. It already has two
> `expression_is_true` tests — one ensuring `original_crm_id <> callback_crm_id` and one ensuring
> `callback_created_ts > original_created_ts`. Use these as a syntax reference for your own tests.

---

## Part C — Ops Analysis Layer (`ops_analysis/properties.yml`)

### Task

Open `call_center/models/ops_analysis/properties.yml`. Each of the five metric models
(`daily_agent_metrics`, `daily_agent_manager_metrics`, `monthly_agent_metrics`,
`monthly_agent_manager_metrics`, `monthly_manager_metrics`) contains a `# TODO` comment block.

Add `dbt_utils.expression_is_true` tests to **all five models** for the following metrics:

| Column | Business Rule | Null handling |
|---|---|---|
| `customer_resolution_time` | Average call duration — must be ≥ 0 | Always present when there are calls |
| `avg_hold_time` | Average hold time — can be zero but never negative | Always present when there are calls |
| `transfer_rate` | Proportion of calls transferred — a rate, must be between 0 and 1 | Always present |
| `first_call_resolution` | FCR rate — must be between 0 and 1 | **Can be NULL** if no CRM data joined; write a null-safe assertion |
| `csat` | Average CSAT score — based on 1–5 scale | **Can be NULL** if no surveys; write a null-safe assertion |
| `nps` | Net Promoter Score — ranges from -100 to 100 | **Can be NULL** if no surveys; write a null-safe assertion |

> **Hint on null-safe assertions:** `expression_is_true` fails on `NULL` (a NULL expression is not
> "true"). For columns that can be NULL, wrap your expression:
> `"col is null or (col >= X and col <= Y)"`

---

## Running Your Tests

### Via Dagster

```
# In the Dagster UI, trigger the `call_center_dbt` asset group
# Tests run automatically as part of the dbt execution
```

### Via dbt CLI

```bash
cd call_center
uv run dbt test --select staging
uv run dbt test --select data_marts
uv run dbt test --select ops_analysis
```

### Verify all tests pass before submitting

```bash
uv run dbt test
```

---

## Written Response Questions

Answer the following in your submission document:

1. **Testing philosophy:** Why do we add `not_null` and `unique` tests at the staging layer rather
   than (or in addition to) the mart layer? What category of bugs does each layer catch?

2. **Null-safe assertions:** Explain why `first_call_resolution`, `csat`, and `nps` can be NULL in
   the ops analysis models, but `customer_resolution_time`, `avg_hold_time`, and `transfer_rate`
   cannot. What does this tell you about how those columns are computed?

3. **Binary indicators:** The `fcr_denom` field is always `1`. Why is it useful to store a field
   that never changes, and why is it important to test for this? What would happen to downstream
   dashboards if a data pipeline bug set `fcr_denom` to `2` for a subset of rows?

4. **Temporal assertions:** You added `end_ts > start_ts` to both `stg_calls` and `mart_calls`.
   Describe a realistic data pipeline scenario where a test could pass in staging but fail in the
   mart (or vice versa). Why is redundancy in testing valuable?

5. **Production impact:** Imagine you are on call and a Dagster alert fires at 2 AM because
   `transfer_rate between 0 and 1` is failing. Walk through your investigation steps. What are the
   most likely root causes?
