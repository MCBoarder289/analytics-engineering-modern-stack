# Module 8 Assignment: Medallion Architecture — Gold Layer Aggregations

## Overview

You've already built the staging (silver) and mart (silver/gold boundary) layers. In this
assignment you'll complete the **gold layer**: the aggregated, analysis-ready tables that power
dashboards and management reporting.

These models sit at the top of the `ops_analysis/` directory and answer questions like:
- How is each agent performing day-to-day?
- How does an agent's FCR rate trend month-over-month?
- How does my team compare to other teams on key metrics?

---

## Background: Medallion Architecture

| Layer  | Our models                        | Characteristics                          |
|--------|-----------------------------------|------------------------------------------|
| Bronze | `raw_calls`, `raw_crm`, `raw_surveys` | Raw ingest, no transforms            |
| Silver | `stg_*`, `mart_*`                 | Cleaned, deduplicated, business keys     |
| Gold   | `daily_agent_metrics`, `monthly_*` | Aggregated, dashboard-ready             |

The gold layer is optimized for **reads**, not writes. BI tools (Metabase, Tableau, etc.) query these
tables directly. Getting the granularity and join keys right is critical.

---

## Setup

Initialize your environment if you haven't already.
From the root of the repo directory, run the following (answer Y when prompted to reset your state):
```bash
uv run python manage.py init-env
```

On your branch, you need to set up this scenario by running:
```bash
uv run python manage.py assignment --module 8
```

This replaced the ops_analysis aggregation models with stubs. The three **source models** that
feed into them are untouched and will run as-is:
- `daily_agent_call_metrics_source` — per-agent daily call KPIs
- `daily_agent_crm_metrics_source`  — per-agent daily FCR numerators/denominators
- `daily_agent_survey_metrics_source` — per-agent daily survey KPIs

Inspect these source models before writing any aggregation code — they define all the columns
and pre-aggregate sums that your models will consume.

---

## Part 1 — Agent-level daily and monthly metrics

### `daily_agent_metrics.sql`

This model produces one row per agent per day with all call center KPIs joined together.

Implement three aggregation CTEs from the source models:

**`daily_call_metrics`** — group by `agent_id, agent_name, call_date`
- `call_count`: total calls
- `customer_resolution_time`: average call duration (`sum(duration_sum) / sum(call_count)`)
- `avg_hold_time`: average hold time (`sum(agent_hold_time_sum) / sum(call_count)`)
- `transfer_rate`: fraction of calls transferred (`sum(transfer_sum) / sum(call_count)`)

**`daily_crm_metrics`** — group by `agent_id, agent_name, call_date`
- `first_call_resolution`: FCR rate (`sum(fcr_sum) / sum(fcr_denom)`)

**`daily_survey_metrics`** — group by `agent_id, agent_name, survey_date` (alias as `call_date`)
- `survey_count`, `csat`, `nps`
- `csat` is simply computed as the average of the 1-5 (think 5 star rating)
- `nps` needs to be computed correctly but you'll need to scale it properly so the output is between -100 an +100

Final SELECT: use `daily_call_metrics` as the base. Left join the other two on `agent_id + call_date`.
Use `coalesce(s.survey_count, 0)` — not every agent gets surveys every day.

### `monthly_agent_metrics.sql`

Same logic as daily, but truncate dates to month:
```sql
cast(date_trunc('month', call_date) as date) as call_month
```

---

## Part 2 — Manager-level daily and monthly metrics

### `daily_agent_manager_metrics.sql`

Same as `daily_agent_metrics` but adds `manager_id` and `manager_name` to all group-bys and
joins. The source models already include these columns (they join to `agent_assignments` and
`managers` internally).

Join key in the final SELECT: `agent_id + manager_id + call_date`.

### `monthly_agent_manger_metrics.sql`

Monthly version of the above. Truncate to `call_month`, include `manager_id` + `manager_name`.

### `monthly_manager_metrics.sql`

This model rolls up **entirely to the manager level** — no `agent_id` dimension. Every row
represents a manager's team performance for a given month.

Group by: `manager_id, manager_name, call_month` only.

Think about: what does `transfer_rate` mean at the manager level? Is it the average of each
agent's rate, or the team's overall rate? (Hint: summing the raw numerators and denominators
before dividing is more accurate than averaging the rates.)

---

## Part 3 — Verify and explore

After implementing all five models, run the full dbt pipeline.

1. Open DuckDB and write queries to answer:
   - Which agent has the highest average FCR rate over the full date range? Which has the lowest?
   - Which manager's team has the highest average CSAT in the most recent month?
   - Is there a correlation between `avg_hold_time` and `transfer_rate` at the agent level?

2. Bonus Points stretch: Sketch (or build) a simple dashboard layout in Metabase using at least two of
   these gold-layer tables.

Include your observations in the written response section

---

## Written Response

Answer the following questions:

1. Why do we store both daily and monthly aggregations as separate materialized tables rather
   than just querying the daily table with a `date_trunc` in a BI tool?

2. The `monthly_manager_metrics` model has no agent dimension. What information is *lost* when
   you aggregate to this level? Is that acceptable for its intended use case?

3. These models are all `incremental` with `delete+insert`. What would happen if you ran a
   full refresh (`dbt run --full-refresh`) on `monthly_manager_metrics`? Why might you want to
   avoid that in production?

4. Document your responses to the previous section (DuckDB queries and bonus points for plans on what a dashboard might look like)

---

## Deliverables

1. Implemented stubs for all five ops_analysis models
2. Screenshots of passing dbt runs in the Dagster UI
3. Query results for the three exploration questions in Part 3
4. Written responses to Part 3 questions
