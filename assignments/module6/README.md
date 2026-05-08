# Module 6 Assignment: Advanced dbt — Incrementals & Metrics

## Overview

This assignment has two parts. In Part 1 you'll translate a business metric definition into a
SQL expression. In Part 2 you'll evolve an existing model to incorporate new source data and
reason about the downstream impact on a key performance indicator.

Both parts reinforce the same theme: **the mart layer is where business logic lives**, and
changing that logic requires understanding both the data model and the business context.

---

## Setup

Your instructor has already run:
```bash
python manage.py assignment --module 6
```

This replaced `mart_surveys.sql` and `mart_first_call_resolution.sql` with stub versions.
Your environment should already be initialized and data generated from Module 5.

---

## Part 1 — NPS Calculation (`mart_surveys.sql`)

### Background

After every call, customers receive a short survey. One of the questions is:

> *"On a scale of 0–10, how likely are you to recommend this service to a friend or colleague?"*

This is the **Net Promoter Score (NPS)** question. We store the raw response in the `nps` column
(integer, 0–10). But for analysis and dashboards, we need a *calculated* value that reflects the
standard NPS classification:

| Raw Score | Category   | Calculated Value |
|-----------|------------|-----------------|
| 0 – 6     | Detractor  | **-1**          |
| 7 – 8     | Passive    |  **0**          |
| 9 – 10    | Promoter   | **+1**          |

The `nps_calc` field is later summed and averaged across agents to produce a score between -1
and +1. A positive value means more promoters than detractors; negative means the opposite.

### Your task

Open `mart_surveys.sql`. Find the `TODO` block and implement the `nps_calc` column using a
`CASE` expression that maps the raw `nps` score to -1, 0, or +1.

After implementing, run the dbt model and verify the `accepted_values` test on `nps_calc`
passes (valid values are -1, 0, 1).

**Written response:**
- Look at the `daily_agent_survey_metrics_source.sql` model. How is `nps_calc` aggregated there?
- What does a manager-level `nps` value of `0.15` mean in plain English?

---

## Part 2 — FCR Evolution (`mart_first_call_resolution.sql`)

### Background

**First Call Resolution (FCR)** measures whether a customer's issue was resolved on the first
call — without needing to call back. Our current definition:

> A call **fails** FCR if the same customer calls back within 3 days with the **same reason code
> and sub-reason code**.

This logic lives in the `disqualifying_callbacks` CTE in `mart_first_call_resolution.sql`.

### The new data

Agents have started flagging in the CRM system whether a callback is about a previous issue.
This field — `previous_issue_flag` — is captured per CRM record and flows through to
`mart_crm_callbacks` as `callback_previous_issue_flag`.

The business has decided:

> *"If an agent flags that a callback is about a previous issue, that callback should disqualify
> the original call from FCR — regardless of whether the reason codes match."*

### Your task

1. Open `mart_first_call_resolution.sql`. Find the `TODO` block in `disqualifying_callbacks`.

2. Add `callback_previous_issue_flag` as an **additional disqualifying condition** using `OR`.
   A callback now disqualifies if:
   - It has the same reason AND sub-reason code, **OR**
   - The agent flagged it as being about a previous issue

3. Re-run the full dbt pipeline (you may need to do a full refresh on FCR and downstream models).

4. Compare `first_call_resolution` rates in `daily_agent_metrics` before and after your change.

**Written response:**
- Did FCR go up or down after the change? Explain why.
- Look at `mart_crm_callbacks`. What percentage of callbacks have `callback_previous_issue_flag = true`
  that do *not* already have matching reason codes? (These are the "new" disqualifiers your change adds.)
- Is the updated definition more or less strict? Is it a more accurate measure of FCR? Argue your position.
- What are the risks of changing a KPI definition mid-stream in a production analytics system?
  How would you communicate this change to stakeholders?

---

## Deliverables

1. Updated `mart_surveys.sql` with `nps_calc` implemented
2. Updated `mart_first_call_resolution.sql` with `callback_previous_issue_flag` added
3. Screenshots: passing dbt tests after each change
4. Written responses to both sets of questions
