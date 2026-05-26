# Call Center dbt Project

This directory is the [dbt](https://docs.getdbt.com/) project for the course. It contains all of the transformation logic that models the call center data warehouse from the ingested raw data.

## What is dbt?

dbt (data build tool) is an open-source framework that lets you transform data in your warehouse using SQL. You write modular SQL `SELECT` statements (called *models*), and dbt handles dependency resolution, testing, and documentation. It follows software engineering best practices — version control, testing, and CI/CD — applied to data transformation.

## What's in this project?

| Directory / File | Description |
|---|---|
| `models/sources/` | Declares the raw source tables ingested by the pipelines |
| `models/staging/` | Light cleaning and standardization of raw source data |
| `models/ops_analysis/` | Intermediate models for call center operational analysis |
| `models/data_marts/` | Final, business-facing data mart models |
| `seeds/` | Static CSV data (agents, managers, customers, assignments) loaded into the warehouse by dbt |
| `tests/` | Custom data quality tests |
| `macros/` | Reusable Jinja/SQL macros |
| `dbt_project.yml` | Main dbt project configuration |
| `packages.yml` | dbt package dependencies |
