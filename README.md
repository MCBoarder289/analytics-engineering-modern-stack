# Analytics Engineering in the Modern Data Stack

## Initial data setup

The data_generation directory contains the code that will generate data for the project.
It contains logic that can be tweaked to produce different datasets.

Currently, everything is keyed off of the "call date", but the surveys are intentionally keyed off of "survey" date".
This is to emulate late-arriving data realistically.

## Sequence of project development
After setting up the primary data model, the next steps will follow the DAG linearly:
* ELT jobs to ingest the source data
  * Since [dlt](https://dlthub.com/docs/intro) will be used, going to introduce a [Dagster](https://docs.dagster.io/) project scaffolding
* Transformational layer
  * [dbt](https://docs.getdbt.com/docs/introduction) will be used for this, so we will scaffold a dbt project after working through the ELT jobs.
* BI/Visualization layer
* Operationalization work
  * Buttoning up the repo for spinning up the entire system
    * Data setup
    * Running core platform/jobs
    * Cleanup/reset of state
  * Injecting scenarios to work around:
    * Duplicate data
    * Errors, etc.