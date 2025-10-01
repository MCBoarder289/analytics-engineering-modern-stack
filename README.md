# Analytics Engineering in the Modern Data Stack

## Initial data setup

The data_generation directory contains the code that will generate data for the project.
It contains logic that can be tweaked to produce different datasets.

Currently, everything is keyed off of the "call date", but the surveys are intentionally keyed off of the survey's response date.
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

## Managing Environments

### Generating source data
```bash
uv run python manage.py generate-source-data
```

### Setting up Dagster
You need to make your Dagster environment persistent so that when you run assets/pipelines, those states are saved.
To do this, you need to copy the `.env.example` file to a `.env` files and update the path to be the specific location.

There is a helper function to do this for you:
```bash
uv run python manage.py init-env
```
If there is a .env file present, it will ask if you want to overwrite it.

If you want to do this without answering the prompts, simply pass the `-no-prompt` flag:
```bash
uv run python manage.py init-env --no-prompt
```

### Cleaning up and resetting environment

Because this project runs locally, there may be times when you need to clear things out and start fresh.
The following commands should help you reset state for the various components (dlt state, warehouse state, dagster state, etc.)
You need to run these commands from the root of this project.

You can also combine any options (dagster, dlt, warehouse, source-data) into a single command.
Simply provide the options after one another, as in this example that will clear eveyrthing but the source data:

```bash
uv run python manage.py reset dagster dlt warehouse
```

#### Clear dagster state
```bash
uv run python manage.py reset dagster
```

#### Clear dlt state
```bash
uv run python manage.py reset dlt
```

#### Clear warehouse state
```bash
uv run python manage.py reset warehouse
```

### Clear Metabase BI/Visualization state
```bash
uv run python manage.py reset metabase
```

#### Clear all state
```bash
uv run python manage.py reset all
```
