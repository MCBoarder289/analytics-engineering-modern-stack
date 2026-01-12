# Analytics Engineering in the Modern Data Stack

This repository is a monorepo that contains an example full-stack analytics engineering implementation.
The following sections will describe how to get this running locally in your environment.

## Local Setup
### Prerequisites:
1. Install [uv](https://docs.astral.sh/uv/getting-started/installation/) to manage the various virtual environments, python versions, and dependencies
2. Install [docker](https://docs.docker.com/desktop/) to manage the containers (mainly for metabase and its backing postgres db)

### Installing Virtual Environments
There are two virtual environments that need to be set up.

The first is for the overall project here, which will include dependencies for data generation, dbt, and linting/formatting.

From the root of this repository, simply run:
```bash
uv sync
```

The second is the repository for the dagster project, which has its own dependencies.
In another terminal, navigate to the dagster project and then run uv sync as follows:

```bash
cd ./analytics_system

uv sync
```

### Generating data
This repo will generate some data that will be used for all of the pipelines.

To generate that data, simply run the following command from the top of this repository:
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

### Running Dagster
Dagster will be the "single pane of glass" application where you will run the various pipelines to create the assets.

To run dagster, in your terminal, move to the `analytics_system` directory and run the `dg dev command as follows:

```bash
cd ./analytics_system

uv run dg dev
```

Then open your browser to the url in the log that shows `Serving dagster-webserver on http://<your local host>:<port>` and you should see the Dagster UI.

### Running Metabase
Once you have built your data warehouse via Dagster, you can run metabase as a container using docker.

To do so, in your terminal simply navigate to the `data_vis_metabase` directory and run the docker compose commands as follows:

```bash
cd ./data_vis_metabase

docker compose up -d  
```

Once your docker images are up and running, open your browser to: [http://localhost](http://localhost/).

If this is your first time using the container, you need to fill out data for metabase and set up your warehouse.

You will select `DuckDB` from the options and use the following path for the warehouse:
```
/warehouse/warehouse_dev.duckdb
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

#### Clear Metabase BI/Visualization state
```bash
uv run python manage.py reset metabase
```

#### Clear all state
```bash
uv run python manage.py reset all
```

## High Level Structure of the monorepo

```
.
├── analytics_system
├── call_center
├── data
├── data_generation
├── data_vis_metabase
└── notes
```

### analytics_system
This is the directory that houses the [dagster](https://docs.dagster.io/) and orchestration code.

It is its own self-contained module that co-exists next to items like `call_center` so that it can reference the dbt definitions/lineages.
This is also the layer that directly defines the ingestion pipelines from the raw data sources.

### call_center
This is the [dbt](https://docs.getdbt.com/) project, which defines all of the templated dynamic SQL for the project.
In other words, this houses all of the transformation logic that models our data warehouse from the ingested data.

### data
This directory houses the raw data for each datasource, as well as the actual data warehouse duckdb files.

### data_generation
This module holds the logic the is used to simulate the call center.
It is used by the `manage.py` cli command `generate-source-data`, and also manages the creation of the agents, managers, and their relational assignments

### data_vis_metabase
This is where the docker logic lives that will run the [Metabase](https://www.metabase.com/) business intelligence tool.
The postgres database that backs the Metabase instance's state will also live here.

### notes
This is just where internal notes, presentations, and any other miscellaneous items are stored.
