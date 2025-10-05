# analytics_system

This is the module that hosts the primary logic for the entire end-to-end orchestration of the data pipelines.
The name `analytics_system` is just to represent that this portion of the codebase is the main system through which analytics are built.

## Getting started

### Installing dependencies

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

## Managing Environments

### Setting up Dagster
You need to make your Dagster environment persistent so that when you run assets/pipelines, those states are saved.
To do this, you need to copy the `.env.example` file to a `.env` files and update the path to be the specific location.

There is a helper function to do this for you:
```bash
uv run python ../manage.py init-env
```
If there is a .env file present, it will ask if you want to overwrite it.

### Cleaning up and resetting environment

Because this project runs locally, there may be times when you need to clear things out and start fresh.
The following commands should help you reset state for the various components (dlt state, warehouse state, dagster state, etc.)
You need to run these commands from the root of this project.

### Clear dagster state
```bash
uv run python ../manage.py reset-dagster
```

### Clear dlt state
```bash
uv run python ../manage.py reset-dlt
```

### Clear warehouse state
```bash
uv run python ../manage.py reset-warehouse
```

### Clear all state
```bash
uv run python ../manage.py reset-all
```

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

### Launching assets/pipelines from the dg cli
For a partition range:
```bash
dg launch --assets dlt_filesystem_calls_source_calls --partition-range 2025-01-01...2025-01-05
```
Or a single partition:
```bash
dg launch --assets dlt_filesystem_surveys_source_surveys --partition 2025-02-01
```


Open http://localhost:3000 in your browser to see the project.

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
