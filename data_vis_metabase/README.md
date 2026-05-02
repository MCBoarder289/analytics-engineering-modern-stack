# Metabase for Data Visualization

This directory holds the code to create a Docker image and run a [Metabase](https://www.metabase.com/) instance for data visualization. Metabase has an open source offering, which is what we will be using here.

## Installing Docker

Docker is required to run Metabase locally. To install it, download and install [Docker Desktop](https://docs.docker.com/desktop/) for your operating system (Mac, Windows, or Linux). Docker Desktop includes both the Docker engine and the `docker compose` CLI tool used below.

Once installed, verify Docker is running by opening a terminal and running:

```bash
docker --version
```

## Running Metabase

This project uses Docker Compose, which will **automatically build the custom Metabase image** (bundled with the DuckDB driver) and start all required containers — no separate `docker build` step is needed.

From this directory, simply run:

```bash
docker compose up -d
```

Once the containers are up, open your browser to [http://localhost](http://localhost/).

> See the top-level [README](../README.md) for full setup instructions, including how to configure your DuckDB warehouse connection on first launch.