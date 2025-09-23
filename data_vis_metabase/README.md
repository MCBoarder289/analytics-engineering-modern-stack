# Metabase for Data Visualizatoin

This directory holds the code to create a docker image and run a Metabase instance for data visualization.

Metabase has an open source offering, which is what we will be using here.

## TODOs:
* Add instructions on how to install docker
* Structure instructions for building the docker image
  * [DuckDB + Metabase Docs](https://github.com/motherduckdb/metabase_duckdb_driver?tab=readme-ov-file#docker)



## Build the image

```bash
docker build . --tag metaduck:latest`
```