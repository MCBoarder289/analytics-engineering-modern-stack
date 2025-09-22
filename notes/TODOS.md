# TODOS

## dbt stuff
* need to add instructions on setting up their local profiles.yml
  * Give example of a profiles.yml
  * ensure the databases are in the proper place

Here's how the .dbt folder and profiles.yml are created:
* Manual Creation: The .dbt folder is typically created in the user's home directory (e.g., ~/.dbt on macOS/Linux or C:\Users\<YourUser>\.dbt on Windows).
* profiles.yml Creation: Inside this .dbt folder, the user must manually create a profiles.yml file. This file contains the connection details for the data warehouse(s) dbt will interact with.
* Configuration: The profiles.yml file is then configured with the necessary credentials and connection parameters for the specific data platform (e.g., Snowflake, BigQuery, Redshift) and the corresponding profile name that the dbt project's dbt_project.yml file references.

### Difference in dbt commands
1. dbt compile
   * **Purpose**: Generates executable SQL from your dbt project files (models, tests, analyses, etc.) without actually executing them against the database.
   * **Functionality**: Resolves Jinja templating, compiles SQL code, and creates a manifest and run results artifact, but does not materialize tables or views.
   * Use **Case**: Useful for debugging and understanding the final SQL that will be executed, or for generating documentation without running transformations.
2. dbt run
   * **Purpose**: Executes compiled SQL models against your target database to materialize them (create or update tables/views).
   * **Functionality**: Connects to the database, runs the compiled SQL for selected models in the correct dependency order, and applies specified materialization strategies (e.g., table, view, incremental).
   * **Use** Case: The core command for transforming raw data into analysis-ready formats. 
3. dbt build
   * **Purpose**: An all-in-one command that orchestrates the execution of models, tests, seeds, and snapshots in a single invocation.
   * **Functionality**:
     * **Compiles**: resources.
     * **Runs**: models and seeds.
     * **Tests**: all selected resources (models, seeds, snapshots).
     * **Snapshots**: data if configured.

### dbt compile issues with incremental models
* You need to pass the vars if they're referenced in the model (ex: min_date, max_date), or else you get errors.

### Update dbt docs
* need to update docs to have the vars for incremental models
* need to update the source columns to match source tables in the sources.yml file

### dbt issues
* dealing with seeds with no partitions vs. partitions
  * Was able to solve this with a hacky single json partition (this is likely a bug in dagster)
* Ran into failures running backfills on dbt assets (write conflicts in duckdb)
  * maybe this should be solved via op tags? 
  * [see source code link](https://github.com/dagster-io/dagster/blob/fedf5745ba90331eb99485832a41171f21f123b5/python_modules/libraries/dagster-dbt/dagster_dbt/components/dbt_project/component.py#L139)
  * This looks like a similar issue: [Link](https://github.com/dagster-io/dagster/discussions/21574)
    * Initial trial didn't work...
  * notes on dagster dbt components:
    * what is possible [Link](https://docs.dagster.io/guides/build/components/building-pipelines-with-components/post-processing-components)
  * Might need to switch to the pythonic way of creating the dbt project:
    * https://docs.dagster.io/integrations/libraries/dbt/dbt-pythonic 
    * https://docs.dagster.io/api/libraries/dagster-dbt#dbt-core
      * See "prepare_if_dev" to ensure changes to dbt get refreshed when running dg dev


### Data Generation
* Fully understand the gap of survey response times
  * Right now, the "day" partition is the day we receive the surveys
  * The staging partition re-runs everything minus a lookback window to cover this diff.
* Need to figure out the `customers` dimension table
* [DONE] Get dlt to run with a single-run backfill policy
  * This was done by better understanding the context partition_key_range.
  * The default backfill policy of a single run makes sense because then we don't have to worry about that coordination. We let dagster handle that under the hood.
  * Because we're writing to separate duckdb databases in ingestion, we can parallelize the execution, which eliminates the need for the pool.
  * Notes:
    ```python
    # TODO: Configure concurrency so that it doesn't fail.
    # Concurrency notes: I was able to materialize all 89 partitions of a single asset pretty easily without any
    # concurrency limits. However, when I tried to backfill all 3 for all the same partitions, I ran into
    # a bunch of failures. Looking at the dagster run logs, if I filtered for the first failure, it was due to this error:
    # _duckdb.IOException: IO Error: Could not set lock on file ".../data_warehouse.duckdb": Conflicting lock is held in
    # .../Python (PID 22584) by user michaelchapman. See also https://duckdb.org/docs/stable/connect/concurrency
    # In the end, it was about ~30 something partitions that were completed per data source, so likely that's some limit
    # given my machine.
    #
    # I was able to re-run the failed paritions, and all but one failed, but that certainly caused dupes in the source data
    # It was nice to know that dagster pretty gracefully only ran jobs for failed partitions within each source, which in
    # theory should not have produced dupes from the source, but I bet some of the partition ranges overlapped or something?
    
    # Putting them on pools and making the pool limit value 1 made this complete successfully without the write errors.
    # Removing concurrency increased the pipeline time to bout 8.5 minutes for the entire dataset (not great).
    # This requires going into the UI because you can't set that in the dagster.yaml, except for a default for all
    ``` 