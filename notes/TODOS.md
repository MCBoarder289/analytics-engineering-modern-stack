# TODOS

## Warehousing / Modeling
* Build `fct_` tables, supplementing the `stg_` tables with denormalized data from other joined tables
  * ex: join in customer data metadata, 
* Build `'mart_` tables for operational analytics
  * ex: hierarchical summaries for managers, agents
  * ex: summarized/aggregated tables daily
* Update data generation for:
  * Variable survey take rates by call reason / customer program
  * [DONE] Make NPS score more relatable (currently basically 0)
* Test what incremental loading looks like for agent/daily aggregation tables
  * Do I need a unique/composite unique key? Will it work with just the dates?


## dagster stuff
* Update automation conditions for partitions that are retroactively updated
  * Not sure if this is possible, but the declarative automation parts of a dagster asset would potentially work?
    * see [dagster docs on automation conditions](https://docs.dagster.io/guides/automate/declarative-automation/customizing-automation-conditions/customizing-on-missing-condition#updating-older-time-partitions)
    * see [customizing dbt automation conditions](https://docs.dagster.io/integrations/libraries/dbt/reference#customizing-automation-conditions)
* Seeing a 60-second gap between dlt sources and dbt downstream asset jobs starting
  * Need to figure out why this has been introduced as of recent commits (9/28/25)
  * Not seeing this anymore, and could have been a transient system issue? Will continue to monitor

## dlt stuff
* Incremental partition problems:
  * I want the filesystem source to be idempotent based on the modification_date of the files in the directories, but this has posed a problem.
  * When I first run the pipeline, the state doesn't update for both my source and pipeline, just the source. A second run will duplicate, but then it will be idempotent after that.
  * I think I've identified the issue as somehow related to the source decorator and how dagster has to instantiate the source
    * See [dlt slack question](https://dlthub-community.slack.com/archives/C04DQA7JJN6/p1758997014552919?thread_ts=1758995720.987159&cid=C04DQA7JJN6)
  * RESOLUTION: I ended up needing to name the pipeline name the same thing as the soruce's name/function name. That way, the state that is saved ends up being shared properly.
    * See [dagster slack thread](https://dagster.slack.com/archives/C066HKS7EG1/p1759021949908949?thread_ts=1759019650.736699&cid=C066HKS7EG1)

## Performance
* Noticed dagster says runs take at least 1 minute, even if the processing is in sub-second completions.
  * For example, A run that completed in 262ms, says it took 1:04.
    * Only step log message: Finished execution of step "calls_ingestion_assets" in 269ms.
    * Then a minute later we get: Multiprocess executor: parent process exiting after 1m3s (pid: 22822)
    * I wonder if this is all due to some weird polling due to multiprocessing queues in local dev?
* Noticing intermittent failures which are due to a race condition of attaching ingestion duckdb files to the dbt connection
  * Can easily re-run failed portions to resolve the issue, but might be worth hardening.

## dbt stuff
* need to add instructions on setting up their local profiles.yml
  * Give example of a profiles.yml
  * ensure the databases are in the proper place

* **dbt tests**
  * tests can be defined in the tests directory, and they're automatically assigned to the models they reference
  * can also define generic tests that can be re-used and placed in the models yaml files
  * **seed tests**
    * note that if we don't run all of our seeds together, we can't test referential integrity
    * I have a referential integrity test between agent_assignments and the agents and managers tables, which will fail if those aren't present.
      * Note that it will still write the table, just fail the asset checks until the other pieces are materialized
      * This is an interesting instructional point that the DAG should show sequence
        * We could model an ephemeral cte that would simply be to test this asset, but that seems to be onerous?

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
* Update call volume patterns. Currently, the seasonality and weekday multipliers are being dwarfed by call times and arrivals.
  * Basically, because agents are pretty much fully utilized, the desired call volume of ~3000 calls per day ends up being a third of that due to how long calls are taking, and that we cut things off at the workday end.
  * See "Simulation Customer Overlap fix" conversation in ChatGPT project for more details/context.
* Fully understand the gap of survey response times
  * Right now, the "day" partition is the day we receive the surveys
  * The staging partition re-runs everything minus a lookback window to cover this diff.
* [DONE] Make sure Data Generation is deterministic
  * [DONE] Test that parquet outputs contain same records across all runs.
* [DONE] Need to figure out the `customers` dimension table
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
