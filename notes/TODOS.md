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