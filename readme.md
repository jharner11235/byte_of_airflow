Bytes and pieces of Airflow that have been useful in prior roles, mainly for brainstorming new setups.

I'm still not happy with the resources that exist (many tutorials show you how to use Airflow but not
how we use it in the wild and with resource constraints... which can vary greatly.) so I'm hoping that
this will provide some info and aid to new and curious DEs. I'm also hoping that any better methods that
are found can be shared to this project to improve it (win-win? Those are nice.)

NOTE: This is meant as a side-car to airflow and not an all-encompassing project.
This may change in the future if it's deemed easier to replicate this env with both 
projects combined, which means I'll need to keep this updated with the latest version
of airflow

My current folder structure is like this:
```
~/dev
|   airflow
|   byte_of_airflow
```
in that airflow and this repo are side-by-side and airflow.cfg is pointing here for DAGs/etc

This repo utilizes a postgres docker instance (which I also use as the airflow metadata db) but the SQL
could be easily thrown in ChatGPT and translated to MySQL, etc if desired  
We're pretending that we have a postgres instance for the backend of the website, as well as a postgres  
instance acting as an Operational Data Store (even though practically we're just using different schemas  
in the same PG docker instance - feel free to spice it up if you like)

Setup:  
 1. Prep a normal [airflow](https://github.com/apache/airflow) env
 2. Set paths in your env to look at the proper folders in this repo
 3. Turn on database_builder DAG
    * It'll self-trigger and run once, it can be turned off after
    * This will add connections for our 'source' and 'target' dbs if they don't
      already exist and then build out the schemas within the PG database
 4. Turn on data_builder DAG
    * This will run every minute and randomly add data
 5. Test away!
    * You'll have randomly-generated data that replicates an ecom prod database
      that you can practice building ETL pipelines with
    * One example already exists, with what I've found to be one of the fastest
      and least compute-intensive ETL methods


TODO: Give a more wholesome start-to-finish setup, and possibly record as well
