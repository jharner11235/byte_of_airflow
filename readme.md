Bits and pieces of Airflow that have been useful in prior roles, mainly for brainstorming new setups

My current folder structure is like this:
```
~/dev
|   airflow
|   airflow_bits
```
in that airflow and this repo are side-by-side and airflow.cfg is pointing here for DAGs/etc

This repo utilizes a postgres docker instance (which I also use as the airflow metadata db) but the SQL
could be easily thrown in ChatGPT and translated to MySQL, etc if desired  
We're pretending that we have a postgres instance for the backend of the website, as well as a postgres  
instance acting as an Operational Data Store (even though practically we're just using different schemas  
in the same PG docker instance - feel free to spice it up if you like)