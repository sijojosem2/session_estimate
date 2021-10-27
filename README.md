# Overview 

 The project is broken down into three parts: ETL, Analytics, Q&A. I use python/postgres for the ETL load
 
## ETL 

### Initial Setup 

   Since I am working out a much more efficient way to airflow dockerize the whole ETL setup , I am temporarily using the crude method of Makefile deployment. Just to ensure package homogeneity and full ETL setup to deploy, kindly follow these steps:
   
   *  Clone the repo into a local env 
   *  Source a virtual env, preferably python3.8 (please do not use 3.10 at any cost yet! It ate away too much time to revert the damage caused by compatibility issues)
   *  Go to the [Makefile](https://github.com/sijojosem2/session_estimate/blob/main/Makefile) directory and run `make install-packages`
   *  If all is good this should execute without any problems, since the python3.8 venv will be a blank slate for all the packages to be written upon
   *  Go to the [envfile](https://github.com/sijojosem2/session_estimate/blob/main/scripts/envfile) and provide the postgres connection string details
   *  We're set! 

 
### ETL Setup 

  The ETL code may seem a bit of an overkill, but this is a pipeline format that I have used a list of actions are performed sequentially, I have used this in other repos as a quick and easy way to get data from multiple sources like S3,API,fixed/variable length csv into tables.
  
  The main set of actions are prescribed in the [pipeline](https://github.com/sijojosem2/session_estimate/blob/main/pipeline.py) file. The first action in the sequence:
  ```python 
          {
            "dataset_name": "pre_execution",
            "description": "file execution pre etl",
            "exec_sql": {"name": "pre_exec.sql"}
        },
  ```      
  
  is to create the basic table that will be populated from the csv. The [sql](https://github.com/sijojosem2/session_estimate/tree/main/sql) location contains the [pre_exec.sql](https://github.com/sijojosem2/session_estimate/blob/main/sql/pre_exec.sql) file which creates this basic table. At 1st I only extract the available csv columns as an initial step.
  
  The second step in the etl pipeline:
  ``` python
          {
            "dataset_name": "local_file",
            "description": "local tier session",
            "local": "local_files/mobile_events_2020.csv",
            "write_to_csv": False,
            "var_length": True,
            "pd_dataframe": {
                "params": {
                    "sep": ";", "header": "infer"},
                "drop_cols": []
            },
            "target_table": {"name": "mobile_events", "if_exists": "append", "index": False}
        },
  ``` 
  does the actual loading of the csv into the created table. This is a simple one-one load. The pandas utility is used for the data load here and the parameters facilitate this activity.
  
  The third and final step in the pipeline consists of the [post_exec.sql](https://github.com/sijojosem2/session_estimate/blob/main/sql/post_exec.sql) file:
  ```python 
          {
            "dataset_name": "post_execution_sql_script_example",
            "description": "file execution post etl",
            "exec_sql": {"name": "post_exec.sql"}
        },
  ```
  This creates the post execution activities after ETL is complete, including MD5 hash column and index creation, the explaination of which I shall provide in the Q & A below
  
  
### Initial Run

Once the initial setup is complete and when the postgres database is up, go to the [Makefile](https://github.com/sijojosem2/session_estimate/blob/main/Makefile) directory and run ` make run-data-pull`. The logs for this run will be available in [logs](https://github.com/sijojosem2/session_estimate/tree/main/logs)
  
 
 
 
 ## Analytics
 
 The answer to the question given in TASK 1:
 
 TASK 1

    Take the included tracking data and write a transformation SQL query or a Python script to bring the attached data into a session format

    * Feel free to take decisions around how you define sessions (think about how customers use the app)

    * In the end we would like to understand whether a user was looking for a scooter and was able to book a scooter or not
 
  The query for this is available in [session_queries.sql](https://github.com/sijojosem2/session_estimate/blob/main/sql/session_queries.sql).
  
  In the Simple version, I have broken down the csv data into 15 minute chunks calculated by the **created_at** column. I have assumed the successful booking as any event  culminating with a 'Ride   Started', 'Ride Done' or 'Rating Screen' within a running series of 15 min chunks. This covers the case when the ride or application open/signup takes more than 15 min. The Flag that marks the successful booking is 'successful_booking' and this denotes either Y for successful booking or N for unsuccessful booking if the above-mentioned status events are not available at the end of the session. Thus, from this version both the question , ie session definition and whether a booking was successful or not is fully answered.
  
  In the Extended version , which was my initial attempt at deciphering the session info, I had initially thought of bunching up created_at times between 'Application Opened' and 'Ride Started - Successful', the booking conditions remains the same as above, i.e. booking is considered successful only if any event culminating with a 'Ride Started', 'Ride Done' or 'Rating Screen' is available in a series of 15 minute event chunks. This allowed me to find times taken between opening application and booking a ride , and from booking a ride to completing a ride. Even though this was not the question that was asked , I decided to keep this as a reference , just in case a similar analysis is required in the future 
   
  
  
  ## Q & A  
  
 TASK 2

Answer the following questions:

    1. If you had to deploy it into production, what extra considerations would you make? Would you need to change/improve anything?

    2. Do you see any possible future performance bottlenecks that we should worry about?

    3. What logic would you use to turn this data model into incremental (processing only new/delta data)?
     
### 1. If you had to deploy it into production, what extra considerations would you make? Would you need to change/improve anything?
  
   As an immediate initial step, I have added the indices:
   
   
   ``` sql
     CREATE INDEX if not exists idx_sort_id ON mobile_events (lower(anonymous_id), lower(context_device_id) , created_at);

     CREATE INDEX if not exists idx_col_hash ON mobile_events (col_hash);
   ```
     
     
 * Since the window uses the sort key (lower(anonymous_id), lower(context_device_id) , created_at) in the same order, an index would substantially speed up the execution. Additionally, I have added an index on the col_hash column to answer question 3.
 * I am assuming that the normalized data in the csv has been gleaned from various dimensions for analytics purposes. That being said, generating and keeping a UUID PRIMARY_KEY would be highly recommended, considering the volume and scale of records, a UUID will be ideally suited for this purpose. To further save some space we can use a v1 timestamp UUID which we can use for both sequential sorting and deriving the timestamp.

  
### 2. Do you see any possible future performance bottlenecks that we should worry about?

 * Since this is a text heavy search analytic query, the bigger the data grows the bigger the execution plan/time this takes. I had tested the dataset using the postgres TSQUERY and TSVECTOR functionality , which are excellent at text index matching functionalities, but I had to drop this approach, because of time constraints as well as the fact that at the current size the query plan execution is much faster with the LIKE operator than the TSQUERY/TSVECTOR functionality, but I need to research on this more.
 * Apart from the above, i would recommend an automatic partitioning. I had tried unsuccessful to implement [this](https://github.com/pgpartman/pg_partman), since I found it super interesting and time saving, but I had to drop the idea because each time I change the postgresql.conf file postgres refuses to restart. If this works, this would be a great solution!  
  
### 3. What logic would you use to turn this data model into incremental (processing only new/delta data)?
  
 * Set the MD5 hash against all the record values (or only the required values) I am assuming delta data refers to new data to be inserted and since this is an initial run, I   have set this accordingly
 * In subsequent runs, use an intermediate ETL Tool / Python Script / temp_table that holds records and check against the existing hash before insert. I would not recommend a trigger check, assuming the huge size of data and the time this will take considering future scale
 * Once check is complete against the existing records, insert only the new hash records
  
