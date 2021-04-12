## Incremetal Data Loading
### Task description
It is required to implement an ETL process that incrementally unloads data 
from the DBMS on an ongoing basis using Apache Airflow.
The unloading process should take place in 3 stages:
1. Uploading data from a source (DBMS) to a temporary file storage (local, s3, mini, etc) in csv format.
2. Unloading data from file storage into a temporary table dwh (e.g. Greenplum or any other DBMS).
3. Moving data from the temporary table to the target one. 

### Data format
1. Source table: 
```SQL
CREATE TABLE "order" (
  id bigint primary key,
  student_id bigint,
  teacher_id bigint,
  stage varchar(10),
  status varchar(512),
  created_at timestamp,
  updated_at timestamp
);
```

2. Destination table in dwh:
```SQL
CREATE TABLE raw_order (
   id bigint primary key,
   order_id bigint, 
   student_id bigint, 
   teacher_id bigint, 
   stage varchar(10), 
   status varchar(512),
   row_hash bigint,
   created_at timestamp, 
   updated_at timestamp
);
*row_hash - hash of all the columns in source-table
```
3. Mapping:
```SQL
Source        Destination
null       -> id (primary key)
id         -> order_id
student_id -> student_id
teacher_id -> teacher_id
stage	   -> stage
status	   -> status
null       -> row_hash
created_at -> created_at
updated_at -> updated_at
```

## What was used 
1. Ubuntu 20.04
2. PostgreSQL 13.2
3. Python 3.8
4. SQLAlchemy 1.3.22
5. Airflow 2.0.1

## How use it
1. Create 2 test databases in PostgreSQL to imitate 
   `source_db` and `destination_db`. Add some data to them.
2. Edit `dbinfo.json` to connect to your databases:
   
    2.1. Edit user login
    
    2.2. Edit user passwords

    2.3. Edit hostname and port

    2.4. Edit database names
3. To test script just run it: `python3 script.py`
4. If the script works ok, you will see in colsole:
```shell
#Type of id-column updated successfully.
#query: INSERT INTO <...>
#Process finished with exit code 0
```
5. [Install airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation.html). 
   
6. Create DAG with script in Airflow
7. Add `dag_file_airflow.py` in path: `~/airflow/dags`
8. Edit dag-file: add absolute path of script.py in `SCRIPT_ABSPATH`
9. Check dag-file added correctly:
```shell
airflow dags list

#dag_id            | filepath           | owner   | paused
#==================+====================+=========+=======
#data_loading_task | dag_file_airflow.py | airflow | False 
```
10. Run Airflow webserver and scheduler:
```shell
airflow webserver
airflow scheduler
```
## Options
1. You can set `LIMIT_TO_LOAD` in `script.py` to select the number of lines to 
transfer from source_db to destination_db per script execution. Default: 1.
   
2. You can change `schedule_interval` in dag-file to make load process more 
often or rare. Default: once in hour.

## What's wrong
1. Credentials of databases is open in `dbinfo.json`
2. When script unloads data from source it's not add quotes to char-types 
   so when we make "insert into"-operation we need add its in code
   directly
3. Changing type of id-column (when we want to add DEFAULT-value in 
   destination_db)

## Project goal
Grow up skills with Linux, PostgreSQL, Airflow.