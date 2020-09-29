# Uber-self-driving-logs-analytics
In this project, an entire end to end architecture is created to read log files from S3, process the logs collected and load them to Redshift Data Warehouse. The
steps involved and design considerations are clearly explained below.

### Tech Stack
1. Data Storage   : Amazon SimpleStorageService(S3)
2. Data Warehouse : Amazon Redshift
3. Cluster Platform : Amazon ElasticMapReduce(EMR)
4. Orchestration  : Apache AirFlow


### Data Architecture
![Data Architecture](/imgs/Architecture.png)


### Assumptions
1. It is assumed that all the log files collected every day are stored in an S3 bucket dedicated to logs in the following format.
 Example: All files collected on January 1st 2020 are stored in the location s3:://{logs_bucket}//raw_logs//20200101
2. It is also assumed that one has already set up an AWS account, Initialized redshift cluster and has S3 with two buckets, one to store log files and the other to store scripts


### Data Model
Following is the schema of the table created in Amazon Redshift

![Entity Relationship Diagram](/imgs/data_model.png)

The goal is for the data scientists to query the table and understand the time taken by function call as well as make histogram reports. There are four columns 
in the table:
1. vehicle_id    -- varchar   -- vehicle id to which the log record belongs to
2. function_name -- varchar   -- name of the function
3. start_time    -- timestamp -- timestamp when the function call is made
4. duration      -- decimal   -- duration of the function

The records of the table are stored in the **sorted order** of the columns in the order --> function_name, start_time

### ETL Flow
The overall set up is divided into two dags. The first dag is run only once. The second dag is scheduled to run daily.

DAG 1:
* set_up: This is a one-time job that is run when initially setting up the system. There are two tasks defined in this DAG.
  1. Moves pyspark script to respective s3 location to be later accessed by EMR
  2. Creates Redshift table log_functions_analysis 
  
DAG 2:  
* process_logs_daily: This job is scheduled to run every day. This DAG has 5 tasks
  1. Initialize EMR cluster
  2. Add steps to EMR. The steps are read from a json file (airflow/dags/scripts/emr/process_log_file_emr.json). There are 3 steps in EMR
      a. Distributed copy the log files from S3 (s3://{log_bucket}//raw_logs//{execution_date}) to EMR location /logs
      b. Load the pyspark script from S3 (s3://{scripts_bucket}//spark/transform_log_data_for_analytics.py) and execute the script of raw logs data. Once the script is executed, the logs are written in parque format to EMR location /output.
      c. Distributed unload the processed files from /output to S3 (s3://{log_bucket}//processed_logs//{execution_date}) in parquet format
  3. EMR step senor task. This task is to make sure that all the steps in EMR are working correctly.
  4. After all the EMR steps are successfully run, the EMR cluster is terminated.
  5. The processed logs are copied in a distributed fashion from S3 (s3://{log_bucket}//processed_logs//{execution_date}) to Redshift table.
  
  
 ## Reasons driving Architecture Decision:
  1. The logs can grow significantly large as number of vehicles and trips increase over time. Hence, Amazon S3 and Amazon Redshift are chosen as they are both horizontally scalable and Reshift is designed for faster reads and writes.
  2. Amazon Redshift provides an option to store the table values in sorted format. The log files are sorted on compound kye (function_name, start_time). Doing  this, whenever the data scientists 
  perform any aggreagte function, only the nodes containing the specific function name are read so that queries are fast and performance is high. 
  3. Sorting by the second column start_time enables data scientists to query the time taken a given function across time and the performance is high for these queries for the same reason mentioned above.
  4. Amazon EMR cluster is created with auto scale option = True which lets the cluster to increase its size to handle the data accordingly. Thus scalability while processing the log file using Spark script is handled with this design.
  5. With respect to optimizations on memory, the spark script has a series of window functions that divide the data across partitions. This is helpful where large log files are split across multiple nodes through these partitions by the driver and the executor performs operations within the node for the partition and results are sent back to master node for aggregating the values.
  6. Apache Airflow is chosen given the flexibility and ease that it provides with scheduling the jobs as well as providing prebuilt operators to leverage common ETL tasks. 
  7. Backfilling: DAG 2 in the architecture is scheduled to run daily. To backfill the logs for previous dates, one has to set the start_date in default_parameters to the data one wants to process the logs from and the dag automatically runs for all the previous days until the current day. If one needs to process the logs over a specified date range, set the paramters  start_date and end_date in default parameters and all the dates that fall under this range are calculated.
  
  8. Amazon Redshift can be easily integrated with BI tools like Tableau, Looker etc which enables advanced analytics easily.
  9. All the AWS Services used (S3, EMR, Redshift) are self managed and hence very low maintenance required for maintenance. Also, EMR is configured through Airflow so that EMR is only active during the run time and it is terminated immediately after the job is executed and hence cost effective.
  10. Amazon Redshift contains replicas for nodes and hence data is available most of the times.
  
  
### Deployment
1. git clone the project into a folder
2. Set up env variables 
   a. echo "AIRFLOW_HOME=$PWD/airflow/dags" >> .env
   b. echo "s3_access_key_id=<your_s3_access_key_id>" >> .env
   c. echo "s3_secret_access_key=<your_s3_secret_access_key>" >> .env
   d. echo "region=<us-region_redshift_cluster_resides_in>" >> .env
   Note: .env already has a few environment variables like scripts_bucket, logs_bucket, redshift_region. Please change based on your AWS account
3. pipenv shell
4. pip install -r requirements.tx
5. airflow initdb
6. airflow webserver -p 8080
7. airflow scheduler(In a new terminal)
8. open http://0.0.0.0:8080/admin/ in your webbrowser and navigate to Admin/Connections
9. Add default connections for Redshift(redshift_conn), AWS(aws_default), and EMR(emr_default)
10. Move the toggle on the two dags from Off to On and the dags begin running.


### Next Steps:
1. Create a Data Quality Operator to check if the data has been loaded to Redshift table or node
2. Add an additional task to run Vaccuum operation on Redshift tables once the data is uploaded to Redshift so that the new values appended are stored in sorted ordder.

## Alternative Design Considerations
1. Once the data is reasonaly large(Terabytes of data inside Redshift), if the data is evenly distributed across all the functions, then we can alter the redshift table to have Key Based Distribution Style where the dist key is function name.
This way, a node will have only the data contained by a particular function and this helps improve the performance of queries(for the given usecase) on top of this data.


