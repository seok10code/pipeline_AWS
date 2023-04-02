import datetime
import os
import boto3
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
# from airflow.contrib.operators.s3_delete_objects_operator import S3DeleteObjectsOperator
from airflow.hooks import S3_hook
import sys
import time
from airflow import DAG
from datetime import datetime, timedelta


### glue job specific variables
# glue_job_name = "teamo2Job"
glue_iam_role = "AWSGlueServiceRole"
region_name = "ap-northeast-2"
email_recipient = "swkim@cloocus.com"


default_args = {
    'owner': 'admin',
    'retry_delay': timedelta(minutes=3),
}


dag = DAG(
    dag_id="concurrent_test",
    default_args=default_args,
    default_view="graph",
    # schedule_interval="23 01 10 2 30", #### Change cron entry to schedule the job
    # start_date=datetime.datetime(2023, 1, 10), ### Modify start date accordingly
    start_date=datetime(2023, 1, 10), ### Modify start date accordingly
	catchup=False, ### set it to True if backfill is required.
    tags=["example"],
)


glue_task1 = GlueJobOperator(  
    task_id="glue_task1",  
    job_name="teamo2Job1",
    script_location = "s3://airflow-cloocus-test-bucket/scripts/teamo2Job1.py",
    concurrent_run_limit=4,
    region_name = region_name,
    s3_bucket = "s3://cloocus-test-bucket/mwaaLog/",
    create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 10, "WorkerType": "G.1X"}, 
    iam_role_name='AWSGlueServiceRoleDefault',  
    dag=dag)



    
glue_task2 = GlueJobOperator(  
    task_id="glue_task2",  
    job_name="teamo2Job2",
    script_location = "s3://airflow-cloocus-test-bucket/scripts/teamo2Job2.py",
    region_name = region_name,
    concurrent_run_limit=4,
    s3_bucket = "s3://cloocus-test-bucket/mwaaLog/",
    create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 10, "WorkerType": "G.1X"}, 
    iam_role_name='AWSGlueServiceRoleDefault',  
    dag=dag)

glue_task3 = GlueJobOperator(  
    task_id="glue_task3",  
    job_name="teamo2Job3",
    concurrent_run_limit=4,
    script_location = "s3://airflow-cloocus-test-bucket/scripts/teamo2Job3.py",
    region_name = region_name,
    s3_bucket = "s3://cloocus-test-bucket/mwaaLog/",
    create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 10, "WorkerType": "G.1X"}, 
    iam_role_name='AWSGlueServiceRoleDefault',  
    dag=dag)




# 10분


# glue_task2 = AwsGlueJobOperator(  
#     task_id="glue_task2",  
#     job_name='catalogTest-seok',
#     iam_role_name='AWSGlueServiceRoleDefault',  
#     dag=dag)
# 10분

# 10개 -> 20분
# 20개 -> 10분
[glue_task1,glue_task2,glue_task3]







# https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/operators/glue/index.html
# https://aws.amazon.com/ko/blogs/big-data/orchestrate-aws-glue-databrew-jobs-using-amazon-managed-workflows-for-apache-airflow/
