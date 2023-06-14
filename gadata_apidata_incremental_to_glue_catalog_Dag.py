from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from utils.slack_alert import SlackAlert
import pendulum

### glue job specific variables
glue_iam_role = "Camore_Glue_Job_Role"

region_name = "ap-*********-2"

### slack api specific variables
alert = SlackAlert('#message') # 메세지를 보낼 슬랙 채널명을 파라미터로 넣어줍니다.


### argument setting
default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 2, 28, tzinfo=pendulum.timezone("Asia/Seoul")),
    'retry_delay': timedelta(minutes=3),
    'schedule_interval':'00 1 * * *',
    'on_failure_callback': alert.on_failure
}


with DAG(dag_id="GA_DATA_Api_Incremental_DATA_PipeLine", default_args=default_args) as dag:

    gadata_incremental_playstore_incremental_job = GlueJobOperator(  
    task_id="gadata_incremental_playstore_incremental",  
    job_name="gadata_incrementalPlaystore_Incremental_Job",
    script_location = "s3://*********-bucket/scripts/gadata_incrementalPlaystore_Incremental_Job.py",
    region_name = region_name,
    s3_bucket = "s3://dataproject-bucket/mwaaLog/",
    create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 10, "WorkerType": "G.1X"},
    iam_role_name='*********',  
    dag=dag)

    Api_Data_to_glue_catalog_job = GlueJobOperator(  
    task_id="Api_Data_to_glue_catalog",  
    job_name="Api_Data_to_glue_catalog_job",
    script_location = "s3://dataproject-*********/scripts/Api_Data_to_glue_catalog_job.py",
    region_name = region_name,
    s3_bucket = "s3://dataproject-bucket/mwaaLog/",
    create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 10, "WorkerType": "G.1X"},
    iam_role_name='*********',  
    dag=dag)

    



    [gadata_incremental_playstore_incremental_job, Api_Data_to_glue_catalog_job]




