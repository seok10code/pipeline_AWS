from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow import DAG
from datetime import datetime, timedelta
from query.bi_query import user_information_mart_query, reservation_information_mart_query, company_integration_mart_query, domestic_settlement_mart_query, create_table_query
from airflow.utils.task_group import TaskGroup
from utils.slack_alert import SlackAlert
import pendulum

### current datetime
time_now = pendulum.now(tz="Asia/Seoul")

today = time_now.strftime("%Y-%m-%d 00:00:00")


### glue job specific variables
rds_mysql_carmore_table_migration_job = "rds_mysql_carmore_table_migration_job"
carmore_AllSchema_extracting_job = "carmore_AllSchema_extracting_job"
mustgo_job_all = "rds_mysql_mustgo_table_migration_job"
klook_job_all = "rds_mysql_klook_table_migration_job"

user_information_query = user_information_mart_query()
company_integration_query = company_integration_mart_query()
reservation_information_query = reservation_information_mart_query(today)
domestic_settlement_query = domestic_settlement_mart_query()


glue_iam_role = "Camore_Glue_Job_Role"

region_name = "ap-northeast-2"

                ### slack api specific variables
alert = SlackAlert('#백엔드개발팀_데이터프로젝트_알림') # 메세지를 보낼 슬랙 채널명을 파라미터로 넣어줍니다.


default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 6, 13, tzinfo=pendulum.timezone("Asia/Seoul")),
    'retry_delay': timedelta(minutes=3),
    'schedule_interval': "0 3 * * *",
    'on_failure_callback': alert.on_failure
}


with DAG(dag_id="Aurora_MySQLDB_Data_PipeLine_Daily", default_args=default_args) as dag:
    carmore_all_schema_extracting_job = GlueJobOperator(  
    task_id="carmore_AllSchema_extracting_job",  
    job_name=carmore_AllSchema_extracting_job,
    script_location = "s3://dataproject-bucket/scripts/carmore_AllSchema_extracting_job.py",
    region_name = region_name,
    s3_bucket = "s3://dataproject-bucket/mwaaLog/",
    create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 10, "WorkerType": "G.1X"},
    iam_role_name='Carmore_Glue_Job_Role',  
    dag=dag)

    carmore_delta_table = GlueJobOperator(  
    task_id="carmore_delta_table",  
    job_name=rds_mysql_carmore_table_migration_job,
    script_location = "s3://dataproject-bucket/scripts/rds_mysql_carmore_table_migration_job.py",
    region_name = region_name,
    s3_bucket = "s3://dataproject-bucket/mwaaLog/",
    create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 32, "WorkerType": "G.2X"},
    iam_role_name='Carmore_Glue_Job_Role',  
    dag=dag)

    klook_job = GlueJobOperator(  
    task_id="klook_job",  
    job_name=klook_job_all,
    script_location = "s3://dataproject-bucket/scripts/rds_mysql_klook_table_migration_job.py",
    region_name = region_name,
    s3_bucket = "s3://dataproject-bucket/mwaaLog/",
    create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 10, "WorkerType": "G.1X"},
    iam_role_name='Carmore_Glue_Job_Role',
    dag=dag)

    mustgo_job = GlueJobOperator(  
    task_id="mustgo_job",  
    job_name=mustgo_job_all,
    script_location = "s3://dataproject-bucket/scripts/rds_mysql_mustgo_table_migration_job.py",
    region_name = region_name,
    s3_bucket = "s3://dataproject-bucket/mwaaLog/",
    create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 10, "WorkerType": "G.1X"},
    iam_role_name='Carmore_Glue_Job_Role',  
    dag=dag)


    company_integration_table = "bi_company_integration"
    user_information_table = "bi_user_information"
    reservation_information_table = "bi_reservation_information"
    domestic_settlement_table = "bi_domestic_settlement"


    with TaskGroup(group_id = "Create_Table_Query_Group") as CreateTableQuery:
        company_integration_Create_Table_query = AthenaOperator(
            task_id='company_integration_Athena_Query_Create_Table',
            query=f"""{create_table_query(company_integration_table)}{company_integration_query}""",
            output_location='s3://dataproject-bucket/mwaaLog/athena_output/',
            database='carmore'
        )

        user_information_Create_Table_query = AthenaOperator(
            task_id='user_information_Athena_Query_Create_Table',
            query=f"""{create_table_query(user_information_table)}{user_information_query}""",
            output_location='s3://dataproject-bucket/mwaaLog/athena_output/',
            database='carmore'
        )

        reservation_information_Create_Table_query = AthenaOperator(
            task_id='reservation_information_Athena_Query_Create_Table',
            query=f"""{create_table_query(reservation_information_table)}{reservation_information_query}""",
            output_location='s3://dataproject-bucket/mwaaLog/athena_output/',
            database='carmore'
        )

        domestic_settlement_Create_Table_query = AthenaOperator(
            task_id='domestic_settlement_Athena_Query_Create_Table',
            query=f"""{create_table_query(domestic_settlement_table)}{domestic_settlement_query}""",
            output_location='s3://dataproject-bucket/mwaaLog/athena_output/',
            database='carmore'
        )



    with TaskGroup(group_id = "Insert_Query_Group") as InsertQuery:
        company_integration_Insert_query = AthenaOperator(
            task_id='company_integration_Athena_Query_Insert',
            query=f"""INSERT INTO carmore.bi_company_integration {company_integration_query}""",
            output_location='s3://dataproject-bucket/mwaaLog/athena_output/',
            database='carmore'
        )

        user_information_Insert_query = AthenaOperator(
            task_id='user_information_Athena_Query_Insert',
            query=f"""INSERT INTO carmore.bi_user_information {user_information_query}""",
            output_location='s3://dataproject-bucket/mwaaLog/athena_output/',
            database='carmore'
        )

        reservation_information_Insert_query = AthenaOperator(
            task_id='reservation_information_Athena_Query_Insert',
            query=f"""INSERT INTO carmore.bi_reservation_information {reservation_information_query}""",
            output_location='s3://dataproject-bucket/mwaaLog/athena_output/',
            database='carmore'
        )

        domestic_settlement_Insert_query = AthenaOperator(
            task_id='domestic_settlement_Athena_Query_Insert',
            query=f"""INSERT INTO carmore.bi_domestic_settlement {domestic_settlement_query}""",
            output_location='s3://dataproject-bucket/mwaaLog/athena_output/',
            database='carmore'
        )



    with TaskGroup(group_id = "Delete_Query_Group") as DeleteQuery:
        company_integration_Delete_query = AthenaOperator(
            task_id='company_integration_Athena_Query_Delete',
            query='DELETE FROM carmore.bi_company_integration',
            output_location='s3://dataproject-bucket/mwaaLog/athena_output/',
            database='carmore'
        )

        user_information_Delete_query = AthenaOperator(
            task_id='user_information_Athena_Query_Delete',
            query='DELETE FROM carmore.bi_user_information',
            output_location='s3://dataproject-bucket/mwaaLog/athena_output/',
            database='carmore'
        )

        reservation_information_Delete_query = AthenaOperator(
            task_id='reservation_information_Athena_Query_Delete',
            query='DELETE FROM carmore.bi_reservation_information',
            output_location='s3://dataproject-bucket/mwaaLog/athena_output/',
            database='carmore'
        )

        domestic_settlement_Delete_query = AthenaOperator(
            task_id='domestic_settlement_Athena_Query_Delete',
            query='DELETE FROM carmore.bi_domestic_settlement',
            output_location='s3://dataproject-bucket/mwaaLog/athena_output/',
            database='carmore'
        )

    carmore_all_schema_extracting_job >> carmore_delta_table >> CreateTableQuery >> DeleteQuery >> InsertQuery
    [klook_job, mustgo_job]