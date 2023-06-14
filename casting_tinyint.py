import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType
from pyspark.conf import SparkConf
import subprocess
from delta import *
import json
from datetime import datetime

error_cnt = 0


def getSchem(df): ##데이터프레임 스키마 포메팅 함수
    sche = df.schema.json()
    dic_schem  = json.loads(sche)
    result = ""

    for idx, val in enumerate(dic_schem['fields']):
        result += f"`{val['name']}`"+ f" {val['type'].upper()}"
        if len(dic_schem['fields'])-1 != idx:
            result += ', '
    return result

def query_formating_function(dbname, tbname, all_list, datetime_list, date_list, time_list, tinyint_list): ##RDS에 jDBC 쿼리 포메팅 함수
    convert_list = datetime_list + date_list + time_list
    bool_tiny_list = tinyint_list
    field_query = ""
    for single_col in all_list:
        if single_col in convert_list:
            add_query = f"CONVERT({single_col}, CHAR) AS {single_col},"
        elif single_col in bool_tiny_list:
            add_query = f"CAST({single_col} AS DECIMAL) AS {single_col},"
            # print(add_query)
        else:
            add_query = single_col + ","
        
        field_query += add_query
    field_query = field_query[:-1]

    query = f"SELECT {field_query} FROM {dbname}.{tbname}"
    return query
    
def date_etl_function(df, column): ## date fields 전처리 함수
    df = df.withColumn(column, regexp_replace(column, "0000-00-00", "1900-01-01"))
    df = df.withColumn(column, regexp_replace(column, "-00", "-01"))
    df = df.withColumn(column, to_date(col(column)))
    return df 
    
def datetime_etl_function(df, column): ## datetime fields 전처리 함수
    df = df.withColumn(column, regexp_replace(column, "0000-00-00 00:00:00", "1970-01-01 00:00:00"))
    df = df.withColumn(column, to_timestamp(col(column), "yyyy-MM-dd HH:mm:ss"))
    return df
#################################  
        
def directJDBCSource(
    glueContext,
    connectionName,
    connectionType,
    database,
    table,
    query,
    redshiftTmpDir

) -> DynamicFrame: ## 커넥터 연결하여 dynamic 데이터프레임으로 읽는 함수

    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": table,
        "connectionName": connectionName,
        "sampleQuery":query,
    }

    if redshiftTmpDir:
        connection_options["redshiftTmpDir"] = redshiftTmpDir

    return glueContext.create_dynamic_frame.from_options(
        connection_type=connectionType,
        connection_options=connection_options,

    )

conf = (SparkConf()\
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .set("spark.sql.catalog.glue_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.sql.catalog.glue_catalog.warehouse", "s3://dataproject-bucket/data/******/")
    .set("hive.metastore.warehouse.dir", "s3://dataproject-bucket/data/*****/")
    .set("spark.databricks.hive.metastore.glueCatalog.enabled", "true")
    )


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

      
path1 = "s3://dataproject-bucket/table/carmore_all_table_final.csv" ## 테이블 schema, 테이블 명 담겨있는 csv파일
tableN = spark.read.option("header", "true").option("inferSchema", "true").csv(path1)
db_tables=tableN.select("database","table").collect()

## 테이블 스키마 담겨있는 csv 파일 
allSchemaDf = spark.read.option("header","true").option("inferschema", "true").csv("s3://dataproject-bucket/data/caremore_allSchema/*.csv") 



for dbtb in range(3):  
    database_name = 'carmore'
    table_name ='workspace_api_reservation_inventory'
    
    tableSchemaDf = allSchemaDf.filter(col("TABLE_NAME")==table_name) ## 스키마 csv 파일에서 해당 테이블 field 가져오기
    
    all_col_list = tableSchemaDf.select("COLUMN_NAME").rdd.flatMap(lambda x:x).collect()
    datetime_col_list = tableSchemaDf.filter(col("DATA_TYPE")=="datetime").select("COLUMN_NAME").rdd.flatMap(lambda x:x).collect() ## datetime fields
    date_col_list = tableSchemaDf.filter(col("DATA_TYPE")=="date").select("COLUMN_NAME").rdd.flatMap(lambda x:x).collect() ## date fields
    time_col_list = tableSchemaDf.filter(col("DATA_TYPE")=="time").select("COLUMN_NAME").rdd.flatMap(lambda x:x).collect() ## time fields
    tinyint_col_list = tableSchemaDf.filter(col('DATA_TYPE') == 'tinyint').select('COLUMN_NAME').rdd.flatMap(lambda x:x).collect() ## tinyint fields
    
    que = query_formating_function(dbname = database_name, 
                                   tbname = table_name, 
                                   all_list = all_col_list,
                                   datetime_list = datetime_col_list,
                                   date_list = date_col_list,
                                   time_list = time_col_list,
                                   tinyint_list = tinyint_col_list)
    
    JDBCConnection_node1 = directJDBCSource(
        glueContext,
        connectionName="Aws_RDS_MySQL_connection",
        connectionType="mysql",
        database=database_name,
        table= table_name,
        query = que,
        redshiftTmpDir=""
    )
    jdbcDF = JDBCConnection_node1.toDF()
    
    
    if not jdbcDF.rdd.isEmpty():
        try:
            if len(datetime_col_list) > 0: 
                for datetime_col in datetime_col_list:
                    jdbcDF = datetime_etl_function(jdbcDF, datetime_col)
                
            if len(date_col_list) > 0:
                for date_col in date_col_list:
                    jdbcDF = date_etl_function(jdbcDF, date_col)
                    
            subprocess.run(['aws', 's3', 'rm', f's3://dataproject-bucket/data/{database_name}_delta/{table_name}/', '--recursive'])
            jdbcDF.write.format('delta').mode('overwrite').save(f"s3://dataproject-bucket/data/{database_name}_delta/{table_name}/") ## 테이블 저장
            # spark.sql(f"DROP TABLE {table_name} CASCADE CONSTRAINT")
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}_delta")
            result_schem = getSchem(jdbcDF)
            
            deltaTable = DeltaTable.forPath(spark, f"s3://dataproject-bucket/data/{database_name}_delta/{table_name}/")
            deltaTable.generate("symlink_format_manifest")
            ## Glue Catalog 저장 
            spark.sql(f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{table_name}({result_schem})
             ROW FORMAT SERDE "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" 
             STORED AS INPUTFORMAT "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat" 
             OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat" 
             LOCATION "s3://dataproject-bucket/data/{database_name}_delta/{table_name}/_symlink_format_manifest/"
            """)
        except Exception as e:
            error_cnt += 1
            print(dbtb[0], dbtb[1],e)

print(error_cnt)
subprocess.run(['aws', 's3', 'rm', 's3://dataproject-bucket/sparkHistorylog/', '--recursive'])
job.commit()


