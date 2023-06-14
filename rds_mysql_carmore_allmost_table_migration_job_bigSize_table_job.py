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

def query_formating_function(dbname, tbname, all_list, datetime_list, date_list, time_list, tinyint_list): ##RDS에 jDBC 푸시다운쿼리 포메팅 함수
    convert_list = datetime_list + date_list + time_list
    bool_tiny_list = tinyint_list
    field_query = ""
    for single_col in all_list:
        if single_col in convert_list:
            add_query = f"CONVERT({single_col}, CHAR) AS {single_col},"
        elif single_col in bool_tiny_list:
            add_query = f"CAST({single_col} AS DECIMAL) AS {single_col},"
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
    
def tinyint_etl_function(df, column):
    df = df.withColumn(column, col(column).cast(IntegerType()))
    return df
#################################  
        
def directJDBCSource(
    glueContext,
    connectionName,
    connectionType,
    database,
    table,
    query,
    fetch,
    # useCursorFetchTest,
    redshiftTmpDir

) -> DynamicFrame: ## 커넥터 연결하여 dynamic 데이터프레임으로 읽는 함수

    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": table,
        "connectionName": connectionName,
        "sampleQuery":query,
        "fetchsize":fetch,
        # "useCursorFetchTest":useCursorFetch
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
    .set("spark.sql.catalog.glue_catalog.warehouse", "s3://dataproject-bucket/data/****/")
    .set("hive.metastore.warehouse.dir", "s3://dataproject-bucket/data/****/")
    .set("spark.databricks.hive.metastore.glueCatalog.enabled", "true")
    .set("spark.sql.debug.maxToStringFields", "10000")

    )


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

     
path1 = "s3://dataproject-bucket/table/****_all_table_final_big_size.csv" ## 테이블 schema, 테이블 명 담겨있는 csv파일
path2 = "s3://dataproject-bucket/table/****_all_table_final_small_size.csv"
tableN = spark.read.option("header", "true").option("inferSchema", "true").csv(path1)
db_tables=tableN.select("database","table", "key").collect()

#테이블 스키마 담겨있는 csv 파일 
allSchemaDf = spark.read.option("header","true").option("inferschema", "true").csv("s3://dataproject-bucket/data/caremore_allSchema/*.csv") 

for dbtb in db_tables:  

    database_name = dbtb[0]
    table_name = dbtb[1]
    key_name = dbtb[2]
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
                              
        #pk의 맥스값 저장
    JDBCConnection_node1 = directJDBCSource(
    glueContext,
    connectionName="Aws_RDS_Aurora_MySQL_connection",
    connectionType="mysql",
    database=database_name,
    table= table_name,
    query = f"select min({key_name}), max({key_name}) from {database_name}.{table_name}",
    fetch="4000",
    # useCursorFetch="TRUE",
    redshiftTmpDir=""
    )
    minmaxDF = JDBCConnection_node1.toDF()
    
    max_num = minmaxDF.collect()[0][1]
    min_num = minmaxDF.collect()[0][0]
    
    iterate_num = min_num
    cycle_pk = 100000

    JDBCConnection_node1 = directJDBCSource(
    glueContext,
    connectionName="Aws_RDS_Aurora_MySQL_connection",
    connectionType="mysql",
    database=database_name,
    table= table_name,
    query = que + " limit 1",
    fetch="4000",
    # useCursorFetch="TRUE",
    redshiftTmpDir=""
    )
    schemaDF = JDBCConnection_node1.toDF()
    result_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schemaDF.schema)

    
    while max_num >= iterate_num:

        JDBCConnection_node1 = directJDBCSource(
        glueContext,
        connectionName="Aws_RDS_Aurora_MySQL_connection",
        connectionType="mysql",
        database=database_name,
        table= table_name,
        query = que + f" where {key_name} >= {iterate_num} and {key_name} <= {iterate_num+cycle_pk}",
        fetch="4000",
        # useCursorFetch="TRUE",
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
                        
                if len(tinyint_col_list) > 0:
                    for tinyint_col in tinyint_col_list:
                        jdbcDF = tinyint_etl_function(jdbcDF, tinyint_col)
                if iterate_num == min_num:
                    result_df = jdbcDF

                else:
                    result_df = result_df.union(jdbcDF)

            except Exception as e:
                error_cnt += 1
                print(dbtb[0], dbtb[1],e)

        iterate_num = result_df.agg(max(key_name)).collect()[0][0]

    result_df.dropDuplicates([key_name])
    result_df.write.format('delta').option("mergeSchema", "true").mode('overwrite').save(f"s3://dataproject-bucket/data/{database_name}/{table_name}/")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    result_schem = getSchem(result_df)
    spark.sql(f"DROP TABLE IF EXISTS {database_name}.{table_name}")
    deltaTable = DeltaTable.forPath(spark, f"s3://dataproject-bucket/data/{database_name}/{table_name}/")
    deltaTable.generate("symlink_format_manifest")
    ## Glue Catalog 저장 
    spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{table_name}({result_schem})
     ROW FORMAT SERDE "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" 
     STORED AS INPUTFORMAT "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat" 
     OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat" 
     LOCATION "s3://dataproject-bucket/data/{database_name}/{table_name}/_symlink_format_manifest/"
    """)

print(error_cnt)
job.commit()
