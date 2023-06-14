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

def getSchem(df):
    sche = df.schema.json()
    dic_schem  = json.loads(sche)
    result = ""

    for idx, val in enumerate(dic_schem['fields']):
        result += f"`{val['name']}`"+ f" {val['type'].upper()}"
        if len(dic_schem['fields'])-1 != idx:
            result += ', '
    return result


def directJDBCSource(
    glueContext,
    connectionName,
    connectionType,
    database,
    table,
    query,
    redshiftTmpDir

) -> DynamicFrame:

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
    .set("hive.metastore.warehouse.dir", "s3://dataproject-bucket/data/*******/")
    .set("spark.databricks.hive.metastore.glueCatalog.enabled", "true")
    )

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

database_name = "information_schema"
table_name = "COLUMNS"
que = """
SELECT
	 table_schema,
     TABLE_NAME,
     COLUMN_NAME,
     IS_NULLABLE,
     DATA_TYPE,
     COLUMN_TYPE,
     COLUMN_KEY,
     EXTRA
FROM
   information_schema.COLUMNS
WHERE table_schema = 'carmore'
"""

JDBCConnection_node1 = directJDBCSource(
    glueContext,
    connectionName="Aws_RDS_MySQL_connection",
    connectionType="mysql",
    database=database_name,
    table = table_name,
    query = que,
    redshiftTmpDir=""
)
jdbcDF = JDBCConnection_node1.toDF()

jdbcDF.repartition(1).write.format('parquet').mode('overwrite').save("s3://dataproject-bucket/data/caremore_allSchema/")

job.commit()
