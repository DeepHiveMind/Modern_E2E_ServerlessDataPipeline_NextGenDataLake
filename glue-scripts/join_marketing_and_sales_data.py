
import sys
import pyspark.sql.functions as func
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_output_path', 's3_sales_data_path', 's3_marketing_data_path'])
s3_output_path = args['s3_output_path']
s3_sales_data_path = args['s3_sales_data_path']
s3_marketing_data_path = args['s3_marketing_data_path']

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

salesForecastedByDate_DF = \
    glueContext.spark_session.read.option("header", "true")\
        .load(s3_sales_data_path, format="parquet")

mktg_DF = \
    glueContext.spark_session.read.option("header", "true")\
        .load(s3_marketing_data_path, format="parquet")


salesForecastedByDate_DF\
    .join(mktg_DF, 'date', 'inner')\
    .orderBy(salesForecastedByDate_DF['date']) \
    .write \
    .format('csv') \
    .option('header', 'true') \
    .mode('overwrite') \
    .save(s3_output_path)
