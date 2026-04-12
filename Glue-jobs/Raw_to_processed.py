
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
dyf = glueContext.create_dynamic_frame.from_catalog(database='database_name', table_name='table_name')
dyf.printSchema()
df = dyf.toDF()
df.show()
import matplotlib.pyplot as plt

# Set X-axis and Y-axis values
x = [5, 2, 8, 4, 9]
y = [10, 4, 8, 5, 2]
  
# Create a bar chart 
plt.bar(x, y)
  
# Show the plot
s3output = glueContext.getSink(
  path="s3://bucket_name/folder_name",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase="demo", catalogTableName="populations"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(DyF)
dyf = glueContext.create_dynamic_frame.from_catalog(database='freelancing', table_name='raw_data')
dyf.printSchema()
df = dyf.toDF()
df.show(3)
df.dtypes
from pyspark.sql.functions import when, col, upper,initcap,lower,regexp_replace,trim
df = df.withColumn(
    "gender",
    when(upper(col("gender")) == "F", "Female")
    .when(upper(col("gender")) == "M", "Male")
    .otherwise(col("gender"))
)
df = df.withColumn(
    "gender",
    initcap(lower(col("gender")))
    )
df = df.withColumn(
    "age",
    col("age").cast("int")
    ).withColumn(
    "years_of_experience",
    col("years_of_experience").cast("int")
    ).withColumn(
    "hourly_rate (usd)",
    regexp_replace(col("hourly_rate (usd)"), "[^0-9]", "")
    ).withColumn(
    "hourly_rate (usd)",
    col("hourly_rate (usd)").cast("int")
    )
df = df.fillna({"hourly_rate (usd)" : 0, "years_of_experience" : 0})
df.show()
df.select("is_active").distinct().show()
df = df.withColumn(
    "is_active",
    when(lower(trim(col("is_active"))).isin("yes", "true","y", "1"), "Yes")
    .when(lower(trim(col("is_active"))).isin("no", "false","n", "0"), "No")
)
df.select("is_active").distinct().show()
df.groupBy("is_active").count().show()
df = df.withColumn(
    "client_satisfaction",
    regexp_replace(col("client_satisfaction"), "[^0-9]", "")
    ).withColumn(
    "client_satisfaction",
    col("client_satisfaction").cast("int")
    )
df = df.fillna(
    {"client_satisfaction" : "0"}
    )
df.show()
df = df.withColumnRenamed("client_satisfaction", "client_satisfaction (%)")
df.printSchema()
df.write.mode("overwrite").parquet("s3://myfirst-bucket-126606499301-eu-north-1-an/Processed-Data/")
job.commit()