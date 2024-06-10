#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
from pyspark.sql.functions import to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import col, count, avg, log, to_timestamp
from google.cloud import bigquery
import pandas as pd
import logging

# Initialize a Spark session
spark = SparkSession.builder.appName("YouTubeAnalytics").getOrCreate()

# Define the schema
schema = StructType([
    StructField("video_id", StringType(), True),
    StructField("channelTitle", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("publishedAt", StringType(), True),
    StructField("viewCount", FloatType(), True),
    StructField("likeCount", FloatType(), True),
    StructField("favouriteCount", FloatType(), True),
    StructField("commentCount", FloatType(), True),
    StructField("duration", StringType(), True),
    StructField("definition", StringType(), True),
    StructField("caption", BooleanType(), True),
    StructField("publishDayName", StringType(), True),
    StructField("durationSecs", FloatType(), True),
    StructField("tagsCount", IntegerType(), True),
    StructField("likeRatio", FloatType(), True),
    StructField("commentRatio", FloatType(), True),
    StructField("titleLength", IntegerType(), True)
])

project_id = "de2-project-424816"
client = bigquery.Client(project=project_id)

# Load the CSV file from GCS into a DataFrame with the specified schema
gcs_path = "gs://de2_project_youtube_analysis/data.csv"
dfs = spark.read.option("header", "true") \
    .option("multiline", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .schema(schema) \
    .csv(gcs_path)

#Null Value Imputation
dfs = dfs.fillna({'likeCount':0, 'likeRatio':0, 'commentRatio':0, 'commentCount':0, 'description':'N/A', 'tags':'N/A'})
dfs = dfs.drop('favouriteCount','duration')
dfs = dfs.dropDuplicates()

#changing datatypes
dfs = dfs.withColumn('publishedAt', to_timestamp('publishedAt', 'yyyy-MM-dd HH:mm:ssXXX'))

#creating fact table
df_info = dfs.select('video_id','channelTitle','title', 'caption', 'definition')
df_info = df_info.dropDuplicates()

#creating description table
df_description = dfs.select('video_id','description','tags')
df_description = df_description.dropDuplicates()
df_description = df_description.toPandas()

#creating count table
df_count = dfs.select('video_id', 'durationSecs', 'viewCount', 'likeCount', 'commentCount', 'tagsCount', 'likeRatio', 'commentRatio', 'titleLength' )
df_count = df_count.dropDuplicates()
df_count = df_count.toPandas()

#creating publish_info
df_publish_info = dfs.select('video_id', 'publishedAt', 'publishDayName')
df_publish_info = df_publish_info.dropDuplicates()
df_publish_info = df_publish_info.toPandas()

#creating definition table
df_hd = dfs.select('definition')
df_hd = df_hd.dropDuplicates()
df_hd = df_hd.withColumn("definition_id", monotonically_increasing_id())

#creating caption table
df_cap = dfs.select('caption')
df_cap = df_cap.dropDuplicates()
df_cap = df_cap.withColumn("caption_id", monotonically_increasing_id())

#merging definition with fact table
df_info = df_info.join(df_hd, on = 'definition', how = 'inner')

#merging caption with fact table
df_info = df_info.join(df_cap, on = 'caption', how = 'inner')
df_info = df_info.drop('caption', 'definition')
df_info = df_info.toPandas()
df_hd = df_hd.toPandas()
df_cap = df_cap.toPandas()

# Define table IDs
table_id1 = "de2-project-424816.youtube_analytics.info"
table_id2 = "de2-project-424816.youtube_analytics.description"
table_id3 = "de2-project-424816.youtube_analytics.count"
table_id4 = "de2-project-424816.youtube_analytics.publish_info"
table_id5 = "de2-project-424816.youtube_analytics.definition"
table_id6 = "de2-project-424816.youtube_analytics.caption"

#creating schemas for tables
job_config1 = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("video_id", "STRING"),
        bigquery.SchemaField("channelTitle", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("definition_id", "INTEGER"),
        bigquery.SchemaField("caption_id", "INTEGER")
    ],
    write_disposition="WRITE_TRUNCATE"
)

job_config2 = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("video_id", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("tags", "STRING")
    ],
    write_disposition="WRITE_TRUNCATE"
)

job_config3 = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("video_id", "STRING"),
        bigquery.SchemaField("durationSecs", "FLOAT"),
        bigquery.SchemaField("viewCount", "FLOAT"),
        bigquery.SchemaField("likeCount", "FLOAT"),
        bigquery.SchemaField("commentCount", "FLOAT"),
        bigquery.SchemaField("tagsCount", "INTEGER"),
        bigquery.SchemaField("likeRatio", "FLOAT"),
        bigquery.SchemaField("commentRatio", "FLOAT"),
        bigquery.SchemaField("titleLength", "INTEGER"),
    ],
    write_disposition="WRITE_TRUNCATE"
)

job_config4 = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("video_id", "STRING"),
        bigquery.SchemaField("publishedAt", "TIMESTAMP"),
        bigquery.SchemaField("publishDayName", "STRING")
    ],
    write_disposition="WRITE_TRUNCATE"
)

job_config5 = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("definition", "STRING"),
        bigquery.SchemaField("definition_id", "INTEGER"),
    ],
    write_disposition="WRITE_TRUNCATE"
)

job_config6 = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("caption", "BOOLEAN"),
        bigquery.SchemaField("caption_id", "INTEGER"),
    ],
    write_disposition="WRITE_TRUNCATE"
)

# Function to upload DataFrame to BigQuery
def upload_to_bigquery(df, table_id, job_config):
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        table = client.get_table(table_id)  # Make an API request
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
    except Exception as e:
        logging.error(f"Failed to upload data to BigQuery: {e}")

# Upload DataFrames to BigQuery
upload_to_bigquery(df_info, table_id1, job_config1)
upload_to_bigquery(df_description, table_id2, job_config2)
upload_to_bigquery(df_count, table_id3, job_config3)
upload_to_bigquery(df_publish_info, table_id4, job_config4)
upload_to_bigquery(df_hd, table_id5, job_config5)
upload_to_bigquery(df_cap, table_id6, job_config6)

# Stop the Spark session
spark.stop()

