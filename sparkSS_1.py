import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import sys
from pyspark.sql.types import *

def fun(senti_val):
    try:
        if senti_val < 0: return 'NEGATIVE'
        elif senti_val == 0: return 'NEUTRAL'
        else: return 'POSITIVE'
    except TypeError:
        return 'NEUTRAL'



schema = StructType([StructField("text",StringType(), True),StructField("senti_val", DoubleType(), True)])

spark = SparkSession \
    .builder \
    .appName("StreamingApp") \
    .getOrCreate()

streamingInputDF = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "ec2-3-15-26-49.us-east-2.compute.amazonaws.com:9092") \
  .option("subscribe", "twitter") \
  .load()

lines = streamingInputDF.selectExpr("CAST(value AS STRING)")

tweets_table = lines.select(from_json(col("value"), schema).alias("data")).select("data.*")

#val_table = tweets_table.select(('text'),('senti_val').alias('senti_val'))

to_status = udf(fun, StringType())

df = tweets_table.withColumn("status", to_status("senti_val"))

sentimentCount = df.select("status")
sentimentCount = sentimentCount.groupBy("status").count()


def toMetric(df,id):
    n = df.count()
    if (n >= 3):
        PosCount = df.where(df.status == "POSITIVE").select('count').collect()[0]['count']
        NegCount = df.where(df.status == "NEGATIVE").select('count').collect()[0]['count']
        NeuCount = df.where(df.status == "NEUTRAL").select('count').collect()[0]['count']
        client = boto3.client('cloudwatch')
        client.put_metric_data(
            MetricData=[
                {
                    'MetricName': 'Sentiment',
                    'Dimensions': [
                        {
                            'Name': 'Type',
                            'Value': 'Positive'
                        },
                    ],
                    'Unit': 'Count',
                    'Value': PosCount
                }
            ],
            Namespace='Twitter'
        )
        client.put_metric_data(
            MetricData=[
                {
                    'MetricName': 'Sentiment',
                    'Dimensions': [
                        {
                            'Name': 'Type',
                            'Value': 'Negative'
                        },
                    ],
                    'Unit': 'Count',
                    'Value': NegCount
                },
            ],
            Namespace='Twitter'
        )
        client.put_metric_data(
            MetricData=[
                {
                    'MetricName': 'Sentiment',
                    'Dimensions': [
                        {
                            'Name': 'Type',
                            'Value': 'Neutral'
                        },
                    ],
                    'Unit': 'Count',
                    'Value': NeuCount
                },
            ],
            Namespace='Twitter'
        )



query = sentimentCount \
    .writeStream \
    .foreachBatch(toMetric) \
    .outputMode("complete") \
    .start()
query.awaitTermination()
