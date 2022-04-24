import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import window
import json

# we initiate the StreamingContext with 10 second batch interval.
spark = spark = SparkSession.builder.appName("hashtagcount").getOrCreate()

lines= spark.readStream.format("socket").option("host","localhost").option("port",5555).load()
hashtags = lines.select(explode(split(lines.value, '#')).alias("hashtag"))
hashtags=hashtags.withColumn("timestamp", current_timestamp())
hashtag_counts=hashtags.withWatermark("timestamp", "5 minutes").groupBy(window("timestamp","5 minutes"),hashtags.hashtag).count()
query = hashtag_counts.writeStream.outputMode("append").format("console").option("path","/home/chakita/DBT/output.csv").option("checkpointLocation", "/tmp/dbt/checkpoint").start()
query.awaitTermination()
