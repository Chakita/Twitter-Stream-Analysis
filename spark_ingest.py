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
spark = SparkSession.builder.appName("hashtagcount").getOrCreate()
print(spark.sparkContext.defaultMinPartitions )
lines= spark.readStream.format("socket").option("host","localhost").option("port",5555).load()
hashtags = lines.select(lines["value"].alias("topic"))
hashtags=hashtags.withColumn("timestamp", current_timestamp())
#q2=hashtags.writeStream.outputMode("append").format("console").option("path","/home/chakita/DBT-output").option("checkpointLocation", "/tmp/dbt/checkpoint").start()
hashtag_counts=hashtags.withWatermark("timestamp", "1 second").groupBy(window("timestamp","5 minutes"),hashtags.topic).count()
hashtag_counts=hashtag_counts.select(hashtag_counts["topic"],hashtag_counts["count"].alias("value"))
query = hashtag_counts.writeStream.outputMode("append").format("console").option("path","/home/chakita/DBT-output").option("checkpointLocation", "/tmp/dbt/checkpoint").start()
query.awaitTermination()
#q2.awaitTermination()
