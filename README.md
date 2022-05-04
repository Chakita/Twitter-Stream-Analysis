# DBT-Project
## Steps to run the code:
- Start confluent services using ```confluent local services start```
- Open up a new terminal and run ```python3 twitter_stream.py```
- Open another terminal and run ```spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.0 spark_ingest.py```
- Open a third terminal and run the Kafka consumer using ```python3 consumer.py```
- You will be able to observe the consumer reading the spark data. Check the database to see if the data has been entered into the db.

