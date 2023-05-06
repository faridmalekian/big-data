import findspark
findspark.init('/home/farid/spark/')
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka import KafkaProducer
import json

spark = SparkSession.builder.master("local").appName("send").getOrCreate()
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
df = spark.read.options(inferSchema='True',header='True').csv("test.csv")
df=df.select(["Lat","Lon"])
df.show()
train, test = df.randomSplit([0.8, 0.2], seed=10)
print(f"Sending {test.count()} rows.")
for row in test.toJSON().collect():
    producer.send("mytopic12345678", row.encode('utf-8'))
print("sending complete.")
producer.close()
