import findspark
findspark.init('/home/farid/spark/')
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.linalg import VectorUDT
import threading
from kafka import KafkaConsumer
import json
import time



spark = SparkSession.builder.master("local").appName("receive").getOrCreate()
schema = StructType([
    StructField('Lat', DoubleType(), True),
    StructField('Lon', DoubleType(), True),
])




df = spark.read.options(inferSchema='True',header='True').csv("uber-raw-data-aug14.csv").select(["Lat","Lon"])

model = kmeans.load(cluster)




consumer = KafkaConsumer(
    'mytopic12345678',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_group'
)

input_col_order = ["Lat","Lon"]
finalschema = StructType([
    StructField('Lat', DoubleType(), True),
    StructField('Lon', DoubleType(), True),
    StructField('features', VectorUDT(), True),
    StructField('prediction', IntegerType(), True)])
mydf = spark.createDataFrame([], finalschema)

Queue=[]

lock=False
def QueueProcessorThread():
    global mydf
    global Queue
    global lock
    idle=0
    print("QueueProcessorThread is Running")
    while True:
        time.sleep(0.1)
        if len(Queue)>=2500:
            lock=True
            print(f"QueueProcessorThread: received {len(Queue)} rows. predicting...")
            dict_list = [json.loads(s) for s in Queue]
            Queue = []
            idle=0
            df = spark.createDataFrame(dict_list)
            assembler = VectorAssembler(inputCols=input_col_order, outputCol="features")
            df = assembler.transform(df)
            predictions = model.transform(df)
            cache=mydf.union(predictions)
            mydf=cache
            print("QueueProcessorThread: predicting for 2500 rows complete. saved in mydf")
            lock=False
        else:
            idle=idle+1
            if idle%10==0:
                print(f"QueueProcessorThread: Idle for {idle/10}")
        if idle>=50:
            if idle>=2000:
                break
            elif not len(Queue)==0:
                lock=True
                remain=len(Queue)
                print(f"QueueProcessorThread: predicting for {remain} rows")
                dict_list = [json.loads(s) for s in Queue]
                Queue = []
                df = spark.createDataFrame(dict_list)
                assembler = VectorAssembler(inputCols=input_col_order, outputCol="features")
                df = assembler.transform(df)
                predictions = model.transform(df)
                cache=mydf.union(predictions)
                mydf=cache
                print(f"QueueProcessorThread: predicting for {remain} rows complete. saved in mydf")
                print("QueueProcessorThread: received all rows and predicted based on k-beans with k=8")
                mydf.select("Lat", "Lon", "prediction").coalesce(1).write.mode('overwrite').csv("/home/farid/Documents/bigdata/predict.csv")
                print(mydf.select("Lat", "Lon", "prediction"))
                
                print("QueueProcessorThread: Saved mydf with predictions.")
                idle=0
                lock=False


def QueueReceiverThread():
    global Queue
    global lock
    print("QueueReceiverThread is Running")        
    for message in consumer:
        while lock:
            ...
        Queue.append(message.value.decode('utf-8'))
        if len(Queue)>=2500:
            lock=True


t1 = threading.Thread(target=QueueProcessorThread)
t2 = threading.Thread(target=QueueReceiverThread)
t1.start()
t2.start()
t1.join()
t2.join()
