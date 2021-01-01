from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark.sql import Row
import os
import json
from bs4 import BeautifulSoup
import re

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.6, pyspark-shell'   

sc = SparkContext(appName = 'StreamingKafka')   #Create SparkContext
sc.setLogLevel('WARN')                          #Enable Warning Logs
ssc = StreamingContext(sc, 5)                   #Start streaming context
sqlconf = json.load(open('sqlconfig.json'))     #load MySQL DB params

#Start Kafka stream listener
kafkaStream = KafkaUtils.createStream(ssc = ssc, 
                                      zkQuorum = 'localhost:2181',
                                      groupId = 'hn-streaming',
                                      topics = {'hncomments': 1})


def getSparkSessionInstance(sparkConf):
    """
    Used for creating a temporary spark session for converting the transformed data to a DataFrame
    """
    if ("sparkSessionSingletonInscance" not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession \
            .builder \
            .config(conf = sparkConf) \
            .getOrCreate()
        return globals()["sparkSessionSingletonInstance"]

def clean(document):
    """
    Very simple method for normalizing comments
    1. Remove html tags
    2. Remove numbers
    3. Convert to lowercase
    """
    document = BeautifulSoup(document, 'lxml').get_text()
    document = re.sub(r'[0-9]', '', document)
    return document.lower()

#TODO: Skip Dataframe transformation step and export rdd straight to db
def process(time, rdd):
    if not rdd.isEmpty():
        print('====== %s ======' % str(time))
        spark = getSparkSessionInstance(rdd.context.getConf())
        
        #Transforming step
        clean_rdd = rdd.map(lambda x: (x[0], clean(x[1]))) #normalize comments
        rowRdd = clean_rdd.map(lambda x: Row(comment_id = x[0], comment = x[1]))
        df = spark.createDataFrame(rowRdd)

        #Loading Step
        #Write transformed comments to MySql table
        df.write.format('jdbc').options(**sqlconf).mode('append').save()
        df.show()

kafkaStream.foreachRDD(process)

ssc.start()
ssc.awaitTermination()