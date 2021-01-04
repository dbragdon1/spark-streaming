from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark.sql import Row
from pyspark.sql.functions import lit
import os
import json
from bs4 import BeautifulSoup
import re
import spacy
from string import punctuation


"""os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.6," \
                                    "org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 " \
                                    "--conf spark.mongodb.output.uri=mongodb://127.0.0.1/test.mycollection"
"""
sc = SparkContext(appName = 'StreamingKafka')   #Create SparkContext
sc.setLogLevel('WARN')                          #Enable Warning Logs
ssc = StreamingContext(sc, 5)                   #Start streaming context
sqlconf = json.load(open('sqlconfig.json'))     #load MySQL DB params
spacy_model = spacy.load('en_core_web_sm')

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
    #document = BeautifulSoup(document, 'lxml').get_text()
    document = re.sub(r'[0-9]', '', document)
    document = "".join([c for c in document if c not in punctuation])
    return document.lower()

def get_ents(document):
    return [ent.text for ent in spacy_model(document).ents]

#TODO: Skip Dataframe transformation step and export rdd straight to db
def process(time, rdd):
    if not rdd.isEmpty():
        print('====== %s ======' % str(time))
        spark = getSparkSessionInstance(rdd.context.getConf())

        #preprocess comments by removing html tags
        remove_html = rdd.map(lambda x: (x[0], BeautifulSoup(x[1], 'lxml').get_text()))
        
        #normalize comments and get entities
        cleaned = remove_html.map(lambda x: Row(date_posted = x[0], 
                                         comment = clean(x[1]), 
                                         entities = get_ents(x[1])))

        cleaned = spark.createDataFrame(cleaned)
        cleaned.show()

        #Write to MongoDB schema
        cleaned.write.format("mongo").mode("append").save()

        #Loading Step
        #Write transformed comments to MySql table
        #df.write.format('jdbc').options(**sqlconf).mode('append').save()
        #df.show()

kafkaStream.foreachRDD(process)

ssc.start()
ssc.awaitTermination()