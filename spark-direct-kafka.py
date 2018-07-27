# -*- coding: utf-8 -*-
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition


offsetRanges = []

def storeOffsetRanges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd

def printOffsetRanges(rdd):
    for o in offsetRanges:
        print "%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset)

#directKafkaStream \
#    .transform(storeOffsetRanges) \
#   .foreachRDD(printOffsetRanges)

if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 10)

#    brokers, topic = sys.argv[1:]
    brokers  = ':9092'
    topic = 'cdh-test'
    kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers}, fromOffsets={TopicAndPartition(topic, 0): long(0)})
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a+b)
    counts.pprint()
    kvs.transform(storeOffsetRanges).foreachRDD(printOffsetRanges)
    ssc.start()
    ssc.awaitTermination()
