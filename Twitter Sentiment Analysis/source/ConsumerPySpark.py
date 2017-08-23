'''
Created on Apr 15, 2017
@author: Lopamudra
'''
import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from kafka import KafkaConsumer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from elasticsearch import Elasticsearch

import json
from _collections import defaultdict

# DataType and Index for Elasticsearch
dType = "hw3"
twIndex = "sentiment"

es = Elasticsearch()
if not es.indices.exists(twIndex):  # create if the index does not exist
    es.indices.create(twIndex)
    
vader = SentimentIntensityAnalyzer()

# mapping data for elasticsearch    
mapping = {
            dType: {
                "properties": {
                "author":{"type": "string"},
                "sentiment":{"type": "string"},
                "tagType":{"type": "integer"},
                # "polarity":{"type": "string"},
                "date":{"type": "date", "format": "EEE MMM dd HH:mm:ss Z yyyy"},
                "message":{"type": "string", "index": "not_analyzed"},
                "hashtags":{"type": "string", "index": "not_analyzed"},
                "location":{"type": "geo_point", "index": "not_analyzed"},
                }
            }
        }
es.indices.put_mapping(index=twIndex, doc_type=dType, body=mapping)
tagType = 0


def test_es(tweet):
    global tagType
    
    if tweet == None:
        return

    rcvd_data = tweet
    # print "Rcvd Data: ",rcvd_data
    tweet_text = rcvd_data.get("text").encode('ascii', 'ignore')
    # print "tw text: ",tweet_text
    if rcvd_data.get("place"):
            location = rcvd_data.get("place").get("bounding_box").get("coordinates")
            print "Geo:Latitude", location
            # print "Tweet: ",tweet_text
            score = vader.polarity_scores(tweet_text)
            print score
            # determine if sentiment is positive, negative, or neutral
            if score["compound"] < 0:
                sentiment = "negative"
            elif score["compound"] == 0:
                sentiment = "neutral"
            else:
                sentiment = "positive"
            # Retrieve Location(Lat,Lon) from bounding box       
            tLeft = rcvd_data.get("place").get("bounding_box").get("coordinates")[0][0]
            # while sending location in string the format will be "Lon,Lat"
            tLeft.reverse()
            tLeftString = ','.join(map(str, tLeft))
            print "Location:", tLeftString
        
            if "obama" in tweet_text.lower():
                    tagType = 1
            elif "trump" in tweet_text.lower():
                    tagType = 2
            content = {"author":rcvd_data["user"]["screen_name"].encode('ascii', 'ignore'),
                       "sentiment":sentiment,
                       # "polarity":score,
                     "date":rcvd_data["created_at"].encode('ascii', 'ignore'),
                     "message": tweet_text,
                     "location":tLeftString,
                     "tagType":tagType}
            
            print content
            
    return content
 
def test_es3(rdd):
    #print "Inside test_es3"
    
    rdd.saveAsNewAPIHadoopFile(path='-',
                                   outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                                   keyClass="org.apache.hadoop.io.NullWritable",
                                   valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                                   conf={ "es.resource" : "sentiment/hw3" })

    
def main():
    
    conf = SparkConf().setMaster("local[2]").setAppName("SentimentConsumer")
    
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)
    kstream = KafkaUtils.createDirectStream(ssc, topics=['twitter-stream'],
                                            kafkaParams={"metadata.broker.list": 'localhost:9092'})
    
    parsed_json = kstream.map(lambda (k, v): json.loads(v))
    
    parsed = parsed_json.map(test_es)
    
    parsed_mapped_data = parsed.map(lambda item: ('key', {"author":item.get("author"), "sentiment":item.get("sentiment"),
                                        "date":item.get("date"), "message": item.get("message"), "location":item.get("location"),
                                        "tagType":item.get("tagType")}))
    
    # parsed2 = parsed1.map(lambda item: ('key', item))
    parsed_mapped_data.foreachRDD(test_es3)
    
    parsed_mapped_data.pprint(1)
    # Start the computation
    ssc.start() 
    ssc.awaitTermination()
    # ssc.stop(stopGraceFully = True)
 
if __name__ == "__main__":
    main()
