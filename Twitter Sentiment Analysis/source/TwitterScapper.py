'''
Created on Apr 11, 2017
 
@author: Lopamudra
'''
import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import json
import Config
from kafka import SimpleProducer, KafkaClient
 
# Kafka settings
topic = b'twitter-stream'
# setting up Kafka producer
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)
 
 
#Track By hashTag
TRACK_BY = ['#trump', '#obama']
parentpath='/Users/atishpatra/SECOND_SEM_UTD/BigDataMgmt/Homework3'
today = time.strftime("%Y%m%d")
jsonfilepath_info=parentpath+'/twitterData_'+today+'.json'
 
class MyListener(StreamListener):
    """Custom StreamListener for streaming data."""
 
    def on_data(self, data):
        try:
            # with open(jsonfilepath_info, 'a') as f:
            pdata = json.loads(data)     
            print "Screen Name:", pdata.get("user").get("screen_name")
            if pdata.get("place"):
                producer.send_messages(topic, data.encode())
                # f.write(data)
                print "Geo:Latitude", pdata.get("place").get("bounding_box").get("coordinates")[0]
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            time.sleep(5)
        return True
 
    def on_error(self, status):
        print("Error received in kafka producer")
        print(status)
        return True
 
 
if __name__ == '__main__':
    auth = OAuthHandler(Config.consumer_key, Config.consumer_secret)
    auth.set_access_token(Config.access_token, Config.access_secret)
    api = tweepy.API(auth)
 
    twitter_stream = Stream(auth, MyListener())
    twitter_stream.filter(track=['#trump','#obama'])
