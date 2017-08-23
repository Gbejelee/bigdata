Objective:
———————————————————————————————————————
The objective of the project is to performs SENTIMENT analysis of particular hash tags in tweeter data in real-time. 
For example, we want to do the sentiment analysis for all the tweets for #trump, #obama and show them 
(e.g., positive, neutral, negative, etc. tweets) on a map. When we show tweets on a map, we plot them using their latitude and longitude. 
It has three components:

1. Scrapper: The scrapper is a standalone program written in PYTHON to collect all tweets and sends them to Kafka for analytics.
   It has following functionalities.
	a. Collecting tweets in real-time with particular hash tags. For example, collect all tweets with #trump, #obama.
	b. After getting tweets, filter them based on their latitude and longitude. If any tweet does not have latitude and longitude, we will discard them.
	c. Next we will send them (tweets with lat/lng) to Kafka.
	d. Kafka API (producer) collect the data and send it to the Kafka consumer.

2. Streaming: 

In Spark Streaming, Kafka consumer is created that periodically collect filtered tweets from scrapper/Kafka producer. 
For each has tag, sentiment analysis using Sentiment Analyzing tool (nltk.sentiment.vader) is performed. 
Then for each hash tag, output is sent to Elasticsearch for visualization.

3. Visualizer:
A visualization index is created in Elasticsearch. A map is created using Kibana to show all kinds of tweets. After that, a dashboard showing the map is created. 
Dashboard requires data to be time stamped. In the dashboard set a Map refresh time to 2 min as an example.

Steps to execute the program:
——————————————————————————————————————————

1. Command to run the zookeeper
	zookeeper-server-start.sh /usr/local/etc/kafka/zookeeper.properties
2. Command to run the Kafka server
	kafka-server-start /usr/local/etc/kafka/server.properties
3. Command to run the elasticsearch
	elasticsearch
4. Command to run kibana
	kibana
5. Run the TwitterScapper
	python TwitterScapper.py
6. Run the pySparkSentiment Analyser
	spark-submit --jars ~/Downloads/spark-streaming-kafka-assembly_2.10-1.6.3.jar,/usr/local/spark/jars/elasticsearch-hadoop-5.3.0.jar ConsumerPySpark.py

####################################################
Dependency resolution:
1. To send the data to elasticsearch from the pyspark download below jar file from https://mvnrepository.com/
	elasticsearch-hadoop-5.3.0.jar
	Group: org.elasticsearch
	Artifact : elasticsearch-hadoop

2. To receive the data from kafka in pySpark download below jar file from https://mvnrepository.com/artifact/org.apache.spark
	spark-streaming-kafka-assembly_2.10-1.6.3.jar
	Group: org.apache.spark
