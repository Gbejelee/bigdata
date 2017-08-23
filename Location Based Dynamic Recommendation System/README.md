Dynamic Resturant Recommendation system:

For coping up with the cut throat competition restaurants work on negative reviews for getting better.
For example, if the restaurant which didn’t serve better pizza yesterday tries to improve. 
This project will recommend the user depending on the reviews given on the same day, which helps him/her to choose the best restaurant on that day.
As getting real time streaming data from yelp is difficult to obtain, so an online streaming process is simulated with a Python program using Kafka and Zookeeper servers,
which streams the customer reviews of the restaurants nearby. It recommends top 5 TRENDING restaurants in a city according to user given food category and location.

Technology Used:
---------------------------------------------------------------------
1. Python (as Kafka Producer/Consumer)
2. Apache Kafka
3. Zookeeper
4. Scala (Removing unnecessary attributes)
5. Elasticsearch(for storing JSON file)  
6. Kibana (for visualization)

Steps to run
---------------------------------------------------------------------
1. Start the following servers
	a) Zookeeper
	b) Kafka
	c) Elastic Search
	d) Kibana
2. Run the Kafka Producer(yelpScrapper.py) in PySpark
3. Run the Kafka Consumer(consumer.py) in PySpark
4. Run the Recommender program (recommender.py) in PySpark
5. Open the kibana server in local browser(localhost:5601) for the visualization.

N.B. This project was completed based on data from LasVegas city only. But it can be easily
extended for any city given the availability of Yelp data.  
