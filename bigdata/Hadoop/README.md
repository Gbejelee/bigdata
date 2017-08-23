
This project derives various statstics from Yelp academic dataset using Hadoop map-reduce. 

Data Format
------------------------------------------------------------------------------
The dataset files are as follows and columns are separate using ‘::’ 
business.csv.
review.csv.
user.csv.

Dataset Description.
The dataset comprises of three csv files, namely user.csv, business.csv and review.csv.  

Business.csv file contain basic information about local businesses. 
Business.csv file contains the following columns "business_id"::"full_address"::"categories"

'business_id': (a unique identifier for the business)
'full_address': (localized address), 
'categories': [(localized category names)]  

review.csv file contains the star rating given by a user to a business. Use user_id to associate this review with others by the same user. Use business_id to associate this review with others of the same business. 

review.csv file contains the following columns "review_id"::"user_id"::"business_id"::"stars"
 'review_id': (a unique identifier for the review)
 'user_id': (the identifier of the reviewed business), 
 'business_id': (the identifier of the authoring user), 
 'stars': (star rating, integer 1-5),the rating given by the user to a business

user.csv file contains aggregate information about a single user across all of Yelp
user.csv file contains the following columns "user_id"::"name"::"url"
user_id': (unique user identifier), 
'name': (first name, last initial, like 'Matt J.'), this column has been made anonymous to preserve privacy 
'url': url of the user on yelp

Reduce programs in Java to find the following information.  
NB:            ::  is Column separator  in the files.

All the data files are present in /data folder.

Questions:
--------------------------------------------------------------------------------
Q1:
List the unique categories of business located in “Palo Alto” 

Q2: 
Find the top ten rated businesses using the average ratings. Top rated business will come first. Recall that 4th column in review.csv file represents the rating.

Q3:
List the  business_id , full address and categories of the Top 10 businesses using the average ratings.  
Please use reduce side join and job chaining technique to answer this problem.

Q4: 
List the 'user id' and 'rating' of users that reviewed businesses located in Stanford using In Memory Join technique.


Steps to run each program separately
--------------------------------------------------------------------------------
	Q1. 
	Copy the .jar file to the home directory of the cluster:
	--------------------------------------------------------
	scp PaloAltoQ1.jar <utdid>@csgrads1.utdallas.edu:<path to copy>

	Execute the hadoop Program from the above path
	--------------------------------------------------------
	hadoop jar PaloAltoQ1.jar CS6350.cs6350.PaloAltoQ1 business.csv output_Q1

	Copy the output for verification purpose
	--------------------------------------------------------
	hdfs dfs -get output_Q1/part-r-00000


	Q2. 
	Copy the .jar file to the home directory of the cluster:
	--------------------------------------------------------
	scp RatingQ2.jar <utdid>@csgrads1.utdallas.edu:<path to copy>

	Execute the hadoop Program from the above path
	--------------------------------------------------------
	hadoop jar RatingQ2.jar CS6350.cs6350.RatingQ2 review.csv output_Q2

	Copy the output for verification purpose
	--------------------------------------------------------
	hdfs dfs -get output_Q2/part-r-00000


	Q3. 
	Copy the .jar file to the home directory of the cluster:
	--------------------------------------------------------
	scp Rating_Bdetails_Q3.jar <utdid>@csgrads1.utdallas.edu:<path to copy>

	Execute the hadoop Program from the above path
	--------------------------------------------------------
	hdfs dfs -rm -r hdfs://cshadoop1/intermediate_output
	hadoop jar Rating_Bdetails_Q3.jar CS6350.cs6350.ReduceSideJoinQ3 review.csv business.csv output_Q3

	Copy the output for verification purpose
	--------------------------------------------------------
	hdfs dfs -get output_Q3/part-r-00000


	Q4. 
	Copy the .jar file to the home directory of the cluster:
	--------------------------------------------------------
	scp InMemoryJoinQ4.jar <utdid>@csgrads1.utdallas.edu:<path to copy>

	Execute the hadoop Program from the above path
	--------------------------------------------------------
	hadoop jar InMemoryJoinQ4.jar CS6350.cs6350.InMemoryJoinQ4  review.csv  business.csv output_Q4

	Copy the output for verification purpose
	--------------------------------------------------------
	hdfs dfs -get output_Q4/part-r-00000

