// read the tip json data
// transform it
// store it to hdfs 
val tipDF = sqlContext.jsonFile("/user/cloudera/rawdata/yelp/tips")
val refinedTipDF = tipDF.select("user_id","business_id","likes","text","date").repartition(1)

refinedTipDF.write.parquet("/user/cloudera/output/yelp/tips")

//we build the hive tip table on top of this output to allow users query our data
import org.apache.spark.sql.Row


//integrating spark and hive using the hive context
import org.apache.spark.sql.hive.HiveContext

val hiveCtx = new HiveContext(sc)
import hiveCtx.implicits._

//Another wai write directly to a hive table already created in hive using insertInto command

val reviewDF = hiveCtx.jsonFile("/user/cloudera/rawdata/yelp/reviews").
		select("review_id","user_id","business_id","stars","text","date","votes.cool","votes.funny","votes.useful")
reviewDF.write.insertInto("yelp.review")


// normalizing the user dataset to a many-to-many self referencing model
val userDF = hiveCtx.jsonFile("/user/cloudera/rawdata/yelp/users")

val friends = userDF.select("user_id", "friends").rdd

def parseRow(row: Row) : Seq[(String, String)] ={
	val user_Id = row.getAs[String](0)
	val fList = row.getAs[scala.collection.mutable.WrappedArray[String]](1)

	fList.map(s => (user_Id, s))
}


val userfriendDF = friends.flatMap(parseRow(_)).toDF
//write into database table
userfriendDF.write.insertInto("user_friends")


val userMainDF = userDF.select("user_id","name","review_count","yelping_since","votes.useful","votes.funny","votes.cool","fans","elite","average_stars","compliments.hot",  "compliments.more", "compliments.profile", "compliments.cute", "compliments.list", "compliments.note", "compliments.plain", "compliments.cool", "compliments.funny", "compliments.writer", "compliments.photos")

 userMainDF.write.insertInto("user")


//Top states and cities in total number of reviews(Top State Cities)
val top_states_cities= hiveCtx.jsonFile("hdfs://0.0.0.0:19000/rawdata/business/business.json").select("state","ci
ty","review_count","stars").repartition(1)
states_cities.write.csv("hdfs://0.0.0.0:19000/output/states_cities")


//Average number of reviews per business star rating (Review Per Stars)
//Top businesses with high review counts (> 1000)

val review_per_star= hiveCtx.jsonFile("hdfs://0.0.0.0:19000/rawdata/business/business.json").select("business_id","review_count","stars").repartition(1)
review_per_star.write.csv("hdfs://0.0.0.0:19000/output/review_per_star")

//Check the Saturday open and close times for few businesses (Saturday Open Close)
val saturday_open_close= hiveCtx.jsonFile("hdfs://0.0.0.0:19000/rawdata/business/business.json").select("business_id","hours").repartition(1)

saturday_open_close.write.csv("hdfs://0.0.0.0:19000/output/saturday_open_close")



//Top restaurants in number of listed categories (Restaurant Categories)
val restaurant_categories= hiveCtx.jsonFile("hdfs://0.0.0.0:19000/rawdata/business/business.json").select("business_id","categories").repartition(1)

restaurant_categories.write.json("hdfs://0.0.0.0:19000/output/restaurant_categories")

//Top businesses with different rated reviews (i.e. cool, funny, useful) (Business Review Rated)
//Total reviews over time - This indicates the health of the business
val business_review_rated= hiveCtx.sql("select date,business_id,useful,funny,cool from yelp.review").repartition(1)

business_review_rated.write.csv("hdfs://0.0.0.0:19000/output/business_review_rated")

//Top categories used in business reviews (Categories Business Review)

val categories_business_review= hiveCtx.jsonFile("hdfs://0.0.0.0:19000/rawdata/business/business.json").select("business_id","categories","review_count").repartition(1)
categories_business_review.write.json("hdfs://0.0.0.0:19000/output/categories_business_review")

//Business Checkins (Business_Checkins)
//Checkins by day of the week - This indicates when to the customers like to visit the business
//Checkins by hour - This indicates popular time of the business, which would help determine operating hours as well as better staff allocation to serve the customers

val business_checkins= hiveCtx.jsonFile("hdfs://0.0.0.0:19000/rawdata/checkins/checkin.json").select("business_id
","date").repartition(1)


business_checkins.write.json("hdfs://0.0.0.0:19000/output/business_checkins")

