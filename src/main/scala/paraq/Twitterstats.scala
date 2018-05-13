package paraq


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import java.io._
import java.util.Locale
import org.apache.tika.language.LanguageIdentifier
import java.util.regex.Matcher
import java.util.regex.Pattern

object Twitterstats
{ 
	var firstTime = true
	var t0: Long = 0
	val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("twitterLog.txt"), "UTF-8"))
	var window : Int = 0
	// This function will be called periodically after each batch interval seconds to log the output.

	def write2Log(a: Array[((Long,String,String,Long))])
	{ //
		//Elapsed seconds, TweetID , LanguageOftweet, TotalRetweetCount, TextOfTweet
		if (firstTime)
		{
			bw.write("Seconds,TweetID, TwitterUser, TextOfTweet, LanguageOftweet, TotalRetweetCount\n")
			t0 = System.currentTimeMillis
			firstTime = false
		}
		else
		{
			val seconds = (System.currentTimeMillis - t0) / 1000
			
			if (seconds < window)
			{
				println("Elapsed time = " + seconds + " seconds. Logging will be started after "+window+" seconds.")
				return
			}
			
			println("Logging the output to the log file\nElapsed time = " + seconds + " seconds\n-----------------------------------")
			
			for(i <-0 until a.size)
			{
				val id = a(i)._1
				val textStr = a(i)._2.replaceAll("\\r|\\n", " ")	//removing new line characters from the text
				val lang = getLangName(a(i)._3)
				val retweetCount = a(i)._4
				
				
				
				bw.write("(" + seconds + ")," + id  + "," + lang + "," + 
					retweetCount + "," + textStr  + "\n")
			}
		}
	}
  
	// Pass the text of the retweet to this function to get the Language (in two letter code form) of that text.
	def getLang(s: String) : String =
	{	
		if (s.isEmpty) return "inv"
		val inputStr = s.replaceFirst("RT", "").replaceAll("@\\p{L}+", "").replaceAll("https?://\\S+\\s?", "")
		var langCode = new LanguageIdentifier(inputStr).getLanguage
		
		// Detect if japanese
		var pat = Pattern.compile("\\p{InHiragana}") 
		var m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ja"
		// Detect if korean
		pat = Pattern.compile("\\p{IsHangul}");
		m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ko"
		
		return langCode
	}
  
	// Gets Language's name from its code
	def getLangName(code: String) : String =
	{	if (code == "inv") return "No text available"
		return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}

	//get max and min values of retweetcount of tweets within window
	val reduce = (x: (String,String,Long,Long), y: (String,String,Long,Long)) => {
   	//
   	( y._1,y._2,math.max(x._3, y._3),math.min(x._4, y._4))

	
	}

	// Only select more than 500 retweeted tweets
	def filterTweets (tweet: twitter4j.Status) : Boolean =
	{	
		if (tweet.isRetweet() && tweet.getRetweetedStatus().getRetweetCount() > 500) return true else return false
		
		
	}
	
	

	// maps twitter4j.Status to (TweetID, (TextOfTweet, LanguageOftweet, TotalRetweetCount, TotalRetweetCount ))
	def initMap (tweet: twitter4j.Status) : (Long,(String,String,Long,Long)) =
	{
		return (tweet.getRetweetedStatus().getId(),(tweet.getText(),getLang(tweet.getRetweetedStatus().getText()),
			tweet.getRetweetedStatus().getRetweetCount(),tweet.getRetweetedStatus().getRetweetCount()))
	}
	
	//TweetID, TextOfTweet, LanguageOftweet, TotalRetweetCount
	// where TotalRetweetCount = max(retweetCount) - min(retweetCount) + 1
	def finalMap (rdd: (Long,(String,String,Long,Long))) : (Long,String,String,Long) =
	{
		val values = rdd._2
		return (rdd._1,values._1,values._2,values._3-values._4+1)
	}
	
	
	
	def main(args: Array[String]) 
	{
		// Configure Twitter credentials
		val apiKey = " MnVNvmaFsXDsynk4itBhQYIkB"
		val apiSecret = "JFzxiXxNskhiE7D8t9Pn8qQbraDu8Qz10f2auhXIYEafqsOTXs"
		val accessToken = " 784442412043296768-zrWqfEIy4zXF9aij0Tc9GaDo2Kiswiu"
		val accessTokenSecret = " 8XaAqR9LOoJh7sKtkxqcNitdrA29qb6zOdaMBDKWNiaOU"

		//authentication of twitter application
		Helper.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)

		//setting time interval, window and sliding time
		val interval = args(0).toInt
		window = args(1).toInt
		val sliding = args(2).toInt

		//create streaming context
		val ssc = new StreamingContext(new SparkConf(), Seconds(interval))
		val tweets = TwitterUtils.createStream(ssc, None)
		
		
		//======================================================Transformations on Dstream=========================================

		//Filtering retweeted tweets
		val filter_re = tweets.filter(filterTweets)
		
		
		//convert twitter4j.status into required tuple
		val statuses = filter_re.map(initMap)

		//get maximum and minimum retweetcount of a tweet
		val statuses_reduce=statuses.reduceByKeyAndWindow(reduce,Seconds(window), Seconds(sliding))

		//calculate retweetcount during batch interval and sort according to retweet count
		val final_rdd = statuses_reduce.map(finalMap).transform( rdd => rdd.sortBy(_._4,false))

		//write to log file
		final_rdd.foreachRDD(rdd => write2Log(rdd.collect))
	
		new java.io.File("cpdir").mkdirs // make new directory for checkpoint
		ssc.checkpoint("cpdir")// to enable periodic RDD checkpointing.Checkpoint is used get enough data so that storage can recover
					// from failures
		ssc.start()
		ssc.awaitTermination()
	}
}


