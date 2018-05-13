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
	
	// This function will be called periodically after each 5 seconds to log the output. 
	// Elements of a are of type (lang, totalRetweetsInThatLang, idOfOriginalTweet, maxRetweetCount, minRetweetCount,RetweetCount,text)
	def write2Log(a: Array[((Long,String,String,String,Long))])
	{ // i have changed the write2lof function according to my convenience
		//TweetID, TwitterUser, TextOfTweet, LanguageOftweet, TotalRetweetCount
		if (firstTime)
		{
			bw.write("Seconds,TweetID, TwitterUser, TextOfTweet, LanguageOftweet, TotalRetweetCount\n")
			t0 = System.currentTimeMillis
			firstTime = false
		}
		else
		{
			val seconds = (System.currentTimeMillis - t0) / 1000
			
			if (seconds < 60)
			{
				println("Elapsed time = " + seconds + " seconds. Logging will be started after 60 seconds.")
				return
			}
			
			println("Logging the output to the log file\nElapsed time = " + seconds + " seconds\n-----------------------------------")
			
			for(i <-0 until a.size)
			{
				val id = a(i)._1
				val username = a(i)._2
				val textStr = a(i)._3.replaceAll("\\r|\\n", " ")	//removing new line characters from the text
				val lang = getLangName(a(i)._4)
				val retweetCount = a(i)._5
				
				
				
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
	{
		return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}

	val reduce = (x: (String,String,String,Long,Long), y: (String,String,String,Long,Long)) => {
   	//
   	( y._1,y._2,y._3,math.max(x._4, y._4),math.min(x._5, y._5))	
   	/*	reduce function will give following output from current and previous values of (langcode,langname,Maxretweetcount,Minretweetcount,text)
	langcode,langname and text=> function will select respective current values
	 Maxretweetcount =>  math.max(x._3, y._3) , it will select grater value between current and previous retweetcount
	 Minretweetcount =>  math.min(x._4, y._4) , it will select smaller value between current and previous retweetcount
	 */
	
	}
	
	/*	filterTweets function can be used to filter out tweets
	 * 
	 */
	def filterTweets (tweet: twitter4j.Status) : Boolean =
	{	
		if (tweet.isRetweet()) return true else return false
		
		
	}
	
	
	/*	map function to get desired format:
	 * (TweetID,(LangCode,TotalRetweetCout,TotalRetweetCout,TweetText))
	 * 
	 * out
	 * TweetID, TwitterUser, TextOfTweet, LanguageOftweet, TotalRetweetCount
	 * 
	 * 
	 * class MyClass {
  def func1(s: String): String = { ... }
  def doStuff(rdd: RDD[String]): RDD[String] = { rdd.map(func1) }
}
	 * (TweetID, (TwitterUser, TextOfTweet, LanguageOftweet, TotalRetweetCount, TotalRetweetCount ))
	 * 
	 */
	def initMap (tweet: twitter4j.Status) : (Long,(String,String,String,Long,Long)) =
	{
		return (tweet.getRetweetedStatus().getId(),(tweet.getRetweetedStatus().getUser().getName(),tweet.getText(),
		getLang(tweet.getRetweetedStatus().getText()),tweet.getRetweetedStatus().getRetweetCount(),tweet.getRetweetedStatus().getRetweetCount()))
	}
	
	//TweetID, TwitterUser, TextOfTweet, LanguageOftweet, TotalRetweetCount
	def finalMap (rdd: (Long,(String,String,String,Long,Long))) : (Long,String,String,String,Long) =
	{
		val values = rdd._2
		return (rdd._1,values._1,values._2,values._3,values._4-values._5+1) 
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
		
		val ssc = new StreamingContext(new SparkConf(), Seconds(5))
		val tweets = TwitterUtils.createStream(ssc, None)
		
		
		//======================================================Transformations on Dstream=========================================

		
		val filter_re = tweets.filter(filterTweets)
		
		
		
		val statuses = filter_re.map(initMap)
		

		
		
		val statuses_reduce=statuses.reduceByKeyAndWindow(reduce,Seconds(60), Seconds(5))
		/* 
		   
		*/

			
		val final_rdd = statuses_reduce.map(finalMap)

		
		final_rdd.foreachRDD(rdd => write2Log(rdd.collect)) 
	
		new java.io.File("cpdir").mkdirs // make new directory for checkpoint
		ssc.checkpoint("cpdir")// to enable periodic RDD checkpointing because reduceByKeyAndWindow is used in this application.Checkpoint is used get enough data so that storage can recover
					// from failures
		ssc.start()
		ssc.awaitTermination()
	}
}


