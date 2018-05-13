package paraq

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext




class Test_Twitterstats extends FunSuite with BeforeAndAfterAll with  SharedSparkContext {
	
  test("test simple") {
	  
	  
    val list = List(1, 2, 3, 4)
    val rdd = sc.parallelize(list)

    assert(rdd.count === list.length)
  }
  
  
  test("Test for language codes"){
	  val en_text = "RT @dracomallfoys: libra: physical appearance •like taureans they are also ruled by venus which makes them conventionally beautiful"
	  val nl_text = "Ik vind dat een hele goeie vraag."
	  val de_text = "Für meine Freunde im Deutchland, bitte gib uns nicht auf"
	  val fr_text = "Tu as sûrement vu les posters dans Paris"
	  
	  assert("en" === Twitterstats.getLang(en_text))
	  assert("nl" === Twitterstats.getLang(nl_text))
	  assert("de" === Twitterstats.getLang(de_text))
	  assert("fr" === Twitterstats.getLang(fr_text))
	  assert("inv" === Twitterstats.getLang(""))

	  }
  
	test ("Tests for empty inputs") {
		
		
		
		}
  

}
