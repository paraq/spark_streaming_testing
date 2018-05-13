package paraq

import org.scalatest.{FunSuite}
import com.holdenkarau.spark.testing.{SharedSparkContext,RDDComparisons}


class Test_Twitterstats extends FunSuite with  SharedSparkContext with RDDComparisons {
	
  test("Tests for transformation logic") {

		val input = List ((995286878424219648L,("en","Some Text1",24L,24L)),(995286878424219648L,("en","Some Text1",2L,2L)), (995286878424219650L,("en","Some Text2",1L,1L)))

    val rdd = sc.parallelize(input)
    val rdd_reduced = rdd.reduceByKey(Twitterstats.reduce)
    val final_rdd = rdd_reduced.map(Twitterstats.finalMap)

		val rdd_reduced_expected = sc.parallelize(List ((995286878424219648L,("en","Some Text1",24L,2L)),	(995286878424219650L,("en","Some Text2",1L,1L))))
    val final_rdd_expected = sc.parallelize(List ((995286878424219648L,"en","Some Text1",23L),	(995286878424219650L,"en","Some Text2",1L)))


		assertRDDEquals(rdd_reduced,rdd_reduced_expected)
    assertRDDEquals(final_rdd,final_rdd_expected)
    assert(final_rdd.count === rdd_reduced.count )
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
    assert("No text available" === Twitterstats.getLangName("inv"))
	  }

}
