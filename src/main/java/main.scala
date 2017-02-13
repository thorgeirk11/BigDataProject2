import java.util

import info.debatty.java.stringsimilarity.interfaces.StringSimilarity
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import info.debatty.java.stringsimilarity.Levenshtein
import info.debatty.java.stringsimilarity.JaroWinkler
import info.debatty.java.stringsimilarity.NGram

import org.apache.spark.broadcast.Broadcast;

case class Person(
  EinstID: Int,
  Nafn: String,
  Fdagur: String,
  Kyn: String,
  FelagISI : String,
  Netfang : String,
  Heimilisfang1 : String,
  Heimilisfang2 : String,
  Heimilisfang3 : String,
  Simi1 : String,
  Simi2 : String,
  Simi3 : String,
  Timastimpill: String,
  Haed : String)

object BigDataProject2 {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master(args(0))
      .appName("BigDataProject2")
      .getOrCreate()
    import session.implicits._

    val df: DataFrame = session.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("inferSchema", "true") // Automatically infer data types
      .option("header", "true") // Use first line of all files as header
      .load("DataCSV/blak-einstaklingar.csv")
     df.createOrReplaceTempView("People")
    //df.show()
    val jaro = new JaroWinkler()
    val testSQL = session.sql("Select * from People")
    val l = List()
    val x = testSQL.collect()
    // a <- 1 to 3; b <- 1 to 3
    for(i <- 1 to x.length-1) {
      for (j <- i+1 to x.length-1) {
        val jar =jaro.distance(x(i)(1).toString(),x(j)(1).toString())
        if(jar < 0.1) {
          println(x(i)(1).toString()+ " | " +  x(j)(1).toString()+" | " +jar)
        }
      }
    }
    //l.foreach(println)
    //l.foreach(x => println(x._1 + " | " + x._2 + " | " + x._3))
    /*val search = df.as[Person]
    search.show()
    val broadcastVar = session.sparkContext.broadcast(search)
*/


    /*val stuff = search.flatMap((row: Person) => {
      val x = broadcastVar.value.map( b => {
        (row.EinstID.toInt, b.EinstID.toInt, j.distance( row.Nafn.toString(), b.Nafn.toString() ))
      } )
      x.collect()
    }).filter(x=>x._3 < 0.1)
      .distinct()
    
//    val x = search.map( b => {
//      (search.first().EinstID, b.EinstID, j.distance( search.first().Nafn, b.Nafn ))
//    } )

    stuff.foreach(x=> println( x._1 + " | " + x._2 + " | " + x._3))*/
  }
}