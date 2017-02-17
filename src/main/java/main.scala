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

import collection.mutable.{HashMap, MultiMap, Set}
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable;

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
  val jaroWink = new JaroWinkler()
  val lev = new Levenshtein()

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
      .load("DataCSV/einstaklingar-cleaned.csv")
     df.createOrReplaceTempView("People")
    //df.show()
   //val jaro = new JaroWinkler()
   //val testSQL = session.sql("Select * from People")
   //val l = List()
   //val x = testSQL.collect()
   //// a <- 1 to 3; b <- 1 to 3
   //for(i <- 1 to x.length-1) { 
   //  for (j <- i+1 to x.length-1) {
   //    val jar =jaro.distance(x(i)(1).toString(),x(j)(1).toString())
   //    if(jar < 0.1) {
   //      println(x(i)(1).toString()+ " | " +  x(j)(1).toString()+" | " +jar)
   //    }
   //  }
   //}
    val people = df.as[Person].collect()

    val broadcastVar = session.sparkContext.broadcast(people)
    //realids.foreach(println)
    val stuff = people.flatMap( p => {
      var list: List[(Int, Int, Double)] = Nil
      var personArr = broadcastVar.value
      var index = personArr.indexOf( p )
      for (i <- (index + 1) to (broadcastVar.value.length-1)) {
        val b = personArr(i)
        val nameComp = Comparison(p,b)

        list = (p.EinstID, b.EinstID, nameComp) :: list
      }
      //list.foreach(x=> println( x._1 + " | " + x._2 + " | " + x._3))
      list
    }).filter(x =>  x._3 > 0.9)
    var uf = new WeightedQuickUnionPathCompressionUF(4593);
    for(i <- stuff){
      if(!uf.connected(i._1,i._2)){
        uf.union(i._1,i._2)
      }
    }
    val groups = people.groupBy(x => uf.find(x.EinstID))
    println("Size: " +groups.size)
    for(i <- groups){
      println("ParentID:" + i._1)
      for(j <- i._2){
        println("Person: " + j.EinstID + ", " + j.Nafn + ", " + j.Fdagur)
      }
      println("-----------------------------")
    }

    //stuff.foreach(x=> println(people.find(y => y.EinstID == x._1) +" | " + people.find(z => z.EinstID ==uf.find(x._1))))
    //stuff.foreach(x=> println( x._1 + " | " + x._2 + " | " + x._3))

    //println(stuff.length)
  }


  def Comparison(person1 : Person, person2: Person) : Double = {
    val nameWeight = 60
    val dayWeight = 30
    val phoneWeight = 10
    val nameComp = 1-jaroWink.distance(person1.Nafn, person2.Nafn)
    var weightedPercentage = nameComp * nameWeight
    val fDagurComp = lev.distance(person1.Fdagur, person2.Fdagur)


    if(fDagurComp >2){
      weightedPercentage += 0
    }
    else if(fDagurComp==2){
      weightedPercentage += 0.85*dayWeight
    }else if(fDagurComp==1){
      weightedPercentage += 0.95*dayWeight
    }else{
      weightedPercentage += dayWeight
    }
    var lowestphoneComp = 8.0
    var currentphoneComp = 0.0
    if(person1.Simi1 != null){
      if(person2.Simi1 != null){
        lowestphoneComp = phoneCompare(person1.Simi1,person2.Simi1,lowestphoneComp)
      }
      if(person2.Simi2 != null){
        lowestphoneComp = phoneCompare(person1.Simi1,person2.Simi2,lowestphoneComp)
      }
      if(person2.Simi3 != null){
        lowestphoneComp = phoneCompare(person1.Simi1,person2.Simi3,lowestphoneComp)
      }
    }
    if(person1.Simi2 != null){
      if(person2.Simi1 != null){
        lowestphoneComp = phoneCompare(person1.Simi2,person2.Simi1,lowestphoneComp)
      }
      if(person2.Simi2 != null){
        lowestphoneComp = phoneCompare(person1.Simi2,person2.Simi2,lowestphoneComp)
      }
      if(person2.Simi3 != null){
        lowestphoneComp = phoneCompare(person1.Simi2,person2.Simi3,lowestphoneComp)
      }
    }
    if(person1.Simi3 != null){
      if(person2.Simi1 != null){
        lowestphoneComp = phoneCompare(person1.Simi3,person2.Simi1,lowestphoneComp)
      }
      if(person2.Simi2 != null){
        lowestphoneComp = phoneCompare(person1.Simi3,person2.Simi2,lowestphoneComp)
      }
      if(person2.Simi3 != null){
        lowestphoneComp = phoneCompare(person1.Simi3,person2.Simi3,lowestphoneComp)
      }
    }
    if((person1.Simi1 == null && person1.Simi2 == null && person1.Simi3 == null) || (person2.Simi1 == null && person2.Simi2 == null && person2.Simi3 == null)){
      weightedPercentage /= 90
    }else {
      if (lowestphoneComp == 0) {
        weightedPercentage += phoneWeight
      } else if (lowestphoneComp == 1) {
        weightedPercentage += 0.95 * phoneWeight
      } else if (lowestphoneComp == 2) {
        weightedPercentage += 0.85 * phoneWeight
      }
      weightedPercentage /= 100
    }
    return weightedPercentage
  }
  def phoneCompare(phone1 : String, phone2 : String, curr : Double) : Double =  {
    var compare = lev.distance(phone1,phone2)
    if(compare < curr){
      return compare
    }
    return curr
  }
}


