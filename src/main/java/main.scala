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
    val mm = new mutable.HashMap[Int,Set[Int]] with mutable.MultiMap[Int, Int]
    val people = df.as[Person].collect()
    val broadcastVar = session.sparkContext.broadcast(people)
    var realIds : List[(Int)] = Nil

    val stuff = people.flatMap( p => {
      var list: List[(Int, Int, Double)] = Nil
      var personArr = broadcastVar.value
      var index = personArr.indexOf( p )
      for (i <- (index + 1) to (broadcastVar.value.length-1)) {
        val b = personArr(i)
        val nameComp = Comparison(p,b)
        if(nameComp > 0.9){
          if(!mm.contains(p.EinstID) && !mm.contains(b.EinstID)) {
            mm.addBinding(p.EinstID,b.EinstID)
          }else if(!mm.contains(p.EinstID) && mm.contains(b.EinstID)){
            mm.addBinding(b.EinstID,p.EinstID)
          }else if(mm.contains(p.EinstID) && !mm.contains(b.EinstID)){
            mm.addBinding(p.EinstID,b.EinstID)
          }
        }
        list = (p.EinstID, b.EinstID, nameComp) :: list
      }
      //list.foreach(x=> println( x._1 + " | " + x._2 + " | " + x._3))
      list
    }).filter(x =>  x._3 > 0.9)
    mm.foreach(println)
    //stuff.foreach(x=> println( x._1 + " | " + x._2 + " | " + x._3))
    //println(stuff.length)
  }


  def Comparison(person1 : Person, person2: Person) : Double = {
    val nameWeight = 0.6
    val dayWeight = 0.3
    val phoneWeight = 0.1
    val nameComp = 1-jaroWink.distance(person1.Nafn, person2.Nafn)
    var weightedPercentage = nameComp * nameWeight
    val fDagurComp = lev.distance(person1.Fdagur, person2.Fdagur)


    if(fDagurComp >2){
      weightedPercentage += 0
    }
    else if(fDagurComp==2){
      weightedPercentage += 0.7*dayWeight
    }else if(fDagurComp==1){
      weightedPercentage += 0.9*dayWeight
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
      weightedPercentage += phoneWeight
    }else {
      if(lowestphoneComp == 0){
        weightedPercentage += phoneWeight
      }else if(lowestphoneComp == 1){
        weightedPercentage += 0.9*phoneWeight
      }else if(lowestphoneComp == 2){
        weightedPercentage += 0.7*phoneWeight
      }

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


