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
  Haed : String){
  def print(): Unit = {
    println("Person: "
      + EinstID + ", " + Nafn + ", " + Fdagur+  ", "
      + Kyn + ", " + Netfang + ", " + Simi1+ ", "
      + Simi2 + ", " + Simi3 + ", " + Haed )

  }
}

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
      .load("DataCSV/einstaklingar-nytt.csv")
     df.createOrReplaceTempView("People")

    val people = df.as[Person].collect()

    val broadcastVar = session.sparkContext.broadcast(people)
    val stuff = people.flatMap( p => {
      var list: List[(Int, Int, Double)] = Nil
      var personArr = broadcastVar.value
      var index = personArr.indexOf( p )
      for (i <- (index + 1) to (broadcastVar.value.length-1)) {
        val b = personArr(i)
        val nameComp = Comparison(p,b)

        list = (p.EinstID, b.EinstID, nameComp) :: list
      }
      list
    }).filter(x =>  x._3 > 0.9)

    var uf = new WeightedQuickUnionPathCompressionUF(4593);
    for(i <- stuff){
      if(!uf.connected(i._1,i._2)){
        uf.union(i._1,i._2)
      }
    }
    val groups = people.groupBy(x => uf.find(x.EinstID)).filter(_._2.length >1)
    println("Size: " +groups.size)
    for(i <- groups){
      println("ParentID:" + i._1)
      for(j <- i._2){
        j.print()
      }
      println("-----------------------------")
    }
  }


  def Comparison(person1 : Person, person2: Person) : Double = {
    val nameWeight = 10.0
    val dayWeight = 10.0
    val phoneWeight = 10.0
    val emailWeight = 10.0
    val genderWeight = 10.0
    var Total = nameWeight + dayWeight + phoneWeight + emailWeight + genderWeight

    val nameComp = 1 - jaroWink.distance( person1.Nafn, person2.Nafn )
    var weightedPercentage = nameComp * nameWeight

    val fDagurComp = lev.distance( person1.Fdagur, person2.Fdagur )
    if (fDagurComp > 2) weightedPercentage += 0
    else if (fDagurComp == 2) weightedPercentage += 0.85 * dayWeight
    else if (fDagurComp == 1) weightedPercentage += 0.95 * dayWeight
    else weightedPercentage += dayWeight

    val emailComp = emailCompare( person1.Netfang, person2.Netfang )
    if (emailComp != Double.PositiveInfinity)
      weightedPercentage += emailWeight
    else
      Total -= emailWeight

    if (person1.Kyn == person2.Kyn) weightedPercentage += genderWeight;

    val p1 = List(person1.Simi1,person1.Simi2,person1.Simi3)
    val p2 = List(person2.Simi1,person2.Simi2,person2.Simi3)
    val phoneMatch = p1.flatMap( x => p2.map( phoneCompare( x, _ ) ) ).min

    if(phoneMatch != Double.PositiveInfinity) {
      if (phoneMatch == 0) weightedPercentage += phoneWeight
      else if (phoneMatch == 1) weightedPercentage += 0.95 * phoneWeight
      else if (phoneMatch == 2) weightedPercentage += 0.85 * phoneWeight
    }
    else
      Total -= phoneWeight

    return weightedPercentage / Total
  }
  def phoneCompare(phone1 : String, phone2 : String) : Double =  {
    if (phone1 == null || phone2 == null) return Double.PositiveInfinity;
    return lev.distance(phone1,phone2)
  }
  def emailCompare(e1 : String, e2: String) : Double =  {
    val f1 = firstPartOfemail(e1)
    val f2 = firstPartOfemail(e2)
    if (f1._2 && f2._2)
      return jaroWink.distance(f1._1,f2._1)
    else
      return Double.PositiveInfinity
  }

  def firstPartOfemail(email: String): (String, Boolean) = {
    if (email == null) return  ("",false)
    val i = email.indexOf("@")
    if (i < 0 ) return ("",false)
    return (email.substring(i),true);
  }
}


