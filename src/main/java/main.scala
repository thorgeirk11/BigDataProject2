import info.debatty.java.stringsimilarity.interfaces.StringSimilarity
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import info.debatty.java.stringsimilarity.Levenshtein;
import info.debatty.java.stringsimilarity.NGram;

object BigDataProject2 {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master(args(0))
      .appName("BigDataProject2")
      .getOrCreate()

    val df = session.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("inferSchema", "true") // Automatically infer data types
      .option("header", "true") // Use first line of all files as header
      .load("DataCSV/blak-einstaklingar.csv")
    df.show()

    var l = new Levenshtein();
    var nGram = new NGram(3);
    println(l.distance("Quick fox jumped over the lazy brown dog","quick dog"));
    println(nGram.distance("Quick fox jumped over the lazy brown dog","quick dog"));

  }
}