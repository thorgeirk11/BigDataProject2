import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object BigDataProject2 {

  def main(args: Array[String]): Unit = {

    println("Hello World")
    println(args.length)

    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName("BigDataProject2")
      .setSparkHome(args(1))
      .setJars(SparkContext.jarOfObject(this).toSeq);

    val sc = new SparkContext(conf);
    println(sc.getConf.toDebugString);
    val textfiles = sc.wholeTextFiles("DataCSV");
    textfiles.foreach(println)
  }
}