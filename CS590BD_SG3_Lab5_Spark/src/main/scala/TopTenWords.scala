
package main.scala

// Spark
import org.apache.spark.{
  SparkContext,
  SparkConf
}
import SparkContext._

/*
 * @author Jeff Lanning SG3
 */
object TopTenWords {
  def main(args: Array[String]) {
    val dataFile = "hdfs://quickstart.cloudera:8020/user/cloudera/topten/input/"
    val conf = new SparkConf().setAppName("TopTenWords Application")
    val sc = new SparkContext(conf)
    
    // split each document into words
    val tokenized = sc.textFile(args(0)).flatMap(_.replaceAll("[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']", " ").split("\\s+"))
    
    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)
    
    // filter out words with less than threshold occurrences
    //val filtered = wordCounts.filter(_._2 >= threshold)
    val filtered = wordCounts.sortBy(_._2, false).take(10)
    
    // count characters
    val charCounts = wordCounts.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)
 
    val results = filtered.mkString(", ")
    System.out.println(results)
    
    sc.parallelize(List(results)).saveAsTextFile(args(1))
  }
  
  // Split a piece of text into individual words.
  private def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}