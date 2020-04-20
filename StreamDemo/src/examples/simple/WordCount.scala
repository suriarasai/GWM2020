package examples.simple

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger


object WordCount extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
   val spark = SparkSession
    .builder
    .appName("WordCount")
    .master("local[*]")
    .getOrCreate()
import spark.implicits._
  // Create DataFrame representing the
  // stream of input lines from
  // connection to localhost:9999
  val lines = spark.readStream
    .format("socket")
    .option("host", "local")
    .option("port", 9998)
    .load()
  // Split the lines into words
  val words = lines.as[String].flatMap(_.split(" "))
  // Generate running word count
  val wordCounts = words
    .groupBy("value").count()
  // Start running the query that prints
  //  the running counts to the console
  val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()

}