package examples.files

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType

object FileStreamDemo extends App {

  val logsDirectory = "C:/EBA5006/data"

  val spark = SparkSession
    .builder
    .appName("FileStreamDemo")
    .master("local[2]")
    .getOrCreate()

  val rawLogs = spark.read.json(logsDirectory)
  import spark.implicits._
  import java.sql.Timestamp
  case class WebLog(
    host:       String,
    timestamp:  Timestamp,
    request:    String,
    http_reply: Int,
    bytes:      Long)
  import spark.implicits._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types.IntegerType
  // we need to narrow the `Interger` type because
  // the JSON representation is interpreted as `BigInteger`
  val preparedLogs = rawLogs.withColumn("http_reply", $"http_reply".cast(IntegerType))
  val weblogs = preparedLogs.as[WebLog]
  val recordCount = weblogs.count
  val topDailyURLs = weblogs.withColumn("dayOfMonth", dayofmonth($"timestamp"))
    .select($"request", $"dayOfMonth")
    .groupBy($"dayOfMonth", $"request")
    .agg(count($"request").alias("count"))
    .orderBy(desc("count"))

  topDailyURLs.show()

  val urlExtractor = """^GET (.+) HTTP/\d.\d""".r
  val allowedExtensions = Set(".html", ".htm", "")
  val contentPageLogs = weblogs.filter { log =>
    log.request match {
      case urlExtractor(url) =>
        val ext = url.takeRight(5).dropWhile(c => c != '.')
        allowedExtensions.contains(ext)
      case _ => false
    }
  }

  val topContentPages = contentPageLogs.withColumn("dayOfMonth", dayofmonth($"timestamp")).select($"request", $"dayOfMonth").groupBy($"dayOfMonth", $"request").agg(count($"request").alias("count")).orderBy(desc("count"))

  topContentPages.show()

}