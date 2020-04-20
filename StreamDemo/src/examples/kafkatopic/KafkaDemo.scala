package examples.kafkatopic

import org.apache.spark.sql.SparkSession

object KafkaDemo extends App {

  val kafkaBootstrapServer = "localhost:9092"
  val targetTopic = "iot"

  // File system
  val workDir = "C:/tmp/streaming-with-spark"

  // Generator
  val sensorCount = 100000

  val spark = SparkSession
    .builder
    .appName("KafkaStreamDemo")
    .master("local[2]")
    .getOrCreate()

  case class SensorData(sensorId: Int, timestamp: Long, value: Double)
  object SensorData {
    import scala.util.Random
    def randomGen(maxId: Int) = {
      SensorData(Random.nextInt(maxId), System.currentTimeMillis, Random.nextDouble())
    }
  }

  case class Rate(timestamp: Long, value: Long)

  val baseStream = spark.readStream.format("rate").option("recordsPerSecond", 100).load()
  import spark.implicits._
  import java.sql.Timestamp
  val sensorValues = baseStream.as[Rate].map(_ => SensorData.randomGen(sensorCount))
  import org.apache.spark.sql.kafka010._

  import org.apache.spark.sql.ForeachWriter
  val writer = new ForeachWriter[SensorData] {
    override def open(partitionId: Long, version: Long) = true
    override def process(value: SensorData) = println(value)
    override def close(errorOrNull: Throwable) = {}
  }

  val query = sensorValues.writeStream.format("kafka")
    .queryName("kafkaWriter")
    .outputMode("append")
    .option("kafka.bootstrap.servers", kafkaBootstrapServer) // comma-separated list of host:port
    .option("topic", targetTopic)
    .option("checkpointLocation", workDir + "/generator-checkpoint")
    .option("failOnDataLoss", "false") // use this option when testing
    .foreach(writer)
    .start()
}