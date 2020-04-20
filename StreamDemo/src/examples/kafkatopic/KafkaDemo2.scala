package examples.kafkatopic

import org.apache.spark.sql.SparkSession

object KafkaDemo2 extends App {

  val spark = SparkSession
    .builder
    .appName("KafkaStreamDemo2")
    .master("local[2]")
    .getOrCreate()

  import org.apache.spark.sql.kafka010._
  import java.io.File
  // Kafka
  val kafkaBootstrapServer = "127.0.0.1:9092"
  val topic = "demo"

  // File system
  val workDir = "D:/tmp"
  val referenceFile = "sensor-records.parquet"
  val targetFile = "structured_enrichedIoTStream.parquet"
  val targetPath = new File(workDir, targetFile).getAbsolutePath
  val unknownSensorsTargetFile = "unknownSensorsStream.parquet"
  val unknownSensorsTargetPath = new File(workDir, unknownSensorsTargetFile).getAbsolutePath
  val rawData = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaBootstrapServer)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()
  rawData.isStreaming
  rawData.printSchema()
  case class SensorData(sensorId: Int, timestamp: Long, value: Double)
  
  import spark.implicits._
  import java.sql.Timestamp
  import scala.util.Try
  val iotData = rawData.select($"value").as[String].flatMap{record =>  
      val fields = record.split(",") 
       Try {    
        SensorData(fields(0).toInt, fields(1).toLong, fields(2).toDouble)  
        }.toOption
        }
  val sensorRef = spark.read.parquet(s"$workDir/$referenceFile")
  sensorRef.cache()
  val sensorWithInfo = sensorRef.join(iotData, Seq("sensorId"), "inner")
  val knownSensors = sensorWithInfo  
                    .withColumn("dnvalue", $"value"*($"maxRange"-$"minRange")+$"minRange")  
                    .drop("value", "maxRange", "minRange")

}