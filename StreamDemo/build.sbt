name := "StreamDemo"

version := "1.0"

scalaVersion := "2.11.11"

val sparkVersion = "2.4.5"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.2",
  "com.itextpdf" % "itextpdf" % "5.5.6",
  "org.jfree" % "jfreesvg" % "3.0",
  "com.databricks" % "spark-csv_2.11" % "1.4.0",
  "com.github.wookietreiber" % "scala-chart_2.11" % "0.5.0",
  "org.apache.kafka" % "kafka_2.10" % "0.8.1"
      exclude("javax.jms", "jms")
      exclude("com.sun.jdmk", "jmxtools")
      exclude("com.sun.jmx", "jmxri"),
  "org.twitter4j" % "twitter4j-core" % "4.0.2",
  "org.twitter4j" % "twitter4j-stream" % "4.0.2",
  "org.codehaus.jackson" % "jackson-core-asl" % "1.6.1",
  "com.google.code.gson" % "gson" % "2.6.2",
  "commons-cli" % "commons-cli" % "1.3.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.0.2" % "provided"
)