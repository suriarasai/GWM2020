name := "SparkGraphDemos"

version := "1.0"

scalaVersion := "2.11.11"

val sparkVersion = "2.4.5"


resolvers ++= Seq( 
"apache-snapshots" at "http://repository.apache.org/snapshots/", 
"SparkPackages" at "https://dl.bintray.com/spark-packages/maven"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "com.databricks" %% "spark-csv" % "1.3.0", 
  "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"
)