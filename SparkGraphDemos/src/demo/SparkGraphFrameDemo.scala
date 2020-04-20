package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.expr

object SparkGraphFrameDemo extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession
    .builder
    .appName("SparkGraphFrameDemo")
    .master("local[*]")
    .getOrCreate()

  // in Scala
  val bikeStations = spark.read.option("header", "true")
    .csv("C:/EBA5006/workspace/SparkGraphDemos/data/station_data.csv")
  val tripData = spark.read.option("header", "true")
    .csv("C:/EBA5006/workspace/SparkGraphDemos/data/trip_data.csv")

  //Build a Graph
  val stationVertices = bikeStations.withColumnRenamed("name", "id").distinct()
  val tripEdges = tripData
    .withColumnRenamed("Start Station", "src")
    .withColumnRenamed("End Station", "dst")
  import org.graphframes.GraphFrame
  val stationGraph = GraphFrame(stationVertices, tripEdges)
  stationGraph.cache()
  // in Scala
  println(s"Total Number of Stations: ${stationGraph.vertices.count()}")
  println(s"Total Number of Trips in Graph: ${stationGraph.edges.count()}")
  println(s"Total Number of Trips in Original Data: ${tripData.count()}")

  // Query a Graph
  println(s"Query a Graph:") 
  stationGraph.edges.groupBy("src", "dst").count().orderBy(desc("count")).show(10)
  stationGraph.edges
    .where("src = 'Townsend at 7th' OR dst = 'Townsend at 7th'")
    .groupBy("src", "dst").count()
    .orderBy(desc("count"))
    .show(10)

  // Sub Graph and Motif
    println(s"Motif:")
  val townAnd7thEdges = stationGraph.edges.where("src = 'Townsend at 7th' OR dst = 'Townsend at 7th'")
  val subgraph = GraphFrame(stationGraph.vertices, townAnd7thEdges)
  val motifs = stationGraph.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)")
  motifs.selectExpr(
    "*",
    "to_timestamp(ab.`Start Date`, 'MM/dd/yyyy HH:mm') as abStart",
    "to_timestamp(bc.`Start Date`, 'MM/dd/yyyy HH:mm') as bcStart",
    "to_timestamp(ca.`Start Date`, 'MM/dd/yyyy HH:mm') as caStart")
    .where("ca.`Bike #` = bc.`Bike #`").where("ab.`Bike #` = bc.`Bike #`")
    .where("a.id != b.id").where("b.id != c.id")
    .where("abStart < bcStart").where("bcStart < caStart")
    .orderBy(expr("cast(caStart as long) - cast(abStart as long)"))
    .selectExpr("a.id", "b.id", "c.id", "ab.`Start Date`", "ca.`End Date`")
    .limit(1).show(false)

  // Page Ranking
  println(s"Page Ranking:")
  val ranks = stationGraph.pageRank.resetProbability(0.15).maxIter(10).run()
  ranks.vertices.orderBy(desc("pagerank")).select("id", "pagerank").show(10)

  // Breadth First Search
  println(s"BFS:")
  stationGraph.bfs.fromExpr("id = 'Townsend at 7th'").toExpr("id = 'Spear at  Folsom'").maxPathLength(1).run().show(10)

}