package demo

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ Row, SparkSession }

object SparkGraphXDemo extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession
    .builder
    .appName("GraphXDemo")
    .master("local[*]")
    .getOrCreate()
  //Define a new type VertexId
  type VertexId = Long

  //Create RDD of vertices
  val vertices: RDD[(VertexId, String)] = spark.sparkContext.parallelize(List((1L, "Suria"), (2L, "Mohammad"), (3L, "Ravi"), (4L, "Alice"), (5L, "Raj")))

  //Create RDD of edges
  val edges: RDD[Edge[String]] = spark.sparkContext.parallelize(List(Edge(1L, 2L, "friend"), Edge(1L, 3L, "brother"), Edge(3L, 5L, "brother"), Edge(1L, 4L, "friend"), Edge(4L, 5L, "wife")))

  //Finally define a graph
  val graph = Graph(vertices, edges)

  //Prefixing 'Hi' with each name
  val newGraph = graph.mapVertices((VertexId, name) => "Hi " + name)
  newGraph.edges.foreach(println)

  // Adding Relation Label
  val newGraph2 = newGraph.mapEdges(e => "relation : " + e.attr)
  newGraph2.edges.foreach(println)

  //Filtering the edges which have relation other than 'friend'
  val edgeFilterGraph = graph.subgraph(epred = (edge) => edge.attr.equals("friend"))
  edgeFilterGraph.edges.collect().foreach(println)

  //Filtering out the vertices which have name as 'Ravi'
  val vertexFilterGraph = graph.subgraph(vpred = (id, name) => !name.equals("Ravi"))
  vertexFilterGraph.vertices.collect().foreach(println)

}