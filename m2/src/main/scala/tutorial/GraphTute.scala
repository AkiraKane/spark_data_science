package tutorial

import org.apache.spark.graphx.{VertexRDD, VertexId, Edge, Graph}
import org.apache.spark.rdd.RDD
import utils.SetUpSpark
import org.apache.spark.graphx.util.GraphGenerators
import org.graphframes.GraphFrame
/**
  * Created by edwardcannon on 11/05/2016.
  */
object GraphTute {
  /**
    * Creates test graph
    * @param sc SparkContext
    */
  def createTestGraph(sc : org.apache.spark.SparkContext): Unit ={
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
        (4L, ("peter", "student"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
        Edge(4L, 0L, "student"),   Edge(5L, 0L, "colleague")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin)
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect.foreach(println(_))
    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
  }

  /**
    * Create test graph with vertex values following log normal
    * @param sc SparkContext
    */
  def createLogNormalGraph(sc : org.apache.spark.SparkContext): Graph[Double,Int] ={
    val graph: Graph[Double, Int] =
      GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices( (id, _) => id.toDouble )
    // Compute the number of older followers and their total age
    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function
        if (triplet.srcAttr > triplet.dstAttr) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst(1, triplet.srcAttr)
        }
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )
    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
      olderFollowers.mapValues( (id, value) => value match { case (count, totalAge) => totalAge / count } )
    graph
  }

  /***
    * Identifies vertex in graph with highest degree
    * @param a A graph vertex
    * @param b A graph vertex
    * @return Graph vertex with largest degree
    */
  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }

  def main(args: Array[String]): Unit = {
    println("Using GraphFrames")
    val sc = SetUpSpark.configure()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val graph = GraphTute.createLogNormalGraph(sc)
    val maxInDegree: (VertexId, Int)  = graph.inDegrees.reduce(GraphTute.max)
    val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(GraphTute.max)
    val maxDegrees: (VertexId, Int)   = graph.degrees.reduce(GraphTute.max)
    println(maxInDegree)
    println(maxOutDegree)
    println(maxDegrees)
   // Create a Vertex DataFrame with unique ID column "id"
   val v = sqlContext.createDataFrame(List(
     ("a", "Alice", 34),
     ("b", "Bob", 36),
     ("c", "Charlie", 30)
   )).toDF("id", "name", "age")
    // Create an Edge DataFrame with "src" and "dst" columns
    val e = sqlContext.createDataFrame(List(
      ("a", "b", "friend"),
      ("b", "c", "follow"),
      ("c", "b", "follow")
    )).toDF("src", "dst", "relationship")
    // Create a GraphFrame
    val g = GraphFrame(v, e)

    // Query: Get in-degree of each vertex.
    g.inDegrees.show()

    // Query: Count the number of "follow" connections in the graph.
    g.edges.filter("relationship = 'follow'").count()

    // Run PageRank algorithm, and show results.
    val results = g.pageRank.resetProbability(0.01).maxIter(5).run()
    results.vertices.select("id", "pagerank").show()
    val resultLPA = g.labelPropagation.maxIter(5).run()
    resultLPA.show()
  }
}
