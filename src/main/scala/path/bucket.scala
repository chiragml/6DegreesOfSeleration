package path

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkShortestPath {
  val infinity: Double = Double.PositiveInfinity
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkShortestPath")
    val sc = new SparkContext(conf)
    val source = 101  // Source node ID

    
    val filteredData: RDD[String] = sc.textFile(args(0)) 
      .map(_.trim)  // Trim whitespaces
      .filter(line => !line.startsWith("#") && !line.isEmpty())  // Remove comments and empty lines

   
    // Assume args(0) is the path to the input data file
    val rawEdges: RDD[(Int, Int)] = filteredData
      .map(line => {
        val parts = line.split("\t") // The edges are separated by tabs
        if(parts.length < 2 || parts(0).isEmpty || parts(1).isEmpty) {
          (-1,-1)
        } else {
          (parts(0).toInt, parts(1).toInt)
        }
      }).filter(_._1 != -1)

    

    val undirectedEdges: RDD[(Int, Int)] = rawEdges.flatMap {
      case (node1, node2) => Seq((node1, node2), (node2, node1))
    }
   
    val adjacencyList: RDD[(Int, List[Int])] = undirectedEdges
      .aggregateByKey(List.empty[Int])(
        (acc, node) => node :: acc,  // Add the node to the existing list
        (list1, list2) => list1 ::: list2  // Combine lists from different partitions
      ).map { case (node,list) =>
        // If the current node is the source node, add -1 to its list
        if (node == source)  (node,-1 :: list) else (node, list)
      }
  
    
    val partitionedGraph = adjacencyList.partitionBy(new org.apache.spark.HashPartitioner(100))
    partitionedGraph.persist()
    
    var distances = partitionedGraph.mapValues(adjList =>
      if (adjList.head == -1) 0 else infinity
    )
    
    for(_ <- 1 to 7) {
      distances = partitionedGraph.join(distances).flatMap {
        case (n, (adjList, currentDistanceOfN)) => extractVertices(n, adjList, currentDistanceOfN)
      }.reduceByKey((x,y) => math.min(x,y))
    }
    distances.saveAsTextFile(args(1))
    sc.stop()
  }

  def extractVertices(n: Int, adjList: List[Int], currentDistance: Double): Seq[(Int, Double)] = {
    // adjList.map(dest => (dest, currentDistance + 1)) :+ (n, currentDistance)
    (n, currentDistance) :: adjList.map(dest => (dest, currentDistance + 1))
  }
}
