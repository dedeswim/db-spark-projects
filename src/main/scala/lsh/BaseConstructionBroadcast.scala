package lsh

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.Map
import scala.util.Random

class BaseConstructionBroadcast(sqlContext: SQLContext, data: RDD[(String, List[String])]) extends Construction with Serializable {
  /*
  * Initialize LSH data structures here
  * You need to broadcast the data structures to all executors and use them locally
  * */

  private val seed = System.currentTimeMillis()
  Random.setSeed(seed)

  private val dictionary: Set[String] = data.flatMap(_._2).collect().toSet
  private val mapDict: Broadcast[Map[String, Int]] =
    sqlContext.sparkContext
      .broadcast(Random.shuffle(dictionary.toIndexedSeq).zipWithIndex.toMap)

  private val minHashMap: Broadcast[Map[Int, Set[String]]] = sqlContext.sparkContext.broadcast(
    data
      .map(t => (t._1, t._2.map(mapDict.value).min))
      .groupBy(_._2)
      .map(t => (t._1, t._2.map(_._1).toSet))
      .collectAsMap()
  )

  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    /*
    * This method performs a near-neighbor computation for the data points in rdd against the data points in data.
    * You need to perform the queries by using LSH with min-hash.
    * The perturbations needs to be consistent - decided once and randomly for each BaseConstructor object
    * sqlContext: current SQLContext
    * data: data points in (movie_name, [keyword_list]) format to compare against
    * rdd: data points in (movie_name, [keyword_list]) format that represent the queries
    * return near-neighbors in (movie_name, [nn_movie_names]) as an RDD[(String, Set[String])]
    * */
    rdd
      .map(t => (t._1, t._2.map(x => mapDict.value.get(x)).min))
      .filter(_._2.isDefined)
      .map(t => (t._1, minHashMap.value.get(t._2.get)))
      .filter(_._2.isDefined)
      .map(t => (t._1, t._2.get))
  }
}
