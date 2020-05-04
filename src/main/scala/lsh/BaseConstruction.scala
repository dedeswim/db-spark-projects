package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.Map
import scala.util.Random

class BaseConstruction(sqlContext: SQLContext, data: RDD[(String, List[String])]) extends Construction {
  /*
  * Initialize LSH data structures here
  * */

//  private val dictionary: RDD[String] = data.flatMap(_._2).distinct()
//  private val indices: RDD[Long] = sqlContext.sparkContext.parallelize(Random.shuffle((0 to dictionary.count()).toIndexedSeq))
//  private val mapDict: RDD[(String, Long)] = dictionary.zip(indices)
//  private val minHashMap: RDD[(Int, Set[String])] =
//    data
//    .map(t => (t._1, t._2)
//    .groupBy(_._2)
//    .map(t => (t._1, t._2.map(_._1).toSet))
//    .collectAsMap()

//  def computeMinHash(keywords: List[String]): Int = {
//    keywords.map(mapDict).min
//  }

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
//    rdd
//      .map(t => (t._1, t._2.map(mapDict).min))
//      .map(t => (t._1, minHashMap(t._2)))
    null
  }
}
