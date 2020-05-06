package lsh


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.util.Random

class BaseConstruction(sqlContext: SQLContext, data: RDD[(String, List[String])]) extends Construction {
  /*
  * Initialize LSH data structures here
  * */
  private val seed = System.currentTimeMillis()
  Random.setSeed(seed)

  // Create the dictionary, getting the unique keywords and zip each word with an index
  private val dictionary: RDD[(Long, String)] =
    data
      .flatMap(_._2)
      .distinct()
      .zipWithIndex()
      .map(t => (t._2, t._1))

  // Assign a random index to each keyword index
  private val indices: RDD[(Long, Long)] =
    dictionary.sparkContext
      .parallelize(Random.shuffle((0L to dictionary.count()).toIndexedSeq))
      .zipWithIndex()
      .map(t => (t._2, t._1))

  // Assign the random index to each dictionary entry
  private val dictHash: RDD[(String, Long)] = dictionary.join(indices).map(_._2)

  // Compute the MinHash of each data point, grouping all the movies with the same MinHash
  private val dataProc: RDD[(Long, Set[String])] =
    computeMinHash(data)
      // Group by MinHash
      .groupBy(_._1)
      .map { case (h, films) => (h, films.map(_._2).toSet) }

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
    val rddProc: RDD[(Long, String)] = computeMinHash(rdd)

    val nn: RDD[(String, Set[String])] =
      rddProc
        .join(dataProc)
        .map { case (_, t) => t }

    nn
  }

  private def computeMinHash(rdd: RDD[(String, List[String])]): RDD[(Long, String)] = {
    rdd
      // Create an array entry for each (keyword, movie) couple
      .flatMap { case (t, keys) => keys.map((_, t)) }
      // Get the random index associated to each word
      .join(dictHash)
      .map { case (_, (t, hash)) => (t, hash) }
      // Group by movie, and get the smallest index in each movie (the MinHash)
      .groupBy(_._1)
      .map { case (t, hl) => (hl.map(_._2).min, t) }
  }
}
