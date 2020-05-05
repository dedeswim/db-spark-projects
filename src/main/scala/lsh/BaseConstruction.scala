package lsh


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.Map
import scala.util.Random

class BaseConstruction(sqlContext: SQLContext, data: RDD[(String, List[String])]) extends Construction {
  /*
  * Initialize LSH data structures here
  * */
  private val seed = System.currentTimeMillis()
  Random.setSeed(seed)
  private val dictionary: RDD[(String, Long)] = data.flatMap(_._2).distinct().zipWithIndex()
  private val dataProc: RDD[(Long, String)] =
    data
      .flatMap{ case (t, keys) => keys.map( (_, t))}
      .join(dictionary)
      .map{ case (_, (t, hash)) => (t, hash)}
      .groupBy(_._1)
      .map{case (t, hl) => (t, hl.map(_._2).toList.sorted)}
      .map{case (t, hl) => (t, Random.shuffle(hl))}
      .map{ case (t, hl) => (hl.head, t)}

    private val test =
      data
        .flatMap{ case (t, keys) => keys.map( (_, t))}
        .join(dictionary)
        .map{ case (_, (t, hash)) => (t, hash)}
        .groupBy(_._1)
        .map{case (t, hl) => (t, hl.map(_._2).toList.sorted)}
        .map{case (t, hl) => (t, Random.shuffle(hl))}


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
    val rddProc: RDD[(Long, String)] =
      rdd
        .flatMap{ case (t, keys) => keys.map( (_, t))}
        .join(dictionary)
        .map{ case (_, (t, hash)) => (t, hash)}
        .groupBy(_._1)
        .map{case (t, hl) => (t, hl.map(_._2).toList.sorted)}
        .map{case (t, hl) => (t, Random.shuffle(hl))}
        .map{ case (t, hl) => (hl.head, t)}

    val testRDD =
      rdd
        .flatMap{ case (t, keys) => keys.map( (_, t))}
        .join(dictionary)
        .map{ case (_, (t, hash)) => (t, hash)}
        .groupBy(_._1)
        .map{case (t, hl) => (t, hl.map(_._2).toList.sorted)}
        .map{case (t, hl) => (t, Random.shuffle(hl))}

    val nn: RDD[(String, Set[String])] =
      rddProc
      .join(dataProc)
      .map{ case (_, t) => t}
      .groupBy(_._1)
      .map{case (q, nn) => (q, nn.map(_._2).toSet)}

    nn
  }
}
