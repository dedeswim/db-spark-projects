package lsh

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Utils.{runOnCluster, runOnLocal}


object Main {
  private val cluster = false
  private val sc = if (cluster) {
    runOnCluster().sparkContext
  } else {
    runOnLocal().sparkContext
  }
  private val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  private val corpusFile = "/lsh-corpus-small.csv"
  private val rddCorpus = loadRDD(sc, corpusFile).cache()
  private val exact: Construction = new ExactNN(sqlContext, rddCorpus, 0.3)
  private val getQueryFileName = (x: Int) => s"/lsh-query-$x.csv"

  private def computeMetric(ground_truth: RDD[(String, Set[String])], lsh_truth: RDD[(String, Set[String])], singleMetric: (Set[String], Set[String]) => Double): Double = {
    val results = ground_truth
      .join(lsh_truth)
      .map(_._2)
      .map { case (exact, estimated) => singleMetric(exact, estimated) }

    results.mean()
  }

  def recall(ground_truth: RDD[(String, Set[String])], lsh_truth: RDD[(String, Set[String])]): Double = {
    /*
    * Compute the recall for each near-neighbor LSH query against the accurate result
    * Then, compute the average across all queries
    * ground_truth: results of queries in (movie_name, [nn_movie_names]) format produced by ExactNN
    * lsh_truth: results of queries in (movie_name, [nn_movie_names]) format produced by an LSH Construction
    * returns average recall
    * */

    val result = computeMetric(
      ground_truth, lsh_truth, (exact, estimated) => exact.intersect(estimated).size.toDouble / exact.size.toDouble
    )

    result
  }

  def precision(ground_truth: RDD[(String, Set[String])], lsh_truth: RDD[(String, Set[String])]): Double = {
    /*
    * Compute the precision for each near-neighbor LSH query against the accurate result
    * Then, compute the average across all queries
    * ground_truth: results of queries in (movie_name, [nn_movie_names]) format produced by ExactNN
    * lsh_truth: results of queries in (movie_name, [nn_movie_names]) format produced by an LSH Construction
    * returns average precision
    * */

    computeMetric(
      ground_truth, lsh_truth, (exact, estimated) => exact.intersect(estimated).size.toDouble / estimated.size.toDouble
    )
  }

  def main(args: Array[String]) {
    val n = 10
    query0(n)
    // query1(n)
    // query2(n)
  }

  private def query0(n: Int): Unit = {
    // Query 0 -> Tradeoff between FPs and FNs, but rec > pr so we should keep FNs low, thus we use AND+OR
    val RECALL = 0.83
    val PRECISION = 0.70
    val (r, b) = (2, 2)
    val queryRdd = loadRDD(sc, getQueryFileName(0)).cache()
    val ground = exact.eval(queryRdd).cache()
    val constructionBuilder = () => CompositeConstruction.andOrBase(sqlContext, rddCorpus, r, b)
    val correctPercentage = query(constructionBuilder, ground, queryRdd, PRECISION, RECALL, n)
    println(s"Correct percentage query 0: $correctPercentage")
  }

  private def query1(n: Int): Unit = {
    // Query 1 -> Recall is kinda low and precision is high, we should avoid FPs, we can admit more FNs, we use AND
    val RECALL = 0.7
    val PRECISION = 0.98
    val r = 3
    val queryRdd = loadRDD(sc, getQueryFileName(1)).cache()
    val ground = exact.eval(queryRdd).cache()
    val constructionBuilder = () => ANDConstruction.getConstructionBroadcast(sqlContext, rddCorpus, r)
    val correctPercentage = query(constructionBuilder, ground, queryRdd, PRECISION, RECALL, n)
    println(s"Correct percentage query 1: $correctPercentage")
  }

  def query(constructionBuilder: () => Construction, ground: RDD[(String, Set[String])],
            queryRdd: RDD[(String, List[String])], reqPrecision: Double, reqRecall: Double, times: Int = 1): Double = {

    var correctResults = 0

    for (_ <- 0 until times) {
      val construction = constructionBuilder()
      val res = construction.eval(queryRdd)
      val queryPrecision = precision(ground, res)
      val queryRecall = recall(ground, res)

      if (queryPrecision > reqPrecision & queryRecall > reqRecall) {
        correctResults += 1
      }
    }

    correctResults.doubleValue() / times.doubleValue()
  }

  private def query2(n: Int): Unit = {
    // Query 2 -> Recall is high and precision is low, we should avoid FNs, we can admit more FPs, we use OR with b = 3
    val RECALL = 0.9
    val PRECISION = 0.45
    val b = 3
    val queryRdd = loadRDD(sc, getQueryFileName(2)).cache()
    val ground = exact.eval(queryRdd).cache()
    val constructionBuilder = () => ORConstruction.getConstructionBroadcast(b, sqlContext, rddCorpus)
    val correctPercentage = query(constructionBuilder, ground, queryRdd, PRECISION, RECALL, n)
    println(s"Correct percentage query 2: $correctPercentage")
  }

  def loadRDD(sc: SparkContext, filename: String): RDD[(String, List[String])] = {
    val input = new File(getClass.getResource(filename).getFile).getPath
    sc
      .textFile(input)
      .map(x => x.split('|'))
      .map(x => (x(0), x.slice(1, x.length).toList))
  }
}
