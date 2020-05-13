package lsh

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object Main {
  private val conf = new SparkConf().setAppName("Session-group-15")
  private val sc = SparkContext.getOrCreate(conf)
  private val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  private val corpusFile = "/lsh-corpus-small.csv"
  private val rddCorpus = loadRDD(sc, corpusFile).cache()
  private val exact: Construction = new ExactNN(sqlContext, rddCorpus, 0.3)
  private val getQueryFileName = (x: Int) => s"lsh-query-$x.csv"
  private val REQUIRED_PRECISIONS_RECALLS = IndexedSeq(
    (0.83, 0.70),
    (0.7, 0.98),
    (0.9, 0.45)
  )

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

  def query(queryFile: String,
            composite: (SQLContext, RDD[(String, List[String])], Int, Int) => CompositeConstruction,
            r: Int,
            b: Int,
            requiredPrecision: Double,
            requiredRecall: Double): Unit = {

    val rddCorpus = loadRDD(sc, corpusFile)
    val rddQuery = loadRDD(sc, queryFile)

    val exact: Construction = new ExactNN(sqlContext, rddCorpus, 0.3)
    val lsh: Construction = composite(sqlContext, rddCorpus, r, b)

    val ground = exact.eval(rddQuery)
    val res = lsh.eval(rddQuery)

    val resultRecall = recall(ground, res)
    val resultPrecision = precision(ground, res)
    println(s"Recall: $resultRecall")
    println(s"Precision: $resultPrecision")

    assert(resultRecall > requiredRecall)
    assert(resultPrecision > requiredPrecision)
  }

  def main(args: Array[String]) {
    // Query 0
    val (precisionO, recall0) = REQUIRED_PRECISIONS_RECALLS(0)
    val (r0, b0) = (1, 1)
    val query0Construction = CompositeConstruction.andOrBase(sqlContext, rddCorpus, r0, b0)
    val query0ConstructionBroadcast = CompositeConstruction.andOrBroadcast(sqlContext, rddCorpus, r0, b0)
    query(getQueryFileName(0), query0Construction, precisionO, recall0)
    query(getQueryFileName(0), query0ConstructionBroadcast, precisionO, recall0)

  }

  def query(queryFile: String,
            construction: Construction,
            reqPrecision: Double,
            reqRecall: Double): Unit = {

    val queryRdd = loadRDD(sc, queryFile)

    val ground = exact.eval(queryRdd)
    val res = construction.eval(queryRdd)

    val queryPrecision = precision(ground, res)
    val queryRecall = recall(ground, res)

    assert(queryPrecision > reqPrecision)
    assert(queryRecall > reqRecall)
  }

  def loadRDD(sc: SparkContext, filename: String): RDD[(String, List[String])] = {
    val input = new File(getClass.getResource(filename).getFile).getPath
    sc
      .textFile(input)
      .map(x => x.split('|'))
      .map(x => (x(0), x.slice(1, x.length).toList))
  }
}
