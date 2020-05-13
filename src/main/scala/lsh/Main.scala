package lsh

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Utils._


object Main {
  private val cluster = true
  private val sc = if (cluster) {
    runOnCluster().sparkContext
  } else {
    runOnLocal().sparkContext
  }
  private val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  private val corpusFile = getFilePath(cluster, "lsh-corpus-small.csv")
  private val corpusRdd = loadRDD(sc, corpusFile).cache()
  private val exact: Construction = new ExactNN(sqlContext, corpusRdd, 0.3)
  private val getQueryFileName = (x: Int) => getFilePath(cluster, s"lsh-query-$x.csv")

  def main(args: Array[String]) {
    // Run the queries for which composite constructions are required
    val times = 1000
    val res0 = (0, query0(times))
    val res1 = (1, query1(times))
    val res2 = (2, query2(times))

    // If on the cluster, save results to file
    if (cluster) {
      val compositeQueriesResults = IndexedSeq(res0, res1, res2)
      sc.parallelize(compositeQueriesResults)
        .coalesce(1, shuffle = true)
        .saveAsTextFile(s"/user/group-15/lsh/composite_queries_results_$times.txt")
    }

    if (cluster) {
      // Run queries 3 to 7 with base and broadcast to assess their speed
      val baseConstructionBuilder = () => new BaseConstruction(sqlContext, corpusRdd)
      runSimpleQuery(times, baseConstructionBuilder, "base")

      val broadcastConstructionBuilder = () => new BaseConstructionBroadcast(sqlContext, corpusRdd)
      runSimpleQuery(times, broadcastConstructionBuilder, "broadcast")

    }
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

  def query(constructionBuilder: () => Construction, ground: RDD[(String, Set[String])],
            queryRdd: RDD[(String, List[String])], reqPrecision: Double, reqRecall: Double, times: Int = 1): Double = {

    implicit def bool2int(b: Boolean): Int = if (b) 1 else 0

    def singleQuery(): Int = {
      val construction = constructionBuilder()
      val res = construction.eval(queryRdd)
      val queryPrecision = precision(ground, res)
      val queryRecall = recall(ground, res)

      if (!cluster & (queryPrecision < reqPrecision | queryRecall < reqRecall)) {
        println(s"Precision: $queryPrecision, required: $reqPrecision")
        println(s"Recall: $queryRecall, required: $reqRecall")
      }

      queryPrecision > reqPrecision & queryRecall > reqRecall
    }

    val correctResults = 0.until(times).map(_ => singleQuery()).sum
    val correctResultsPercentage = correctResults.doubleValue() / times.doubleValue()

    if (!cluster) {
      println(s"Percentage of correct results: $correctResultsPercentage")
    }

    correctResultsPercentage
  }

  def doWarmUp(queryRdd: RDD[(String, List[String])], constructionBuilder: () => Construction): Unit = {
    0.until(3).map(_ => measureOneComputation(queryRdd, constructionBuilder))
  }

  def loadRDD(sc: SparkContext, filename: String): RDD[(String, List[String])] = {
    val input = if (cluster) filename else new File(getClass.getResource(filename).getFile).getPath
    sc
      .textFile(input)
      .map(x => x.split('|'))
      .map(x => (x(0), x.slice(1, x.length).toList))
  }

  private def computeMetric(ground_truth: RDD[(String, Set[String])],
                            lsh_truth: RDD[(String, Set[String])],
                            singleMetric: (Set[String], Set[String]) => Double): Double = {

    val results = ground_truth
      .join(lsh_truth)
      .map(_._2)
      .map { case (exact, estimated) => singleMetric(exact, estimated) }

    results.mean()
  }

  private def query0(times: Int): Double = {
    // Query 0 -> Tradeoff between FPs and FNs, but rec > pr so we should keep FNs low, thus we use AND+OR
    val RECALL = 0.83
    val PRECISION = 0.70
    val (r, b) = (2, 2)
    val queryRdd = loadRDD(sc, getQueryFileName(0)).cache()
    val ground = exact.eval(queryRdd).cache()
    val constructionBuilder = () => CompositeConstruction.andOrBase(sqlContext, corpusRdd, r, b)
    val correctPercentage = query(constructionBuilder, ground, queryRdd, PRECISION, RECALL, times)

    queryRdd.unpersist()
    ground.unpersist()

    correctPercentage
  }

  private def query1(times: Int): Double = {
    // Query 1 -> Recall is kinda low and precision is high, we should avoid FPs, we can admit more FNs, we use AND
    val RECALL = 0.7
    val PRECISION = 0.98
    val r = 3
    val queryRdd = loadRDD(sc, getQueryFileName(1)).cache()
    val ground = exact.eval(queryRdd).cache()
    val constructionBuilder = () => ANDConstruction.getConstructionBroadcast(sqlContext, corpusRdd, r)
    val correctPercentage = query(constructionBuilder, ground, queryRdd, PRECISION, RECALL, times)

    queryRdd.unpersist()
    ground.unpersist()

    correctPercentage
  }

  private def query2(times: Int): Double = {
    // Query 2 -> Recall is high and precision is low, we should avoid FNs, we can admit more FPs, we use OR with b = 3
    val RECALL = 0.9
    val PRECISION = 0.45
    val b = 3
    val queryRdd = loadRDD(sc, getQueryFileName(2)).cache()
    val ground = exact.eval(queryRdd).cache()
    val constructionBuilder = () => ORConstruction.getConstructionBroadcast(b, sqlContext, corpusRdd)
    val correctPercentage = query(constructionBuilder, ground, queryRdd, PRECISION, RECALL, times)

    queryRdd.unpersist()
    ground.unpersist()

    correctPercentage
  }

  private def runSimpleQuery(times: Int, baseConstructionBuilder: () => Construction, constructorType: String): Unit = {
    val baseQueriesResults = 3.until(8)
      .map(measureStatistics(_, times, baseConstructionBuilder))

    sc.parallelize(baseQueriesResults)
      .coalesce(1, shuffle = true)
      .saveAsTextFile(s"/user/group-15/lsh/${constructorType}_queries_results_$times.txt")
  }

  private def measureStatistics(queryN: Int, n: Int, constructionBuilder: () => Construction): (IndexedSeq[Double], Double, Double, Long) = {
    val queryRdd = loadRDD(sc, getQueryFileName(queryN)).cache()

    doWarmUp(queryRdd, constructionBuilder)

    val timeList: IndexedSeq[Double] = 0.until(n).map(_ => measureOneComputation(queryRdd, constructionBuilder))

    val querySize = queryRdd.count()
    queryRdd.unpersist()

    (timeList, mean(timeList), stdDev(timeList), querySize)
  }

  private def measureOneComputation(queryRdd: RDD[(String, List[String])], constructionBuilder: () => Construction): Double = {
    val baseConstruction = constructionBuilder()
    val baseRes = baseConstruction.eval(queryRdd)
    val baseStart = System.nanoTime()
    baseRes.count()
    (baseStart - System.nanoTime()) / 1e9
  }
}
