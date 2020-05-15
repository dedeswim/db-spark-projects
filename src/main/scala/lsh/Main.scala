package lsh

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import utils.Utils._


object Main extends Serializable {

  def main(args: Array[String]) {

    val cluster = true
    val evaluate02 = false
    val sc = if (cluster) {
      runOnCluster().sparkContext
    } else {
      runOnLocal().sparkContext
    }
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val corpusFile = getFilePath(cluster, "lsh-corpus-small.csv")
    val corpusRdd = loadRDD(sc, corpusFile, cluster).cache()
    val totCountCorpus = corpusRdd.count()
    //private val mapCorpus = corpusRdd.collectAsMap()
    val exact: Construction = new ExactNN(sqlContext, corpusRdd, 0.3)
    val getQueryFileName = (x: Int) => getFilePath(cluster, s"lsh-query-$x.csv")

    // Run the queries for which composite constructions are required
    if (evaluate02) {
      val timesEval02 = 1000
      val res0 = (0, query0(timesEval02, sc, getQueryFileName, exact, sqlContext, corpusRdd, cluster))
      val res1 = (1, query1(timesEval02, sc, getQueryFileName, exact, sqlContext, corpusRdd, cluster))
      val res2 = (2, query2(timesEval02, sc, getQueryFileName, exact, sqlContext, corpusRdd, cluster))

      // If on the cluster, save results to file
      if (cluster) {
        val compositeQueriesResults = IndexedSeq(res0, res1, res2)
        sc.parallelize(compositeQueriesResults)
          .coalesce(1, shuffle = true)
          .saveAsTextFile(s"/user/group-15/lsh/composite_queries_results_$timesEval02.txt")
      }

    }

    if (cluster) {
      val timesEvalAll = 10
      // Run queries 0 to 7 with base and broadcast to assess their speed and performance metrics
      val baseConstructionBuilder = () => new BaseConstruction(sqlContext, corpusRdd)
      runSimpleQuery(timesEvalAll, baseConstructionBuilder, getQueryFileName, "base", sc, exact, sqlContext, corpusRdd, totCountCorpus, cluster)

      val broadcastConstructionBuilder = () => new BaseConstructionBroadcast(sqlContext, corpusRdd)
      runSimpleQuery(timesEvalAll, broadcastConstructionBuilder, getQueryFileName, "broadcast", sc, exact, sqlContext, corpusRdd, totCountCorpus, cluster)

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

  def accuracy(ground_truth: RDD[(String, Set[String])], lsh_truth: RDD[(String, Set[String])], corpusCount: Long): Double = {
    /*
    * Compute the accuracy for each near-neighbor LSH query against the accurate result
    * Then, compute the average across all queries
    * ground_truth: results of queries in (movie_name, [nn_movie_names]) format produced by ExactNN
    * lsh_truth: results of queries in (movie_name, [nn_movie_names]) format produced by an LSH Construction
    * returns average accuracy
    * */

    val accMetric = (truth:Set[String], lsh:Set[String], corpusCount: Long) => {
      val tp = truth.intersect(lsh).size.doubleValue()
      val fn = truth.diff(lsh).size.doubleValue()
      val fp = lsh.diff(truth).size.doubleValue()
      val tn = (corpusCount - (truth.union(lsh).size)).doubleValue()

      (tp+tn)/(tp+tn+fn+fp)
    }

    computeMetric(
      ground_truth, lsh_truth, (exact, estimated) => accMetric(exact, estimated, corpusCount)
    )
  }

  def avgDistances(nn: RDD[(String, Set[String])], queryRDD: RDD[(String, List[String])], corpusRdd: RDD[(String, List[String])]): (RDD[(String, Double)], Double) = {
    val mapQuery = queryRDD.collectAsMap()
    val mapCorpus = corpusRdd.collectAsMap()

    val distances =
      nn
      .map{case(movie, neighbors) => (movie, mapQuery(movie), neighbors.map(mapCorpus))}
      .map{case(movie, movieKeys, neighKeys) => (movie, neighKeys.map(nKey => jaccard(movieKeys, nKey)))}
      .map{case(movie, neighDist) => (movie, mean(neighDist))}

    val meanDist = distances.map(_._2).mean()
    (distances, meanDist)
  }

  private def jaccard(query: List[String], data: List[String]): Double = {
    val querySet = query.toSet
    val dataSet = data.toSet
    val jaccard = querySet.intersect(dataSet).size.doubleValue()/querySet.union(dataSet).size.doubleValue()
    jaccard
  }

  def query(constructionBuilder: () => Construction, ground: RDD[(String, Set[String])],
            queryRdd: RDD[(String, List[String])], reqPrecision: Double, reqRecall: Double, times: Int = 1, cluster: Boolean): Double = {

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

  def loadRDD(sc: SparkContext, filename: String, cluster: Boolean): RDD[(String, List[String])] = {
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

  private def query0(times: Int, sc: SparkContext, getQueryFileName: Int => String, exact: Construction, sqlContext: SQLContext, corpusRdd: RDD[(String, List[String])], cluster: Boolean): Double = {
    // Query 0 -> Tradeoff between FPs and FNs, but rec > pr so we should keep FNs low, thus we use AND+OR
    val RECALL = 0.83
    val PRECISION = 0.70
    val (r, b) = (2, 2)
    val queryRdd = loadRDD(sc, getQueryFileName(0), cluster).cache()
    val ground = exact.eval(queryRdd).cache()
    val constructionBuilder = () => CompositeConstruction.andOrBase(sqlContext, corpusRdd, r, b)
    val correctPercentage = query(constructionBuilder, ground, queryRdd, PRECISION, RECALL, times, cluster)

    queryRdd.unpersist()
    ground.unpersist()

    correctPercentage
  }

  private def query1(times: Int, sc: SparkContext, getQueryFileName: Int => String, exact: Construction, sqlContext: SQLContext, corpusRdd: RDD[(String, List[String])], cluster: Boolean) = {
    // Query 1 -> Recall is kinda low and precision is high, we should avoid FPs, we can admit more FNs, we use AND
    val RECALL = 0.7
    val PRECISION = 0.98
    val r = 3
    val queryRdd = loadRDD(sc, getQueryFileName(1), cluster).cache()
    val ground = exact.eval(queryRdd).cache()
    val constructionBuilder = () => ANDConstruction.getConstructionBroadcast(sqlContext, corpusRdd, r)
    val correctPercentage = query(constructionBuilder, ground, queryRdd, PRECISION, RECALL, times, cluster)

    queryRdd.unpersist()
    ground.unpersist()

    correctPercentage
  }

  private def query2(times: Int, sc: SparkContext, getQueryFileName: Int => String, exact: Construction, sqlContext: SQLContext, corpusRdd: RDD[(String, List[String])], cluster: Boolean): Double = {
    // Query 2 -> Recall is high and precision is low, we should avoid FNs, we can admit more FPs, we use OR with b = 3
    val RECALL = 0.9
    val PRECISION = 0.45
    val b = 3
    val queryRdd = loadRDD(sc, getQueryFileName(2), cluster).cache()
    val ground = exact.eval(queryRdd).cache()
    val constructionBuilder = () => ORConstruction.getConstructionBroadcast(b, sqlContext, corpusRdd)
    val correctPercentage = query(constructionBuilder, ground, queryRdd, PRECISION, RECALL, times, cluster)

    queryRdd.unpersist()
    ground.unpersist()

    correctPercentage
  }

  private def runSimpleQuery(times: Int, baseConstructionBuilder: () => Construction, getQueryFileName: Int => String, constructorType: String, sc: SparkContext, exact: Construction, sqlContext: SQLContext, corpusRdd: RDD[(String, List[String])], totCountCorpus: Long, cluster: Boolean): Unit = {
    val baseQueriesResults = 0.until(8)
      .map(measureStatistics(_, times, baseConstructionBuilder, getQueryFileName, sc, exact, sqlContext, corpusRdd, totCountCorpus, cluster))

    sc.parallelize(baseQueriesResults.map(_._1))
      .coalesce(1, shuffle = true)
      .saveAsTextFile(s"/user/group-15/lsh/testWithExact/${constructorType}_queries_results_$times.txt")

    baseQueriesResults
      .map(_._2)
      .foreach{ case (queryN, distanceDifferences) =>
        distanceDifferences
          .coalesce(1, shuffle = true)
          .saveAsTextFile(s"/user/group-15/lsh/testWithExact/${constructorType}_query${queryN}_distance_diff_$times.txt")
      }
  }


  private def doWarmUpExact(exact: Construction, queryRDD: RDD[(String, List[String])]): Unit = {
    0.until(3).map(_ => exact.eval(queryRDD))
  }

  private def measureOneComputationExact(exact: Construction, queryRDD: RDD[(String, List[String])]): Double = {
    val baseStart = System.nanoTime()
    val baseRes = exact.eval(queryRDD)
    baseRes.count()
    (System.nanoTime() - baseStart) / 1e9
  }

  private def measureStatistics(queryN: Int, n: Int, constructionBuilder: () => Construction, getQueryFileName: Int => String, sc: SparkContext, exact: Construction, sqlContext: SQLContext, corpusRdd: RDD[(String, List[String])], totCountCorpus: Long, cluster: Boolean): (((Int, IndexedSeq[Double], Double, Double, IndexedSeq[Double], Double, Double, IndexedSeq[(Double, Double, Double)], Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Long), (Int, RDD[(String, Double)]))) = {
    val queryRDD = loadRDD(sc, getQueryFileName(queryN), cluster).cache()

    doWarmUpExact(exact, queryRDD)
    val exactTimeList: IndexedSeq[Double] = 0.until(n).map(_ => measureOneComputationExact(exact, queryRDD))

    val ground = exact.eval(queryRDD).cache()

    print(s"Starting Warm-up for query ${queryN}")
    doWarmUp(queryRDD, constructionBuilder)

    val timeList: IndexedSeq[Double] = 0.until(n).map(_ => measureOneComputation(queryRDD, constructionBuilder))
    val performancesList: IndexedSeq[(Double, Double, Double)] = 0.until(n).map(_ => measureOnePerformance(queryRDD, constructionBuilder, ground, totCountCorpus))
    val accuracyMeasures = (mean(performancesList.map(_._1)), stdDev(performancesList.map(_._1)))
    val precisionMeasures = (mean(performancesList.map(_._2)), stdDev(performancesList.map(_._2)))
    val recallMeasures = (mean(performancesList.map(_._3)), stdDev(performancesList.map(_._3)))

    val (distanceDifferences, meanDistDiffTot, stdDistDiffTot, meanDistDiffTotPointwise, stdDistDiffTotPointwise) = measureDistances(ground, queryRDD, constructionBuilder, queryN, n, corpusRdd)

    val querySize = queryRDD.count()
    queryRDD.unpersist()

    ((queryN,
      timeList, mean(timeList), stdDev(timeList),
      exactTimeList, mean(exactTimeList), stdDev(exactTimeList),
      performancesList,
      accuracyMeasures._1, accuracyMeasures._2,
      precisionMeasures._1, precisionMeasures._2,
      recallMeasures._1, recallMeasures._2,
      meanDistDiffTot, stdDistDiffTot,
      meanDistDiffTotPointwise, stdDistDiffTotPointwise,
      querySize), (queryN, distanceDifferences))
  }

  private def measureOneComputation(queryRDD: RDD[(String, List[String])], constructionBuilder: () => Construction): Double = {
    val baseConstruction = constructionBuilder()
    val baseRes = baseConstruction.eval(queryRDD)
    val baseStart = System.nanoTime()
    baseRes.count()
    (System.nanoTime() - baseStart) / 1e9
  }

  private def measureOnePerformance(queryRDD: RDD[(String, List[String])], constructionBuilder: () => Construction, ground: RDD[(String, Set[String])], totCountCorpus: Long): (Double, Double, Double) = {
    val baseConstruction = constructionBuilder()
    val baseRes = baseConstruction.eval(queryRDD)
    val queryRecall = recall(ground, baseRes)
    val queryPrecision = precision(ground, baseRes)
    val queryAccuracy = accuracy(ground, baseRes, totCountCorpus)

    (queryAccuracy, queryPrecision, queryRecall)
  }

  private def measureDistances(ground: RDD[(String, Set[String])], queryRDD: RDD[(String, List[String])], constructionBuilder: () => Construction, queryN: Int, n: Int, corpusRdd: RDD[(String, List[String])]): (RDD[(String, Double)], Double, Double, Double, Double) = {
    val (exactDistances, meanExactDistances) = avgDistances(ground, queryRDD, corpusRdd)
    val distancesRes = 0.until(n).map(_ => distanceDiff(exactDistances, meanExactDistances, queryRDD, constructionBuilder, corpusRdd))
    val meanDistDiffPointwise =
      distancesRes
        .map(_._1)
        .reduce(_ union _)
        .groupBy(_._1)
        .map{case(movie, differences) => (movie, mean(differences.map(_._2)))}
    val meanDistDiffTot = mean(distancesRes.map(_._2))
    val stdDistDiffTot = stdDev(distancesRes.map(_._2))
    val meanDistDiffTotPointwise = meanDistDiffPointwise.map(_._2).mean()
    val stdDistDiffTotPointwise = meanDistDiffPointwise.map(_._2).stdev()
    (meanDistDiffPointwise, meanDistDiffTot, stdDistDiffTot, meanDistDiffTotPointwise, stdDistDiffTotPointwise)
  }

  private def distanceDiff(exactDistances: RDD[(String, Double)], meanExactDistances: Double, queryRDD: RDD[(String, List[String])], constructionBuilder: () => Construction, corpusRdd: RDD[(String, List[String])]): (RDD[(String, Double)], Double) = {
    val baseConstruction = constructionBuilder()
    val baseRes = baseConstruction.eval(queryRDD)
    val (lshDistances, meanLSHDistances) = avgDistances(baseRes, queryRDD, corpusRdd)
    (exactDistances
      .join(lshDistances)
      .map{ case (movie, (exact, approx)) => (movie, exact - approx)},
      meanExactDistances - meanLSHDistances)
  }

}
