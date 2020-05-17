package lsh

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import utils.Utils._


object Main extends Serializable {

  def main(args: Array[String]) {

    val cluster = true
    val evaluateComposites = false
    val evaluateBasePerformance = true

    val sc = if (cluster) {
      runOnCluster().sparkContext
    } else {
      runOnLocal().sparkContext
    }

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val getQueryFileName = (x: Int) => getFilePath(cluster, s"lsh-query-$x.csv")


    // Run the queries for which composite constructions are required
    if (evaluateComposites) {
      val corpusFileComposite = getFilePath(cluster, "lsh-corpus-small.csv")
      val corpusRDDComposite = loadRDD(sc, corpusFileComposite, cluster).cache()
      val totCountCorpusComposite = corpusRDDComposite.count()
      val exactComposite: Construction = new ExactNN(sqlContext, corpusRDDComposite, 0.3)

      val timesComposites = 100
      val res0 =
        query0(timesComposites, sc, getQueryFileName, exactComposite, sqlContext, corpusRDDComposite, cluster, totCountCorpusComposite).map((0, _))
      val res1 =
        query1(timesComposites, sc, getQueryFileName, exactComposite, sqlContext, corpusRDDComposite, cluster, totCountCorpusComposite).map((1, _))
      val res2 =
        query2(timesComposites, sc, getQueryFileName, exactComposite, sqlContext, corpusRDDComposite, cluster, totCountCorpusComposite).map((2, _))

      // If on the cluster, save results to file
      val compositeQueriesResults =
        IndexedSeq(res0, res1, res2)
          .flatten
          .map { case (query, (precision, recall, accuracy)) => s"$query,$precision,$recall,$accuracy" }

      if (cluster) {
        sc.parallelize(compositeQueriesResults)
          .coalesce(1, shuffle = true)
          .saveAsTextFile(s"/user/group-15/lsh/composite_queries_results_$timesComposites")
      } else {
        compositeQueriesResults.foreach(println)
      }

    }

    if (cluster & evaluateBasePerformance) {
      val timesEvalAll = 5
      // Run queries 0 to 7 with base and broadcast to assess their speed and performance metrics
      runSimpleQuery(timesEvalAll, getQueryFileName, "base", sc, sqlContext, cluster)

      runSimpleQuery(timesEvalAll, getQueryFileName, "broadcast", sc, sqlContext, cluster)

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

    val accMetric = (truth: Set[String], lsh: Set[String], corpusCount: Long) => {
      val tp = truth.intersect(lsh).size.doubleValue()
      val fn = truth.diff(lsh).size.doubleValue()
      val fp = lsh.diff(truth).size.doubleValue()
      val tn = (corpusCount - truth.union(lsh).size).doubleValue()

      (tp + tn) / (tp + tn + fn + fp)
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
        .map { case (movie, neighbors) => (movie, mapQuery(movie), neighbors.map(mapCorpus)) }
        .map { case (movie, movieKeys, neighKeys) => (movie, neighKeys.map(nKey => jaccard(movieKeys, nKey))) }
        .map { case (movie, neighDist) => (movie, mean(neighDist)) }

    val meanDist = distances.map(_._2).mean()
    (distances, meanDist)
  }

  def query(constructionBuilder: () => Construction, ground: RDD[(String, Set[String])],
            queryRdd: RDD[(String, List[String])], reqPrecision: Double, reqRecall: Double,
            times: Int = 1, cluster: Boolean, totCountCorpus: Long): Seq[(Double, Double, Double)] = {

    def singleQuery(): (Double, Double, Double) = {
      val construction = constructionBuilder()
      val res = construction.eval(queryRdd)
      val queryPrecision = precision(ground, res)
      val queryRecall = recall(ground, res)
      val queryAccuracy = accuracy(ground, res, totCountCorpus)

      if (!cluster & (queryPrecision < reqPrecision | queryRecall < reqRecall)) {
        println(s"Precision: $queryPrecision, required: $reqPrecision")
        println(s"Recall: $queryRecall, required: $reqRecall")
      }

      (queryPrecision, queryRecall, queryAccuracy)
    }

    val correctResults = 0.until(times).map(_ => singleQuery())

    if (!cluster) {
      println(s"Results:")
      correctResults.foreach(t => println(s"Precision: ${t._1}, Recall: ${t._2}, Accuracy: ${t._3}"))
    }

    correctResults
  }

  private def jaccard(query: List[String], data: List[String]): Double = {
    val querySet = query.toSet
    val dataSet = data.toSet
    val jaccard = querySet.intersect(dataSet).size.doubleValue() / querySet.union(dataSet).size.doubleValue()
    jaccard
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

  private def query0(times: Int, sc: SparkContext, getQueryFileName: Int => String, exact: Construction,
                     sqlContext: SQLContext, corpusRdd: RDD[(String, List[String])],
                     cluster: Boolean, totCountCorpus: Long): Seq[(Double, Double, Double)] = {
    // Query 0 -> Tradeoff between FPs and FNs, but rec > pr so we should keep FNs low, thus we use AND+OR
    val RECALL = 0.83
    val PRECISION = 0.70
    val (r, b) = (2, 2)
    val queryRdd = loadRDD(sc, getQueryFileName(0), cluster).cache()
    val ground = exact.eval(queryRdd).cache()
    val constructionBuilder = () => CompositeConstruction.andOrBase(sqlContext, corpusRdd, r, b)
    val queryResults = query(constructionBuilder, ground, queryRdd, PRECISION, RECALL, times, cluster, totCountCorpus)

    queryRdd.unpersist()
    ground.unpersist()

    queryResults
  }

  private def query1(times: Int, sc: SparkContext, getQueryFileName: Int => String, exact: Construction,
                     sqlContext: SQLContext, corpusRdd: RDD[(String, List[String])],
                     cluster: Boolean, totCountCorpus: Long): Seq[(Double, Double, Double)] = {
    // Query 1 -> Recall is kinda low and precision is high, we should avoid FPs, we can admit more FNs, we use AND
    val RECALL = 0.7
    val PRECISION = 0.98
    val r = 3
    val queryRdd = loadRDD(sc, getQueryFileName(1), cluster).cache()
    val ground = exact.eval(queryRdd).cache()
    val constructionBuilder = () => ANDConstruction.getConstructionBroadcast(sqlContext, corpusRdd, r)
    val queryResults = query(constructionBuilder, ground, queryRdd, PRECISION, RECALL, times, cluster, totCountCorpus)

    queryRdd.unpersist()
    ground.unpersist()

    queryResults
  }

  private def query2(times: Int, sc: SparkContext, getQueryFileName: Int => String, exact: Construction,
                     sqlContext: SQLContext, corpusRdd: RDD[(String, List[String])],
                     cluster: Boolean, totCountCorpus: Long): Seq[(Double, Double, Double)] = {
    // Query 2 -> Recall is high and precision is low, we should avoid FNs, we can admit more FPs, we use OR with b = 3
    val RECALL = 0.9
    val PRECISION = 0.45
    val b = 3
    val queryRdd = loadRDD(sc, getQueryFileName(2), cluster).cache()
    val ground = exact.eval(queryRdd).cache()
    val constructionBuilder = () => ORConstruction.getConstructionBroadcast(b, sqlContext, corpusRdd)
    val queryResults = query(constructionBuilder, ground, queryRdd, PRECISION, RECALL, times, cluster, totCountCorpus)

    queryRdd.unpersist()
    ground.unpersist()

    queryResults
  }

  private def runSimpleQuery(times: Int, getQueryFileName: Int => String, constructorType: String, sc: SparkContext, sqlContext: SQLContext, cluster: Boolean): Unit = {
    val getCorpusFileName = (dim: String) => getFilePath(cluster, s"lsh-corpus-$dim.csv")
    val getCorpusDim = (x: Int) => if (0 to 2 contains x) "small" else if (3 to 5 contains x) "medium" else "large"

    val slices = IndexedSeq(0, 3, 6, 8)
    for (i <- 0 to 2) {
      val baseQueriesResults = slices(i).until(slices(i+1))
        .map(n => measureStatistics(n, times, getQueryFileName, constructorType, getCorpusFileName(getCorpusDim(n)), sc, sqlContext, cluster))

      val queryRange = slices(i) + "_to_" + (slices(i+1)-1)
      sc.parallelize(baseQueriesResults.map(_._1))
        .coalesce(1, shuffle = true)
        .saveAsTextFile(s"/user/group-15/lsh/testWithBigCorpuses/${constructorType}_queries_${queryRange}_results_$times.txt")

      baseQueriesResults
        .map(_._2)
        .foreach { case (queryN, distanceDifferences) =>
          distanceDifferences
            .coalesce(1, shuffle = true)
            .saveAsTextFile(s"/user/group-15/lsh/testWithBigCorpuses/${constructorType}_query${queryN}_distance_diff_$times.txt")
        }
    }

  }


  private def doWarmUpExact(exact: Construction, queryRDD: RDD[(String, List[String])]): Unit = {
    0.until(3).map(_ => measureOneComputationExact(exact, queryRDD))
  }

  private def measureOneComputationExact(exact: Construction, queryRDD: RDD[(String, List[String])]): Double = {
    val baseStart = System.nanoTime()
    val baseRes = exact.eval(queryRDD)
    baseRes.count()
    (System.nanoTime() - baseStart) / 1e9
  }

  private def measureStatistics(queryN: Int, n: Int, getQueryFileName: Int => String, constructorType: String,
                                corpusFilePath: String, sc: SparkContext, sqlContext: SQLContext,cluster: Boolean):
  ((Int, IndexedSeq[Double], Double, Double, IndexedSeq[Double], Double, Double, IndexedSeq[(Double, Double, Double)],
    Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Long), (Int, RDD[(String, Double)])) = {

    val queryRDD = loadRDD(sc, getQueryFileName(queryN), cluster).cache()
    queryRDD.count()
    val corpusRdd = loadRDD(sc, corpusFilePath, cluster).cache()
    val totCountCorpus = corpusRdd.count()
    val exact = new ExactNN(sqlContext, corpusRdd, 0.3)

    var exactTimeList: IndexedSeq[Double] = 0.until(n).map(n => n.toDouble)
    if (constructorType=="base") {
      doWarmUpExact(exact, queryRDD)
      exactTimeList = exactTimeList.map(_ => measureOneComputationExact(exact, queryRDD))
    }
    else {
      exactTimeList = exactTimeList.map(_ => 0.0)
    }



    val ground = exact.eval(queryRDD).cache()

    print(s"Starting Warm-up for query $queryN")
    val constructionBuilder = constructorType match {
      case "base" => () => new BaseConstruction(sqlContext, corpusRdd)
      case "broadcast" => () => new BaseConstructionBroadcast(sqlContext, corpusRdd)
    }

    doWarmUp(queryRDD, constructionBuilder)

    val timeList: IndexedSeq[Double] = 0.until(n).map(_ => measureOneComputation(queryRDD, constructionBuilder))
    val performancesList: IndexedSeq[(Double, Double, Double)] = 0.until(n).map(_ => measureOnePerformance(queryRDD, constructionBuilder, ground, totCountCorpus))
    val accuracyMeasures = (mean(performancesList.map(_._1)), stdDev(performancesList.map(_._1)))
    val precisionMeasures = (mean(performancesList.map(_._2)), stdDev(performancesList.map(_._2)))
    val recallMeasures = (mean(performancesList.map(_._3)), stdDev(performancesList.map(_._3)))

    val (distanceDifferences, meanDistDiffTot, stdDistDiffTot, meanDistDiffTotPointwise, stdDistDiffTotPointwise) = measureDistances(ground, queryRDD, constructionBuilder, n, corpusRdd)

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

  private def measureDistances(ground: RDD[(String, Set[String])], queryRDD: RDD[(String, List[String])], constructionBuilder: () => Construction, n: Int, corpusRdd: RDD[(String, List[String])]): (RDD[(String, Double)], Double, Double, Double, Double) = {
    val (exactDistances, meanExactDistances) = avgDistances(ground, queryRDD, corpusRdd)
    val distancesRes = 0.until(n).map(_ => distanceDiff(exactDistances, meanExactDistances, queryRDD, constructionBuilder, corpusRdd))
    val meanDistDiffPointwise =
      distancesRes
        .map(_._1)
        .reduce(_ union _)
        .groupBy(_._1)
        .map { case (movie, differences) => (movie, mean(differences.map(_._2))) }
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
      .map { case (movie, (exact, approx)) => (movie, exact - approx) },
      meanExactDistances - meanLSHDistances)
  }

}
