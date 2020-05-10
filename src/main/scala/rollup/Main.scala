package rollup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import utils.Utils._

object Main {

  val rollup = new RollupOperator
  val aggAttributeIndex = 8
  val agg = "AVG"

  // Rollup using the following attributes.
  // lo_suppkey, lo_orderpriority, lo_shipmode, lo_tax, lo_discount, lo_linenumber, lo_custkey, lo_partkey, lo_orderkey
  val groupingList = List(4, 6, 16, 14, 11, 1, 2, 3, 0)

  def main(args: Array[String]) {

    val cluster = true

    val spark = if (cluster) {
      runOnCluster()
    } else {
      runOnLocal()
    }

    val filenames = IndexedSeq("lineorder_big.tbl") // lineorder_small.tbl", "lineorder_medium.tbl")

    for (filename <- filenames) {
      // size, n_indices, results, avg, std
      var naiveResults = spark.sparkContext.emptyRDD[(Int, Int, List[Double], Double, Double)]
      var optResults = spark.sparkContext.emptyRDD[(Int, Int, List[Double], Double, Double)]

      val filePath = getFilePath(cluster, filename)
      val entireRdd = loadRDD(spark.sqlContext, filePath, cluster, toLimit = false, delimiter = "|", header = "true")

      val rddSize = entireRdd.count().toInt
      val splits = IndexedSeq(3, 2, 1)

      for (i <- splits) {
        val size = rddSize / i
        val rdd = loadRDD(spark.sqlContext, filePath, cluster, size, delimiter = "|", header = "true").cache()

        println("Starting warm-up...")
        doWarmUp(rdd)
        println("Warm-up ended")
        println("----------------------------------------------------------------")

        for (n_indices <- groupingList.indices) {
          println(s"File: $filename, limit: $size, number of indices: $n_indices")
          val runGroupingList = groupingList.take(n_indices)
          val (naiveResList, naiveAvg, naiveStd) = measureStatistics(rdd, runGroupingList, "naive")
          val (optResList, optAvg, optStd) = measureStatistics(rdd, runGroupingList, "optimized")
          val naiveResRun =
            spark.sparkContext.parallelize(IndexedSeq((size, n_indices, naiveResList, naiveAvg, naiveStd)))
          val optResRun =
            spark.sparkContext.parallelize(IndexedSeq((size, n_indices, optResList, optAvg, optStd)))

          naiveResults = naiveResults.union(naiveResRun)
          optResults = optResults.union(optResRun)
        }

        rdd.unpersist()
      }

      // Take small, medium, big
      val fileSize = filename.split(raw"\.")(0).drop(10)
      val run = 0

      naiveResults
        .sortBy(t => (t._1, t._2))
        .coalesce(1, shuffle = true)
        .saveAsTextFile(s"/user/group-15/results_rollup_naive_${run}_avg_$fileSize")

      optResults
        .sortBy(t => (t._1, t._2))
        .coalesce(1, shuffle = true)
        .saveAsTextFile(s"/user/group-15/results_rollup_opt_${run}_avg_$fileSize")
    }

    /*// use the following code to evaluate the correctness of your results
    val correctRes = df.rollup("lo_suppkey", "lo_orderkey", "lo_linenumber", "lo_partkey", "lo_shipmode")
      .agg(avg("lo_quantity")).rdd
      .map(row => (row.toSeq.toList.dropRight(1).filter(x => x != null), row(row.size - 1)))
    // correctRes.foreach(x => println(x))

    res.subtract(correctRes.asInstanceOf[RDD[(scala.List[Any], Double)]]).collect.foreach(println)
    correctRes.asInstanceOf[RDD[(scala.List[Any], Double)]].subtract(res).collect.foreach(println)*/
  }

  def measureStatistics(rdd: RDD[Row], groupingList: List[Int], method: String, runs: Int = 5): (List[Double], Double, Double) = {
    var time_list = List[Double]()
    for (_ <- 0 until runs) {
      val t = measureOneComputation(rdd, groupingList, method)
      time_list = t :: time_list
    }
    (time_list, mean(time_list), stdDev(time_list))
  }

  def measureOneComputation(rdd: RDD[Row], groupingList: List[Int], method: String): Double = {
    val res = if (method == "naive") {
      rollup.rollup_naive(rdd, groupingList, aggAttributeIndex, agg)
    } else {
      rollup.rollup(rdd, groupingList, aggAttributeIndex, agg)
    }
    val start = System.nanoTime()
    res.count()
    val end = System.nanoTime()
    (end - start) / 1e9
  }

  private def doWarmUp(rdd: RDD[Row]): Unit = {
    val groupingLists = List(
      List(4, 0),
      List(4, 0, 1),
      List(4, 0, 1, 3)
    )

    for (groupingList <- groupingLists) {
      val naiveResult = rollup.rollup_naive(rdd, groupingList, aggAttributeIndex, agg)
      naiveResult.count()
      val optResult = rollup.rollup(rdd, groupingList, aggAttributeIndex, agg)
      optResult.count()
    }
  }
}