package thetajoin

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import utils.Utils._


object Main {

  def main(args: Array[String]) {

    val cluster = true

    val spark = if (cluster) {
      runOnCluster()
    } else {
      runOnLocal()
    }

    val filename1 = "taxA4K.csv"
    val filePath1 = getFilePath(cluster, filename1)
    val filename2 = "taxB4K.csv"
    val filePath2 = getFilePath(cluster, filename2)

    //    val rdd1 = loadRDD(spark.sqlContext, "/skewed_data.csv")
    //    val rdd2 = loadRDD(spark.sqlContext, "/skewed_data.csv")

    for (n <- 1000 to 4000 by 1000) {

      val rdd1 = loadRDD(spark.sqlContext, filePath1, cluster, n).cache()
      val rdd2 = loadRDD(spark.sqlContext, filePath2, cluster, n).cache()

      println("Starting warm-up...")
      doWarmUp(rdd1, rdd2)
      println("Warm-up ended")
      println("----------------------------------------------------------------")

      var results = spark.sparkContext.emptyRDD[(Int, String, Int, Int, List[Double], Double, Double)]
      val partitionsL = IndexedSeq(1,2,4,8,16,32,64,128,256,512)
      val conditions = IndexedSeq("<", ">")
      for (partitions <- partitionsL) {
        for (condition <- conditions) {
          for (attrIndex1 <- 1 to 2; attrIndex2 <- 1 to 2) {
            println(s"Limit: $n, Partitions: $partitions, condition: $condition, attrIndex1: $attrIndex1, attrIndex2: $attrIndex2")
            val (resList, avg, std) = measureStatistics(rdd1, rdd2, partitions, attrIndex1, attrIndex2, condition)
            val resRun = spark.sparkContext.parallelize(List((partitions, condition, attrIndex1, attrIndex2, resList, avg, std)))
            results = results.union(resRun)
          }
        }
      }
      results
        .sortBy(t => (t._1, t._2, t._3, t._4))
        .coalesce(1, shuffle = true)
        .saveAsTextFile(s"/user/group-15/results_thetajoin_count7_$n")

      rdd1.unpersist()
      rdd2.unpersist()
    }

    //    println("----------------------")
    //     use the cartesian product to verify correctness of your result
    //    val cartesianRes = rdd1.cartesian(rdd2)
    //      .filter(x => x._1(attrIndex1).asInstanceOf[Int] > x._2(attrIndex2).asInstanceOf[Int])
    //      .map(x => (x._1(attrIndex1).asInstanceOf[Int], x._2(attrIndex2).asInstanceOf[Int]))
    //     cartesianRes.foreach(x => println(x))


    //    assert(res.sortBy(x => (x._1, x._2)).collect().toList.equals(cartesianRes.sortBy(x => (x._1, x._2)).collect.toList))

    //    val startAssert = System.currentTimeMillis()
    //assert(res.subtract(cartesianRes).count() == 0)
    //assert(cartesianRes.subtract(res).count() == 0)
    //    val endAssert = System.currentTimeMillis()
    //    println(s"Assertion Took ${(endAssert - startAssert).floatValue() / 1000}s")
  }

  def doWarmUp(rdd1: RDD[Row], rdd2: RDD[Row]): Unit = {
    val partitionsL = List(1, 64, 128)
    for (partitions <- partitionsL) {
      for (condition <- IndexedSeq("<", ">")) {
        val thetaJoin = new ThetaJoin(partitions)
        val res1 = thetaJoin.ineq_join(rdd1, rdd2, 1, 1, condition)
        print(res1.count())
        val res2 = thetaJoin.ineq_join(rdd1, rdd2, 2, 2, condition)
        print(res2.count())
      }
    }
  }

  def measureStatistics(rdd1: RDD[Row], rdd2: RDD[Row], partitions: Int, attrIndex1: Int, attrIndex2: Int, condition: String, runs: Int = 5): (List[Double], Double, Double) = {
    var time_list = List[Double]()
    for (_ <- 0 until runs) {
      val t = measureOneComputation(rdd1, rdd2, partitions, attrIndex1, attrIndex2, condition)
      time_list = t :: time_list
    }
    (time_list, mean(time_list), stdDev(time_list))
  }

  def measureOneComputation(rdd1: RDD[Row], rdd2: RDD[Row], partitions: Int, attrIndex1: Int, attrIndex2: Int, condition: String): Double = {
    val start = System.nanoTime()
    val thetaJoin = new ThetaJoin(partitions)
    val res = thetaJoin.ineq_join(rdd1, rdd2, attrIndex1, attrIndex2, condition)
    print(res.count())
    val end = System.nanoTime()
    (end - start) / 1e9
  }

}
