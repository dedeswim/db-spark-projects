package thetajoin

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import Numeric.Implicits._


object Main {

  def main(args: Array[String]) {

    val cluster = true

    val spark = if (cluster) {
      runOnCluster()
    } else {
      runOnLocal()
    }

    val filename1 = "taxA4K.csv"
    val filepath1 = getFilePath(cluster, filename1)
    val filename2 = "taxB4K.csv"
    val filepath2 = getFilePath(cluster, filename2)

//    val rdd1 = loadRDD(spark.sqlContext, "/skewed_data.csv")
//    val rdd2 = loadRDD(spark.sqlContext, "/skewed_data.csv")

    for (n <- 1000 to 4000 by 1000) {

      val rdd1 = loadRDD(spark.sqlContext, filepath1, cluster, n).cache()
      val rdd2 = loadRDD(spark.sqlContext, filepath2, cluster, n).cache()

      println("Starting warm-up...")
      doWarmUp(rdd1, rdd2)
      println("Warm-up ended")
      println("----------------------------------------------------------------")

      var results = spark.sparkContext.emptyRDD[(Int, String, Int, Int, List[Double], Double, Double)]
      val partitionsL = IndexedSeq(1, 2, 4, 8, 16, 32, 64, 128, 256, 512)
      val conditions = IndexedSeq("<", ">")
      for (partitions <- partitionsL) {
        for (condition <- conditions) {
          for (attrIndex1 <- 1 to 2; attrIndex2 <- 1 to 2) {
            val (resList, avg, std) = measureStatistics(rdd1, rdd2, partitions, attrIndex1, attrIndex2, condition)
            val resRun = spark.sparkContext.parallelize(List((partitions, condition, attrIndex1, attrIndex2, resList, avg, std)))
            results = results.union(resRun)
          }
        }
      }
      results.coalesce(1, shuffle = true).saveAsTextFile(s"/user/group-15/results_thetajoin_$n.txt")
      rdd1.unpersist()
      rdd2.unpersist()
    }

//    println("----------------------")
    // use the cartesian product to verify correctness of your result
//    val cartesianRes = rdd1.cartesian(rdd2)
//      .filter(x => x._1(attrIndex1).asInstanceOf[Int] > x._2(attrIndex2).asInstanceOf[Int])
//      .map(x => (x._1(attrIndex1).asInstanceOf[Int], x._2(attrIndex2).asInstanceOf[Int]))
    // cartesianRes.foreach(x => println(x))


    //    assert(res.sortBy(x => (x._1, x._2)).collect().toList.equals(cartesianRes.sortBy(x => (x._1, x._2)).collect.toList))

    //    val startAssert = System.currentTimeMillis()
    //assert(res.subtract(cartesianRes).count() == 0)
    //assert(cartesianRes.subtract(res).count() == 0)
    //    val endAssert = System.currentTimeMillis()
    //    println(s"Assertion Took ${(endAssert - startAssert).floatValue() / 1000}s")
  }

  def loadRDD(sqlContext: SQLContext, file: String, cluster: Boolean, limit: Int = 4000): RDD[Row] = {

    val partial =
      sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ",")

    val loaded = if (cluster) {
        partial
          .load(file)
    } else {
      val input = new File(getClass.getResource(file).getFile).getPath
      partial
          .load(input)
    }
    loaded
      .limit(limit)
      .rdd
  }

  def runOnCluster(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("Project2-group-15")
      .getOrCreate()

    spark
  }

  def runOnLocal(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("Project2-group-15")
      .master("local[*]")
      .getOrCreate()

    spark
  }

  def getFilePath(cluster: Boolean, filename: String): String = {
    if (cluster) {
      s"/user/cs422/$filename"
    } else {
      s"/$filename"
    }
  }

  def doWarmUp(rdd1: RDD[Row], rdd2: RDD[Row]): Unit = {
    val partitionsL = List(1, 128)
    for (partitions <- partitionsL) {
      for (condition <- IndexedSeq("<", ">")) {
          val thetaJoin = new ThetaJoin(partitions)
        val res1 = thetaJoin.ineq_join(rdd1, rdd2, 1, 1, condition)
        res1.collect()
        val res2 = thetaJoin.ineq_join(rdd1, rdd2, 2, 2, condition)
        res2.collect()
        }
      }
    }

  def time[T](f: => T) : Double = {
    val start = System.nanoTime()
    f
    val end = System.nanoTime()
    (end - start)/1e9
  }

  def measureOneComputation(rdd1: RDD[Row], rdd2: RDD[Row], partitions: Int, attrIndex1: Int, attrIndex2: Int, condition: String): Double = {
    val thetaJoin = new ThetaJoin(partitions)
    val res = thetaJoin.ineq_join(rdd1, rdd2, attrIndex1, attrIndex2, condition)
    time(res.collect())
  }

  def measureStatistics(rdd1: RDD[Row], rdd2: RDD[Row], partitions: Int, attrIndex1: Int, attrIndex2: Int, condition: String, runs: Int = 5): (List[Double], Double, Double) = {
    var time_list = List[Double]()
    for (_ <-0 until runs) {
      val t = measureOneComputation(rdd1, rdd2, partitions, attrIndex1, attrIndex2, condition)
      time_list = t :: time_list
    }
    (time_list, mean(time_list), stdDev(time_list))
  }

  def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)
    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))

}
