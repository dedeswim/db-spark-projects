package thetajoin

import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

object Main {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Project2-group-15")
      // .master("local[*]")
      .getOrCreate()

    val attrIndex1 = 2
    val attrIndex2 = 2

    // val rdd1 = loadRDD(spark.sqlContext, "/dat1_4.csv")
    // val rdd2 = loadRDD(spark.sqlContext, "/dat2_4.csv")
    //
    //    val thetaJoin = new ThetaJoin(4)
    //    val res = thetaJoin.ineq_join(rdd1, rdd2, attrIndex1, attrIndex2, "<")
    //    res.foreach(x => println(x))


    val rdd1 = loadRDD(spark.sqlContext, "/taxA4K.csv")
    val rdd2 = loadRDD(spark.sqlContext, "/taxB4K.csv")

    // val rdd1 = loadLineorderRdd(spark.sqlContext, "/partition_scratch.tsv")
    // val rdd2 = loadLineorderRdd(spark.sqlContext, "/partition_scratch.tsv")

    val thetaJoin = new ThetaJoin(16)
    val res = thetaJoin.ineq_join(rdd1, rdd2, attrIndex1, attrIndex2, "<")
    // println(res.count)

    val start = System.currentTimeMillis()
    res.collect
    val end = System.currentTimeMillis()
    println(s"Took ${(end - start).floatValue() / 1000}s")

    println("----------------------")
    // use the cartesian product to verify correctness of your result
    val cartesianRes = rdd1.cartesian(rdd2)
      .filter(x => x._1(attrIndex1).asInstanceOf[Int] < x._2(attrIndex2).asInstanceOf[Int])
      .map(x => (x._1(attrIndex1).asInstanceOf[Int], x._2(attrIndex2).asInstanceOf[Int]))
    // cartesianRes.foreach(x => println(x))

    //assert(res.sortBy(x => (x._1, x._2)).collect().toList.equals(cartesianRes.sortBy(x => (x._1, x._2)).collect.toList))

    assert(res.subtract(cartesianRes).count() == 0)
    assert(cartesianRes.subtract(res).count() == 0)
  }

  def loadRDD(sqlContext: SQLContext, file: String): RDD[Row] = {
    // val input = new File(getClass.getResource(file).getFile).getPath

    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(s"/user/cs422$file")
      // .limit(1000)
      .rdd
  }

  def loadLineorderRdd(sqlContext: SQLContext, file: String): RDD[Row] = {
    val input = new File(getClass.getResource(file).getFile).getPath

    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "fa")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(input) //
      // .limit(1000)
      .rdd
  }

}
