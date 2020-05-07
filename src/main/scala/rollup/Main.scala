package rollup

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Project2-group-15")
      .master("local[*]")
      .getOrCreate()


    val input = new File(getClass.getResource("/lineorder_small.tbl").getFile).getPath
    val df = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      // .load("/user/cs422/lineorder_big.tbl")
      .load(input)

    val rdd = df.rdd

    val groupingList = List(4, 0, 1, 3, 16)
    val rollup = new RollupOperator

    // val res = rollup.rollup_naive(rdd, groupingList, 8, "AVG")
    val res = rollup.rollup(rdd, groupingList, 8, "AVG")
    // res.foreach(x => println(x))

    // use the following code to evaluate the correctness of your results
    val correctRes = df.rollup("lo_suppkey", "lo_orderkey", "lo_linenumber", "lo_partkey", "lo_shipmode")
      .agg(avg("lo_quantity")).rdd
      .map(row => (row.toSeq.toList.dropRight(1).filter(x => x != null), row(row.size - 1)))
    // correctRes.foreach(x => println(x))

    res.subtract(correctRes.asInstanceOf[RDD[(scala.List[Any], Double)]]).collect.foreach(println)
    correctRes.asInstanceOf[RDD[(scala.List[Any], Double)]].subtract(res).collect.foreach(println)
  }
}