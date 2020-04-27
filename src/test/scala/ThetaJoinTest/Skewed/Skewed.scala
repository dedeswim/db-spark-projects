package ThetaJoinTest.Skewed

import java.io.File

import ThetaJoinTest.ThetaJoinTest
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import thetajoin.ThetaJoin

class Skewed extends ThetaJoinTest {
  def testSkewed(amount: Int = 4000): (ThetaJoin, String) => Unit = {
    val rdd1 = loadSkewedRDD("/skewed_data.csv", amount)
    val rdd2 = loadSkewedRDD("/skewed_data.csv", amount)
    super.testInequalityJoin(0, 1, rdd1, rdd2)
  }

  def loadSkewedRDD(file: String, amount: Int): RDD[Row] = {
    val input = new File(getClass.getResource(file).getFile).getPath

    spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(input)
      .limit(amount)
      .rdd
  }
}
