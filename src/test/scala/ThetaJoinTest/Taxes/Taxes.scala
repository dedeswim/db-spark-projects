package ThetaJoinTest.Taxes

import java.io.File

import ThetaJoinTest.ThetaJoinTest
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import thetajoin.ThetaJoin

class Taxes extends ThetaJoinTest {
  def testTaxes(amount: Int = 4000): (ThetaJoin, String) => Unit = {
    val rdd1 = loadTaxesRDD("/taxA4K.csv", amount)
    val rdd2 = loadTaxesRDD("/taxB4K.csv", amount)
    testInequalityJoin(2, 2, rdd1, rdd2)
  }

  def loadTaxesRDD(filename: String, amount: Int = 4000): RDD[Row] = {
    val input = new File(getClass.getResource(filename).getFile).getPath

    spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(input)
      .limit(amount)
      .rdd
  }
}
