package ThetaJoinTest

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import thetajoin.ThetaJoin

class ThetaJoinTestSkewed extends ThetaJoinTest {
  def testSkewed(amount: Int = 4000): (ThetaJoin, String) => Unit = {
    val rdd1 = loadSkewedRDD("/skewed_data.csv", amount)
    val rdd2 = loadSkewedRDD("/skewed_data.csv", amount)
    testInequalityJoin(0, 1, rdd1, rdd2)
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

  class ShortTests extends AnyFunSuite {
    test("ThetaJoin.ineq_joinTestSkewed1k1PartitionLarger") {
      testSkewed(1000)(thetaJoin1, ">")
    }

    test("ThetaJoin.ineq_joinTestSkewed1k1PartitionSmaller") {
      testSkewed(1000)(thetaJoin1, "<")
    }

    test("ThetaJoin.ineq_joinTestSkewed1k16PartitionsLarger") {
      testSkewed(1000)(thetaJoin16, ">")
    }

    test("ThetaJoin.ineq_joinTestSkewed1k16PartitionsSmaller") {
      testSkewed(1000)(thetaJoin16, "<")
    }

    test("ThetaJoin.ineq_joinTestSkewed1k128PartitionsLarger") {
      testSkewed(1000)(thetaJoin128, ">")
    }

    test("ThetaJoin.ineq_joinTestSkewed1k128PartitionsSmaller") {
      testSkewed(1000)(thetaJoin128, "<")
    }
  }

  test("ThetaJoin.ineq_joinTestSkewed4k1PartitionLarger") {
    testSkewed()(thetaJoin1, ">")
  }

  test("ThetaJoin.ineq_joinTestSkewed4k1PartitionSmaller") {
    testSkewed()(thetaJoin1, "<")
  }


  test("ThetaJoin.ineq_joinTestSkewed4k16PartitionsLarger") {
    testSkewed()(thetaJoin16, ">")
  }

  test("ThetaJoin.ineq_joinTestSkewed4k16PartitionsSmaller") {
    testSkewed()(thetaJoin16, "<")
  }

  test("ThetaJoin.ineq_joinTestSkewed4k128PartitionsLarger") {
    testSkewed()(thetaJoin128, ">")
  }

  test("ThetaJoin.ineq_joinTestSkewed4k128PartitionsSmaller") {
    testSkewed()(thetaJoin128, "<")
  }
}
