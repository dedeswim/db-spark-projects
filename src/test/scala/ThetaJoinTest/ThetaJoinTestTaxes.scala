package ThetaJoinTest

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import thetajoin.ThetaJoin

class ThetaJoinTestTaxes extends ThetaJoinTest {
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

  class ShortTests extends AnyFunSuite {
    test("ThetaJoin.ineq_joinTestTax1k1PartitionLarger") {
      testTaxes(1000)(thetaJoin1, ">")
    }

    test("ThetaJoin.ineq_joinTestTax1k1PartitionSmaller") {
      testTaxes(1000)(thetaJoin1, "<")
    }

    test("ThetaJoin.ineq_joinTestTax1k16PartitionsLarger") {
      testTaxes(1000)(thetaJoin16, ">")
    }

    test("ThetaJoin.ineq_joinTestTax1k16PartitionsSmaller") {
      testTaxes(1000)(thetaJoin16, "<")
    }

    test("ThetaJoin.ineq_joinTestTax1k128PartitionsLarger") {
      testTaxes(1000)(thetaJoin128, ">")
    }

    test("ThetaJoin.ineq_joinTestTax1k128PartitionsSmaller") {
      testTaxes(1000)(thetaJoin128, "<")
    }
  }

  test("ThetaJoin.ineq_joinTestTax4k1PartitionLarger") {
    testTaxes()(thetaJoin1, ">")
  }

  test("ThetaJoin.ineq_joinTestTax4k1PartitionSmaller") {
    testTaxes()(thetaJoin1, "<")
  }

  test("ThetaJoin.ineq_joinTestTax4k16PartitionsLarger") {
    testTaxes()(thetaJoin16, ">")
  }

  test("ThetaJoin.ineq_joinTestTax4k16PartitionsSmaller") {
    testTaxes()(thetaJoin16, "<")
  }

  test("ThetaJoin.ineq_joinTestTax4k128PartitionsLarger") {
    testTaxes()(thetaJoin128, ">")
  }

  test("ThetaJoin.ineq_joinTestTax4k128PartitionsSmaller") {
    testTaxes()(thetaJoin128, "<")
  }

}
