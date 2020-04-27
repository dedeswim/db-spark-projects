package ThetaJoinTest

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import thetajoin.ThetaJoin

class ThetaJoinTest extends AnyFunSuite {
  val thetaJoin1 = new ThetaJoin(1)
  val thetaJoin2 = new ThetaJoin(2)
  val thetaJoin16 = new ThetaJoin(16)
  val thetaJoin128 = new ThetaJoin(128)
  val condition = "<"
  val cR = 11
  val cS = 11
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Project2-group-15")
    .master("local[*]")
    .getOrCreate()

  def testInequalityJoin(attrIndex1: Int, attrIndex2: Int, rdd1: RDD[Row], rdd2: RDD[Row])(thetaJoin: ThetaJoin, condition: String): Unit = {
    val res = thetaJoin.ineq_join(rdd1, rdd2, attrIndex1, attrIndex2, condition)
    val cartesianRes = rdd1.cartesian(rdd2)
      .filter {
        condition match {
          case ">" => x => x._1(attrIndex1).asInstanceOf[Int] > x._2(attrIndex2).asInstanceOf[Int]
          case "<" => x => x._1(attrIndex1).asInstanceOf[Int] < x._2(attrIndex2).asInstanceOf[Int]
        }
      } map (x => (x._1(attrIndex1).asInstanceOf[Int], x._2(attrIndex2).asInstanceOf[Int]))
    assert(res.subtract(cartesianRes).count() == 0)
    assert(cartesianRes.subtract(res).count() == 0)
  }

  test("ThetaJoin.getRegions1") {
    val relation = "R"
    val result = thetaJoin128.getRegions(0, cS, cR, relation)
    val expected = IndexedSeq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    assert(result == expected)
  }

  test("ThetaJoin.getRegions2") {
    val relation = "R"
    val result = thetaJoin128.getRegions(1, cS, cR, relation)
    val expected = IndexedSeq(11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21)
    assert(result == expected)
  }

  test("ThetaJoin.getPartitionsToPrune") {
    val quantilesS = IndexedSeq(31, 50, 50, 50, 50, 50, 50, 50, 50, 62)
    val quantilesR = IndexedSeq(27, 53, 80, 80, 80, 80, 80, 80, 80, 80)
    val result = thetaJoin128.getPartitionsToPrune(quantilesR, quantilesS, condition).toSet
    val expected = (33 until 120).toSet - 43 - 54 - 65 - 76 - 87 - 98 - 109 - 120 + 22 + 23
    println(expected.diff(result).toIndexedSeq.sorted)
    println(result.diff(expected).toIndexedSeq.sorted)
    assert(result == expected)
  }
}
