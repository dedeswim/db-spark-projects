package thetajoin

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import scala.collection.immutable.TreeMap

class ThetaJoin(partitions: Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("ThetaJoin")

  /*
  this method takes as input two datasets (dat1, dat2) and returns the pairs of join keys that satisfy the theta join condition.
  attrIndex1: is the index of the join key of dat1
  attrIndex2: is the index of the join key of dat2
  condition: Is the join condition. Assume only "<", ">" will be given as input
  Assume condition only on integer fields.
  Returns and RDD[(Int, Int)]: projects only the join keys.
   */
  def ineq_join(dat1: RDD[Row], dat2: RDD[Row], attrIndex1: Int, attrIndex2: Int, condition: String): RDD[(Int, Int)] = {

    val R = dat1.map(_(attrIndex1).asInstanceOf[Int])
    val S = dat2.map(_(attrIndex2).asInstanceOf[Int])

    val cardR = dat1.count()
    val cardS = dat2.count()
    val cR = (cardR / scala.math.sqrt(cardS * cardR / partitions)).toInt
    val cS = (cardS / scala.math.sqrt(cardS * cardR / partitions)).toInt

    val quantilesR = R.takeSample(false, cR - 1).sorted
    val quantilesS = S.takeSample(false, cS - 1).sorted

    val regionsR: RDD[(Int, IndexedSeq[Int])] = R
      .map(r => (r, getBucket(r, quantilesR)))
      //.map(t => (t._1, getRowOrCols(t._2, cR, R)))
      .map(t => (t._1, getRegions(t._2, cS, "R")))

    val mapR: RDD[(Int, Iterable[Int])] = regionsR
      .flatMap { case (attr, regions) => regions.map((attr, _)) }
      .groupBy(t => t._2)
      .map(t => (t._1, t._2.map(v => v._1)))

    val regionsS: RDD[(Int, IndexedSeq[Int])] = S
      .map(r => (r, getBucket(r, quantilesS)))
      //.map(t => (t._1, getRowOrCols(t._2, cR, R)))
      .map(t => (t._1, getRegions(t._2, cS, "S")))

    val mapS: RDD[(Int, Iterable[Int])] = regionsS
      .flatMap { case (attr, regions) => regions.map((attr, _)) }
      .groupBy(t => t._2)
      .map(t => (t._1, t._2.map(v => v._1)))


    ???
  }

  def getBucket(attr: Int, quantiles: Array[Int]): Int = {
    var bucket = 0
    for ((v, i) <- quantiles.zipWithIndex) {
      if (attr >= v) {
        bucket = i+1
        return bucket
      }
    }
    bucket
  }

  def getRowOrCols(bucket: Int, c: Int, dim: Long): Int = {
    val buckL = dim/c
    val start = bucket*buckL.toInt
    val end   = bucket match {
      case `c` => dim.toInt
      case _ => (bucket+1)*buckL.toInt
    }
    val rnd = new scala.util.Random
    start + rnd.nextInt(end - start)
  }

  def getRegions(bucket: Int, cS: Int, relation: String): IndexedSeq[Int] = {
    relation match {
      case "R" => bucket*cS until (bucket*cS + cS)
      case "S" => bucket until partitions by cS
    }
  }
}
