package thetajoin

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import org.slf4j.LoggerFactory

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
  def ineq_join(dat1: RDD[Row], dat2: RDD[Row], attrIndex1: Int, attrIndex2: Int, condition:String): RDD[(Int, Int)] = {
    null
  }
}
