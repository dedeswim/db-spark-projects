package thetajoin

import org.apache.spark.Partitioner
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
  def ineq_join(dat1: RDD[Row], dat2: RDD[Row], attrIndex1: Int, attrIndex2: Int, condition: String): RDD[(Int, Int)] = {

    val R = dat1.map(_ (attrIndex1).asInstanceOf[Int])
    val S = dat2.map(_ (attrIndex2).asInstanceOf[Int])

    val cardR = dat1.count()
    val cardS = dat2.count()
    val cR = (cardR / scala.math.sqrt(cardS * cardR / partitions)).toInt
    val cS = (cardS / scala.math.sqrt(cardS * cardR / partitions)).toInt

    val quantilesR = R.takeSample(withReplacement = false, cR - 1).sorted
    val quantilesS = S.takeSample(withReplacement = false, cS - 1).sorted

    // val sortedR = R.collect.sorted
    // val sortedS = S.collect.sorted
    // val rQuantilesIndices = 0 to sortedR.length by (sortedR.length / cR)

    println(s"R quantiles: ${quantilesR.mkString(", ")}")
    println(s"S quantiles: ${quantilesS.mkString(", ")}")

    val regionsR: RDD[(Int, IndexedSeq[Int])] =
      R
        .map(r => (r, getBucket(r, quantilesR)))
        //.map(t => (t._1, getRowOrCols(t._2, cR, R)))
        .map(t => (t._1, getRegions(t._2, cS, "R")))

    val mapR: RDD[(Int, (Int, String))] =
      regionsR
        .flatMap { case (attr, regions) => regions.map((attr, _)) }
        .map(t => (t._2, (t._1, "R")))

    val regionsS: RDD[(Int, IndexedSeq[Int])] =
      S
        .map(r => (r, getBucket(r, quantilesS)))
        //.map(t => (t._1, getRowOrCols(t._2, cR, R)))
        .map(t => (t._1, getRegions(t._2, cS, "S")))

    val mapS: RDD[(Int, (Int, String))] =
      regionsS
        .flatMap { case (attr, regions) => regions.map((attr, _)) }
        .map(t => (t._2, (t._1, "S")))

    //    val mapR: RDD[(Int, Int)] = regionsR
    //      .flatMap { case (attr, regions) => regions.map((attr, _)) }
    //
    //    val mapS: RDD[(Int, Int)] = regionsS
    //      .flatMap { case (attr, regions) => regions.map((attr, _)) }

    val M: RDD[(Int, (Int, String))] =
      mapR
        .union(mapS)
        .partitionBy(new BucketPartitioner(partitions))

    //    println(M.getNumPartitions)
    //    var p = M.glom().collect()
    //    p.foreach(println)

    val joined: RDD[(Int, Int)] = M.mapPartitionsWithIndex((i, t) => reducePartition(t, condition, i))

    joined
  }

  @scala.annotation.tailrec
  private def findQuantile(attr: Int, keys: Array[Int]): Int = {
    if (keys.length == 1) {
      keys(0)
    } else {
      keys(keys.length / 2) match {
        case quantile if quantile == attr => keys(keys.length / 2)
        case quantile if attr > quantile => findQuantile(attr, keys.takeRight(keys.length / 2))
        case quantile if attr < quantile => findQuantile(attr, keys.take(keys.length / 2))
      }
    }
  }

  private def getBucket(attr: Int, quantiles: Array[Int]): Int = {
    val groupedQuantiles =
      quantiles
        .zipWithIndex
        .groupBy(_._1)
        .map { case (quantile, tuples) => (quantile, tuples.map(_._2)) }

    val quantile = findQuantile(attr, groupedQuantiles.keys.toArray)
    val quantileBuckets = groupedQuantiles(quantile)

    val rnd = new scala.util.Random
    quantileBuckets(rnd.nextInt(quantileBuckets.length))
  }

  private def getRegions(bucket: Int, cS: Int, relation: String): IndexedSeq[Int] = {
    relation match {
      case "R" => bucket * cS until (bucket * cS + cS)
      case "S" => bucket until partitions by cS
    }
  }

  private def reducePartition(tuples: Iterator[(Int, (Int, String))], condition: String, partitionIndex: Int): Iterator[(Int, Int)] = {

    val tupTot = tuples.toList.partition(t => t._2._2 == "R")

    println(s"Input reducer $partitionIndex: ${tupTot._1.size + tupTot._2.size}")

    val tupR = tupTot._1.map(t => t._2._1)
    val tupS = tupTot._2.map(t => t._2._1)

    val cross = tupR.flatMap(x => tupS.map(y => (x, y)))

    val result = condition match {
      case "<" => cross.filter(c => c._1 < c._2)
      case ">" => cross.filter(c => c._1 > c._2)
      case _ => throw new IllegalArgumentException("Wrong condition selected")
    }

    println(s"Output reducer $partitionIndex: ${result.size}")

    result.toIterator

  }

  class BucketPartitioner(override val numPartitions: Int) extends Partitioner {
    def getPartition(key: Any): Int = key.asInstanceOf[Int]
  }

  private def getRowOrCols(bucket: Int, c: Int, dim: Long): Int = {
    val buckL = dim / c
    val start = bucket * buckL.toInt
    val end = bucket match {
      case `c` => dim.toInt
      case _ => (bucket + 1) * buckL.toInt
    }
    val rnd = new scala.util.Random
    start + rnd.nextInt(end - start)
  }
}
