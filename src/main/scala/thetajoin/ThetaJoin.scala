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

    // Project the needed attribute
    val R = dat1.map(_ (attrIndex1).asInstanceOf[Int])
    val S = dat2.map(_ (attrIndex2).asInstanceOf[Int])

    // Compute C_r and C_s
    val cardR = dat1.count
    val cardS = dat2.count
    val cR = (cardR / scala.math.sqrt(cardS * cardR / partitions)).toInt
    val cS = (cardS / scala.math.sqrt(cardS * cardR / partitions)).toInt

    // Take sample to compute the quantiles
    val samplesFactor = 100
    val rSamples = R.takeSample(withReplacement = false, (cR - 1) * samplesFactor).sorted
    val sSamples = S.takeSample(withReplacement = false, (cS - 1) * samplesFactor).sorted

    // Compute estimated k-quantiles separators
    val rQuantilesSeparators = getQuantiles(rSamples, cR - 1)
    val sQuantilesSeparators = getQuantiles(sSamples, cS - 1)

    // Get the complete relations, sort them and get the true quantiles
    // val sortedR = R.collect.sorted
    // val sortedS = S.collect.sorted
    // val rQuantilesSeparators = getQuantiles(sortedR, cR - 1)
    // val sQuantilesSeparators = getQuantiles(sortedS, cS - 1)

    println(s"R estimated quantiles: ${rQuantilesSeparators.mkString(", ")}")
    println(s"S estimated quantiles: ${sQuantilesSeparators.mkString(", ")}")

    // Compute which partitions to ignore, due to specific conditions
    val partitionsToPrune = getPartitionsToPrune(rQuantilesSeparators, sQuantilesSeparators, condition).toSet

    println(s"Partitions to prune: ${partitionsToPrune.mkString(", ")}")

    // Map each R-tuple and S-tuple to their respective partitions
    val mapR: RDD[(Int, (Int, String))] = mapRelation(R, rQuantilesSeparators, "R", cS)
    val mapS: RDD[(Int, (Int, String))] = mapRelation(S, sQuantilesSeparators, "S", cS)

    // Define a lambda to filter partitions to prune
    val isToPrune: ((Int, (Int, String))) => Boolean = {
      case (partition, _) => !partitionsToPrune.contains(partition)
    }

    // Create unique RDD for R and S, and partition, removing those to prune
    val M: RDD[(Int, (Int, String))] =
      mapR
        .union(mapS)
        .partitionBy(new BucketPartitioner(partitions))
        .filter(isToPrune)

    // Map each partition and reduce
    val joined: RDD[(Int, Int)] = M.mapPartitions(t => reducePartition(t, condition))

    joined
  }

  /** Gets which partitions are not necessary to be reduced, since the join condition is never satisfied for them.
   *
   * @param quantilesR the k-quantiles delimiters for R
   * @param quantilesS the k-quantiles delimiters for S
   * @param condition  the join condition
   * @return the indices of the partitions that do not need to be reduced and can be pruned.
   */
  def getPartitionsToPrune(quantilesR: Array[Int], quantilesS: Array[Int], condition: String): Array[Int] = {
    // Create an array of tuple with the lower bounds of the one which must be greater, and the upperbounds of the
    // one which must be lower. -Int.MaxValue is prepended to the lower one to be the lowest value and take always the
    // 0th bucket, the opposite happens with the higher one. Since we don't know what's the minimum value of the
    // relation that must be larger, nor the maximum value of the relation that must be smaller.
    val upperLowerBounds = condition match {
      case "<" => (Int.MinValue +: quantilesR).flatMap(r => (quantilesS :+ Int.MaxValue).map(s => (r, s)))
      case ">" => (quantilesR :+ Int.MaxValue).flatMap(r => (Int.MinValue +: quantilesS).map(s => (r, s)))
    }

    def canBePruned(r: Int, s: Int): Boolean = condition match {
      // If R < S, we can ignore the squares where the lowest number of R is larger than the largest number of S
      case "<" => r >= s
      // The opposite
      case _ => r <= s
    }

    // Remove indices that satisfy the condition, and return the indices only
    upperLowerBounds
      .zipWithIndex
      .filter { case ((r, s), _) => canBePruned(r, s) }
      .map { case ((_, _), i) => i }
  }

  /** Maps a relation to an RDD of tuples containing the id of the partition in which each tuple must be reduced.
   *
   * @param relation            the relation to be mapped.
   * @param quantilesSeparators the separators of the k-quantiles.
   * @param relationString      the string containing the relation being mapped, must be on of `"R"` or `"S"`.
   * @param cS                  the number of horizontal buckets.
   * @return the mapped relation.
   */
  private def mapRelation(relation: RDD[Int], quantilesSeparators: Array[Int], relationString: String, cS: Int) = {
    relation
      // Get the estimated quantile bucket of each tuple
      .map(value => (value, getBucket(value, quantilesSeparators)))
      // Get the regions crossed by each bucket
      .map { case (value, bucket) => (value, getRegions(bucket, cS, relationString)) }
      // Unpack all the regions and create different tuples each
      .flatMap { case (value, regions) => regions.map((value, _)) }
      // Add the relation name, to be used in the reduction
      .map { case (value, region) => (region, (value, relationString)) }
  }

  /** Get the k-quantile delimiters given a sorted array and k
   *
   * @param sortedArray the array in which the quantiles need to be found
   * @param kMinus1     the number of delimiters (i.e. k-1, where k is the number of wanted quantiles)
   * @return an array of size `kMinus1` containing the k-quantiles separators.
   */
  private def getQuantiles(sortedArray: Array[Int], kMinus1: Int): Array[Int] = {
    // Get the span of each quantile
    val quantileSpan = sortedArray.length / (kMinus1 + 1) + 1
    // Get the index of each quantile
    val indices = (1 to kMinus1).map(_ * quantileSpan - 1)
    // Get the quantiles
    indices.map(sortedArray).toArray
  }

  /** Searches for the lower bound of the bucket to which `value` belongs. It performs a binary search.
   *
   * @param value              the value to be searched.
   * @param bucketsLowerBounds the array containing the lower bounds to each bucket.
   * @return the lower bound of the bucket in which `value` must be added.
   */
  @scala.annotation.tailrec
  private def findBucketLowerBound(value: Int, bucketsLowerBounds: Array[Int]): Int = {
    if (bucketsLowerBounds.length == 1) {
      // Return the last remaining bucket
      bucketsLowerBounds(0)
    } else {
      bucketsLowerBounds(bucketsLowerBounds.length / 2) match {
        // If matches the quantile, then it is the quantile lower bound, so return it
        case middleQuantile if middleQuantile == value => middleQuantile
        // If it is the value is larger than the middle quantile, then search in the right half
        case middleQuantile if value > middleQuantile =>
          findBucketLowerBound(value, bucketsLowerBounds.takeRight(bucketsLowerBounds.length / 2))
        // Else search in the left half
        case middleQuantile if value < middleQuantile =>
          findBucketLowerBound(value, bucketsLowerBounds.take(bucketsLowerBounds.length / 2))
      }
    }
  }

  /** Gets the bucket to which `value` belongs.
   *
   * @param value               the value to be assigned to a bucket.
   * @param quantilesSeparators the k-quantiles separators.
   * @return the bucket to which `value` belong.
   */
  private def getBucket(value: Int, quantilesSeparators: Array[Int]): Int = {
    // Prepend MinValue to the k-quantiles separators to have a value assigned as a lower bound to the 0-th bucket
    val bucketsLowerBounds = Int.MinValue +: quantilesSeparators

    // Group the lower bounds which are equal
    val groupedLowerBounds =
      bucketsLowerBounds
        .zipWithIndex // Assign an index to each bucket
        .groupBy(_._1) // Group by the quantile lower bound
        .map { case (quantile, tuples) => (quantile, tuples.map(_._2)) } // Keep only the indices

    // Get the lower bound of the quantile to which the value belongs
    val quantile = findBucketLowerBound(value, groupedLowerBounds.keys.toArray.sorted)

    // Get the corresponding bucket indices
    val quantileBuckets = groupedLowerBounds(quantile)

    // Randomly get one index to evenly distribute the load
    val rnd = new scala.util.Random
    val bucket = quantileBuckets(rnd.nextInt(quantileBuckets.length))

    bucket
  }

  private def getRegions(bucket: Int, cS: Int, relation: String): IndexedSeq[Int] = {
    relation match {
      case "R" => bucket * cS until (bucket * cS + cS)
      case "S" => bucket until partitions by cS
    }
  }

  private def reducePartition(tuples: Iterator[(Int, (Int, String))], condition: String): Iterator[(Int, Int)] = {
    val tuplesSeq = tuples.toIndexedSeq

    if (tuplesSeq.isEmpty) {
      return Iterator[(Int, Int)]()
    }

    val tupTot = tuplesSeq.partition(t => t._2._2 == "R")

    val partitionIndex = tuplesSeq(0)._1

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

}
