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
    val rSamples = R.takeSample(withReplacement = false, (cR - 1) * samplesFactor, seed = 42).sorted
    val sSamples = S.takeSample(withReplacement = false, (cS - 1) * samplesFactor, seed = 0).sorted

    // Compute estimated k-quantiles separators
    val rQuantilesSeparators = getQuantiles(rSamples, cR - 1)
    val sQuantilesSeparators = getQuantiles(sSamples, cS - 1)

    // Set to true if information about true k-quantiles separators are needed
    val verbose = false
    if (verbose) {
      // Get the complete relations, sort them and get the true quantiles
      val sortedR = R.collect.sorted
      val sortedS = S.collect.sorted
      val rQuantilesSeparators = getQuantiles(sortedR, cR - 1)
      val sQuantilesSeparators = getQuantiles(sortedS, cS - 1)
      logger.info(s"R true quantiles: ${rQuantilesSeparators.mkString(", ")}")
      logger.info(s"S true quantiles: ${sQuantilesSeparators.mkString(", ")}")
    }


    logger.info(s"R estimated quantiles: ${rQuantilesSeparators.mkString(", ")}")
    logger.info(s"S estimated quantiles: ${sQuantilesSeparators.mkString(", ")}")

    // Compute which partitions to ignore, due to specific conditions
    val partitionsToPrune = getPartitionsToPrune(rQuantilesSeparators, sQuantilesSeparators, condition).toSet

    if (partitionsToPrune.nonEmpty) {
      logger.info(s"Partitions to prune: ${partitionsToPrune.mkString(", ")}")
    } else {
      logger.info("There are no partitions to prune")
    }


    // Map each R-tuple and S-tuple to their respective partitions
    val mapR: RDD[(Int, (Int, String))] = mapRelation(R, rQuantilesSeparators, "R", cS, cR)
    val mapS: RDD[(Int, (Int, String))] = mapRelation(S, sQuantilesSeparators, "S", cS, cR)

    // Define a lambda to filter partitions to prune
    val isToPrune: ((Int, (Int, String))) => Boolean = {
      case (partition, _) => !partitionsToPrune.contains(partition)
    }

    val partitionsToKeep = (0 until Math.min(partitions, cR*cS)).toSet.diff(partitionsToPrune).toIndexedSeq.sorted
    val newPartitionsMap = partitionsToKeep.zipWithIndex.toMap

    // Create unique RDD for R and S, and partition, removing those to prune
    val M: RDD[(Int, (Int, String))] =
      mapR
        .union(mapS)
        .filter(isToPrune)
        .map { case (partition, tuple) => (newPartitionsMap(partition), tuple) }
        .partitionBy(new BucketPartitioner(partitionsToKeep.length))

    //Map each partition and reduce
    val joined: RDD[(Int, Int)] = M.mapPartitions(t => reducePartition(t, condition, verbose = false))

    joined
  }

  /** Computes the cartesian product between two arrays.
   *
   * @param r the right array.
   * @param s the left array.
   * @return the computed cross product
   */
  def cartesianProduct(r: IndexedSeq[Int], s: IndexedSeq[Int]): IndexedSeq[(Int, Int)] = r.flatMap(r => s.map(s => (r, s)))

  /** Gets which partitions are not necessary to be reduced, since the join condition is never satisfied for them.
   *
   * @param quantilesR the k-quantiles delimiters for R
   * @param quantilesS the k-quantiles delimiters for S
   * @param condition  the join condition
   * @return the indices of the partitions that do not need to be reduced and can be pruned.
   */
  def getPartitionsToPrune(quantilesR: IndexedSeq[Int], quantilesS: IndexedSeq[Int], condition: String): IndexedSeq[Int] = {
    // Create an array of tuple with the lower bounds of the one which must be greater, and the upperbounds of the
    // one which must be lower. -Int.MaxValue is prepended to the lower one to be the lowest value and take always the
    // 0th bucket, the opposite happens with the higher one. Since we don't know what's the minimum value of the
    // relation that must be larger, nor the maximum value of the relation that must be smaller.
    val upperLowerBounds = condition match {
      case "<" => cartesianProduct(Int.MinValue +: quantilesR, quantilesS :+ Int.MaxValue)
      case ">" => cartesianProduct(quantilesR :+ Int.MaxValue, Int.MinValue +: quantilesS)
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
   * @param cR                  the number of vertical buckets
   * @return the mapped relation.
   */
  private def mapRelation(relation: RDD[Int], quantilesSeparators: IndexedSeq[Int], relationString: String, cS: Int, cR: Int) = {
    val mappedRelation = relation
      // Get the estimated quantile bucket of each tuple
      .map(value => (value, getBucket(value, quantilesSeparators)))
      // Get the regions crossed by each bucket
      .map { case (value, bucket) => (value, getRegions(bucket, cS, cR, relationString)) }
      // Unpack all the regions and create different tuples each
      .flatMap { case (value, regions) => regions.map((value, _)) }
      // Add the relation name, to be used in the reduction
      .map { case (value, region) => (region, (value, relationString)) }

    mappedRelation
  }

  /** Get the k-quantile delimiters given a sorted array and k
   *
   * @param sortedSeq the array in which the quantiles need to be found
   * @param kMinus1   the number of delimiters (i.e. k-1, where k is the number of wanted quantiles)
   * @return an array of size `kMinus1` containing the k-quantiles separators.
   */
  private def getQuantiles(sortedSeq: IndexedSeq[Int], kMinus1: Int): IndexedSeq[Int] = {
    // Get the span of each quantile
    val quantileSpan = sortedSeq.length.floatValue() / (kMinus1 + 1)
    // Get the index of each quantile
    val indices = (1 to kMinus1).map(_ * quantileSpan).map(_.toInt)
    // Get the quantiles
    indices.map(sortedSeq)
  }

  /** Searches for the lower bound of the bucket to which `value` belongs. It performs a binary search.
   *
   * @param value              the value to be searched.
   * @param bucketsLowerBounds the array containing the lower bounds to each bucket.
   * @return the lower bound of the bucket in which `value` must be added.
   */
  @scala.annotation.tailrec
  private def findBucketLowerBound(value: Int, bucketsLowerBounds: IndexedSeq[Int]): Int = {
    // Base case: if two elements are in the array, the lower bound must be in these two
    if (bucketsLowerBounds.length == 2) {
      if (value >= bucketsLowerBounds(1))
        bucketsLowerBounds(1)
      else
        bucketsLowerBounds(0)
    }
    else {
      bucketsLowerBounds(bucketsLowerBounds.length / 2) match {
        // If matches the quantile, then it is the quantile lower bound, so return it
        case middleQuantile if middleQuantile == value => middleQuantile
        // If it is the value is larger than the middle quantile, then search in the right half
        case middleQuantile if value > middleQuantile =>
          findBucketLowerBound(value, bucketsLowerBounds.takeRight(bucketsLowerBounds.length / 2 + 1))
        // Else search in the left half
        case middleQuantile if value < middleQuantile =>
          findBucketLowerBound(value, bucketsLowerBounds.take(bucketsLowerBounds.length / 2 + 1))
      }
    }
  }

  /** Gets the bucket to which `value` belongs.
   *
   * @param value               the value to be assigned to a bucket.
   * @param quantilesSeparators the k-quantiles separators.
   * @return the bucket to which `value` belong.
   */
  private def getBucket(value: Int, quantilesSeparators: IndexedSeq[Int]): Int = {
    // Prepend MinValue to the k-quantiles separators to have a value assigned as a lower bound to the 0-th bucket
    val bucketsLowerBounds = Int.MinValue +: quantilesSeparators

    // Group the lower bounds which are equal
    val groupedLowerBounds =
      bucketsLowerBounds
        .zipWithIndex // Assign an index to each bucket
        .groupBy(_._1) // Group by the quantile lower bound
        .map { case (quantile, tuples) => (quantile, tuples.map(_._2)) } // Keep only the indices

    // Get the lower bound of the quantile to which the value belongs
    val quantile = findBucketLowerBound(value, groupedLowerBounds.keys.toIndexedSeq.sorted)

    // Get the corresponding bucket indices
    val quantileBuckets = groupedLowerBounds(quantile)

    // Randomly get one index to evenly distribute the load
    val rnd = new scala.util.Random(42)
    val bucket = quantileBuckets(rnd.nextInt(quantileBuckets.length))

    bucket
  }

  /** Gets the regions intersected by the given bucket.
   *
   * @param bucket   the index of the bucket.
   * @param cS       the number of horizontal buckets.
   * @param cR                  the number of vertical buckets
   * @param relation the relation of the bucket. Must be one of `"R"` or `"S"`
   * @return the array with the intersected regions.
   */
  private def getRegions(bucket: Int, cS: Int, cR: Int, relation: String): IndexedSeq[Int] = {
    relation match {
      case "R" => bucket * cS until (bucket * cS + cS)
      case "S" => bucket until Math.min(partitions, cR*cS) by cS
    }
  }

  /** Reduces the partition computing the given join on the input tuples.
   *
   * @param tuples    the tuples on which the join has to be computed.
   * @param condition the join condition.
   * @param verbose   whether debugging info should be printed. Slows down the execution, to be used only for debugging
   *                  purposes.
   * @return the iterator containing the joined tuples.
   * @throws IllegalArgumentException if the given condition is not one of ">" or "<".
   */
  private def reducePartition(tuples: Iterator[(Int, (Int, String))], condition: String, verbose: Boolean = true): Iterator[(Int, Int)] = {
    val tuplesSeq = tuples.toIndexedSeq

    // Split the tuples between those in R and those in S
    val tupTot = tuplesSeq.partition(t => t._2._2 == "R")

    // Get the value to be joined from each relation
    val getValue: ((Int, (Int, String))) => Int = {
      case (_, (value, _)) => value
    }
    val tupR = tupTot._1.map(getValue)
    val tupS = tupTot._2.map(getValue)

    // Remove the values of the relation that must be larger which are smaller of the smallest
    // value of the other relation
    val rMin = tupR.min
    val sMin = tupS.min
    val (filteredS, filteredR) = condition match {
      case "<" => (tupS.filter(_ > rMin), tupR)
      case ">" => (tupS, tupR.filter(_ > sMin))
    }

    // Get the minimum and maximum values of R and S
    val (filteredRMax, filteredRMin) = (filteredR.max, filteredR.min)
    val (filteredSMax, filteredSMin) = (filteredS.max, filteredS.min)

    // Take the values that for sure will be included in the final result, as they always satisfy the join condition.
    // In particular, in case of "<" we can take tha values in R that are smaller than the smallest value of S, and
    // for S we can take the values that are larger than the largest values of R.
    val ((toTakeR, toCompareR), (toTakeS, toCompareS)) = condition match {
      case "<" => (filteredR.partition(_ < filteredSMin), filteredS.partition(_ > filteredRMax))
      case ">" => (filteredR.partition(_ > filteredSMax), filteredS.partition(_ < filteredRMin))
    }

    // Compute the cartesian products that do not need to be filtered.
    // On the right, we compute toCompareR X toTakeS in order to avoid duplicates.
    val ready = cartesianProduct(toTakeR, filteredS) ++: cartesianProduct(toCompareR, toTakeS)

    // Compute the cartesian product among the relations that need to be compared
    val cartesian = cartesianProduct(toCompareR, toCompareS)

    // Filter the cartesian product according to the join condition.
    val crossResult = condition match {
      case "<" => cartesian.filter(c => c._1 < c._2)
      case ">" => cartesian.filter(c => c._1 > c._2)
      case _ => throw new IllegalArgumentException("Wrong condition selected")
    }

    // Get the final result
    val result = crossResult ++: ready

    if (verbose) {
      val partitionIndex = tuplesSeq.head._1
      val completeCross = tupR.flatMap(x => tupS.map(y => (x, y)))
      logger.info(s"Saved comparisons: ${completeCross.size - cartesian.size}")
      logger.info(s"Input reducer $partitionIndex: ${tupTot._1.size + tupTot._2.size}")
      logger.info(s"Output reducer $partitionIndex: ${crossResult.size}")
    }

    result.toIterator
  }

  /** Partitions an RDD based on the given bucket,
   *
   * @param numPartitions the number of available partitions.
   */
  class BucketPartitioner(override val numPartitions: Int) extends Partitioner {
    def getPartition(key: Any): Int = key.asInstanceOf[Int]
  }

}
