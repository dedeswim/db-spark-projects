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

    val (minR, maxR) = (R.min(), R.max())
    val (minS, maxS) = (S.min(), S.max())

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
    val partitionsToPrune = getPartitionsToPrune(rQuantilesSeparators, sQuantilesSeparators, condition, minR, maxR, minS, maxS).toSet

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

    val partitionsToKeep = (0 until Math.min(partitions, cR * cS)).toSet.diff(partitionsToPrune).toIndexedSeq.sorted
    val newPartitionsMap = partitionsToKeep.zipWithIndex.toMap

    // Create unique RDD for R and S, and partition, removing those to prune
    val M: RDD[(Int, (Int, String))] =
      mapR
        .union(mapS)
        .filter(isToPrune)
        .map { case (partition, tuple) => (newPartitionsMap(partition), tuple) }
        .partitionBy(new BucketPartitioner(partitionsToKeep.length))

    // Map each partition and reduce
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
  def getPartitionsToPrune(
                            quantilesR: IndexedSeq[Int],
                            quantilesS: IndexedSeq[Int], condition: String,
                            minR: Int, maxR: Int, minS: Int, maxS: Int
                          ): IndexedSeq[Int] = {

    // Create an array of tuple with the lower bounds of the one which must be greater, and the upperbounds of the
    // one which must be lower. -Int.MaxValue is prepended to the lower one to be the lowest value and take always the
    // 0th bucket, the opposite happens with the higher one. Since we don't know what's the minimum value of the
    // relation that must be larger, nor the maximum value of the relation that must be smaller.

    def indexedCartesianProduct(r: IndexedSeq[(Int, Int)], s: IndexedSeq[Int]): IndexedSeq[(Int, (Int, Int))] =
      r.flatMap { case (r, row) => s.map(s => (row, (r, s))) }

    val upperLowerBoundsRows = condition match {
      case "<" =>
        indexedCartesianProduct((minR +: quantilesR).zipWithIndex, quantilesS :+ maxS)
      case ">" =>
        indexedCartesianProduct((quantilesR :+ maxR).zipWithIndex, minS +: quantilesS)
    }

    val upperLowerBoundsRowMap = upperLowerBoundsRows.groupBy(identity).mapValues(_.length)

    val upperLowerBoundsCols = condition match {
      case "<" =>
        indexedCartesianProduct((quantilesS :+ maxS).zipWithIndex, minR +: quantilesR)
      case ">" =>
        indexedCartesianProduct((minS +: quantilesS).zipWithIndex, quantilesR :+ maxR)
    }

    val upperLowerBoundsColMap = upperLowerBoundsCols.groupBy(identity).mapValues(_.length)


    def canBePruned(t: (Int, Int)): Boolean = condition match {
      // If R < S, we can ignore the squares where the lowest number of R is larger than the largest number of S
      case "<" => t._1 >= t._2
      // The opposite
      case ">" => t._1 <= t._2
    }

    def expandValues(key: (Int, (Int, Int)), toPrune: Boolean, values: Int, nextPreviousSValue: Int): IndexedSeq[((Int, (Int, Int)), Boolean)] = {

      // Get first element to prune and see if next need so too
      val first = (key, toPrune)
      val (_, (r, nextS)) = key
      val othersToPrune = condition match {
        case "<" => r >= nextPreviousSValue
        case ">" => r <= nextPreviousSValue || r <= nextS
      }

      val others = IndexedSeq.fill(values - 1)((key, othersToPrune))

      first +: others
    }

    def checkPruneOverCols(key: (Int, (Int, Int)), toPrune: Boolean, values: Int, nextRValue: Int): IndexedSeq[((Int, (Int, Int)), Boolean)] = {

      // If the first value in the col is not prunable, neither the next values will be
      if (!toPrune) {
        return IndexedSeq.fill(values)((key, toPrune))
      }

      // Check if the column need pruning
      val (_, (s, _)) = key
      val needPruning = condition match {
        case "<" => nextRValue >= s
        case ">" => nextRValue <= s
      }

      // The first element in the column must be pruned anyway, so we don't flip it
      val first = (key, toPrune)
      val others = IndexedSeq.fill(values - 1)((key, needPruning))

      first +: others
    }

    // Create Map of (s, predS) over the choices of S
    val uniqueSQuantiles = quantilesS.toSet.toIndexedSeq.sorted
    val sNextPreviousValuesMap = condition match {
      case "<" =>
        uniqueSQuantiles.zip(uniqueSQuantiles.drop(1) :+ maxS).toMap + (maxS -> maxS)
      case ">" =>
        uniqueSQuantiles.zip(minS +: uniqueSQuantiles.dropRight(1)).toMap + (minS -> minS)
    }

    // Create Map of (r, nextR) over the choices of R
    val uniqueRQuantiles = quantilesR.toSet.toIndexedSeq.sorted
    val rPreviousNextValuesMap = condition match {
      case "<" =>
        uniqueRQuantiles.zip(
          uniqueRQuantiles.drop(1) :+ maxR).toMap + (minR -> {
          if (quantilesR.isEmpty) maxR else quantilesR.head
        })
      case ">" =>
        uniqueRQuantiles.zip(uniqueRQuantiles.drop(1) :+ maxR).toMap + (maxR -> maxR)
    }

    // Remove indices that satisfy the condition, and return the indices only
    val prunableTuples =
      upperLowerBoundsRowMap
        .map { case (tuple, repetitions) => (tuple, canBePruned(tuple._2), repetitions) }
        .flatMap { case (key, toPrune, values) => expandValues(key, toPrune, values, sNextPreviousValuesMap(key._2._2)) }

    // Compute the de-prunable tuples over the columns, to be sure the pruning phase didn't prune more than necessary
    val dePrunableTuples =
      upperLowerBoundsColMap
        .map { case (tuple, repetitions) => (tuple, canBePruned(tuple._2._2, tuple._2._1), repetitions) }
        .flatMap { case (key, toPrune, values) => checkPruneOverCols(key, toPrune, values, rPreviousNextValuesMap(key._2._2)) }
        .map { case (key, toPrune) => ((key._1, (key._2._2, key._2._1)), toPrune) }
        .toIndexedSeq
        .sortBy(_._1)
        .zipWithIndex
        .map { case (tuple, index) => ((index % (quantilesR.length + 1), tuple._1._2), tuple._2) }


    // We can prune only if both pruning stages say so, merge the pruning and update pruned reducers
    val completePruning =
      prunableTuples.toIndexedSeq.sortBy(_._1)
        .zip(dePrunableTuples.sortBy(_._1))
        .map({ case (row, col) => (row._1, row._2 && col._2) })


    // Return corresponding reducers
    val prunableIndices =
      completePruning
        .zipWithIndex
        .filter(_._1._2)
        .map(_._2)

    prunableIndices
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

    if (tupR.isEmpty | tupS.isEmpty) {
      return Iterator[(Int, Int)]()
    }

    // Remove the values of the relation that must be larger which are smaller of the smallest
    // value of the other relation
    val rMin = tupR.min
    val sMin = tupS.min
    val (filteredS, filteredR) = condition match {
      case "<" => (tupS.filter(_ > rMin), tupR)
      case ">" => (tupS, tupR.filter(_ > sMin))
    }

    if (filteredR.isEmpty | filteredS.isEmpty) {
      return Iterator[(Int, Int)]()
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
    val cartesianResult = condition match {
      case "<" => cartesian.filter(c => c._1 < c._2)
      case ">" => cartesian.filter(c => c._1 > c._2)
      case _ => throw new IllegalArgumentException("Wrong condition selected")
    }

    // Get the final result
    val result = cartesianResult ++: ready

    if (verbose) {
      val partitionIndex = tuplesSeq.head._1
      val completeCross = tupR.flatMap(x => tupS.map(y => (x, y)))
      logger.info(s"Saved comparisons: ${completeCross.size - cartesian.size}")
      logger.info(s"Input reducer $partitionIndex: ${tupTot._1.size + tupTot._2.size}")
      logger.info(s"Output reducer $partitionIndex: ${result.size}")
    }

    result.toIterator
  }

  /** Gets the regions intersected by the given bucket.
   *
   * @param bucket   the index of the bucket.
   * @param cS       the number of horizontal buckets.
   * @param cR       the number of vertical buckets
   * @param relation the relation of the bucket. Must be one of `"R"` or `"S"`
   * @return the array with the intersected regions.
   */
  def getRegions(bucket: Int, cS: Int, cR: Int, relation: String): IndexedSeq[Int] = {
    relation match {
      case "R" => bucket * cS until Math.min(partitions, (bucket * cS + cS))
      case "S" => bucket until Math.min(partitions, cR * cS) by cS
    }
  }

  /** Gets the bucket to which `value` belongs.
   *
   * @param value               the value to be assigned to a bucket.
   * @param quantilesSeparators the k-quantiles separators.
   * @return the bucket to which `value` belong.
   */
  private def getBucket(value: Int, quantilesSeparators: IndexedSeq[Int]): Int = {

    // There is just one partition working.
    if (quantilesSeparators.isEmpty) {
      return 0
    }

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
    val randomIndex = rnd.nextInt(quantileBuckets.length)
    val bucket = quantileBuckets(randomIndex)

    bucket
  }

  /** Partitions an RDD based on the given bucket,
   *
   * @param numPartitions the number of available partitions.
   */
  class BucketPartitioner(override val numPartitions: Int) extends Partitioner {
    def getPartition(key: Any): Int = key.asInstanceOf[Int]
  }

}
