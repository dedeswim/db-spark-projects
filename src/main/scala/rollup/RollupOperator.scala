package rollup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class RollupOperator() extends Serializable {
  /*
 * This method gets as input one dataset, the indexes of the grouping attributes of the rollup (ROLLUP clause)
 * the index of the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = List[Any], value = Double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */

  def rollup(dataset: RDD[Row], groupingAttributeIndexes: List[Int], aggAttributeIndex: Int, agg: String): RDD[(List[Any], Double)] = {

    // Compute the aggregates recursively
    rollup_indices(dataset, groupingAttributeIndexes, aggAttributeIndex, agg)
      .map(t => (t._1, t._2)) // Remove the aggregate counters

  }

  def rollup_naive(dataset: RDD[Row], groupingAttributeIndexes: List[Int], aggAttributeIndex: Int, agg: String): RDD[(List[Any], Double)] = {

    def rollup_indices(indices: List[Int]): RDD[(List[Any], Double)] = {

      // Group the needed values
      val grouped: RDD[(List[Any], Iterable[Row])] = indices match {
        case Nil => dataset
          .groupBy(row => List[Any]())
        case _ => dataset
          .groupBy(row => indices.map(i => row(i)))
      }

      // Get the value to be aggregated and cast it to `Double`
      val groupedCast: RDD[(List[Any], Iterable[Double])] = grouped
        .map(t => (t._1, t._2.map(r => r.get(aggAttributeIndex))))
        .map(t => (t._1, t._2.map(castField)))

      // Compute the aggregate
      val aggregated: RDD[(List[Any], Double)] = agg match {
        case "SUM" => groupedCast.map(t => (t._1, t._2.sum))
        case "MIN" => groupedCast.map(t => (t._1, t._2.min))
        case "MAX" => groupedCast.map(t => (t._1, t._2.max))
        case "COUNT" => groupedCast.map(t => (t._1, t._2.size))
        case "AVG" => groupedCast.map(t => (t._1, t._2.sum / t._2.size))
        case _ => throw new IllegalArgumentException(s"$agg is not a valid aggregate.")
      }

      aggregated
    }

    val rollups = (groupingAttributeIndexes.indices :+ groupingAttributeIndexes.size)
      // Get a list of lists containing the possible indices
      .map(i => groupingAttributeIndexes.take(i))
      // Reverse to start from the longest list (to make the more granular aggregates first
      .reverse
      // Compute the aggregates for every list of indices
      .map(rollup_indices)
      // Unite all the results
      .reduce((a, b) => a.union(b))

    rollups

  }

  /** Casts the field to be aggregated to a Double.
   *
   * @return the cast number.
   * @throws IllegalArgumentException if the value is not numeric.
   */
  private def castField: Any => Double = {
    case f: Int => f.toDouble
    case f: Double => f
    case f: Long => f.toDouble
    case f: Float => f.toDouble
    case _ => throw new IllegalArgumentException("The field is not numeric.")
  }

  /** Computes the first, most granular, aggregate, starting from an `RDD[Row]`.
   *
   * If there is only one rollup to be computed, the results are returned immediately. Otherwise, the overloaded method
   * computing the further aggregations is called.
   *
   * @param dataset                  the dataset on which the aggregate must be computed.
   * @param groupingAttributeIndexes the indices to be used to group the dataset.
   * @param aggAttributeIndex        the index of the field to be used for the aggregation.
   * @param agg                      the aggregation to be performed.
   * @return the aggregated dataset.
   */
  private def rollup_indices(dataset: RDD[Row], groupingAttributeIndexes: List[Int], aggAttributeIndex: Int, agg: String): RDD[(List[Any], Double, Int)] = {

    val grouped: RDD[(List[Any], Iterable[(Double, Int)])] =
      dataset
        // Group by the needed indices
        .groupBy(row => groupingAttributeIndexes.map(i => row(i)))
        // Get the attribute to aggregate
        .map(t => (t._1, t._2.map(s => s(aggAttributeIndex))))
        // Transform the attribute to Double and couple it with 1,
        // since at the first iteration every element will have weight 1
        .map(t => (t._1, t._2.map(s => (castField(s), 1))))

    // Aggregate grouped values
    val aggregated: RDD[(List[Any], Double, Int)] = computeAggregate(agg, grouped)

    // If there are no more indices left, return the final result
    // Otherwise, recurse to the next aggregation level, passing the current results as accumulator
    groupingAttributeIndexes match {
      case Nil => aggregated
      case _ => rollup_indices(groupingAttributeIndexes.indices.dropRight(1).toList, aggregated, aggregated, agg)
    }

  }

  /** Overloaded helper method that computes the other aggregates, starting from an `RDD[(List[Any], Double, Int)]`.
   *
   * This method is tail recursive, having an accumulator `datasetAccumulator`, to which the results of the current
   * recursion are united. The result is passed to the next recursion step, if the rollup is not finished, otherwise
   * it is simply returned.
   *
   * @param groupingIndices    the indices to be used to group the dataset.
   * @param lastAggregated     the RDD containing the aggregated dataset from the above recursion. It is used to cheaply
   *                           compute the aggregated in this recursion.
   * @param datasetAccumulator the RDD containing all the aggregation computed so far.
   * @param agg                the aggregation to be performed.
   * @return the aggregated dataset united with the dataset coming from previous recursions.
   * */
  @scala.annotation.tailrec
  private def rollup_indices(groupingIndices: List[Int], lastAggregated: RDD[(List[Any], Double, Int)], datasetAccumulator: RDD[(List[Any], Double, Int)], agg: String): RDD[(List[Any], Double, Int)] = {

    val grouped: RDD[(List[Any], Iterable[(Double, Int)])] =
      lastAggregated
        .groupBy(t => groupingIndices.map(i => t._1(i))) // Group by the needed indices
        .map(t => (t._1, t._2.map(s => (s._2, s._3)))) // Couple the numbers to aggregate with their weight.

    // Aggregate grouped values
    val aggregated: RDD[(List[Any], Double, Int)] = computeAggregate(agg, grouped)

    // If there are no more indices left, return the final result
    // Otherwise, recurse to the next aggregation level, passing the current results as accumulator
    groupingIndices match {
      case Nil => aggregated.union(datasetAccumulator)
      case _ => rollup_indices(groupingIndices.dropRight(1), aggregated, aggregated.union(datasetAccumulator), agg)
    }

  }

  /** Computes an aggregate on an `RDD[(List[Any], Iterable[(Double, Int)])]`, given aggregate `agg`
   *
   * @param agg     the aggregate to be computed, on of "SUM", "MIN", "MAX", "COUNT", "AVG".
   * @param grouped the RDD to be aggregated.
   * @return the aggregated RDD.
   * @throws IllegalArgumentException if the aggregate is not one of those mentioned above.
   **/
  private def computeAggregate(agg: String, grouped: RDD[(List[Any], Iterable[(Double, Int)])]): RDD[(List[Any], Double, Int)] = {

    /** Computes the weighted  of a tuple containing the group key and a list of numbers and their weights.
     *
     * @param t the Tuple on which compute the average.
     * @return a `Tuple3` containing the group key, the average and the weight for the group in the next recursion.
     **/
    def avg(t: (List[Any], Iterable[(Double, Int)])): (List[Any], Double, Int) = {

      val weightedSum = t._2.map(s => s._1 * s._2).sum
      val denominator = t._2.map(_._2).sum
      val average = weightedSum / denominator

      (t._1, average, t._2.map(_._2).sum)
    }


    agg match {
      case "SUM" => grouped.map(t => (t._1, t._2.map(_._1).sum, t._2.size))
      case "MIN" => grouped.map(t => (t._1, t._2.map(_._1).min, t._2.size))
      case "MAX" => grouped.map(t => (t._1, t._2.map(_._1).max, t._2.size))
      case "COUNT" => grouped.map(t => (t._1, t._2.map(_._2).sum, t._2.map(_._2).sum))
      case "AVG" => grouped.map(avg)
      case _ => throw new IllegalArgumentException(s"$agg is not a valid aggregate.")
    }
  }
}
