package rollup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class RollupOperator() extends Serializable {

  def rollup_indices(dataset: RDD[Row], groupingAttributeIndexes: List[Int], aggAttributeIndex: Int, agg: String): RDD[(List[Any], Double, Int)] = {

    val grouped: RDD[(List[Any], Iterable[(Double, Int)])] =
      dataset
        .groupBy(row => groupingAttributeIndexes.map(i => row(i))) // Group by the needed indices
        .map(t => (t._1, t._2.map(s => s(aggAttributeIndex)))) // Get the attribute to aggregate
        .map(t => (t._1, t._2.map(s => (castField(s), 1)))) // Transform the attribute to Double and couple with 1

    val aggregated: RDD[(List[Any], Double, Int)] = computeAggregate(agg, grouped)

    groupingAttributeIndexes match {
      case Nil => aggregated
      case _ => rollup_indices(groupingAttributeIndexes.indices.dropRight(1).toList, aggregated, aggregated, agg)
    }

  }

  def rollup(dataset: RDD[Row], groupingAttributeIndexes: List[Int], aggAttributeIndex: Int, agg: String): RDD[(List[Any], Double)] = {

    rollup_indices(dataset, groupingAttributeIndexes, aggAttributeIndex, agg)
      .map(t => (t._1, t._2))

  }

  def rollup_naive(dataset: RDD[Row], groupingAttributeIndexes: List[Int], aggAttributeIndex: Int, agg: String): RDD[(List[Any], Double)] = {

    def rollup_indices(indices: List[Int]): RDD[(List[Any], Double)] = {

      val grouped: RDD[(List[Any], Iterable[Row])] = indices match {
        case Nil => dataset
          .groupBy(row => List[Any]())
        case _ => dataset
          .groupBy(row => indices.map(i => row(i)))
      }

      val groupedCast: RDD[(List[Any], Iterable[Double])] = grouped
        .map(t => (t._1, t._2.map(r => r.get(aggAttributeIndex))))
        .map(t => (t._1, t._2.map(castField)))

      val aggregated: RDD[(List[Any], Double)] = agg match {
        case "SUM" => groupedCast.map(t => (t._1, t._2.sum))
        case "MIN" => groupedCast.map(t => (t._1, t._2.min))
        case "MAX" => groupedCast.map(t => (t._1, t._2.max))
        case "COUNT" => groupedCast.map(t => (t._1, t._2.size))
        case "AVG" => groupedCast.map(t => (t._1, t._2.sum / t._2.size))
      }

      aggregated
    }

    val rollups = (groupingAttributeIndexes.indices :+ groupingAttributeIndexes.size)
      .map(i => groupingAttributeIndexes.take(i))
      .reverse
      .map(rollup_indices)
      .reduce((a, b) => a.union(b))

    rollups

  }

  private def castField: Any => Double = {
    case f: Int => f.toDouble
    case f: Double => f
    case f: Long => f.toDouble
    case f: Float => f.toDouble
    case _ => throw new IllegalArgumentException("The field is not numeric.")
  }

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

  @scala.annotation.tailrec
  private def rollup_indices(indices: List[Int], last_aggregated: RDD[(List[Any], Double, Int)], dataset: RDD[(List[Any], Double, Int)], agg: String): RDD[(List[Any], Double, Int)] = {

    val grouped: RDD[(List[Any], Iterable[(Double, Int)])] =
      last_aggregated
        .groupBy(t => indices.map(i => t._1(i))) // Group by the needed indices
        .map(t => (t._1, t._2.map(s => (s._2, s._3)))) // Save the couple of aggregated elements

    val aggregated: RDD[(List[Any], Double, Int)] = computeAggregate(agg, grouped)

    indices match {
      case Nil => aggregated.union(dataset)
      case _ => rollup_indices(indices.dropRight(1), aggregated, aggregated.union(dataset), agg)
    }

  }

  private def computeAggregate(agg: String, grouped: RDD[(List[Any], Iterable[(Double, Int)])]): RDD[(List[Any], Double, Int)] = {

    def avg(t: (List[Any], Iterable[(Double, Int)])): (List[Any], Double, Int) = {

      val weightedSum = t._2.map(s => s._1 * s._2).sum
      val denominator = t._2.map(_._2).sum
      val average = weightedSum / denominator

      (t._1, average, t._2.size)
    }


    agg match {
      case "SUM" => grouped.map(t => (t._1, t._2.map(_._1).sum, t._2.size))
      case "MIN" => grouped.map(t => (t._1, t._2.map(_._1).min, t._2.size))
      case "MAX" => grouped.map(t => (t._1, t._2.map(_._1).max, t._2.size))
      case "COUNT" => grouped.map(t => (t._1, t._2.map(_._2).sum, t._2.map(_._2).sum))
      case "AVG" => grouped.map(avg)
    }
  }
}
