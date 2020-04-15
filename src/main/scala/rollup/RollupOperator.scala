package rollup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class RollupOperator() {

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
    //TODO Task 1
    null
  }

  def rollup_naive(dataset: RDD[Row], groupingAttributeIndexes: List[Int], aggAttributeIndex: Int, agg: String): RDD[(List[Any], Double)] = {

    def castField(field: Any): Double = {
      field match {
        case f: Int => f.toDouble
        case f: Double => f
        case f: Long => f.toDouble
        case f: Float => f.toDouble
        case _ => throw new IllegalArgumentException("The field is not numeric.")
      }
    }

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
}
