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
    //TODO naive algorithm for cube computation
    null
  }
}
