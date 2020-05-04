package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class ExactNN(sqlContext: SQLContext, data: RDD[(String, List[String])], threshold : Double) extends Construction with Serializable {

  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    /*
    * This method performs a near-neighbor computation for the data points in rdd against the data points in data.
    * Near-neighbors are defined as the points with a Jaccard similarity that exceeds the threshold
    * data: data points in (movie_name, [keyword_list]) format to compare against
    * rdd: data points in (movie_name, [keyword_list]) format that represent the queries
    * threshold: the similarity threshold that defines near-neighbors
    * return near-neighbors in (movie_name, [nn_movie_names]) as an RDD[(String, Set[String])]
    * */

    rdd.cartesian(data)
      .groupBy(_._1)
      .map(t => (t._1._1, t._2.filter(i => jaccard(i._1._2, i._2._2) >= threshold)))
      .map(t => (t._1, t._2.map(i => i._2._1).toSet))
  }

  def jaccard(query: List[String], data: List[String]): Double = {
    val querySet = query.toSet
    val dataSet = data.toSet
    val jaccard = querySet.intersect(dataSet).size.doubleValue()/querySet.union(dataSet).size.doubleValue()
    jaccard
  }
}
