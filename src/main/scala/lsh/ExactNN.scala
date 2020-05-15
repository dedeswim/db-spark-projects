package lsh

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, udf}

class ExactNN(sqlContext: SQLContext, data: RDD[(String, List[String])], threshold : Double) extends Construction with Serializable {

  // Define schema for the dataframe
  private def dfSchema(df: String): StructType =
    StructType(
      Seq(
        StructField(name = s"movie_$df", dataType = StringType, nullable = false),
        StructField(name = s"keywords_$df", dataType = ArrayType(StringType), nullable = false)
      )
    )

  // Convert to Row
  private def row(movie: String, keywords: List[String]): Row = Row(movie, keywords)

  // Create DF for data rdd
  private val dataDF = sqlContext.createDataFrame(data.map(t => row(t._1, t._2)), dfSchema("data"))

  // Define jaccard function and register as lambda
  def jaccard(query: Seq[String], data: Seq[String]): Double = {
    val querySet = query.toSet
    val dataSet = data.toSet
    val jaccard = querySet.intersect(dataSet).size.doubleValue()/querySet.union(dataSet).size.doubleValue()
    jaccard
  }
  val jaccardLambda = (query: Seq[String], data: Seq[String]) => jaccard(query, data)

  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    /*
    * This method performs a near-neighbor computation for the data points in rdd against the data points in data.
    * Near-neighbors are defined as the points with a Jaccard similarity that exceeds the threshold
    * data: data points in (movie_name, [keyword_list]) format to compare against
    * rdd: data points in (movie_name, [keyword_list]) format that represent the queries
    * threshold: the similarity threshold that defines near-neighbors
    * return near-neighbors in (movie_name, [nn_movie_names]) as an RDD[(String, Set[String])]
    * */

    // Create DF for rdd (query points)
    val rddDF = sqlContext.createDataFrame(rdd.map(t => row(t._1, t._2)), dfSchema("rdd"))

    // Register jaccard as UDF
    sqlContext.udf.register("jaccard", jaccardLambda)
    val jaccardUDF = udf(jaccardLambda)

    rddDF
      // Cross join
      .crossJoin(dataDF)
      // Compute jaccard for each (query,poss_neighbor) pair
      .select(col("movie_rdd"), col("movie_data"), jaccardUDF(col("keywords_rdd"), col("keywords_data")) as "jaccard")
      // Take only the one above threshold
      .filter(col("jaccard") >= threshold)
      .rdd
      .map(r => (r(0).asInstanceOf[String], r(1).asInstanceOf[String]))
      // Group by movie and collect neighbors as set
      .groupBy(_._1)
      .map{ case (movie, neighbors) => (movie, neighbors.map(_._2).toSet) }

  }
}
