package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class ORConstruction(children: List[Construction]) extends Construction {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    /*
    * Implement an ORConstruction for one or more LSH constructions (simple or composite)
    * children: LSH constructions to compose
    * rdd: data points in (movie_name, [keyword_list]) format that represent the queries
    * return near-neighbors in (movie_name, [nn_movie_names]) as an RDD[(String, Set[String])]
    * */

    children
      .map(_.eval(rdd))
      .reduce((rddA, rddB) => rddA.union(rddB))
      .reduceByKey((queryPoint, neighbors) => queryPoint.union(neighbors))

  }
}

object ORConstruction {
  def getConstructionBase(b: Int, sqlContext: SQLContext, data: RDD[(String, List[String])]): ORConstruction = {
    val innerConstructions = List.fill(b)(new BaseConstruction(sqlContext, data))
    new ORConstruction(innerConstructions)
  }

  def getConstructionBroadcast(b: Int, sqlContext: SQLContext, data: RDD[(String, List[String])]): ORConstruction = {
    val innerConstructions = List.fill(b)(new BaseConstructionBroadcast(sqlContext, data))
    new ORConstruction(innerConstructions)
  }
}
