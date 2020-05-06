package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class CompositeConstruction(sqlContext: SQLContext,
                            data: RDD[(String, List[String])],
                            r: Int, b: Int,
                            base: (SQLContext, RDD[(String, List[String])]) => Construction,
                            outer: List[Construction] => Construction,
                            inner: List[Construction] => Construction
                           ) extends Construction {

  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    val innerEvals = List.fill(b)(inner(List.fill(r)(base(sqlContext, data))))

    outer(innerEvals).eval(rdd)
  }


}

object CompositeConstruction {
  def andOrBase(sqlContext: SQLContext, data: RDD[(String, List[String])], r: Int, b: Int): CompositeConstruction = {
    new CompositeConstruction(
      sqlContext,
      data,
      r, b,
      (c, d) => new BaseConstruction(c, d),
      l => new ANDConstruction(l),
      l => new ORConstruction(l)
    )
  }

  def orAndBase(sqlContext: SQLContext, data: RDD[(String, List[String])], r: Int, b: Int): CompositeConstruction = {
    new CompositeConstruction(
      sqlContext,
      data,
      r, b,
      (c, d) => new BaseConstruction(c, d),
      l => new ORConstruction(l),
      l => new ANDConstruction(l)
    )
  }

  def andOrBroadcast(sqlContext: SQLContext, data: RDD[(String, List[String])], r: Int, b: Int): CompositeConstruction = {
    new CompositeConstruction(
      sqlContext,
      data,
      r, b,
      (c, d) => new BaseConstructionBroadcast(c, d),
      l => new ANDConstruction(l),
      l => new ORConstruction(l)
    )
  }

  def orAndBroadcast(sqlContext: SQLContext, data: RDD[(String, List[String])], r: Int, b: Int): CompositeConstruction = {
    new CompositeConstruction(
      sqlContext,
      data,
      r, b,
      (c, d) => new BaseConstructionBroadcast(c, d),
      l => new ORConstruction(l),
      l => new ANDConstruction(l)
    )
  }
}
