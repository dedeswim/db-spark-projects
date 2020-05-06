package LSHTest

import lsh.Main.{loadRDD, precision, recall}
import lsh.{CompositeConstruction, Construction, ExactNN}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuite

class LSHTest extends AnyFunSuite {
  private val conf =
    new SparkConf()
      .setAppName("app")
      .setMaster("local[*]")

  private val spark = SparkContext.getOrCreate(conf)
  private val sqlContext = new org.apache.spark.sql.SQLContext(spark)
  private val corpusFile = "/lsh-corpus-small.csv"

  def query(queryFile: String,
            composite: (SQLContext, RDD[(String, List[String])], Int, Int) => CompositeConstruction,
            r: Int,
            b: Int,
            requiredPrecision: Double,
            requiredRecall: Double): Unit = {

    val rddCorpus = loadRDD(spark, corpusFile)
    val rddQuery = loadRDD(spark, queryFile)

    val exact: Construction = new ExactNN(sqlContext, rddCorpus, 0.3)
    val lsh: Construction = composite(sqlContext, rddCorpus, r, b)

    val ground = exact.eval(rddQuery)
    val res = lsh.eval(rddQuery)

    val resultRecall = recall(ground, res)
    val resultPrecision = precision(ground, res)
    println(s"Recall: $resultRecall")
    println(s"Precision: $resultPrecision")

    assert(resultRecall > requiredRecall)
    assert(resultPrecision > requiredPrecision)
  }

  test("queryO") {
    val queryFile = "/lsh-query-0.csv"
    val requiredRecall = 0.83
    val requiredPrecision = 0.70
    query(queryFile, CompositeConstruction.andOrBroadcast, 5, 5, requiredPrecision, requiredRecall)
  }

  test("query1") {
    val queryFile = "/lsh-query-1.csv"
    val requiredRecall = 0.7
    val requiredPrecision = 0.98
    query(queryFile, CompositeConstruction.andOrBroadcast, 5, 5, requiredPrecision, requiredRecall)
  }

  test("query2") {
    val queryFile = "/lsh-query-2.csv"
    val requiredRecall = 0.9
    val requiredPrecision = 0.45
    query(queryFile, CompositeConstruction.andOrBroadcast, 5, 5, requiredPrecision, requiredRecall)
  }
}
