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

  test("precisionRecall1") {
    val ground = spark.parallelize(IndexedSeq(
      ("a", Set("b", "c", "d")),
      ("b", Set("b", "c", "d")),
      ("c", Set("b", "c", "d")),
      ("d", Set("b", "c", "d")),
      ("e", Set("b", "c", "d"))
    ))

    val lsh = spark.parallelize(IndexedSeq(
      ("a", Set("b", "c", "d")),
      ("b", Set("b", "c", "d")),
      ("c", Set("b", "c", "d")),
      ("d", Set("b", "c", "d")),
      ("e", Set("b", "c", "d"))
    ))

    assert(precision(ground, lsh) == 1)
    assert(recall(ground, lsh) == 1)
  }

  test("precisionRecall2") {
    val ground = spark.parallelize(IndexedSeq(
      ("a", Set("b", "c", "d")),
      ("b", Set("b", "c", "d")),
      ("c", Set("b", "c", "d")),
      ("d", Set("b", "c", "d"))
    ))

    val lsh = spark.parallelize(IndexedSeq(
      ("a", Set("b", "c", "d")), // precision: 1, recall 1
      ("b", Set("b", "c", "e")), // precision: 2 / 3, recall: 2 / 3
      ("c", Set("b", "g")), // precision: 1 / 2, recall: 1/3
      ("d", Set("b", "c", "d", "e", "f", "g")) // precision: 1 / 2, recall = 1
    ))

    val precisionResult = precision(ground, lsh)
    val recallResult = recall(ground, lsh)

    println(precisionResult)
    println(recallResult)

    assert(((2 doubleValue()) / (3 doubleValue())) == precisionResult)
    assert((3 doubleValue()) / (4 doubleValue()) == recallResult)
  }
}
