package lsh

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object Main {
  private val conf = new SparkConf().setAppName("Session-group-15")
  private val sc = SparkContext.getOrCreate(conf)
  private val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  private val corpusFile = "/lsh-corpus-small.csv"

  def recall(ground_truth: RDD[(String, Set[String])], lsh_truth: RDD[(String, Set[String])]): Double = {
    /*
    * Compute the recall for each near-neighbor LSH query against the accurate result
    * Then, compute the average across all queries
    * ground_truth: results of queries in (movie_name, [nn_movie_names]) format produced by ExactNN
    * lsh_truth: results of queries in (movie_name, [nn_movie_names]) format produced by an LSH Construction
    * returns average recall
    * */

    val results =
      ground_truth
        .join(lsh_truth)
        .map(_._2)
        .map { case (exact, estimated) => exact.intersect(estimated).size.toDouble / exact.size.toDouble }
        .sum()

    results / lsh_truth.count()
  }

  def precision(ground_truth: RDD[(String, Set[String])], lsh_truth: RDD[(String, Set[String])]): Double = {
    /*
    * Compute the precision for each near-neighbor LSH query against the accurate result
    * Then, compute the average across all queries
    * ground_truth: results of queries in (movie_name, [nn_movie_names]) format produced by ExactNN
    * lsh_truth: results of queries in (movie_name, [nn_movie_names]) format produced by an LSH Construction
    * returns average precision
    * */

    val results =
      ground_truth
        .join(lsh_truth)
        .map(_._2)
        .map { case (exact, estimated) => exact.intersect(estimated).size.toDouble / estimated.size.toDouble }
        .sum()

    results / lsh_truth.count()
  }

  def query(queryFile: String,
            composite: (SQLContext, RDD[(String, List[String])], Int, Int) => CompositeConstruction,
            r: Int,
            b: Int,
            requiredPrecision: Double,
            requiredRecall: Double): Unit = {

    val rddCorpus = loadRDD(sc, corpusFile)
    val rddQuery = loadRDD(sc, queryFile)

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

  def query2(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val corpus_file = new File(getClass.getResource("/lsh-corpus-small.csv").getFile).getPath

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/lsh-query-2.csv").getFile).getPath

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))


    val exact: Construction = new ExactNN(sqlContext, rdd_corpus, 0.3)
    val lsh: Construction = CompositeConstruction.orAndBase(sqlContext, rdd_corpus, 5, 9)

    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)

    val resultRecall = recall(ground, res)
    val resultPrecision = precision(ground, res)
    println(s"Recall: $resultRecall")
    println(s"Precision: $resultPrecision")

    assert(resultRecall > 0.9)
    assert(resultPrecision > 0.45)
  }

  def query0(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val corpus_file = new File(getClass.getResource("/lsh_test_corpus.csv").getFile).getPath

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    //    val rdd_corpus = loadRDD(sqlContext, "/lsh-corpus-small.csv")

    val query_file = new File(getClass.getResource("/lsh_test_query.csv").getFile).getPath

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    //    val rdd_query = loadRDD(sqlContext, "/lsh-query-0.csv")

    val exact: Construction = new ExactNN(sqlContext, rdd_corpus, 0.3)
    val lsh: Construction = CompositeConstruction.andOrBroadcast(sqlContext, rdd_corpus, 5, 5)

    val ground = exact.eval(rdd_query)
    //    ground.collect().foreach(println)
    val res = lsh.eval(rdd_query)
    res.collect().foreach(println)

    println(s"Recall: ${recall(ground, res)}")
    println(s"Precision: ${precision(ground, res)}")

    assert(recall(ground, res) > 0.83)
    assert(precision(ground, res) > 0.70)
  }

  def main(args: Array[String]) {
    // query0(sc, sqlContext)
    // query1(sc, sqlContext)
    query2(sc, sqlContext)
  }

  def query(sc: SparkContext, sqlContext: SQLContext, corpusFile: String, queryFile: String):
  (RDD[(String, Set[String])], RDD[(String, Set[String])]) = {

    val rddCorpus = loadRDD(sc, corpusFile)
    val rddQuery = loadRDD(sc, queryFile)

    val exact: Construction = new ExactNN(sqlContext, rddCorpus, 0.3)
    val lsh: Construction = CompositeConstruction.andOrBase(sqlContext, rddCorpus, 5, 5)

    val ground = exact.eval(rddQuery)
    val res = lsh.eval(rddQuery)

    (ground, res)
  }

  def query1(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val corpus_file = new File(getClass.getResource("/lsh-corpus-small.csv").getFile).getPath

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/lsh-query-1.csv").getFile).getPath

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val exact: Construction = new ExactNN(sqlContext, rdd_corpus, 0.3)
    val lsh: Construction = CompositeConstruction.andOrBase(sqlContext, rdd_corpus, 5, 5)

    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)

    val resultRecall = recall(ground, res)
    val resultPrecision = precision(ground, res)
    println(s"Recall: $resultRecall")
    println(s"Precision: $resultPrecision")

    assert(resultRecall > 0.7)
    assert(resultPrecision > 0.98)
  }

  def loadRDD(sc: SparkContext, filename: String): RDD[(String, List[String])] = {
    val input = new File(getClass.getResource(filename).getFile).getPath
    sc
      .textFile(input)
      .map(x => x.split('|'))
      .map(x => (x(0), x.slice(1, x.length).toList))
  }
}
